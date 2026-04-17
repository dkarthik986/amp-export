package com.swift.platform.service;

import jakarta.annotation.PostConstruct;
import com.swift.platform.config.AppConfig;
import com.swift.platform.dto.ExportJobCreateRequest;
import com.swift.platform.dto.ExportJobListResponse;
import com.swift.platform.dto.ExportJobResponse;
import com.swift.platform.dto.SearchResponse;
import com.swift.platform.model.ExportJob;
import com.swift.platform.model.ExportJobStatus;
import com.swift.platform.repository.ExportJobRepository;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.gridfs.GridFsResource;
import org.springframework.data.mongodb.gridfs.GridFsTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Service
public class ExportJobService {

    private static final List<String> DEFAULT_TARGET_KEYS = List.of("table");
    private static final List<String> SUPPORTED_FORMATS = List.of("csv", "excel", "json", "pdf", "word", "xml", "txt", "rje", "dospcc");

    private final ExportJobRepository exportJobRepository;
    private final SearchService searchService;
    private final ExportRenderService exportRenderService;
    private final AppConfig appConfig;
    private final Executor exportJobExecutor;
    private final MongoTemplate mongoTemplate;
    private final GridFsTemplate gridFsTemplate;
    private final String instanceId = UUID.randomUUID().toString();
    private final AtomicBoolean localWorkerBusy = new AtomicBoolean(false);

    public ExportJobService(ExportJobRepository exportJobRepository,
                            SearchService searchService,
                            ExportRenderService exportRenderService,
                            AppConfig appConfig,
                            @Qualifier("exportJobExecutor") Executor exportJobExecutor,
                            MongoTemplate mongoTemplate,
                            GridFsTemplate gridFsTemplate) {
        this.exportJobRepository = exportJobRepository;
        this.searchService = searchService;
        this.exportRenderService = exportRenderService;
        this.appConfig = appConfig;
        this.exportJobExecutor = exportJobExecutor;
        this.mongoTemplate = mongoTemplate;
        this.gridFsTemplate = gridFsTemplate;
    }

    @PostConstruct
    void resumeQueuedExports() {
        dispatchNextQueuedJob();
    }

    public ExportJobResponse createJob(ExportJobCreateRequest request, String requestedBy, String requestedByName) {
        String requester = requestedBy == null || requestedBy.isBlank() ? "anonymous" : requestedBy;
        String format = normalizeFormat(request == null ? null : request.getFormat());
        if (!SUPPORTED_FORMATS.contains(format)) {
            throw new IllegalArgumentException("Unsupported export format: " + format);
        }

        long totalCount = resolveTotalCount(request);
        if (totalCount <= appConfig.getExportBackgroundThreshold()) {
            throw new IllegalArgumentException("Background export is available only above " + appConfig.getExportBackgroundThreshold() + " messages.");
        }

        ExportJob job = ExportJob.builder()
                .requestedBy(requester)
                .requestedByName(requestedByName)
                .requestedFormat(format)
                .scope(normalizeScope(request == null ? null : request.getScope()))
                .selectedSections(normalizeTargetKeys(request == null ? null : request.getTargetKeys()))
                .columns(request == null || request.getColumns() == null ? Collections.emptyList() : List.copyOf(request.getColumns()))
                .filters(normalizeFilters(request == null ? null : request.getFilters()))
                .references(request == null || request.getReferences() == null ? Collections.emptyList() : List.copyOf(request.getReferences()))
                .totalCount(totalCount)
                .processedCount(0L)
                .progressPercentage(0)
                .status(ExportJobStatus.QUEUED)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();

        ExportJob saved = exportJobRepository.save(job);
        dispatchNextQueuedJob();
        return toResponse(saved);
    }

    public ExportJobResponse getJob(String jobId, String requestedBy) {
        return toResponse(loadOwnedJob(jobId, requestedBy));
    }

    public ExportJobResponse cancelJob(String jobId, String requestedBy) {
        ExportJob job = loadOwnedJob(jobId, requestedBy);
        if (job.getStatus() == ExportJobStatus.COMPLETED
                || job.getStatus() == ExportJobStatus.FAILED
                || job.getStatus() == ExportJobStatus.CANCELLED) {
            return toResponse(job);
        }

        job.setStatus(ExportJobStatus.CANCELLED);
        job.setCompletedAt(Instant.now());
        job.setUpdatedAt(Instant.now());
        job.setErrorMessage("Export cancelled by user.");
        exportJobRepository.save(job);
        dispatchNextQueuedJob();
        return toResponse(job);
    }

    public ExportJobListResponse getRecentJobs(String requestedBy, int limit) {
        String requester = requestedBy == null || requestedBy.isBlank() ? "anonymous" : requestedBy;
        int safeLimit = Math.min(Math.max(limit, 1), 50);
        List<ExportJobResponse> jobs = exportJobRepository
                .findByRequestedByOrderByCreatedAtDesc(requester, PageRequest.of(0, safeLimit))
                .stream()
                .map(this::toResponse)
                .toList();
        return new ExportJobListResponse(appConfig.getExportJobPollIntervalMs(), jobs);
    }

    public JobDownload resolveDownload(String jobId, String requestedBy) {
        ExportJob job = loadOwnedJob(jobId, requestedBy);
        if (job.getStatus() != ExportJobStatus.COMPLETED) {
            throw new IllegalStateException("Export is not ready for download.");
        }

        if (notBlank(job.getOutputStorageId())) {
            GridFsResource resource = loadGridFsResource(job.getOutputStorageId());
            if (resource == null || !resource.exists()) {
                throw new IllegalStateException("Export file is no longer available.");
            }
            return new JobDownload(resource, job.getOutputFileName(), job.getContentType());
        }

        if (!notBlank(job.getOutputPath())) {
            throw new IllegalStateException("Export file is no longer available.");
        }
        Path path = Path.of(job.getOutputPath());
        if (!Files.exists(path)) {
            throw new IllegalStateException("Export file is no longer available.");
        }
        return new JobDownload(new FileSystemResource(path), job.getOutputFileName(), job.getContentType());
    }

    @Scheduled(
            fixedDelayString = "${search.export.dispatch-poll-interval-ms:5000}",
            initialDelayString = "${search.export.dispatch-poll-interval-ms:5000}"
    )
    public void pollQueuedJobs() {
        dispatchNextQueuedJob();
    }

    @Scheduled(
            fixedDelayString = "#{T(java.time.Duration).ofHours(${search.export.cleanup-interval-hours:6}).toMillis()}",
            initialDelayString = "#{T(java.time.Duration).ofMinutes(${search.export.cleanup-initial-delay-minutes:30}).toMillis()}"
    )
    public void cleanupExpiredJobs() {
        Instant cutoff = Instant.now().minus(Duration.ofHours(Math.max(1, appConfig.getExportCleanupRetentionHours())));
        for (ExportJob job : exportJobRepository.findByCompletedAtBefore(cutoff)) {
            deleteJobArtifacts(job);
            exportJobRepository.delete(job);
        }
    }

    public record JobDownload(Resource resource, String fileName, String contentType) {}

    private void processJob(String jobId) {
        try {
            ExportJob job = exportJobRepository.findById(jobId).orElse(null);
            if (job == null || job.getStatus() == ExportJobStatus.CANCELLED) {
                return;
            }

            Path jobDirectory = jobDirectory(jobId);
            ensureJobActive(jobId);
            Files.createDirectories(jobDirectory);
            if (isMtRawExport(job.getRequestedFormat())) {
                CompletedArtifact artifact = writeMtRawPayloadExport(job, jobDirectory);
                markCompleted(jobId, artifact);
                return;
            }

            List<Path> partFiles = new ArrayList<>();
            List<SearchResponse> batch = new ArrayList<>();
            AtomicInteger partIndex = new AtomicInteger(1);
            AtomicLong processed = new AtomicLong(0);

            consumeJobMessages(job, response -> {
                batch.add(response);
                long nextProcessed = processed.incrementAndGet();
                persistProgress(jobId, nextProcessed, job.getTotalCount());
                if (batch.size() >= Math.max(1, appConfig.getExportMaxRecordsPerFile())) {
                    partFiles.add(writeChunk(job, jobDirectory, batch, partIndex.getAndIncrement()));
                    batch.clear();
                }
            }, missingCount -> persistProgress(jobId, processed.addAndGet(missingCount), job.getTotalCount()));

            if (!batch.isEmpty() || partFiles.isEmpty()) {
                partFiles.add(writeChunk(job, jobDirectory, batch, partIndex.getAndIncrement()));
                batch.clear();
            }

            CompletedArtifact artifact = finalizeArtifacts(job, jobDirectory, partFiles);
            markCompleted(jobId, artifact);
        } catch (ExportJobCancelledException ex) {
            deleteJobDirectory(jobDirectory(jobId));
            markCancelled(jobId, ex.getMessage());
        } catch (Exception ex) {
            deleteJobDirectory(jobDirectory(jobId));
            markFailed(jobId, ex);
        } finally {
            localWorkerBusy.set(false);
            dispatchNextQueuedJob();
        }
    }

    private void consumeJobMessages(ExportJob job,
                                    ThrowingConsumer<SearchResponse> consumer,
                                    ThrowingLongConsumer missingProgressConsumer) throws Exception {
        List<String> references = job.getReferences() == null ? Collections.emptyList() : job.getReferences();
        if (!references.isEmpty()) {
            int batchSize = Math.max(1, appConfig.getPayloadFetchBatchSize());
            for (int start = 0; start < references.size(); start += batchSize) {
                ensureJobActive(job.getId());
                List<String> batchRefs = references.subList(start, Math.min(start + batchSize, references.size()));
                Map<String, SearchResponse> byReference = new LinkedHashMap<>();
                for (SearchResponse response : searchService.getMessageDetailsByReferences(batchRefs)) {
                    if (response.getReference() != null) {
                        byReference.put(response.getReference(), response);
                    }
                }
                long missing = 0;
                for (String reference : batchRefs) {
                    ensureJobActive(job.getId());
                    SearchResponse response = byReference.get(reference);
                    if (response != null) {
                        consumer.accept(response);
                    } else {
                        missing += 1;
                    }
                }
                if (missing > 0) {
                    missingProgressConsumer.accept(missing);
                }
            }
            return;
        }

        searchService.forEachDetailedExportResponse(job.getFilters(), response -> {
            try {
                ensureJobActive(job.getId());
                consumer.accept(response);
            } catch (ExportJobCancelledException ex) {
                throw ex;
            } catch (RuntimeException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    private Path writeChunk(ExportJob job,
                            Path jobDirectory,
                            List<SearchResponse> batch,
                            int partIndex) {
        try {
            ensureJobActive(job.getId());
            return exportRenderService.renderChunk(
                    jobDirectory,
                    exportRenderService.buildBaseFileName(job.getRequestedFormat(), job.getSelectedSections(), job.getScope()),
                    job.getRequestedFormat(),
                    List.copyOf(batch),
                    job.getSelectedSections(),
                    job.getColumns(),
                    partIndex
            );
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private CompletedArtifact finalizeArtifacts(ExportJob job,
                                                Path jobDirectory,
                                                List<Path> partFiles) throws IOException {
        ensureJobActive(job.getId());
        if (partFiles.size() == 1) {
            Path single = partFiles.get(0);
            String targetName = exportRenderService.buildBaseFileName(job.getRequestedFormat(), job.getSelectedSections(), job.getScope())
                    + "." + exportRenderService.extensionFor(job.getRequestedFormat());
            Path finalPath = single.resolveSibling(targetName);
            Files.move(single, finalPath, StandardCopyOption.REPLACE_EXISTING);
            return new CompletedArtifact(finalPath, finalPath.getFileName().toString(), exportRenderService.contentTypeFor(job.getRequestedFormat()));
        }

        Path zipPath = jobDirectory.resolve(exportRenderService.buildBaseFileName(job.getRequestedFormat(), job.getSelectedSections(), job.getScope()) + ".zip");
        try (OutputStream outputStream = Files.newOutputStream(zipPath);
             ZipOutputStream zipOutputStream = new ZipOutputStream(outputStream, StandardCharsets.UTF_8)) {
            for (Path partFile : partFiles) {
                ensureJobActive(job.getId());
                zipOutputStream.putNextEntry(new ZipEntry(partFile.getFileName().toString()));
                Files.copy(partFile, zipOutputStream);
                zipOutputStream.closeEntry();
            }
        }
        for (Path partFile : partFiles) {
            Files.deleteIfExists(partFile);
        }
        return new CompletedArtifact(zipPath, zipPath.getFileName().toString(), "application/zip");
    }

    private CompletedArtifact writeMtRawPayloadExport(ExportJob job, Path jobDirectory) throws Exception {
        String baseName = exportRenderService.buildBaseFileName(job.getRequestedFormat(), job.getSelectedSections(), job.getScope());
        String extension = "rje".equals(job.getRequestedFormat()) ? "rje" : "dos";
        Path outputFile = jobDirectory.resolve(baseName + "." + extension);
        AtomicLong processed = new AtomicLong(0);
        AtomicLong exported = new AtomicLong(0);

        if ("rje".equals(job.getRequestedFormat())) {
            try (BufferedWriter writer = Files.newBufferedWriter(outputFile, StandardCharsets.US_ASCII)) {
                consumeJobMessages(job, response -> {
                    ensureJobActive(job.getId());
                    String payload = normalizeCrLf(rawPayloadFor(response));
                    long nextProcessed = processed.incrementAndGet();
                    if (isMtMessage(response) && !payload.isBlank()) {
                        writer.write(payload);
                        writer.write("$");
                        exported.incrementAndGet();
                    }
                    persistProgress(job.getId(), nextProcessed, job.getTotalCount());
                }, missingCount -> persistProgress(job.getId(), processed.addAndGet(missingCount), job.getTotalCount()));
            }
        } else {
            try (OutputStream outputStream = Files.newOutputStream(outputFile)) {
                consumeJobMessages(job, response -> {
                    ensureJobActive(job.getId());
                    String payload = normalizeCrLf(rawPayloadFor(response));
                    long nextProcessed = processed.incrementAndGet();
                    if (isMtMessage(response) && !payload.isBlank()) {
                        outputStream.write(buildDosPccChunk(payload));
                        exported.incrementAndGet();
                    }
                    persistProgress(job.getId(), nextProcessed, job.getTotalCount());
                }, missingCount -> persistProgress(job.getId(), processed.addAndGet(missingCount), job.getTotalCount()));
            }
        }

        if (exported.get() == 0) {
            throw new IllegalStateException("No MT raw payload data was available for this export.");
        }
        return new CompletedArtifact(outputFile, outputFile.getFileName().toString(), "rje".equals(job.getRequestedFormat()) ? "text/plain;charset=US-ASCII" : "application/octet-stream");
    }

    private byte[] buildDosPccChunk(String payload) {
        byte[] body = payload.getBytes(StandardCharsets.US_ASCII);
        byte[] framed = new byte[body.length + 2];
        framed[0] = 0x01;
        System.arraycopy(body, 0, framed, 1, body.length);
        framed[framed.length - 1] = 0x03;

        int sectorSize = 512;
        int paddedLength = (int) Math.ceil(framed.length / (double) sectorSize) * sectorSize;
        byte[] padded = new byte[paddedLength];
        java.util.Arrays.fill(padded, (byte) 0x20);
        System.arraycopy(framed, 0, padded, 0, framed.length);
        return padded;
    }

    private boolean isMtRawExport(String format) {
        return "rje".equals(format) || "dospcc".equals(format);
    }

    private boolean isMtMessage(SearchResponse response) {
        String messageType = response.getMessageType();
        String messageCode = response.getMessageCode();
        return (messageType != null && "MT".equalsIgnoreCase(messageType))
                || (messageCode != null && messageCode.toUpperCase(Locale.ROOT).startsWith("MT"));
    }

    private String rawPayloadFor(SearchResponse response) {
        if (response.getRawFin() != null && !response.getRawFin().isBlank()) {
            return response.getRawFin();
        }
        if (response.getRawMessage() != null) {
            Object payload = response.getRawMessage().get("mtPayload");
            if (payload instanceof Map<?, ?> map) {
                Object rawFin = map.get("rawFin");
                if (rawFin != null) {
                    return String.valueOf(rawFin);
                }
            }
        }
        return "";
    }

    private String normalizeCrLf(String value) {
        return value == null ? "" : value.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\r\n");
    }

    private void persistProgress(String jobId, long processedCount, Long totalCount) {
        Optional<ExportJob> currentOpt = exportJobRepository.findById(jobId);
        if (currentOpt.isEmpty()) {
            return;
        }
        ExportJob current = currentOpt.get();
        current.setProcessedCount(processedCount);
        current.setProgressPercentage(progressPercentage(processedCount, totalCount));
        current.setUpdatedAt(Instant.now());
        if (processedCount % Math.max(1, appConfig.getExportProgressSaveInterval()) == 0 || processedCount >= Math.max(0L, totalCount == null ? 0L : totalCount)) {
            exportJobRepository.save(current);
        }
    }

    private void updateStatus(ExportJob job, ExportJobStatus status, String errorMessage) {
        ExportJob current = exportJobRepository.findById(job.getId()).orElse(job);
        if (current.getStatus() == ExportJobStatus.CANCELLED && status == ExportJobStatus.PROCESSING) {
            return;
        }
        current.setStatus(status);
        current.setErrorMessage(errorMessage);
        current.setUpdatedAt(Instant.now());
        if (status == ExportJobStatus.FAILED) {
            current.setCompletedAt(Instant.now());
        }
        exportJobRepository.save(current);
    }

    private void markCompleted(String jobId, CompletedArtifact artifact) {
        ExportJob job = exportJobRepository.findById(jobId).orElseThrow();
        if (job.getStatus() == ExportJobStatus.CANCELLED) {
            try {
                Files.deleteIfExists(artifact.path());
            } catch (IOException ignored) {
            }
            return;
        }
        StoredArtifact storedArtifact = storeArtifact(job, artifact);
        ExportJob latest = exportJobRepository.findById(jobId).orElseThrow();
        if (latest.getStatus() == ExportJobStatus.CANCELLED) {
            deleteStoredArtifact(ExportJob.builder().outputStorageId(storedArtifact.storageId()).build());
            if (storedArtifact.outputPath() != null) {
                try {
                    Files.deleteIfExists(Path.of(storedArtifact.outputPath()));
                } catch (IOException ignored) {
                }
            }
            return;
        }
        job.setStatus(ExportJobStatus.COMPLETED);
        job.setProcessedCount(job.getTotalCount());
        job.setProgressPercentage(100);
        job.setCompletedAt(Instant.now());
        job.setUpdatedAt(Instant.now());
        job.setOutputPath(storedArtifact.outputPath());
        job.setOutputStorageId(storedArtifact.storageId());
        job.setOutputFileName(storedArtifact.fileName());
        job.setContentType(storedArtifact.contentType());
        job.setErrorMessage(null);
        exportJobRepository.save(job);
    }

    private void markFailed(String jobId, Exception ex) {
        ExportJob job = exportJobRepository.findById(jobId).orElse(null);
        if (job == null) {
            return;
        }
        if (job.getStatus() == ExportJobStatus.CANCELLED) {
            return;
        }
        job.setStatus(ExportJobStatus.FAILED);
        job.setCompletedAt(Instant.now());
        job.setUpdatedAt(Instant.now());
        job.setErrorMessage(ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage());
        exportJobRepository.save(job);
    }

    private void markCancelled(String jobId, String message) {
        ExportJob job = exportJobRepository.findById(jobId).orElse(null);
        if (job == null) {
            return;
        }
        job.setStatus(ExportJobStatus.CANCELLED);
        job.setCompletedAt(job.getCompletedAt() == null ? Instant.now() : job.getCompletedAt());
        job.setUpdatedAt(Instant.now());
        job.setErrorMessage(message == null || message.isBlank() ? "Export cancelled by user." : message);
        exportJobRepository.save(job);
    }

    private ExportJob loadOwnedJob(String jobId, String requestedBy) {
        String requester = requestedBy == null || requestedBy.isBlank() ? "anonymous" : requestedBy;
        return exportJobRepository.findByIdAndRequestedBy(jobId, requester)
                .orElseThrow(() -> new IllegalArgumentException("Export job not found."));
    }

    private ExportJobResponse toResponse(ExportJob job) {
        boolean downloadReady = job.getStatus() == ExportJobStatus.COMPLETED && isArtifactAvailable(job);
        return ExportJobResponse.builder()
                .id(job.getId())
                .requestedBy(job.getRequestedBy())
                .requestedByName(job.getRequestedByName())
                .requestedFormat(job.getRequestedFormat())
                .scope(job.getScope())
                .selectedSections(job.getSelectedSections())
                .totalCount(job.getTotalCount())
                .processedCount(job.getProcessedCount())
                .progressPercentage(job.getProgressPercentage())
                .status(job.getStatus())
                .createdAt(job.getCreatedAt())
                .updatedAt(job.getUpdatedAt())
                .completedAt(job.getCompletedAt())
                .outputFileName(job.getOutputFileName())
                .contentType(job.getContentType())
                .errorMessage(job.getErrorMessage())
                .downloadReady(downloadReady)
                .downloadUrl(downloadReady ? "/api/export-jobs/" + job.getId() + "/download" : null)
                .build();
    }

    private long resolveTotalCount(ExportJobCreateRequest request) {
        if (request != null && request.getReferences() != null && !request.getReferences().isEmpty()) {
            return request.getReferences().stream().filter(ref -> ref != null && !ref.isBlank()).count();
        }
        if (request != null && request.getTotalCount() != null && request.getTotalCount() > 0) {
            return request.getTotalCount();
        }
        return searchService.countSearchResults(normalizeFilters(request == null ? null : request.getFilters()));
    }

    private String normalizeFormat(String format) {
        return format == null ? "" : format.trim().toLowerCase(Locale.ROOT);
    }

    private String normalizeScope(String scope) {
        return scope == null || scope.isBlank() ? "all" : scope.trim().toLowerCase(Locale.ROOT);
    }

    private List<String> normalizeTargetKeys(List<String> targetKeys) {
        if (targetKeys == null || targetKeys.isEmpty()) {
            return DEFAULT_TARGET_KEYS;
        }
        List<String> normalized = new ArrayList<>();
        for (String targetKey : targetKeys) {
            if (targetKey != null && !targetKey.isBlank()) {
                normalized.add(targetKey.trim().toLowerCase(Locale.ROOT));
            }
        }
        return normalized.isEmpty() ? DEFAULT_TARGET_KEYS : normalized;
    }

    private Map<String, String> normalizeFilters(Map<String, String> filters) {
        Map<String, String> normalized = new LinkedHashMap<>();
        if (filters == null) {
            return normalized;
        }
        filters.forEach((key, value) -> {
            if (key != null && value != null && !value.isBlank()) {
                normalized.put(key, value);
            }
        });
        return normalized;
    }

    private int progressPercentage(long processedCount, Long totalCount) {
        long total = totalCount == null || totalCount <= 0 ? 0 : totalCount;
        if (total <= 0) {
            return 0;
        }
        return (int) Math.min(100, Math.round((processedCount * 100.0) / total));
    }

    private Path jobDirectory(String jobId) {
        return Path.of(appConfig.getExportOutputDir()).resolve(jobId);
    }

    private void deleteJobArtifacts(ExportJob job) {
        deleteStoredArtifact(job);
        if (job.getOutputPath() != null && !job.getOutputPath().isBlank()) {
            try {
                Files.deleteIfExists(Path.of(job.getOutputPath()));
            } catch (IOException ignored) {
            }
        }
        try {
            Path directory = jobDirectory(job.getId());
            if (Files.isDirectory(directory)) {
                try (Stream<Path> files = Files.list(directory)) {
                    if (files.findAny().isEmpty()) {
                        Files.deleteIfExists(directory);
                    }
                }
            }
        } catch (Exception ignored) {
        }
    }

    private boolean isArtifactAvailable(ExportJob job) {
        if (job == null) {
            return false;
        }
        if (notBlank(job.getOutputStorageId())) {
            return gridFsFileExists(job.getOutputStorageId());
        }
        return notBlank(job.getOutputPath()) && Files.exists(Path.of(job.getOutputPath()));
    }

    private StoredArtifact storeArtifact(ExportJob job, CompletedArtifact artifact) {
        if (!appConfig.isExportUseGridFs()) {
            return new StoredArtifact(artifact.path().toString(), null, artifact.fileName(), artifact.contentType());
        }

        try (InputStream inputStream = Files.newInputStream(artifact.path())) {
            Document metadata = new Document("jobId", job.getId())
                    .append("requestedBy", job.getRequestedBy())
                    .append("format", job.getRequestedFormat())
                    .append("scope", job.getScope())
                    .append("contentType", artifact.contentType())
                    .append("createdAt", Instant.now().toString());
            ObjectId fileId = gridFsTemplate.store(inputStream, artifact.fileName(), artifact.contentType(), metadata);
            deleteJobDirectory(artifact.path().getParent());
            return new StoredArtifact(null, fileId.toHexString(), artifact.fileName(), artifact.contentType());
        } catch (IOException ex) {
            throw new RuntimeException("Failed to store export in MongoDB.", ex);
        }
    }

    private GridFsResource loadGridFsResource(String storageId) {
        if (!notBlank(storageId)) {
            return null;
        }
        try {
            var gridFsFile = gridFsTemplate.findOne(new Query(Criteria.where("_id").is(new ObjectId(storageId))));
            return gridFsFile == null ? null : gridFsTemplate.getResource(gridFsFile);
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    private boolean gridFsFileExists(String storageId) {
        return loadGridFsResource(storageId) != null;
    }

    private void deleteStoredArtifact(ExportJob job) {
        if (job == null || !notBlank(job.getOutputStorageId())) {
            return;
        }
        try {
            gridFsTemplate.delete(new Query(Criteria.where("_id").is(new ObjectId(job.getOutputStorageId()))));
        } catch (IllegalArgumentException ignored) {
        }
    }

    private void deleteJobDirectory(Path directory) {
        if (directory == null || !Files.exists(directory)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(directory)) {
            paths.sorted((left, right) -> right.compareTo(left))
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException ignored) {
                        }
                    });
        } catch (IOException ignored) {
        }
    }

    private void ensureJobActive(String jobId) {
        ExportJob current = exportJobRepository.findById(jobId).orElseThrow(() ->
                new ExportJobCancelledException("Export job no longer exists."));
        if (current.getStatus() == ExportJobStatus.CANCELLED) {
            throw new ExportJobCancelledException("Export cancelled by user.");
        }
    }

    private void dispatchNextQueuedJob() {
        if (!localWorkerBusy.compareAndSet(false, true)) {
            return;
        }
        if (!tryAcquireDispatchLock()) {
            localWorkerBusy.set(false);
            return;
        }
        try {
            ExportJob nextJob = claimNextQueuedJob();
            if (nextJob != null) {
                try {
                    exportJobExecutor.execute(() -> processJob(nextJob.getId()));
                } catch (RuntimeException ex) {
                    localWorkerBusy.set(false);
                    throw ex;
                }
                return;
            }
        } finally {
            releaseDispatchLock();
        }
        localWorkerBusy.set(false);
    }

    private ExportJob claimNextQueuedJob() {
        Query query = new Query(Criteria.where("status").is(ExportJobStatus.QUEUED)).limit(1);
        query.with(org.springframework.data.domain.Sort.by(org.springframework.data.domain.Sort.Direction.ASC, "createdAt"));
        Update update = new Update()
                .set("status", ExportJobStatus.PROCESSING)
                .set("updatedAt", Instant.now())
                .set("completedAt", null)
                .set("errorMessage", null);
        return mongoTemplate.findAndModify(query, update, FindAndModifyOptions.options().returnNew(true), ExportJob.class);
    }

    private boolean tryAcquireDispatchLock() {
        Instant now = Instant.now();
        Instant expiresAt = now.plusSeconds(Math.max(5, appConfig.getExportDispatchLockTimeoutSeconds()));
        Query query = new Query(new Criteria().andOperator(
                Criteria.where("_id").is("dispatch"),
                new Criteria().orOperator(
                        Criteria.where("expiresAt").lt(now),
                        Criteria.where("ownerId").is(instanceId),
                        Criteria.where("expiresAt").exists(false)
                )
        ));
        Update update = new Update()
                .set("ownerId", instanceId)
                .set("expiresAt", expiresAt)
                .set("updatedAt", now);
        Document claimed = mongoTemplate.findAndModify(
                query,
                update,
                FindAndModifyOptions.options().upsert(true).returnNew(true),
                Document.class,
                appConfig.getExportJobLocksCollection()
        );
        return claimed != null && instanceId.equals(claimed.getString("ownerId"));
    }

    private void releaseDispatchLock() {
        Query query = new Query(new Criteria().andOperator(
                Criteria.where("_id").is("dispatch"),
                Criteria.where("ownerId").is(instanceId)
        ));
        Update update = new Update()
                .set("expiresAt", Instant.now())
                .set("updatedAt", Instant.now());
        mongoTemplate.updateFirst(query, update, appConfig.getExportJobLocksCollection());
    }

    private boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }

    private record CompletedArtifact(Path path, String fileName, String contentType) {}
    private record StoredArtifact(String outputPath, String storageId, String fileName, String contentType) {}

    private static class ExportJobCancelledException extends RuntimeException {
        private ExportJobCancelledException(String message) {
            super(message);
        }
    }

    @FunctionalInterface
    private interface ThrowingConsumer<T> {
        void accept(T value) throws Exception;
    }

    @FunctionalInterface
    private interface ThrowingLongConsumer {
        void accept(long value) throws Exception;
    }
}
