package com.swift.platform.service;

import com.swift.platform.config.AppConfig;
import com.swift.platform.dto.DropdownOptionsResponse;
import com.swift.platform.dto.ExportColumnRequest;
import com.swift.platform.dto.PagedResponse;
import com.swift.platform.dto.SearchResponse;
import com.mongodb.client.model.ReplaceOptions;
import lombok.RequiredArgsConstructor;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import com.mongodb.client.FindIterable;

@Service
@RequiredArgsConstructor
public class SearchService {

    private static final String PAYLOAD_ALIAS = "payloadDoc";
    private static final int DROPDOWN_SNAPSHOT_VERSION = 2;
    private static final int EXCEL_HARD_MAX_ROWS_PER_SHEET = 1_048_575;
    private static final int EXCEL_MAX_CELL_CHARS = 32_767;
    private static final Pattern TEXT_PAYLOAD_TAG_LINE_PATTERN = Pattern.compile("^\\s*(.*?)\\s*:(\\d{2}[A-Z]?):\\s*(.*)$");
    private static final Pattern GENERIC_TEXT_PAYLOAD_LABEL_PATTERN = Pattern.compile("(?i)^line\\s+\\d+.*$");
    private static final Map<String, Field> SEARCH_RESPONSE_FIELDS = initSearchResponseFields();
    private static final Set<String> LOOKUP_REQUIRED_PARAMS = Set.of(
            "mur", "correspondent", "block4Tag", "block4Value", "freeSearchText"
    );

    private static final Set<String> HANDLED_PARAMS = Set.of(
            "messageType", "messageCode", "io", "status", "phase", "action", "reason", "messagePriority",
            "networkProtocol", "networkChannel", "networkPriority", "deliveryMode", "service",
            "backendChannelProtocol", "backendChannelCode",
            "owner", "workflow", "workflowModel", "originatorApplication", "sourceSystem",
            "processingType", "processPriority", "profileCode",
            "ccy", "sender", "receiver", "correspondent",
            "possibleDuplicate", "digestMCheckResult", "digest2CheckResult",
            "reference", "transactionReference", "transferReference", "relatedReference", "mur", "uetr",
            "mxInputReference", "mxOutputReference", "networkReference", "e2eMessageId", "amlDetails",
            "logicalTerminalAddress", "applicationId", "serviceId", "finReceiversAddress",
            "startDate", "endDate", "valueDateFrom", "valueDateTo",
            "statusDateFrom", "statusDateTo", "receivedDateFrom", "receivedDateTo",
            "amountFrom", "amountTo", "seqFrom", "seqTo", "sessionNumber",
            "historyEntity", "historyDescription", "historyPhase", "historyAction", "historyUser", "historyChannel",
            "block4Tag", "block4Value", "freeSearchText", "page", "size"
    );

    private static final Map<String, String> FIN_TAG_LABELS = Map.ofEntries(
            Map.entry("20", "Transaction Reference Number"),
            Map.entry("21", "Related Reference"),
            Map.entry("23B", "Bank Operation Code"),
            Map.entry("32A", "Value Date / Currency / Interbank Settled Amount"),
            Map.entry("33B", "Currency / Instructed Amount"),
            Map.entry("50A", "Ordering Customer (BIC)"),
            Map.entry("50F", "Ordering Customer (Structured)"),
            Map.entry("50K", "Ordering Customer"),
            Map.entry("52A", "Ordering Institution (BIC)"),
            Map.entry("52D", "Ordering Institution"),
            Map.entry("53A", "Sender's Correspondent (BIC)"),
            Map.entry("53B", "Sender's Correspondent (Location)"),
            Map.entry("53D", "Sender's Correspondent"),
            Map.entry("56A", "Intermediary Institution (BIC)"),
            Map.entry("57A", "Account With Institution (BIC)"),
            Map.entry("57C", "Account With Institution (Account)"),
            Map.entry("57D", "Account With Institution"),
            Map.entry("59", "Beneficiary Customer"),
            Map.entry("59A", "Beneficiary Customer (BIC)"),
            Map.entry("70", "Remittance Information"),
            Map.entry("71A", "Details of Charges"),
            Map.entry("71F", "Sender's Charges"),
            Map.entry("71G", "Receiver's Charges"),
            Map.entry("72", "Sender to Receiver Information")
    );

    private final MongoTemplate mongoTemplate;
    private final AppConfig appConfig;
    private volatile CachedValue<Long> unfilteredCountCache;

    public DropdownOptionsResponse getDropdownOptions() {
        DropdownOptionsResponse response = readDropdownOptionsSnapshot();
        return response != null ? response : refreshDropdownOptionsSnapshot();
    }

    @Scheduled(
            fixedDelayString = "#{T(java.time.Duration).ofHours(${search.dropdown.refresh-interval-hours:6}).toMillis()}",
            initialDelayString = "#{T(java.time.Duration).ofMinutes(${search.dropdown.refresh-initial-delay-minutes:5}).toMillis()}"
    )
    public void refreshDropdownOptionsSnapshotOnSchedule() {
        if (!appConfig.isDropdownRefreshEnabled()) {
            return;
        }
        refreshDropdownOptionsSnapshot();
    }

    public DropdownOptionsResponse refreshDropdownOptionsSnapshot() {
        DropdownOptionsResponse response = buildFreshDropdownOptions();
        saveDropdownOptionsSnapshot(response);
        return response;
    }

    private DropdownOptionsResponse buildFreshDropdownOptions() {
        String messagesCol = appConfig.getSwiftCollection();
        String payloadsCol = appConfig.getPayloadsCollection();

        DropdownOptionsResponse res = new DropdownOptionsResponse();
        res.setFormats(Arrays.asList("MT", "MX"));

        List<String> mtCodes = distinctMessageCodesByFamily(messagesCol, "MT");
        List<String> mxCodes = distinctMessageCodesByFamily(messagesCol, "MX");
        LinkedHashSet<String> mergedCodes = new LinkedHashSet<>();
        mergedCodes.addAll(mtCodes);
        mergedCodes.addAll(mxCodes);
        mergedCodes.addAll(distinctMerged(messagesCol, "messageTypeCode", "header.messageTypeCode"));
        List<String> codes = mergedCodes.stream().sorted().collect(Collectors.toList());

        res.setMessageCodes(codes);
        res.setTypes(codes);
        res.setMtTypes(mtCodes);
        res.setMxTypes(mxCodes);
        res.setAllMtMxTypes(Collections.emptyList());

        res.setStatuses(distinctMerged(messagesCol, "currentStatus", "status.current"));
        res.setPhases(distinctMerged(messagesCol, "statusPhase", "status.phase"));
        res.setActions(distinctMerged(messagesCol, "statusAction", "status.action"));
        res.setIoDirections(distinctMerged(messagesCol, "direction", "header.direction"));
        res.setDirections(distinctMerged(messagesCol, "direction", "header.direction"));
        res.setReasons(distinctMerged(messagesCol, "statusReason", "status.reason"));

        res.setNetworkProtocols(distinctMerged(messagesCol, "protocol", "header.protocol"));
        res.setNetworks(distinctMerged(messagesCol, "protocol", "header.protocol"));
        res.setNetworkChannels(distinctMerged(messagesCol, "networkChannel", "header.networkChannel"));
        res.setBackendChannels(distinctMerged(messagesCol, "backendChannel", "header.backendChannel"));
        res.setNetworkPriorities(distinctMerged(messagesCol, "networkPriority", "header.networkPriority"));
        res.setNetworkStatuses(Collections.emptyList());
        res.setDeliveryModes(distinctMerged(messagesCol, "communicationType", "channel.communicationType"));
        res.setServices(distinctMerged(messagesCol, "service", "header.service"));

        res.setSenders(distinctMerged(messagesCol, "senderAddress", "header.senderAddress"));
        res.setReceivers(distinctMerged(messagesCol, "receiverAddress", "header.receiverAddress"));
        res.setCountries(Collections.emptyList());
        res.setOriginCountries(Collections.emptyList());
        res.setDestinationCountries(Collections.emptyList());

        res.setOwners(distinctMerged(messagesCol, "owner", "header.owner"));
        res.setOwnerUnits(distinctMerged(messagesCol, "owner", "header.owner"));
        res.setWorkflows(distinctMerged(messagesCol, "workflow", "header.workflow"));
        res.setWorkflowModels(distinctMerged(messagesCol, "workflowModel", "header.workflowModel"));
        res.setSourceSystems(distinctMerged(messagesCol, "originatorApplication", "header.originatorApplication"));
        res.setOriginatorApplications(distinctMerged(messagesCol, "originatorApplication", "header.originatorApplication"));

        List<String> currencies = new ArrayList<>(distinctMerged(messagesCol, "ampCurrency", "extractedFields.currency"));
        currencies.addAll(distinct(payloadsCol, "currency"));
        res.setCurrencies(currencies.stream()
                .filter(value -> value != null && !value.isBlank())
                .collect(Collectors.toCollection(LinkedHashSet::new))
                .stream().sorted().collect(Collectors.toList()));

        res.setProcessingTypes(distinctMerged(messagesCol, "processingType", "header.processingType"));
        res.setProcessPriorities(distinctMerged(messagesCol, "processPriority", "header.processPriority"));
        res.setProfileCodes(distinctMerged(messagesCol, "profileCode", "header.profileCode"));
        res.setEnvironments(Collections.emptyList());

        res.setAmlStatuses(Collections.emptyList());
        res.setFinCopies(Collections.emptyList());
        res.setFinCopyServices(Collections.emptyList());
        res.setMessagePriorities(distinctMerged(messagesCol, "finMessagePriority", "protocolParams.messagePriority"));
        res.setNackCodes(Collections.emptyList());
        res.setCopyIndicators(Collections.emptyList());
        return res;
    }

    private DropdownOptionsResponse readDropdownOptionsSnapshot() {
        try {
            List<DropdownFieldSpec> specs = dropdownFieldSpecs();
            List<String> snapshotIds = specs.stream()
                    .map(spec -> buildDropdownSnapshotId(spec.key()))
                    .collect(Collectors.toList());

            List<Document> docs = mongoTemplate.getCollection(appConfig.getDropdownOptionsCollection())
                    .find(new Document("_id", new Document("$in", snapshotIds)))
                    .into(new ArrayList<>());

            if (docs.size() < specs.size()) {
                return null;
            }

            Map<String, List<String>> valuesByField = new HashMap<>();
            for (Document doc : docs) {
                Object snapshotVersion = doc.get("schemaVersion");
                if (!(snapshotVersion instanceof Number number) || number.intValue() != DROPDOWN_SNAPSHOT_VERSION) {
                    return null;
                }
                String field = firstNonBlank(doc.getString("field"), extractFieldKeyFromSnapshotId(doc.getString("_id")));
                if (notBlank(field)) {
                    valuesByField.put(field, normalizeDropdownValues(toStringList(doc.get("values"))));
                }
            }

            if (specs.stream().map(DropdownFieldSpec::key).anyMatch(field -> !valuesByField.containsKey(field))) {
                return null;
            }

            DropdownOptionsResponse response = new DropdownOptionsResponse();
            for (DropdownFieldSpec spec : specs) {
                spec.setter().accept(response, valuesByField.getOrDefault(spec.key(), Collections.emptyList()));
            }
            return response;
        } catch (Exception ignored) {
            return null;
        }
    }

    private void saveDropdownOptionsSnapshot(DropdownOptionsResponse response) {
        if (response == null) {
            return;
        }

        ReplaceOptions upsert = new ReplaceOptions().upsert(true);
        String collectionName = appConfig.getDropdownOptionsCollection();
        String groupId = appConfig.getDropdownOptionsDocumentId();
        String updatedAt = Instant.now().toString();

        for (DropdownFieldSpec spec : dropdownFieldSpecs()) {
            List<String> values = normalizeDropdownValues(spec.getter().apply(response));
            Document doc = new Document("_id", buildDropdownSnapshotId(spec.key()))
                    .append("groupId", groupId)
                    .append("schemaVersion", DROPDOWN_SNAPSHOT_VERSION)
                    .append("field", spec.key())
                    .append("values", values)
                    .append("updatedAt", updatedAt);
            mongoTemplate.getCollection(collectionName)
                    .replaceOne(new Document("_id", doc.getString("_id")), doc, upsert);
        }
    }

    private String buildDropdownSnapshotId(String fieldKey) {
        return appConfig.getDropdownOptionsDocumentId() + ":" + fieldKey;
    }

    private String extractFieldKeyFromSnapshotId(String snapshotId) {
        if (!notBlank(snapshotId)) {
            return null;
        }
        int separatorIndex = snapshotId.indexOf(':');
        if (separatorIndex < 0 || separatorIndex >= snapshotId.length() - 1) {
            return snapshotId;
        }
        return snapshotId.substring(separatorIndex + 1);
    }

    private List<String> normalizeDropdownValues(List<String> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }
        return values.stream()
                .filter(this::notBlank)
                .map(String::trim)
                .collect(Collectors.toCollection(LinkedHashSet::new))
                .stream()
                .sorted()
                .collect(Collectors.toList());
    }

    private List<DropdownFieldSpec> dropdownFieldSpecs() {
        return List.of(
                new DropdownFieldSpec("formats", DropdownOptionsResponse::setFormats, DropdownOptionsResponse::getFormats),
                new DropdownFieldSpec("messageCodes", DropdownOptionsResponse::setMessageCodes, DropdownOptionsResponse::getMessageCodes),
                new DropdownFieldSpec("types", DropdownOptionsResponse::setTypes, DropdownOptionsResponse::getTypes),
                new DropdownFieldSpec("mtTypes", DropdownOptionsResponse::setMtTypes, DropdownOptionsResponse::getMtTypes),
                new DropdownFieldSpec("mxTypes", DropdownOptionsResponse::setMxTypes, DropdownOptionsResponse::getMxTypes),
                new DropdownFieldSpec("allMtMxTypes", DropdownOptionsResponse::setAllMtMxTypes, DropdownOptionsResponse::getAllMtMxTypes),
                new DropdownFieldSpec("statuses", DropdownOptionsResponse::setStatuses, DropdownOptionsResponse::getStatuses),
                new DropdownFieldSpec("phases", DropdownOptionsResponse::setPhases, DropdownOptionsResponse::getPhases),
                new DropdownFieldSpec("actions", DropdownOptionsResponse::setActions, DropdownOptionsResponse::getActions),
                new DropdownFieldSpec("ioDirections", DropdownOptionsResponse::setIoDirections, DropdownOptionsResponse::getIoDirections),
                new DropdownFieldSpec("directions", DropdownOptionsResponse::setDirections, DropdownOptionsResponse::getDirections),
                new DropdownFieldSpec("networkProtocols", DropdownOptionsResponse::setNetworkProtocols, DropdownOptionsResponse::getNetworkProtocols),
                new DropdownFieldSpec("networks", DropdownOptionsResponse::setNetworks, DropdownOptionsResponse::getNetworks),
                new DropdownFieldSpec("networkChannels", DropdownOptionsResponse::setNetworkChannels, DropdownOptionsResponse::getNetworkChannels),
                new DropdownFieldSpec("backendChannels", DropdownOptionsResponse::setBackendChannels, DropdownOptionsResponse::getBackendChannels),
                new DropdownFieldSpec("networkPriorities", DropdownOptionsResponse::setNetworkPriorities, DropdownOptionsResponse::getNetworkPriorities),
                new DropdownFieldSpec("networkStatuses", DropdownOptionsResponse::setNetworkStatuses, DropdownOptionsResponse::getNetworkStatuses),
                new DropdownFieldSpec("deliveryModes", DropdownOptionsResponse::setDeliveryModes, DropdownOptionsResponse::getDeliveryModes),
                new DropdownFieldSpec("services", DropdownOptionsResponse::setServices, DropdownOptionsResponse::getServices),
                new DropdownFieldSpec("senders", DropdownOptionsResponse::setSenders, DropdownOptionsResponse::getSenders),
                new DropdownFieldSpec("receivers", DropdownOptionsResponse::setReceivers, DropdownOptionsResponse::getReceivers),
                new DropdownFieldSpec("countries", DropdownOptionsResponse::setCountries, DropdownOptionsResponse::getCountries),
                new DropdownFieldSpec("originCountries", DropdownOptionsResponse::setOriginCountries, DropdownOptionsResponse::getOriginCountries),
                new DropdownFieldSpec("destinationCountries", DropdownOptionsResponse::setDestinationCountries, DropdownOptionsResponse::getDestinationCountries),
                new DropdownFieldSpec("owners", DropdownOptionsResponse::setOwners, DropdownOptionsResponse::getOwners),
                new DropdownFieldSpec("ownerUnits", DropdownOptionsResponse::setOwnerUnits, DropdownOptionsResponse::getOwnerUnits),
                new DropdownFieldSpec("workflows", DropdownOptionsResponse::setWorkflows, DropdownOptionsResponse::getWorkflows),
                new DropdownFieldSpec("workflowModels", DropdownOptionsResponse::setWorkflowModels, DropdownOptionsResponse::getWorkflowModels),
                new DropdownFieldSpec("sourceSystems", DropdownOptionsResponse::setSourceSystems, DropdownOptionsResponse::getSourceSystems),
                new DropdownFieldSpec("originatorApplications", DropdownOptionsResponse::setOriginatorApplications, DropdownOptionsResponse::getOriginatorApplications),
                new DropdownFieldSpec("currencies", DropdownOptionsResponse::setCurrencies, DropdownOptionsResponse::getCurrencies),
                new DropdownFieldSpec("processingTypes", DropdownOptionsResponse::setProcessingTypes, DropdownOptionsResponse::getProcessingTypes),
                new DropdownFieldSpec("processPriorities", DropdownOptionsResponse::setProcessPriorities, DropdownOptionsResponse::getProcessPriorities),
                new DropdownFieldSpec("profileCodes", DropdownOptionsResponse::setProfileCodes, DropdownOptionsResponse::getProfileCodes),
                new DropdownFieldSpec("environments", DropdownOptionsResponse::setEnvironments, DropdownOptionsResponse::getEnvironments),
                new DropdownFieldSpec("amlStatuses", DropdownOptionsResponse::setAmlStatuses, DropdownOptionsResponse::getAmlStatuses),
                new DropdownFieldSpec("finCopies", DropdownOptionsResponse::setFinCopies, DropdownOptionsResponse::getFinCopies),
                new DropdownFieldSpec("finCopyServices", DropdownOptionsResponse::setFinCopyServices, DropdownOptionsResponse::getFinCopyServices),
                new DropdownFieldSpec("messagePriorities", DropdownOptionsResponse::setMessagePriorities, DropdownOptionsResponse::getMessagePriorities),
                new DropdownFieldSpec("nackCodes", DropdownOptionsResponse::setNackCodes, DropdownOptionsResponse::getNackCodes),
                new DropdownFieldSpec("copyIndicators", DropdownOptionsResponse::setCopyIndicators, DropdownOptionsResponse::getCopyIndicators),
                new DropdownFieldSpec("reasons", DropdownOptionsResponse::setReasons, DropdownOptionsResponse::getReasons)
        );
    }

    public PagedResponse<SearchResponse> search(Map<String, String> filters, int page, int size) {
        String messagesCol = appConfig.getSwiftCollection();
        int pageSize = Math.min(size, appConfig.getMaxPageSize());

        int safePage = Math.max(page, 0);
        SearchPlan plan = buildSearchPlan(filters);

        long total;
        List<SearchResponse> rows;
        if (appConfig.isOptimizeWithoutLookup() && !plan.requiresLookup()) {
            total = countMessages(messagesCol, plan.messageMatch(), filters);
            List<Document> docs = findMessagePage(messagesCol, plan.messageMatch(), safePage, pageSize);
            Map<String, Document> payloadByReference = fetchPayloadsByReference(docs);
            rows = docs.stream()
                    .map(doc -> toSearchRowResponse(withPayloadDoc(doc, payloadByReference.get(stringValue(doc.get("messageReference"))))))
                    .collect(Collectors.toList());
        } else {
            List<Document> basePipeline = buildLookupPipeline(plan);
            total = aggregateCount(messagesCol, basePipeline, filters);

            List<Document> rowsPipeline = new ArrayList<>(basePipeline);
            rowsPipeline.add(new Document("$sort", new Document("header.dateCreated", -1).append("dateCreated", -1)));
            rowsPipeline.add(new Document("$skip", (long) safePage * pageSize));
            rowsPipeline.add(new Document("$limit", pageSize));

            List<Document> docs = aggregate(messagesCol, rowsPipeline);
            rows = docs.stream().map(this::toSearchRowResponse).collect(Collectors.toList());
        }

        int totalPages = pageSize > 0 ? (int) Math.ceil((double) total / pageSize) : 0;
        return new PagedResponse<>(rows, total, totalPages, safePage, pageSize, safePage == 0, safePage >= totalPages - 1);
    }

    public List<SearchResponse> searchAllForExport(Map<String, String> filters) {
        List<SearchResponse> results = new ArrayList<>();
        forEachExportResponse(filters, results::add);
        return results;
    }

    public long countSearchResults(Map<String, String> filters) {
        String messagesCol = appConfig.getSwiftCollection();
        SearchPlan plan = buildSearchPlan(filters);
        if (appConfig.isOptimizeWithoutLookup() && !plan.requiresLookup()) {
            return countMessages(messagesCol, plan.messageMatch(), filters);
        }
        return aggregateCount(messagesCol, buildLookupPipeline(plan), filters);
    }

    public void forEachDetailedExportResponse(Map<String, String> filters, Consumer<SearchResponse> consumer) {
        forEachExportResponse(filters, consumer);
    }

    public void streamResultTableExport(Map<String, String> filters,
                                        List<ExportColumnRequest> columns,
                                        String format,
                                        OutputStream outputStream) throws IOException {
        List<ExportColumnRequest> safeColumns = normalizeExportColumns(columns);
        if ("excel".equalsIgnoreCase(format)) {
            streamResultTableExcel(filters, safeColumns, outputStream);
            return;
        }
        streamResultTableCsv(filters, safeColumns, outputStream);
    }

    public SearchResponse getMessageDetail(String reference) {
        if (!notBlank(reference)) {
            return null;
        }
        Document match = new Document("$or", List.of(
                new Document("messageReference", reference),
                new Document("header.messageReference", reference)
        ));
        List<Document> pipeline = buildLookupPipeline(new SearchPlan(match, new Document(), true));
        pipeline.add(new Document("$limit", 1));
        List<Document> docs = aggregate(appConfig.getSwiftCollection(), pipeline);
        return docs.isEmpty() ? null : toResponse(docs.get(0));
    }

    public List<SearchResponse> getMessageDetailsByReferences(List<String> references) {
        List<String> uniqueReferences = references == null ? Collections.emptyList() : references.stream()
                .filter(this::notBlank)
                .distinct()
                .collect(Collectors.toList());
        if (uniqueReferences.isEmpty()) {
            return Collections.emptyList();
        }

        List<Document> messageDocs = mongoTemplate.getCollection(appConfig.getSwiftCollection())
                .find(new Document("messageReference", new Document("$in", uniqueReferences)))
                .into(new ArrayList<>());
        Map<String, Document> payloadByReference = fetchPayloadsByReference(messageDocs);
        return messageDocs.stream()
                .map(doc -> toResponse(withPayloadDoc(doc, payloadByReference.get(stringValue(doc.get("messageReference"))))))
                .collect(Collectors.toList());
    }

    private List<Document> buildLookupPipeline(SearchPlan plan) {
        List<Document> pipeline = new ArrayList<>();
        pipeline.add(new Document("$lookup",
                new Document("from", appConfig.getPayloadsCollection())
                        .append("localField", "messageReference")
                        .append("foreignField", "messageReference")
                        .append("as", PAYLOAD_ALIAS)
        ));
        pipeline.add(new Document("$unwind",
                new Document("path", "$" + PAYLOAD_ALIAS)
                        .append("preserveNullAndEmptyArrays", true)
        ));

        if (!plan.messageMatch().isEmpty()) {
            pipeline.add(new Document("$match", plan.messageMatch()));
        }
        if (!plan.postLookupMatch().isEmpty()) {
            pipeline.add(new Document("$match", plan.postLookupMatch()));
        }
        return pipeline;
    }

    private SearchPlan buildSearchPlan(Map<String, String> filters) {
        boolean requiresLookup = requiresPayloadLookup(filters);
        if (!requiresLookup) {
            return new SearchPlan(buildMessageOnlyMatch(filters), new Document(), false);
        }
        return new SearchPlan(buildMessageOnlyMatch(filters), buildMatch(filters), true);
    }

    private boolean requiresPayloadLookup(Map<String, String> filters) {
        if (filters == null || filters.isEmpty()) {
            return false;
        }
        return filters.keySet().stream().anyMatch(param -> LOOKUP_REQUIRED_PARAMS.contains(param) || !HANDLED_PARAMS.contains(param));
    }

    private Document buildMessageOnlyMatch(Map<String, String> filters) {
        List<Document> clauses = new ArrayList<>();

        exactIfAny(clauses, filters, "messageType", "messageFamily");
        exactIfAny(clauses, filters, "messageCode", "messageTypeCode", "header.messageTypeCode");
        exactIfAny(clauses, filters, "io", "direction", "header.direction");
        exactIfAny(clauses, filters, "status", "currentStatus", "status.current");
        exactIfAny(clauses, filters, "phase", "statusPhase", "status.phase");
        exactIfAny(clauses, filters, "action", "statusAction", "status.action");
        exactIfAny(clauses, filters, "reason", "statusReason", "status.reason");
        exactIfAny(clauses, filters, "messagePriority", "finMessagePriority", "protocolParams.messagePriority");

        exactIfAny(clauses, filters, "networkProtocol", "protocol", "header.protocol");
        exactIfAny(clauses, filters, "networkChannel", "networkChannel", "header.networkChannel", "header.backendChannel");
        exactIfAny(clauses, filters, "networkPriority", "networkPriority", "header.networkPriority");
        exactIfAny(clauses, filters, "deliveryMode", "communicationType", "channel.communicationType");
        exactIfAny(clauses, filters, "service", "service", "header.service");
        exactIfAny(clauses, filters, "backendChannelProtocol", "channelProtocol", "channel.protocol");
        exactIfAny(clauses, filters, "backendChannelCode", "backendChannelCode", "channel.backendChannelCode");

        exactIfAny(clauses, filters, "owner", "owner", "header.owner");
        exactIfAny(clauses, filters, "workflow", "workflow", "header.workflow");
        exactIfAny(clauses, filters, "workflowModel", "workflowModel", "header.workflowModel");
        exactIfAny(clauses, filters, "originatorApplication", "originatorApplication", "header.originatorApplication");
        exactIfAny(clauses, filters, "sourceSystem", "originatorApplication", "header.originatorApplication");
        exactIfAny(clauses, filters, "processingType", "processingType", "header.processingType");
        exactIfAny(clauses, filters, "processPriority", "processPriority", "header.processPriority");
        exactIfAny(clauses, filters, "profileCode", "profileCode", "header.profileCode");

        exactIfAny(clauses, filters, "ccy", "ampCurrency", "extractedFields.currency");
        exactIfAny(clauses, filters, "sender", "senderAddress", "header.senderAddress");
        exactIfAny(clauses, filters, "receiver", "receiverAddress", "header.receiverAddress");

        booleanIfAny(clauses, filters, "possibleDuplicate", "pdeIndication", "header.pdeIndication");
        booleanIfAny(clauses, filters, "digestMCheckResult", "digestMCheckResult", "protocolParams.digestMCheckResult");
        booleanIfAny(clauses, filters, "digest2CheckResult", "digest2CheckResult", "protocolParams.digest2CheckResult");

        exactIfAny(clauses, filters, "reference", "messageReference", "header.messageReference");
        exactIfAny(clauses, filters, "transactionReference", "transactionReference", "header.transactionReference");
        exactIfAny(clauses, filters, "transferReference", "transferReference");
        exactIfAny(clauses, filters, "relatedReference", "relatedReference");
        exactIfAny(clauses, filters, "uetr", "uetr");
        exactIfAny(clauses, filters, "mxInputReference", "mxInputReference");
        exactIfAny(clauses, filters, "mxOutputReference", "mxOutputReference");
        exactIfAny(clauses, filters, "networkReference", "networkReference");
        regexIfAny(clauses, filters, "e2eMessageId", "e2eMessageId");
        regexIfAny(clauses, filters, "amlDetails", "amlDetails");

        exactIfAny(clauses, filters, "logicalTerminalAddress", "finLogicalTerminal", "protocolParams.logicalTerminal");
        exactIfAny(clauses, filters, "applicationId", "finAppId", "protocolParams.appId");
        exactIfAny(clauses, filters, "serviceId", "finServiceId", "protocolParams.serviceId");
        exactIfAny(clauses, filters, "sessionNumber", "finSessionNumber", "protocolParams.sessionNumber");
        exactIfAny(clauses, filters, "finReceiversAddress", "finReceiversAddress", "protocolParams.receiversAddress");

        dateRangeIfAny(clauses, filters, "startDate", "endDate", "dateCreated", "header.dateCreated");
        dateRangeIfAny(clauses, filters, "valueDateFrom", "valueDateTo", "ampValueDate", "extractedFields.valueDate");
        dateRangeIfAny(clauses, filters, "statusDateFrom", "statusDateTo", "statusDate", "status.date");
        dateRangeIfAny(clauses, filters, "receivedDateFrom", "receivedDateTo", "dateReceived", "header.dateReceived");

        numericRangeIf(clauses, filters, "amountFrom", "amountTo", false, "ampAmount", "extractedFields.amount");
        numericRangeIf(clauses, filters, "seqFrom", "seqTo", true, "finSequenceNumber", "protocolParams.sequenceNumber");

        addHistoryFilters(clauses, filters);

        if (clauses.isEmpty()) {
            return new Document();
        }
        return new Document("$and", clauses);
    }

    private Document buildMatch(Map<String, String> filters) {
        List<Document> clauses = new ArrayList<>();

        exactIfAny(clauses, filters, "messageType", "messageFamily");
        exactIfAny(clauses, filters, "messageCode", "messageTypeCode", "header.messageTypeCode");
        exactIfAny(clauses, filters, "io", "direction", "header.direction");
        exactIfAny(clauses, filters, "status", "currentStatus", "status.current");
        exactIfAny(clauses, filters, "phase", "statusPhase", "status.phase");
        exactIfAny(clauses, filters, "action", "statusAction", "status.action");
        exactIfAny(clauses, filters, "reason", "statusReason", "status.reason");
        exactIfAny(clauses, filters, "messagePriority", "finMessagePriority", "protocolParams.messagePriority");

        exactIfAny(clauses, filters, "networkProtocol", "protocol", "header.protocol");
        exactIfAny(clauses, filters, "networkChannel", "networkChannel", "header.networkChannel", "header.backendChannel");
        exactIfAny(clauses, filters, "networkPriority", "networkPriority", "header.networkPriority");
        exactIfAny(clauses, filters, "deliveryMode", "communicationType", "channel.communicationType");
        exactIfAny(clauses, filters, "service", "service", "header.service");
        exactIfAny(clauses, filters, "backendChannelProtocol", "channelProtocol", "channel.protocol");
        exactIfAny(clauses, filters, "backendChannelCode", "backendChannelCode", "channel.backendChannelCode");

        exactIfAny(clauses, filters, "owner", "owner", "header.owner");
        exactIfAny(clauses, filters, "workflow", "workflow", "header.workflow");
        exactIfAny(clauses, filters, "workflowModel", "workflowModel", "header.workflowModel");
        exactIfAny(clauses, filters, "originatorApplication", "originatorApplication", "header.originatorApplication");
        exactIfAny(clauses, filters, "sourceSystem", "originatorApplication", "header.originatorApplication");
        exactIfAny(clauses, filters, "processingType", "processingType", "header.processingType");
        exactIfAny(clauses, filters, "processPriority", "processPriority", "header.processPriority");
        exactIfAny(clauses, filters, "profileCode", "profileCode", "header.profileCode");

        exactIfAny(clauses, filters, "ccy", "ampCurrency", "extractedFields.currency", PAYLOAD_ALIAS + ".currency");

        exactIfAny(clauses, filters, "sender", "senderAddress", "header.senderAddress", PAYLOAD_ALIAS + ".senderAddress");
        exactIfAny(clauses, filters, "receiver", "receiverAddress", "header.receiverAddress", PAYLOAD_ALIAS + ".receiverAddress");
        regexIfAny(clauses, filters, "correspondent", "correspondent", PAYLOAD_ALIAS + ".senderCorrespondent");

        booleanIfAny(clauses, filters, "possibleDuplicate", "pdeIndication", "header.pdeIndication");
        booleanIfAny(clauses, filters, "digestMCheckResult", "digestMCheckResult", "protocolParams.digestMCheckResult");
        booleanIfAny(clauses, filters, "digest2CheckResult", "digest2CheckResult", "protocolParams.digest2CheckResult");

        exactIfAny(clauses, filters, "reference", "messageReference", "header.messageReference");
        exactIfAny(clauses, filters, "transactionReference", "transactionReference", "header.transactionReference");
        exactIfAny(clauses, filters, "transferReference", "transferReference");
        exactIfAny(clauses, filters, "relatedReference", "relatedReference");
        regexIfAny(clauses, filters, "mur", "mtPayload.transactionReference", PAYLOAD_ALIAS + ".mtParsedPayload.transactionReference");
        exactIfAny(clauses, filters, "uetr", "uetr");
        exactIfAny(clauses, filters, "mxInputReference", "mxInputReference");
        exactIfAny(clauses, filters, "mxOutputReference", "mxOutputReference");
        exactIfAny(clauses, filters, "networkReference", "networkReference");
        regexIfAny(clauses, filters, "e2eMessageId", "e2eMessageId");
        regexIfAny(clauses, filters, "amlDetails", "amlDetails");

        exactIfAny(clauses, filters, "logicalTerminalAddress",
                "finLogicalTerminal", "protocolParams.logicalTerminal", PAYLOAD_ALIAS + ".mtParsedPayload.block1.logicalTerminalAddress");
        exactIfAny(clauses, filters, "applicationId",
                "finAppId", "protocolParams.appId", PAYLOAD_ALIAS + ".mtParsedPayload.block1.applicationId");
        exactIfAny(clauses, filters, "serviceId",
                "finServiceId", "protocolParams.serviceId", PAYLOAD_ALIAS + ".mtParsedPayload.block1.serviceId");
        exactIfAny(clauses, filters, "sessionNumber",
                "finSessionNumber", "protocolParams.sessionNumber", PAYLOAD_ALIAS + ".mtParsedPayload.block1.sessionNumber");
        exactIfAny(clauses, filters, "finReceiversAddress",
                "finReceiversAddress", "protocolParams.receiversAddress", PAYLOAD_ALIAS + ".mtParsedPayload.block2.receiverAddress");

        dateRangeIfAny(clauses, filters, "startDate", "endDate", "dateCreated", "header.dateCreated");
        dateRangeIfAny(clauses, filters, "valueDateFrom", "valueDateTo", "ampValueDate", "extractedFields.valueDate");
        dateRangeIfAny(clauses, filters, "statusDateFrom", "statusDateTo", "statusDate", "status.date");
        dateRangeIfAny(clauses, filters, "receivedDateFrom", "receivedDateTo", "dateReceived", "header.dateReceived");

        numericRangeIf(clauses, filters, "amountFrom", "amountTo",
                false, "ampAmount", "extractedFields.amount", PAYLOAD_ALIAS + ".amount");
        numericRangeIf(clauses, filters, "seqFrom", "seqTo",
                true, "finSequenceNumber", "protocolParams.sequenceNumber", PAYLOAD_ALIAS + ".mtParsedPayload.block1.sequenceNumber");

        addHistoryFilters(clauses, filters);
        addPayloadFilters(clauses, filters);
        addFreeTextFilter(clauses, filters);

        filters.forEach((param, value) -> {
            if (!HANDLED_PARAMS.contains(param) && notBlank(value)) {
                clauses.add(buildDynamicClause(param, value));
            }
        });

        if (clauses.isEmpty()) {
            return new Document();
        }
        return new Document("$and", clauses);
    }

    private void addHistoryFilters(List<Document> clauses, Map<String, String> filters) {
        String historyEntity = filters.get("historyEntity");
        if (notBlank(historyEntity)) {
            clauses.add(new Document("historyLines",
                    new Document("$elemMatch", regexCondition("entity", historyEntity))));
        }

        String historyDescription = filters.get("historyDescription");
        if (notBlank(historyDescription)) {
            clauses.add(new Document("historyLines",
                    new Document("$elemMatch", regexCondition("comment", historyDescription))));
        }

        String historyPhase = filters.get("historyPhase");
        if (notBlank(historyPhase)) {
            clauses.add(new Document("historyLines",
                    new Document("$elemMatch", new Document("phase", historyPhase))));
        }

        String historyAction = filters.get("historyAction");
        if (notBlank(historyAction)) {
            clauses.add(new Document("historyLines",
                    new Document("$elemMatch", new Document("action", historyAction))));
        }

        String historyUser = filters.get("historyUser");
        if (notBlank(historyUser)) {
            clauses.add(new Document("historyLines",
                    new Document("$elemMatch", regexCondition("user", historyUser))));
        }

        String historyChannel = filters.get("historyChannel");
        if (notBlank(historyChannel)) {
            clauses.add(new Document("historyLines",
                    new Document("$elemMatch", regexCondition("channel", historyChannel))));
        }
    }

    private void addPayloadFilters(List<Document> clauses, Map<String, String> filters) {
        String tag = filters.get("block4Tag");
        String value = filters.get("block4Value");

        if (notBlank(tag)) {
            clauses.add(regexClause(PAYLOAD_ALIAS + ".mtParsedPayload.rawBlock4", ":" + tag + ":"));
        }

        if (notBlank(value)) {
            clauses.add(new Document("$or", Arrays.asList(
                    new Document("mtPayload.block4Fields",
                            new Document("$elemMatch", regexCondition("rawValue", value))),
                    regexClause(PAYLOAD_ALIAS + ".mtParsedPayload.rawBlock4", value),
                    regexClause(PAYLOAD_ALIAS + ".mtParsedPayload.rawFields._raw", value)
            )));
        }
    }

    private void addFreeTextFilter(List<Document> clauses, Map<String, String> filters) {
        String freeText = filters.get("freeSearchText");
        if (!notBlank(freeText)) {
            return;
        }

        List<Document> orClauses = new ArrayList<>(Arrays.asList(
                regexClause("messageReference", freeText),
                regexClause("header.messageReference", freeText),
                regexClause("transactionReference", freeText),
                regexClause("header.transactionReference", freeText),
                regexClause("senderAddress", freeText),
                regexClause("header.senderAddress", freeText),
                regexClause("receiverAddress", freeText),
                regexClause("header.receiverAddress", freeText),
                regexClause("senderName", freeText),
                regexClause("header.senderName", freeText),
                regexClause("receiverName", freeText),
                regexClause("header.receiverName", freeText),
                regexClause("owner", freeText),
                regexClause("header.owner", freeText),
                regexClause("workflow", freeText),
                regexClause("header.workflow", freeText),
                regexClause("currentStatus", freeText),
                regexClause("status.current", freeText),
                regexClause("statusMessage", freeText),
                regexClause("status.message", freeText),
                regexClause("body.rawPayload", freeText),
                regexClause(PAYLOAD_ALIAS + ".mtParsedPayload.transactionReference", freeText),
                regexClause(PAYLOAD_ALIAS + ".mtParsedPayload.rawBlock4", freeText),
                regexClause(PAYLOAD_ALIAS + ".orderingCustomer", freeText),
                regexClause(PAYLOAD_ALIAS + ".beneficiaryCustomer", freeText),
                regexClause(PAYLOAD_ALIAS + ".remittanceInfo", freeText)
        ));

        orClauses.add(new Document("historyLines",
                new Document("$elemMatch",
                        new Document("$or", Arrays.asList(
                                regexCondition("comment", freeText),
                                regexCondition("entity", freeText)
                        )))));

        clauses.add(new Document("$or", orClauses));
    }

    private void exactIfAny(List<Document> clauses, Map<String, String> filters, String paramKey, String... fieldPaths) {
        String value = filters.get(paramKey);
        if (!notBlank(value)) {
            return;
        }
        clauses.add(fieldPaths.length == 1
                ? new Document(fieldPaths[0], value)
                : new Document("$or", Arrays.stream(fieldPaths).map(path -> new Document(path, value)).collect(Collectors.toList())));
    }

    private void regexIfAny(List<Document> clauses, Map<String, String> filters, String paramKey, String... fieldPaths) {
        String value = filters.get(paramKey);
        if (!notBlank(value)) {
            return;
        }
        clauses.add(fieldPaths.length == 1
                ? regexClause(fieldPaths[0], value)
                : new Document("$or", Arrays.stream(fieldPaths).map(path -> regexClause(path, value)).collect(Collectors.toList())));
    }

    private void booleanIfAny(List<Document> clauses, Map<String, String> filters, String paramKey, String... fieldPaths) {
        String value = filters.get(paramKey);
        if (!notBlank(value)) {
            return;
        }

        boolean boolValue = Boolean.parseBoolean(value);
        List<Document> orClauses = new ArrayList<>();
        for (String fieldPath : fieldPaths) {
            orClauses.add(new Document(fieldPath, boolValue));
            orClauses.add(new Document(fieldPath, String.valueOf(boolValue)));
            orClauses.add(new Document(fieldPath, value));
        }
        clauses.add(new Document("$or", orClauses));
    }

    private void dateRangeIfAny(List<Document> clauses, Map<String, String> filters,
                                String fromKey, String toKey, String... fieldPaths) {
        String from = filters.get(fromKey);
        String to = filters.get(toKey);
        if (!notBlank(from) && !notBlank(to)) {
            return;
        }

        List<Document> pathClauses = new ArrayList<>();
        for (String fieldPath : fieldPaths) {
            Document range = new Document();
            if (notBlank(from)) {
                range.append("$gte", from);
            }
            if (notBlank(to)) {
                range.append("$lte", to + "T23:59:59Z");
            }
            pathClauses.add(new Document(fieldPath, range));
        }

        clauses.add(pathClauses.size() == 1 ? pathClauses.get(0) : new Document("$or", pathClauses));
    }

    private void numericRangeIf(List<Document> clauses, Map<String, String> filters,
                                String fromKey, String toKey, boolean integer, String... fieldPaths) {
        String from = filters.get(fromKey);
        String to = filters.get(toKey);
        if (!notBlank(from) && !notBlank(to)) {
            return;
        }

        try {
            List<Document> exprClauses = new ArrayList<>();
            Object fieldExpr = integer ? integerExpr(fieldPaths) : decimalExpr(fieldPaths);
            if (notBlank(from)) {
                exprClauses.add(new Document("$gte",
                        Arrays.asList(fieldExpr, integer ? Integer.parseInt(from.trim()) : Double.parseDouble(from.trim()))));
            }
            if (notBlank(to)) {
                exprClauses.add(new Document("$lte",
                        Arrays.asList(fieldExpr, integer ? Integer.parseInt(to.trim()) : Double.parseDouble(to.trim()))));
            }
            Object expr = exprClauses.size() == 1 ? exprClauses.get(0) : new Document("$and", exprClauses);
            clauses.add(new Document("$expr", expr));
        } catch (NumberFormatException ignored) {
        }
    }

    private Document buildDynamicClause(String param, String value) {
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            boolean boolValue = Boolean.parseBoolean(value);
            return new Document("$or", Arrays.asList(
                    new Document(param, boolValue),
                    new Document(param, String.valueOf(boolValue)),
                    new Document(param, value)
            ));
        }
        return regexClause(param, value);
    }

    private SearchResponse toResponse(Document doc) {
        SearchResponse response = new SearchResponse();

        Document header = documentAt(doc, "header");
        Document status = documentAt(doc, "status");
        Document protocolParams = documentAt(doc, "protocolParams");
        Document channel = documentAt(doc, "channel");
        Document extractedFields = documentAt(doc, "extractedFields");
        Document bulkInfo = documentAt(doc, "bulkInfo");
        Document body = documentAt(doc, "body");
        Document payloadDoc = documentAt(doc, PAYLOAD_ALIAS);
        Document mtParsedPayload = documentAt(payloadDoc, "mtParsedPayload");
        Document block1 = documentAt(mtParsedPayload, "block1");
        Document block2 = documentAt(mtParsedPayload, "block2");
        String messageType = firstNonBlank(doc.getString("messageFamily"), payloadDoc.getString("messageFamily"));
        String messageCode = firstNonBlank(doc.getString("messageTypeCode"), header.getString("messageTypeCode"), payloadDoc.getString("messageTypeCode"));

        String rawFin = firstNonBlank(
                stringValue(mtParsedPayloadAt(mtParsedPayload, "rawFields", "_raw")),
                firstNonBlank(payloadDoc.getString("rawFin"), mtParsedPayload.getString("rawFin")),
                composeRawFin(mtParsedPayload),
                body.getString("rawPayload")
        );
        List<Map<String, Object>> block4Fields = buildBlock4Fields(payloadDoc, mtParsedPayload, rawFin, messageType, messageCode);
        String rawBlock1 = firstNonBlank(mtParsedPayload.getString("rawBlock1"), extractBlock1Content(rawFin));

        response.setId(stringValue(doc.get("_id")));
        parseIntStr(firstNonBlank(
                doc.getString("finSequenceNumber"),
                protocolParams.getString("sequenceNumber"),
                block1.getString("sequenceNumber"),
                parseSequenceNumberFromBlock1(rawBlock1)
        ), response::setSequenceNumber);
        response.setSessionNumber(firstNonBlank(
                doc.getString("finSessionNumber"),
                protocolParams.getString("sessionNumber"),
                block1.getString("sessionNumber"),
                parseSessionNumberFromBlock1(rawBlock1)
        ));

        response.setMessageType(messageType);
        response.setMessageCode(messageCode);
        response.setMessageFormat(firstNonBlank(doc.getString("messageFormat"), header.getString("messageFormat"), payloadDoc.getString("messageFormat")));
        response.setMessageTypeDescription(firstNonBlank(doc.getString("messageTypeDescription"), header.getString("messageTypeDescription")));

        response.setStatus(firstNonBlank(doc.getString("currentStatus"), status.getString("current")));
        response.setPhase(firstNonBlank(doc.getString("statusPhase"), status.getString("phase")));
        response.setAction(firstNonBlank(doc.getString("statusAction"), status.getString("action")));
        response.setReason(firstNonBlank(doc.getString("statusReason"), status.getString("reason")));
        response.setStatusMessage(firstNonBlank(doc.getString("statusMessage"), status.getString("message")));
        response.setStatusChangeSource(firstNonBlank(doc.getString("statusChangeSource"), status.getString("changeSource")));
        response.setStatusDecision(firstNonBlank(doc.getString("statusDecision"), status.getString("decision")));
        response.setIo(firstNonBlank(doc.getString("direction"), header.getString("direction")));

        response.setCreationDate(firstNonBlank(doc.getString("dateCreated"), header.getString("dateCreated")));
        response.setReceivedDT(firstNonBlank(doc.getString("dateReceived"), header.getString("dateReceived")));
        response.setStatusDate(firstNonBlank(doc.getString("statusDate"), status.getString("date")));
        response.setValueDate(firstNonBlank(doc.getString("ampValueDate"), extractedFields.getString("valueDate")));

        response.setSender(firstNonBlank(doc.getString("senderAddress"), header.getString("senderAddress"), payloadDoc.getString("senderAddress")));
        response.setReceiver(firstNonBlank(doc.getString("receiverAddress"), header.getString("receiverAddress"), payloadDoc.getString("receiverAddress")));
        response.setSenderInstitutionName(firstNonBlank(doc.getString("senderName"), header.getString("senderName")));
        response.setReceiverInstitutionName(firstNonBlank(doc.getString("receiverName"), header.getString("receiverName")));

        response.setReference(firstNonBlank(doc.getString("messageReference"), header.getString("messageReference")));
        response.setTransactionReference(firstNonBlank(doc.getString("transactionReference"), header.getString("transactionReference")));

        response.setAmount(parseFlexibleDouble(firstNonBlank(doc.getString("ampAmount"), extractedFields.getString("amount"), payloadDoc.getString("amount"))));
        response.setCcy(firstNonBlank(doc.getString("ampCurrency"), extractedFields.getString("currency"), payloadDoc.getString("currency")));
        response.setDetailsOfCharges(firstNonBlank(doc.getString("ampDetailsOfCharges"), extractedFields.getString("detailsOfCharges"), payloadDoc.getString("detailsOfCharges"), mtParsedPayload.getString("detailsOfCharges")));
        response.setRemittanceInfo(firstNonBlank(doc.getString("ampRemittanceInformation"), extractedFields.getString("remittanceInformation"), payloadDoc.getString("remittanceInfo"), mtParsedPayload.getString("remittanceInfo")));

        response.setNetworkProtocol(firstNonBlank(doc.getString("protocol"), header.getString("protocol"), payloadDoc.getString("protocol")));
        response.setNetworkChannel(firstNonBlank(doc.getString("networkChannel"), header.getString("networkChannel"), header.getString("backendChannel")));
        response.setNetworkPriority(firstNonBlank(doc.getString("networkPriority"), header.getString("networkPriority")));
        response.setDeliveryMode(firstNonBlank(doc.getString("communicationType"), channel.getString("communicationType")));
        response.setCommunicationType(response.getDeliveryMode());
        response.setService(firstNonBlank(doc.getString("service"), header.getString("service")));
        response.setBackendChannel(firstNonBlank(doc.getString("backendChannel"), header.getString("backendChannel"), channel.getString("backendChannelName")));
        response.setBackendChannelCode(firstNonBlank(doc.getString("backendChannelCode"), channel.getString("backendChannelCode")));
        response.setBackendChannelDescription(firstNonBlank(doc.getString("backendChannelDescription"), channel.getString("backendChannelDescription")));
        response.setChannelCode(firstNonBlank(doc.getString("channelCode"), channel.getString("code")));
        response.setBackendChannelProtocol(firstNonBlank(doc.getString("channelProtocol"), channel.getString("protocol")));

        response.setOwner(firstNonBlank(doc.getString("owner"), header.getString("owner")));
        response.setWorkflow(firstNonBlank(doc.getString("workflow"), header.getString("workflow")));
        response.setWorkflowModel(firstNonBlank(doc.getString("workflowModel"), header.getString("workflowModel")));
        response.setProcessingType(firstNonBlank(doc.getString("processingType"), header.getString("processingType")));
        response.setProcessPriority(firstNonBlank(doc.getString("processPriority"), header.getString("processPriority")));
        response.setProfileCode(firstNonBlank(doc.getString("profileCode"), header.getString("profileCode")));
        response.setOriginatorApplication(firstNonBlank(doc.getString("originatorApplication"), header.getString("originatorApplication")));

        response.setApplicationId(firstNonBlank(doc.getString("finAppId"), protocolParams.getString("appId"), block1.getString("applicationId")));
        response.setServiceId(firstNonBlank(doc.getString("finServiceId"), protocolParams.getString("serviceId"), block1.getString("serviceId")));
        response.setLogicalTerminalAddress(firstNonBlank(doc.getString("finLogicalTerminal"), protocolParams.getString("logicalTerminal"), block1.getString("logicalTerminalAddress")));
        response.setMessagePriority(firstNonBlank(doc.getString("finMessagePriority"), protocolParams.getString("messagePriority"), block2.getString("messagePriority")));
        response.setFinDirectionId(firstNonBlank(doc.getString("finDirectionId"), protocolParams.getString("directionId"), block2.getString("directionId")));
        response.setFinMessageType(firstNonBlank(doc.getString("finMessageType"), protocolParams.getString("messageType"), block2.getString("messageType")));
        response.setFinReceiversAddress(firstNonBlank(doc.getString("finReceiversAddress"), protocolParams.getString("receiversAddress"), block2.getString("receiverAddress")));

        response.setDigestMCheckResult(firstNonBlank(doc.getString("digestMCheckResult"), stringValue(protocolParams.get("digestMCheckResult"))));
        response.setDigest2CheckResult(firstNonBlank(doc.getString("digest2CheckResult"), stringValue(protocolParams.get("digest2CheckResult"))));

        response.setBulkType(firstNonBlank(doc.getString("bulkType"), bulkInfo.getString("bulkType")));
        parseIntObj(firstNonBlankObject(doc.get("bulkSequenceNumber"), bulkInfo.get("sequenceNumber")), response::setBulkSequenceNumber);
        parseIntObj(firstNonBlankObject(doc.get("bulkTotalMessages"), bulkInfo.get("totalMessages")), response::setBulkTotalMessages);

        response.setPdeIndication(firstNonBlank(doc.getString("pdeIndication"), stringValue(header.get("pdeIndication"))));
        response.setPossibleDuplicate("true".equalsIgnoreCase(response.getPdeIndication()));

        response.setMur(firstNonBlank(
                documentAt(doc, "mtPayload").getString("transactionReference"),
                mtParsedPayload.getString("transactionReference"),
                payloadDoc.getString("messageReference")
        ));
        response.setBankOperationCode(firstNonBlank(documentAt(doc, "mtPayload").getString("bankOperationCode"), payloadDoc.getString("bankOperationCode"), mtParsedPayload.getString("bankOperationCode")));
        response.setPayloadCurrency(firstNonBlank(documentAt(doc, "mtPayload").getString("currency"), payloadDoc.getString("currency"), mtParsedPayload.getString("currency")));
        response.setPayloadValueDate(firstNonBlank(documentAt(doc, "mtPayload").getString("valueDate"), payloadDoc.getString("valueDate"), mtParsedPayload.getString("valueDate")));
        response.setInterbankSettledAmount(firstNonBlank(documentAt(doc, "mtPayload").getString("interbankSettledAmount"), mtParsedPayload.getString("interbankSettledAmount")));
        response.setInstructedCurrency(firstNonBlank(documentAt(doc, "mtPayload").getString("instructedCurrency"), payloadDoc.getString("instructedCurrency"), mtParsedPayload.getString("instructedCurrency")));
        response.setInstructedAmount(firstNonBlank(documentAt(doc, "mtPayload").getString("instructedAmount"), payloadDoc.getString("instructedAmount"), mtParsedPayload.getString("instructedAmount")));

        response.setOrderingCustomer(firstNonBlank(documentAt(doc, "mtPayload").getString("orderingCustomer"), payloadDoc.getString("orderingCustomer"), mtParsedPayload.getString("orderingCustomer")));
        response.setOrderingInstitution(firstNonBlank(documentAt(doc, "mtPayload").getString("orderingInstitution"), payloadDoc.getString("orderingInstitution"), mtParsedPayload.getString("orderingInstitution")));
        response.setSenderCorrespondent(firstNonBlank(documentAt(doc, "mtPayload").getString("senderCorrespondent"), payloadDoc.getString("senderCorrespondent"), mtParsedPayload.getString("senderCorrespondent")));
        response.setAccountWithInstitution(firstNonBlank(documentAt(doc, "mtPayload").getString("accountWithInstitution"), payloadDoc.getString("accountWithInstitution"), mtParsedPayload.getString("accountWithInstitution")));
        response.setBeneficiaryCustomer(firstNonBlank(documentAt(doc, "mtPayload").getString("beneficiaryCustomer"), payloadDoc.getString("beneficiaryCustomer"), mtParsedPayload.getString("beneficiaryCustomer")));

        response.setCorrespondent(firstNonBlank(response.getSenderCorrespondent(), doc.getString("correspondent")));

        response.setPayloadFieldCount(computePayloadFieldCount(mtParsedPayload, block4Fields));
        response.setPayloadSize(firstNonBlank(documentAt(doc, "mtPayload").getString("payloadSize"), payloadDoc.getString("payloadSize")));
        response.setRawFin(rawFin);
        response.setBlock4Fields(block4Fields);
        response.setHistoryLines(toMapList(doc.get("historyLines")));

        Document rawMessage = new Document(doc);
        rawMessage.remove(PAYLOAD_ALIAS);
        if (!payloadDoc.isEmpty()) {
            rawMessage.put("mtPayload", buildCompatibilityPayload(payloadDoc, mtParsedPayload, block4Fields, rawFin));
        }
        response.setRawMessage(new LinkedHashMap<>(rawMessage));

        response.setFormat(response.getMessageType());
        response.setType(response.getMessageCode());
        response.setDate(dateOnly(response.getCreationDate()));
        response.setTime(timeOnly(response.getCreationDate()));
        response.setDirection(response.getIo());
        response.setNetwork(response.getNetworkProtocol());
        response.setOwnerUnit(response.getOwner());
        response.setCurrency(response.getCcy());
        response.setFinCopy(response.getFinCopyService());
        response.setSourceSystem(response.getOriginatorApplication());

        return response;
    }

    private SearchResponse toSearchRowResponse(Document doc) {
        SearchResponse response = toResponse(doc);
        response.setRawFin(null);
        response.setBlock4Fields(Collections.emptyList());
        response.setHistoryLines(Collections.emptyList());
        response.setRawMessage(null);
        return response;
    }

    private Document buildCompatibilityPayload(Document payloadDoc, Document mtParsedPayload,
                                               List<Map<String, Object>> block4Fields, String rawFin) {
        Document compatibility = new Document();
        if (!mtParsedPayload.isEmpty()) {
            compatibility.putAll(mtParsedPayload);
        }
        compatibility.put("block1", new Document(documentAt(mtParsedPayload, "block1")));
        compatibility.put("block2", new Document(documentAt(mtParsedPayload, "block2")));
        compatibility.put("block4Fields", block4Fields);
        compatibility.put("rawFin", rawFin);
        compatibility.put("fieldCount", computePayloadFieldCount(mtParsedPayload, block4Fields));
        compatibility.put("payloadSize", payloadDoc.getString("payloadSize"));
        compatibility.put("payloadEncoding", payloadDoc.getString("payloadEncoding"));
        compatibility.put("textPayload", payloadDoc.getString("textPayload"));
        compatibility.put("digest", payloadDoc.getString("digest"));
        compatibility.put("digestAlgorithm", payloadDoc.getString("digestAlgorithm"));
        return compatibility;
    }

    private List<Map<String, Object>> buildBlock4Fields(Document payloadDoc, Document mtParsedPayload, String rawFin,
                                                        String messageType, String messageCode) {
        List<Map<String, Object>> existingRows = existingBlock4Fields(payloadDoc, mtParsedPayload);
        if (!existingRows.isEmpty()) {
            return enrichMtBlock4Labels(messageType, messageCode, existingRows);
        }

        Document rawFields = documentAt(mtParsedPayload, "rawFields");
        if (!rawFields.isEmpty()) {
            List<Map<String, Object>> rows = new ArrayList<>();
            for (Map.Entry<String, Object> entry : rawFields.entrySet()) {
                String tag = entry.getKey();
                if ("_raw".equals(tag)) {
                    continue;
                }
                List<String> values = toStringList(entry.getValue());
                if (values.isEmpty()) {
                    rows.add(block4Row(tag, null, null));
                    continue;
                }
                for (String value : values) {
                    rows.add(block4Row(tag, null, value));
                }
            }
            return enrichMtBlock4Labels(messageType, messageCode, rows);
        }

        List<Map<String, Object>> rawFinRows = buildBlock4FieldsFromRawFin(rawFin);
        if (!rawFinRows.isEmpty()) {
            return enrichMtBlock4Labels(messageType, messageCode, rawFinRows);
        }

        String textPayload = firstNonBlank(payloadDoc.getString("textPayload"), mtParsedPayload.getString("textPayload"));
        return enrichMtBlock4Labels(messageType, messageCode, buildBlock4FieldsFromTextPayload(textPayload));
    }

    private List<Map<String, Object>> buildBlock4FieldsFromRawFin(String rawFin) {
        String block4 = extractBlock4Content(rawFin);
        if (!notBlank(block4)) {
            return Collections.emptyList();
        }

        List<Map<String, Object>> rows = new ArrayList<>();
        String currentTag = null;
        StringBuilder currentValue = new StringBuilder();
        String normalized = block4.replace("\r\n", "\n").replace('\r', '\n');

        for (String line : normalized.split("\n")) {
            if (!notBlank(line)) {
                if (currentTag != null) {
                    currentValue.append('\n');
                }
                continue;
            }

            if ("-}".equals(line.trim())) {
                continue;
            }

            if (line.startsWith(":")) {
                int secondColon = line.indexOf(':', 1);
                if (secondColon > 1) {
                    if (currentTag != null) {
                        rows.add(block4Row(currentTag, null, cleanBlock4Value(currentValue.toString())));
                    }
                    currentTag = line.substring(1, secondColon).trim();
                    currentValue.setLength(0);
                    currentValue.append(line.substring(secondColon + 1));
                    continue;
                }
            }

            if (currentTag != null) {
                if (currentValue.length() > 0) {
                    currentValue.append('\n');
                }
                currentValue.append(line);
            }
        }

        if (currentTag != null) {
            rows.add(block4Row(currentTag, null, cleanBlock4Value(currentValue.toString())));
        }

        return rows;
    }

    private List<Map<String, Object>> buildBlock4FieldsFromTextPayload(String textPayload) {
        if (!notBlank(textPayload)) {
            return Collections.emptyList();
        }

        List<Map<String, Object>> rows = new ArrayList<>();
        String currentTag = null;
        String currentLabel = null;
        String pendingLabel = null;
        StringBuilder currentValue = new StringBuilder();
        String normalized = textPayload.replace("\r\n", "\n").replace('\r', '\n');

        for (String rawLine : normalized.split("\n")) {
            String line = rawLine == null ? "" : rawLine;
            String trimmed = line.trim();

            if (trimmed.isEmpty()) {
                if (currentTag != null && currentValue.length() > 0) {
                    currentValue.append('\n');
                }
                continue;
            }

            Matcher matcher = TEXT_PAYLOAD_TAG_LINE_PATTERN.matcher(line);
            if (matcher.matches()) {
                if (currentTag != null) {
                    rows.add(block4Row(currentTag, currentLabel, cleanBlock4Value(currentValue.toString())));
                }
                currentTag = matcher.group(2).trim();
                currentLabel = resolveTextPayloadLabel(matcher.group(1), pendingLabel);
                pendingLabel = null;
                currentValue.setLength(0);
                String initialValue = cleanBlock4Value(matcher.group(3));
                if (notBlank(initialValue)) {
                    currentValue.append(initialValue);
                }
                continue;
            }

            if (currentTag != null) {
                if (currentValue.length() > 0) {
                    currentValue.append('\n');
                }
                currentValue.append(line.stripTrailing());
            } else {
                pendingLabel = trimmed;
            }
        }

        if (currentTag != null) {
            rows.add(block4Row(currentTag, currentLabel, cleanBlock4Value(currentValue.toString())));
        }

        return rows;
    }

    private Map<String, Object> block4Row(String tag, String label, String value) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("tag", tag);
        row.put("label", label);
        row.put("rawValue", value == null || value.isBlank() ? "—" : value);
        row.put("components", Collections.emptyMap());
        return row;
    }

    private String resolveTextPayloadLabel(String inlineLabel, String pendingLabel) {
        String cleanedInlineLabel = cleanTextPayloadLabel(inlineLabel);
        String cleanedPendingLabel = cleanTextPayloadLabel(pendingLabel);
        if (notBlank(cleanedInlineLabel) && !GENERIC_TEXT_PAYLOAD_LABEL_PATTERN.matcher(cleanedInlineLabel).matches()) {
            return cleanedInlineLabel;
        }
        if (notBlank(cleanedPendingLabel)) {
            return cleanedPendingLabel;
        }
        return notBlank(cleanedInlineLabel) ? cleanedInlineLabel : null;
    }

    private String cleanTextPayloadLabel(String label) {
        if (!notBlank(label)) {
            return null;
        }
        String cleaned = label.strip();
        return cleaned.isEmpty() ? null : cleaned;
    }

    private List<Map<String, Object>> existingBlock4Fields(Document payloadDoc, Document mtParsedPayload) {
        List<Map<String, Object>> payloadLevel = toMapList(payloadDoc.get("block4Fields"));
        if (!payloadLevel.isEmpty()) {
            return payloadLevel.stream().map(this::normalizeBlock4FieldRow).collect(Collectors.toList());
        }

        List<Map<String, Object>> parsedLevel = toMapList(mtParsedPayload.get("block4Fields"));
        if (!parsedLevel.isEmpty()) {
            return parsedLevel.stream().map(this::normalizeBlock4FieldRow).collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    private Map<String, Object> normalizeBlock4FieldRow(Map<String, Object> source) {
        Map<String, Object> row = new LinkedHashMap<>(source);
        row.put("tag", stringValue(source.get("tag")));
        row.put("label", stringValue(source.get("label")));
        row.put("rawValue", firstNonBlank(stringValue(source.get("rawValue")), "â€”"));
        Object components = source.get("components");
        row.put("components", components instanceof Map<?, ?> ? components : Collections.emptyMap());
        return row;
    }

    private List<Map<String, Object>> enrichMtBlock4Labels(String messageType, String messageCode, List<Map<String, Object>> rows) {
        if (rows == null || rows.isEmpty()) {
            return Collections.emptyList();
        }

        List<Map<String, Object>> normalizedRows = rows.stream()
                .map(this::normalizeBlock4FieldRow)
                .collect(Collectors.toList());

        if (!isMtMessage(messageType, messageCode)) {
            normalizedRows.forEach(this::applyBuiltInLabelIfMissing);
            return normalizedRows;
        }

        List<String> missingTags = normalizedRows.stream()
                .filter(row -> shouldLookupMtLabel(stringValue(row.get("label"))))
                .map(row -> stringValue(row.get("tag")))
                .filter(this::notBlank)
                .distinct()
                .collect(Collectors.toList());

        Map<String, String> mongoLabelsByTag = lookupMtLabels(messageCode, missingTags);

        for (Map<String, Object> row : normalizedRows) {
            String existingLabel = stringValue(row.get("label"));
            if (!shouldLookupMtLabel(existingLabel)) {
                continue;
            }

            String tag = stringValue(row.get("tag"));
            String resolvedLabel = mongoLabelsByTag.get(tag);
            if (!notBlank(resolvedLabel)) {
                resolvedLabel = FIN_TAG_LABELS.get(tag);
            }
            if (notBlank(resolvedLabel)) {
                row.put("label", resolvedLabel);
            }
        }

        return normalizedRows;
    }

    private Map<String, String> lookupMtLabels(String messageCode, List<String> tags) {
        if (!notBlank(messageCode) || tags == null || tags.isEmpty()) {
            return Collections.emptyMap();
        }

        List<String> messageTypeCandidates = buildMtLabelMessageTypeCandidates(messageCode);
        List<Document> docs = mongoTemplate.getCollection(appConfig.getMtLabelsCollection())
                .find(new Document("messageType", new Document("$in", messageTypeCandidates))
                        .append("tag", new Document("$in", tags)))
                .into(new ArrayList<>());

        Map<String, Integer> messageTypePriority = new HashMap<>();
        for (int i = 0; i < messageTypeCandidates.size(); i++) {
            messageTypePriority.put(messageTypeCandidates.get(i), i);
        }

        Map<String, List<Document>> byTag = docs.stream()
                .filter(doc -> notBlank(doc.getString("tag")) && notBlank(doc.getString("label")))
                .collect(Collectors.groupingBy(doc -> doc.getString("tag")));

        Map<String, String> resolved = new HashMap<>();
        for (String tag : tags) {
            List<Document> candidates = byTag.getOrDefault(tag, Collections.emptyList());
            candidates.stream()
                    .sorted(Comparator
                            .comparingInt((Document doc) -> messageTypePriority.getOrDefault(doc.getString("messageType"), Integer.MAX_VALUE))
                            .thenComparingInt(doc -> notBlank(doc.getString("qualifier")) ? 1 : 0))
                    .map(doc -> doc.getString("label"))
                    .filter(this::notBlank)
                    .findFirst()
                    .ifPresent(label -> resolved.put(tag, label));
        }
        return resolved;
    }

    private List<String> buildMtLabelMessageTypeCandidates(String messageCode) {
        if (!notBlank(messageCode)) {
            return Collections.emptyList();
        }
        String trimmed = messageCode.trim().toUpperCase();
        String digits = trimmed.startsWith("MT") ? trimmed.substring(2) : trimmed;
        LinkedHashSet<String> candidates = new LinkedHashSet<>();
        candidates.add(trimmed);
        if (!digits.isBlank()) {
            candidates.add("MT " + digits);
            candidates.add(digits);
        }
        return new ArrayList<>(candidates);
    }

    private boolean isMtMessage(String messageType, String messageCode) {
        return (notBlank(messageType) && "MT".equalsIgnoreCase(messageType.trim()))
                || (notBlank(messageCode) && messageCode.trim().toUpperCase().startsWith("MT"));
    }

    private boolean shouldLookupMtLabel(String label) {
        return !notBlank(label) || "â€”".equals(label) || label.toUpperCase().startsWith("TAG ");
    }

    private void applyBuiltInLabelIfMissing(Map<String, Object> row) {
        String label = stringValue(row.get("label"));
        if (!shouldLookupMtLabel(label)) {
            return;
        }
        String tag = stringValue(row.get("tag"));
        if (notBlank(tag) && FIN_TAG_LABELS.containsKey(tag)) {
            row.put("label", FIN_TAG_LABELS.get(tag));
        }
    }

    private String extractBlock4Content(String rawFin) {
        if (!notBlank(rawFin)) {
            return null;
        }

        int blockStart = rawFin.indexOf("{4:");
        if (blockStart >= 0) {
            int contentStart = blockStart + 3;
            int blockEnd = rawFin.indexOf("-}", contentStart);
            String content = blockEnd >= 0
                    ? rawFin.substring(contentStart, blockEnd)
                    : rawFin.substring(contentStart);
            return content.strip();
        }

        return rawFin.strip();
    }

    private String extractBlock1Content(String rawFin) {
        if (!notBlank(rawFin)) {
            return null;
        }

        int blockStart = rawFin.indexOf("{1:");
        if (blockStart < 0) {
            return null;
        }

        int contentStart = blockStart + 3;
        int blockEnd = rawFin.indexOf('}', contentStart);
        if (blockEnd < 0) {
            return rawFin.substring(contentStart).strip();
        }
        return rawFin.substring(contentStart, blockEnd).strip();
    }

    private String parseSessionNumberFromBlock1(String rawBlock1) {
        if (!notBlank(rawBlock1)) {
            return null;
        }
        String compact = rawBlock1.trim();
        if (compact.length() < 10) {
            return null;
        }
        return compact.substring(compact.length() - 10, compact.length() - 6);
    }

    private String parseSequenceNumberFromBlock1(String rawBlock1) {
        if (!notBlank(rawBlock1)) {
            return null;
        }
        String compact = rawBlock1.trim();
        if (compact.length() < 6) {
            return null;
        }
        return compact.substring(compact.length() - 6);
    }

    private String cleanBlock4Value(String value) {
        if (!notBlank(value)) {
            return null;
        }
        String cleaned = value.replaceAll("\\s*-}$", "").strip();
        return cleaned.isEmpty() ? null : cleaned;
    }

    private Integer computePayloadFieldCount(Document mtParsedPayload, List<Map<String, Object>> block4Fields) {
        Document rawFields = documentAt(mtParsedPayload, "rawFields");
        if (!rawFields.isEmpty()) {
            int count = (int) rawFields.keySet().stream().filter(key -> !"_raw".equals(key)).count();
            return count == 0 ? null : count;
        }
        return block4Fields == null || block4Fields.isEmpty() ? null : block4Fields.size();
    }

    private String composeRawFin(Document mtParsedPayload) {
        String rawBlock1 = mtParsedPayload.getString("rawBlock1");
        String rawBlock2 = mtParsedPayload.getString("rawBlock2");
        String rawBlock4 = mtParsedPayload.getString("rawBlock4");
        if (!notBlank(rawBlock1) && !notBlank(rawBlock2) && !notBlank(rawBlock4)) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        if (notBlank(rawBlock1)) {
            builder.append("{1:").append(rawBlock1).append("}");
        }
        if (notBlank(rawBlock2)) {
            builder.append("{2:").append(rawBlock2).append("}");
        }
        if (notBlank(rawBlock4)) {
            builder.append("{4:\n").append(rawBlock4);
            if (!rawBlock4.endsWith("-}")) {
                builder.append("-}");
            }
        }
        return builder.toString();
    }

    private List<Map<String, Object>> toMapList(Object value) {
        if (!(value instanceof List<?> list)) {
            return Collections.emptyList();
        }

        return list.stream()
                .map(item -> {
                    if (item instanceof Document document) {
                        return new LinkedHashMap<String, Object>(document);
                    }
                    if (item instanceof Map<?, ?> map) {
                        Map<String, Object> converted = new LinkedHashMap<>();
                        map.forEach((key, itemValue) -> converted.put(String.valueOf(key), itemValue));
                        return converted;
                    }
                    return null;
                })
                .filter(item -> item != null && !item.isEmpty())
                .collect(Collectors.toList());
    }

    private List<Document> aggregate(String collection, List<Document> pipeline) {
        return mongoTemplate.getCollection(collection).aggregate(pipeline).into(new ArrayList<>());
    }

    private long aggregateCount(String collection, List<Document> pipeline, Map<String, String> filters) {
        if (filters == null || filters.isEmpty()) {
            CachedValue<Long> cache = unfilteredCountCache;
            if (cache != null && !cache.isExpired(appConfig.getMetadataCacheTtlMs())) {
                return cache.value();
            }
        }
        List<Document> countPipeline = new ArrayList<>(pipeline);
        countPipeline.add(new Document("$count", "total"));
        List<Document> countRows = aggregate(collection, countPipeline);
        long total;
        if (countRows.isEmpty()) {
            total = 0L;
        } else {
            Object totalObj = countRows.get(0).get("total");
            total = totalObj instanceof Number number ? number.longValue() : 0L;
        }
        if (filters == null || filters.isEmpty()) {
            unfilteredCountCache = new CachedValue<>(total, System.currentTimeMillis());
        }
        return total;
    }

    private long countMessages(String collection, Document match, Map<String, String> filters) {
        if ((filters == null || filters.isEmpty()) && (match == null || match.isEmpty())) {
            CachedValue<Long> cache = unfilteredCountCache;
            if (cache != null && !cache.isExpired(appConfig.getMetadataCacheTtlMs())) {
                return cache.value();
            }
        }

        long total = (match == null || match.isEmpty())
                ? mongoTemplate.getCollection(collection).countDocuments()
                : mongoTemplate.getCollection(collection).countDocuments(match);

        if (filters == null || filters.isEmpty()) {
            unfilteredCountCache = new CachedValue<>(total, System.currentTimeMillis());
        }
        return total;
    }

    private List<Document> findMessagePage(String collection, Document match, int page, int pageSize) {
        var finder = mongoTemplate.getCollection(collection).find(match == null ? new Document() : match)
                .sort(new Document("header.dateCreated", -1).append("dateCreated", -1))
                .skip(page * pageSize)
                .limit(pageSize);
        return finder.into(new ArrayList<>());
    }

    private Map<String, Document> fetchPayloadsByReference(List<Document> docs) {
        List<String> references = docs.stream()
                .map(doc -> stringValue(doc.get("messageReference")))
                .filter(this::notBlank)
                .distinct()
                .collect(Collectors.toList());
        if (references.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Document> payloadByReference = new LinkedHashMap<>();
        int batchSize = Math.max(1, appConfig.getPayloadFetchBatchSize());
        String payloadCollection = appConfig.getPayloadsCollection();

        for (int start = 0; start < references.size(); start += batchSize) {
            List<String> batch = references.subList(start, Math.min(start + batchSize, references.size()));
            List<Document> payloads = mongoTemplate.getCollection(payloadCollection)
                    .find(new Document("messageReference", new Document("$in", batch)))
                    .into(new ArrayList<>());
            for (Document payload : payloads) {
                String reference = stringValue(payload.get("messageReference"));
                if (notBlank(reference) && !payloadByReference.containsKey(reference)) {
                    payloadByReference.put(reference, payload);
                }
            }
        }

        return payloadByReference;
    }

    private void appendExportBatch(List<SearchResponse> target, List<Document> docs) {
        appendExportBatch(docs, target::add);
    }

    private void appendExportBatch(List<Document> docs, Consumer<SearchResponse> consumer) {
        Map<String, Document> payloadByReference = fetchPayloadsByReference(docs);
        docs.stream()
                .map(doc -> toResponse(withPayloadDoc(doc, payloadByReference.get(stringValue(doc.get("messageReference"))))))
                .forEach(consumer);
    }

    private void forEachExportResponse(Map<String, String> filters, Consumer<SearchResponse> consumer) {
        String messagesCol = appConfig.getSwiftCollection();
        SearchPlan plan = buildSearchPlan(filters);
        int batchSize = Math.max(1, appConfig.getExportFetchBatchSize());

        if (appConfig.isOptimizeWithoutLookup() && !plan.requiresLookup()) {
            List<Document> batch = new ArrayList<>(batchSize);
            FindIterable<Document> iterable = mongoTemplate.getCollection(messagesCol)
                    .find(plan.messageMatch().isEmpty() ? new Document() : plan.messageMatch())
                    .sort(new Document("header.dateCreated", -1).append("dateCreated", -1))
                    .batchSize(batchSize);

            for (Document doc : iterable) {
                batch.add(doc);
                if (batch.size() >= batchSize) {
                    appendExportBatch(batch, consumer);
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                appendExportBatch(batch, consumer);
            }
            return;
        }

        for (Document doc : mongoTemplate.getCollection(messagesCol)
                .aggregate(buildLookupPipeline(plan))
                .allowDiskUse(true)
                .batchSize(batchSize)) {
            consumer.accept(toResponse(doc));
        }
    }

    private void streamResultTableCsv(Map<String, String> filters,
                                      List<ExportColumnRequest> columns,
                                      OutputStream outputStream) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))) {
            writer.write(columns.stream()
                    .map(ExportColumnRequest::getLabel)
                    .map(this::escapeCsvCell)
                    .collect(Collectors.joining(",")));
            writer.newLine();

            try {
                forEachExportResponse(filters, response -> writeCsvRow(writer, columns, response));
            } catch (UncheckedIOException ex) {
                throw ex.getCause();
            }
            writer.flush();
        }
    }

    private void streamResultTableExcel(Map<String, String> filters,
                                        List<ExportColumnRequest> columns,
                                        OutputStream outputStream) throws IOException {
        int maxRowsPerSheet = Math.max(1, Math.min(EXCEL_HARD_MAX_ROWS_PER_SHEET, appConfig.getExportExcelMaxRowsPerSheet()));

        try (SXSSFWorkbook workbook = new SXSSFWorkbook(500)) {
            workbook.setCompressTempFiles(true);

            ExcelSheetCursor cursor = createExcelSheet(workbook, columns, 1);
            try {
                forEachExportResponse(filters, response -> {
                    if (cursor.dataRows >= maxRowsPerSheet) {
                        cursor.sheetIndex += 1;
                        cursor.sheet = createSheetWithHeader(workbook, columns, cursor.sheetIndex);
                        cursor.rowIndex = 1;
                        cursor.dataRows = 0;
                    }
                    Row row = cursor.sheet.createRow(cursor.rowIndex++);
                    writeExcelRow(row, columns, response);
                    cursor.dataRows += 1;
                });
                workbook.write(outputStream);
                outputStream.flush();
            } finally {
                workbook.dispose();
            }
        }
    }

    private ExcelSheetCursor createExcelSheet(SXSSFWorkbook workbook, List<ExportColumnRequest> columns, int sheetIndex) {
        Sheet sheet = createSheetWithHeader(workbook, columns, sheetIndex);
        return new ExcelSheetCursor(sheet, sheetIndex, 1, 0);
    }

    private Sheet createSheetWithHeader(SXSSFWorkbook workbook, List<ExportColumnRequest> columns, int sheetIndex) {
        Sheet sheet = workbook.createSheet(sheetIndex == 1 ? "Export" : "Export " + sheetIndex);
        Row headerRow = sheet.createRow(0);
        for (int i = 0; i < columns.size(); i++) {
            headerRow.createCell(i).setCellValue(toExcelCellValue(columns.get(i).getLabel()));
            sheet.setColumnWidth(i, 24 * 256);
        }
        return sheet;
    }

    private void writeCsvRow(BufferedWriter writer, List<ExportColumnRequest> columns, SearchResponse response) {
        try {
            String line = columns.stream()
                    .map(column -> escapeCsvCell(resolveExportColumnValue(response, column.getKey())))
                    .collect(Collectors.joining(","));
            writer.write(line);
            writer.newLine();
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private void writeExcelRow(Row row, List<ExportColumnRequest> columns, SearchResponse response) {
        for (int i = 0; i < columns.size(); i++) {
            row.createCell(i).setCellValue(toExcelCellValue(resolveExportColumnValue(response, columns.get(i).getKey())));
        }
    }

    private String toExcelCellValue(String value) {
        if (value == null) {
            return "";
        }
        return value.length() <= EXCEL_MAX_CELL_CHARS ? value : value.substring(0, EXCEL_MAX_CELL_CHARS);
    }

    private List<ExportColumnRequest> normalizeExportColumns(List<ExportColumnRequest> columns) {
        if (columns == null || columns.isEmpty()) {
            return List.of(new ExportColumnRequest("reference", "Reference"));
        }
        Map<String, ExportColumnRequest> unique = new LinkedHashMap<>();
        for (ExportColumnRequest column : columns) {
            if (column == null || !notBlank(column.getKey())) continue;
            String key = column.getKey().trim();
            String label = notBlank(column.getLabel()) ? column.getLabel().trim() : key;
            unique.putIfAbsent(key, new ExportColumnRequest(key, label));
        }
        if (unique.isEmpty()) {
            return List.of(new ExportColumnRequest("reference", "Reference"));
        }
        return List.copyOf(unique.values());
    }

    private String resolveExportColumnValue(SearchResponse response, String key) {
        if (response == null || !notBlank(key)) return "";
        return switch (key) {
            case "reference" -> stringifyExportValue(buildExportReference(response));
            case "format" -> stringifyExportValue(normalizeExportFormat(response.getFormat()));
            case "type" -> stringifyExportValue(response.getType());
            default -> stringifyExportValue(readSearchResponseField(response, key));
        };
    }

    private String buildExportReference(SearchResponse response) {
        if (response == null) return "";
        String uetr = response.getUetr();
        String id = firstNonBlank(response.getId(),
                response.getSequenceNumber() == null ? null : String.valueOf(response.getSequenceNumber()));
        return firstNonBlank(
                response.getReference(),
                response.getMur(),
                response.getTransactionReference(),
                response.getTransferReference(),
                response.getRelatedReference(),
                response.getUserReference(),
                response.getReference(),
                uetr == null ? null : "UETR-" + uetr.substring(0, Math.min(8, uetr.length())).toUpperCase(),
                id == null ? null : "ID-" + id.substring(0, Math.min(10, id.length()))
        );
    }

    private String normalizeExportFormat(String rawFormat) {
        if (!notBlank(rawFormat)) return rawFormat;
        return rawFormat.replace("ALL-MT&MX", "ALL MT&MX");
    }

    private Object readSearchResponseField(SearchResponse response, String key) {
        Field field = SEARCH_RESPONSE_FIELDS.get(key);
        if (field == null) return null;
        try {
            return field.get(response);
        } catch (IllegalAccessException ignored) {
            return null;
        }
    }

    private String stringifyExportValue(Object value) {
        if (value == null) return "";
        if (value instanceof String str) return str;
        if (value instanceof Number || value instanceof Boolean) return String.valueOf(value);
        return value instanceof Instant instant ? instant.toString() : stringifyFallback(value);
    }

    private String stringifyFallback(Object value) {
        try {
            return value instanceof Map || value instanceof List ? new Document("value", value).toJson().replaceFirst("^\\{\"value\":", "").replaceFirst("}$", "") : String.valueOf(value);
        } catch (Exception ignored) {
            return String.valueOf(value);
        }
    }

    private String escapeCsvCell(String value) {
        String safe = value == null ? "" : value.replace("\"", "\"\"");
        return "\"" + safe + "\"";
    }

    private static Map<String, Field> initSearchResponseFields() {
        Map<String, Field> fields = new LinkedHashMap<>();
        for (Field field : SearchResponse.class.getDeclaredFields()) {
            field.setAccessible(true);
            fields.put(field.getName(), field);
        }
        return Map.copyOf(fields);
    }

    private static final class ExcelSheetCursor {
        private Sheet sheet;
        private int sheetIndex;
        private int rowIndex;
        private int dataRows;

        private ExcelSheetCursor(Sheet sheet, int sheetIndex, int rowIndex, int dataRows) {
            this.sheet = sheet;
            this.sheetIndex = sheetIndex;
            this.rowIndex = rowIndex;
            this.dataRows = dataRows;
        }
    }

    private Document withPayloadDoc(Document doc, Document payloadDoc) {
        Document enriched = new Document(doc);
        if (payloadDoc != null && !payloadDoc.isEmpty()) {
            enriched.put(PAYLOAD_ALIAS, payloadDoc);
        }
        return enriched;
    }

    private List<String> distinct(String collection, String fieldPath) {
        try {
            return mongoTemplate.findDistinct(new Query(), fieldPath, collection, String.class).stream()
                    .filter(value -> value != null && !value.isBlank())
                    .sorted()
                    .collect(Collectors.toList());
        } catch (Exception ignored) {
            return Collections.emptyList();
        }
    }

    private List<String> distinctMerged(String collection, String... fieldPaths) {
        Set<String> merged = new LinkedHashSet<>();
        for (String fieldPath : fieldPaths) {
            merged.addAll(distinct(collection, fieldPath));
        }
        return merged.stream().sorted().collect(Collectors.toList());
    }

    private List<String> distinctMessageCodesByFamily(String collection, String family) {
        try {
            List<Document> pipeline = List.of(
                    new Document("$project", new Document("messageFamily",
                            new Document("$ifNull", Arrays.asList("$messageFamily", "$header.messageFamily")))
                            .append("messageTypeCode",
                                    new Document("$ifNull", Arrays.asList("$messageTypeCode", "$header.messageTypeCode")))),
                    new Document("$match", new Document("messageFamily", family)
                            .append("messageTypeCode", new Document("$nin", Arrays.asList(null, "")))),
                    new Document("$group", new Document("_id", "$messageTypeCode")),
                    new Document("$sort", new Document("_id", 1))
            );

            return mongoTemplate.getCollection(collection)
                    .aggregate(pipeline)
                    .into(new ArrayList<>())
                    .stream()
                    .map(doc -> stringValue(doc.get("_id")))
                    .filter(this::notBlank)
                    .collect(Collectors.toList());
        } catch (Exception ignored) {
            return Collections.emptyList();
        }
    }

    private Document regexClause(String fieldPath, String value) {
        return new Document(fieldPath, new Document("$regex", escapeRegex(value)).append("$options", "i"));
    }

    private Document regexCondition(String fieldPath, String value) {
        return new Document(fieldPath, new Document("$regex", escapeRegex(value)).append("$options", "i"));
    }

    private Object decimalExpr(String... fieldPaths) {
        return new Document("$toDouble",
                new Document("$replaceAll",
                        new Document("input", coalesceFieldExpression("0", fieldPaths))
                                .append("find", ",")
                                .append("replacement", ".")
                )
        );
    }

    private Object integerExpr(String... fieldPaths) {
        return new Document("$toInt", coalesceFieldExpression("0", fieldPaths));
    }

    private Object coalesceFieldExpression(String defaultValue, String... fieldPaths) {
        Object expression = defaultValue;
        for (int i = fieldPaths.length - 1; i >= 0; i--) {
            expression = new Document("$ifNull", Arrays.asList("$" + fieldPaths[i], expression));
        }
        return expression;
    }

    private Document documentAt(Document source, String key) {
        Object value = source.get(key);
        return value instanceof Document document ? document : new Document();
    }

    private Object mtParsedPayloadAt(Document mtParsedPayload, String objectKey, String innerKey) {
        Document nested = documentAt(mtParsedPayload, objectKey);
        Object value = nested.get(innerKey);
        if (value instanceof List<?> list && !list.isEmpty()) {
            return list.get(0);
        }
        return value;
    }

    private List<String> toStringList(Object value) {
        if (value instanceof List<?> list) {
            return list.stream().map(this::stringValue).filter(item -> item != null && !item.isBlank()).collect(Collectors.toList());
        }
        String scalar = stringValue(value);
        return scalar == null || scalar.isBlank() ? Collections.emptyList() : Collections.singletonList(scalar);
    }

    private Double parseFlexibleDouble(String value) {
        if (!notBlank(value)) {
            return null;
        }
        String normalized = value.replace(" ", "");
        if (normalized.contains(",") && !normalized.contains(".")) {
            normalized = normalized.replace(",", ".");
        } else if (normalized.contains(",") && normalized.contains(".")) {
            normalized = normalized.replace(",", "");
        }
        try {
            return Double.parseDouble(normalized);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private void parseIntStr(String value, Consumer<Integer> setter) {
        if (!notBlank(value)) {
            return;
        }
        try {
            setter.accept(Integer.parseInt(value.trim()));
        } catch (NumberFormatException ignored) {
        }
    }

    private void parseIntObj(Object value, Consumer<Integer> setter) {
        if (value instanceof Number number) {
            setter.accept(number.intValue());
            return;
        }
        parseIntStr(stringValue(value), setter);
    }

    private Object firstNonBlankObject(Object... values) {
        for (Object value : values) {
            if (value instanceof String stringValue) {
                if (notBlank(stringValue)) {
                    return stringValue;
                }
                continue;
            }
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (notBlank(value)) {
                return value;
            }
        }
        return null;
    }

    private boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }

    private String stringValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Document document) {
            if (document.containsKey("$oid")) {
                return stringValue(document.get("$oid"));
            }
            if (document.containsKey("$date")) {
                return stringValue(document.get("$date"));
            }
            if (document.containsKey("$numberLong")) {
                return stringValue(document.get("$numberLong"));
            }
        }
        return String.valueOf(value);
    }

    private String escapeRegex(String value) {
        return value.replaceAll("[\\\\^$.|?*+()\\[\\]{}]", "\\\\$0");
    }

    private String dateOnly(String isoValue) {
        if (isoValue == null || isoValue.length() < 10) {
            return null;
        }
        return isoValue.substring(0, 10).replace("-", "/");
    }

    private String timeOnly(String isoValue) {
        if (isoValue == null || isoValue.length() < 19) {
            return null;
        }
        return isoValue.substring(11, 19);
    }

    private record SearchPlan(Document messageMatch, Document postLookupMatch, boolean requiresLookup) {
    }

    private record DropdownFieldSpec(
            String key,
            BiConsumer<DropdownOptionsResponse, List<String>> setter,
            Function<DropdownOptionsResponse, List<String>> getter
    ) {
    }

    private record CachedValue<T>(T value, long loadedAtMs) {
        private boolean isExpired(long ttlMs) {
            return ttlMs <= 0 || (System.currentTimeMillis() - loadedAtMs) > ttlMs;
        }
    }
}
