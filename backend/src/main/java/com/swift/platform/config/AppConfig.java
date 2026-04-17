package com.swift.platform.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * Central configuration bean.
 * All @Value fields live here — override any with OS env vars, no rebuild needed.
 *
 *   export MONGO_COLLECTION_SWIFT=my_swift_msgs
 *   export JWT_SECRET=my-secret-key
 *   java -jar swift-backend.jar
 */
@Getter
@Configuration
public class AppConfig {

    // ── MongoDB collections ────────────────────────────────────────────────
    @Value("${mongo.collection.swift:amp_messages}")
    private String swiftCollection;

    @Value("${mongo.collection.payloads:amp_payloads}")
    private String payloadsCollection;

    @Value("${mongo.collection.users:user_data}")
    private String usersCollection;

    @Value("${mongo.collection.audit:audit_logs}")
    private String auditCollection;

    // ── Search ────────────────────────────────────────────────────────────
    @Value("${search.default-page-size:20}")
    private int defaultPageSize;

    @Value("${search.max-page-size:500}")
    private int maxPageSize;

    @Value("${search.metadata-cache-ttl-ms:300000}")
    private long metadataCacheTtlMs;

    @Value("${search.field-discovery-sample-size:200}")
    private int fieldDiscoverySampleSize;

    @Value("${search.payload-fetch-batch-size:1000}")
    private int payloadFetchBatchSize;

    @Value("${search.export-fetch-batch-size:5000}")
    private int exportFetchBatchSize;

    @Value("${search.export.excel-max-rows-per-sheet:1048575}")
    private int exportExcelMaxRowsPerSheet;

    @Value("${search.optimize-without-lookup:true}")
    private boolean optimizeWithoutLookup;

    @Value("${search.ensure-indexes:true}")
    private boolean ensureIndexes;

    @Value("${mongo.collection.rawcopies:amp_raw_copies}")
    private String rawCopiesCollection;

    @Value("${mongo.collection.mt-labels:mt_lable}")
    private String mtLabelsCollection;

    @Value("${mongo.collection.dropdown-options:drop_down}")
    private String dropdownOptionsCollection;

    @Value("${mongo.collection.export-jobs:export_jobs}")
    private String exportJobsCollection;

    @Value("${mongo.collection.export-job-locks:export_job_locks}")
    private String exportJobLocksCollection;

    @Value("${search.dropdown.document-id:search_dropdown_options}")
    private String dropdownOptionsDocumentId;

    @Value("${search.dropdown.refresh-enabled:true}")
    private boolean dropdownRefreshEnabled;

    @Value("${search.dropdown.refresh-interval-hours:6}")
    private long dropdownRefreshIntervalHours;

    @Value("${search.dropdown.refresh-initial-delay-minutes:5}")
    private long dropdownRefreshInitialDelayMinutes;

    @Value("${search.export.background-threshold:1000}")
    private int exportBackgroundThreshold;

    @Value("${search.export.use-gridfs:true}")
    private boolean exportUseGridFs;

    @Value("${search.export.output-dir:#{systemProperties['java.io.tmpdir'] + '/swift-export-jobs'}}")
    private String exportOutputDir;

    @Value("${search.export.job-poll-interval-ms:8000}")
    private long exportJobPollIntervalMs;

    @Value("${search.export.dispatch-poll-interval-ms:5000}")
    private long exportDispatchPollIntervalMs;

    @Value("${search.export.dispatch-lock-timeout-seconds:30}")
    private long exportDispatchLockTimeoutSeconds;

    @Value("${search.export.max-records-per-file:500}")
    private int exportMaxRecordsPerFile;

    @Value("${search.export.progress-save-interval:25}")
    private int exportProgressSaveInterval;

    @Value("${search.export.cleanup-retention-hours:48}")
    private long exportCleanupRetentionHours;

    @Value("${search.export.cleanup-interval-hours:6}")
    private long exportCleanupIntervalHours;

    @Value("${search.export.cleanup-initial-delay-minutes:30}")
    private long exportCleanupInitialDelayMinutes;

    @Value("${search.export.executor.core-pool-size:1}")
    private int exportExecutorCorePoolSize;

    @Value("${search.export.executor.max-pool-size:1}")
    private int exportExecutorMaxPoolSize;

    @Value("${search.export.executor.queue-capacity:20}")
    private int exportExecutorQueueCapacity;

    // ── Admin ─────────────────────────────────────────────────────────────
    @Value("${admin.protected-id:ADMIN001}")
    private String protectedAdminId;

    // ── JWT ───────────────────────────────────────────────────────────────
    @Value("${jwt.secret:SwiftPlatformSecretKey2024!@#$%^&*()ABCDEF_MUST_BE_32_CHARS_MIN}")
    private String jwtSecret;

    @Value("${jwt.expiration:86400000}")
    private long jwtExpiration;
}
