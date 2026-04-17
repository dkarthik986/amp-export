package com.swift.platform.model;

import com.swift.platform.dto.ExportColumnRequest;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "#{@appConfig.exportJobsCollection}")
public class ExportJob {
    @Id
    private String id;

    @Indexed
    private String requestedBy;

    private String requestedByName;
    private String requestedFormat;
    private String scope;
    private List<String> selectedSections;
    private List<ExportColumnRequest> columns;
    private Map<String, String> filters;
    private List<String> references;
    private Long totalCount;
    private Long processedCount;
    private Integer progressPercentage;

    @Indexed
    private ExportJobStatus status;

    @Indexed
    private Instant createdAt;

    @Indexed
    private Instant updatedAt;

    @Indexed
    private Instant completedAt;

    private String outputPath;
    private String outputStorageId;
    private String outputFileName;
    private String contentType;
    private String errorMessage;
}
