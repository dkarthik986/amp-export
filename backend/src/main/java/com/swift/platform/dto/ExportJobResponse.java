package com.swift.platform.dto;

import com.swift.platform.model.ExportJobStatus;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.List;

@Data
@Builder
public class ExportJobResponse {
    private String id;
    private String requestedBy;
    private String requestedByName;
    private String requestedFormat;
    private String scope;
    private List<String> selectedSections;
    private Long totalCount;
    private Long processedCount;
    private Integer progressPercentage;
    private ExportJobStatus status;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant completedAt;
    private String outputFileName;
    private String contentType;
    private String errorMessage;
    private String downloadUrl;
    private boolean downloadReady;
}
