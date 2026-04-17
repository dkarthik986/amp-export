package com.swift.platform.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExportJobListResponse {
    private long pollIntervalMs;
    private List<ExportJobResponse> jobs;
}
