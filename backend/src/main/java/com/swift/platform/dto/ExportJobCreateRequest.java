package com.swift.platform.dto;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class ExportJobCreateRequest {
    private String format;
    private String scope;
    private List<String> targetKeys;
    private List<ExportColumnRequest> columns;
    private Map<String, String> filters;
    private List<String> references;
    private Long totalCount;
}
