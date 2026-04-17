package com.swift.platform.controller;

import com.swift.platform.dto.ExportJobCreateRequest;
import com.swift.platform.dto.ExportJobListResponse;
import com.swift.platform.dto.ExportJobResponse;
import com.swift.platform.service.AuditService;
import com.swift.platform.service.ExportJobService;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/export-jobs")
@RequiredArgsConstructor
public class ExportJobController {

    private final ExportJobService exportJobService;
    private final AuditService auditService;

    @PostMapping
    public ExportJobResponse createExportJob(@RequestBody ExportJobCreateRequest request,
                                             HttpServletRequest httpReq) {
        String employeeId = (String) httpReq.getAttribute("employeeId");
        String name = (String) httpReq.getAttribute("name");
        ExportJobResponse response;
        try {
            response = exportJobService.createJob(request, employeeId, name);
        } catch (IllegalArgumentException ex) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage(), ex);
        }
        auditService.log(employeeId, "EXPORT_JOB_CREATE",
                "Format: " + response.getRequestedFormat() + ", Scope: " + response.getScope() + ", Total: " + response.getTotalCount(),
                httpReq.getRemoteAddr());
        return response;
    }

    @GetMapping("/recent")
    public ExportJobListResponse recentJobs(@RequestParam(defaultValue = "10") int limit,
                                            HttpServletRequest httpReq) {
        String employeeId = (String) httpReq.getAttribute("employeeId");
        return exportJobService.getRecentJobs(employeeId, limit);
    }

    @GetMapping("/{jobId}")
    public ExportJobResponse getJob(@PathVariable String jobId,
                                    HttpServletRequest httpReq) {
        String employeeId = (String) httpReq.getAttribute("employeeId");
        try {
            return exportJobService.getJob(jobId, employeeId);
        } catch (IllegalArgumentException ex) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, ex.getMessage(), ex);
        }
    }

    @PostMapping("/{jobId}/cancel")
    public ExportJobResponse cancelJob(@PathVariable String jobId,
                                       HttpServletRequest httpReq) {
        String employeeId = (String) httpReq.getAttribute("employeeId");
        ExportJobResponse response;
        try {
            response = exportJobService.cancelJob(jobId, employeeId);
        } catch (IllegalArgumentException ex) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, ex.getMessage(), ex);
        }
        auditService.log(employeeId, "EXPORT_JOB_CANCEL", "Job: " + jobId, httpReq.getRemoteAddr());
        return response;
    }

    @GetMapping("/{jobId}/download")
    public ResponseEntity<org.springframework.core.io.Resource> download(@PathVariable String jobId,
                                                                         HttpServletRequest httpReq) {
        String employeeId = (String) httpReq.getAttribute("employeeId");
        ExportJobService.JobDownload download;
        try {
            download = exportJobService.resolveDownload(jobId, employeeId);
        } catch (IllegalArgumentException ex) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, ex.getMessage(), ex);
        } catch (IllegalStateException ex) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, ex.getMessage(), ex);
        }
        auditService.log(employeeId, "EXPORT_JOB_DOWNLOAD", "Job: " + jobId, httpReq.getRemoteAddr());
        MediaType mediaType = download.contentType() == null || download.contentType().isBlank()
                ? MediaType.APPLICATION_OCTET_STREAM
                : MediaType.parseMediaType(download.contentType());
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + download.fileName() + "\"")
                .contentType(mediaType)
                .body(download.resource());
    }
}
