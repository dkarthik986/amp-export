package com.swift.platform.repository;

import com.swift.platform.model.ExportJob;
import com.swift.platform.model.ExportJobStatus;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface ExportJobRepository extends MongoRepository<ExportJob, String> {
    List<ExportJob> findByRequestedByOrderByCreatedAtDesc(String requestedBy, Pageable pageable);
    Optional<ExportJob> findByIdAndRequestedBy(String id, String requestedBy);
    List<ExportJob> findByCompletedAtBefore(Instant cutoff);
    Optional<ExportJob> findFirstByStatusOrderByCreatedAtAsc(ExportJobStatus status);
    boolean existsByStatus(ExportJobStatus status);
}
