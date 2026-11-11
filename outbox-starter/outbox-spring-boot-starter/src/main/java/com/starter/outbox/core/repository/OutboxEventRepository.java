package com.starter.outbox.core.repository;

import com.starter.outbox.core.entity.OutboxEvent;
import com.starter.outbox.core.entity.OutboxStatus;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, String> {

    /**
     * Fetch a batch of events ready for delivery, using SELECT … FOR UPDATE SKIP LOCKED
     * so that multiple dispatcher instances don't compete for the same rows.
     *
     * <p>This is the PostgreSQL-optimised path. For other databases the SKIP LOCKED
     * hint can be replaced with an application-level advisory lock.</p>
     */
    @Query(value = """
        SELECT e.* FROM outbox_event e
        WHERE e.status IN ('PENDING', 'RETRYING')
          AND e.next_attempt_at <= :now
        ORDER BY e.next_attempt_at ASC
        LIMIT :batchSize
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
    List<OutboxEvent> findAndLockBatch(
            @Param("now") Instant now,
            @Param("batchSize") int batchSize);

    /** Count events by status (for metrics / admin). */
    long countByStatus(OutboxStatus status);

    /** Retrieve events by status for admin inspection. */
    List<OutboxEvent> findByStatusOrderByCreatedAtDesc(OutboxStatus status, Pageable pageable);

    /** Find events by aggregate for debugging. */
    List<OutboxEvent> findByAggregateTypeAndAggregateIdOrderByCreatedAtDesc(
            String aggregateType, String aggregateId);

    /** Find dead-letter events for a specific destination. */
    List<OutboxEvent> findByStatusAndDestinationOrderByCreatedAtDesc(
            OutboxStatus status, String destination, Pageable pageable);

    /** Archive: delete processed events older than the given cutoff. */
    @Modifying
    @Query("DELETE FROM OutboxEvent e WHERE e.status = 'PROCESSED' AND e.processedAt < :cutoff")
    int deleteProcessedBefore(@Param("cutoff") Instant cutoff);

    /** Bulk move old retrying events to dead-letter (circuit breaker). */
    @Modifying
    @Query("""
        UPDATE OutboxEvent e SET e.status = 'DEAD_LETTER',
               e.lastError = 'Forcibly dead-lettered: exceeded age threshold'
        WHERE e.status = 'RETRYING' AND e.createdAt < :cutoff
        """)
    int deadLetterStaleRetrying(@Param("cutoff") Instant cutoff);

    /** Check for duplicate idempotency key. */
    boolean existsByIdempotencyKey(String idempotencyKey);
}
