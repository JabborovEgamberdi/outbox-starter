package com.starter.outbox.core.archive;

import com.starter.outbox.core.config.OutboxProperties;
import com.starter.outbox.core.metrics.OutboxMetrics;
import com.starter.outbox.core.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * Periodic job that deletes or archives processed outbox events
 * older than the configured retention period.
 *
 * <p>Also force-dead-letters stale RETRYING events to prevent
 * indefinite retry loops.</p>
 */
public class OutboxArchiveJob {

    private static final Logger log = LoggerFactory.getLogger(OutboxArchiveJob.class);

    private final OutboxEventRepository repository;
    private final OutboxProperties properties;
    private final OutboxMetrics metrics;

    public OutboxArchiveJob(OutboxEventRepository repository,
                            OutboxProperties properties,
                            OutboxMetrics metrics) {
        this.repository = repository;
        this.properties = properties;
        this.metrics = metrics;
    }

    @Scheduled(cron = "${outbox.archive.cron:0 0 3 * * *}")
    @Transactional
    public void cleanUp() {
        if (!properties.getArchive().isEnabled()) {
            return;
        }

        Instant processedCutoff = Instant.now().minus(properties.getArchive().getRetention());
        int deleted = repository.deleteProcessedBefore(processedCutoff);
        if (deleted > 0) {
            log.info("Archived (deleted) {} processed outbox events older than {}",
                    deleted, processedCutoff);
            metrics.recordArchived(deleted);
        }

        Instant staleCutoff = Instant.now().minus(properties.getStaleRetryThreshold());
        int deadLettered = repository.deadLetterStaleRetrying(staleCutoff);
        if (deadLettered > 0) {
            log.warn("Force dead-lettered {} stale retrying outbox events older than {}",
                    deadLettered, staleCutoff);
        }
    }
}
