package com.starter.outbox.core.dispatcher;

import com.starter.outbox.core.config.OutboxProperties;
import com.starter.outbox.core.entity.OutboxEvent;
import com.starter.outbox.core.entity.OutboxStatus;
import com.starter.outbox.core.metrics.OutboxMetrics;
import com.starter.outbox.core.repository.OutboxEventRepository;
import com.starter.outbox.delivery.DeliveryHandlerRegistry;
import com.starter.outbox.delivery.OutboxDeliveryException;
import com.starter.outbox.delivery.OutboxDeliveryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Core dispatcher that polls the outbox table and delivers events to
 * external systems via the {@link DeliveryHandlerRegistry}.
 *
 * <h3>Concurrency model</h3>
 * Uses {@code SELECT ... FOR UPDATE SKIP LOCKED} to allow multiple application
 * instances to process events concurrently without duplicating work.
 *
 * <h3>Retry strategy</h3>
 * Exponential backoff: {@code baseDelay * 2^(retries-1)}, capped at
 * {@code maxBackoff}. After {@code maxRetries} failures the event is
 * moved to DEAD_LETTER status.
 */
public class OutboxDispatcher {

    private static final Logger log = LoggerFactory.getLogger(OutboxDispatcher.class);

    private final OutboxEventRepository repository;
    private final DeliveryHandlerRegistry registry;
    private final OutboxProperties properties;
    private final OutboxMetrics metrics;
    private final AtomicBoolean enabled = new AtomicBoolean(true);

    public OutboxDispatcher(OutboxEventRepository repository,
                            DeliveryHandlerRegistry registry,
                            OutboxProperties properties,
                            OutboxMetrics metrics) {
        this.repository = repository;
        this.registry = registry;
        this.properties = properties;
        this.metrics = metrics;
    }

    /**
     * Main dispatch cycle — called by the scheduler.
     *
     * <p>Each invocation fetches one batch using row-level locks,
     * delivers each event, and updates its status.</p>
     */
    @Transactional
    public int dispatchBatch() {
        if (!enabled.get()) {
            log.trace("Outbox dispatcher is paused");
            return 0;
        }

        Instant now = Instant.now();
        List<OutboxEvent> batch = repository.findAndLockBatch(now, properties.getBatchSize());

        if (batch.isEmpty()) {
            log.trace("No outbox events ready for dispatch");
            return 0;
        }

        log.debug("Dispatching {} outbox events", batch.size());
        int successCount = 0;

        for (OutboxEvent event : batch) {
            try {
                deliverEvent(event);
                event.markProcessed();
                repository.save(event);
                successCount++;

                metrics.recordDelivered(event.getDestination(), event.getEventType());
                log.debug("Outbox event delivered: id={}, type={}, dest={}",
                        event.getId(), event.getEventType(), event.getDestination());

            } catch (OutboxDeliveryException ex) {
                handleFailure(event, ex, ex.isRetryable());
            } catch (Exception ex) {
                handleFailure(event, ex, true);
            }
        }

        metrics.recordBatchProcessed(batch.size(), successCount);
        return successCount;
    }

    private void deliverEvent(OutboxEvent event) {
        OutboxDeliveryHandler handler = registry.resolve(event);
        handler.deliver(event);
    }

    private void handleFailure(OutboxEvent event, Exception ex, boolean retryable) {
        String errorMsg = truncate(ex.getMessage(), 2000);

        if (!retryable) {
            event.markDeadLetter(errorMsg);
            log.error("Outbox event dead-lettered (non-retryable): id={}, error={}",
                    event.getId(), errorMsg, ex);
        } else {
            Instant nextAttempt = calculateNextAttempt(event.getRetries());
            event.markFailed(errorMsg, nextAttempt);

            if (event.getStatus() == OutboxStatus.DEAD_LETTER) {
                log.error("Outbox event dead-lettered (max retries): id={}, retries={}, error={}",
                        event.getId(), event.getRetries(), errorMsg, ex);
            } else {
                log.warn("Outbox event delivery failed, scheduling retry: id={}, retries={}, next={}",
                        event.getId(), event.getRetries(), nextAttempt);
            }
        }

        repository.save(event);
        metrics.recordFailed(event.getDestination(), event.getEventType(),
                event.getStatus() == OutboxStatus.DEAD_LETTER);
    }

    /**
     * Exponential backoff: baseDelay * 2^retries, capped at maxBackoff.
     */
    Instant calculateNextAttempt(int currentRetries) {
        long baseMs = properties.getRetryBaseDelay().toMillis();
        long maxMs = properties.getRetryMaxBackoff().toMillis();
        long delayMs = Math.min(baseMs * (1L << currentRetries), maxMs);
        return Instant.now().plusMillis(delayMs);
    }

    /** Pause dispatching (admin / circuit breaker). */
    public void pause() {
        enabled.set(false);
        log.info("Outbox dispatcher PAUSED");
    }

    /** Resume dispatching. */
    public void resume() {
        enabled.set(true);
        log.info("Outbox dispatcher RESUMED");
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    private static String truncate(String s, int maxLen) {
        if (s == null) return "null";
        return s.length() > maxLen ? s.substring(0, maxLen) + "…" : s;
    }
}
