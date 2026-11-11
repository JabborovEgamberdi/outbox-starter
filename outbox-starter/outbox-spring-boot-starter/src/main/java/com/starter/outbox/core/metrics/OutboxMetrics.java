package com.starter.outbox.core.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Gauge;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Micrometer metrics for the outbox system.
 *
 * <h3>Published metrics</h3>
 * <ul>
 *   <li>{@code outbox.events.published} — events written to the outbox table</li>
 *   <li>{@code outbox.events.delivered} — events successfully delivered</li>
 *   <li>{@code outbox.events.failed} — delivery failures</li>
 *   <li>{@code outbox.events.dead_lettered} — events moved to dead-letter</li>
 *   <li>{@code outbox.batch.size} — events per dispatch batch</li>
 *   <li>{@code outbox.batch.success_rate} — success ratio per batch</li>
 *   <li>{@code outbox.dispatcher.enabled} — 1 if active, 0 if paused</li>
 * </ul>
 */
public class OutboxMetrics {

    private static final String PREFIX = "outbox";

    private final MeterRegistry registry;
    private final AtomicLong dispatcherActive = new AtomicLong(1);

    public OutboxMetrics(MeterRegistry registry) {
        this.registry = registry;

        Gauge.builder(PREFIX + ".dispatcher.enabled", dispatcherActive, AtomicLong::doubleValue)
                .description("Whether the outbox dispatcher is active")
                .register(registry);
    }

    public void recordPublished(String destination, String eventType) {
        Counter.builder(PREFIX + ".events.published")
                .tag("destination", sanitize(destination))
                .tag("event_type", eventType)
                .register(registry)
                .increment();
    }

    public void recordDelivered(String destination, String eventType) {
        Counter.builder(PREFIX + ".events.delivered")
                .tag("destination", sanitize(destination))
                .tag("event_type", eventType)
                .register(registry)
                .increment();
    }

    public void recordFailed(String destination, String eventType, boolean deadLettered) {
        Counter.builder(PREFIX + ".events.failed")
                .tag("destination", sanitize(destination))
                .tag("event_type", eventType)
                .tag("dead_lettered", String.valueOf(deadLettered))
                .register(registry)
                .increment();

        if (deadLettered) {
            Counter.builder(PREFIX + ".events.dead_lettered")
                    .tag("destination", sanitize(destination))
                    .register(registry)
                    .increment();
        }
    }

    public void recordBatchProcessed(int total, int success) {
        registry.summary(PREFIX + ".batch.size").record(total);
        if (total > 0) {
            registry.summary(PREFIX + ".batch.success_rate").record((double) success / total);
        }
    }

    public void recordArchived(int count) {
        Counter.builder(PREFIX + ".events.archived")
                .register(registry)
                .increment(count);
    }

    public void setDispatcherActive(boolean active) {
        dispatcherActive.set(active ? 1 : 0);
    }

    private static String sanitize(String destination) {
        // Limit cardinality: extract prefix only (e.g. "kafka:" from "kafka:orders-topic")
        int colonIdx = destination.indexOf(':');
        return colonIdx > 0 ? destination.substring(0, colonIdx) : destination;
    }
}
