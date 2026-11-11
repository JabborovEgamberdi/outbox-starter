package com.starter.outbox.delivery;

import com.starter.outbox.core.entity.OutboxEvent;

/**
 * Strategy interface for delivering outbox events to external systems.
 *
 * <p>Each implementation handles a specific <b>destination prefix</b>, e.g.:
 * <ul>
 *   <li>{@code "kafka:"} → {@code KafkaDeliveryHandler}</li>
 *   <li>{@code "rest:"} → {@code RestApiDeliveryHandler}</li>
 *   <li>{@code "db:"} → {@code DatabaseDeliveryHandler}</li>
 * </ul>
 *
 * <p>Implementations are discovered automatically via Spring's component scan
 * and registered by their {@link #destinationPrefix()}.</p>
 *
 * <h3>Contract</h3>
 * <ul>
 *   <li>MUST throw an exception if delivery fails (triggers retry).</li>
 *   <li>SHOULD be idempotent — the same event may be delivered more than once.</li>
 *   <li>MUST NOT modify the outbox event status (the dispatcher owns that).</li>
 * </ul>
 */
public interface OutboxDeliveryHandler {

    /**
     * The destination prefix this handler is responsible for.
     * The dispatcher matches {@code event.getDestination().startsWith(prefix)}.
     *
     * @return prefix string, e.g. "kafka:", "rest:", "db:"
     */
    String destinationPrefix();

    /**
     * Deliver the event to the external system.
     *
     * @param event the outbox event to deliver
     * @throws OutboxDeliveryException if delivery fails
     */
    void deliver(OutboxEvent event) throws OutboxDeliveryException;

    /**
     * Extract the target-specific identifier from the destination string.
     * For example, "kafka:orders-topic" → "orders-topic".
     */
    default String extractTarget(OutboxEvent event) {
        return event.getDestination().substring(destinationPrefix().length());
    }
}
