package com.starter.outbox.core.publisher;

import com.starter.outbox.core.entity.OutboxEvent;

import java.util.Map;

/**
 * Primary API for publishing outbox events within a business transaction.
 *
 * <p>Usage:
 * <pre>{@code
 * @Transactional
 * public Order createOrder(CreateOrderRequest req) {
 *     Order order = orderRepository.save(toEntity(req));
 *     outboxPublisher.publish(
 *         OutboxMessage.builder()
 *             .eventType("OrderCreated")
 *             .aggregateType("Order")
 *             .aggregateId(order.getId().toString())
 *             .destination("kafka:orders")
 *             .payload(objectMapper.writeValueAsString(order))
 *             .build()
 *     );
 *     return order;
 * }
 * }</pre>
 */
public interface OutboxPublisher {

    /**
     * Persist an outbox event in the current transaction.
     * The event will be picked up by the dispatcher asynchronously.
     *
     * @return the persisted event (with generated id)
     */
    OutboxEvent publish(OutboxMessage message);

    /**
     * Convenience builder record.
     */
    record OutboxMessage(
            String eventType,
            String aggregateType,
            String aggregateId,
            String payload,
            Map<String, String> headers,
            String destination,
            Integer maxRetries,
            String idempotencyKey
    ) {
        public static Builder builder() { return new Builder(); }

        public static class Builder {
            private String eventType;
            private String aggregateType;
            private String aggregateId;
            private String payload;
            private Map<String, String> headers;
            private String destination;
            private Integer maxRetries;
            private String idempotencyKey;

            public Builder eventType(String v)     { this.eventType = v; return this; }
            public Builder aggregateType(String v) { this.aggregateType = v; return this; }
            public Builder aggregateId(String v)   { this.aggregateId = v; return this; }
            public Builder payload(String v)       { this.payload = v; return this; }
            public Builder headers(Map<String, String> v) { this.headers = v; return this; }
            public Builder destination(String v)   { this.destination = v; return this; }
            public Builder maxRetries(Integer v)   { this.maxRetries = v; return this; }
            public Builder idempotencyKey(String v){ this.idempotencyKey = v; return this; }

            public OutboxMessage build() {
                return new OutboxMessage(
                    eventType, aggregateType, aggregateId,
                    payload, headers, destination, maxRetries, idempotencyKey
                );
            }
        }
    }
}
