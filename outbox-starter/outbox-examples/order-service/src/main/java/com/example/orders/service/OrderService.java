package com.example.orders.service;

import com.example.orders.domain.Order;
import com.example.orders.domain.OrderRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starter.outbox.core.publisher.OutboxPublisher;
import com.starter.outbox.core.publisher.OutboxPublisher.OutboxMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Example service that demonstrates publishing outbox events to
 * multiple targets using the SAME outbox core.
 *
 * <p>All three delivery targets reuse the same publisher API —
 * the only difference is the destination prefix.</p>
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final OutboxPublisher outboxPublisher;
    private final ObjectMapper objectMapper;

    public OrderService(OrderRepository orderRepository,
                        OutboxPublisher outboxPublisher,
                        ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxPublisher = outboxPublisher;
        this.objectMapper = objectMapper;
    }

    // ═══════════════════════════════════════════════════════════════
    //  EXAMPLE 1: Write to Kafka
    // ═══════════════════════════════════════════════════════════════

    /**
     * Creates an order and publishes an "OrderCreated" event to Kafka.
     *
     * <p>Both the order INSERT and the outbox INSERT happen in the
     * SAME transaction. If either fails, both are rolled back.</p>
     */
    @Transactional
    public Order createOrderWithKafka(String email, String product, BigDecimal amount) {
        Order order = orderRepository.save(new Order(email, product, amount));

        outboxPublisher.publish(OutboxMessage.builder()
                .eventType("OrderCreated")
                .aggregateType("Order")
                .aggregateId(order.getId())
                .destination("kafka:orders-topic")            // ← Kafka target
                .payload(toJson(order))
                .headers(Map.of("content-type", "application/json",
                                "correlation-id", order.getId()))
                .build());

        log.info("Order created (Kafka): {}", order.getId());
        return order;
    }

    // ═══════════════════════════════════════════════════════════════
    //  EXAMPLE 2: Call an external REST API
    // ═══════════════════════════════════════════════════════════════

    /**
     * Creates an order and notifies an external payment service via REST.
     */
    @Transactional
    public Order createOrderWithRestNotification(String email, String product, BigDecimal amount) {
        Order order = orderRepository.save(new Order(email, product, amount));

        outboxPublisher.publish(OutboxMessage.builder()
                .eventType("OrderCreated")
                .aggregateType("Order")
                .aggregateId(order.getId())
                .destination("rest:payment-service")          // ← REST target
                .payload(toJson(order))
                .headers(Map.of("X-Tenant-Id", "acme-corp"))
                .build());

        log.info("Order created (REST): {}", order.getId());
        return order;
    }

    // ═══════════════════════════════════════════════════════════════
    //  EXAMPLE 3: Write to another database (audit store)
    // ═══════════════════════════════════════════════════════════════

    /**
     * Creates an order and writes an audit event to an external database.
     */
    @Transactional
    public Order createOrderWithAuditDb(String email, String product, BigDecimal amount) {
        Order order = orderRepository.save(new Order(email, product, amount));

        outboxPublisher.publish(OutboxMessage.builder()
                .eventType("OrderCreated")
                .aggregateType("Order")
                .aggregateId(order.getId())
                .destination("db:audit-store")                // ← Database target
                .payload(toJson(order))
                .build());

        log.info("Order created (DB audit): {}", order.getId());
        return order;
    }

    // ═══════════════════════════════════════════════════════════════
    //  EXAMPLE 4: Internal module notification
    // ═══════════════════════════════════════════════════════════════

    /**
     * Creates an order and notifies an internal notification module.
     */
    @Transactional
    public Order createOrderWithInternalNotification(String email, String product, BigDecimal amount) {
        Order order = orderRepository.save(new Order(email, product, amount));

        outboxPublisher.publish(OutboxMessage.builder()
                .eventType("OrderCreated")
                .aggregateType("Order")
                .aggregateId(order.getId())
                .destination("internal:notifications")        // ← Internal target
                .payload(toJson(order))
                .build());

        log.info("Order created (internal): {}", order.getId());
        return order;
    }

    // ═══════════════════════════════════════════════════════════════
    //  EXAMPLE 5: Fan-out to multiple targets in one transaction
    // ═══════════════════════════════════════════════════════════════

    /**
     * Demonstrates publishing to MULTIPLE destinations in a single
     * business transaction.
     */
    @Transactional
    public Order createOrderWithFanOut(String email, String product, BigDecimal amount) {
        Order order = orderRepository.save(new Order(email, product, amount));
        String payload = toJson(order);

        // Kafka for event streaming
        outboxPublisher.publish(OutboxMessage.builder()
                .eventType("OrderCreated")
                .aggregateType("Order")
                .aggregateId(order.getId())
                .destination("kafka:orders-topic")
                .payload(payload)
                .idempotencyKey("kafka-" + order.getId())
                .build());

        // REST for payment processing
        outboxPublisher.publish(OutboxMessage.builder()
                .eventType("OrderCreated")
                .aggregateType("Order")
                .aggregateId(order.getId())
                .destination("rest:payment-service")
                .payload(payload)
                .idempotencyKey("rest-" + order.getId())
                .build());

        // DB for audit trail
        outboxPublisher.publish(OutboxMessage.builder()
                .eventType("OrderCreated")
                .aggregateType("Order")
                .aggregateId(order.getId())
                .destination("db:audit-store")
                .payload(payload)
                .idempotencyKey("db-" + order.getId())
                .build());

        log.info("Order created (fan-out): {}", order.getId());
        return order;
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException("JSON serialization failed", e);
        }
    }
}
