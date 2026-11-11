package com.starter.outbox.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.starter.outbox.autoconfigure.OutboxAutoConfiguration;
import com.starter.outbox.core.config.OutboxProperties;
import com.starter.outbox.core.dispatcher.OutboxDispatcher;
import com.starter.outbox.core.entity.OutboxEvent;
import com.starter.outbox.core.entity.OutboxStatus;
import com.starter.outbox.core.metrics.OutboxMetrics;
import com.starter.outbox.core.publisher.DefaultOutboxPublisher;
import com.starter.outbox.core.publisher.OutboxPublisher;
import com.starter.outbox.core.repository.OutboxEventRepository;
import com.starter.outbox.delivery.DeliveryHandlerRegistry;
import com.starter.outbox.delivery.OutboxDeliveryException;
import com.starter.outbox.delivery.OutboxDeliveryHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Full integration test: PostgreSQL + Outbox lifecycle.
 *
 * Tests the complete flow:
 * 1. Publish event within a transaction
 * 2. Dispatcher picks it up
 * 3. Delivery handler is called
 * 4. Event is marked as PROCESSED
 * 5. Retry on failure
 * 6. Dead-letter after max retries
 */
@SpringBootTest(classes = OutboxIntegrationTest.TestConfig.class)
@Testcontainers
class OutboxIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("outbox_test")
            .withUsername("test")
            .withPassword("test");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("outbox.poll-interval-ms", () -> "60000"); // disable auto-poll
    }

    @Autowired private OutboxPublisher publisher;
    @Autowired private OutboxDispatcher dispatcher;
    @Autowired private OutboxEventRepository repository;

    // Shared state with test delivery handler
    static final CopyOnWriteArrayList<OutboxEvent> deliveredEvents = new CopyOnWriteArrayList<>();
    static final AtomicInteger failUntilCount = new AtomicInteger(0);

    @BeforeEach
    void setUp() {
        repository.deleteAll();
        deliveredEvents.clear();
        failUntilCount.set(0);
    }

    @Test
    @Transactional
    void shouldPublishAndPersistEvent() {
        OutboxEvent event = publisher.publish(OutboxPublisher.OutboxMessage.builder()
                .eventType("OrderCreated")
                .aggregateType("Order")
                .aggregateId("order-1")
                .destination("test:target")
                .payload("{\"orderId\":\"order-1\"}")
                .build());

        assertThat(event.getId()).isNotNull();
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(repository.findById(event.getId())).isPresent();
    }

    @Test
    void shouldDispatchAndMarkProcessed() {
        // Publish in a transaction
        publishTestEvent("order-1");

        // Manually trigger dispatch
        int delivered = dispatcher.dispatchBatch();

        assertThat(delivered).isEqualTo(1);
        assertThat(deliveredEvents).hasSize(1);
        assertThat(deliveredEvents.get(0).getAggregateId()).isEqualTo("order-1");

        OutboxEvent fromDb = repository.findAll().get(0);
        assertThat(fromDb.getStatus()).isEqualTo(OutboxStatus.PROCESSED);
        assertThat(fromDb.getProcessedAt()).isNotNull();
    }

    @Test
    void shouldRetryOnFailureAndEventuallySucceed() {
        failUntilCount.set(2); // fail first 2 attempts

        publishTestEvent("order-retry");

        // First attempt: fails
        dispatcher.dispatchBatch();
        OutboxEvent afterFirst = repository.findAll().get(0);
        assertThat(afterFirst.getStatus()).isEqualTo(OutboxStatus.RETRYING);
        assertThat(afterFirst.getRetries()).isEqualTo(1);

        // Override next_attempt_at so it's eligible again
        afterFirst.resetForRetry();
        repository.save(afterFirst);
        failUntilCount.set(1); // fail one more time

        dispatcher.dispatchBatch(); // second attempt: fails
        afterFirst = repository.findAll().get(0);
        afterFirst.resetForRetry();
        repository.save(afterFirst);
        failUntilCount.set(0); // now succeed

        dispatcher.dispatchBatch(); // third attempt: succeeds
        OutboxEvent afterSuccess = repository.findAll().get(0);
        assertThat(afterSuccess.getStatus()).isEqualTo(OutboxStatus.PROCESSED);
    }

    @Test
    void shouldDeadLetterAfterMaxRetries() {
        failUntilCount.set(999); // always fail

        publishTestEventWithMaxRetries("order-dl", 2);

        // Dispatch until dead-lettered
        for (int i = 0; i < 3; i++) {
            dispatcher.dispatchBatch();
            OutboxEvent e = repository.findAll().get(0);
            if (e.getStatus() != OutboxStatus.DEAD_LETTER) {
                e.resetForRetry();
                repository.save(e);
            }
        }

        // After max retries: should eventually reach DEAD_LETTER via markFailed
        // Let's test directly
        OutboxEvent event = repository.findAll().get(0);
        // Reset and retry until dead letter
        event.resetForRetry();
        repository.save(event);

        // Override max retries to 1 for quick dead letter
        // We verify the dead letter mechanism via the entity directly
        OutboxEvent testEvent = OutboxEvent.builder()
                .eventType("Test").aggregateType("Test").aggregateId("dl-test")
                .payload("{}").destination("test:target").maxRetries(1).build();
        repository.save(testEvent);
        failUntilCount.set(999);
        dispatcher.dispatchBatch();

        OutboxEvent dlEvent = repository.findByAggregateTypeAndAggregateIdOrderByCreatedAtDesc("Test", "dl-test").get(0);
        assertThat(dlEvent.getStatus()).isEqualTo(OutboxStatus.DEAD_LETTER);
    }

    @Test
    void shouldHandleEmptyBatchGracefully() {
        int result = dispatcher.dispatchBatch();
        assertThat(result).isZero();
    }

    @Test
    void shouldSupportIdempotencyKey() {
        publishTestEventWithIdempotencyKey("order-idem", "idem-key-1");

        boolean exists = repository.existsByIdempotencyKey("idem-key-1");
        assertThat(exists).isTrue();
    }

    // ─── helpers ─────────────────────────────────────────────────

    @Transactional
    void publishTestEvent(String aggregateId) {
        publisher.publish(OutboxPublisher.OutboxMessage.builder()
                .eventType("OrderCreated")
                .aggregateType("Order")
                .aggregateId(aggregateId)
                .destination("test:target")
                .payload("{\"orderId\":\"" + aggregateId + "\"}")
                .build());
    }

    @Transactional
    void publishTestEventWithMaxRetries(String aggregateId, int maxRetries) {
        publisher.publish(OutboxPublisher.OutboxMessage.builder()
                .eventType("OrderCreated")
                .aggregateType("Order")
                .aggregateId(aggregateId)
                .destination("test:target")
                .payload("{}")
                .maxRetries(maxRetries)
                .build());
    }

    @Transactional
    void publishTestEventWithIdempotencyKey(String aggregateId, String key) {
        publisher.publish(OutboxPublisher.OutboxMessage.builder()
                .eventType("OrderCreated")
                .aggregateType("Order")
                .aggregateId(aggregateId)
                .destination("test:target")
                .payload("{}")
                .idempotencyKey(key)
                .build());
    }

    // ─── test configuration ──────────────────────────────────────

    @Configuration
    @EnableAutoConfiguration
    static class TestConfig {

        @Bean
        public OutboxDeliveryHandler testDeliveryHandler() {
            return new OutboxDeliveryHandler() {
                @Override
                public String destinationPrefix() { return "test:"; }

                @Override
                public void deliver(OutboxEvent event) throws OutboxDeliveryException {
                    if (failUntilCount.getAndDecrement() > 0) {
                        throw new OutboxDeliveryException("Simulated failure", true);
                    }
                    deliveredEvents.add(event);
                }
            };
        }
    }
}
