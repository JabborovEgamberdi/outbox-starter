package com.starter.outbox.core;

import com.starter.outbox.core.entity.OutboxEvent;
import com.starter.outbox.core.entity.OutboxStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.*;

class OutboxEventTest {

    @Test
    void shouldBuildEventWithAllRequiredFields() {
        OutboxEvent event = OutboxEvent.builder()
                .eventType("OrderCreated")
                .aggregateType("Order")
                .aggregateId("order-123")
                .payload("{\"id\":\"order-123\"}")
                .destination("kafka:orders-topic")
                .build();

        assertThat(event.getEventType()).isEqualTo("OrderCreated");
        assertThat(event.getAggregateType()).isEqualTo("Order");
        assertThat(event.getAggregateId()).isEqualTo("order-123");
        assertThat(event.getDestination()).isEqualTo("kafka:orders-topic");
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(event.getRetries()).isZero();
        assertThat(event.getMaxRetries()).isEqualTo(5);
    }

    @Test
    void shouldRejectBuildWithMissingFields() {
        assertThatThrownBy(() -> OutboxEvent.builder()
                .eventType("OrderCreated")
                .build())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void markProcessed_shouldSetStatusAndTimestamp() {
        OutboxEvent event = buildSampleEvent();
        event.markProcessed();

        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PROCESSED);
        assertThat(event.getProcessedAt()).isNotNull();
    }

    @Test
    void markFailed_shouldIncrementRetriesAndScheduleNext() {
        OutboxEvent event = buildSampleEvent();
        Instant next = Instant.now().plusSeconds(30);

        event.markFailed("Connection refused", next);

        assertThat(event.getStatus()).isEqualTo(OutboxStatus.RETRYING);
        assertThat(event.getRetries()).isEqualTo(1);
        assertThat(event.getLastError()).isEqualTo("Connection refused");
        assertThat(event.getNextAttemptAt()).isEqualTo(next);
    }

    @Test
    void markFailed_shouldDeadLetterWhenMaxRetriesExceeded() {
        OutboxEvent event = OutboxEvent.builder()
                .eventType("Test")
                .aggregateType("Test")
                .aggregateId("1")
                .payload("{}")
                .destination("kafka:test")
                .maxRetries(2)
                .build();

        // Simulate reaching max retries
        event.markFailed("Error 1", Instant.now().plusSeconds(5));
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.RETRYING);

        event.markFailed("Error 2", Instant.now().plusSeconds(10));
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.DEAD_LETTER);
    }

    @Test
    void resetForRetry_shouldClearStateAndResetToPending() {
        OutboxEvent event = buildSampleEvent();
        event.markFailed("Some error", Instant.now().plusSeconds(60));

        event.resetForRetry();

        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(event.getRetries()).isZero();
        assertThat(event.getLastError()).isNull();
        assertThat(event.getProcessedAt()).isNull();
    }

    @Test
    void markDeadLetter_shouldForceDeadLetterStatus() {
        OutboxEvent event = buildSampleEvent();
        event.markDeadLetter("Non-retryable error: 404");

        assertThat(event.getStatus()).isEqualTo(OutboxStatus.DEAD_LETTER);
        assertThat(event.getLastError()).isEqualTo("Non-retryable error: 404");
    }

    private OutboxEvent buildSampleEvent() {
        return OutboxEvent.builder()
                .eventType("OrderCreated")
                .aggregateType("Order")
                .aggregateId("order-123")
                .payload("{\"id\":\"order-123\"}")
                .destination("kafka:orders-topic")
                .build();
    }
}
