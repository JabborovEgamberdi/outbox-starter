package com.starter.outbox.core;

import com.starter.outbox.core.config.OutboxProperties;
import com.starter.outbox.core.dispatcher.OutboxDispatcher;
import com.starter.outbox.core.entity.OutboxEvent;
import com.starter.outbox.core.entity.OutboxStatus;
import com.starter.outbox.core.metrics.OutboxMetrics;
import com.starter.outbox.core.repository.OutboxEventRepository;
import com.starter.outbox.delivery.DeliveryHandlerRegistry;
import com.starter.outbox.delivery.OutboxDeliveryException;
import com.starter.outbox.delivery.OutboxDeliveryHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxDispatcherTest {

    @Mock private OutboxEventRepository repository;
    @Mock private DeliveryHandlerRegistry registry;
    @Mock private OutboxDeliveryHandler deliveryHandler;

    private OutboxDispatcher dispatcher;
    private OutboxProperties properties;
    private OutboxMetrics metrics;

    @BeforeEach
    void setUp() {
        properties = new OutboxProperties();
        properties.setBatchSize(10);
        properties.setRetryBaseDelay(Duration.ofSeconds(5));
        properties.setRetryMaxBackoff(Duration.ofHours(1));

        metrics = new OutboxMetrics(new SimpleMeterRegistry());
        dispatcher = new OutboxDispatcher(repository, registry, properties, metrics);
    }

    @Test
    void dispatchBatch_shouldDeliverAndMarkProcessed() {
        OutboxEvent event = buildEvent("kafka:orders");
        when(repository.findAndLockBatch(any(), anyInt())).thenReturn(List.of(event));
        when(registry.resolve(event)).thenReturn(deliveryHandler);

        int result = dispatcher.dispatchBatch();

        assertThat(result).isEqualTo(1);
        verify(deliveryHandler).deliver(event);
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PROCESSED);
        verify(repository).save(event);
    }

    @Test
    void dispatchBatch_shouldRetryOnFailure() {
        OutboxEvent event = buildEvent("kafka:orders");
        when(repository.findAndLockBatch(any(), anyInt())).thenReturn(List.of(event));
        when(registry.resolve(event)).thenReturn(deliveryHandler);
        doThrow(new OutboxDeliveryException("Broker down", true))
                .when(deliveryHandler).deliver(event);

        int result = dispatcher.dispatchBatch();

        assertThat(result).isZero();
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.RETRYING);
        assertThat(event.getRetries()).isEqualTo(1);
        assertThat(event.getLastError()).contains("Broker down");
    }

    @Test
    void dispatchBatch_shouldDeadLetterOnNonRetryable() {
        OutboxEvent event = buildEvent("rest:payment");
        when(repository.findAndLockBatch(any(), anyInt())).thenReturn(List.of(event));
        when(registry.resolve(event)).thenReturn(deliveryHandler);
        doThrow(new OutboxDeliveryException("404 Not Found", false))
                .when(deliveryHandler).deliver(event);

        dispatcher.dispatchBatch();

        assertThat(event.getStatus()).isEqualTo(OutboxStatus.DEAD_LETTER);
    }

    @Test
    void dispatchBatch_shouldReturnZeroWhenEmpty() {
        when(repository.findAndLockBatch(any(), anyInt())).thenReturn(Collections.emptyList());

        int result = dispatcher.dispatchBatch();

        assertThat(result).isZero();
        verifyNoInteractions(registry);
    }

    @Test
    void dispatchBatch_shouldDoNothingWhenPaused() {
        dispatcher.pause();
        int result = dispatcher.dispatchBatch();

        assertThat(result).isZero();
        verifyNoInteractions(repository);
    }

    @Test
    void calculateNextAttempt_shouldUseExponentialBackoff() {
        // retry 0 → 5s, retry 1 → 10s, retry 2 → 20s, retry 3 → 40s
        Instant now = Instant.now();

        Instant next0 = dispatcher.calculateNextAttempt(0);
        Instant next1 = dispatcher.calculateNextAttempt(1);
        Instant next2 = dispatcher.calculateNextAttempt(2);

        assertThat(next0).isBefore(next1);
        assertThat(next1).isBefore(next2);
    }

    @Test
    void pause_resume_shouldToggleState() {
        assertThat(dispatcher.isEnabled()).isTrue();
        dispatcher.pause();
        assertThat(dispatcher.isEnabled()).isFalse();
        dispatcher.resume();
        assertThat(dispatcher.isEnabled()).isTrue();
    }

    private OutboxEvent buildEvent(String destination) {
        return OutboxEvent.builder()
                .eventType("OrderCreated")
                .aggregateType("Order")
                .aggregateId("order-1")
                .payload("{}")
                .destination(destination)
                .maxRetries(5)
                .build();
    }
}
