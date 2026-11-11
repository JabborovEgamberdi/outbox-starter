package com.starter.outbox.delivery;

import com.starter.outbox.core.entity.OutboxEvent;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class DeliveryHandlerRegistryTest {

    @Test
    void shouldResolveCorrectHandlerByPrefix() {
        OutboxDeliveryHandler kafkaHandler = mock(OutboxDeliveryHandler.class);
        when(kafkaHandler.destinationPrefix()).thenReturn("kafka:");
        OutboxDeliveryHandler restHandler = mock(OutboxDeliveryHandler.class);
        when(restHandler.destinationPrefix()).thenReturn("rest:");

        DeliveryHandlerRegistry registry = new DeliveryHandlerRegistry(
                List.of(kafkaHandler, restHandler));

        OutboxEvent kafkaEvent = OutboxEvent.builder()
                .eventType("Test").aggregateType("Test").aggregateId("1")
                .payload("{}").destination("kafka:orders").build();

        OutboxEvent restEvent = OutboxEvent.builder()
                .eventType("Test").aggregateType("Test").aggregateId("1")
                .payload("{}").destination("rest:payments").build();

        assertThat(registry.resolve(kafkaEvent)).isSameAs(kafkaHandler);
        assertThat(registry.resolve(restEvent)).isSameAs(restHandler);
    }

    @Test
    void shouldThrowNonRetryableForUnknownDestination() {
        DeliveryHandlerRegistry registry = new DeliveryHandlerRegistry(List.of());

        OutboxEvent event = OutboxEvent.builder()
                .eventType("Test").aggregateType("Test").aggregateId("1")
                .payload("{}").destination("unknown:target").build();

        assertThatThrownBy(() -> registry.resolve(event))
                .isInstanceOf(OutboxDeliveryException.class)
                .satisfies(ex -> assertThat(((OutboxDeliveryException) ex).isRetryable()).isFalse());
    }

    @Test
    void shouldExposeRegisteredPrefixes() {
        OutboxDeliveryHandler handler = mock(OutboxDeliveryHandler.class);
        when(handler.destinationPrefix()).thenReturn("kafka:");

        DeliveryHandlerRegistry registry = new DeliveryHandlerRegistry(List.of(handler));

        assertThat(registry.registeredPrefixes()).containsExactly("kafka:");
    }
}
