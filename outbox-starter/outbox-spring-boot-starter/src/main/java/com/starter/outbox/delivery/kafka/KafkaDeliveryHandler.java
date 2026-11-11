package com.starter.outbox.delivery.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starter.outbox.core.entity.OutboxEvent;
import com.starter.outbox.delivery.OutboxDeliveryException;
import com.starter.outbox.delivery.OutboxDeliveryHandler;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Delivers outbox events to Apache Kafka topics.
 *
 * <p>Destination format: {@code kafka:<topic-name>}</p>
 *
 * <p>The event's {@code aggregateId} is used as the Kafka message key
 * to guarantee ordering within a partition for the same aggregate.</p>
 *
 * <h3>Example</h3>
 * <pre>{@code
 * outboxPublisher.publish(OutboxMessage.builder()
 *     .eventType("OrderCreated")
 *     .aggregateType("Order")
 *     .aggregateId("order-123")
 *     .destination("kafka:orders-topic")
 *     .payload(json)
 *     .build());
 * }</pre>
 */
public class KafkaDeliveryHandler implements OutboxDeliveryHandler {

    private static final Logger log = LoggerFactory.getLogger(KafkaDeliveryHandler.class);
    private static final String PREFIX = "kafka:";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final long sendTimeoutSeconds;

    public KafkaDeliveryHandler(KafkaTemplate<String, String> kafkaTemplate,
                                ObjectMapper objectMapper,
                                long sendTimeoutSeconds) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.sendTimeoutSeconds = sendTimeoutSeconds;
    }

    @Override
    public String destinationPrefix() {
        return PREFIX;
    }

    @Override
    public void deliver(OutboxEvent event) throws OutboxDeliveryException {
        String topic = extractTarget(event);

        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    topic, null, event.getAggregateId(), event.getPayload());

            // Copy outbox headers to Kafka headers
            addHeaders(record, event);

            // Standard tracing headers
            record.headers().add(new RecordHeader("outbox-event-id",
                    event.getId().getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("outbox-event-type",
                    event.getEventType().getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("outbox-idempotency-key",
                    event.getIdempotencyKey().getBytes(StandardCharsets.UTF_8)));

            kafkaTemplate.send(record).get(sendTimeoutSeconds, TimeUnit.SECONDS);

            log.debug("Kafka delivery success: topic={}, key={}, eventId={}",
                    topic, event.getAggregateId(), event.getId());

        } catch (Exception e) {
            throw new OutboxDeliveryException(
                    "Kafka delivery failed for topic '%s': %s".formatted(topic, e.getMessage()),
                    e, true);
        }
    }

    private void addHeaders(ProducerRecord<String, String> record, OutboxEvent event) {
        if (event.getHeaders() == null) return;
        try {
            Map<String, String> headers = objectMapper.readValue(
                    event.getHeaders(), new TypeReference<>() {});
            headers.forEach((k, v) ->
                    record.headers().add(new RecordHeader(k, v.getBytes(StandardCharsets.UTF_8))));
        } catch (Exception e) {
            log.warn("Failed to parse outbox headers for event {}: {}", event.getId(), e.getMessage());
        }
    }
}
