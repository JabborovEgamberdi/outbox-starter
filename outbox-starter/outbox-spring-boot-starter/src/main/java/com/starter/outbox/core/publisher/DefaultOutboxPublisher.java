package com.starter.outbox.core.publisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starter.outbox.core.entity.OutboxEvent;
import com.starter.outbox.core.metrics.OutboxMetrics;
import com.starter.outbox.core.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Default publisher that persists {@link OutboxEvent} rows inside the
 * caller's existing transaction.
 *
 * <p><b>Important:</b> This bean MUST participate in the same transaction
 * as the business operation — {@code Propagation.MANDATORY} enforces this
 * at runtime.</p>
 */
public class DefaultOutboxPublisher implements OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(DefaultOutboxPublisher.class);

    private final OutboxEventRepository repository;
    private final ObjectMapper objectMapper;
    private final OutboxMetrics metrics;
    private final int defaultMaxRetries;

    public DefaultOutboxPublisher(OutboxEventRepository repository,
                                  ObjectMapper objectMapper,
                                  OutboxMetrics metrics,
                                  int defaultMaxRetries) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
        this.defaultMaxRetries = defaultMaxRetries;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public OutboxEvent publish(OutboxMessage message) {
        String headersJson = serializeHeaders(message.headers());

        OutboxEvent event = OutboxEvent.builder()
                .eventType(message.eventType())
                .aggregateType(message.aggregateType())
                .aggregateId(message.aggregateId())
                .payload(message.payload())
                .headers(headersJson)
                .destination(message.destination())
                .maxRetries(message.maxRetries() != null ? message.maxRetries() : defaultMaxRetries)
                .idempotencyKey(message.idempotencyKey())
                .build();

        OutboxEvent saved = repository.save(event);

        log.debug("Outbox event published: id={}, type={}, dest={}",
                saved.getId(), saved.getEventType(), saved.getDestination());

        metrics.recordPublished(saved.getDestination(), saved.getEventType());
        return saved;
    }

    private String serializeHeaders(java.util.Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) return null;
        try {
            return objectMapper.writeValueAsString(headers);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot serialize outbox headers", e);
        }
    }
}
