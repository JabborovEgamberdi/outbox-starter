package com.starter.outbox.core.entity;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

/**
 * Core outbox event entity. Each row represents one message that must
 * be delivered to an external system exactly once (at-least-once semantics).
 *
 * <p>Written in the SAME transaction as the business data so that the
 * message is guaranteed to exist if-and-only-if the business state changed.</p>
 */
@Entity
@Table(
    name = "outbox_event",
    indexes = {
        @Index(name = "idx_outbox_status_next_attempt", columnList = "status, next_attempt_at"),
        @Index(name = "idx_outbox_aggregate", columnList = "aggregate_type, aggregate_id"),
        @Index(name = "idx_outbox_destination", columnList = "destination"),
        @Index(name = "idx_outbox_created_at", columnList = "created_at")
    }
)
public class OutboxEvent {

    @Id
    @Column(name = "id", updatable = false, nullable = false, length = 36)
    private String id;

    /** Logical event type, e.g. "OrderCreated", "PaymentCompleted" */
    @Column(name = "event_type", nullable = false, length = 255)
    private String eventType;

    /** Domain aggregate type, e.g. "Order", "Invoice" */
    @Column(name = "aggregate_type", nullable = false, length = 255)
    private String aggregateType;

    /** Domain aggregate identifier */
    @Column(name = "aggregate_id", nullable = false, length = 255)
    private String aggregateId;

    /** Serialized payload (usually JSON) */
    @Column(name = "payload", nullable = false, columnDefinition = "TEXT")
    private String payload;

    /** Arbitrary headers as JSON map (correlation-id, content-type, etc.) */
    @Column(name = "headers", columnDefinition = "TEXT")
    private String headers;

    /**
     * Logical destination — resolved to a physical target by a matching
     * {@link com.starter.outbox.delivery.OutboxDeliveryHandler}.
     * Examples: "kafka:orders-topic", "rest:payment-service", "db:audit-store"
     */
    @Column(name = "destination", nullable = false, length = 500)
    private String destination;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private OutboxStatus status = OutboxStatus.PENDING;

    @Column(name = "retries", nullable = false)
    private int retries = 0;

    @Column(name = "max_retries", nullable = false)
    private int maxRetries = 5;

    @Column(name = "next_attempt_at", nullable = false)
    private Instant nextAttemptAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Column(name = "processed_at")
    private Instant processedAt;

    /** Last error message for diagnostics */
    @Column(name = "last_error", columnDefinition = "TEXT")
    private String lastError;

    /**
     * Idempotency key — consumers can use this to de-duplicate.
     * Defaults to the event id, but can be overridden.
     */
    @Column(name = "idempotency_key", nullable = false, length = 255)
    private String idempotencyKey;

    /** Optimistic lock for safe concurrent updates */
    @Version
    @Column(name = "version")
    private long version;

    // ── lifecycle callbacks ───────────────────────────────────────────

    @PrePersist
    protected void onCreate() {
        Instant now = Instant.now();
        if (id == null) {
            id = UUID.randomUUID().toString();
        }
        if (idempotencyKey == null) {
            idempotencyKey = id;
        }
        createdAt = now;
        updatedAt = now;
        if (nextAttemptAt == null) {
            nextAttemptAt = now;
        }
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = Instant.now();
    }

    // ── constructors ─────────────────────────────────────────────────

    protected OutboxEvent() { /* JPA */ }

    private OutboxEvent(Builder b) {
        this.id            = b.id;
        this.eventType     = b.eventType;
        this.aggregateType = b.aggregateType;
        this.aggregateId   = b.aggregateId;
        this.payload       = b.payload;
        this.headers       = b.headers;
        this.destination   = b.destination;
        this.maxRetries    = b.maxRetries;
        this.idempotencyKey = b.idempotencyKey;
    }

    public static Builder builder() { return new Builder(); }

    // ── builder ──────────────────────────────────────────────────────

    public static class Builder {
        private String id;
        private String eventType;
        private String aggregateType;
        private String aggregateId;
        private String payload;
        private String headers;
        private String destination;
        private int    maxRetries = 5;
        private String idempotencyKey;

        public Builder id(String id)                     { this.id = id; return this; }
        public Builder eventType(String eventType)       { this.eventType = eventType; return this; }
        public Builder aggregateType(String aggregateType) { this.aggregateType = aggregateType; return this; }
        public Builder aggregateId(String aggregateId)   { this.aggregateId = aggregateId; return this; }
        public Builder payload(String payload)           { this.payload = payload; return this; }
        public Builder headers(String headers)           { this.headers = headers; return this; }
        public Builder destination(String destination)   { this.destination = destination; return this; }
        public Builder maxRetries(int maxRetries)        { this.maxRetries = maxRetries; return this; }
        public Builder idempotencyKey(String key)        { this.idempotencyKey = key; return this; }

        public OutboxEvent build() {
            if (eventType == null || aggregateType == null || aggregateId == null
                    || payload == null || destination == null) {
                throw new IllegalStateException(
                    "eventType, aggregateType, aggregateId, payload, and destination are required");
            }
            return new OutboxEvent(this);
        }
    }

    // ── getters & domain methods ─────────────────────────────────────

    public String getId()            { return id; }
    public String getEventType()     { return eventType; }
    public String getAggregateType() { return aggregateType; }
    public String getAggregateId()   { return aggregateId; }
    public String getPayload()       { return payload; }
    public String getHeaders()       { return headers; }
    public String getDestination()   { return destination; }
    public OutboxStatus getStatus()  { return status; }
    public int getRetries()          { return retries; }
    public int getMaxRetries()       { return maxRetries; }
    public Instant getNextAttemptAt() { return nextAttemptAt; }
    public Instant getCreatedAt()    { return createdAt; }
    public Instant getUpdatedAt()    { return updatedAt; }
    public Instant getProcessedAt()  { return processedAt; }
    public String getLastError()     { return lastError; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public long getVersion()         { return version; }

    /** Mark as successfully delivered. */
    public void markProcessed() {
        this.status = OutboxStatus.PROCESSED;
        this.processedAt = Instant.now();
    }

    /** Record a delivery failure and schedule the next retry. */
    public void markFailed(String error, Instant nextAttempt) {
        this.retries++;
        this.lastError = error;
        if (this.retries >= this.maxRetries) {
            this.status = OutboxStatus.DEAD_LETTER;
        } else {
            this.status = OutboxStatus.RETRYING;
            this.nextAttemptAt = nextAttempt;
        }
    }

    /** Force into dead-letter state (manual or policy-based). */
    public void markDeadLetter(String reason) {
        this.status = OutboxStatus.DEAD_LETTER;
        this.lastError = reason;
    }

    /** Reset for manual re-processing via admin API. */
    public void resetForRetry() {
        this.status = OutboxStatus.PENDING;
        this.retries = 0;
        this.lastError = null;
        this.nextAttemptAt = Instant.now();
        this.processedAt = null;
    }

    @Override
    public String toString() {
        return "OutboxEvent{id='%s', type='%s', aggregate='%s/%s', dest='%s', status=%s, retries=%d}"
                .formatted(id, eventType, aggregateType, aggregateId, destination, status, retries);
    }
}
