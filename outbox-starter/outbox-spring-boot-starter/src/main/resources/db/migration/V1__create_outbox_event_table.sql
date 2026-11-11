-- =============================================================
-- V1__create_outbox_event_table.sql
-- Transactional Outbox Pattern — core schema
-- =============================================================

CREATE TABLE outbox_event (
    id              VARCHAR(36)     PRIMARY KEY,
    event_type      VARCHAR(255)    NOT NULL,
    aggregate_type  VARCHAR(255)    NOT NULL,
    aggregate_id    VARCHAR(255)    NOT NULL,
    payload         TEXT            NOT NULL,
    headers         TEXT,
    destination     VARCHAR(500)    NOT NULL,
    status          VARCHAR(20)     NOT NULL DEFAULT 'PENDING',
    retries         INTEGER         NOT NULL DEFAULT 0,
    max_retries     INTEGER         NOT NULL DEFAULT 5,
    next_attempt_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    processed_at    TIMESTAMP WITH TIME ZONE,
    last_error      TEXT,
    idempotency_key VARCHAR(255)    NOT NULL,
    version         BIGINT          NOT NULL DEFAULT 0
);

-- Index for the dispatcher's polling query (most critical)
CREATE INDEX idx_outbox_status_next_attempt
    ON outbox_event (status, next_attempt_at)
    WHERE status IN ('PENDING', 'RETRYING');

-- Index for aggregate-level queries
CREATE INDEX idx_outbox_aggregate
    ON outbox_event (aggregate_type, aggregate_id);

-- Index for destination-based queries
CREATE INDEX idx_outbox_destination
    ON outbox_event (destination);

-- Index for archival queries
CREATE INDEX idx_outbox_created_at
    ON outbox_event (created_at);

-- Index for processed event cleanup
CREATE INDEX idx_outbox_processed_at
    ON outbox_event (processed_at)
    WHERE status = 'PROCESSED';

-- Unique constraint on idempotency key
CREATE UNIQUE INDEX idx_outbox_idempotency_key
    ON outbox_event (idempotency_key);

COMMENT ON TABLE outbox_event IS
    'Transactional outbox: messages written alongside business data, '
    'delivered asynchronously with at-least-once semantics.';
