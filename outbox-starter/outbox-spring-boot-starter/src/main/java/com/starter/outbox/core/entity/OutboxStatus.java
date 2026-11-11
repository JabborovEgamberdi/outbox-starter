package com.starter.outbox.core.entity;

/**
 * Lifecycle states of an outbox event.
 *
 * <pre>
 *  PENDING ──► PROCESSED
 *    │
 *    ▼
 *  RETRYING ──► PROCESSED
 *    │
 *    ▼
 *  DEAD_LETTER ──(manual reset)──► PENDING
 * </pre>
 */
public enum OutboxStatus {

    /** Awaiting first delivery attempt. */
    PENDING,

    /** Delivery failed; will be retried after backoff. */
    RETRYING,

    /** Successfully delivered. Safe to archive/delete after retention. */
    PROCESSED,

    /** Exhausted all retry attempts. Requires operator intervention. */
    DEAD_LETTER
}
