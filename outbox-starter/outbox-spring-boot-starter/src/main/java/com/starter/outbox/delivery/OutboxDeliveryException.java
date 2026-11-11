package com.starter.outbox.delivery;

/**
 * Thrown by {@link OutboxDeliveryHandler} implementations when delivery fails.
 * The dispatcher catches this and schedules a retry (or dead-letters).
 */
public class OutboxDeliveryException extends RuntimeException {

    private final boolean retryable;

    public OutboxDeliveryException(String message, boolean retryable) {
        super(message);
        this.retryable = retryable;
    }

    public OutboxDeliveryException(String message, Throwable cause, boolean retryable) {
        super(message, cause);
        this.retryable = retryable;
    }

    /**
     * If {@code false}, the event is immediately dead-lettered
     * without further retries (e.g. 4xx from a REST API).
     */
    public boolean isRetryable() {
        return retryable;
    }
}
