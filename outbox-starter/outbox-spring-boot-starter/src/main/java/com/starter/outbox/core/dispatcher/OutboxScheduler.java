package com.starter.outbox.core.dispatcher;

import com.starter.outbox.core.config.OutboxProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Spring-scheduled trigger for the {@link OutboxDispatcher}.
 *
 * <p>Uses a fixed-delay schedule (configurable via properties) to
 * continuously poll for pending outbox events.</p>
 */
public class OutboxScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxScheduler.class);

    private final OutboxDispatcher dispatcher;

    public OutboxScheduler(OutboxDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Scheduled(fixedDelayString = "${outbox.poll-interval-ms:1000}")
    public void poll() {
        try {
            dispatcher.dispatchBatch();
        } catch (Exception e) {
            log.error("Outbox dispatch cycle failed", e);
        }
    }
}
