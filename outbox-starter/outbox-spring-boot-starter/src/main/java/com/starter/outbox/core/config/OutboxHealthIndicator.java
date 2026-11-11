package com.starter.outbox.core.config;

import com.starter.outbox.core.dispatcher.OutboxDispatcher;
import com.starter.outbox.core.entity.OutboxStatus;
import com.starter.outbox.core.repository.OutboxEventRepository;
import com.starter.outbox.delivery.DeliveryHandlerRegistry;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Spring Boot Actuator health indicator for the outbox system.
 *
 * <p>Reports UP when the dispatcher is active and the dead-letter
 * queue is below threshold. Reports DOWN if the dispatcher is paused
 * or there are too many dead-lettered events.</p>
 */
public class OutboxHealthIndicator implements HealthIndicator {

    private static final long DEAD_LETTER_THRESHOLD = 100;

    private final OutboxDispatcher dispatcher;
    private final OutboxEventRepository repository;
    private final DeliveryHandlerRegistry registry;

    public OutboxHealthIndicator(OutboxDispatcher dispatcher,
                                 OutboxEventRepository repository,
                                 DeliveryHandlerRegistry registry) {
        this.dispatcher = dispatcher;
        this.repository = repository;
        this.registry = registry;
    }

    @Override
    public Health health() {
        long pending = repository.countByStatus(OutboxStatus.PENDING);
        long retrying = repository.countByStatus(OutboxStatus.RETRYING);
        long deadLettered = repository.countByStatus(OutboxStatus.DEAD_LETTER);
        boolean active = dispatcher.isEnabled();

        Health.Builder builder = active ? Health.up() : Health.down();

        builder.withDetail("dispatcherEnabled", active)
               .withDetail("pending", pending)
               .withDetail("retrying", retrying)
               .withDetail("deadLettered", deadLettered)
               .withDetail("registeredHandlers", registry.registeredPrefixes());

        if (deadLettered > DEAD_LETTER_THRESHOLD) {
            builder.down()
                   .withDetail("warning", "Dead-letter count exceeds threshold: " + DEAD_LETTER_THRESHOLD);
        }

        return builder.build();
    }
}
