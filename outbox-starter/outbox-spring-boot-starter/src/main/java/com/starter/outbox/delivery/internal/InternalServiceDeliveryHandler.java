package com.starter.outbox.delivery.internal;

import com.starter.outbox.core.entity.OutboxEvent;
import com.starter.outbox.delivery.OutboxDeliveryException;
import com.starter.outbox.delivery.OutboxDeliveryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Delivers outbox events to in-process handlers (other modules/services
 * in the same application).
 *
 * <p>Destination format: {@code internal:<handler-name>}</p>
 *
 * <p>Handlers register themselves at startup via {@link #register(String, Consumer)}.</p>
 *
 * <h3>Example</h3>
 * <pre>{@code
 * // Registration (e.g. in a @PostConstruct)
 * internalHandler.register("notifications", event -> {
 *     notificationService.sendNotification(event.getPayload());
 * });
 *
 * // Publishing
 * outboxPublisher.publish(OutboxMessage.builder()
 *     .eventType("OrderCreated")
 *     .aggregateType("Order")
 *     .aggregateId("order-123")
 *     .destination("internal:notifications")
 *     .payload(json)
 *     .build());
 * }</pre>
 */
public class InternalServiceDeliveryHandler implements OutboxDeliveryHandler {

    private static final Logger log = LoggerFactory.getLogger(InternalServiceDeliveryHandler.class);
    private static final String PREFIX = "internal:";

    private final Map<String, Consumer<OutboxEvent>> handlers = new ConcurrentHashMap<>();

    @Override
    public String destinationPrefix() {
        return PREFIX;
    }

    /**
     * Register a named handler for internal delivery.
     */
    public void register(String name, Consumer<OutboxEvent> handler) {
        handlers.put(name, handler);
        log.info("Registered internal outbox handler: {}", name);
    }

    @Override
    public void deliver(OutboxEvent event) throws OutboxDeliveryException {
        String handlerName = extractTarget(event);
        Consumer<OutboxEvent> handler = handlers.get(handlerName);

        if (handler == null) {
            throw new OutboxDeliveryException(
                    "No internal handler registered for: " + handlerName, false);
        }

        try {
            handler.accept(event);
            log.debug("Internal delivery success: handler={}, eventId={}",
                    handlerName, event.getId());
        } catch (Exception e) {
            throw new OutboxDeliveryException(
                    "Internal handler '%s' failed: %s".formatted(handlerName, e.getMessage()),
                    e, true);
        }
    }
}
