package com.starter.outbox.delivery;

import com.starter.outbox.core.entity.OutboxEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry that maps destination prefixes to their {@link OutboxDeliveryHandler}.
 *
 * <p>Handlers are registered at startup. At dispatch time the registry selects
 * the handler whose prefix matches the event's destination.</p>
 */
public class DeliveryHandlerRegistry {

    private static final Logger log = LoggerFactory.getLogger(DeliveryHandlerRegistry.class);

    private final Map<String, OutboxDeliveryHandler> handlers = new ConcurrentHashMap<>();

    public DeliveryHandlerRegistry(List<OutboxDeliveryHandler> handlerList) {
        for (OutboxDeliveryHandler handler : handlerList) {
            String prefix = handler.destinationPrefix();
            OutboxDeliveryHandler previous = handlers.put(prefix, handler);
            if (previous != null) {
                log.warn("Duplicate outbox delivery handler for prefix '{}': {} replaced by {}",
                        prefix, previous.getClass().getSimpleName(), handler.getClass().getSimpleName());
            }
            log.info("Registered outbox delivery handler: prefix='{}' → {}",
                    prefix, handler.getClass().getSimpleName());
        }
    }

    /**
     * Resolve the handler for the given event's destination.
     *
     * @throws OutboxDeliveryException if no handler matches (non-retryable)
     */
    public OutboxDeliveryHandler resolve(OutboxEvent event) {
        String destination = event.getDestination();
        for (Map.Entry<String, OutboxDeliveryHandler> entry : handlers.entrySet()) {
            if (destination.startsWith(entry.getKey())) {
                return entry.getValue();
            }
        }
        throw new OutboxDeliveryException(
                "No delivery handler registered for destination: " + destination, false);
    }

    /** All registered prefixes (for health checks / admin). */
    public Set<String> registeredPrefixes() {
        return Collections.unmodifiableSet(handlers.keySet());
    }
}
