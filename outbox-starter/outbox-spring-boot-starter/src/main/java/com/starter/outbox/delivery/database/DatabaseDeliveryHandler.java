package com.starter.outbox.delivery.database;

import com.starter.outbox.core.entity.OutboxEvent;
import com.starter.outbox.delivery.OutboxDeliveryException;
import com.starter.outbox.delivery.OutboxDeliveryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;

/**
 * Delivers outbox events by writing to an external database (audit store,
 * data warehouse, another service's DB, etc.).
 *
 * <p>Destination format: {@code db:<datasource-name>}</p>
 *
 * <p>The target table is a generic event sink:
 * <pre>
 * CREATE TABLE external_events (
 *     event_id        VARCHAR(36) PRIMARY KEY,
 *     event_type      VARCHAR(255),
 *     aggregate_type  VARCHAR(255),
 *     aggregate_id    VARCHAR(255),
 *     payload         TEXT,
 *     received_at     TIMESTAMP
 * );
 * </pre>
 *
 * <p><b>Note:</b> This writes to a DIFFERENT database/DataSource, so
 * it is NOT part of the outbox's own transaction. The outbox dispatcher
 * treats it as an external call (at-least-once delivery).</p>
 *
 * <h3>Example</h3>
 * <pre>{@code
 * outboxPublisher.publish(OutboxMessage.builder()
 *     .eventType("OrderCreated")
 *     .aggregateType("Order")
 *     .aggregateId("order-123")
 *     .destination("db:audit-store")
 *     .payload(json)
 *     .build());
 * }</pre>
 */
public class DatabaseDeliveryHandler implements OutboxDeliveryHandler {

    private static final Logger log = LoggerFactory.getLogger(DatabaseDeliveryHandler.class);
    private static final String PREFIX = "db:";

    private static final String INSERT_SQL = """
            INSERT INTO external_events (event_id, event_type, aggregate_type,
                                         aggregate_id, payload, idempotency_key, received_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (event_id) DO NOTHING
            """;

    private final Map<String, DataSource> dataSourceMap;

    public DatabaseDeliveryHandler(Map<String, DataSource> dataSourceMap) {
        this.dataSourceMap = dataSourceMap;
    }

    @Override
    public String destinationPrefix() {
        return PREFIX;
    }

    @Override
    public void deliver(OutboxEvent event) throws OutboxDeliveryException {
        String dsName = extractTarget(event);
        DataSource ds = dataSourceMap.get(dsName);

        if (ds == null) {
            throw new OutboxDeliveryException(
                    "No DataSource configured for: " + dsName, false);
        }

        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {

            ps.setString(1, event.getId());
            ps.setString(2, event.getEventType());
            ps.setString(3, event.getAggregateType());
            ps.setString(4, event.getAggregateId());
            ps.setString(5, event.getPayload());
            ps.setString(6, event.getIdempotencyKey());
            ps.setTimestamp(7, Timestamp.from(Instant.now()));

            int rows = ps.executeUpdate();
            log.debug("DB delivery success: ds={}, rows={}, eventId={}", dsName, rows, event.getId());

        } catch (OutboxDeliveryException e) {
            throw e;
        } catch (Exception e) {
            throw new OutboxDeliveryException(
                    "DB delivery to '%s' failed: %s".formatted(dsName, e.getMessage()),
                    e, true);
        }
    }
}
