package com.example.orders.config;

import com.starter.outbox.delivery.database.DatabaseDeliveryHandler;
import com.starter.outbox.delivery.internal.InternalServiceDeliveryHandler;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.Map;

/**
 * Example: how to configure the database and internal delivery handlers
 * in your application.
 */
@Configuration
public class DeliveryConfiguration {

    private static final Logger log = LoggerFactory.getLogger(DeliveryConfiguration.class);

    // ─── Database delivery handler ───────────────────────────────

    @Bean
    @ConditionalOnProperty(prefix = "outbox.delivery.database", name = "enabled", havingValue = "true")
    public DatabaseDeliveryHandler databaseDeliveryHandler() {
        // In a real app, configure these from properties
        DataSource auditDs = DataSourceBuilder.create()
                .url("jdbc:postgresql://localhost:5432/audit_db")
                .username("postgres")
                .password("postgres")
                .driverClassName("org.postgresql.Driver")
                .build();

        return new DatabaseDeliveryHandler(Map.of("audit-store", auditDs));
    }

    // ─── Internal service delivery handler ───────────────────────

    @Bean
    public InternalServiceDeliveryHandler internalServiceDeliveryHandler() {
        return new InternalServiceDeliveryHandler();
    }

    @PostConstruct
    public void registerInternalHandlers() {
        // Simulating an internal notification module
        internalServiceDeliveryHandler().register("notifications", event -> {
            log.info("📧 Internal notification received: type={}, aggregateId={}, payload={}",
                    event.getEventType(), event.getAggregateId(),
                    event.getPayload().substring(0, Math.min(200, event.getPayload().length())));
            // In reality: call notificationService.send(...)
        });
    }
}
