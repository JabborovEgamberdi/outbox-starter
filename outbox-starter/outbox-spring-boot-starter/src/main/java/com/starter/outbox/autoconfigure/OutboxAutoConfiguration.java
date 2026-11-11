package com.starter.outbox.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.starter.outbox.core.admin.OutboxAdminController;
import com.starter.outbox.core.archive.OutboxArchiveJob;
import com.starter.outbox.core.config.OutboxHealthIndicator;
import com.starter.outbox.core.config.OutboxProperties;
import com.starter.outbox.core.dispatcher.OutboxDispatcher;
import com.starter.outbox.core.dispatcher.OutboxScheduler;
import com.starter.outbox.core.metrics.OutboxMetrics;
import com.starter.outbox.core.publisher.DefaultOutboxPublisher;
import com.starter.outbox.core.publisher.OutboxPublisher;
import com.starter.outbox.core.repository.OutboxEventRepository;
import com.starter.outbox.delivery.DeliveryHandlerRegistry;
import com.starter.outbox.delivery.OutboxDeliveryHandler;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.List;

/**
 * Spring Boot autoconfiguration for the Transactional Outbox starter.
 *
 * <p>Wires up all core beans when {@code outbox.enabled=true} (default).
 * Delivery handlers are collected from the application context and
 * registered in the {@link DeliveryHandlerRegistry}.</p>
 */
@AutoConfiguration
@EnableConfigurationProperties(OutboxProperties.class)
@EnableJpaRepositories(basePackages = "com.starter.outbox.core.repository")
@EntityScan(basePackages = "com.starter.outbox.core.entity")
@EnableScheduling
@ConditionalOnProperty(prefix = "outbox", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OutboxAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public OutboxMetrics outboxMetrics(MeterRegistry meterRegistry) {
        return new OutboxMetrics(meterRegistry);
    }

    @Bean
    @ConditionalOnMissingBean
    public DeliveryHandlerRegistry deliveryHandlerRegistry(
            List<OutboxDeliveryHandler> handlers) {
        return new DeliveryHandlerRegistry(handlers);
    }

    @Bean
    @ConditionalOnMissingBean(OutboxPublisher.class)
    public DefaultOutboxPublisher outboxPublisher(
            OutboxEventRepository repository,
            ObjectMapper objectMapper,
            OutboxMetrics metrics,
            OutboxProperties properties) {
        return new DefaultOutboxPublisher(repository, objectMapper, metrics,
                properties.getDefaultMaxRetries());
    }

    @Bean
    @ConditionalOnMissingBean
    public OutboxDispatcher outboxDispatcher(
            OutboxEventRepository repository,
            DeliveryHandlerRegistry registry,
            OutboxProperties properties,
            OutboxMetrics metrics) {
        return new OutboxDispatcher(repository, registry, properties, metrics);
    }

    @Bean
    @ConditionalOnMissingBean
    public OutboxScheduler outboxScheduler(OutboxDispatcher dispatcher) {
        return new OutboxScheduler(dispatcher);
    }

    @Bean
    @ConditionalOnMissingBean
    public OutboxArchiveJob outboxArchiveJob(
            OutboxEventRepository repository,
            OutboxProperties properties,
            OutboxMetrics metrics) {
        return new OutboxArchiveJob(repository, properties, metrics);
    }

    @Bean
    @ConditionalOnMissingBean
    public OutboxHealthIndicator outboxHealthIndicator(
            OutboxDispatcher dispatcher,
            OutboxEventRepository repository,
            DeliveryHandlerRegistry registry) {
        return new OutboxHealthIndicator(dispatcher, repository, registry);
    }

    @Bean
    @ConditionalOnMissingBean
    public OutboxAdminController outboxAdminController(
            OutboxEventRepository repository,
            OutboxDispatcher dispatcher) {
        return new OutboxAdminController(repository, dispatcher);
    }
}
