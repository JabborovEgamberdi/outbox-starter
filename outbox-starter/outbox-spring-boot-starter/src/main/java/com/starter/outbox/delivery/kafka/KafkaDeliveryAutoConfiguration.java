package com.starter.outbox.delivery.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Auto-configures the Kafka delivery handler when spring-kafka is on the classpath
 * and {@code outbox.delivery.kafka.enabled=true}.
 */
@Configuration
@ConditionalOnClass(KafkaTemplate.class)
@ConditionalOnProperty(prefix = "outbox.delivery.kafka", name = "enabled", havingValue = "true")
public class KafkaDeliveryAutoConfiguration {

    @Bean
    public KafkaDeliveryHandler kafkaDeliveryHandler(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper) {
        return new KafkaDeliveryHandler(kafkaTemplate, objectMapper, 30);
    }
}
