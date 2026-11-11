package com.starter.outbox.delivery.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Configuration
@ConditionalOnClass(WebClient.class)
@ConditionalOnProperty(prefix = "outbox.delivery.rest", name = "enabled", havingValue = "true")
public class RestDeliveryAutoConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "outbox.delivery.rest")
    public RestDeliveryProperties restDeliveryProperties() {
        return new RestDeliveryProperties();
    }

    @Bean
    public RestApiDeliveryHandler restApiDeliveryHandler(
            WebClient.Builder webClientBuilder,
            ObjectMapper objectMapper,
            RestDeliveryProperties props) {
        return new RestApiDeliveryHandler(
                webClientBuilder, objectMapper,
                props.getEndpoints(), props.getTimeout());
    }

    public static class RestDeliveryProperties {
        private boolean enabled = false;
        private Duration timeout = Duration.ofSeconds(30);
        private Map<String, String> endpoints = new HashMap<>();

        public boolean isEnabled()               { return enabled; }
        public void setEnabled(boolean v)        { this.enabled = v; }
        public Duration getTimeout()             { return timeout; }
        public void setTimeout(Duration v)       { this.timeout = v; }
        public Map<String, String> getEndpoints(){ return endpoints; }
        public void setEndpoints(Map<String, String> v) { this.endpoints = v; }
    }
}
