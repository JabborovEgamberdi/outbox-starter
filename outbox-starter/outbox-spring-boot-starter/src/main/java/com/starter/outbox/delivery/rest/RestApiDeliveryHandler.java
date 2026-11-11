package com.starter.outbox.delivery.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starter.outbox.core.entity.OutboxEvent;
import com.starter.outbox.delivery.OutboxDeliveryException;
import com.starter.outbox.delivery.OutboxDeliveryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;

/**
 * Delivers outbox events to external REST APIs via HTTP POST.
 *
 * <p>Destination format: {@code rest:<endpoint-name>}</p>
 * <p>The endpoint name is resolved to a URL from configuration:</p>
 * <pre>
 * outbox:
 *   delivery:
 *     rest:
 *       enabled: true
 *       timeout: 30s
 *       endpoints:
 *         payment-service: https://payments.example.com/api/webhooks
 *         notification-service: https://notify.example.com/events
 * </pre>
 *
 * <h3>Example</h3>
 * <pre>{@code
 * outboxPublisher.publish(OutboxMessage.builder()
 *     .eventType("OrderPaid")
 *     .aggregateType("Order")
 *     .aggregateId("order-123")
 *     .destination("rest:payment-service")
 *     .payload(json)
 *     .headers(Map.of("Authorization", "Bearer token"))
 *     .build());
 * }</pre>
 */
public class RestApiDeliveryHandler implements OutboxDeliveryHandler {

    private static final Logger log = LoggerFactory.getLogger(RestApiDeliveryHandler.class);
    private static final String PREFIX = "rest:";

    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final Map<String, String> endpointMap;
    private final Duration timeout;

    public RestApiDeliveryHandler(WebClient.Builder webClientBuilder,
                                  ObjectMapper objectMapper,
                                  Map<String, String> endpointMap,
                                  Duration timeout) {
        this.webClient = webClientBuilder.build();
        this.objectMapper = objectMapper;
        this.endpointMap = endpointMap;
        this.timeout = timeout;
    }

    @Override
    public String destinationPrefix() {
        return PREFIX;
    }

    @Override
    public void deliver(OutboxEvent event) throws OutboxDeliveryException {
        String endpointName = extractTarget(event);
        String url = endpointMap.get(endpointName);

        if (url == null) {
            throw new OutboxDeliveryException(
                    "No REST endpoint configured for: " + endpointName, false);
        }

        try {
            WebClient.RequestBodySpec request = webClient.post()
                    .uri(url)
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("X-Outbox-Event-Id", event.getId())
                    .header("X-Outbox-Event-Type", event.getEventType())
                    .header("X-Outbox-Idempotency-Key", event.getIdempotencyKey());

            // Add custom headers from the event
            addCustomHeaders(request, event);

            HttpStatusCode status = request
                    .bodyValue(event.getPayload())
                    .exchangeToMono(response -> Mono.just(response.statusCode()))
                    .block(timeout);

            if (status == null) {
                throw new OutboxDeliveryException(
                        "REST delivery to '%s' timed out after %s".formatted(url, timeout),
                        true);
            }

            if (status.is2xxSuccessful()) {
                log.debug("REST delivery success: endpoint={}, url={}, status={}, eventId={}",
                        endpointName, url, status.value(), event.getId());
            } else if (status.is4xxClientError()) {
                // 4xx = non-retryable (bad payload, auth error, etc.)
                throw new OutboxDeliveryException(
                        "REST delivery to '%s' returned %d (non-retryable)".formatted(url, status.value()),
                        false);
            } else {
                // 5xx or other = retryable
                throw new OutboxDeliveryException(
                        "REST delivery to '%s' returned %d".formatted(url, status.value()),
                        true);
            }

        } catch (OutboxDeliveryException e) {
            throw e;
        } catch (Exception e) {
            throw new OutboxDeliveryException(
                    "REST delivery to '%s' failed: %s".formatted(url, e.getMessage()),
                    e, true);
        }
    }

    private void addCustomHeaders(WebClient.RequestBodySpec request, OutboxEvent event) {
        if (event.getHeaders() == null) return;
        try {
            Map<String, String> headers = objectMapper.readValue(
                    event.getHeaders(), new TypeReference<>() {});
            headers.forEach(request::header);
        } catch (Exception e) {
            log.warn("Failed to parse custom headers for event {}: {}", event.getId(), e.getMessage());
        }
    }
}
