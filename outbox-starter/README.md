# Transactional Outbox Pattern — Spring Boot Starter

A production-ready, reusable Spring Boot starter implementing the **Transactional Outbox Pattern** for reliable, exactly-once* message delivery across integration boundaries.

> \* Technically **at-least-once** delivery with **idempotency support** to achieve effectively-once semantics.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [How It Works](#how-it-works)
3. [Project Structure](#project-structure)
4. [Quick Start](#quick-start)
5. [Core Concepts](#core-concepts)
6. [Delivery Handlers](#delivery-handlers)
7. [Configuration Reference](#configuration-reference)
8. [Observability](#observability)
9. [Admin Operations](#admin-operations)
10. [Testing](#testing)
11. [Design Decisions & Trade-offs](#design-decisions--trade-offs)
12. [When to Use This vs Alternatives](#when-to-use-this-vs-alternatives)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     Your Application                            │
│                                                                 │
│  ┌──────────────┐    ┌──────────────────┐                       │
│  │   Business   │───▶│ OutboxPublisher  │                       │
│  │   Service    │    │  .publish(msg)   │                       │
│  └──────────────┘    └────────┬─────────┘                       │
│         │                     │                                 │
│         │        SAME TRANSACTION                               │
│         ▼                     ▼                                 │
│  ┌──────────────┐    ┌──────────────────┐                       │
│  │   Business   │    │  outbox_event    │                       │
│  │   Tables     │    │  table           │                       │
│  └──────────────┘    └──────────────────┘                       │
│                              │                                  │
│                    ┌─────────┴──────────┐                       │
│                    │  OutboxDispatcher  │ (polls + delivers)    │
│                    │  ┌───────────────┐ │                       │
│                    │  │ SELECT ... FOR│ │                       │
│                    │  │ UPDATE SKIP   │ │                       │
│                    │  │ LOCKED        │ │                       │
│                    │  └───────────────┘ │                       │
│                    └─────────┬──────────┘                       │
│                              │                                  │
│                    ┌─────────┴──────────┐                       │
│                    │ DeliveryHandler    │                       │
│                    │ Registry           │                       │
│                    └──┬─────┬─────┬─────┘                       │
│                       │     │     │                             │
└───────────────────────┼─────┼─────┼─────────────────────────────┘
                        │     │     │
               ┌────────┘     │     └────────┐
               ▼              ▼              ▼
        ┌──────────┐  ┌──────────┐  ┌──────────┐
        │  Kafka   │  │ REST API │  │ External │
        │  Broker  │  │ Service  │  │ Database │
        └──────────┘  └──────────┘  └──────────┘
```

### Key Properties

| Property | Guarantee |
|----------|-----------|
| **Atomicity** | Business data + outbox event written in the SAME database transaction |
| **Delivery** | At-least-once — events are retried until acknowledged or dead-lettered |
| **Ordering** | Per-aggregate ordering via `aggregate_id` (partition key for Kafka) |
| **Concurrency** | `SELECT ... FOR UPDATE SKIP LOCKED` prevents duplicate processing across instances |
| **Idempotency** | Built-in idempotency keys for consumer-side deduplication |

---

## How It Works

### Sequence Diagram: Happy Path

```
 Client          OrderService        Database         Dispatcher        Kafka
   │                  │                  │                │               │
   │ POST /orders     │                  │                │               │
   │─────────────────▶│                  │                │               │
   │                  │ BEGIN TX         │                │               │
   │                  │─────────────────▶│                │               │
   │                  │ INSERT order     │                │               │
   │                  │─────────────────▶│                │               │
   │                  │ INSERT outbox    │                │               │
   │                  │─────────────────▶│                │               │
   │                  │ COMMIT           │                │               │
   │                  │─────────────────▶│                │               │
   │◀─────────────────│                  │                │               │
   │ 201 Created      │                  │                │               │
   │                  │                  │                │               │
   │                  │                  │ SELECT + LOCK  │               │
   │                  │                  │◀───────────────│               │
   │                  │                  │ rows           │               │
   │                  │                  │───────────────▶│               │
   │                  │                  │                │ produce()     │
   │                  │                  │                │──────────────▶│
   │                  │                  │                │ ack           │
   │                  │                  │                │◀──────────────│
   │                  │                  │ UPDATE status  │               │
   │                  │                  │◀───────────────│               │
   │                  │                  │ = PROCESSED    │               │
```

### Sequence Diagram: Failure & Retry

```
 Dispatcher       Database          Kafka
   │                │                 │
   │ SELECT + LOCK  │                 │
   │───────────────▶│                 │
   │ event (PENDING)│                 │
   │◀───────────────│                 │
   │                │                 │
   │ produce()      │                 │
   │────────────────────────────────▶│
   │                │     TIMEOUT ✗  │
   │◀────────────────────────────────│
   │                │                 │
   │ UPDATE status  │                 │
   │───────────────▶│                 │
   │ = RETRYING     │                 │
   │ next_attempt   │                 │
   │ = now + backoff│                 │
   │                │                 │
   │ ... (after backoff) ...          │
   │                │                 │
   │ SELECT + LOCK  │                 │
   │───────────────▶│                 │
   │ event (RETRYING)                 │
   │◀───────────────│                 │
   │ produce()      │                 │
   │────────────────────────────────▶│
   │                │          ACK ✓ │
   │◀────────────────────────────────│
   │ UPDATE status  │                 │
   │───────────────▶│                 │
   │ = PROCESSED    │                 │
```

---

## Project Structure

```
outbox-starter/
├── pom.xml                              # Parent POM
├── outbox-spring-boot-starter/          # ◀ THE REUSABLE STARTER
│   ├── pom.xml
│   └── src/main/java/com/starter/outbox/
│       ├── autoconfigure/
│       │   └── OutboxAutoConfiguration.java      # Spring Boot wiring
│       ├── core/
│       │   ├── entity/
│       │   │   ├── OutboxEvent.java              # JPA entity
│       │   │   └── OutboxStatus.java             # PENDING → PROCESSED
│       │   ├── repository/
│       │   │   └── OutboxEventRepository.java    # Locking queries
│       │   ├── publisher/
│       │   │   ├── OutboxPublisher.java          # Public API interface
│       │   │   └── DefaultOutboxPublisher.java   # Transactional impl
│       │   ├── dispatcher/
│       │   │   ├── OutboxDispatcher.java         # Batch delivery
│       │   │   └── OutboxScheduler.java          # Polling trigger
│       │   ├── config/
│       │   │   ├── OutboxProperties.java         # @ConfigurationProperties
│       │   │   └── OutboxHealthIndicator.java    # Actuator health
│       │   ├── metrics/
│       │   │   └── OutboxMetrics.java            # Micrometer gauges/counters
│       │   ├── admin/
│       │   │   └── OutboxAdminController.java    # REST admin endpoints
│       │   └── archive/
│       │       └── OutboxArchiveJob.java         # Cleanup scheduler
│       └── delivery/                             # ◀ STRATEGY PATTERN
│           ├── OutboxDeliveryHandler.java         # SPI interface
│           ├── OutboxDeliveryException.java       # Retryable/non-retryable
│           ├── DeliveryHandlerRegistry.java       # Prefix → handler map
│           ├── kafka/
│           │   ├── KafkaDeliveryHandler.java
│           │   └── KafkaDeliveryAutoConfiguration.java
│           ├── rest/
│           │   ├── RestApiDeliveryHandler.java
│           │   └── RestDeliveryAutoConfiguration.java
│           ├── database/
│           │   └── DatabaseDeliveryHandler.java
│           └── internal/
│               └── InternalServiceDeliveryHandler.java
│
└── outbox-examples/
    └── order-service/                   # ◀ EXAMPLE APPLICATION
        └── src/main/java/com/example/orders/
            ├── service/OrderService.java         # 5 usage examples
            ├── controller/OrderController.java
            └── config/DeliveryConfiguration.java
```

---

## Quick Start

### 1. Add the dependency

```xml
<dependency>
    <groupId>com.starter</groupId>
    <artifactId>outbox-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

### 2. Configure (application.yml)

```yaml
outbox:
  enabled: true
  batch-size: 50
  poll-interval-ms: 1000
  delivery:
    kafka:
      enabled: true
```

### 3. Publish events in your business transaction

```java
@Service
public class OrderService {

    private final OrderRepository orderRepo;
    private final OutboxPublisher outbox;
    private final ObjectMapper json;

    @Transactional
    public Order createOrder(CreateOrderRequest req) {
        Order order = orderRepo.save(new Order(req));

        outbox.publish(OutboxMessage.builder()
            .eventType("OrderCreated")
            .aggregateType("Order")
            .aggregateId(order.getId())
            .destination("kafka:orders-topic")
            .payload(json.writeValueAsString(order))
            .build());

        return order;
    }
}
```

That's it. The starter handles polling, delivery, retries, and dead-lettering automatically.

---

## Core Concepts

### OutboxEvent Lifecycle

```
PENDING ──(delivered)──► PROCESSED
   │
   │ (delivery fails)
   ▼
RETRYING ──(delivered)──► PROCESSED
   │
   │ (max retries exhausted)
   ▼
DEAD_LETTER ──(admin reset)──► PENDING
```

### Transaction Boundary

The publisher uses `@Transactional(propagation = MANDATORY)` — it MUST be called within an existing transaction. This guarantees that the outbox record is committed atomically with the business data.

### Locking Strategy

The dispatcher uses PostgreSQL's `SELECT ... FOR UPDATE SKIP LOCKED` to enable safe concurrent processing across multiple application instances without leader election or distributed locks.

---

## Delivery Handlers

### Implementing a Custom Handler

```java
@Component
public class SmsDeliveryHandler implements OutboxDeliveryHandler {

    @Override
    public String destinationPrefix() {
        return "sms:";
    }

    @Override
    public void deliver(OutboxEvent event) throws OutboxDeliveryException {
        String phoneNumber = extractTarget(event); // "sms:+1234567890" → "+1234567890"
        try {
            smsClient.send(phoneNumber, event.getPayload());
        } catch (Exception e) {
            throw new OutboxDeliveryException("SMS failed", e, true); // retryable
        }
    }
}
```

Register it as a Spring `@Component` and it's automatically picked up.

### Built-in Handlers

| Prefix | Handler | Destination Example |
|--------|---------|---------------------|
| `kafka:` | KafkaDeliveryHandler | `kafka:orders-topic` |
| `rest:` | RestApiDeliveryHandler | `rest:payment-service` |
| `db:` | DatabaseDeliveryHandler | `db:audit-store` |
| `internal:` | InternalServiceDeliveryHandler | `internal:notifications` |

---

## Configuration Reference

```yaml
outbox:
  # Master switch
  enabled: true                    # default: true

  # Dispatcher
  batch-size: 50                   # events per poll cycle
  poll-interval-ms: 1000           # ms between polls

  # Retry strategy (exponential backoff)
  default-max-retries: 5
  retry-base-delay: 5s             # 5s, 10s, 20s, 40s, 80s...
  retry-max-backoff: 1h            # capped at 1 hour
  stale-retry-threshold: 24h       # force dead-letter after 24h

  # Archive cleanup
  archive:
    enabled: true
    retention: 7d                  # delete PROCESSED events older than 7 days
    cron: "0 0 3 * * *"           # run at 3 AM

  # Admin API
  admin:
    base-path: /admin/outbox

  # ─── Delivery targets ───────────────────────────
  delivery:
    kafka:
      enabled: true

    rest:
      enabled: true
      timeout: 30s
      endpoints:
        payment-service: https://payments.example.com/api/webhooks
        notification-service: https://notify.example.com/events

    database:
      enabled: true
```

---

## Observability

### Micrometer Metrics

| Metric | Type | Tags | Description |
|--------|------|------|-------------|
| `outbox.events.published` | Counter | destination, event_type | Events written to outbox |
| `outbox.events.delivered` | Counter | destination, event_type | Successfully delivered |
| `outbox.events.failed` | Counter | destination, event_type, dead_lettered | Delivery failures |
| `outbox.events.dead_lettered` | Counter | destination | Moved to dead-letter |
| `outbox.events.archived` | Counter | — | Cleaned up old events |
| `outbox.batch.size` | Summary | — | Events per dispatch batch |
| `outbox.batch.success_rate` | Summary | — | Success ratio per batch |
| `outbox.dispatcher.enabled` | Gauge | — | 1=active, 0=paused |

### Health Check

```
GET /actuator/health
```

```json
{
  "status": "UP",
  "components": {
    "outbox": {
      "status": "UP",
      "details": {
        "dispatcherEnabled": true,
        "pending": 12,
        "retrying": 3,
        "deadLettered": 0,
        "registeredHandlers": ["kafka:", "rest:", "db:"]
      }
    }
  }
}
```

### Structured Logging

All outbox operations include structured context:
- `eventId`, `eventType`, `aggregateType`, `aggregateId`, `destination`
- Retry count and next attempt time on failures
- Error messages on dead-lettering

---

## Admin Operations

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/admin/outbox/stats` | GET | Counts by status |
| `/admin/outbox/events?status=DEAD_LETTER&limit=20` | GET | List events |
| `/admin/outbox/events/{id}` | GET | Single event detail |
| `/admin/outbox/events/{id}/retry` | POST | Reset single event for retry |
| `/admin/outbox/events/retry-all-dead-letter` | POST | Bulk retry dead-lettered events |
| `/admin/outbox/dispatcher/pause` | POST | Pause the dispatcher |
| `/admin/outbox/dispatcher/resume` | POST | Resume the dispatcher |

---

## Testing

### Unit Tests

```bash
# Run unit tests only
mvn test -pl outbox-spring-boot-starter -Dtest="*Test" -DfailIfNoTests=false
```

Covers:
- `OutboxEventTest` — entity lifecycle, state transitions
- `OutboxDispatcherTest` — batch processing, retry logic, pause/resume
- `DeliveryHandlerRegistryTest` — routing, unknown destinations

### Integration Tests (Testcontainers)

```bash
# Run integration tests (requires Docker)
mvn test -pl outbox-spring-boot-starter -Dtest="*IntegrationTest"
```

Covers:
- Full PostgreSQL lifecycle with Testcontainers
- Publish → dispatch → deliver → mark processed
- Retry and dead-letter scenarios
- Idempotency key constraints

### Failure Simulation Patterns

The integration test includes a configurable test delivery handler that can:
- Succeed immediately
- Fail N times then succeed (retry testing)
- Always fail (dead-letter testing)

---

## Design Decisions & Trade-offs

### 1. Polling vs CDC (Change Data Capture)

**Chosen: Polling with `SELECT ... FOR UPDATE SKIP LOCKED`**

| Aspect | Polling (this starter) | CDC (Debezium) |
|--------|------------------------|----------------|
| Latency | 1–5s (configurable) | Sub-second |
| Complexity | Low (no extra infra) | High (Kafka Connect, Debezium) |
| Ops burden | Minimal | Significant |
| Schema coupling | None | Tight (reads WAL) |
| Getting started | Add dependency | Deploy connector cluster |

**When to switch to CDC:** If you need sub-second delivery latency and already run Kafka Connect infrastructure.

### 2. Single table vs per-aggregate outbox tables

**Chosen: Single generic table**

Pro: One schema for all event types; simpler to manage.
Con: High-throughput systems might benefit from partitioned tables.
Mitigation: Partial indexes on `(status, next_attempt_at)` keep queries fast.

### 3. JSON payload vs typed columns

**Chosen: Opaque TEXT payload (JSON)**

Keeps the outbox schema stable regardless of event types. Consumers deserialize.

### 4. Synchronous dispatch within transaction vs async

**Chosen: Async (polling)**

The publisher only writes to the DB. Delivery is fully async. This means:
- Business transactions are fast (no network calls)
- Delivery failures don't break business operations
- Natural batching and backpressure

### 5. Retry strategy

**Chosen: Exponential backoff with cap + dead-letter**

`delay = min(baseDelay × 2^retries, maxBackoff)`

Default: 5s → 10s → 20s → 40s → 80s → ... → capped at 1h.
After `maxRetries` (default 5) → moved to DEAD_LETTER.

### 6. Idempotency

**Producer side:** Unique idempotency key per event (defaults to event ID, can be overridden for fan-out).
**Consumer side:** Consumers should use `X-Outbox-Idempotency-Key` header / `event_id` column for deduplication.

---

## When to Use This vs Alternatives

| Pattern | Use When | Trade-off |
|---------|----------|-----------|
| **Outbox (this starter)** | You need reliable async messaging from a relational DB without adding infrastructure | Polling latency (1-5s); requires DB space |
| **2PC (Two-Phase Commit)** | You need synchronous distributed consistency | Performance penalty; limited broker support; not recommended for microservices |
| **Saga** | Long-running business processes with compensation logic | Complex to implement; eventual consistency only |
| **CDC (Debezium)** | You need sub-second latency and already have Kafka Connect | High ops complexity; WAL-level coupling |
| **Direct publish + retry** | Simple fire-and-forget with acceptable data loss | No atomicity guarantee; messages can be lost |

### Decision Flowchart

```
Do you need guaranteed delivery from a DB transaction?
├── No → Direct publish with retry
└── Yes
    ├── Need sub-second latency?
    │   ├── Yes → CDC (Debezium)
    │   └── No → Outbox Pattern (this starter) ✓
    └── Need synchronous consistency?
        ├── Yes → 2PC (but reconsider your architecture)
        └── No → Outbox Pattern (this starter) ✓
```

---

## Database Schema

The outbox table is created by Flyway migration `V1__create_outbox_event_table.sql`:

```sql
CREATE TABLE outbox_event (
    id              VARCHAR(36) PRIMARY KEY,
    event_type      VARCHAR(255) NOT NULL,      -- e.g. "OrderCreated"
    aggregate_type  VARCHAR(255) NOT NULL,      -- e.g. "Order"
    aggregate_id    VARCHAR(255) NOT NULL,      -- e.g. "order-123"
    payload         TEXT NOT NULL,              -- JSON payload
    headers         TEXT,                       -- JSON headers map
    destination     VARCHAR(500) NOT NULL,      -- e.g. "kafka:orders-topic"
    status          VARCHAR(20) NOT NULL,       -- PENDING/RETRYING/PROCESSED/DEAD_LETTER
    retries         INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 5,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    processed_at    TIMESTAMPTZ,
    last_error      TEXT,
    idempotency_key VARCHAR(255) NOT NULL UNIQUE,
    version         BIGINT NOT NULL DEFAULT 0   -- optimistic locking
);
```

Key indexes:
- **Partial index** on `(status, next_attempt_at) WHERE status IN ('PENDING', 'RETRYING')` — fast dispatcher queries
- **Unique index** on `idempotency_key` — prevents duplicate event publishing

---

## License

MIT
