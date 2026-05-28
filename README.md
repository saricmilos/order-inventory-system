# order-inventory-system

Event-driven order and inventory microservices using FastAPI, Kafka, and Docker Compose.

## Architecture

```
POST /orders
     │
     ▼
Order Service (FastAPI)
     │  publishes OrderEvent
     ▼
Kafka topic: orders  (3 partitions)
     │  consumed by
     ▼
Inventory Service (Python consumer)
     │
     ├─ success → log + commit offset
     ├─ insufficient stock → log warning + commit offset (no retry)
     └─ system error → retry with backoff → orders.dlq + commit offset
```

Two independent services communicate exclusively through Kafka. Neither service calls the other directly.

## Services

| Service | Technology | Role |
|---|---|---|
| `order-service` | FastAPI + uvicorn | Exposes REST API, publishes order events |
| `inventory-service` | Python consumer | Consumes events, manages in-memory stock |
| `kafka` | bitnami/kafka 3.7 (KRaft) | Message broker, no Zookeeper |
| `kafka-init` | one-shot container | Creates topics before services start |

## Running Locally

**Prerequisites:** Docker and Docker Compose (or Docker Desktop).

```bash
docker compose up --build
```

All services start in the correct order via `depends_on` health checks. The system is ready when you see `consumer_started` in the inventory-service logs.

To run detached:

```bash
docker compose up --build -d
docker compose logs -f
```

## API

### POST /orders

Place an order. Returns HTTP 202 immediately after publishing to Kafka.

```bash
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD-001", "quantity": 5, "customer_id": "CUST-123"}'
```

Response:
```json
{"order_id": "550e8400-e29b-41d4-a716-446655440000", "status": "PENDING"}
```

### GET /health

```bash
curl http://localhost:8000/health
```

Response:
```json
{"status": "ok", "service": "order-service"}
```

**Validation:** Pydantic rejects invalid payloads with HTTP 422 (e.g. `quantity < 1`, missing fields).

## Pre-seeded Inventory

| Product | Initial Stock | Behavior |
|---|---|---|
| `PROD-001` | 100 | Accepts orders up to available quantity |
| `PROD-002` | 50 | Accepts orders up to available quantity |
| `PROD-003` | 0 | Always rejects (insufficient stock) |
| anything else | — | Triggers retry + DLQ (unknown product) |

## Production-Grade Integration Features

### Manual Offset Commits
`enable.auto.commit=False`. The consumer commits only after the message is either successfully processed or sent to the DLQ. A crash mid-processing will redeliver the message — no silent data loss.

### Retry with Exponential Backoff → Dead Letter Queue
System errors (unexpected exceptions) trigger up to 3 retry attempts with delays of 1s, 2s, 4s. After exhaustion the message is published to `orders.dlq` with failure metadata in Kafka headers (`x-failure-reason`, `x-retry-count`, `x-original-topic`, `x-original-partition`, `x-original-offset`). The offset is then committed.

### Business vs System Error Distinction
`InsufficientStockError` is a valid business outcome — the offset is committed immediately without retry. Retrying a stock shortage accomplishes nothing. Only unexpected/system exceptions trigger the retry → DLQ path.

### Idempotency
`order_id` (UUID4, server-assigned) is the Kafka message key — all events for the same order route to the same partition. The `InventoryStore` tracks processed order IDs in memory: if the same message is redelivered after a crash, stock is not double-deducted.

### Correlation IDs
`POST /orders` reads `X-Request-ID` from the HTTP header (or generates a UUID). This ID is attached as a Kafka message header (`x-correlation-id`) and included in every log line in both services, enabling end-to-end request tracing using only logs.

### Producer Durability
`acks=all` + `enable.idempotence=True` on the producer. Kafka deduplicates producer retries at the broker level. `flush()` is called on shutdown to drain in-flight messages before the process exits.

### Explicit Topic Creation
`AUTO_CREATE_TOPICS_ENABLE=false`. A `kafka-init` container creates `orders` (3 partitions) and `orders.dlq` (1 partition) with correct configuration before any service starts. Auto-creation is an anti-pattern in production (wrong defaults silently applied).

### Graceful Shutdown
`SIGTERM`/`SIGINT` handlers set a shutdown flag. The poll loop checks it each iteration. On shutdown: `consumer.close()` (commits current offsets, leaves the consumer group cleanly) then `dlq_producer.flush()`.

## Verifying the Integration

```bash
# Check topics were created
docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

# Place a successful order
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD-001", "quantity": 5, "customer_id": "CUST-123"}'

# Watch the consumer process it (look for "order_processed")
docker compose logs inventory-service --follow

# Trigger insufficient stock (PROD-003 has 0 stock)
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD-003", "quantity": 1, "customer_id": "CUST-456"}'

# Trigger DLQ path (unknown product → retries → DLQ)
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD-UNKNOWN", "quantity": 1, "customer_id": "CUST-789"}'

# Inspect DLQ
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders.dlq \
  --from-beginning \
  --max-messages 1

# Test Pydantic validation (expect HTTP 422)
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD-001", "quantity": -1, "customer_id": "CUST-123"}'
```

## Assumptions and Decisions

- **No database.** In-memory storage is intentional per the task requirements. The idempotency set and stock dict are process-scoped — a restart resets them.
- **Confluent-kafka over kafka-python.** The C-backed client provides first-class manual commit APIs and per-message delivery callbacks needed for the reliability guarantees implemented here.
- **KRaft (no Zookeeper).** Single-container Kafka broker, simpler Compose file, and KRaft has been production-stable since Kafka 3.3.
- **Separate Pydantic models per service.** The `OrderEvent` schema is duplicated rather than shared via a common package to keep the services independently deployable without a shared library dependency.
- **Structured JSON logging.** Both services log newline-delimited JSON to stdout. In a real deployment these would be collected by a log aggregator (e.g. Loki, Datadog).
