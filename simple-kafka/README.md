# simple-kafka

The **simplest possible** Kafka version of the order/inventory system. Two services
talking through one Kafka topic. No production-grade machinery.

```
POST /orders → Order Service → Kafka topic "orders" → Inventory Service → deduct stock
```

## What this deliberately leaves out

This is the bare bones to show the event-driven pattern. It intentionally does **not** have:

- Dead-letter queue / retries / backoff
- Manual offset commits (uses Kafka's default auto-commit)
- Idempotency / de-duplication
- Producer `acks=all` or `enable.idempotence`
- Delivery callbacks
- Correlation IDs / structured logging
- Explicit topic creation (topic auto-creates on first use)
- Graceful shutdown

If you want all of that, see the production-grade version in the repo root.

## Run

```bash
cd simple-kafka
docker compose up --build
```

Send an order (open http://localhost:8000/docs, or):

```bash
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD-001", "quantity": 5, "customer_id": "CUST-1"}'
```

Watch the consumer handle it:

```bash
docker compose logs inventory-service -f
# processed PROD-001: -5, remaining 95
```

## Stock (in-memory, resets on restart)

| Product | Stock |
|---|---|
| PROD-001 | 100 |
| PROD-002 | 50 |
| PROD-003 | 0 (always rejected) |
