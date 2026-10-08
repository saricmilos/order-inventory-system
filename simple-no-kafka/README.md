# simple-no-kafka

The **simplest possible** version with **no message broker at all**. The order service
calls the inventory service directly over HTTP and waits for the answer.

```
POST /orders → Order Service → (HTTP POST /reserve) → Inventory Service → deduct stock
                            ←───────── 200 / 409 ──────────┘
```

This is synchronous: the order is confirmed or rejected in the same request. No Kafka,
no topics, no consumer loop, no eventual consistency.

## What this trades off vs. the Kafka version

- **Simpler:** no broker to run, immediate result, easy to reason about.
- **Tighter coupling:** if the inventory service is down, orders fail right away.
- **No buffering / replay:** there's no event log; a failed call is just a failed call.

## Run

```bash
cd simple-no-kafka
docker compose up --build
```

Send an order (open http://localhost:8000/docs, or):

```bash
# success
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD-001", "quantity": 5, "customer_id": "CUST-1"}'
# {"status":"CONFIRMED","product_id":"PROD-001","quantity":5,...,"remaining_stock":95}

# rejected (PROD-003 has 0 stock) → HTTP 409
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD-003", "quantity": 1, "customer_id": "CUST-2"}'
```

## Stock (in-memory, resets on restart)

| Product | Stock |
|---|---|
| PROD-001 | 100 |
| PROD-002 | 50 |
| PROD-003 | 0 (always rejected) |
