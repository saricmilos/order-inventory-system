# Demo Guide — Order / Inventory System

A live walkthrough script for demoing this event-driven system. The key idea:

> **The API always returns `202 PENDING` instantly — no matter the outcome.**
> The *real* behavior (stock deduction, business rejection, retries, dead-letter routing)
> happens asynchronously in the consumer and is only visible in the **logs**.
> Show both sides at once and that contrast tells the whole story.

```
Postman / /docs  ──POST──▶  order-service  ──Kafka──▶  inventory-service
   (you see this:                                          (you see this:
    202 PENDING)                                            actual result)
```

---

## One-time setup (before the demo)

**0. Open Docker Desktop** — launch it, wait ~1 min until the whale icon is steady.

**1. Fresh start** (resets stock to a clean 100 / 50 / 0 — the in-memory store resets on restart):

```powershell
docker compose down
docker compose up --build -d
```

> Use `-d` (detached) so the terminal stays free for the log commands. Without `-d`,
> `docker compose up --build` takes over the terminal and you'd need a **second**
> PowerShell window for the `Select-String` commands.

**2. Verify everything is up:**

```powershell
docker compose ps
```

All three containers up, Kafka `(healthy)`.

**3. Open the API UI:** http://localhost:8000/docs

**Pre-seeded stock:** `PROD-001` = 100, `PROD-002` = 50, `PROD-003` = 0, anything else = unknown.

---

## Scenario 1 — ✅ Success

**Send** (`POST /orders` → Try it out → Execute):

```json
{ "product_id": "PROD-001", "quantity": 5, "customer_id": "CUST-123" }
```

**Response:** `202` → `{ "order_id": "...", "status": "PENDING" }`

**See what happened** (paste the order_id from the response):

```powershell
docker compose logs inventory-service --tail 200 | Select-String "<paste-order_id>"
```

**You'll see:** `message_received` → `order_processed`, `remaining_stock: 95`.

> *Say:* "202 instantly, then the consumer deducts stock asynchronously. 100 → 95."

---

## Scenario 2 — ⛔ Insufficient stock (zero-stock product)

**Send:**

```json
{ "product_id": "PROD-003", "quantity": 1, "customer_id": "CUST-456" }
```

**Response:** `202` → `PENDING` *(identical to success — that's the point)*

**See what happened:**

```powershell
docker compose logs inventory-service --tail 200 | Select-String "bd307be8-458d-481b-90eb-6c30dd3cb68b"
```

**You'll see:** `message_received` → `order_rejected_insufficient_stock` (a **warning**).
No retry, offset committed immediately.

> *Say:* "PROD-003 has 0 stock. This is a *business* rejection, not a system failure —
> so it's logged and committed, never retried. Retrying a stock shortage accomplishes nothing."

---

## Scenario 3 — ⛔ Insufficient stock (over-ordering a real product)

Stronger than Scenario 2 — proves the rejection is **computed from live stock**, not a hard-coded product.

**Send:**

```json
{ "product_id": "PROD-002", "quantity": 999, "customer_id": "CUST-456" }
```

**Response:** `202` → `PENDING`

**See what happened:**

```powershell
docker compose logs inventory-service --tail 200 | Select-String "<paste-order_id>"
```

**You'll see:** `order_rejected_insufficient_stock` with `available=50, requested=999` in the message.

> *Say:* "Same rejection path, but a real product — the check is `stock < quantity`, evaluated dynamically."

---

## Scenario 4 — ☠️ Unknown product → retry → Dead Letter Queue

**Send:**

```json
{ "product_id": "PROD-UNKNOWN", "quantity": 1, "customer_id": "CUST-789" }
```

**Response:** `202` → `PENDING`

**See what happened** (this one takes ~3 seconds to fully resolve):

```powershell
docker compose logs inventory-service --tail 200 | Select-String "<paste-order_id>"
```

**You'll see:** `message_received` → `processing_failed_retrying` (attempt 1) →
`processing_failed_retrying` (attempt 2) → `message_sent_to_dlq`.

**Then prove the message really landed in the DLQ:**

```powershell
docker compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders.dlq --from-beginning --max-messages 1
```

> *Say:* "Unknown product raises a `ValueError` — an *unexpected* error, so it takes the
> retry path: exponential backoff, then after exhausting retries it's routed to a dead-letter
> queue with failure metadata in the Kafka headers. No message is ever silently lost."

⚠️ **Heads-up — README/code mismatch:** the README says backoff is "1s, 2s, 4s", but with
`MAX_RETRY_ATTEMPTS=3` the loop only sleeps **1s then 2s** (3 attempts = 2 backoff waits),
then DLQs. The "4s" wait only happens with a 4th attempt. So you'll see **two**
`processing_failed_retrying` lines, not three. Either say "1s, 2s backoff" out loud, or bump
`MAX_RETRY_ATTEMPTS` to 4 in `.env` if you want the README to match.

---

## Scenario 5 — 🚫 Validation rejected at the API (422)

**Send** (quantity below 1):

```json
{ "product_id": "PROD-001", "quantity": 0, "customer_id": "CUST-123" }
```

**Response:** `422 Unprocessable Entity` — **no** `202`, **no** order_id, **nothing reaches Kafka**.

> Nothing to grep — that's the point. *Say:* "Pydantic enforces the contract at the edge
> (`quantity >= 1`). Bad input is rejected synchronously before it ever becomes an event."

---

## Bonus — trace one request across BOTH services

After any success, grab the `correlation_id` from the logs and run:

```powershell
docker compose logs | Select-String "<paste-correlation_id>"
```

You'll see `order_received` (order-service) + `message_received` + `order_processed`
(inventory-service) — **one ID, two services, full distributed trace from logs alone.**

---

## Deep analysis — what each behavior proves

| Behavior in the demo | Where it lives in code | The principle |
|---|---|---|
| API returns `202` before processing | `order-service/app/main.py:43` | **Async / event-driven** — caller doesn't block on downstream work |
| Stock rejection ≠ retry; unknown product = retry | `inventory-service/app/consumer.py:145-150` | **Business vs system error distinction** — the most important design decision here |
| Offset committed *after* resolution only | `inventory-service/app/consumer.py:164-165` | **Manual offset commit** (`enable.auto.commit=False`) — a crash mid-processing redelivers, no data loss |
| Backoff then DLQ with metadata headers | `inventory-service/app/consumer.py:141-162` + `_send_to_dlq` | **Retry + Dead Letter Queue** — failures isolated and inspectable, never dropped |
| `order_id` skip-if-seen | `inventory-service/app/inventory.py:16-17` | **Idempotency** — redelivered message won't double-deduct stock |
| Bad JSON → straight to DLQ, no retry | `inventory-service/app/consumer.py:114-124` | **Poison-message handling** — retrying malformed data is pointless |
| Same `correlation_id` everywhere | `inventory-service/app/consumer.py:112` + processor logs | **Distributed tracing** with nothing but structured logs |
| SIGTERM sets flag, loop drains, `consumer.close()` | `inventory-service/app/consumer.py:83-91, 167-170` | **Graceful shutdown** — commits offsets, leaves the group cleanly |

### Two things you can't easily demo live — keep as spoken talking points

- **Idempotency:** the API assigns a *new* UUID per request, so you can't resend the same
  `order_id` through `/docs`. It only triggers on Kafka *redelivery after a crash*. Explain it; don't try to show it.
- **Poison message → DLQ:** the order-service always produces *valid* events, so the
  schema-validation path never fires from the API. It's a defensive guard against other/buggy producers.

---

## Architecture one-liner (opening sentence)

> "Two independent microservices — a FastAPI order API and a Python Kafka consumer —
> that communicate *only* through Kafka. Neither calls the other directly. The API accepts
> orders and returns immediately; the consumer does the real work with production-grade
> reliability: manual offset commits, retry-with-backoff into a dead-letter queue,
> idempotency, and end-to-end log tracing."

---

## Backup plan

If Docker misbehaves live: open `README.md` — it documents the full flow with example log
output, so you can walk the architecture diagram and explain each feature without a running stack.
