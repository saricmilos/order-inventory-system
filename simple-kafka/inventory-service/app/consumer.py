import json
import os

from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ORDERS_TOPIC = "orders"

# In-memory stock. Resets on restart.
stock = {"PROD-001": 100, "PROD-002": 50, "PROD-003": 0}


def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "inventory-service",
        "auto.offset.reset": "earliest",
        # auto-commit is on by default — no manual offset handling
    })
    consumer.subscribe([ORDERS_TOPIC])
    print("inventory consumer started", flush=True)

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("consumer error:", msg.error(), flush=True)
            continue

        order = json.loads(msg.value())
        product_id = order["product_id"]
        quantity = order["quantity"]

        available = stock.get(product_id, 0)
        if available >= quantity:
            stock[product_id] = available - quantity
            print(f"processed {product_id}: -{quantity}, remaining {stock[product_id]}", flush=True)
        else:
            print(f"rejected {product_id}: insufficient stock (have {available}, want {quantity})", flush=True)


if __name__ == "__main__":
    main()
