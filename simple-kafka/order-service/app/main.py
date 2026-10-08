import json
import os

from confluent_kafka import Producer
from fastapi import FastAPI
from pydantic import BaseModel

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ORDERS_TOPIC = "orders"

app = FastAPI(title="Order Service (simple kafka)")
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


class OrderRequest(BaseModel):
    product_id: str
    quantity: int
    customer_id: str


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/orders")
def create_order(order: OrderRequest):
    # Publish the order to Kafka and return immediately. Fire-and-forget:
    # no acks=all, no idempotence, no delivery callback, no correlation id.
    producer.produce(ORDERS_TOPIC, value=json.dumps(order.model_dump()).encode())
    producer.flush()
    return {"status": "PENDING", **order.model_dump()}
