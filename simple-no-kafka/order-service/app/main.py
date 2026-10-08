import os

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

INVENTORY_URL = os.environ.get("INVENTORY_URL", "http://localhost:8001")

app = FastAPI(title="Order Service (no kafka)")


class OrderRequest(BaseModel):
    product_id: str
    quantity: int
    customer_id: str


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/orders")
def create_order(order: OrderRequest):
    # Call the inventory service directly over HTTP and wait for the answer.
    # Synchronous: the order is confirmed (or rejected) before we respond.
    resp = httpx.post(f"{INVENTORY_URL}/reserve", json=order.model_dump())
    if resp.status_code == 409:
        raise HTTPException(status_code=409, detail="insufficient stock")
    resp.raise_for_status()

    return {"status": "CONFIRMED", **order.model_dump(), **resp.json()}
