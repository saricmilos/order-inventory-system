from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Inventory Service (no kafka)")

# In-memory stock. Resets on restart.
stock = {"PROD-001": 100, "PROD-002": 50, "PROD-003": 0}


class ReserveRequest(BaseModel):
    product_id: str
    quantity: int
    customer_id: str


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/reserve")
def reserve(req: ReserveRequest):
    available = stock.get(req.product_id, 0)
    if available < req.quantity:
        raise HTTPException(status_code=409, detail="insufficient stock")

    stock[req.product_id] = available - req.quantity
    return {"product_id": req.product_id, "remaining_stock": stock[req.product_id]}
