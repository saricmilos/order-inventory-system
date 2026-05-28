from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, Field


class OrderRequest(BaseModel):
    product_id: str
    quantity: int = Field(ge=1)
    customer_id: str


class OrderEvent(BaseModel):
    order_id: str = Field(default_factory=lambda: str(uuid4()))
    product_id: str
    quantity: int
    customer_id: str
    status: Literal["PENDING"] = "PENDING"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @classmethod
    def from_request(cls, request: OrderRequest) -> OrderEvent:
        return cls(
            product_id=request.product_id,
            quantity=request.quantity,
            customer_id=request.customer_id,
        )
