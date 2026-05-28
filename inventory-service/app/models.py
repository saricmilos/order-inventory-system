from datetime import datetime

from pydantic import BaseModel, Field


class OrderEvent(BaseModel):
    order_id: str
    product_id: str
    quantity: int = Field(ge=1)
    customer_id: str
    status: str
    created_at: datetime
