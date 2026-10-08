"""Tests for the thin business layer that sits between the consumer loop and
the inventory store.

The processor's job is to call ``reserve`` and surface the right exception type
so the consumer can decide: commit-without-retry (business reject) vs
retry-then-DLQ (system error).
"""

from datetime import datetime, timezone

import pytest

from app.inventory import InsufficientStockError, InventoryStore
from app.models import OrderEvent
from app.processor import OrderProcessor


def _event(product_id: str, quantity: int, order_id: str = "order-1") -> OrderEvent:
    return OrderEvent(
        order_id=order_id,
        product_id=product_id,
        quantity=quantity,
        customer_id="CUST-1",
        status="PENDING",
        created_at=datetime.now(timezone.utc),
    )


def test_successful_order_deducts_stock():
    store = InventoryStore()
    processor = OrderProcessor(store)
    processor.process(_event("PROD-001", 5))
    assert store.get_stock("PROD-001") == 95


def test_insufficient_stock_is_reraised():
    """A business rejection must propagate so the consumer commits without
    retrying."""
    store = InventoryStore()
    processor = OrderProcessor(store)
    with pytest.raises(InsufficientStockError):
        processor.process(_event("PROD-003", 1))  # 0 in stock


def test_unknown_product_propagates_value_error():
    """A system-style error must propagate as-is so the consumer takes the
    retry -> DLQ path."""
    store = InventoryStore()
    processor = OrderProcessor(store)
    with pytest.raises(ValueError):
        processor.process(_event("PROD-UNKNOWN", 1))


def test_redelivered_order_is_idempotent_through_processor():
    store = InventoryStore()
    processor = OrderProcessor(store)
    processor.process(_event("PROD-001", 5, order_id="dup"))
    processor.process(_event("PROD-001", 5, order_id="dup"))
    assert store.get_stock("PROD-001") == 95  # deducted once
