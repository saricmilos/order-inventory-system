"""Unit tests for the in-memory inventory store.

This is the core business logic of the inventory-service: stock deduction,
the insufficient-stock and unknown-product rules, and crash-safe idempotency.
No Kafka, no network — pure logic that runs in milliseconds.
"""

import pytest

from app.inventory import InsufficientStockError, InventoryStore


def test_reserve_reduces_stock():
    store = InventoryStore()
    store.reserve("order-1", "PROD-001", 5)
    assert store.get_stock("PROD-001") == 95


def test_reserve_affects_only_target_product():
    store = InventoryStore()
    store.reserve("order-1", "PROD-001", 10)
    assert store.get_stock("PROD-001") == 90
    assert store.get_stock("PROD-002") == 50  # untouched


def test_insufficient_stock_raises():
    store = InventoryStore()
    with pytest.raises(InsufficientStockError):
        store.reserve("order-1", "PROD-003", 1)  # PROD-003 starts at 0


def test_insufficient_stock_leaves_stock_unchanged():
    store = InventoryStore()
    with pytest.raises(InsufficientStockError):
        store.reserve("order-1", "PROD-002", 51)  # only 50 available
    assert store.get_stock("PROD-002") == 50  # no partial deduction


def test_reserve_exact_available_amount_succeeds():
    store = InventoryStore()
    store.reserve("order-1", "PROD-002", 50)  # exactly what's in stock
    assert store.get_stock("PROD-002") == 0


def test_reserve_one_over_available_amount_fails():
    store = InventoryStore()
    store.reserve("order-1", "PROD-002", 50)
    with pytest.raises(InsufficientStockError):
        store.reserve("order-2", "PROD-002", 1)  # boundary: nothing left


def test_unknown_product_raises_value_error():
    store = InventoryStore()
    with pytest.raises(ValueError):
        store.reserve("order-1", "PROD-UNKNOWN", 1)


def test_idempotent_reservation_deducts_once():
    """Redelivery of the same order_id (e.g. after a consumer crash) must
    not double-deduct stock."""
    store = InventoryStore()
    store.reserve("order-1", "PROD-001", 5)
    store.reserve("order-1", "PROD-001", 5)  # same order_id → no-op
    assert store.get_stock("PROD-001") == 95  # NOT 90


def test_different_orders_each_deduct():
    store = InventoryStore()
    store.reserve("order-1", "PROD-001", 5)
    store.reserve("order-2", "PROD-001", 5)
    assert store.get_stock("PROD-001") == 90


def test_get_stock_unknown_product_returns_zero():
    store = InventoryStore()
    assert store.get_stock("PROD-UNKNOWN") == 0


def test_failed_reservation_is_not_marked_processed():
    """A business rejection must not consume the idempotency slot: if the
    same order_id later becomes serviceable, it should still go through."""
    store = InventoryStore()
    with pytest.raises(InsufficientStockError):
        store.reserve("order-1", "PROD-002", 60)  # too many, rejected
    # Same order_id, now a serviceable quantity — must succeed, not be skipped.
    store.reserve("order-1", "PROD-002", 10)
    assert store.get_stock("PROD-002") == 40
