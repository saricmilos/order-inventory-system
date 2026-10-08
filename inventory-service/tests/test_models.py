"""Schema-validation tests for the inbound OrderEvent.

The consumer deserializes Kafka message bytes with
``OrderEvent.model_validate_json``. Malformed or invalid payloads must fail
loudly here so the consumer can route them to the DLQ instead of crashing.
"""

import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from app.models import OrderEvent


def _valid_payload() -> dict:
    return {
        "order_id": "order-1",
        "product_id": "PROD-001",
        "quantity": 5,
        "customer_id": "CUST-1",
        "status": "PENDING",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def test_valid_event_parses():
    event = OrderEvent.model_validate_json(json.dumps(_valid_payload()))
    assert event.order_id == "order-1"
    assert event.quantity == 5
    assert isinstance(event.created_at, datetime)


def test_quantity_below_one_is_rejected():
    payload = _valid_payload()
    payload["quantity"] = 0
    with pytest.raises(ValidationError):
        OrderEvent.model_validate_json(json.dumps(payload))


def test_missing_field_is_rejected():
    payload = _valid_payload()
    del payload["customer_id"]
    with pytest.raises(ValidationError):
        OrderEvent.model_validate_json(json.dumps(payload))


def test_malformed_json_is_rejected():
    with pytest.raises(ValidationError):
        OrderEvent.model_validate_json(b"this is not json")


def test_wrong_type_is_rejected():
    payload = _valid_payload()
    payload["quantity"] = "not-a-number"
    with pytest.raises(ValidationError):
        OrderEvent.model_validate_json(json.dumps(payload))
