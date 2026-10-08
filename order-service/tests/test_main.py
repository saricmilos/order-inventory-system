"""API tests for the order-service HTTP endpoints.

The Kafka producer is replaced with a mock so these tests exercise routing,
validation, status codes, and the publish call without needing a broker.
"""

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client():
    """TestClient with a mocked producer.

    The app is constructed without running the lifespan (no real KafkaProducer),
    so we attach a mock to app.state, which is what the endpoint reads.
    """
    app.state.producer = MagicMock()
    return TestClient(app)


def test_health_returns_ok(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "service": "order-service"}


def test_create_order_returns_202_and_order_id(client):
    resp = client.post(
        "/orders",
        json={"product_id": "PROD-001", "quantity": 5, "customer_id": "CUST-1"},
    )
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "PENDING"
    assert body["order_id"]


def test_create_order_publishes_to_kafka(client):
    client.post(
        "/orders",
        json={"product_id": "PROD-001", "quantity": 5, "customer_id": "CUST-1"},
    )
    app.state.producer.send_order_event.assert_called_once()


def test_create_order_forwards_request_id_as_correlation_id(client):
    client.post(
        "/orders",
        json={"product_id": "PROD-001", "quantity": 5, "customer_id": "CUST-1"},
        headers={"x-request-id": "trace-abc"},
    )
    _, kwargs = app.state.producer.send_order_event.call_args
    assert kwargs["correlation_id"] == "trace-abc"


def test_invalid_quantity_returns_422(client):
    resp = client.post(
        "/orders",
        json={"product_id": "PROD-001", "quantity": 0, "customer_id": "CUST-1"},
    )
    assert resp.status_code == 422


def test_missing_field_returns_422(client):
    resp = client.post(
        "/orders",
        json={"product_id": "PROD-001", "quantity": 1},
    )
    assert resp.status_code == 422
