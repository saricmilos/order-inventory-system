import logging
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from pythonjsonlogger import jsonlogger

from .config import settings
from .models import OrderEvent, OrderRequest
from .producer import KafkaProducer


def _setup_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(jsonlogger.JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(settings.log_level)


logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    _setup_logging()
    app.state.producer = KafkaProducer()
    logger.info("order_service_started")
    yield
    app.state.producer.flush()
    logger.info("order_service_stopped")


app = FastAPI(title="Order Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": "order-service"}


@app.post("/orders", status_code=202)
def create_order(body: OrderRequest, request: Request):
    correlation_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    event = OrderEvent.from_request(body)

    logger.info(
        "order_received",
        extra={
            "order_id": event.order_id,
            "product_id": event.product_id,
            "customer_id": event.customer_id,
            "quantity": event.quantity,
            "correlation_id": correlation_id,
        },
    )

    request.app.state.producer.send_order_event(event, correlation_id=correlation_id)

    return {"order_id": event.order_id, "status": event.status}
