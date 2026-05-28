import logging

from confluent_kafka import Producer

from .config import settings
from .models import OrderEvent

logger = logging.getLogger(__name__)


class KafkaProducer:
    def __init__(self) -> None:
        self._producer = Producer({
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
        })

    def send_order_event(self, event: OrderEvent, correlation_id: str | None = None) -> None:
        headers: list[tuple[str, bytes]] = []
        if correlation_id:
            headers.append(("x-correlation-id", correlation_id.encode()))

        self._producer.produce(
            topic=settings.orders_topic,
            key=event.order_id.encode(),
            value=event.model_dump_json().encode(),
            headers=headers,
            on_delivery=self._delivery_callback,
        )
        # Non-blocking callback drain — does not block the HTTP response path
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout)

    def _delivery_callback(self, err, msg) -> None:
        if err:
            logger.error("delivery_failed", extra={"error": str(err), "topic": msg.topic()})
        else:
            logger.info(
                "delivery_confirmed",
                extra={
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                },
            )
