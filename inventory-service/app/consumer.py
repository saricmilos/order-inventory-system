import logging
import signal
import time

from confluent_kafka import Consumer, KafkaError, Producer
from pydantic import ValidationError
from pythonjsonlogger import jsonlogger

from .config import settings
from .inventory import InsufficientStockError, InventoryStore
from .models import OrderEvent
from .processor import OrderProcessor

logger = logging.getLogger(__name__)


def _setup_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(jsonlogger.JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(settings.log_level)


def _extract_header(headers, key: str) -> str | None:
    if not headers:
        return None
    for k, v in headers:
        if k == key:
            return v.decode() if isinstance(v, bytes) else v
    return None


def _send_to_dlq(
    dlq_producer: Producer,
    original_msg,
    error: Exception,
    attempt: int,
) -> None:
    headers = [
        ("x-failure-reason", str(error).encode()),
        ("x-retry-count", str(attempt).encode()),
        ("x-original-topic", (original_msg.topic() or "").encode()),
        ("x-original-partition", str(original_msg.partition()).encode()),
        ("x-original-offset", str(original_msg.offset()).encode()),
    ]
    dlq_producer.produce(
        topic=settings.dlq_topic,
        key=original_msg.key(),
        value=original_msg.value(),
        headers=headers,
    )
    dlq_producer.poll(0)
    logger.warning(
        "message_sent_to_dlq",
        extra={
            "dlq_topic": settings.dlq_topic,
            "failure_reason": str(error),
            "retry_count": attempt,
        },
    )


def run() -> None:
    _setup_logging()

    inventory = InventoryStore()
    processor = OrderProcessor(inventory)

    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": settings.consumer_group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    dlq_producer = Producer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "acks": "all",
    })

    shutdown = False

    def _handle_signal(signum, frame) -> None:
        nonlocal shutdown
        logger.info("graceful_shutdown_initiated", extra={"signal": signum})
        shutdown = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    consumer.subscribe([settings.orders_topic])
    logger.info(
        "consumer_started",
        extra={"topic": settings.orders_topic, "group": settings.consumer_group_id},
    )

    try:
        while not shutdown:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                # PARTITION_EOF is informational, not an error
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("consumer_error", extra={"error": str(msg.error())})
                continue

            correlation_id = _extract_header(msg.headers(), "x-correlation-id")

            # Deserialize and validate — schema failures go straight to DLQ (retrying won't fix bad data)
            try:
                event = OrderEvent.model_validate_json(msg.value())
            except (ValidationError, ValueError) as exc:
                logger.error(
                    "invalid_message_schema",
                    extra={"error": str(exc), "correlation_id": correlation_id},
                )
                _send_to_dlq(dlq_producer, msg, exc, attempt=0)
                consumer.commit(message=msg)
                continue

            logger.info(
                "message_received",
                extra={
                    "order_id": event.order_id,
                    "product_id": event.product_id,
                    "quantity": event.quantity,
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "correlation_id": correlation_id,
                },
            )

            # Process: business failures (InsufficientStock) are committed without retry;
            # system/unexpected failures are retried with exponential backoff then sent to DLQ.
            for attempt in range(settings.max_retry_attempts):
                try:
                    processor.process(event, correlation_id=correlation_id)
                    break
                except InsufficientStockError:
                    # Valid business outcome — commit and move on
                    break
                except Exception as exc:
                    if attempt == settings.max_retry_attempts - 1:
                        _send_to_dlq(dlq_producer, msg, exc, attempt=attempt + 1)
                    else:
                        delay = settings.retry_base_delay_seconds * (2 ** attempt)
                        logger.warning(
                            "processing_failed_retrying",
                            extra={
                                "order_id": event.order_id,
                                "error": str(exc),
                                "attempt": attempt + 1,
                                "retry_delay_seconds": delay,
                            },
                        )
                        time.sleep(delay)

            # Commit only after processing is fully resolved (success, business reject, or DLQ)
            consumer.commit(message=msg)

    finally:
        consumer.close()
        dlq_producer.flush()
        logger.info("consumer_stopped")


if __name__ == "__main__":
    run()
