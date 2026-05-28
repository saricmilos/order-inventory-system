import logging

from .inventory import InventoryStore, InsufficientStockError
from .models import OrderEvent

logger = logging.getLogger(__name__)


class OrderProcessor:
    def __init__(self, inventory: InventoryStore) -> None:
        self._inventory = inventory

    def process(self, event: OrderEvent, correlation_id: str | None = None) -> None:
        try:
            self._inventory.reserve(event.order_id, event.product_id, event.quantity)
        except InsufficientStockError:
            logger.warning(
                "order_rejected_insufficient_stock",
                extra={
                    "order_id": event.order_id,
                    "product_id": event.product_id,
                    "quantity": event.quantity,
                    "correlation_id": correlation_id,
                },
            )
            raise

        remaining = self._inventory.get_stock(event.product_id)
        logger.info(
            "order_processed",
            extra={
                "order_id": event.order_id,
                "product_id": event.product_id,
                "quantity": event.quantity,
                "remaining_stock": remaining,
                "correlation_id": correlation_id,
            },
        )
