class InsufficientStockError(Exception):
    pass


class InventoryStore:
    def __init__(self) -> None:
        self._stock: dict[str, int] = {
            "PROD-001": 100,
            "PROD-002": 50,
            "PROD-003": 0,
        }
        # Tracks successfully processed order IDs to prevent double-deduction on redelivery
        self._processed_orders: set[str] = set()

    def reserve(self, order_id: str, product_id: str, quantity: int) -> None:
        if order_id in self._processed_orders:
            return

        if product_id not in self._stock:
            raise ValueError(f"Unknown product: {product_id}")

        if self._stock[product_id] < quantity:
            raise InsufficientStockError(
                f"Insufficient stock for {product_id}: "
                f"available={self._stock[product_id]}, requested={quantity}"
            )

        self._stock[product_id] -= quantity
        self._processed_orders.add(order_id)

    def get_stock(self, product_id: str) -> int:
        return self._stock.get(product_id, 0)
