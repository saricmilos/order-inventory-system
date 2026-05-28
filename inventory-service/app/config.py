from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    orders_topic: str = "orders"
    dlq_topic: str = "orders.dlq"
    consumer_group_id: str = "inventory-service-group"
    max_retry_attempts: int = 3
    retry_base_delay_seconds: float = 1.0
    log_level: str = "INFO"

    model_config = {"env_file": ".env"}


settings = Settings()
