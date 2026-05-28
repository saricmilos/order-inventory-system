from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    orders_topic: str = "orders"
    log_level: str = "INFO"

    model_config = {"env_file": ".env"}


settings = Settings()
