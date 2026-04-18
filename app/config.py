from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
    debug: bool = False

    postgres_user: str = "payments"
    postgres_password: SecretStr = SecretStr("payments")
    postgres_db: str = "payments"
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    pool_recycle: int = 3600
    driver: str = "postgresql+asyncpg"
    service_name: str = "payments_processor"
    min_pool_size: int = 10
    max_pool_size: int = 20

    rabbitmq_host: str = "rabbitmq"
    rabbitmq_port: int = 5672
    rabbitmq_user: str = "guest"
    rabbitmq_password: SecretStr = SecretStr("guest")

    api_key: SecretStr = SecretStr("secret-api-key")
    log_level: str = "INFO"

    outbox_poll_interval: float = 1.0
    outbox_batch_size: int = 50

    webhook_timeout: int = 10
    webhook_max_attempts: int = Field(default=3, ge=1)

    payments_exchange: str = "payments"
    payments_new_queue: str = "payments.new"
    payments_dlx: str = "payments.dlx"
    payments_dlq: str = "payments.dlq"

    consumer_max_attempts: int = Field(default=3, ge=1)

    @property
    def rabbitmq_url(self) -> str:
        return (
            f"amqp://{self.rabbitmq_user}:{self.rabbitmq_password.get_secret_value()}"
            f"@{self.rabbitmq_host}:{self.rabbitmq_port}/"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
