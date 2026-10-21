from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    # Application
    PROJECT_NAME: str = "RelayHook - Resilient Webhook Engine"
    API_V1_STR: str = "/api/v1"
    DEBUG: bool = False

    # PostgreSQL Database Configuration
    POSTGRES_SERVER: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_DB: str = "webhook_db"

    @computed_field
    @property
    def SQLALCHEMY_DATABASE_URI(self) -> str:
        """Construct async PostgreSQL connection URI."""
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_SERVER}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    # Redis Configuration
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379

    @computed_field
    @property
    def REDIS_URL(self) -> str:
        """Construct Redis connection URI."""
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/0"

    # Webhook Engine Defaults
    DEFAULT_TIMEOUT_SECONDS: float = 5.0
    MAX_RETRY_ATTEMPTS: int = 5
    MAX_BACKOFF_SECONDS: float = 300.0
    RATE_LIMIT_PER_MINUTE: int = 100

    # Retry Worker Configuration
    WORKER_POLL_INTERVAL_SECONDS: float = 5.0
    WORKER_BATCH_SIZE: int = 50


settings = Settings()
