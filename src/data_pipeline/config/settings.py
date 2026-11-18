from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables and .env files."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: str = "development"
    log_level: str = "INFO"

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "ecommerce"
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"

    dataset_dir: str = "data/raw"
    processed_data_dir: str = "data/processed"
    quarantine_dir: str = "data/quarantine"
    sample_data_dir: str = "data/sample"


@lru_cache
def get_settings() -> Settings:
    """Return a cached settings instance."""
    return Settings()
