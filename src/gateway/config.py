"""Configuration for Cortex Gateway."""

import os
from dataclasses import dataclass


def _get_int_env(key: str, default: str) -> int:
    """Get integer from environment variable with error handling."""
    try:
        return int(os.environ.get(key, default))
    except ValueError as e:
        raise ValueError(f"Invalid integer value for {key}: {os.environ.get(key)}") from e


@dataclass
class Config:
    """Gateway configuration from environment variables."""

    # Postgres
    postgres_host: str = os.environ.get("POSTGRES_HOST", "localhost")
    postgres_port: int = _get_int_env("POSTGRES_PORT", "5432")
    postgres_db: str = os.environ.get("POSTGRES_DB", "cortex")
    postgres_user: str = os.environ.get("POSTGRES_USER", "cortex")
    postgres_password: str = os.environ["POSTGRES_PASSWORD"]

    # DuckDB API
    duckdb_api_url: str = os.environ.get("DUCKDB_API_URL", "http://localhost:8081")

    # Server
    host: str = os.environ.get("HOST", "0.0.0.0")
    port: int = _get_int_env("PORT", "8080")
    metrics_port: int = _get_int_env("METRICS_PORT", "8001")

    @property
    def postgres_dsn(self) -> str:
        """Build Postgres connection string."""
        return (
            f"host={self.postgres_host} "
            f"port={self.postgres_port} "
            f"dbname={self.postgres_db} "
            f"user={self.postgres_user} "
            f"password={self.postgres_password}"
        )


config = Config()
