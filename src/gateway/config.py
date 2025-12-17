"""Configuration for Cortex Gateway."""

import os
from dataclasses import dataclass


@dataclass
class Config:
    """Gateway configuration from environment variables."""

    # Postgres
    postgres_host: str = os.environ.get("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.environ.get("POSTGRES_PORT", "5432"))
    postgres_db: str = os.environ.get("POSTGRES_DB", "cortex")
    postgres_user: str = os.environ.get("POSTGRES_USER", "cortex")
    postgres_password: str = os.environ.get("POSTGRES_PASSWORD", "")

    # DuckDB API
    duckdb_api_url: str = os.environ.get("DUCKDB_API_URL", "http://localhost:8081")

    # Server
    host: str = os.environ.get("HOST", "0.0.0.0")
    port: int = int(os.environ.get("PORT", "8080"))
    metrics_port: int = int(os.environ.get("METRICS_PORT", "8001"))

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
