"""Pytest configuration and fixtures."""

import os

# Set required environment variables before importing app modules
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_DB", "cortex_test")
os.environ.setdefault("POSTGRES_USER", "test")
os.environ.setdefault("POSTGRES_PASSWORD", "test")
os.environ.setdefault("DUCKDB_API_URL", "http://localhost:8081")
os.environ.setdefault("OAUTH_TOKEN_PATH", "/tmp/test-token.json")
os.environ.setdefault("OAUTH_SECRET_KEY", "test-secret-key-for-testing-only")
