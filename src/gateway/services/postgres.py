"""Postgres database service."""

from typing import Any

from psycopg2 import pool
from psycopg2.extras import RealDictCursor

from gateway.config import config

# Connection pool (initialized lazily)
_pool: pool.ThreadedConnectionPool | None = None


def init_pool(minconn: int = 2, maxconn: int = 10) -> None:
    """Initialize the connection pool."""
    global _pool
    if _pool is None:
        _pool = pool.ThreadedConnectionPool(
            minconn,
            maxconn,
            config.postgres_dsn,
        )


def get_connection() -> Any:
    """Get a connection from the pool."""
    if _pool is None:
        init_pool()
    assert _pool is not None
    return _pool.getconn()


def put_connection(conn: Any) -> None:
    """Return a connection to the pool."""
    if _pool is not None:
        _pool.putconn(conn)


class ConnectionContext:
    """Context manager for database connections."""

    def __init__(self) -> None:
        self.conn: Any = None

    def __enter__(self) -> Any:
        self.conn = get_connection()
        return self.conn

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.conn:
            if exc_type is not None:
                self.conn.rollback()
            put_connection(self.conn)


def execute_query(query: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
    """Execute a query and return results as list of dicts."""
    with ConnectionContext() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()


def execute_one(query: str, params: tuple[Any, ...] | None = None) -> dict[str, Any] | None:
    """Execute a query and return single result as dict."""
    results = execute_query(query, params)
    return results[0] if results else None


def execute_update(query: str, params: tuple[Any, ...] | None = None) -> int:
    """Execute an update/insert/delete and return affected row count."""
    with ConnectionContext() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            conn.commit()
            return cur.rowcount
