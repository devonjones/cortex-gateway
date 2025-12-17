"""Flask application factory for Cortex Gateway."""

import httpx
import structlog
from cortex_utils.api import MetricsMiddleware, health_bp, register_health_check
from cortex_utils.metrics import start_metrics_server
from flask import Flask

from gateway.blueprints import backfill_bp, emails_bp, queue_bp, sync_bp, triage_bp
from gateway.config import config
from gateway.services.postgres import get_connection, init_pool

logger = structlog.get_logger()


def create_app() -> Flask:
    """Create and configure the Flask application."""
    app = Flask("cortex-gateway")

    # Apply metrics middleware
    app.wsgi_app = MetricsMiddleware(app.wsgi_app, "cortex-gateway")  # type: ignore[method-assign]

    # Register blueprints
    app.register_blueprint(health_bp)
    app.register_blueprint(emails_bp, url_prefix="/emails")
    app.register_blueprint(queue_bp, url_prefix="/queue")
    app.register_blueprint(backfill_bp, url_prefix="/backfill")
    app.register_blueprint(triage_bp, url_prefix="/triage")
    app.register_blueprint(sync_bp, url_prefix="/sync")

    # Register health checks
    register_health_check(app, check_postgres)
    register_health_check(app, check_duckdb)

    # Initialize on first request
    with app.app_context():
        init_pool()

    # Start metrics server
    start_metrics_server(port=config.metrics_port)

    logger.info(
        "gateway_started",
        host=config.host,
        port=config.port,
        metrics_port=config.metrics_port,
    )

    return app


def check_postgres() -> tuple[str, bool]:
    """Health check for Postgres connection."""
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return ("postgres", True)
    except Exception as e:
        logger.warning("postgres_health_check_failed", error=e)
        return ("postgres", False)


def check_duckdb() -> tuple[str, bool]:
    """Health check for DuckDB API."""
    try:
        resp = httpx.get(f"{config.duckdb_api_url}/health", timeout=5.0)
        return ("duckdb", resp.status_code == 200)
    except httpx.RequestError as e:
        logger.warning("duckdb_health_check_failed", error=e)
        return ("duckdb", False)


if __name__ == "__main__":
    # For local development only
    application = create_app()
    application.run(host=config.host, port=config.port, debug=True)
