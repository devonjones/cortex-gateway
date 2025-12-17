"""DuckDB API client service."""

from typing import Any

import httpx

from gateway.config import config


def get_body(gmail_id: str) -> dict[str, Any] | None:
    """Fetch email body from DuckDB API."""
    try:
        resp = httpx.get(
            f"{config.duckdb_api_url}/body",
            params={"gmail_id": gmail_id},
            timeout=10.0,
        )
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 404:
            return None
        resp.raise_for_status()
    except httpx.HTTPError:
        return None
    return None


def get_bodies(gmail_ids: list[str]) -> list[dict[str, Any]]:
    """Fetch multiple email bodies from DuckDB API."""
    try:
        resp = httpx.get(
            f"{config.duckdb_api_url}/bodies",
            params={"gmail_ids": ",".join(gmail_ids)},
            timeout=30.0,
        )
        if resp.status_code == 200:
            return resp.json().get("bodies", [])
        resp.raise_for_status()
    except httpx.HTTPError:
        return []
    return []


def get_mail_text(gmail_id: str) -> str | None:
    """Fetch decoded plain text from email body."""
    try:
        resp = httpx.get(
            f"{config.duckdb_api_url}/mail_text",
            params={"gmail_id": gmail_id},
            timeout=10.0,
        )
        if resp.status_code == 200:
            return resp.json().get("text")
        elif resp.status_code == 404:
            return None
        resp.raise_for_status()
    except httpx.HTTPError:
        return None
    return None


def get_stats() -> dict[str, Any]:
    """Get DuckDB stats."""
    try:
        resp = httpx.get(f"{config.duckdb_api_url}/stats", timeout=5.0)
        if resp.status_code == 200:
            return resp.json()
        resp.raise_for_status()
    except httpx.HTTPError:
        return {"error": "Failed to fetch stats"}
    return {}
