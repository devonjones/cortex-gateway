"""Email-related API endpoints."""

import json
from typing import Any

from flask import Blueprint, Response, jsonify, request

from gateway.services import duckdb, postgres

emails_bp = Blueprint("emails", __name__)


@emails_bp.route("/")
def list_emails():
    """List emails with pagination."""
    limit = request.args.get("limit", 50, type=int)
    offset = request.args.get("offset", 0, type=int)
    label = request.args.get("label")

    # Clamp limit
    limit = min(limit, 100)

    query = """
        SELECT
            er.gmail_id,
            ep.from_address,
            ep.to_addresses,
            ep.subject,
            ep.date,
            er.label_ids
        FROM emails_raw er
        LEFT JOIN emails_parsed ep ON er.gmail_id = ep.gmail_id
    """
    params: list[str] = []

    if label:
        query += " WHERE er.label_ids @> %s::jsonb"
        params.append(json.dumps([label]))

    query += " ORDER BY ep.date DESC NULLS LAST LIMIT %s OFFSET %s"
    params.extend([str(limit), str(offset)])

    results = postgres.execute_query(query, tuple(params))

    return jsonify({
        "emails": results,
        "limit": limit,
        "offset": offset,
        "count": len(results),
    })


@emails_bp.route("/<gmail_id>")
def get_email(gmail_id: str) -> Response | tuple[Response, int]:
    """Get email details by Gmail ID."""
    query = """
        SELECT
            er.gmail_id,
            er.history_id,
            er.label_ids,
            er.headers,
            er.internal_date,
            er.created_at,
            ep.from_address,
            ep.to_addresses,
            ep.cc_addresses,
            ep.subject,
            ep.date,
            ep.message_id,
            ep.in_reply_to,
            ep.references
        FROM emails_raw er
        LEFT JOIN emails_parsed ep ON er.gmail_id = ep.gmail_id
        WHERE er.gmail_id = %s
    """

    result = postgres.execute_one(query, (gmail_id,))
    if not result:
        return jsonify({"error": "Email not found"}), 404

    # Get classification if exists
    classification_query = """
        SELECT matched_rule, label, action, llm_category, confidence, created_at
        FROM classifications
        WHERE gmail_id = %s
        ORDER BY created_at DESC
        LIMIT 1
    """
    classification = postgres.execute_one(classification_query, (gmail_id,))
    if classification:
        result["classification"] = classification

    return jsonify(result)


@emails_bp.route("/<gmail_id>/body")
def get_email_body(gmail_id: str) -> Response | tuple[Response, int]:
    """Get email body from DuckDB."""
    body = duckdb.get_body(gmail_id)
    if not body:
        return jsonify({"error": "Body not found"}), 404

    return jsonify(body)


@emails_bp.route("/<gmail_id>/text")
def get_email_text(gmail_id: str) -> Response | tuple[Response, int]:
    """Get decoded plain text from email."""
    text = duckdb.get_mail_text(gmail_id)
    if text is None:
        return jsonify({"error": "Text not found"}), 404

    return jsonify({"gmail_id": gmail_id, "text": text})


@emails_bp.route("/stats")
def get_stats():
    """Get email statistics."""
    # Count by sync status
    count_query = """
        SELECT
            COUNT(*) as total_emails,
            COUNT(DISTINCT ep.gmail_id) as parsed_emails,
            COUNT(DISTINCT c.gmail_id) as classified_emails
        FROM emails_raw er
        LEFT JOIN emails_parsed ep ON er.gmail_id = ep.gmail_id
        LEFT JOIN classifications c ON er.gmail_id = c.gmail_id
    """
    counts = postgres.execute_one(count_query)

    # DuckDB stats
    duckdb_stats = duckdb.get_stats()

    return jsonify({
        "postgres": counts,
        "duckdb": duckdb_stats,
    })
