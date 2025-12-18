"""Email-related API endpoints."""

import json

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
            ep.from_addr,
            ep.to_addrs,
            ep.subject,
            ep.date_header,
            er.label_ids
        FROM emails_raw er
        LEFT JOIN emails_parsed ep ON er.gmail_id = ep.gmail_id
    """
    params: list[str] = []

    if label:
        query += " WHERE er.label_ids @> %s::jsonb"
        params.append(json.dumps([label]))

    query += " ORDER BY ep.date_header DESC NULLS LAST LIMIT %s OFFSET %s"
    params.extend([str(limit), str(offset)])

    results = postgres.execute_query(query, tuple(params))

    return jsonify(
        {
            "emails": results,
            "limit": limit,
            "offset": offset,
            "count": len(results),
        }
    )


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
            ep.from_addr,
            ep.from_name,
            ep.to_addrs,
            ep.cc_addrs,
            ep.subject,
            ep.date_header,
            ep.message_id,
            ep.in_reply_to,
            ep.refs
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

    return jsonify(
        {
            "postgres": counts,
            "duckdb": duckdb_stats,
        }
    )


@emails_bp.route("/by-label/<label_id>")
def get_emails_by_label(label_id: str) -> Response:
    """Get emails with a specific Gmail label ID.

    Useful for finding emails to backfill or debug label issues.

    Args:
        label_id: Gmail label ID (e.g., 'Label_117')

    Query params:
        limit: Number of emails to return (default 50, max 100)
        offset: Pagination offset (default 0)
    """
    limit = request.args.get("limit", 50, type=int)
    offset = request.args.get("offset", 0, type=int)
    limit = min(limit, 100)

    query = """
        SELECT
            er.gmail_id,
            ep.from_addr,
            ep.subject,
            ep.date_header,
            er.label_ids
        FROM emails_raw er
        JOIN emails_parsed ep ON er.gmail_id = ep.gmail_id
        WHERE %s = ANY(er.label_ids)
        ORDER BY ep.date_header DESC NULLS LAST
        LIMIT %s OFFSET %s
    """

    results = postgres.execute_query(query, (label_id, limit, offset))

    # Get label name for context
    label_query = "SELECT id, name FROM gmail_labels WHERE id = %s"
    label_info = postgres.execute_one(label_query, (label_id,))

    return jsonify(
        {
            "label": label_info,
            "emails": results,
            "limit": limit,
            "offset": offset,
            "count": len(results),
        }
    )


@emails_bp.route("/sender/<path:from_addr>/classifications")
def get_sender_classifications(from_addr: str) -> Response:
    """Get classification breakdown for a sender.

    Shows how emails from this sender are classified, useful for
    debugging rules or identifying if old classifications exist.

    Args:
        from_addr: Email sender address
    """
    query = """
        SELECT
            c.action_taken->>'label' as label,
            COUNT(*) as count
        FROM classifications c
        JOIN emails_parsed ep ON c.gmail_id = ep.gmail_id
        WHERE ep.from_addr = %s
        GROUP BY c.action_taken->>'label'
        ORDER BY count DESC
    """

    results = postgres.execute_query(query, (from_addr,))

    return jsonify(
        {
            "from_addr": from_addr,
            "classifications": results,
            "total": sum(r["count"] for r in results),
        }
    )


@emails_bp.route("/classifications/distribution")
def get_classification_distribution() -> Response:
    """Get distribution of classifications by label.

    Shows top labels by unique email count, useful for understanding
    rule coverage and identifying high-volume senders.

    Query params:
        limit: Number of labels to return (default 50, max 200)
    """
    limit = request.args.get("limit", 50, type=int)
    limit = min(limit, 200)

    query = """
        SELECT
            action_taken->>'label' as label,
            COUNT(DISTINCT gmail_id) as count
        FROM classifications
        WHERE action_taken->>'label' IS NOT NULL
        GROUP BY action_taken->>'label'
        ORDER BY count DESC
        LIMIT %s
    """

    results = postgres.execute_query(query, (limit,))

    return jsonify(
        {
            "labels": results,
            "limit": limit,
            "count": len(results),
        }
    )


@emails_bp.route("/uncategorized/top-senders")
def get_uncategorized_top_senders() -> Response:
    """Get top senders with emails only classified as Uncategorized.

    These are senders where no email has been correctly categorized,
    indicating a missing rule.

    Query params:
        limit: Number of senders to return (default 20, max 100)
    """
    limit = request.args.get("limit", 20, type=int)
    limit = min(limit, 100)

    query = """
        WITH uncategorized_emails AS (
            SELECT DISTINCT c.gmail_id
            FROM classifications c
            WHERE c.action_taken->>'label' = 'Cortex/Uncategorized'
        ),
        emails_with_other_labels AS (
            SELECT DISTINCT c.gmail_id
            FROM classifications c
            WHERE c.action_taken->>'label' != 'Cortex/Uncategorized'
              AND c.action_taken->>'label' IS NOT NULL
        ),
        only_uncategorized AS (
            SELECT gmail_id FROM uncategorized_emails
            EXCEPT
            SELECT gmail_id FROM emails_with_other_labels
        )
        SELECT
            ep.from_addr,
            COUNT(DISTINCT ep.gmail_id) as count
        FROM only_uncategorized ou
        JOIN emails_parsed ep ON ep.gmail_id = ou.gmail_id
        GROUP BY ep.from_addr
        ORDER BY count DESC
        LIMIT %s
    """

    results = postgres.execute_query(query, (limit,))

    return jsonify(
        {
            "senders": results,
            "limit": limit,
            "count": len(results),
        }
    )
