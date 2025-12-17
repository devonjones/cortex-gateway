"""Triage-related API endpoints."""

import json
from datetime import datetime, timedelta

from flask import Blueprint, jsonify, request

from gateway.services import postgres

triage_bp = Blueprint("triage", __name__)


@triage_bp.route("/stats")
def triage_stats():
    """Get classification statistics."""
    # Overall counts by rule/category
    query = """
        SELECT
            COALESCE(matched_rule, 'llm') as classifier,
            label,
            action,
            COUNT(*) as count
        FROM classifications
        GROUP BY matched_rule, label, action
        ORDER BY count DESC
        LIMIT 50
    """
    by_rule = postgres.execute_query(query)

    # Recent activity (last 24h)
    recent_query = """
        SELECT
            date_trunc('hour', created_at) as hour,
            COUNT(*) as count
        FROM classifications
        WHERE created_at >= NOW() - INTERVAL '24 hours'
        GROUP BY date_trunc('hour', created_at)
        ORDER BY hour
    """
    recent = postgres.execute_query(recent_query)

    # Method breakdown (rule vs llm)
    method_query = """
        SELECT
            CASE WHEN matched_rule IS NOT NULL THEN 'rule' ELSE 'llm' END as method,
            COUNT(*) as count
        FROM classifications
        GROUP BY CASE WHEN matched_rule IS NOT NULL THEN 'rule' ELSE 'llm' END
    """
    methods = postgres.execute_query(method_query)

    return jsonify({
        "by_classifier": by_rule,
        "recent_hourly": recent,
        "methods": {row["method"]: row["count"] for row in methods},
    })


@triage_bp.route("/rerun", methods=["POST"])
def rerun_triage():
    """Re-run triage on emails.

    Request body:
        gmail_ids: List of specific Gmail IDs to rerun
        label: Label filter (e.g., "Cortex/Uncategorized")
        days: Number of days to look back (default: 7)
        force: If true, rerun even if already classified
        priority: Queue priority (default: -100)
    """
    data = request.get_json() or {}

    gmail_ids = data.get("gmail_ids", [])
    label = data.get("label")
    days = data.get("days", 7)
    force = data.get("force", False)
    priority = data.get("priority", -100)

    if not gmail_ids and not label:
        return jsonify({
            "error": "Must specify either gmail_ids or label filter"
        }), 400

    # Build insert query
    if gmail_ids:
        # Specific IDs
        placeholders = ",".join(["%s"] * len(gmail_ids))
        query = f"""
            INSERT INTO queue (queue_name, gmail_id, payload, priority, status, created_at)
            SELECT
                'triage',
                er.gmail_id,
                jsonb_build_object('gmail_id', er.gmail_id, 'rerun', true),
                %s,
                'pending',
                NOW()
            FROM emails_raw er
            WHERE er.gmail_id IN ({placeholders})
        """
        params: list[str | int] = [priority] + gmail_ids
    else:
        # Label filter with date range
        cutoff = datetime.utcnow() - timedelta(days=days)
        query = """
            INSERT INTO queue (queue_name, gmail_id, payload, priority, status, created_at)
            SELECT
                'triage',
                er.gmail_id,
                jsonb_build_object('gmail_id', er.gmail_id, 'rerun', true),
                %s,
                'pending',
                NOW()
            FROM emails_raw er
            WHERE er.created_at >= %s
            AND er.label_ids @> %s::jsonb
        """
        params = [priority, cutoff.isoformat(), json.dumps([label])]

    # Avoid duplicates unless force
    if not force:
        query += """
            AND NOT EXISTS (
                SELECT 1 FROM queue q
                WHERE q.queue_name = 'triage'
                AND q.gmail_id = er.gmail_id
                AND q.status IN ('pending', 'processing')
            )
        """

    count = postgres.execute_update(query, tuple(params))

    return jsonify({
        "message": f"Enqueued {count} emails for triage rerun",
        "gmail_ids": gmail_ids if gmail_ids else None,
        "label": label,
        "days": days if not gmail_ids else None,
        "force": force,
        "priority": priority,
        "count": count,
    })


@triage_bp.route("/classifications")
def list_classifications():
    """List recent classifications."""
    limit = request.args.get("limit", 50, type=int)
    offset = request.args.get("offset", 0, type=int)
    label = request.args.get("label")
    action = request.args.get("action")

    limit = min(limit, 100)

    query = """
        SELECT
            c.gmail_id,
            c.matched_rule,
            c.label,
            c.action,
            c.llm_category,
            c.confidence,
            c.created_at,
            ep.subject,
            ep.from_address
        FROM classifications c
        LEFT JOIN emails_parsed ep ON c.gmail_id = ep.gmail_id
        WHERE 1=1
    """
    params: list[str] = []

    if label:
        query += " AND c.label = %s"
        params.append(label)

    if action:
        query += " AND c.action = %s"
        params.append(action)

    query += " ORDER BY c.created_at DESC LIMIT %s OFFSET %s"
    params.extend([str(limit), str(offset)])

    results = postgres.execute_query(query, tuple(params))

    return jsonify({
        "classifications": results,
        "limit": limit,
        "offset": offset,
        "count": len(results),
    })
