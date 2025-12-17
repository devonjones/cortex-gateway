"""Backfill API endpoints for historical email processing."""

from datetime import datetime, timedelta

from flask import Blueprint, jsonify, request

from gateway.services import postgres

backfill_bp = Blueprint("backfill", __name__)


@backfill_bp.route("/", methods=["POST"])
def trigger_backfill():
    """Trigger backfill of emails to a queue.

    Request body:
        queue: Target queue name (triage, parse, attachment)
        days: Number of days to backfill (default: 7)
        label: Optional label filter
        priority: Queue priority (default: -100 for backfill)
    """
    data = request.get_json() or {}

    queue_name = data.get("queue", "triage")
    days = data.get("days", 7)
    label = data.get("label")
    priority = data.get("priority", -100)  # Low priority for backfill

    if queue_name not in ("triage", "parse", "attachment"):
        return jsonify({"error": f"Invalid queue: {queue_name}"}), 400

    # Calculate date cutoff
    cutoff = datetime.utcnow() - timedelta(days=days)

    # Build query to find emails to backfill
    query = """
        INSERT INTO queue (queue_name, gmail_id, payload, priority, status, created_at)
        SELECT
            %s,
            er.gmail_id,
            jsonb_build_object('gmail_id', er.gmail_id, 'backfill', true),
            %s,
            'pending',
            NOW()
        FROM emails_raw er
        WHERE er.created_at >= %s
    """
    params: list[str | int] = [queue_name, priority, cutoff.isoformat()]

    if label:
        query += " AND er.label_ids @> %s::jsonb"
        params.append(f'["{label}"]')

    # Avoid duplicates
    query += """
        AND NOT EXISTS (
            SELECT 1 FROM queue q
            WHERE q.queue_name = %s
            AND q.gmail_id = er.gmail_id
            AND q.status IN ('pending', 'processing')
        )
    """
    params.append(queue_name)

    count = postgres.execute_update(query, tuple(params))

    return jsonify({
        "message": f"Enqueued {count} emails for backfill",
        "queue": queue_name,
        "days": days,
        "label": label,
        "priority": priority,
        "count": count,
    })


@backfill_bp.route("/status")
def backfill_status():
    """Get status of backfill jobs (low priority queue items)."""
    query = """
        SELECT
            queue_name,
            status,
            COUNT(*) as count
        FROM queue
        WHERE priority < 0
        GROUP BY queue_name, status
        ORDER BY queue_name, status
    """
    results = postgres.execute_query(query)

    # Reshape into nested structure
    status: dict[str, dict[str, int]] = {}
    for row in results:
        queue_name = row["queue_name"]
        job_status = row["status"]
        count = row["count"]
        if queue_name not in status:
            status[queue_name] = {}
        status[queue_name][job_status] = count

    return jsonify({"backfill_status": status})


@backfill_bp.route("/cancel", methods=["POST"])
def cancel_backfill():
    """Cancel pending backfill jobs for a queue."""
    data = request.get_json() or {}
    queue_name = data.get("queue")

    if not queue_name:
        return jsonify({"error": "queue parameter required"}), 400

    update_query = """
        UPDATE queue
        SET status = 'cancelled', updated_at = NOW()
        WHERE queue_name = %s
        AND priority < 0
        AND status = 'pending'
    """
    count = postgres.execute_update(update_query, (queue_name,))

    return jsonify({
        "message": f"Cancelled {count} backfill jobs",
        "queue": queue_name,
        "count": count,
    })
