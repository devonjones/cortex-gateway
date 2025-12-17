"""Queue management API endpoints."""

from flask import Blueprint, Response, jsonify, request

from gateway.services import postgres

queue_bp = Blueprint("queue", __name__)


@queue_bp.route("/stats")
def queue_stats():
    """Get queue depths by name and status."""
    query = """
        SELECT
            queue_name,
            status,
            COUNT(*) as count
        FROM queue
        GROUP BY queue_name, status
        ORDER BY queue_name, status
    """
    results = postgres.execute_query(query)

    # Reshape into nested structure
    stats: dict[str, dict[str, int]] = {}
    for row in results:
        queue_name = row["queue_name"]
        status = row["status"]
        count = row["count"]
        if queue_name not in stats:
            stats[queue_name] = {}
        stats[queue_name][status] = count

    return jsonify({"queues": stats})


@queue_bp.route("/failed")
def list_failed():
    """List failed jobs (dead letters)."""
    limit = request.args.get("limit", 50, type=int)
    offset = request.args.get("offset", 0, type=int)
    queue_name = request.args.get("queue")

    limit = min(limit, 100)

    query = """
        SELECT
            id,
            queue_name,
            gmail_id,
            payload,
            error,
            attempts,
            created_at,
            updated_at
        FROM queue
        WHERE status = 'failed'
    """
    params: list[str] = []

    if queue_name:
        query += " AND queue_name = %s"
        params.append(queue_name)

    query += " ORDER BY updated_at DESC LIMIT %s OFFSET %s"
    params.extend([str(limit), str(offset)])

    results = postgres.execute_query(query, tuple(params))

    return jsonify(
        {
            "failed_jobs": results,
            "limit": limit,
            "offset": offset,
            "count": len(results),
        }
    )


@queue_bp.route("/failed/<int:job_id>/retry", methods=["POST"])
def retry_failed(job_id: int) -> Response | tuple[Response, int]:
    """Retry a failed job by resetting its status to pending."""
    # First verify it exists and is failed
    check_query = """
        SELECT id, queue_name, gmail_id, status
        FROM queue
        WHERE id = %s
    """
    job = postgres.execute_one(check_query, (job_id,))

    if not job:
        return jsonify({"error": "Job not found"}), 404

    if job["status"] != "failed":
        return jsonify({"error": f"Job is not failed (current status: {job['status']})"}), 400

    # Reset to pending
    update_query = """
        UPDATE queue
        SET status = 'pending', error = NULL, attempts = 0, updated_at = NOW()
        WHERE id = %s
    """
    postgres.execute_update(update_query, (job_id,))

    return jsonify(
        {
            "message": "Job queued for retry",
            "job_id": job_id,
            "queue_name": job["queue_name"],
        }
    )


@queue_bp.route("/failed/<int:job_id>", methods=["DELETE"])
def delete_failed(job_id: int) -> Response | tuple[Response, int]:
    """Delete a failed job."""
    # First verify it exists and is failed
    check_query = """
        SELECT id, queue_name, status
        FROM queue
        WHERE id = %s
    """
    job = postgres.execute_one(check_query, (job_id,))

    if not job:
        return jsonify({"error": "Job not found"}), 404

    if job["status"] != "failed":
        return (
            jsonify({"error": f"Can only delete failed jobs (current status: {job['status']})"}),
            400,
        )

    delete_query = "DELETE FROM queue WHERE id = %s"
    postgres.execute_update(delete_query, (job_id,))

    return jsonify(
        {
            "message": "Job deleted",
            "job_id": job_id,
        }
    )


@queue_bp.route("/failed/retry-all", methods=["POST"])
def retry_all_failed():
    """Retry all failed jobs for a queue."""
    queue_name = request.args.get("queue")
    if not queue_name:
        return jsonify({"error": "queue parameter required"}), 400

    update_query = """
        UPDATE queue
        SET status = 'pending', error = NULL, attempts = 0, updated_at = NOW()
        WHERE status = 'failed' AND queue_name = %s
    """
    count = postgres.execute_update(update_query, (queue_name,))

    return jsonify(
        {
            "message": f"Retried {count} failed jobs",
            "queue_name": queue_name,
            "count": count,
        }
    )
