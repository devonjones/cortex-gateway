"""Sync API endpoints for Gmail historical sync operations.

These endpoints trigger backfill of emails FROM Gmail API into the pipeline.
This is distinct from /backfill/* which re-enqueues existing emails to worker queues.

Flow: CLI/Gateway -> backfill_jobs table -> gmail-sync polls and executes
"""

from datetime import datetime, timedelta

from flask import Blueprint, Response, jsonify, request

from gateway.services import postgres

sync_bp = Blueprint("sync", __name__)


@sync_bp.route("/backfill", methods=["POST"])
def trigger_sync_backfill():
    """Trigger a Gmail API backfill to fetch historical emails.

    Request body:
        days: Number of days to backfill (mutually exclusive with after)
        after: ISO date string YYYY-MM-DD to backfill from (mutually exclusive with days)

    Returns:
        Job details including id for status tracking.
    """
    data = request.get_json() or {}

    days = data.get("days")
    after = data.get("after")

    if days and after:
        return jsonify({"error": "Provide either 'days' or 'after', not both"}), 400

    if not days and not after:
        return jsonify({"error": "Provide either 'days' or 'after' parameter"}), 400

    # Validate and build query string
    if days:
        if not isinstance(days, int) or days < 1:
            return jsonify({"error": "days must be a positive integer"}), 400
        after_date = (datetime.utcnow() - timedelta(days=days)).date()
        query = f"after:{after_date.strftime('%Y/%m/%d')}"
    else:
        try:
            # after is guaranteed to be str here (not None) due to the earlier check
            after_date = datetime.strptime(str(after), "%Y-%m-%d").date()
            query = f"after:{after_date.strftime('%Y/%m/%d')}"
        except ValueError:
            return (
                jsonify({"error": f"Invalid date format: '{after}'. Expected YYYY-MM-DD"}),
                400,
            )

    # Insert job into backfill_jobs table
    insert_query = """
        INSERT INTO backfill_jobs (query, days, after_date)
        VALUES (%s, %s, %s)
        RETURNING id, status, query, days, after_date, created_at
    """
    results = postgres.execute_query(insert_query, (query, days, after_date.isoformat()))

    if not results:
        return jsonify({"error": "Failed to create backfill job"}), 500

    job = results[0]
    return (
        jsonify(
            {
                "id": job["id"],
                "status": job["status"],
                "query": job["query"],
                "days": job["days"],
                "after_date": str(job["after_date"]) if job["after_date"] else None,
                "created_at": job["created_at"].isoformat() if job["created_at"] else None,
            }
        ),
        201,
    )


@sync_bp.route("/backfill", methods=["GET"])
def list_sync_backfill_jobs():
    """List recent backfill jobs.

    Query params:
        limit: Max jobs to return (default 20)
        status: Filter by status (pending, running, completed, cancelled, failed)
    """
    limit = request.args.get("limit", 20, type=int)
    status = request.args.get("status")

    query = """
        SELECT id, status, query, days, after_date, processed, stored, updated,
               error, created_at, started_at, completed_at
        FROM backfill_jobs
    """
    params: list[str | int] = []

    if status:
        query += " WHERE status = %s"
        params.append(status)

    query += " ORDER BY created_at DESC LIMIT %s"
    params.append(limit)

    results = postgres.execute_query(query, tuple(params))

    jobs = []
    for row in results:
        jobs.append(
            {
                "id": row["id"],
                "status": row["status"],
                "query": row["query"],
                "days": row["days"],
                "after_date": str(row["after_date"]) if row["after_date"] else None,
                "processed": row["processed"],
                "stored": row["stored"],
                "updated": row["updated"],
                "error": row["error"],
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                "started_at": row["started_at"].isoformat() if row["started_at"] else None,
                "completed_at": row["completed_at"].isoformat() if row["completed_at"] else None,
            }
        )

    return jsonify({"jobs": jobs})


@sync_bp.route("/backfill/<job_id>", methods=["GET"])
def get_sync_backfill_job(job_id: str) -> Response | tuple[Response, int]:
    """Get status of a specific backfill job."""
    query = """
        SELECT id, status, query, days, after_date, processed, stored, updated,
               error, created_at, started_at, completed_at
        FROM backfill_jobs
        WHERE id = %s
    """
    results = postgres.execute_query(query, (job_id,))

    if not results:
        return jsonify({"error": "Job not found"}), 404

    row = results[0]
    return jsonify(
        {
            "id": row["id"],
            "status": row["status"],
            "query": row["query"],
            "days": row["days"],
            "after_date": str(row["after_date"]) if row["after_date"] else None,
            "processed": row["processed"],
            "stored": row["stored"],
            "updated": row["updated"],
            "error": row["error"],
            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            "started_at": row["started_at"].isoformat() if row["started_at"] else None,
            "completed_at": row["completed_at"].isoformat() if row["completed_at"] else None,
        }
    )


@sync_bp.route("/backfill/<job_id>/cancel", methods=["POST"])
def cancel_sync_backfill_job(job_id: str) -> Response | tuple[Response, int]:
    """Cancel a pending or running backfill job.

    Running jobs will stop at the next page boundary.
    """
    # Only cancel pending or running jobs
    update_query = """
        UPDATE backfill_jobs
        SET status = 'cancelled'
        WHERE id = %s AND status IN ('pending', 'running')
        RETURNING id, status
    """
    results = postgres.execute_query(update_query, (job_id,))

    if not results:
        # Check if job exists but wasn't cancellable
        check_query = "SELECT id, status FROM backfill_jobs WHERE id = %s"
        check_results = postgres.execute_query(check_query, (job_id,))

        if not check_results:
            return jsonify({"error": "Job not found"}), 404

        current_status = check_results[0]["status"]
        return (
            jsonify(
                {
                    "error": f"Job cannot be cancelled (status: {current_status})",
                    "id": job_id,
                    "status": current_status,
                }
            ),
            400,
        )

    return jsonify({"id": job_id, "status": "cancelled"})
