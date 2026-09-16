"""Sync API endpoints for Gmail historical sync operations.

These endpoints trigger backfill of emails FROM Gmail API into the pipeline.
This is distinct from /backfill/* which re-enqueues existing emails to worker queues.

Flow: CLI/Gateway -> backfill_jobs table -> gmail-sync polls and executes
"""

from datetime import datetime, timedelta
from typing import Any

from flask import Blueprint, Response, jsonify, request

from gateway.services import postgres

sync_bp = Blueprint("sync", __name__)


@sync_bp.route("/backfill", methods=["POST"])
def trigger_sync_backfill():
    """Trigger a Gmail API backfill to fetch historical emails.

    Request body:
        days: Number of days to backfill (mutually exclusive with after)
        after: ISO date string YYYY-MM-DD to backfill from (mutually exclusive with days)
        before: Optional ISO date string YYYY-MM-DD bounding the window.

    Without `before`, the Gmail query is open-ended (`after:X`) and re-scans
    every message from that date to now. For walking history backwards a slice
    at a time, pass both and each job touches only its own window.
    `backfill_jobs.before_date` and GmailSync.backfill() have always supported
    this; only the endpoint did not expose it.

    Returns:
        Job details including id for status tracking.
    """
    data = request.get_json() or {}

    days = data.get("days")
    after = data.get("after")
    before = data.get("before")

    if days and after:
        return jsonify({"error": "Provide either 'days' or 'after', not both"}), 400

    if not days and not after:
        return jsonify({"error": "Provide either 'days' or 'after' parameter"}), 400

    before_date = None
    # `is not None`, not a truthiness test: `before: ""` would otherwise fall
    # through as "no bound" and silently produce the open-ended scan this
    # parameter exists to prevent. A present-but-empty value is a client bug,
    # so let it fail the parse below rather than changing the window's shape.
    if before is not None:
        try:
            before_date = datetime.strptime(str(before), "%Y-%m-%d").date()
        except ValueError:
            return (
                jsonify({"error": f"Invalid date format: '{before}'. Expected YYYY-MM-DD"}),
                400,
            )

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

    if before_date:
        if before_date <= after_date:
            return (
                jsonify({"error": f"before ({before_date}) must be after after ({after_date})"}),
                400,
            )
        query += f" before:{before_date.strftime('%Y/%m/%d')}"

    # Insert job into backfill_jobs table
    insert_query = """
        INSERT INTO backfill_jobs (query, days, after_date, before_date)
        VALUES (%s, %s, %s, %s)
        RETURNING id, status, query, days, after_date, before_date, created_at
    """
    # execute_update_returning, NOT execute_query. execute_query does not
    # commit -- it is for SELECTs -- and ConnectionContext only rolls back on
    # an exception, so the pool discards the INSERT on putconn. RETURNING
    # still hands back a row, so this endpoint answered 201 with a job id for
    # a job that was never written.
    #
    # Found in production 2026-09-16: the nightly walker reported
    # "queued <uuid>" ten times and backfill_jobs never gained a row. Every
    # test here patched execute_query itself, so no test could see it.
    results = postgres.execute_update_returning(
        insert_query,
        (query, days, after_date.isoformat(), before_date.isoformat() if before_date else None),
    )

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
                "before_date": str(job["before_date"]) if job["before_date"] else None,
                "created_at": job["created_at"].isoformat() if job["created_at"] else None,
            }
        ),
        201,
    )


_JOB_COLUMNS = (
    "id, status, query, days, after_date, before_date, processed, "
    "stored, updated, error, created_at, started_at, completed_at"
)


def _serialise_job(row: dict[str, Any]) -> dict[str, Any]:
    """One shape for a backfill job, shared by the list and single-job routes.

    These were two hand-maintained copies of the same serialiser, and they
    drifted in the same direction: both omitted before_date, which the
    cortex-utils backfill walker filters on. Every job therefore looked
    open-ended, the walker's watermark could never advance, and it would have
    re-queued the same month nightly forever while reporting success.

    A second copy of a serialiser is a second place to forget a column, so
    there is now one. The column list is shared for the same reason -- a
    serialiser cannot return what the query never fetched.
    """

    def _date(value: object) -> str | None:
        return str(value) if value else None

    def _stamp(value: object) -> str | None:
        return value.isoformat() if value else None  # type: ignore[attr-defined]

    return {
        "id": row["id"],
        "status": row["status"],
        "query": row["query"],
        "days": row["days"],
        "after_date": _date(row["after_date"]),
        "before_date": _date(row["before_date"]),
        "processed": row["processed"],
        "stored": row["stored"],
        "updated": row["updated"],
        "error": row["error"],
        "created_at": _stamp(row["created_at"]),
        "started_at": _stamp(row["started_at"]),
        "completed_at": _stamp(row["completed_at"]),
    }


@sync_bp.route("/backfill", methods=["GET"])
def list_sync_backfill_jobs():
    """List recent backfill jobs.

    Query params:
        limit: Max jobs to return (default 20)
        status: Filter by status (pending, running, completed, cancelled, failed)
    """
    limit = request.args.get("limit", 20, type=int)
    status = request.args.get("status")

    query = f"""
        SELECT {_JOB_COLUMNS}
        FROM backfill_jobs
    """
    params: list[str | int] = []

    if status:
        query += " WHERE status = %s"
        params.append(status)

    query += " ORDER BY created_at DESC LIMIT %s"
    params.append(limit)

    results = postgres.execute_query(query, tuple(params))

    return jsonify({"jobs": [_serialise_job(row) for row in results]})


@sync_bp.route("/backfill/<job_id>", methods=["GET"])
def get_sync_backfill_job(job_id: str) -> Response | tuple[Response, int]:
    """Get status of a specific backfill job."""
    query = f"""
        SELECT {_JOB_COLUMNS}
        FROM backfill_jobs
        WHERE id = %s
    """
    results = postgres.execute_query(query, (job_id,))

    if not results:
        return jsonify({"error": "Job not found"}), 404

    return jsonify(_serialise_job(results[0]))


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
    # execute_update, not execute_query: execute_query never commits, and
    # ConnectionContext only rolls back on an exception, so psycopg2's pool
    # rolled this back on putconn. The endpoint returned the RETURNING rows and
    # a 200 while persisting nothing.
    results = postgres.execute_update_returning(update_query, (job_id,))

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
