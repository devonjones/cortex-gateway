"""Window validation on POST /sync/backfill.

The endpoint gained a `before` parameter so history can be walked backwards a
slice at a time. Without validation an inverted or malformed window is
accepted and handed to Gmail, where `after:2025/01/01 before:2024/12/01`
matches nothing -- the job "succeeds", ingests zero messages, and the walker
advances its watermark past a month that was never fetched.

That failure is silent, which is the reason these tests exist.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from flask import Flask

from gateway.blueprints import sync_bp


@pytest.fixture
def client():
    app = Flask(__name__)
    app.register_blueprint(sync_bp, url_prefix="/sync")
    return app.test_client()


def _post(client, payload):
    return client.post("/sync/backfill", json=payload)


def test_before_must_be_after_after(client) -> None:
    r = _post(client, {"after": "2025-01-01", "before": "2024-12-01"})
    assert r.status_code == 400
    assert "before" in r.get_json()["error"]


def test_before_equal_to_after_is_rejected(client) -> None:
    """A zero-width window would ingest nothing while looking successful."""
    r = _post(client, {"after": "2025-01-01", "before": "2025-01-01"})
    assert r.status_code == 400


def test_malformed_before_is_rejected(client) -> None:
    r = _post(client, {"after": "2024-12-01", "before": "not-a-date"})
    assert r.status_code == 400
    assert "Invalid date format" in r.get_json()["error"]


def test_days_and_after_are_still_mutually_exclusive(client) -> None:
    r = _post(client, {"days": 7, "after": "2024-12-01"})
    assert r.status_code == 400


def test_one_of_days_or_after_is_still_required(client) -> None:
    assert _post(client, {"before": "2025-01-01"}).status_code == 400
    assert _post(client, {}).status_code == 400


def test_a_valid_window_reaches_the_database_with_before_date(client) -> None:
    """The Gmail query must carry `before:` and the row must persist it.

    Asserting only on the 201 would pass while `before` is parsed and
    dropped -- the shape of bug this parameter exists to avoid.
    """
    captured: dict[str, object] = {}

    def fake_execute_query(query, params):
        captured["query"] = query
        captured["params"] = params
        return [
            {
                "id": "job-1",
                "status": "pending",
                "query": params[0],
                "days": params[1],
                "after_date": params[2],
                "before_date": params[3],
                "created_at": None,
            }
        ]

    with patch("gateway.blueprints.sync.postgres.execute_update_returning", fake_execute_query):
        r = _post(client, {"after": "2024-12-01", "before": "2025-01-02"})

    assert r.status_code == 201
    body = r.get_json()
    assert body["before_date"] == "2025-01-02"
    assert "after:2024/12/01" in body["query"]
    assert "before:2025/01/02" in body["query"], "the Gmail query must bound the window"
    assert captured["params"][3] == "2025-01-02", "before_date must be persisted"


def test_an_open_ended_window_still_works(client) -> None:
    """Omitting `before` keeps the old behaviour for catching up recent mail."""

    def fake_execute_query(query, params):
        return [
            {
                "id": "job-2",
                "status": "pending",
                "query": params[0],
                "days": params[1],
                "after_date": params[2],
                "before_date": params[3],
                "created_at": None,
            }
        ]

    with patch("gateway.blueprints.sync.postgres.execute_update_returning", fake_execute_query):
        r = _post(client, {"after": "2024-12-01"})

    assert r.status_code == 201
    body = r.get_json()
    assert body["before_date"] is None
    assert "before:" not in body["query"]


# --- the `days` + `before` combination: round 4 ------------------------------
#
# `days` and `before` are NOT mutually exclusive -- only `days` and `after`
# are -- so this branch is reachable, and it is the one where the window's
# lower bound is computed rather than supplied. Round 4 review showed it had
# zero coverage: changing the guard to `if before_date and after:` left all
# seven tests green while letting {"days": 7, "before": "2020-01-01"} return
# 201 with an inverted window. That is exactly the silent watermark advance
# this file's docstring exists to prevent.


def test_days_with_an_inverted_before_is_rejected(client) -> None:
    """`days` computes after_date; `before` must still sit after it."""
    r = _post(client, {"days": 7, "before": "2020-01-01"})
    assert r.status_code == 400
    assert "before" in r.get_json()["error"]


def test_days_with_a_valid_before_bounds_the_query(client) -> None:
    from datetime import date, timedelta

    tomorrow = (date.today() + timedelta(days=1)).isoformat()

    def fake_execute_query(query, params):
        return [
            {
                "id": "job-3",
                "status": "pending",
                "query": params[0],
                "days": params[1],
                "after_date": params[2],
                "before_date": params[3],
                "created_at": None,
            }
        ]

    with patch("gateway.blueprints.sync.postgres.execute_update_returning", fake_execute_query):
        r = _post(client, {"days": 7, "before": tomorrow})

    assert r.status_code == 201
    body = r.get_json()
    assert body["before_date"] == tomorrow
    assert "before:" in body["query"], "a days-based window must still honour before"


def test_an_empty_before_is_rejected_rather_than_silently_unbounded(client) -> None:
    """`before: ""` must not degrade into an open-ended scan.

    A falsy check treats present-but-empty as absent, which turns a bounded
    request into the unbounded one the caller was trying to avoid -- silently,
    and reported as success.
    """
    r = _post(client, {"after": "2024-12-01", "before": ""})
    assert r.status_code == 400
    assert "Invalid date format" in r.get_json()["error"]


def test_an_explicit_null_before_is_still_open_ended(client) -> None:
    """JSON null means "no bound", which is a supported request."""

    def fake_execute_query(query, params):
        return [
            {
                "id": "job-4",
                "status": "pending",
                "query": params[0],
                "days": params[1],
                "after_date": params[2],
                "before_date": params[3],
                "created_at": None,
            }
        ]

    with patch("gateway.blueprints.sync.postgres.execute_update_returning", fake_execute_query):
        r = _post(client, {"after": "2024-12-01", "before": None})

    assert r.status_code == 201
    assert r.get_json()["before_date"] is None


# --- GET must return before_date: the round 5 P1 -----------------------------
#
# The backfill walker derives its watermark from COMPLETED WINDOWED jobs --
# those with both an after_date and a before_date. GET /sync/backfill selected
# and serialised neither bound's upper half, so every job came back looking
# open-ended, the walker's filter matched nothing, and the watermark never left
# the seed. It would have re-queued the same month every night, forever,
# reporting success each run.
#
# Verified against the live gateway before the fix: 9 jobs, 9 with after_date,
# 0 with before_date. Nothing in either repo's tests noticed, because the
# walker's own tests synthesise job dicts rather than reading a real payload.
# This is a CONTRACT between two repos, so it is asserted on the producing side.


def _job_row(**over):
    from datetime import date

    row = {
        "id": "job-9",
        "status": "completed",
        "query": "after:2024/12/01 before:2025/01/02",
        "days": None,
        "after_date": date(2024, 12, 1),
        "before_date": date(2025, 1, 2),
        "processed": 10,
        "stored": 10,
        "updated": 0,
        "error": None,
        "created_at": None,
        "started_at": None,
        "completed_at": None,
    }
    row.update(over)
    return row


def test_listing_backfill_jobs_returns_before_date(client) -> None:
    with patch(
        "gateway.blueprints.sync.postgres.execute_query",
        lambda q, p: [_job_row()],
    ):
        r = client.get("/sync/backfill")

    assert r.status_code == 200
    job = r.get_json()["jobs"][0]
    assert job["before_date"] == "2025-01-02", (
        "the walker filters on before_date; without it every job looks "
        "open-ended and the watermark never advances"
    )
    assert job["after_date"] == "2024-12-01"


def test_listing_an_open_ended_job_reports_before_date_as_null(client) -> None:
    """Open-ended jobs must be distinguishable from windowed ones, not absent."""
    with patch(
        "gateway.blueprints.sync.postgres.execute_query",
        lambda q, p: [_job_row(before_date=None)],
    ):
        r = client.get("/sync/backfill")

    job = r.get_json()["jobs"][0]
    assert "before_date" in job, "the key must be present even when null"
    assert job["before_date"] is None


def test_fetching_one_backfill_job_returns_before_date(client) -> None:
    with patch(
        "gateway.blueprints.sync.postgres.execute_query",
        lambda q, p: [_job_row()],
    ):
        r = client.get("/sync/backfill/job-9")

    assert r.status_code == 200
    assert r.get_json()["before_date"] == "2025-01-02"


def test_both_backfill_routes_issue_sql_that_fetches_before_date(client) -> None:
    """Assert on the SQL the routes actually issue, not on the source text.

    A serialiser cannot return a column the query never fetched, and with the
    column list now shared this is the one place the two could still come
    apart. Capturing the executed SQL survives refactoring of how the query is
    built -- the previous version read the inline SELECT out of the source and
    broke the moment the columns moved into a constant, which is exactly the
    brittleness that makes people delete a test.
    """
    seen: list[str] = []

    def capture(query, params):
        seen.append(query)
        return [_job_row()]

    with patch("gateway.blueprints.sync.postgres.execute_query", capture):
        assert client.get("/sync/backfill").status_code == 200
        assert client.get("/sync/backfill/job-9").status_code == 200

    assert len(seen) == 2, "expected the list and single-job queries"
    for sql in seen:
        assert "after_date" in sql, f"query does not fetch after_date: {sql}"
        assert "before_date" in sql, (
            "query fetches after_date without before_date -- a job with only a "
            f"lower bound is what the walker mistakes for open-ended: {sql}"
        )


def test_the_two_routes_share_one_job_serialiser() -> None:
    """Two hand-maintained copies is what let before_date go missing twice.

    Both routes drifted in the same direction because the serialiser was
    duplicated. Pin the single definition so a future edit to one route cannot
    silently diverge from the other.
    """
    import gateway.blueprints.sync as _sync

    assert hasattr(_sync, "_serialise_job")
    assert "before_date" in _sync._JOB_COLUMNS
    assert "after_date" in _sync._JOB_COLUMNS


# --- the insert must COMMIT: the production bug of 2026-09-16 ---------------
#
# POST /sync/backfill used postgres.execute_query for its INSERT ... RETURNING.
# execute_query does not commit -- it is for SELECTs -- and ConnectionContext
# only rolls back on an exception, so the pool discarded the INSERT on
# putconn. RETURNING still produced a row, so the endpoint answered 201 with a
# job id for a job that was never written.
#
# The nightly walker logged "queued <uuid>" ten times against an unchanged
# backfill_jobs table. Nothing caught it because every test above patches the
# very function whose real behaviour was wrong -- mocking the bug out of
# existence. These two tests do not.


def test_the_insert_goes_through_the_committing_helper() -> None:
    """Pin the helper by name, since the two differ only in whether they commit.

    Patching execute_update_returning is itself the guard: route the INSERT
    back through execute_query and this fake is never called, so the test
    fails rather than silently passing on a mock that no longer matches.
    """
    called: dict[str, object] = {}

    def fake(query, params):
        called["query"] = query
        return [
            {
                "id": "job-c",
                "status": "pending",
                "query": params[0],
                "days": params[1],
                "after_date": params[2],
                "before_date": params[3],
                "created_at": None,
            }
        ]

    app = Flask(__name__)
    app.register_blueprint(sync_bp, url_prefix="/sync")
    with patch("gateway.blueprints.sync.postgres.execute_update_returning", fake):
        r = app.test_client().post("/sync/backfill", json={"after": "2024-12-01"})

    assert r.status_code == 201
    assert "INSERT INTO backfill_jobs" in str(called.get("query", "")), (
        "the INSERT must be routed through execute_update_returning, which "
        "commits; execute_query does not, and the pool discards the write"
    )


def test_no_write_in_this_blueprint_uses_the_non_committing_helper() -> None:
    """Static sweep, so a future INSERT/UPDATE cannot regress the same way.

    Checks the pairing directly: any SQL string handed to execute_query must
    not be a write.
    """
    import re
    from pathlib import Path

    import gateway.blueprints.sync as _sync

    source = Path(_sync.__file__).read_text(encoding="utf-8")
    offenders = []
    for m in re.finditer(r"execute_query\(\s*([A-Za-z_]+)\s*,", source):
        var = m.group(1)
        # (?<![A-Za-z_]) or "query" also matches "insert_query = \"\"\"INSERT ...",
        # which reported the POST's own INSERT as a violation of the GET SELECTs.
        assign = re.search(
            rf"(?<![A-Za-z_]){re.escape(var)}\s*=\s*(?:f?\"\"\")(.*?)(?:\"\"\")",
            source,
            re.DOTALL,
        )
        body = (assign.group(1) if assign else "").upper()
        if re.search(r"\b(INSERT|UPDATE|DELETE)\b", body):
            offenders.append(f"{var} (line {source[:m.start()].count(chr(10)) + 1})")

    assert not offenders, (
        f"write(s) routed through the non-committing execute_query: {offenders}. "
        "Use execute_update_returning -- execute_query never commits, so the "
        "pool discards the write while RETURNING still reports success."
    )
