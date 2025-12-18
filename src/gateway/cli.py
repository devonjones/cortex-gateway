"""Cortex Gateway CLI - Command-line interface for the Cortex API."""

import json
import os
import sys
from typing import Any

import click
import httpx

# Default gateway URL
DEFAULT_GATEWAY_URL = os.environ.get("CORTEX_GATEWAY_URL", "http://10.5.2.21:8097")


def get_client() -> httpx.Client:
    """Get HTTP client for gateway."""
    return httpx.Client(base_url=DEFAULT_GATEWAY_URL, timeout=30.0)


def output_json(data: Any) -> None:
    """Pretty-print JSON output."""
    click.echo(json.dumps(data, indent=2, default=str))


def output_table(rows: list[dict[str, Any]], columns: list[str]) -> None:
    """Output data as a simple table."""
    if not rows:
        click.echo("No results")
        return

    # Calculate column widths
    widths = {col: len(col) for col in columns}
    for row in rows:
        for col in columns:
            val = str(row.get(col, ""))
            widths[col] = max(widths[col], min(len(val), 60))

    # Header
    header = " | ".join(col.ljust(widths[col]) for col in columns)
    click.echo(header)
    click.echo("-" * len(header))

    # Rows
    for row in rows:
        line = " | ".join(str(row.get(col, ""))[:60].ljust(widths[col]) for col in columns)
        click.echo(line)


@click.group()
@click.option("--url", envvar="CORTEX_GATEWAY_URL", default=DEFAULT_GATEWAY_URL, help="Gateway URL")
@click.option("--json-output", "-j", is_flag=True, help="Output raw JSON")
@click.pass_context
def cli(ctx: click.Context, url: str, json_output: bool) -> None:
    """Cortex Gateway CLI - Manage email pipeline."""
    ctx.ensure_object(dict)
    ctx.obj["url"] = url
    ctx.obj["json"] = json_output


# =============================================================================
# Emails commands
# =============================================================================


@cli.group()
def emails() -> None:
    """Email operations."""
    pass


@emails.command("list")
@click.option("--limit", "-n", default=20, help="Number of emails")
@click.option("--offset", default=0, help="Pagination offset")
@click.option("--label", "-l", help="Filter by Gmail label ID")
@click.pass_context
def emails_list(ctx: click.Context, limit: int, offset: int, label: str | None) -> None:
    """List emails."""
    with get_client() as client:
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if label:
            params["label"] = label
        resp = client.get("/emails/", params=params)
        data = resp.json()

    if ctx.obj["json"]:
        output_json(data)
    else:
        output_table(data["emails"], ["gmail_id", "from_addr", "subject", "date_header"])


@emails.command("get")
@click.argument("gmail_id")
@click.pass_context
def emails_get(ctx: click.Context, gmail_id: str) -> None:
    """Get email details."""
    with get_client() as client:
        resp = client.get(f"/emails/{gmail_id}")
        data = resp.json()

    output_json(data)


@emails.command("body")
@click.argument("gmail_id")
@click.pass_context
def emails_body(ctx: click.Context, gmail_id: str) -> None:
    """Get email body."""
    with get_client() as client:
        resp = client.get(f"/emails/{gmail_id}/body")
        data = resp.json()

    if "error" in data:
        click.echo(f"Error: {data['error']}", err=True)
        sys.exit(1)
    output_json(data)


@emails.command("text")
@click.argument("gmail_id")
@click.pass_context
def emails_text(ctx: click.Context, gmail_id: str) -> None:
    """Get email plain text."""
    with get_client() as client:
        resp = client.get(f"/emails/{gmail_id}/text")
        data = resp.json()

    if "error" in data:
        click.echo(f"Error: {data['error']}", err=True)
        sys.exit(1)

    if ctx.obj["json"]:
        output_json(data)
    else:
        click.echo(data.get("text", ""))


@emails.command("stats")
@click.pass_context
def emails_stats(ctx: click.Context) -> None:
    """Get email statistics."""
    with get_client() as client:
        resp = client.get("/emails/stats")
        data = resp.json()

    output_json(data)


@emails.command("by-label")
@click.argument("label_id")
@click.option("--limit", "-n", default=50, help="Number of emails")
@click.option("--offset", default=0, help="Pagination offset")
@click.pass_context
def emails_by_label(ctx: click.Context, label_id: str, limit: int, offset: int) -> None:
    """Get emails with a Gmail label ID."""
    with get_client() as client:
        resp = client.get(f"/emails/by-label/{label_id}", params={"limit": limit, "offset": offset})
        data = resp.json()

    if ctx.obj["json"]:
        output_json(data)
    else:
        if data.get("label"):
            click.echo(f"Label: {data['label'].get('name', label_id)}")
            click.echo()
        output_table(data["emails"], ["gmail_id", "from_addr", "subject"])


@emails.command("sender")
@click.argument("from_addr")
@click.pass_context
def emails_sender_classifications(ctx: click.Context, from_addr: str) -> None:
    """Get classification breakdown for a sender."""
    with get_client() as client:
        resp = client.get(f"/emails/sender/{from_addr}/classifications")
        data = resp.json()

    if ctx.obj["json"]:
        output_json(data)
    else:
        click.echo(f"Sender: {data['from_addr']}")
        click.echo(f"Total classifications: {data['total']}")
        click.echo()
        output_table(data["classifications"], ["label", "count"])


@emails.command("distribution")
@click.option("--limit", "-n", default=50, help="Number of labels")
@click.pass_context
def emails_distribution(ctx: click.Context, limit: int) -> None:
    """Get classification distribution by label."""
    with get_client() as client:
        resp = client.get("/emails/classifications/distribution", params={"limit": limit})
        data = resp.json()

    if ctx.obj["json"]:
        output_json(data)
    else:
        output_table(data["labels"], ["label", "count"])


@emails.command("uncategorized")
@click.option("--limit", "-n", default=20, help="Number of senders")
@click.pass_context
def emails_uncategorized(ctx: click.Context, limit: int) -> None:
    """Get top senders only in Uncategorized (missing rules)."""
    with get_client() as client:
        resp = client.get("/emails/uncategorized/top-senders", params={"limit": limit})
        data = resp.json()

    if ctx.obj["json"]:
        output_json(data)
    else:
        output_table(data["senders"], ["from_addr", "count"])


# =============================================================================
# Queue commands
# =============================================================================


@cli.group()
def queue() -> None:
    """Queue operations."""
    pass


@queue.command("stats")
@click.pass_context
def queue_stats(ctx: click.Context) -> None:
    """Get queue depths by name and status."""
    with get_client() as client:
        resp = client.get("/queue/stats")
        data = resp.json()

    if ctx.obj["json"]:
        output_json(data)
    else:
        for queue_name, statuses in data.get("queues", {}).items():
            click.echo(f"{queue_name}:")
            for status, count in statuses.items():
                click.echo(f"  {status}: {count}")


@queue.command("failed")
@click.option("--queue", "-q", "queue_name", help="Filter by queue name")
@click.option("--limit", "-n", default=20, help="Number of jobs")
@click.pass_context
def queue_failed(ctx: click.Context, queue_name: str | None, limit: int) -> None:
    """List failed jobs."""
    with get_client() as client:
        params: dict[str, Any] = {"limit": limit}
        if queue_name:
            params["queue"] = queue_name
        resp = client.get("/queue/failed", params=params)
        data = resp.json()

    if ctx.obj["json"]:
        output_json(data)
    else:
        output_table(data["failed_jobs"], ["id", "queue_name", "gmail_id", "error", "attempts"])


@queue.command("retry")
@click.argument("job_id", type=int)
@click.pass_context
def queue_retry(ctx: click.Context, job_id: int) -> None:
    """Retry a failed job."""
    with get_client() as client:
        resp = client.post(f"/queue/failed/{job_id}/retry")
        data = resp.json()

    if resp.status_code != 200:
        click.echo(f"Error: {data.get('error', 'Unknown error')}", err=True)
        sys.exit(1)

    click.echo(f"Retried job {job_id} on queue {data.get('queue_name')}")


@queue.command("delete")
@click.argument("job_id", type=int)
@click.pass_context
def queue_delete(ctx: click.Context, job_id: int) -> None:
    """Delete a failed job."""
    with get_client() as client:
        resp = client.delete(f"/queue/failed/{job_id}")
        data = resp.json()

    if resp.status_code != 200:
        click.echo(f"Error: {data.get('error', 'Unknown error')}", err=True)
        sys.exit(1)

    click.echo(f"Deleted job {job_id}")


@queue.command("retry-all")
@click.argument("queue_name")
@click.pass_context
def queue_retry_all(ctx: click.Context, queue_name: str) -> None:
    """Retry all failed jobs for a queue."""
    with get_client() as client:
        resp = client.post("/queue/failed/retry-all", params={"queue": queue_name})
        data = resp.json()

    if resp.status_code != 200:
        click.echo(f"Error: {data.get('error', 'Unknown error')}", err=True)
        sys.exit(1)

    click.echo(f"Retried {data.get('count', 0)} failed jobs on queue {queue_name}")


# =============================================================================
# Backfill commands (re-enqueue existing emails to worker queues)
# =============================================================================


@cli.group()
def backfill() -> None:
    """Backfill operations (re-enqueue existing emails)."""
    pass


@backfill.command("trigger")
@click.option("--queue", "-q", "queue_name", default="triage", help="Target queue")
@click.option("--days", "-d", default=7, help="Days to backfill")
@click.option("--label", "-l", help="Label filter")
@click.option("--priority", "-p", default=-100, help="Queue priority")
@click.pass_context
def backfill_trigger(
    ctx: click.Context, queue_name: str, days: int, label: str | None, priority: int
) -> None:
    """Trigger backfill of emails to a worker queue."""
    with get_client() as client:
        payload = {"queue": queue_name, "days": days, "priority": priority}
        if label:
            payload["label"] = label
        resp = client.post("/backfill/", json=payload)
        data = resp.json()

    if ctx.obj["json"]:
        output_json(data)
    else:
        click.echo(f"Enqueued {data.get('count', 0)} emails for {queue_name} backfill")


@backfill.command("status")
@click.pass_context
def backfill_status(ctx: click.Context) -> None:
    """Get backfill job status."""
    with get_client() as client:
        resp = client.get("/backfill/status")
        data = resp.json()

    output_json(data)


@backfill.command("cancel")
@click.argument("queue_name")
@click.pass_context
def backfill_cancel(ctx: click.Context, queue_name: str) -> None:
    """Cancel pending backfill jobs."""
    with get_client() as client:
        resp = client.post("/backfill/cancel", json={"queue": queue_name})
        data = resp.json()

    click.echo(f"Cancelled {data.get('count', 0)} backfill jobs")


# =============================================================================
# Triage commands
# =============================================================================


@cli.group()
def triage() -> None:
    """Triage operations."""
    pass


@triage.command("stats")
@click.pass_context
def triage_stats(ctx: click.Context) -> None:
    """Get classification statistics."""
    with get_client() as client:
        resp = client.get("/triage/stats")
        data = resp.json()

    output_json(data)


@triage.command("rerun")
@click.option("--gmail-id", "-i", multiple=True, help="Specific Gmail IDs")
@click.option("--label", "-l", help="Label filter")
@click.option("--days", "-d", default=7, help="Days to look back")
@click.option("--force", "-f", is_flag=True, help="Force rerun even if pending")
@click.option("--priority", "-p", default=-100, help="Queue priority")
@click.pass_context
def triage_rerun(
    ctx: click.Context,
    gmail_id: tuple[str, ...],
    label: str | None,
    days: int,
    force: bool,
    priority: int,
) -> None:
    """Re-run triage on emails."""
    payload: dict[str, Any] = {"days": days, "force": force, "priority": priority}
    if gmail_id:
        payload["gmail_ids"] = list(gmail_id)
    if label:
        payload["label"] = label

    if not gmail_id and not label:
        click.echo("Error: Must specify --gmail-id or --label", err=True)
        sys.exit(1)

    with get_client() as client:
        resp = client.post("/triage/rerun", json=payload)
        data = resp.json()

    if resp.status_code != 200:
        click.echo(f"Error: {data.get('error', 'Unknown error')}", err=True)
        sys.exit(1)

    if ctx.obj["json"]:
        output_json(data)
    else:
        click.echo(f"Enqueued {data.get('count', 0)} emails for triage rerun")


@triage.command("list")
@click.option("--limit", "-n", default=20, help="Number of classifications")
@click.option("--label", "-l", help="Filter by label")
@click.pass_context
def triage_list(ctx: click.Context, limit: int, label: str | None) -> None:
    """List recent classifications."""
    with get_client() as client:
        params: dict[str, Any] = {"limit": limit}
        if label:
            params["label"] = label
        resp = client.get("/triage/classifications", params=params)
        data = resp.json()

    if ctx.obj["json"]:
        output_json(data)
    else:
        output_table(data["classifications"], ["gmail_id", "label", "matched_rule", "created_at"])


# =============================================================================
# Sync commands (Gmail API backfill)
# =============================================================================


@cli.group()
def sync() -> None:
    """Gmail sync operations (fetch from Gmail API)."""
    pass


@sync.command("backfill")
@click.option("--days", "-d", type=int, help="Days to backfill")
@click.option("--after", "-a", help="Date to backfill from (YYYY-MM-DD)")
@click.pass_context
def sync_backfill(ctx: click.Context, days: int | None, after: str | None) -> None:
    """Trigger Gmail API backfill."""
    if not days and not after:
        click.echo("Error: Must specify --days or --after", err=True)
        sys.exit(1)

    payload: dict[str, Any] = {}
    if days:
        payload["days"] = days
    if after:
        payload["after"] = after

    with get_client() as client:
        resp = client.post("/sync/backfill", json=payload)
        data = resp.json()

    if resp.status_code not in (200, 201):
        click.echo(f"Error: {data.get('error', 'Unknown error')}", err=True)
        sys.exit(1)

    output_json(data)


@sync.command("jobs")
@click.option("--limit", "-n", default=20, help="Number of jobs")
@click.option("--status", "-s", help="Filter by status")
@click.pass_context
def sync_jobs(ctx: click.Context, limit: int, status: str | None) -> None:
    """List sync backfill jobs."""
    with get_client() as client:
        params: dict[str, Any] = {"limit": limit}
        if status:
            params["status"] = status
        resp = client.get("/sync/backfill", params=params)
        data = resp.json()

    if ctx.obj["json"]:
        output_json(data)
    else:
        output_table(data["jobs"], ["id", "status", "query", "processed", "stored"])


@sync.command("job")
@click.argument("job_id")
@click.pass_context
def sync_job(ctx: click.Context, job_id: str) -> None:
    """Get sync backfill job status."""
    with get_client() as client:
        resp = client.get(f"/sync/backfill/{job_id}")
        data = resp.json()

    if resp.status_code != 200:
        click.echo(f"Error: {data.get('error', 'Unknown error')}", err=True)
        sys.exit(1)

    output_json(data)


@sync.command("cancel")
@click.argument("job_id")
@click.pass_context
def sync_cancel(ctx: click.Context, job_id: str) -> None:
    """Cancel a sync backfill job."""
    with get_client() as client:
        resp = client.post(f"/sync/backfill/{job_id}/cancel")
        data = resp.json()

    if resp.status_code != 200:
        click.echo(f"Error: {data.get('error', 'Unknown error')}", err=True)
        sys.exit(1)

    click.echo(f"Cancelled job {job_id}")


# =============================================================================
# Health command
# =============================================================================


@cli.command()
@click.pass_context
def health(ctx: click.Context) -> None:
    """Check gateway health."""
    with get_client() as client:
        try:
            resp = client.get("/health")
            data = resp.json()
            output_json(data)
        except httpx.ConnectError:
            click.echo(f"Error: Cannot connect to gateway at {ctx.obj['url']}", err=True)
            sys.exit(1)


def main() -> None:
    """Entry point."""
    cli(obj={})


if __name__ == "__main__":
    main()
