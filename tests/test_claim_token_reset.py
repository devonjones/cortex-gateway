"""Every UPDATE that returns a queue row to 'pending' must release the claim.

A failed row can still carry claimed_at/claimed_by from the attempt that failed
it. Re-queueing it without clearing them hands the next claimant a row still
certified to a previous worker -- and that worker, if it was slow rather than
dead, can then report on a job someone else is mid-flight on. Worse than
carrying no token at all, because the token vouches for the wrong claimant.

Gateway was missed when this was fixed at six sites across postmark and triage:
the audit enumerated those two repos and cortex-utils, and not this one. Hence
the count assertion below -- a new endpoint that re-queues has to declare itself
here rather than being quietly uncovered.

Source assertions rather than live ones; gateway has no Postgres test layer and
building one for two SQL changes isn't proportionate.
"""

import re
from pathlib import Path

import pytest

SRC = Path(__file__).parent.parent / "src" / "gateway" / "blueprints"

# Any UPDATE of queue whose SET list makes the row claimable again. `.*?` and
# not `[^)]*?`: a SET list may legitimately contain parentheses.
TO_PENDING = re.compile(r"UPDATE queue\s+SET status = 'pending'.*?(?=WHERE)", re.S)

EXPECTED = {"queue.py": 2, "backfill.py": 0}


@pytest.mark.parametrize("name,count", sorted(EXPECTED.items()))
def test_returning_a_row_to_pending_also_releases_the_claim(name: str, count: int) -> None:
    matches = TO_PENDING.findall((SRC / name).read_text())
    assert len(matches) == count, (
        f"expected {count} queue->pending UPDATE(s) in {name}, found {len(matches)} "
        "-- a new one needs the same guard, or one has moved"
    )
    for sql in matches:
        assert (
            "claimed_by = NULL" in sql and "claimed_at = NULL" in sql
        ), f"{name} returns a row to pending without releasing the claim:\n{sql}"
