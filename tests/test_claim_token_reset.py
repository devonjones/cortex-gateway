"""Every UPDATE that returns a queue row to 'pending' must release the claim.

A row put back on the pending pile still carrying claimed_at/claimed_by is a row
certified to a worker that no longer holds it. It bites when the next claimant
sets status='processing' WITHOUT writing its own token -- which is exactly what
every cortex worker does today, since they hand-write their claim SQL -- because
then the stale worker's complete() still matches on claimed_by and retires a job
someone else is mid-flight on.

(Through the cortex_utils primitives alone the window is closed: ops.claim()
overwrites the token, ops.complete() also requires status='processing', and
ops.fail_or_retry() clears both columns when it retires a row. So this guard is
about the partially-ported fleet, not about the primitives.)

## Why this test is shaped the way it is

The first version enumerated two filenames and matched with an unbounded
lookahead. Both choices were wrong in the same way the audit that missed this
repo was wrong -- they described where the problem had been found rather than
where it could be:

* enumerating files meant a new blueprint with a retry endpoint was not scanned
  at all. It now globs.
* `.*?(?=WHERE)` ran past the end of its own statement. A WHERE-less UPDATE --
  'retry everything on this queue', the most dangerous shape there is -- matched
  forward into the NEXT statement and borrowed its `claimed_by = NULL`,
  defeating the content assertion and the count assertion at once. The SET list
  is now bounded by the first WHERE/RETURNING/terminator.
* the pattern was case-sensitive, space-sensitive and alias-blind, so eight
  plausible regressions passed silently: `UPDATE queue q SET`, `UPDATE queue AS
  q`, a reordered SET list, `status='pending'`, `status  =  'pending'`,
  lowercase, a newline after UPDATE, and `UPDATE public.queue`.
"""

import re
from pathlib import Path

BLUEPRINTS = Path(__file__).parent.parent / "src" / "gateway" / "blueprints"

# UPDATE [schema.]queue [AS] [alias] SET ... -- case and whitespace tolerant.
_UPDATE_QUEUE_SET = re.compile(
    r"UPDATE\s+(?:\w+\.)?queue\b(?:\s+(?:AS\s+)?(?!SET\b)\w+)?\s+SET\b",
    re.I | re.S,
)
# The SET list ends at the first of these, never at "the next one in the file".
_END_OF_SET = re.compile(r"\bWHERE\b|\bRETURNING\b|;|\"\"\"|\'\'\'", re.I)
_TO_PENDING = re.compile(r"status\s*=\s*\'pending\'", re.I)


def _pending_updates(text: str):
    """Every UPDATE of queue whose SET list makes the row claimable again."""
    for m in _UPDATE_QUEUE_SET.finditer(text):
        rest = text[m.end() :]
        end = _END_OF_SET.search(rest)
        set_list = rest[: end.start()] if end else rest
        if _TO_PENDING.search(set_list):
            yield text[: m.start()].count("\n") + 1, set_list


def test_returning_a_row_to_pending_also_releases_the_claim() -> None:
    found = []
    for path in sorted(BLUEPRINTS.glob("*.py")):
        for line, set_list in _pending_updates(path.read_text()):
            found.append((path.name, line))
            assert "claimed_by" in set_list and "claimed_at" in set_list, (
                f"{path.name}:{line} returns a row to pending without releasing "
                f"the claim:\n{set_list.strip()}"
            )

    # A total, not a per-file map: a new blueprint is covered by the glob above,
    # and this only has to notice that the number moved.
    assert len(found) == 2, (
        f"expected 2 queue->pending UPDATEs across blueprints/, found {len(found)}: "
        f"{found}. A new one needs the same guard; if it has it, update this count."
    )
