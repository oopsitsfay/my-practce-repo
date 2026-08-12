#!/usr/bin/env python3
"""Render a run's summary.json as GitHub job-summary markdown.

Kept as a file rather than inlined in the workflow so it can be tested, and so
the YAML does not need a nested heredoc.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def render(path: Path) -> str:
    if not path.exists():
        return (
            "## Wallet filter\n\n"
            "The run did not finish, so there is no split to report.\n\n"
            "Re-run this workflow to resume: every wallet already answered is in the "
            "cached checkpoint and will not be queried again.\n"
        )

    s = json.loads(path.read_text(encoding="utf-8"))
    total = s.get("total", 0)
    share = (s["passed"] / total * 100) if total else 0.0

    lines = [
        "## Wallet filter",
        "",
        "| | |",
        "| --- | --- |",
        f"| **Passed** (>= {s['min_tx']} tx) | **{s['passed']:,}** ({share:.1f}%) |",
        f"| Filtered out | {s['filtered']:,} |",
    ]
    if s.get("errors"):
        lines.append(f"| Unresolved | {s['errors']:,} |")
    lines.append("")

    if s.get("errors"):
        lines += [
            f"> **{s['errors']:,} wallets could not be resolved**, so the split above does not "
            "cover the whole list. Re-run this workflow to retry only those — everything already "
            "answered is kept in the cache.",
            "",
        ]

    lines.append("Download the CSVs from the **Artifacts** section at the bottom of this page.")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    out = Path(sys.argv[1] if len(sys.argv) > 1 else "out/summary.json")
    sys.stdout.write(render(out))
