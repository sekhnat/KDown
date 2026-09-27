#!/usr/bin/env python3
"""Release-evidence fragment writer (release-verification task 5.4).

Every verification lane (CI job, scheduled job, local gate run) records what it
actually did as a JSON fragment. The fragments are merged into
`release/evidence-manifest.json`, which `scripts/release_gate.py` validates
before a production-stable verdict.

Design rules enforced here:

* A lane can never record `passed` without naming the command(s) that ran.
* `unavailable` (tool missing, environment limitation) is a distinct status:
  it never counts as passing; the gate blocks unless a reviewed exception or a
  documented equivalent exists.
* `approves_release` defaults to false: a check must opt in explicitly, so an
  incidental "green" run (for example PR smoke) cannot approve a release.

Usage:
  evidence_io.py record --out FILE --id ID --category CAT --status STATUS \
      [--source TEXT] [--detail TEXT] [--command CMD]... [--artifact PATH]... \
      [--toolchain TEXT] [--limitation TEXT] [--equivalent TEXT] \
      [--duration-s F] [--max-age-days N] [--approves-release] [--commit SHA]
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

SCHEMA_VERSION = 1
VALID_STATUS = ("passed", "failed", "unavailable", "passed_equivalent")


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def resolve_commit(explicit: str | None) -> str:
    for candidate in (explicit, os.environ.get("GITHUB_SHA"), os.environ.get("EVIDENCE_COMMIT")):
        if candidate:
            return candidate
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False, timeout=20
        )
        if out.returncode == 0:
            return out.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pass
    return "unknown"


def build(args: argparse.Namespace) -> dict:
    if args.status not in VALID_STATUS:
        raise SystemExit(f"--status must be one of {', '.join(VALID_STATUS)}")
    if args.status == "passed" and not args.command:
        raise SystemExit("--status passed requires at least one --command: evidence must name what ran")
    if args.status == "passed_equivalent" and not (args.equivalent and args.command):
        raise SystemExit(
            "--status passed_equivalent requires --equivalent and the --command(s) that ran"
        )
    if args.status == "unavailable" and not args.limitation:
        raise SystemExit("--status unavailable requires --limitation describing what could not run")

    fragment = {
        "schema_version": SCHEMA_VERSION,
        "id": args.id,
        "category": args.category,
        "mandatory": not args.advisory,
        "approves_release": bool(args.approves_release),
        "status": args.status,
        "source": args.source or "local",
        "recorded_at": utc_now(),
        "commit": resolve_commit(args.commit),
        "commands": args.command,
        "artifacts": args.artifact,
        "toolchain": args.toolchain or "unknown",
        "limitation": args.limitation,
        "equivalent": args.equivalent,
        "detail": args.detail or "",
        "duration_s": args.duration_s,
        "max_age_days": args.max_age_days,
    }
    return fragment


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("record", nargs="?", help="subcommand (only 'record' exists)")
    parser.add_argument("--out", required=True)
    parser.add_argument("--id", required=True)
    parser.add_argument("--category", required=True)
    parser.add_argument("--status", required=True)
    parser.add_argument("--source")
    parser.add_argument("--detail")
    parser.add_argument("--command", action="append", default=[])
    parser.add_argument("--artifact", action="append", default=[])
    parser.add_argument("--toolchain")
    parser.add_argument("--limitation")
    parser.add_argument("--equivalent")
    parser.add_argument("--duration-s", type=float, default=None, dest="duration_s")
    parser.add_argument("--max-age-days", type=int, default=30, dest="max_age_days")
    parser.add_argument("--approves-release", action="store_true")
    parser.add_argument("--advisory", action="store_true", help="record a non-mandatory check")
    parser.add_argument("--commit")
    args = parser.parse_args(argv)
    if args.record != "record":
        parser.error("expected the 'record' subcommand")

    fragment = build(args)
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(fragment, handle, indent=2, sort_keys=True)
        handle.write("\n")
    print(f"recorded {fragment['id']} status={fragment['status']} -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
