#!/usr/bin/env python3
"""Machine-checkable release gate (reliability-verification task 5.4).

Reads `release/evidence-manifest.json` (gate definitions + recorded evidence
fragments + reviewed exceptions + high-severity defect triage) and decides
whether a production-stable verdict is supported.

Subcommands:
  merge  FRAGMENT_OR_DIR...   merge evidence fragments into the manifest
  check                       print the verdict; non-zero exit unless stable
  status                      human-readable per-gate table
  self-test                   prove, with synthetic manifests, that missing /
                              failed / stale / future / other-commit / missing-revision /
                              non-approving / unavailable / untriaged-defect evidence
                              all block the verdict (and that PR smoke alone never
                              approves release)

Blocking rules (spec: "Layered CI and auditable release evidence"):
  * a required gate with no evidence                -> blocked
  * status `failed`                                 -> blocked (never waivable)
  * status `unavailable`                            -> blocked unless a reviewed
                                                       exception is recorded
  * status `passed`/`passed_equivalent` but the
    evidence does not approve release               -> blocked
  * `passed_equivalent` without a named equivalent  -> blocked
  * older than the gate's `max_age_days`            -> blocked (stale)
  * recorded against a different commit             -> blocked (stale)
  * a production verdict requested without a release
    revision                                         -> blocked (fail closed)
  * freshness/commit checks apply to non-approving
    prerequisite gates too (PR/smoke evidence is
    never satisfied by stale or other-commit data)
  * a high-severity defect without triage and a
    documented mitigation/exception                 -> blocked
A category with no passing gate also blocks: deleting a gate definition cannot
quietly shrink the evidence set.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from datetime import datetime, timedelta, timezone

DEFAULT_MANIFEST = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "release", "evidence-manifest.json"
)
PASSING = ("passed", "passed_equivalent")


def parse_time(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def load_manifest(path: str) -> dict:
    with open(path, encoding="utf-8") as handle:
        manifest = json.load(handle)
    if manifest.get("schema_version") != 1:
        raise SystemExit(f"{path}: unsupported schema_version {manifest.get('schema_version')!r}")
    return manifest


def save_manifest(path: str, manifest: dict) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")


def collect_fragments(targets: list[str]) -> list[str]:
    files: list[str] = []
    for target in targets:
        if os.path.isdir(target):
            files.extend(sorted(glob.glob(os.path.join(target, "**", "*.json"), recursive=True)))
        else:
            files.append(target)
    return files


def merge(manifest: dict, fragments: list[str]) -> int:
    by_id = {entry["id"]: entry for entry in manifest.get("evidence", [])}
    merged = 0
    for path in fragments:
        with open(path, encoding="utf-8") as handle:
            fragment = json.load(handle)
        if "id" not in fragment or "status" not in fragment:
            print(f"skip {path}: not an evidence fragment", file=sys.stderr)
            continue
        current = by_id.get(fragment["id"])
        if current is None or parse_time(fragment["recorded_at"]) >= parse_time(
            current["recorded_at"]
        ):
            by_id[fragment["id"]] = fragment
            merged += 1
    manifest["evidence"] = sorted(by_id.values(), key=lambda entry: entry["id"])
    return merged


def evaluate(manifest: dict, now: datetime, commit: str | None) -> dict:
    gates = {gate["id"]: gate for gate in manifest.get("required_gates", [])}
    evidence = {entry["id"]: entry for entry in manifest.get("evidence", [])}
    exceptions = {exc["id"]: exc for exc in manifest.get("reviewed_exceptions", [])}

    blocked: list[dict] = []
    satisfied: list[str] = []
    approving: list[str] = []
    waived: list[str] = []

    def block(gate_id: str, reason: str) -> None:
        blocked.append({"id": gate_id, "reason": reason})


    # An empty or damaged manifest cannot support a stability claim: with no
    # declared gates there is nothing that could have been verified.
    if not gates or not manifest.get("required_categories"):
        block(
            "manifest",
            "manifest declares no required gates/categories; evidence coverage cannot be established",
        )
    # Production stability is revision-bound: a verdict requested without
    # a candidate revision fails closed instead of skipping the binding
    # check (task 6.1).
    if commit is None:
        block(
            "release-commit",
            "no release revision supplied; production stability is revision-bound",
        )
    for gate_id, gate in gates.items():
        entry = evidence.get(gate_id)
        if entry is None:
            block(gate_id, "no evidence recorded")
            continue
        status = entry.get("status")
        if status == "failed":
            block(gate_id, "evidence status is failed")
            continue
        if status == "unavailable":
            exception = exceptions.get(gate_id)
            if exception is None:
                block(
                    gate_id,
                    f"checker unavailable ({entry.get('limitation') or 'no limitation recorded'}) "
                    "and no reviewed exception",
                )
            elif exception.get("expires_at") and parse_time(exception["expires_at"]) < now:
                block(gate_id, f"reviewed exception expired at {exception['expires_at']}")
            elif not exception.get("review_ref"):
                block(gate_id, "reviewed exception has no review_ref")
            else:
                waived.append(gate_id)
            continue
        if status not in PASSING:
            block(gate_id, f"unknown evidence status {status!r}")
            continue
        if status == "passed_equivalent" and not entry.get("equivalent"):
            block(gate_id, "passed_equivalent without a named equivalent")
            continue
        # Freshness and revision binding are checked for EVERY passing
        # gate — including non-approving prerequisites — before the evidence
        # is categorized as approving or advisory (task 6.1).
        max_age = int(gate.get("max_age_days", 30))
        try:
            recorded = parse_time(entry["recorded_at"])
        except (KeyError, ValueError):
            block(gate_id, "evidence has no usable recorded_at timestamp")
            continue
        age = now - recorded
        if age > timedelta(days=max_age):
            block(gate_id, f"evidence is stale ({age.days}d old, limit {max_age}d)")
            continue
        if age < timedelta(0):
            block(gate_id, "evidence timestamp is in the future")
            continue
        if commit is not None and entry.get("commit") != commit:
            block(
                gate_id,
                f"evidence commit {entry.get('commit')!r} does not match the release commit "
                f"{commit!r}",
            )
            continue
        if not entry.get("approves_release", False):
            if gate.get("approves_release", True):
                block(gate_id, "evidence does not approve release (smoke/advisory only)")
                continue
            # A passing non-approving prerequisite (for example the PR
            # benchmark smoke): fresh and revision-bound, but it never
            # counts as release-approving evidence for its category.
            satisfied.append(gate_id)
            continue
        satisfied.append(gate_id)
        approving.append(gate_id)

    for category in manifest.get("required_categories", []):
        passed_ids = [
            gate_id
            for gate_id in approving
            if gates.get(gate_id, {}).get("category") == category
        ]
        if not passed_ids and category not in {
            gates.get(gate_id, {}).get("category") for gate_id in waived
        }:
            block(f"category:{category}", "no passing evidence for this required category")

    for defect in manifest.get("high_severity_defects", []):
        if defect.get("severity") not in ("high", "critical"):
            continue
        if not defect.get("triaged", False):
            block(
                f"defect:{defect.get('id', 'unknown')}",
                "high-severity defect is not triaged",
            )
        elif not (defect.get("mitigation") or defect.get("exception_ref")):
            block(
                f"defect:{defect.get('id', 'unknown')}",
                "triaged high-severity defect has no mitigation or approved exception",
            )

    stable = not blocked
    return {
        "production_stable": stable,
        "release_commit": commit,
        "checked_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "required_gates": len(gates),
        "satisfied": sorted(satisfied),
        "release_approving": sorted(approving),
        "waived": sorted(waived),
        "blocked": blocked,
        "verdict": (
            "PRODUCTION-STABLE SUPPORTED: every required gate passed"
            if stable
            else f"PRODUCTION STABILITY NOT DECLARED: {len(blocked)} blocking item(s)"
        ),
    }


def cmd_merge(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    fragments = collect_fragments(args.targets)
    if not fragments:
        print("no evidence fragments found", file=sys.stderr)
        return 2
    merged = merge(manifest, fragments)
    save_manifest(args.manifest, manifest)
    print(f"merged {merged} fragment(s) from {len(fragments)} file(s) into {args.manifest}")
    return 0


def cmd_check(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    now = parse_time(args.now) if args.now else datetime.now(timezone.utc)
    verdict = evaluate(manifest, now, args.commit)
    if args.json:
        json.dump(verdict, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
    else:
        print(verdict["verdict"])
        for item in verdict["blocked"]:
            print(f"  blocked: {item['id']}: {item['reason']}")
        for gate_id in verdict["waived"]:
            print(f"  waived (reviewed exception): {gate_id}")
    return 0 if verdict["production_stable"] else 1


def cmd_status(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    now = parse_time(args.now) if args.now else datetime.now(timezone.utc)
    verdict = evaluate(manifest, now, args.commit)
    evidence = {entry["id"]: entry for entry in manifest.get("evidence", [])}
    print(f"{'gate':34} {'status':18} {'category':18} {'age(d)':8}")
    for gate in manifest.get("required_gates", []):
        entry = evidence.get(gate["id"])
        status = entry["status"] if entry else "missing"
        age = "-"
        if entry and entry.get("recorded_at"):
            try:
                age = str((now - parse_time(entry["recorded_at"])).days)
            except ValueError:
                age = "?"
        print(f"{gate['id']:34} {status:18} {gate['category']:18} {age:8}")
    print()
    print(verdict["verdict"])
    for item in verdict["blocked"]:
        print(f"  blocked: {item['id']}: {item['reason']}")
    return 0 if verdict["production_stable"] else 1


def _synthetic(gates: list[dict], evidence: list[dict]) -> dict:
    return {
        "schema_version": 1,
        "required_categories": sorted({gate["category"] for gate in gates}),
        "required_gates": gates,
        "reviewed_exceptions": [],
        "evidence": evidence,
        "high_severity_defects": [],
    }


def _entry(gate_id: str, category: str, **overrides: object) -> dict:
    entry = {
        "id": gate_id,
        "category": category,
        "status": "passed",
        "approves_release": True,
        "recorded_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "commit": "abc123",
        "commands": ["cargo test --locked"],
        "equivalent": None,
        "limitation": None,
    }
    entry.update(overrides)
    return entry


def cmd_self_test(args: argparse.Namespace) -> int:
    now = datetime.now(timezone.utc)
    failures: list[str] = []

    gate = {
        "id": "g",
        "category": "correctness",
        "summary": "synthetic",
        "producer": "self-test",
        "max_age_days": 30,
        "approves_release": True,
    }

    def verdict(evidence: list[dict], **manifest_overrides: object) -> dict:
        manifest = _synthetic([gate], evidence)
        manifest.update(manifest_overrides)
        return evaluate(manifest, now, "abc123")

    def expect_block(label: str, result: dict) -> None:
        if result["production_stable"]:
            failures.append(f"{label}: expected the verdict to be blocked")
        elif not result["blocked"]:
            failures.append(f"{label}: blocked without a reason")

    def expect_stable(label: str, result: dict) -> None:
        if not result["production_stable"]:
            failures.append(f"{label}: expected a stable verdict, got {result['blocked']}")

    expect_stable("all-passing", verdict([_entry("g", "correctness")]))
    expect_block("missing evidence", verdict([]))
    expect_block(
        "failed evidence", verdict([_entry("g", "correctness", status="failed")])
    )
    expect_block(
        "non-approving evidence",
        verdict([_entry("g", "correctness", approves_release=False)]),
    )
    stale = (now - timedelta(days=31)).strftime("%Y-%m-%dT%H:%M:%SZ")
    expect_block("stale evidence", verdict([_entry("g", "correctness", recorded_at=stale)]))
    expect_block(
        "other-commit evidence", verdict([_entry("g", "correctness", commit="def456")])
    )
    expect_block(
        "unavailable evidence",
        verdict([_entry("g", "correctness", status="unavailable", limitation="no miri")]),
    )
    expect_block(
        "unavailable without limitation",
        verdict([_entry("g", "correctness", status="unavailable")]),
    )
    expect_block(
        "equivalent without a named substitute",
        verdict([_entry("g", "correctness", status="passed_equivalent", equivalent=None)]),
    )
    expect_stable(
        "equivalent with a named substitute",
        verdict(
            [
                _entry(
                    "g",
                    "correctness",
                    status="passed_equivalent",
                    equivalent="address sanitizer",
                )
            ]
        ),
    )
    expect_stable(
        "unavailable waived by a reviewed exception",
        verdict(
            [_entry("g", "correctness", status="unavailable", limitation="no miri")],
            reviewed_exceptions=[
                {
                    "id": "g",
                    "review_ref": "docs/regression-triage.md#dynamic-checker-limitations",
                    "reason": "runner limitation",
                    "expires_at": (now + timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
            ],
        ),
    )
    expect_block(
        "expired exception",
        verdict(
            [_entry("g", "correctness", status="unavailable", limitation="no miri")],
            reviewed_exceptions=[
                {
                    "id": "g",
                    "review_ref": "docs/x.md",
                    "reason": "stale waiver",
                    "expires_at": (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
            ],
        ),
    )
    expect_block(
        "exception cannot waive a failure",
        verdict(
            [_entry("g", "correctness", status="failed")],
            reviewed_exceptions=[{"id": "g", "review_ref": "docs/x.md", "reason": "nope"}],
        ),
    )
    expect_block(
        "untriaged high-severity defect",
        verdict(
            [_entry("g", "correctness")],
            high_severity_defects=[{"id": "K-1", "severity": "high", "triaged": False}],
        ),
    )
    expect_block(
        "triaged defect without mitigation",
        verdict(
            [_entry("g", "correctness")],
            high_severity_defects=[{"id": "K-1", "severity": "high", "triaged": True}],
        ),
    )
    expect_stable(
        "triaged defect with mitigation",
        verdict(
            [_entry("g", "correctness")],
            high_severity_defects=[
                {"id": "K-1", "severity": "high", "triaged": True, "mitigation": "fixed in #12"}
            ],
        ),
    )

    # Non-approving prerequisites are freshness- and revision-checked too
    # (task 6.1): a stale or wrong-commit smoke fragment is not satisfied.
    prereq_gates = [
        {
            "id": "approving",
            "category": "correctness",
            "max_age_days": 30,
            "approves_release": True,
            "summary": "approving",
            "producer": "self-test",
        },
        {
            "id": "smoke",
            "category": "correctness",
            "max_age_days": 7,
            "approves_release": False,
            "summary": "smoke",
            "producer": "self-test",
        },
    ]
    for label, overrides in (
        ("stale prerequisite", {"recorded_at": stale}),
        ("wrong-commit prerequisite", {"commit": "def456"}),
    ):
        result = evaluate(
            _synthetic(
                prereq_gates,
                [
                    _entry("approving", "correctness"),
                    _entry("smoke", "correctness", approves_release=False, **overrides),
                ],
            ),
            now,
            "abc123",
        )
        if result["production_stable"]:
            failures.append(f"{label}: expected the verdict to be blocked")
        if "smoke" not in {item["id"] for item in result["blocked"]}:
            failures.append(f"{label}: the prerequisite gate was not reported as blocked")

    # No release revision: production stability fails closed.
    no_commit = evaluate(_synthetic([gate], [_entry("g", "correctness")]), now, None)
    if no_commit["production_stable"]:
        failures.append("missing commit: expected the verdict to be blocked")
    if "release-commit" not in {item["id"] for item in no_commit["blocked"]}:
        failures.append("missing commit: the revision binding was not reported as blocked")

    # Future-dated evidence is not fresh (clock skew or fabrication).
    expect_block(
        "future evidence",
        verdict(
            [
                _entry(
                    "g",
                    "correctness",
                    recorded_at=(now + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                )
            ]
        ),
    )
    # A category with no passing gate blocks even when every listed gate passes:
    # deleting a gate definition must not shrink the evidence set.
    expect_block(
        "required category without a gate",
        verdict([_entry("g", "correctness")], required_categories=["correctness", "performance"]),
    )
    # A non-approving prerequisite gate (definition approves_release false) may
    # pass without approving release, and must not satisfy its category alone.
    prereq_manifest = _synthetic(
        [
            {"id": "smoke", "category": "performance", "max_age_days": 30,
             "approves_release": False, "summary": "smoke", "producer": "self-test"},
            {"id": "loopback", "category": "performance", "max_age_days": 45,
             "approves_release": True, "summary": "loopback", "producer": "self-test"},
        ],
        [_entry("smoke", "performance", approves_release=False)],
    )
    prereq = evaluate(prereq_manifest, now, "abc123")
    blocked_ids = {item["id"] for item in prereq["blocked"]}
    if "smoke" in blocked_ids:
        failures.append("prerequisite gate: a passing non-approving prerequisite was reported as blocked")
    if "loopback" not in blocked_ids:
        failures.append("prerequisite gate: the missing release-approving gate was not blocked")
    if "category:performance" not in blocked_ids:
        failures.append("prerequisite gate: smoke alone satisfied the performance category")

    # PR smoke alone: the definition is mandatory but its evidence never
    # approves a release, so the other performance gates stay blocked.
    smoke_manifest = _synthetic(
        [
            {"id": "performance_loopback", "category": "performance", "max_age_days": 45,
             "approves_release": True, "summary": "loopback", "producer": "self-test"},
            {"id": "performance_wan", "category": "performance", "max_age_days": 45,
             "approves_release": True, "summary": "wan", "producer": "self-test"},
            {"id": "performance_pr_smoke", "category": "performance", "max_age_days": 30,
             "approves_release": False, "summary": "smoke", "producer": "self-test"},
        ],
        [
            _entry("performance_pr_smoke", "performance", approves_release=False),
        ],
    )
    smoke = evaluate(smoke_manifest, now, "abc123")
    if smoke["production_stable"]:
        failures.append("pr-smoke-only: expected the verdict to be blocked")
    blocked_ids = {item["id"] for item in smoke["blocked"]}
    for expected in ("performance_wan", "performance_loopback"):
        if expected not in blocked_ids:
            failures.append(f"pr-smoke-only: {expected} was not reported as blocked")
    if "performance_pr_smoke" in blocked_ids:
        failures.append("pr-smoke-only: a passing prerequisite smoke gate was reported as blocked")
    if "performance_pr_smoke" in smoke["release_approving"]:
        failures.append("pr-smoke-only: smoke evidence was counted as release-approving")

    # Merging must keep the newest fragment per id.
    tmp = os.path.join(args.tmp_dir or "/tmp", "kdown-gate-self-test.json")
    manifest = _synthetic([gate], [])
    older = _entry("g", "correctness", recorded_at=(now - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ"))
    newer = _entry("g", "correctness", recorded_at=now.strftime("%Y-%m-%dT%H:%M:%SZ"))
    for fragment in (older, newer):
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(fragment, handle)
        merge(manifest, [tmp])
    os.unlink(tmp)
    if len(manifest["evidence"]) != 1 or manifest["evidence"][0]["recorded_at"] != newer["recorded_at"]:
        failures.append("merge: did not keep exactly the newest fragment per gate id")

    # A missing manifest section must not crash the gate.
    empty = evaluate({"schema_version": 1}, now, None)
    if empty["production_stable"]:
        failures.append("empty manifest: expected the verdict to be blocked")

    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        print(f"release_gate self-test FAILED ({len(failures)} case(s))")
        return 1
    print("release_gate self-test PASSED")
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--manifest", default=DEFAULT_MANIFEST)

    merge_parser = sub.add_parser("merge", parents=[common])
    merge_parser.add_argument("targets", nargs="+")
    merge_parser.set_defaults(func=cmd_merge)

    for name, func in (("check", cmd_check), ("status", cmd_status)):
        check_parser = sub.add_parser(name, parents=[common])
        check_parser.add_argument(
            "--commit",
            default=None,
            help="release revision to bind a production verdict to; without it the verdict is blocked",
        )
        check_parser.add_argument("--now", default=None, help="override the evaluation time (ISO-8601)")
        check_parser.add_argument("--json", action="store_true")
        check_parser.set_defaults(func=func)

    self_parser = sub.add_parser("self-test")
    self_parser.add_argument("--tmp-dir", default=None)
    self_parser.set_defaults(func=cmd_self_test)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
