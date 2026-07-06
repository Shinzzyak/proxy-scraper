#!/usr/bin/env python3
"""publish_snapshot.py — throttled VPS/local snapshot publisher.

This script is intentionally conservative:
- exports final snapshots from the local SQLite pool
- refuses to publish low-quality snapshots
- throttles commits to avoid GitHub spam
- stages only generated snapshot artifacts, never source code
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
STATE_FILE = DATA_DIR / "publish_snapshot_state.json"

ARTIFACT_FILES = [
    "proxies.txt",
    "proxies.json",
    "proxies-by-country.json",
    "proxies-by-protocol.json",
    "proxies-stats.json",
    "source-health.json",
    "report-weekly.md",
    "heatmap.html",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run(cmd: list[str], *, check: bool = True, capture: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
    )
    if check and proc.returncode != 0:
        output = proc.stdout or ""
        raise RuntimeError("command failed ({}): {}\n{}".format(proc.returncode, " ".join(cmd), output))
    return proc


def load_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text())
    except Exception:
        return default
    return default


def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True))


def git_status_paths() -> list[str]:
    proc = run(["git", "status", "--porcelain"], check=True)
    paths: list[str] = []
    for line in (proc.stdout or "").splitlines():
        if not line.strip():
            continue
        # porcelain v1: XY PATH or XY OLD -> NEW
        path = line[3:]
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        paths.append(path)
    return paths


def artifact_dirty(paths: list[str]) -> list[str]:
    artifacts = set(ARTIFACT_FILES)
    return [p for p in paths if p in artifacts]


def non_artifact_dirty(paths: list[str]) -> list[str]:
    artifacts = set(ARTIFACT_FILES)
    return [p for p in paths if p not in artifacts]


def stash_artifacts_if_needed(paths: list[str]) -> bool:
    """Stash artifact changes so git pull can rebase cleanly.

    Freshen often modifies generated artifacts before publishing. We need to pull
    latest code first, but `git pull --rebase` refuses dirty tracked files. The
    artifact stash is discarded after pull, then snapshots are re-exported from DB.
    """
    dirty = artifact_dirty(paths)
    if not dirty:
        return False
    proc = run(["git", "stash", "push", "-m", "publish_snapshot_artifacts", "--", *dirty], check=True)
    out = proc.stdout or ""
    return "No local changes" not in out


def drop_artifact_stash(stashed: bool) -> None:
    if stashed:
        run(["git", "stash", "drop"], check=True)


def load_stats() -> dict[str, Any]:
    stats = load_json(ROOT / "proxies-stats.json", {})
    proxies = load_json(ROOT / "proxies.json", [])
    if isinstance(proxies, list):
        stats.setdefault("total", len(proxies))
    return stats if isinstance(stats, dict) else {}


def load_source_health() -> dict[str, Any]:
    data = load_json(ROOT / "source-health.json", {})
    return data if isinstance(data, dict) else {}


def quality_gate(stats: dict[str, Any], args: argparse.Namespace, state: dict[str, Any]) -> tuple[bool, str]:
    total = int(stats.get("total") or 0)
    countries = len(stats.get("by_country") or {})

    if total < args.min_total:
        return False, "total {} < min_total {}".format(total, args.min_total)
    if countries < args.min_countries:
        return False, "countries {} < min_countries {}".format(countries, args.min_countries)

    last_total = int(state.get("last_published_total") or 0)
    if last_total and not args.force:
        drop_pct = 100.0 * max(0, last_total - total) / max(last_total, 1)
        if drop_pct > args.max_drop_pct:
            return False, "quality drop {:.1f}% > max_drop_pct {:.1f}% ({} -> {})".format(
                drop_pct, args.max_drop_pct, last_total, total
            )

    return True, "ok"


def should_publish(stats: dict[str, Any], args: argparse.Namespace, state: dict[str, Any], changed_artifacts: list[str]) -> tuple[bool, str]:
    if args.force:
        return True, "forced"
    if not changed_artifacts:
        return False, "no artifact changes"

    now = time.time()
    last_epoch = float(state.get("last_published_epoch") or 0)
    min_interval_s = float(args.min_interval_hours) * 3600
    if last_epoch and now - last_epoch < min_interval_s:
        prev_total = int(state.get("last_published_total") or 0)
        current_total = int(stats.get("total") or 0)
        change_pct = 100.0 * abs(current_total - prev_total) / max(prev_total, 1) if prev_total else 100.0
        prev_countries = int(state.get("last_published_countries") or 0)
        current_countries = len(stats.get("by_country") or {})
        if change_pct < args.min_change_pct and current_countries == prev_countries:
            remaining = round((min_interval_s - (now - last_epoch)) / 60, 1)
            return False, "throttled; {:.1f}m remaining and change {:.1f}% < {:.1f}%".format(
                remaining, change_pct, args.min_change_pct
            )

    return True, "changed artifacts: {}".format(", ".join(changed_artifacts))


def export_snapshots(max_age_minutes: int) -> int:
    sys.path.insert(0, str(ROOT))
    from freshen_pool import export_pool_snapshots

    return int(export_pool_snapshots(max_age_minutes=max_age_minutes))


def main() -> int:
    ap = argparse.ArgumentParser(description="Publish high-quality VPS/local proxy snapshots to GitHub")
    ap.add_argument("--dry-run", action="store_true", help="Evaluate gates only; do not export, commit, or push")
    ap.add_argument("--force", action="store_true", help="Bypass throttle and drop guard")
    ap.add_argument("--remote", default="origin")
    ap.add_argument("--branch", default="main")
    ap.add_argument("--export-max-age-minutes", type=int, default=1440)
    ap.add_argument("--min-total", type=int, default=1000)
    ap.add_argument("--min-countries", type=int, default=50)
    ap.add_argument("--min-interval-hours", type=float, default=6.0)
    ap.add_argument("--min-change-pct", type=float, default=5.0)
    ap.add_argument("--max-drop-pct", type=float, default=20.0)
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    state = load_json(STATE_FILE, {})
    if not isinstance(state, dict):
        state = {}
    state["last_attempt_at"] = utc_now()

    try:
        branch = (run(["git", "rev-parse", "--abbrev-ref", "HEAD"]).stdout or "").strip()
        if branch != args.branch:
            print("SKIP: current branch {} != {}".format(branch, args.branch))
            state["last_status"] = "skipped_branch"
            save_json(STATE_FILE, state)
            return 0

        initial_dirty = git_status_paths()
        initial_non_artifacts = non_artifact_dirty(initial_dirty)
        if initial_non_artifacts:
            print("SKIP: non-artifact dirty files present:")
            for path in initial_non_artifacts:
                print("  -", path)
            state["last_status"] = "skipped_dirty_repo"
            save_json(STATE_FILE, state)
            return 0

        if args.dry_run:
            stats = load_stats()
            ok, reason = quality_gate(stats, args, state)
            changed = artifact_dirty(initial_dirty)
            publish, publish_reason = should_publish(stats, args, state, changed)
            print("DRY-RUN")
            print("quality:", ok, reason)
            print("publish:", publish, publish_reason)
            print("stats: total={} countries={}".format(stats.get("total"), len(stats.get("by_country") or {})))
            return 0

        # Sync before generating/staging artifacts. Freshen may have already
        # changed artifacts, so stash those briefly to keep pull/rebase clean.
        artifact_stashed = stash_artifacts_if_needed(initial_dirty)
        run(["git", "fetch", args.remote, args.branch], check=True)
        run(["git", "pull", "--rebase", args.remote, args.branch], check=True)
        drop_artifact_stash(artifact_stashed)

        exported = export_snapshots(args.export_max_age_minutes)
        stats = load_stats()
        health = load_source_health()
        ok, reason = quality_gate(stats, args, state)
        if not ok:
            print("SKIP:", reason)
            state.update({
                "last_status": "skipped_quality",
                "last_skip_reason": reason,
                "last_exported_total": exported,
            })
            save_json(STATE_FILE, state)
            return 0

        paths = git_status_paths()
        non_artifacts = non_artifact_dirty(paths)
        if non_artifacts:
            print("SKIP: non-artifact dirty files after export:")
            for path in non_artifacts:
                print("  -", path)
            state["last_status"] = "skipped_dirty_after_export"
            save_json(STATE_FILE, state)
            return 0

        changed = artifact_dirty(paths)
        publish, publish_reason = should_publish(stats, args, state, changed)
        if not publish:
            print("SKIP:", publish_reason)
            state.update({
                "last_status": "skipped_throttle_or_nochange",
                "last_skip_reason": publish_reason,
                "last_exported_total": exported,
            })
            save_json(STATE_FILE, state)
            return 0

        existing_artifacts = [p for p in ARTIFACT_FILES if (ROOT / p).exists()]
        run(["git", "add", "--", *existing_artifacts], check=True)
        staged = run(["git", "diff", "--cached", "--name-only"], check=True).stdout.splitlines()
        if not staged:
            print("SKIP: no staged artifact changes")
            state["last_status"] = "skipped_no_staged_changes"
            save_json(STATE_FILE, state)
            return 0

        total = int(stats.get("total") or 0)
        countries = len(stats.get("by_country") or {})
        alive_sources = health.get("alive_sources", "?")
        total_sources = health.get("total_sources", "?")
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        message = "chore(snapshot): publish {} proxies [{}]".format(total, timestamp)
        body = "countries={} sources={}/{} max_age_minutes={}".format(
            countries, alive_sources, total_sources, args.export_max_age_minutes
        )
        run(["git", "commit", "-m", message, "-m", body], check=True, capture=False)
        run(["git", "push", args.remote, "HEAD:{}".format(args.branch)], check=True, capture=False)

        state.update({
            "last_status": "published",
            "last_published_at": utc_now(),
            "last_published_epoch": time.time(),
            "last_published_total": total,
            "last_published_countries": countries,
            "last_published_commit": (run(["git", "rev-parse", "--short", "HEAD"]).stdout or "").strip(),
            "last_changed_artifacts": changed,
        })
        save_json(STATE_FILE, state)
        print("PUBLISHED: {} proxies, {} countries".format(total, countries))
        return 0

    except Exception as exc:
        state["last_status"] = "error"
        state["last_error"] = str(exc)
        save_json(STATE_FILE, state)
        print("ERROR:", exc, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
