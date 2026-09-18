from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = ROOT / "data" / "state" / "runs"
RECOVERY_CRON = "23 0 * * 2"


def load_settings() -> dict[str, Any]:
    with (ROOT / "config" / "settings.json").open(
        "r", encoding="utf-8-sig"
    ) as handle:
        value = json.load(handle)
    return value if isinstance(value, dict) else {}


def project_today(settings: dict[str, Any]) -> date:
    timezone_name = str(settings.get("timezone") or "UTC")
    try:
        return datetime.now(ZoneInfo(timezone_name)).date()
    except Exception:
        return date.today()


def parse_manifest_date(value: Any) -> date | None:
    text = str(value or "").strip()
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def latest_successful_end(runs_dir: Path) -> date | None:
    latest: date | None = None
    if not runs_dir.is_dir():
        return None
    for path in runs_dir.glob("*.json"):
        try:
            with path.open("r", encoding="utf-8-sig") as handle:
                manifest = json.load(handle)
        except (OSError, UnicodeError, json.JSONDecodeError):
            continue
        if not isinstance(manifest, dict):
            continue
        if str(manifest.get("mode", "")).casefold() != "incremental":
            continue
        if str(manifest.get("run_status", "")).casefold() != "ok":
            continue
        if manifest.get("state_committed") is not True:
            continue
        end = parse_manifest_date(manifest.get("end"))
        if end is not None and (latest is None or end > latest):
            latest = end
    return latest


def decide(
    event_name: str,
    event_schedule: str,
    *,
    today: date | None = None,
    runs_dir: Path = RUNS_DIR,
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    settings = settings or load_settings()
    if event_name != "schedule" or event_schedule != RECOVERY_CRON:
        return {
            "should_run": True,
            "recovery": False,
            "start": "",
            "end": "",
            "reason": "manual or Monday schedule",
        }

    today = today or project_today(settings)
    target_end = today - timedelta(days=1)
    lookback_days = int(settings.get("incremental_lookback_days", 14))
    target_start = target_end - timedelta(days=lookback_days)
    latest_end = latest_successful_end(runs_dir)
    if latest_end is not None and latest_end >= target_end:
        return {
            "should_run": False,
            "recovery": True,
            "start": "",
            "end": "",
            "reason": (
                f"Monday run already committed through {latest_end.isoformat()}"
            ),
        }
    return {
        "should_run": True,
        "recovery": True,
        "start": target_start.isoformat(),
        "end": target_end.isoformat(),
        "reason": (
            f"No committed Monday result through {target_end.isoformat()}; "
            "retrying the same window"
        ),
    }


def write_github_env(path: Path, decision: dict[str, Any]) -> None:
    values = {
        "WEEKLY_SHOULD_RUN": "true" if decision["should_run"] else "false",
        "WEEKLY_RECOVERY": "true" if decision["recovery"] else "false",
        "WEEKLY_START_DATE": decision["start"],
        "WEEKLY_END_DATE": decision["end"],
    }
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        for key, value in values.items():
            handle.write(f"{key}={value}\n")


def write_github_output(path: Path, decision: dict[str, Any]) -> None:
    values = {
        "should_run": "true" if decision["should_run"] else "false",
        "recovery": "true" if decision["recovery"] else "false",
        "start": decision["start"],
        "end": decision["end"],
    }
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        for key, value in values.items():
            handle.write(f"{key}={value}\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Skip a redundant Tuesday recovery when Monday already succeeded."
    )
    parser.add_argument("--event-name", default="")
    parser.add_argument("--event-schedule", default="")
    parser.add_argument("--today", type=date.fromisoformat)
    parser.add_argument("--runs-dir", type=Path, default=RUNS_DIR)
    parser.add_argument("--github-env", type=Path)
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()

    decision = decide(
        args.event_name,
        args.event_schedule,
        today=args.today,
        runs_dir=args.runs_dir,
    )
    if args.github_env is not None:
        write_github_env(args.github_env, decision)
    if args.github_output is not None:
        write_github_output(args.github_output, decision)
    print(json.dumps(decision, ensure_ascii=False))


if __name__ == "__main__":
    main()
