from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from common import (
    CFG,
    FIELDS,
    QUERIES,
    ROOT,
    STATE,
    arxiv_variants,
    commit_seen_keys,
    local_today,
    make_run_id,
    parse_date_info,
    query_variants,
    read_csv_rows,
    write_csv,
    write_json,
)
from run_incremental import (
    ARXIV_OAI_SETS,
    SOURCE_KEYS,
    SourceConfigurationError,
    _new_output_path,
    _safe_component,
    _arxiv_api_url,
    _arxiv_oai_url,
    collect,
    ensure_source_preflight,
    merge_dedupe,
    parse_query_selection,
    parse_source_selection,
    provider_runtime_snapshot,
    reset_provider_runtime_state,
    resolve_enabled_sources,
    live_source_probe,
    source_preflight,
)


def _config_date(name: str, fallback: date) -> date:
    parsed, exact = parse_date_info(CFG.get(name, ""))
    return parsed if parsed and exact else fallback


def _parse_year(value: str) -> int:
    try:
        year = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid year: {value}") from exc
    if year < 1900 or year > local_today().year:
        raise argparse.ArgumentTypeError(f"year must be between 1900 and {local_today().year}")
    return year


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _task_plan(
    end_year: int,
    *,
    start_year: int | None,
    query_ids: set[str] | None,
    source_keys: list[str],
) -> list[dict[str, Any]]:
    main_start = _config_date("main_backfill_start", date(2014, 1, 1))
    foundational_start = _config_date(
        "foundational_backfill_start_Q6", date(2000, 1, 1)
    )
    selected = query_ids or set(QUERIES)
    tasks: list[dict[str, Any]] = []
    for qid in QUERIES:
        if qid not in selected:
            continue
        first_year = (
            start_year
            if start_year is not None
            else foundational_start.year if qid == "Q6" else main_start.year
        )
        for year in range(first_year, end_year + 1):
            window_end = date(year, 12, 31)
            if year == local_today().year:
                window_end = local_today()
            for source_key in source_keys:
                tasks.append(
                    {
                        "task_id": f"{qid}:{year}:{source_key}",
                        "query_id": qid,
                        "year": year,
                        "source_key": source_key,
                        "start": date(year, 1, 1),
                        "end": window_end,
                    }
                )
    return tasks


def _checkpoint_dir(output_dir: Path, run_id: str) -> Path:
    if output_dir == ROOT / "data" / "normalized":
        return STATE / "backfill_checkpoints" / _safe_component(run_id)
    return output_dir / f".{_safe_component(run_id)}.checkpoint"


def _load_completed_rows(
    checkpoint: dict[str, Any],
    plan: list[dict[str, Any]],
) -> list[dict[str, str]]:
    completed = set(checkpoint.get("completed_tasks", []))
    task_results = checkpoint.get("task_results", {})
    rows: list[dict[str, str]] = []
    for task in plan:
        task_id = task["task_id"]
        if task_id not in completed:
            continue
        part_value = (task_results.get(task_id) or {}).get("part_path")
        if not part_value:
            raise ValueError(f"checkpoint task has no part file: {task_id}")
        part_path = Path(part_value)
        if not part_path.exists():
            raise FileNotFoundError(f"checkpoint part is missing: {part_path}")
        rows.extend(read_csv_rows(part_path))
    return rows


def run_backfill(
    end_year: int,
    max_per_query_year: int,
    *,
    start_year: int | None = None,
    query_ids: set[str] | None = None,
    include_arxiv: bool = True,
    include_ieee: bool | None = None,
    include_dblp: bool | None = None,
    include_openalex: bool | None = None,
    include_semantic_scholar: bool | None = None,
    run_id: str | None = None,
    output_dir: Path | None = None,
    commit_state: bool = True,
    allow_partial_commit: bool = False,
    strict_dates: bool | None = False,
    state_path: Path | None = None,
    resume: bool = False,
    strict_credentials: bool = True,
    defer_failed_sources: bool = False,
) -> dict[str, Any]:
    if max_per_query_year < 1:
        raise ValueError("max_per_query_year must be positive")
    if end_year < 1900 or end_year > local_today().year:
        raise ValueError(f"end_year must be between 1900 and {local_today().year}")
    if start_year is not None and (start_year < 1900 or start_year > end_year):
        raise ValueError("start_year must be between 1900 and end_year")
    if query_ids is not None and not query_ids:
        raise ValueError("query_ids must not be empty")
    unknown_queries = set(query_ids or set()) - set(QUERIES)
    if unknown_queries:
        raise ValueError(f"unknown query ids: {', '.join(sorted(unknown_queries))}")
    if allow_partial_commit:
        raise ValueError(
            "partial state commits are disabled for resumable backfills"
        )

    enabled_sources = resolve_enabled_sources(
        include_arxiv=include_arxiv,
        include_ieee=include_ieee,
        include_dblp=include_dblp,
        include_openalex=include_openalex,
        include_semantic_scholar=include_semantic_scholar,
    )
    preflight = ensure_source_preflight(
        enabled_sources, strict_credentials=strict_credentials
    )
    reset_provider_runtime_state()
    run_id = run_id or make_run_id("backfill")
    output_dir = Path(output_dir) if output_dir else ROOT / "data" / "normalized"
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = (
        (STATE / "runs" / f"{_safe_component(run_id)}.json")
        if output_dir == ROOT / "data" / "normalized"
        else output_dir / f"{_safe_component(run_id)}.manifest.json"
    )
    checkpoint_dir = _checkpoint_dir(output_dir, run_id)
    checkpoint_path = checkpoint_dir / "checkpoint.json"
    parts_dir = checkpoint_dir / "parts"
    seen_path = Path(state_path) if state_path else STATE / "seen_keys.txt"
    main_start = _config_date("main_backfill_start", date(2014, 1, 1))
    foundational_start = _config_date(
        "foundational_backfill_start_Q6", date(2000, 1, 1)
    )
    selected_query_ids = set(query_ids or QUERIES.keys())
    selected_source_keys = [
        key for key in SOURCE_KEYS if enabled_sources.get(key, False)
    ]
    plan = _task_plan(
        end_year,
        start_year=start_year,
        query_ids=selected_query_ids,
        source_keys=selected_source_keys,
    )
    if not plan:
        raise ValueError("the selected year/query range contains no tasks")
    arxiv_tasks = [task for task in plan if task["source_key"] == "arxiv"]
    arxiv_oai_cache_path = (
        checkpoint_dir / "arxiv_oai_cache.sqlite3" if arxiv_tasks else None
    )
    arxiv_oai_start = (
        min(task["start"] for task in arxiv_tasks) if arxiv_tasks else None
    )
    arxiv_oai_set_specs = tuple(
        set_spec
        for set_spec in ARXIV_OAI_SETS
        if set_spec != "cs:cs:AI" or "Q3" in selected_query_ids
    )
    parameters = {
        "start_year": start_year,
        "end_year": end_year,
        "max_per_query_year": max_per_query_year,
        "query_ids": sorted(selected_query_ids),
        "enabled_sources": enabled_sources,
        "strict_dates": strict_dates,
        "state_path": str(seen_path),
        "commit_state": commit_state,
    }

    if resume:
        if not checkpoint_path.exists():
            raise FileNotFoundError(
                f"resume checkpoint does not exist: {checkpoint_path}"
            )
        checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        if checkpoint.get("version") != 2:
            raise ValueError(
                "checkpoint uses the retired query/year task schema; "
                "start a new run id so successful sources can be checkpointed separately"
            )
        if checkpoint.get("parameters") != parameters:
            raise ValueError(
                "resume parameters do not match the existing checkpoint; "
                "use the same years, query ids, sources, limit and state mode"
            )
        checkpoint["resume_count"] = int(checkpoint.get("resume_count", 0)) + 1
    else:
        if manifest_path.exists():
            raise FileExistsError(f"run manifest already exists: {manifest_path}")
        if checkpoint_path.exists():
            raise FileExistsError(
                f"run checkpoint already exists: {checkpoint_path}; use --resume"
            )
        final_output = _new_output_path(output_dir, f"backfill_through_{end_year}")
        partial_output = output_dir / (
            f"backfill_through_{end_year}_{_safe_component(run_id)}.partial.csv"
        )
        checkpoint = {
            "version": 2,
            "task_granularity": "source_query_year",
            "run_id": run_id,
            "status": "running",
            "created_at": _utc_now(),
            "updated_at": _utc_now(),
            "parameters": parameters,
            "total_tasks": len(plan),
            "completed_tasks": [],
            "failed_tasks": {},
            "task_results": {},
            "source_calls": [],
            "attempt_failures": [],
            "resume_count": 0,
            "final_output": str(final_output),
            "partial_output": str(partial_output),
            "state_committed": False,
        }
        write_json(checkpoint_path, checkpoint)

    completed = set(checkpoint.get("completed_tasks", []))
    blocked_sources: set[str] = set()
    deferred_source_keys: set[str] = set()
    failed_this_attempt = False
    for task in plan:
        task_id = task["task_id"]
        if task_id in completed:
            continue
        source_key = task["source_key"]
        if source_key in deferred_source_keys:
            continue
        source_selection = {key: key == source_key for key in SOURCE_KEYS}
        rows, calls, call_failures = collect(
            task["start"],
            task["end"],
            max_per_query_year,
            call_log=[],
            include_arxiv=source_selection["arxiv"],
            include_ieee=source_selection["ieee"],
            include_dblp=source_selection["dblp"],
            include_openalex=source_selection["openalex"],
            include_semantic_scholar=source_selection["semantic_scholar"],
            parent_ids={task["query_id"]},
            include_foundational=True,
            strict_dates=strict_dates,
            arxiv_oai_cache_path=arxiv_oai_cache_path,
            arxiv_oai_cache_start=arxiv_oai_start,
            arxiv_oai_set_specs=arxiv_oai_set_specs,
            blocked_sources=blocked_sources,
            validate_configuration=False,
            stop_on_failure=True,
        )
        checkpoint["source_calls"].extend(calls)
        task_result = {
            "task_id": task_id,
            "query_id": task["query_id"],
            "year": task["year"],
            "source_key": source_key,
            "start": task["start"].isoformat(),
            "end": task["end"].isoformat(),
            "raw_rows": len(rows),
            "failures": len(call_failures),
            "updated_at": _utc_now(),
        }
        if call_failures:
            failed_this_attempt = True
            task_result["status"] = "failed"
            checkpoint["failed_tasks"][task_id] = call_failures
            checkpoint["attempt_failures"].extend(
                {**failure, "task_id": task_id, "attempt": checkpoint["resume_count"]}
                for failure in call_failures
            )
            checkpoint["task_results"][task_id] = task_result
            checkpoint["status"] = "interrupted"
            checkpoint["updated_at"] = _utc_now()
            write_json(checkpoint_path, checkpoint)
            if defer_failed_sources:
                deferred_source_keys.add(source_key)
                print(
                    f"{task_id}: deferred {source_key} after "
                    f"{len(call_failures)} source failure(s); continuing other sources"
                )
                continue
            print(
                f"{task_id}: stopped after {len(call_failures)} source failure(s); "
                f"resume with the same run id"
            )
            break

        part_path = parts_dir / f"{source_key}_{task['query_id']}_{task['year']}.csv"
        write_csv(part_path, rows, FIELDS)
        task_result["status"] = "complete"
        task_result["part_path"] = str(part_path)
        checkpoint["task_results"][task_id] = task_result
        checkpoint["failed_tasks"].pop(task_id, None)
        completed.add(task_id)
        checkpoint["completed_tasks"] = [
            item["task_id"] for item in plan if item["task_id"] in completed
        ]
        checkpoint["updated_at"] = _utc_now()
        write_json(checkpoint_path, checkpoint)
        print(
            f"{task_id}: {len(rows)} accepted records "
            f"({len(completed)}/{len(plan)} tasks complete)"
        )

    allrows = _load_completed_rows(checkpoint, plan)
    merged, dedupe_report = merge_dedupe(allrows, return_report=True)
    date_precision_counts = Counter(
        str(row.get("_date_precision") or "unknown") for row in merged
    )
    all_complete = len(completed) == len(plan) and not checkpoint["failed_tasks"]
    output_path = Path(
        checkpoint["final_output"] if all_complete else checkpoint["partial_output"]
    )
    write_csv(output_path, merged)
    active_failures = [
        failure
        for task_failures in checkpoint["failed_tasks"].values()
        for failure in task_failures
    ]
    checkpoint["status"] = "complete" if all_complete else "interrupted"
    checkpoint["updated_at"] = _utc_now()
    checkpoint["output"] = str(output_path)
    write_json(checkpoint_path, checkpoint)

    should_commit = commit_state and all_complete
    per_year = [
        checkpoint["task_results"][task["task_id"]]
        for task in plan
        if task["task_id"] in checkpoint["task_results"]
    ]
    manifest = {
        "run_id": run_id,
        "mode": "backfill",
        "created_at": checkpoint["created_at"],
        "updated_at": _utc_now(),
        "start_year_override": start_year,
        "end_year": end_year,
        "main_start": main_start.isoformat(),
        "foundational_start_Q6": foundational_start.isoformat(),
        "max_per_query_year": max_per_query_year,
        "selected_query_ids": sorted(selected_query_ids),
        "enabled_sources": enabled_sources,
        "task_granularity": "source_query_year",
        "raw_rows": len(allrows),
        "unique_rows": len(merged),
        "source_failures": active_failures,
        "attempt_failures": checkpoint["attempt_failures"],
        "run_status": "ok" if all_complete else "degraded" if merged else "failed",
        "source_calls": checkpoint["source_calls"],
        "source_preflight": preflight,
        "blocked_sources": sorted(blocked_sources),
        "deferred_sources": sorted(deferred_source_keys),
        "provider_runtime": provider_runtime_snapshot(),
        "per_year": per_year,
        "query_catalog": list(query_variants(parent_ids=selected_query_ids)),
        "provider_query_catalog": {
            source: list(
                query_variants(parent_ids=selected_query_ids, source=source)
            )
            for source in ("OpenAlex", "Semantic Scholar", "DBLP", "IEEE Xplore")
        },
        "arxiv_catalog": list(arxiv_variants(parent_ids=selected_query_ids)),
        "arxiv_api_url": _arxiv_api_url(),
        "arxiv_transport": "OAI-PMH" if arxiv_tasks else "disabled",
        "arxiv_oai_url": _arxiv_oai_url() if arxiv_tasks else None,
        "arxiv_oai_cache": (
            str(arxiv_oai_cache_path) if arxiv_oai_cache_path else None
        ),
        "arxiv_oai_coverage_start": (
            arxiv_oai_start.isoformat() if arxiv_oai_start else None
        ),
        "arxiv_oai_sets": list(arxiv_oai_set_specs) if arxiv_tasks else [],
        "dedupe": dedupe_report,
        "date_precision_counts": dict(date_precision_counts),
        "strict_dates": strict_dates,
        "include_foundational": True,
        "state_path": str(seen_path),
        "state_commit_requested": commit_state,
        "allow_partial_commit": False,
        "state_commit_reason": (
            "source_failures"
            if not all_complete
            else "dry_run" if not commit_state else "pending"
        ),
        "state_committed": bool(checkpoint.get("state_committed", False)),
        "output": str(output_path),
        "final_output": checkpoint["final_output"],
        "checkpoint": str(checkpoint_path),
        "resumable": not all_complete,
        "resume_count": checkpoint["resume_count"],
        "total_tasks": len(plan),
        "completed_tasks": len(completed),
        "remaining_tasks": len(plan) - len(completed),
        "failed_this_attempt": failed_this_attempt,
    }
    write_json(manifest_path, manifest)
    if should_commit:
        commit_seen_keys(merged, seen_path)
        manifest["state_committed"] = True
        manifest["state_commit_reason"] = "committed"
        checkpoint["state_committed"] = True
        checkpoint["updated_at"] = _utc_now()
        write_json(checkpoint_path, checkpoint)
        write_json(manifest_path, manifest)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a year-partitioned literature backfill.")
    parser.add_argument("--start-year", type=_parse_year)
    parser.add_argument("--end-year", type=_parse_year, default=local_today().year)
    parser.add_argument("--max-per-query-year", type=int, default=300)
    parser.add_argument("--query-id", action="append", help="limit to a query family; repeat or use commas")
    parser.add_argument("--sources", help="comma-separated source list: openalex,semantic_scholar,arxiv,ieee,dblp")
    parser.add_argument("--preflight-only", action="store_true", help="validate sources and credentials without network access")
    parser.add_argument("--live-probe-only", action="store_true", help="make one real request per enabled source without writing results or state")
    arxiv_group = parser.add_mutually_exclusive_group()
    arxiv_group.add_argument("--no-arxiv", action="store_true", help="opt out of arXiv backfill")
    # Kept as a compatibility spelling for existing README/automation
    # commands; arXiv is enabled by default for backfill.
    arxiv_group.add_argument("--include-arxiv", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--no-openalex", action="store_true")
    parser.add_argument("--no-semantic-scholar", action="store_true")
    parser.add_argument("--enable-dblp", action="store_true")
    parser.add_argument("--no-dblp", action="store_true")
    parser.add_argument("--enable-ieee", action="store_true")
    parser.add_argument("--no-ieee", action="store_true")
    parser.add_argument("--run-id")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--resume", action="store_true", help="continue an interrupted run using the same run id")
    parser.add_argument(
        "--defer-failed-sources",
        action="store_true",
        help="leave a failed source pending and continue unfinished tasks from other sources",
    )
    parser.add_argument("--allow-partial-commit", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.preflight_only and args.live_probe_only:
        parser.error("--preflight-only and --live-probe-only are mutually exclusive")
    if args.max_per_query_year < 1:
        parser.error("--max-per-query-year must be positive")
    if args.start_year is not None and args.start_year > args.end_year:
        parser.error("--start-year must not be after --end-year")
    if args.resume and not args.run_id:
        parser.error("--resume requires --run-id")
    if args.allow_partial_commit:
        parser.error("partial state commits are disabled for backfills")
    legacy_source_flags = any(
        (
            args.no_arxiv,
            args.no_openalex,
            args.no_semantic_scholar,
            args.enable_dblp,
            args.no_dblp,
            args.enable_ieee,
            args.no_ieee,
        )
    )
    if args.sources and legacy_source_flags:
        parser.error("--sources cannot be combined with --no-* or --enable-* source flags")
    if args.enable_dblp and args.no_dblp:
        parser.error("--enable-dblp and --no-dblp are mutually exclusive")
    if args.enable_ieee and args.no_ieee:
        parser.error("--enable-ieee and --no-ieee are mutually exclusive")
    try:
        selected_sources = parse_source_selection(args.sources) if args.sources else None
        query_ids = parse_query_selection(args.query_id)
    except ValueError as exc:
        parser.error(str(exc))
    if selected_sources is not None:
        source_overrides = {key: key in selected_sources for key in SOURCE_KEYS}
    else:
        source_overrides = {
            "openalex": False if args.no_openalex else None,
            "semantic_scholar": False if args.no_semantic_scholar else None,
            "arxiv": False if args.no_arxiv else True,
            "ieee": False if args.no_ieee else True if args.enable_ieee else None,
            "dblp": False if args.no_dblp else True if args.enable_dblp else None,
        }
    if args.preflight_only:
        enabled = resolve_enabled_sources(
            include_openalex=source_overrides["openalex"],
            include_semantic_scholar=source_overrides["semantic_scholar"],
            include_arxiv=source_overrides["arxiv"],
            include_ieee=source_overrides["ieee"],
            include_dblp=source_overrides["dblp"],
        )
        report = source_preflight(enabled, strict_credentials=True)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        if report["errors"]:
            raise SystemExit(2)
        return
    if args.live_probe_only:
        enabled = resolve_enabled_sources(
            include_openalex=source_overrides["openalex"],
            include_semantic_scholar=source_overrides["semantic_scholar"],
            include_arxiv=source_overrides["arxiv"],
            include_ieee=source_overrides["ieee"],
            include_dblp=source_overrides["dblp"],
        )
        report = live_source_probe(
            enabled,
            strict_credentials=True,
            arxiv_transport="oai",
        )
        print(json.dumps(report, ensure_ascii=False, indent=2))
        if report["status"] != "ok":
            raise SystemExit(2)
        return
    run_id = args.run_id or make_run_id("backfill")
    output_dir = args.output_dir
    if args.dry_run and output_dir is None:
        output_dir = ROOT / "data" / "validation" / run_id
    try:
        manifest = run_backfill(
            args.end_year,
            args.max_per_query_year,
            start_year=args.start_year,
            query_ids=query_ids,
            include_arxiv=source_overrides["arxiv"],
            include_ieee=source_overrides["ieee"],
            include_dblp=source_overrides["dblp"],
            include_openalex=source_overrides["openalex"],
            include_semantic_scholar=source_overrides["semantic_scholar"],
            run_id=run_id,
            output_dir=output_dir,
            commit_state=not args.dry_run,
            resume=args.resume,
            defer_failed_sources=args.defer_failed_sources,
        )
    except (SourceConfigurationError, FileNotFoundError, FileExistsError, ValueError) as exc:
        parser.error(str(exc))
    print(
        json.dumps(
            {
                "run_id": manifest["run_id"],
                "raw": manifest["raw_rows"],
                "unique": manifest["unique_rows"],
                "failures": len(manifest["source_failures"]),
                "run_status": manifest["run_status"],
                "state_committed": manifest["state_committed"],
                "state_commit_reason": manifest["state_commit_reason"],
                "output": manifest["output"],
                "checkpoint": manifest["checkpoint"],
                "completed_tasks": manifest["completed_tasks"],
                "remaining_tasks": manifest["remaining_tasks"],
                "resumable": manifest["resumable"],
                "deferred_sources": manifest["deferred_sources"],
            },
            ensure_ascii=False,
        )
    )
    if manifest["run_status"] != "ok":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
