from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
from pathlib import Path
from typing import Any

from common import FIELDS, commit_seen_keys, read_csv_rows


ROOT = Path(__file__).resolve().parents[1]


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"manifest must contain a JSON object: {path}")
    return value


def _csv_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(next(csv.reader(handle), []))


def _single_path(paths: list[Path], label: str) -> Path:
    if len(paths) != 1:
        raise ValueError(f"expected exactly one {label}; found {len(paths)}")
    return paths[0]


def _copy(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def recover_artifact(
    artifact_dir: Path,
    source_run_id: str,
    repository_root: Path = ROOT,
) -> dict[str, Any]:
    source_run_id = str(source_run_id).strip()
    if not re.fullmatch(r"[1-9][0-9]*", source_run_id):
        raise ValueError("source run id must contain digits only")

    artifact_dir = artifact_dir.resolve()
    repository_root = repository_root.resolve()
    archive_run_id = f"weekly-{source_run_id}"
    manifest_source = artifact_dir / "data" / "state" / "runs" / f"{archive_run_id}.json"
    latest_csv_source = artifact_dir / "exports" / "latest_new.csv"
    latest_ris_source = artifact_dir / "exports" / "latest_new.ris"
    normalized_dir = artifact_dir / "data" / "normalized"

    for path in (manifest_source, latest_csv_source, latest_ris_source):
        if not path.is_file():
            raise FileNotFoundError(f"required recovery artifact is missing: {path}")

    manifest = _read_json(manifest_source)
    if str(manifest.get("run_id", "")) != archive_run_id:
        raise ValueError("artifact manifest run_id does not match the requested run")
    if str(manifest.get("mode", "")).casefold() != "incremental":
        raise ValueError("artifact manifest is not an incremental run")
    if str(manifest.get("run_status", "")).casefold() != "ok":
        raise ValueError("artifact run_status is not ok")
    if manifest.get("state_committed") is not True:
        raise ValueError("artifact manifest says retrieval state was not committed")
    if manifest.get("source_failures") not in (None, [], {}):
        raise ValueError("artifact manifest contains source failures")

    all_source = _single_path(
        sorted(normalized_dir.glob(f"candidates_all_*_{archive_run_id}.csv")),
        "all-candidates CSV",
    )
    new_source = _single_path(
        sorted(normalized_dir.glob(f"new_candidates_*_{archive_run_id}.csv")),
        "new-candidates CSV",
    )
    for path in (all_source, new_source, latest_csv_source):
        if _csv_header(path) != FIELDS:
            raise ValueError(f"CSV header does not match the canonical contract: {path}")

    all_rows = read_csv_rows(all_source)
    new_rows = read_csv_rows(new_source)
    latest_rows = read_csv_rows(latest_csv_source)
    expected_unique = int(manifest.get("unique_rows", -1))
    expected_new = int(manifest.get("new_rows", -1))
    if len(all_rows) != expected_unique:
        raise ValueError(
            f"all-candidates row count mismatch: {len(all_rows)} != {expected_unique}"
        )
    if len(new_rows) != expected_new or len(latest_rows) != expected_new:
        raise ValueError(
            "new-candidates row count does not match the manifest or latest export"
        )

    _copy(all_source, repository_root / "data" / "normalized" / all_source.name)
    _copy(new_source, repository_root / "data" / "normalized" / new_source.name)
    _copy(manifest_source, repository_root / "data" / "state" / "runs" / manifest_source.name)
    _copy(latest_csv_source, repository_root / "exports" / "latest_new.csv")
    _copy(latest_ris_source, repository_root / "exports" / "latest_new.ris")
    seen_path = repository_root / "data" / "state" / "seen_keys.txt"
    committed_keys = commit_seen_keys(all_rows, seen_path)

    return {
        "archive_run_id": archive_run_id,
        "unique_rows": len(all_rows),
        "new_rows": len(new_rows),
        "seen_keys": len(committed_keys),
        "manifest": str(repository_root / "data" / "state" / "runs" / manifest_source.name),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Restore a successful weekly run from its GitHub artifact."
    )
    parser.add_argument("--artifact-dir", type=Path, required=True)
    parser.add_argument("--source-run-id", required=True)
    parser.add_argument("--repository-root", type=Path, default=ROOT)
    args = parser.parse_args()
    result = recover_artifact(
        args.artifact_dir,
        args.source_run_id,
        args.repository_root,
    )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
