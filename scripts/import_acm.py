"""Import an ACM Digital Library export without scraping the ACM website.

Accepted inputs are RIS, CSV/TSV and a conservative BibTeX subset.  The
import is append-only and uses the same canonical fields, version clustering
and delayed ``seen_keys`` commit as the API collectors.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
from pathlib import Path
from typing import Any

from common import (
    FIELDS,
    MANIFEST_DIR,
    ROOT,
    STATE,
    commit_seen_keys,
    is_arxiv_doi,
    make_key,
    make_run_id,
    norm_arxiv_id,
    norm_doi,
    norm_text,
    now_iso,
    parse_date_info,
    parse_year_safe,
    read_csv_rows,
    split_authors,
    write_csv,
    write_json,
)
from run_incremental import _new_output_path, _safe_component, merge_dedupe


def _read_text(path: Path) -> str:
    errors = []
    for encoding in ("utf-8-sig", "utf-8", "gb18030", "latin-1"):
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError as exc:
            errors.append(f"{encoding}: {exc}")
    raise ValueError(f"cannot decode {path}: {'; '.join(errors)}")


def _first(record: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = record.get(key)
        if isinstance(value, list):
            if value:
                return value[0]
        elif norm_text(value):
            return value
    return ""


def _normalise_record(record: dict[str, Any], *, source_id: str = "") -> dict[str, Any]:
    title = norm_text(_first(record, "title", "TI", "T1", "article_title"))
    abstract = norm_text(_first(record, "abstract", "AB", "N2", "description"))
    author_value = record.get("authors") or record.get("AU") or record.get("A1") or record.get("author") or ""
    authors = "; ".join(split_authors(author_value))
    venue = norm_text(
        _first(
            record,
            "venue",
            "T2",
            "JF",
            "JO",
            "journal",
            "booktitle",
            "publication_title",
            "container_title",
        )
    )
    publication_date = norm_text(_first(record, "publication_date", "DA", "date", "issued"))
    year_value = _first(record, "publication_year", "PY", "year") or parse_year_safe(publication_date) or ""
    year = parse_year_safe(year_value) or ""
    doi = norm_doi(_first(record, "doi", "DO", "DOI"))
    url = norm_text(_first(record, "url", "UR", "URL", "link"))
    identifier = norm_text(source_id or _first(record, "original_record_id", "ID", "accession", "key"))
    if not identifier:
        identifier = doi or hashlib.sha1((title + "|" + authors + "|" + str(year)).encode("utf-8")).hexdigest()
    type_value = norm_text(_first(record, "type", "TY", "entry_type", "document_type"))
    cited_by_count = norm_text(
        _first(record, "cited_by_count", "citation_count", "citationcount", "citations")
    )
    is_preprint = is_arxiv_doi(doi) or "preprint" in type_value.casefold() or "arxiv" in venue.casefold()
    return {
        "record_key": "",
        "title": title,
        "abstract": abstract,
        "authors": authors,
        "publication_year": year,
        "publication_date": publication_date or (str(year) if year else ""),
        "venue": venue,
        "doi": doi,
        "url": url,
        "type": type_value,
        "cited_by_count": cited_by_count,
        "source_database": "ACM DL",
        "query_id": "ACM.MANUAL",
        "original_record_id": identifier,
        "retrieved_at": now_iso(),
        "is_preprint": "1" if is_preprint else "0",
        "arxiv_id": (
            norm_arxiv_id(doi.rsplit("arxiv.", 1)[-1])
            if is_arxiv_doi(doi)
            else ""
        ),
        "formal_doi": "" if is_arxiv_doi(doi) else doi,
    }


def parse_ris(text: str) -> tuple[list[dict[str, Any]], list[str]]:
    records: list[dict[str, Any]] = []
    issues: list[str] = []
    current: dict[str, Any] = {}
    last_tag = ""

    def finish() -> None:
        nonlocal current, last_tag
        if not current:
            return
        record = _normalise_record(current)
        if not record["title"]:
            issues.append("RIS record missing title")
        records.append(record)
        current = {}
        last_tag = ""

    for line_number, raw_line in enumerate(text.splitlines(), 1):
        line = raw_line.rstrip("\r\n")
        match = re.match(r"^\s*([A-Za-z0-9]{2})\s*-\s?(.*)$", line)
        if not match:
            if line.strip() and current and last_tag:
                continuation = line.strip()
                previous = current.get(last_tag, "")
                if isinstance(previous, list):
                    if previous:
                        previous[-1] = f"{previous[-1]} {continuation}".strip()
                    else:
                        previous.append(continuation)
                else:
                    current[last_tag] = f"{previous} {continuation}".strip()
            elif line.strip():
                issues.append(f"RIS line {line_number} is not a tag")
            continue
        tag, value = match.group(1).upper(), norm_text(match.group(2))
        if tag == "ER":
            finish()
            continue
        if tag == "TY" and current:
            finish()
        if tag in {"AU", "A1", "A2", "KW"}:
            current.setdefault(tag, []).append(value)
        else:
            current[tag] = value
        last_tag = tag
    finish()
    return records, issues


def _csv_value(row: dict[str, Any], names: set[str]) -> Any:
    for key, value in row.items():
        normal = re.sub(r"[^a-z0-9]+", "", str(key).casefold())
        if normal in names and norm_text(value):
            return value
    return ""


def parse_csv_export(text: str, delimiter: str = ",") -> tuple[list[dict[str, Any]], list[str], list[str]]:
    reader = csv.DictReader(io.StringIO(text, newline=""), delimiter=delimiter)
    if not reader.fieldnames:
        return [], ["CSV is missing a header"], []
    normalized_headers = [
        re.sub(r"[^a-z0-9]+", "", norm_text(header).casefold())
        for header in reader.fieldnames
    ]
    duplicate_headers = sorted({
        header
        for header in normalized_headers
        if normalized_headers.count(header) > 1 and header
    })
    known = {
        "title", "articletitle", "ti", "abstract", "description", "authors", "author", "creator",
        "publicationyear", "year", "publicationdate", "date", "venue", "journaltitle", "publicationtitle",
        "booktitle", "containertitle", "doi", "url", "link", "type", "documenttype", "key", "id", "accession",
        "citedby", "citedbycount", "citationcount", "citations",
    }
    unknown_headers = [
        str(header) for header in reader.fieldnames
        if re.sub(r"[^a-z0-9]+", "", str(header).casefold()) not in known
    ]
    records: list[dict[str, Any]] = []
    issues: list[str] = []
    if duplicate_headers:
        issues.append(
            "CSV header has duplicate columns: " + ", ".join(duplicate_headers)
        )
    for index, row in enumerate(reader, 2):
        if None in row:
            issues.append(f"CSV row {index} has extra columns")
        if any(value is None for value in row.values()):
            issues.append(f"CSV row {index} has missing columns")
        raw = {
            "title": _csv_value(row, {"title", "articletitle", "ti"}),
            "abstract": _csv_value(row, {"abstract", "description"}),
            "authors": _csv_value(row, {"authors", "author", "creator"}),
            "publication_year": _csv_value(row, {"publicationyear", "year"}),
            "publication_date": _csv_value(row, {"publicationdate", "date"}),
            "venue": _csv_value(row, {"venue", "journaltitle", "publicationtitle", "booktitle", "containertitle"}),
            "doi": _csv_value(row, {"doi"}),
            "url": _csv_value(row, {"url", "link"}),
            "type": _csv_value(row, {"type", "documenttype"}),
            "cited_by_count": _csv_value(row, {"citedby", "citedbycount", "citationcount", "citations"}),
            "original_record_id": _csv_value(row, {"key", "id", "accession"}),
        }
        record = _normalise_record(raw)
        if not record["title"]:
            issues.append(f"CSV row {index} missing title")
        records.append(record)
    return records, issues, unknown_headers


def parse_bibtex(text: str) -> tuple[list[dict[str, Any]], list[str]]:
    records: list[dict[str, Any]] = []
    issues: list[str] = []
    entry_re = re.compile(r"@(?P<kind>[^\s{]+)\s*\{(?P<key>[^,]+),(?P<body>.*?)\n\s*\}", re.S)
    field_re = re.compile(r"(?P<name>[A-Za-z][A-Za-z0-9_-]*)\s*=\s*(?:\{(?P<brace>.*?)\}|\"(?P<quote>.*?)\"|(?P<bare>[^,\n]+))\s*,?", re.S)
    for match in entry_re.finditer(text):
        fields = {item.group("name").casefold(): norm_text(item.group("brace") or item.group("quote") or item.group("bare") or "") for item in field_re.finditer(match.group("body"))}
        fields["key"] = norm_text(match.group("key"))
        fields["entry_type"] = norm_text(match.group("kind"))
        record = _normalise_record(fields)
        if not record["title"]:
            issues.append(f"BibTeX entry {fields['key']} missing title")
        records.append(record)
    if not records:
        issues.append("no BibTeX entries parsed")
    return records, issues


def parse_export(path: Path) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    text = _read_text(path)
    suffix = path.suffix.casefold()
    if suffix in {".ris", ".rdf"} or re.search(r"^\s*TY\s*-", text, re.M):
        records, issues = parse_ris(text)
        return records, issues, []
    if suffix in {".bib", ".bibtex"} or re.search(r"^\s*@\w+\s*\{", text):
        records, issues = parse_bibtex(text)
        return records, issues, []
    delimiter = "\t" if suffix in {".tsv", ".tab"} else ","
    records, issues, unknown = parse_csv_export(text, delimiter)
    return records, issues, unknown


def run_import(
    input_path: Path,
    *,
    against: Path | None = None,
    output_dir: Path | None = None,
    run_id: str | None = None,
    commit_state: bool = True,
    allow_partial_commit: bool = False,
    allow_unmapped_columns: bool = False,
    state_path: Path | None = None,
) -> dict[str, Any]:
    run_id = run_id or make_run_id("acm")
    output_dir = Path(output_dir) if output_dir else ROOT / "data" / "normalized"
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = (
        MANIFEST_DIR / f"{_safe_component(run_id)}.json"
        if output_dir == ROOT / "data" / "normalized"
        else output_dir / f"{_safe_component(run_id)}.manifest.json"
    )
    if manifest_path.exists():
        raise FileExistsError(f"run manifest already exists: {manifest_path}")
    imported, issues, unknown_headers = parse_export(input_path)
    existing: list[dict[str, Any]] = []
    if against:
        existing = read_csv_rows(Path(against))
    merged, dedupe = merge_dedupe(existing + imported, return_report=True)
    output_path = _new_output_path(output_dir, f"acm_import_{_safe_component(run_id)}")
    write_csv(output_path, merged, FIELDS)
    seen_path = Path(state_path) if state_path else STATE / "seen_keys.txt"
    failures = [{"type": "parse", "message": issue} for issue in issues]
    manifest = {
        "run_id": run_id,
        "mode": "acm_manual_import",
        "input": str(input_path),
        "against": str(against) if against else None,
        "raw_imported": len(imported),
        "raw_rows": len(existing) + len(imported),
        "unique_rows": len(merged),
        "parse_issues": failures,
        "unknown_headers": unknown_headers,
        "run_status": "degraded" if failures or unknown_headers else "ok",
        "dedupe": dedupe,
        "output": str(output_path),
        "state_path": str(seen_path),
        "state_commit_requested": commit_state,
        "allow_partial_commit": allow_partial_commit,
        "allow_unmapped_columns": allow_unmapped_columns,
        "state_commit_reason": (
            "dry_run"
            if not commit_state
            else "unknown_headers"
            if unknown_headers and not allow_unmapped_columns
            else "parse_issues"
            if failures and not allow_partial_commit
            else "pending"
        ),
        "state_committed": False,
    }
    write_json(manifest_path, manifest)
    # An unknown column means that the importer could not establish a
    # complete field mapping.  Never advance seen state in that case: doing
    # so would make the unrecognised data impossible to recover on a retry.
    # ``--allow-partial-commit`` is intentionally limited to parse issues and
    # does not override this schema/data-loss guard.  Unmapped columns can be
    # explicitly acknowledged with ``--allow-unmapped-columns`` after the
    # manifest has been reviewed; they are never silently treated as mapped.
    schema_ok = not unknown_headers or allow_unmapped_columns
    should_commit = commit_state and schema_ok and (not failures or allow_partial_commit)
    if should_commit:
        commit_seen_keys(merged, seen_path)
        manifest["state_committed"] = True
        manifest["state_commit_reason"] = (
            "committed_with_unmapped_columns_and_parse_issues"
            if unknown_headers and failures
            else "committed_with_unmapped_columns"
            if unknown_headers
            else "committed_with_parse_issues"
            if failures
            else "committed"
        )
        write_json(manifest_path, manifest)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Import a local ACM RIS/CSV/BibTeX export.")
    parser.add_argument("input", type=Path)
    parser.add_argument("--against", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--allow-partial-commit", action="store_true")
    parser.add_argument(
        "--allow-unmapped-columns",
        action="store_true",
        help="explicitly acknowledge non-canonical CSV columns after reviewing the manifest",
    )
    args = parser.parse_args()
    if not args.input.exists():
        parser.error(f"input does not exist: {args.input}")
    manifest = run_import(
        args.input,
        against=args.against,
        output_dir=args.output_dir,
        run_id=args.run_id,
        commit_state=not args.dry_run,
        allow_partial_commit=args.allow_partial_commit,
        allow_unmapped_columns=args.allow_unmapped_columns,
    )
    print(json.dumps({"run_id": manifest["run_id"], "imported": manifest["raw_imported"], "unique": manifest["unique_rows"], "issues": len(manifest["parse_issues"]), "run_status": manifest["run_status"], "state_committed": manifest["state_committed"], "state_commit_reason": manifest["state_commit_reason"], "output": manifest["output"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
