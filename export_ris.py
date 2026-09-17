from __future__ import annotations

import argparse
from pathlib import Path

from common import ROOT, _atomic_write, norm_text, read_csv_rows, split_authors


def esc(value) -> str:
    return norm_text(value)


def ris_type(value: str) -> str:
    text = norm_text(value).casefold()
    if any(token in text for token in ("journal", "article", "periodical")):
        return "JOUR"
    if any(token in text for token in ("conference", "proceedings", "inproceedings", "symposium")):
        return "CONF"
    if any(token in text for token in ("preprint", "eprint", "arxiv")):
        return "EPRINT"
    if "report" in text:
        return "RPRT"
    if any(token in text for token in ("thesis", "dissertation")):
        return "THES"
    return "GEN"


def row_to_ris(row: dict[str, str]) -> str:
    lines = [f"TY  - {ris_type(row.get('type', ''))}", f"TI  - {esc(row.get('title', ''))}"]
    for author in split_authors(row.get("authors", "")):
        lines.append(f"AU  - {esc(author)}")
    if norm_text(row.get("venue", "")):
        lines.append(f"T2  - {esc(row['venue'])}")
    if norm_text(row.get("publication_year", "")):
        lines.append(f"PY  - {esc(row['publication_year'])}")
    if norm_text(row.get("publication_date", "")):
        lines.append(f"DA  - {esc(row['publication_date'])}")
    if norm_text(row.get("doi", "")):
        lines.append(f"DO  - {esc(row['doi'])}")
    if norm_text(row.get("url", "")):
        lines.append(f"UR  - {esc(row['url'])}")
    if norm_text(row.get("abstract", "")):
        lines.append(f"AB  - {esc(row['abstract'])}")
    for field in ("record_key", "source_database", "query_id", "original_record_id", "retrieved_at", "arxiv_id", "formal_doi"):
        value = norm_text(row.get(field, ""))
        if value:
            lines.append(f"N1  - {field}={value}")
    if norm_text(row.get("is_preprint", "")) in {"1", "true", "yes"}:
        lines.append("KW  - source-preprint")
    lines.append("ER  -")
    return "\n".join(lines)


def export(src: Path, out: Path) -> Path:
    if not src.exists():
        raise FileNotFoundError(f"input CSV does not exist: {src}")
    rows = read_csv_rows(src)
    payload = "\n\n".join(row_to_ris(row) for row in rows) + ("\n" if rows else "")
    _atomic_write(out, lambda temp: temp.write_text(payload, encoding="utf-8"))
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Export monitor CSV records to RIS for Zotero.")
    parser.add_argument("--input", type=Path, default=ROOT / "exports" / "latest_new.csv")
    parser.add_argument("--output", type=Path, default=ROOT / "exports" / "latest_new.ris")
    args = parser.parse_args()
    print(export(args.input, args.output))


if __name__ == "__main__":
    main()
