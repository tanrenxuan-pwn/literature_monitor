from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import tempfile
import unicodedata
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from dateutil.parser import parse as dtparse


ROOT = Path(__file__).resolve().parents[1]
CFG = json.loads((ROOT / "config" / "settings.json").read_text(encoding="utf-8"))
QUERIES = json.loads((ROOT / "config" / "queries.json").read_text(encoding="utf-8"))
STATE = ROOT / "data" / "state"
MANIFEST_DIR = STATE / "runs"

FIELDS = [
    "record_key",
    "title",
    "abstract",
    "authors",
    "publication_year",
    "publication_date",
    "venue",
    "doi",
    "url",
    "type",
    "cited_by_count",
    "source_database",
    "query_id",
    "original_record_id",
    "retrieved_at",
    "is_preprint",
    "arxiv_id",
    "formal_doi",
    "screening_status",
    "screening_label",
    "screening_confidence",
    "screening_reason",
]


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def local_today() -> date:
    """Use the configured project timezone for date windows, not host timezone."""
    try:
        return datetime.now(ZoneInfo(CFG.get("timezone", "UTC"))).date()
    except Exception:
        return date.today()


def make_run_id(prefix: str = "run") -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}-{stamp}"


def norm_text(value: Any) -> str:
    """Normalize Unicode text while retaining non-Latin titles and removing controls."""
    if value is None:
        return ""
    if isinstance(value, (str, bytes)) and not value:
        return ""
    if isinstance(value, (list, tuple, dict, set)) and not value:
        return ""
    # Keep legitimate numeric zero values (citation counts and years are
    # commonly returned as 0) instead of turning them into empty strings.
    text = unicodedata.normalize("NFKC", str(value))
    text = "".join(" " if unicodedata.category(ch).startswith("C") else ch for ch in text)
    return re.sub(r"\s+", " ", text).strip()


def norm_title(value: Any) -> str:
    """Return a punctuation-insensitive, Unicode-safe title key."""
    text = norm_text(value).casefold()
    out = []
    for ch in text:
        if ch.isalnum():
            out.append(ch)
        elif ch.isspace():
            out.append(" ")
        else:
            out.append(" ")
    return re.sub(r"\s+", " ", "".join(out)).strip()


def norm_doi(value: Any) -> str:
    text = norm_text(value).strip("<> ").casefold()
    text = re.sub(r"^(?:https?://)?(?:dx\.)?doi\.org/", "", text)
    text = re.sub(r"^doi\s*:\s*", "", text)
    text = re.sub(r"^https?://doi\.org/", "", text)
    text = text.split("?", 1)[0].split("#", 1)[0]
    return text.strip(" .,;:)]}\"'")


def norm_arxiv_id(value: Any) -> str:
    text = norm_text(value).casefold()
    text = re.sub(r"^(?:https?://)?(?:export\.)?arxiv\.org/(?:abs|pdf)/", "", text)
    text = re.sub(r"^arxiv:\s*", "", text)
    text = text.split("?", 1)[0].split("#", 1)[0].strip(" .,;:)]}")
    text = re.sub(r"\.pdf$", "", text)
    return re.sub(r"v\d+$", "", text)


def is_arxiv_doi(value: Any) -> bool:
    return bool(re.match(r"^10\.48550/arxiv\.", norm_doi(value), re.I))


def parse_year_safe(value: Any) -> int | None:
    text = norm_text(value)
    if not text:
        return None
    match = re.search(r"(?:19|20)\d{2}", text)
    return int(match.group()) if match else None


def row_year(row: dict[str, Any]) -> int | None:
    """Resolve a record year from its explicit year or a publication date."""
    return parse_year_safe(row.get("publication_year", "")) or parse_year_safe(
        row.get("publication_date", "")
    )


def _first_author(value: Any) -> str:
    if isinstance(value, list):
        value = value[0] if value else ""
    text = norm_text(value)
    if not text:
        return ""
    return norm_title(re.split(r"\s*[,;]\s*", text, maxsplit=1)[0])


def dedupe_aliases(row: dict[str, Any]) -> set[str]:
    """Return aliases used for cross-provider duplicate/version clustering."""
    aliases: set[str] = set()
    record_key = norm_text(row.get("record_key", ""))
    if record_key:
        aliases.add(f"key:{record_key}")
    for raw in (row.get("doi", ""), row.get("formal_doi", "")):
        doi = norm_doi(raw)
        if not doi:
            continue
        if is_arxiv_doi(doi):
            arx = norm_arxiv_id(doi.rsplit("arxiv.", 1)[-1])
            if arx:
                aliases.add(f"arxiv:{arx}")
        else:
            aliases.add(f"doi:{doi}")

    arxiv_id = norm_arxiv_id(row.get("arxiv_id", ""))
    if arxiv_id:
        aliases.add(f"arxiv:{arxiv_id}")

    title = norm_title(row.get("title", ""))
    year = row_year(row)
    author = _first_author(row.get("authors", ""))
    if title and year:
        # Title + year is the protocol's primary non-identifier alias.  Keep
        # the author-bearing form as a second guard against same-title works.
        aliases.add(f"title_year:{title}|{year}")
        if author:
            aliases.add(f"author_year:{author}|{year}|{title}")
    elif title:
        # A title-only alias is safe only when the provider did not expose a
        # year.  It is deliberately not used for ordinary year-bearing rows.
        aliases.add(f"title_missing_year:{title}")

    # A provider identifier is a useful last-resort identity when a payload
    # has no title or DOI. Include the provider namespace so IDs from
    # different indexes cannot collide accidentally.
    provider = norm_title(row.get("source_database", ""))
    original = norm_text(row.get("original_record_id", ""))
    if provider and original:
        aliases.add(f"provider:{provider}|{original.casefold()}")
    return aliases


def strong_identity_aliases(row: dict[str, Any]) -> set[str]:
    """Return only DOI/arXiv aliases used to guard soft title merges."""
    return {
        alias
        for alias in dedupe_aliases(row)
        if alias.startswith("doi:") or alias.startswith("arxiv:")
    }


def all_seen_aliases(row: dict[str, Any]) -> set[str]:
    aliases = set(dedupe_aliases(row))
    strong = strong_identity_aliases(row)
    key = (row.get("record_key") or "").strip()
    # A legacy ``record_key`` can be a title-only hash.  Once a row has a
    # DOI/arXiv identifier, carrying that stale key into the seen-state
    # lookup would suppress a different work that happens to share a title.
    # Keep raw keys for sparse records, but for identified records retain
    # only the current canonical key (added below).
    if strong:
        aliases = {alias for alias in aliases if not alias.startswith("key:")}
    if key and not strong:
        aliases.add(f"key:{key}")
    # Rows written by the pre-1.1 monitor used a title-only SHA-1 key.  Keep
    # that alias readable for sparse rows during migration.  Do not attach it
    # to DOI/arXiv-identified rows: a later work with the same title must be
    # allowed through when its strong identifier differs.
    title = norm_title(row.get("title", ""))
    if title and not strong:
        legacy_hash = hashlib.sha1(title.encode("utf-8")).hexdigest()
        aliases.add("legacy_title:" + legacy_hash)
        aliases.add("title:" + legacy_hash)
    source = norm_title(row.get("source_database", ""))
    original = norm_text(row.get("original_record_id", ""))
    if source and original:
        aliases.add(f"provider:{source}|{original.casefold()}")
    if strong:
        # ``make_key`` prefers a formal DOI over an arXiv DOI and is the
        # canonical identity written to current CSVs.  Add its namespaced
        # form without reintroducing any stale title-only alias.
        aliases.add("key:" + make_key(row))
    return aliases


def make_key(row: dict[str, Any]) -> str:
    """Create a stable canonical key, including formal DOI and safe empty-title fallbacks."""
    # Prefer a publisher DOI when a record carries both an arXiv DOI and the
    # DOI of its formally published version.
    for raw in (row.get("formal_doi", ""), row.get("doi", "")):
        doi = norm_doi(raw)
        if doi:
            if is_arxiv_doi(doi):
                arx = norm_arxiv_id(doi.rsplit("arxiv.", 1)[-1])
                if arx:
                    return "arxiv:" + arx
            return "doi:" + doi

    arxiv_id = norm_arxiv_id(row.get("arxiv_id", ""))
    if arxiv_id:
        return "arxiv:" + arxiv_id

    title = norm_title(row.get("title", ""))
    if title:
        year = row_year(row)
        author = _first_author(row.get("authors", ""))
        identity = "|".join([title, str(year or ""), author])
        return "title:" + hashlib.sha1(identity.encode("utf-8")).hexdigest()

    provider = norm_title(row.get("source_database", ""))
    original = norm_text(row.get("original_record_id", ""))
    if provider and original:
        return "provider:" + hashlib.sha1(
            f"{provider}|{original.casefold()}".encode("utf-8")
        ).hexdigest()

    fallback = "|".join(
        [
            norm_text(row.get("authors", "")),
            norm_text(row.get("publication_year", "")),
            norm_text(row.get("venue", "")),
            norm_text(row.get("abstract", ""))[:500],
        ]
    )
    return "unidentified:" + hashlib.sha1(fallback.encode("utf-8")).hexdigest()


def record_completeness(row: dict[str, Any]) -> int:
    fields = [
        "title",
        "abstract",
        "authors",
        "publication_year",
        "publication_date",
        "venue",
        "doi",
        "url",
        "type",
        "original_record_id",
    ]
    return sum(bool(norm_text(row.get(field, ""))) for field in fields)


def split_authors(value: Any) -> list[str]:
    """Split common Zotero/provider author encodings without breaking names.

    Semicolon-delimited provider output is preferred.  For legacy
    ``Last, First, Last2, First2`` values, pair the comma-separated tokens;
    a two-token surname-first value is kept as one author when appropriate.
    The function is deliberately conservative and never invents an author
    when the field is empty.
    """
    if isinstance(value, (list, tuple)):
        raw = [norm_text(item) for item in value]
        return [item for item in raw if item]
    text = norm_text(value)
    if not text:
        return []
    if ";" in text:
        return [part.strip() for part in text.split(";") if part.strip()]
    if re.search(r"\s+and\s+", text, flags=re.I):
        return [part.strip() for part in re.split(r"\s+and\s+", text, flags=re.I) if part.strip()]
    if "\n" in text:
        return [part.strip() for part in text.splitlines() if part.strip()]
    parts = [part.strip() for part in text.split(",") if part.strip()]
    if len(parts) == 2:
        # ``Last, First`` is a single bibliographic author, while
        # ``Alice Smith, Bob Jones`` is normally two already-complete names.
        # Treat the value as two authors only when both sides look complete;
        # this avoids corrupting the common RIS/Zotero surname-first form.
        if all(len(part.split()) >= 2 for part in parts):
            return parts
        return [text]
    if len(parts) >= 4 and len(parts) % 2 == 0:
        # Pair only when every odd token looks like a surname and every even
        # token looks like a given name.  This avoids joining ordinary
        # ``Alice Smith, Bob Jones`` values incorrectly.
        if all(len(part.split()) <= 4 for part in parts):
            return [f"{parts[i]}, {parts[i + 1]}" for i in range(0, len(parts), 2)]
    return parts or [text]


def abstract_from_inverted(inv: Any) -> str:
    if not inv or not isinstance(inv, dict):
        return ""
    pairs = []
    for word, positions in inv.items():
        if not isinstance(positions, list):
            continue
        for position in positions:
            try:
                pairs.append((int(position), str(word)))
            except (TypeError, ValueError):
                continue
    pairs.sort()
    return norm_text(" ".join(word for _, word in pairs))


_FULL_DATE_RE = re.compile(
    r"(?<!\d)(?P<year>(?:19|20)\d{2})[-/.](?P<month>\d{1,2})[-/.](?P<day>\d{1,2})(?!\d)"
)
_YEAR_RE = re.compile(r"(?<!\d)(?:19|20)\d{2}(?!\d)")
_EXPLICIT_DAY_RE = re.compile(
    r"(?:\b\d{1,2}\b\s+(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b|"
    r"\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{1,2}\b)",
    re.I,
)


def parse_date_info(value: Any) -> tuple[date | None, bool]:
    """Return a date and a precision flag without inventing a day.

    Provider payloads mix ISO datetimes, ``YYYY-MM-DD`` strings and year-only
    values.  We parse an explicit day first and only use January 1 as an
    internal representative for a year-level value; callers must inspect the
    boolean flag before applying a narrow date window.
    """
    text = norm_text(value)
    if not text:
        return None, False
    match = _FULL_DATE_RE.search(text)
    if match:
        try:
            return (
                date(
                    int(match.group("year")),
                    int(match.group("month")),
                    int(match.group("day")),
                ),
                True,
            )
        except ValueError:
            return None, False

    # Do not let dateutil turn a bare year or an arbitrary identifier into a
    # day-level date.  It is used only for month names and other explicit
    # calendar representations that contain a day token.
    if _EXPLICIT_DAY_RE.search(text) and _YEAR_RE.search(text):
        try:
            parsed = dtparse(text, dayfirst=False, yearfirst=False, fuzzy=False).date()
            if _YEAR_RE.search(str(parsed.year)):
                return parsed, True
        except Exception:
            pass

    year_match = _YEAR_RE.search(text)
    if year_match:
        return date(int(year_match.group()), 1, 1), False
    return None, False


def parse_date_safe(value: Any) -> date | None:
    return parse_date_info(value)[0]


def within(value: Any, start: date, end: date) -> bool:
    parsed, exact = parse_date_info(value)
    return bool(parsed and exact and start <= parsed <= end)


def year_in_window(value: Any, start: date, end: date) -> bool:
    """Conservative year-level fallback used when a provider lacks day precision."""
    parsed, _ = parse_date_info(value)
    return bool(parsed and start.year <= parsed.year <= end.year)


def date_matches_window(
    value: Any,
    start: date,
    end: date,
    *,
    allow_year_only: bool = True,
) -> tuple[bool, str]:
    """Return ``(matches, precision)`` for an inclusive publication window.

    ``precision`` is ``day``, ``year`` or ``unknown``.  A year-only value is
    accepted only when ``allow_year_only`` is true; this prevents Semantic
    Scholar/DBLP year-only records from flooding a 14-day incremental run.
    """
    parsed, exact = parse_date_info(value)
    if parsed is None:
        return False, "unknown"
    if exact:
        return start <= parsed <= end, "day"
    return (allow_year_only and start.year <= parsed.year <= end.year), "year"


def narrow_window(start: date, end: date) -> bool:
    """Whether a window is narrow enough that year-only dates are unsafe."""
    return (end - start).days < 120


def date_window() -> tuple[date, date]:
    end = local_today()
    start = end - timedelta(days=int(CFG.get("incremental_lookback_days", 14)))
    return start, end


def _normalise_groups(value: Any) -> list[list[str]]:
    groups: list[list[str]] = []
    if not isinstance(value, list):
        return groups
    for group in value:
        if isinstance(group, str):
            terms = [group]
        elif isinstance(group, list):
            terms = [str(term) for term in group]
        elif isinstance(group, dict):
            terms = group.get("any") or group.get("terms") or []
            if isinstance(terms, str):
                terms = [terms]
        else:
            terms = []
        clean = [norm_text(term) for term in terms if norm_text(term)]
        if clean:
            groups.append(clean)
    return groups


def query_variants(
    *,
    parent_ids: set[str] | None = None,
    include_seed: bool = False,
    source: str | None = None,
) -> Iterable[dict[str, Any]]:
    """Yield stable, auditable query variants.

    Each variant may provide a provider-specific query in ``source_queries``;
    this is what prevents OpenAlex/Semantic Scholar from receiving one broad
    canonical Boolean expression.  The old ``short_queries`` shape remains a
    compatibility fallback for existing configurations.
    """
    wanted = {norm_text(item) for item in (parent_ids or set()) if norm_text(item)}
    for qid, spec in QUERIES.items():
        if wanted and qid not in wanted:
            continue
        configured = spec.get("variants") or [
            {"id": f"{qid}.{i}", "query": query, "lane": "legacy"}
            for i, query in enumerate(spec.get("short_queries", []), 1)
        ]
        for index, raw_variant in enumerate(configured, 1):
            variant = {"query": raw_variant} if isinstance(raw_variant, str) else dict(raw_variant)
            base_query = norm_text(variant.get("query") or variant.get("search") or "")
            source_queries = variant.get("source_queries") or {}
            selected_query = base_query
            if source:
                source_key = re.sub(r"[^a-z0-9]+", "", norm_text(source).casefold())
                selected_query = next(
                    (
                        norm_text(value)
                        for key, value in source_queries.items()
                        if re.sub(r"[^a-z0-9]+", "", norm_text(key).casefold()) == source_key
                        and norm_text(value)
                    ),
                    base_query,
                )
            if not selected_query:
                continue
            variant_id = norm_text(variant.get("id") or f"{qid}.{index}")
            yield {
                "id": variant_id,
                "parent_id": qid,
                "name": spec.get("name", qid),
                "query": selected_query,
                "base_query": base_query or selected_query,
                "source_queries": {str(k): norm_text(v) for k, v in source_queries.items() if norm_text(v)},
                "canonical": spec.get("canonical", ""),
                "lane": norm_text(variant.get("lane") or "default"),
                "required_groups": _normalise_groups(variant.get("required_groups")),
                "min_required_groups": int(variant.get("min_required_groups") or 0),
                "exclude_terms": [
                    norm_text(term)
                    for term in (variant.get("exclude_terms") or [])
                    if norm_text(term)
                ],
                "gate": bool(variant.get("gate", True)),
                "backfill_only": bool(variant.get("backfill_only", False)),
            }

    if include_seed and (not wanted or "SEED" in wanted):
        for seed in read_seed_titles():
            # Seed searches are validation-only by default and are explicitly
            # marked so they cannot be mistaken for production query families.
            escaped_seed = seed.replace("\\", "\\\\").replace('"', '\\"')
            yield {
                "id": "SEED." + hashlib.sha1(norm_title(seed).encode("utf-8")).hexdigest()[:12],
                "parent_id": "SEED",
                "name": "Seed recall",
                "query": f'"{escaped_seed}"',
                "base_query": f'"{escaped_seed}"',
                "source_queries": {},
                "canonical": "",
                "lane": "seed",
                "required_groups": [],
                "min_required_groups": 0,
                "exclude_terms": [],
                "gate": False,
                "seed_title": seed,
            }


def query_for_source(variant: dict[str, Any], source: str) -> dict[str, Any]:
    """Copy a variant with its provider-specific query selected."""
    result = dict(variant)
    source_queries = variant.get("source_queries") or {}
    source_key = re.sub(r"[^a-z0-9]+", "", norm_text(source).casefold())
    selected = next(
        (
            norm_text(value)
            for key, value in source_queries.items()
            if re.sub(r"[^a-z0-9]+", "", norm_text(key).casefold()) == source_key
            and norm_text(value)
        ),
        "",
    )
    result["query"] = selected or norm_text(variant.get("base_query") or variant.get("query") or "")
    return result


def arxiv_variants(*, parent_ids: set[str] | None = None) -> Iterable[dict[str, Any]]:
    wanted = {norm_text(item) for item in (parent_ids or set()) if norm_text(item)}
    for qid, spec in QUERIES.items():
        if wanted and qid not in wanted:
            continue
        for index, query in enumerate(spec.get("arxiv_queries", []), 1):
            query = norm_text(query)
            if query:
                yield {
                    "id": f"{qid}.A{index}",
                    "parent_id": qid,
                    "query": query,
                    "name": spec.get("name", qid),
                    "lane": "arxiv",
                    "required_groups": [],
                    "gate": False,
                }


def variant_matches(row: dict[str, Any], variant: dict[str, Any]) -> bool:
    """Apply an auditable, conservative lexical gate after provider search.

    The gate is intentionally soft: a variant can request a minimum number of
    concept groups, and an empty group list never rejects a provider result.
    This reduces obvious cross-domain noise without replacing human screening.
    """
    if not variant.get("gate", True):
        return True
    groups = variant.get("required_groups") or []
    if not groups:
        return True
    corpus = norm_text(" ".join(
        [norm_text(row.get("title", "")), norm_text(row.get("abstract", "")), norm_text(row.get("venue", ""))]
    )).casefold()
    if not corpus:
        return False
    normalized_corpus = norm_title(corpus)

    def contains(term: Any) -> bool:
        raw = norm_text(term).casefold()
        if not raw:
            return False
        if re.search(r"(?<![a-z0-9])" + re.escape(raw) + r"(?![a-z0-9])", corpus):
            return True
        normalized_term = norm_title(raw)
        # Punctuation-only distinctions such as C++ are handled by the raw
        # branch; avoid reducing them to a one-letter normalized token.
        if len(normalized_term) < 2:
            return False
        return bool(
            re.search(
                r"(?<![a-z0-9])" + re.escape(normalized_term) + r"(?![a-z0-9])",
                normalized_corpus,
            )
        )

    for term in variant.get("exclude_terms") or []:
        if contains(term):
            return False
    matched = 0
    for group in groups:
        if any(contains(term) for term in group):
            matched += 1
    minimum = variant.get("min_required_groups") or len(groups)
    return matched >= max(1, min(int(minimum), len(groups)))


def _atomic_write(path: Path, writer) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(fd)
    temp_path = Path(temp_name)
    try:
        writer(temp_path)
        os.replace(temp_path, path)
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    fields = fields or FIELDS

    def writer(temp_path: Path) -> None:
        with temp_path.open("w", newline="", encoding="utf-8-sig") as handle:
            output = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
            output.writeheader()
            for row in rows:
                current = dict(row)
                current["record_key"] = current.get("record_key") or make_key(current)
                output.writerow({field: current.get(field, "") for field in fields})

    _atomic_write(Path(path), writer)


def write_json(path: Path, value: Any) -> None:
    def writer(temp_path: Path) -> None:
        temp_path.write_text(
            json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )

    _atomic_write(Path(path), writer)


def read_seen_keys(path: Path | None = None) -> set[str]:
    path = path or (ROOT / "data" / "state" / "seen_keys.txt")
    if not path.exists():
        return set()
    return {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def commit_seen_keys(
    rows: Iterable[dict[str, Any]] | Iterable[str], path: Path | None = None
) -> set[str]:
    """Atomically commit canonical keys and all known aliases after outputs succeed."""
    path = path or (ROOT / "data" / "state" / "seen_keys.txt")
    current = read_seen_keys(path)
    for item in rows:
        if isinstance(item, dict):
            current.update(all_seen_aliases(item))
            current.add(make_key(item))
        else:
            current.add(str(item).strip())
    content = "\n".join(sorted(x for x in current if x)) + "\n"
    _atomic_write(path, lambda temp: temp.write_text(content, encoding="utf-8"))
    return current


def row_is_seen(row: dict[str, Any], seen: set[str]) -> bool:
    aliases = all_seen_aliases(row)
    canonical = make_key(row)
    aliases.add(canonical)
    strong = strong_identity_aliases(row)
    if strong:
        # For an identified row, only exact DOI/arXiv/provider identities (or
        # the current canonical key) are safe state matches.  Title/year and
        # author/year aliases are deliberately soft: using them here would
        # hide a distinct DOI-bearing work that shares a title with an older
        # record.  Sparse incoming rows still use those aliases below so an
        # enrichment of an already-seen item does not create needless noise.
        aliases = {
            alias
            for alias in aliases
            if alias.startswith("doi:")
            or alias.startswith("arxiv:")
            or alias.startswith("provider:")
            or alias == canonical
        }
    return bool(aliases & seen)


def read_seed_titles(path: Path | None = None) -> list[str]:
    path = path or (ROOT / "config" / "seed_titles.txt")
    if not path.exists():
        return []
    seeds = []
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        line = re.sub(r"^P(?:aper|paer)\s*\d+\s*", "", line, flags=re.I)
        if line:
            seeds.append(line)
    return seeds


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    """Read a monitor CSV with BOM/UTF-8/GB18030 compatibility."""
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "utf-8", "gb18030"):
        try:
            with Path(path).open("r", newline="", encoding=encoding) as handle:
                reader = csv.DictReader(handle)
                if not reader.fieldnames:
                    raise ValueError(f"missing CSV header: {path}")
                normalized_headers = [
                    re.sub(r"[^a-z0-9]+", "", norm_text(header).casefold())
                    for header in reader.fieldnames
                ]
                if any(not norm_text(header) for header in reader.fieldnames):
                    raise ValueError(f"CSV header has blank columns: {path}")
                duplicate_headers = sorted({
                    header
                    for header in normalized_headers
                    if normalized_headers.count(header) > 1 and header
                })
                if duplicate_headers:
                    raise ValueError(
                        f"CSV header has duplicate columns ({', '.join(duplicate_headers)}): {path}"
                    )
                rows: list[dict[str, str]] = []
                for line_number, row in enumerate(reader, 2):
                    if None in row:
                        raise ValueError(
                            f"CSV row {line_number} has more fields than its header: {path}"
                        )
                    if any(value is None for value in row.values()):
                        raise ValueError(
                            f"CSV row {line_number} has fewer fields than its header: {path}"
                        )
                    rows.append({
                        str(key): (value if value is not None else "")
                        for key, value in row.items()
                    })
                return rows
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error:
        raise ValueError(f"cannot decode CSV: {path}") from last_error
    raise ValueError(f"cannot read CSV: {path}")
