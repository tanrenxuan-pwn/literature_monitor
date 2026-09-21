"""Validation utilities for the literature monitor.

The default checks are deterministic and never contact a provider.  A live
14-day comparison is opt-in and always runs with ``commit_state=False`` in a
separate output directory.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import sys
import tempfile
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import date
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
from unittest.mock import patch

import common
import import_acm
import run_backfill as backfill
import run_incremental as monitor
import export_ris
import weekly_recovery_gate as recovery_gate

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def _read_csv(path: Path) -> list[dict[str, str]]:
    errors: list[str] = []
    text = None
    for encoding in ("utf-8-sig", "utf-8", "gb18030"):
        try:
            text = path.read_text(encoding=encoding)
            break
        except UnicodeDecodeError as exc:
            errors.append(f"{encoding}: {exc}")
    if text is None:
        raise ValueError(f"cannot decode {path}: {'; '.join(errors)}")
    reader = csv.DictReader(io.StringIO(text, newline=""))
    if not reader.fieldnames:
        raise ValueError(f"missing CSV header: {path}")
    normalized_headers = [
        re.sub(r"[^a-z0-9]+", "", common.norm_text(header).casefold())
        for header in reader.fieldnames
    ]
    if any(not common.norm_text(header) for header in reader.fieldnames):
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


def _title_similarity(left: str, right: str) -> float:
    a = common.norm_title(left)
    b = common.norm_title(right)
    if not a or not b:
        return 0.0
    if a == b or a in b or b in a:
        return 1.0
    return SequenceMatcher(None, a, b).ratio()


def seed_recall(rows: list[dict[str, Any]], seeds: list[str]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for seed in seeds:
        matches = []
        for row in rows:
            score = _title_similarity(seed, row.get("title", ""))
            if score >= 0.92:
                matches.append(
                    {
                        "record_key": row.get("record_key", ""),
                        "title": row.get("title", ""),
                        "score": round(score, 4),
                        "match_kind": "exact" if common.norm_title(seed) == common.norm_title(row.get("title", "")) else "near",
                        "query_id": row.get("query_id", ""),
                        "source_database": row.get("source_database", ""),
                    }
                )
        result[seed] = {
            "matched": any(item["match_kind"] == "exact" for item in matches),
            "near_only": bool(matches) and not any(item["match_kind"] == "exact" for item in matches),
            "matches": matches[:10],
        }
    return result


def summarize_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    keys = [row.get("record_key") or common.make_key(row) for row in rows]
    dois = [common.norm_doi(row.get("doi", "")) for row in rows]
    sources = Counter()
    queries = Counter()
    for row in rows:
        for source in (row.get("source_database", "") or "").split(";"):
            if source.strip():
                sources[source.strip()] += 1
        for query_id in (row.get("query_id", "") or "").split(";"):
            if query_id.strip():
                queries[query_id.strip()] += 1
    return {
        "rows": len(rows),
        "unique_record_keys": len(set(keys)),
        "duplicate_key_rows": len(keys) - len(set(keys)),
        "doi_rows": sum(bool(value) for value in dois),
        "abstract_rows": sum(bool(common.norm_text(row.get("abstract", ""))) for row in rows),
        "source_counts": dict(sources),
        "query_counts": dict(queries),
    }


def date_range(rows: list[dict[str, Any]]) -> dict[str, str | None]:
    values = [
        common.parse_date_info(row.get("publication_date", ""))[0]
        for row in rows
    ]
    values = [value for value in values if value is not None]
    return {
        "min": min(values).isoformat() if values else None,
        "max": max(values).isoformat() if values else None,
    }


def validate_rows(
    rows: list[dict[str, Any]],
    *,
    expected_fields: list[str] | None = None,
    start: date | None = None,
    end: date | None = None,
    manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    keys = [row.get("record_key") or common.make_key(row) for row in rows]
    checks: dict[str, bool] = {
        "rows_have_unique_record_keys": len(keys) == len(set(keys)),
        "rows_have_titles_or_provider_ids": all(
            bool(common.norm_text(row.get("title", "")) or common.norm_text(row.get("original_record_id", "")))
            for row in rows
        ),
    }
    if expected_fields:
        checks["stable_fields_present"] = set(expected_fields).issubset(rows[0].keys() if rows else set(expected_fields))
    if start is not None and end is not None:
        out_of_window = 0
        for row in rows:
            parsed, exact = common.parse_date_info(row.get("publication_date", ""))
            if parsed is None or (exact and not (start <= parsed <= end)):
                out_of_window += 1
        checks["exact_dates_within_window"] = out_of_window == 0
    else:
        out_of_window = None
    if manifest is not None:
        checks["validation_did_not_commit_state"] = manifest.get("state_committed") is False
        checks["source_failures_recorded"] = isinstance(manifest.get("source_failures", []), list)
    return {
        "checks": checks,
        "all_checks_pass": all(checks.values()),
        "out_of_window_rows": out_of_window,
        "date_range": date_range(rows),
    }


def compare_rows(
    baseline: list[dict[str, Any]],
    candidate: list[dict[str, Any]],
    seeds: list[str],
    *,
    start: date | None = None,
    end: date | None = None,
) -> dict[str, Any]:
    before = summarize_rows(baseline)
    after = summarize_rows(candidate)
    return {
        "baseline": before,
        "candidate": after,
        "baseline_date_range": date_range(baseline),
        "candidate_date_range": date_range(candidate),
        "delta": {
            field: after[field] - before[field]
            for field in ("rows", "unique_record_keys", "doi_rows", "abstract_rows")
        },
        "seed_recall_baseline": seed_recall(baseline, seeds),
        "seed_recall_candidate": seed_recall(candidate, seeds),
        "window": {
            "start": start.isoformat() if start else None,
            "end": end.isoformat() if end else None,
        },
    }


def run_seed_probe(
    start: date,
    end: date,
    limit: int,
    *,
    include_openalex: bool = True,
    include_semantic_scholar: bool = True,
    include_dblp: bool = False,
    include_ieee: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Run exact-title probes over a broad window without touching state."""
    rows, calls, failures = monitor.collect(
        start,
        end,
        max(1, limit),
        include_arxiv=False,
        include_openalex=include_openalex,
        include_semantic_scholar=include_semantic_scholar,
        include_dblp=include_dblp,
        include_ieee=include_ieee,
        include_seed_queries=True,
        seed_only=True,
        strict_dates=False,
    )
    merged, dedupe = monitor.merge_dedupe(rows, return_report=True)
    return merged, {"source_calls": calls, "source_failures": failures, "dedupe": dedupe}


class _FakeResponse:
    def __init__(
        self,
        payload: Any = None,
        content: bytes | None = None,
        *,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
    ):
        self._payload = payload
        self.content = content or b""
        self.headers = headers or {}
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise monitor.requests.HTTPError(
                f"HTTP {self.status_code}", response=self
            )
        return None


def _oa_item(identifier: str, title: str, cursor: str | None, day: str = "2026-09-10") -> dict[str, Any]:
    return {
        "id": f"https://openalex.org/{identifier}",
        "display_name": title,
        "publication_date": day,
        "publication_year": int(day[:4]),
        "doi": f"https://doi.org/10.1000/{identifier.lower()}",
        "ids": {"openalex": f"https://openalex.org/{identifier}"},
        "authorships": [],
        "abstract_inverted_index": {"vulnerability": [0], "analysis": [1]},
        "primary_location": {"source": {"display_name": "Test Venue"}, "landing_page_url": "https://example.test"},
        "type": "article",
        "cited_by_count": 1,
        "_next": cursor,
    }


def run_self_tests() -> dict[str, Any]:
    checks: dict[str, str] = {}
    start, end = date(2026, 9, 1), date(2026, 9, 14)

    oa_payloads = {
        "*": {"meta": {"count": 3, "next_cursor": "c1"}, "results": [_oa_item("W1", "One", "c1")]},
        "c1": {"meta": {"count": 3, "next_cursor": None}, "results": [_oa_item("W2", "Two", None), _oa_item("W3", "Three", None)]},
    }
    oa_seen: list[str] = []

    def fake_oa(url, *, params, **kwargs):
        oa_seen.append(params["cursor"])
        return _FakeResponse(oa_payloads[params["cursor"]])

    with patch.object(monitor, "_get_with_retry", side_effect=fake_oa), patch.object(monitor, "_sleep_delay", return_value=None):
        rows = monitor.openalex("T.oa", "test", start, end, 3, strict_dates=True)
    assert len(rows) == 3 and oa_seen == ["*", "c1"], (len(rows), oa_seen)
    checks["openalex_cursor_pagination"] = "ok"

    partial_calls: list[str] = []

    def fake_oa_partial(url, *, params, **kwargs):
        partial_calls.append(params["cursor"])
        if params["cursor"] == "*":
            return _FakeResponse(oa_payloads["*"])
        raise RuntimeError("simulated page failure")

    with patch.object(monitor, "_get_with_retry", side_effect=fake_oa_partial), patch.object(monitor, "_sleep_delay", return_value=None):
        try:
            monitor.openalex("T.partial", "test", start, end, 3, strict_dates=True)
        except monitor.ProviderPartialError as exc:
            assert len(exc.rows) == 1 and partial_calls == ["*", "c1"]
        else:
            raise AssertionError("partial provider failure was not surfaced")
    checks["partial_page_preservation"] = "ok"

    s2_payloads = {
        "": {"total": 2, "token": "t1", "data": [{"paperId": "P1", "title": "One", "abstract": "x", "year": 2026, "publicationDate": "2026-09-03", "authors": [], "externalIds": {}}]},
        "t1": {"total": 2, "data": [{"paperId": "P2", "title": "Two", "abstract": "x", "year": 2026, "publicationDate": "2026-09-04", "authors": [], "externalIds": {}}]},
    }
    s2_seen: list[str] = []

    def fake_s2(url, *, params, **kwargs):
        token = params.get("token", "")
        s2_seen.append(token)
        return _FakeResponse(s2_payloads[token])

    with patch.object(monitor, "_get_with_retry", side_effect=fake_s2), patch.object(monitor, "_sleep_delay", return_value=None):
        rows = monitor.semantic_scholar("T.s2", "test", start, end, 2, strict_dates=True)
    assert len(rows) == 2 and s2_seen == ["", "t1"], (len(rows), s2_seen)
    checks["semantic_scholar_token_pagination"] = "ok"

    def atom_page(first: int, last: int) -> bytes:
        atom_entries = "".join(
            f"<entry><id>http://arxiv.org/abs/2609.{index:05d}v1</id>"
            "<published>2026-09-03T00:00:00Z</published>"
            f"<title>P{index}</title><summary>S</summary>"
            "<author><name>Alice</name></author></entry>"
            for index in range(first, last + 1)
        )
        return (
            "<?xml version='1.0'?><feed xmlns='http://www.w3.org/2005/Atom'>"
            + atom_entries
            + "</feed>"
        ).encode()

    arxiv_offsets: list[int] = []
    arxiv_params: list[dict[str, Any]] = []

    def fake_arxiv(url, *, params, **kwargs):
        arxiv_offsets.append(params["start"])
        arxiv_params.append(dict(params))
        if params["start"] == 0:
            return _FakeResponse(content=atom_page(1, 2000))
        if params["start"] == 2000:
            return _FakeResponse(content=atom_page(2001, 2001))
        return _FakeResponse(content=b"<feed xmlns='http://www.w3.org/2005/Atom'></feed>")

    with patch.object(monitor, "_get_with_retry", side_effect=fake_arxiv), patch.object(monitor, "_sleep_delay", return_value=None):
        rows = monitor.arxiv("T.ax", "all:test", start, end, 2001, strict_dates=True)
    assert len(rows) == 2001 and arxiv_offsets == [0, 2000], (len(rows), arxiv_offsets)
    assert all(item["max_results"] == 2000 for item in arxiv_params)
    assert all("sortBy" not in item and "sortOrder" not in item for item in arxiv_params)
    assert monitor._provider_from_url("https://arxiv.org/api/query") == "arxiv"
    checks["arxiv_offset_pagination"] = "ok"

    oai_page_one = b"""<?xml version='1.0' encoding='UTF-8'?>
<OAI-PMH xmlns='http://www.openarchives.org/OAI/2.0/' xmlns:arxiv='http://arxiv.org/OAI/arXiv/'>
  <ListRecords>
    <record>
      <header><identifier>oai:arXiv.org:2401.00001</identifier><datestamp>2026-09-01</datestamp><setSpec>cs:cs:CR</setSpec></header>
      <metadata><arxiv:arXiv>
        <arxiv:id>2401.00001</arxiv:id><arxiv:created>2024-01-10</arxiv:created><arxiv:updated>2024-02-01</arxiv:updated>
        <arxiv:authors><arxiv:author><arxiv:keyname>Doe</arxiv:keyname><arxiv:forenames>Jane</arxiv:forenames></arxiv:author></arxiv:authors>
        <arxiv:title>Static analysis vulnerability false positive validation</arxiv:title>
        <arxiv:categories>cs.CR cs.SE</arxiv:categories>
        <arxiv:comments>Accepted paper</arxiv:comments><arxiv:journal-ref>TestConf 2024</arxiv:journal-ref>
        <arxiv:doi>10.1000/oai-test</arxiv:doi><arxiv:abstract>We validate vulnerability alerts produced by static analysis.</arxiv:abstract>
      </arxiv:arXiv></metadata>
    </record>
    <resumptionToken expirationDate='2099-01-01T00:00:00Z'>token-1</resumptionToken>
  </ListRecords>
</OAI-PMH>"""
    oai_page_two = b"""<?xml version='1.0' encoding='UTF-8'?>
<OAI-PMH xmlns='http://www.openarchives.org/OAI/2.0/' xmlns:arxiv='http://arxiv.org/OAI/arXiv/'>
  <ListRecords>
    <record>
      <header><identifier>oai:arXiv.org:1305.00002</identifier><datestamp>2026-09-02</datestamp><setSpec>cs:cs:CR</setSpec></header>
      <metadata><arxiv:arXiv>
        <arxiv:id>1305.00002</arxiv:id><arxiv:created>2013-05-01</arxiv:created><arxiv:updated>2026-09-02</arxiv:updated>
        <arxiv:authors><arxiv:author><arxiv:keyname>Roe</arxiv:keyname></arxiv:author></arxiv:authors>
        <arxiv:title>Unrelated graph processing</arxiv:title><arxiv:categories>cs.CR</arxiv:categories>
        <arxiv:abstract>No matching security terms.</arxiv:abstract>
      </arxiv:arXiv></metadata>
    </record>
    <resumptionToken completeListSize='2' cursor='1'></resumptionToken>
  </ListRecords>
</OAI-PMH>"""
    oai_params: list[dict[str, Any]] = []

    def fake_oai(url, *, params, **kwargs):
        oai_params.append(dict(params))
        return _FakeResponse(
            content=oai_page_two if params.get("resumptionToken") else oai_page_one
        )

    monitor._ARXIV_OAI_READY.clear()
    with tempfile.TemporaryDirectory() as temp:
        cache_path = Path(temp) / "arxiv-oai.sqlite3"
        call_log: list[dict[str, Any]] = []
        with patch.object(monitor, "_get_with_retry", side_effect=fake_oai):
            oai_rows = monitor.arxiv(
                "T.oai",
                '(cat:cs.CR OR cat:cs.SE) AND all:"static analysis" AND '
                '(all:vulnerability OR all:security) AND '
                '(all:"false positive" OR all:triage OR all:validation)',
                date(2024, 1, 1),
                date(2024, 12, 31),
                10,
                call_log=call_log,
                oai_cache_path=cache_path,
                oai_cache_start=date(2014, 1, 1),
                oai_set_specs=("cs:cs:CR",),
            )
        assert len(oai_rows) == 1 and oai_rows[0]["arxiv_id"] == "2401.00001"
        assert oai_rows[0]["doi"] == "10.1000/oai-test"
        assert oai_params == [
            {
                "verb": "ListRecords",
                "metadataPrefix": "arXiv",
                "set": "cs:cs:CR",
                "from": "2014-01-01",
            },
            {"verb": "ListRecords", "resumptionToken": "token-1"},
        ]
        assert call_log[0]["transport"] == "OAI-PMH cache"
        assert call_log[0]["cache_records"] == 2
        with patch.object(
            monitor,
            "_get_with_retry",
            side_effect=AssertionError("completed OAI cache contacted the network"),
        ):
            cached_rows = monitor.arxiv(
                "T.oai.cached",
                'cat:cs.CR AND all:"static analysis" AND all:vulnerability',
                date(2024, 1, 1),
                date(2024, 12, 31),
                10,
                oai_cache_path=cache_path,
                oai_cache_start=date(2014, 1, 1),
                oai_set_specs=("cs:cs:CR",),
            )
        assert len(cached_rows) == 1
    monitor._ARXIV_OAI_READY.clear()
    assert monitor._arxiv_oai_token_expired("2000-01-01T00:00:00Z")
    assert not monitor._arxiv_oai_token_expired("2099-01-01T00:00:00Z")
    assert monitor._provider_from_url("https://oaipmh.arxiv.org/oai") == "arxiv_oai"
    checks["arxiv_oai_checkpoint_cache_and_local_filter"] = "ok"

    fake_addresses = [
        (
            monitor.socket.AF_INET,
            monitor.socket.SOCK_STREAM,
            6,
            "",
            ("10.0.0.1", 443),
        ),
        (
            monitor.socket.AF_INET,
            monitor.socket.SOCK_STREAM,
            6,
            "",
            ("10.0.0.2", 443),
        ),
    ]
    observed_addresses: list[str] = []

    def fake_oai_transport(url, **kwargs):
        resolved = monitor.socket.getaddrinfo(
            "oaipmh.arxiv.org", 443, type=monitor.socket.SOCK_STREAM
        )
        observed_addresses.append(resolved[0][4][0])
        if len(observed_addresses) == 1:
            raise monitor.requests.exceptions.ConnectionError(
                "simulated TLS reset"
            )
        return _FakeResponse(content=b"<OAI-PMH />")

    monitor.reset_provider_runtime_state()
    with patch.object(
        monitor.socket, "getaddrinfo", return_value=fake_addresses
    ), patch.object(
        monitor.requests, "get", side_effect=fake_oai_transport
    ), patch.object(
        monitor, "_sleep_delay", return_value=None
    ):
        monitor._get_with_retry(
            "https://oaipmh.arxiv.org/oai",
            params={"verb": "Identify"},
            provider="arxiv_oai",
            max_retries=1,
            expected_format="xml",
        )
        monitor._get_with_retry(
            "https://oaipmh.arxiv.org/oai",
            params={"verb": "Identify"},
            provider="arxiv_oai",
            max_retries=0,
            expected_format="xml",
        )
    assert observed_addresses == ["10.0.0.1", "10.0.0.2", "10.0.0.2"]
    assert (
        monitor.provider_runtime_snapshot()["arXiv OAI-PMH"][
            "dns_rotation_attempts"
        ]
        == 1
    )
    monitor.reset_provider_runtime_state()
    checks["arxiv_oai_dns_failover"] = "ok"

    monitor.reset_provider_runtime_state()
    throttle_waits: list[float] = []
    observed_timeouts: list[int] = []

    def fake_http_ok(url, *, timeout, **kwargs):
        observed_timeouts.append(timeout)
        return _FakeResponse({})

    with patch.object(monitor.requests, "get", side_effect=fake_http_ok), patch.object(
        monitor, "_sleep_delay", side_effect=throttle_waits.append
    ):
        monitor._get_with_retry(
            "https://export.arxiv.org/api/query",
            params={},
            provider="arxiv",
            max_retries=0,
        )
        monitor._get_with_retry(
            "https://export.arxiv.org/api/query",
            params={},
            provider="arxiv",
            max_retries=0,
        )
    assert observed_timeouts == [90, 90]
    assert throttle_waits and max(throttle_waits) >= 3.99, throttle_waits
    checks["provider_request_throttle_and_timeout"] = "ok"

    assert float(
        monitor._provider_policy("semantic_scholar")["min_interval_seconds"]
    ) >= 2.0
    assert float(monitor._provider_policy("ieee")["min_interval_seconds"]) >= 2.0
    checks["rate_limited_provider_pacing"] = "ok"

    with tempfile.TemporaryDirectory() as temp:
        previous_lock_dir = os.environ.get("LIT_MONITOR_RATE_LOCK_DIR")
        os.environ["LIT_MONITOR_RATE_LOCK_DIR"] = temp
        shared_waits: list[float] = []
        s2_policy = dict(monitor._provider_policy("semantic_scholar"))
        s2_policy["initial_delay_seconds"] = 0.0
        try:
            with patch.object(
                monitor, "_provider_policy", return_value=s2_policy
            ), patch.object(monitor, "_sleep_delay", side_effect=shared_waits.append):
                monitor.reset_provider_runtime_state()
                monitor._wait_for_provider("semantic_scholar")
                monitor.reset_provider_runtime_state()
                monitor._wait_for_provider("semantic_scholar")
        finally:
            monitor.reset_provider_runtime_state()
            if previous_lock_dir is None:
                os.environ.pop("LIT_MONITOR_RATE_LOCK_DIR", None)
            else:
                os.environ["LIT_MONITOR_RATE_LOCK_DIR"] = previous_lock_dir
        assert shared_waits and max(shared_waits) >= 1.0, shared_waits
    checks["cross_process_rate_limit_coordination"] = "ok"

    monitor.reset_provider_runtime_state()
    denied = _FakeResponse(
        content=b"Awaiting IEEE activation",
        status_code=403,
        headers={"Content-Type": "text/html"},
    )
    old_ieee_key_for_error = os.environ.get("IEEE_API_KEY")
    os.environ["IEEE_API_KEY"] = "secret-self-test-key"
    try:
        with patch.object(monitor.requests, "get", return_value=denied) as denied_get:
            try:
                monitor._get_with_retry(
                    "https://ieeexploreapi.ieee.org/api/v1/search/articles",
                    params={"apikey": "secret-self-test-key"},
                    provider="ieee",
                    expected_format="json",
                )
            except monitor.ProviderHTTPStatusError as exc:
                assert exc.status_code == 403
                assert "secret-self-test-key" not in str(exc)
            else:
                raise AssertionError("IEEE 403 was not surfaced")
        assert denied_get.call_count == 1
        assert monitor.provider_runtime_snapshot()["IEEE Xplore"]["access_denied_events"] == 1
    finally:
        if old_ieee_key_for_error is None:
            os.environ.pop("IEEE_API_KEY", None)
        else:
            os.environ["IEEE_API_KEY"] = old_ieee_key_for_error
    checks["access_denied_fails_fast"] = "ok"

    monitor.reset_provider_runtime_state()
    quota_denied = _FakeResponse(
        content=b"<h1>Developer Over Rate</h1>",
        status_code=403,
        headers={"Content-Type": "text/html"},
    )
    old_ieee_key_for_quota = os.environ.get("IEEE_API_KEY")
    os.environ["IEEE_API_KEY"] = "secret-self-test-key"
    try:
        with patch.object(monitor.requests, "get", return_value=quota_denied) as quota_get, patch.object(
            monitor, "_sleep_delay", return_value=None
        ):
            try:
                monitor._get_with_retry(
                    "https://ieeexploreapi.ieee.org/api/v1/search/articles",
                    params={"apikey": "secret-self-test-key"},
                    provider="ieee",
                    expected_format="json",
                )
            except monitor.ProviderHTTPStatusError as exc:
                assert exc.status_code == 403
                assert exc.rate_limited is True
                assert (monitor._failure_metadata(exc))["code"] == "RATE_LIMITED"
                assert (exc.retry_after_seconds or 0) >= 86400
            else:
                raise AssertionError("IEEE daily quota response was not surfaced")
        assert quota_get.call_count == 1
        runtime = monitor.provider_runtime_snapshot()["IEEE Xplore"]
        assert runtime["rate_limit_events"] == 1
        assert runtime["access_denied_events"] == 0
    finally:
        if old_ieee_key_for_quota is None:
            os.environ.pop("IEEE_API_KEY", None)
        else:
            os.environ["IEEE_API_KEY"] = old_ieee_key_for_quota
    checks["ieee_daily_quota_is_classified"] = "ok"

    monitor.reset_provider_runtime_state()
    qps_denied = _FakeResponse(
        content=b"<h1>Service Over Qps</h1>",
        status_code=403,
        headers={"Content-Type": "text/html"},
    )
    qps_recovered = _FakeResponse(
        {},
        status_code=200,
        headers={"Content-Type": "application/json"},
    )
    qps_waits: list[float] = []
    with patch.object(
        monitor.requests,
        "get",
        side_effect=[qps_denied, qps_recovered],
    ) as qps_get, patch.object(
        monitor, "_sleep_delay", side_effect=qps_waits.append
    ):
        recovered = monitor._get_with_retry(
            "https://ieeexploreapi.ieee.org/api/v1/search/articles",
            params={"apikey": "secret-self-test-key"},
            provider="ieee",
            expected_format="json",
            max_retries=1,
        )
    assert recovered.status_code == 200
    assert qps_get.call_count == 2
    assert qps_waits and max(qps_waits) >= 10.0, qps_waits
    runtime = monitor.provider_runtime_snapshot()["IEEE Xplore"]
    assert runtime["rate_limit_events"] == 1
    assert runtime["access_denied_events"] == 0
    assert runtime["retries"] == 1
    checks["ieee_qps_limit_is_retried"] = "ok"

    monitor.reset_provider_runtime_state()
    malformed = _FakeResponse(content=b"<html>temporary gateway page</html>")
    with patch.object(monitor.requests, "get", return_value=malformed) as malformed_get, patch.object(
        monitor, "_sleep_delay", return_value=None
    ):
        try:
            monitor._get_with_retry(
                "https://dblp.org/search/publ/api",
                params={"format": "json"},
                provider="dblp",
                max_retries=1,
                expected_format="json",
            )
        except monitor.ProviderInvalidResponseError as exc:
            assert exc.status_code == 200
            assert "Content-Type" in str(exc)
        else:
            raise AssertionError("malformed DBLP response was not surfaced")
    assert malformed_get.call_count == 2
    assert monitor.provider_runtime_snapshot()["DBLP"]["invalid_response_events"] == 2
    checks["invalid_response_is_diagnosed"] = "ok"

    monitor.reset_provider_runtime_state()
    threshold = int(
        monitor._provider_policy("arxiv")["circuit_breaker_threshold"]
    )
    rate_response = _FakeResponse(status_code=429)
    with patch.object(monitor.requests, "get", return_value=rate_response) as get_mock, patch.object(
        monitor, "_sleep_delay", return_value=None
    ):
        try:
            monitor._get_with_retry(
                "https://export.arxiv.org/api/query",
                params={},
                provider="arxiv",
            )
        except monitor.ProviderCircuitOpenError:
            pass
        else:
            raise AssertionError("repeated 429 responses did not open the circuit")
    runtime = monitor.provider_runtime_snapshot()["arXiv"]
    assert get_mock.call_count == threshold
    assert runtime["rate_limit_events"] == threshold
    assert runtime["circuit_trips"] == 1
    monitor.reset_provider_runtime_state()
    checks["rate_limit_circuit_breaker"] = "ok"

    duplicate_rows = [
        {"title": "Versioned Paper", "publication_year": 2025, "authors": "Doe; A", "doi": "10.1000/x", "source_database": "OpenAlex"},
        {"title": "Versioned Paper", "publication_year": 2025, "authors": "Doe; A", "doi": "https://doi.org/10.1000/X", "source_database": "Semantic Scholar"},
        {"title": "Versioned Paper", "publication_year": 2026, "authors": "Doe; A", "arxiv_id": "2601.00001v2", "is_preprint": "1", "source_database": "arXiv"},
    ]
    merged, report = monitor.merge_dedupe(duplicate_rows, return_report=True)
    assert len(merged) == 1 and report["duplicate_rows"] == 2, report

    date_only_version = dict(duplicate_rows[0])
    date_only_version.pop("publication_year")
    date_only_version["publication_date"] = "2025-07-12"
    year_only_version = dict(duplicate_rows[1])
    year_only_version.pop("publication_date", None)
    assert len(monitor.merge_dedupe([date_only_version, year_only_version])) == 1
    imprecise = {
        "title": "Precision Merge",
        "publication_year": 2026,
        "publication_date": "2026",
        "authors": "Doe; A",
        "doi": "10.1000/precision-merge",
        "source_database": "IEEE Xplore",
    }
    precise = dict(imprecise)
    precise["publication_date"] = "2026-09-03"
    precise["source_database"] = "Semantic Scholar"
    precise_merged = monitor.merge_dedupe([imprecise, precise])
    assert len(precise_merged) == 1
    assert precise_merged[0]["publication_date"] == "2026-09-03"
    assert precise_merged[0]["_date_precision"] == "day"
    checks["date_precision_merge"] = "ok"
    checks["doi_version_deduplication"] = "ok"

    formal_version = {
        "doi": "10.48550/arxiv.2601.00001",
        "formal_doi": "https://doi.org/10.1000/formal-version",
        "title": "Versioned Paper",
        "publication_year": 2026,
        "authors": "Doe; A",
    }
    assert common.make_key(formal_version) == "doi:10.1000/formal-version"
    assert common.norm_text(0) == "0"
    checks["canonical_key_priority"] = "ok"

    distinct_dois = [
        {
            "title": "Same title, separate deposits",
            "publication_year": 2026,
            "authors": "Doe; A",
            "doi": "10.1000/deposit-a",
            "is_preprint": "1",
            "source_database": "OpenAlex",
        },
        {
            "title": "Same title, separate deposits",
            "publication_year": 2026,
            "authors": "Doe; A",
            "doi": "10.1000/deposit-b",
            "is_preprint": "1",
            "source_database": "OpenAlex",
        },
    ]
    assert len(monitor.merge_dedupe(distinct_dois)) == 2
    checks["strong_identifier_conflict_guard"] = "ok"

    with tempfile.TemporaryDirectory() as temp:
        state = Path(temp) / "seen.txt"
        row = {"title": "transaction", "publication_year": 2026, "authors": "A", "source_database": "OpenAlex"}
        assert monitor.seen_filter([row], state) == [row] and not state.exists()
        common.commit_seen_keys([row], state)
        assert monitor.seen_filter([row], state) == []

        # A pre-1.1 title-only state key must not hide a distinct DOI-bearing
        # work that happens to reuse the same title.
        legacy_title = "Same title, separate DOI"
        legacy_hash = hashlib.sha1(common.norm_title(legacy_title).encode("utf-8")).hexdigest()
        identified_state = Path(temp) / "identified-seen.txt"
        identified_state.write_text("title:" + legacy_hash + "\n", encoding="utf-8")
        doi_a = {
            "title": legacy_title,
            "publication_year": 2026,
            "authors": "A",
            "doi": "10.1000/identified-a",
            "source_database": "OpenAlex",
        }
        doi_b = dict(doi_a, doi="10.1000/identified-b")
        assert monitor.seen_filter([doi_a], identified_state) == [doi_a]
        common.commit_seen_keys([doi_a], identified_state)
        assert monitor.seen_filter([doi_a], identified_state) == []
        assert monitor.seen_filter([doi_b], identified_state) == [doi_b]
        checks["legacy_title_state_conflict_guard"] = "ok"

        malformed = Path(temp) / "malformed.csv"
        malformed.write_text("title,year\nA,2026,extra\n", encoding="utf-8")
        try:
            common.read_csv_rows(malformed)
        except ValueError as exc:
            assert "more fields than its header" in str(exc)
        else:
            raise AssertionError("malformed CSV row was silently accepted")
        short_row = Path(temp) / "short.csv"
        short_row.write_text("title,year,doi\nA,2026\n", encoding="utf-8")
        for reader_name, reader_fn in (("common", common.read_csv_rows), ("validator", _read_csv)):
            try:
                reader_fn(short_row)
            except ValueError as exc:
                assert "fewer fields than its header" in str(exc), reader_name
            else:
                raise AssertionError(f"{reader_name} CSV reader accepted a short row")
        duplicate_header = Path(temp) / "duplicate-header.csv"
        duplicate_header.write_text("title,Title,year\nA,B,2026\n", encoding="utf-8")
        for reader_name, reader_fn in (("common", common.read_csv_rows), ("validator", _read_csv)):
            try:
                reader_fn(duplicate_header)
            except ValueError as exc:
                assert "duplicate columns" in str(exc), reader_name
            else:
                raise AssertionError(f"{reader_name} CSV reader accepted duplicate headers")
        blank_header = Path(temp) / "blank-header.csv"
        blank_header.write_text("title,,year\nA,B,2026\n", encoding="utf-8")
        for reader_name, reader_fn in (("common", common.read_csv_rows), ("validator", _read_csv)):
            try:
                reader_fn(blank_header)
            except ValueError as exc:
                assert "blank columns" in str(exc), reader_name
            else:
                raise AssertionError(f"{reader_name} CSV reader accepted a blank header")
        try:
            _read_csv(malformed)
        except ValueError as exc:
            assert "more fields than its header" in str(exc)
        else:
            raise AssertionError("validator CSV reader accepted an extra column")

        unknown_schema = Path(temp) / "unknown-schema.csv"
        unknown_schema.write_text(
            "Title,Authors,Year,DOI,Unmapped\n"
            "Schema Guard,Doe,2026,10.1000/schema-guard,opaque\n",
            encoding="utf-8",
        )
        unknown_state = Path(temp) / "unknown-seen.txt"
        unknown_manifest = import_acm.run_import(
            unknown_schema,
            output_dir=Path(temp) / "unknown-run",
            run_id="unknown-schema",
            state_path=unknown_state,
            commit_state=True,
        )
        assert unknown_manifest["run_status"] == "degraded"
        assert unknown_manifest["state_committed"] is False
        assert unknown_manifest["state_commit_reason"] == "unknown_headers"
        assert not unknown_state.exists()
        acknowledged_state = Path(temp) / "acknowledged-seen.txt"
        acknowledged_manifest = import_acm.run_import(
            unknown_schema,
            output_dir=Path(temp) / "acknowledged-run",
            run_id="acknowledged-schema",
            state_path=acknowledged_state,
            commit_state=True,
            allow_unmapped_columns=True,
        )
        assert acknowledged_manifest["run_status"] == "degraded"
        assert acknowledged_manifest["state_committed"] is True
        assert acknowledged_manifest["state_commit_reason"] == "committed_with_unmapped_columns"
        assert acknowledged_state.exists()
        checks["unknown_schema_blocks_state_commit"] = "ok"

        output_dir = Path(temp) / "run"
        failure_state = Path(temp) / "failed-seen.txt"
        with patch.object(
            monitor,
            "collect",
            return_value=([row], [], [{"source": "OpenAlex", "error": "simulated"}]),
        ):
            failed_manifest = monitor.run_incremental(
                start,
                end,
                5,
                run_id="state-failure",
                output_dir=output_dir,
                state_path=failure_state,
                commit_state=True,
                include_openalex=True,
                include_semantic_scholar=False,
                include_arxiv=False,
                include_ieee=False,
                include_dblp=False,
                strict_credentials=False,
            )
        assert not failed_manifest["state_committed"] and not failure_state.exists()
        assert failed_manifest["run_status"] == "degraded"
        assert failed_manifest["state_commit_reason"] == "source_failures"

        with patch.object(monitor, "collect", return_value=([row], [], [])):
            ok_manifest = monitor.run_incremental(
                start,
                end,
                5,
                run_id="state-success",
                output_dir=output_dir,
                state_path=failure_state,
                commit_state=True,
                include_openalex=True,
                include_semantic_scholar=False,
                include_arxiv=False,
                include_ieee=False,
                include_dblp=False,
                strict_credentials=False,
            )
        assert ok_manifest["state_committed"] and failure_state.exists()
        assert ok_manifest["run_status"] == "ok"
        assert ok_manifest["state_commit_reason"] == "committed"

        try:
            monitor.run_incremental(end, start, 5, output_dir=output_dir)
        except ValueError as exc:
            assert "start date" in str(exc)
        else:
            raise AssertionError("inverted incremental date window was accepted")

        try:
            monitor.run_incremental(start, end, 0, output_dir=output_dir)
        except ValueError as exc:
            assert "limit" in str(exc)
        else:
            raise AssertionError("non-positive incremental limit was accepted")
    checks["state_commit_is_explicit"] = "ok"
    checks["csv_parse_failures_are_explicit"] = "ok"
    checks["runtime_parameter_validation"] = "ok"

    assert common.parse_date_info("2026-09") == (date(2026, 1, 1), False)
    assert common.date_matches_window("2026", start, end, allow_year_only=False)[0] is False
    previous_year_policy = monitor.CFG.get("include_current_year_only_dates")
    monitor.CFG["include_current_year_only_dates"] = False
    assert monitor._date_allowed("2026", start, end, True) == (False, "year")
    monitor.CFG["include_current_year_only_dates"] = True
    assert monitor._date_allowed("2026", start, end, True) == (True, "year")
    monitor.CFG["include_current_year_only_dates"] = previous_year_policy
    checks["date_precision_guard"] = "ok"

    oa_variants = list(common.query_variants(source="OpenAlex"))
    s2_variants = list(common.query_variants(source="Semantic Scholar"))
    assert len({item["id"] for item in oa_variants}) == len(oa_variants)
    assert len({item["id"] for item in s2_variants}) == len(s2_variants)
    assert len({item["query"] for item in oa_variants if item["parent_id"] == "Q1"}) >= 3
    assert len({item["query"] for item in s2_variants if item["parent_id"] == "Q1"}) >= 3
    assert any(
        oa["query"] != s2["query"]
        for oa, s2 in zip(oa_variants, s2_variants)
        if oa["id"] == s2["id"]
    )
    assert all(item["query"] != item["canonical"] for item in oa_variants if item["canonical"])
    assert not any(item["parent_id"] == "SEED" for item in common.query_variants(parent_ids={"Q1"}, include_seed=True))
    assert any(item["id"] == "Q6.v4" and item["backfill_only"] for item in common.query_variants())
    checks["provider_query_variants"] = "ok"

    gate_variant = {
        "gate": True,
        "required_groups": [["source sink", "source-to-sink"], ["vulnerability"]],
        "min_required_groups": 2,
    }
    assert common.variant_matches(
        {"title": "Source–Sink Reachability", "abstract": "vulnerability analysis"}, gate_variant
    )
    assert not common.variant_matches({"title": "Codec optimization", "abstract": ""}, gate_variant)
    checks["lexical_gate_boundaries"] = "ok"

    seed = "A Known Vulnerability Study"
    recall = seed_recall(
        [{"record_key": "exact", "title": seed}, {"record_key": "artifact", "title": seed + " Artifact Package"}],
        [seed],
    )[seed]
    assert recall["matched"] and any(item["match_kind"] == "near" for item in recall["matches"])
    checks["seed_exact_near_classification"] = "ok"

    observed_ids: list[str] = []

    def fake_query_source(query_id, query, *args, **kwargs):
        observed_ids.append(query_id)
        return []

    with patch.object(monitor, "openalex", side_effect=fake_query_source), patch.object(monitor, "_sleep_delay", return_value=None):
        monitor.collect(
            start,
            end,
            5,
            include_openalex=True,
            include_semantic_scholar=False,
            include_arxiv=False,
            include_ieee=False,
            include_dblp=False,
            parent_ids={"Q6"},
            include_foundational=False,
        )
    assert "Q6.v4" not in observed_ids
    observed_ids.clear()
    with patch.object(monitor, "openalex", side_effect=fake_query_source), patch.object(monitor, "_sleep_delay", return_value=None):
        monitor.collect(
            start,
            end,
            5,
            include_openalex=True,
            include_semantic_scholar=False,
            include_arxiv=False,
            include_ieee=False,
            include_dblp=False,
            parent_ids={"Q6"},
            include_foundational=True,
        )
    assert "Q6.v4" in observed_ids
    checks["foundation_lane_is_backfill_only"] = "ok"

    ieee_observed: list[str] = []
    old_ieee_key_for_routing = os.environ.get("IEEE_API_KEY")
    os.environ["IEEE_API_KEY"] = "self-test"

    def fake_ieee_routing(query_id, query, *args, **kwargs):
        ieee_observed.append(query_id)
        return []

    try:
        with patch.object(monitor, "ieee", side_effect=fake_ieee_routing), patch.object(monitor, "_sleep_delay", return_value=None):
            monitor.collect(
                start,
                end,
                5,
                include_openalex=False,
                include_semantic_scholar=False,
                include_arxiv=False,
                include_dblp=False,
                include_ieee=True,
                parent_ids={"Q6"},
                include_foundational=False,
            )
        assert "Q6.v4" not in ieee_observed
        ieee_observed.clear()
        with patch.object(monitor, "ieee", side_effect=fake_ieee_routing), patch.object(monitor, "_sleep_delay", return_value=None):
            monitor.collect(
                start,
                end,
                5,
                include_openalex=False,
                include_semantic_scholar=False,
                include_arxiv=False,
                include_dblp=False,
                include_ieee=True,
                parent_ids={"Q6"},
                include_foundational=True,
            )
        assert "Q6.v4" in ieee_observed
    finally:
        if old_ieee_key_for_routing is None:
            os.environ.pop("IEEE_API_KEY", None)
        else:
            os.environ["IEEE_API_KEY"] = old_ieee_key_for_routing
    checks["foundation_lane_applies_to_ieee"] = "ok"

    routed: dict[str, list[tuple[str, str]]] = {"oa": [], "s2": []}

    def fake_oa_route(query_id, query, *args, **kwargs):
        routed["oa"].append((query_id, query))
        return []

    def fake_s2_route(query_id, query, *args, **kwargs):
        routed["s2"].append((query_id, query))
        return []

    with patch.object(monitor, "openalex", side_effect=fake_oa_route), patch.object(monitor, "semantic_scholar", side_effect=fake_s2_route), patch.object(monitor, "_sleep_delay", return_value=None):
        monitor.collect(
            start,
            end,
            5,
            include_openalex=True,
            include_semantic_scholar=True,
            include_arxiv=False,
            include_ieee=False,
            include_dblp=False,
            parent_ids={"Q1"},
        )
    assert [item[0] for item in routed["oa"]] == [item[0] for item in routed["s2"]]
    assert any(left[1] != right[1] for left, right in zip(routed["oa"], routed["s2"]))
    checks["provider_query_routing"] = "ok"

    assert monitor.parse_source_selection("OpenAlex,s2,arxiv") == {
        "openalex",
        "semantic_scholar",
        "arxiv",
    }
    assert monitor.parse_query_selection(["q1,Q2", "Q3"]) == {
        "Q1",
        "Q2",
        "Q3",
    }
    checks["cli_scope_selection"] = "ok"

    credential_names = (
        "OPENALEX_API_KEY",
        "OPENALEX_POLITE_EMAIL",
        "S2_API_KEY",
        "IEEE_API_KEY",
    )
    old_credentials = {
        name: os.environ.pop(name, None) for name in credential_names
    }
    try:
        enabled = monitor.resolve_enabled_sources(
            include_openalex=True,
            include_semantic_scholar=True,
            include_arxiv=False,
            include_ieee=True,
            include_dblp=False,
        )
        strict_report = monitor.source_preflight(
            enabled, strict_credentials=True
        )
        assert {item["env_var"] for item in strict_report["errors"]} == {
            "OPENALEX_API_KEY",
            "S2_API_KEY",
            "IEEE_API_KEY",
        }
        loose_report = monitor.source_preflight(
            enabled, strict_credentials=False
        )
        assert [item["env_var"] for item in loose_report["errors"]] == [
            "IEEE_API_KEY"
        ]
    finally:
        for name, value in old_credentials.items():
            if value is not None:
                os.environ[name] = value
    checks["credential_preflight_is_non_secret_and_strict"] = "ok"

    old_ieee_key = os.environ.get("IEEE_API_KEY")
    os.environ["IEEE_API_KEY"] = "self-test"
    ieee_starts: list[int] = []
    ieee_windows: list[tuple[int, int]] = []

    def fake_ieee(url, *, params, **kwargs):
        ieee_starts.append(int(params["start_record"]))
        ieee_windows.append((int(params["start_year"]), int(params["end_year"])))
        if params["start_record"] == 1:
            articles = [
                {"article_number": f"I{index}", "title": chr(64 + index), "abstract": "", "publication_date": f"2026-09-{index:02d}", "publication_year": 2026, "authors": {"authors": []}}
                for index in range(1, 6)
            ]
        else:
            articles = [{"article_number": "I6", "title": "F", "abstract": "", "publication_date": "2026-09-06", "publication_year": 2026, "authors": {"authors": []}}, {"article_number": "I7", "title": "G", "abstract": "", "publication_date": "2026-09-07", "publication_year": 2026, "authors": {"authors": []}}]
        return _FakeResponse({"total_records": 7, "articles": articles})

    try:
        with patch.object(monitor, "_get_with_retry", side_effect=fake_ieee), patch.object(monitor, "_sleep_delay", return_value=None):
            rows = monitor.ieee("T.ieee", "test", start, end, 7, strict_dates=True)
    finally:
        if old_ieee_key is None:
            os.environ.pop("IEEE_API_KEY", None)
        else:
            os.environ["IEEE_API_KEY"] = old_ieee_key
    assert len(rows) == 7 and ieee_starts == [1, 6], (len(rows), ieee_starts)
    assert ieee_windows == [(2026, 2026)] * 2, ieee_windows
    checks["ieee_offset_pagination"] = "ok"

    previous_ieee_key = os.environ.pop("IEEE_API_KEY", None)
    try:
        rows, calls, failures = monitor.collect(
            start,
            end,
            2,
            include_openalex=False,
            include_semantic_scholar=False,
            include_arxiv=False,
            include_dblp=False,
            include_ieee=True,
            parent_ids={"Q4"},
            strict_dates=True,
        )
    finally:
        if previous_ieee_key is not None:
            os.environ["IEEE_API_KEY"] = previous_ieee_key
    assert not rows and len(failures) == 1
    assert all(item["error"].startswith("disabled:") for item in failures)
    assert len(calls) == 1 and calls[0]["status"] == "disabled"
    checks["enabled_source_unavailable_is_failure"] = "ok"

    dblp_offsets: list[int] = []

    def fake_dblp(url, *, params, **kwargs):
        offset = int(params["f"])
        dblp_offsets.append(offset)
        count = 3 if offset == 0 else 2
        hits = [
            {"info": {"key": f"conf/test/{offset + index}", "title": f"D{offset + index}", "year": "2026", "authors": {"author": [{"text": "Doe"}]}, "venue": "Venue"}}
            for index in range(count)
        ]
        return _FakeResponse({"result": {"hits": {"@total": "5", "hit": hits}}})

    with patch.object(monitor, "_get_with_retry", side_effect=fake_dblp), patch.object(monitor, "_sleep_delay", return_value=None):
        rows = monitor.dblp("T.dblp", "test", start, end, 5, strict_dates=False)
    assert len(rows) == 5 and dblp_offsets == [0, 3], (len(rows), dblp_offsets)
    checks["dblp_offset_pagination"] = "ok"

    ris_text = (
        "TY  - JOUR\n"
        "TI  - ACM Import Test\n"
        "AU  - Doe, Jane\n"
        "AU  - Roe, John\n"
        "AB  - First line\n"
        "  continued line\n"
        "PY  - 2024\n"
        "DO  - https://doi.org/10.1000/acm-test\n"
        "ER  - \n"
    )
    acm_rows, acm_issues = import_acm.parse_ris(ris_text)
    assert not acm_issues and len(acm_rows) == 1
    assert acm_rows[0]["authors"] == "Doe, Jane; Roe, John"
    assert "First line continued line" in acm_rows[0]["abstract"]
    assert common.split_authors("Doe, Jane") == ["Doe, Jane"]
    assert common.split_authors("Alice Smith, Bob Jones") == ["Alice Smith", "Bob Jones"]
    csv_rows, csv_issues, unknown = import_acm.parse_csv_export(
        "Title,Authors,Year,DOI,Unexpected\nACM CSV Test,Doe; Jane,2024,10.1000/csv-test,x\n"
    )
    assert len(csv_rows) == 1 and not csv_issues and unknown == ["Unexpected"]
    bib_rows, bib_issues = import_acm.parse_bibtex(
        '@inproceedings{acm1,\n title={ACM Bib Test},\n author={Doe and Jane},\n booktitle={Proceedings of the ACM Test Conference},\n year={2023},\n doi={10.1000/bib-test}\n}\n'
    )
    assert len(bib_rows) == 1 and not bib_issues and bib_rows[0]["doi"] == "10.1000/bib-test"
    assert bib_rows[0]["venue"] == "Proceedings of the ACM Test Conference"
    checks["acm_local_import_parsers"] = "ok"

    with tempfile.TemporaryDirectory() as temp:
        temp_dir = Path(temp)
        source_path = temp_dir / "acm.ris"
        source_path.write_text(ris_text, encoding="utf-8")
        imported_manifest = import_acm.run_import(
            source_path,
            output_dir=temp_dir / "normalized",
            run_id="acm-output-test",
            commit_state=False,
        )
        assert Path(imported_manifest["output"]).exists()
        assert imported_manifest["state_committed"] is False
        assert imported_manifest["run_status"] == "ok"
        assert imported_manifest["state_commit_reason"] == "dry_run"
        ris_out = temp_dir / "roundtrip.ris"
        export_ris.export(Path(imported_manifest["output"]), ris_out)
        assert "ACM Import Test" in ris_out.read_text(encoding="utf-8")
    checks["acm_output_roundtrip"] = "ok"

    backfill_calls: list[dict[str, Any]] = []

    def fake_backfill_collect(*args, **kwargs):
        backfill_calls.append(kwargs)
        return ([{"title": "Foundational", "publication_year": 2000, "publication_date": "2000", "authors": "A", "source_database": "OpenAlex"}], [], [])

    with tempfile.TemporaryDirectory() as temp:
        with patch.object(backfill, "collect", side_effect=fake_backfill_collect):
            manifest = backfill.run_backfill(
                2000,
                2,
                include_arxiv=False,
                include_openalex=True,
                include_semantic_scholar=False,
                include_ieee=False,
                include_dblp=False,
                output_dir=Path(temp),
                run_id="backfill-self-test",
                commit_state=False,
                strict_credentials=False,
            )
        assert len(backfill_calls) == 1
        assert backfill_calls[0]["parent_ids"] == {"Q6"}
        assert backfill_calls[0]["include_foundational"] is True
        assert not manifest["state_committed"]
        assert manifest["run_status"] == "ok"
        assert manifest["state_commit_reason"] == "dry_run"
    checks["foundational_backfill_window"] = "ok"

    arxiv_backfill_calls: list[dict[str, Any]] = []

    def fake_arxiv_backfill_collect(*args, **kwargs):
        arxiv_backfill_calls.append(kwargs)
        return (
            [
                {
                    "title": "OAI Backfill",
                    "publication_year": 2014,
                    "publication_date": "2014-01-01",
                    "authors": "A",
                    "source_database": "arXiv",
                    "arxiv_id": "1401.00001",
                }
            ],
            [],
            [],
        )

    with tempfile.TemporaryDirectory() as temp:
        output_dir = Path(temp)
        with patch.object(
            backfill, "collect", side_effect=fake_arxiv_backfill_collect
        ):
            arxiv_manifest = backfill.run_backfill(
                2014,
                2,
                start_year=2014,
                query_ids={"Q3"},
                include_arxiv=True,
                include_openalex=False,
                include_semantic_scholar=False,
                include_ieee=False,
                include_dblp=False,
                output_dir=output_dir,
                run_id="arxiv-oai-backfill-self-test",
                commit_state=False,
                strict_credentials=False,
            )
        assert len(arxiv_backfill_calls) == 1
        expected_cache = (
            output_dir
            / ".arxiv-oai-backfill-self-test.checkpoint"
            / "arxiv_oai_cache.sqlite3"
        )
        assert arxiv_backfill_calls[0]["arxiv_oai_cache_path"] == expected_cache
        assert arxiv_backfill_calls[0]["arxiv_oai_cache_start"] == date(2014, 1, 1)
        assert arxiv_backfill_calls[0]["arxiv_oai_set_specs"] == monitor.ARXIV_OAI_SETS
        assert arxiv_manifest["arxiv_transport"] == "OAI-PMH"
        assert Path(arxiv_manifest["arxiv_oai_cache"]) == expected_cache
    checks["backfill_routes_arxiv_through_oai"] = "ok"

    first_attempt_years: list[int] = []

    def fake_resumable_collect(start_date, end_date, *args, **kwargs):
        first_attempt_years.append(start_date.year)
        if start_date.year == 2015:
            return (
                [],
                [],
                [
                    {
                        "source": "arXiv",
                        "query_id": "Q1.A1",
                        "error": "simulated 429",
                        "code": "CIRCUIT_OPEN",
                    }
                ],
            )
        return (
            [
                {
                    "title": f"Checkpoint {start_date.year}",
                    "publication_year": start_date.year,
                    "publication_date": str(start_date.year),
                    "authors": "A",
                    "source_database": "OpenAlex",
                }
            ],
            [],
            [],
        )

    with tempfile.TemporaryDirectory() as temp:
        resume_dir = Path(temp) / "resume"
        with patch.object(
            backfill, "collect", side_effect=fake_resumable_collect
        ):
            interrupted = backfill.run_backfill(
                2015,
                2,
                start_year=2014,
                query_ids={"Q1"},
                include_arxiv=False,
                include_openalex=True,
                include_semantic_scholar=False,
                include_ieee=False,
                include_dblp=False,
                output_dir=resume_dir,
                run_id="resume-self-test",
                commit_state=False,
                strict_credentials=False,
            )
        assert first_attempt_years == [2014, 2015]
        assert interrupted["run_status"] == "degraded"
        assert interrupted["completed_tasks"] == 1
        assert interrupted["remaining_tasks"] == 1
        assert interrupted["resumable"] is True

        resumed_years: list[int] = []

        def fake_resume_success(start_date, end_date, *args, **kwargs):
            resumed_years.append(start_date.year)
            return (
                [
                    {
                        "title": f"Checkpoint {start_date.year}",
                        "publication_year": start_date.year,
                        "publication_date": str(start_date.year),
                        "authors": "A",
                        "source_database": "OpenAlex",
                    }
                ],
                [],
                [],
            )

        with patch.object(
            backfill, "collect", side_effect=fake_resume_success
        ):
            resumed = backfill.run_backfill(
                2015,
                2,
                start_year=2014,
                query_ids={"Q1"},
                include_arxiv=False,
                include_openalex=True,
                include_semantic_scholar=False,
                include_ieee=False,
                include_dblp=False,
                output_dir=resume_dir,
                run_id="resume-self-test",
                commit_state=False,
                resume=True,
                strict_credentials=False,
            )
        assert resumed_years == [2015]
        assert resumed["run_status"] == "ok"
        assert resumed["completed_tasks"] == 2
        assert resumed["remaining_tasks"] == 0
        assert resumed["source_failures"] == []
        assert resumed["resumable"] is False
        assert resumed["state_commit_reason"] == "dry_run"
        assert Path(resumed["output"]).exists()
        assert len(_read_csv(Path(resumed["output"]))) == 2
    checks["backfill_checkpoint_resume"] = "ok"

    source_attempts: list[tuple[str, int]] = []

    def fake_source_checkpoint_collect(start_date, end_date, *args, **kwargs):
        source_key = next(
            key
            for key in monitor.SOURCE_KEYS
            if kwargs.get(f"include_{key}") is True
        )
        source_attempts.append((source_key, start_date.year))
        if source_key == "semantic_scholar" and start_date.year == 2014 and len(source_attempts) == 2:
            return (
                [],
                [],
                [
                    {
                        "source": "Semantic Scholar",
                        "query_id": "Q1.v1",
                        "error": "simulated 429",
                        "code": "RATE_LIMITED",
                    }
                ],
            )
        return (
            [
                {
                    "title": f"{source_key}-{start_date.year}",
                    "publication_year": start_date.year,
                    "publication_date": f"{start_date.year}-01-01",
                    "authors": "A",
                    "source_database": source_key,
                }
            ],
            [],
            [],
        )

    with tempfile.TemporaryDirectory() as temp:
        source_resume_dir = Path(temp) / "source-resume"
        with patch.object(
            backfill, "collect", side_effect=fake_source_checkpoint_collect
        ):
            interrupted = backfill.run_backfill(
                2015,
                2,
                start_year=2014,
                query_ids={"Q1"},
                include_arxiv=False,
                include_openalex=True,
                include_semantic_scholar=True,
                include_ieee=False,
                include_dblp=False,
                output_dir=source_resume_dir,
                run_id="source-resume-self-test",
                commit_state=False,
                strict_credentials=False,
            )
        assert interrupted["completed_tasks"] == 1
        assert interrupted["remaining_tasks"] == 3
        assert interrupted["task_granularity"] == "source_query_year"
        assert source_attempts == [("openalex", 2014), ("semantic_scholar", 2014)]
        with patch.object(
            backfill, "collect", side_effect=fake_source_checkpoint_collect
        ):
            resumed = backfill.run_backfill(
                2015,
                2,
                start_year=2014,
                query_ids={"Q1"},
                include_arxiv=False,
                include_openalex=True,
                include_semantic_scholar=True,
                include_ieee=False,
                include_dblp=False,
                output_dir=source_resume_dir,
                run_id="source-resume-self-test",
                commit_state=False,
                resume=True,
                strict_credentials=False,
            )
        assert resumed["run_status"] == "ok"
        assert resumed["completed_tasks"] == 4
        assert resumed["remaining_tasks"] == 0
        assert source_attempts == [
            ("openalex", 2014),
            ("semantic_scholar", 2014),
            ("semantic_scholar", 2014),
            ("openalex", 2015),
            ("semantic_scholar", 2015),
        ]
        assert len(_read_csv(Path(resumed["output"]))) == 4
    checks["source_level_checkpoint_resume"] = "ok"

    deferred_attempts: list[tuple[str, int]] = []

    def fake_deferred_source_collect(start_date, end_date, *args, **kwargs):
        source_key = next(
            key
            for key in monitor.SOURCE_KEYS
            if kwargs.get(f"include_{key}") is True
        )
        deferred_attempts.append((source_key, start_date.year))
        if source_key == "semantic_scholar":
            return (
                [],
                [],
                [
                    {
                        "source": "Semantic Scholar",
                        "query_id": "Q1.v1",
                        "error": "simulated quota limit",
                        "code": "RATE_LIMITED",
                    }
                ],
            )
        return (
            [
                {
                    "title": f"{source_key}-{start_date.year}",
                    "publication_year": start_date.year,
                    "publication_date": f"{start_date.year}-01-01",
                    "authors": "A",
                    "source_database": source_key,
                }
            ],
            [],
            [],
        )

    with tempfile.TemporaryDirectory() as temp:
        deferred_dir = Path(temp) / "deferred-source"
        with patch.object(
            backfill, "collect", side_effect=fake_deferred_source_collect
        ):
            deferred = backfill.run_backfill(
                2015,
                2,
                start_year=2014,
                query_ids={"Q1"},
                include_arxiv=False,
                include_openalex=True,
                include_semantic_scholar=True,
                include_ieee=False,
                include_dblp=False,
                output_dir=deferred_dir,
                run_id="deferred-source-self-test",
                commit_state=False,
                strict_credentials=False,
                defer_failed_sources=True,
            )
        assert deferred_attempts == [
            ("openalex", 2014),
            ("semantic_scholar", 2014),
            ("openalex", 2015),
        ]
        assert deferred["run_status"] == "degraded"
        assert deferred["completed_tasks"] == 2
        assert deferred["remaining_tasks"] == 2
        assert deferred["deferred_sources"] == ["semantic_scholar"]
        assert len(deferred["source_failures"]) == 1

        with patch.object(
            backfill, "collect", side_effect=fake_resume_success
        ):
            resumed = backfill.run_backfill(
                2015,
                2,
                start_year=2014,
                query_ids={"Q1"},
                include_arxiv=False,
                include_openalex=True,
                include_semantic_scholar=True,
                include_ieee=False,
                include_dblp=False,
                output_dir=deferred_dir,
                run_id="deferred-source-self-test",
                commit_state=False,
                resume=True,
                strict_credentials=False,
            )
        assert resumed["run_status"] == "ok"
        assert resumed["completed_tasks"] == 4
        assert resumed["remaining_tasks"] == 0
        assert resumed["source_failures"] == []
    checks["backfill_defer_failed_source"] = "ok"

    with tempfile.TemporaryDirectory() as temp:
        runs_dir = Path(temp)
        target_end = date(2026, 9, 21)
        successful = {
            "mode": "incremental",
            "run_status": "ok",
            "state_committed": True,
            "end": target_end.isoformat(),
        }
        (runs_dir / "successful.json").write_text(
            json.dumps(successful), encoding="utf-8"
        )
        skipped = recovery_gate.decide(
            "schedule",
            recovery_gate.RECOVERY_CRON,
            today=date(2026, 9, 22),
            runs_dir=runs_dir,
            settings={"incremental_lookback_days": 14},
        )
        assert skipped["should_run"] is False
        (runs_dir / "successful.json").unlink()
        old_success = dict(successful, end="2026-09-14")
        (runs_dir / "old.json").write_text(
            json.dumps(old_success), encoding="utf-8"
        )
        retry = recovery_gate.decide(
            "schedule",
            recovery_gate.RECOVERY_CRON,
            today=date(2026, 9, 22),
            runs_dir=runs_dir,
            settings={"incremental_lookback_days": 14},
        )
        assert retry["should_run"] is True
        assert retry["start"] == "2026-09-07" and retry["end"] == "2026-09-21"
        manual = recovery_gate.decide("workflow_dispatch", "", today=date(2026, 9, 22), runs_dir=runs_dir)
        assert manual["should_run"] is True and manual["recovery"] is False
    checks["weekly_recovery_gate"] = "ok"
    return {"status": "pass", "checks": checks}


def _parse_date(value: str) -> date:
    parsed, exact = common.parse_date_info(value)
    if not parsed or not exact:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD: {value}")
    return parsed


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate retrieval pagination, recall and 14-day deltas.")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--candidate", type=Path, help="candidate CSV from a dry-run")
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--seed-file", type=Path, default=common.ROOT / "config" / "seed_titles.txt")
    parser.add_argument("--report", type=Path)
    parser.add_argument("--run-live", action="store_true", help="run a non-committing 14-day retrieval")
    parser.add_argument("--seed-probe-live", action="store_true", help="run exact-title probes over a broad window")
    parser.add_argument("--start-date", type=_parse_date)
    parser.add_argument("--end-date", type=_parse_date)
    parser.add_argument("--seed-start-date", type=_parse_date, default=date(2000, 1, 1))
    parser.add_argument("--seed-end-date", type=_parse_date)
    parser.add_argument("--seed-limit", type=int, default=100)
    parser.add_argument("--include-seed-queries", action="store_true")
    parser.add_argument("--limit", type=int, default=int(common.CFG.get("max_results_per_short_query", 100)))
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--sources", help="comma-separated source list")
    parser.add_argument("--query-id", action="append", help="limit live retrieval to query families")
    parser.add_argument("--enable-dblp", action="store_true")
    parser.add_argument("--no-dblp", action="store_true")
    parser.add_argument("--enable-ieee", action="store_true")
    parser.add_argument("--no-ieee", action="store_true")
    parser.add_argument("--no-arxiv", action="store_true")
    parser.add_argument("--no-openalex", action="store_true")
    parser.add_argument("--no-semantic-scholar", action="store_true")
    parser.add_argument("--allow-year-only-dates", action="store_true")
    parser.add_argument(
        "--http-max-retries",
        type=int,
        help="temporarily override retry count for a bounded live validation",
    )
    parser.add_argument(
        "--http-timeout",
        type=int,
        help="temporarily override request timeout seconds for a bounded live validation",
    )
    args = parser.parse_args()

    if args.http_max_retries is not None:
        if args.http_max_retries < 0:
            parser.error("--http-max-retries must be non-negative")
        monitor.CFG["http_max_retries"] = args.http_max_retries
        for policy in monitor.CFG.get("request_policies", {}).values():
            policy["max_retries"] = args.http_max_retries
    if args.http_timeout is not None:
        if args.http_timeout < 1:
            parser.error("--http-timeout must be positive")
        monitor.CFG["http_timeout_seconds"] = args.http_timeout
        for policy in monitor.CFG.get("request_policies", {}).values():
            policy["timeout_seconds"] = args.http_timeout

    if args.self_test:
        result = run_self_tests()
    else:
        legacy_source_flags = any(
            (
                args.enable_dblp,
                args.no_dblp,
                args.enable_ieee,
                args.no_ieee,
                args.no_arxiv,
                args.no_openalex,
                args.no_semantic_scholar,
            )
        )
        if args.sources and legacy_source_flags:
            parser.error("--sources cannot be combined with source enable/disable flags")
        if args.enable_dblp and args.no_dblp:
            parser.error("--enable-dblp and --no-dblp are mutually exclusive")
        if args.enable_ieee and args.no_ieee:
            parser.error("--enable-ieee and --no-ieee are mutually exclusive")
        try:
            selected_sources = (
                monitor.parse_source_selection(args.sources) if args.sources else None
            )
            parent_ids = monitor.parse_query_selection(args.query_id)
        except ValueError as exc:
            parser.error(str(exc))
        if selected_sources is not None:
            source_overrides = {
                key: key in selected_sources for key in monitor.SOURCE_KEYS
            }
        else:
            source_overrides = {
                "openalex": False if args.no_openalex else None,
                "semantic_scholar": False if args.no_semantic_scholar else None,
                "arxiv": False if args.no_arxiv else None,
                "ieee": False if args.no_ieee else True if args.enable_ieee else None,
                "dblp": False if args.no_dblp else True if args.enable_dblp else None,
            }
        seeds = common.read_seed_titles(args.seed_file)
        candidate_path = args.candidate
        manifest = None
        if args.manifest:
            manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        if args.run_live:
            start, end = args.start_date, args.end_date
            if start is None or end is None:
                start, end = common.date_window()
            run_id = common.make_run_id("validation")
            out_dir = args.output_dir or (common.ROOT / "data" / "validation" / run_id)
            os.environ.setdefault("LIT_MONITOR_NO_SLEEP", "")
            try:
                manifest = monitor.run_incremental(
                    start,
                    end,
                    max(1, args.limit),
                    run_id=run_id,
                    output_dir=out_dir,
                    commit_state=False,
                    include_seed_queries=args.include_seed_queries,
                    include_dblp=source_overrides["dblp"],
                    include_ieee=source_overrides["ieee"],
                    include_openalex=source_overrides["openalex"],
                    include_semantic_scholar=source_overrides["semantic_scholar"],
                    strict_dates=not args.allow_year_only_dates,
                    include_arxiv=source_overrides["arxiv"],
                    parent_ids=parent_ids,
                )
            except monitor.SourceConfigurationError as exc:
                parser.error(str(exc))
            candidate_path = Path(manifest["output_files"][0])
        if (args.run_live or args.candidate) and candidate_path and not candidate_path.exists():
            parser.error(f"candidate CSV does not exist: {candidate_path}")
        if not candidate_path and not args.seed_probe_live:
            parser.error("provide --candidate or use --run-live")
        validation_start, validation_end = args.start_date, args.end_date
        if manifest is not None:
            if validation_start is None:
                validation_start, _ = common.parse_date_info(manifest.get("start", ""))
            if validation_end is None:
                validation_end, _ = common.parse_date_info(manifest.get("end", ""))
        if candidate_path:
            candidate_rows = _read_csv(candidate_path)
            if args.baseline:
                baseline_rows = _read_csv(args.baseline)
                result = compare_rows(
                    baseline_rows,
                    candidate_rows,
                    seeds,
                    start=validation_start,
                    end=validation_end,
                )
            else:
                result = {"candidate": summarize_rows(candidate_rows), "seed_recall_candidate": seed_recall(candidate_rows, seeds)}
            result["candidate_validation"] = validate_rows(
                candidate_rows,
                expected_fields=common.FIELDS,
                start=validation_start,
                end=validation_end,
                manifest=manifest,
            )
        else:
            result = {}
        if manifest is not None:
            result["manifest"] = {
                "run_id": manifest.get("run_id"),
                "raw_rows": manifest.get("raw_rows"),
                "unique_rows": manifest.get("unique_rows"),
                "source_failures": manifest.get("source_failures", []),
                "run_status": manifest.get("run_status"),
                "source_calls": manifest.get("source_calls", []),
                "state_committed": manifest.get("state_committed"),
                "date_precision_counts": manifest.get("date_precision_counts", {}),
                "include_foundational": manifest.get("include_foundational"),
                "source_preflight": manifest.get("source_preflight", {}),
                "provider_runtime": manifest.get("provider_runtime", {}),
            }
        if args.seed_probe_live:
            seed_end = args.seed_end_date or common.local_today()
            seed_rows, seed_meta = run_seed_probe(
                args.seed_start_date,
                seed_end,
                max(1, args.seed_limit),
                include_dblp=bool(source_overrides["dblp"]),
                include_ieee=bool(source_overrides["ieee"]),
                include_openalex=source_overrides["openalex"] is not False,
                include_semantic_scholar=source_overrides["semantic_scholar"] is not False,
            )
            result["seed_probe"] = {
                "window": {"start": args.seed_start_date.isoformat(), "end": seed_end.isoformat()},
                "summary": summarize_rows(seed_rows),
                "recall": seed_recall(seed_rows, seeds),
                "meta": seed_meta,
            }
        failures = list((manifest or {}).get("source_failures", []) or [])
        seed_failures = list((result.get("seed_probe", {}).get("meta", {}).get("source_failures", []) or []))
        validation_failed = bool(
            result.get("candidate_validation")
            and not result["candidate_validation"].get("all_checks_pass", False)
        )
        if validation_failed or (manifest or {}).get("run_status") == "failed":
            result["status"] = "fail"
        elif failures or seed_failures:
            result["status"] = "degraded"
        else:
            result["status"] = "pass"

    payload = json.dumps(result, ensure_ascii=False, indent=2)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(payload + "\n", encoding="utf-8")
    print(payload)
    if result.get("status") != "pass":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
