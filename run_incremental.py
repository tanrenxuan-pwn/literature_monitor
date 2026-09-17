from __future__ import annotations

import argparse
import json
import os
import re
import socket
import sqlite3
import tempfile
import threading
import time
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from contextlib import closing, contextmanager
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

import requests

from common import (
    CFG,
    FIELDS,
    MANIFEST_DIR as COMMON_MANIFEST_DIR,
    QUERIES,
    ROOT,
    STATE as COMMON_STATE,
    all_seen_aliases,
    abstract_from_inverted,
    arxiv_variants,
    commit_seen_keys,
    date_matches_window,
    date_window,
    dedupe_aliases,
    is_arxiv_doi,
    local_today,
    make_key,
    make_run_id,
    now_iso,
    norm_arxiv_id,
    norm_doi,
    norm_title,
    norm_text,
    parse_date_info,
    parse_year_safe,
    query_variants,
    query_for_source,
    read_seen_keys,
    split_authors,
    strong_identity_aliases,
    record_completeness,
    row_is_seen,
    row_year,
    variant_matches,
    write_csv,
    write_json,
    year_in_window,
)


NORM = ROOT / "data" / "normalized"
STATE = COMMON_STATE
MANIFEST_DIR = COMMON_MANIFEST_DIR
RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}
SOURCE_KEYS = ("openalex", "semantic_scholar", "arxiv", "ieee", "dblp")
SOURCE_LABELS = {
    "openalex": "OpenAlex",
    "semantic_scholar": "Semantic Scholar",
    "arxiv": "arXiv",
    "arxiv_oai": "arXiv OAI-PMH",
    "ieee": "IEEE Xplore",
    "dblp": "DBLP",
}
SOURCE_API_KEY_ENV = {
    "openalex": "OPENALEX_API_KEY",
    "semantic_scholar": "S2_API_KEY",
    "ieee": "IEEE_API_KEY",
}
SOURCE_INPUT_ALIASES = {
    "openalex": "openalex",
    "semantic_scholar": "semantic_scholar",
    "semantic-scholar": "semantic_scholar",
    "semanticscholar": "semantic_scholar",
    "s2": "semantic_scholar",
    "arxiv": "arxiv",
    "ieee": "ieee",
    "ieee_xplore": "ieee",
    "ieee-xplore": "ieee",
    "dblp": "dblp",
}
HOST_PROVIDERS = {
    "api.openalex.org": "openalex",
    "api.semanticscholar.org": "semantic_scholar",
    "export.arxiv.org": "arxiv",
    "arxiv.org": "arxiv",
    "oaipmh.arxiv.org": "arxiv_oai",
    "ieeexploreapi.ieee.org": "ieee",
    "dblp.org": "dblp",
}

ARXIV_OAI_URL = "https://oaipmh.arxiv.org/oai"
ARXIV_OAI_NAMESPACES = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "arxiv": "http://arxiv.org/OAI/arXiv/",
}
ARXIV_OAI_SETS = ("cs:cs:CR", "cs:cs:SE", "cs:cs:AI")
_ARXIV_OAI_READY: set[tuple[str, str, str]] = set()
_DNS_ROTATION_LOCK = threading.Lock()
_OAI_DNS_PREFERRED: dict[str, str] = {}

_PROVIDER_RUNTIME: dict[str, dict[str, Any]] = {}


@contextmanager
def _locked_rate_state(provider: str):
    configured_dir = os.getenv("LIT_MONITOR_RATE_LOCK_DIR", "").strip()
    lock_dir = (
        Path(configured_dir)
        if configured_dir
        else Path(tempfile.gettempdir()) / "security-literature-monitor-rate-limits"
    )
    lock_dir.mkdir(parents=True, exist_ok=True)
    path = lock_dir / f"{provider}.lock"
    with path.open("a+b") as handle:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0\n")
            handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield handle
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield handle
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class ProviderPartialError(RuntimeError):
    """Carry records fetched before a later provider page failed."""

    def __init__(self, cause: Exception, rows: list[dict[str, Any]]):
        self.cause = cause
        self.rows = rows
        super().__init__(f"{type(cause).__name__}: {cause}")


class ProviderCircuitOpenError(RuntimeError):
    """Stop a throttled source so the backfill can resume from a checkpoint."""

    def __init__(self, provider: str, cooldown_seconds: float):
        self.provider = provider
        self.cooldown_seconds = cooldown_seconds
        self.retry_not_before = (
            datetime.now(timezone.utc) + timedelta(seconds=cooldown_seconds)
        ).replace(microsecond=0).isoformat()
        label = SOURCE_LABELS.get(provider, provider)
        super().__init__(
            f"{label} rate-limit circuit opened; retry after at least "
            f"{int(round(cooldown_seconds))} seconds "
            f"(not before {self.retry_not_before})"
        )


class ProviderHTTPStatusError(RuntimeError):
    """HTTP failure rendered without credential-bearing request URLs."""

    def __init__(
        self,
        provider: str,
        status_code: int,
        *,
        detail: str = "",
        retry_after_seconds: float | None = None,
    ):
        self.provider = provider
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds
        label = SOURCE_LABELS.get(provider, provider)
        message = f"{label} returned HTTP {status_code}"
        if detail:
            message += f"; response={detail}"
        if retry_after_seconds is not None:
            message += f"; retry after at least {int(round(retry_after_seconds))} seconds"
        super().__init__(message)


class ProviderInvalidResponseError(RuntimeError):
    """A nominally successful provider response was empty or malformed."""

    def __init__(
        self,
        provider: str,
        expected_format: str,
        *,
        status_code: int,
        content_type: str,
        detail: str,
    ):
        self.provider = provider
        self.expected_format = expected_format
        self.status_code = status_code
        label = SOURCE_LABELS.get(provider, provider)
        message = (
            f"{label} returned invalid {expected_format} "
            f"(HTTP {status_code}, Content-Type={content_type or 'missing'})"
        )
        if detail:
            message += f"; response={detail}"
        super().__init__(message)


class SourceConfigurationError(RuntimeError):
    """Raised before network access when an enabled source is not runnable."""

    def __init__(self, report: dict[str, Any]):
        self.report = report
        details = "; ".join(
            f"{item['source']}: missing {item['env_var']}"
            for item in report.get("errors", [])
        )
        super().__init__(f"source preflight failed: {details}")


def resolve_enabled_sources(
    *,
    include_arxiv: bool | None = None,
    include_ieee: bool | None = None,
    include_dblp: bool | None = None,
    include_openalex: bool | None = None,
    include_semantic_scholar: bool | None = None,
) -> dict[str, bool]:
    configured = CFG.get("sources", {})

    def enabled(name: str, override: bool | None) -> bool:
        if override is not None:
            return bool(override)
        return bool(configured.get(name, {}).get("enabled", False))

    return {
        "openalex": enabled("openalex", include_openalex),
        "semantic_scholar": enabled("semantic_scholar", include_semantic_scholar),
        "arxiv": enabled("arxiv", include_arxiv),
        "ieee": enabled("ieee", include_ieee),
        "dblp": enabled("dblp", include_dblp),
    }


def source_preflight(
    enabled_sources: dict[str, bool],
    *,
    strict_credentials: bool,
) -> dict[str, Any]:
    """Validate credentials without exposing their values or contacting APIs."""
    configured = CFG.get("sources", {})
    credentials = {
        env_name: bool(os.getenv(env_name, "").strip())
        for env_name in (
            "OPENALEX_API_KEY",
            "OPENALEX_POLITE_EMAIL",
            "S2_API_KEY",
            "IEEE_API_KEY",
        )
    }
    errors: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    for source, env_name in SOURCE_API_KEY_ENV.items():
        if not enabled_sources.get(source, False):
            continue
        required = source == "ieee" or (
            strict_credentials
            and bool(configured.get(source, {}).get("require_api_key", False))
        )
        if not credentials[env_name]:
            item = {
                "source": SOURCE_LABELS[source],
                "code": "MISSING_CREDENTIAL",
                "env_var": env_name,
            }
            (errors if required else warnings).append(item)
    if enabled_sources.get("openalex") and not credentials["OPENALEX_POLITE_EMAIL"]:
        warnings.append(
            {
                "source": "OpenAlex",
                "code": "MISSING_POLITE_EMAIL",
                "env_var": "OPENALEX_POLITE_EMAIL",
            }
        )
    return {
        "checked_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "strict_credentials": strict_credentials,
        "enabled_sources": {
            SOURCE_LABELS[key]: bool(enabled_sources.get(key, False))
            for key in SOURCE_KEYS
        },
        "credentials_configured": credentials,
        "errors": errors,
        "warnings": warnings,
    }


def ensure_source_preflight(
    enabled_sources: dict[str, bool],
    *,
    strict_credentials: bool = True,
) -> dict[str, Any]:
    report = source_preflight(
        enabled_sources, strict_credentials=strict_credentials
    )
    if report["errors"]:
        raise SourceConfigurationError(report)
    return report


def _provider_policy(provider: str) -> dict[str, float | int]:
    defaults: dict[str, float | int] = {
        "min_interval_seconds": 0.0,
        "initial_delay_seconds": 0.0,
        "timeout_seconds": int(CFG.get("http_timeout_seconds", 60)),
        "max_retries": int(CFG.get("http_max_retries", 4)),
        "rate_limit_backoff_seconds": 5.0,
        "invalid_response_backoff_seconds": 2.0,
        "max_backoff_seconds": 120.0,
        "circuit_breaker_threshold": 3,
        "circuit_cooldown_seconds": 120.0,
    }
    configured = (CFG.get("request_policies", {}) or {}).get(provider, {}) or {}
    defaults.update(configured)
    return defaults


def reset_provider_runtime_state() -> None:
    _PROVIDER_RUNTIME.clear()
    with _DNS_ROTATION_LOCK:
        _OAI_DNS_PREFERRED.clear()


def _provider_state(provider: str) -> dict[str, Any]:
    state = _PROVIDER_RUNTIME.get(provider)
    if state is None:
        initial_delay = max(
            0.0,
            float(_provider_policy(provider).get("initial_delay_seconds", 0.0)),
        )
        state = {
            "last_request_started": 0.0,
            "not_before": time.monotonic() + initial_delay,
            "circuit_open_until": 0.0,
            "consecutive_429": 0,
            "requests": 0,
            "retries": 0,
            "rate_limit_events": 0,
            "invalid_response_events": 0,
            "access_denied_events": 0,
            "circuit_trips": 0,
            "throttle_wait_seconds": 0.0,
            "backoff_wait_seconds": 0.0,
            "dns_rotation_attempts": 0,
        }
        _PROVIDER_RUNTIME[provider] = state
    return state


def provider_runtime_snapshot() -> dict[str, dict[str, Any]]:
    now = time.monotonic()
    result: dict[str, dict[str, Any]] = {}
    for provider, state in _PROVIDER_RUNTIME.items():
        result[SOURCE_LABELS.get(provider, provider)] = {
            "requests": int(state["requests"]),
            "retries": int(state["retries"]),
            "rate_limit_events": int(state["rate_limit_events"]),
            "invalid_response_events": int(state["invalid_response_events"]),
            "access_denied_events": int(state["access_denied_events"]),
            "circuit_trips": int(state["circuit_trips"]),
            "throttle_wait_seconds": round(float(state["throttle_wait_seconds"]), 3),
            "backoff_wait_seconds": round(float(state["backoff_wait_seconds"]), 3),
            "dns_rotation_attempts": int(state["dns_rotation_attempts"]),
            "circuit_remaining_seconds": round(
                max(0.0, float(state["circuit_open_until"]) - now), 3
            ),
        }
    return result


def _redact_sensitive_text(value: Any) -> str:
    message = str(value)
    for env_name in SOURCE_API_KEY_ENV.values():
        secret = os.getenv(env_name, "").strip()
        if secret:
            message = message.replace(secret, "<redacted>")
    message = re.sub(
        r"([?&](?:api[_-]?key|apikey|token)=)[^&\s]+",
        r"\1<redacted>",
        message,
        flags=re.I,
    )
    return re.sub(
        r'((?:api[_-]?key|apikey|x-api-key|token)["\'\s:=]+)[^,;\s"\']+',
        r"\1<redacted>",
        message,
        flags=re.I,
    )


def _safe_error_message(exc: Exception) -> str:
    """Render provider errors without leaking credentials from request URLs."""
    return _redact_sensitive_text(f"{type(exc).__name__}: {exc}")


def _response_detail(response: requests.Response, limit: int = 240) -> str:
    raw = getattr(response, "text", None)
    if raw is None:
        content = getattr(response, "content", b"") or b""
        raw = content.decode("utf-8", errors="replace")
    compact = " ".join(str(raw).split())
    return _redact_sensitive_text(compact[:limit])


def _date_allowed(
    value: Any,
    start: date,
    end: date,
    strict_dates: bool | None,
) -> tuple[bool, str]:
    """Apply exact-date filtering with an explicit year-only fallback."""
    parsed, exact = parse_date_info(value)
    if parsed is None:
        return False, "unknown"
    if exact:
        return start <= parsed <= end, "day"
    broad_window = (end - start).days >= int(CFG.get("year_only_date_window_days", 120))
    if strict_dates is False or (strict_dates is None and broad_window):
        return start.year <= parsed.year <= end.year, "year"
    # S2/DBLP sometimes expose only the current publication year.  Keep those
    # rows for recall in a narrow run, but retain the precision marker so the
    # downstream audit can distinguish them from day-confirmed rows.
    if bool(CFG.get("include_current_year_only_dates", True)) and parsed.year == end.year:
        return True, "year"
    return False, "year"


def _set_date_meta(row: dict[str, Any], precision: str) -> dict[str, Any]:
    # Underscore-prefixed fields are retained in manifests/tests but ignored
    # by the stable 22-column CSV contract.
    row["_date_precision"] = precision
    return row


def _retry_after(
    response: requests.Response,
    fallback: float,
    maximum: float,
) -> float:
    value = response.headers.get("Retry-After", "")
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        try:
            retry_at = parsedate_to_datetime(value)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            parsed = (retry_at - datetime.now(timezone.utc)).total_seconds()
        except (TypeError, ValueError, OverflowError):
            parsed = fallback
    return max(0.0, min(maximum, max(fallback, parsed)))


def _provider_from_url(url: str) -> str:
    host = (urlparse(url).hostname or "").casefold()
    return HOST_PROVIDERS.get(host, host or "unknown")


def _wait_for_provider(provider: str) -> None:
    state = _provider_state(provider)
    now = time.monotonic()
    remaining = float(state["circuit_open_until"]) - now
    if remaining > 0:
        raise ProviderCircuitOpenError(provider, remaining)
    interval = max(
        0.0, float(_provider_policy(provider).get("min_interval_seconds", 0.0))
    )
    elapsed = now - float(state["last_request_started"])
    interval_wait = (
        max(0.0, interval - elapsed) if state["last_request_started"] else 0.0
    )
    initial_wait = max(0.0, float(state.get("not_before", 0.0)) - now)
    wait_seconds = max(interval_wait, initial_wait)
    if wait_seconds:
        state["throttle_wait_seconds"] += wait_seconds
        _sleep_delay(wait_seconds)
    state["not_before"] = 0.0

    # Coordinate the request-start interval across separate CLI processes.
    # This is required for provider quotas such as Semantic Scholar's
    # cumulative one-request-per-second limit.
    with _locked_rate_state(provider) as handle:
        handle.seek(0)
        try:
            last_shared_start = float(handle.read().decode("ascii").strip() or "0")
        except (UnicodeDecodeError, ValueError):
            last_shared_start = 0.0
        shared_wait = max(0.0, interval - (time.time() - last_shared_start))
        if shared_wait:
            state["throttle_wait_seconds"] += shared_wait
            _sleep_delay(shared_wait)
        handle.seek(0)
        handle.truncate()
        handle.write(f"{time.time():.9f}\n".encode("ascii"))
        handle.flush()
        os.fsync(handle.fileno())
    state["last_request_started"] = time.monotonic()


def _invalid_response_error(
    response: requests.Response,
    provider: str,
    expected_format: str,
) -> ProviderInvalidResponseError | None:
    content_type = str(response.headers.get("Content-Type", ""))
    content = getattr(response, "content", b"") or b""
    try:
        if expected_format == "json":
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("top-level JSON value is not an object")
        elif expected_format == "xml":
            if not content:
                raise ValueError("empty response body")
            ET.fromstring(content)
        else:
            raise ValueError(f"unsupported expected response format: {expected_format}")
    except (ET.ParseError, TypeError, ValueError) as exc:
        detail = _response_detail(response) or f"<{type(exc).__name__}: {exc}>"
        return ProviderInvalidResponseError(
            provider,
            expected_format,
            status_code=int(response.status_code),
            content_type=content_type,
            detail=detail,
        )
    return None


def _http_status_error(
    response: requests.Response,
    provider: str,
    *,
    retry_after_seconds: float | None = None,
) -> ProviderHTTPStatusError:
    return ProviderHTTPStatusError(
        provider,
        int(response.status_code),
        detail=_response_detail(response),
        retry_after_seconds=retry_after_seconds,
    )


@contextmanager
def _rotated_dns_resolution(
    url: str,
    provider: str,
    attempt: int,
):
    """Rotate OAI addresses after a TLS/connect failure while preserving SNI."""
    if provider != "arxiv_oai":
        yield ""
        return
    parsed_url = urlparse(url)
    host = (parsed_url.hostname or "").casefold()
    port = parsed_url.port or 443
    if not host:
        yield ""
        return
    with _DNS_ROTATION_LOCK:
        original_getaddrinfo = socket.getaddrinfo
        try:
            initial = original_getaddrinfo(
                host, port, type=socket.SOCK_STREAM
            )
        except OSError:
            yield ""
            return
        addresses = list(
            dict.fromkeys(
                str(item[4][0]) for item in initial if item[4]
            )
        )
        if not addresses:
            yield ""
            return
        previous = _OAI_DNS_PREFERRED.get(host)
        base_index = addresses.index(previous) if previous in addresses else 0
        preferred = addresses[(base_index + attempt) % len(addresses)]

        def rotated_getaddrinfo(name, resolved_port, *args, **kwargs):
            records = original_getaddrinfo(
                name, resolved_port, *args, **kwargs
            )
            if str(name).casefold() != host:
                return records
            return sorted(
                records,
                key=lambda item: str(item[4][0]) != preferred,
            )

        socket.getaddrinfo = rotated_getaddrinfo
        try:
            yield preferred
        finally:
            socket.getaddrinfo = original_getaddrinfo


def _get_with_retry(
    url: str,
    *,
    params: dict[str, Any],
    headers: dict[str, str] | None = None,
    timeout: int | None = None,
    max_retries: int | None = None,
    provider: str | None = None,
    expected_format: str | None = None,
) -> requests.Response:
    """GET with provider pacing, validation, retries and a 429 circuit breaker."""
    provider = provider or _provider_from_url(url)
    policy = _provider_policy(provider)
    timeout = max(1, int(timeout or policy["timeout_seconds"]))
    if max_retries is None:
        max_retries = max(0, int(policy["max_retries"]))
    max_backoff = max(1.0, float(policy["max_backoff_seconds"]))
    rate_backoff = max(1.0, float(policy["rate_limit_backoff_seconds"]))
    invalid_backoff = max(
        1.0, float(policy.get("invalid_response_backoff_seconds", 2.0))
    )
    circuit_threshold = max(1, int(policy["circuit_breaker_threshold"]))
    circuit_cooldown = max(1.0, float(policy["circuit_cooldown_seconds"]))
    wait_through_circuit = bool(policy.get("wait_through_circuit", False))
    state = _provider_state(provider)
    last_error: Exception | None = None
    for attempt in range(max_retries + 1):
        _wait_for_provider(provider)
        state["requests"] += 1
        try:
            with _rotated_dns_resolution(
                url, provider, attempt
            ) as preferred_address:
                if preferred_address and attempt > 0:
                    state["dns_rotation_attempts"] += 1
                response = requests.get(
                    url, params=params, headers=headers or {}, timeout=timeout
                )
                if preferred_address:
                    host = (urlparse(url).hostname or "").casefold()
                    _OAI_DNS_PREFERRED[host] = preferred_address
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if attempt >= max_retries:
                raise
            delay = min(max_backoff, float(2**attempt))
            state["retries"] += 1
            state["backoff_wait_seconds"] += delay
            _sleep_delay(delay)
            continue

        if response.status_code not in RETRYABLE_STATUS:
            state["consecutive_429"] = 0
            if response.status_code >= 400:
                if response.status_code in {401, 403}:
                    state["access_denied_events"] += 1
                raise _http_status_error(response, provider)
            if expected_format:
                invalid = _invalid_response_error(
                    response, provider, expected_format
                )
                if invalid is not None:
                    last_error = invalid
                    state["invalid_response_events"] += 1
                    if attempt >= max_retries:
                        raise invalid
                    delay = min(max_backoff, invalid_backoff * (2**attempt))
                    state["retries"] += 1
                    state["backoff_wait_seconds"] += delay
                    _sleep_delay(delay)
                    continue
            return response

        if response.status_code == 429:
            state["rate_limit_events"] += 1
            state["consecutive_429"] += 1
            fallback = min(max_backoff, rate_backoff * (2**attempt))
            delay = _retry_after(response, fallback, max_backoff)
            last_error = _http_status_error(
                response, provider, retry_after_seconds=delay
            )
            if state["consecutive_429"] >= circuit_threshold:
                cooldown = max(circuit_cooldown, delay)
                state["circuit_open_until"] = time.monotonic() + cooldown
                state["circuit_trips"] += 1
                raise ProviderCircuitOpenError(provider, cooldown) from last_error
        else:
            state["consecutive_429"] = 0
            delay = min(max_backoff, float(2**attempt))
            last_error = _http_status_error(response, provider)
        if attempt >= max_retries:
            raise last_error
        state["retries"] += 1
        state["backoff_wait_seconds"] += delay
        _sleep_delay(delay)

    if last_error:
        raise last_error
    raise RuntimeError(f"request failed: {url}")


def _call_start(
    call_log: list[dict[str, Any]] | None,
    *,
    source: str,
    query_id: str,
    query: str,
    start: date,
    end: date,
) -> dict[str, Any]:
    call = {
        "source": source,
        "query_id": query_id,
        "query": query,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "started_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "status": "started",
        "pages": 0,
        "returned": 0,
    }
    if call_log is not None:
        call_log.append(call)
    return call


def _call_ok(call: dict[str, Any], returned: int, *, total: int | None = None) -> None:
    call["status"] = "ok"
    call["returned"] = returned
    if total is not None:
        call["provider_total"] = total
    call["finished_at"] = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _call_error(call: dict[str, Any], exc: Exception) -> None:
    call["status"] = "error"
    call["error"] = _safe_error_message(exc)
    call["finished_at"] = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _call_disabled(call: dict[str, Any], reason: str) -> None:
    call["status"] = "disabled"
    call["reason"] = reason
    call["finished_at"] = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _sleep_delay(seconds: float) -> None:
    if os.getenv("LIT_MONITOR_NO_SLEEP", "").strip().casefold() in {"1", "true", "yes"}:
        return
    if seconds > 0:
        time.sleep(seconds)


def _timeout(provider: str, default: int) -> int:
    try:
        return max(
            1,
            int(_provider_policy(provider).get("timeout_seconds", default)),
        )
    except (TypeError, ValueError):
        return default


def _doi_fields(doi: str, arxiv_id: str = "") -> tuple[str, str]:
    normalized = norm_doi(doi)
    if is_arxiv_doi(normalized):
        embedded = norm_arxiv_id(normalized.rsplit("arxiv.", 1)[-1])
        return normalized, embedded or norm_arxiv_id(arxiv_id)
    return normalized, norm_arxiv_id(arxiv_id)


def _is_preprint(type_value: Any, venue: Any, doi: str) -> str:
    text = norm_text(type_value).casefold()
    venue_text = norm_text(venue).casefold()
    return "1" if "preprint" in text or venue_text == "arxiv" or is_arxiv_doi(doi) else "0"


def openalex(
    query_id: str,
    q: str,
    start: date,
    end: date,
    limit: int,
    *,
    call_log: list[dict[str, Any]] | None = None,
    strict_dates: bool | None = None,
) -> list[dict[str, Any]]:
    call = _call_start(call_log, source="OpenAlex", query_id=query_id, query=q, start=start, end=end)
    key = os.getenv("OPENALEX_API_KEY", "").strip()
    params: dict[str, Any] = {
        "search": q,
        "filter": f"from_publication_date:{start.isoformat()},to_publication_date:{end.isoformat()}",
        "sort": "publication_date:desc",
        "per-page": min(100, max(1, limit)),
    }
    if key:
        params["api_key"] = key
    polite_email = os.getenv("OPENALEX_POLITE_EMAIL", "").strip()
    if polite_email:
        params["mailto"] = polite_email
    user_agent = "security-literature-monitor/1.1 (academic-literature-research)"
    if polite_email:
        user_agent += f"; mailto:{polite_email}"

    rows: list[dict[str, Any]] = []
    seen_provider_ids: set[str] = set()
    cursor: str | None = "*"
    seen_cursors: set[str] = set()
    try:
        while cursor and len(rows) < limit:
            if cursor in seen_cursors:
                raise RuntimeError("OpenAlex repeated cursor; pagination aborted")
            seen_cursors.add(cursor)
            params["cursor"] = cursor
            response = _get_with_retry(
                "https://api.openalex.org/works",
                params=params,
                headers={"User-Agent": user_agent},
                timeout=_timeout("openalex", 60),
                provider="openalex",
                expected_format="json",
            )
            data = response.json()
            call["pages"] += 1
            meta = data.get("meta") or {}
            if isinstance(meta.get("count"), int):
                call["provider_total"] = meta["count"]
            results = data.get("results") or []
            if not results:
                break
            for item in results:
                ids = item.get("ids") or {}
                provider_id = norm_text(item.get("id", "") or ids.get("openalex", ""))
                raw_doi = item.get("doi", "") or ids.get("doi", "")
                raw_arxiv = ids.get("arxiv", "")
                if provider_id and provider_id in seen_provider_ids:
                    continue
                if provider_id:
                    seen_provider_ids.add(provider_id)
                location = item.get("primary_location") or {}
                source = location.get("source") or {}
                title = norm_text(item.get("display_name") or item.get("title") or "")
                venue = norm_text(source.get("display_name", ""))
                doi, arxiv_id = _doi_fields(raw_doi, raw_arxiv)
                authorships = item.get("authorships") or []
                if isinstance(authorships, dict):
                    authorships = [authorships]
                author_names = []
                for authorship in authorships:
                    if isinstance(authorship, dict):
                        author_obj = authorship.get("author") or {}
                        if isinstance(author_obj, dict):
                            name = norm_text(author_obj.get("display_name", ""))
                        else:
                            name = norm_text(author_obj)
                    else:
                        name = norm_text(authorship)
                    if name:
                        author_names.append(name)
                publication_date = norm_text(item.get("publication_date", ""))
                matches, precision = _date_allowed(
                    publication_date or item.get("publication_year", ""), start, end, strict_dates
                )
                if not matches:
                    call["date_filtered"] = call.get("date_filtered", 0) + 1
                    continue
                rows.append(_set_date_meta({
                        "title": title,
                        "abstract": norm_text(abstract_from_inverted(item.get("abstract_inverted_index"))),
                        "authors": "; ".join(author_names),
                        "publication_year": item.get("publication_year", "") or "",
                        "publication_date": publication_date,
                        "venue": venue,
                        "doi": doi,
                        "url": norm_text(location.get("landing_page_url", "")) or doi or provider_id,
                        "type": norm_text(item.get("type", "")),
                        "cited_by_count": item.get("cited_by_count", "") or "",
                        "source_database": "OpenAlex",
                        "query_id": query_id,
                        "original_record_id": provider_id,
                        "retrieved_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
                        "is_preprint": _is_preprint(item.get("type", ""), venue, doi),
                        "arxiv_id": arxiv_id,
                        "formal_doi": "" if is_arxiv_doi(doi) else doi,
                    }, precision))
                if len(rows) >= limit:
                    break
            cursor = norm_text(meta.get("next_cursor", "")) or None
        _call_ok(call, len(rows), total=call.get("provider_total"))
        return rows
    except Exception as exc:
        _call_error(call, exc)
        if rows:
            raise ProviderPartialError(exc, rows) from exc
        raise


def semantic_scholar(
    query_id: str,
    q: str,
    start: date,
    end: date,
    limit: int,
    *,
    call_log: list[dict[str, Any]] | None = None,
    strict_dates: bool | None = None,
) -> list[dict[str, Any]]:
    call = _call_start(
        call_log, source="Semantic Scholar", query_id=query_id, query=q, start=start, end=end
    )
    headers: dict[str, str] = {}
    key = os.getenv("S2_API_KEY", "").strip()
    if key:
        headers["x-api-key"] = key
    headers.setdefault("User-Agent", "security-literature-monitor/1.1 (academic-literature-research)")
    params: dict[str, Any] = {
        "query": q,
        "fields": "paperId,externalIds,title,abstract,authors,year,publicationDate,venue,url,citationCount,publicationTypes",
        "sort": "publicationDate:desc",
        "limit": min(100, max(1, limit)),
    }

    rows: list[dict[str, Any]] = []
    token: str | None = None
    seen_tokens: set[str] = set()
    seen_paper_ids: set[str] = set()
    try:
        while len(rows) < limit:
            if token:
                if token in seen_tokens:
                    raise RuntimeError("Semantic Scholar repeated token; pagination aborted")
                seen_tokens.add(token)
                params["token"] = token
            else:
                params.pop("token", None)
            response = _get_with_retry(
                "https://api.semanticscholar.org/graph/v1/paper/search/bulk",
                params=params,
                headers=headers,
                timeout=_timeout("semantic_scholar", 60),
                provider="semantic_scholar",
                expected_format="json",
            )
            data = response.json()
            call["pages"] += 1
            if isinstance(data.get("total"), int):
                call["provider_total"] = data["total"]
            page_items = data.get("data") or []
            if not page_items:
                # A token with an empty page is not useful progress and can
                # otherwise create a long token loop on transient API output.
                break
            for item in page_items:
                paper_id = norm_text(item.get("paperId", ""))
                if paper_id and paper_id in seen_paper_ids:
                    continue
                if paper_id:
                    seen_paper_ids.add(paper_id)
                publication_date = norm_text(item.get("publicationDate") or "")
                if not publication_date and item.get("year"):
                    publication_date = str(item["year"])
                matches, precision = _date_allowed(publication_date, start, end, strict_dates)
                if not matches:
                    call["date_filtered"] = call.get("date_filtered", 0) + 1
                    continue

                external = item.get("externalIds") or {}
                arxiv_raw = next(
                    (value for key, value in external.items() if norm_text(key).casefold() in {"arxiv", "arxiv_id"}),
                    "",
                )
                doi_raw = next(
                    (value for key, value in external.items() if norm_text(key).casefold() == "doi"),
                    "",
                )
                doi, arxiv_id = _doi_fields(doi_raw, arxiv_raw)
                venue = norm_text(item.get("venue", ""))
                raw_authors = item.get("authors") or []
                if isinstance(raw_authors, dict):
                    raw_authors = [raw_authors]
                author_names = []
                for author in raw_authors:
                    if isinstance(author, dict):
                        name = norm_text(author.get("name", ""))
                    else:
                        name = norm_text(author)
                    if name:
                        author_names.append(name)
                rows.append(_set_date_meta({
                        "title": norm_text(item.get("title", "")),
                        "abstract": norm_text(item.get("abstract", "") or ""),
                        "authors": "; ".join(author_names),
                        "publication_year": item.get("year", "") or "",
                        "publication_date": publication_date,
                        "venue": venue,
                        "doi": doi,
                        "url": norm_text(item.get("url", "")),
                        "type": ";".join(item.get("publicationTypes") or []),
                        "cited_by_count": item.get("citationCount", "") or "",
                        "source_database": "Semantic Scholar",
                        "query_id": query_id,
                        "original_record_id": paper_id,
                        "retrieved_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
                        "is_preprint": _is_preprint(item.get("publicationTypes", []), venue, doi),
                        "arxiv_id": arxiv_id,
                        "formal_doi": "" if is_arxiv_doi(doi) else doi,
                    }, precision))
                if len(rows) >= limit:
                    break
            next_token = norm_text(data.get("token", "")) or None
            if not next_token:
                break
            if next_token == token:
                raise RuntimeError("Semantic Scholar response repeated its pagination token")
            token = next_token
        _call_ok(call, len(rows), total=call.get("provider_total"))
        return rows
    except Exception as exc:
        _call_error(call, exc)
        if rows:
            raise ProviderPartialError(exc, rows) from exc
        raise


def _arxiv_query_with_dates(search: str, start: date, end: date) -> str:
    return (
        f"({search}) AND submittedDate:[{start.strftime('%Y%m%d')}0000 TO "
        f"{end.strftime('%Y%m%d')}2359]"
    )


def _arxiv_user_agent() -> str:
    user_agent = "security-literature-monitor/1.2 (academic-literature-research)"
    contact = os.getenv("OPENALEX_POLITE_EMAIL", "").strip()
    if contact:
        user_agent += f"; mailto:{contact}"
    return user_agent


def _arxiv_api_url() -> str:
    configured = (CFG.get("request_policies", {}) or {}).get("arxiv", {}) or {}
    value = norm_text(os.getenv("ARXIV_API_URL", "").strip())
    value = value or norm_text(configured.get("api_url", ""))
    return value or "https://arxiv.org/api/query"


def _arxiv_oai_url() -> str:
    configured = (CFG.get("request_policies", {}) or {}).get("arxiv_oai", {}) or {}
    value = norm_text(os.getenv("ARXIV_OAI_URL", "").strip())
    value = value or norm_text(configured.get("api_url", ""))
    return value or ARXIV_OAI_URL


def _arxiv_oai_connect(cache_path: Path) -> sqlite3.Connection:
    cache_path = Path(cache_path)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(cache_path, timeout=60)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS records (
            arxiv_id TEXT PRIMARY KEY,
            created TEXT NOT NULL,
            updated TEXT NOT NULL,
            title TEXT NOT NULL,
            abstract TEXT NOT NULL,
            authors TEXT NOT NULL,
            categories TEXT NOT NULL,
            comments TEXT NOT NULL,
            journal_ref TEXT NOT NULL,
            report_no TEXT NOT NULL,
            doi TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS progress (
            set_spec TEXT PRIMARY KEY,
            coverage_start TEXT NOT NULL,
            resumption_token TEXT NOT NULL,
            token_expires_at TEXT NOT NULL,
            complete INTEGER NOT NULL,
            pages INTEGER NOT NULL,
            records_seen INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_records_created
            ON records(created, arxiv_id);
        """
    )
    return connection


def _arxiv_oai_text(parent: ET.Element, path: str) -> str:
    return norm_text(parent.findtext(path, default="", namespaces=ARXIV_OAI_NAMESPACES))


def _arxiv_oai_record(element: ET.Element) -> dict[str, str] | None:
    header = element.find("oai:header", ARXIV_OAI_NAMESPACES)
    if header is None or norm_text(header.get("status", "")).casefold() == "deleted":
        return None
    metadata = element.find("oai:metadata/arxiv:arXiv", ARXIV_OAI_NAMESPACES)
    if metadata is None:
        return None
    arxiv_id = norm_arxiv_id(_arxiv_oai_text(metadata, "arxiv:id"))
    created = _arxiv_oai_text(metadata, "arxiv:created")
    if not arxiv_id or not created:
        return None
    author_names: list[str] = []
    for author in metadata.findall("arxiv:authors/arxiv:author", ARXIV_OAI_NAMESPACES):
        parts = [
            _arxiv_oai_text(author, "arxiv:forenames"),
            _arxiv_oai_text(author, "arxiv:keyname"),
            _arxiv_oai_text(author, "arxiv:suffix"),
        ]
        name = norm_text(" ".join(part for part in parts if part))
        if name:
            author_names.append(name)
    return {
        "arxiv_id": arxiv_id,
        "created": created,
        "updated": _arxiv_oai_text(metadata, "arxiv:updated"),
        "title": _arxiv_oai_text(metadata, "arxiv:title"),
        "abstract": _arxiv_oai_text(metadata, "arxiv:abstract"),
        "authors": "; ".join(author_names),
        "categories": _arxiv_oai_text(metadata, "arxiv:categories"),
        "comments": _arxiv_oai_text(metadata, "arxiv:comments"),
        "journal_ref": _arxiv_oai_text(metadata, "arxiv:journal-ref"),
        "report_no": _arxiv_oai_text(metadata, "arxiv:report-no"),
        "doi": norm_doi(_arxiv_oai_text(metadata, "arxiv:doi")),
    }


def _arxiv_oai_token_expired(value: str) -> bool:
    if not value:
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return True
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed <= datetime.now(timezone.utc) + timedelta(seconds=30)


def _prepare_arxiv_oai_cache(
    cache_path: Path,
    coverage_start: date,
    *,
    set_specs: tuple[str, ...] = ARXIV_OAI_SETS,
) -> dict[str, Any]:
    """Harvest arXiv metadata once for a backfill and checkpoint every OAI page."""
    cache_path = Path(cache_path)
    set_specs = tuple(dict.fromkeys(set_specs))
    if not set_specs:
        raise ValueError("arXiv OAI set list must not be empty")
    ready_key = (
        str(cache_path.absolute()),
        coverage_start.isoformat(),
        ",".join(set_specs),
    )
    if ready_key in _ARXIV_OAI_READY:
        with closing(_arxiv_oai_connect(cache_path)) as connection:
            count = int(connection.execute("SELECT COUNT(*) FROM records").fetchone()[0])
        return {"cache_path": str(cache_path), "records": count, "reused": True}

    connection = _arxiv_oai_connect(cache_path)
    try:
        existing_starts = [
            value
            for (value,) in connection.execute(
                "SELECT DISTINCT coverage_start FROM progress"
            ).fetchall()
        ]
        if existing_starts and any(value > coverage_start.isoformat() for value in existing_starts):
            connection.execute("DELETE FROM progress")
            connection.commit()

        for set_spec in set_specs:
            connection.execute(
                """
                INSERT OR IGNORE INTO progress
                    (set_spec, coverage_start, resumption_token, token_expires_at,
                     complete, pages, records_seen, updated_at)
                VALUES (?, ?, '', '', 0, 0, 0, ?)
                """,
                (set_spec, coverage_start.isoformat(), now_iso()),
            )
        connection.commit()

        for set_spec in set_specs:
            token_restarts = 0
            while True:
                row = connection.execute(
                    """
                    SELECT coverage_start, resumption_token, token_expires_at,
                           complete, pages, records_seen
                    FROM progress WHERE set_spec = ?
                    """,
                    (set_spec,),
                ).fetchone()
                if row is None:
                    raise RuntimeError(f"arXiv OAI progress missing for {set_spec}")
                stored_start, token, token_expires, complete, pages, records_seen = row
                if complete and stored_start <= coverage_start.isoformat():
                    break
                if token and _arxiv_oai_token_expired(token_expires):
                    token = ""
                    token_restarts += 1
                    connection.execute(
                        """
                        UPDATE progress
                        SET resumption_token = '', token_expires_at = '', pages = 0,
                            records_seen = 0, updated_at = ?
                        WHERE set_spec = ?
                        """,
                        (now_iso(), set_spec),
                    )
                    connection.commit()

                params: dict[str, Any] = {"verb": "ListRecords"}
                if token:
                    params["resumptionToken"] = token
                else:
                    params.update(
                        {
                            "metadataPrefix": "arXiv",
                            "set": set_spec,
                            "from": coverage_start.isoformat(),
                        }
                    )
                response = _get_with_retry(
                    _arxiv_oai_url(),
                    params=params,
                    headers={"User-Agent": _arxiv_user_agent()},
                    timeout=_timeout("arxiv_oai", 180),
                    provider="arxiv_oai",
                    expected_format="xml",
                )
                root = ET.fromstring(response.content)
                error = root.find(".//oai:error", ARXIV_OAI_NAMESPACES)
                if error is not None:
                    code = norm_text(error.get("code", ""))
                    if code == "noRecordsMatch":
                        connection.execute(
                            """
                            UPDATE progress SET complete = 1, resumption_token = '',
                                token_expires_at = '', updated_at = ? WHERE set_spec = ?
                            """,
                            (now_iso(), set_spec),
                        )
                        connection.commit()
                        break
                    if code == "badResumptionToken" and token_restarts < 2:
                        token_restarts += 1
                        connection.execute(
                            """
                            UPDATE progress SET resumption_token = '', token_expires_at = '',
                                pages = 0, records_seen = 0, updated_at = ? WHERE set_spec = ?
                            """,
                            (now_iso(), set_spec),
                        )
                        connection.commit()
                        continue
                    raise RuntimeError(
                        f"arXiv OAI error {code or 'unknown'}: {norm_text(error.text)}"
                    )

                parsed_records = [
                    item
                    for item in (
                        _arxiv_oai_record(element)
                        for element in root.findall(".//oai:record", ARXIV_OAI_NAMESPACES)
                    )
                    if item is not None
                ]
                connection.executemany(
                    """
                    INSERT INTO records
                        (arxiv_id, created, updated, title, abstract, authors,
                         categories, comments, journal_ref, report_no, doi)
                    VALUES
                        (:arxiv_id, :created, :updated, :title, :abstract, :authors,
                         :categories, :comments, :journal_ref, :report_no, :doi)
                    ON CONFLICT(arxiv_id) DO UPDATE SET
                        created = excluded.created,
                        updated = excluded.updated,
                        title = excluded.title,
                        abstract = excluded.abstract,
                        authors = excluded.authors,
                        categories = excluded.categories,
                        comments = excluded.comments,
                        journal_ref = excluded.journal_ref,
                        report_no = excluded.report_no,
                        doi = excluded.doi
                    """,
                    parsed_records,
                )
                token_element = root.find(".//oai:resumptionToken", ARXIV_OAI_NAMESPACES)
                next_token = norm_text(token_element.text if token_element is not None else "")
                expiration = norm_text(
                    token_element.get("expirationDate", "") if token_element is not None else ""
                )
                connection.execute(
                    """
                    UPDATE progress
                    SET resumption_token = ?, token_expires_at = ?, complete = ?,
                        pages = ?, records_seen = ?, updated_at = ?
                    WHERE set_spec = ?
                    """,
                    (
                        next_token,
                        expiration,
                        0 if next_token else 1,
                        int(pages) + 1,
                        int(records_seen) + len(parsed_records),
                        now_iso(),
                        set_spec,
                    ),
                )
                connection.commit()
                print(
                    f"arXiv OAI {set_spec}: page {int(pages) + 1}, "
                    f"{len(parsed_records)} records checkpointed"
                )
                if not next_token:
                    break

        count = int(connection.execute("SELECT COUNT(*) FROM records").fetchone()[0])
        progress = [
            {
                "set_spec": row[0],
                "pages": int(row[1]),
                "records_seen": int(row[2]),
                "complete": bool(row[3]),
            }
            for row in connection.execute(
                "SELECT set_spec, pages, records_seen, complete FROM progress ORDER BY set_spec"
            ).fetchall()
            if row[0] in set_specs
        ]
    finally:
        connection.close()
    _ARXIV_OAI_READY.add(ready_key)
    return {
        "cache_path": str(cache_path),
        "records": count,
        "sets": progress,
        "reused": False,
    }


_ARXIV_QUERY_TOKEN = re.compile(
    r'\s*(ANDNOT|AND|OR|\(|\)|[A-Za-z][A-Za-z0-9_]*:(?:"(?:\\.|[^"])*"|[^\s()]+))',
    re.I,
)


def _arxiv_query_tokens(search: str) -> list[str]:
    tokens: list[str] = []
    position = 0
    for match in _ARXIV_QUERY_TOKEN.finditer(search):
        if search[position:match.start()].strip():
            raise ValueError(f"unsupported arXiv query syntax near: {search[position:match.start()]}")
        tokens.append(match.group(1))
        position = match.end()
    if search[position:].strip():
        raise ValueError(f"unsupported arXiv query syntax near: {search[position:]}")
    return tokens


def _arxiv_term_matches(term: str, record: dict[str, str]) -> bool:
    field, raw_value = term.split(":", 1)
    value = raw_value
    if len(value) >= 2 and value[0] == value[-1] == '"':
        value = value[1:-1].replace('\\"', '"').replace("\\\\", "\\")
    field = field.casefold()
    if field == "cat":
        return value.casefold() in {
            item.casefold() for item in record.get("categories", "").split()
        }
    field_values = {
        "ti": record.get("title", ""),
        "abs": record.get("abstract", ""),
        "au": record.get("authors", ""),
        "co": record.get("comments", ""),
        "jr": record.get("journal_ref", ""),
        "rn": record.get("report_no", ""),
        "id": record.get("arxiv_id", ""),
        "doi": record.get("doi", ""),
    }
    if field == "all":
        corpus = " ".join(
            record.get(key, "")
            for key in (
                "title", "abstract", "authors", "categories", "comments",
                "journal_ref", "report_no", "doi", "arxiv_id",
            )
        )
    elif field in field_values:
        corpus = field_values[field]
    else:
        raise ValueError(f"unsupported arXiv query field: {field}")
    normalized_value = norm_title(value)
    normalized_corpus = norm_title(corpus)
    if not normalized_value or not normalized_corpus:
        return False
    return bool(
        re.search(
            r"(?<![a-z0-9])" + re.escape(normalized_value) + r"(?![a-z0-9])",
            normalized_corpus,
        )
    )


def _arxiv_query_matches(search: str, record: dict[str, str]) -> bool:
    tokens = _arxiv_query_tokens(search)
    output: list[str] = []
    operators: list[str] = []
    precedence = {"OR": 1, "AND": 2, "ANDNOT": 2}
    for token in tokens:
        upper = token.upper()
        if upper in precedence:
            while (
                operators
                and operators[-1] != "("
                and precedence.get(operators[-1], 0) >= precedence[upper]
            ):
                output.append(operators.pop())
            operators.append(upper)
        elif token == "(":
            operators.append(token)
        elif token == ")":
            while operators and operators[-1] != "(":
                output.append(operators.pop())
            if not operators:
                raise ValueError("unbalanced arXiv query parentheses")
            operators.pop()
        else:
            output.append(token)
    while operators:
        operator = operators.pop()
        if operator == "(":
            raise ValueError("unbalanced arXiv query parentheses")
        output.append(operator)

    values: list[bool] = []
    for token in output:
        if token in precedence:
            if len(values) < 2:
                raise ValueError("invalid arXiv Boolean expression")
            right = values.pop()
            left = values.pop()
            values.append(
                left or right
                if token == "OR"
                else left and not right
                if token == "ANDNOT"
                else left and right
            )
        else:
            values.append(_arxiv_term_matches(token, record))
    if len(values) != 1:
        raise ValueError("invalid arXiv Boolean expression")
    return values[0]


def _arxiv_from_oai_cache(
    query_id: str,
    search: str,
    start: date,
    end: date,
    limit: int,
    cache_path: Path,
    coverage_start: date,
    call: dict[str, Any],
    *,
    set_specs: tuple[str, ...],
) -> list[dict[str, Any]]:
    cache_report = _prepare_arxiv_oai_cache(
        cache_path, coverage_start, set_specs=set_specs
    )
    with closing(_arxiv_oai_connect(cache_path)) as connection:
        columns = (
            "arxiv_id", "created", "updated", "title", "abstract", "authors",
            "categories", "comments", "journal_ref", "report_no", "doi",
        )
        candidates = [
            dict(zip(columns, row))
            for row in connection.execute(
                """
                SELECT arxiv_id, created, updated, title, abstract, authors,
                       categories, comments, journal_ref, report_no, doi
                FROM records WHERE created >= ? AND created <= ?
                ORDER BY created, arxiv_id
                """,
                (start.isoformat(), end.isoformat()),
            ).fetchall()
        ]
    matches = [record for record in candidates if _arxiv_query_matches(search, record)]
    retrieved_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    rows: list[dict[str, Any]] = []
    for record in matches[:limit]:
        doi = norm_doi(record.get("doi", ""))
        rows.append(
            _set_date_meta(
                {
                    "title": record.get("title", ""),
                    "abstract": record.get("abstract", ""),
                    "authors": record.get("authors", ""),
                    "publication_year": record.get("created", "")[:4],
                    "publication_date": record.get("created", "")[:10],
                    "venue": "arXiv",
                    "doi": doi,
                    "url": f"https://arxiv.org/abs/{record['arxiv_id']}",
                    "type": "preprint",
                    "cited_by_count": "",
                    "source_database": "arXiv",
                    "query_id": query_id,
                    "original_record_id": record["arxiv_id"],
                    "retrieved_at": retrieved_at,
                    "is_preprint": "1",
                    "arxiv_id": record["arxiv_id"],
                    "formal_doi": "" if is_arxiv_doi(doi) else doi,
                },
                "day",
            )
        )
    call["transport"] = "OAI-PMH cache"
    call["cache_path"] = str(cache_path)
    call["cache_records"] = cache_report["records"]
    call["cache_reused"] = bool(cache_report.get("reused", False))
    call["cache_coverage_start"] = coverage_start.isoformat()
    call["cache_sets"] = list(set_specs)
    if cache_report.get("sets"):
        call["cache_harvest"] = cache_report["sets"]
    call["truncated"] = len(matches) > limit
    _call_ok(call, len(rows), total=len(matches))
    return rows


def arxiv(
    query_id: str,
    search: str,
    start: date,
    end: date,
    limit: int,
    *,
    call_log: list[dict[str, Any]] | None = None,
    strict_dates: bool | None = None,
    oai_cache_path: Path | None = None,
    oai_cache_start: date | None = None,
    oai_set_specs: tuple[str, ...] = ARXIV_OAI_SETS,
) -> list[dict[str, Any]]:
    call = _call_start(call_log, source="arXiv", query_id=query_id, query=search, start=start, end=end)
    if oai_cache_path is not None:
        try:
            return _arxiv_from_oai_cache(
                query_id,
                search,
                start,
                end,
                limit,
                Path(oai_cache_path),
                oai_cache_start or start,
                call,
                set_specs=oai_set_specs,
            )
        except Exception as exc:
            _call_error(call, exc)
            raise
    url = _arxiv_api_url()
    page_size = min(2000, max(1, limit))
    offset = 0
    rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    seen_offsets: set[int] = set()
    try:
        while len(rows) < limit:
            if offset in seen_offsets:
                raise RuntimeError("arXiv repeated pagination offset")
            seen_offsets.add(offset)
            params = {
                "search_query": _arxiv_query_with_dates(search, start, end),
                "start": offset,
                "max_results": page_size,
            }
            response = _get_with_retry(
                url,
                params=params,
                headers={
                    "User-Agent": _arxiv_user_agent()
                },
                timeout=_timeout("arxiv", 90),
                provider="arxiv",
                expected_format="xml",
            )
            root = ET.fromstring(response.content)
            call["pages"] += 1
            entries = root.findall("a:entry", {"a": "http://www.w3.org/2005/Atom", "arxiv": "http://arxiv.org/schemas/atom"})
            if not entries:
                break
            ns = {"a": "http://www.w3.org/2005/Atom", "arxiv": "http://arxiv.org/schemas/atom"}
            for entry in entries:
                published = norm_text(entry.findtext("a:published", default="", namespaces=ns))
                matches, precision = _date_allowed(published, start, end, strict_dates)
                if not matches:
                    call["date_filtered"] = call.get("date_filtered", 0) + 1
                    continue
                identifier = norm_text(entry.findtext("a:id", default="", namespaces=ns))
                arxiv_id = norm_arxiv_id(identifier.rsplit("/", 1)[-1])
                if not arxiv_id or arxiv_id in seen_ids:
                    continue
                seen_ids.add(arxiv_id)
                doi, embedded_arxiv = _doi_fields(
                    entry.findtext("arxiv:doi", default="", namespaces=ns), arxiv_id
                )
                rows.append(_set_date_meta({
                        "title": norm_text(entry.findtext("a:title", default="", namespaces=ns)),
                        "abstract": norm_text(entry.findtext("a:summary", default="", namespaces=ns)),
                        "authors": "; ".join(
                            norm_text(author.findtext("a:name", default="", namespaces=ns))
                            for author in entry.findall("a:author", ns)
                            if author.findtext("a:name", default="", namespaces=ns)
                        ),
                        "publication_year": published[:4],
                        "publication_date": published[:10],
                        "venue": "arXiv",
                        "doi": doi,
                        "url": identifier,
                        "type": "preprint",
                        "cited_by_count": "",
                        "source_database": "arXiv",
                        "query_id": query_id,
                        "original_record_id": arxiv_id,
                        "retrieved_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
                        "is_preprint": "1",
                        "arxiv_id": embedded_arxiv or arxiv_id,
                        "formal_doi": "" if is_arxiv_doi(doi) else doi,
                    }, precision))
                if len(rows) >= limit:
                    break
            offset += len(entries)
            if len(entries) < page_size:
                break
        _call_ok(call, len(rows), total=call.get("provider_total"))
        return rows
    except Exception as exc:
        _call_error(call, exc)
        if rows:
            raise ProviderPartialError(exc, rows) from exc
        raise


def ieee(
    query_id: str,
    canonical: str,
    start: date,
    end: date,
    limit: int,
    *,
    call_log: list[dict[str, Any]] | None = None,
    strict_dates: bool | None = None,
) -> list[dict[str, Any]]:
    call = _call_start(call_log, source="IEEE Xplore", query_id=query_id, query=canonical, start=start, end=end)
    key = os.getenv("IEEE_API_KEY", "").strip()
    if not key:
        _call_disabled(call, "IEEE_API_KEY is not configured")
        return []

    rows: list[dict[str, Any]] = []
    offset = 1
    seen_ids: set[str] = set()
    seen_offsets: set[int] = set()
    try:
        while len(rows) < limit:
            if offset in seen_offsets:
                raise RuntimeError("IEEE repeated pagination offset")
            seen_offsets.add(offset)
            requested_page_size = min(200, max(1, limit - len(rows)))
            params = {
                "apikey": key,
                "format": "json",
                "querytext": canonical,
                # IEEE Xplore's Metadata API exposes publication-year filters,
                # not day-level start/end date filters.  Keep the year bounds
                # in the request and apply the exact-day check below whenever
                # the returned record contains a day; year-only records remain
                # marked as such and are rejected from narrow strict runs.
                "start_year": start.year,
                "end_year": end.year,
                "max_records": requested_page_size,
                "start_record": offset,
            }
            response = _get_with_retry(
               "https://ieeexploreapi.ieee.org/api/v1/search/articles",
    params=params,
    headers={
        "User-Agent": "Mozilla/5.0 literature-monitor/1.0",
        "Accept": "application/json",
    },
    timeout=90,
    provider="ieee",
    expected_format="json",
            )
            data = response.json()
            call["pages"] += 1
            total = data.get("total_records") or data.get("totalRecords")
            try:
                call["provider_total"] = int(total)
            except (TypeError, ValueError):
                pass
            articles = data.get("articles") or []
            if not articles:
                break
            for item in articles:
                article_id = norm_text(item.get("article_number", "") or item.get("articleNumber", ""))
                if not article_id:
                    article_id = norm_doi(item.get("doi", "")) or norm_text(item.get("title", ""))
                if article_id and article_id in seen_ids:
                    continue
                if article_id:
                    seen_ids.add(article_id)
                doi = norm_doi(item.get("doi", ""))
                publication_date = norm_text(
                    item.get("publication_date", "")
                    or item.get("publicationDate", "")
                    or item.get("publication_year", "")
                )
                matches, precision = _date_allowed(publication_date, start, end, strict_dates)
                if not matches:
                    call["date_filtered"] = call.get("date_filtered", 0) + 1
                    continue
                raw_authors = item.get("authors") or []
                if isinstance(raw_authors, dict):
                    authors = raw_authors.get("authors") or []
                else:
                    authors = raw_authors
                if isinstance(authors, dict):
                    authors = [authors]
                elif isinstance(authors, str):
                    authors = split_authors(authors)
                rows.append(_set_date_meta({
                        "title": norm_text(item.get("title", "")),
                        "abstract": norm_text(item.get("abstract", "") or ""),
                        "authors": "; ".join(
                            norm_text(
                                author.get("full_name", "")
                                if isinstance(author, dict)
                                else author
                            )
                            for author in authors
                            if norm_text(
                                author.get("full_name", "")
                                if isinstance(author, dict)
                                else author
                            )
                        ),
                        "publication_year": item.get("publication_year", "") or "",
                        "publication_date": publication_date,
                        "venue": norm_text(item.get("publication_title", "")),
                        "doi": doi,
                        "url": norm_text(item.get("html_url", "") or item.get("pdf_url", "")),
                        "type": norm_text(item.get("content_type", "")),
                        "cited_by_count": "",
                        "source_database": "IEEE Xplore",
                        "query_id": query_id,
                        "original_record_id": article_id,
                        "retrieved_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
                        "is_preprint": "0",
                        "arxiv_id": "",
                        "formal_doi": doi,
                    }, precision))
                if len(rows) >= limit:
                    break
            # IEEE can return a short page even when ``total_records`` says
            # that more records remain (for example because of an internal
            # result cap). Continue while the provider total confirms that
            # the next offset is still in range.
            next_offset = offset + len(articles)
            if next_offset <= offset:
                break
            offset = next_offset
            provider_total = call.get("provider_total")
            if isinstance(provider_total, int) and offset < provider_total:
                continue
            if len(articles) < requested_page_size:
                break
        _call_ok(call, len(rows), total=call.get("provider_total"))
        return rows
    except Exception as exc:
        _call_error(call, exc)
        if rows:
            raise ProviderPartialError(exc, rows) from exc
        raise


def dblp(
    query_id: str,
    query: str,
    start: date,
    end: date,
    limit: int,
    *,
    call_log: list[dict[str, Any]] | None = None,
    strict_dates: bool | None = None,
) -> list[dict[str, Any]]:
    """Retrieve bibliographic records from DBLP's public publication search API."""
    call = _call_start(call_log, source="DBLP", query_id=query_id, query=query, start=start, end=end)
    rows: list[dict[str, Any]] = []
    offset = 0
    seen_ids: set[str] = set()
    seen_offsets: set[int] = set()
    try:
        while len(rows) < limit:
            if offset in seen_offsets:
                raise RuntimeError("DBLP repeated pagination offset")
            seen_offsets.add(offset)
            page_size = min(100, limit - len(rows))
            response = _get_with_retry(
                "https://dblp.org/search/publ/api",
                params={"q": query, "format": "json", "h": page_size, "f": offset},
                headers={
                    "Accept": "application/json",
                    "User-Agent": _arxiv_user_agent(),
                },
                timeout=_timeout("dblp", 60),
                provider="dblp",
                expected_format="json",
            )
            data = response.json()
            call["pages"] += 1
            hits = (data.get("result") or {}).get("hits") or {}
            total = hits.get("@total")
            try:
                call["provider_total"] = int(total)
            except (TypeError, ValueError):
                pass
            raw_hits = hits.get("hit") or []
            if isinstance(raw_hits, dict):
                raw_hits = [raw_hits]
            if not raw_hits:
                break
            for hit in raw_hits:
                info = hit.get("info") or {}
                identifier = norm_text(info.get("key", ""))
                if identifier and identifier in seen_ids:
                    continue
                if identifier:
                    seen_ids.add(identifier)
                year = parse_year_safe(info.get("year", ""))
                date_value = info.get("date") or info.get("year", "")
                matches, precision = _date_allowed(date_value, start, end, strict_dates)
                if not matches:
                    call["date_filtered"] = call.get("date_filtered", 0) + 1
                    continue
                raw_authors = info.get("authors") or {}
                author_data = raw_authors.get("author") if isinstance(raw_authors, dict) else raw_authors
                author_data = author_data or []
                if isinstance(author_data, dict):
                    author_data = [author_data]
                authors = "; ".join(
                    norm_text(a.get("text", "") if isinstance(a, dict) else a)
                    for a in author_data
                    if norm_text(a.get("text", "") if isinstance(a, dict) else a)
                )
                doi = norm_doi(info.get("doi", ""))
                url_value = info.get("ee", "") or info.get("url", "")
                if isinstance(url_value, list):
                    url_value = url_value[0] if url_value else ""
                if isinstance(url_value, dict):
                    url_value = url_value.get("text") or url_value.get("href") or ""
                rows.append(_set_date_meta({
                        "title": norm_text(info.get("title", "")),
                        "abstract": "",
                        "authors": authors,
                        "publication_year": year or "",
                        "publication_date": norm_text(date_value),
                        "venue": norm_text(info.get("venue", "")),
                        "doi": doi,
                        "url": norm_text(url_value),
                        "type": norm_text(info.get("type", "")),
                        "cited_by_count": "",
                        "source_database": "DBLP",
                        "query_id": query_id,
                        "original_record_id": identifier,
                        "retrieved_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
                        "is_preprint": "0",
                        "arxiv_id": "",
                        "formal_doi": doi,
                    }, precision))
                if len(rows) >= limit:
                    break
            # DBLP may emit fewer hits than requested while ``@total`` still
            # indicates additional results. Use the reported total when it is
            # available, and require a strictly advancing offset.
            next_offset = offset + len(raw_hits)
            if next_offset <= offset:
                break
            offset = next_offset
            provider_total = call.get("provider_total")
            if isinstance(provider_total, int) and offset < provider_total:
                continue
            if len(raw_hits) < page_size:
                break
        _call_ok(call, len(rows), total=call.get("provider_total"))
        return rows
    except Exception as exc:
        _call_error(call, exc)
        if rows:
            raise ProviderPartialError(exc, rows) from exc
        raise


def _union_find(size: int):
    parent = list(range(size))

    def find(value: int) -> int:
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    def union(left: int, right: int) -> None:
        a, b = find(left), find(right)
        if a != b:
            parent[b] = a

    return parent, find, union


def _source_rank(row: dict[str, Any]) -> tuple[int, int, int, int, str]:
    doi = norm_doi(row.get("doi", ""))
    formal = bool(norm_doi(row.get("formal_doi", "")) or (doi and not is_arxiv_doi(doi)))
    preprint = str(row.get("is_preprint", "")).casefold() in {"1", "true", "yes"}
    source_names = {part.strip().casefold() for part in norm_text(row.get("source_database", "")).split(";")}
    authority = max(
        ({"ieee xplore": 4, "acm dl": 4, "dblp": 3, "openalex": 2, "semantic scholar": 1, "arxiv": 0}.get(name, 0) for name in source_names),
        default=0,
    )
    return (int(formal), int(not preprint), authority, record_completeness(row), norm_text(row.get("original_record_id", "")))


def _split_field_values(rows: list[dict[str, Any]], field: str) -> set[str]:
    values: set[str] = set()
    for row in rows:
        for raw in norm_text(row.get(field, "")).split(";"):
            value = norm_text(raw)
            if value:
                values.add(value)
    return values


def merge_dedupe(
    allrows: list[dict[str, Any]], *, return_report: bool = False
) -> list[dict[str, Any]] | tuple[list[dict[str, Any]], dict[str, Any]]:
    """Cluster DOI/arXiv/title-year aliases and keep the most complete representative."""
    rows = [dict(row) for row in allrows]
    if not rows:
        result: list[dict[str, Any]] = []
        return (result, {"clusters": [], "duplicate_rows": 0}) if return_report else result

    parent, find, union = _union_find(len(rows))
    component_strong: dict[int, set[str]] = {
        index: set(strong_identity_aliases(row)) for index, row in enumerate(rows)
    }

    def join(left: int, right: int, *, soft: bool = False) -> bool:
        """Union two rows, refusing a soft edge between conflicting IDs."""
        left_root, right_root = find(left), find(right)
        if left_root == right_root:
            return True
        if soft and component_strong.get(left_root) and component_strong.get(right_root):
            if component_strong[left_root].isdisjoint(component_strong[right_root]):
                return False
        union(left_root, right_root)
        merged_root = find(left_root)
        merged_ids = set(component_strong.get(left_root, set()))
        merged_ids.update(component_strong.get(right_root, set()))
        component_strong[merged_root] = merged_ids
        if left_root != merged_root:
            component_strong.pop(left_root, None)
        if right_root != merged_root:
            component_strong.pop(right_root, None)
        return True

    soft_prefixes = ("title_year:", "author_year:", "title_missing_year:")
    alias_members: dict[str, list[int]] = defaultdict(list)
    for index, row in enumerate(rows):
        for alias in dedupe_aliases(row):
            alias_members[alias].append(index)

    # First join exact identifiers, provider IDs and existing canonical keys.
    for alias, members in alias_members.items():
        if alias.startswith(soft_prefixes):
            continue
        owner = members[0]
        for index in members[1:]:
            # Legacy title-derived keys can collide across different DOI
            # records, so keep the strong-identifier conflict guard for them.
            join(index, owner, soft=alias.startswith("key:"))

    # Soft title/year aliases are useful for a DOI-bearing row and a sparse
    # provider row, but must never collapse two different DOI/arXiv records.
    for alias, members in alias_members.items():
        if not alias.startswith(soft_prefixes) or len(members) < 2:
            continue
        strong_rows = {
            frozenset(strong_identity_aliases(rows[index]))
            for index in members
            if strong_identity_aliases(rows[index])
        }
        ambiguous_ids = len(strong_rows) > 1
        sparse = [index for index in members if not strong_identity_aliases(rows[index])]
        identified = [index for index in members if strong_identity_aliases(rows[index])]
        if ambiguous_ids:
            # Keep sparse rows together, but do not arbitrarily attach them to
            # one of several different DOI identities.
            soft_groups = [sparse]
            identified_groups: dict[frozenset[str], list[int]] = defaultdict(list)
            for index in identified:
                identified_groups[frozenset(strong_identity_aliases(rows[index]))].append(index)
            soft_groups.extend(identified_groups.values())
        else:
            soft_groups = [members]
        for group in soft_groups:
            if len(group) < 2:
                continue
            owner = group[0]
            for index in group[1:]:
                join(index, owner, soft=True)

    # Link a preprint and its formal publication when providers expose the
    # same title/lead author but different years.  The year distance and
    # preprint guard avoid collapsing unrelated same-title papers.
    title_buckets: dict[tuple[str, str], list[int]] = defaultdict(list)
    for index, row in enumerate(rows):
        title = norm_title(row.get("title", ""))
        authors = split_authors(row.get("authors", ""))
        author = norm_title(authors[0] if authors else "")
        if title and author:
            title_buckets[(title, author)].append(index)
    for members in title_buckets.values():
        for left_pos, left in enumerate(members):
            left_year = row_year(rows[left])
            left_preprint = str(rows[left].get("is_preprint", "")).casefold() in {"1", "true", "yes"}
            for right in members[left_pos + 1 :]:
                right_year = row_year(rows[right])
                right_preprint = str(rows[right].get("is_preprint", "")).casefold() in {"1", "true", "yes"}
                if left_year is None or right_year is None:
                    continue
                if abs(left_year - right_year) <= 2 and (left_preprint or right_preprint):
                    left_ids = strong_identity_aliases(rows[left])
                    right_ids = strong_identity_aliases(rows[right])
                    conflicting = bool(left_ids and right_ids and left_ids.isdisjoint(right_ids))
                    cross_version = (
                        any(value.startswith("arxiv:") for value in left_ids)
                        and any(value.startswith("doi:") for value in right_ids)
                    ) or (
                        any(value.startswith("doi:") for value in left_ids)
                        and any(value.startswith("arxiv:") for value in right_ids)
                    )
                    if not conflicting or cross_version:
                        join(left, right)

    groups: dict[int, list[int]] = defaultdict(list)
    for index in range(len(rows)):
        groups[find(index)].append(index)

    merged: list[dict[str, Any]] = []
    cluster_report: list[dict[str, Any]] = []
    for member_indexes in sorted(groups.values(), key=lambda values: min(values)):
        members = [rows[index] for index in member_indexes]
        representative = max(members, key=_source_rank)
        current = dict(representative)
        current["source_database"] = ";".join(sorted(_split_field_values(members, "source_database")))
        current["query_id"] = ";".join(sorted(_split_field_values(members, "query_id")))
        # Prefer the longest non-empty abstract and fill metadata gaps from the cluster.
        abstracts = [norm_text(row.get("abstract", "")) for row in members if norm_text(row.get("abstract", ""))]
        if abstracts:
            current["abstract"] = max(abstracts, key=len)
        for field in [
            "title",
            "authors",
            "publication_year",
            "publication_date",
            "venue",
            "doi",
            "url",
            "type",
            "arxiv_id",
            "formal_doi",
        ]:
            candidates = [
                row.get(field, "")
                for row in members
                if norm_text(row.get(field, ""))
            ]
            if not candidates:
                continue
            if field == "publication_date":
                # Prefer a valid day-level date over a year-only value even
                # when the source-ranked representative already has a
                # non-empty (but less precise) date.  This keeps the stored
                # value and the precision marker consistent after merging.
                def date_rank(value: Any) -> tuple[int, int, int]:
                    parsed, exact = parse_date_info(value)
                    return (
                        int(parsed is not None),
                        int(exact),
                        len(norm_text(value)),
                    )

                best_date = max(candidates, key=date_rank)
                current_date = current.get(field, "")
                if date_rank(best_date) > date_rank(current_date):
                    current[field] = best_date
            elif not norm_text(current.get(field, "")):
                current[field] = max(candidates, key=lambda value: len(norm_text(value)))
        counts = []
        for row in members:
            try:
                counts.append(int(row.get("cited_by_count") or 0))
            except (TypeError, ValueError):
                pass
        if counts:
            current["cited_by_count"] = max(counts)
        precisions: set[str] = set()
        for row in members:
            precision = norm_text(row.get("_date_precision", ""))
            if not precision:
                parsed, exact = parse_date_info(
                    row.get("publication_date", "") or row.get("publication_year", "")
                )
                precision = "day" if exact else "year" if parsed else "unknown"
            precisions.add(precision)
        if "day" in precisions:
            current["_date_precision"] = "day"
        elif "year" in precisions:
            current["_date_precision"] = "year"
        else:
            current["_date_precision"] = "unknown"
        current["doi"] = norm_doi(current.get("doi", ""))
        current["formal_doi"] = norm_doi(current.get("formal_doi", ""))
        if current["formal_doi"] and not is_arxiv_doi(current["formal_doi"]):
            current["is_preprint"] = "0"
        elif current["doi"] and not is_arxiv_doi(current["doi"]):
            current["formal_doi"] = current["formal_doi"] or current["doi"]
            current["is_preprint"] = "0"
        current["record_key"] = make_key(current)
        merged.append(current)
        cluster_report.append(
            {
                "record_key": current["record_key"],
                "member_count": len(members),
                "member_source_ids": [norm_text(row.get("original_record_id", "")) for row in members],
                "aliases": sorted({alias for row in members for alias in dedupe_aliases(row)}),
            }
        )

    report = {
        "clusters": cluster_report,
        "duplicate_rows": sum(max(0, item["member_count"] - 1) for item in cluster_report),
        "raw_rows": len(rows),
        "unique_rows": len(merged),
    }
    return (merged, report) if return_report else merged


def seen_filter(rows: list[dict[str, Any]], seen_path: Path | None = None) -> list[dict[str, Any]]:
    """Filter without side effects; state is committed only after all outputs succeed."""
    seen = read_seen_keys(seen_path or (STATE / "seen_keys.txt"))
    return [row for row in rows if not row_is_seen(row, seen)]


def _failure_metadata(exc: Exception) -> dict[str, Any]:
    cause = getattr(exc, "cause", exc)
    result: dict[str, Any] = {"code": "PROVIDER_ERROR", "block_source": False}
    if isinstance(cause, ProviderCircuitOpenError):
        result.update(
            code="CIRCUIT_OPEN",
            block_source=True,
            retry_not_before=cause.retry_not_before,
        )
    elif isinstance(cause, ProviderInvalidResponseError):
        result.update(
            code="INVALID_RESPONSE",
            block_source=True,
            http_status=cause.status_code,
        )
    elif isinstance(cause, ProviderHTTPStatusError):
        result["http_status"] = cause.status_code
        if cause.status_code in {401, 403}:
            result.update(code="API_ACCESS_DENIED", block_source=True)
        elif cause.status_code == 429:
            result.update(code="RATE_LIMITED", block_source=True)
            if cause.retry_after_seconds is not None:
                result["retry_not_before"] = (
                    datetime.now(timezone.utc)
                    + timedelta(seconds=cause.retry_after_seconds)
                ).replace(microsecond=0).isoformat()
        elif cause.status_code >= 500:
            result.update(code="TRANSIENT_HTTP", block_source=True)
    elif isinstance(cause, requests.exceptions.Timeout):
        result.update(code="TIMEOUT", block_source=True)
    elif isinstance(cause, requests.exceptions.ConnectionError):
        result.update(code="CONNECTION_ERROR", block_source=True)
    return result


def _run_source(
    function: Callable[..., list[dict[str, Any]]],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    failures: list[dict[str, Any]],
    call_log: list[dict[str, Any]],
    variant: dict[str, Any],
    source: str,
    blocked_sources: set[str],
) -> list[dict[str, Any]]:
    if source in blocked_sources:
        return []
    before = len(call_log)

    def finish(raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        accepted = [row for row in raw_rows if variant_matches(row, variant)]
        for row in accepted:
            row.setdefault("_query_parent_id", variant.get("parent_id", ""))
            row.setdefault("_query_lane", variant.get("lane", ""))
            if variant.get("seed_title"):
                row["_seed_title"] = variant["seed_title"]
        for call in call_log[before:]:
            call["raw_returned"] = len(raw_rows)
            call["accepted"] = len(accepted)
            call["filtered_by_variant"] = max(0, len(raw_rows) - len(accepted))
            # A deliberately enabled optional source with no credential is a
            # degraded run, not a successful zero-result query.  Record it as
            # a failure so the caller will not advance seen state by default.
            if call.get("status") == "disabled":
                failures.append(
                    {
                        "source": call.get("source", source),
                        "query_id": call.get("query_id", variant.get("id", "")),
                        "error": f"disabled: {call.get('reason', 'source unavailable')}",
                        "partial_rows": len(raw_rows),
                    }
                )
        _sleep_delay(float(CFG.get("between_query_delay_seconds", 0.15)))
        return accepted

    try:
        raw_rows = function(*args, **kwargs)
    except Exception as exc:
        partial_rows = list(getattr(exc, "rows", []) or [])
        metadata = _failure_metadata(exc)
        if metadata.pop("block_source"):
            blocked_sources.add(source)
        failures.append({
            "source": source,
            "query_id": variant.get("id", ""),
            "error": _safe_error_message(exc),
            "partial_rows": len(partial_rows),
            "start": args[2].isoformat() if len(args) > 2 else "",
            "end": args[3].isoformat() if len(args) > 3 else "",
            **metadata,
        })
        print(f"{function.__name__} warning: {_safe_error_message(exc)}")
        return finish(partial_rows) if partial_rows else []

    return finish(raw_rows)


def collect(
    start: date,
    end: date,
    limit: int,
    *,
    call_log: list[dict[str, Any]] | None = None,
    include_arxiv: bool | None = None,
    include_ieee: bool | None = None,
    include_dblp: bool | None = None,
    include_openalex: bool | None = None,
    include_semantic_scholar: bool | None = None,
    parent_ids: set[str] | None = None,
    include_seed_queries: bool = False,
    seed_only: bool = False,
    include_foundational: bool = False,
    strict_dates: bool | None = None,
    arxiv_oai_cache_path: Path | None = None,
    arxiv_oai_cache_start: date | None = None,
    arxiv_oai_set_specs: tuple[str, ...] = ARXIV_OAI_SETS,
    blocked_sources: set[str] | None = None,
    validate_configuration: bool = True,
    stop_on_failure: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    allrows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    call_log = call_log if call_log is not None else []
    blocked_sources = blocked_sources if blocked_sources is not None else set()
    enabled = resolve_enabled_sources(
        include_arxiv=include_arxiv,
        include_ieee=include_ieee,
        include_dblp=include_dblp,
        include_openalex=include_openalex,
        include_semantic_scholar=include_semantic_scholar,
    )
    if validate_configuration:
        preflight = source_preflight(enabled, strict_credentials=False)
        if preflight["errors"]:
            for item in preflight["errors"]:
                call_log.append(
                    {
                        "source": item["source"],
                        "query_id": "PREFLIGHT",
                        "status": "disabled",
                        "reason": f"{item['env_var']} is not configured",
                        "pages": 0,
                        "returned": 0,
                        "started_at": preflight["checked_at"],
                        "finished_at": preflight["checked_at"],
                    }
                )
                failures.append(
                    {
                        "source": item["source"],
                        "query_id": "PREFLIGHT",
                        "error": f"disabled: {item['env_var']} is not configured",
                        "partial_rows": 0,
                        "code": item["code"],
                    }
                )
            return allrows, call_log, failures

    openalex_enabled = enabled["openalex"]
    semantic_enabled = enabled["semantic_scholar"]
    arxiv_enabled = enabled["arxiv"]
    ieee_enabled = enabled["ieee"]
    dblp_enabled = enabled["dblp"]

    generic_variants = [
        variant
        for variant in query_variants(
            parent_ids=parent_ids,
            include_seed=include_seed_queries,
            source="OpenAlex",
        )
        if include_foundational or not variant.get("backfill_only", False)
    ]
    if seed_only:
        generic_variants = [variant for variant in generic_variants if variant.get("parent_id") == "SEED"]
    for variant in generic_variants:
        query_id = variant["id"]
        query = variant["query"]
        if openalex_enabled:
            allrows += _run_source(
                openalex,
                (query_id, query, start, end, limit),
                {"call_log": call_log, "strict_dates": strict_dates},
                failures=failures,
                call_log=call_log,
                variant=variant,
                source="OpenAlex",
                blocked_sources=blocked_sources,
            )
            if stop_on_failure and failures:
                return allrows, call_log, failures
        if semantic_enabled:
            s2_variant = query_for_source(variant, "Semantic Scholar")
            allrows += _run_source(
                semantic_scholar,
                (s2_variant["id"], s2_variant["query"], start, end, limit),
                {"call_log": call_log, "strict_dates": strict_dates},
                failures=failures,
                call_log=call_log,
                variant=s2_variant,
                source="Semantic Scholar",
                blocked_sources=blocked_sources,
            )
            if stop_on_failure and failures:
                return allrows, call_log, failures
        if dblp_enabled:
            dblp_variant = query_for_source(variant, "DBLP")
            allrows += _run_source(
                dblp,
                (dblp_variant["id"], dblp_variant["query"], start, end, limit),
                {"call_log": call_log, "strict_dates": strict_dates},
                failures=failures,
                call_log=call_log,
                variant=dblp_variant,
                source="DBLP",
                blocked_sources=blocked_sources,
            )
            if stop_on_failure and failures:
                return allrows, call_log, failures

    if arxiv_enabled and not seed_only:
        for variant in arxiv_variants(parent_ids=parent_ids):
            allrows += _run_source(
                arxiv,
                (variant["id"], variant["query"], start, end, limit),
                {
                    "call_log": call_log,
                    "strict_dates": strict_dates,
                    "oai_cache_path": arxiv_oai_cache_path,
                    "oai_cache_start": arxiv_oai_cache_start,
                    "oai_set_specs": arxiv_oai_set_specs,
                },
                failures=failures,
                call_log=call_log,
                variant=variant,
                source="arXiv",
                blocked_sources=blocked_sources,
            )
            if stop_on_failure and failures:
                return allrows, call_log, failures

    if ieee_enabled:
        ieee_variants = [
            variant
            for variant in query_variants(
                parent_ids=parent_ids,
                include_seed=include_seed_queries,
                source="IEEE Xplore",
            )
            if include_foundational or not variant.get("backfill_only", False)
        ]
        if seed_only:
            ieee_variants = [
                variant
                for variant in ieee_variants
                if variant.get("parent_id") == "SEED"
            ]
        for variant in ieee_variants:
            allrows += _run_source(
                ieee,
                (variant["id"], variant["query"], start, end, limit),
                {"call_log": call_log, "strict_dates": strict_dates},
                failures=failures,
                call_log=call_log,
                variant=variant,
                source="IEEE Xplore",
                blocked_sources=blocked_sources,
            )
            if stop_on_failure and failures:
                return allrows, call_log, failures

    return allrows, call_log, failures


def _probe_schema_error(
    response: requests.Response,
    provider: str,
    expected: str,
) -> ProviderInvalidResponseError:
    return ProviderInvalidResponseError(
        provider,
        expected,
        status_code=int(response.status_code),
        content_type=str(response.headers.get("Content-Type", "")),
        detail=_response_detail(response) or "<unexpected response schema>",
    )


def _live_probe_request(
    source: str,
    *,
    arxiv_transport: str = "search",
) -> requests.Response:
    """Issue a minimal request against the endpoint used by retrieval."""
    contact = os.getenv("OPENALEX_POLITE_EMAIL", "").strip()
    user_agent = _arxiv_user_agent()
    if source == "openalex":
        params: dict[str, Any] = {
            "search": "software security",
            "per-page": 1,
        }
        key = os.getenv("OPENALEX_API_KEY", "").strip()
        if key:
            params["api_key"] = key
        if contact:
            params["mailto"] = contact
        response = _get_with_retry(
            "https://api.openalex.org/works",
            params=params,
            headers={"User-Agent": user_agent},
            provider=source,
            max_retries=0,
            expected_format="json",
        )
        if "results" not in response.json():
            raise _probe_schema_error(response, source, "OpenAlex JSON schema")
        return response
    if source == "semantic_scholar":
        response = _get_with_retry(
            "https://api.semanticscholar.org/graph/v1/paper/search/bulk",
            params={
                "query": "software security",
                "fields": "paperId",
                "limit": 1,
            },
            headers={
                "x-api-key": os.getenv("S2_API_KEY", "").strip(),
                "User-Agent": user_agent,
            },
            provider=source,
            max_retries=0,
            expected_format="json",
        )
        if "data" not in response.json():
            raise _probe_schema_error(
                response, source, "Semantic Scholar JSON schema"
            )
        return response
    if source == "arxiv":
        if arxiv_transport == "oai":
            response = _get_with_retry(
                _arxiv_oai_url(),
                params={"verb": "Identify"},
                headers={"User-Agent": user_agent},
                provider="arxiv_oai",
                max_retries=2,
                expected_format="xml",
            )
            root = ET.fromstring(response.content)
            if root.find(".//oai:Identify", ARXIV_OAI_NAMESPACES) is None:
                raise _probe_schema_error(
                    response, "arxiv_oai", "OAI-PMH Identify response"
                )
            return response
        if arxiv_transport != "search":
            raise ValueError(
                f"unsupported arXiv probe transport: {arxiv_transport}"
            )
        response = _get_with_retry(
            _arxiv_api_url(),
            params={
                "search_query": "cat:cs.CR",
                "start": 0,
                "max_results": 1,
            },
            headers={"User-Agent": user_agent},
            provider=source,
            max_retries=0,
            expected_format="xml",
        )
        root = ET.fromstring(response.content)
        if not root.tag.endswith("feed"):
            raise _probe_schema_error(response, source, "Atom feed")
        return response
    if source == "ieee":
        current_year = local_today().year
        response = _get_with_retry(
            "https://ieeexploreapi.ieee.org/api/v1/search/articles",
            params={
                "apikey": os.getenv("IEEE_API_KEY", "").strip(),
                "format": "json",
                "querytext": "software security",
                "start_year": current_year - 1,
                "end_year": current_year,
                "max_records": 1,
                "start_record": 1,
            },
            headers={"User-Agent": user_agent},
            provider=source,
            max_retries=0,
            expected_format="json",
        )
        payload = response.json()
        if "articles" not in payload and "total_records" not in payload:
            raise _probe_schema_error(response, source, "IEEE Xplore JSON schema")
        return response
    if source == "dblp":
        response = _get_with_retry(
            "https://dblp.org/search/publ/api",
            params={"q": "software security", "format": "json", "h": 1, "f": 0},
            headers={"Accept": "application/json", "User-Agent": user_agent},
            provider=source,
            max_retries=0,
            expected_format="json",
        )
        if "result" not in response.json():
            raise _probe_schema_error(response, source, "DBLP JSON schema")
        return response
    raise ValueError(f"unsupported source probe: {source}")


def live_source_probe(
    enabled_sources: dict[str, bool],
    *,
    strict_credentials: bool = True,
    arxiv_transport: str = "search",
) -> dict[str, Any]:
    """Validate real endpoint access with one HTTP attempt per enabled source."""
    if arxiv_transport not in {"search", "oai"}:
        raise ValueError(
            f"unsupported arXiv probe transport: {arxiv_transport}"
        )
    preflight = source_preflight(
        enabled_sources, strict_credentials=strict_credentials
    )
    report: dict[str, Any] = {
        "checked_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "mode": "live_source_probe",
        "arxiv_transport": arxiv_transport,
        "local_preflight": preflight,
        "probes": {},
        "status": "failed" if preflight["errors"] else "running",
    }
    if preflight["errors"]:
        report["provider_runtime"] = {}
        return report

    reset_provider_runtime_state()
    for source in SOURCE_KEYS:
        if not enabled_sources.get(source, False):
            continue
        label = SOURCE_LABELS[source]
        started_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        try:
            response = _live_probe_request(
                source, arxiv_transport=arxiv_transport
            )
            report["probes"][label] = {
                "status": "ok",
                "http_status": int(response.status_code),
                "started_at": started_at,
                "finished_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            }
        except Exception as exc:
            metadata = _failure_metadata(exc)
            metadata.pop("block_source", None)
            report["probes"][label] = {
                "status": "error",
                "error": _safe_error_message(exc),
                "started_at": started_at,
                "finished_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                **metadata,
            }
    report["provider_runtime"] = provider_runtime_snapshot()
    report["status"] = (
        "ok"
        if report["probes"]
        and all(item["status"] == "ok" for item in report["probes"].values())
        else "failed"
    )
    return report


def _safe_component(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in str(value))


def _new_output_path(directory: Path, stem: str, suffix: str = ".csv") -> Path:
    """Return a collision-free path; retrieval runs are append-only archives."""
    candidate = directory / f"{stem}{suffix}"
    if not candidate.exists():
        return candidate
    index = 2
    while True:
        candidate = directory / f"{stem}_{index}{suffix}"
        if not candidate.exists():
            return candidate
        index += 1


def run_incremental(
    start: date,
    end: date,
    limit: int,
    *,
    run_id: str | None = None,
    output_dir: Path | None = None,
    commit_state: bool = True,
    allow_partial_commit: bool = False,
    include_arxiv: bool | None = None,
    include_ieee: bool | None = None,
    include_dblp: bool | None = None,
    include_openalex: bool | None = None,
    include_semantic_scholar: bool | None = None,
    parent_ids: set[str] | None = None,
    include_seed_queries: bool = False,
    include_foundational: bool = False,
    strict_dates: bool | None = True,
    state_path: Path | None = None,
    strict_credentials: bool = True,
) -> dict[str, Any]:
    if start > end:
        raise ValueError("start date must not be after end date")
    if limit < 1:
        raise ValueError("limit must be positive")
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
    run_id = run_id or make_run_id("incremental")
    output_dir = Path(output_dir) if output_dir else NORM
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = (
        MANIFEST_DIR / f"{_safe_component(run_id)}.json"
        if output_dir == NORM
        else output_dir / f"{_safe_component(run_id)}.manifest.json"
    )
    if manifest_path.exists():
        raise FileExistsError(f"run manifest already exists: {manifest_path}")
    call_log: list[dict[str, Any]] = []
    blocked_sources: set[str] = set()
    allrows, call_log, failures = collect(
        start,
        end,
        limit,
        call_log=call_log,
        include_arxiv=enabled_sources["arxiv"],
        include_ieee=enabled_sources["ieee"],
        include_dblp=enabled_sources["dblp"],
        include_openalex=enabled_sources["openalex"],
        include_semantic_scholar=enabled_sources["semantic_scholar"],
        parent_ids=parent_ids,
        include_seed_queries=include_seed_queries,
        include_foundational=include_foundational,
        strict_dates=strict_dates,
        blocked_sources=blocked_sources,
        validate_configuration=False,
    )
    merged, dedupe_report = merge_dedupe(allrows, return_report=True)
    date_precision_counts = Counter(
        norm_text(row.get("_date_precision", "unknown")) or "unknown" for row in merged
    )
    seen_path = Path(state_path) if state_path else STATE / "seen_keys.txt"
    new = seen_filter(merged, seen_path)
    stamp = end.isoformat()
    archive_id = _safe_component(run_id)
    all_path = _new_output_path(output_dir, f"candidates_all_{stamp}_{archive_id}")
    new_path = _new_output_path(output_dir, f"new_candidates_{stamp}_{archive_id}")
    manifest = {
        "run_id": run_id,
        "mode": "incremental",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "limit_per_variant": limit,
        "created_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "raw_rows": len(allrows),
        "unique_rows": len(merged),
        "new_rows": len(new),
        "source_failures": failures,
        "source_preflight": preflight,
        "blocked_sources": sorted(blocked_sources),
        "provider_runtime": provider_runtime_snapshot(),
        "run_status": (
            "failed" if failures and not merged else "degraded" if failures else "ok"
        ),
        "source_calls": call_log,
        "query_catalog": [
            variant
            for variant in query_variants(
                parent_ids=parent_ids,
                include_seed=include_seed_queries,
            )
            if include_foundational or not variant.get("backfill_only", False)
        ],
        "provider_query_catalog": {
            source: [
                variant
                for variant in query_variants(
                    parent_ids=parent_ids,
                    include_seed=include_seed_queries,
                    source=source,
                )
                if include_foundational or not variant.get("backfill_only", False)
            ]
            for source in ("OpenAlex", "Semantic Scholar", "DBLP", "IEEE Xplore")
        },
        "arxiv_catalog": list(arxiv_variants(parent_ids=parent_ids)),
        "arxiv_api_url": _arxiv_api_url(),
        "dedupe": dedupe_report,
        "date_precision_counts": dict(date_precision_counts),
        "strict_dates": strict_dates,
        "include_foundational": include_foundational,
        "state_path": str(seen_path),
        "state_commit_requested": commit_state,
        "allow_partial_commit": allow_partial_commit,
        "state_commit_reason": (
            "dry_run"
            if not commit_state
            else "source_failures"
            if failures and not allow_partial_commit
            else "pending"
        ),
        "output_files": [],
        "state_committed": False,
    }
    # All candidate files are written before touching seen state.  A failed
    # source or failed write therefore remains retryable on the next run.
    write_csv(all_path, merged, FIELDS)
    write_csv(new_path, new, FIELDS)
    manifest["output_files"] = [str(all_path), str(new_path)]
    if output_dir == NORM:
        latest_path = ROOT / "exports" / "latest_new.csv"
        write_csv(latest_path, new, FIELDS)
        manifest["output_files"].append(str(latest_path))

    # Persist a provisional manifest before committing state.  If this write
    # fails, the run remains completely retryable.
    write_json(manifest_path, manifest)

    should_commit = commit_state and (not failures or allow_partial_commit)
    if should_commit:
        commit_seen_keys(merged, seen_path)
        manifest["state_committed"] = True
        manifest["state_commit_reason"] = (
            "committed_with_source_failures" if failures else "committed"
        )
        write_json(manifest_path, manifest)
    return manifest


def _parse_cli_date(value: str) -> date:
    parsed, exact = parse_date_info(value)
    if not parsed or not exact:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD: {value}")
    return parsed


def parse_source_selection(value: str) -> set[str]:
    selected: set[str] = set()
    unknown: list[str] = []
    for raw in value.split(","):
        name = raw.strip().casefold().replace(" ", "_")
        if not name:
            continue
        resolved = SOURCE_INPUT_ALIASES.get(name)
        if resolved:
            selected.add(resolved)
        else:
            unknown.append(raw.strip())
    if unknown:
        raise ValueError(f"unknown sources: {', '.join(unknown)}")
    if not selected:
        raise ValueError("at least one source must be selected")
    return selected


def parse_query_selection(values: list[str] | None) -> set[str] | None:
    if not values:
        return None
    selected = {
        item.strip().upper()
        for value in values
        for item in value.split(",")
        if item.strip()
    }
    unknown = sorted(selected - set(QUERIES))
    if unknown:
        raise ValueError(f"unknown query ids: {', '.join(unknown)}")
    return selected


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the literature monitor incremental retrieval.")
    parser.add_argument("--start-date", type=_parse_cli_date)
    parser.add_argument("--end-date", type=_parse_cli_date)
    parser.add_argument(
        "--max-results-per-short-query",
        "--limit",
        type=int,
        default=int(CFG.get("max_results_per_short_query", 100)),
    )
    parser.add_argument("--run-id")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--dry-run", action="store_true", help="write to a validation directory and never commit seen state")
    parser.add_argument("--sources", help="comma-separated source list: openalex,semantic_scholar,arxiv,ieee,dblp")
    parser.add_argument("--query-id", action="append", help="limit to a query family; repeat or use commas")
    parser.add_argument("--preflight-only", action="store_true", help="validate enabled sources and credentials without network access")
    parser.add_argument("--live-probe-only", action="store_true", help="make one real request per enabled source without writing results or state")
    parser.add_argument("--no-arxiv", action="store_true")
    parser.add_argument("--no-openalex", action="store_true")
    parser.add_argument("--no-semantic-scholar", action="store_true")
    parser.add_argument("--enable-dblp", action="store_true")
    parser.add_argument("--no-dblp", action="store_true")
    parser.add_argument("--enable-ieee", action="store_true")
    parser.add_argument("--no-ieee", action="store_true")
    parser.add_argument("--include-seed-queries", action="store_true", help="run exact-title seed probes for recall validation")
    parser.add_argument("--include-foundational", action="store_true", help="include backfill-only foundation variants in an incremental run")
    parser.add_argument("--allow-year-only-dates", action="store_true", help="include records whose provider exposes only a publication year")
    parser.add_argument("--allow-partial-commit", action="store_true")
    args = parser.parse_args()

    if args.preflight_only and args.live_probe_only:
        parser.error("--preflight-only and --live-probe-only are mutually exclusive")

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
        parent_ids = parse_query_selection(args.query_id)
    except ValueError as exc:
        parser.error(str(exc))

    if selected_sources is not None:
        source_overrides = {
            key: key in selected_sources for key in SOURCE_KEYS
        }
    else:
        source_overrides = {
            "openalex": False if args.no_openalex else None,
            "semantic_scholar": False if args.no_semantic_scholar else None,
            "arxiv": False if args.no_arxiv else None,
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
        report = live_source_probe(enabled, strict_credentials=True)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        if report["status"] != "ok":
            raise SystemExit(2)
        return

    default_start, default_end = date_window()
    start = args.start_date or default_start
    end = args.end_date or default_end
    if start > end:
        parser.error("start date must not be after end date")
    run_id = args.run_id or make_run_id("incremental")
    output_dir = args.output_dir
    if args.dry_run and output_dir is None:
        output_dir = ROOT / "data" / "validation" / run_id
    try:
        manifest = run_incremental(
            start,
            end,
            max(1, args.max_results_per_short_query),
            run_id=run_id,
            output_dir=output_dir,
            commit_state=not args.dry_run,
            allow_partial_commit=args.allow_partial_commit,
            include_arxiv=source_overrides["arxiv"],
            include_ieee=source_overrides["ieee"],
            include_dblp=source_overrides["dblp"],
            include_openalex=source_overrides["openalex"],
            include_semantic_scholar=source_overrides["semantic_scholar"],
            parent_ids=parent_ids,
            include_seed_queries=args.include_seed_queries,
            include_foundational=args.include_foundational,
            strict_dates=not args.allow_year_only_dates,
        )
    except SourceConfigurationError as exc:
        parser.error(str(exc))
    print(
        json.dumps(
            {
                "run_id": manifest["run_id"],
                "start": manifest["start"],
                "end": manifest["end"],
                "raw": manifest["raw_rows"],
                "unique": manifest["unique_rows"],
                "new": manifest["new_rows"],
                "failures": len(manifest["source_failures"]),
                "run_status": manifest["run_status"],
                "state_committed": manifest["state_committed"],
                "state_commit_reason": manifest["state_commit_reason"],
            },
            ensure_ascii=False,
        )
    )
    if manifest["run_status"] != "ok":
        raise SystemExit(2)


if __name__ == "__main__":
    main()