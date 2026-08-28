"""Test URL edge cases, model validation, and specialized errors."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

import pytest

from archivist.core._urls import (
    sanitize_url_for_log,
    validate_cdx_query,
    validate_service_url,
    validate_target_url,
)
from archivist.core.errors import (
    CaptureFailedError,
    InvalidTargetURLError,
    PollingTimeoutError,
)
from archivist.core.models import ArchiveRecord, CaptureJob, PagedSearchResult


@pytest.mark.parametrize("query", ["", "example.com/a path", "example.com/\x00path"])
def test_cdx_query_rejects_empty_whitespace_and_control_characters(query: str) -> None:
    """Reject empty CDX queries and queries containing unsafe characters."""
    with pytest.raises(InvalidTargetURLError):
        validate_cdx_query(query)


def test_url_validation_rejects_non_strings_bad_ports_and_service_suffixes() -> None:
    """Reject invalid target types, ports, and service URL suffixes."""
    with pytest.raises(InvalidTargetURLError, match="non-empty string"):
        validate_target_url(cast("Any", 42))
    with pytest.raises(InvalidTargetURLError, match="not a valid URL"):
        validate_target_url("https://example.com:not-a-port")

    for url in (
        "https://service.example/base?token=secret",
        "https://service.example/base#fragment",
        "https://service.example/base?",
        "https://service.example/base#",
    ):
        with pytest.raises(InvalidTargetURLError, match="query or fragment"):
            validate_service_url(url)


def test_log_url_sanitizing_handles_ipv6_default_paths_and_parse_errors() -> None:
    """Sanitize IPv6 URLs and safely handle URL parsing failures."""
    assert sanitize_url_for_log("https://[2001:db8::1]") == "https://[2001:db8::1]/"
    assert sanitize_url_for_log("https://example.com") == "https://example.com/"
    assert sanitize_url_for_log("https://example.com:not-a-port") == "<invalid-url>"
    assert sanitize_url_for_log(cast("Any", None)) == "<invalid-url>"


def test_archive_record_accepts_an_absent_timestamp_and_normalizes_to_utc() -> None:
    """Accept missing archive timestamps and normalize present ones to UTC."""
    assert (
        ArchiveRecord(service="Test", archive_url="https://archive.example").archived_at
        is None
    )

    east = datetime.fromisoformat("2020-01-01T02:00:00+02:00")
    record = ArchiveRecord(
        service="Test", archive_url="https://archive.example", archived_at=east
    )
    assert record.archived_at == datetime(2020, 1, 1, tzinfo=UTC)


def test_shared_model_representations_hide_sensitive_urls() -> None:
    """Keep target and archive URLs out of shared model representations."""
    secret = "https://user:password@example.com/private?token=secret"
    representation = repr(
        (
            ArchiveRecord(service="Test", archive_url=secret, original_url=secret),
            CaptureJob(service="Test", job_id="job", target_url=secret, message=secret),
        )
    )
    assert secret not in representation
    assert "password" not in representation
    assert "token" not in representation


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"page": 0}, "page must be at least 1"),
        ({"page_count": 0}, "page_count must be at least 1"),
        ({"total_count": -1}, "total_count cannot be negative"),
    ],
)
def test_paged_search_result_rejects_invalid_counts(
    kwargs: dict[str, int], message: str
) -> None:
    """Reject invalid page and result counts."""
    with pytest.raises(ValueError, match=message):
        PagedSearchResult(items=(), **kwargs)


def test_specialized_errors_retain_non_sensitive_metadata() -> None:
    """Retain safe service, job, status, and timeout metadata."""
    server_error_status = 500
    polling_timeout = 3.5
    capture_error = CaptureFailedError(
        "capture failed",
        service="Test",
        status_code=server_error_status,
        job_id="job-1",
        service_code="failed",
    )
    assert capture_error.service == "Test"
    assert capture_error.status_code == server_error_status
    assert capture_error.job_id == "job-1"
    assert capture_error.service_code == "failed"

    timeout_error = PollingTimeoutError(
        "capture timed out",
        service="Test",
        job_id="job-2",
        timeout=polling_timeout,
    )
    assert timeout_error.service == "Test"
    assert timeout_error.status_code is None
    assert timeout_error.job_id == "job-2"
    assert timeout_error.timeout == polling_timeout
