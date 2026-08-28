"""Test shared core models, URL validation, HTTP parsing, and errors."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from typing import cast

import pytest

from archivist import (
    ArchiveRecord,
    ArchivistError,
    AuthenticationError,
    InvalidServiceResponseError,
    InvalidTargetURLError,
    PagedSearchResult,
    RateLimitError,
)
from archivist.core._http import (
    ResponseLike,
    parse_retry_after,
    redact_mapping,
    response_json,
    response_text,
    translate_request_error,
)
from archivist.core._urls import (
    sanitize_url_for_log,
    validate_cdx_query,
    validate_service_url,
    validate_target_url,
)


@pytest.mark.parametrize(
    "url",
    ["", "example.com", "ftp://example.com", "https://", "https://exa mple.com"],
)
def test_validate_target_url_rejects_invalid_values(url: str) -> None:
    """Reject malformed and unsupported target URLs."""
    with pytest.raises(InvalidTargetURLError):
        validate_target_url(url)


def test_url_validation_and_log_sanitizing() -> None:
    """Validate service URLs while removing secrets from logged URLs."""
    target = "https://user:password@example.com:8443/a/path?token=secret#fragment"
    assert validate_target_url(target) == target
    assert sanitize_url_for_log(target) == "https://example.com:8443/a/path"
    assert sanitize_url_for_log("not a URL") == "<invalid-url>"
    assert (
        validate_service_url("https://archive.example/base/")
        == "https://archive.example/base"
    )
    with pytest.raises(InvalidTargetURLError):
        validate_service_url("https://user:secret@archive.example/")


def test_shared_models_are_frozen_and_require_aware_timestamps() -> None:
    """Enforce immutable models and timezone-aware archive timestamps."""
    record = ArchiveRecord(
        service="test",
        archive_url="https://archive.example/id",
        archived_at=datetime(2020, 1, 1, tzinfo=UTC),
    )
    with pytest.raises(FrozenInstanceError):
        record.service = "changed"  # ty: ignore[invalid-assignment]
    with pytest.raises(ValueError, match="timezone"):
        ArchiveRecord(
            service="test",
            archive_url="https://archive.example/id",
            archived_at=datetime(2020, 1, 1),
        )
    page = PagedSearchResult(items=(record,), page=1, page_count=2, total_count=1)
    assert list(page) == [record]
    assert len(page) == 1


def test_retry_after_and_safe_errors() -> None:
    """Parse retry metadata and retain only safe error attributes."""
    retry_seconds = 12.0
    rate_limit_status = 429
    assert parse_retry_after({"Retry-After": "12"}) == (retry_seconds, "12")
    parsed, raw = parse_retry_after({"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"})
    assert parsed == datetime(2015, 10, 21, 7, 28, tzinfo=UTC)
    assert raw == "Wed, 21 Oct 2015 07:28:00 GMT"
    assert parse_retry_after({"Retry-After": "Infinity"}) == (None, "Infinity")
    error = RateLimitError(
        "limited",
        status_code=rate_limit_status,
        retry_after=retry_seconds,
        retry_after_raw="12",
    )
    assert isinstance(error, ArchivistError)
    assert error.retry_after == retry_seconds
    assert error.status_code == rate_limit_status


def test_transport_errors_and_request_metadata_do_not_copy_secrets() -> None:
    """Exclude request secrets from translated errors and metadata."""
    source = OSError("https://user:password@example.com/?token=secret")
    translated = translate_request_error(source, service="Example")
    assert "password" not in str(translated)
    assert "secret" not in str(translated)
    assert redact_mapping(
        {"Authorization": "LOW secret", "target_password": "secret", "safe": "value"}
    ) == {
        "Authorization": "<redacted>",
        "target_password": "<redacted>",
        "safe": "value",
    }


def test_exception_metadata_is_available_without_response_bodies() -> None:
    """Expose safe exception metadata without retaining response content."""
    authentication_status = 401
    error = AuthenticationError(
        "credentials rejected",
        service="Archive",
        status_code=authentication_status,
    )
    assert error.service == "Archive"
    assert error.status_code == authentication_status
    assert str(error) == "credentials rejected"


class InvalidJsonResponse:
    """Provide a response whose JSON body cannot be decoded."""

    status_code = 200
    text = "account-body-that-must-not-be-retained"
    url = "https://archive.example/api"

    def __init__(self) -> None:
        """Initialize the response with empty headers."""
        self.headers: Mapping[str, str] = {}

    def json(self) -> object:
        """Raise the JSON decoding failure under test."""
        raise json.JSONDecodeError("invalid", self.text, 0)


def test_invalid_json_error_does_not_retain_response_body() -> None:
    """Discard response bodies and chained exceptions after invalid JSON."""
    with pytest.raises(InvalidServiceResponseError) as raised:
        response_json(InvalidJsonResponse(), service="Example")
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


def test_cdx_queries_accept_wildcards_without_weakening_target_validation() -> None:
    """Allow CDX wildcards without permitting them in target URLs."""
    assert validate_cdx_query("*.example.com/*") == "*.example.com/*"
    assert validate_cdx_query("example.com/about/") == "example.com/about/"
    assert validate_cdx_query("example.com:8080/about/") == "example.com:8080/about/"
    with pytest.raises(InvalidTargetURLError):
        validate_cdx_query("https://[::1")
    with pytest.raises(InvalidTargetURLError):
        validate_target_url("*.example.com/*")


def test_non_text_response_raises_a_package_error() -> None:
    """Translate a non-text response into a public package error."""
    response = InvalidJsonResponse()
    response.text = None  # ty: ignore[invalid-assignment]
    with pytest.raises(InvalidServiceResponseError, match="non-text"):
        response_text(cast("ResponseLike", response), service="Example")
