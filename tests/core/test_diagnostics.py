"""Verify bounded diagnostic parsing, redaction, adapters, and serialization."""

from __future__ import annotations

import asyncio
import json
import pickle
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import cast

import pytest

from archivist.core import _diagnostics
from archivist.core._diagnostics import (
    MAX_BODY_BYTES,
    MAX_FIELD_LENGTH,
    MAX_HEADER_LENGTH,
    MAX_MESSAGE_LENGTH,
    async_response_diagnostics,
    parse_response_diagnostics,
    response_diagnostics,
)
from archivist.core._http import (
    ResponseLike,
    async_raise_for_common_status,
    raise_for_common_status,
)
from archivist.core.errors import (
    AuthenticationError,
    CaptureFailedError,
    InvalidServiceResponseError,
    NetworkError,
    PollingTimeoutError,
    RateLimitError,
    ServiceError,
    TLSVerificationError,
)
from archivist.services.internet_archive.item_models import (
    InternetArchiveRecoveryDeferredError,
    InternetArchiveRecoveryError,
    InternetArchiveRecoveryPhase,
    InternetArchiveRecoveryResult,
    InternetArchiveUploadError,
    InternetArchiveUploadFileResult,
    InternetArchiveUploadResult,
)
from tests.conftest import FIXTURES
from tests.core.test_http import BrokenTextResponse, awaiting


@pytest.mark.parametrize("namespace", ["", ' xmlns="urn:s3"'])
@pytest.mark.parametrize("code", ["SlowDown", "ServiceUnavailable", "FutureCode"])
def test_s3_fields_remain_strings(namespace: str, code: str) -> None:
    """Retain unknown codes and namespace-independent S3 support identifiers."""
    data = parse_response_diagnostics(
        f"<Error{namespace}><Code>{code}</Code><Message>Try later</Message>"
        "<RequestId>request-123</RequestId><Resource>private</Resource></Error>",
        {"x-amz-request-id": "header-id"},
        request_phase="item-create",
    )
    assert data["service_code"] == code
    assert data["service_message"] == "Try later"
    assert data["request_id"] == "request-123"
    assert data["request_phase"] == "item-create"
    assert "private" not in repr(data)


@pytest.mark.parametrize("field", ["error", "message", "detail", "reason", "Message"])
def test_recognized_json_explanations(field: str) -> None:
    """Accept the existing fixture-confirmed JSON reason fields and S3 casing."""
    data = parse_response_diagnostics(json.dumps({field: "Rejected"}), {})
    assert data["service_message"] == "Rejected"


def test_archive_simulation_fixture_matches_the_documented_s3_error_shape() -> None:
    """Parse a public SlowDown simulation captured on 2026-10-12, with a dummy ID."""
    body = (FIXTURES / "internet_archive" / "s3_slow_down.xml").read_text(
        encoding="utf-8"
    )
    data = parse_response_diagnostics(body, {})
    assert data["service_code"] == "SlowDown"
    assert data["service_message"] == "Please reduce your request rate."
    assert data["request_id"] == "fixture-request-id"


@pytest.mark.parametrize(
    "body",
    [
        None,
        b"bytes",
        "",
        "plain-text secret",
        "<html>password=secret</html>",
        "<Error>",
        '{"error":',
        "{",
        "[]",
        '{"error":false,"message":""}',
        '<!DOCTYPE Error [<!ENTITY x "secret">]><Error><Message>&x;</Message></Error>',
        '<!ENTITY x "secret"><Error/>',
        "x" * (MAX_BODY_BYTES + 1),
        pytest.param(
            json.dumps({"Message": "é" * MAX_BODY_BYTES}), id="oversized-unicode"
        ),
        '{"error":' + "[" * 2000 + "0" + "]" * 2000 + "}",
    ],
)
def test_unusable_bodies_keep_only_headers(body: object) -> None:
    """Malformed, unstructured, entity-bearing, or oversized bodies never escape."""
    data = parse_response_diagnostics(body, {"X-Amz-Request-Id": "support-id"})
    assert data["service_code"] is data["service_message"] is None
    assert data["request_id"] == "support-id"


def test_invalid_html_character_reference_does_not_replace_service_failure() -> None:
    """Discard malformed diagnostic strings that exceed integer decoder limits."""
    message = "&#" + "9" * 5000 + ";"
    data = parse_response_diagnostics(json.dumps({"Message": message}), {})
    assert data["service_message"] is None


def test_fields_and_header_allowlist_are_bounded_copied_and_sanitized() -> None:
    """Apply limits to structured values and exclude secret-bearing headers."""
    headers = {
        "Content-Type": "x" * 1000,
        "X-Amz-Request-Id": "r" * 1000,
        "Retry-After": "bad" * 1000,
        "Authorization": "secret",
        "Cookie": "private",
        "Set-Cookie": "private",
        "Location": "https://private.invalid/?signature=secret",
        "X-Other": "secret",
    }
    data = parse_response_diagnostics(
        json.dumps(
            {"Code": "c" * 1000, "Message": "m" * 2000, "RequestId": "i" * 1000}
        ),
        headers,
    )
    assert len(data["service_code"] or "") == MAX_FIELD_LENGTH
    assert len(data["request_id"] or "") == MAX_FIELD_LENGTH
    assert len(data["service_message"] or "") == MAX_MESSAGE_LENGTH
    assert len(data["retry_after_raw"] or "") == MAX_HEADER_LENGTH
    assert set(data["diagnostic_headers"]) == {
        "content-type",
        "retry-after",
        "x-amz-request-id",
    }
    assert all(
        len(value) == MAX_HEADER_LENGTH for value in data["diagnostic_headers"].values()
    )
    headers.clear()
    assert data["diagnostic_headers"]
    assert "secret" not in repr(data)


@pytest.mark.parametrize(
    "message",
    [
        "password=secret",
        'password="secret word"',
        "secret_key='secret word'",
        'reflected {"password": "secret"}',
        "Authorization: LOW access:secret",
        "Cookie: session=secret; other=secret",
        "Set-Cookie: session=secret",
        "Bearer secret",
        "Basic secret",
        "LOW access:secret",
        "s3secretkey=secret",
        "https://user:secret@example.invalid/path?X-Amz-Signature=secret",
        "https&#58;//example.invalid/private?signature=secret",
        '<a href="https://example.invalid/?signature=secret">Denied</a>',
        "user@example.invalid password=secret",
        "\x00\ud800",
        "<![bogus]>",
        "<b></b>",
    ],
)
def test_reflected_credentials_and_urls_are_not_exposed(message: str) -> None:
    """Redact before field truncation, including escaped URLs and quoted secrets."""
    data = parse_response_diagnostics(json.dumps({"Message": message}), {})
    assert "secret" not in repr(data)
    assert "example.invalid" not in repr(data)
    assert "\x00" not in (data["service_message"] or "")
    assert "\ud800" not in (data["service_message"] or "")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("12.5", 12.5),
        ("-2", 0.0),
        ("NaN", None),
        ("bad", None),
        ("Wed, 21 Oct 2015 07:28:00 GMT", datetime(2015, 10, 21, 7, 28, tzinfo=UTC)),
    ],
)
def test_retry_hints(raw: str, expected: float | datetime | None) -> None:
    """Keep relative, absolute, and malformed Retry-After without policy decisions."""
    data = parse_response_diagnostics("", {"RETRY-AFTER": raw})
    assert data["retry_after"] == expected
    assert data["retry_after_raw"] == raw
    assert (
        parse_response_diagnostics("", {"Content-Type": ""})["diagnostic_headers"] == {}
    )


@pytest.mark.asyncio
async def test_text_adapters_handle_regular_awaitable_broken_and_nontext_access() -> (
    None
):
    """Await lazy async text and ignore it safely on the synchronous adapter."""
    xml = "<Error><Code>SlowDown</Code></Error>"
    normal = SimpleNamespace(text=xml, headers={})
    assert (
        response_diagnostics(cast("ResponseLike", normal))["service_code"] == "SlowDown"
    )
    assert (await async_response_diagnostics(normal))["service_code"] == "SlowDown"
    assert (
        await async_response_diagnostics(
            SimpleNamespace(text=awaiting(xml), headers={})
        )
    )["service_code"] == "SlowDown"
    lazy = SimpleNamespace(text=awaiting(xml), headers={})
    assert response_diagnostics(cast("ResponseLike", lazy))["service_code"] is None
    broken = BrokenTextResponse()
    assert response_diagnostics(cast("ResponseLike", broken))["service_message"] is None
    assert (await async_response_diagnostics(broken))["service_message"] is None
    assert (await async_response_diagnostics(SimpleNamespace(text=None, headers={})))[
        "service_message"
    ] is None


@pytest.mark.parametrize(
    "error_type",
    [
        ServiceError,
        AuthenticationError,
        RateLimitError,
        InvalidServiceResponseError,
        CaptureFailedError,
        NetworkError,
        TLSVerificationError,
        PollingTimeoutError,
    ],
)
def test_service_subclasses_pickle_diagnostics(error_type: type[ServiceError]) -> None:
    """Existing subclasses retain the common fields through pickle construction."""
    headers = {"retry-after": "12"}
    error = error_type(
        "safe failure",
        service="Internet Archive",
        service_code="SlowDown",
        service_message="Try later",
        request_id="id",
        request_phase="item-create",
        retry_after=12.0,
        retry_after_raw="12",
        diagnostic_headers=headers,
    )
    headers.clear()
    restored = pickle.loads(pickle.dumps(error))
    assert type(restored) is error_type
    assert restored.args == error.args
    assert restored.__dict__ == error.__dict__
    assert restored.diagnostic_headers == {"retry-after": "12"}


@pytest.mark.asyncio
async def test_lazy_diagnostic_reads_have_a_separate_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stalled optional diagnostic read retains headers and the original failure."""
    monkeypatch.setattr(_diagnostics, "MAX_ASYNC_READ_SECONDS", 0.01)

    async def stalled() -> str:
        await asyncio.Event().wait()
        raise AssertionError("stalled diagnostic read resumed")

    response = SimpleNamespace(text=stalled(), headers={"Retry-After": "5"})
    data = await async_response_diagnostics(response)
    assert data["service_message"] is None
    assert data["retry_after"] == float(response.headers["Retry-After"])


def test_wrapper_pickle_preserves_cause_results_and_keyword_fields() -> None:
    """Restore wrappers without invoking their result-dependent constructors."""
    cause = ServiceError(
        "safe",
        status_code=503,
        service_code="SlowDown",
        retry_after=datetime(2026, 1, 1, tzinfo=UTC),
    )
    upload = InternetArchiveUploadError(
        InternetArchiveUploadResult(
            "item", (InternetArchiveUploadFileResult("file", 1, True),)
        ),
        cause,
        failed_file="later",
    )
    recovery = InternetArchiveRecoveryError(
        InternetArchiveRecoveryResult("item", ()),
        cause,
        phase=InternetArchiveRecoveryPhase.TRANSFER,
        failed_file="later",
    )
    deferred = InternetArchiveRecoveryDeferredError(
        recovery.result,
        cause,
        phase=InternetArchiveRecoveryPhase.RECONCILIATION,
    )
    for error in (upload, recovery, deferred):
        restored = pickle.loads(pickle.dumps(error))
        assert restored.result == error.result
        assert restored.args == error.args
        assert restored.cause.__dict__ == cause.__dict__
        assert restored.failed_file == error.failed_file
    assert pickle.loads(pickle.dumps(recovery)).phase == recovery.phase


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [403, 429, 503])
async def test_common_http_failures_retain_diagnostics_in_both_transports(
    status: int,
) -> None:
    """Keep retry hints and shared fields outside the item-specific helpers too."""
    body = json.dumps({"Message": "Try later", "Code": "FutureCode"})
    response = SimpleNamespace(
        status_code=status,
        text=body,
        headers={"Retry-After": "5", "Content-Type": "application/json"},
        json=lambda: json.loads(body),
    )
    with pytest.raises(ServiceError) as sync_failure:
        raise_for_common_status(cast("ResponseLike", response), service="Test")
    response.text = awaiting(body)
    with pytest.raises(type(sync_failure.value)) as async_failure:
        await async_raise_for_common_status(response, service="Test")
    assert sync_failure.value.__dict__ == async_failure.value.__dict__
    assert async_failure.value.retry_after == float(response.headers["Retry-After"])
    assert async_failure.value.request_phase == "request"
    await async_raise_for_common_status(
        SimpleNamespace(status_code=200), service="Test"
    )
