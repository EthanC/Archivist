"""Exercise the IAS3 check_limit reading through both client transports."""

# ruff: noqa: PLR2004 - Assertions describe exact protocol values and request counts.

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, cast

import pytest
import pytest_asyncio

from archivist import (
    AsyncInternetArchiveClient,
    InternetArchiveAccount,
    InternetArchiveApiKey,
    InternetArchiveClient,
    InternetArchiveUploadLimit,
    InvalidOptionError,
    InvalidServiceResponseError,
    NetworkError,
    RateLimitError,
    ServiceError,
)
from archivist.services.internet_archive import _items
from tests.services.internet_archive.test_client_edge_cases import (
    AsyncSession,
    StubResponse,
    SyncSession,
    as_async_session,
    as_sync_session,
)
from tests.services.internet_archive.test_item_recovery import Client, resolve

if TYPE_CHECKING:
    from tests.conftest import ServerState

IDENTIFIER = "limit-item"
S3_ERROR = (
    "<Error><Code>SlowDown</Code><Message>Try later</Message>"
    "<RequestId>xml-id</RequestId></Error>"
)
# Integer detail fields from a live check_limit reply observed on 2026-11-09.
LIVE_COUNTERS = {
    "accesskey_ration": 499,
    "accesskey_tasks_queued": 0,
    "bucket_limit": 999,
    "bucket_ration": 149,
    "bucket_tasks_queued": 0,
    "rationing_engaged": 1,
    "rationing_level": 9999,
    "total_global_limit": 11999,
    "total_tasks_queued": 10202,
}


@pytest_asyncio.fixture(
    params=[InternetArchiveClient, AsyncInternetArchiveClient], ids=["sync", "async"]
)
async def client(request: pytest.FixtureRequest) -> AsyncIterator[Client]:
    """Provide both real transports with a configured S3 key pair."""
    instance = request.param(api_key=InternetArchiveApiKey("access", "secret"))
    try:
        yield instance
    finally:
        await resolve(instance.close())


def respond(state: ServerState, status: int, body: object) -> None:
    """Override the fixture check_limit reply with a raw or JSON-encoded body."""
    text = body if isinstance(body, str) else json.dumps(body)
    state.item_responses["GET", "/ia/s3"] = (
        status,
        text,
        {"X-Concurrency": "1", "X-Concurrency-Limit": "100"},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("over_limit", "expected"), [(0, False), (1, True)], ids=["under", "over"]
)
async def test_reading_reports_over_limit_counters_and_reason(
    ia_endpoints: ServerState,
    client: Client,
    over_limit: int,
    expected: bool,
) -> None:
    """Copy every integer detail field and send only the documented query."""
    detail = {**LIVE_COUNTERS, "limit_reason": "bucket <b>queue</b> full"}
    respond(
        ia_endpoints,
        200,
        {
            "accesskey": "access",
            "bucket": IDENTIFIER,
            "over_limit": over_limit,
            "detail": detail,
        },
    )
    result = await resolve(client.upload_limit(IDENTIFIER))
    assert result == InternetArchiveUploadLimit(
        over_limit=expected,
        limit_reason="bucket queue full",
        counters=LIVE_COUNTERS,
    )
    (request,) = ia_endpoints.requests
    assert (request.method, request.path) == ("GET", "/ia/s3")
    assert request.query == {
        "check_limit": ["1"],
        "accesskey": ["access"],
        "bucket": [IDENTIFIER],
    }
    headers = {key.lower() for key in request.headers}
    assert "authorization" not in headers
    assert "cookie" not in headers
    assert request.headers["Accept"] == "application/json"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        {"over_limit": 0},
        {"over_limit": 0, "detail": None},
        {"over_limit": 0, "detail": "internal"},
        {"over_limit": 0, "detail": {}},
    ],
    ids=["missing", "null", "string", "empty"],
)
async def test_reading_without_detail_has_no_counters(
    ia_endpoints: ServerState, client: Client, body: dict[str, object]
) -> None:
    """Treat an absent or non-object detail as carrying no counters or reason."""
    respond(ia_endpoints, 200, body)
    result = await resolve(client.upload_limit(IDENTIFIER))
    assert result.over_limit is False
    assert result.limit_reason is None
    assert dict(result.counters) == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value",
    ["1", True, None, 1.0, [1], "missing"],
    ids=["string", "bool", "null", "float", "list", "missing"],
)
async def test_non_integer_over_limit_is_unknown(
    ia_endpoints: ServerState, client: Client, value: object
) -> None:
    """Leave the deferral decision to the caller when over_limit is unreadable."""
    body: dict[str, object] = {"detail": {"total_tasks_queued": 5}}
    if value != "missing":
        body["over_limit"] = value
    respond(ia_endpoints, 200, body)
    result = await resolve(client.upload_limit(IDENTIFIER))
    assert result.over_limit is None
    assert dict(result.counters) == {"total_tasks_queued": 5}


def test_parser_skips_non_integer_counters_and_sanitizes_reason() -> None:
    """Keep only integer counters and redact or bound reason text."""
    result = _items.parse_upload_limit(
        {
            "over_limit": 2,
            "detail": {
                "flag": True,
                "ratio": 0.5,
                "label": "x",
                "nested": {"a": 1},
                "count": -3,
                "limit_reason": "see https://example.invalid/x token=abc\x00 "
                + "z" * 400,
            },
        }
    )
    assert result.over_limit is True
    assert dict(result.counters) == {"count": -3}
    assert result.limit_reason is not None
    assert "example.invalid" not in result.limit_reason
    assert "abc" not in result.limit_reason
    assert "\x00" not in result.limit_reason
    assert len(result.limit_reason) <= 256
    assert _items.parse_upload_limit({"detail": {"limit_reason": "  "}}) == (
        InternetArchiveUploadLimit()
    )


def test_model_counters_are_read_only_copies() -> None:
    """Freeze counters so a caller cannot mutate a shared reading."""
    source = {"total_tasks_queued": 1}
    result = InternetArchiveUploadLimit(over_limit=False, counters=source)
    source["total_tasks_queued"] = 2
    assert result.counters["total_tasks_queued"] == 1
    with pytest.raises(TypeError):
        cast("Any", result.counters)["total_tasks_queued"] = 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body", ["not json", "[1, 2]"], ids=["invalid-json", "not-an-object"]
)
async def test_unreadable_body_is_a_classified_failure(
    ia_endpoints: ServerState, client: Client, body: str
) -> None:
    """Raise a service-response error attributed to the upload-limit request."""
    respond(ia_endpoints, 200, body)
    with pytest.raises(InvalidServiceResponseError) as raised:
        await resolve(client.upload_limit(IDENTIFIER))
    assert raised.value.request_phase == "upload-limit"
    assert raised.value.status_code == 200


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "error_type"),
    [(503, InvalidServiceResponseError), (429, RateLimitError), (302, None)],
)
async def test_http_failures_are_classified_with_diagnostics(
    ia_endpoints: ServerState,
    client: Client,
    status: int,
    error_type: type[ServiceError] | None,
) -> None:
    """Reuse item-status classification, including rejecting redirects."""
    ia_endpoints.item_responses["GET", "/ia/s3"] = (
        status,
        S3_ERROR,
        {"Retry-After": "7", "Location": "https://example.invalid/"},
    )
    with pytest.raises(error_type or InvalidServiceResponseError) as raised:
        await resolve(client.upload_limit(IDENTIFIER))
    error = raised.value
    assert error.request_phase == "upload-limit"
    assert error.status_code == status
    assert error.service_code == "SlowDown"
    assert error.request_id == "xml-id"
    assert error.retry_after == 7.0
    assert len(ia_endpoints.requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_network_failure_is_attributed_without_status(
    asynchronous: bool,
) -> None:
    """Translate transport failures without echoing exception text."""
    responses: list[StubResponse | BaseException] = [OSError("private secret")]
    if asynchronous:
        instance: Client = AsyncInternetArchiveClient(
            session=as_async_session(AsyncSession(responses)),
            api_key=InternetArchiveApiKey("access", "secret"),
        )
    else:
        instance = InternetArchiveClient(
            session=as_sync_session(SyncSession(responses)),
            api_key=InternetArchiveApiKey("access", "secret"),
        )
    with pytest.raises(NetworkError) as raised:
        await resolve(instance.upload_limit(IDENTIFIER))
    assert raised.value.request_phase == "upload-limit"
    assert raised.value.status_code is None
    assert "private secret" not in str(raised.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(("client_timeout", "expected"), [(30.0, 15.0), (5.0, 5.0)])
async def test_injected_transport_gets_isolated_bounded_request(
    asynchronous: bool,
    client_timeout: float,
    expected: float,
) -> None:
    """Send no credentials beyond the query key and cap the request timeout."""
    response = StubResponse({"over_limit": 1, "detail": {"bucket_ration": 3}})
    session: SyncSession | AsyncSession
    if asynchronous:
        session = AsyncSession([response])
        instance: Client = AsyncInternetArchiveClient(
            session=as_async_session(session),
            api_key=InternetArchiveApiKey("access", "secret"),
            timeout=client_timeout,
        )
    else:
        session = SyncSession([response])
        instance = InternetArchiveClient(
            session=as_sync_session(session),
            api_key=InternetArchiveApiKey("access", "secret"),
            timeout=client_timeout,
        )
    result = await resolve(instance.upload_limit(IDENTIFIER))
    assert result.over_limit is True
    assert dict(result.counters) == {"bucket_ration": 3}
    ((method, url, kwargs),) = session.requests
    assert (method, url) == ("GET", _items.S3_URL)
    assert kwargs["headers"] == {"Accept": "application/json"}
    assert kwargs["params"] == {
        "check_limit": "1",
        "accesskey": "access",
        "bucket": IDENTIFIER,
    }
    assert kwargs["cookies"] == {}
    assert kwargs["auth"] is None
    assert kwargs["allow_redirects"] is False
    assert kwargs["timeout"] == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "client_type", [InternetArchiveClient, AsyncInternetArchiveClient]
)
async def test_account_key_is_resolved_once_and_reused(
    ia_endpoints: ServerState, client_type: type[Client]
) -> None:
    """Resolve the uploader key once, as uploads do, and reuse it afterward."""
    instance = client_type(account=InternetArchiveAccount("user", "password"))
    try:
        first = await resolve(instance.upload_limit(IDENTIFIER))
        second = await resolve(instance.upload_limit("other-item"))
    finally:
        await resolve(instance.close())
    assert first.over_limit is False
    assert second.over_limit is False
    assert len(ia_endpoints.matching("/ia/upload", "GET")) == 1
    checks = ia_endpoints.matching("/ia/s3", "GET")
    assert [request.query["accesskey"] for request in checks] == [
        ["dummy-access"],
        ["dummy-access"],
    ]
    assert [request.query["bucket"] for request in checks] == [
        [IDENTIFIER],
        ["other-item"],
    ]
    assert all("authorization" not in map(str.lower, r.headers) for r in checks)


@pytest.mark.asyncio
@pytest.mark.parametrize("identifier", ["", "-bad", "a/b", "a b", None])
async def test_invalid_identifier_is_rejected_before_any_request(
    ia_endpoints: ServerState, client: Client, identifier: object
) -> None:
    """Validate identifiers like the other item methods, without network access."""
    with pytest.raises(InvalidOptionError):
        await resolve(client.upload_limit(cast("Any", identifier)))
    assert ia_endpoints.requests == []


@pytest.mark.asyncio
async def test_closed_client_is_rejected(client: Client) -> None:
    """Refuse to issue a check from a closed client."""
    await resolve(client.close())
    with pytest.raises(RuntimeError, match="closed"):
        await resolve(client.upload_limit(IDENTIFIER))
