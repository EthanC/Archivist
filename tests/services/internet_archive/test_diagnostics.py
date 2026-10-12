"""Exercise response diagnostics through real sync and async item transports."""

# ruff: noqa: PLR2004 - Assertions describe exact protocol statuses and request counts.

from __future__ import annotations

import asyncio
import pickle
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from io import BytesIO
from typing import TYPE_CHECKING, cast

import pytest
import pytest_asyncio

from archivist import (
    AsyncInternetArchiveClient,
    AuthenticationError,
    InternetArchiveAccount,
    InternetArchiveApiKey,
    InternetArchiveClient,
    InternetArchiveCookies,
    InternetArchiveRecoveryError,
    InternetArchiveUploadError,
    InternetArchiveUploadFile,
    InternetArchiveUploadOptions,
    InvalidServiceResponseError,
    NetworkError,
    RateLimitError,
    ServiceError,
)
from archivist.services.internet_archive import _diagnostics
from tests.core.test_http import awaiting
from tests.services.internet_archive.test_client_edge_cases import (
    AsyncSession,
    StubResponse,
    SyncSession,
    as_async_session,
    as_sync_session,
)
from tests.services.internet_archive.test_item_recovery import (
    EXPECTED,
    IDENTIFIER,
    Client,
    existing_item,
    resolve,
)

if TYPE_CHECKING:
    from tests.conftest import ServerState

S3_ERROR = (
    "<Error><Code>SlowDown</Code><Message>Try later</Message>"
    "<RequestId>xml-id</RequestId></Error>"
)


def files() -> list[InternetArchiveUploadFile]:
    """Provide two borrowed sources so partial outcomes can be checked."""
    return [
        InternetArchiveUploadFile(BytesIO(b"first"), "first.bin"),
        InternetArchiveUploadFile(BytesIO(b"second"), "second.bin"),
    ]


def options() -> InternetArchiveUploadOptions:
    """Return minimal metadata for one test item."""
    return InternetArchiveUploadOptions(
        "diagnostic-item", "Title", "Description", ["test"]
    )


@pytest_asyncio.fixture(
    params=[InternetArchiveClient, AsyncInternetArchiveClient], ids=["sync", "async"]
)
async def client(request: pytest.FixtureRequest) -> AsyncIterator[Client]:
    """Run identical protocol tests through both real transports."""
    instance = request.param(api_key=InternetArchiveApiKey("access", "secret"))
    instance._account = InternetArchiveAccount("user", "password")
    try:
        yield instance
    finally:
        await resolve(instance.close())


@pytest.mark.asyncio
@pytest.mark.parametrize("code", ["SlowDown", "ServiceUnavailable", "FutureCode"])
@pytest.mark.parametrize("status", [200, 429, 503])
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("15", 15.0),
        ("Wed, 21 Oct 2015 07:28:00 GMT", datetime(2015, 10, 21, 7, 28, tzinfo=UTC)),
    ],
)
async def test_create_failure_preserves_s3_diagnostics_without_retry(  # noqa: PLR0913,PLR0917 - Fixture response matrix.
    ia_endpoints: ServerState,
    client: Client,
    code: str,
    status: int,
    raw: str,
    expected: float | datetime,
) -> None:
    """Retain retry hints on 503 as well as 429 and issue exactly one create PUT."""
    ia_endpoints.item_responses["PUT", "/ia/s3/diagnostic-item"] = (
        status,
        S3_ERROR.replace("SlowDown", code),
        {"Retry-After": raw, "X-Amz-Request-Id": "header-id", "Set-Cookie": "secret"},
    )
    with pytest.raises(InternetArchiveUploadError) as raised:
        await resolve(client.upload(files(), options()))
    error = raised.value
    cause = error.cause
    assert cause.status_code == status
    assert cause.service_code == code
    assert cause.service_message == "Try later"
    assert cause.request_id == "xml-id"
    assert cause.request_phase == "item-create"
    assert cause.retry_after == expected
    assert cause.retry_after_raw == raw
    assert isinstance(
        cause, RateLimitError if status == 429 else InvalidServiceResponseError
    )
    assert not any(file.transferred for file in error.result.files)
    assert error.failed_file is None
    assert [(request.method, request.path) for request in ia_endpoints.requests] == [
        ("POST", "/ia/upload-api"),
        ("PUT", "/ia/s3/diagnostic-item"),
    ]
    assert "secret" not in repr(cause.__dict__)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [200, 403, 503])
async def test_partial_upload_retains_outcomes_and_enriched_serializable_cause(
    ia_endpoints: ServerState,
    client: Client,
    status: int,
) -> None:
    """Unexpected acknowledgement bodies never count as successful file transfers."""
    ia_endpoints.item_responses["PUT", "/ia/s3/diagnostic-item/second.bin"] = (
        status,
        S3_ERROR,
        {},
    )
    with pytest.raises(InternetArchiveUploadError) as raised:
        await resolve(client.upload(files(), options()))
    restored = pickle.loads(pickle.dumps(raised.value))
    assert restored.failed_file == "second.bin"
    assert [file.transferred for file in restored.result.files] == [True, False]
    assert restored.cause.request_phase == "file-upload"
    assert restored.cause.service_code == "SlowDown"
    assert restored.cause.status_code == status
    assert isinstance(
        restored.cause,
        AuthenticationError if status == 403 else InvalidServiceResponseError,
    )
    assert len(ia_endpoints.requests) == 4
    assert [request.method for request in ia_endpoints.requests] == [
        "POST",
        "PUT",
        "PUT",
        "PUT",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "phase", "status", "body", "error_type"),
    [
        ("/ia/csrf", "csrf", 403, S3_ERROR, InvalidServiceResponseError),
        ("/ia/csrf", "csrf", 200, '{"success":false}', InvalidServiceResponseError),
        (
            "/ia/csrf",
            "csrf",
            200,
            '{"success":true,"value":{}}',
            InvalidServiceResponseError,
        ),
        ("/ia/csrf", "csrf", 200, "not JSON", InvalidServiceResponseError),
        (
            "/ia/login",
            "login",
            400,
            "<html>Bad request password=secret</html>",
            InvalidServiceResponseError,
        ),
        (
            "/ia/login",
            "login",
            400,
            '{"success":false,"error":"Rejected"}',
            AuthenticationError,
        ),
        (
            "/ia/login",
            "login",
            200,
            '{"success":false,"error":"Rejected"}',
            AuthenticationError,
        ),
        ("/ia/login", "login", 200, "{}", InvalidServiceResponseError),
        ("/ia/login", "login", 200, "[false]", InvalidServiceResponseError),
        ("/ia/login", "login", 200, "not JSON", InvalidServiceResponseError),
        ("/ia/login", "login", 401, S3_ERROR, AuthenticationError),
    ],
)
async def test_login_classification_and_phase_are_response_specific(  # noqa: PLR0913,PLR0917 - Fixture response matrix.
    ia_endpoints: ServerState,
    client: Client,
    path: str,
    phase: str,
    status: int,
    body: str,
    error_type: type[ServiceError],
) -> None:
    """Distinguish credential rejection from public-token and parsing failures."""
    method = "GET" if phase == "csrf" else "POST"
    ia_endpoints.item_responses[method, path] = (
        status,
        body,
        {"X-Amz-Request-Id": "login-id"},
    )
    with pytest.raises(error_type) as raised:
        await resolve(client.login())
    error = raised.value
    assert type(error) is error_type
    assert error.request_phase == phase
    assert error.status_code == status
    assert error.request_id in {"xml-id", "login-id"}
    assert not client._account_authenticated
    assert "secret" not in str(error)
    assert "secret" not in repr(error.__dict__)
    assert len(ia_endpoints.requests) == (1 if phase == "csrf" else 2)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "phase"),
    [("/ia/upload", "upload-key"), ("/ia/upload-api", "identifier-check")],
)
async def test_upload_preflight_failures_have_diagnostics(
    ia_endpoints: ServerState,
    client: Client,
    path: str,
    phase: str,
) -> None:
    """Expose preflight diagnostics without sending any mutation PUT."""
    client._api_key = None
    client._cookies = InternetArchiveCookies("user", "sig")
    method = "GET" if phase == "upload-key" else "POST"
    ia_endpoints.item_responses[method, path] = (200, S3_ERROR, {})
    with pytest.raises(InvalidServiceResponseError) as raised:
        await resolve(client.upload(files(), options()))
    assert raised.value.request_phase == phase
    assert raised.value.service_code == "SlowDown"
    assert raised.value.status_code == 200
    assert not any(request.method == "PUT" for request in ia_endpoints.requests)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "phase", "method"),
    [
        ("/ia/user", "identity", "GET"),
        (f"/ia/metadata/{IDENTIFIER}", "item-metadata", "GET"),
        ("/ia/upload-api", "request", "GET"),
        (f"/ia/s3/{IDENTIFIER}/second.bin", "file-upload", "PUT"),
    ],
)
@pytest.mark.parametrize("status", [200, 503])
async def test_recovery_failures_preserve_diagnostics_and_partial_results(  # noqa: PLR0913,PLR0917 - Fixture response matrix.
    ia_endpoints: ServerState,
    client: Client,
    path: str,
    phase: str,
    method: str,
    status: int,
) -> None:
    """Attribute recovery failures to their request and retain acknowledged files."""
    existing_item(ia_endpoints)
    ia_endpoints.item_responses[method, path] = (
        status,
        S3_ERROR,
        {"Retry-After": "12"},
    )
    with pytest.raises(InternetArchiveRecoveryError) as raised:
        await resolve(client.add_files(IDENTIFIER, files(), expected_metadata=EXPECTED))
    error = pickle.loads(pickle.dumps(raised.value))
    assert error.cause.request_phase == phase
    assert error.cause.status_code == status
    assert error.cause.service_code == "SlowDown"
    assert error.cause.retry_after == 12.0
    if method == "PUT":
        assert error.failed_file == "second.bin"
        assert [file.transferred for file in error.result.files] == [True, False]
        assert len(ia_endpoints.matching(path, "PUT")) == 1
    else:
        assert not any(file.transferred for file in error.result.files)
        assert not any(request.method == "PUT" for request in ia_endpoints.requests)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        "",
        "plain secret",
        "<html>password=secret</html>",
        "<Error><Code>SlowDown",
        '{"error":',
        pytest.param("x" * 17000, id="oversized"),
        '<!DOCTYPE Error [<!ENTITY x "secret">]><Error><Message>&x;</Message></Error>',
        '<Error xmlns="urn:s3"><Code>SlowDown</Code><Message>password=secret '
        "https://example.invalid/private?signature=secret</Message></Error>",
    ],
)
async def test_untrusted_diagnostics_never_hide_the_http_failure(
    ia_endpoints: ServerState,
    client: Client,
    body: str,
) -> None:
    """Keep failure status and support headers even when the body cannot be used."""
    ia_endpoints.item_responses["PUT", "/ia/s3/diagnostic-item"] = (
        503,
        body,
        {"Retry-After": "invalid", "X-Amz-Request-Id": "support-id"},
    )
    with pytest.raises(InternetArchiveUploadError) as raised:
        await resolve(client.upload(files(), options()))
    error = raised.value.cause
    assert error.status_code == 503
    assert error.request_phase == "item-create"
    assert error.request_id == "support-id"
    assert error.retry_after is None
    assert error.retry_after_raw == "invalid"
    assert "secret" not in repr(error.__dict__)
    assert "example.invalid" not in repr(error.__dict__)
    assert len(ia_endpoints.requests) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_network_failure_after_success_has_no_stale_status(
    asynchronous: bool,
) -> None:
    """Clear the preceding create/file response before the next network attempt."""
    responses: list[StubResponse | BaseException] = [
        StubResponse({"success": True, "identifier": "diagnostic-item"}),
        StubResponse({}, headers={"X-Amz-Request-Id": "stale-create"}),
        StubResponse({}, headers={"X-Amz-Request-Id": "stale-file"}),
        OSError("private secret"),
    ]
    if asynchronous:
        session = AsyncSession(responses)
        instance: Client = AsyncInternetArchiveClient(
            session=as_async_session(session),
            api_key=InternetArchiveApiKey("access", "secret"),
        )
    else:
        sync_session = SyncSession(responses)
        instance = InternetArchiveClient(
            session=as_sync_session(sync_session),
            api_key=InternetArchiveApiKey("access", "secret"),
        )
    with pytest.raises(InternetArchiveUploadError) as raised:
        await resolve(instance.upload(files(), options()))
    error = raised.value
    assert isinstance(error.cause, NetworkError)
    assert error.cause.request_phase == "file-upload"
    assert error.cause.status_code is None
    assert error.cause.request_id is None
    assert error.cause.diagnostic_headers == {}
    assert error.failed_file == "second.bin"
    assert [file.transferred for file in error.result.files] == [True, False]


class LazyResponse:
    """Model the niquests representation with awaitable text and JSON access."""

    def __init__(self, payload: object, text: str, status: int) -> None:
        """Retain only dummy data for asynchronous adapter testing."""
        self.payload = payload
        self.body = text
        self.status_code = status
        self.headers = {"Retry-After": "3"}

    @property
    def text(self) -> object:
        """Return a fresh awaitable on each access like a lazy response."""
        return awaiting(self.body)

    def json(self) -> object:
        """Return decoded JSON through awaitable access."""
        return awaiting(self.payload)


@pytest.mark.asyncio
async def test_async_login_and_create_use_awaitable_response_access() -> None:
    """Await both login JSON and the failed S3 response's text."""
    session = AsyncSession(
        cast(
            "list[StubResponse | BaseException]",
            [
                LazyResponse({"success": True, "value": {"token": "token"}}, "", 200),
                LazyResponse({"success": True}, "", 200),
                LazyResponse(
                    {"success": True, "identifier": "diagnostic-item"}, "", 200
                ),
                LazyResponse({}, S3_ERROR, 503),
            ],
        )
    )
    instance = AsyncInternetArchiveClient(
        session=as_async_session(session),
        account=InternetArchiveAccount("user", "password"),
        api_key=InternetArchiveApiKey("access", "secret"),
    )
    await instance.login()
    with pytest.raises(InternetArchiveUploadError) as raised:
        await instance.upload(files(), options())
    assert raised.value.cause.service_code == "SlowDown"
    assert raised.value.cause.retry_after == 3.0
    assert raised.value.cause.request_phase == "item-create"


@pytest.mark.asyncio
async def test_nested_login_failure_does_not_acquire_outer_upload_phase(
    ia_endpoints: ServerState,
    client: Client,
) -> None:
    """Keep nested authentication failures attributed to login rather than upload."""
    client._api_key = None
    ia_endpoints.item_responses["POST", "/ia/login"] = (400, '{"success":false}', {})
    with pytest.raises(AuthenticationError) as raised:
        await resolve(client.upload(files(), options()))
    assert raised.value.request_phase == "login"
    assert raised.value.status_code == 400


@pytest.mark.asyncio
async def test_async_diagnostic_context_is_task_local_and_cleared() -> None:
    """Concurrent failed operations retain separate request IDs and release scopes."""

    class RoutingSession(AsyncSession):
        async def request(
            self, method: str, url: str, **kwargs: object
        ) -> StubResponse:
            await asyncio.sleep(0)
            return StubResponse(
                {},
                status_code=503,
                text=S3_ERROR.replace("xml-id", url.rsplit("/", 1)[-1]),
            )

    instance = AsyncInternetArchiveClient(session=as_async_session(RoutingSession()))
    errors = await asyncio.gather(
        instance.get_item("one"), instance.get_item("two"), return_exceptions=True
    )
    assert [cast("ServiceError", error).request_id for error in errors] == [
        "one",
        "two",
    ]
    assert _diagnostics._CONTEXT.get() is None
    _diagnostics.received_response(object())


@pytest.mark.asyncio
async def test_get_item_http_failure_is_enriched(
    ia_endpoints: ServerState,
    client: Client,
) -> None:
    """Expose metadata service failures separately from valid unavailable snapshots."""
    ia_endpoints.item_responses["GET", "/ia/metadata/item"] = (503, S3_ERROR, {})
    with pytest.raises(InvalidServiceResponseError) as raised:
        await resolve(client.get_item("item"))
    assert raised.value.request_phase == "item-metadata"
    assert raised.value.request_id == "xml-id"
