"""Cover item validation, streaming ownership, cancellation, and polling edges."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from io import BufferedReader, BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import niquests
import pytest
from niquests.adapters import AsyncHTTPAdapter, HTTPAdapter

from archivist import (
    AsyncInternetArchiveClient,
    AuthenticationError,
    InternetArchiveAccount,
    InternetArchiveApiKey,
    InternetArchiveClient,
    InternetArchiveCookies,
    InvalidOptionError,
    InvalidServiceResponseError,
    NetworkError,
    PollingTimeoutError,
    ServiceError,
)
from archivist.services.internet_archive import _common, _items
from archivist.services.internet_archive import async_client as async_module
from archivist.services.internet_archive import client as sync_module
from archivist.services.internet_archive.item_models import (
    InternetArchiveUploadError,
    InternetArchiveUploadFile,
    InternetArchiveUploadOptions,
)
from tests.services.internet_archive.test_client_edge_cases import (
    AsyncSession,
    StubResponse,
    SyncSession,
    as_async_session,
    as_sync_session,
)
from tests.services.internet_archive.test_item_clients import (
    PAYLOAD,
    Client,
    options,
    resolve,
)
from tests.services.internet_archive.test_item_clients import (
    item_client as item_client,  # noqa: PLC0414 - Import the parametrized fixture.
)

if TYPE_CHECKING:
    from tests.conftest import ServerState

MAX_READ = 1024 * 1024
POLL_TIMEOUT = 0.03


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("operation", ["upload", "remove"])
@pytest.mark.parametrize(
    "policy", ["count", "retry", "unlimited", "s3", "manage", "login", "upload-api"]
)
async def test_item_operations_reject_injected_retries_before_network(
    ia_endpoints: ServerState,
    asynchronous: bool,
    operation: str,
    policy: str,
) -> None:
    """Reject retry-enabled adapters before login without changing caller policy."""
    retries = niquests.RetryConfiguration(
        total=None if policy == "unlimited" else 1,
        status_forcelist=[503],
        allowed_methods=None,
    )
    configured = retries if policy in {"retry", "unlimited"} else int(policy == "count")
    if asynchronous:
        session = niquests.AsyncSession(retries=configured)
        client = AsyncInternetArchiveClient(
            session=session, account=InternetArchiveAccount("user", "password")
        )
    else:
        session = niquests.Session(retries=configured)
        client = InternetArchiveClient(
            session=session, account=InternetArchiveAccount("user", "password")
        )
    prefixes = {
        "s3": f"{_items.S3_URL}/fixture-item/file",
        "manage": _items.MANAGE_URL,
        "login": _common.LOGIN_URL,
        "upload-api": _items.UPLOAD_API_URL,
    }
    if policy in prefixes:
        if isinstance(session, niquests.AsyncSession):
            session.mount(prefixes[policy], AsyncHTTPAdapter(max_retries=retries))
        else:
            session.mount(prefixes[policy], HTTPAdapter(max_retries=retries))
    original_retries = session.retries
    original_adapters = dict(session.adapters)
    original_policies = [
        cast("HTTPAdapter | AsyncHTTPAdapter", adapter).max_retries
        for adapter in session.adapters.values()
    ]
    stream = BytesIO(b"payload")
    try:
        with pytest.raises(InvalidOptionError, match="disable retries"):
            await resolve(
                client.upload([InternetArchiveUploadFile(stream, "file")], options())
                if operation == "upload"
                else client.remove_items(["fixture-item"])
            )
        assert not ia_endpoints.requests
        assert session.retries is original_retries
        assert session.adapters == original_adapters
        assert all(
            cast("HTTPAdapter | AsyncHTTPAdapter", adapter).max_retries is original
            for adapter, original in zip(
                session.adapters.values(), original_policies, strict=True
            )
        )
        assert stream.tell() == 0
        assert not stream.closed
    finally:
        await resolve(client.close())
        await resolve(session.close())


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("operation", ["upload", "remove"])
@pytest.mark.parametrize("policy", [0, False, "total-zero", "total-false"])
async def test_item_operations_accept_injected_disabled_retries(
    ia_endpoints: ServerState,
    asynchronous: bool,
    operation: str,
    policy: int | bool | str,
) -> None:
    """Allow zero/False retry budgets and never replay a failed mutation."""
    retries = (
        niquests.RetryConfiguration(
            total=0 if policy == "total-zero" else False,
            connect=5,
            status=5,
            status_forcelist=[503],
            allowed_methods=None,
        )
        if isinstance(policy, str)
        else policy
    )
    if asynchronous:
        session = niquests.AsyncSession(retries=retries)
        client = AsyncInternetArchiveClient(
            session=session,
            api_key=InternetArchiveApiKey("access", "secret"),
            cookies=InternetArchiveCookies("user", "signature"),
        )
    else:
        session = niquests.Session(retries=retries)
        client = InternetArchiveClient(
            session=session,
            api_key=InternetArchiveApiKey("access", "secret"),
            cookies=InternetArchiveCookies("user", "signature"),
        )
    method, path = (
        ("PUT", "/ia/s3/fixture-item")
        if operation == "upload"
        else ("POST", "/ia/manage/")
    )
    ia_endpoints.item_responses[method, path] = (503, "failure", {})
    original_retries = session.retries
    try:
        with pytest.raises(ServiceError):
            await resolve(
                client.upload(
                    [InternetArchiveUploadFile(BytesIO(b"a"), "a")], options()
                )
                if operation == "upload"
                else client.remove_items(["fixture-item"])
            )
        assert len(ia_endpoints.matching(path, method)) == 1
        assert session.retries is original_retries
    finally:
        await resolve(client.close())
        await resolve(session.close())


class BoundedStream(BytesIO):
    """Reject eager reads while recording niquests' actual transport read sizes."""

    def __init__(self, data: bytes) -> None:
        """Initialize a stream whose payload exceeds the permitted read size."""
        super().__init__(data)
        self.read_sizes: list[int] = []

    def read(self, size: int | None = -1) -> bytes:
        """Fail if preparation or transport attempts to buffer the complete file."""
        assert size is not None and 0 <= size <= MAX_READ
        self.read_sizes.append(size)
        return super().read(size)


class BoundedFile(BufferedReader):
    """Observe bounded reads from an actual disk file, not a preloaded buffer."""

    def read(self, size: int | None = -1) -> bytes:
        """Reject unbounded reads performed by either preparation or transport."""
        assert size is not None and 0 <= size <= MAX_READ
        return super().read(size)


@pytest.mark.asyncio
async def test_real_transport_streams_paths_and_caller_handles_in_bounded_reads(
    ia_endpoints: ServerState,
    item_client: Client,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Check bounded niquests reads and library versus caller stream ownership."""
    data = PAYLOAD * 12
    path = tmp_path / "owned.bin"
    path.write_bytes(data)
    owned = BoundedFile(path.open("rb", buffering=0))
    caller = BoundedStream(b"skip" + data)
    caller.seek(4)
    monkeypatch.setattr(Path, "open", lambda *args, **kwargs: owned)
    result = await resolve(
        item_client.upload(
            [path, InternetArchiveUploadFile(caller, "caller.bin")],
            options(),
        )
    )
    assert all(file.transferred for file in result.files)
    assert owned.closed
    assert not caller.closed
    assert caller.read_sizes[0] == 1
    assert len(caller.read_sizes) > 1
    assert ia_endpoints.matching("/ia/s3/fixture-item/owned.bin")[0].body == data
    assert ia_endpoints.matching("/ia/s3/fixture-item/caller.bin")[0].body == data


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid", ["empty", "duplicate", "generated", "missing", "generator"]
)
async def test_entire_batch_preflight_closes_owned_files_without_auth_or_network(
    ia_endpoints: ServerState,
    item_client: Client,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    invalid: str,
) -> None:
    """Validate later files before authentication and restore caller stream offsets."""
    owned = BytesIO(b"owned")
    caller = BytesIO(b"prefix-data")
    caller.seek(7)
    item_client._api_key = None
    path = tmp_path / "owned.bin"

    def open_path(value: Path, *args: object, **kwargs: object) -> BytesIO:
        if value == path:
            return owned
        raise FileNotFoundError

    monkeypatch.setattr(Path, "open", open_path)
    files: list[InternetArchiveUploadFile | Path] = [
        path,
        InternetArchiveUploadFile(caller, "caller"),
    ]
    if invalid == "empty":
        files.append(InternetArchiveUploadFile(BytesIO(), "empty"))
    elif invalid == "duplicate":
        files.append(InternetArchiveUploadFile(BytesIO(b"x"), "owned.bin"))
    elif invalid == "generated":
        files.append(InternetArchiveUploadFile(BytesIO(b"x"), "fixture-item_meta.xml"))
    elif invalid == "missing":
        files.append(tmp_path / "missing")

    def sources() -> Iterator[InternetArchiveUploadFile | Path]:
        yield from files
        if invalid == "generator":
            raise InvalidOptionError("failed to enumerate inputs")

    with pytest.raises(InvalidOptionError):
        await resolve(item_client.upload(sources(), options()))
    assert owned.closed
    assert not caller.closed
    assert caller.tell() == len(b"prefix-")
    assert not ia_endpoints.requests


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["options", "wait", "timeout", "interval"])
async def test_upload_options_are_validated_before_network(
    ia_endpoints: ServerState,
    item_client: Client,
    invalid: str,
) -> None:
    """Reject wrong public argument types and invalid durations locally."""
    with pytest.raises(InvalidOptionError):
        await resolve(
            item_client.upload(
                [InternetArchiveUploadFile(BytesIO(b"x"), "a")],
                cast("InternetArchiveUploadOptions", None)
                if invalid == "options"
                else options(),
                wait=cast("bool", 1) if invalid == "wait" else False,
                timeout=0 if invalid == "timeout" else 300,
                poll_interval=0 if invalid == "interval" else 2,
            )
        )
    assert not ia_endpoints.requests


@pytest.mark.asyncio
async def test_upload_requires_auth_and_closed_clients_cannot_mutate(
    ia_endpoints: ServerState,
    item_client: Client,
) -> None:
    """Fail unauthenticated uploads and both operations on closed clients."""
    item_client._api_key = None
    files = [InternetArchiveUploadFile(BytesIO(b"a"), "a")]
    with pytest.raises(AuthenticationError):
        await resolve(item_client.upload(files, options()))
    await resolve(item_client.close())
    with pytest.raises(RuntimeError, match="closed"):
        await resolve(item_client.upload(files, options()))
    with pytest.raises(RuntimeError, match="closed"):
        await resolve(item_client.remove_items(["fixture-item"]))
    assert not ia_endpoints.requests


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stage", ["bootstrap", "availability", "creation", "transfer", "poll"]
)
async def test_owned_handles_close_after_service_failures(
    ia_endpoints: ServerState,
    item_client: Client,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    """Close library handles on every failing phase without closing caller streams."""
    owned = BytesIO(PAYLOAD)
    caller = BytesIO(PAYLOAD)
    monkeypatch.setattr(Path, "open", lambda *args, **kwargs: owned)
    if stage == "bootstrap":
        item_client._api_key = None
        item_client._cookies = InternetArchiveCookies("user", "signature")
    method, path = {
        "bootstrap": ("GET", "/ia/upload"),
        "availability": ("POST", "/ia/upload-api"),
        "creation": ("PUT", "/ia/s3/fixture-item"),
        "transfer": ("PUT", "/ia/s3/fixture-item/a"),
        "poll": ("GET", "/ia/upload-api"),
    }[stage]
    ia_endpoints.item_responses[method, path] = (503, "failure", {})
    with pytest.raises(ServiceError):
        await resolve(
            item_client.upload(
                [Path("a"), InternetArchiveUploadFile(caller, "b")],
                options(),
                wait=True,
            )
        )
    assert owned.closed
    assert not caller.closed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,body", [(201, ""), (200, "<Error>failure</Error>"), (204, "")]
)
async def test_creation_rejects_ambiguous_success(
    ia_endpoints: ServerState,
    item_client: Client,
    status: int,
    body: str,
) -> None:
    """Do not upload files unless exclusive bucket creation is acknowledged."""
    ia_endpoints.item_responses["PUT", "/ia/s3/fixture-item"] = (status, body, {})
    with pytest.raises(InternetArchiveUploadError) as failure:
        await resolve(
            item_client.upload(
                [InternetArchiveUploadFile(BytesIO(b"a"), "a")], options()
            )
        )
    assert isinstance(failure.value.cause, InvalidServiceResponseError)
    assert failure.value.failed_file is None
    assert not ia_endpoints.matching("/ia/s3/fixture-item/a")


@pytest.mark.asyncio
async def test_file_put_rejects_error_body_with_success_status(
    ia_endpoints: ServerState,
    item_client: Client,
) -> None:
    """Do not report transfer success for an XML error returned with HTTP 200."""
    ia_endpoints.item_responses["PUT", "/ia/s3/fixture-item/a"] = (
        200,
        "<Error><Code>InternalError</Code></Error>",
        {},
    )
    with pytest.raises(InternetArchiveUploadError) as failure:
        await resolve(
            item_client.upload(
                [InternetArchiveUploadFile(BytesIO(b"a"), "a")], options()
            )
        )
    assert isinstance(failure.value.cause, InvalidServiceResponseError)
    assert failure.value.failed_file == "a"
    assert not failure.value.result.files[0].transferred


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        "{}",
        '<input type="password">',
        '<input type="hidden" class="js-uploader-args" value="bad">',
    ],
)
async def test_bootstrap_parser_failure_does_not_leak_secrets_or_mutate(
    ia_endpoints: ServerState,
    item_client: Client,
    body: str,
) -> None:
    """Reject malformed uploader credentials and explicit logged-out HTML."""
    item_client._api_key = None
    item_client._cookies = InternetArchiveCookies("user", "signature")
    ia_endpoints.item_responses["GET", "/ia/upload"] = (200, body, {})
    with pytest.raises((AuthenticationError, InvalidServiceResponseError)) as failure:
        await resolve(
            item_client.upload(
                [InternetArchiveUploadFile(BytesIO(b"a"), "a")], options()
            )
        )
    assert body not in str(failure.value)
    assert len(ia_endpoints.requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        "{}",
        '{"success":false}',
        '{"success":true,"rows":null}',
        '{"success":true,"rows":[{"cmd":"archive.php","wait_admin":2}]}',
    ],
)
async def test_polling_malformed_or_blocked_catalog_preserves_transfers(
    ia_endpoints: ServerState,
    item_client: Client,
    body: str,
) -> None:
    """Separate successful transfers from catalog parser and admin failures."""
    ia_endpoints.item_responses["GET", "/ia/upload-api"] = (200, body, {})
    with pytest.raises(InternetArchiveUploadError) as failure:
        await resolve(
            item_client.upload(
                [InternetArchiveUploadFile(BytesIO(b"a"), "a")], options(), wait=True
            )
        )
    assert failure.value.result.files[0].transferred
    assert failure.value.failed_file is None
    assert not failure.value.result.processing_complete


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,body",
    [
        (302, "redirect"),
        (429, "slow down"),
        (200, "success"),
        (
            200,
            "<p>Item: 'a' queued for \"make_dark\" operation - task ID: 1</p>"
            "<p>Item: 'a' queued for \"make_dark\" operation - task ID: 2</p>",
        ),
    ],
)
async def test_removal_http_and_parser_failures_are_not_retried(
    ia_endpoints: ServerState,
    item_client: Client,
    status: int,
    body: str,
) -> None:
    """Reject ambiguous or conflicting manage responses without another mutation."""
    item_client._cookies = InternetArchiveCookies("user", "signature")
    ia_endpoints.item_responses["POST", "/ia/manage/"] = (
        status,
        body,
        {"Location": f"{ia_endpoints.base_url}/redirect-target"},
    )
    with pytest.raises(ServiceError):
        await resolve(item_client.remove_items(["a"]))
    assert len(ia_endpoints.requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("identifiers", [[], ["a", "a"], ["a", "../bad"], "a"])
async def test_removal_prevalidates_entire_batch(
    ia_endpoints: ServerState,
    item_client: Client,
    identifiers: list[str] | str,
) -> None:
    """Validate explicit batches before login, even when no account is configured."""
    with pytest.raises(InvalidOptionError):
        await resolve(item_client.remove_items(identifiers))
    assert not ia_endpoints.requests


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["availability", "creation", "transfer", "poll"])
async def test_async_cancellation_propagates_and_closes_only_owned_streams(
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    """Do not wrap task cancellation or leak opened path handles at any phase."""
    owned = BytesIO(b"owned")
    caller = BytesIO(b"caller")
    monkeypatch.setattr(Path, "open", lambda *args, **kwargs: owned)
    responses: list[StubResponse | BaseException] = [
        StubResponse({"success": True, "identifier": "fixture-item"}),
        StubResponse(None),
        StubResponse(None),
        StubResponse(None),
        StubResponse({"success": True, "rows": []}),
    ]
    responses[{"availability": 0, "creation": 1, "transfer": 2, "poll": 4}[phase]] = (
        asyncio.CancelledError()
    )
    session = AsyncSession(responses)
    async with AsyncInternetArchiveClient(
        session=as_async_session(session), api_key=InternetArchiveApiKey("a", "s")
    ) as client:
        with pytest.raises(asyncio.CancelledError):
            await client.upload(
                [Path("owned"), InternetArchiveUploadFile(caller, "caller")],
                options(),
                wait=True,
            )
    assert owned.closed
    assert not caller.closed
    assert not session.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize(
    "case",
    [
        "before",
        "after",
        "expired-error",
        "timeout",
        "network",
        "long-timeout",
        "unknown",
    ],
)
async def test_polling_deadlines_and_network_error_classification(
    monkeypatch: pytest.MonkeyPatch,
    asynchronous: bool,
    case: str,
) -> None:
    """Bound request timeouts and distinguish transport failures from the deadline."""
    times = {
        "before": [0.0, 2.0],
        "after": [0.0, 0.0, 2.0],
        "expired-error": [0.0, 0.0, 2.0],
        "timeout": [0.0, 0.0, 0.0],
        "network": [0.0, 0.0, 0.0],
        "long-timeout": [0.0, 0.0, 0.0],
        "unknown": [0.0, 0.0, 0.0],
    }[case]
    clock = iter(times)
    module = async_module if asynchronous else sync_module
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=lambda: next(clock)))
    response: StubResponse | BaseException
    if case in {"before", "after"}:
        response = StubResponse({"success": True, "rows": []})
    elif case in {"timeout", "long-timeout"}:
        response = niquests.exceptions.Timeout("private")
    elif case == "unknown":
        response = NetworkError("network")
    else:
        response = niquests.exceptions.ConnectionError("private")
    session = AsyncSession([response]) if asynchronous else SyncSession([response])
    request_timeout = 0.5 if case == "long-timeout" else 30
    client = (
        AsyncInternetArchiveClient(
            session=as_async_session(cast("AsyncSession", session)),
            timeout=request_timeout,
        )
        if asynchronous
        else InternetArchiveClient(
            session=as_sync_session(cast("SyncSession", session)),
            timeout=request_timeout,
        )
    )
    expected = (
        NetworkError
        if case in {"network", "long-timeout", "unknown"}
        else PollingTimeoutError
    )
    with pytest.raises(expected):
        await resolve(
            client._wait_for_upload("fixture-item", wait_timeout=1, poll_interval=0.01)
        )
    if session.requests:
        assert session.requests[0][2]["timeout"] == min(request_timeout, 1)
        assert session.requests[0][2]["allow_redirects"] is False
    await resolve(client.close())


@pytest.mark.asyncio
async def test_polling_timeout_wraps_complete_transfer_outcomes(
    ia_endpoints: ServerState,
    item_client: Client,
) -> None:
    """Bound a perpetually pending ingest queue and preserve transfer success."""
    ia_endpoints.item_responses["GET", "/ia/upload-api"] = (
        200,
        '{"success":true,"rows":[{"cmd":"archive.php","wait_admin":0}]}',
        {},
    )
    with pytest.raises(InternetArchiveUploadError) as failure:
        await resolve(
            item_client.upload(
                [InternetArchiveUploadFile(BytesIO(b"a"), "a")],
                options(),
                wait=True,
                timeout=POLL_TIMEOUT,
                poll_interval=1,
            )
        )
    assert isinstance(failure.value.cause, PollingTimeoutError)
    assert failure.value.cause.job_id == "fixture-item"
    assert failure.value.cause.timeout == POLL_TIMEOUT
    assert failure.value.result.files[0].transferred


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("phase", ["creation", "first", "second"])
async def test_network_failure_retains_unknown_and_unattempted_outcomes(
    asynchronous: bool,
    phase: str,
) -> None:
    """Do not claim rollback or retry when a mutation's response is lost."""
    responses: list[StubResponse | BaseException] = [
        StubResponse({"success": True, "identifier": "fixture-item"}),
        StubResponse(None),
        StubResponse(None, headers={"ETag": '"first"'}),
        StubResponse(None),
    ]
    responses[{"creation": 1, "first": 2, "second": 3}[phase]] = (
        niquests.exceptions.ConnectionError("private server data")
    )
    session = AsyncSession(responses) if asynchronous else SyncSession(responses)
    key = InternetArchiveApiKey("access", "secret")
    client = (
        AsyncInternetArchiveClient(
            session=as_async_session(cast("AsyncSession", session)), api_key=key
        )
        if asynchronous
        else InternetArchiveClient(
            session=as_sync_session(cast("SyncSession", session)), api_key=key
        )
    )
    streams = [BytesIO(b"one"), BytesIO(b"two")]
    with pytest.raises(InternetArchiveUploadError) as failure:
        await resolve(
            client.upload(
                [
                    InternetArchiveUploadFile(stream, name)
                    for stream, name in zip(streams, ("first", "second"), strict=True)
                ],
                options(),
            )
        )
    error = failure.value
    assert isinstance(error.cause, NetworkError)
    assert error.failed_file == (None if phase == "creation" else phase)
    assert [file.transferred for file in error.result.files] == [
        phase == "second",
        False,
    ]
    assert "private server data" not in str(error)
    assert "private server data" not in str(error.cause)
    assert all(not stream.closed for stream in streams)
    assert all(request[2]["allow_redirects"] is False for request in session.requests)
    await resolve(client.close())


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["upload", "remove"])
async def test_task_cancellation_interrupts_inflight_mutation(
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    """Propagate external task cancellation during a real asynchronous suspension."""
    entered = asyncio.Event()
    never = asyncio.Event()
    owned = BytesIO(b"owned")
    caller = BytesIO(b"caller")
    monkeypatch.setattr(Path, "open", lambda *args, **kwargs: owned)

    class WaitingSession(AsyncSession):
        """Suspend at the first mutation so the owner can cancel the task."""

        async def request(
            self, method: str, url: str, **kwargs: object
        ) -> StubResponse:
            """Allow availability, then block until externally cancelled."""
            if method == "POST" and "upload_api" in url:
                return StubResponse({"success": True, "identifier": "fixture-item"})
            entered.set()
            await never.wait()
            raise AssertionError("cancelled mutation resumed")

    client = AsyncInternetArchiveClient(
        session=as_async_session(WaitingSession()),
        api_key=InternetArchiveApiKey("access", "secret"),
        cookies=InternetArchiveCookies("user", "signature"),
    )
    task = asyncio.create_task(
        client.upload(
            [Path("owned"), InternetArchiveUploadFile(caller, "caller")], options()
        )
        if operation == "upload"
        else client.remove_items(["fixture-item"])
    )
    try:
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert owned.closed == (operation == "upload")
        assert not caller.closed
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["bootstrap", "availability"])
@pytest.mark.parametrize("status", [302, 403, 429, 503])
async def test_precreation_http_failures_do_not_redirect_retry_or_claim_results(
    ia_endpoints: ServerState,
    item_client: Client,
    stage: str,
    status: int,
) -> None:
    """Leave pre-creation authentication and availability errors unwrapped."""
    if stage == "bootstrap":
        item_client._api_key = None
        item_client._cookies = InternetArchiveCookies("user", "signature")
    method, path = (
        ("GET", "/ia/upload") if stage == "bootstrap" else ("POST", "/ia/upload-api")
    )
    ia_endpoints.item_responses[method, path] = (
        status,
        "failure",
        {"Location": f"{ia_endpoints.base_url}/redirect-target"},
    )
    with pytest.raises(ServiceError) as failure:
        await resolve(
            item_client.upload(
                [InternetArchiveUploadFile(BytesIO(b"a"), "a")], options()
            )
        )
    assert not isinstance(failure.value, InternetArchiveUploadError)
    assert len(ia_endpoints.requests) == 1
