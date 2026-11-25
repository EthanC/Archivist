"""Pin the guarded single-file transfer contract through both HTTP clients."""

from __future__ import annotations

import pickle
from collections.abc import AsyncIterator, Awaitable, Iterator
from inspect import isawaitable
from io import BytesIO
from typing import TYPE_CHECKING, Any, TypeVar, cast

import niquests
import pytest
import pytest_asyncio
from niquests.adapters import AsyncHTTPAdapter, HTTPAdapter

import archivist.core
from archivist import (
    AsyncInternetArchiveClient,
    InternetArchiveApiKey,
    InternetArchiveClient,
    InternetArchiveRecoveryConflictError,
    InternetArchiveRecoveryDeferredError,
    InternetArchiveRecoveryError,
    InternetArchiveRecoveryPhase,
    InternetArchiveRecoveryReason,
    InternetArchiveRecoveryResult,
    InternetArchiveUploadError,
    InternetArchiveUploadFile,
    InternetArchiveUploadFileResult,
    InternetArchiveUploadResult,
    InvalidOptionError,
    InvalidServiceResponseError,
    RateLimitError,
    ServiceError,
    parse_retry_after,
)
from archivist.services.internet_archive import _recovery
from tests.conftest import s3_error

if TYPE_CHECKING:
    from tests.conftest import ServerState

T = TypeVar("T")
Client = InternetArchiveClient | AsyncInternetArchiveClient
IDENTIFIER = "transfer-item"
NAME = "folder/original file.bin"
PUT_PATH = f"/ia/s3/{IDENTIFIER}/folder/original%20file.bin"
METADATA_PATH = f"/ia/metadata/{IDENTIFIER}"
OWNER = "account@example.invalid"
EXPECTED = {"source": "https://example.invalid/exact", "operation": "op-123"}
PAYLOAD = b"guarded payload"
SLOW_DOWN = 503
RETRY_SECONDS = 120.0
Reason = InternetArchiveRecoveryReason


async def resolve(value: T | Awaitable[T]) -> T:
    """Resolve matching synchronous and asynchronous client calls."""
    if isawaitable(value):
        return await cast("Awaitable[T]", value)
    return cast("T", value)


@pytest_asyncio.fixture(
    params=[InternetArchiveClient, AsyncInternetArchiveClient], ids=["sync", "async"]
)
async def client(request: pytest.FixtureRequest) -> AsyncIterator[Client]:
    """Provide both clients with one LOW key pair."""
    instance = request.param(
        api_key=InternetArchiveApiKey("test-access", "test-secret")
    )
    try:
        yield instance
    finally:
        await resolve(instance.close())


def snapshot(**changes: object) -> dict[str, object]:
    """Return an owned, authoritative item snapshot with optional overrides."""
    data: dict[str, object] = {
        "metadata": {"identifier": IDENTIFIER, "uploader": OWNER, **EXPECTED},
        "files": [{"name": "other.bin", "source": "original", "size": "1"}],
        "files_count": 1,
        "workable_servers": ["fixture"],
    }
    data.update(changes)
    return data


def owned_item(state: ServerState, **changes: object) -> None:
    """Serve one item snapshot and accept PUTs into its bucket."""
    state.item_buckets.add(IDENTIFIER)
    state.item_snapshots[IDENTIFIER] = [snapshot(**changes)]


def transfer(  # noqa: PLR0913 - Mirrors the public keyword arguments.
    client: Client,
    *,
    payload: bytes = PAYLOAD,
    expected_metadata: object = EXPECTED,
    expected_size: object = None,
    queue_derive: object = True,
    interactive_priority: object = True,
    file: object = None,
) -> InternetArchiveUploadResult | Awaitable[InternetArchiveUploadResult]:
    """Call ``transfer_missing_file`` with valid defaults."""
    return client.transfer_missing_file(
        IDENTIFIER,
        cast(
            "InternetArchiveUploadFile",
            file
            if file is not None
            else InternetArchiveUploadFile(BytesIO(payload), NAME),
        ),
        expected_metadata=cast("Any", expected_metadata),
        expected_size=cast(
            "int", len(payload) if expected_size is None else expected_size
        ),
        queue_derive=cast("bool", queue_derive),
        interactive_priority=cast("bool", interactive_priority),
    )


def lower_headers(request: Any) -> dict[str, str]:  # noqa: ANN401 - Recorded request.
    """Return recorded headers with lowercase names."""
    return {name.lower(): value for name, value in request.headers.items()}


def puts(state: ServerState) -> list[object]:
    """Return every PUT the fixture server received."""
    return [request for request in state.requests if request.method == "PUT"]


@pytest.mark.asyncio
async def test_transfer_follows_the_specified_request_sequence(
    ia_endpoints: ServerState, client: Client
) -> None:
    """Read identity, read metadata, then send exactly one create-only PUT."""
    owned_item(ia_endpoints)
    stream = BytesIO(b"skip:" + PAYLOAD)
    stream.seek(len(b"skip:"))
    result = await resolve(
        transfer(client, file=InternetArchiveUploadFile(stream, NAME))
    )

    assert result == InternetArchiveUploadResult(
        IDENTIFIER,
        (
            InternetArchiveUploadFileResult(
                NAME, len(PAYLOAD), transferred=True, etag='"fixture-etag"'
            ),
        ),
    )
    assert [(request.method, request.path) for request in ia_endpoints.requests] == [
        ("GET", "/ia/user"),
        ("GET", METADATA_PATH),
        ("PUT", PUT_PATH),
    ]
    identity, metadata, put = ia_endpoints.requests
    assert identity.query == {"op": ["whoami"]}
    assert metadata.query == {"extended_err": ["1"]}
    for request in ia_endpoints.requests:
        headers = lower_headers(request)
        assert headers["authorization"] == "LOW test-access:test-secret"
        assert "cookie" not in headers
    assert put.body == PAYLOAD
    headers = lower_headers(put)
    assert headers["content-type"] == "application/octet-stream"
    assert headers["content-length"] == str(len(PAYLOAD))
    assert headers["if-none-match"] == "*"
    assert headers["x-archive-interactive-priority"] == "1"
    assert "x-archive-queue-derive" not in headers
    assert not any(key.startswith("x-archive-meta") for key in headers)
    assert ia_endpoints.item_files[IDENTIFIER] == {NAME: PAYLOAD}


@pytest.mark.asyncio
@pytest.mark.parametrize("queue_derive", [True, False])
@pytest.mark.parametrize("interactive_priority", [True, False])
async def test_transfer_header_combinations(
    ia_endpoints: ServerState,
    client: Client,
    *,
    queue_derive: bool,
    interactive_priority: bool,
) -> None:
    """Send the optional queue headers only when their options ask for them."""
    owned_item(ia_endpoints)
    await resolve(
        transfer(
            client,
            queue_derive=queue_derive,
            interactive_priority=interactive_priority,
        )
    )
    (put,) = ia_endpoints.matching(PUT_PATH, "PUT")
    headers = lower_headers(put)
    assert headers.get("x-archive-queue-derive") == (None if queue_derive else "0")
    assert headers.get("x-archive-interactive-priority") == (
        "1" if interactive_priority else None
    )
    assert headers["if-none-match"] == "*"


@pytest.mark.asyncio
async def test_one_element_list_metadata_matches_a_scalar(
    ia_endpoints: ServerState, client: Client
) -> None:
    """Treat a stored one-element list as the scalar the caller expects."""
    owned_item(
        ia_endpoints,
        metadata={
            "identifier": IDENTIFIER,
            "uploader": OWNER,
            "source": [EXPECTED["source"]],
            "operation": EXPECTED["operation"],
        },
    )
    result = await resolve(transfer(client))
    assert result.files[0].transferred


DEFERRED_CASES = {
    "metadata-missing": (
        {"metadata": None},
        Reason.METADATA_UNRESOLVED,
    ),
    "empty-response": ({"__replace__": {}}, Reason.METADATA_UNRESOLVED),
    "uploader-missing": (
        {"metadata": {"identifier": IDENTIFIER, **EXPECTED}},
        Reason.METADATA_UNRESOLVED,
    ),
    "slot-occupied": (
        {
            "files": [{"name": NAME, "source": "original", "size": "1"}],
            "files_count": 1,
        },
        Reason.SLOT_OCCUPIED,
    ),
    "files-missing": (
        {"files": None, "files_count": None},
        Reason.LISTING_NOT_AUTHORITATIVE,
    ),
    "count-mismatch": ({"files_count": 2}, Reason.LISTING_NOT_AUTHORITATIVE),
    "servers-unavailable": (
        {"servers_unavailable": True},
        Reason.LISTING_NOT_AUTHORITATIVE,
    ),
    "pending-creation": ({"errcode": 101}, Reason.LISTING_NOT_AUTHORITATIVE),
    "secondary-copy": ({"errcode": 106}, Reason.LISTING_NOT_AUTHORITATIVE),
}


def _apply(state: ServerState, changes: dict[str, object]) -> None:
    if "__replace__" in changes:
        state.item_buckets.add(IDENTIFIER)
        state.item_snapshots[IDENTIFIER] = [
            cast("dict[str, object]", changes["__replace__"])
        ]
        return
    owned_item(state)
    data = state.item_snapshots[IDENTIFIER][0]
    for key, value in changes.items():
        if value is None:
            data.pop(key, None)
        else:
            data[key] = value


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("changes", "reason"), DEFERRED_CASES.values(), ids=DEFERRED_CASES.keys()
)
async def test_deferred_branches_never_put(
    ia_endpoints: ServerState,
    client: Client,
    changes: dict[str, object],
    reason: InternetArchiveRecoveryReason,
) -> None:
    """Defer with a stable reason when current state cannot authorize a write."""
    _apply(ia_endpoints, changes)
    with pytest.raises(InternetArchiveRecoveryDeferredError) as raised:
        await resolve(transfer(client))
    error = raised.value
    assert error.reason is reason
    assert not isinstance(error, InternetArchiveRecoveryConflictError)
    assert error.result.identifier == IDENTIFIER
    assert error.result.files == ()
    assert isinstance(error.cause, ServiceError)
    assert not puts(ia_endpoints)
    if reason is Reason.SLOT_OCCUPIED:
        assert str(error.cause) == "file slot is occupied"


CONFLICT_CASES = {
    "uploader-mismatch": (
        {
            "metadata": {
                "identifier": IDENTIFIER,
                "uploader": "else@example.invalid",
                **EXPECTED,
            }
        },
        Reason.OWNERSHIP_MISMATCH,
    ),
    "provenance-mismatch": (
        {
            "metadata": {
                "identifier": IDENTIFIER,
                "uploader": OWNER,
                **EXPECTED,
                "operation": "op-999",
            }
        },
        Reason.PROVENANCE_MISSING,
    ),
    "provenance-absent": (
        {
            "metadata": {
                "identifier": IDENTIFIER,
                "uploader": OWNER,
                "source": EXPECTED["source"],
            }
        },
        Reason.PROVENANCE_MISSING,
    ),
    "provenance-two-values": (
        {
            "metadata": {
                "identifier": IDENTIFIER,
                "uploader": OWNER,
                "source": [EXPECTED["source"], EXPECTED["source"]],
                "operation": EXPECTED["operation"],
            }
        },
        Reason.PROVENANCE_MISSING,
    ),
    "has-redrow": ({"has_redrow": True}, Reason.ITEM_RESTRICTED),
    "is-dark": ({"is_dark": True}, Reason.ITEM_RESTRICTED),
    "nodownload": ({"nodownload": True}, Reason.ITEM_RESTRICTED),
    # Ownership is decided before listing authority and the occupied slot.
    "mismatch-beats-unavailable": (
        {
            "metadata": {"identifier": IDENTIFIER, "uploader": "else@example.invalid"},
            "errcode": 106,
            "files": [{"name": NAME}],
        },
        Reason.OWNERSHIP_MISMATCH,
    ),
    "restricted-beats-occupied": (
        {"is_dark": True, "files": [{"name": NAME}]},
        Reason.ITEM_RESTRICTED,
    ),
}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("changes", "reason"), CONFLICT_CASES.values(), ids=CONFLICT_CASES.keys()
)
async def test_conflict_branches_never_put(
    ia_endpoints: ServerState,
    client: Client,
    changes: dict[str, object],
    reason: InternetArchiveRecoveryReason,
) -> None:
    """Refuse with a stable reason when item state contradicts the caller."""
    _apply(ia_endpoints, changes)
    with pytest.raises(InternetArchiveRecoveryConflictError) as raised:
        await resolve(transfer(client))
    error = raised.value
    assert error.reason is reason
    assert isinstance(error, InternetArchiveRecoveryError)
    assert not isinstance(error, InternetArchiveRecoveryDeferredError)
    assert error.phase is (
        InternetArchiveRecoveryPhase.OWNERSHIP
        if reason is Reason.OWNERSHIP_MISMATCH
        else InternetArchiveRecoveryPhase.RECONCILIATION
    )
    assert not puts(ia_endpoints)


LOCAL_CONFLICTS = {
    "size-larger": ({"expected_size": len(PAYLOAD) + 1}, Reason.SIZE_CHANGED),
    "size-smaller": ({"expected_size": len(PAYLOAD) - 1}, Reason.SIZE_CHANGED),
    "size-zero": ({"expected_size": 0, "payload": b""}, Reason.SIZE_CHANGED),
    "size-negative": ({"expected_size": -1}, Reason.SIZE_CHANGED),
    "metadata-empty": ({"expected_metadata": {}}, Reason.PROVENANCE_MISSING),
    "metadata-empty-value": (
        {"expected_metadata": {**EXPECTED, "operation": ""}},
        Reason.PROVENANCE_MISSING,
    ),
    "metadata-empty-key": (
        {"expected_metadata": {**EXPECTED, "": "value"}},
        Reason.PROVENANCE_MISSING,
    ),
}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kwargs", "reason"), LOCAL_CONFLICTS.values(), ids=LOCAL_CONFLICTS.keys()
)
async def test_local_conflicts_send_no_request(
    ia_endpoints: ServerState,
    client: Client,
    kwargs: dict[str, Any],
    reason: InternetArchiveRecoveryReason,
) -> None:
    """Refuse a changed size or missing provenance before any network access."""
    owned_item(ia_endpoints)
    with pytest.raises(InternetArchiveRecoveryConflictError) as raised:
        await resolve(transfer(client, **kwargs))
    assert raised.value.reason is reason
    assert ia_endpoints.requests == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"queue_derive": 1}, "queue_derive"),
        ({"interactive_priority": None}, "interactive_priority"),
        ({"expected_size": True}, "expected_size"),
        ({"expected_size": 1.0}, "expected_size"),
        ({"expected_metadata": [("source", "x")]}, "expected_metadata"),
        ({"expected_metadata": {"source": ("x",)}}, "expected_metadata"),
        ({"file": "local.bin"}, "file"),
    ],
)
async def test_invalid_arguments_raise_before_network(
    ia_endpoints: ServerState, client: Client, kwargs: dict[str, Any], message: str
) -> None:
    """Apply the add_files type checks without contacting the service."""
    with pytest.raises(InvalidOptionError, match=message):
        await resolve(transfer(client, **kwargs))
    assert ia_endpoints.requests == []


@pytest.mark.asyncio
async def test_invalid_identifier_raises_before_network(
    ia_endpoints: ServerState, client: Client
) -> None:
    """Validate the identifier like the other item methods."""
    with pytest.raises(InvalidOptionError):
        await resolve(
            client.transfer_missing_file(
                "../bad",
                InternetArchiveUploadFile(BytesIO(PAYLOAD), NAME),
                expected_metadata=EXPECTED,
                expected_size=len(PAYLOAD),
            )
        )
    assert ia_endpoints.requests == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "body"),
    [(201, ""), (204, ""), (200, "unexpected body")],
    ids=["created", "no-content", "body"],
)
async def test_unacknowledged_put_is_wrapped(
    ia_endpoints: ServerState, client: Client, status: int, body: str
) -> None:
    """Treat anything but an empty 200 as an unacknowledged transfer."""
    owned_item(ia_endpoints)
    ia_endpoints.item_responses["PUT", PUT_PATH] = (status, body, {"ETag": '"x"'})
    with pytest.raises(InternetArchiveUploadError) as raised:
        await resolve(transfer(client))
    error = raised.value
    assert error.failed_file == NAME
    assert isinstance(error.cause, InvalidServiceResponseError)
    assert str(error.cause) == "file transfer was not acknowledged"
    assert error.cause.status_code == status
    assert error.result == InternetArchiveUploadResult(
        IDENTIFIER, (InternetArchiveUploadFileResult(NAME, len(PAYLOAD)),)
    )
    assert len(puts(ia_endpoints)) == 1


@pytest.mark.asyncio
async def test_put_service_error_is_wrapped_with_retry_hint(
    ia_endpoints: ServerState, client: Client
) -> None:
    """Keep the classified cause, its phase, and Retry-After for the caller."""
    owned_item(ia_endpoints)
    ia_endpoints.item_responses["PUT", PUT_PATH] = (
        SLOW_DOWN,
        s3_error("SlowDown"),
        {"Content-Type": "application/xml", "Retry-After": f"{RETRY_SECONDS:g}"},
    )
    with pytest.raises(InternetArchiveUploadError) as raised:
        await resolve(transfer(client))
    error = raised.value
    assert error.failed_file == NAME
    assert isinstance(error.cause, RateLimitError)
    assert error.cause.service_code == "SlowDown"
    assert error.cause.request_phase == "file-upload"
    assert error.cause.retry_after == RETRY_SECONDS
    assert error.status_code == SLOW_DOWN
    assert not error.result.files[0].transferred
    assert len(puts(ia_endpoints)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "phase"),
    [("/ia/user", "identity"), (METADATA_PATH, "item-metadata")],
)
async def test_read_failures_report_their_request_phase(
    ia_endpoints: ServerState, client: Client, path: str, phase: str
) -> None:
    """Report identity and item-metadata failures unwrapped, before any PUT."""
    owned_item(ia_endpoints)
    ia_endpoints.item_responses["GET", path] = (
        500,
        '{"error": "broken"}',
        {"Content-Type": "application/json"},
    )
    with pytest.raises(ServiceError) as raised:
        await resolve(transfer(client))
    assert not isinstance(raised.value, InternetArchiveUploadError)
    assert raised.value.request_phase == phase
    assert not puts(ia_endpoints)


@pytest.mark.asyncio
async def test_etag_is_passed_through_verbatim(
    ia_endpoints: ServerState, client: Client
) -> None:
    """Return the acknowledgement ETag exactly as IAS3 sent it."""
    owned_item(ia_endpoints)
    ia_endpoints.item_responses["PUT", PUT_PATH] = (200, "", {"ETag": '"abc-123"'})
    result = await resolve(transfer(client))
    assert result.files[0].etag == '"abc-123"'


@pytest.mark.asyncio
async def test_missing_etag_is_none(ia_endpoints: ServerState, client: Client) -> None:
    """Report a missing ETag as None rather than failing an acknowledged PUT."""
    owned_item(ia_endpoints)
    ia_endpoints.item_responses["PUT", PUT_PATH] = (200, "", {})
    result = await resolve(transfer(client))
    assert result.files[0].transferred
    assert result.files[0].etag is None


class CountingBody:
    """Wrap a sized sync body and count the bytes the transport actually read."""

    def __init__(self, body: Any, counter: list[int]) -> None:  # noqa: ANN401 - Transport body.
        """Retain the wrapped body and shared counter."""
        self._body = body
        self._counter = counter

    def __len__(self) -> int:
        """Preserve Content-Length."""
        return len(self._body)

    def __iter__(self) -> Iterator[bytes]:
        """Count synchronous chunks."""
        for chunk in self._body:
            self._counter.append(len(chunk))
            yield chunk


class AsyncCountingBody:
    """Wrap a sized async body and count the bytes the transport actually read."""

    def __init__(self, body: Any, counter: list[int]) -> None:  # noqa: ANN401 - Transport body.
        """Retain the wrapped body and shared counter."""
        self._body = body
        self._counter = counter

    def __len__(self) -> int:
        """Preserve Content-Length."""
        return len(self._body)

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """Count asynchronous chunks."""
        async for chunk in self._body:
            self._counter.append(len(chunk))
            yield chunk


class LimiterAdapter:
    """Stand in for a caller's bandwidth-limiting adapter by wrapping a real one."""

    max_retries = False

    def __init__(self, counter: list[int]) -> None:
        """Wrap a retry-free transport adapter."""
        self.inner = HTTPAdapter(max_retries=0)
        self.counter = counter
        self.methods: list[str] = []

    def send(self, request: Any, **kwargs: Any) -> Any:  # noqa: ANN401 - Adapter API.
        """Throttle point: wrap every streamed request body."""
        self.methods.append(request.method)
        if request.body is not None and not isinstance(request.body, bytes):
            request.body = CountingBody(request.body, self.counter)
        return self.inner.send(request, **kwargs)

    def close(self) -> None:
        """Close the wrapped adapter."""
        self.inner.close()


class AsyncLimiterAdapter:
    """Stand in for a caller's async bandwidth-limiting adapter."""

    max_retries = False

    def __init__(self, counter: list[int]) -> None:
        """Wrap a retry-free transport adapter."""
        self.inner = AsyncHTTPAdapter(max_retries=0)
        self.counter = counter
        self.methods: list[str] = []

    async def send(self, request: Any, **kwargs: Any) -> Any:  # noqa: ANN401 - Adapter API.
        """Throttle point: wrap every streamed request body."""
        self.methods.append(request.method)
        if request.body is not None and not isinstance(request.body, bytes):
            request.body = AsyncCountingBody(request.body, self.counter)
        return await self.inner.send(request, **kwargs)

    async def close(self) -> None:
        """Close the wrapped adapter."""
        await self.inner.close()


def test_sync_put_body_passes_through_mounted_limiter(
    ia_endpoints: ServerState,
) -> None:
    """Stream the PUT body through the session's mounted adapter."""
    owned_item(ia_endpoints)
    counter: list[int] = []
    adapter = LimiterAdapter(counter)
    with niquests.Session(retries=0) as session:
        for prefix in list(session.adapters):
            session.mount(prefix, HTTPAdapter(max_retries=0))
        session.mount("http://", cast("Any", adapter))
        client = InternetArchiveClient(
            session=session, api_key=InternetArchiveApiKey("test-access", "test-secret")
        )
        result = client.transfer_missing_file(
            IDENTIFIER,
            InternetArchiveUploadFile(BytesIO(PAYLOAD), NAME),
            expected_metadata=EXPECTED,
            expected_size=len(PAYLOAD),
        )
    assert result.files[0].transferred
    assert adapter.methods == ["GET", "GET", "PUT"]
    assert sum(counter) == len(PAYLOAD)
    assert ia_endpoints.item_files[IDENTIFIER][NAME] == PAYLOAD


@pytest.mark.asyncio
async def test_async_put_body_passes_through_mounted_limiter(
    ia_endpoints: ServerState,
) -> None:
    """Stream the async PUT body through the session's mounted adapter."""
    owned_item(ia_endpoints)
    counter: list[int] = []
    adapter = AsyncLimiterAdapter(counter)
    async with niquests.AsyncSession(retries=0) as session:
        for prefix in list(session.adapters):
            session.mount(prefix, AsyncHTTPAdapter(max_retries=0))
        session.mount("http://", cast("Any", adapter))
        client = AsyncInternetArchiveClient(
            session=session, api_key=InternetArchiveApiKey("test-access", "test-secret")
        )
        result = await client.transfer_missing_file(
            IDENTIFIER,
            InternetArchiveUploadFile(BytesIO(PAYLOAD), NAME),
            expected_metadata=EXPECTED,
            expected_size=len(PAYLOAD),
        )
    assert result.files[0].transferred
    assert adapter.methods == ["GET", "GET", "PUT"]
    assert sum(counter) == len(PAYLOAD)
    assert ia_endpoints.item_files[IDENTIFIER][NAME] == PAYLOAD


def test_conflict_and_deferred_errors_pickle_with_reason() -> None:
    """Retain the stable reason across process boundaries."""
    # Round-trips bytes this test produced itself; nothing untrusted is loaded.
    for reason, deferred in [
        (Reason.SLOT_OCCUPIED, True),
        (Reason.OWNERSHIP_MISMATCH, False),
    ]:
        error = _recovery.transfer_error(
            IDENTIFIER, reason, "message", deferred=deferred
        )
        restored = pickle.loads(pickle.dumps(error))
        assert type(restored) is type(error)
        assert restored.reason is reason
        assert restored.phase is error.phase
        assert restored.args == error.args


def test_add_files_errors_have_no_reason() -> None:
    """Keep ``reason`` optional for the existing recovery errors."""
    error = InternetArchiveRecoveryDeferredError(
        InternetArchiveRecoveryResult(IDENTIFIER, ()),
        ServiceError("x"),
        phase=InternetArchiveRecoveryPhase.INGEST,
    )
    assert error.reason is None


def test_parse_retry_after_is_public() -> None:
    """Expose Retry-After parsing without the private HTTP module."""
    assert archivist.core.parse_retry_after is parse_retry_after
    assert parse_retry_after({"Retry-After": "5"}) == (5.0, "5")
    assert parse_retry_after({}) == (None, None)
