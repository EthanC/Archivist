"""Exercise conservative file-level recovery through both HTTP clients."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable
from inspect import isawaitable
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar, cast

import niquests
import pytest
import pytest_asyncio

from archivist import (
    AsyncInternetArchiveClient,
    InternetArchiveApiKey,
    InternetArchiveChecksumState,
    InternetArchiveClient,
    InternetArchiveCookies,
    InternetArchiveFileDisposition,
    InternetArchiveItemAvailability,
    InternetArchiveRecoveryDeferredError,
    InternetArchiveRecoveryError,
    InternetArchiveRecoveryPhase,
    InternetArchiveUploadFile,
    InvalidOptionError,
    InvalidServiceResponseError,
    NetworkError,
    PollingTimeoutError,
)
from archivist.services.internet_archive import _recovery
from archivist.services.internet_archive import async_client as async_module
from archivist.services.internet_archive import client as sync_module
from tests.services.internet_archive.test_async_item_io import (
    IOGate,
    ObservedStream,
    heartbeat,
)
from tests.services.internet_archive.test_client_edge_cases import (
    AsyncSession,
    StubResponse,
    as_async_session,
)

if TYPE_CHECKING:
    from tests.conftest import ServerState

T = TypeVar("T")
Client = InternetArchiveClient | AsyncInternetArchiveClient
IDENTIFIER = "recovery-item"
EXPECTED = {"source": "https://example.invalid/exact", "operation": "op-123"}
TWO_READS = 2


async def resolve(value: T | Awaitable[T]) -> T:
    """Resolve matching synchronous and asynchronous client calls."""
    if isawaitable(value):
        return await cast("Awaitable[T]", value)
    return cast("T", value)


@pytest_asyncio.fixture(
    params=[InternetArchiveClient, AsyncInternetArchiveClient], ids=["sync", "async"]
)
async def recovery_client(request: pytest.FixtureRequest) -> AsyncIterator[Client]:
    """Provide both clients with one LOW key pair."""
    client = request.param(api_key=InternetArchiveApiKey("test-access", "test-secret"))
    try:
        yield client
    finally:
        await resolve(client.close())


def existing_item(state: ServerState) -> None:
    """Create an owned fixture item whose catalog is ready for recovery."""
    state.item_buckets.add(IDENTIFIER)
    state.item_metadata[IDENTIFIER] = {
        "identifier": IDENTIFIER,
        "uploader": "account@example.invalid",
        **EXPECTED,
    }
    state.status_calls[f"catalog:{IDENTIFIER}"] = 1


@pytest.mark.asyncio
async def test_get_item_retains_all_files_tasks_and_unresolved_visibility(
    ia_endpoints: ServerState, recovery_client: Client
) -> None:
    """Expose originals separately without discarding collision-relevant names."""
    ia_endpoints.item_snapshots[IDENTIFIER] = [
        {
            "metadata": {
                "identifier": IDENTIFIER,
                "uploader": "account@example.invalid",
                **EXPECTED,
            },
            "files": [
                {
                    "name": "original.bin",
                    "source": "original",
                    "size": "1",
                    "md5": "0cc175b9c0f1b6a831c399e269772661",
                    "sha1": "86f7e437faa5a7fce15d1ddcb9eaeaea377667b8",
                },
                {
                    "name": "derived.txt",
                    "source": "derivative",
                    "size": "2",
                },
                {"name": f"{IDENTIFIER}_meta.xml", "size": "3"},
            ],
            "files_count": 3,
            "tasks": [
                {"cmd": "archive.php", "state": "running", "wait_admin": 0},
                {"cmd": "derive.php", "state": "complete", "wait_admin": 0},
            ],
            "workable_servers": ["one", "two"],
        }
    ]
    item = await resolve(recovery_client.get_item(IDENTIFIER))
    assert item.availability is InternetArchiveItemAvailability.AVAILABLE
    assert [file.name for file in item.files] == [
        "original.bin",
        "derived.txt",
        f"{IDENTIFIER}_meta.xml",
    ]
    assert [file.name for file in item.original_files] == ["original.bin"]
    assert item.files[1].md5 is item.files[1].sha1 is None
    assert [task.command for task in item.pending_tasks] == ["archive.php"]
    assert item.workable_servers == ("one", "two")
    request = ia_endpoints.matching(f"/ia/metadata/{IDENTIFIER}", "GET")[0]
    assert request.query == {"extended_err": ["1"]}

    ia_endpoints.item_snapshots["unknown-item"] = [{}]
    unresolved = await resolve(recovery_client.get_item("unknown-item"))
    assert unresolved.availability is InternetArchiveItemAvailability.UNCERTAIN
    assert not unresolved.available


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["is_dark", "nodownload", "has_redrow"])
async def test_get_item_retains_state_without_metadata(
    ia_endpoints: ServerState, recovery_client: Client, field: str
) -> None:
    """Expose server-confirmed flags even after the item metadata is removed."""
    ia_endpoints.item_snapshots[IDENTIFIER] = [{field: True}]
    item = await resolve(recovery_client.get_item(IDENTIFIER))
    assert getattr(item, field) is True
    assert not item.available
    assert item.metadata == {}
    assert not any(request.method != "GET" for request in ia_endpoints.requests)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state", [{"errcode": 101}, {"errcode": 106}, {"servers_unavailable": True}]
)
async def test_unreliable_metadata_never_authorizes_a_put(
    ia_endpoints: ServerState, recovery_client: Client, state: dict[str, object]
) -> None:
    """Honor wire errors even with matching provenance and a completed catalog."""
    existing_item(ia_endpoints)
    ia_endpoints.item_snapshots[IDENTIFIER] = [
        {
            "metadata": ia_endpoints.item_metadata[IDENTIFIER],
            "files": [],
            "files_count": 0,
            **state,
        }
    ]
    with pytest.raises(InternetArchiveRecoveryDeferredError):
        await resolve(
            recovery_client.add_files(
                IDENTIFIER,
                [InternetArchiveUploadFile(BytesIO(b"data"), "file.bin")],
                expected_metadata=EXPECTED,
            )
        )
    assert not any(request.method != "GET" for request in ia_endpoints.requests)


@pytest.mark.asyncio
async def test_add_files_transfers_only_missing_then_reconciles_idempotently(
    ia_endpoints: ServerState, recovery_client: Client
) -> None:
    """Use isolated LOW requests and never send create or metadata headers."""
    existing_item(ia_endpoints)
    payload = b"prefix:payload"
    stream = BytesIO(payload)
    stream.seek(len(b"prefix:"))
    result = await resolve(
        recovery_client.add_files(
            IDENTIFIER,
            [InternetArchiveUploadFile(stream, "folder/file.bin")],
            expected_metadata=EXPECTED,
        )
    )
    file = result.files[0]
    assert result.details_url == f"https://archive.org/details/{IDENTIFIER}"
    assert file.disposition is InternetArchiveFileDisposition.TRANSFERRED
    assert file.transferred and file.etag == '"fixture-etag"'
    assert file.checksum_state is InternetArchiveChecksumState.PENDING
    assert ia_endpoints.item_files[IDENTIFIER]["folder/file.bin"] == b"payload"
    assert not stream.closed

    put = ia_endpoints.matching(f"/ia/s3/{IDENTIFIER}/folder/file.bin", "PUT")[0]
    headers = {name.lower(): value for name, value in put.headers.items()}
    assert headers["authorization"] == "LOW test-access:test-secret"
    assert "cookie" not in headers
    assert "x-amz-auto-make-bucket" not in headers
    assert "x-archive-ignore-preexisting-bucket" not in headers
    assert not any(name.startswith("x-archive-meta") for name in headers)
    assert not ia_endpoints.matching(f"/ia/s3/{IDENTIFIER}", "PUT")
    assert not ia_endpoints.matching("/ia/upload-api", "POST")

    puts_before = len(
        [request for request in ia_endpoints.requests if request.method == "PUT"]
    )
    repeated = await resolve(
        recovery_client.add_files(
            IDENTIFIER,
            [InternetArchiveUploadFile(BytesIO(b"payload"), "folder/file.bin")],
            expected_metadata=EXPECTED,
        )
    )
    assert (
        repeated.files[0].disposition is InternetArchiveFileDisposition.ALREADY_MATCHING
    )
    assert not repeated.files[0].transferred
    assert repeated.files[0].checksum_state is InternetArchiveChecksumState.VERIFIED
    assert (
        len([request for request in ia_endpoints.requests if request.method == "PUT"])
        == puts_before
    )


@pytest.mark.asyncio
async def test_wait_covers_ingest_and_server_checksum_verification(
    ia_endpoints: ServerState, recovery_client: Client
) -> None:
    """Refresh after acknowledgement until the exact server hashes are visible."""
    existing_item(ia_endpoints)
    ia_endpoints.status_calls[f"catalog:{IDENTIFIER}"] = 0
    result = await resolve(
        recovery_client.add_files(
            IDENTIFIER,
            [InternetArchiveUploadFile(BytesIO(b"verified"), "verified.bin")],
            expected_metadata=EXPECTED,
            wait=True,
            timeout=5,
            poll_interval=0.001,
        )
    )
    assert result.processing_complete
    assert result.verification_complete
    assert result.files[0].transferred
    assert result.files[0].checksum_state is InternetArchiveChecksumState.VERIFIED
    assert len(ia_endpoints.matching(f"/ia/metadata/{IDENTIFIER}", "GET")) >= TWO_READS


@pytest.mark.asyncio
async def test_recovery_suppresses_derivation_until_the_last_missing_file(
    ia_endpoints: ServerState, recovery_client: Client
) -> None:
    """Transfer a complete missing batch without a dummy derive request."""
    existing_item(ia_endpoints)
    result = await resolve(
        recovery_client.add_files(
            IDENTIFIER,
            [
                InternetArchiveUploadFile(BytesIO(b"a"), "a.bin"),
                InternetArchiveUploadFile(BytesIO(b"b"), "b.bin"),
            ],
            expected_metadata=EXPECTED,
        )
    )
    assert all(file.transferred for file in result.files)
    puts = [request for request in ia_endpoints.requests if request.method == "PUT"]
    assert [
        {name.lower(): value for name, value in request.headers.items()}.get(
            "x-archive-queue-derive"
        )
        for request in puts
    ] == ["0", None]


@pytest.mark.asyncio
@pytest.mark.parametrize("wait", [False, True])
async def test_acknowledged_batch_file_must_be_visible_before_the_next_put(
    ia_endpoints: ServerState, recovery_client: Client, wait: bool
) -> None:
    """Defer or poll when an accepted file is not yet visible in metadata."""
    existing_item(ia_endpoints)
    empty: dict[str, object] = {
        "metadata": ia_endpoints.item_metadata[IDENTIFIER],
        "files": [],
        "files_count": 0,
        "workable_servers": ["fixture"],
    }
    first = {
        "name": "a.bin",
        "source": "original",
        "size": "1",
        "md5": "0cc175b9c0f1b6a831c399e269772661",
        "sha1": "86f7e437faa5a7fce15d1ddcb9eaeaea377667b8",
    }
    second = {
        "name": "b.bin",
        "source": "original",
        "size": "1",
        "md5": "92eb5ffee6ae2fec3ad71c777531578f",
        "sha1": "e9d71f5ee7c92d6dc9e92ffdad17b8bd49418f98",
    }
    snapshots: list[dict[str, object]] = [empty, empty]
    if wait:
        snapshots.extend(
            [
                {**empty, "files": [first], "files_count": 1},
                {**empty, "files": [first, second], "files_count": 2},
            ]
        )
    ia_endpoints.item_snapshots[IDENTIFIER] = snapshots

    def recover() -> object:
        return recovery_client.add_files(
            IDENTIFIER,
            [
                InternetArchiveUploadFile(BytesIO(b"a"), "a.bin"),
                InternetArchiveUploadFile(BytesIO(b"b"), "b.bin"),
            ],
            expected_metadata=EXPECTED,
            wait=wait,
            timeout=5,
            poll_interval=0.001,
        )

    if wait:
        result = await resolve(cast("Any", recover()))
        assert result.verification_complete
        assert all(file.transferred for file in result.files)
    else:
        with pytest.raises(InternetArchiveRecoveryDeferredError) as failure:
            await resolve(cast("Any", recover()))
        assert failure.value.phase is InternetArchiveRecoveryPhase.VERIFICATION
        assert (
            failure.value.result.files[0].disposition
            is InternetArchiveFileDisposition.UNCERTAIN
        )
    puts = [request for request in ia_endpoints.requests if request.method == "PUT"]
    assert len(puts) == (2 if wait else 1)


@pytest.mark.asyncio
async def test_conflict_rejects_the_complete_batch_before_any_put(
    ia_endpoints: ServerState, recovery_client: Client
) -> None:
    """Do not transfer a missing neighbor when another exact name conflicts."""
    existing_item(ia_endpoints)
    ia_endpoints.item_files[IDENTIFIER] = {"conflict.bin": b"old"}
    with pytest.raises(InternetArchiveRecoveryError) as failure:
        await resolve(
            recovery_client.add_files(
                IDENTIFIER,
                [
                    InternetArchiveUploadFile(BytesIO(b"new"), "conflict.bin"),
                    InternetArchiveUploadFile(BytesIO(b"missing"), "missing.bin"),
                ],
                expected_metadata=EXPECTED,
            )
        )
    assert failure.value.phase is InternetArchiveRecoveryPhase.RECONCILIATION
    outcomes = {file.name: file.disposition for file in failure.value.result.files}
    assert outcomes == {
        "conflict.bin": InternetArchiveFileDisposition.CONFLICTING,
        "missing.bin": InternetArchiveFileDisposition.UNATTEMPTED,
    }
    assert not any(request.method == "PUT" for request in ia_endpoints.requests)


@pytest.mark.asyncio
@pytest.mark.parametrize("wait", [False, True])
async def test_acknowledged_file_retains_post_transfer_checksum_conflict(
    ia_endpoints: ServerState, recovery_client: Client, wait: bool
) -> None:
    """Keep acknowledgement and mismatch during inter-file or final verification."""
    existing_item(ia_endpoints)
    empty: dict[str, object] = {
        "metadata": ia_endpoints.item_metadata[IDENTIFIER],
        "files": [],
        "files_count": 0,
    }
    ia_endpoints.item_snapshots[IDENTIFIER] = [
        empty,
        {
            **empty,
            "files": [
                {"name": "file.bin", "size": "4", "md5": "0" * 32, "sha1": "0" * 40}
            ],
            "files_count": 1,
        },
    ]
    files = [InternetArchiveUploadFile(BytesIO(b"data"), "file.bin")]
    if not wait:
        files.append(InternetArchiveUploadFile(BytesIO(b"later"), "later.bin"))
    with pytest.raises(InternetArchiveRecoveryError) as failure:
        await resolve(
            recovery_client.add_files(
                IDENTIFIER, files, expected_metadata=EXPECTED, wait=wait
            )
        )
    error = failure.value
    assert error.phase is InternetArchiveRecoveryPhase.RECONCILIATION
    outcome = error.result.files[0]
    assert outcome.transferred
    assert outcome.etag == '"fixture-etag"'
    assert outcome.disposition is InternetArchiveFileDisposition.CONFLICTING
    assert outcome.checksum_state is InternetArchiveChecksumState.MISMATCH
    assert not error.result.verification_complete
    if not wait:
        assert (
            error.result.files[1].disposition
            is InternetArchiveFileDisposition.UNATTEMPTED
        )
    puts = [request for request in ia_endpoints.requests if request.method == "PUT"]
    assert [request.path for request in puts] == [f"/ia/s3/{IDENTIFIER}/file.bin"]


@pytest.mark.asyncio
async def test_incomplete_checksums_defer_without_writes(
    ia_endpoints: ServerState, recovery_client: Client
) -> None:
    """A missing digest remains unresolved rather than receiving a default."""
    existing_item(ia_endpoints)
    ia_endpoints.item_snapshots[IDENTIFIER] = [
        {
            "metadata": ia_endpoints.item_metadata[IDENTIFIER],
            "files": [
                {
                    "name": "file.bin",
                    "source": "original",
                    "size": "4",
                    "md5": "8d777f385d3dfec8815d20f7496026dc",
                }
            ],
            "files_count": 1,
            "workable_servers": ["fixture"],
        }
    ]
    with pytest.raises(InternetArchiveRecoveryDeferredError) as failure:
        await resolve(
            recovery_client.add_files(
                IDENTIFIER,
                [InternetArchiveUploadFile(BytesIO(b"data"), "file.bin")],
                expected_metadata=EXPECTED,
            )
        )
    assert failure.value.phase is InternetArchiveRecoveryPhase.VERIFICATION
    assert (
        failure.value.result.files[0].disposition
        is InternetArchiveFileDisposition.DEFERRED
    )
    assert not any(request.method == "PUT" for request in ia_endpoints.requests)


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ["uploader", "provenance"])
async def test_ownership_and_provenance_are_exact_write_preconditions(
    ia_endpoints: ServerState, recovery_client: Client, mismatch: str
) -> None:
    """Reject account and marker mismatches instead of substituting credentials."""
    existing_item(ia_endpoints)
    if mismatch == "uploader":
        ia_endpoints.item_metadata[IDENTIFIER]["uploader"] = "other@example.invalid"
    else:
        ia_endpoints.item_metadata[IDENTIFIER]["source"] = (
            "https://example.invalid/exact/"
        )
    with pytest.raises(InternetArchiveRecoveryError) as failure:
        await resolve(
            recovery_client.add_files(
                IDENTIFIER,
                [InternetArchiveUploadFile(BytesIO(b"data"), "file.bin")],
                expected_metadata=EXPECTED,
            )
        )
    assert (
        failure.value.phase
        is {
            "uploader": InternetArchiveRecoveryPhase.OWNERSHIP,
            "provenance": InternetArchiveRecoveryPhase.RECONCILIATION,
        }[mismatch]
    )
    assert not any(request.method == "PUT" for request in ia_endpoints.requests)
    whoami = ia_endpoints.matching("/ia/user", "GET")[0]
    assert whoami.headers["Authorization"] == "LOW test-access:test-secret"
    assert "Cookie" not in whoami.headers


@pytest.mark.asyncio
async def test_recovery_bootstraps_and_pins_cookie_derived_low_keys(
    ia_endpoints: ServerState, recovery_client: Client
) -> None:
    """Use cookies only to resolve a LOW pair, then isolate every recovery read."""
    existing_item(ia_endpoints)
    recovery_client._api_key = None
    recovery_client._cookies = InternetArchiveCookies("user", "signature")
    result = await resolve(
        recovery_client.add_files(
            IDENTIFIER,
            [InternetArchiveUploadFile(BytesIO(b"data"), "file.bin")],
            expected_metadata=EXPECTED,
        )
    )
    assert result.files[0].transferred
    assert len(ia_endpoints.matching("/ia/upload", "GET")) == 1
    whoami = ia_endpoints.matching("/ia/user", "GET")[0]
    assert whoami.headers["Authorization"] == "LOW dummy-access:dummy-secret"
    assert "Cookie" not in whoami.headers


@pytest.mark.asyncio
async def test_recovery_validates_wait_before_network(
    ia_endpoints: ServerState, recovery_client: Client
) -> None:
    """Reject truthy non-booleans before hashing or authentication."""
    with pytest.raises(InvalidOptionError, match="wait"):
        await resolve(
            recovery_client.add_files(
                IDENTIFIER,
                [InternetArchiveUploadFile(BytesIO(b"data"), "file.bin")],
                expected_metadata=EXPECTED,
                wait=cast("Any", 1),
            )
        )
    assert not ia_endpoints.requests


@pytest.mark.asyncio
async def test_item_reads_reject_redirects(
    ia_endpoints: ServerState, recovery_client: Client
) -> None:
    """Do not follow a Metadata API redirect or reinterpret it as a snapshot."""
    ia_endpoints.item_responses["GET", f"/ia/metadata/{IDENTIFIER}"] = (
        302,
        "{}",
        {"Location": f"{ia_endpoints.base_url}/redirect"},
    )
    with pytest.raises(InvalidServiceResponseError):
        await resolve(recovery_client.get_item(IDENTIFIER))
    assert not ia_endpoints.matching("/redirect")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["ownership", "metadata", "catalog", "transfer", "transfer_body"]
)
async def test_recovery_http_failures_retain_phase_and_file(
    ia_endpoints: ServerState, recovery_client: Client, failure: str
) -> None:
    """Retain ingest and transfer HTTP failures without another mutation."""
    existing_item(ia_endpoints)
    method, path, status, body = {
        "ownership": ("GET", "/ia/user", 503, "{}"),
        "metadata": ("GET", f"/ia/metadata/{IDENTIFIER}", 403, "{}"),
        "catalog": ("GET", "/ia/upload-api", 503, "{}"),
        "transfer": ("PUT", f"/ia/s3/{IDENTIFIER}/file.bin", 503, "{}"),
        "transfer_body": (
            "PUT",
            f"/ia/s3/{IDENTIFIER}/file.bin",
            200,
            "unexpected",
        ),
    }[failure]
    ia_endpoints.item_responses[method, path] = (status, body, {})
    with pytest.raises(InternetArchiveRecoveryError) as error:
        await resolve(
            recovery_client.add_files(
                IDENTIFIER,
                [InternetArchiveUploadFile(BytesIO(b"data"), "file.bin")],
                expected_metadata=EXPECTED,
            )
        )
    assert (
        error.value.phase
        is {
            "ownership": InternetArchiveRecoveryPhase.OWNERSHIP,
            "metadata": InternetArchiveRecoveryPhase.RECONCILIATION,
            "catalog": InternetArchiveRecoveryPhase.INGEST,
            "transfer": InternetArchiveRecoveryPhase.TRANSFER,
            "transfer_body": InternetArchiveRecoveryPhase.TRANSFER,
        }[failure]
    )
    assert error.value.failed_file == (
        "file.bin" if failure in {"transfer", "transfer_body"} else None
    )
    outcome = error.value.result.files[0]
    assert not outcome.transferred
    assert outcome.etag is None
    assert outcome.disposition is (
        InternetArchiveFileDisposition.UNCERTAIN
        if method == "PUT"
        else InternetArchiveFileDisposition.UNATTEMPTED
    )
    assert outcome.checksum_state is (
        InternetArchiveChecksumState.PENDING
        if method == "PUT"
        else InternetArchiveChecksumState.UNVERIFIED
    )
    assert error.value.status_code == status
    assert error.value.cause.status_code == status
    assert len(ia_endpoints.matching(path)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(("status", "body"), [(503, "{}"), (200, "unexpected")])
async def test_ambiguous_put_retains_prior_and_unattempted_file_outcomes(
    ia_endpoints: ServerState, recovery_client: Client, status: int, body: str
) -> None:
    """Distinguish the failed second dispatch from the untouched third file."""
    existing_item(ia_endpoints)
    ia_endpoints.item_responses["PUT", f"/ia/s3/{IDENTIFIER}/b.bin"] = (
        status,
        body,
        {},
    )
    with pytest.raises(InternetArchiveRecoveryError) as failure:
        await resolve(
            recovery_client.add_files(
                IDENTIFIER,
                [
                    InternetArchiveUploadFile(BytesIO(name.encode()), name)
                    for name in ("a.bin", "b.bin", "c.bin")
                ],
                expected_metadata=EXPECTED,
            )
        )
    error = failure.value
    first, second, third = error.result.files
    assert error.phase is InternetArchiveRecoveryPhase.TRANSFER
    assert error.failed_file == "b.bin"
    assert error.status_code == error.cause.status_code == status
    assert first.transferred and first.etag == '"fixture-etag"'
    assert first.disposition is InternetArchiveFileDisposition.TRANSFERRED
    assert first.checksum_state is InternetArchiveChecksumState.VERIFIED
    assert not second.transferred and second.etag is None
    assert second.disposition is InternetArchiveFileDisposition.UNCERTAIN
    assert second.checksum_state is InternetArchiveChecksumState.PENDING
    assert not third.transferred and third.etag is None
    assert third.disposition is InternetArchiveFileDisposition.UNATTEMPTED
    assert third.checksum_state is InternetArchiveChecksumState.UNVERIFIED
    puts = [request for request in ia_endpoints.requests if request.method == "PUT"]
    assert [request.path for request in puts] == [
        f"/ia/s3/{IDENTIFIER}/a.bin",
        f"/ia/s3/{IDENTIFIER}/b.bin",
    ]


@pytest.mark.asyncio
async def test_recovery_shared_deadline_can_expire_before_identity_read(
    ia_endpoints: ServerState, recovery_client: Client
) -> None:
    """Report the ownership phase when the single deadline is already exhausted."""
    with pytest.raises(InternetArchiveRecoveryError) as error:
        await resolve(
            recovery_client.add_files(
                IDENTIFIER,
                [InternetArchiveUploadFile(BytesIO(b"data"), "file.bin")],
                expected_metadata=EXPECTED,
                timeout=1e-300,
            )
        )
    assert error.value.phase is InternetArchiveRecoveryPhase.OWNERSHIP
    assert isinstance(error.value.cause, PollingTimeoutError)
    assert not ia_endpoints.requests


@pytest.mark.asyncio
@pytest.mark.parametrize("interruption", ["deadline", "cancel"])
@pytest.mark.parametrize("suspension", ["request", "body"])
@pytest.mark.parametrize(
    ("stage", "phase"),
    [
        ("bootstrap", InternetArchiveRecoveryPhase.OWNERSHIP),
        ("identity", InternetArchiveRecoveryPhase.OWNERSHIP),
        ("metadata", InternetArchiveRecoveryPhase.RECONCILIATION),
        ("catalog", InternetArchiveRecoveryPhase.INGEST),
        ("transfer", InternetArchiveRecoveryPhase.TRANSFER),
        ("verification", InternetArchiveRecoveryPhase.RECONCILIATION),
    ],
)
async def test_async_recovery_interrupts_network_awaits(
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
    phase: InternetArchiveRecoveryPhase,
    suspension: str,
    interruption: str,
) -> None:
    """Bound requests and body reads without converting external cancellation."""
    entered = asyncio.Event()
    never = asyncio.Event()
    owned, borrowed = BytesIO(b"data"), BytesIO(b"later")
    monkeypatch.setattr(Path, "open", lambda *args, **kwargs: owned)

    async def suspend() -> str:
        entered.set()
        await never.wait()
        raise AssertionError("interrupted request resumed")

    class WaitingResponse:
        status_code = 200
        headers: ClassVar[dict[str, str]] = {}

        @property
        def text(self) -> Awaitable[str]:
            return suspend()

        async def json(self) -> object:
            return await suspend()

    responses = [
        ("identity", StubResponse({"success": True, "value": {"username": "owner"}})),
        (
            "metadata",
            StubResponse(
                {
                    "metadata": {
                        "identifier": IDENTIFIER,
                        "uploader": "owner",
                        **EXPECTED,
                    },
                    "files": [],
                }
            ),
        ),
        ("catalog", StubResponse({"success": True, "rows": []})),
        ("transfer", StubResponse(None, headers={"ETag": '"acknowledged"'})),
        ("verification", StubResponse(None)),
    ]
    if stage == "bootstrap":
        responses.insert(0, ("bootstrap", StubResponse(None)))

    class WaitingSession(AsyncSession):
        async def request(
            self, method: str, url: str, **kwargs: object
        ) -> StubResponse:
            current = responses[len(self.requests)][0]
            response = await super().request(method, url, **kwargs)
            if current == stage:
                if suspension == "request":
                    await suspend()
                return cast("StubResponse", WaitingResponse())
            return response

    session = WaitingSession([response for _, response in responses])
    timeout = 0.1 if interruption == "deadline" else 5
    async with AsyncInternetArchiveClient(
        session=as_async_session(session),
        api_key=None if stage == "bootstrap" else InternetArchiveApiKey("a", "s"),
        cookies=InternetArchiveCookies("user", "signature"),
    ) as client:
        task = asyncio.create_task(
            client.add_files(
                IDENTIFIER,
                [Path("file.bin"), InternetArchiveUploadFile(borrowed, "later.bin")],
                expected_metadata=EXPECTED,
                wait=True,
                timeout=timeout,
            )
        )
        try:
            await asyncio.wait_for(entered.wait(), 2)
            if interruption == "cancel":
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                with pytest.raises(InternetArchiveRecoveryError) as failure:
                    await asyncio.wait_for(task, 2)
                error = failure.value
                assert error.phase is phase
                assert isinstance(error.cause, PollingTimeoutError)
                assert error.cause.timeout == timeout
                assert error.cause.job_id == IDENTIFIER
                first, second = error.result.files
                assert first.disposition is {
                    "transfer": InternetArchiveFileDisposition.UNCERTAIN,
                    "verification": InternetArchiveFileDisposition.TRANSFERRED,
                }.get(stage, InternetArchiveFileDisposition.UNATTEMPTED)
                assert first.transferred == (stage == "verification")
                assert first.etag == (
                    '"acknowledged"' if stage == "verification" else None
                )
                assert second.disposition is InternetArchiveFileDisposition.UNATTEMPTED
                assert error.failed_file == (
                    "file.bin" if stage == "transfer" else None
                )
            assert owned.closed
            assert not borrowed.closed
            assert not session.closed
            assert (
                len(session.requests)
                == [name for name, _ in responses].index(stage) + 1
            )
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["deferred", "unresolved_transfer"])
async def test_recovery_deadline_expires_while_waiting_for_safe_reconciliation(
    ia_endpoints: ServerState,
    recovery_client: Client,
    monkeypatch: pytest.MonkeyPatch,
    state: str,
) -> None:
    """Apply the shared deadline to deferred reads and transfer verification."""
    existing_item(ia_endpoints)
    empty: dict[str, object] = {
        "metadata": ia_endpoints.item_metadata[IDENTIFIER],
        "files": [],
        "files_count": 0,
        "workable_servers": ["fixture"],
    }
    files = [InternetArchiveUploadFile(BytesIO(b"data"), "file.bin")]
    if state == "deferred":
        ia_endpoints.item_snapshots[IDENTIFIER] = [
            {
                **empty,
                "files": [
                    {
                        "name": "file.bin",
                        "source": "original",
                        "size": "4",
                        "md5": "8d777f385d3dfec8815d20f7496026dc",
                    }
                ],
                "files_count": 1,
            }
        ]
    else:
        ia_endpoints.item_snapshots[IDENTIFIER] = [empty, empty]
    module = (
        async_module
        if isinstance(recovery_client, AsyncInternetArchiveClient)
        else sync_module
    )
    clock = 0.0
    allow_post_read_check = False
    reconcile = _recovery.reconcile
    calls = 0

    def monotonic() -> float:
        nonlocal allow_post_read_check
        if allow_post_read_check:
            allow_post_read_check = False
            return 0.0
        return clock

    def expire_after_reconciliation(
        *args: object, **kwargs: object
    ) -> _recovery.Reconciliation:
        nonlocal clock, calls, allow_post_read_check
        calls += 1
        try:
            return cast("Any", reconcile)(*args, **kwargs)
        finally:
            if state == "deferred" or calls == TWO_READS:
                clock = 2.0
                # Expire at the next wait, after the async post-read deadline check.
                allow_post_read_check = (
                    isinstance(recovery_client, AsyncInternetArchiveClient)
                    and state == "unresolved_transfer"
                )

    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=monotonic))
    monkeypatch.setattr(_recovery, "reconcile", expire_after_reconciliation)

    with pytest.raises(InternetArchiveRecoveryError) as failure:
        await resolve(
            recovery_client.add_files(
                IDENTIFIER,
                files,
                expected_metadata=EXPECTED,
                wait=True,
                timeout=1,
                poll_interval=0.001,
            )
        )
    assert failure.value.phase is InternetArchiveRecoveryPhase.VERIFICATION
    assert isinstance(failure.value.cause, PollingTimeoutError)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["rewind", "transfer"])
@pytest.mark.parametrize("cancel_during_drain", [False, True])
async def test_async_recovery_deadline_drains_active_source_io(
    monkeypatch: pytest.MonkeyPatch, stage: str, cancel_during_drain: bool
) -> None:
    """Drain source workers on expiry and preserve any later external cancellation."""
    gate = IOGate()
    owned = ObservedStream(gate, "unused")
    borrowed = BytesIO(b"later")
    monkeypatch.setattr(Path, "open", lambda *args, **kwargs: owned)

    class RecoveryStreamingSession(AsyncSession):
        async def request(
            self, method: str, url: str, **kwargs: object
        ) -> StubResponse:
            response = await super().request(method, url, **kwargs)
            if kwargs.get("params") == {
                "name": "catalogRows",
                "identifier": IDENTIFIER,
            }:
                owned.stage = "restore" if stage == "rewind" else "transfer"
            body = kwargs.get("data")
            if isinstance(body, async_module._items.AsyncPreparedFile):
                async for _ in body:
                    pass
            return response

    session = RecoveryStreamingSession(
        [
            StubResponse({"success": True, "value": {"username": "owner"}}),
            StubResponse(
                {
                    "metadata": {
                        "identifier": IDENTIFIER,
                        "uploader": "owner",
                        **EXPECTED,
                    },
                    "files": [],
                }
            ),
            StubResponse({"success": True, "rows": []}),
            StubResponse(None),
        ]
    )
    timeout = 0.1
    async with AsyncInternetArchiveClient(
        session=as_async_session(session), api_key=InternetArchiveApiKey("a", "s")
    ) as client:
        task = asyncio.create_task(
            client.add_files(
                IDENTIFIER,
                [Path("file.bin"), InternetArchiveUploadFile(borrowed, "later.bin")],
                expected_metadata=EXPECTED,
                timeout=timeout,
            )
        )
        try:
            await asyncio.wait_for(gate.entered.wait(), 2)
            await asyncio.sleep(timeout * 2)
            assert not task.done()
            assert not owned.closed
            assert not gate.finished
            if cancel_during_drain:
                for _ in range(2):
                    task.cancel("external cancellation")
                    await heartbeat()
                    assert not task.done()
                    assert not owned.closed
            gate.release.set()
            if cancel_during_drain:
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                with pytest.raises(InternetArchiveRecoveryError) as failure:
                    await task
                error = failure.value
                assert isinstance(error.cause, PollingTimeoutError)
                assert error.phase is InternetArchiveRecoveryPhase.TRANSFER
                assert error.result.files[0].disposition is (
                    InternetArchiveFileDisposition.UNATTEMPTED
                    if stage == "rewind"
                    else InternetArchiveFileDisposition.UNCERTAIN
                )
                assert not error.result.files[0].transferred
                assert (
                    error.result.files[1].disposition
                    is InternetArchiveFileDisposition.UNATTEMPTED
                )
            assert gate.finished and owned.closed
            assert not borrowed.closed
            assert sum(method == "PUT" for method, _, _ in session.requests) == (
                1 if stage == "transfer" else 0
            )
        finally:
            gate.release.set()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_lost_response_is_uncertain_and_new_client_reconciles_without_replay(
    ia_endpoints: ServerState,
    recovery_client: Client,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retain one accepted PUT and reconcile it through a fresh client instance."""
    existing_item(ia_endpoints)
    original = recovery_client._recovery_request
    if isinstance(recovery_client, AsyncInternetArchiveClient):

        async def async_lost_response(
            method: str,
            url: str,
            key: InternetArchiveApiKey,
            **kwargs: object,
        ) -> object:
            response = await cast("Any", original)(method, url, key, **kwargs)
            if method == "PUT":
                raise NetworkError("lost response", service="Internet Archive")
            return response

        monkeypatch.setattr(recovery_client, "_recovery_request", async_lost_response)
    else:

        def sync_lost_response(
            method: str,
            url: str,
            key: InternetArchiveApiKey,
            **kwargs: object,
        ) -> object:
            response = cast("Any", original)(method, url, key, **kwargs)
            if method == "PUT":
                raise NetworkError("lost response", service="Internet Archive")
            return response

        monkeypatch.setattr(recovery_client, "_recovery_request", sync_lost_response)

    with pytest.raises(InternetArchiveRecoveryError) as failure:
        await resolve(
            recovery_client.add_files(
                IDENTIFIER,
                [InternetArchiveUploadFile(BytesIO(b"stored"), "stored.bin")],
                expected_metadata=EXPECTED,
            )
        )
    assert failure.value.failed_file == "stored.bin"
    assert (
        failure.value.result.files[0].disposition
        is InternetArchiveFileDisposition.UNCERTAIN
    )
    puts_before = len(
        [request for request in ia_endpoints.requests if request.method == "PUT"]
    )
    client_type = type(recovery_client)
    fresh = client_type(api_key=InternetArchiveApiKey("test-access", "test-secret"))
    try:
        reconciled = await resolve(
            fresh.add_files(
                IDENTIFIER,
                [InternetArchiveUploadFile(BytesIO(b"stored"), "stored.bin")],
                expected_metadata=EXPECTED,
            )
        )
    finally:
        await resolve(fresh.close())
    assert (
        reconciled.files[0].disposition
        is InternetArchiveFileDisposition.ALREADY_MATCHING
    )
    assert (
        len([request for request in ia_endpoints.requests if request.method == "PUT"])
        == puts_before
    )


class TypeErrorAdapter:
    """Count adapter calls while reproducing niquests' TypeError fallback trigger."""

    max_retries = False

    def __init__(self) -> None:
        """Initialize the mutation call counter."""
        self.calls = 0

    def send(self, request: object, **kwargs: object) -> object:
        """Raise after the one mutation attempt that must not be repeated."""
        self.calls += 1
        raise TypeError("simulated adapter incompatibility after send")

    def close(self) -> None:
        """Satisfy the adapter lifecycle interface."""


def test_sync_recovery_mutation_bypasses_type_error_resend_fallback() -> None:
    """Call a mutation adapter once even though Session.send would retry it."""
    adapter = TypeErrorAdapter()
    with niquests.Session(retries=0) as session:
        session.mount("https://", cast("Any", adapter))
        client = InternetArchiveClient(
            session=session,
            api_key=InternetArchiveApiKey("access", "secret"),
        )
        with pytest.raises(NetworkError):
            client._recovery_request(
                "PUT",
                "https://example.invalid/item/file",
                InternetArchiveApiKey("access", "secret"),
                request_timeout=1,
                mutation=True,
                headers={"Content-Length": "1"},
                data=b"x",
            )
    assert adapter.calls == 1


def test_sync_recovery_ignores_contaminated_session_identity(
    ia_endpoints: ServerState,
) -> None:
    """Do not merge session cookies, auth, hooks, or metadata headers into recovery."""
    existing_item(ia_endpoints)
    hook_calls: list[object] = []
    with niquests.Session(retries=0) as session:
        session.headers.update(
            {
                "Authorization": "LOW wrong:wrong",
                "Cookie": "logged-in-user=wrong; logged-in-sig=wrong",
                "x-archive-meta-source": "wrong",
            }
        )
        session.auth = ("wrong", "wrong")
        session.hooks["pre_request"].append(hook_calls.append)
        client = InternetArchiveClient(
            session=session,
            api_key=InternetArchiveApiKey("test-access", "test-secret"),
        )
        result = client.add_files(
            IDENTIFIER,
            [InternetArchiveUploadFile(BytesIO(b"safe"), "safe.bin")],
            expected_metadata=EXPECTED,
        )
    assert result.files[0].transferred
    assert hook_calls == []
    for request in ia_endpoints.requests:
        assert request.headers.get("Authorization") == "LOW test-access:test-secret"
        assert "Cookie" not in request.headers
        assert "x-archive-meta-source" not in request.headers
