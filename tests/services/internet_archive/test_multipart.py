"""Exercise resumable IAS3 multipart uploads through both HTTP clients."""

from __future__ import annotations

import json
import pickle
import time
from collections.abc import AsyncIterator, Awaitable
from dataclasses import replace
from hashlib import md5, sha1
from inspect import isawaitable
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar, cast

import pytest
import pytest_asyncio

from archivist import (
    AsyncInternetArchiveClient,
    InternetArchiveApiKey,
    InternetArchiveClient,
    InternetArchiveItemFileSource,
    InternetArchiveMultipartError,
    InternetArchiveMultipartPart,
    InternetArchiveMultipartPhase,
    InternetArchiveMultipartResult,
    InternetArchiveMultipartState,
    InternetArchiveUploadFile,
    InvalidOptionError,
    InvalidServiceResponseError,
    NetworkError,
    PollingTimeoutError,
    ServiceError,
)
from archivist.services.internet_archive import _multipart, _recovery
from tests.conftest import quoted_md5, s3_error
from tests.services.internet_archive.test_item_clients import options

if TYPE_CHECKING:
    from tests.conftest import RecordedRequest, ServerState

T = TypeVar("T")
Client = InternetArchiveClient | AsyncInternetArchiveClient
DATA = b"0123456789abcdefghij"
PART = 8
NAME = "file.bin"
ID = "fixture-item"
EXISTING = "existing-item"
EXPECTED = {"source": "https://example.invalid/exact"}
OBJECT = f"/ia/s3/{ID}/{NAME}"
EXISTING_OBJECT = f"/ia/s3/{EXISTING}/{NAME}"
SLOW = (503, s3_error("SlowDown"), {"Retry-After": "5"})
FAST: dict[str, Any] = {"timeout": 5.0, "poll_interval": 0.01}
STATES_PER_UPLOAD = 6
THREE_PARTS = 3


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


def source(tmp_path: Path, data: bytes = DATA) -> Path:
    """Write a source file named like the uploaded object."""
    path = tmp_path / NAME
    path.write_bytes(data)
    return path


def existing_item(state: ServerState) -> None:
    """Create an owned item whose catalog is ready for new files."""
    state.item_buckets.add(EXISTING)
    state.item_metadata[EXISTING] = {
        "identifier": EXISTING,
        "uploader": "account@example.invalid",
        **EXPECTED,
    }
    state.status_calls[f"catalog:{EXISTING}"] = 1


def lower(request: RecordedRequest) -> dict[str, str]:
    """Return request headers with lowercase names."""
    return {key.lower(): value for key, value in request.headers.items()}


def part_puts(state: ServerState, path: str = OBJECT) -> list[int]:
    """Return part numbers sent to one object, in request order."""
    return [
        int(request.query["partNumber"][0])
        for request in state.matching(path, "PUT")
        if "partNumber" in request.query
    ]


def state_for(**changes: Any) -> InternetArchiveMultipartState:  # noqa: ANN401
    """Return a valid state for DATA split into 8-byte parts."""
    values: dict[str, Any] = {
        "identifier": ID,
        "name": NAME,
        "upload_id": "upload-x",
        "size": len(DATA),
        "part_size": PART,
        "md5": md5(DATA).hexdigest(),
        "sha1": sha1(DATA).hexdigest(),
    }
    values.update(changes)
    return InternetArchiveMultipartState(**values)


def all_parts() -> tuple[InternetArchiveMultipartPart, ...]:
    """Return acknowledged parts matching DATA."""
    return tuple(
        InternetArchiveMultipartPart(
            number, quoted_md5(DATA[(number - 1) * PART : number * PART]), size
        )
        for number, size in ((1, 8), (2, 8), (3, 4))
    )


async def fail_upload(
    client: Client,
    path: Path,
    **kwargs: Any,  # noqa: ANN401
) -> InternetArchiveMultipartError:
    """Run a new-item upload expected to fail and return its error."""
    with pytest.raises(InternetArchiveMultipartError) as caught:
        await resolve(
            client.upload_multipart(path, options(), part_size=PART, **FAST, **kwargs)
        )
    return caught.value


@pytest.mark.asyncio
async def test_new_item_happy_path(
    ia_endpoints: ServerState, client: Client, tmp_path: Path
) -> None:
    """Claim, initiate, send each part, and complete with quoted ETags."""
    states: list[InternetArchiveMultipartState] = []
    result = await resolve(
        client.upload_multipart(
            source(tmp_path), options(), part_size=PART, on_state=states.append
        )
    )
    assert isinstance(result, InternetArchiveMultipartResult)
    assert (result.identifier, result.name, result.size) == (ID, NAME, len(DATA))
    assert (result.md5, result.sha1) == (md5(DATA).hexdigest(), sha1(DATA).hexdigest())
    assert not result.verified
    assert result.details_url == f"https://archive.org/details/{ID}"
    assert result.state is not None
    assert result.state.phase is InternetArchiveMultipartPhase.COMPLETED
    assert result.state.parts == all_parts()
    assert ia_endpoints.item_files[ID][NAME] == DATA
    assert [state.phase for state in states] == [
        InternetArchiveMultipartPhase.INITIATED
    ] * 4 + [
        InternetArchiveMultipartPhase.COMPLETING,
        InternetArchiveMultipartPhase.COMPLETED,
    ]
    assert [len(state.parts) for state in states[:4]] == [0, 1, 2, 3]
    create = ia_endpoints.matching(f"/ia/s3/{ID}", "PUT")[0]
    assert lower(create)["x-archive-queue-derive"] == "0"
    assert lower(create)["x-archive-meta-title"].startswith("uri(")
    initiate, complete = ia_endpoints.matching(OBJECT, "POST")
    assert "uploads" in initiate.query
    assert lower(initiate)["x-archive-size-hint"] == str(len(DATA))
    assert lower(initiate)["x-archive-interactive-priority"] == "1"
    assert lower(initiate)["authorization"] == "LOW test-access:test-secret"
    assert part_puts(ia_endpoints) == [1, 2, 3]
    assert complete.query["uploadId"] == ["upload-1"]
    assert "x-archive-queue-derive" not in lower(complete)
    assert complete.body.decode() == (
        "<CompleteMultipartUpload>"
        + "".join(
            f"<Part><PartNumber>{part.number}</PartNumber>"
            f"<ETag>{part.etag}</ETag></Part>"
            for part in all_parts()
        )
        + "</CompleteMultipartUpload>"
    )


@pytest.mark.asyncio
async def test_wait_verifies_and_queue_derive_is_sent_on_completion(
    ia_endpoints: ServerState, client: Client, tmp_path: Path
) -> None:
    """Send the derive choice on completion and verify assembly in metadata."""
    ia_endpoints.item_metadata[ID] = {"identifier": ID}
    result = await resolve(
        client.upload_multipart(
            source(tmp_path),
            replace(options(), queue_derive=False),
            part_size=PART,
            wait=True,
            **FAST,
        )
    )
    assert result.verified
    assert result.state is not None
    assert not result.state.queue_derive
    complete = ia_endpoints.matching(OBJECT, "POST")[-1]
    assert lower(complete)["x-archive-queue-derive"] == "0"


def priorities(state: ServerState) -> list[str | None]:
    """Return the interactive-priority header of every mutation, in order."""
    return [
        lower(request).get("x-archive-interactive-priority")
        for request in state.requests
        if request.method in {"PUT", "POST", "DELETE"} and "/ia/s3/" in request.path
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("interactive_priority", [True, False])
async def test_new_item_interactive_priority_applies_to_every_mutation(
    ia_endpoints: ServerState,
    client: Client,
    tmp_path: Path,
    interactive_priority: bool,
) -> None:
    """Create, initiate, parts, completion, and a resume all share one setting."""
    ia_endpoints.queued_responses["PUT", OBJECT] = [SLOW]
    upload_options = replace(options(), interactive_priority=interactive_priority)
    path = source(tmp_path)
    with pytest.raises(InternetArchiveMultipartError) as caught:
        await resolve(client.upload_multipart(path, upload_options, part_size=PART))
    saved = caught.value.state
    assert saved is not None
    assert saved.interactive_priority is interactive_priority
    restored = InternetArchiveMultipartState.from_dict(
        json.loads(json.dumps(saved.to_dict()))
    )
    result = await resolve(
        client.upload_multipart(path, upload_options, state=restored)
    )
    assert result.state is not None
    assert result.state.phase is InternetArchiveMultipartPhase.COMPLETED
    # Create, initiate, the failed part, three parts, and completion.
    sent = priorities(ia_endpoints)
    assert len(sent) == 1 + 1 + 1 + THREE_PARTS + 1
    assert sent == ["1" if interactive_priority else None] * len(sent)


@pytest.mark.asyncio
@pytest.mark.parametrize("interactive_priority", [True, False])
async def test_existing_item_and_abort_keep_the_saved_priority(
    ia_endpoints: ServerState,
    client: Client,
    tmp_path: Path,
    interactive_priority: bool,
) -> None:
    """add_file_multipart saves the setting, and abort reuses it."""
    existing_item(ia_endpoints)
    ia_endpoints.queued_responses["PUT", EXISTING_OBJECT] = [SLOW]
    with pytest.raises(InternetArchiveMultipartError) as caught:
        await resolve(
            client.add_file_multipart(
                EXISTING,
                source(tmp_path),
                expected_metadata=EXPECTED,
                part_size=PART,
                interactive_priority=interactive_priority,
            )
        )
    saved = caught.value.state
    assert saved is not None
    assert saved.interactive_priority is interactive_priority
    await resolve(client.abort_multipart(saved))
    assert ia_endpoints.matching(EXISTING_OBJECT, "DELETE")
    # Initiate, the failed part, and the abort.
    sent = priorities(ia_endpoints)
    assert len(sent) == 1 + 1 + 1
    assert sent == ["1" if interactive_priority else None] * len(sent)


@pytest.mark.asyncio
async def test_new_item_waits_out_missing_bucket(
    ia_endpoints: ServerState, client: Client, tmp_path: Path
) -> None:
    """Retry only the initiate while IAS3 still reports NoSuchBucket."""
    ia_endpoints.missing_bucket_initiates[ID] = 2
    await resolve(
        client.upload_multipart(source(tmp_path), options(), part_size=PART, **FAST)
    )
    initiates = [
        r for r in ia_endpoints.matching(OBJECT, "POST") if "uploads" in r.query
    ]
    assert len(initiates) == THREE_PARTS
    assert len(ia_endpoints.matching(f"/ia/s3/{ID}", "PUT")) == 1


@pytest.mark.asyncio
async def test_missing_bucket_until_deadline(
    ia_endpoints: ServerState, client: Client, tmp_path: Path
) -> None:
    """Stop waiting for the new item at the deadline, without an upload ID."""
    ia_endpoints.missing_bucket_initiates[ID] = 10_000
    with pytest.raises(InternetArchiveMultipartError) as caught:
        await resolve(
            client.upload_multipart(
                source(tmp_path),
                options(),
                part_size=PART,
                timeout=0.05,
                poll_interval=0.01,
            )
        )
    assert caught.value.state is None
    assert isinstance(caught.value.cause, PollingTimeoutError)


@pytest.mark.asyncio
async def test_existing_identifier_is_a_conflict(
    ia_endpoints: ServerState, client: Client, tmp_path: Path
) -> None:
    """A 409 on the create-only PUT stops before any initiate."""
    ia_endpoints.item_buckets.add(ID)
    error = await fail_upload(client, source(tmp_path))
    assert error.state is None
    assert error.cause.status_code == 409  # noqa: PLR2004
    assert error.cause.request_phase == "item-create"
    assert not ia_endpoints.matching(OBJECT)


@pytest.mark.asyncio
async def test_unexpected_create_body_is_rejected(
    ia_endpoints: ServerState, client: Client, tmp_path: Path
) -> None:
    """Item creation needs an empty 200 acknowledgement."""
    ia_endpoints.queued_responses["PUT", f"/ia/s3/{ID}"] = [(200, "surprise", {})]
    error = await fail_upload(client, source(tmp_path))
    assert isinstance(error.cause, InvalidServiceResponseError)


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["initiate", "part", "complete"])
async def test_slow_down_retains_diagnostics_and_state(
    ia_endpoints: ServerState, client: Client, tmp_path: Path, method: str
) -> None:
    """SlowDown keeps its code, retry hint, phase, and the latest state."""
    key = ("PUT" if method == "part" else "POST", OBJECT)
    queue: list[tuple[int, str, dict[str, str]] | None] = (
        [SLOW] if method == "initiate" else [None, SLOW]
    )
    ia_endpoints.queued_responses[key] = queue
    error = await fail_upload(client, source(tmp_path))
    cause = error.cause
    assert (cause.status_code, cause.service_code) == (503, "SlowDown")
    assert cause.retry_after == 5  # noqa: PLR2004
    assert cause.request_phase == f"multipart-{method}"
    if method == "initiate":
        assert error.state is None
    elif method == "part":
        assert error.state is not None
        assert [part.number for part in error.state.parts] == [1]
    else:
        assert error.state is not None
        assert error.state.phase is InternetArchiveMultipartPhase.COMPLETING
    assert not error.uncertain


@pytest.mark.asyncio
async def test_resume_after_interruption_between_parts(
    ia_endpoints: ServerState, client: Client, tmp_path: Path
) -> None:
    """Resume lists parts and sends only the unacknowledged ones."""
    path = source(tmp_path)
    ia_endpoints.queued_responses["PUT", OBJECT] = [None, SLOW]
    error = await fail_upload(client, path)
    assert error.state is not None
    states: list[InternetArchiveMultipartState] = []
    result = await resolve(
        client.upload_multipart(
            path, options(), state=error.state, on_state=states.append
        )
    )
    assert result.state is not None
    assert result.state.phase is InternetArchiveMultipartPhase.COMPLETED
    assert part_puts(ia_endpoints) == [1, 2, 2, 3]
    assert len(ia_endpoints.matching(OBJECT, "GET")) == 1
    assert len(ia_endpoints.matching(f"/ia/s3/{ID}", "PUT")) == 1
    assert ia_endpoints.item_files[ID][NAME] == DATA
    assert [len(state.parts) for state in states[:2]] == [2, 3]


@pytest.mark.asyncio
async def test_resume_accepts_a_part_whose_ack_was_lost(
    ia_endpoints: ServerState, client: Client, tmp_path: Path
) -> None:
    """A listed part matching the source counts even if never acknowledged."""
    path = source(tmp_path)
    states: list[InternetArchiveMultipartState] = []
    ia_endpoints.queued_responses["PUT", OBJECT] = [None, None, SLOW]
    with pytest.raises(InternetArchiveMultipartError):
        await resolve(
            client.upload_multipart(
                path, options(), part_size=PART, on_state=states.append, **FAST
            )
        )
    stale = states[1]
    assert [part.number for part in stale.parts] == [1]
    result = await resolve(client.upload_multipart(path, options(), state=stale))
    assert result.state is not None
    assert part_puts(ia_endpoints) == [1, 2, 3, 3]


@pytest.mark.asyncio
async def test_resume_resends_a_listed_part_that_does_not_match(
    ia_endpoints: ServerState, client: Client, tmp_path: Path
) -> None:
    """A listed part with the wrong ETag or size is uploaded again."""
    path = source(tmp_path)
    ia_endpoints.queued_responses["PUT", OBJECT] = [None, None, SLOW]
    error = await fail_upload(client, path)
    assert error.state is not None
    ia_endpoints.multipart_uploads["upload-1"].parts[2] = b"corrupt!"
    await resolve(client.upload_multipart(path, options(), state=error.state))
    assert part_puts(ia_endpoints) == [1, 2, 3, 2, 3]
    assert ia_endpoints.item_files[ID][NAME] == DATA


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        (200, "", {"ETag": '"00000000000000000000000000000000"'}),
        (200, "", {}),
        (200, "unexpected", {"ETag": quoted_md5(DATA[:PART])}),
    ],
    ids=["wrong-etag", "missing-etag", "body"],
)
async def test_unconfirmed_part_is_not_acknowledged(
    ia_endpoints: ServerState,
    client: Client,
    tmp_path: Path,
    response: tuple[int, str, dict[str, str]],
) -> None:
    """Only an empty 200 whose ETag is the local MD5 acknowledges a part."""
    ia_endpoints.queued_responses["PUT", OBJECT] = [response]
    error = await fail_upload(client, source(tmp_path))
    assert isinstance(error.cause, InvalidServiceResponseError)
    assert error.cause.request_phase == "multipart-part"
    assert error.state is not None
    assert error.state.parts == ()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        (400, s3_error("InvalidPart"), {}),
        (204, "", {}),
        (200, "<Unexpected/>", {}),
    ],
    ids=["invalid-part", "no-content", "wrong-xml"],
)
async def test_failed_completion_is_resent_while_the_upload_is_open(
    ia_endpoints: ServerState,
    client: Client,
    tmp_path: Path,
    response: tuple[int, str, dict[str, str]],
) -> None:
    """ListParts 200 after a failed completion makes sending it again safe."""
    path = source(tmp_path)
    ia_endpoints.queued_responses["POST", OBJECT] = [None, response]
    error = await fail_upload(client, path)
    assert error.state is not None
    assert error.state.phase is InternetArchiveMultipartPhase.COMPLETING
    result = await resolve(client.upload_multipart(path, options(), state=error.state))
    assert result.state is not None
    assert result.state.phase is InternetArchiveMultipartPhase.COMPLETED
    assert part_puts(ia_endpoints) == [1, 2, 3]
    assert ia_endpoints.item_files[ID][NAME] == DATA


async def completed_behind_our_back(
    ia_endpoints: ServerState, client: Client, path: Path
) -> InternetArchiveMultipartState:
    """Lose a completion response after the server accepted the completion."""
    ia_endpoints.queued_responses["POST", OBJECT] = [None, SLOW]
    error = await fail_upload(client, path)
    assert error.state is not None
    del ia_endpoints.multipart_uploads["upload-1"]
    return error.state


@pytest.mark.asyncio
@pytest.mark.parametrize("evidence", ["file", "task", "file-mismatch-task"])
async def test_lost_completion_is_confirmed_by_evidence(
    ia_endpoints: ServerState, client: Client, tmp_path: Path, evidence: str
) -> None:
    """NoSuchUpload after a completion resolves through metadata or tasks."""
    path = source(tmp_path)
    state = await completed_behind_our_back(ia_endpoints, client, path)
    ia_endpoints.completed_uploads.append((ID, "upload-1"))
    if evidence != "task":
        ia_endpoints.item_metadata[ID] = {"identifier": ID}
        ia_endpoints.item_files.setdefault(ID, {})[NAME] = (
            DATA if evidence == "file" else b"other"
        )
    states: list[InternetArchiveMultipartState] = []
    result = await resolve(
        client.upload_multipart(path, options(), state=state, on_state=states.append)
    )
    assert result.verified is (evidence == "file")
    assert result.state is not None
    assert result.state.phase is InternetArchiveMultipartPhase.COMPLETED
    assert [s.phase for s in states] == [InternetArchiveMultipartPhase.COMPLETED]
    assert bool(ia_endpoints.matching("/ia/tasks")) is (evidence != "file")


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", ["none", "failed", "other"])
async def test_lost_completion_without_evidence_is_uncertain(
    ia_endpoints: ServerState, client: Client, tmp_path: Path, rows: str
) -> None:
    """With no file and no live completion task, report an uncertain outcome."""
    path = source(tmp_path)
    state = await completed_behind_our_back(ia_endpoints, client, path)
    if rows != "none":
        row = {
            "status": "error" if rows == "failed" else "queued",
            "args": {
                "comment": "s3-put-complete-multipart",
                "complete_multipart_upload_id": (
                    "upload-1" if rows == "failed" else "other"
                ),
            },
        }
        ia_endpoints.item_responses["GET", "/ia/tasks"] = (
            200,
            json.dumps({"success": True, "value": {"catalog": [row], "history": None}}),
            {},
        )
    with pytest.raises(InternetArchiveMultipartError) as caught:
        await resolve(client.upload_multipart(path, options(), state=state))
    assert caught.value.uncertain
    assert caught.value.state == state
    assert caught.value.cause.service_code == "NoSuchUpload"
    assert caught.value.cause.request_phase == "multipart-list"


@pytest.mark.asyncio
async def test_resume_of_vanished_open_upload_fails_without_evidence_checks(
    ia_endpoints: ServerState, client: Client, tmp_path: Path
) -> None:
    """NoSuchUpload while parts are still being sent is reported, not resolved."""
    with pytest.raises(InternetArchiveMultipartError) as caught:
        await resolve(
            client.upload_multipart(source(tmp_path), options(), state=state_for())
        )
    assert not caught.value.uncertain
    assert caught.value.cause.service_code == "NoSuchUpload"
    assert not ia_endpoints.matching("/ia/tasks")


@pytest.mark.asyncio
async def test_list_failure_while_completing_is_not_uncertain(
    ia_endpoints: ServerState, client: Client, tmp_path: Path
) -> None:
    """Only NoSuchUpload triggers completion reconciliation."""
    ia_endpoints.queued_responses["GET", OBJECT] = [SLOW]
    state = state_for(phase=InternetArchiveMultipartPhase.COMPLETING, parts=all_parts())
    with pytest.raises(InternetArchiveMultipartError) as caught:
        await resolve(client.upload_multipart(source(tmp_path), options(), state=state))
    assert not caught.value.uncertain
    assert caught.value.cause.request_phase == "multipart-list"


@pytest.mark.asyncio
async def test_evidence_read_failure_keeps_the_state_resumable(
    ia_endpoints: ServerState, client: Client, tmp_path: Path
) -> None:
    """A failed evidence read leaves the completing state for a later resume."""
    state = state_for(phase=InternetArchiveMultipartPhase.COMPLETING, parts=all_parts())
    ia_endpoints.item_responses["GET", f"/ia/metadata/{ID}"] = (503, "{}", {})
    with pytest.raises(InternetArchiveMultipartError) as caught:
        await resolve(client.upload_multipart(source(tmp_path), options(), state=state))
    assert not caught.value.uncertain
    assert caught.value.state == state
    assert caught.value.cause.request_phase == "item-metadata"


@pytest.mark.asyncio
async def test_completed_state_only_verifies(
    ia_endpoints: ServerState, client: Client, tmp_path: Path
) -> None:
    """A completed state sends nothing; with wait it reads metadata only."""
    state = state_for(phase=InternetArchiveMultipartPhase.COMPLETED, parts=all_parts())
    path = source(tmp_path)
    result = await resolve(client.upload_multipart(path, options(), state=state))
    assert result.state == state
    assert not result.verified
    assert not ia_endpoints.requests
    ia_endpoints.item_metadata[ID] = {"identifier": ID}
    ia_endpoints.item_files[ID] = {NAME: DATA}
    result = await resolve(
        client.upload_multipart(path, options(), state=state, wait=True, **FAST)
    )
    assert result.verified
    assert {request.path for request in ia_endpoints.requests} == {f"/ia/metadata/{ID}"}


@pytest.mark.asyncio
@pytest.mark.parametrize("stored", [b"other", None])
async def test_verification_failures(
    ia_endpoints: ServerState, client: Client, tmp_path: Path, stored: bytes | None
) -> None:
    """A conflicting file fails at once; a missing file waits until the deadline."""
    state = state_for(phase=InternetArchiveMultipartPhase.COMPLETED, parts=all_parts())
    ia_endpoints.item_metadata[ID] = {"identifier": ID}
    if stored is not None:
        ia_endpoints.item_files[ID] = {NAME: stored}
    with pytest.raises(InternetArchiveMultipartError) as caught:
        await resolve(
            client.upload_multipart(
                source(tmp_path),
                options(),
                state=state,
                wait=True,
                timeout=FAST["timeout"] if stored else 0.05,
                poll_interval=0.01,
            )
        )
    expected = InvalidServiceResponseError if stored else PollingTimeoutError
    assert type(caught.value.cause) is expected


@pytest.mark.asyncio
async def test_existing_item_upload_and_verification(
    ia_endpoints: ServerState, client: Client, tmp_path: Path
) -> None:
    """add_file_multipart checks ownership, then uploads without claiming."""
    existing_item(ia_endpoints)
    ia_endpoints.status_calls.pop(f"catalog:{EXISTING}")
    result = await resolve(
        client.add_file_multipart(
            EXISTING,
            InternetArchiveUploadFile(BytesIO(DATA), NAME),
            expected_metadata=EXPECTED,
            part_size=PART,
            queue_derive=False,
            wait=True,
            **FAST,
        )
    )
    assert result.verified
    assert result.state is not None
    assert not result.state.queue_derive
    assert not ia_endpoints.matching(f"/ia/s3/{EXISTING}", "PUT")
    assert part_puts(ia_endpoints, EXISTING_OBJECT) == [1, 2, 3]
    complete = ia_endpoints.matching(EXISTING_OBJECT, "POST")[-1]
    assert lower(complete)["x-archive-queue-derive"] == "0"
    assert ia_endpoints.matching("/ia/user")[0].query == {"op": ["whoami"]}


@pytest.mark.asyncio
async def test_existing_item_with_matching_file_uploads_nothing(
    ia_endpoints: ServerState, client: Client, tmp_path: Path
) -> None:
    """An identical file already present returns a verified result."""
    existing_item(ia_endpoints)
    ia_endpoints.item_files[EXISTING] = {NAME: DATA}
    result = await resolve(
        client.add_file_multipart(
            EXISTING, source(tmp_path), expected_metadata=EXPECTED, part_size=PART
        )
    )
    assert result.state is None
    assert result.verified
    assert not ia_endpoints.matching(EXISTING_OBJECT)


@pytest.mark.asyncio
@pytest.mark.parametrize("problem", ["conflict", "provenance", "deferred"])
async def test_existing_item_refusals(
    ia_endpoints: ServerState, client: Client, tmp_path: Path, problem: str
) -> None:
    """Conflicts and provenance mismatches stop; deferral waits for the deadline."""
    existing_item(ia_endpoints)
    expected = EXPECTED
    if problem == "conflict":
        ia_endpoints.item_files[EXISTING] = {NAME: b"different"}
    elif problem == "provenance":
        expected = {"source": "https://example.invalid/other"}
    else:
        ia_endpoints.item_snapshots[EXISTING] = [
            {"metadata": ia_endpoints.item_metadata[EXISTING], "pending_creation": True}
        ]
    with pytest.raises(InternetArchiveMultipartError) as caught:
        await resolve(
            client.add_file_multipart(
                EXISTING,
                source(tmp_path),
                expected_metadata=expected,
                part_size=PART,
                # Only deferral should reach the deadline; the others must stop
                # on their own, not because a slow runner ran out of time.
                timeout=0.05 if problem == "deferred" else FAST["timeout"],
                poll_interval=0.01,
            )
        )
    assert caught.value.state is None
    expected_cause = PollingTimeoutError if problem == "deferred" else ServiceError
    assert type(caught.value.cause) is expected_cause
    assert not ia_endpoints.matching(EXISTING_OBJECT)


@pytest.mark.parametrize(
    ("cause_type", "remaining", "expected"),
    [
        ("ReadTimeout", 1.0, PollingTimeoutError),
        ("ConnectTimeout", 1.0, PollingTimeoutError),
        ("ReadTimeout", 60.0, NetworkError),
        ("ConnectionError", 1.0, NetworkError),
        (None, 1.0, NetworkError),
    ],
)
def test_sync_deadline_capped_request_timeout_is_a_polling_timeout(
    cause_type: str | None, remaining: float, expected: type[ServiceError]
) -> None:
    """Only a timeout caused by the deadline's cap ends the wait, as in async."""
    client = InternetArchiveClient(timeout=30)
    sent: list[float] = []

    def send(request_timeout: float) -> None:
        sent.append(request_timeout)
        raise NetworkError("network", cause_type=cause_type)

    with pytest.raises(ServiceError) as caught:
        client._within_deadline(time.monotonic() + remaining, ID, remaining, send)
    assert type(caught.value) is expected
    assert 0 < sent[0] <= remaining
    client.close()


@pytest.mark.asyncio
async def test_existing_item_does_not_wait_out_missing_bucket(
    ia_endpoints: ServerState, client: Client, tmp_path: Path
) -> None:
    """NoSuchBucket on an existing item is an error, not a creation delay."""
    existing_item(ia_endpoints)
    ia_endpoints.missing_bucket_initiates[EXISTING] = 1
    with pytest.raises(InternetArchiveMultipartError) as caught:
        await resolve(
            client.add_file_multipart(
                EXISTING, source(tmp_path), expected_metadata=EXPECTED, part_size=PART
            )
        )
    assert caught.value.cause.service_code == "NoSuchBucket"
    assert len(ia_endpoints.matching(EXISTING_OBJECT, "POST")) == 1


@pytest.mark.asyncio
async def test_existing_item_resume(
    ia_endpoints: ServerState, client: Client, tmp_path: Path
) -> None:
    """A resumed add_file_multipart trusts the upload ID and skips ownership reads."""
    existing_item(ia_endpoints)
    path = source(tmp_path)
    ia_endpoints.queued_responses["PUT", EXISTING_OBJECT] = [SLOW]
    with pytest.raises(InternetArchiveMultipartError) as caught:
        await resolve(
            client.add_file_multipart(
                EXISTING, path, expected_metadata=EXPECTED, part_size=PART
            )
        )
    reads = len(ia_endpoints.matching("/ia/user"))
    result = await resolve(
        client.add_file_multipart(
            EXISTING, path, expected_metadata=EXPECTED, state=caught.value.state
        )
    )
    assert result.state is not None
    assert len(ia_endpoints.matching("/ia/user")) == reads


@pytest.mark.asyncio
async def test_async_on_state_may_be_awaitable(
    ia_endpoints: ServerState, tmp_path: Path
) -> None:
    """The async client awaits an awaitable returned by on_state."""
    seen: list[InternetArchiveMultipartPhase] = []

    async def persist(state: InternetArchiveMultipartState) -> None:
        seen.append(state.phase)

    async with AsyncInternetArchiveClient(
        api_key=InternetArchiveApiKey("test-access", "test-secret")
    ) as client:
        await client.upload_multipart(
            source(tmp_path), options(), part_size=PART, on_state=persist
        )
    assert len(seen) == STATES_PER_UPLOAD


@pytest.mark.asyncio
async def test_abort(ia_endpoints: ServerState, client: Client, tmp_path: Path) -> None:
    """Abort deletes an open upload; a second abort reports NoSuchUpload."""
    ia_endpoints.queued_responses["PUT", OBJECT] = [SLOW]
    error = await fail_upload(client, source(tmp_path))
    assert error.state is not None
    await resolve(client.abort_multipart(error.state))
    assert not ia_endpoints.multipart_uploads
    deletes = ia_endpoints.matching(OBJECT, "DELETE")
    assert deletes[0].query == {"uploadId": ["upload-1"]}
    assert lower(deletes[0])["x-archive-interactive-priority"] == "1"
    with pytest.raises(InternetArchiveMultipartError) as caught:
        await resolve(client.abort_multipart(error.state))
    assert caught.value.cause.service_code == "NoSuchUpload"
    assert caught.value.cause.request_phase == "multipart-abort"


@pytest.mark.asyncio
async def test_abort_requires_an_empty_acknowledgement(
    ia_endpoints: ServerState, client: Client
) -> None:
    """An abort answered with an unexpected status or body is not confirmed."""
    ia_endpoints.queued_responses["DELETE", OBJECT] = [(202, "", {}), (200, "x", {})]
    for _ in range(2):
        with pytest.raises(InternetArchiveMultipartError) as caught:
            await resolve(client.abort_multipart(state_for()))
        assert isinstance(caught.value.cause, InvalidServiceResponseError)


@pytest.mark.asyncio
async def test_abort_rejects_invalid_states(client: Client) -> None:
    """Abort needs an open upload's state."""
    with pytest.raises(InvalidOptionError, match="InternetArchiveMultipartState"):
        await resolve(client.abort_multipart(cast("Any", {})))
    completed = state_for(
        phase=InternetArchiveMultipartPhase.COMPLETED, parts=all_parts()
    )
    with pytest.raises(InvalidOptionError, match="cannot be aborted"):
        await resolve(client.abort_multipart(completed))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"options": cast("Any", {})}, "options must be"),
        ({"state": cast("Any", {})}, "state must be"),
        ({"on_state": cast("Any", 1)}, "on_state must be"),
        ({"wait": cast("Any", 1)}, "wait must be"),
        ({"timeout": 0}, "timeout"),
        ({"part_size": 0}, "part_size must be"),
        ({"part_size": True}, "part_size must be"),
        ({"part_size": "8"}, "part_size must be"),
        ({"part_size": _multipart.MAX_PART_SIZE + 1}, "part_size must be"),
        ({"part_size": 1, "data": b"x" * 1001}, "at most 1000 parts"),
        ({"file": cast("Any", b"bytes")}, "file must be"),
        ({"name": "__ia_spool/part.bin"}, "spool"),
        ({"state": state_for(identifier="other-item")}, "different item"),
        ({"state": state_for(name="other.bin")}, "different item"),
        ({"state": state_for(md5="0" * 32)}, "source changed"),
    ],
)
async def test_upload_validation_happens_before_network(
    ia_endpoints: ServerState,
    client: Client,
    tmp_path: Path,
    changes: dict[str, Any],
    message: str,
) -> None:
    """Reject invalid arguments and changed sources without any request."""
    changes = dict(changes)
    data = changes.pop("data", DATA)
    name = changes.pop("name", NAME)
    arguments: dict[str, Any] = {
        "file": InternetArchiveUploadFile(BytesIO(data), name),
        "options": options(),
        "part_size": PART,
    }
    arguments.update(changes)
    with pytest.raises(InvalidOptionError, match=message):
        await resolve(client.upload_multipart(**arguments))
    assert not ia_endpoints.requests


@pytest.mark.asyncio
async def test_add_file_validation_happens_before_network(
    ia_endpoints: ServerState, client: Client, tmp_path: Path
) -> None:
    """add_file_multipart validates its own options before any request."""
    path = source(tmp_path)
    with pytest.raises(InvalidOptionError, match="queue_derive"):
        await resolve(
            client.add_file_multipart(
                EXISTING,
                path,
                expected_metadata=EXPECTED,
                queue_derive=cast("Any", 1),
            )
        )
    with pytest.raises(InvalidOptionError, match="interactive_priority"):
        await resolve(
            client.add_file_multipart(
                EXISTING,
                path,
                expected_metadata=EXPECTED,
                interactive_priority=cast("Any", 1),
            )
        )
    with pytest.raises(InvalidOptionError, match="expected_metadata"):
        await resolve(client.add_file_multipart(EXISTING, path, expected_metadata={}))
    with pytest.raises(InvalidOptionError, match="file must be"):
        await resolve(
            client.add_file_multipart(
                EXISTING, cast("Any", None), expected_metadata=EXPECTED
            )
        )
    assert not ia_endpoints.requests


def test_state_round_trips_through_json_values() -> None:
    """to_dict output rebuilds an equal state and survives pickling."""
    state = state_for(phase=InternetArchiveMultipartPhase.COMPLETING, parts=all_parts())
    data = state.to_dict()
    assert data["version"] == 1
    assert cast("list[object]", data["parts"])[0] == [1, all_parts()[0].etag, 8]
    assert InternetArchiveMultipartState.from_dict(data) == state
    assert pickle.loads(pickle.dumps(state)) == state
    assert state.part_count == THREE_PARTS
    assert state.details_url.endswith(ID)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"version": 2}, "version"),
        ({"extra": 1}, "missing or unknown"),
        ({"parts": "x"}, r"\[number, etag, size\]"),
        ({"parts": [[1, 2]]}, r"\[number, etag, size\]"),
        ({"phase": 3}, "unknown phase"),
        ({"phase": "done"}, "unknown phase"),
        ({"size": True}, "size must be"),
        ({"part_size": 0}, "part_size must be"),
        ({"part_size": 1, "size": 1001}, "at most 1000"),
        ({"upload_id": " "}, "upload_id"),
        ({"md5": "A" * 32}, "md5"),
        ({"sha1": "x"}, "sha1"),
        ({"queue_derive": 1}, "queue_derive"),
        ({"interactive_priority": 1}, "interactive_priority"),
        (
            {"parts": [[2, '"' + "0" * 32 + '"', 8], [1, '"' + "0" * 32 + '"', 8]]},
            "ascending",
        ),
        ({"parts": [[4, '"' + "0" * 32 + '"', 8]]}, "layout"),
        ({"parts": [[3, '"' + "0" * 32 + '"', 8]]}, "layout"),
        ({"parts": [[0, '"' + "0" * 32 + '"', 8]]}, "part number"),
        ({"parts": [[1, "0" * 32, 8]]}, "quoted"),
        ({"parts": [[1, '"' + "0" * 32 + '"', 0]]}, "part size"),
        ({"phase": "completing"}, "every part"),
    ],
)
def test_state_from_dict_rejects_invalid_values(
    change: dict[str, object], message: str
) -> None:
    """Every field is validated when a saved state is loaded."""
    data = state_for().to_dict()
    data.update(change)
    with pytest.raises(InvalidOptionError, match=message):
        InternetArchiveMultipartState.from_dict(data)


def test_state_rejects_invalid_direct_values() -> None:
    """Direct construction checks types that from_dict cannot produce."""
    with pytest.raises(InvalidOptionError, match="mapping"):
        InternetArchiveMultipartState.from_dict(cast("Any", []))
    with pytest.raises(InvalidOptionError, match="phase must be"):
        state_for(phase="completed")
    with pytest.raises(InvalidOptionError, match="multipart part values"):
        state_for(parts=(cast("Any", (1, "x", 8)),))


def test_multipart_error_pickles_with_state_and_cause() -> None:
    """The wrapper keeps state, cause diagnostics, and uncertainty when pickled."""
    cause = ServiceError("boom", status_code=404, service_code="NoSuchUpload")
    error = InternetArchiveMultipartError(state_for(), cause, uncertain=True)
    restored = pickle.loads(pickle.dumps(error))
    assert restored.state == state_for()
    assert restored.cause.service_code == "NoSuchUpload"
    assert restored.uncertain
    assert restored.status_code == 404  # noqa: PLR2004


def test_spool_files_are_not_originals() -> None:
    """Open multipart parts are classified separately from originals."""
    item = _recovery.parse_item(
        {
            "metadata": {"identifier": ID},
            "files": [
                {"name": "__ia_spool/upload_00001_ias3_part.bin", "source": "original"},
                {"name": NAME, "source": "original"},
            ],
        },
        ID,
    )
    assert [file.source for file in item.files] == [
        InternetArchiveItemFileSource.SPOOL,
        InternetArchiveItemFileSource.ORIGINAL,
    ]
    assert [file.name for file in item.original_files] == [NAME]


TARGET = f"<Bucket>{ID}</Bucket><Key>{NAME}</Key>"


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("<!DOCTYPE x><InitiateMultipartUploadResult/>", "unsupported"),
        ("<InitiateMultipartUploadResult>", "malformed"),
        ("<Other/>", "unexpected"),
        (
            "<InitiateMultipartUploadResult><Bucket>x</Bucket><Key>y</Key>"
            "</InitiateMultipartUploadResult>",
            "another item",
        ),
        (
            f"<InitiateMultipartUploadResult>{TARGET}</InitiateMultipartUploadResult>",
            "upload ID",
        ),
        (
            f"<InitiateMultipartUploadResult>{TARGET}<UploadId> u </UploadId>"
            "</InitiateMultipartUploadResult>",
            "upload ID",
        ),
        (
            f"<InitiateMultipartUploadResult>{TARGET}{TARGET}"
            "</InitiateMultipartUploadResult>",
            "ambiguous",
        ),
    ],
)
def test_parse_initiate_rejects_bad_xml(text: str, message: str) -> None:
    """Initiation replies must name this object and one clean upload ID."""
    with pytest.raises(InvalidServiceResponseError, match=message):
        _multipart.parse_initiate(text, ID, NAME)


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ("<UploadId>other</UploadId>", "another upload"),
        ("<IsTruncated>true</IsTruncated>", "truncated"),
        (
            "<Part><PartNumber>x</PartNumber><ETag>e</ETag><Size>1</Size></Part>",
            "invalid multipart part",
        ),
        (
            "<Part><PartNumber>1</PartNumber><Size>1</Size></Part>",
            "invalid multipart part",
        ),
        (
            "<Part><PartNumber>1</PartNumber><ETag>e</ETag><Size>1</Size></Part>" * 2,
            "invalid multipart part",
        ),
    ],
)
def test_parse_list_parts_rejects_bad_listings(body: str, message: str) -> None:
    """Listings must belong to the upload, be complete, and be unambiguous."""
    text = f"<ListPartsResult>{TARGET}{body}</ListPartsResult>"
    with pytest.raises(InvalidServiceResponseError, match=message):
        _multipart.parse_list_parts(text, state_for())


def test_parse_list_parts_normalizes_etags_and_skips_other_elements() -> None:
    """Uppercase ETags are normalized and unrelated elements are ignored."""
    text = (
        f"<ListPartsResult>{TARGET}<Owner/><Part><PartNumber>1</PartNumber>"
        '<ETag> "ABC" </ETag><Size>8</Size></Part></ListPartsResult>'
    )
    assert _multipart.parse_list_parts(text, state_for()) == {1: ('"abc"', 8)}


def test_completion_task_parsing() -> None:
    """Task data must be a successful listing of mappings."""
    for data in ({"success": False}, {"success": True, "value": []}):
        with pytest.raises(InvalidServiceResponseError):
            _multipart.completion_task_found(data, "u")
    for rows in ("x", ["x"]):
        with pytest.raises(InvalidServiceResponseError):
            _multipart.completion_task_found(
                {"success": True, "value": {"catalog": rows}}, "u"
            )
    row = {
        "status": "queued",
        "args": {
            "comment": "s3-put-complete-multipart",
            "complete_multipart_upload_id": "u",
        },
    }
    assert _multipart.completion_task_found(
        {"success": True, "value": {"catalog": [{"args": None}, row]}}, "u"
    )


class BrokenStream(BytesIO):
    """A stream whose reads or seeks fail after preparation."""

    def __init__(self, data: bytes) -> None:
        """Start with working reads and seeks."""
        super().__init__(data)
        self.fail_seek = False
        self.fail_read = False

    def read(self, size: int | None = -1) -> bytes:
        """Read unless reads are set to fail."""
        if self.fail_read:
            raise OSError("read failed")
        return super().read(size)

    def seek(self, offset: int, whence: int = 0) -> int:
        """Seek unless seeks are set to fail."""
        if self.fail_seek:
            raise OSError("seek failed")
        return super().seek(offset, whence)


def test_hash_and_part_body_report_unstable_sources() -> None:
    """Read and seek failures become option errors, never partial uploads."""
    stream = BrokenStream(DATA)
    from archivist.services.internet_archive import _items  # noqa: PLC0415

    prepared = _items.PreparedFile(NAME, len(DATA), stream)
    stream.fail_read = True
    with pytest.raises(InvalidOptionError, match="hashed"):
        _multipart.hash_source(prepared, PART)
    stream.fail_read = False
    digest = _multipart.hash_source(prepared, PART)
    assert digest.part_md5s == tuple(
        md5(DATA[start : start + PART]).hexdigest() for start in (0, 8, 16)
    )
    stream.fail_seek = True
    with pytest.raises(InvalidOptionError, match="positioned"):
        digest.part_body(2)
    short = _items.PreparedFile(NAME, len(DATA) + 1, BytesIO(DATA))
    with pytest.raises(InvalidOptionError, match="hashed"):
        _multipart.hash_source(short, PART)
    bad_tell = BytesIO(DATA)
    bad_tell.tell = lambda: -1  # type: ignore[method-assign]
    with pytest.raises(InvalidOptionError, match="hashed"):
        _multipart.hash_source(_items.PreparedFile(NAME, len(DATA), bad_tell), PART)


def test_file_status_compares_size_md5_and_sha1() -> None:
    """Only a listed size and MD5 decide; a listed SHA-1 must also agree."""
    state = state_for()

    def status(**file: object) -> bool | None:
        item = _recovery.parse_item(
            {"metadata": {"identifier": ID}, "files": [{"name": NAME, **file}]}, ID
        )
        return _multipart.file_status(item, state)

    assert status() is None
    assert status(size="20") is None
    assert status(size="20", md5=state.md5) is True
    assert status(size="20", md5=state.md5, sha1=state.sha1) is True
    assert status(size="20", md5=state.md5, sha1="0" * 40) is False
    assert status(size="21", md5=state.md5) is False
