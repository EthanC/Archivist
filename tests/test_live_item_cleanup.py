"""Exercise live-test cleanup entirely offline, without credentials or network."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from http import HTTPStatus
from pathlib import Path

import pytest

from archivist import (
    InternetArchiveAccount,
    InternetArchiveRemovalResult,
    InternetArchiveUploadError,
    InternetArchiveUploadFileResult,
    InternetArchiveUploadOptions,
    InternetArchiveUploadResult,
    InvalidServiceResponseError,
)
from tests.live import test_items as live_items


class FakeClient:
    """Record ownership checks and cleanup without an HTTP transport."""

    def __init__(self) -> None:
        """Initialize controllable upload failures and removal synchronization."""
        self.failure_kind = "cancel"
        self.failure: BaseException | None = None
        self.options: InternetArchiveUploadOptions | None = None
        self.marker_matches = True
        self.proven = False
        self.removed: list[str] = []
        self.closed = False
        self.removal_error: BaseException | None = None
        self.close_error: BaseException | None = None
        self.block_removal = False
        self.removal_started = asyncio.Event()
        self.release_removal = asyncio.Event()
        self.removal_cancelled = False

    def upload(
        self, files: object, options: InternetArchiveUploadOptions, **kwargs: object
    ) -> InternetArchiveUploadResult:
        """Simulate failures after the upload attempt has captured its marker."""
        self.options = options
        outcome = InternetArchiveUploadFileResult(
            "probe.txt", len(b"Synthetic live test.\n"), transferred=True
        )
        if self.failure_kind == "success":
            return InternetArchiveUploadResult(
                options.identifier, (outcome,), processing_complete=True
            )
        if self.failure_kind == "cancel":
            self.failure = asyncio.CancelledError("original upload cancellation")
        elif self.failure_kind == "keyboard":
            self.failure = KeyboardInterrupt("original upload interruption")
        else:
            late = self.failure_kind == "late409"
            self.failure = InternetArchiveUploadError(
                InternetArchiveUploadResult(
                    options.identifier,
                    (outcome,) if late else (),
                ),
                InvalidServiceResponseError(
                    "controlled conflict", status_code=HTTPStatus.CONFLICT
                ),
                failed_file="second.bin" if late else None,
            )
        raise self.failure

    async def remove_items(
        self, identifiers: list[str], *, comment: str
    ) -> tuple[InternetArchiveRemovalResult, ...]:
        """Require marker proof before recording a single removal attempt."""
        assert self.proven
        assert self.options is not None
        assert identifiers == [self.options.identifier]
        self.removal_started.set()
        if self.removal_error is not None:
            raise self.removal_error
        if self.block_removal:
            try:
                await self.release_removal.wait()
            except asyncio.CancelledError:
                self.removal_cancelled = True
                raise
        self.removed.extend(identifiers)
        return (InternetArchiveRemovalResult(identifiers[0], True, "123"),)

    async def close(self) -> None:
        """Record closure and optionally simulate an unsafe transport message."""
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


@pytest.fixture
def fake_client(monkeypatch: pytest.MonkeyPatch) -> FakeClient:
    """Replace every live-test network entry point, bypassing no safety checks."""
    client = FakeClient()
    monkeypatch.setattr(live_items, "InternetArchiveClient", lambda **kwargs: client)
    monkeypatch.setattr(
        live_items, "AsyncInternetArchiveClient", lambda **kwargs: client
    )

    @asynccontextmanager
    async def reader(*, retries: int) -> AsyncIterator[object]:
        assert retries == 0
        yield object()

    async def metadata(
        session: object,
        identifier: str,
        predicate: Callable[[dict[str, object]], bool],
        budget: float,
    ) -> dict[str, object]:
        assert client.options is not None
        data: dict[str, object]
        if client.removed:
            data = {"is_dark": True}
        else:
            data = {
                "metadata": {
                    "identifier": identifier,
                    "source": (
                        client.options.metadata["source"]
                        if client.marker_matches
                        else "https://example.invalid/preexisting-item"
                    ),
                }
            }
        if predicate(data):
            if not client.removed:
                client.proven = True
            return data
        raise TimeoutError("sensitive response text must not appear in cleanup notes")

    async def verify(*args: object) -> None:
        return None

    monkeypatch.setattr(live_items.niquests, "AsyncSession", reader)
    monkeypatch.setattr(live_items, "wait_for_metadata", metadata)
    monkeypatch.setattr(live_items, "verify_item", verify)
    return client


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("failure_kind", ["cancel", "keyboard", "late409", "create409"])
@pytest.mark.parametrize("marker_matches", [False, True], ids=["preexisting", "owned"])
async def test_lifecycle_cleanup_requires_marker_proof(
    fake_client: FakeClient,
    tmp_path: Path,
    asynchronous: bool,
    failure_kind: str,
    marker_matches: bool,
) -> None:
    """Recover owned partial items without deleting preexisting conflicts."""
    fake_client.failure_kind = failure_kind
    fake_client.marker_matches = marker_matches
    error_type = {
        "cancel": asyncio.CancelledError,
        "keyboard": KeyboardInterrupt,
        "late409": InternetArchiveUploadError,
        "create409": InternetArchiveUploadError,
    }[failure_kind]
    with pytest.raises(error_type) as failure:
        await live_items.test_item_lifecycle_live(
            InternetArchiveAccount("offline-user", "offline-password"),
            tmp_path,
            asynchronous,
            1,
        )
    assert failure.value is fake_client.failure
    assert fake_client.options is not None
    assert fake_client.removed == (
        [fake_client.options.identifier] if marker_matches else []
    )
    assert fake_client.closed
    if not marker_matches:
        notes = " ".join(failure.value.__notes__)
        assert fake_client.options.identifier in notes
        assert "Ownership verification" in notes
        assert "sensitive response text" not in notes


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["removal", "close"])
async def test_cleanup_failure_does_not_replace_original(
    fake_client: FakeClient, tmp_path: Path, stage: str
) -> None:
    """Preserve exception identity and omit cleanup exception messages."""
    error = RuntimeError("credential-bearing cleanup response")
    if stage == "removal":
        fake_client.removal_error = error
    else:
        fake_client.close_error = error
    with pytest.raises(asyncio.CancelledError) as failure:
        await live_items.test_item_lifecycle_live(
            InternetArchiveAccount("offline-user", "offline-password"),
            tmp_path,
            True,
            1,
        )
    assert failure.value is fake_client.failure
    assert fake_client.options is not None
    notes = " ".join(failure.value.__notes__)
    assert fake_client.options.identifier in notes
    assert "RuntimeError" in notes
    assert "credential-bearing" not in notes
    assert fake_client.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_kind", ["cancel", "success"])
async def test_repeated_cancellation_does_not_cancel_cleanup(
    fake_client: FakeClient, tmp_path: Path, failure_kind: str
) -> None:
    """Keep the cleanup task alive when another cancellation arrives."""
    fake_client.failure_kind = failure_kind
    fake_client.block_removal = True
    task = asyncio.create_task(
        live_items.test_item_lifecycle_live(
            InternetArchiveAccount("offline-user", "offline-password"),
            tmp_path,
            True,
            1,
        )
    )
    try:
        await asyncio.wait_for(fake_client.removal_started.wait(), 1)
        task.cancel("second cancellation")
        await asyncio.sleep(0)
        assert not task.done()
        assert not fake_client.removal_cancelled
    finally:
        fake_client.release_removal.set()
        with pytest.raises(asyncio.CancelledError) as failure:
            await task
    if fake_client.failure is not None:
        assert failure.value is fake_client.failure
    assert fake_client.removed
    assert fake_client.closed


@pytest.mark.asyncio
async def test_cancelled_cleanup_task_preserves_original(
    fake_client: FakeClient, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Do not spin on a cleanup task cancelled before its recovery handler runs."""

    async def cancelled(*args: object) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(live_items, "cleanup_owned_item", cancelled)
    with pytest.raises(asyncio.CancelledError) as failure:
        await live_items.test_item_lifecycle_live(
            InternetArchiveAccount("offline-user", "offline-password"),
            tmp_path,
            True,
            1,
        )
    assert failure.value is fake_client.failure
    assert "Cleanup task cancelled" in " ".join(failure.value.__notes__)
    assert not fake_client.removed
    assert fake_client.closed


@pytest.mark.asyncio
async def test_cleanup_has_a_deadline(
    fake_client: FakeClient, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Cancel stalled cleanup at its deadline without losing the upload exception."""
    monkeypatch.setattr(live_items, "CLEANUP_BUDGET", 0.01)
    fake_client.block_removal = True
    with pytest.raises(asyncio.CancelledError) as failure:
        await live_items.test_item_lifecycle_live(
            InternetArchiveAccount("offline-user", "offline-password"),
            tmp_path,
            True,
            1,
        )
    assert failure.value is fake_client.failure
    assert fake_client.removal_cancelled
    assert "TimeoutError" in " ".join(failure.value.__notes__)
    assert fake_client.closed


@pytest.mark.asyncio
async def test_successful_upload_does_not_replace_ownership_proof(
    fake_client: FakeClient, tmp_path: Path
) -> None:
    """Never remove an item with a mismatched marker, even after a success result."""
    fake_client.failure_kind = "success"
    fake_client.marker_matches = False
    with pytest.raises(pytest.fail.Exception, match="Ownership verification"):
        await live_items.test_item_lifecycle_live(
            InternetArchiveAccount("offline-user", "offline-password"),
            tmp_path,
            True,
            1,
        )
    assert not fake_client.removed
    assert fake_client.closed


@pytest.mark.parametrize(
    "metadata",
    [
        {},
        {"identifier": "different", "source": "marker"},
        {"identifier": "expected", "source": "different"},
    ],
)
def test_ownership_requires_both_identifier_and_marker(
    metadata: dict[str, str],
) -> None:
    """Reject incomplete or mismatched ownership evidence."""
    assert not live_items.owns_item({"metadata": metadata}, "expected", "marker")
