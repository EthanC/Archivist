"""Keep upload source I/O off-loop and drain workers before returning ownership."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from io import SEEK_END, BytesIO
from pathlib import Path
from threading import Event, get_ident
from typing import TYPE_CHECKING

import pytest

from archivist import AsyncInternetArchiveClient, InternetArchiveApiKey, NetworkError
from archivist.services.internet_archive import _items
from archivist.services.internet_archive.item_models import (
    InternetArchiveUploadError,
    InternetArchiveUploadFile,
)
from tests.services.internet_archive.test_client_edge_cases import (
    AsyncSession,
    StubResponse,
    as_async_session,
)
from tests.services.internet_archive.test_item_clients import PAYLOAD, options

if TYPE_CHECKING:
    from tests.conftest import ServerState


class IOGate:
    """Hold a worker until the test has observed a responsive loop."""

    def __init__(self, *, fail: bool = False) -> None:
        """Capture the loop and create explicit entry and release barriers."""
        self.loop = asyncio.get_running_loop()
        self.loop_thread = get_ident()
        self.entered = asyncio.Event()
        self.release = Event()
        self.finished = False
        self.fail = fail

    def block(self) -> None:
        """Block only a worker, with a watchdog to prevent a hung failing test."""
        assert get_ident() != self.loop_thread
        self.loop.call_soon_threadsafe(self.entered.set)
        try:
            assert self.release.wait(5), "test did not release source I/O"
            if self.fail:
                raise OSError("late source failure")
        finally:
            self.finished = True


class ObservedStream(BytesIO):
    """Record source operations and optionally pause one preparation or read step."""

    def __init__(self, gate: IOGate, stage: str) -> None:
        """Use multiple transfer chunks to exercise iterator exhaustion too."""
        super().__init__(PAYLOAD)
        self.gate = gate
        self.stage = stage
        self.operations: list[tuple[str, int]] = []
        self.read_sizes: list[int | None] = []
        self.close_gate: IOGate | None = None

    def observe(self, stage: str) -> None:
        """Record the calling thread and pause the selected operation once."""
        self.operations.append((stage, get_ident()))
        if stage == self.stage:
            self.stage = "finished"
            self.gate.block()

    def read(self, size: int | None = -1) -> bytes:
        """Distinguish the one-byte preparation probe from transfer reads."""
        assert not self.closed
        self.read_sizes.append(size)
        self.observe("read" if size == 1 else "transfer")
        return super().read(size)

    def seek(self, offset: int, whence: int = 0) -> int:
        """Observe size discovery and offset restoration."""
        self.observe("seek" if whence == SEEK_END else "restore")
        return super().seek(offset, whence)

    def close(self) -> None:
        """Reject cleanup racing an active operation and optionally pause close."""
        assert not self.gate.entered.is_set() or self.gate.finished
        self.observe("close")
        try:
            if self.close_gate is not None:
                self.close_gate.block()
        finally:
            super().close()


async def heartbeat() -> None:
    """Wait for an explicit loop callback, not a timing-dependent sleep."""
    tick = asyncio.Event()
    asyncio.get_running_loop().call_soon(tick.set)
    await tick.wait()


class StreamingSession(AsyncSession):
    """Consume the same async body protocol as niquests without socket side effects."""

    async def request(self, method: str, url: str, **kwargs: object) -> StubResponse:
        """Record mutations and consume transfer bodies before acknowledging them."""
        self.requests.append((method, url, kwargs))
        body = kwargs.get("data")
        if isinstance(body, _items.AsyncPreparedFile):
            async for _ in body:
                pass
        if method == "POST":
            return StubResponse({"success": True, "identifier": "fixture-item"})
        return StubResponse(None)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["iter", "open", "read", "seek", "transfer", "close"])
async def test_real_niquests_source_io_keeps_loop_responsive(
    ia_endpoints: ServerState, monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    """Exercise real async HTTP, fixed length, worker reads, and offloaded cleanup."""
    gate = IOGate()
    stream = ObservedStream(gate, stage)

    def open_source(*args: object, **kwargs: object) -> ObservedStream:
        assert get_ident() != gate.loop_thread
        if stage == "open":
            gate.block()
        return stream

    def sources() -> Iterator[Path]:
        assert get_ident() != gate.loop_thread
        if stage == "iter":
            gate.block()
        yield Path("owned")

    monkeypatch.setattr(Path, "open", open_source)
    async with AsyncInternetArchiveClient(
        api_key=InternetArchiveApiKey("access", "secret")
    ) as client:
        task = asyncio.create_task(client.upload(sources(), options()))
        try:
            await asyncio.wait_for(gate.entered.wait(), 5)
            await heartbeat()
            assert not task.done()
            assert not stream.closed
            if stage in {"iter", "open", "read", "seek"}:
                assert not ia_endpoints.requests
            gate.release.set()
            result = await asyncio.wait_for(task, 5)
        finally:
            gate.release.set()
            await asyncio.gather(task, return_exceptions=True)
    assert result.files[0].transferred
    assert stream.closed
    assert all(thread != gate.loop_thread for _, thread in stream.operations)
    assert stream.read_sizes == [1, 65536, 65536, 65536, 65536]
    request = ia_endpoints.matching("/ia/s3/fixture-item/owned", "PUT")[0]
    assert request.body == PAYLOAD
    assert request.headers["Content-Length"] == str(len(PAYLOAD))
    assert "Transfer-Encoding" not in request.headers


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["read", "seek", "transfer"])
@pytest.mark.parametrize("owned_source", [False, True], ids=["caller", "owned"])
@pytest.mark.parametrize("fail", [False, True], ids=["success", "late-error"])
async def test_repeated_cancellation_drains_active_source_before_cleanup(
    monkeypatch: pytest.MonkeyPatch, stage: str, owned_source: bool, fail: bool
) -> None:
    """Retain the first cancellation while restoring or closing only owned sources."""
    gate = IOGate(fail=fail)
    slow = ObservedStream(gate, stage)
    other = BytesIO(b"other")
    owned, caller = (slow, other) if owned_source else (other, slow)
    monkeypatch.setattr(Path, "open", lambda *args, **kwargs: owned)
    session = StreamingSession()
    existing_tasks = asyncio.all_tasks()
    async with AsyncInternetArchiveClient(
        session=as_async_session(session), api_key=InternetArchiveApiKey("a", "s")
    ) as client:
        task = asyncio.create_task(
            client.upload(
                [Path("owned"), InternetArchiveUploadFile(caller, "caller")], options()
            )
        )
        try:
            await asyncio.wait_for(gate.entered.wait(), 5)
            workers = asyncio.all_tasks() - existing_tasks - {task}
            assert len(workers) == 1
            for message in ("original", "again", "again"):
                task.cancel(message)
                await heartbeat()
                assert not task.done()
                assert not owned.closed
                assert not caller.closed
                assert not gate.finished
            if stage != "transfer":
                assert not session.requests
            gate.release.set()
            with pytest.raises(asyncio.CancelledError, match="original"):
                await task
            assert gate.finished
            assert all(worker.done() for worker in workers)
            # Task's flag is cleared only when its failure has been retrieved.
            assert all(not worker._log_traceback for worker in workers)
            assert asyncio.all_tasks() == existing_tasks
            assert owned.closed
            assert not caller.closed
            if stage != "transfer":
                assert caller.tell() == 0
        finally:
            gate.release.set()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_read", [False, True])
@pytest.mark.parametrize("fail_close", [False, True])
async def test_slow_close_drains_despite_repeated_cancellation(
    monkeypatch: pytest.MonkeyPatch, cancel_read: bool, fail_close: bool
) -> None:
    """Wait for close itself and preserve an earlier cancellation across cleanup."""
    read_gate = IOGate()
    close_gate = IOGate(fail=fail_close)
    stream = ObservedStream(read_gate, "transfer" if cancel_read else "unused")
    stream.close_gate = close_gate
    monkeypatch.setattr(Path, "open", lambda *args, **kwargs: stream)
    async with AsyncInternetArchiveClient(
        session=as_async_session(StreamingSession()),
        api_key=InternetArchiveApiKey("a", "s"),
    ) as client:
        task = asyncio.create_task(client.upload([Path("owned")], options()))
        try:
            if cancel_read:
                await asyncio.wait_for(read_gate.entered.wait(), 5)
                task.cancel("original")
                await heartbeat()
                read_gate.release.set()
            await asyncio.wait_for(close_gate.entered.wait(), 5)
            for message in ("close", "again"):
                task.cancel(message)
                await heartbeat()
                assert not task.done()
                assert not stream.closed
            close_gate.release.set()
            with pytest.raises(
                asyncio.CancelledError, match="original" if cancel_read else "close"
            ):
                await task
            assert stream.closed
            assert close_gate.finished
        finally:
            read_gate.release.set()
            close_gate.release.set()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_transfer_source_error_keeps_network_error_translation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Translate worker read failures and retain the failed file without retrying."""
    gate = IOGate(fail=True)
    gate.release.set()
    stream = ObservedStream(gate, "transfer")
    monkeypatch.setattr(Path, "open", lambda *args, **kwargs: stream)
    session = StreamingSession()
    async with AsyncInternetArchiveClient(
        session=as_async_session(session), api_key=InternetArchiveApiKey("a", "s")
    ) as client:
        with pytest.raises(InternetArchiveUploadError) as failure:
            await client.upload([Path("owned")], options())
    assert isinstance(failure.value.cause, NetworkError)
    assert failure.value.failed_file == "owned"
    assert not failure.value.result.files[0].transferred
    assert stream.closed
    assert [method for method, _, _ in session.requests] == ["POST", "PUT", "PUT"]
