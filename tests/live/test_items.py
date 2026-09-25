"""Run doubly opt-in item uploads and owner-verified make_dark cleanup."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from contextlib import asynccontextmanager
from datetime import date
from http import HTTPStatus
from inspect import isawaitable
from io import BytesIO
from pathlib import Path
from typing import TypeVar, cast
from urllib.parse import quote
from uuid import uuid4

import niquests
import pytest

from archivist import (
    AsyncInternetArchiveClient,
    InternetArchiveAccount,
    InternetArchiveClient,
    InternetArchiveUploadFile,
    InternetArchiveUploadOptions,
)

pytestmark = pytest.mark.live
T = TypeVar("T")
Client = InternetArchiveClient | AsyncInternetArchiveClient
# Live derivation has delayed make_dark for more than 30 minutes.
CLEANUP_BUDGET = 3900


async def resolve(value: T | Awaitable[T]) -> T:
    """Use the same lifecycle checks for sync and async public clients."""
    if isawaitable(value):
        return await cast("Awaitable[T]", value)
    return cast("T", value)


@pytest.fixture
def live_account() -> Iterator[InternetArchiveAccount]:
    """Require both gates before reading credentials or constructing clients."""
    __tracebackhide__ = True
    if (
        os.environ.get("ARCHIVIST_RUN_LIVE") != "1"
        or os.environ.get("ARCHIVIST_RUN_LIVE_MUTATIONS") != "1"
    ):
        pytest.skip("set both ARCHIVIST_RUN_LIVE and ARCHIVIST_RUN_LIVE_MUTATIONS to 1")
    username = os.environ.get("INTERNET_ARCHIVE_EMAIL")
    password = os.environ.get("INTERNET_ARCHIVE_PASSWORD")
    if not username or not password:
        pytest.skip("set INTERNET_ARCHIVE_EMAIL and INTERNET_ARCHIVE_PASSWORD")
    previous = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        yield InternetArchiveAccount(username, password, remember=False)
    finally:
        logging.disable(previous)


async def wait_for_metadata(
    session: niquests.AsyncSession,
    identifier: str,
    predicate: Callable[[dict[str, object]], bool],
    budget: float,
) -> dict[str, object]:
    """Bound read-only polling without sharing authentication with public reads."""
    __tracebackhide__ = True
    async with asyncio.timeout(budget):
        while True:
            response = await session.get(
                f"https://archive.org/metadata/{identifier}",
                params={"research": uuid4().hex},
                timeout=30,
                allow_redirects=False,
            )
            await session.gather(response)
            if response.status_code != HTTPStatus.OK:
                pytest.fail(f"Metadata read returned HTTP {response.status_code}")
            data = await resolve(response.json())
            if not isinstance(data, dict):
                pytest.fail("Metadata endpoint did not return an object")
            if predicate(data):
                return cast("dict[str, object]", data)
            await asyncio.sleep(10)


def owns_item(data: dict[str, object], identifier: str, marker: str) -> bool:
    """Match both the requested identifier and this test's unique source marker."""
    metadata = data.get("metadata")
    return (
        isinstance(metadata, dict)
        and metadata.get("identifier") == identifier
        and metadata.get("source") == marker
    )


async def verify_item(
    session: niquests.AsyncSession,
    options: InternetArchiveUploadOptions,
    payloads: dict[str, bytes],
) -> None:
    """Check stored metadata and public downloads independently of ingest polling."""
    data = await wait_for_metadata(
        session,
        options.identifier,
        lambda value: (
            isinstance(value.get("files"), list)
            and all(
                any(
                    isinstance(file, dict) and file.get("name") == name
                    for file in cast("list[object]", value["files"])
                )
                for name in payloads
            )
        ),
        600,
    )
    metadata = cast("dict[str, object]", data["metadata"])
    expected = {
        "identifier": options.identifier,
        "title": options.title,
        "description": options.description,
        "subject": list(options.subjects),
        "creator": options.creator,
        "date": options.date.isoformat() if options.date is not None else None,
        "language": options.language,
        "licenseurl": options.license,
        "mediatype": options.media_type,
        "source": options.metadata["source"],
        "custom_key": list(options.metadata["custom_key"]),
    }
    observed = {key: metadata.get(key) for key in expected}
    assert observed == expected
    collections = metadata.get("collection")
    assert collections == "test_collection" or (
        isinstance(collections, list) and "test_collection" in collections
    )
    for name, payload in payloads.items():
        response = await session.get(
            f"https://archive.org/download/{options.identifier}/"
            f"{quote(name, safe='/')}",
            timeout=60,
        )
        await session.gather(response)
        assert response.status_code == HTTPStatus.OK
        assert await resolve(response.content) == payload


async def remove_owned_item(
    client: Client, session: niquests.AsyncSession, identifier: str
) -> None:
    """Submit removal once for a confirmed-owned identifier and require dark state."""
    removals = await resolve(
        client.remove_items(
            [identifier], comment="Archivist opt-in synthetic test cleanup"
        )
    )
    assert len(removals) == 1
    assert removals[0].identifier == identifier
    assert removals[0].accepted, f"Removal not acknowledged for {identifier}"
    assert removals[0].task_id
    try:
        await wait_for_metadata(
            session, identifier, lambda data: data.get("is_dark") is True, 3600
        )
    except TimeoutError:
        pytest.fail(f"Removal accepted but darkness not confirmed for {identifier}")


async def cleanup_owned_item(
    client: Client,
    session: niquests.AsyncSession,
    identifier: str,
    marker: str,
) -> str | None:
    """Require marker proof before removal and return only sanitized failure notes."""
    stage = "Ownership verification"
    try:
        async with asyncio.timeout(CLEANUP_BUDGET):
            data = await wait_for_metadata(
                session,
                identifier,
                lambda value: owns_item(value, identifier, marker),
                180,
            )
            if not owns_item(data, identifier, marker):
                return f"Ownership unconfirmed for {identifier}; not removed"
            stage = "Removal or dark-state verification"
            await remove_owned_item(client, session, identifier)
    except BaseException as error:
        # Transport messages and response bodies can contain account secrets.
        return f"{stage} failed for {identifier} ({type(error).__name__})"
    return None


@asynccontextmanager
async def cleanup_after_upload(
    client: Client,
    session: niquests.AsyncSession,
    identifier: str,
    marker: str,
) -> AsyncIterator[None]:
    """Arm cleanup before upload and preserve failures while shielding recovery."""
    original: BaseException | None = None
    try:
        yield
    except BaseException as error:
        original = error
        raise
    finally:
        task = asyncio.create_task(
            cleanup_owned_item(client, session, identifier, marker)
        )
        interruption: BaseException | None = None
        while True:
            try:
                note = await asyncio.shield(task)
                break
            except (asyncio.CancelledError, KeyboardInterrupt) as error:
                if interruption is None:
                    interruption = error
                if task.cancelled():
                    note = (
                        f"Cleanup task cancelled for {identifier}; cleanup unconfirmed"
                    )
                    break
        failure = original if original is not None else interruption
        if note is not None:
            if failure is None:
                pytest.fail(note)
            failure.add_note(note)
        if original is None and interruption is not None:
            raise interruption


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("file_count", [1, 2], ids=["single", "multi"])
async def test_item_lifecycle_live(
    live_account: InternetArchiveAccount,
    tmp_path: Path,
    asynchronous: bool,
    file_count: int,
) -> None:
    """Upload tiny synthetic items and never clean up conflicts or unknown owners."""
    identifier = f"archivist-live-{uuid4().hex}"
    marker = f"https://example.invalid/archivist-live/{uuid4().hex}"
    options = InternetArchiveUploadOptions(
        identifier=identifier,
        title="Synthetic caf\u00e9 upload",
        description="<p>Synthetic <b>HTML</b> &amp; Unicode: \u96ea.</p>",
        subjects=["synthetic test", "caf\u00e9"],
        creator="Archivist live test",
        date=date(2026, 9, 25),
        language="eng",
        license="https://creativecommons.org/publicdomain/zero/1.0/",
        media_type="data",
        test_item=True,
        metadata={"source": marker, "custom_key": ["first", "second \u96ea"]},
    )
    client = (
        AsyncInternetArchiveClient(account=live_account, timeout=60)
        if asynchronous
        else InternetArchiveClient(account=live_account, timeout=60)
    )
    try:
        async with niquests.AsyncSession(retries=0) as reader:
            with BytesIO(b"skip:Synthetic live test.\n") as stream:
                stream.seek(5)
                files: list[InternetArchiveUploadFile | str | Path] = [
                    InternetArchiveUploadFile(stream, "probe.txt")
                ]
                payloads = {"probe.txt": b"Synthetic live test.\n"}
                if file_count > 1:
                    path = tmp_path / "second.bin"
                    payloads[path.name] = b"\x00\x01Synthetic binary test.\xff"
                    path.write_bytes(payloads[path.name])
                    files.append(path)
                async with cleanup_after_upload(client, reader, identifier, marker):
                    result = await resolve(
                        client.upload(
                            files, options, wait=True, timeout=600, poll_interval=5
                        )
                    )
                    assert result.identifier == identifier
                    assert result.processing_complete
                    assert all(file.transferred for file in result.files)
                    assert {file.name: file.size for file in result.files} == {
                        name: len(payload) for name, payload in payloads.items()
                    }
                    assert not stream.closed
                    await verify_item(reader, options, payloads)
    finally:
        original = sys.exception()
        try:
            await resolve(client.close())
        except BaseException as error:
            note = f"Client close failed for {identifier} ({type(error).__name__})"
            if original is None:
                raise AssertionError(note) from None
            original.add_note(note)
