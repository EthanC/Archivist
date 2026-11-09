"""Run a doubly opt-in multipart upload with an interruption and a resume."""

from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path
from uuid import uuid4

import niquests
import pytest

from archivist import (
    AsyncInternetArchiveClient,
    InternetArchiveAccount,
    InternetArchiveClient,
    InternetArchiveMultipartPhase,
    InternetArchiveMultipartState,
    InternetArchiveUploadOptions,
)
from tests.live.test_items import cleanup_after_upload, resolve
from tests.live.test_items import (
    live_account as live_account,  # noqa: PLC0414 - Import the gated fixture.
)

pytestmark = pytest.mark.live
MIB = 1024 * 1024
PART_SIZE = MIB


class InterruptedUploadError(Exception):
    """Stop the first attempt after a part, as a crash would."""


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
async def test_multipart_resume_live(
    live_account: InternetArchiveAccount, tmp_path: Path, asynchronous: bool
) -> None:
    """Interrupt after the first part, resume from saved state, verify assembly."""
    identifier = f"archivist-live-mp-{uuid4().hex}"
    marker = f"https://example.invalid/archivist-live/{uuid4().hex}"
    options = InternetArchiveUploadOptions(
        identifier=identifier,
        title="Synthetic multipart upload",
        description="Synthetic multipart resume test.",
        subjects=["synthetic test"],
        date=date(2026, 11, 9),
        media_type="data",
        test_item=True,
        queue_derive=False,
        metadata={"source": marker},
    )
    path = tmp_path / "multipart.bin"
    path.write_bytes(os.urandom(2 * PART_SIZE + 12345))
    saved: list[dict[str, object]] = []

    def persist(state: InternetArchiveMultipartState) -> None:
        saved.append(state.to_dict())
        if len(state.parts) == 1 and len(saved) == 2:  # noqa: PLR2004
            raise InterruptedUploadError

    client = (
        AsyncInternetArchiveClient(account=live_account, timeout=120)
        if asynchronous
        else InternetArchiveClient(account=live_account, timeout=120)
    )
    try:
        async with (
            niquests.AsyncSession(retries=0) as reader,
            cleanup_after_upload(client, reader, identifier, marker),
        ):
            with pytest.raises(InterruptedUploadError):
                await resolve(
                    client.upload_multipart(
                        path,
                        options,
                        part_size=PART_SIZE,
                        on_state=persist,
                        timeout=600,
                        poll_interval=10,
                    )
                )
            state = InternetArchiveMultipartState.from_dict(saved[-1])
            assert [part.number for part in state.parts] == [1]
            result = await resolve(
                client.upload_multipart(
                    path,
                    options,
                    state=state,
                    on_state=persist,
                    wait=True,
                    timeout=1800,
                    poll_interval=15,
                )
            )
            assert result.verified
            assert result.state is not None
            assert result.state.phase is InternetArchiveMultipartPhase.COMPLETED
            assert len(result.state.parts) == 3  # noqa: PLR2004
    finally:
        original = sys.exception()
        try:
            await resolve(client.close())
        except BaseException as error:
            note = f"Client close failed for {identifier} ({type(error).__name__})"
            if original is None:
                raise AssertionError(note) from None
            original.add_note(note)
