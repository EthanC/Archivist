"""Exercise single-Path uploads through both item clients."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock, Mock

import niquests
import pytest

from archivist import AsyncInternetArchiveClient, InvalidOptionError, NetworkError
from archivist.services.internet_archive.item_models import InternetArchiveUploadFile
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


@pytest.mark.asyncio
@pytest.mark.parametrize("transport_failure", [False, True])
async def test_single_path_upload_closes_owned_file(
    ia_endpoints: ServerState,
    item_client: Client,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    transport_failure: bool,
) -> None:
    """Send the basename and exact bytes, closing the file even on network failure."""
    path = tmp_path / "single.bin"
    path.write_bytes(PAYLOAD)
    with path.open("rb") as owned:
        monkeypatch.setattr(Path, "open", Mock(return_value=owned))
        if transport_failure:
            mock = (
                AsyncMock
                if isinstance(item_client, AsyncInternetArchiveClient)
                else Mock
            )
            monkeypatch.setattr(
                item_client._session,
                "request",
                mock(
                    side_effect=niquests.exceptions.ConnectionError("connection lost")
                ),
            )
            with pytest.raises(NetworkError):
                await resolve(item_client.upload(path, options()))
            assert not ia_endpoints.requests
        else:
            result = await resolve(item_client.upload(path, options()))
            assert [
                (file.name, file.size, file.transferred) for file in result.files
            ] == [(path.name, len(PAYLOAD), True)]
            requests = ia_endpoints.matching("/ia/s3/fixture-item/single.bin", "PUT")
            assert len(requests) == 1
            assert requests[0].body == PAYLOAD
            assert requests[0].headers["Content-Length"] == str(len(PAYLOAD))
        assert owned.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["missing", "str", "bytes", "upload-file"])
async def test_single_path_upload_rejects_invalid_input_before_network(
    ia_endpoints: ServerState,
    item_client: Client,
    tmp_path: Path,
    invalid: str,
) -> None:
    """Reject missing paths and other single-file forms before authentication."""
    path = tmp_path / "single.bin"
    path.write_bytes(PAYLOAD)
    files = {
        "missing": tmp_path / "missing.bin",
        "str": str(path),
        "bytes": str(path).encode(),
        "upload-file": InternetArchiveUploadFile(path),
    }[invalid]
    item_client._api_key = None
    message = "could not be opened" if invalid == "missing" else "files must be"
    with pytest.raises(InvalidOptionError, match=message):
        await resolve(item_client.upload(cast("Any", files), options()))
    assert not ia_endpoints.requests
