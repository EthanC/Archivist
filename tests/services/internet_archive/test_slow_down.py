"""Pin one rate-limit rule for 503 SlowDown and 429 across Internet Archive paths."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import pytest_asyncio

from archivist import (
    AsyncInternetArchiveClient,
    InternetArchiveApiKey,
    InternetArchiveClient,
    InternetArchiveMultipartPhase,
    InternetArchiveUploadFile,
    InvalidServiceResponseError,
    RateLimitError,
    ServiceError,
)
from tests.conftest import s3_error
from tests.services.internet_archive import test_item_recovery as recovery
from tests.services.internet_archive import test_multipart as multipart
from tests.services.internet_archive.test_item_recovery import Client, resolve

if TYPE_CHECKING:
    from tests.conftest import ServerState

Response = tuple[int, str, dict[str, str]]
Call = Callable[[Client, Path], object]

# Each variant: (response, expected type, expected service_code, retry_after).
VARIANTS: dict[str, tuple[Response, type[ServiceError], str | None, float | None]] = {
    "503-slowdown": ((503, s3_error("SlowDown"), {}), RateLimitError, "SlowDown", None),
    "503-slowdown-retry-after": (
        (503, s3_error("SlowDown"), {"Retry-After": "5"}),
        RateLimitError,
        "SlowDown",
        5.0,
    ),
    "503-retry-after-only": (
        (503, "", {"Retry-After": "7"}),
        RateLimitError,
        None,
        7.0,
    ),
    "503-bare": ((503, "", {}), InvalidServiceResponseError, None, None),
    "429": ((429, s3_error("SlowDown"), {}), RateLimitError, "SlowDown", None),
}


def _file() -> list[InternetArchiveUploadFile]:
    return [InternetArchiveUploadFile(BytesIO(b"data"), multipart.NAME)]


def _upload(client: Client, _: Path) -> object:
    return client.upload(_file(), multipart.options())


def _add_files(client: Client, _: Path) -> object:
    return client.add_files(
        recovery.IDENTIFIER, _file(), expected_metadata=recovery.EXPECTED
    )


def _upload_multipart(client: Client, tmp_path: Path) -> object:
    return client.upload_multipart(
        multipart.source(tmp_path),
        multipart.options(),
        part_size=multipart.PART,
        **multipart.FAST,
    )


def _resume_completing(client: Client, tmp_path: Path) -> object:
    state = multipart.state_for(
        phase=InternetArchiveMultipartPhase.COMPLETING, parts=multipart.all_parts()
    )
    return client.upload_multipart(
        multipart.source(tmp_path), multipart.options(), state=state
    )


def _add_file_multipart(client: Client, tmp_path: Path) -> object:
    return client.add_file_multipart(
        multipart.EXISTING,
        multipart.source(tmp_path),
        expected_metadata=multipart.EXPECTED,
        part_size=multipart.PART,
    )


def _abort(client: Client, _: Path) -> object:
    return client.abort_multipart(multipart.state_for())


def _upload_limit(client: Client, _: Path) -> object:
    return client.upload_limit("limit-item")


def _no_setup(_: ServerState) -> None:
    return None


# Each path: (method, request path, responses before the failing one, setup,
# call, request phase).
PATHS: dict[
    str,
    tuple[str, str, int, Callable[[ServerState], None], Call, str],
] = {
    "upload-create": (
        "PUT",
        f"/ia/s3/{multipart.ID}",
        0,
        _no_setup,
        _upload,
        "item-create",
    ),
    "upload-file": ("PUT", multipart.OBJECT, 0, _no_setup, _upload, "file-upload"),
    "upload-limit": ("GET", "/ia/s3", 0, _no_setup, _upload_limit, "upload-limit"),
    "add-files-transfer": (
        "PUT",
        f"/ia/s3/{recovery.IDENTIFIER}/{multipart.NAME}",
        0,
        recovery.existing_item,
        _add_files,
        "file-upload",
    ),
    "add-files-identity": (
        "GET",
        "/ia/user",
        0,
        recovery.existing_item,
        _add_files,
        "identity",
    ),
    "multipart-initiate": (
        "POST",
        multipart.OBJECT,
        0,
        _no_setup,
        _upload_multipart,
        "multipart-initiate",
    ),
    "multipart-part": (
        "PUT",
        multipart.OBJECT,
        1,
        _no_setup,
        _upload_multipart,
        "multipart-part",
    ),
    "multipart-complete": (
        "POST",
        multipart.OBJECT,
        1,
        _no_setup,
        _upload_multipart,
        "multipart-complete",
    ),
    "multipart-list": (
        "GET",
        multipart.OBJECT,
        0,
        _no_setup,
        _resume_completing,
        "multipart-list",
    ),
    "multipart-abort": (
        "DELETE",
        multipart.OBJECT,
        0,
        _no_setup,
        _abort,
        "multipart-abort",
    ),
    "add-file-multipart-part": (
        "PUT",
        multipart.EXISTING_OBJECT,
        0,
        multipart.existing_item,
        _add_file_multipart,
        "multipart-part",
    ),
}


@pytest_asyncio.fixture(
    params=[InternetArchiveClient, AsyncInternetArchiveClient], ids=["sync", "async"]
)
async def client(request: pytest.FixtureRequest) -> AsyncIterator[Client]:
    """Provide both transports with one LOW key pair."""
    instance = request.param(
        api_key=InternetArchiveApiKey("test-access", "test-secret")
    )
    try:
        yield instance
    finally:
        await resolve(instance.close())


async def _failure(call: Callable[[], object]) -> ServiceError:
    """Run the call and return the underlying service error.

    Upload, recovery, and multipart wrappers expose the request failure as
    ``cause``.
    """
    with pytest.raises(ServiceError) as raised:
        await resolve(call())
    return getattr(raised.value, "cause", raised.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("variant", list(VARIANTS))
@pytest.mark.parametrize("path_name", list(PATHS))
async def test_every_path_uses_one_rate_limit_rule(
    ia_endpoints: ServerState,
    client: Client,
    tmp_path: Path,
    path_name: str,
    variant: str,
) -> None:
    """SlowDown, Retry-After 503, and 429 are rate limits; a bare 503 is not."""
    method, path, skip, setup, call, phase = PATHS[path_name]
    response, error_type, service_code, retry_after = VARIANTS[variant]
    setup(ia_endpoints)
    queue: list[Any] = [None] * skip + [response]
    ia_endpoints.queued_responses[method, path] = queue
    cause = await _failure(lambda: call(client, tmp_path))
    assert type(cause) is error_type
    assert cause.status_code == response[0]
    assert cause.request_phase == phase
    assert cause.service_code == service_code
    assert cause.retry_after == retry_after
    if service_code is not None:
        assert cause.service_message == "fixture SlowDown"
        assert cause.request_id == "fixture-request-id"
