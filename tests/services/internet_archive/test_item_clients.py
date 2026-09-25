"""Exercise item mutations through real synchronous and asynchronous HTTP."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable
from datetime import date
from http import HTTPStatus
from inspect import isawaitable
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar, cast
from urllib.parse import quote

import pytest
import pytest_asyncio

from archivist import (
    AsyncInternetArchiveClient,
    AuthenticationError,
    InternetArchiveAccount,
    InternetArchiveApiKey,
    InternetArchiveClient,
    InternetArchiveCookies,
    InvalidOptionError,
    InvalidServiceResponseError,
    RateLimitError,
    ServiceError,
)
from archivist.services.internet_archive.item_models import (
    InternetArchiveUploadError,
    InternetArchiveUploadFile,
    InternetArchiveUploadOptions,
)

if TYPE_CHECKING:
    from tests.conftest import ServerState

T = TypeVar("T")
Client = InternetArchiveClient | AsyncInternetArchiveClient
PAYLOAD = bytes(range(256)) * 1024
TWO_REQUESTS = 2
RETRY_AFTER = 7


async def resolve(value: T | Awaitable[T]) -> T:
    """Await asynchronous client operations while retaining synchronous parity."""
    if isawaitable(value):
        return await cast("Awaitable[T]", value)
    return cast("T", value)


@pytest_asyncio.fixture(
    params=[InternetArchiveClient, AsyncInternetArchiveClient], ids=["sync", "async"]
)
async def item_client(request: pytest.FixtureRequest) -> AsyncIterator[Client]:
    """Provide both clients with credentials without accessing external services."""
    client = request.param(api_key=InternetArchiveApiKey("test-access", "test-secret"))
    try:
        yield client
    finally:
        await resolve(client.close())


def options(identifier: str = "fixture-item") -> InternetArchiveUploadOptions:
    """Supply every uploader metadata field, including repeated Unicode values."""
    return InternetArchiveUploadOptions(
        identifier=identifier,
        title="Title \u00e9 & /",
        description="<p>Description \u96ea & text</p>",
        subjects=["first", "\u96ea", "first"],
        creator="Creator \u00e9",
        date=date(2026, 9, 25),
        collection="test_collection",
        language="eng",
        license="https://creativecommons.org/publicdomain/zero/1.0/",
        media_type="data",
        test_item=True,
        metadata={
            "source": "https://example.invalid/?a=1&b=2",
            "custom_key": ["a", "\u96ea", "a"],
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("authentication", ["key", "cookies", "account"])
async def test_upload_binary_metadata_and_ingest(
    ia_endpoints: ServerState,
    item_client: Client,
    tmp_path: Path,
    authentication: str,
) -> None:
    """Send current stream offsets and path bytes with every metadata header."""
    if authentication != "key":
        item_client._api_key = None
        if authentication == "cookies":
            item_client._cookies = InternetArchiveCookies("user", "signature")
        else:
            item_client._account = InternetArchiveAccount("user", "password")
    path = tmp_path / "first.bin"
    path.write_bytes(PAYLOAD)
    stream = BytesIO(b"skip" + PAYLOAD[::-1])
    stream.seek(4)
    name = "folder/\u96ea +%.bin"
    metadata = options()
    result = await resolve(
        item_client.upload(
            [path, InternetArchiveUploadFile(stream, name)],
            metadata,
            wait=True,
            timeout=5,
            poll_interval=0.001,
        )
    )
    assert not stream.closed
    assert result.identifier == metadata.identifier
    assert result.details_url == "https://archive.org/details/fixture-item"
    assert result.processing_complete
    assert [
        (file.name, file.size, file.transferred, file.etag) for file in result.files
    ] == [
        ("first.bin", len(PAYLOAD), True, '"fixture-etag"'),
        (name, len(PAYLOAD), True, '"fixture-etag"'),
    ]
    availability = ia_endpoints.matching("/ia/upload-api", "POST")
    assert len(availability) == 1
    assert availability[0].headers["Content-Type"].startswith("multipart/form-data;")
    assert availability[0].form["identifier"] == [metadata.identifier]
    assert availability[0].form["name"] == ["identifierAvailable"]
    assert availability[0].form["findUnique"] == ["0"]
    puts = [request for request in ia_endpoints.requests if request.method == "PUT"]
    assert [request.path for request in puts] == [
        "/ia/s3/fixture-item",
        "/ia/s3/fixture-item/first.bin",
        f"/ia/s3/fixture-item/{quote(name, safe='/')}",
    ]
    assert [request.body for request in puts] == [b"", PAYLOAD, PAYLOAD[::-1]]
    for index, request in enumerate(puts):
        headers = {key.lower(): value for key, value in request.headers.items()}
        assert "x-amz-auto-make-bucket" not in headers
        assert "x-archive-ignore-preexisting-bucket" not in headers
        assert headers.get("x-archive-queue-derive") == (
            "0" if index < len(puts) - 1 else None
        )
        expected_key = (
            "test-access:test-secret"
            if authentication == "key"
            else "dummy-access:dummy-secret"
        )
        assert headers["authorization"] == f"LOW {expected_key}"
    headers = {key.lower(): value for key, value in puts[0].headers.items()}
    expected = {
        "title": metadata.title,
        "description": metadata.description,
        "creator": metadata.creator,
        "date": metadata.date.isoformat() if metadata.date is not None else None,
        "collection": metadata.collection,
        "language": metadata.language,
        "licenseurl": metadata.license,
        "mediatype": metadata.media_type,
        "source": metadata.metadata["source"],
    }
    for key, value in expected.items():
        assert isinstance(value, str)
        assert headers[f"x-archive-meta-{key}"] == f"uri({quote(value, safe='')})"
    for key, values in (
        ("subject", metadata.subjects),
        ("custom--key", metadata.metadata["custom_key"]),
    ):
        for index, value in enumerate(values, start=1):
            assert (
                headers[f"x-archive-meta{index:02d}-{key}"]
                == f"uri({quote(value, safe='')})"
            )
    assert headers["x-archive-size-hint"] == str(2 * len(PAYLOAD))
    expected_metadata = {
        key: value for key, value in headers.items() if key.startswith("x-archive-meta")
    }
    for request in puts[1:]:
        file_headers = {key.lower(): value for key, value in request.headers.items()}
        assert {
            key: value
            for key, value in file_headers.items()
            if key.startswith("x-archive-meta")
        } == expected_metadata
        assert file_headers["content-length"] == str(len(PAYLOAD))
        assert file_headers["x-archive-size-hint"] == str(2 * len(PAYLOAD))
    polls = ia_endpoints.matching("/ia/upload-api", "GET")
    assert len(polls) == TWO_REQUESTS
    assert all(request.query["name"] == ["catalogRows"] for request in polls)
    assert bool(ia_endpoints.matching("/ia/upload", "GET")) == (authentication != "key")
    assert bool(ia_endpoints.matching("/ia/login", "POST")) == (
        authentication == "account"
    )


@pytest.mark.asyncio
async def test_upload_without_wait_and_duplicate_identifier(
    ia_endpoints: ServerState,
    item_client: Client,
) -> None:
    """Return transferred outcomes without polling and reject existing identifiers."""
    stream = BytesIO(PAYLOAD)
    result = await resolve(
        item_client.upload([InternetArchiveUploadFile(stream, "file.bin")], options())
    )
    assert result.files[0].transferred
    assert not result.processing_complete
    assert not ia_endpoints.matching("/ia/upload-api", "GET")
    stream.seek(0)
    with pytest.raises(InvalidOptionError):
        await resolve(
            item_client.upload(
                [InternetArchiveUploadFile(stream, "file.bin")], options()
            )
        )
    assert (
        len([request for request in ia_endpoints.requests if request.method == "PUT"])
        == TWO_REQUESTS
    )
    assert not stream.closed


@pytest.mark.asyncio
async def test_bucket_claim_conflict_after_advisory_availability(
    ia_endpoints: ServerState,
    item_client: Client,
) -> None:
    """Reject an exclusive-create race without overwriting the existing item."""
    ia_endpoints.item_buckets.add("fixture-item")
    ia_endpoints.item_responses["POST", "/ia/upload-api"] = (
        200,
        '{"success":true,"identifier":"fixture-item"}',
        {},
    )
    with pytest.raises(InternetArchiveUploadError) as failure:
        await resolve(
            item_client.upload(
                [InternetArchiveUploadFile(BytesIO(b"a"), "a")], options()
            )
        )
    assert failure.value.status_code == HTTPStatus.CONFLICT
    assert failure.value.failed_file is None
    assert not failure.value.result.files[0].transferred
    assert not ia_endpoints.matching("/ia/s3/fixture-item/a")
    assert len(ia_endpoints.matching("/ia/s3/fixture-item", "PUT")) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["creation", "first", "second", "poll"])
@pytest.mark.parametrize("status", [302, 403, 409, 429, 503])
async def test_upload_http_failures_preserve_partial_results(
    ia_endpoints: ServerState,
    item_client: Client,
    stage: str,
    status: int,
) -> None:
    """Never retry or redirect a failed mutation, retaining completed file results."""
    path = {
        "creation": "/ia/s3/fixture-item",
        "first": "/ia/s3/fixture-item/a",
        "second": "/ia/s3/fixture-item/b",
        "poll": "/ia/upload-api",
    }[stage]
    method = "GET" if stage == "poll" else "PUT"
    ia_endpoints.item_responses[method, path] = (
        status,
        "<Error><Code>Failure</Code></Error>",
        {"Location": f"{ia_endpoints.base_url}/redirect-target", "Retry-After": "7"},
    )
    streams = [BytesIO(PAYLOAD), BytesIO(PAYLOAD)]
    with pytest.raises(InternetArchiveUploadError) as failure:
        await resolve(
            item_client.upload(
                [
                    InternetArchiveUploadFile(stream, name)
                    for stream, name in zip(streams, ("a", "b"), strict=True)
                ],
                options(),
                wait=stage == "poll",
                poll_interval=0.001,
            )
        )
    error = failure.value
    assert error.status_code == status
    assert isinstance(error.cause, ServiceError)
    if status == HTTPStatus.TOO_MANY_REQUESTS:
        assert isinstance(error.cause, RateLimitError)
        assert error.cause.retry_after == RETRY_AFTER
    assert (
        error.failed_file
        == {"creation": None, "first": "a", "second": "b", "poll": None}[stage]
    )
    assert [file.transferred for file in error.result.files] == {
        "creation": [False, False],
        "first": [False, False],
        "second": [True, False],
        "poll": [True, True],
    }[stage]
    assert not error.result.processing_complete
    assert len(ia_endpoints.matching(path, method)) == 1
    assert not ia_endpoints.matching("/redirect-target")
    assert all(not stream.closed for stream in streams)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body", ["{}", '{"success":true,"identifier":"different"}', "[]", "not json"]
)
async def test_availability_malformed_success_never_mutates(
    ia_endpoints: ServerState,
    item_client: Client,
    body: str,
) -> None:
    """Reject ambiguous availability acknowledgements before creating a bucket."""
    ia_endpoints.item_responses["POST", "/ia/upload-api"] = (200, body, {})
    with pytest.raises(InvalidServiceResponseError):
        await resolve(
            item_client.upload(
                [InternetArchiveUploadFile(BytesIO(b"a"), "a")], options()
            )
        )
    assert not any(request.method == "PUT" for request in ia_endpoints.requests)


@pytest.mark.asyncio
@pytest.mark.parametrize("authentication", ["key", "cookies", "account"])
async def test_remove_items_requires_account_and_reports_queue_acceptance(
    ia_endpoints: ServerState,
    item_client: Client,
    authentication: str,
) -> None:
    """Use the make_dark form, not file deletion, and require account cookies."""
    if authentication == "key":
        with pytest.raises(AuthenticationError):
            await resolve(item_client.remove_items(["first", "second"]))
        assert not ia_endpoints.requests
        return
    if authentication == "cookies":
        item_client._cookies = InternetArchiveCookies("user", "signature")
    else:
        item_client._account = InternetArchiveAccount("user", "password")
    results = await resolve(
        item_client.remove_items(
            (name for name in ("first", "second")), comment="Test \u96ea"
        )
    )
    assert [
        (result.identifier, result.accepted, result.task_id) for result in results
    ] == [
        ("first", True, "1"),
        ("second", True, "2"),
    ]
    request = ia_endpoints.matching("/ia/manage/", "POST")[0]
    assert request.form == {
        "identifier": ["first,second"],
        "admin": ["make_dark"],
        "curation[state]": ["dark"],
        "curation[comment]": ["Test \u96ea"],
    }
    assert "logged-in-user=" in request.headers["Cookie"]
    assert not any(request.method == "DELETE" for request in ia_endpoints.requests)


@pytest.mark.asyncio
async def test_removal_partial_acknowledgement_is_not_implied_success(
    ia_endpoints: ServerState,
    item_client: Client,
) -> None:
    """Retain unknown removal outcomes and ignore unrequested acknowledgements."""
    item_client._cookies = InternetArchiveCookies("user", "signature")
    ia_endpoints.item_responses["POST", "/ia/manage/"] = (
        200,
        "<p>Item: 'first' queued for \"make_dark\" operation - task ID: 42</p>"
        "<p>Item: 'other' queued for \"make_dark\" operation - task ID: 43</p>",
        {},
    )
    results = await resolve(item_client.remove_items(["first", "second"]))
    assert [(result.accepted, result.task_id) for result in results] == [
        (True, "42"),
        (False, None),
    ]
    assert ia_endpoints.matching("/ia/manage/")[0].form["curation[comment]"] == [
        "Removed with Archivist"
    ]
