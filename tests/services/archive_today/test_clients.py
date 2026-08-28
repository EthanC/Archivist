"""Client tests for Archive.today's stable read interfaces."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol, cast

import niquests
import pytest

from archivist import (
    ArchiveTodayClient,
    AsyncArchiveTodayClient,
    InvalidServiceResponseError,
    NetworkError,
)

EXPECTED_MEMENTOS = 2
EXPECTED_REQUESTS = 4


class RecordedRequest(Protocol):
    """Describe request fields inspected by this module."""

    method: str
    headers: dict[str, str]


class ArchiveServer(Protocol):
    """Describe the shared fixture server state."""

    base_url: str
    requests: list[RecordedRequest]

    def matching(self, path: str) -> list[RecordedRequest]:
        """Return requests matching a path."""
        ...


def test_sync_client_reads_memento_and_rss_endpoints(
    archive_server: ArchiveServer,
) -> None:
    """Read each stable interface without issuing a state-changing request."""
    state = archive_server
    base_url = state.base_url
    with ArchiveTodayClient(mirror=base_url) as client:
        timemap = client.timemap("https://example.com/")
        closest = client.closest(
            "https://example.com/", datetime(2020, 1, 1, tzinfo=UTC)
        )
        first = client.first("https://example.com/")
        last = client.last("https://example.com/")
        feed = client.recent_captures()

    assert len(timemap) == EXPECTED_MEMENTOS
    assert timemap.first is timemap.items[0]
    assert timemap.last is timemap.items[1]
    assert closest is not None
    assert closest.archived_at == datetime(2020, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert first == timemap.first
    assert last == timemap.last
    assert feed.title == "archive.test"
    assert feed.updated_at == datetime(2021, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert len(feed) == 1
    assert feed.items[0].capture_id == "abcde"
    assert feed.items[0].description == "Standard RSS description"
    assert all(request.method == "GET" for request in state.requests)
    timegate = state.matching("/timegate/https://example.com/")[0]
    assert timegate.headers["Accept-Datetime"] == "Wed, 01 Jan 2020 00:00:00 GMT"


@pytest.mark.asyncio
async def test_async_client_matches_sync_behavior(
    archive_server: ArchiveServer,
) -> None:
    """Return the same models from asynchronous operations."""
    state = archive_server
    async with AsyncArchiveTodayClient(mirror=state.base_url) as client:
        timemap = await client.timemap("https://example.com/")
        closest = await client.closest(
            "https://example.com/", datetime(2020, 1, 1, tzinfo=UTC)
        )
        first = await client.first("https://example.com/")
        last = await client.last("https://example.com/")
        feed = await client.recent_captures()

    assert len(timemap) == EXPECTED_MEMENTOS
    assert closest is not None
    assert first is not None
    assert closest.archive_url == first.archive_url
    assert last == timemap.last
    assert len(feed) == 1


@dataclass
class StubResponse:
    """Represent response fields consumed by the clients."""

    status_code: int
    headers: Mapping[str, str] = field(default_factory=dict)
    text: str = ""
    url: str = "https://archive.is/test"

    def json(self) -> object:
        """Reject unexpected JSON decoding."""
        raise NotImplementedError


class StubSession:
    """Return queued synchronous responses or one transport error."""

    def __init__(
        self,
        responses: list[StubResponse] | None = None,
        *,
        error: OSError | None = None,
    ) -> None:
        """Initialize queued responses and an optional transport error."""
        self.responses = list(responses or [])
        self.error = error
        self.gathered = 0

    def get(self, url: str, **kwargs: object) -> StubResponse:
        """Return the next response."""
        if self.error is not None:
            raise self.error
        return self.responses.pop(0)

    def gather(self, response: object) -> None:
        """Record eager response gathering."""
        self.gathered += 1

    def close(self) -> None:
        """Provide the injected-session close interface."""
        return


class AsyncStubSession:
    """Return queued asynchronous responses or one transport error."""

    def __init__(
        self,
        responses: list[StubResponse] | None = None,
        *,
        error: OSError | None = None,
    ) -> None:
        """Initialize queued responses and an optional transport error."""
        self.responses = list(responses or [])
        self.error = error
        self.gathered = 0

    async def get(self, url: str, **kwargs: object) -> StubResponse:
        """Return the next response."""
        if self.error is not None:
            raise self.error
        return self.responses.pop(0)

    async def gather(self, response: object) -> None:
        """Record eager response gathering."""
        self.gathered += 1

    async def close(self) -> None:
        """Provide the injected-session close interface."""
        return


def test_sync_client_logs_routes_without_target_credentials(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Keep credentials in embedded target URLs out of synchronous logs."""
    session = StubSession([StubResponse(404) for _ in range(EXPECTED_REQUESTS)])
    client = ArchiveTodayClient(session=cast("niquests.Session", session))
    target = "https://target-user:target-password@example.com/private"

    with caplog.at_level(
        logging.DEBUG, logger="archivist.services.archive_today.client"
    ):
        client.timemap(target)
        client.closest(target, datetime(2020, 1, 1, tzinfo=UTC))
        client.first(target)
        client.last(target)

    messages = "\n".join(
        record.getMessage()
        for record in caplog.records
        if record.name == "archivist.services.archive_today.client"
    )
    for route in ("timemap", "timegate", "oldest", "newest"):
        assert f"GET https://archive.is/{route}" in messages
    assert "target-user" not in messages
    assert "target-password" not in messages


@pytest.mark.asyncio
async def test_async_client_logs_routes_without_target_credentials(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Keep credentials in embedded target URLs out of asynchronous logs."""
    session = AsyncStubSession([StubResponse(404) for _ in range(EXPECTED_REQUESTS)])
    client = AsyncArchiveTodayClient(session=cast("niquests.AsyncSession", session))
    target = "https://target-user:target-password@example.com/private"

    with caplog.at_level(
        logging.DEBUG, logger="archivist.services.archive_today.async_client"
    ):
        await client.timemap(target)
        await client.closest(target, datetime(2020, 1, 1, tzinfo=UTC))
        await client.first(target)
        await client.last(target)

    messages = "\n".join(
        record.getMessage()
        for record in caplog.records
        if record.name == "archivist.services.archive_today.async_client"
    )
    for route in ("timemap", "timegate", "oldest", "newest"):
        assert f"GET https://archive.is/{route}" in messages
    assert "target-user" not in messages
    assert "target-password" not in messages


def test_sync_client_handles_absence_unexpected_status_and_transport_errors() -> None:
    """Map missing records and reject unusable responses."""
    session = StubSession(
        [StubResponse(404), StubResponse(404), StubResponse(204), StubResponse(204)]
    )
    client = ArchiveTodayClient(session=cast("niquests.Session", session))
    assert client.timemap("https://example.com/").items == ()
    assert client.first("https://example.com/") is None
    with pytest.raises(InvalidServiceResponseError, match="unexpected HTTP"):
        client.last("https://example.com/")
    with pytest.raises(InvalidServiceResponseError, match="unexpected HTTP"):
        client.recent_captures()
    assert session.gathered == EXPECTED_REQUESTS

    failing = ArchiveTodayClient(
        session=cast("niquests.Session", StubSession(error=OSError("offline")))
    )
    with pytest.raises(NetworkError):
        failing.timemap("https://example.com/")

    detailed = ArchiveTodayClient(
        session=cast(
            "niquests.Session",
            StubSession(
                [
                    StubResponse(
                        400,
                        {"Content-Type": "text/html;charset=utf-8"},
                        "Invalid Accept-Datetime header `bad`",
                    )
                ]
            ),
        )
    )
    with pytest.raises(InvalidServiceResponseError, match="Invalid Accept-Datetime"):
        detailed.closest("https://example.com/", datetime(2020, 1, 1, tzinfo=UTC))


@pytest.mark.asyncio
async def test_async_client_handles_absence_unexpected_status_and_errors() -> None:
    """Map asynchronous missing records and transport failures."""
    session = AsyncStubSession(
        [StubResponse(404), StubResponse(404), StubResponse(204), StubResponse(204)]
    )
    client = AsyncArchiveTodayClient(session=cast("niquests.AsyncSession", session))
    assert (await client.timemap("https://example.com/")).items == ()
    assert await client.last("https://example.com/") is None
    with pytest.raises(InvalidServiceResponseError, match="unexpected HTTP"):
        await client.first("https://example.com/")
    with pytest.raises(InvalidServiceResponseError, match="unexpected HTTP"):
        await client.recent_captures()
    assert session.gathered == EXPECTED_REQUESTS

    failing = AsyncArchiveTodayClient(
        session=cast(
            "niquests.AsyncSession", AsyncStubSession(error=OSError("offline"))
        )
    )
    with pytest.raises(NetworkError):
        await failing.timemap("https://example.com/")

    detailed = AsyncArchiveTodayClient(
        session=cast(
            "niquests.AsyncSession",
            AsyncStubSession(
                [
                    StubResponse(
                        400,
                        {"Content-Type": "text/html;charset=utf-8"},
                        "Invalid Accept-Datetime header `bad`",
                    )
                ]
            ),
        )
    )
    with pytest.raises(InvalidServiceResponseError, match="Invalid Accept-Datetime"):
        await detailed.closest("https://example.com/", datetime(2020, 1, 1, tzinfo=UTC))


def test_removed_unstable_methods_are_not_exposed() -> None:
    """Keep capture submission and HTML search outside the stable client."""
    client = ArchiveTodayClient(session=cast("niquests.Session", StubSession()))
    assert not hasattr(client, "save")
    assert not hasattr(client, "search")
    client.close()
    client.close()


@pytest.mark.asyncio
async def test_async_close_is_idempotent() -> None:
    """Allow repeated asynchronous cleanup."""
    client = AsyncArchiveTodayClient(
        session=cast("niquests.AsyncSession", AsyncStubSession())
    )
    await client.close()
    await client.close()
