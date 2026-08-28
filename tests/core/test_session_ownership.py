"""Test client ownership rules for injected and internal sessions."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol, cast

import niquests
import pytest

from archivist import (
    ArchiveTodayClient,
    AsyncArchiveTodayClient,
    AsyncInternetArchiveClient,
    InternetArchiveClient,
    InvalidOptionError,
    InvalidTargetURLError,
)


class SyncClient(Protocol):
    """Describe lifecycle operations shared by synchronous clients."""

    def __enter__(self) -> SyncClient:
        """Enter the client context."""
        ...

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        """Exit the client context."""
        ...

    def close(self) -> None:
        """Close the client."""
        ...


class AsyncClient(Protocol):
    """Describe lifecycle operations shared by asynchronous clients."""

    async def __aenter__(self) -> AsyncClient:
        """Enter the asynchronous client context."""
        ...

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        """Exit the asynchronous client context."""
        ...

    async def close(self) -> None:
        """Close the asynchronous client."""
        ...


class SyncSession:
    """Track whether a synchronous test session was closed."""

    def __init__(self) -> None:
        """Initialize the session as open."""
        self.closed = False

    def close(self) -> None:
        """Mark the session as closed."""
        self.closed = True


class AsyncSession:
    """Track whether an asynchronous test session was closed."""

    def __init__(self) -> None:
        """Initialize the session as open."""
        self.closed = False

    async def close(self) -> None:
        """Mark the session as closed."""
        self.closed = True


def test_archive_today_clients_validate_before_creating_sessions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject invalid options without allocating an internal session."""

    def unexpected_session(**_options: object) -> None:
        raise AssertionError("session created before option validation")

    monkeypatch.setattr(niquests, "Session", unexpected_session)
    monkeypatch.setattr(niquests, "AsyncSession", unexpected_session)

    for client_type in (ArchiveTodayClient, AsyncArchiveTodayClient):
        with pytest.raises(InvalidOptionError):
            client_type(timeout=0)
        with pytest.raises(InvalidTargetURLError):
            client_type(mirror="")


@pytest.mark.parametrize(
    "client_type,kwargs",
    [
        (InternetArchiveClient, {}),
        (ArchiveTodayClient, {"mirror": "https://archive.is"}),
    ],
)
def test_sync_clients_do_not_close_injected_sessions(
    client_type: Callable[..., SyncClient], kwargs: dict[str, object]
) -> None:
    """Leave injected synchronous sessions open when clients close."""
    session = SyncSession()
    client = client_type(session=cast("niquests.Session", session), **kwargs)
    with client:
        pass
    assert session.closed is False
    with pytest.raises(RuntimeError, match="closed"):
        client.__enter__()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "client_type,kwargs",
    [
        (AsyncInternetArchiveClient, {}),
        (AsyncArchiveTodayClient, {"mirror": "https://archive.is"}),
    ],
)
async def test_async_clients_do_not_close_injected_sessions(
    client_type: Callable[..., AsyncClient], kwargs: dict[str, object]
) -> None:
    """Leave injected asynchronous sessions open when clients close."""
    session = AsyncSession()
    client = client_type(session=cast("niquests.AsyncSession", session), **kwargs)
    async with client:
        pass
    assert session.closed is False
    with pytest.raises(RuntimeError, match="closed"):
        await client.__aenter__()


@pytest.mark.parametrize(
    "client_type,kwargs",
    [
        (InternetArchiveClient, {}),
        (ArchiveTodayClient, {"mirror": "https://archive.is"}),
    ],
)
def test_sync_clients_close_sessions_they_create(
    monkeypatch: pytest.MonkeyPatch,
    client_type: Callable[..., SyncClient],
    kwargs: dict[str, object],
) -> None:
    """Close synchronous sessions created internally by clients."""
    session = SyncSession()
    monkeypatch.setattr(niquests, "Session", lambda **options: session)
    client = client_type(**kwargs)
    client.close()
    assert session.closed is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "client_type,kwargs",
    [
        (AsyncInternetArchiveClient, {}),
        (AsyncArchiveTodayClient, {"mirror": "https://archive.is"}),
    ],
)
async def test_async_clients_close_sessions_they_create(
    monkeypatch: pytest.MonkeyPatch,
    client_type: Callable[..., AsyncClient],
    kwargs: dict[str, object],
) -> None:
    """Close asynchronous sessions created internally by clients."""
    session = AsyncSession()
    monkeypatch.setattr(niquests, "AsyncSession", lambda **options: session)
    client = client_type(**kwargs)
    await client.close()
    assert session.closed is True
