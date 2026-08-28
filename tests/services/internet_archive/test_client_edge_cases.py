"""Exercise Internet Archive client lifecycle, validation, and polling edges."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from inspect import iscoroutinefunction
from types import SimpleNamespace
from typing import Any, cast

import niquests
import pytest

from archivist import (
    AsyncInternetArchiveClient,
    AuthenticationError,
    InternetArchiveAccount,
    InternetArchiveApiKey,
    InternetArchiveCaptureJob,
    InternetArchiveClient,
    InternetArchiveCookies,
    InternetArchivePendingStatus,
    InternetArchiveSaveOptions,
    InternetArchiveSuccessStatus,
    InvalidOptionError,
    NetworkError,
    OptionCombinationError,
    PollingTimeoutError,
)
from archivist.services.internet_archive import async_client as async_module
from archivist.services.internet_archive import client as sync_module


class StubResponse:
    """Provide the response attributes consumed by both clients."""

    def __init__(
        self, payload: object, *, headers: dict[str, str] | None = None
    ) -> None:
        """Initialize a successful response around a JSON payload."""
        self.status_code = 200
        self.headers = headers or {}
        self.text = ""
        self.url = "https://example.invalid"
        self.payload = payload

    def json(self) -> object:
        """Return the configured JSON payload."""
        return self.payload


class SyncSession:
    """Record synchronous transport operations and return queued responses."""

    def __init__(
        self, responses: list[StubResponse | BaseException] | None = None
    ) -> None:
        """Initialize the session with queued responses or exceptions."""
        self.responses = responses or []
        self.requests: list[tuple[str, str, dict[str, object]]] = []
        self.gathered: list[StubResponse] = []
        self.closed = False

    def request(self, method: str, url: str, **kwargs: object) -> StubResponse:
        """Record a request and return or raise the next queued result."""
        self.requests.append((method, url, kwargs))
        result = self.responses.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result

    def gather(self, response: StubResponse) -> None:
        """Record a gathered multiplexed response."""
        self.gathered.append(response)

    def close(self) -> None:
        """Mark the session as closed."""
        self.closed = True


class AsyncSession:
    """Record asynchronous transport operations and return queued responses."""

    def __init__(
        self, responses: list[StubResponse | BaseException] | None = None
    ) -> None:
        """Initialize the session with queued responses or exceptions."""
        self.responses = responses or []
        self.requests: list[tuple[str, str, dict[str, object]]] = []
        self.gathered: list[StubResponse] = []
        self.closed = False

    async def request(self, method: str, url: str, **kwargs: object) -> StubResponse:
        """Record a request and return or raise the next queued result."""
        self.requests.append((method, url, kwargs))
        result = self.responses.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result

    async def gather(self, response: StubResponse) -> None:
        """Record a gathered multiplexed response."""
        self.gathered.append(response)

    async def close(self) -> None:
        """Mark the session as closed."""
        self.closed = True


def as_sync_session(session: SyncSession) -> niquests.Session:
    """Cast the synchronous test double to the client session interface."""
    return cast("niquests.Session", session)


def as_async_session(session: AsyncSession) -> niquests.AsyncSession:
    """Cast the asynchronous test double to the client session interface."""
    return cast("niquests.AsyncSession", session)


def key() -> InternetArchiveApiKey:
    """Return valid API credentials for isolated client tests."""
    return InternetArchiveApiKey("access", "secret")


def test_async_wait_and_save_are_coroutine_functions() -> None:
    """Expose public asynchronous operations as coroutine functions."""
    assert iscoroutinefunction(AsyncInternetArchiveClient.wait)
    assert iscoroutinefunction(AsyncInternetArchiveClient.save)


def success(job_id: str = "job") -> InternetArchiveSuccessStatus:
    """Build a successful capture status for the requested job."""
    return InternetArchiveSuccessStatus(
        job_id, "https://example.com", datetime(2020, 1, 1, tzinfo=UTC)
    )


def test_sync_lifecycle_transport_and_header_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cover synchronous ownership, closure, headers, and transport errors."""
    external = SyncSession([OSError("private transport detail")])
    client = InternetArchiveClient(session=as_sync_session(external))
    assert client._headers() == {"Accept": "application/json"}
    assert client._request_cookies() is None
    with pytest.raises(NetworkError) as failure:
        client._request("GET", "https://user:secret@example.invalid/path")
    assert "private transport detail" not in str(failure.value)
    client.close()
    client.close()
    assert external.closed is False
    with pytest.raises(RuntimeError, match="closed"):
        client.__enter__()

    owned = SyncSession()
    monkeypatch.setattr(sync_module.niquests, "Session", lambda **kwargs: owned)
    owned_client = InternetArchiveClient()
    owned_client.close()
    assert owned.closed is True


@pytest.mark.asyncio
async def test_async_lifecycle_transport_and_header_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cover asynchronous ownership, closure, headers, and transport errors."""
    external = AsyncSession([OSError("private transport detail")])
    client = AsyncInternetArchiveClient(session=as_async_session(external))
    assert client._headers() == {"Accept": "application/json"}
    assert client._request_cookies() is None
    with pytest.raises(NetworkError) as failure:
        await client._request("GET", "https://user:secret@example.invalid/path")
    assert "private transport detail" not in str(failure.value)
    await client.close()
    await client.close()
    assert external.closed is False
    with pytest.raises(RuntimeError, match="closed"):
        await client.__aenter__()

    owned = AsyncSession()
    monkeypatch.setattr(async_module.niquests, "AsyncSession", lambda **kwargs: owned)
    owned_client = AsyncInternetArchiveClient()
    await owned_client.close()
    assert owned.closed is True


@pytest.mark.parametrize(
    "token_payload",
    [
        {"success": False, "value": {"token": "token"}},
        {"success": True, "value": []},
        {"success": True, "value": {"token": 1}},
        {"success": True, "value": {"token": ""}},
    ],
)
def test_sync_login_rejects_every_invalid_csrf_shape(token_payload: object) -> None:
    """Reject every malformed synchronous CSRF response shape."""
    session = SyncSession([StubResponse(token_payload)])
    client = InternetArchiveClient(
        session=as_sync_session(session),
        account=InternetArchiveAccount("user", "password"),
    )
    with pytest.raises(AuthenticationError, match="CSRF"):
        client.login()


@pytest.mark.parametrize(
    "token_payload",
    [
        {"success": False, "value": {"token": "token"}},
        {"success": True, "value": []},
        {"success": True, "value": {"token": 1}},
        {"success": True, "value": {"token": ""}},
    ],
)
@pytest.mark.asyncio
async def test_async_login_rejects_every_invalid_csrf_shape(
    token_payload: object,
) -> None:
    """Reject every malformed asynchronous CSRF response shape."""
    session = AsyncSession([StubResponse(token_payload)])
    client = AsyncInternetArchiveClient(
        session=as_async_session(session),
        account=InternetArchiveAccount("user", "password"),
    )
    with pytest.raises(AuthenticationError, match="CSRF"):
        await client.login()


def test_sync_login_failure_success_and_authentication_shortcuts() -> None:
    """Cover synchronous login outcomes and existing-auth shortcuts."""
    with pytest.raises(AuthenticationError, match="not configured"):
        InternetArchiveClient(session=as_sync_session(SyncSession())).login()

    failed = SyncSession(
        [StubResponse({"success": True, "value": {"token": "token"}}), StubResponse({})]
    )
    client = InternetArchiveClient(
        session=as_sync_session(failed),
        account=InternetArchiveAccount("user", "password"),
    )
    with pytest.raises(AuthenticationError, match="login failed"):
        client.login()

    session = SyncSession(
        [
            StubResponse({"success": True, "value": {"token": "token"}}),
            StubResponse({"success": True}),
        ]
    )
    client = InternetArchiveClient(
        session=as_sync_session(session),
        account=InternetArchiveAccount("user", "password", remember=False),
    )
    client.login()
    client.login()
    client._ensure_api_authentication()
    client._ensure_account_authentication()
    login_payload = cast("dict[str, object]", session.requests[1][2]["json"])
    assert login_payload["remember"] == "false"

    cookie_client = InternetArchiveClient(
        session=as_sync_session(SyncSession()),
        cookies=InternetArchiveCookies("user", "signature"),
    )
    cookie_client._ensure_api_authentication()
    cookie_client._ensure_account_authentication()


@pytest.mark.asyncio
async def test_async_login_failure_success_and_authentication_shortcuts() -> None:
    """Cover asynchronous login outcomes and existing-auth shortcuts."""
    with pytest.raises(AuthenticationError, match="not configured"):
        await AsyncInternetArchiveClient(
            session=as_async_session(AsyncSession())
        ).login()

    failed = AsyncSession(
        [StubResponse({"success": True, "value": {"token": "token"}}), StubResponse({})]
    )
    client = AsyncInternetArchiveClient(
        session=as_async_session(failed),
        account=InternetArchiveAccount("user", "password"),
    )
    with pytest.raises(AuthenticationError, match="login failed"):
        await client.login()

    session = AsyncSession(
        [
            StubResponse({"success": True, "value": {"token": "token"}}),
            StubResponse({"success": True}),
        ]
    )
    client = AsyncInternetArchiveClient(
        session=as_async_session(session),
        account=InternetArchiveAccount("user", "password", remember=False),
    )
    await client.login()
    await client.login()
    await client._ensure_api_authentication()
    await client._ensure_account_authentication()
    login_payload = cast("dict[str, object]", session.requests[1][2]["json"])
    assert login_payload["remember"] == "false"

    cookie_client = AsyncInternetArchiveClient(
        session=as_async_session(AsyncSession()),
        cookies=InternetArchiveCookies("user", "signature"),
    )
    await cookie_client._ensure_api_authentication()
    await cookie_client._ensure_account_authentication()

    account_client = AsyncInternetArchiveClient(
        session=as_async_session(
            AsyncSession(
                [
                    StubResponse({"success": True, "value": {"token": "token"}}),
                    StubResponse({"success": True}),
                ]
            )
        ),
        account=InternetArchiveAccount("user", "password"),
    )
    await account_client._ensure_api_authentication()


def test_sync_status_validation_authentication_and_public_wrapper() -> None:
    """Validate synchronous status inputs, authentication, and wrappers."""
    session = SyncSession()
    client = InternetArchiveClient(session=as_sync_session(session), api_key=key())
    for call in (
        lambda: client.status(""),
        lambda: client.status_batch([]),
        lambda: client.status_batch(["job", ""]),
        lambda: client.status_outlinks(""),
    ):
        with pytest.raises(InvalidOptionError):
            call()

    response = StubResponse({"status": "pending", "job_id": "job", "resources": []})
    session.responses.append(response)
    assert client.status("job") == InternetArchivePendingStatus("job")

    unauthenticated = InternetArchiveClient(session=as_sync_session(SyncSession()))
    with pytest.raises(AuthenticationError, match="account cookies"):
        unauthenticated.add_to_my_web_archive(success())


@pytest.mark.asyncio
async def test_async_status_validation_authentication_and_public_wrapper() -> None:
    """Validate asynchronous status inputs, authentication, and wrappers."""
    session = AsyncSession()
    client = AsyncInternetArchiveClient(
        session=as_async_session(session), api_key=key()
    )
    calls: tuple[Callable[[], Awaitable[object]], ...] = (
        lambda: client.status(""),
        lambda: client.status_batch([]),
        lambda: client.status_batch(["job", ""]),
        lambda: client.status_outlinks(""),
    )
    for call in calls:
        with pytest.raises(InvalidOptionError):
            await call()

    response = StubResponse({"status": "pending", "job_id": "job", "resources": []})
    session.responses.append(response)
    assert await client.status("job") == InternetArchivePendingStatus("job")

    unauthenticated = AsyncInternetArchiveClient(
        session=as_async_session(AsyncSession())
    )
    with pytest.raises(AuthenticationError, match="credentials are required"):
        await unauthenticated.user_status()
    with pytest.raises(AuthenticationError, match="selected options"):
        await unauthenticated.submit(
            "https://example.com",
            InternetArchiveSaveOptions(capture_screenshot=True),
        )
    with pytest.raises(AuthenticationError, match="account cookies"):
        await unauthenticated.add_to_my_web_archive(success())


def test_sync_wait_validates_jobs_and_all_timeout_locations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validate jobs and both synchronous polling timeout locations."""
    client = InternetArchiveClient(
        session=as_sync_session(SyncSession()), api_key=key()
    )
    with pytest.raises(InvalidOptionError):
        client.wait("")

    monkeypatch.setattr(sync_module.time, "monotonic", iter([0.0, 2.0]).__next__)
    with pytest.raises(PollingTimeoutError):
        client.wait(InternetArchiveCaptureJob("job", "https://example.com"), timeout=1)

    monkeypatch.setattr(
        client,
        "_status",
        lambda job_id, request_timeout=None: InternetArchivePendingStatus(job_id),
    )
    monkeypatch.setattr(sync_module.time, "monotonic", iter([0.0, 0.5, 2.0]).__next__)
    with pytest.raises(PollingTimeoutError):
        client.wait("job", timeout=1)


@pytest.mark.asyncio
async def test_async_wait_validates_jobs_and_all_timeout_locations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validate jobs and both asynchronous polling timeout locations."""
    client = AsyncInternetArchiveClient(
        session=as_async_session(AsyncSession()), api_key=key()
    )
    with pytest.raises(InvalidOptionError):
        await client.wait("")

    monkeypatch.setattr(
        async_module, "time", SimpleNamespace(monotonic=iter([0.0, 2.0]).__next__)
    )
    with pytest.raises(PollingTimeoutError):
        await client.wait(
            InternetArchiveCaptureJob("job", "https://example.com"), timeout=1
        )

    async def pending(
        job_id: str, *, request_timeout: float | None = None
    ) -> InternetArchivePendingStatus:
        return InternetArchivePendingStatus(job_id)

    monkeypatch.setattr(client, "_status", pending)
    monkeypatch.setattr(
        async_module, "time", SimpleNamespace(monotonic=iter([0.0, 0.5, 2.0]).__next__)
    )
    with pytest.raises(PollingTimeoutError):
        await client.wait("job", timeout=1)


@pytest.mark.parametrize(
    ("error", "clock", "wait_timeout", "becomes_timeout"),
    [
        (NetworkError("network", cause_type="ReadTimeout"), [0.0, 0.5], 1, True),
        (NetworkError("network", cause_type="Other"), [0.0, 0.5, 2.0], 1, True),
        (NetworkError("network", cause_type=None), [0.0, 0.5, 0.6], 1, False),
        (
            NetworkError("network", cause_type="ReadTimeout"),
            [0.0, 35.0, 35.1],
            70,
            False,
        ),
    ],
)
def test_sync_wait_translates_only_deadline_network_errors(
    monkeypatch: pytest.MonkeyPatch,
    error: NetworkError,
    clock: list[float],
    wait_timeout: float,
    becomes_timeout: bool,
) -> None:
    """Translate only synchronous network errors caused by the deadline."""
    client = InternetArchiveClient(
        session=as_sync_session(SyncSession()), api_key=key(), timeout=30
    )

    def fail(job_id: str, *, request_timeout: float | None = None) -> object:
        raise error

    monkeypatch.setattr(client, "_status", fail)
    monkeypatch.setattr(sync_module.time, "monotonic", iter(clock).__next__)
    expected = PollingTimeoutError if becomes_timeout else NetworkError
    with pytest.raises(expected):
        client.wait("job", timeout=wait_timeout)


@pytest.mark.parametrize(
    ("error", "clock", "wait_timeout", "becomes_timeout"),
    [
        (NetworkError("network", cause_type="ReadTimeout"), [0.0, 0.5], 1, True),
        (NetworkError("network", cause_type="Other"), [0.0, 0.5, 2.0], 1, True),
        (NetworkError("network", cause_type=None), [0.0, 0.5, 0.6], 1, False),
        (
            NetworkError("network", cause_type="ReadTimeout"),
            [0.0, 35.0, 35.1],
            70,
            False,
        ),
    ],
)
@pytest.mark.asyncio
async def test_async_wait_translates_only_deadline_network_errors(
    monkeypatch: pytest.MonkeyPatch,
    error: NetworkError,
    clock: list[float],
    wait_timeout: float,
    becomes_timeout: bool,
) -> None:
    """Translate only asynchronous network errors caused by the deadline."""
    client = AsyncInternetArchiveClient(
        session=as_async_session(AsyncSession()), api_key=key(), timeout=30
    )

    async def fail(job_id: str, *, request_timeout: float | None = None) -> object:
        raise error

    monkeypatch.setattr(client, "_status", fail)
    monkeypatch.setattr(
        async_module, "time", SimpleNamespace(monotonic=iter(clock).__next__)
    )
    expected = PollingTimeoutError if becomes_timeout else NetworkError
    with pytest.raises(expected):
        await client.wait("job", timeout=wait_timeout)


def test_sync_wait_repeated_state_and_defensive_assertion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Handle repeated synchronous states and reject unknown terminal states."""
    client = InternetArchiveClient(
        session=as_sync_session(SyncSession()), api_key=key()
    )
    states = iter(
        [
            InternetArchivePendingStatus("job"),
            InternetArchivePendingStatus("job"),
            success(),
        ]
    )
    monkeypatch.setattr(
        client, "_status", lambda job_id, request_timeout=None: next(states)
    )
    monkeypatch.setattr(sync_module.time, "sleep", lambda delay: None)
    monkeypatch.setattr(
        sync_module.time,
        "monotonic",
        iter([0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6]).__next__,
    )
    assert client.wait("job", timeout=1, poll_interval=0.01).status == "success"

    class UnknownStatus:
        status = "unknown"

    monkeypatch.setattr(
        client, "_status", lambda job_id, request_timeout=None: UnknownStatus()
    )
    monkeypatch.setattr(sync_module.time, "monotonic", iter([0.0, 0.1, 0.2]).__next__)
    with pytest.raises(AssertionError, match="unreachable"):
        client.wait("job", timeout=1)


@pytest.mark.asyncio
async def test_async_wait_repeated_state_and_defensive_assertion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Handle repeated asynchronous states and reject unknown terminal states."""
    client = AsyncInternetArchiveClient(
        session=as_async_session(AsyncSession()), api_key=key()
    )
    states = iter(
        [
            InternetArchivePendingStatus("job"),
            InternetArchivePendingStatus("job"),
            success(),
        ]
    )

    async def next_status(
        job_id: str, *, request_timeout: float | None = None
    ) -> object:
        return next(states)

    async def no_sleep(delay: float) -> None:
        return None

    monkeypatch.setattr(client, "_status", next_status)
    monkeypatch.setattr(async_module.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(
        async_module,
        "time",
        SimpleNamespace(monotonic=iter([0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6]).__next__),
    )
    assert (await client.wait("job", timeout=1, poll_interval=0.01)).status == "success"

    class UnknownStatus:
        status = "unknown"

    async def unknown(job_id: str, *, request_timeout: float | None = None) -> object:
        return UnknownStatus()

    monkeypatch.setattr(client, "_status", unknown)
    monkeypatch.setattr(
        async_module, "time", SimpleNamespace(monotonic=iter([0.0, 0.1, 0.2]).__next__)
    )
    with pytest.raises(AssertionError, match="unreachable"):
        await client.wait("job", timeout=1)


def test_sync_save_and_query_optional_branches() -> None:
    """Cover optional synchronous save, availability, and search branches."""
    client = InternetArchiveClient(
        session=as_sync_session(SyncSession()), api_key=key()
    )
    with pytest.raises(OptionCombinationError):
        client.save("https://example.com", tags=("tag",))

    system_session = SyncSession([StubResponse({"status": "ok"})])
    system_client = InternetArchiveClient(session=as_sync_session(system_session))
    assert system_client.system_status().status == "ok"
    assert system_session.requests[0][2]["headers"] is None

    availability_session = SyncSession(
        [StubResponse({"url": "https://example.com", "archived_snapshots": {}})]
    )
    availability_client = InternetArchiveClient(
        session=as_sync_session(availability_session)
    )
    assert availability_client.availability("https://example.com").closest is None
    assert availability_session.requests[0][2]["params"] == {
        "url": "https://example.com"
    }

    search_session = SyncSession([StubResponse([["timestamp", "original"]])])
    search_client = InternetArchiveClient(session=as_sync_session(search_session))
    result = search_client.search(
        "https://example.com",
        match_type="prefix",
        from_timestamp="2020",
        to_timestamp="2021",
        limit=-2,
        resume_key="resume",
        fast_latest=True,
    )
    assert len(result) == 0
    params = cast("list[tuple[str, str]]", search_session.requests[0][2]["params"])
    assert ("from", "2020") in params
    assert ("to", "2021") in params
    assert ("resumeKey", "resume") in params
    assert ("fastLatest", "true") in params

    for kwargs in (
        {"match_type": "bad"},
        {"limit": True},
        {"limit": 0},
        {"resume_key": ""},
    ):
        with pytest.raises(InvalidOptionError):
            cast("Any", search_client.search)("https://example.com", **kwargs)


@pytest.mark.asyncio
async def test_async_save_and_query_optional_branches() -> None:
    """Cover optional asynchronous save, availability, and search branches."""
    client = AsyncInternetArchiveClient(
        session=as_async_session(AsyncSession()), api_key=key()
    )
    with pytest.raises(OptionCombinationError):
        await client.save("https://example.com", tags=("tag",))
    with pytest.raises(AuthenticationError, match="My Web Archive"):
        await AsyncInternetArchiveClient(
            session=as_async_session(AsyncSession()), api_key=key()
        ).save("https://example.com", InternetArchiveSaveOptions(save_to_archive=True))

    system_session = AsyncSession([StubResponse({"status": "ok"})])
    system_client = AsyncInternetArchiveClient(session=as_async_session(system_session))
    assert (await system_client.system_status()).status == "ok"
    assert system_session.requests[0][2]["headers"] is None

    availability_session = AsyncSession(
        [StubResponse({"url": "https://example.com", "archived_snapshots": {}})]
    )
    availability_client = AsyncInternetArchiveClient(
        session=as_async_session(availability_session)
    )
    await availability_client.availability("https://example.com", timestamp="2020")
    assert availability_session.requests[0][2]["params"] == {
        "url": "https://example.com",
        "timestamp": "2020",
    }

    search_session = AsyncSession([StubResponse([["timestamp", "original"]])])
    search_client = AsyncInternetArchiveClient(session=as_async_session(search_session))
    result = await search_client.search(
        "https://example.com",
        match_type="domain",
        from_timestamp="2020",
        to_timestamp="2021",
        limit=-2,
        resume_key="resume",
        fast_latest=True,
    )
    assert len(result) == 0
    params = cast("list[tuple[str, str]]", search_session.requests[0][2]["params"])
    assert ("from", "2020") in params
    assert ("to", "2021") in params
    assert ("resumeKey", "resume") in params
    assert ("fastLatest", "true") in params

    for kwargs in (
        {"match_type": "bad"},
        {"limit": True},
        {"limit": 0},
        {"resume_key": ""},
    ):
        with pytest.raises(InvalidOptionError):
            await cast("Any", search_client.search)("https://example.com", **kwargs)
