"""Exercise Internet Archive clients against the local endpoint fixture."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import niquests
import pytest

if TYPE_CHECKING:
    from tests.conftest import ServerState

from archivist import (
    AsyncInternetArchiveClient,
    AuthenticationError,
    CaptureFailedError,
    InternetArchiveAccount,
    InternetArchiveApiKey,
    InternetArchiveClient,
    InternetArchiveCookies,
    InternetArchiveFailedStatus,
    InternetArchivePendingStatus,
    InternetArchiveSaveOptions,
    InternetArchiveSuccessStatus,
    InvalidOptionError,
    PollingTimeoutError,
)

AVAILABLE_CAPTURES = 12
CDX_RECORD_LENGTH = 42


def api_key() -> InternetArchiveApiKey:
    """Return API credentials accepted by the endpoint fixture."""
    return InternetArchiveApiKey("test-access", "test-secret")


def test_sync_client_exercises_spn_status_and_wayback_apis(
    ia_endpoints: ServerState,
) -> None:
    """Exercise the synchronous SPN, status, availability, and CDX APIs."""
    options = InternetArchiveSaveOptions(capture_all=True, capture_screenshot=True)
    with InternetArchiveClient(api_key=api_key()) as client:
        capture = client.save(
            "https://example.com/path?private=query",
            options,
            timeout=1,
            poll_interval=0.001,
        )
        batch = client.status_batch(["a", "b"])
        outlinks = client.status_outlinks("job-1")
        user = client.user_status()
        system = client.system_status()
        available = client.availability(
            "https://example.com/", timestamp=datetime(2020, 1, 1, tzinfo=UTC)
        )
        cdx = client.search(
            "https://example.com/",
            filters=("statuscode:200", "mimetype:text/html"),
            collapse=("digest",),
            limit=10,
            show_resume_key=True,
        )

    assert isinstance(capture, InternetArchiveSuccessStatus)
    assert capture.original_url == "https://example.com/final"
    assert isinstance(batch[0], InternetArchivePendingStatus)
    assert isinstance(batch[1], InternetArchiveFailedStatus)
    assert outlinks[0].job_id == "child"
    assert (user.available, user.processing) == (12, 3)
    assert system.queues["spn2-api"] == 1
    assert available.closest is not None
    assert available.closest.timestamp == datetime(2020, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert cdx.resume_key == "resume-token"
    assert cdx.items[0].length == CDX_RECORD_LENGTH

    submission = ia_endpoints.matching("/ia/save", "POST")[0]
    assert submission.headers["Authorization"] == "LOW test-access:test-secret"
    assert submission.form["capture_all"] == ["1"]
    assert submission.form["capture_screenshot"] == ["1"]
    cdx_request = ia_endpoints.matching("/ia/cdx", "GET")[0]
    assert cdx_request.query["filter"] == ["statuscode:200", "mimetype:text/html"]
    assert cdx_request.query["showResumeKey"] == ["true"]


@pytest.mark.asyncio
async def test_async_client_matches_sync_transport_behavior(
    ia_endpoints: ServerState,
) -> None:
    """Verify that the asynchronous client matches synchronous behavior."""
    async with AsyncInternetArchiveClient(api_key=api_key()) as client:
        capture = await client.save(
            "https://example.com/async", timeout=1, poll_interval=0.001
        )
        batch = await client.status_batch(["a", "b"])
        outlinks = await client.status_outlinks("job-1")
        user = await client.user_status()
        system = await client.system_status()
        available = await client.availability("https://example.com/")
        cdx = await client.search("https://example.com/", show_resume_key=True)

    assert isinstance(capture, InternetArchiveSuccessStatus)
    assert [item.status for item in batch] == ["pending", "error"]
    assert outlinks[0].job_id == "child"
    assert user.available == AVAILABLE_CAPTURES
    assert system.status == "ok"
    assert available.closest is not None
    assert cdx.resume_key == "resume-token"


def test_sync_client_saves_anonymously(ia_endpoints: ServerState) -> None:
    """Submit and poll a capture without Internet Archive credentials."""
    with InternetArchiveClient() as client:
        capture = client.save(
            "https://example.com/anonymous",
            InternetArchiveSaveOptions(capture_all=True, capture_outlinks=True),
            timeout=1,
            poll_interval=0.001,
        )

    assert isinstance(capture, InternetArchiveSuccessStatus)
    submission = next(
        request
        for request in ia_endpoints.requests
        if request.path.startswith("/ia/save/")
    )
    assert "Authorization" not in submission.headers
    assert submission.form["url"] == ["https://example.com/anonymous"]
    assert submission.form["capture_all"] == ["1"]
    assert submission.form["capture_outlinks"] == ["1"]


@pytest.mark.asyncio
async def test_async_client_saves_anonymously(ia_endpoints: ServerState) -> None:
    """Submit and poll an anonymous capture through the asynchronous client."""
    async with AsyncInternetArchiveClient() as client:
        capture = await client.save(
            "https://example.com/anonymous-async",
            timeout=1,
            poll_interval=0.001,
        )

    assert isinstance(capture, InternetArchiveSuccessStatus)
    submission = next(
        request
        for request in ia_endpoints.requests
        if request.path.startswith("/ia/save/")
    )
    assert "Authorization" not in submission.headers


@pytest.mark.asyncio
async def test_multiplexed_async_session_is_gathered_before_parsing(
    ia_endpoints: ServerState,
) -> None:
    """Gather multiplexed responses before parsing their payloads."""
    session = niquests.AsyncSession(multiplexed=True, retries=0)
    try:
        client = AsyncInternetArchiveClient(session=session)
        available = await client.availability("https://example.com/")
    finally:
        await session.close()
    assert available.closest is not None


def test_account_login_is_lazy_and_my_web_archive_uses_capture_timestamp(
    ia_endpoints: ServerState,
) -> None:
    """Delay account login and submit the resulting capture timestamp."""
    account = InternetArchiveAccount("account@example.invalid", "account-password")
    options = InternetArchiveSaveOptions(save_to_archive=True)
    with InternetArchiveClient(api_key=api_key(), account=account) as client:
        result = client.save(
            "https://example.com/account",
            options,
            timeout=1,
            poll_interval=0.001,
            tags=("research",),
        )

    paths = [request.path for request in ia_endpoints.requests]
    assert paths.index("/ia/csrf") > paths.index("/ia/status/job-1")
    login = ia_endpoints.matching("/ia/login", "POST")[0]
    headers = {key.lower(): value for key, value in login.headers.items()}
    assert headers["x-csrf-token"] == "test-csrf-token"
    assert login.json == {
        "username": "account@example.invalid",
        "password": "account-password",
        "remember": "true",
        "t": "test-csrf-token",
    }
    my_archive = ia_endpoints.matching("/ia/mwa", "POST")[0]
    assert my_archive.headers["Content-Type"] == "application/json"
    assert my_archive.json == {
        "url": "https://example.com/final",
        "snapshot": result.wayback_timestamp,
        "tags": ["research"],
    }


@pytest.mark.asyncio
async def test_async_account_login_and_my_web_archive(
    ia_endpoints: ServerState,
) -> None:
    """Authenticate and add an asynchronous capture to My Web Archive."""
    options = InternetArchiveSaveOptions(save_to_archive=True)
    async with AsyncInternetArchiveClient(
        api_key=api_key(),
        account=InternetArchiveAccount("account@example.invalid", "password"),
    ) as client:
        result = await client.save(
            "https://example.com/account-async",
            options,
            timeout=1,
            poll_interval=0.001,
        )
    assert result.wayback_timestamp == "20260823110413"
    assert len(ia_endpoints.matching("/ia/login", "POST")) == 1
    my_archive = ia_endpoints.matching("/ia/mwa", "POST")
    assert len(my_archive) == 1
    assert my_archive[0].headers["Content-Type"] == "application/json"


def test_account_only_login_supplies_both_spn_cookies(
    ia_endpoints: ServerState,
) -> None:
    """Supply both SPN cookies after account-only authentication."""
    with InternetArchiveClient(
        account=InternetArchiveAccount("account@example.invalid", "password")
    ) as client:
        job = client.submit("https://example.com/account-only")
    assert job.job_id == "job-1"
    submission = ia_endpoints.matching("/ia/save", "POST")[0]
    assert "logged-in-user=test%40example.invalid" in submission.headers["Cookie"]
    assert "logged-in-sig=test-signature" in submission.headers["Cookie"]


def test_cookie_authentication_and_terminal_errors(
    ia_endpoints: ServerState,
) -> None:
    """Use cookie authentication and surface terminal polling errors."""
    cookies = InternetArchiveCookies("test%40example.invalid", "cookie-signature")
    with InternetArchiveClient(cookies=cookies) as client:
        assert client.user_status().available == AVAILABLE_CAPTURES
        with pytest.raises(CaptureFailedError) as failure:
            client.wait("failed", timeout=1, poll_interval=0.001)
        with pytest.raises(PollingTimeoutError):
            client.wait("pending", timeout=0.2, poll_interval=0.01)
    assert failure.value.job_id == "failed"
    assert failure.value.service_code == "error:cannot-fetch"
    assert str(failure.value).endswith(": capture failed")
    request = ia_endpoints.matching("/ia/status/user", "GET")[0]
    assert "logged-in-user=test%40example.invalid" in request.headers["Cookie"]
    assert "logged-in-sig=cookie-signature" in request.headers["Cookie"]


@pytest.mark.asyncio
async def test_async_cookie_authentication_and_terminal_errors(
    ia_endpoints: ServerState,
) -> None:
    """Use cookies and surface terminal errors through the async client."""
    cookies = InternetArchiveCookies("test%40example.invalid", "cookie-signature")
    async with AsyncInternetArchiveClient(cookies=cookies) as client:
        assert (await client.user_status()).available == AVAILABLE_CAPTURES
        with pytest.raises(CaptureFailedError, match="capture failed"):
            await client.wait("failed", timeout=1, poll_interval=0.001)
        with pytest.raises(PollingTimeoutError):
            await client.wait("pending", timeout=0.2, poll_interval=0.01)
    request = ia_endpoints.matching("/ia/status/user", "GET")[0]
    assert "logged-in-sig=cookie-signature" in request.headers["Cookie"]


def test_account_operations_fail_before_transport_without_credentials(
    ia_endpoints: ServerState,
) -> None:
    """Reject account operations before sending an unauthenticated request."""
    with InternetArchiveClient() as client, pytest.raises(AuthenticationError):
        client.user_status()
    assert ia_endpoints.requests == []


def test_authenticated_save_options_fail_before_anonymous_submission(
    ia_endpoints: ServerState,
) -> None:
    """Require credentials only when an enabled capture option needs them."""
    with (
        InternetArchiveClient() as client,
        pytest.raises(AuthenticationError, match="selected options"),
    ):
        client.submit(
            "https://example.com/",
            InternetArchiveSaveOptions(capture_screenshot=True),
        )
    assert ia_endpoints.requests == []


def test_my_web_archive_auth_is_checked_before_capture_submission(
    ia_endpoints: ServerState,
) -> None:
    """Check My Web Archive authentication before capture submission."""
    with (
        InternetArchiveClient(api_key=api_key()) as client,
        pytest.raises(AuthenticationError, match="My Web Archive"),
    ):
        client.save(
            "https://example.com/",
            InternetArchiveSaveOptions(save_to_archive=True),
        )
    assert ia_endpoints.requests == []


def test_polling_arguments_are_checked_before_capture_submission(
    ia_endpoints: ServerState,
) -> None:
    """Validate polling arguments before capture submission."""
    with (
        InternetArchiveClient(api_key=api_key()) as client,
        pytest.raises(InvalidOptionError),
    ):
        client.save("https://example.com/", timeout=float("nan"))
    assert ia_endpoints.requests == []


def test_collection_arguments_reject_bare_strings_and_cdx_accepts_wildcards(
    ia_endpoints: ServerState,
) -> None:
    """Reject string collections while accepting CDX wildcard targets."""
    with InternetArchiveClient(api_key=api_key()) as client:
        with pytest.raises(InvalidOptionError):
            client.status_batch("job-1")
        with pytest.raises(InvalidOptionError):
            client.save("https://example.com/", tags="research")
        with pytest.raises(InvalidOptionError):
            client.search("https://example.com/", filters="statuscode:200")
        result = client.search("*.example.com/*", show_resume_key=True)
    assert result.resume_key == "resume-token"
