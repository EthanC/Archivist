"""Asynchronous Internet Archive client."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any, Unpack, cast
from urllib.parse import quote

import niquests

from archivist.core._http import (
    ResponseLike,
    async_response_json,
    async_response_mapping,
    async_response_text,
    parse_retry_after,
    raise_for_common_status,
    translate_request_error,
)
from archivist.core._urls import (
    sanitize_url_for_log,
    validate_cdx_query,
    validate_target_url,
)
from archivist.core.errors import (
    AuthenticationError,
    CaptureFailedError,
    InvalidOptionError,
    NetworkError,
    OptionCombinationError,
    PollingTimeoutError,
)
from archivist.services.internet_archive import _common
from archivist.services.internet_archive.models import (
    InternetArchiveAccount,
    InternetArchiveApiKey,
    InternetArchiveAvailability,
    InternetArchiveCaptureJob,
    InternetArchiveCaptureStatus,
    InternetArchiveCdxResult,
    InternetArchiveCookies,
    InternetArchiveFailedStatus,
    InternetArchivePendingStatus,
    InternetArchiveSaveOptions,
    InternetArchiveSuccessStatus,
    InternetArchiveSystemStatus,
    InternetArchiveUserStatus,
)

logger = logging.getLogger(__name__)


class AsyncInternetArchiveClient:
    """Asynchronous client for Save Page Now and Wayback APIs."""

    def __init__(
        self,
        session: niquests.AsyncSession | None = None,
        *,
        api_key: InternetArchiveApiKey | None = None,
        cookies: InternetArchiveCookies | None = None,
        account: InternetArchiveAccount | None = None,
        timeout: float = 30.0,
    ) -> None:
        """Initialize a client with optional authentication and session state."""
        timeout = _common.validate_duration(timeout, name="timeout", allow_zero=False)
        self._session = (
            session if session is not None else niquests.AsyncSession(retries=0)
        )
        self._owns_session = session is None
        self._api_key = api_key
        self._cookies = cookies
        self._account = account
        self._account_authenticated = False
        self._timeout = timeout
        self._closed = False

    async def __aenter__(self) -> AsyncInternetArchiveClient:
        """Return this open client as an asynchronous context manager."""
        self._ensure_open()
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        """Close client-owned resources when leaving an asynchronous context."""
        await self.close()

    async def close(self) -> None:
        """Close the client-owned session, if any."""
        if self._closed:
            return
        self._closed = True
        if self._owns_session:
            await self._session.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("client is closed")

    async def _request(
        self,
        method: str,
        url: str,
        *,
        request_timeout: float | None = None,
        request_log_url: str | None = None,
        **kwargs: Any,  # noqa: ANN401 - niquests accepts heterogeneous request options.
    ) -> object:
        self._ensure_open()
        logger.debug("%s %s", method, sanitize_url_for_log(request_log_url or url))
        effective_timeout = (
            self._timeout
            if request_timeout is None
            else min(self._timeout, request_timeout)
        )
        try:
            response = await self._session.request(
                method, url, timeout=effective_timeout, **kwargs
            )
            await self._session.gather(cast("Any", response))
            return response
        except (niquests.exceptions.RequestException, OSError) as exc:
            error = translate_request_error(exc, service=_common.SERVICE)
        else:
            return response
        raise error from None

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self._api_key is not None:
            headers["Authorization"] = (
                f"LOW {self._api_key.access_key}:{self._api_key.secret_key}"
            )
        return headers

    def _request_cookies(self) -> dict[str, str] | None:
        if self._cookies is None:
            return None
        return {
            "logged-in-user": self._cookies.logged_in_user,
            "logged-in-sig": self._cookies.logged_in_sig,
        }

    async def _ensure_api_authentication(self) -> None:
        if (
            self._api_key is not None
            or self._cookies is not None
            or self._account_authenticated
        ):
            return
        if self._account is not None:
            await self.login()
            return
        raise AuthenticationError(
            "Internet Archive credentials are required for this operation",
            service=_common.SERVICE,
        )

    async def _ensure_account_authentication(self) -> None:
        if self._cookies is not None or self._account_authenticated:
            return
        if self._account is not None:
            await self.login()
            return
        raise AuthenticationError(
            "Internet Archive account cookies are required for My Web Archive",
            service=_common.SERVICE,
        )

    async def login(self) -> None:
        """Authenticate the session with configured Archive.org account credentials."""
        if self._account_authenticated:
            return
        if self._account is None:
            raise AuthenticationError(
                "Archive.org account credentials were not configured",
                service=_common.SERVICE,
            )
        token_response = await self._request(
            "GET", _common.CSRF_URL, headers={"Accept": "application/json"}
        )
        raise_for_common_status(
            cast("ResponseLike", token_response), service=_common.SERVICE
        )
        token_data = await async_response_mapping(
            token_response, service=_common.SERVICE
        )
        token_container = token_data.get("value")
        token = (
            token_container.get("token")
            if isinstance(token_container, Mapping)
            else None
        )
        if (
            token_data.get("success") is not True
            or not isinstance(token, str)
            or not token
        ):
            raise AuthenticationError(
                "Archive.org did not provide a CSRF token", service=_common.SERVICE
            )

        account = self._account
        login_response = await self._request(
            "POST",
            _common.LOGIN_URL,
            headers={"Accept": "application/json", "X-CSRF-Token": token},
            json={
                "username": account.username,
                "password": account.password,
                "remember": "true" if account.remember else "false",
                "t": token,
            },
        )
        raise_for_common_status(
            cast("ResponseLike", login_response),
            service=_common.SERVICE,
            authentication_statuses=frozenset({400, 401, 403}),
        )
        login_data = await async_response_mapping(
            login_response, service=_common.SERVICE
        )
        if login_data.get("success") is not True:
            raise AuthenticationError(
                "Archive.org account login failed", service=_common.SERVICE
            )
        self._account_authenticated = True
        logger.info("authenticated an Archive.org account session")

    async def submit(
        self,
        target_url: str,
        options: InternetArchiveSaveOptions | None = None,
    ) -> InternetArchiveCaptureJob:
        """Submit a Save Page Now capture and return its job."""
        target_url = validate_target_url(target_url)
        effective_options = options or InternetArchiveSaveOptions()
        anonymous = (
            self._api_key is None
            and self._cookies is None
            and self._account is None
            and not self._account_authenticated
        )
        if anonymous and _common.save_options_require_authentication(effective_options):
            raise AuthenticationError(
                "Internet Archive credentials are required for the selected options",
                service=_common.SERVICE,
            )
        if not anonymous:
            await self._ensure_api_authentication()
        payload = {"url": target_url}
        payload.update(effective_options.to_form())
        logger.info(
            "submitting Internet Archive capture for %s",
            sanitize_url_for_log(target_url),
        )
        response = await self._request(
            "POST",
            f"{_common.SAVE_URL}/{target_url}" if anonymous else _common.SAVE_URL,
            headers=None if anonymous else self._headers(),
            cookies=self._request_cookies(),
            data=payload,
            request_log_url=_common.SAVE_URL if anonymous else None,
        )
        raise_for_common_status(cast("ResponseLike", response), service=_common.SERVICE)
        job = (
            _common.parse_anonymous_submission(
                await async_response_text(response, service=_common.SERVICE),
                target_url=target_url,
            )
            if anonymous
            else _common.parse_submission(
                await async_response_mapping(response, service=_common.SERVICE),
                target_url=target_url,
            )
        )
        logger.info("Internet Archive created capture job %s", job.job_id)
        return job

    async def status(self, job_id: str) -> InternetArchiveCaptureStatus:
        """Return the current state of one Save Page Now job."""
        return await self._status(job_id)

    async def _status(
        self, job_id: str, *, request_timeout: float | None = None
    ) -> InternetArchiveCaptureStatus:
        if not job_id:
            raise InvalidOptionError("job_id cannot be empty")
        response = await self._request(
            "GET",
            f"{_common.STATUS_URL}/{quote(job_id, safe='')}",
            headers=self._headers(),
            cookies=self._request_cookies(),
            params={"_t": str(int(time.time() * 1000))},
            request_timeout=request_timeout,
        )
        raise_for_common_status(cast("ResponseLike", response), service=_common.SERVICE)
        retry_after, _ = parse_retry_after(cast("ResponseLike", response).headers)
        return _common.parse_capture_status(
            await async_response_mapping(response, service=_common.SERVICE),
            fallback_job_id=job_id,
            retry_after=retry_after,
        )

    async def status_batch(
        self, job_ids: Iterable[str]
    ) -> tuple[InternetArchiveCaptureStatus, ...]:
        """Return states for several Save Page Now jobs."""
        ids = _common.string_items(job_ids, name="job_ids")
        if not ids or any(not job_id for job_id in ids):
            raise InvalidOptionError(
                "job_ids must contain at least one non-empty job ID"
            )
        response = await self._request(
            "POST",
            _common.STATUS_URL,
            headers=self._headers(),
            cookies=self._request_cookies(),
            data={"job_ids": ",".join(ids)},
        )
        raise_for_common_status(cast("ResponseLike", response), service=_common.SERVICE)
        return _common.parse_status_collection(
            await async_response_json(response, service=_common.SERVICE)
        )

    async def status_outlinks(
        self, job_id: str
    ) -> tuple[InternetArchiveCaptureStatus, ...]:
        """Return the child-job states created for captured outlinks."""
        if not job_id:
            raise InvalidOptionError("job_id cannot be empty")
        response = await self._request(
            "POST",
            _common.STATUS_URL,
            headers=self._headers(),
            cookies=self._request_cookies(),
            data={"job_id_outlinks": job_id},
        )
        raise_for_common_status(cast("ResponseLike", response), service=_common.SERVICE)
        return _common.parse_outlink_status_collection(
            await async_response_json(response, service=_common.SERVICE)
        )

    async def wait(
        self,
        job: InternetArchiveCaptureJob | str,
        *,
        timeout: float = 300.0,  # noqa: ASYNC109 - Public polling deadline.
        poll_interval: float = 2.0,
    ) -> InternetArchiveSuccessStatus:
        """Poll a Save Page Now job until it succeeds, fails, or times out."""
        return await self._wait(job, wait_timeout=timeout, poll_interval=poll_interval)

    async def _wait(
        self,
        job: InternetArchiveCaptureJob | str,
        *,
        wait_timeout: float,
        poll_interval: float,
    ) -> InternetArchiveSuccessStatus:
        wait_timeout = _common.validate_duration(
            wait_timeout, name="timeout", allow_zero=False
        )
        poll_interval = _common.validate_duration(
            poll_interval, name="poll_interval", allow_zero=False
        )
        job_id = job.job_id if isinstance(job, InternetArchiveCaptureJob) else job
        if not job_id:
            raise InvalidOptionError("job_id cannot be empty")
        deadline = time.monotonic() + wait_timeout
        previous_state: str | None = None
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise PollingTimeoutError(
                    f"{_common.SERVICE} capture did not finish "
                    f"within {wait_timeout:g} seconds",
                    service=_common.SERVICE,
                    job_id=job_id,
                    timeout=wait_timeout,
                )
            try:
                current = await self._status(job_id, request_timeout=remaining)
            except NetworkError as exc:
                deadline_timeout = (
                    exc.cause_type is not None
                    and "timeout" in exc.cause_type.lower()
                    and remaining < self._timeout
                )
                if deadline_timeout or deadline - time.monotonic() <= 0:
                    raise PollingTimeoutError(
                        f"{_common.SERVICE} capture did not finish "
                        f"within {wait_timeout:g} seconds",
                        service=_common.SERVICE,
                        job_id=job_id,
                        timeout=wait_timeout,
                    ) from None
                raise
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise PollingTimeoutError(
                    f"{_common.SERVICE} capture did not finish "
                    f"within {wait_timeout:g} seconds",
                    service=_common.SERVICE,
                    job_id=job_id,
                    timeout=wait_timeout,
                )
            if current.status != previous_state:
                logger.info("Internet Archive job %s is %s", job_id, current.status)
                previous_state = current.status
            if isinstance(current, InternetArchiveSuccessStatus):
                logger.info("Internet Archive job %s completed", job_id)
                return current
            if isinstance(current, InternetArchiveFailedStatus):
                reason_suffix = (
                    f": {current.message}" if current.message is not None else ""
                )
                raise CaptureFailedError(
                    f"{_common.SERVICE} capture job failed{reason_suffix}",
                    service=_common.SERVICE,
                    job_id=job_id,
                    service_code=current.service_code,
                )
            if not isinstance(current, InternetArchivePendingStatus):
                raise AssertionError("unreachable capture status")
            delay = _common.polling_delay(
                current.retry_after, poll_interval=poll_interval
            )
            await asyncio.sleep(min(delay, remaining))

    async def save(
        self,
        target_url: str,
        options: InternetArchiveSaveOptions | None = None,
        *,
        timeout: float = 300.0,  # noqa: ASYNC109 - Public polling deadline.
        poll_interval: float = 2.0,
        tags: Iterable[str] = (),
    ) -> InternetArchiveSuccessStatus:
        """Submit a capture, wait for it, and optionally save it to My Web Archive."""
        return await self._save(
            target_url,
            options,
            save_timeout=timeout,
            poll_interval=poll_interval,
            tags=tags,
        )

    async def _save(
        self,
        target_url: str,
        options: InternetArchiveSaveOptions | None,
        *,
        save_timeout: float,
        poll_interval: float,
        tags: Iterable[str],
    ) -> InternetArchiveSuccessStatus:
        save_timeout = _common.validate_duration(
            save_timeout, name="timeout", allow_zero=False
        )
        poll_interval = _common.validate_duration(
            poll_interval, name="poll_interval", allow_zero=False
        )
        effective_options = options or InternetArchiveSaveOptions()
        archive_tags = _common.string_items(tags, name="tags")
        if archive_tags and not effective_options.save_to_archive:
            raise OptionCombinationError("tags require save_to_archive to be enabled")
        if (
            effective_options.save_to_archive
            and self._cookies is None
            and self._account is None
            and not self._account_authenticated
        ):
            raise AuthenticationError(
                "Internet Archive account credentials are required for My Web Archive",
                service=_common.SERVICE,
            )
        job = await self.submit(target_url, effective_options)
        result = await self.wait(job, timeout=save_timeout, poll_interval=poll_interval)
        if effective_options.save_to_archive:
            await self.add_to_my_web_archive(result, tags=archive_tags)
        return result

    async def add_to_my_web_archive(
        self,
        capture: InternetArchiveSuccessStatus,
        *,
        tags: Iterable[str] = (),
    ) -> None:
        """Add a completed capture to the authenticated account's web archive."""
        archive_tags = _common.string_items(tags, name="tags")
        await self._ensure_account_authentication()
        response = await self._request(
            "POST",
            _common.MY_WEB_ARCHIVE_URL,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            cookies=self._request_cookies(),
            json={
                "url": capture.original_url,
                "snapshot": capture.wayback_timestamp,
                "tags": list(archive_tags),
            },
        )
        raise_for_common_status(cast("ResponseLike", response), service=_common.SERVICE)
        logger.info("added Internet Archive job %s to My Web Archive", capture.job_id)

    async def user_status(self) -> InternetArchiveUserStatus:
        """Return the authenticated account's SPN capacity."""
        await self._ensure_api_authentication()
        response = await self._request(
            "GET",
            _common.USER_STATUS_URL,
            headers=self._headers(),
            cookies=self._request_cookies(),
            params={"_t": str(int(time.time() * 1000))},
        )
        raise_for_common_status(cast("ResponseLike", response), service=_common.SERVICE)
        return _common.parse_user_status(
            await async_response_mapping(response, service=_common.SERVICE)
        )

    async def system_status(self) -> InternetArchiveSystemStatus:
        """Return Save Page Now system health and queue metrics."""
        authenticated = self._api_key is not None or self._cookies is not None
        response = await self._request(
            "GET",
            _common.SYSTEM_STATUS_URL,
            headers=self._headers() if authenticated else None,
            cookies=self._request_cookies(),
        )
        raise_for_common_status(cast("ResponseLike", response), service=_common.SERVICE)
        return _common.parse_system_status(
            await async_response_mapping(response, service=_common.SERVICE)
        )

    async def availability(
        self,
        target_url: str,
        *,
        timestamp: datetime | str | None = None,
    ) -> InternetArchiveAvailability:
        """Return the closest capture from the Wayback Availability API."""
        target_url = validate_target_url(target_url)
        timestamp_value = _common.format_wayback_query_timestamp(timestamp)
        params: dict[str, str] = {"url": target_url}
        if timestamp_value is not None:
            params["timestamp"] = timestamp_value
        response = await self._request("GET", _common.AVAILABILITY_URL, params=params)
        raise_for_common_status(cast("ResponseLike", response), service=_common.SERVICE)
        return _common.parse_availability(
            await async_response_mapping(response, service=_common.SERVICE)
        )

    async def search(
        self,
        target_url: str,
        **options: Unpack[_common._CdxSearchOptions],
    ) -> InternetArchiveCdxResult:
        """Search Wayback CDX capture records."""
        target_url = validate_cdx_query(target_url)
        params: list[tuple[str, str]] = [
            ("url", target_url),
            ("output", "json"),
            ("fl", "timestamp,original,mimetype,statuscode,digest,length"),
        ]
        params.extend(_common.cdx_search_params(options))
        response = await self._request("GET", _common.CDX_URL, params=params)
        raise_for_common_status(cast("ResponseLike", response), service=_common.SERVICE)
        return _common.parse_cdx(
            await async_response_json(response, service=_common.SERVICE)
        )
