"""Asynchronous client for Archive.today's stable read interfaces."""

from __future__ import annotations

import logging
from datetime import datetime
from http import HTTPStatus
from typing import Any, cast

import niquests

from archivist.core._http import (
    ResponseLike,
    async_response_text,
    raise_for_common_status,
    translate_request_error,
)
from archivist.core._urls import (
    sanitize_url_for_log,
    validate_service_url,
    validate_target_url,
)
from archivist.core.errors import InvalidServiceResponseError
from archivist.services.archive_today import _common
from archivist.services.archive_today.models import (
    ArchiveTodayMemento,
    ArchiveTodayRecentFeed,
    ArchiveTodayTimeMap,
)

logger = logging.getLogger(__name__)


class AsyncArchiveTodayClient:
    """Asynchronous client for Archive.today Memento and RSS endpoints."""

    def __init__(
        self,
        session: niquests.AsyncSession | None = None,
        *,
        mirror: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        """Initialize a client for an Archive.today mirror."""
        mirror = _common.DEFAULT_MIRROR if mirror is None else mirror
        mirror = validate_service_url(mirror)
        timeout = _common.validate_duration(timeout, name="timeout")
        self._mirror = mirror
        self._session = (
            session if session is not None else niquests.AsyncSession(retries=0)
        )
        self._owns_session = session is None
        self._timeout = timeout
        self._closed = False

    async def __aenter__(self) -> AsyncArchiveTodayClient:
        """Enter the asynchronous client context."""
        self._ensure_open()
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        """Close client-owned resources."""
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
        url: str,
        *,
        request_log_url: str | None = None,
        **kwargs: Any,  # noqa: ANN401 - niquests accepts heterogeneous request options.
    ) -> object:
        self._ensure_open()
        logger.debug("GET %s", sanitize_url_for_log(request_log_url or url))
        try:
            response = await self._session.get(url, timeout=self._timeout, **kwargs)
            await self._session.gather(cast("Any", response))
        except (niquests.exceptions.RequestException, OSError) as exc:
            error = translate_request_error(exc, service=_common.SERVICE)
        else:
            return response
        raise error from None

    @staticmethod
    def _require_status(response: ResponseLike, expected: set[int]) -> None:
        raise_for_common_status(response, service=_common.SERVICE)
        if response.status_code not in expected:
            raise InvalidServiceResponseError(
                f"Archive.today returned unexpected HTTP {response.status_code}",
                service=_common.SERVICE,
                status_code=response.status_code,
            )

    async def timemap(self, target_url: str) -> ArchiveTodayTimeMap:
        """Return the RFC 7089 TimeMap for an original URL."""
        target_url = validate_target_url(target_url)
        response = await self._request(
            f"{self._mirror}/timemap/{target_url}",
            request_log_url=f"{self._mirror}/timemap",
            allow_redirects=False,
        )
        typed = cast("ResponseLike", response)
        if typed.status_code == HTTPStatus.NOT_FOUND:
            return _common.empty_timemap(mirror=self._mirror, original_url=target_url)
        self._require_status(typed, {HTTPStatus.OK})
        return _common.parse_timemap(
            await async_response_text(response, service=_common.SERVICE),
            mirror=self._mirror,
        )

    async def closest(
        self, target_url: str, timestamp: datetime
    ) -> ArchiveTodayMemento | None:
        """Return the Memento closest to an aware datetime."""
        target_url = validate_target_url(target_url)
        response = await self._request(
            f"{self._mirror}/timegate/{target_url}",
            request_log_url=f"{self._mirror}/timegate",
            headers={"Accept-Datetime": _common.format_accept_datetime(timestamp)},
            allow_redirects=False,
        )
        return self._redirect_result(
            response, target_url=target_url, relation="closest"
        )

    async def first(self, target_url: str) -> ArchiveTodayMemento | None:
        """Return the oldest Memento for an original URL."""
        return await self._lookup(target_url, route="oldest", relation="first")

    async def last(self, target_url: str) -> ArchiveTodayMemento | None:
        """Return the newest Memento for an original URL."""
        return await self._lookup(target_url, route="newest", relation="last")

    async def _lookup(
        self, target_url: str, *, route: str, relation: str
    ) -> ArchiveTodayMemento | None:
        target_url = validate_target_url(target_url)
        response = await self._request(
            f"{self._mirror}/{route}/{target_url}",
            request_log_url=f"{self._mirror}/{route}",
            allow_redirects=False,
        )
        return self._redirect_result(response, target_url=target_url, relation=relation)

    def _redirect_result(
        self,
        response: object,
        *,
        target_url: str,
        relation: str,
    ) -> ArchiveTodayMemento | None:
        typed = cast("ResponseLike", response)
        if typed.status_code == HTTPStatus.NOT_FOUND:
            return None
        self._require_status(typed, {301, 302, 303, 307, 308})
        return _common.parse_memento_redirect(
            typed.headers,
            mirror=self._mirror,
            original_url=target_url,
            relation=relation,
        )

    async def recent_captures(self) -> ArchiveTodayRecentFeed:
        """Return Archive.today's standard RSS recent-capture feed."""
        response = await self._request(f"{self._mirror}/rss", allow_redirects=False)
        typed = cast("ResponseLike", response)
        self._require_status(typed, {HTTPStatus.OK})
        return _common.parse_rss(
            await async_response_text(response, service=_common.SERVICE),
            mirror=self._mirror,
        )
