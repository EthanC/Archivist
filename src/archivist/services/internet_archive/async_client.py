"""Asynchronous Internet Archive client."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Iterable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime
from http import HTTPStatus
from pathlib import Path
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
    InvalidServiceResponseError,
    NetworkError,
    OptionCombinationError,
    PollingTimeoutError,
    ServiceError,
)
from archivist.services.internet_archive import _common, _items, _recovery
from archivist.services.internet_archive.item_models import (
    InternetArchiveChecksumState,
    InternetArchiveFileDisposition,
    InternetArchiveItem,
    InternetArchiveRecoveryDeferredError,
    InternetArchiveRecoveryError,
    InternetArchiveRecoveryFileResult,
    InternetArchiveRecoveryPhase,
    InternetArchiveRecoveryResult,
    InternetArchiveRemovalResult,
    InternetArchiveUploadError,
    InternetArchiveUploadFile,
    InternetArchiveUploadFileResult,
    InternetArchiveUploadOptions,
    InternetArchiveUploadResult,
)
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
    """Asynchronous client for Wayback APIs and Archive.org items."""

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
            "Internet Archive account cookies are required for My Web Archive "
            "and item account operations",
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
            "GET",
            _common.CSRF_URL,
            headers={"Accept": "application/json"},
            allow_redirects=False,
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
            allow_redirects=False,
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

    def _ensure_item_transport(self) -> None:
        if not isinstance(self._session, niquests.AsyncSession):
            return
        # Path-specific mounts can override session.retries, including during login.
        for adapter in self._session.adapters.values():
            retries = getattr(adapter, "max_retries", None)
            if retries is not False and getattr(retries, "total", None) != 0:
                raise InvalidOptionError(
                    "Internet Archive item operations require every session adapter "
                    "to disable retries (total=0 or False)"
                )

    async def _recovery_request(  # noqa: PLR0913 - Explicit request security fields.
        self,
        method: str,
        url: str,
        key: InternetArchiveApiKey,
        *,
        request_timeout: float,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, str] | None = None,
        data: Any = None,  # noqa: ANN401 - Niquests accepts heterogeneous bodies.
    ) -> object:
        """Send a recovery request without session cookies, auth, headers, or hooks."""
        self._ensure_open()
        explicit_headers = {
            "Accept": "application/json",
            "Authorization": f"LOW {key.access_key}:{key.secret_key}",
            **dict(headers or {}),
        }
        effective_timeout = min(self._timeout, request_timeout)
        logger.debug("%s %s", method, sanitize_url_for_log(url))
        try:
            if isinstance(self._session, niquests.AsyncSession):
                request = niquests.Request(
                    method=method,
                    url=url,
                    headers=explicit_headers,
                    params=dict(params or {}),
                    data=data,
                    cookies={},
                    auth=None,
                    hooks={},
                ).prepare()
                assert request.url is not None
                settings = self._session.merge_environment_settings(
                    request.url, {}, False, None, None
                )
                response = await self._session.send(
                    request,
                    timeout=effective_timeout,
                    allow_redirects=False,
                    **settings,
                )
                await self._session.gather(cast("Any", response))
                return response
            return await self._request(
                method,
                url,
                request_timeout=request_timeout,
                headers=explicit_headers,
                params=dict(params or {}),
                data=data,
                cookies={},
                auth=None,
                hooks={},
                allow_redirects=False,
            )
        except (niquests.exceptions.RequestException, OSError, TypeError) as exc:
            error = translate_request_error(exc, service=_common.SERVICE)
        raise error from None

    async def get_item(self, identifier: str) -> InternetArchiveItem:
        """Return one typed Metadata API snapshot without mutating the item."""
        _items._validate_identifier(identifier)
        response = await self._request(
            "GET",
            f"{_recovery.METADATA_URL}/{identifier}",
            params={"extended_err": "1"},
            allow_redirects=False,
        )
        typed = cast("ResponseLike", response)
        if typed.status_code in {401, 403, 429} or (
            HTTPStatus.MULTIPLE_CHOICES <= typed.status_code < HTTPStatus.BAD_REQUEST
        ):
            _items.raise_for_item_status(typed)
        data = await async_response_mapping(response, service=_common.SERVICE)
        return _recovery.parse_item(data, identifier, status_code=typed.status_code)

    async def _recovery_key(self) -> InternetArchiveApiKey:
        """Resolve and return the LOW pair that remains pinned for one operation."""
        if self._api_key is None:
            await self._ensure_account_authentication()
            response = await self._request(
                "GET",
                _items.UPLOAD_URL,
                cookies=self._request_cookies(),
                allow_redirects=False,
            )
            _items.raise_for_item_status(cast("ResponseLike", response))
            self._api_key = _items.parse_upload_key(
                await async_response_text(response, service=_common.SERVICE)
            )
        return self._api_key

    async def add_files(  # noqa: PLR0912,PLR0913,PLR0915 - Recovery state machine.
        self,
        identifier: str,
        files: Path | Iterable[InternetArchiveUploadFile | str | Path],
        *,
        expected_metadata: Mapping[str, str | Sequence[str]],
        wait: bool = False,
        timeout: float = 300.0,  # noqa: ASYNC109 - Public recovery deadline.
        poll_interval: float = 2.0,
    ) -> InternetArchiveRecoveryResult:
        """Recover missing files on an owned item after conservative reconciliation.

        Source inspection and hashing run off-loop. Sources must remain unchanged
        through return. One deadline covers every readiness and verification read.
        """
        self._ensure_open()
        if not isinstance(wait, bool):
            raise InvalidOptionError("wait must be a boolean")
        timeout = _common.validate_duration(timeout, name="timeout", allow_zero=False)
        poll_interval = _common.validate_duration(
            poll_interval, name="poll_interval", allow_zero=False
        )
        expected = _recovery.normalize_expected_metadata(expected_metadata)
        async with _items.async_prepare_files(files, identifier) as source_files:
            prepared = await _items._source_io(
                _recovery.hash_prepared_files, source_files
            )
            self._ensure_item_transport()
            outcomes = [
                InternetArchiveRecoveryFileResult(
                    file.name, file.size, file.md5, file.sha1
                )
                for file in prepared
            ]
            deadline = time.monotonic() + timeout
            timeout_error = PollingTimeoutError(
                "Internet Archive file recovery did not finish within "
                f"{timeout:g} seconds",
                service=_common.SERVICE,
                job_id=identifier,
                timeout=timeout,
            )

            def result(
                *, processing: bool = False, verification: bool = False
            ) -> InternetArchiveRecoveryResult:
                return InternetArchiveRecoveryResult(
                    identifier,
                    tuple(outcomes),
                    processing_complete=processing,
                    verification_complete=verification,
                )

            def fail(
                cause: ServiceError,
                phase: InternetArchiveRecoveryPhase,
                *,
                deferred: bool = False,
                failed_file: str | None = None,
            ) -> None:
                if deferred:
                    raise InternetArchiveRecoveryDeferredError(
                        result(), cause, phase=phase
                    ) from None
                raise InternetArchiveRecoveryError(
                    result(), cause, phase=phase, failed_file=failed_file
                ) from None

            def remaining(phase: InternetArchiveRecoveryPhase) -> float:
                value = deadline - time.monotonic()
                if value <= 0:
                    raise timeout_error
                return value

            @asynccontextmanager
            async def within_deadline(
                phase: InternetArchiveRecoveryPhase,
            ) -> AsyncIterator[None]:
                try:
                    async with asyncio.timeout(remaining(phase)):
                        yield
                        remaining(phase)
                except TimeoutError:
                    raise timeout_error from None

            try:
                async with within_deadline(InternetArchiveRecoveryPhase.OWNERSHIP):
                    key = await self._recovery_key()
                    response = await self._recovery_request(
                        "GET",
                        _common.USER_INFO_URL,
                        key,
                        request_timeout=remaining(
                            InternetArchiveRecoveryPhase.OWNERSHIP
                        ),
                        params={"op": "whoami"},
                    )
                    _items.raise_for_item_status(cast("ResponseLike", response))
                    identity = _recovery.parse_identity(
                        await async_response_mapping(response, service=_common.SERVICE)
                    )
            except ServiceError as exc:
                fail(exc, InternetArchiveRecoveryPhase.OWNERSHIP)

            while True:
                phase = InternetArchiveRecoveryPhase.RECONCILIATION
                try:
                    async with within_deadline(phase):
                        metadata_response = await self._recovery_request(
                            "GET",
                            f"{_recovery.METADATA_URL}/{identifier}",
                            key,
                            request_timeout=remaining(phase),
                            params={"extended_err": "1"},
                        )
                        typed_metadata = cast("ResponseLike", metadata_response)
                        if typed_metadata.status_code in {401, 403, 429} or (
                            HTTPStatus.MULTIPLE_CHOICES
                            <= typed_metadata.status_code
                            < HTTPStatus.BAD_REQUEST
                        ):
                            _items.raise_for_item_status(typed_metadata)
                        item = _recovery.parse_item(
                            await async_response_mapping(
                                metadata_response, service=_common.SERVICE
                            ),
                            identifier,
                            status_code=typed_metadata.status_code,
                        )
                        phase = InternetArchiveRecoveryPhase.INGEST
                        catalog_response = await self._recovery_request(
                            "GET",
                            _items.UPLOAD_API_URL,
                            key,
                            request_timeout=remaining(phase),
                            params={"name": "catalogRows", "identifier": identifier},
                        )
                        _items.raise_for_item_status(
                            cast("ResponseLike", catalog_response)
                        )
                        catalog_complete = _items.parse_catalog(
                            await async_response_mapping(
                                catalog_response, service=_common.SERVICE
                            )
                        )
                        phase = InternetArchiveRecoveryPhase.RECONCILIATION
                        decision = _recovery.reconcile(
                            item,
                            identity,
                            expected,
                            prepared,
                            catalog_complete=catalog_complete,
                        )
                except _recovery.RecoveryDecisionError as exc:
                    for index, outcome in enumerate(outcomes):
                        if outcome.name not in exc.files or (
                            outcome.transferred and not exc.checksum_mismatch
                        ):
                            continue
                        disposition = (
                            InternetArchiveFileDisposition.DEFERRED
                            if exc.deferred
                            else InternetArchiveFileDisposition.CONFLICTING
                        )
                        checksum = (
                            InternetArchiveChecksumState.MISMATCH
                            if exc.checksum_mismatch
                            else InternetArchiveChecksumState.PENDING
                            if exc.deferred
                            else outcome.checksum_state
                        )
                        outcomes[index] = replace(
                            outcome,
                            disposition=disposition,
                            checksum_state=checksum,
                        )
                    if not exc.deferred:
                        fail(ServiceError(str(exc), service=_common.SERVICE), exc.phase)
                    if not wait:
                        fail(
                            ServiceError(str(exc), service=_common.SERVICE),
                            exc.phase,
                            deferred=True,
                        )
                    try:
                        delay = min(poll_interval, remaining(exc.phase))
                    except ServiceError as deadline_error:
                        fail(deadline_error, exc.phase)
                    await asyncio.sleep(delay)
                    continue
                except ServiceError as exc:
                    fail(exc, phase)

                unresolved_transfers = [
                    outcome.name
                    for outcome in outcomes
                    if outcome.transferred
                    and decision.dispositions[outcome.name]
                    is not InternetArchiveFileDisposition.ALREADY_MATCHING
                ]
                if unresolved_transfers:
                    if wait:
                        try:
                            delay = min(
                                poll_interval,
                                remaining(InternetArchiveRecoveryPhase.VERIFICATION),
                            )
                        except ServiceError as deadline_error:
                            fail(
                                deadline_error,
                                InternetArchiveRecoveryPhase.VERIFICATION,
                            )
                        await asyncio.sleep(delay)
                        continue
                    for index, outcome in enumerate(outcomes):
                        if outcome.name in unresolved_transfers:
                            outcomes[index] = replace(
                                outcome,
                                disposition=InternetArchiveFileDisposition.UNCERTAIN,
                            )
                    fail(
                        ServiceError(
                            "an acknowledged transfer is not yet reconcilable",
                            service=_common.SERVICE,
                        ),
                        InternetArchiveRecoveryPhase.VERIFICATION,
                        deferred=True,
                    )

                for index, outcome in enumerate(outcomes):
                    if outcome.transferred:
                        outcomes[index] = replace(
                            outcome,
                            checksum_state=InternetArchiveChecksumState.VERIFIED,
                        )
                        continue
                    disposition = decision.dispositions[outcome.name]
                    outcomes[index] = replace(
                        outcome,
                        disposition=disposition,
                        checksum_state=(
                            InternetArchiveChecksumState.VERIFIED
                            if disposition
                            is InternetArchiveFileDisposition.ALREADY_MATCHING
                            else InternetArchiveChecksumState.UNVERIFIED
                        ),
                    )
                missing = [
                    file
                    for file in prepared
                    if decision.dispositions[file.name]
                    is InternetArchiveFileDisposition.UNATTEMPTED
                ]
                if not missing:
                    verified = all(
                        outcome.checksum_state is InternetArchiveChecksumState.VERIFIED
                        for outcome in outcomes
                    )
                    return result(processing=wait, verification=verified)

                current = missing[0]
                current_index = next(
                    index
                    for index, outcome in enumerate(outcomes)
                    if outcome.name == current.name
                )
                headers = {
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(current.size),
                }
                if len(missing) > 1:
                    headers["x-archive-queue-derive"] = "0"
                try:
                    async with within_deadline(InternetArchiveRecoveryPhase.TRANSFER):
                        await _items._source_io(current.rewind)
                        request_timeout = remaining(
                            InternetArchiveRecoveryPhase.TRANSFER
                        )
                        outcomes[current_index] = replace(
                            outcomes[current_index],
                            disposition=InternetArchiveFileDisposition.UNCERTAIN,
                            checksum_state=InternetArchiveChecksumState.PENDING,
                        )
                        response = await self._recovery_request(
                            "PUT",
                            f"{_items.S3_URL}/{identifier}/"
                            f"{quote(current.name, safe='/')}",
                            key,
                            request_timeout=request_timeout,
                            headers=headers,
                            data=_items.AsyncPreparedFile(current.file),
                        )
                        typed_response = cast("ResponseLike", response)
                        _items.raise_for_item_status(typed_response)
                        if typed_response.status_code != HTTPStatus.OK or (
                            await async_response_text(response, service=_common.SERVICE)
                        ):
                            raise InvalidServiceResponseError(
                                "Internet Archive did not confirm file recovery",
                                service=_common.SERVICE,
                                status_code=typed_response.status_code,
                            )
                        outcomes[current_index] = replace(
                            outcomes[current_index],
                            disposition=InternetArchiveFileDisposition.TRANSFERRED,
                            checksum_state=InternetArchiveChecksumState.PENDING,
                            transferred=True,
                            etag=typed_response.headers.get("ETag"),
                        )
                except ServiceError as exc:
                    fail(
                        exc,
                        InternetArchiveRecoveryPhase.TRANSFER,
                        failed_file=current.name,
                    )
                if not wait and len(missing) == 1:
                    return result()

    async def upload(
        self,
        files: Path | Iterable[InternetArchiveUploadFile | str | Path],
        options: InternetArchiveUploadOptions,
        *,
        wait: bool = False,
        timeout: float = 300.0,  # noqa: ASYNC109 - Public polling deadline.
        poll_interval: float = 2.0,
    ) -> InternetArchiveUploadResult:
        """Create a new item and transfer files sequentially, without rollback.

        Accept a single Path or an iterable of paths or upload files, not a bare
        string or single upload file. Locally opened files are closed on exit.
        Waiting checks uploader ingest only, not derivatives or public visibility.
        Service failures after creation starts retain every file outcome.
        """
        self._ensure_open()
        if not isinstance(options, InternetArchiveUploadOptions):
            raise InvalidOptionError("options must be InternetArchiveUploadOptions")
        if not isinstance(wait, bool):
            raise InvalidOptionError("wait must be a boolean")
        timeout = _common.validate_duration(timeout, name="timeout", allow_zero=False)
        poll_interval = _common.validate_duration(
            poll_interval, name="poll_interval", allow_zero=False
        )
        async with _items.async_prepare_files(files, options.identifier) as prepared:
            self._ensure_item_transport()
            headers = _items.upload_headers(
                options, sum(file.size for file in prepared)
            )
            headers["Content-Type"] = "application/octet-stream"
            if self._api_key is None:
                await self._ensure_account_authentication()
                response = await self._request(
                    "GET",
                    _items.UPLOAD_URL,
                    cookies=self._request_cookies(),
                    allow_redirects=False,
                )
                _items.raise_for_item_status(cast("ResponseLike", response))
                self._api_key = _items.parse_upload_key(
                    await async_response_text(response, service=_common.SERVICE)
                )
            headers.update(self._headers())
            response = await self._request(
                "POST",
                _items.UPLOAD_API_URL,
                headers=self._headers(),
                cookies=self._request_cookies(),
                files={
                    "name": (None, "identifierAvailable"),
                    "identifier": (None, options.identifier),
                    "findUnique": (None, "0"),
                },
                allow_redirects=False,
            )
            _items.raise_for_item_status(cast("ResponseLike", response))
            _items.validate_availability(
                await async_response_mapping(response, service=_common.SERVICE),
                options.identifier,
            )
            outcomes = [
                InternetArchiveUploadFileResult(file.name, file.size)
                for file in prepared
            ]
            failed_file = None
            try:
                # Availability is advisory; only this create-only PUT claims the ID.
                response = await self._request(
                    "PUT",
                    f"{_items.S3_URL}/{options.identifier}",
                    headers={
                        **headers,
                        "Content-Length": "0",
                        "x-archive-queue-derive": "0",
                    },
                    data=b"",
                    allow_redirects=False,
                )
                _items.raise_for_item_status(cast("ResponseLike", response))
                if cast("ResponseLike", response).status_code != HTTPStatus.OK or (
                    await async_response_text(response, service=_common.SERVICE)
                ):
                    raise InvalidServiceResponseError(
                        "Internet Archive did not confirm item creation",
                        service=_common.SERVICE,
                        status_code=cast("ResponseLike", response).status_code,
                    )
                for index, file in enumerate(prepared):
                    failed_file = file.name
                    file_headers = {**headers, "Content-Length": str(file.size)}
                    if index < len(prepared) - 1:
                        file_headers["x-archive-queue-derive"] = "0"
                    response = await self._request(
                        "PUT",
                        f"{_items.S3_URL}/{options.identifier}/"
                        f"{quote(file.name, safe='/')}",
                        headers=file_headers,
                        data=_items.AsyncPreparedFile(file),
                        allow_redirects=False,
                    )
                    _items.raise_for_item_status(cast("ResponseLike", response))
                    if cast("ResponseLike", response).status_code != HTTPStatus.OK or (
                        await async_response_text(response, service=_common.SERVICE)
                    ):
                        raise InvalidServiceResponseError(
                            "Internet Archive did not confirm file transfer",
                            service=_common.SERVICE,
                            status_code=cast("ResponseLike", response).status_code,
                        )
                    outcomes[index] = replace(
                        outcomes[index],
                        transferred=True,
                        etag=cast("ResponseLike", response).headers.get("ETag"),
                    )
                failed_file = None
                if wait:
                    await self._wait_for_upload(
                        options.identifier,
                        wait_timeout=timeout,
                        poll_interval=poll_interval,
                    )
            except ServiceError as exc:
                raise InternetArchiveUploadError(
                    InternetArchiveUploadResult(options.identifier, tuple(outcomes)),
                    exc,
                    failed_file=failed_file,
                ) from None
            return InternetArchiveUploadResult(
                options.identifier, tuple(outcomes), processing_complete=wait
            )

    async def _wait_for_upload(
        self, identifier: str, *, wait_timeout: float, poll_interval: float
    ) -> None:
        deadline = time.monotonic() + wait_timeout
        try:
            async with asyncio.timeout(wait_timeout):
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError
                    try:
                        response = await self._request(
                            "GET",
                            _items.UPLOAD_API_URL,
                            headers=self._headers(),
                            cookies=self._request_cookies(),
                            params={"name": "catalogRows", "identifier": identifier},
                            request_timeout=remaining,
                            allow_redirects=False,
                        )
                        _items.raise_for_item_status(cast("ResponseLike", response))
                        complete = _items.parse_catalog(
                            await async_response_mapping(
                                response, service=_common.SERVICE
                            )
                        )
                    except NetworkError as exc:
                        if deadline - time.monotonic() <= 0 or (
                            exc.cause_type is not None
                            and "timeout" in exc.cause_type.lower()
                            and remaining < self._timeout
                        ):
                            raise TimeoutError from None
                        raise
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError
                    if complete:
                        return
                    await asyncio.sleep(min(poll_interval, remaining))
        except TimeoutError:
            raise PollingTimeoutError(
                "Internet Archive item ingest did not finish within "
                f"{wait_timeout:g} seconds",
                service=_common.SERVICE,
                job_id=identifier,
                timeout=wait_timeout,
            ) from None

    async def remove_items(
        self, identifiers: Iterable[str], *, comment: str = "Removed with Archivist"
    ) -> tuple[InternetArchiveRemovalResult, ...]:
        """Request make_dark for an explicit batch, returning queue acceptance."""
        self._ensure_open()
        form = _items.removal_form(identifiers, comment)
        ids = tuple(form["identifier"].split(","))
        self._ensure_item_transport()
        await self._ensure_account_authentication()
        response = await self._request(
            "POST",
            _items.MANAGE_URL,
            cookies=self._request_cookies(),
            data=form,
            allow_redirects=False,
        )
        _items.raise_for_item_status(cast("ResponseLike", response))
        return _items.parse_removal(
            await async_response_text(response, service=_common.SERVICE), ids
        )

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

    async def my_web_archive_url(self) -> str:
        """Return the authenticated account's public web archive URL."""
        await self._ensure_account_authentication()
        response = await self._request(
            "GET",
            _common.USER_INFO_URL,
            headers={"Accept": "application/json"},
            cookies=self._request_cookies(),
            params={"op": "whoami"},
        )
        raise_for_common_status(cast("ResponseLike", response), service=_common.SERVICE)
        return _common.parse_my_web_archive_url(
            await async_response_mapping(response, service=_common.SERVICE)
        )

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
