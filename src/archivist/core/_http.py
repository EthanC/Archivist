"""Shared HTTP response and exception handling."""

from __future__ import annotations

import logging
import math
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from inspect import isawaitable
from typing import Any, Protocol, cast

import niquests

from archivist.core.errors import (
    AuthenticationError,
    InvalidServiceResponseError,
    NetworkError,
    RateLimitError,
    TLSVerificationError,
)

logger = logging.getLogger(__name__)

_HTTP_ERROR_STATUS = 400
_HTTP_RATE_LIMIT_STATUS = 429
_AUTHENTICATION_STATUSES = frozenset({401, 403})
_ERROR_FIELDS = ("error", "message", "detail", "reason")
_MAX_ERROR_REASON_LENGTH = 500
_EMAIL_ADDRESS = re.compile(
    r"\b[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@"
    r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+\b"
)
_URL_USER_INFO = re.compile(r"(?i)(https?://)[^\s/@]+(?::[^\s/@]*)?@")


class ResponseLike(Protocol):
    status_code: int
    headers: Mapping[str, str]
    text: str
    url: str

    def json(self) -> object:
        """Decode the response as JSON."""
        ...


class AsyncJsonResponseLike(Protocol):
    """Expose fields consumed while decoding an asynchronous JSON response."""

    status_code: int

    def json(self) -> object:
        """Decode the response as JSON, possibly returning an awaitable."""
        ...


class AsyncTextResponseLike(Protocol):
    """Expose fields consumed while decoding an asynchronous text response."""

    status_code: int
    text: object


class _ErrorTextParser(HTMLParser):
    """Collect visible text while discarding markup and its attributes."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        """Collect one visible text fragment."""
        self.parts.append(data)


def parse_retry_after(
    headers: Mapping[str, str],
) -> tuple[float | datetime | None, str | None]:
    """Parse Retry-After while preserving an unrecognized raw value."""
    raw = headers.get("Retry-After") or headers.get("retry-after")
    if raw is None:
        return None, None
    value = raw.strip()
    try:
        seconds = float(value)
    except ValueError:
        try:
            date = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None, raw
        if date.tzinfo is None:
            date = date.replace(tzinfo=UTC)
        return date.astimezone(UTC), raw
    if not math.isfinite(seconds):
        return None, raw
    return max(0.0, seconds), raw


def sanitize_service_reason(value: object) -> str | None:
    """Return bounded visible service text without common reflected credentials."""
    if not isinstance(value, str):
        return None
    parser = _ErrorTextParser()
    parser.feed(value)
    reason = " ".join(" ".join(parser.parts).split())
    reason = _URL_USER_INFO.sub(r"\1<redacted>@", reason)
    reason = _EMAIL_ADDRESS.sub("<redacted-email>", reason)
    return reason[:_MAX_ERROR_REASON_LENGTH].rstrip() or None


def response_error_reason(response: ResponseLike) -> str | None:
    """Extract a bounded, sanitized reason from a recognized error response."""
    content_type = next(
        (
            value
            for key, value in response.headers.items()
            if key.lower() == "content-type"
        ),
        "",
    )
    media_type = content_type.partition(";")[0].strip().lower()
    value: object
    if media_type == "application/json" or media_type.endswith("+json"):
        try:
            data = response.json()
        except (
            niquests.exceptions.JSONDecodeError,
            niquests.exceptions.RequestException,
            NotImplementedError,
            OSError,
            TypeError,
            ValueError,
        ):
            return None
        value = (
            next(
                (
                    data.get(field)
                    for field in _ERROR_FIELDS
                    if isinstance(data.get(field), str) and data.get(field)
                ),
                None,
            )
            if isinstance(data, Mapping)
            else None
        )
    elif media_type.startswith("text/"):
        try:
            value = response.text
        except (niquests.exceptions.RequestException, OSError):
            return None
    else:
        return None
    return sanitize_service_reason(value)


def response_json(response: ResponseLike, *, service: str) -> object:
    """Decode JSON or raise a body-safe service response error."""
    try:
        return response.json()
    except niquests.exceptions.JSONDecodeError:
        error = None
    except (niquests.exceptions.RequestException, OSError) as exc:
        error = translate_request_error(exc, service=service)
    except (TypeError, ValueError):
        error = None
    if error is not None:
        raise error from None
    logger.warning("%s returned invalid JSON", service)
    raise InvalidServiceResponseError(
        f"{service} returned invalid JSON",
        service=service,
        status_code=response.status_code,
    ) from None


def response_mapping(response: ResponseLike, *, service: str) -> Mapping[str, Any]:
    """Decode a JSON object response."""
    value = response_json(response, service=service)
    if not isinstance(value, Mapping):
        logger.warning("%s returned JSON with an unexpected shape", service)
        raise InvalidServiceResponseError(
            f"{service} returned JSON with an unexpected shape",
            service=service,
            status_code=response.status_code,
        )
    return cast("Mapping[str, Any]", value)


async def async_response_json(response: object, *, service: str) -> object:
    """Decode either a regular or multiplexed niquests async response."""
    typed = cast("AsyncJsonResponseLike", response)
    try:
        value = typed.json()
        if isawaitable(value):
            value = await value
        return value
    except niquests.exceptions.JSONDecodeError:
        error = None
    except (niquests.exceptions.RequestException, OSError) as exc:
        error = translate_request_error(exc, service=service)
    except (TypeError, ValueError):
        error = None
    if error is not None:
        raise error from None
    logger.warning("%s returned invalid JSON", service)
    raise InvalidServiceResponseError(
        f"{service} returned invalid JSON",
        service=service,
        status_code=typed.status_code,
    ) from None


async def async_response_mapping(
    response: object, *, service: str
) -> Mapping[str, Any]:
    """Decode an async JSON object response."""
    value = await async_response_json(response, service=service)
    if not isinstance(value, Mapping):
        logger.warning("%s returned JSON with an unexpected shape", service)
        raise InvalidServiceResponseError(
            f"{service} returned JSON with an unexpected shape",
            service=service,
            status_code=cast("AsyncJsonResponseLike", response).status_code,
        )
    return cast("Mapping[str, Any]", value)


def response_text(response: ResponseLike, *, service: str) -> str:
    """Read a regular response body and require decoded text."""
    try:
        value = response.text
    except (niquests.exceptions.RequestException, OSError) as exc:
        error = translate_request_error(exc, service=service)
    else:
        error = None
    if error is not None:
        raise error from None
    if not isinstance(value, str):
        logger.warning("%s returned a non-text response", service)
        raise InvalidServiceResponseError(
            f"{service} returned a non-text response",
            service=service,
            status_code=response.status_code,
        )
    return value


async def async_response_text(response: object, *, service: str) -> str:
    """Read text from either niquests async response representation."""
    typed = cast("AsyncTextResponseLike", response)
    try:
        value = typed.text
        if isawaitable(value):
            value = await value
    except (niquests.exceptions.RequestException, OSError) as exc:
        error = translate_request_error(exc, service=service)
    else:
        error = None
    if error is not None:
        raise error from None
    if not isinstance(value, str):
        logger.warning("%s returned a non-text response", service)
        raise InvalidServiceResponseError(
            f"{service} returned a non-text response",
            service=service,
            status_code=typed.status_code,
        )
    return value


def raise_for_common_status(
    response: ResponseLike,
    *,
    service: str,
    authentication_statuses: frozenset[int] = _AUTHENTICATION_STATUSES,
) -> None:
    """Translate common HTTP failures with a sanitized service reason."""
    status_code = response.status_code
    if status_code < _HTTP_ERROR_STATUS:
        return
    reason = response_error_reason(response)
    reason_suffix = f": {reason}" if reason is not None else ""
    if status_code in authentication_statuses:
        logger.warning(
            "%s rejected request credentials with HTTP %s", service, status_code
        )
        raise AuthenticationError(
            f"{service} rejected the request credentials{reason_suffix}",
            service=service,
            status_code=status_code,
        )
    if status_code == _HTTP_RATE_LIMIT_STATUS:
        retry_after, raw = parse_retry_after(response.headers)
        logger.warning("%s rate limit reached", service)
        raise RateLimitError(
            f"{service} rate limit reached{reason_suffix}",
            service=service,
            status_code=status_code,
            retry_after=retry_after,
            retry_after_raw=raw,
        )
    logger.warning("%s returned HTTP %s", service, status_code)
    raise InvalidServiceResponseError(
        f"{service} returned HTTP {status_code}{reason_suffix}",
        service=service,
        status_code=status_code,
    )


def translate_request_error(exc: BaseException, *, service: str) -> NetworkError:
    """Convert a transport exception without copying its potentially sensitive text."""
    type_name = type(exc).__name__.lower()
    if "ssl" in type_name or "tls" in type_name:
        return TLSVerificationError(
            f"TLS request to {service} failed",
            service=service,
            cause_type=type(exc).__name__,
        )
    return NetworkError(
        f"network request to {service} failed",
        service=service,
        cause_type=type(exc).__name__,
    )


_SECRET_KEYS = frozenset(
    {
        "authorization",
        "cookie",
        "capture_cookie",
        "password",
        "target_password",
        "logged-in-sig",
        "logged-in-user",
        "secret_key",
    }
)


def redact_mapping(values: Mapping[str, object]) -> dict[str, object]:
    """Return a shallow, log-safe copy of request metadata."""
    return {
        key: "<redacted>" if key.lower() in _SECRET_KEYS else value
        for key, value in values.items()
    }
