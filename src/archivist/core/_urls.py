"""URL validation and safe log rendering."""

from __future__ import annotations

import logging
from urllib.parse import SplitResult, urlsplit, urlunsplit

from archivist.core.errors import InvalidTargetURLError

logger = logging.getLogger(__name__)

_ASCII_CONTROL_LIMIT = 32


def _split_http_url(url: str, *, label: str) -> SplitResult:
    if not isinstance(url, str) or not url:
        raise InvalidTargetURLError(f"{label} must be a non-empty string")
    if any(
        character.isspace() or ord(character) < _ASCII_CONTROL_LIMIT
        for character in url
    ):
        raise InvalidTargetURLError(
            f"{label} cannot contain whitespace or control characters"
        )
    try:
        parsed = urlsplit(url)
        _ = parsed.port
    except ValueError as exc:
        raise InvalidTargetURLError(f"{label} is not a valid URL") from exc
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        raise InvalidTargetURLError(f"{label} must be an absolute HTTP or HTTPS URL")
    return parsed


def validate_target_url(url: str) -> str:
    """Validate an archive target without changing its wire representation."""
    _split_http_url(url, label="target URL")
    return url


def validate_cdx_query(query: str) -> str:
    """Validate an absolute URL or wildcard expression accepted by CDX."""
    if not isinstance(query, str) or not query:
        raise InvalidTargetURLError("CDX query must be a non-empty string")
    if any(
        character.isspace() or ord(character) < _ASCII_CONTROL_LIMIT
        for character in query
    ):
        raise InvalidTargetURLError(
            "CDX query cannot contain whitespace or control characters"
        )
    if "://" in query:
        validate_target_url(query)
    return query


def validate_service_url(url: str) -> str:
    """Validate and normalize a configurable archive service base URL."""
    parsed = _split_http_url(url, label="service URL")
    if parsed.username is not None or parsed.password is not None:
        raise InvalidTargetURLError("service URL cannot contain user information")
    if "?" in url or "#" in url:
        raise InvalidTargetURLError("service URL cannot contain a query or fragment")
    return url.rstrip("/")


def sanitize_url_for_log(url: str) -> str:
    """Remove user information, query parameters, and fragments from a URL."""
    try:
        parsed = urlsplit(url)
        hostname = parsed.hostname
        if not parsed.scheme or not hostname:
            return "<invalid-url>"
        if ":" in hostname and not hostname.startswith("["):
            hostname = f"[{hostname}]"
        port = parsed.port
        netloc = f"{hostname}:{port}" if port is not None else hostname
        return urlunsplit((parsed.scheme, netloc, parsed.path or "/", "", ""))
    except (TypeError, ValueError):
        return "<invalid-url>"
