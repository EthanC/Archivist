"""Parsing and validation for Archive.today's Memento and RSS interfaces."""

from __future__ import annotations

import logging
import math
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import format_datetime, parsedate_to_datetime
from functools import wraps
from typing import ParamSpec, TypeVar
from urllib.parse import urlsplit
from xml.etree import ElementTree

from archivist.core.errors import InvalidOptionError, InvalidServiceResponseError
from archivist.services.archive_today.models import (
    ArchiveTodayMemento,
    ArchiveTodayRecentCapture,
    ArchiveTodayRecentFeed,
    ArchiveTodayTimeMap,
)

logger = logging.getLogger(__name__)

SERVICE = "Archive.today"
DEFAULT_MIRROR = "https://archive.is"
_HTTPS_DEFAULT_PORT = 443
_HTTP_DEFAULT_PORT = 80
_QUOTED_VALUE_MIN_LENGTH = 2
_KNOWN_HOSTS = frozenset(
    {
        "archive.is",
        "archive.today",
        "archive.ph",
        "archive.vn",
        "archive.fo",
        "archive.li",
        "archive.md",
    }
)
_MEMENTO_TIMESTAMP = re.compile(r"^/(\d{14})(?:/|$)")
_P = ParamSpec("_P")
_R = TypeVar("_R")


def _log_parse_failures(function: Callable[_P, _R]) -> Callable[_P, _R]:
    @wraps(function)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        try:
            return function(*args, **kwargs)
        except InvalidServiceResponseError:
            logger.warning("Archive.today response parsing failed")
            raise

    return wrapped


def validate_duration(value: object, *, name: str) -> float:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value <= 0
    ):
        raise InvalidOptionError(f"{name} must be a finite positive number")
    return float(value)


def header_value(headers: Mapping[str, str], name: str) -> str | None:
    lowered = name.lower()
    return next(
        (value for key, value in headers.items() if key.lower() == lowered), None
    )


def format_accept_datetime(value: datetime) -> str:
    if not isinstance(value, datetime):
        raise InvalidOptionError("timestamp must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise InvalidOptionError("timestamp must include timezone information")
    return format_datetime(value.astimezone(UTC), usegmt=True)


def _parse_datetime(value: str, *, field: str) -> datetime:
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        parsed = None
    if parsed is None or parsed.tzinfo is None or parsed.utcoffset() is None:
        raise InvalidServiceResponseError(
            f"{SERVICE} returned an invalid {field}", service=SERVICE
        )
    return parsed.astimezone(UTC)


def _origin(url: str) -> tuple[str, str, int] | None:
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError:
        return None
    if (
        parsed.scheme not in {"http", "https"}
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
    ):
        return None
    return (
        parsed.scheme,
        parsed.hostname.lower(),
        port if port is not None else (443 if parsed.scheme == "https" else 80),
    )


def _allowed_archive_url(candidate: str, mirror: str) -> bool:
    candidate_origin = _origin(candidate)
    if candidate_origin is None:
        return False
    if candidate_origin == _origin(mirror):
        return True
    scheme, hostname, port = candidate_origin
    return hostname in _KNOWN_HOSTS and (
        (scheme == "https" and port == _HTTPS_DEFAULT_PORT)
        or (scheme == "http" and port == _HTTP_DEFAULT_PORT)
    )


def _split_quoted(value: str, delimiter: str) -> list[str]:
    parts: list[str] = []
    start = 0
    quoted = False
    angled = False
    escaped = False
    for index, character in enumerate(value):
        if escaped:
            escaped = False
            continue
        if quoted and character == "\\":
            escaped = True
            continue
        if character == '"' and not angled:
            quoted = not quoted
        elif not quoted:
            if character == "<":
                angled = True
            elif character == ">":
                angled = False
            elif character == delimiter and not angled:
                parts.append(value[start:index].strip())
                start = index + 1
    parts.append(value[start:].strip())
    return [part for part in parts if part]


def _unquote_parameter(value: str) -> str:
    stripped = value.strip()
    if len(stripped) >= _QUOTED_VALUE_MIN_LENGTH and stripped[0] == stripped[-1] == '"':
        return re.sub(r"\\(.)", r"\1", stripped[1:-1])
    return stripped


@dataclass(frozen=True, slots=True)
class _Link:
    url: str
    parameters: Mapping[str, str]
    relations: frozenset[str]


def _parse_links(value: str) -> tuple[_Link, ...]:
    links: list[_Link] = []
    for raw_link in _split_quoted(value, ","):
        match = re.fullmatch(r"\s*<([^>]*)>\s*(.*)", raw_link, re.DOTALL)
        if match is None or not match.group(1):
            raise InvalidServiceResponseError(
                f"{SERVICE} returned invalid Memento links", service=SERVICE
            )
        parameters: dict[str, str] = {}
        remainder = match.group(2).strip()
        if remainder:
            if not remainder.startswith(";"):
                raise InvalidServiceResponseError(
                    f"{SERVICE} returned invalid Memento parameters", service=SERVICE
                )
            for raw_parameter in _split_quoted(remainder[1:], ";"):
                name, separator, raw_value = raw_parameter.partition("=")
                if not separator or not name.strip():
                    raise InvalidServiceResponseError(
                        f"{SERVICE} returned invalid Memento parameters",
                        service=SERVICE,
                    )
                parameters[name.strip().lower()] = _unquote_parameter(raw_value)
        relations = frozenset(parameters.get("rel", "").lower().split())
        links.append(_Link(match.group(1), parameters, relations))
    return tuple(links)


def _single_relation(links: tuple[_Link, ...], relation: str) -> _Link:
    matches = [link for link in links if relation in link.relations]
    if len(matches) != 1:
        raise InvalidServiceResponseError(
            f"{SERVICE} TimeMap requires one {relation} link", service=SERVICE
        )
    return matches[0]


@_log_parse_failures
def parse_timemap(body: str, *, mirror: str) -> ArchiveTodayTimeMap:
    links = _parse_links(body)
    original = _single_relation(links, "original")
    timegate = _single_relation(links, "timegate")
    timemap = _single_relation(links, "self")
    if not _allowed_archive_url(timegate.url, mirror) or not _allowed_archive_url(
        timemap.url, mirror
    ):
        raise InvalidServiceResponseError(
            f"{SERVICE} TimeMap changed service origin", service=SERVICE
        )

    items: list[ArchiveTodayMemento] = []
    for link in links:
        if "memento" not in link.relations:
            continue
        if not _allowed_archive_url(link.url, mirror):
            raise InvalidServiceResponseError(
                f"{SERVICE} Memento changed service origin", service=SERVICE
            )
        raw_datetime = link.parameters.get("datetime")
        if raw_datetime is None:
            raise InvalidServiceResponseError(
                f"{SERVICE} Memento omitted datetime", service=SERVICE
            )
        items.append(
            ArchiveTodayMemento(
                archive_url=link.url,
                original_url=original.url,
                archived_at=_parse_datetime(raw_datetime, field="Memento datetime"),
                relations=link.relations,
            )
        )
    return ArchiveTodayTimeMap(
        items=tuple(items),
        original_url=original.url,
        timegate_url=timegate.url,
        timemap_url=timemap.url,
    )


def empty_timemap(*, mirror: str, original_url: str) -> ArchiveTodayTimeMap:
    return ArchiveTodayTimeMap(
        items=(),
        original_url=original_url,
        timegate_url=f"{mirror}/timegate/{original_url}",
        timemap_url=f"{mirror}/timemap/{original_url}",
    )


@_log_parse_failures
def parse_memento_redirect(
    headers: Mapping[str, str],
    *,
    mirror: str,
    original_url: str,
    relation: str,
) -> ArchiveTodayMemento:
    location = header_value(headers, "Location")
    if location is None or not _allowed_archive_url(location, mirror):
        raise InvalidServiceResponseError(
            f"{SERVICE} returned an invalid Memento location", service=SERVICE
        )

    archived_at: datetime | None = None
    raw_links = header_value(headers, "Link")
    if raw_links is not None:
        for link in _parse_links(raw_links):
            if link.url == location and "memento" in link.relations:
                raw_datetime = link.parameters.get("datetime")
                if raw_datetime is not None:
                    archived_at = _parse_datetime(
                        raw_datetime, field="Memento datetime"
                    )
                    break
    if archived_at is None:
        match = _MEMENTO_TIMESTAMP.match(urlsplit(location).path)
        if match is not None:
            try:
                archived_at = datetime.strptime(match.group(1), "%Y%m%d%H%M%S").replace(
                    tzinfo=UTC
                )
            except ValueError:
                archived_at = None
    if archived_at is None:
        raise InvalidServiceResponseError(
            f"{SERVICE} Memento location omitted a valid timestamp", service=SERVICE
        )
    return ArchiveTodayMemento(
        archive_url=location,
        original_url=original_url,
        archived_at=archived_at,
        relations=frozenset({relation, "memento"}),
    )


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _child_text(element: ElementTree.Element, name: str) -> str | None:
    child = next((item for item in element if _local_name(item.tag) == name), None)
    if child is None or child.text is None:
        return None
    return child.text.strip()


@_log_parse_failures
def parse_rss(body: str, *, mirror: str) -> ArchiveTodayRecentFeed:
    try:
        root = ElementTree.fromstring(body)
    except ElementTree.ParseError:
        raise InvalidServiceResponseError(
            f"{SERVICE} returned invalid RSS", service=SERVICE
        ) from None
    channel = next((item for item in root if _local_name(item.tag) == "channel"), None)
    title = _child_text(channel, "title") if channel is not None else None
    if _local_name(root.tag) != "rss" or channel is None or not title:
        raise InvalidServiceResponseError(
            f"{SERVICE} returned an invalid RSS channel", service=SERVICE
        )

    raw_updated = _child_text(channel, "lastBuildDate")
    updated_at = (
        _parse_datetime(raw_updated, field="RSS build date")
        if raw_updated is not None
        else None
    )
    items: list[ArchiveTodayRecentCapture] = []
    for item in channel:
        if _local_name(item.tag) != "item":
            continue
        archive_url = _child_text(item, "link")
        if archive_url is None or not _allowed_archive_url(archive_url, mirror):
            raise InvalidServiceResponseError(
                f"{SERVICE} RSS item has an invalid archive URL", service=SERVICE
            )
        raw_published = _child_text(item, "pubDate")
        path = urlsplit(archive_url).path.rstrip("/")
        items.append(
            ArchiveTodayRecentCapture(
                archive_url=archive_url,
                title=_child_text(item, "title"),
                archived_at=(
                    _parse_datetime(raw_published, field="RSS publication date")
                    if raw_published is not None
                    else None
                ),
                description=_child_text(item, "description"),
                capture_id=path.rsplit("/", 1)[-1] or None,
            )
        )
    return ArchiveTodayRecentFeed(
        items=tuple(items), title=title, updated_at=updated_at
    )
