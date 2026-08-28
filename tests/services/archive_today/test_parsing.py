"""Parser and model tests for Archive.today Memento and RSS responses."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

import pytest

from archivist import InvalidOptionError, InvalidServiceResponseError
from archivist.services.archive_today import _common
from archivist.services.archive_today.models import (
    ArchiveTodayMemento,
    ArchiveTodayRecentCapture,
    ArchiveTodayRecentFeed,
    ArchiveTodayTimeMap,
)

MIRROR = "https://archive.is"
ORIGINAL = "https://example.com/"
EXPECTED_MEMENTOS = 2


def _timemap(*mementos: str) -> str:
    return ",\n".join(
        (
            f'<{ORIGINAL}>; rel="original"',
            f'<{MIRROR}/timegate/{ORIGINAL}>; rel="timegate"',
            *mementos,
            (
                f'<{MIRROR}/timemap/{ORIGINAL}>; rel="self"; '
                'type="application/link-format"'
            ),
        )
    )


def _memento(
    *,
    url: str = f"{MIRROR}/20200102030405/{ORIGINAL}",
    relation: str = "memento",
    archived_at: str = "Thu, 02 Jan 2020 03:04:05 GMT",
) -> str:
    return f'<{url}>; rel="{relation}"; datetime="{archived_at}"'


def test_timemap_parser_returns_typed_mementos_and_sequence_helpers() -> None:
    """Parse RFC 7089 links, quoted commas, and relation metadata."""
    result = _common.parse_timemap(
        _timemap(
            _memento(relation="first memento"),
            _memento(
                url=f"{MIRROR}/20210102030405/{ORIGINAL}",
                relation="last memento",
                archived_at="Sat, 02 Jan 2021 03:04:05 GMT",
            ),
        ),
        mirror=MIRROR,
    )
    assert len(result) == EXPECTED_MEMENTOS
    assert list(result) == list(result.items)
    assert result.first is result.items[0]
    assert result.last is result.items[1]
    assert result.items[0].relations == frozenset({"first", "memento"})
    assert result.items[0].archived_at == datetime(2020, 1, 2, 3, 4, 5, tzinfo=UTC)

    empty = _common.empty_timemap(mirror=MIRROR, original_url=ORIGINAL)
    assert len(empty) == 0
    assert empty.first is None
    assert empty.last is None


@pytest.mark.parametrize(
    "body,match",
    [
        ("not a link", "invalid Memento links"),
        (f"<{ORIGINAL}> rel=original", "invalid Memento parameters"),
        (f"<{ORIGINAL}>; rel", "invalid Memento parameters"),
        (
            _timemap(_memento(archived_at="not-a-date")),
            "invalid Memento datetime",
        ),
        (
            _timemap(f'<{MIRROR}/20200102030405/{ORIGINAL}>; rel="memento"'),
            "omitted datetime",
        ),
        (
            _timemap(_memento(url="https://example.invalid/20200102030405/x")),
            "changed service origin",
        ),
    ],
)
def test_timemap_parser_rejects_malformed_or_unsafe_links(
    body: str, match: str
) -> None:
    """Reject responses that violate the typed Memento contract."""
    with pytest.raises(InvalidServiceResponseError, match=match):
        _common.parse_timemap(body, mirror=MIRROR)


def test_timemap_requires_unique_control_relations() -> None:
    """Require one original, TimeGate, and self link."""
    missing_original = ",".join(
        (
            f'<{MIRROR}/timegate/{ORIGINAL}>; rel="timegate"',
            f'<{MIRROR}/timemap/{ORIGINAL}>; rel="self"',
        )
    )
    duplicate_original = _timemap() + f', <{ORIGINAL}>; rel="original"'
    external_timegate = _timemap().replace(
        f"{MIRROR}/timegate", "https://example.invalid/timegate"
    )
    for body in (missing_original, duplicate_original, external_timegate):
        with pytest.raises(InvalidServiceResponseError):
            _common.parse_timemap(body, mirror=MIRROR)


def test_redirect_parser_uses_link_datetime_then_path_timestamp() -> None:
    """Read TimeGate metadata and fall back to timestamped archive paths."""
    location = f"{MIRROR}/20200102030405/{ORIGINAL}"
    linked = _common.parse_memento_redirect(
        {
            "location": location,
            "link": (
                f'<{location}>; rel="next memento"; '
                'datetime="Thu, 02 Jan 2020 04:04:05 +0100"'
            ),
        },
        mirror=MIRROR,
        original_url=ORIGINAL,
        relation="closest",
    )
    fallback = _common.parse_memento_redirect(
        {"Location": location},
        mirror=MIRROR,
        original_url=ORIGINAL,
        relation="first",
    )
    fallback_after_unrelated_links = _common.parse_memento_redirect(
        {
            "Location": location,
            "Link": (
                '<https://archive.is/other>; rel="memento"; '
                'datetime="Thu, 02 Jan 2020 03:04:05 GMT", '
                f'<{location}>; rel="original", '
                f'<{location}>; rel="memento"'
            ),
        },
        mirror=MIRROR,
        original_url=ORIGINAL,
        relation="last",
    )
    assert linked.archived_at == fallback.archived_at
    assert fallback_after_unrelated_links.archived_at == fallback.archived_at
    assert linked.relations == frozenset({"closest", "memento"})
    assert fallback.relations == frozenset({"first", "memento"})


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Location": "https://example.invalid/archive"},
        {"Location": f"{MIRROR}/short-id"},
        {"Location": f"{MIRROR}/20201302030405/example"},
    ],
)
def test_redirect_parser_rejects_invalid_locations(headers: dict[str, str]) -> None:
    """Reject missing, external, and untimestamped Memento redirects."""
    with pytest.raises(InvalidServiceResponseError):
        _common.parse_memento_redirect(
            headers, mirror=MIRROR, original_url=ORIGINAL, relation="last"
        )


def test_rss_parser_uses_only_standard_rss_fields() -> None:
    """Parse standard feed fields without scraping embedded description HTML."""
    body = f"""<?xml version="1.0"?>
    <rss version="2.0"><channel><title>archive.is</title>
    <lastBuildDate>Sat, 02 Jan 2021 03:04:05 GMT</lastBuildDate>
    <item><title>Example &amp; title</title><link>{MIRROR}/abcde</link>
    <pubDate>Thu, 02 Jan 2020 03:04:05 GMT</pubDate>
    <description><![CDATA[<a href="https://source.example/">source</a>]]></description>
    </item></channel></rss>"""
    result = _common.parse_rss(body, mirror=MIRROR)
    assert len(result) == 1
    assert list(result) == list(result.items)
    assert result.title == "archive.is"
    assert result.items[0].title == "Example & title"
    assert result.items[0].capture_id == "abcde"
    assert result.items[0].description == '<a href="https://source.example/">source</a>'

    minimal = _common.parse_rss(
        f"<rss><channel><title>x</title><item><link>{MIRROR}/id</link></item>"
        "</channel></rss>",
        mirror=MIRROR,
    )
    assert minimal.updated_at is None
    assert minimal.items[0].archived_at is None
    assert minimal.items[0].title is None
    assert minimal.items[0].description is None


@pytest.mark.parametrize(
    "body,match",
    [
        ("<rss>", "invalid RSS"),
        ("<feed />", "invalid RSS channel"),
        ("<rss><channel /></rss>", "invalid RSS channel"),
        (
            "<rss><channel><title>x</title>"
            "<lastBuildDate>bad</lastBuildDate></channel></rss>",
            "invalid RSS build date",
        ),
        (
            "<rss><channel><title>x</title><item>"
            "<link>https://example.invalid/id</link></item></channel></rss>",
            "invalid archive URL",
        ),
        (
            f"<rss><channel><title>x</title><item><link>{MIRROR}/id</link>"
            "<pubDate>bad</pubDate></item></channel></rss>",
            "invalid RSS publication date",
        ),
    ],
)
def test_rss_parser_rejects_malformed_feeds(body: str, match: str) -> None:
    """Reject malformed XML and invalid standard feed fields."""
    with pytest.raises(InvalidServiceResponseError, match=match):
        _common.parse_rss(body, mirror=MIRROR)


@pytest.mark.parametrize("value", [True, 0, -1, float("inf"), float("nan"), "1"])
def test_duration_validation_rejects_invalid_values(value: object) -> None:
    """Require finite positive request timeouts."""
    with pytest.raises(InvalidOptionError):
        _common.validate_duration(value, name="timeout")
    assert _common.validate_duration(1, name="timeout") == 1.0


def test_model_representations_hide_sensitive_urls() -> None:
    """Keep Archive.today URLs and feed descriptions out of representations."""
    secret = "https://user:password@example.com/private?token=secret"
    archived_at = datetime(2020, 1, 1, tzinfo=UTC)
    memento = ArchiveTodayMemento(secret, secret, archived_at)
    representation = repr(
        (
            memento,
            ArchiveTodayTimeMap((memento,), secret, secret, secret),
            ArchiveTodayRecentCapture(secret, None, archived_at, secret),
        )
    )
    assert secret not in representation
    assert "password" not in representation
    assert "token" not in representation


def test_accept_datetime_requires_an_aware_datetime() -> None:
    """Format aware values as RFC 1123 GMT timestamps."""
    shifted = datetime(2020, 1, 1, 1, tzinfo=timezone(timedelta(hours=1)))
    assert _common.format_accept_datetime(shifted) == "Wed, 01 Jan 2020 00:00:00 GMT"
    with pytest.raises(InvalidOptionError):
        _common.format_accept_datetime(datetime(2020, 1, 1))
    with pytest.raises(InvalidOptionError):
        _common.format_accept_datetime(cast("Any", "2020"))


def test_models_validate_timestamps_relations_and_sequence_fallbacks() -> None:
    """Enforce model invariants and normalize timestamps."""
    shifted = datetime(2020, 1, 1, 1, tzinfo=timezone(timedelta(hours=1)))
    memento = ArchiveTodayMemento(
        MIRROR + "/id", ORIGINAL, shifted, frozenset({"MEMENTO"})
    )
    assert memento.archived_at == datetime(2020, 1, 1, tzinfo=UTC)
    assert memento.relations == frozenset({"memento"})
    timemap = ArchiveTodayTimeMap((memento,), ORIGINAL, MIRROR, MIRROR)
    assert timemap.first is memento
    assert timemap.last is memento
    recent = ArchiveTodayRecentCapture(MIRROR + "/id", None, shifted)
    feed = ArchiveTodayRecentFeed((recent,), "feed", shifted)
    assert recent.archived_at == datetime(2020, 1, 1, tzinfo=UTC)
    assert feed.updated_at == datetime(2020, 1, 1, tzinfo=UTC)

    with pytest.raises(ValueError, match="timezone"):
        ArchiveTodayMemento(MIRROR + "/id", ORIGINAL, datetime(2020, 1, 1))
    with pytest.raises(ValueError, match="relations"):
        ArchiveTodayMemento(
            MIRROR + "/id", ORIGINAL, datetime(2020, 1, 1, tzinfo=UTC), frozenset()
        )
    with pytest.raises(ValueError, match="timezone"):
        ArchiveTodayRecentCapture(MIRROR + "/id", None, datetime(2020, 1, 1))
    with pytest.raises(ValueError, match="timezone"):
        ArchiveTodayRecentFeed((), "feed", datetime(2020, 1, 1))


def test_helpers_handle_headers_origins_and_escaped_parameters() -> None:
    """Cover case-insensitive headers and strict service-origin checks."""
    assert _common.header_value({"LiNk": "value"}, "link") == "value"
    assert _common.header_value({}, "link") is None
    assert _common._allowed_archive_url("http://archive.md/id", MIRROR)
    assert _common._allowed_archive_url("https://archive.ph/id", MIRROR)
    assert not _common._allowed_archive_url("https://archive.ph:444/id", MIRROR)
    assert not _common._allowed_archive_url("https://user@archive.is/id", MIRROR)
    assert not _common._allowed_archive_url("https://archive.is:bad/id", MIRROR)
    links = _common._parse_links(
        r'<https://archive.is/id>; rel="memento"; title="a\"b"'
    )
    assert links[0].parameters["title"] == 'a"b'
    assert _common._parse_links("<https://archive.is/id>")[0].parameters == {}
    assert _common._parse_links("<https://archive.is/id>; rel=memento")[
        0
    ].relations == frozenset({"memento"})
