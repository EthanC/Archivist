"""Run opt-in read-only checks against stable archive interfaces."""

from __future__ import annotations

import os

import pytest

from archivist import (
    ArchiveTodayClient,
    InternetArchiveClient,
    RateLimitError,
)

pytestmark = pytest.mark.live


def require_live_tests() -> None:
    """Skip live checks unless explicitly enabled by the environment."""
    if os.environ.get("ARCHIVIST_RUN_LIVE") != "1":
        pytest.skip("set ARCHIVIST_RUN_LIVE=1 to run read-only service checks")


def test_internet_archive_availability_live() -> None:
    """Confirm Internet Archive returns an available example snapshot."""
    require_live_tests()
    try:
        with InternetArchiveClient() as client:
            result = client.availability("https://example.com/")
    except RateLimitError as exc:
        pytest.skip(str(exc))
    assert result.closest is not None
    assert "web.archive.org" in result.closest.archive_url


def test_archive_today_memento_live() -> None:
    """Confirm Archive.today returns an RFC 7089 capture history."""
    require_live_tests()
    try:
        with ArchiveTodayClient() as client:
            result = client.timemap("https://www.iana.org/domains/reserved")
    except RateLimitError as exc:
        pytest.skip(str(exc))
    assert result.items
    assert result.first is not None
    assert result.last is not None
    assert all("memento" in item.relations for item in result)


def test_archive_today_rss_live() -> None:
    """Confirm Archive.today returns a standard RSS recent-capture feed."""
    require_live_tests()
    try:
        with ArchiveTodayClient() as client:
            result = client.recent_captures()
    except RateLimitError as exc:
        pytest.skip(str(exc))
    assert result.items
    assert all(item.archive_url.startswith("http") for item in result)
