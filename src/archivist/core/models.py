"""Service-independent Archivist models."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Generic, TypeVar

logger = logging.getLogger(__name__)


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must include timezone information")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class ArchiveRecord:
    """A capture returned by an archive service."""

    service: str
    archive_url: str = field(repr=False)
    original_url: str | None = field(default=None, repr=False)
    archived_at: datetime | None = None
    capture_id: str | None = None

    def __post_init__(self) -> None:
        """Normalize the archive timestamp to UTC."""
        object.__setattr__(self, "archived_at", _as_utc(self.archived_at))


ItemT = TypeVar("ItemT")


@dataclass(frozen=True, slots=True)
class PagedSearchResult(Generic[ItemT]):
    """One page of service search results."""

    items: tuple[ItemT, ...]
    page: int = 1
    page_count: int = 1
    total_count: int | None = None

    def __post_init__(self) -> None:
        """Validate pagination metadata."""
        if self.page < 1:
            raise ValueError("page must be at least 1")
        if self.page_count < 1:
            raise ValueError("page_count must be at least 1")
        if self.total_count is not None and self.total_count < 0:
            raise ValueError("total_count cannot be negative")

    def __iter__(self) -> Iterator[ItemT]:
        """Iterate over the items on this page."""
        return iter(self.items)

    def __len__(self) -> int:
        """Return the number of items on this page."""
        return len(self.items)


@dataclass(frozen=True, slots=True)
class CaptureJob:
    """An accepted asynchronous capture job."""

    service: str
    job_id: str
    target_url: str = field(repr=False)
    message: str | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class ServiceStatus:
    """A service health summary."""

    service: str
    status: str
    message: str | None = None
