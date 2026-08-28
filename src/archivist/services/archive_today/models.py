"""Public models for Archive.today's stable read interfaces."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must include timezone information")
    return value.astimezone(UTC)


def _required_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must include timezone information")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class ArchiveTodayMemento:
    """One capture advertised by Archive.today's Memento interface."""

    archive_url: str = field(repr=False)
    original_url: str = field(repr=False)
    archived_at: datetime
    relations: frozenset[str] = field(default_factory=lambda: frozenset({"memento"}))
    service: str = field(default="archive_today", init=False)

    def __post_init__(self) -> None:
        """Normalize the capture timestamp and relation names."""
        relations = frozenset(relation.lower() for relation in self.relations)
        if "memento" not in relations:
            raise ValueError("relations must include memento")
        object.__setattr__(self, "archived_at", _required_utc(self.archived_at))
        object.__setattr__(self, "relations", relations)


@dataclass(frozen=True, slots=True)
class ArchiveTodayTimeMap:
    """All Archive.today Mementos advertised for one original URL."""

    items: tuple[ArchiveTodayMemento, ...]
    original_url: str = field(repr=False)
    timegate_url: str = field(repr=False)
    timemap_url: str = field(repr=False)

    def __iter__(self) -> Iterator[ArchiveTodayMemento]:
        """Iterate over Mementos in service order."""
        return iter(self.items)

    def __len__(self) -> int:
        """Return the number of Mementos."""
        return len(self.items)

    @property
    def first(self) -> ArchiveTodayMemento | None:
        """Return the first Memento, if present."""
        return next(
            (item for item in self.items if "first" in item.relations),
            self.items[0] if self.items else None,
        )

    @property
    def last(self) -> ArchiveTodayMemento | None:
        """Return the last Memento, if present."""
        return next(
            (item for item in reversed(self.items) if "last" in item.relations),
            self.items[-1] if self.items else None,
        )


@dataclass(frozen=True, slots=True)
class ArchiveTodayRecentCapture:
    """One item from Archive.today's RSS recent-capture feed."""

    archive_url: str = field(repr=False)
    title: str | None
    archived_at: datetime | None
    description: str | None = field(default=None, repr=False)
    capture_id: str | None = None
    service: str = field(default="archive_today", init=False)

    def __post_init__(self) -> None:
        """Normalize the publication timestamp to UTC."""
        object.__setattr__(self, "archived_at", _as_utc(self.archived_at))


@dataclass(frozen=True, slots=True)
class ArchiveTodayRecentFeed:
    """Archive.today's standard RSS recent-capture feed."""

    items: tuple[ArchiveTodayRecentCapture, ...]
    title: str
    updated_at: datetime | None

    def __post_init__(self) -> None:
        """Normalize the feed timestamp to UTC."""
        object.__setattr__(self, "updated_at", _as_utc(self.updated_at))

    def __iter__(self) -> Iterator[ArchiveTodayRecentCapture]:
        """Iterate over recent captures."""
        return iter(self.items)

    def __len__(self) -> int:
        """Return the number of recent captures."""
        return len(self.items)
