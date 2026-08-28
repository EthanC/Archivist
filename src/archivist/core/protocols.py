"""Structural protocols for archive clients and third-party integrations."""

from __future__ import annotations

import logging
from typing import Protocol, TypeVar, runtime_checkable

logger = logging.getLogger(__name__)

ResultT_co = TypeVar("ResultT_co", covariant=True)


@runtime_checkable
class Archiver(Protocol[ResultT_co]):
    """A synchronous service that can archive one URL."""

    def save(self, target_url: str, /) -> ResultT_co:
        """Archive one URL and return the service result."""
        ...


@runtime_checkable
class AsyncArchiver(Protocol[ResultT_co]):
    """An asynchronous service that can archive one URL."""

    async def save(self, target_url: str, /) -> ResultT_co:
        """Archive one URL and return the service result."""
        ...


@runtime_checkable
class Searcher(Protocol[ResultT_co]):
    """A synchronous service that can search archive records."""

    def search(self, query: str, /) -> ResultT_co:
        """Search records and return a service-specific result."""
        ...


@runtime_checkable
class AsyncSearcher(Protocol[ResultT_co]):
    """An asynchronous service that can search archive records."""

    async def search(self, query: str, /) -> ResultT_co:
        """Search records and return a service-specific result."""
        ...
