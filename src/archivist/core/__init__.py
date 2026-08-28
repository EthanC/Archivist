"""Shared models, protocols, and exceptions."""

import logging

from archivist.core.errors import (
    ArchivistError,
    AuthenticationError,
    CaptureFailedError,
    InvalidOptionError,
    InvalidServiceResponseError,
    InvalidTargetURLError,
    InvalidURLError,
    NetworkError,
    OptionCombinationError,
    PollingTimeoutError,
    RateLimitError,
    ServiceError,
    TLSVerificationError,
)
from archivist.core.models import (
    ArchiveRecord,
    CaptureJob,
    PagedSearchResult,
    ServiceStatus,
)
from archivist.core.protocols import Archiver, AsyncArchiver, AsyncSearcher, Searcher

logger = logging.getLogger(__name__)

__all__ = [
    "ArchiveRecord",
    "Archiver",
    "ArchivistError",
    "AsyncArchiver",
    "AsyncSearcher",
    "AuthenticationError",
    "CaptureFailedError",
    "CaptureJob",
    "InvalidOptionError",
    "InvalidServiceResponseError",
    "InvalidTargetURLError",
    "InvalidURLError",
    "NetworkError",
    "OptionCombinationError",
    "PagedSearchResult",
    "PollingTimeoutError",
    "RateLimitError",
    "Searcher",
    "ServiceError",
    "ServiceStatus",
    "TLSVerificationError",
]
