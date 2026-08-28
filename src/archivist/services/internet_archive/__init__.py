"""Internet Archive clients and models."""

import logging

from archivist.services.internet_archive.async_client import AsyncInternetArchiveClient
from archivist.services.internet_archive.client import InternetArchiveClient
from archivist.services.internet_archive.models import (
    InternetArchiveAccount,
    InternetArchiveApiKey,
    InternetArchiveAvailability,
    InternetArchiveCaptureJob,
    InternetArchiveCaptureStatus,
    InternetArchiveCdxRecord,
    InternetArchiveCdxResult,
    InternetArchiveCookies,
    InternetArchiveFailedStatus,
    InternetArchiveOutlinkAvailability,
    InternetArchivePendingStatus,
    InternetArchiveSaveOptions,
    InternetArchiveSnapshot,
    InternetArchiveSuccessStatus,
    InternetArchiveSystemStatus,
    InternetArchiveUserStatus,
)

logger = logging.getLogger(__name__)

__all__ = [
    "AsyncInternetArchiveClient",
    "InternetArchiveAccount",
    "InternetArchiveApiKey",
    "InternetArchiveAvailability",
    "InternetArchiveCaptureJob",
    "InternetArchiveCaptureStatus",
    "InternetArchiveCdxRecord",
    "InternetArchiveCdxResult",
    "InternetArchiveClient",
    "InternetArchiveCookies",
    "InternetArchiveFailedStatus",
    "InternetArchiveOutlinkAvailability",
    "InternetArchivePendingStatus",
    "InternetArchiveSaveOptions",
    "InternetArchiveSnapshot",
    "InternetArchiveSuccessStatus",
    "InternetArchiveSystemStatus",
    "InternetArchiveUserStatus",
]
