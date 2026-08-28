"""Exceptions raised by Archivist clients."""

from __future__ import annotations

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class ArchivistError(Exception):
    """Base class for all package exceptions."""


class InvalidTargetURLError(ArchivistError, ValueError):
    """The target is not an absolute HTTP or HTTPS URL."""


InvalidURLError = InvalidTargetURLError


class InvalidOptionError(ArchivistError, ValueError):
    """A client option has an invalid value."""


class OptionCombinationError(InvalidOptionError):
    """Two or more otherwise valid options cannot be used together."""


class ServiceError(ArchivistError):
    """An archive service request failed."""

    def __init__(
        self,
        message: str,
        *,
        service: str | None = None,
        status_code: int | None = None,
    ) -> None:
        """Initialize the error with service response context."""
        super().__init__(message)
        self.service = service
        self.status_code = status_code


class NetworkError(ServiceError):
    """A request failed before a usable HTTP response was received."""

    def __init__(
        self,
        message: str,
        *,
        service: str | None = None,
        status_code: int | None = None,
        cause_type: str | None = None,
    ) -> None:
        """Initialize the error with transport failure context."""
        super().__init__(message, service=service, status_code=status_code)
        self.cause_type = cause_type


class TLSVerificationError(NetworkError):
    """TLS negotiation or certificate verification failed."""


class AuthenticationError(ServiceError):
    """A service rejected or requires the supplied credentials."""


class RateLimitError(ServiceError):
    """A service refused a request because a rate limit was reached."""

    def __init__(
        self,
        message: str,
        *,
        service: str | None = None,
        status_code: int | None = None,
        retry_after: float | datetime | None = None,
        retry_after_raw: str | None = None,
    ) -> None:
        """Initialize the error with retry timing information."""
        super().__init__(message, service=service, status_code=status_code)
        self.retry_after = retry_after
        self.retry_after_raw = retry_after_raw


class InvalidServiceResponseError(ServiceError):
    """A service returned an undocumented or malformed response."""


class CaptureFailedError(ServiceError):
    """A service accepted a capture job but later reported failure."""

    def __init__(
        self,
        message: str,
        *,
        service: str | None = None,
        status_code: int | None = None,
        job_id: str | None = None,
        service_code: str | None = None,
    ) -> None:
        """Initialize the error with failed capture details."""
        super().__init__(message, service=service, status_code=status_code)
        self.job_id = job_id
        self.service_code = service_code


class PollingTimeoutError(ServiceError, TimeoutError):
    """A capture did not reach a terminal state before its deadline."""

    def __init__(
        self,
        message: str,
        *,
        service: str | None = None,
        job_id: str | None = None,
        timeout: float | None = None,
    ) -> None:
        """Initialize the error with polling deadline details."""
        super().__init__(message, service=service)
        self.job_id = job_id
        self.timeout = timeout
