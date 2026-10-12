"""Exceptions raised by Archivist clients."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TypedDict, Unpack

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


class ServiceDiagnostics(TypedDict, total=False):
    """Optional, pickle-compatible context for a service failure."""

    service_code: str | None
    service_message: str | None
    request_id: str | None
    request_phase: str | None
    retry_after: float | datetime | None
    retry_after_raw: str | None
    diagnostic_headers: dict[str, str]


def _restore_service_error(
    cls: type[ServiceError], args: tuple[object, ...], state: dict[str, object]
) -> ServiceError:
    """Restore subclasses whose constructors require results or keyword fields."""
    error = cls.__new__(cls)
    Exception.__init__(error, *args)
    error.__dict__.update(state)
    return error


class ServiceError(ArchivistError):
    """An archive service request failed."""

    def __init__(  # noqa: PLR0913 - Public optional diagnostic fields.
        self,
        message: str,
        *,
        service: str | None = None,
        status_code: int | None = None,
        service_code: str | None = None,
        service_message: str | None = None,
        request_id: str | None = None,
        request_phase: str | None = None,
        retry_after: float | datetime | None = None,
        retry_after_raw: str | None = None,
        diagnostic_headers: dict[str, str] | None = None,
    ) -> None:
        """Initialize the error with service response context."""
        super().__init__(message)
        self.service = service
        self.status_code = status_code
        self.service_code = service_code
        self.service_message = service_message
        self.request_id = request_id
        self.request_phase = request_phase
        self.retry_after = retry_after
        self.retry_after_raw = retry_after_raw
        self.diagnostic_headers = dict(diagnostic_headers or {})

    def __reduce__(self) -> tuple[object, tuple[object, ...]]:
        """Retain fields and wrapped causes without invoking subclass constructors."""
        return _restore_service_error, (type(self), self.args, self.__dict__)


class NetworkError(ServiceError):
    """A request failed before a usable HTTP response was received."""

    def __init__(
        self,
        message: str,
        *,
        service: str | None = None,
        status_code: int | None = None,
        cause_type: str | None = None,
        **diagnostics: Unpack[ServiceDiagnostics],
    ) -> None:
        """Initialize the error with transport failure context."""
        super().__init__(
            message, service=service, status_code=status_code, **diagnostics
        )
        self.cause_type = cause_type


class TLSVerificationError(NetworkError):
    """TLS negotiation or certificate verification failed."""


class AuthenticationError(ServiceError):
    """A service rejected or requires the supplied credentials."""


class RateLimitError(ServiceError):
    """A service refused a request because a rate limit was reached."""


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
        **diagnostics: Unpack[ServiceDiagnostics],
    ) -> None:
        """Initialize the error with failed capture details."""
        super().__init__(
            message, service=service, status_code=status_code, **diagnostics
        )
        self.job_id = job_id


class PollingTimeoutError(ServiceError, TimeoutError):
    """A capture did not reach a terminal state before its deadline."""

    def __init__(
        self,
        message: str,
        *,
        service: str | None = None,
        job_id: str | None = None,
        timeout: float | None = None,
        **diagnostics: Unpack[ServiceDiagnostics],
    ) -> None:
        """Initialize the error with polling deadline details."""
        super().__init__(message, service=service, **diagnostics)
        self.job_id = job_id
        self.timeout = timeout
