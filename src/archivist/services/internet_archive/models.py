"""Public models for Internet Archive operations."""

from __future__ import annotations

import logging
import math
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Literal, TypeAlias

from archivist.core.errors import InvalidOptionError

logger = logging.getLogger(__name__)

_MAX_JS_BEHAVIOR_TIMEOUT = 30
_ARCHIVE_AGE_PAIR_LENGTH = 2


def _require_nonempty(value: str, name: str) -> None:
    if not isinstance(value, str) or not value:
        raise InvalidOptionError(f"{name} must be a non-empty string")


@dataclass(frozen=True, slots=True)
class InternetArchiveApiKey:
    """A Save Page Now LOW access and secret key pair."""

    access_key: str = field(repr=False)
    secret_key: str = field(repr=False)

    def __post_init__(self) -> None:
        """Validate both key components."""
        _require_nonempty(self.access_key, "access_key")
        _require_nonempty(self.secret_key, "secret_key")


@dataclass(frozen=True, slots=True)
class InternetArchiveCookies:
    """Direct Internet Archive account cookies."""

    logged_in_user: str = field(repr=False)
    logged_in_sig: str = field(repr=False)

    def __post_init__(self) -> None:
        """Validate both cookie values."""
        _require_nonempty(self.logged_in_user, "logged_in_user")
        _require_nonempty(self.logged_in_sig, "logged_in_sig")


@dataclass(frozen=True, slots=True)
class InternetArchiveAccount:
    """Credentials used for the Archive.org account login flow."""

    username: str = field(repr=False)
    password: str = field(repr=False)
    remember: bool = True

    def __post_init__(self) -> None:
        """Validate account credentials and options."""
        _require_nonempty(self.username, "username")
        _require_nonempty(self.password, "password")
        if not isinstance(self.remember, bool):
            raise InvalidOptionError("remember must be a boolean")


ArchiveAge: TypeAlias = str | int | timedelta


def _format_age(value: ArchiveAge) -> str:
    if isinstance(value, bool):
        raise InvalidOptionError("if_not_archived_within cannot be a boolean")
    if isinstance(value, int):
        if value < 0:
            raise InvalidOptionError("if_not_archived_within cannot be negative")
        return str(value)
    if isinstance(value, timedelta):
        seconds = float(value.total_seconds())
        if seconds < 0 or not seconds.is_integer():
            raise InvalidOptionError(
                "if_not_archived_within timedelta must contain whole "
                "non-negative seconds"
            )
        return str(int(seconds))
    if not isinstance(value, str):
        raise InvalidOptionError(
            "if_not_archived_within must be text, seconds, a timedelta, or a pair"
        )
    if not value.strip():
        raise InvalidOptionError("if_not_archived_within cannot be empty")
    return value.strip()


@dataclass(frozen=True, slots=True)
class InternetArchiveSaveOptions:
    """Options accepted by the Save Page Now 2 capture endpoint."""

    capture_all: bool = False
    capture_outlinks: bool = False
    capture_screenshot: bool = False
    delay_availability: bool = False
    force_get: bool = False
    skip_first_archive: bool = False
    if_not_archived_within: ArchiveAge | tuple[ArchiveAge, ArchiveAge] | None = None
    outlinks_availability: bool = False
    email_result: bool = False
    js_behavior_timeout: int | float | None = None
    capture_cookie: str | None = field(default=None, repr=False)
    user_agent: str | None = None
    target_username: str | None = field(default=None, repr=False)
    target_password: str | None = field(default=None, repr=False)
    disable_adblocker: bool = False
    save_to_archive: bool = False
    email_wacz: bool = False

    def __post_init__(self) -> None:
        """Validate every Save Page Now option."""
        boolean_fields = (
            "capture_all",
            "capture_outlinks",
            "capture_screenshot",
            "delay_availability",
            "force_get",
            "skip_first_archive",
            "outlinks_availability",
            "email_result",
            "disable_adblocker",
            "save_to_archive",
            "email_wacz",
        )
        if any(not isinstance(getattr(self, name), bool) for name in boolean_fields):
            raise InvalidOptionError("Save Page Now boolean options must be booleans")

        timeout = self.js_behavior_timeout
        if timeout is not None and (
            not isinstance(timeout, (int, float))
            or isinstance(timeout, bool)
            or not math.isfinite(timeout)
            or not 0 <= timeout <= _MAX_JS_BEHAVIOR_TIMEOUT
        ):
            raise InvalidOptionError(
                "js_behavior_timeout must be between 0 and 30 seconds"
            )
        age = self.if_not_archived_within
        if age is not None:
            if isinstance(age, tuple):
                if len(age) != _ARCHIVE_AGE_PAIR_LENGTH:
                    raise InvalidOptionError(
                        "if_not_archived_within pair must contain two values"
                    )
                for value in age:
                    _format_age(value)
            else:
                _format_age(age)

        for name in (
            "capture_cookie",
            "user_agent",
            "target_username",
            "target_password",
        ):
            value = getattr(self, name)
            if value is not None and not isinstance(value, str):
                raise InvalidOptionError(f"{name} must be a string or None")

    def to_form(self) -> dict[str, str]:
        """Serialize enabled options for a capture request."""
        form: dict[str, str] = {}
        documented_flags = (
            ("capture_all", "capture_all"),
            ("capture_outlinks", "capture_outlinks"),
            ("capture_screenshot", "capture_screenshot"),
            ("delay_availability", "delay_wb_availability"),
            ("force_get", "force_get"),
            ("skip_first_archive", "skip_first_archive"),
            ("outlinks_availability", "outlinks_availability"),
            ("email_result", "email_result"),
        )
        for option_name, form_name in documented_flags:
            if getattr(self, option_name):
                form[form_name] = "1"

        age = self.if_not_archived_within
        if age is not None:
            if isinstance(age, tuple):
                form["if_not_archived_within"] = ",".join(
                    _format_age(value) for value in age
                )
            else:
                form["if_not_archived_within"] = _format_age(age)

        if self.js_behavior_timeout is not None:
            form["js_behavior_timeout"] = format(self.js_behavior_timeout, "g")
        optional_strings = (
            ("capture_cookie", "capture_cookie"),
            ("user_agent", "use_user_agent"),
            ("target_username", "target_username"),
            ("target_password", "target_password"),
        )
        for option_name, form_name in optional_strings:
            value = getattr(self, option_name)
            if value is not None:
                form[form_name] = value

        if self.disable_adblocker:
            form["disable_adblocker"] = "on"
        if self.save_to_archive:
            form["wm-save-mywebarchive"] = "on"
        if self.email_wacz:
            form["wacz"] = "on"
        return form


@dataclass(frozen=True, slots=True)
class InternetArchiveCaptureJob:
    """A capture accepted by Save Page Now."""

    job_id: str
    target_url: str = field(repr=False)
    message: str | None = field(default=None, repr=False)
    service: str = field(default="internet_archive", init=False)


@dataclass(frozen=True, slots=True)
class InternetArchiveOutlinkAvailability:
    """The closest known capture for one discovered outlink."""

    timestamp: datetime | None


Outlinks: TypeAlias = (
    tuple[str, ...]
    | Mapping[str, str]
    | Mapping[str, InternetArchiveOutlinkAvailability]
    | None
)


@dataclass(frozen=True, slots=True)
class InternetArchivePendingStatus:
    """A Save Page Now job that is still running."""

    job_id: str
    resources: tuple[str, ...] = field(default=(), repr=False)
    download_size: int | float | None = None
    total_size: int | float | None = None
    retry_after: float | datetime | None = None
    status: Literal["pending"] = field(default="pending", init=False)


@dataclass(frozen=True, slots=True)
class InternetArchiveSuccessStatus:
    """A completed Save Page Now capture."""

    job_id: str
    original_url: str = field(repr=False)
    timestamp: datetime
    duration_seconds: float | None = None
    resources: tuple[str, ...] = field(default=(), repr=False)
    message: str | None = field(default=None, repr=False)
    first_archive: bool | None = None
    screenshot: str | None = field(default=None, repr=False)
    outlinks: Outlinks = field(default=None, repr=False)
    status: Literal["success"] = field(default="success", init=False)

    def __post_init__(self) -> None:
        """Normalize the capture timestamp to UTC."""
        if self.timestamp.tzinfo is None or self.timestamp.utcoffset() is None:
            raise ValueError("timestamp must include timezone information")
        object.__setattr__(self, "timestamp", self.timestamp.astimezone(UTC))

    @property
    def wayback_timestamp(self) -> str:
        """Return the 14-digit UTC timestamp used in Wayback URLs."""
        return self.timestamp.strftime("%Y%m%d%H%M%S")

    def archive_url(self) -> str:
        """Return the Wayback URL for this completed capture."""
        return (
            f"https://web.archive.org/web/{self.wayback_timestamp}/{self.original_url}"
        )


@dataclass(frozen=True, slots=True)
class InternetArchiveFailedStatus:
    """A Save Page Now job that ended in failure."""

    job_id: str
    message: str | None = field(default=None, repr=False)
    service_code: str | None = None
    resources: tuple[str, ...] = field(default=(), repr=False)
    status: Literal["error"] = field(default="error", init=False)


InternetArchiveCaptureStatus: TypeAlias = (
    InternetArchivePendingStatus
    | InternetArchiveSuccessStatus
    | InternetArchiveFailedStatus
)


@dataclass(frozen=True, slots=True)
class InternetArchiveUserStatus:
    """Per-account Save Page Now queue capacity."""

    available: int
    processing: int


@dataclass(frozen=True, slots=True)
class InternetArchiveSystemStatus:
    """Save Page Now system health and queue metrics."""

    status: str
    recent_captures: int | None = None
    queues: Mapping[str, int] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        """Copy queue metrics into an immutable mapping."""
        object.__setattr__(self, "queues", MappingProxyType(dict(self.queues)))


@dataclass(frozen=True, slots=True)
class InternetArchiveSnapshot:
    """The capture selected by the Wayback Availability API."""

    archive_url: str = field(repr=False)
    timestamp: datetime
    status_code: str | None = None
    available: bool = True

    def __post_init__(self) -> None:
        """Normalize the snapshot timestamp to UTC."""
        if self.timestamp.tzinfo is None or self.timestamp.utcoffset() is None:
            raise ValueError("timestamp must include timezone information")
        object.__setattr__(self, "timestamp", self.timestamp.astimezone(UTC))


@dataclass(frozen=True, slots=True)
class InternetArchiveAvailability:
    """A Wayback Availability API result."""

    target_url: str = field(repr=False)
    closest: InternetArchiveSnapshot | None
    requested_timestamp: str | None = None


@dataclass(frozen=True, slots=True)
class InternetArchiveCdxRecord:
    """One capture returned by the Wayback CDX server."""

    timestamp: datetime
    original_url: str = field(repr=False)
    mime_type: str | None = None
    status_code: str | None = None
    digest: str | None = None
    length: int | None = None
    fields: Mapping[str, str] = field(
        default_factory=lambda: MappingProxyType({}), repr=False
    )

    def __post_init__(self) -> None:
        """Normalize the timestamp and freeze the raw fields."""
        if self.timestamp.tzinfo is None or self.timestamp.utcoffset() is None:
            raise ValueError("timestamp must include timezone information")
        object.__setattr__(self, "timestamp", self.timestamp.astimezone(UTC))
        object.__setattr__(self, "fields", MappingProxyType(dict(self.fields)))

    def archive_url(self) -> str:
        """Return the Wayback URL for this search result."""
        timestamp = self.timestamp.strftime("%Y%m%d%H%M%S")
        return f"https://web.archive.org/web/{timestamp}/{self.original_url}"


@dataclass(frozen=True, slots=True)
class InternetArchiveCdxResult:
    """CDX records and an optional continuation key."""

    items: tuple[InternetArchiveCdxRecord, ...]
    resume_key: str | None = None

    def __iter__(self) -> Iterator[InternetArchiveCdxRecord]:
        """Iterate over the CDX records."""
        return iter(self.items)

    def __len__(self) -> int:
        """Return the number of CDX records."""
        return len(self.items)
