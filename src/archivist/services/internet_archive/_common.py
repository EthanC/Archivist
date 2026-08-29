"""Pure request and response helpers for Internet Archive clients."""

from __future__ import annotations

import logging
import math
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import wraps
from types import MappingProxyType
from typing import Any, ParamSpec, TypedDict, TypeVar, cast
from urllib.parse import urlsplit

from archivist.core._http import sanitize_service_reason
from archivist.core.errors import (
    CaptureFailedError,
    InvalidOptionError,
    InvalidServiceResponseError,
)
from archivist.services.internet_archive.models import (
    InternetArchiveAvailability,
    InternetArchiveCaptureJob,
    InternetArchiveCaptureStatus,
    InternetArchiveCdxRecord,
    InternetArchiveCdxResult,
    InternetArchiveFailedStatus,
    InternetArchiveOutlinkAvailability,
    InternetArchivePendingStatus,
    InternetArchiveSaveOptions,
    InternetArchiveSnapshot,
    InternetArchiveSuccessStatus,
    InternetArchiveSystemStatus,
    InternetArchiveUserStatus,
    Outlinks,
)

logger = logging.getLogger(__name__)

SERVICE = "Internet Archive"
SAVE_URL = "https://web.archive.org/save"
STATUS_URL = "https://web.archive.org/save/status"
USER_STATUS_URL = "https://web.archive.org/save/status/user"
SYSTEM_STATUS_URL = "https://web.archive.org/save/status/system"
AVAILABILITY_URL = "https://archive.org/wayback/available"
CDX_URL = "https://web.archive.org/cdx/search/cdx"
CSRF_URL = "https://archive.org/services/csrf-token"
LOGIN_URL = "https://archive.org/services/account/login/"
MY_WEB_ARCHIVE_URL = "https://web.archive.org/__wb/web-archive/"

_WAYBACK_TIMESTAMP = re.compile(r"^\d{14}$")
_ANONYMOUS_JOB_ID = re.compile(r"\bwatchJob\(\s*['\"]([^'\"]+)['\"]\s*,")
_WAYBACK_TIMESTAMP_LENGTH = 14
_CDX_RESUME_TRAILER_LENGTH = 2
_HTTP_DEFAULT_PORT = 80
_HTTPS_DEFAULT_PORT = 443
_ASCII_CONTROL_LIMIT = 32
_P = ParamSpec("_P")
_R = TypeVar("_R")


class _CdxSearchOptions(TypedDict, total=False):
    """Keyword options accepted by a Wayback CDX search."""

    match_type: str
    from_timestamp: datetime | str | None
    to_timestamp: datetime | str | None
    filters: Sequence[str]
    collapse: Sequence[str]
    limit: int | None
    resume_key: str | None
    show_resume_key: bool
    fast_latest: bool


@dataclass(slots=True)
class _CdxSearch:
    match_type: str = "exact"
    from_timestamp: datetime | str | None = None
    to_timestamp: datetime | str | None = None
    filters: Sequence[str] = ()
    collapse: Sequence[str] = ()
    limit: int | None = None
    resume_key: str | None = None
    show_resume_key: bool = False
    fast_latest: bool = False


def _log_parse_failures(function: Callable[_P, _R]) -> Callable[_P, _R]:
    @wraps(function)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        try:
            return function(*args, **kwargs)
        except InvalidServiceResponseError:
            logger.warning("Internet Archive response parsing failed")
            raise

    return wrapped


def validate_duration(value: object, *, name: str, allow_zero: bool) -> float:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value < 0
        or (not allow_zero and value == 0)
    ):
        qualifier = "non-negative" if allow_zero else "positive"
        raise InvalidOptionError(f"{name} must be a finite {qualifier} number")
    return float(value)


def string_items(values: object, *, name: str) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise InvalidOptionError(f"{name} must be an iterable of strings, not a string")
    try:
        result = tuple(cast("Iterable[object]", values))
    except TypeError as exc:
        raise InvalidOptionError(f"{name} must be an iterable of strings") from exc
    if not all(isinstance(value, str) for value in result):
        raise InvalidOptionError(f"{name} must contain only strings")
    return cast("tuple[str, ...]", result)


def polling_delay(
    retry_after: float | datetime | None, *, poll_interval: float
) -> float:
    if isinstance(retry_after, datetime):
        suggested = (retry_after.astimezone(UTC) - datetime.now(UTC)).total_seconds()
    elif isinstance(retry_after, (int, float)) and not isinstance(retry_after, bool):
        suggested = float(retry_after)
    else:
        suggested = 0.0
    return max(poll_interval, suggested, 0.0)


def parse_wayback_timestamp(
    value: object, *, field_name: str = "timestamp"
) -> datetime:
    if not isinstance(value, str) or not _WAYBACK_TIMESTAMP.fullmatch(value):
        raise InvalidServiceResponseError(
            f"{SERVICE} returned an invalid {field_name}", service=SERVICE
        )
    try:
        return datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=UTC)
    except ValueError as exc:
        raise InvalidServiceResponseError(
            f"{SERVICE} returned an invalid {field_name}", service=SERVICE
        ) from exc


def format_wayback_query_timestamp(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise InvalidOptionError(
                "Wayback timestamps must include timezone information"
            )
        return value.astimezone(UTC).strftime("%Y%m%d%H%M%S")
    if not 1 <= len(value) <= _WAYBACK_TIMESTAMP_LENGTH or not value.isdigit():
        raise InvalidOptionError("Wayback timestamp must contain 1 to 14 digits")
    return value


def cdx_search_params(options: _CdxSearchOptions) -> list[tuple[str, str]]:
    """Validate CDX search options and return their request parameters."""
    search = _CdxSearch(**options)
    if search.match_type not in {"exact", "prefix", "host", "domain"}:
        raise InvalidOptionError("match_type must be exact, prefix, host, or domain")
    if search.limit is not None and (
        not isinstance(search.limit, int)
        or isinstance(search.limit, bool)
        or search.limit == 0
    ):
        raise InvalidOptionError("CDX limit must be a non-zero integer")
    filter_values = string_items(search.filters, name="filters")
    collapse_values = string_items(search.collapse, name="collapse")
    params = [("matchType", search.match_type)]
    for wire_name, value in (
        ("from", format_wayback_query_timestamp(search.from_timestamp)),
        ("to", format_wayback_query_timestamp(search.to_timestamp)),
    ):
        if value is not None:
            params.append((wire_name, value))
    params.extend(("filter", value) for value in filter_values)
    params.extend(("collapse", value) for value in collapse_values)
    if search.limit is not None:
        params.append(("limit", str(search.limit)))
    if search.resume_key is not None:
        if not search.resume_key:
            raise InvalidOptionError("resume_key cannot be empty")
        params.append(("resumeKey", search.resume_key))
    if search.show_resume_key:
        params.append(("showResumeKey", "true"))
    if search.fast_latest:
        params.append(("fastLatest", "true"))
    return params


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _required_string(data: Mapping[str, Any], name: str) -> str:
    value = data.get(name)
    if not isinstance(value, str) or not value:
        raise InvalidServiceResponseError(
            f"{SERVICE} response is missing {name}", service=SERVICE
        )
    return value


def _resources(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise InvalidServiceResponseError(
            f"{SERVICE} returned invalid capture resources", service=SERVICE
        )
    if not all(isinstance(item, str) for item in value):
        raise InvalidServiceResponseError(
            f"{SERVICE} returned invalid capture resources", service=SERVICE
        )
    return tuple(cast("Sequence[str]", value))


@_log_parse_failures
def parse_submission(
    data: Mapping[str, Any], *, target_url: str
) -> InternetArchiveCaptureJob:
    if data.get("status") == "error":
        code = _optional_string(data.get("status_ext"))
        reason = sanitize_service_reason(data.get("message"))
        reason_suffix = f": {reason}" if reason is not None else ""
        raise CaptureFailedError(
            f"{SERVICE} rejected the capture request{reason_suffix}",
            service=SERVICE,
            service_code=code,
        )
    job_id = _required_string(data, "job_id")
    return InternetArchiveCaptureJob(
        job_id=job_id,
        target_url=_optional_string(data.get("url")) or target_url,
        message=_optional_string(data.get("message")),
    )


@_log_parse_failures
def parse_anonymous_submission(
    body: str, *, target_url: str
) -> InternetArchiveCaptureJob:
    """Extract the public Save Page Now job from its progress page."""
    match = _ANONYMOUS_JOB_ID.search(body)
    if match is None:
        raise InvalidServiceResponseError(
            f"{SERVICE} returned an invalid anonymous capture response",
            service=SERVICE,
        )
    return InternetArchiveCaptureJob(job_id=match.group(1), target_url=target_url)


def save_options_require_authentication(options: InternetArchiveSaveOptions) -> bool:
    """Return whether Save Page Now restricts an enabled option to accounts."""
    return any(
        (
            options.capture_screenshot,
            options.email_result,
            options.save_to_archive,
            options.email_wacz,
        )
    )


def _parse_outlinks(value: object) -> Outlinks:
    if value is None:
        return None
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        if not all(isinstance(item, str) for item in value):
            raise InvalidServiceResponseError(
                f"{SERVICE} returned invalid outlinks", service=SERVICE
            )
        return tuple(cast("Sequence[str]", value))
    if not isinstance(value, Mapping):
        raise InvalidServiceResponseError(
            f"{SERVICE} returned invalid outlinks", service=SERVICE
        )

    if all(isinstance(item, str) for item in value.values()):
        return MappingProxyType(
            {str(key): cast("str", item) for key, item in value.items()}
        )

    parsed: dict[str, InternetArchiveOutlinkAvailability] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not isinstance(item, Mapping):
            raise InvalidServiceResponseError(
                f"{SERVICE} returned invalid outlink availability", service=SERVICE
            )
        timestamp_value = item.get("timestamp")
        timestamp = (
            None
            if timestamp_value is None
            else parse_wayback_timestamp(
                timestamp_value, field_name="outlink timestamp"
            )
        )
        parsed[key] = InternetArchiveOutlinkAvailability(timestamp=timestamp)
    return MappingProxyType(parsed)


@_log_parse_failures
def parse_capture_status(
    data: Mapping[str, Any],
    *,
    fallback_job_id: str | None = None,
    retry_after: float | datetime | None = None,
) -> InternetArchiveCaptureStatus:
    status = data.get("status")
    job_id_value = data.get("job_id", fallback_job_id)
    if not isinstance(job_id_value, str) or not job_id_value:
        raise InvalidServiceResponseError(
            f"{SERVICE} response is missing job_id", service=SERVICE
        )
    resources = _resources(data.get("resources"))

    if status == "pending":
        download_size = data.get("download_size")
        total_size = data.get("total_size")
        for name, value in (
            ("download_size", download_size),
            ("total_size", total_size),
        ):
            if value is not None and (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(value)
            ):
                raise InvalidServiceResponseError(
                    f"{SERVICE} returned invalid {name}", service=SERVICE
                )
        return InternetArchivePendingStatus(
            job_id=job_id_value,
            resources=resources,
            download_size=cast("int | float | None", download_size),
            total_size=cast("int | float | None", total_size),
            retry_after=retry_after,
        )

    if status == "success":
        duration = data.get("duration_sec")
        if duration is not None and (
            not isinstance(duration, (int, float))
            or isinstance(duration, bool)
            or not math.isfinite(duration)
        ):
            raise InvalidServiceResponseError(
                f"{SERVICE} returned invalid duration_sec", service=SERVICE
            )
        first_archive = data.get("first_archive")
        if first_archive is not None and not isinstance(first_archive, bool):
            raise InvalidServiceResponseError(
                f"{SERVICE} returned invalid first_archive", service=SERVICE
            )
        return InternetArchiveSuccessStatus(
            job_id=job_id_value,
            original_url=_required_string(data, "original_url"),
            timestamp=parse_wayback_timestamp(data.get("timestamp")),
            duration_seconds=float(duration) if duration is not None else None,
            resources=resources,
            message=_optional_string(data.get("message")),
            first_archive=cast("bool | None", first_archive),
            screenshot=_optional_string(data.get("screenshot")),
            outlinks=_parse_outlinks(data.get("outlinks")),
        )

    if status == "error":
        return InternetArchiveFailedStatus(
            job_id=job_id_value,
            message=sanitize_service_reason(data.get("message")),
            service_code=_optional_string(data.get("status_ext")),
            resources=resources,
        )

    raise InvalidServiceResponseError(
        f"{SERVICE} returned an unknown capture status", service=SERVICE
    )


@_log_parse_failures
def parse_status_collection(data: object) -> tuple[InternetArchiveCaptureStatus, ...]:
    records: list[tuple[str | None, object]]
    if isinstance(data, Sequence) and not isinstance(data, (str, bytes)):
        records = [(None, item) for item in data]
    elif isinstance(data, Mapping):
        records = [(str(key), value) for key, value in data.items()]
    else:
        raise InvalidServiceResponseError(
            f"{SERVICE} returned an invalid status collection", service=SERVICE
        )

    parsed: list[InternetArchiveCaptureStatus] = []
    for fallback, record in records:
        if not isinstance(record, Mapping):
            raise InvalidServiceResponseError(
                f"{SERVICE} returned an invalid status record", service=SERVICE
            )
        parsed.append(parse_capture_status(record, fallback_job_id=fallback))
    return tuple(parsed)


def parse_outlink_status_collection(
    data: object,
) -> tuple[InternetArchiveCaptureStatus, ...]:
    """Parse child-job statuses, including SPN's empty-result response."""
    if (
        isinstance(data, Mapping)
        and data.get("status") == "error"
        and data.get("message") == "No job_ids found."
    ):
        return ()
    return parse_status_collection(data)


@_log_parse_failures
def parse_user_status(data: Mapping[str, Any]) -> InternetArchiveUserStatus:
    available = data.get("available")
    processing = data.get("processing")
    if (
        not isinstance(available, int)
        or isinstance(available, bool)
        or not isinstance(processing, int)
        or isinstance(processing, bool)
    ):
        raise InvalidServiceResponseError(
            f"{SERVICE} returned invalid user status", service=SERVICE
        )
    return InternetArchiveUserStatus(available=available, processing=processing)


@_log_parse_failures
def parse_system_status(data: Mapping[str, Any]) -> InternetArchiveSystemStatus:
    status = _required_string(data, "status")
    recent = data.get("recent_captures")
    if recent is not None and (not isinstance(recent, int) or isinstance(recent, bool)):
        raise InvalidServiceResponseError(
            f"{SERVICE} returned invalid recent_captures", service=SERVICE
        )
    queues_value = data.get("queues", {})
    if not isinstance(queues_value, Mapping) or not all(
        isinstance(key, str) and isinstance(value, int) and not isinstance(value, bool)
        for key, value in queues_value.items()
    ):
        raise InvalidServiceResponseError(
            f"{SERVICE} returned invalid queue metrics", service=SERVICE
        )
    return InternetArchiveSystemStatus(
        status=status,
        recent_captures=cast("int | None", recent),
        queues=cast("Mapping[str, int]", queues_value),
    )


@_log_parse_failures
def parse_availability(data: Mapping[str, Any]) -> InternetArchiveAvailability:
    target_url = _required_string(data, "url")
    requested = _optional_string(data.get("timestamp"))
    snapshots = data.get("archived_snapshots")
    if not isinstance(snapshots, Mapping):
        raise InvalidServiceResponseError(
            f"{SERVICE} returned invalid availability data", service=SERVICE
        )
    closest_value = snapshots.get("closest")
    if closest_value is None:
        return InternetArchiveAvailability(
            target_url=target_url, closest=None, requested_timestamp=requested
        )
    if not isinstance(closest_value, Mapping):
        raise InvalidServiceResponseError(
            f"{SERVICE} returned invalid closest snapshot", service=SERVICE
        )
    available = closest_value.get("available", True)
    if not isinstance(available, bool):
        raise InvalidServiceResponseError(
            f"{SERVICE} returned invalid availability state", service=SERVICE
        )
    archive_url = _required_string(closest_value, "url")
    try:
        parsed_url = urlsplit(archive_url)
        port = parsed_url.port
    except ValueError:
        parsed_url = None
        port = None
    if (
        parsed_url is None
        or any(
            character.isspace() or ord(character) < _ASCII_CONTROL_LIMIT
            for character in archive_url
        )
        or parsed_url.scheme not in {"http", "https"}
        or parsed_url.hostname is None
        or parsed_url.hostname.lower() != "web.archive.org"
        or parsed_url.username is not None
        or parsed_url.password is not None
        or port
        not in {
            None,
            _HTTPS_DEFAULT_PORT if parsed_url.scheme == "https" else _HTTP_DEFAULT_PORT,
        }
    ):
        raise InvalidServiceResponseError(
            f"{SERVICE} returned an invalid archive URL", service=SERVICE
        )
    snapshot = InternetArchiveSnapshot(
        archive_url=archive_url,
        timestamp=parse_wayback_timestamp(closest_value.get("timestamp")),
        status_code=_optional_string(closest_value.get("status")),
        available=available,
    )
    return InternetArchiveAvailability(
        target_url=target_url, closest=snapshot, requested_timestamp=requested
    )


@_log_parse_failures
def parse_cdx(data: object) -> InternetArchiveCdxResult:
    if not isinstance(data, Sequence) or isinstance(data, (str, bytes)):
        raise InvalidServiceResponseError(
            f"{SERVICE} returned invalid CDX data", service=SERVICE
        )
    if not data:
        return InternetArchiveCdxResult(items=())
    header_value = data[0]
    if not isinstance(header_value, Sequence) or isinstance(header_value, (str, bytes)):
        raise InvalidServiceResponseError(
            f"{SERVICE} returned invalid CDX headers", service=SERVICE
        )
    headers = list(header_value)
    if not all(isinstance(value, str) for value in headers):
        raise InvalidServiceResponseError(
            f"{SERVICE} returned invalid CDX headers", service=SERVICE
        )

    items: list[InternetArchiveCdxRecord] = []
    resume_key: str | None = None
    rows = list(data[1:])
    if len(rows) >= _CDX_RESUME_TRAILER_LENGTH and rows[-2] == []:
        key_row = rows[-1]
        if (
            isinstance(key_row, Sequence)
            and not isinstance(key_row, (str, bytes))
            and len(key_row) == 1
            and isinstance(key_row[0], str)
        ):
            resume_key = key_row[0]
            rows = rows[:-2]

    for row in rows:
        if not isinstance(row, Sequence) or isinstance(row, (str, bytes)):
            raise InvalidServiceResponseError(
                f"{SERVICE} returned invalid CDX row", service=SERVICE
            )
        if len(row) != len(headers) or not all(isinstance(value, str) for value in row):
            raise InvalidServiceResponseError(
                f"{SERVICE} returned invalid CDX row", service=SERVICE
            )
        fields = dict(
            zip(cast("list[str]", headers), cast("Sequence[str]", row), strict=True)
        )
        timestamp = fields.get("timestamp")
        original = fields.get("original")
        if timestamp is None or original is None:
            raise InvalidServiceResponseError(
                f"{SERVICE} CDX response omitted required fields", service=SERVICE
            )
        length_value = fields.get("length")
        try:
            length = None if length_value in {None, "-"} else int(length_value)
        except ValueError as exc:
            raise InvalidServiceResponseError(
                f"{SERVICE} returned invalid CDX length", service=SERVICE
            ) from exc
        items.append(
            InternetArchiveCdxRecord(
                timestamp=parse_wayback_timestamp(timestamp),
                original_url=original,
                mime_type=None
                if fields.get("mimetype") == "-"
                else fields.get("mimetype"),
                status_code=(
                    None
                    if fields.get("statuscode") == "-"
                    else fields.get("statuscode")
                ),
                digest=None if fields.get("digest") == "-" else fields.get("digest"),
                length=length,
                fields=MappingProxyType(fields),
            )
        )
    return InternetArchiveCdxResult(items=tuple(items), resume_key=resume_key)
