"""Exercise validation and parsing behavior at service-response boundaries."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta, timezone, tzinfo
from types import MappingProxyType
from typing import Any, cast

import pytest

from archivist import (
    CaptureFailedError,
    InternetArchiveAccount,
    InternetArchiveApiKey,
    InternetArchiveCaptureJob,
    InternetArchiveCdxRecord,
    InternetArchiveCdxResult,
    InternetArchiveCookies,
    InternetArchiveSaveOptions,
    InternetArchiveSuccessStatus,
    InternetArchiveSystemStatus,
    InvalidOptionError,
    InvalidServiceResponseError,
)
from archivist.services.internet_archive import _common
from archivist.services.internet_archive.models import InternetArchiveSnapshot

CDX_RECORD_LENGTH = 12
FUTURE_DELAY_LOWER_BOUND = 50
POLL_INTERVAL = 0.5


@pytest.mark.parametrize(
    ("model", "args"),
    [
        (InternetArchiveApiKey, ("", "secret")),
        (InternetArchiveApiKey, ("access", "")),
        (InternetArchiveCookies, ("", "signature")),
        (InternetArchiveCookies, ("user", "")),
        (InternetArchiveAccount, ("", "password")),
        (InternetArchiveAccount, ("user", "")),
        (InternetArchiveAccount, ("user", "password", 1)),
    ],
)
def test_credentials_reject_empty_and_invalid_fields(
    model: Callable[..., object], args: tuple[object, ...]
) -> None:
    """Reject empty credential fields and invalid account flags."""
    with pytest.raises(InvalidOptionError):
        model(*args)


@pytest.mark.parametrize(
    "age",
    [True, -1, timedelta(seconds=-1), timedelta(microseconds=1), 1.5, "   "],
)
def test_save_options_reject_invalid_archive_ages(age: object) -> None:
    """Reject invalid minimum archive ages."""
    options_type = cast("Any", InternetArchiveSaveOptions)
    with pytest.raises(InvalidOptionError):
        options_type(if_not_archived_within=age)


def test_save_options_reject_bad_age_pair_and_normalize_single_ages() -> None:
    """Reject malformed age pairs and normalize individual age values."""
    with pytest.raises(InvalidOptionError, match="two values"):
        cast("Any", InternetArchiveSaveOptions)(if_not_archived_within=(1, 2, 3))

    assert InternetArchiveSaveOptions(if_not_archived_within=12).to_form() == {
        "if_not_archived_within": "12"
    }
    assert InternetArchiveSaveOptions(if_not_archived_within=" 2d ").to_form() == {
        "if_not_archived_within": "2d"
    }


@pytest.mark.parametrize(
    "kwargs",
    [
        {"capture_cookie": object()},
        {"user_agent": object()},
        {"target_username": object()},
        {"target_password": object()},
    ],
)
def test_every_optional_save_string_is_runtime_checked(
    kwargs: dict[str, object],
) -> None:
    """Validate every optional save string at runtime."""
    with pytest.raises(InvalidOptionError):
        cast("Any", InternetArchiveSaveOptions)(**kwargs)


def test_timezone_models_reject_naive_values_and_normalize_aware_values() -> None:
    """Reject naive datetimes and normalize aware values to UTC."""
    naive = datetime(2020, 1, 2, 3, 4, 5)
    with pytest.raises(ValueError):
        InternetArchiveSuccessStatus("job", "https://example.com", naive)
    with pytest.raises(ValueError):
        InternetArchiveSnapshot("https://web.archive.org", naive)
    with pytest.raises(ValueError):
        InternetArchiveCdxRecord(naive, "https://example.com")

    eastern = timezone(timedelta(hours=-5))
    success = InternetArchiveSuccessStatus(
        "job", "https://example.com", naive.replace(tzinfo=eastern)
    )
    snapshot = InternetArchiveSnapshot(
        "https://web.archive.org", naive.replace(tzinfo=eastern)
    )
    record = InternetArchiveCdxRecord(
        naive.replace(tzinfo=eastern),
        "https://example.com",
        fields={"statuscode": "200"},
    )
    assert (
        success.timestamp
        == snapshot.timestamp
        == record.timestamp
        == datetime(2020, 1, 2, 8, 4, 5, tzinfo=UTC)
    )
    assert isinstance(record.fields, MappingProxyType)


def test_collection_models_copy_mappings_and_support_sequence_protocol() -> None:
    """Copy input mappings and expose immutable sequence behavior."""
    queues = {"api": 1}
    status = InternetArchiveSystemStatus("ok", queues=queues)
    queues["api"] = 2
    assert status.queues == {"api": 1}
    assert InternetArchiveSystemStatus("ok").queues == {}

    record = InternetArchiveCdxRecord(
        datetime(2020, 1, 1, tzinfo=UTC), "https://example.com"
    )
    result = InternetArchiveCdxResult((record,))
    assert len(result) == 1
    assert list(result) == [record]


@pytest.mark.parametrize(
    ("value", "allow_zero"),
    [
        ("1", False),
        (True, False),
        (float("inf"), False),
        (-1, False),
        (0, False),
    ],
)
def test_validate_duration_rejects_each_invalid_category(
    value: object, allow_zero: bool
) -> None:
    """Reject each invalid polling-duration category."""
    with pytest.raises(InvalidOptionError):
        _common.validate_duration(value, name="duration", allow_zero=allow_zero)


def test_validate_duration_accepts_zero_when_allowed() -> None:
    """Accept a zero duration when explicitly allowed."""
    assert _common.validate_duration(0, name="duration", allow_zero=True) == 0.0


@pytest.mark.parametrize("value", ["jobs", b"jobs", 1, ["job", 2]])
def test_string_items_rejects_invalid_iterables(value: object) -> None:
    """Reject strings and non-string iterable items."""
    with pytest.raises(InvalidOptionError):
        _common.string_items(value, name="jobs")


def test_polling_delay_handles_dates_numbers_and_non_numeric_values() -> None:
    """Calculate polling delays from dates, numbers, and fallback values."""
    assert (
        _common.polling_delay(
            datetime.now(UTC) + timedelta(seconds=60), poll_interval=1
        )
        > FUTURE_DELAY_LOWER_BOUND
    )
    assert _common.polling_delay(-2, poll_interval=1) == 1
    assert _common.polling_delay(True, poll_interval=POLL_INTERVAL) == POLL_INTERVAL
    assert _common.polling_delay(None, poll_interval=POLL_INTERVAL) == POLL_INTERVAL


@pytest.mark.parametrize("value", [None, 20200101000000, "short"])
def test_wayback_timestamp_rejects_wrong_shapes(value: object) -> None:
    """Reject Wayback timestamps with unsupported shapes."""
    with pytest.raises(InvalidServiceResponseError):
        _common.parse_wayback_timestamp(value)


def test_wayback_timestamp_rejects_impossible_calendar_date() -> None:
    """Wrap impossible Wayback calendar dates in a service error."""
    with pytest.raises(InvalidServiceResponseError) as failure:
        _common.parse_wayback_timestamp("20201301000000")
    assert isinstance(failure.value.__cause__, ValueError)


class _NoOffset(tzinfo):
    def utcoffset(self, dt: datetime | None) -> None:
        return None

    def dst(self, dt: datetime | None) -> None:
        return None


@pytest.mark.parametrize(
    "value", [datetime(2020, 1, 1), datetime(2020, 1, 1, tzinfo=_NoOffset())]
)
def test_query_timestamp_requires_a_real_timezone(value: datetime) -> None:
    """Require query datetimes to have a usable UTC offset."""
    with pytest.raises(InvalidOptionError):
        _common.format_wayback_query_timestamp(value)


@pytest.mark.parametrize("value", ["", "1" * 15, "2020-01"])
def test_query_timestamp_rejects_invalid_digit_strings(value: str) -> None:
    """Reject malformed query timestamp strings."""
    with pytest.raises(InvalidOptionError):
        _common.format_wayback_query_timestamp(value)


def test_query_timestamp_formats_all_supported_values() -> None:
    """Format absent, partial, and timezone-aware query timestamps."""
    assert _common.format_wayback_query_timestamp(None) is None
    assert _common.format_wayback_query_timestamp("2020") == "2020"
    assert (
        _common.format_wayback_query_timestamp(
            datetime(2020, 1, 2, 3, 4, 5, tzinfo=timezone(timedelta(hours=2)))
        )
        == "20200102010405"
    )


def test_submission_parser_handles_rejection_and_response_fallbacks() -> None:
    """Parse submission rejections and apply response fallbacks."""
    with pytest.raises(CaptureFailedError) as failure:
        _common.parse_submission(
            {"status": "error", "status_ext": 4}, target_url="https://x"
        )
    assert failure.value.service_code is None

    with pytest.raises(CaptureFailedError) as detailed:
        _common.parse_submission(
            {
                "status": "error",
                "status_ext": "error:invalid-url-syntax",
                "message": (
                    "<strong>URL is invalid</strong> "
                    "<a href='https://user:password@example.com/help'>Help</a>"
                ),
            },
            target_url="https://x",
        )
    assert str(detailed.value).endswith(": URL is invalid Help")
    assert detailed.value.service_code == "error:invalid-url-syntax"

    fallback = _common.parse_submission(
        {"job_id": "job", "url": 4, "message": 5}, target_url="https://fallback"
    )
    explicit = _common.parse_submission(
        {"job_id": "job", "url": "https://response", "message": "queued"},
        target_url="https://fallback",
    )
    assert (fallback.target_url, fallback.message) == ("https://fallback", None)
    assert (explicit.target_url, explicit.message) == ("https://response", "queued")

    for job_id in (None, "", 4):
        with pytest.raises(InvalidServiceResponseError):
            _common.parse_submission({"job_id": job_id}, target_url="https://fallback")


def test_anonymous_submission_parser_extracts_progress_job() -> None:
    """Extract a public Save Page Now job ID without retaining response HTML."""
    job = _common.parse_anonymous_submission(
        '<script>watchJob("spn2-job", "/_static/", 6000, false);</script>',
        target_url="https://example.com",
    )
    assert job == InternetArchiveCaptureJob("spn2-job", "https://example.com")

    with pytest.raises(
        InvalidServiceResponseError, match="invalid anonymous capture response"
    ):
        _common.parse_anonymous_submission(
            "<html>missing job</html>", target_url="https://example.com"
        )


@pytest.mark.parametrize("resources", ["url", 1, ["url", 2]])
def test_capture_status_rejects_invalid_resource_shapes(resources: object) -> None:
    """Reject capture resources that are not sequences of strings."""
    with pytest.raises(InvalidServiceResponseError):
        _common.parse_capture_status(
            {"status": "pending", "job_id": "job", "resources": resources}
        )


@pytest.mark.parametrize("job_id", [None, "", 4])
def test_capture_status_requires_a_nonempty_string_job_id(job_id: object) -> None:
    """Require capture statuses to contain a nonempty string job ID."""
    with pytest.raises(InvalidServiceResponseError):
        _common.parse_capture_status({"status": "pending", "job_id": job_id})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("download_size", "1"),
        ("download_size", True),
        ("download_size", float("nan")),
        ("total_size", "1"),
        ("total_size", True),
        ("total_size", float("inf")),
    ],
)
def test_pending_status_rejects_invalid_sizes(field: str, value: object) -> None:
    """Reject nonnumeric and nonfinite pending-status sizes."""
    with pytest.raises(InvalidServiceResponseError, match=field):
        _common.parse_capture_status(
            {"status": "pending", "job_id": "job", field: value}
        )


@pytest.mark.parametrize("duration", ["1", True, float("inf")])
def test_success_status_rejects_invalid_durations(duration: object) -> None:
    """Reject nonnumeric and nonfinite success durations."""
    with pytest.raises(InvalidServiceResponseError, match="duration_sec"):
        _common.parse_capture_status(
            {
                "status": "success",
                "job_id": "job",
                "original_url": "https://example.com",
                "timestamp": "20200101000000",
                "duration_sec": duration,
            }
        )


def test_success_status_supports_optional_fields_and_every_outlink_shape() -> None:
    """Parse absent fields and each supported outlink representation."""
    base = {
        "status": "success",
        "job_id": "job",
        "original_url": "https://example.com",
        "timestamp": "20200101000000",
    }
    absent = _common.parse_capture_status(base)
    sequence = _common.parse_capture_status({**base, "outlinks": ["https://one"]})
    string_map = _common.parse_capture_status({**base, "outlinks": {1: "https://one"}})
    availability = _common.parse_capture_status(
        {
            **base,
            "outlinks": {"https://one": {"timestamp": None}},
            "first_archive": False,
        }
    )
    assert cast("InternetArchiveSuccessStatus", absent).duration_seconds is None
    assert cast("InternetArchiveSuccessStatus", absent).first_archive is None
    assert cast("InternetArchiveSuccessStatus", sequence).outlinks == ("https://one",)
    assert cast("Any", string_map).outlinks["1"] == "https://one"
    assert cast("Any", availability).outlinks["https://one"].timestamp is None
    assert cast("InternetArchiveSuccessStatus", availability).first_archive is False


@pytest.mark.parametrize(
    "outlinks",
    [
        ["https://one", 2],
        "https://one",
        2,
        {"https://one": 2},
        {2: {"timestamp": None}},
    ],
)
def test_success_status_rejects_invalid_outlinks(outlinks: object) -> None:
    """Reject malformed outlink collections and mappings."""
    with pytest.raises(InvalidServiceResponseError):
        _common.parse_capture_status(
            {
                "status": "success",
                "job_id": "job",
                "original_url": "https://example.com",
                "timestamp": "20200101000000",
                "outlinks": outlinks,
            }
        )


@pytest.mark.parametrize("data", [1, "statuses", [1]])
def test_status_collection_rejects_invalid_collections_and_records(
    data: object,
) -> None:
    """Reject malformed status collections and records."""
    with pytest.raises(InvalidServiceResponseError):
        _common.parse_status_collection(data)


@pytest.mark.parametrize(
    "data",
    [
        {"available": True, "processing": 1},
        {"available": "1", "processing": 1},
        {"available": 1, "processing": True},
        {"available": 1, "processing": "1"},
    ],
)
def test_user_status_rejects_invalid_metrics(data: dict[str, object]) -> None:
    """Reject noninteger user-status metrics."""
    with pytest.raises(InvalidServiceResponseError):
        _common.parse_user_status(data)


@pytest.mark.parametrize(
    "data",
    [
        {"status": "ok", "recent_captures": True},
        {"status": "ok", "recent_captures": "1"},
        {"status": "ok", "queues": []},
        {"status": "ok", "queues": {1: 1}},
        {"status": "ok", "queues": {"api": "1"}},
        {"status": "ok", "queues": {"api": True}},
    ],
)
def test_system_status_rejects_invalid_metrics(data: dict[str, object]) -> None:
    """Reject malformed system-status metrics and queues."""
    with pytest.raises(InvalidServiceResponseError):
        _common.parse_system_status(data)


def test_system_status_accepts_absent_optional_metrics() -> None:
    """Accept system statuses without optional metrics."""
    assert _common.parse_system_status({"status": "ok"}) == InternetArchiveSystemStatus(
        "ok"
    )


@pytest.mark.parametrize(
    "data",
    [
        {"url": "https://example.com", "archived_snapshots": []},
        {"url": "https://example.com", "archived_snapshots": {"closest": []}},
        {
            "url": "https://example.com",
            "archived_snapshots": {"closest": {"available": "yes"}},
        },
    ],
)
def test_availability_rejects_invalid_nested_shapes(data: dict[str, object]) -> None:
    """Reject malformed nested availability records."""
    with pytest.raises(InvalidServiceResponseError):
        _common.parse_availability(data)


def test_availability_supports_no_snapshot_and_snapshot_defaults() -> None:
    """Parse missing snapshots and apply snapshot defaults."""
    missing = _common.parse_availability(
        {"url": "https://example.com", "timestamp": 1, "archived_snapshots": {}}
    )
    available = _common.parse_availability(
        {
            "url": "https://example.com",
            "archived_snapshots": {
                "closest": {
                    "url": "https://web.archive.org/capture",
                    "timestamp": "20200101000000",
                }
            },
        }
    )
    assert missing.closest is None
    assert missing.requested_timestamp is None
    assert available.closest is not None and available.closest.available is True


@pytest.mark.parametrize(
    "archive_url",
    [
        "javascript:alert(1)",
        "https://example.invalid/capture",
        "https://user:password@web.archive.org/capture",
        "https://web.archive.org:444/capture",
        "https://web.archive.org:not-a-port/capture",
        "https://web.archive.org/capture\n",
    ],
)
def test_availability_rejects_unsafe_archive_urls(archive_url: str) -> None:
    """Accept snapshots only from a standard Wayback origin."""
    with pytest.raises(InvalidServiceResponseError, match="invalid archive URL"):
        _common.parse_availability(
            {
                "url": "https://example.com",
                "archived_snapshots": {
                    "closest": {
                        "url": archive_url,
                        "timestamp": "20200101000000",
                    }
                },
            }
        )


@pytest.mark.parametrize(
    "data",
    [
        [["timestamp"], "row"],
        [["timestamp"], ["one", "two"]],
        [["timestamp"], [1]],
        [["timestamp"], ["20200101000000"]],
        [["original"], ["https://example.com"]],
        [["timestamp", "original", "length"], ["20200101000000", "https://x", "bad"]],
    ],
)
def test_cdx_rejects_invalid_rows(data: object) -> None:
    """Reject malformed CDX data rows."""
    with pytest.raises(InvalidServiceResponseError):
        _common.parse_cdx(data)


@pytest.mark.parametrize(
    "data", [["header"], [[1]], [["timestamp"], [], ["key", "extra"]]]
)
def test_cdx_rejects_invalid_headers_and_resume_rows(data: object) -> None:
    """Reject malformed CDX headers and resume-key rows."""
    with pytest.raises(InvalidServiceResponseError):
        _common.parse_cdx(data)


def test_cdx_empty_and_dash_fields() -> None:
    """Parse empty CDX results and normalize dash fields."""
    assert _common.parse_cdx([]) == InternetArchiveCdxResult(())
    result = _common.parse_cdx(
        [
            ["timestamp", "original", "mimetype", "statuscode", "digest", "length"],
            ["20200101000000", "https://example.com", "-", "-", "-", "12"],
        ]
    )
    assert result.items[0].mime_type is None
    assert result.items[0].status_code is None
    assert result.items[0].digest is None
    assert result.items[0].length == CDX_RECORD_LENGTH
