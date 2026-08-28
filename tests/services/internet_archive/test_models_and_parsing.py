"""Verify Internet Archive models and documented response parsing."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from archivist import (
    InternetArchiveAccount,
    InternetArchiveApiKey,
    InternetArchiveAvailability,
    InternetArchiveCaptureJob,
    InternetArchiveCdxRecord,
    InternetArchiveCookies,
    InternetArchiveFailedStatus,
    InternetArchivePendingStatus,
    InternetArchiveSaveOptions,
    InternetArchiveSuccessStatus,
    InvalidOptionError,
    InvalidServiceResponseError,
)
from archivist.services.internet_archive import InternetArchiveSnapshot, _common


def test_every_save_option_has_its_exact_wire_value() -> None:
    """Serialize every public save option to its exact API wire value."""
    options = InternetArchiveSaveOptions(
        capture_all=True,
        capture_outlinks=True,
        capture_screenshot=True,
        delay_availability=True,
        force_get=True,
        skip_first_archive=True,
        if_not_archived_within=(timedelta(hours=1), "2d"),
        outlinks_availability=True,
        email_result=True,
        js_behavior_timeout=5,
        capture_cookie="target=session-secret",
        user_agent="Archivist test agent",
        target_username="target-user",
        target_password="target-secret",
        disable_adblocker=True,
        save_to_archive=True,
        email_wacz=True,
    )
    assert options.to_form() == {
        "capture_all": "1",
        "capture_outlinks": "1",
        "capture_screenshot": "1",
        "delay_wb_availability": "1",
        "force_get": "1",
        "skip_first_archive": "1",
        "if_not_archived_within": "3600,2d",
        "outlinks_availability": "1",
        "email_result": "1",
        "js_behavior_timeout": "5",
        "capture_cookie": "target=session-secret",
        "use_user_agent": "Archivist test agent",
        "target_username": "target-user",
        "target_password": "target-secret",
        "disable_adblocker": "on",
        "wm-save-mywebarchive": "on",
        "wacz": "on",
    }
    representation = repr(options)
    assert "session-secret" not in representation
    assert "target-secret" not in representation


def test_credentials_have_secret_safe_representations() -> None:
    """Keep every credential field out of object representations."""
    credentials = (
        InternetArchiveApiKey("access-value", "secret-value"),
        InternetArchiveCookies("user-value", "signature-value"),
        InternetArchiveAccount("account@example.invalid", "password-value"),
    )
    representation = repr(credentials)
    for secret in (
        "access-value",
        "secret-value",
        "user-value",
        "signature-value",
        "account@example.invalid",
        "password-value",
    ):
        assert secret not in representation


def test_result_representations_hide_sensitive_urls() -> None:
    """Keep target, archive, resource, and raw response URLs out of representations."""
    secret = "https://user:password@example.com/private?token=secret"
    timestamp = datetime(2020, 1, 1, tzinfo=UTC)
    snapshot = InternetArchiveSnapshot(secret, timestamp)
    representation = repr(
        (
            InternetArchiveCaptureJob("job", secret, secret),
            InternetArchivePendingStatus("job", resources=(secret,)),
            InternetArchiveSuccessStatus(
                "job",
                secret,
                timestamp,
                resources=(secret,),
                message=secret,
                screenshot=secret,
                outlinks=(secret,),
            ),
            InternetArchiveFailedStatus("job", message=secret, resources=(secret,)),
            InternetArchiveAvailability(secret, snapshot),
            InternetArchiveCdxRecord(timestamp, secret, fields={"url": secret}),
        )
    )
    assert secret not in representation
    assert "password" not in representation
    assert "token" not in representation


@pytest.mark.parametrize("timeout", [-1, 31, True])
def test_save_options_reject_invalid_javascript_timeouts(timeout: int | bool) -> None:
    """Reject invalid JavaScript behavior timeouts."""
    with pytest.raises(InvalidOptionError):
        InternetArchiveSaveOptions(js_behavior_timeout=timeout)


def test_status_parser_supports_pending_success_failure_and_outlink_shapes() -> None:
    """Parse pending, successful, failed, and outlink status shapes."""
    pending = _common.parse_capture_status(
        {"status": "pending", "job_id": "one", "resources": [], "download_size": 4},
        retry_after=3.0,
    )
    assert pending == InternetArchivePendingStatus(
        job_id="one", download_size=4, retry_after=3.0
    )

    success = _common.parse_capture_status(
        {
            "status": "success",
            "job_id": "two",
            "original_url": "https://example.com/",
            "timestamp": "20200102030405",
            "duration_sec": 1,
            "resources": ["https://example.com/"],
            "outlinks": {"https://example.net/": {"timestamp": "20190102030405"}},
        }
    )
    assert isinstance(success, InternetArchiveSuccessStatus)
    assert success.timestamp == datetime(2020, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert success.wayback_timestamp == "20200102030405"
    assert success.archive_url() == (
        "https://web.archive.org/web/20200102030405/https://example.com/"
    )
    assert success.outlinks is not None

    failed = _common.parse_capture_status(
        {
            "status": "error",
            "job_id": "three",
            "status_ext": "error:blocked",
            "resources": [],
        }
    )
    assert failed == InternetArchiveFailedStatus(
        job_id="three", service_code="error:blocked"
    )


def test_status_collection_and_cdx_parsers_accept_documented_variants() -> None:
    """Parse documented status collections and CDX response variants."""
    statuses = _common.parse_status_collection(
        {
            "one": {"status": "pending", "resources": []},
            "two": {"status": "error", "resources": []},
        }
    )
    assert [status.job_id for status in statuses] == ["one", "two"]
    result = _common.parse_cdx(
        [
            ["timestamp", "original", "mimetype", "statuscode", "digest", "length"],
            ["20200102030405", "https://example.com/", "text/html", "200", "ABC", "-"],
            [],
            ["opaque-key"],
        ]
    )
    assert result.resume_key == "opaque-key"
    assert result.items[0].length is None
    assert result.items[0].fields["digest"] == "ABC"
    assert result.items[0].archive_url() == (
        "https://web.archive.org/web/20200102030405/https://example.com/"
    )


def test_changed_service_shapes_raise_typed_errors() -> None:
    """Raise package errors when service response shapes change."""
    with pytest.raises(InvalidServiceResponseError):
        _common.parse_capture_status({"status": "new", "job_id": "id"})
    with pytest.raises(InvalidServiceResponseError):
        _common.parse_cdx({"unexpected": "object"})
    with pytest.raises(InvalidServiceResponseError, match="first_archive"):
        _common.parse_capture_status(
            {
                "status": "success",
                "job_id": "id",
                "original_url": "https://example.com/",
                "timestamp": "20200102030405",
                "first_archive": "yes",
            }
        )


@pytest.mark.parametrize("value", [float("nan"), float("inf"), True])
def test_polling_durations_must_be_finite_numbers(value: float | bool) -> None:
    """Require finite numeric polling durations."""
    with pytest.raises(InvalidOptionError):
        _common.validate_duration(value, name="timeout", allow_zero=False)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"js_behavior_timeout": "5"},
        {"if_not_archived_within": 1.5},
        {"capture_cookie": 1},
        {"capture_all": 1},
    ],
)
def test_save_option_runtime_types_raise_package_errors(
    kwargs: dict[str, object],
) -> None:
    """Convert invalid save-option runtime types to package errors."""
    options_type = cast("Any", InternetArchiveSaveOptions)
    with pytest.raises(InvalidOptionError):
        options_type(**kwargs)
