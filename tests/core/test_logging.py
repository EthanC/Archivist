"""Test that parser logging excludes sensitive response content."""

from __future__ import annotations

import logging

import pytest

from archivist import InvalidServiceResponseError
from archivist.services.archive_today import _common as archive_today_common
from archivist.services.internet_archive import _common as internet_archive_common


def test_service_parsers_log_failures_without_response_bodies(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Log parser failures without copying response bodies."""
    sensitive_body = "response-body-secret"
    with caplog.at_level(logging.WARNING, logger="archivist"):
        with pytest.raises(InvalidServiceResponseError):
            archive_today_common.parse_timemap(
                sensitive_body, mirror="https://archive.is"
            )
        with pytest.raises(InvalidServiceResponseError):
            internet_archive_common.parse_capture_status(
                {"status": "changed", "job_id": "job"}
            )

    assert "Archive.today response parsing failed" in caplog.text
    assert "Internet Archive response parsing failed" in caplog.text
    assert sensitive_body not in caplog.text
