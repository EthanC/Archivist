"""Cover malformed snapshots and conservative recovery decision edges."""

from __future__ import annotations

from dataclasses import replace
from io import BytesIO
from types import MappingProxyType
from typing import Any, cast

import pytest

from archivist import (
    InternetArchiveFileDisposition,
    InternetArchiveItem,
    InternetArchiveItemAvailability,
    InternetArchiveItemFile,
    InternetArchiveItemTask,
    InternetArchiveItemTaskState,
    InternetArchiveRecoveryPhase,
    InvalidOptionError,
    InvalidServiceResponseError,
)
from archivist.services.internet_archive import _items, _recovery

IDENTIFIER = "item-1"
MD5 = "0cc175b9c0f1b6a831c399e269772661"
SHA1 = "86f7e437faa5a7fce15d1ddcb9eaeaea377667b8"
TWO_TASKS = 2


def snapshot(**overrides: object) -> dict[str, object]:
    """Return a complete one-file metadata response."""
    data: dict[str, object] = {
        "metadata": {
            "identifier": IDENTIFIER,
            "uploader": "owner@example.invalid",
            "source": "exact",
            "tags": ["one", "two"],
        },
        "files": [
            {
                "name": "a.bin",
                "source": "original",
                "size": "1",
                "md5": MD5.upper(),
                "sha1": SHA1.upper(),
            }
        ],
        "files_count": "1",
        "workable_servers": ["server"],
    }
    data.update(overrides)
    return data


@pytest.mark.parametrize(
    "value",
    [
        None,
        {},
        [],
        "source",
        1,
        {1: "value"},
        {"source": 1},
        {"source": []},
        {"source": [1]},
    ],
)
def test_expected_metadata_rejects_invalid_shapes(value: object) -> None:
    """Require a nonempty exact mapping of scalar or repeated strings."""
    with pytest.raises(InvalidOptionError):
        _recovery.normalize_expected_metadata(cast("Any", value))


def test_expected_metadata_copies_sequences_without_canonicalizing() -> None:
    """Freeze repeated values while retaining exact scalar spelling."""
    repeated = ["one", "two"]
    expected = _recovery.normalize_expected_metadata(
        {"source": " https://example.invalid/ ", "tags": repeated}
    )
    repeated.append("later")
    assert expected == {
        "source": " https://example.invalid/ ",
        "tags": ("one", "two"),
    }
    assert isinstance(expected, MappingProxyType)


class InvalidHashStream(BytesIO):
    """Return one configured invalid value while hashing a prepared source."""

    def __init__(self, failure: str) -> None:
        """Initialize one configured stream protocol failure."""
        super().__init__(b"a")
        self.failure = failure

    def tell(self) -> int:
        """Return an invalid offset when requested by the test."""
        if self.failure == "tell":
            return cast("Any", True)
        return super().tell()

    def read(self, size: int | None = -1) -> bytes:
        """Return invalid binary data when requested by the test."""
        if self.failure == "read":
            return cast("Any", "not bytes")
        return super().read(size)

    def seek(self, offset: int, whence: int = 0) -> int:
        """Fail restoration when requested by the test."""
        if self.failure == "seek" and super().tell() != 0:
            raise OSError("private")
        return super().seek(offset, whence)


@pytest.mark.parametrize("failure", ["tell", "read", "seek"])
def test_recovery_hashing_rejects_unstable_sources(failure: str) -> None:
    """Convert invalid offsets, bytes, and restoration failures to safe errors."""
    stream = InvalidHashStream(failure)
    with pytest.raises(InvalidOptionError, match="stable binary"):
        _recovery.hash_prepared_files((_items.PreparedFile("a", 1, stream),))


def test_recovery_file_rewind_failure_is_safe() -> None:
    """Reject a source that can no longer return to its hashed offset."""
    stream = InvalidHashStream("seek")
    stream.read()
    prepared = _recovery.PreparedRecoveryFile(
        _items.PreparedFile("a", 1, stream), 0, MD5, SHA1
    )
    with pytest.raises(InvalidOptionError, match="restored"):
        prepared.rewind()


@pytest.mark.parametrize(
    "payload",
    [
        {"metadata": []},
        cast("Any", []),
        {"metadata": {1: "value"}},
        {"metadata": {"identifier": "other"}},
        {"metadata": {"identifier": IDENTIFIER, "uploader": 1}},
        {"metadata": {"identifier": IDENTIFIER, "uploader": ["owner"]}},
        snapshot(files={}),
        snapshot(files=[1]),
        snapshot(files=[{"name": ""}]),
        snapshot(files=[{"name": "a"}, {"name": "a"}]),
        snapshot(files=[{"name": "a", "size": True}]),
        snapshot(files=[{"name": "a", "md5": "bad"}]),
        snapshot(files=[{"name": "a", "sha1": "bad"}]),
        snapshot(files=[{"name": "a", "source": 1}]),
        snapshot(workable_servers="server"),
        snapshot(workable_servers=[""]),
        snapshot(is_dark=1),
        {"workable_servers": "server"},
        {"workable_servers": [""]},
        {"files": {}},
        {"files_count": "bad"},
        {"error": 1},
    ],
)
def test_item_parser_rejects_malformed_response_parts(
    payload: dict[str, object],
) -> None:
    """Reject ambiguous metadata, files, checksums, flags, and errors."""
    with pytest.raises(InvalidServiceResponseError):
        _recovery.parse_item(payload, IDENTIFIER)


@pytest.mark.parametrize("with_metadata", [False, True])
@pytest.mark.parametrize("code", [None, True, 101.0, "101", "pending", [], {}])
def test_item_parser_rejects_malformed_extended_errors(
    code: object, with_metadata: bool
) -> None:
    """Require the documented integer errcode even when metadata is present."""
    data = snapshot() if with_metadata else {}
    data["errcode"] = code
    with pytest.raises(InvalidServiceResponseError, match="extended error"):
        _recovery.parse_item(data, IDENTIFIER)


@pytest.mark.parametrize("with_metadata", [False, True])
@pytest.mark.parametrize(
    "field",
    ["is_dark", "nodownload", "has_redrow", "servers_unavailable", "pending_creation"],
)
@pytest.mark.parametrize("value", [None, 1, "true"])
def test_item_parser_validates_state_flags_without_metadata(
    field: str, value: object, with_metadata: bool
) -> None:
    """Do not discard malformed flags in otherwise unresolved responses."""
    data = snapshot() if with_metadata else {}
    data[field] = value
    with pytest.raises(InvalidServiceResponseError, match="item state"):
        _recovery.parse_item(data, IDENTIFIER)


@pytest.mark.parametrize("with_metadata", [False, True])
@pytest.mark.parametrize(
    "tasks",
    [
        "task",
        [1],
        [{}],
        [{"cmd": "archive.php", "state": 1}],
        [{"cmd": "archive.php", "wait_admin": True}],
        [{"cmd": "archive.php", "error": 1}],
        {1: {"state": "pending"}},
        {"archive.php": "pending"},
    ],
)
def test_item_parser_rejects_malformed_tasks(
    tasks: object, with_metadata: bool
) -> None:
    """Require exact task containers, commands, states, and admin fields."""
    data = snapshot() if with_metadata else {}
    data["tasks"] = tasks
    with pytest.raises(InvalidServiceResponseError):
        _recovery.parse_item(data, IDENTIFIER)


@pytest.mark.parametrize("with_metadata", [False, True])
@pytest.mark.parametrize("with_error", [False, True])
@pytest.mark.parametrize(
    ("code", "availability", "server_unavailable"),
    [
        (101, InternetArchiveItemAvailability.PENDING, False),
        (102, InternetArchiveItemAvailability.UNAVAILABLE, True),
        (103, InternetArchiveItemAvailability.UNAVAILABLE, False),
        (104, InternetArchiveItemAvailability.UNAVAILABLE, False),
        (105, InternetArchiveItemAvailability.UNAVAILABLE, True),
        (106, InternetArchiveItemAvailability.UNCERTAIN, False),
        (400, InternetArchiveItemAvailability.UNCERTAIN, False),
        (999, InternetArchiveItemAvailability.UNCERTAIN, False),
    ],
)
def test_item_extended_errors_never_authorize_recovery(
    code: int,
    availability: InternetArchiveItemAvailability,
    server_unavailable: bool,
    with_metadata: bool,
    with_error: bool,
) -> None:
    """Retain documented and unknown codes without trusting attached metadata."""
    data = snapshot(files=[], files_count=0) if with_metadata else {}
    data["errcode"] = code
    reason = "Metadata snapshot is not authoritative" if with_error else None
    if with_error:
        data["error"] = reason
    item = _recovery.parse_item(data, IDENTIFIER)
    assert item.extended_error_code == str(code)
    assert item.unavailable_reason == reason
    assert item.availability is availability
    assert item.server_unavailable is server_unavailable
    assert item.uploader == ("owner@example.invalid" if with_metadata else None)
    assert item.metadata.get("source") == ("exact" if with_metadata else None)
    assert item.metadata.get("tags") == (("one", "two") if with_metadata else None)
    with pytest.raises(_recovery.RecoveryDecisionError) as failure:
        _recovery.reconcile(
            item,
            "owner@example.invalid",
            {"source": "exact"},
            (prepared_file(),),
            catalog_complete=True,
        )
    assert failure.value.phase is InternetArchiveRecoveryPhase.RECONCILIATION
    assert failure.value.deferred
    assert not failure.value.checksum_mismatch


@pytest.mark.parametrize("with_metadata", [False, True])
@pytest.mark.parametrize(
    ("fields", "status_code"),
    [
        ({"error": "Unclassified metadata error"}, 200),
        ({"message": "Unclassified metadata warning"}, 200),
        ({}, 404),
        ({}, 503),
    ],
)
def test_item_errors_without_codes_never_authorize_recovery(
    fields: dict[str, object], status_code: int, with_metadata: bool
) -> None:
    """Metadata does not override HTTP failures or unclassified error messages."""
    data = snapshot(files=[], files_count=0) if with_metadata else {}
    data.update(fields)
    item = _recovery.parse_item(data, IDENTIFIER, status_code=status_code)
    assert not item.available
    assert item.extended_error_code is None
    assert item.unavailable_reason == fields.get("error", fields.get("message"))
    with pytest.raises(_recovery.RecoveryDecisionError) as failure:
        _recovery.reconcile(
            item,
            "owner@example.invalid",
            {"source": "exact"},
            (prepared_file(),),
            catalog_complete=True,
        )
    assert failure.value.deferred
    assert not failure.value.checksum_mismatch


@pytest.mark.parametrize(
    ("wire_field", "public_field"),
    [
        ("is_dark", "is_dark"),
        ("nodownload", "nodownload"),
        ("has_redrow", "has_redrow"),
        ("servers_unavailable", "server_unavailable"),
    ],
)
@pytest.mark.parametrize("with_metadata", [False, True])
def test_item_state_fields_are_preserved_and_block_recovery(
    wire_field: str, public_field: str, with_metadata: bool
) -> None:
    """Retain state-only flags and honor the plural server-unavailability wire key."""
    data = snapshot(files=[], files_count=0) if with_metadata else {}
    data[wire_field] = True
    item = _recovery.parse_item(data, IDENTIFIER)
    assert getattr(item, public_field) is True
    with pytest.raises(_recovery.RecoveryDecisionError) as failure:
        _recovery.reconcile(
            item,
            "owner@example.invalid",
            {"source": "exact"},
            (prepared_file(),),
            catalog_complete=True,
        )
    assert not failure.value.checksum_mismatch


def test_item_parser_preserves_files_tasks_and_servers_without_metadata() -> None:
    """Expose state-only evidence without treating its file listing as complete."""
    data = snapshot(tasks=[{"cmd": "archive.php", "state": "running"}])
    data.pop("metadata")
    item = _recovery.parse_item(data, IDENTIFIER)
    assert item.metadata == {}
    assert item.uploader is None
    assert item.availability is InternetArchiveItemAvailability.UNCERTAIN
    assert not item.listing_complete
    assert item.files[0].name == "a.bin"
    assert item.files[0].md5 == MD5
    assert [task.command for task in item.pending_tasks] == ["archive.php"]
    assert item.workable_servers == ("server",)


def test_item_parser_classifies_states_tasks_and_sources() -> None:
    """Retain unavailable details and normalize every documented task state."""
    assert _recovery.parse_item({}, IDENTIFIER, status_code=503).server_unavailable

    data = snapshot(
        files=[
            {"name": "unknown", "source": "other"},
            {"name": "derived", "source": "derivative"},
        ],
        files_count=3,
        tasks={
            "unknown.php": {"state": "new-state"},
            "error.php": {"status": "failed", "message": "operator"},
            "blocked.php": {"state": "pending", "wait_admin": "2"},
        },
        pending_creation=True,
        has_redrow=True,
        nodownload=True,
    )
    item = _recovery.parse_item(data, IDENTIFIER)
    assert item.availability is InternetArchiveItemAvailability.PENDING
    assert not item.listing_complete
    assert len(item.pending_tasks) == 1
    assert len(item.error_tasks) == TWO_TASKS
    assert item.has_redrow and item.nodownload
    without_files = _recovery.parse_item(
        {"metadata": {"identifier": IDENTIFIER}}, IDENTIFIER
    )
    assert not without_files.listing_complete
    without_count = snapshot()
    without_count.pop("files_count")
    assert _recovery.parse_item(without_count, IDENTIFIER).listing_complete


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"success": False, "value": {"username": "owner"}},
        {"success": True, "value": []},
        {"success": True, "value": {}},
        {"success": True, "value": {"username": []}},
        {"success": True, "value": {"username": ""}},
        {"success": True, "value": {"username": "one", "email": "two"}},
    ],
)
def test_identity_parser_rejects_missing_malformed_and_ambiguous_evidence(
    payload: dict[str, object],
) -> None:
    """Accept no account identity substitutes or ambiguous values."""
    with pytest.raises(InvalidServiceResponseError):
        _recovery.parse_identity(payload)
    assert (
        _recovery.parse_identity(
            {"success": True, "value": {"username": "owner", "email": "owner"}}
        )
        == "owner"
    )


def prepared_file() -> _recovery.PreparedRecoveryFile:
    """Return one prepared source matching the module constants."""
    return _recovery.PreparedRecoveryFile(
        _items.PreparedFile("a.bin", 1, BytesIO(b"a")), 0, MD5, SHA1
    )


def usable_item(**overrides: object) -> InternetArchiveItem:
    """Return a snapshot that authorizes reconciliation by default."""
    values: dict[str, object] = {
        "identifier": IDENTIFIER,
        "metadata": {"identifier": IDENTIFIER, "source": "exact"},
        "uploader": "owner@example.invalid",
        "files": (),
        "availability": InternetArchiveItemAvailability.AVAILABLE,
        "listing_complete": True,
    }
    values.update(overrides)
    return cast("Any", InternetArchiveItem)(**values)


@pytest.mark.parametrize(
    ("item", "phase", "deferred"),
    [
        (
            InternetArchiveItem(IDENTIFIER),
            InternetArchiveRecoveryPhase.RECONCILIATION,
            True,
        ),
        (usable_item(uploader=None), InternetArchiveRecoveryPhase.OWNERSHIP, False),
        (
            usable_item(metadata={"source": "wrong"}),
            InternetArchiveRecoveryPhase.RECONCILIATION,
            False,
        ),
        (usable_item(is_dark=True), InternetArchiveRecoveryPhase.RECONCILIATION, False),
        (
            usable_item(
                tasks=(
                    InternetArchiveItemTask(
                        "archive.php", InternetArchiveItemTaskState.ERROR
                    ),
                )
            ),
            InternetArchiveRecoveryPhase.INGEST,
            False,
        ),
        (
            usable_item(listing_complete=False),
            InternetArchiveRecoveryPhase.INGEST,
            True,
        ),
        (
            usable_item(server_unavailable=True),
            InternetArchiveRecoveryPhase.INGEST,
            True,
        ),
        (
            usable_item(
                tasks=(
                    InternetArchiveItemTask(
                        "archive.php", InternetArchiveItemTaskState.UNKNOWN
                    ),
                )
            ),
            InternetArchiveRecoveryPhase.INGEST,
            True,
        ),
    ],
)
def test_reconciliation_rejects_each_unsafe_item_state(
    item: InternetArchiveItem,
    phase: InternetArchiveRecoveryPhase,
    deferred: bool,
) -> None:
    """Classify each state as a permanent failure or read-only deferral."""
    with pytest.raises(_recovery.RecoveryDecisionError) as failure:
        _recovery.reconcile(
            item,
            "owner@example.invalid",
            {"source": "exact"},
            (prepared_file(),),
            catalog_complete=True,
        )
    assert failure.value.phase is phase
    assert failure.value.deferred is deferred
    assert not failure.value.checksum_mismatch


def test_reconciliation_classifies_missing_matching_and_conflicting_files() -> None:
    """Use exact name, size, MD5, and SHA-1 across a complete batch."""
    source = prepared_file()
    decision = _recovery.reconcile(
        usable_item(),
        "owner@example.invalid",
        {"source": "exact"},
        (source,),
        catalog_complete=True,
    )
    assert decision.dispositions == {
        "a.bin": InternetArchiveFileDisposition.UNATTEMPTED
    }
    matching = replace(
        usable_item(),
        files=(InternetArchiveItemFile("a.bin", 1, MD5, SHA1),),
    )
    assert (
        _recovery.reconcile(
            matching,
            "owner@example.invalid",
            {"source": "exact"},
            (source,),
            catalog_complete=True,
        ).dispositions["a.bin"]
        is InternetArchiveFileDisposition.ALREADY_MATCHING
    )
    with pytest.raises(_recovery.RecoveryDecisionError) as catalog:
        _recovery.reconcile(
            usable_item(),
            "owner@example.invalid",
            {"source": "exact"},
            (source,),
            catalog_complete=False,
        )
    assert catalog.value.deferred
    assert not catalog.value.checksum_mismatch


@pytest.mark.parametrize(
    ("field", "value", "checksum_mismatch"),
    [
        ("size", 2, True),
        ("md5", "0" * 32, True),
        ("sha1", "0" * 40, True),
        ("size", None, False),
        ("md5", None, False),
        ("sha1", None, False),
    ],
)
def test_reconciliation_marks_only_actual_size_and_digest_conflicts(
    field: str, value: object, checksum_mismatch: bool
) -> None:
    """Distinguish checksum conflicts from incomplete verification evidence."""
    remote = replace(InternetArchiveItemFile("a.bin", 1, MD5, SHA1), **{field: value})
    with pytest.raises(_recovery.RecoveryDecisionError) as failure:
        _recovery.reconcile(
            usable_item(files=(remote,)),
            "owner@example.invalid",
            {"source": "exact"},
            (prepared_file(),),
            catalog_complete=True,
        )
    assert failure.value.files == ("a.bin",)
    assert failure.value.checksum_mismatch is checksum_mismatch
    assert failure.value.deferred is not checksum_mismatch
    assert failure.value.phase is (
        InternetArchiveRecoveryPhase.RECONCILIATION
        if checksum_mismatch
        else InternetArchiveRecoveryPhase.VERIFICATION
    )
