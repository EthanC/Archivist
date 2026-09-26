"""Pure preparation, parsing, and reconciliation for item file recovery."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from http import HTTPStatus
from types import MappingProxyType
from typing import BinaryIO

from archivist.core.errors import InvalidOptionError, InvalidServiceResponseError
from archivist.services.internet_archive import _items
from archivist.services.internet_archive.item_models import (
    InternetArchiveFileDisposition,
    InternetArchiveItem,
    InternetArchiveItemAvailability,
    InternetArchiveItemFile,
    InternetArchiveItemFileSource,
    InternetArchiveItemTask,
    InternetArchiveItemTaskState,
    InternetArchiveMetadataValue,
    InternetArchiveRecoveryPhase,
    _validate_identifier,
)

METADATA_URL = "https://archive.org/metadata"

_SERVICE = "Internet Archive"
_MD5 = re.compile(r"[0-9a-fA-F]{32}")
_SHA1 = re.compile(r"[0-9a-fA-F]{40}")
_GENERATED_SUFFIXES = (
    "_meta.xml",
    "_files.xml",
    "_dc.xml",
    "_meta.sqlite",
    "_archive.torrent",
)
_PENDING_CODE = "101"
_SERVER_CODES = frozenset({"102", "105"})
_UNAVAILABLE_CODES = frozenset({"102", "103", "104", "105"})
_PENDING_TASK_STATES = frozenset(
    {"pending", "queued", "running", "processing", "waiting", "in_progress"}
)
_COMPLETE_TASK_STATES = frozenset({"complete", "completed", "success", "done"})
_ERROR_TASK_STATES = frozenset({"error", "failed", "failure"})
_ADMIN_BLOCKED = 2


@dataclass(frozen=True, slots=True)
class PreparedRecoveryFile:
    """A prepared upload body with hashes of its exact remaining byte range."""

    file: _items.PreparedFile
    offset: int
    md5: str
    sha1: str

    @property
    def name(self) -> str:
        """Return the item-relative filename."""
        return self.file.name

    @property
    def size(self) -> int:
        """Return the exact prepared byte count."""
        return self.file.size

    @property
    def stream(self) -> BinaryIO:
        """Return the borrowed or operation-owned stream."""
        return self.file.stream

    def rewind(self) -> None:
        """Position the source at the hashed starting offset before transfer."""
        try:
            self.stream.seek(self.offset)
        except (AttributeError, OSError, TypeError, ValueError):
            raise InvalidOptionError(
                "upload source could not be restored before recovery transfer"
            ) from None


@dataclass(frozen=True, slots=True)
class Reconciliation:
    """A complete batch decision made from one fresh item snapshot."""

    dispositions: Mapping[str, InternetArchiveFileDisposition] = field(
        default_factory=lambda: MappingProxyType({})
    )

    def __post_init__(self) -> None:
        """Freeze the per-file decision mapping."""
        object.__setattr__(
            self, "dispositions", MappingProxyType(dict(self.dispositions))
        )


class RecoveryDecisionError(Exception):
    """Internal conservative reconciliation result for client orchestration."""

    def __init__(
        self,
        message: str,
        *,
        phase: InternetArchiveRecoveryPhase,
        deferred: bool = False,
        files: tuple[str, ...] = (),
        checksum_mismatch: bool = False,
    ) -> None:
        super().__init__(message)
        self.phase = phase
        self.deferred = deferred
        self.files = files
        self.checksum_mismatch = checksum_mismatch


def normalize_expected_metadata(
    metadata: Mapping[str, str | Sequence[str]],
) -> Mapping[str, InternetArchiveMetadataValue]:
    """Validate exact, nonempty provenance expectations without canonicalizing."""
    if not isinstance(metadata, Mapping) or not metadata:
        raise InvalidOptionError("expected_metadata must be a nonempty mapping")
    normalized: dict[str, InternetArchiveMetadataValue] = {}
    for key, value in metadata.items():
        if not isinstance(key, str) or not key:
            raise InvalidOptionError("expected_metadata keys must be nonempty strings")
        if isinstance(value, str):
            normalized[key] = value
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            values = tuple(value)
            if not values or any(not isinstance(entry, str) for entry in values):
                raise InvalidOptionError(
                    "expected_metadata sequences must contain strings"
                )
            normalized[key] = values
        else:
            raise InvalidOptionError(
                "expected_metadata values must be strings or sequences of strings"
            )
    return MappingProxyType(normalized)


def hash_prepared_files(
    files: tuple[_items.PreparedFile, ...],
) -> tuple[PreparedRecoveryFile, ...]:
    """Hash every source with bounded reads and restore every starting position."""
    prepared: list[PreparedRecoveryFile] = []
    for file in files:
        stream = file.stream
        try:
            offset = stream.tell()
            if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
                raise OSError
            md5 = hashlib.md5(usedforsecurity=False)
            sha1 = hashlib.sha1(usedforsecurity=False)
            remaining = file.size
            try:
                while remaining:
                    requested = min(65536, remaining)
                    chunk = stream.read(requested)
                    if (
                        not isinstance(chunk, bytes)
                        or not chunk
                        or len(chunk) > requested
                    ):
                        raise OSError
                    md5.update(chunk)
                    sha1.update(chunk)
                    remaining -= len(chunk)
            finally:
                stream.seek(offset)
        except (AttributeError, OSError, TypeError, ValueError):
            raise InvalidOptionError(
                "upload source could not be hashed as stable binary data"
            ) from None
        prepared.append(
            PreparedRecoveryFile(file, offset, md5.hexdigest(), sha1.hexdigest())
        )
    return tuple(prepared)


def _response_error(message: str) -> InvalidServiceResponseError:
    return InvalidServiceResponseError(message, service=_SERVICE)


def _optional_boolean(data: Mapping[str, object], name: str) -> bool:
    value = data.get(name, False)
    if not isinstance(value, bool):
        raise _response_error("Internet Archive returned invalid item state")
    return value


def _metadata_values(
    value: object,
) -> Mapping[str, InternetArchiveMetadataValue]:
    if not isinstance(value, Mapping):
        raise _response_error("Internet Archive returned invalid item metadata")
    result: dict[str, InternetArchiveMetadataValue] = {}
    for key, entry in value.items():
        if not isinstance(key, str):
            raise _response_error("Internet Archive returned invalid item metadata")
        if isinstance(entry, str):
            result[key] = entry
        elif isinstance(entry, list) and all(isinstance(item, str) for item in entry):
            result[key] = tuple(entry)
        else:
            raise _response_error("Internet Archive returned invalid item metadata")
    return MappingProxyType(result)


def _optional_size(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, str) and value.isascii() and value.isdecimal():
        value = int(value)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise _response_error("Internet Archive returned an invalid item file size")
    return value


def _optional_digest(value: object, pattern: re.Pattern[str], name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise _response_error(f"Internet Archive returned an invalid {name} checksum")
    return value.lower()


def _file_source(
    identifier: str, name: str, value: object
) -> tuple[InternetArchiveItemFileSource, str | None]:
    if value is not None and not isinstance(value, str):
        raise _response_error("Internet Archive returned an invalid item file source")
    if name in {identifier + suffix for suffix in _GENERATED_SUFFIXES}:
        return InternetArchiveItemFileSource.GENERATED, value
    if value == "original":
        return InternetArchiveItemFileSource.ORIGINAL, value
    if value == "derivative":
        return InternetArchiveItemFileSource.DERIVATIVE, value
    return InternetArchiveItemFileSource.UNKNOWN, value


def _parse_files(identifier: str, value: object) -> tuple[InternetArchiveItemFile, ...]:
    if not isinstance(value, list):
        raise _response_error("Internet Archive returned an invalid item file listing")
    files: list[InternetArchiveItemFile] = []
    names: set[str] = set()
    for entry in value:
        if not isinstance(entry, Mapping):
            raise _response_error("Internet Archive returned an invalid item file")
        name = entry.get("name")
        if not isinstance(name, str) or not name or name in names:
            raise _response_error("Internet Archive returned an ambiguous item file")
        names.add(name)
        source, raw_source = _file_source(identifier, name, entry.get("source"))
        files.append(
            InternetArchiveItemFile(
                name=name,
                size=_optional_size(entry.get("size")),
                md5=_optional_digest(entry.get("md5"), _MD5, "MD5"),
                sha1=_optional_digest(entry.get("sha1"), _SHA1, "SHA-1"),
                source=source,
                source_value=raw_source,
            )
        )
    return tuple(files)


def _task_state(value: object) -> tuple[InternetArchiveItemTaskState, str | None]:
    if value is None:
        return InternetArchiveItemTaskState.UNKNOWN, None
    if not isinstance(value, str) or not value:
        raise _response_error("Internet Archive returned an invalid item task state")
    normalized = value.casefold()
    if normalized in _PENDING_TASK_STATES:
        state = InternetArchiveItemTaskState.PENDING
    elif normalized in _COMPLETE_TASK_STATES:
        state = InternetArchiveItemTaskState.COMPLETE
    elif normalized in _ERROR_TASK_STATES:
        state = InternetArchiveItemTaskState.ERROR
    else:
        state = InternetArchiveItemTaskState.UNKNOWN
    return state, value


def _parse_tasks(value: object) -> tuple[InternetArchiveItemTask, ...]:
    if value is None:
        return ()
    if isinstance(value, Mapping):
        entries: list[object] = [
            {"cmd": command, **dict(task)}
            if isinstance(command, str) and isinstance(task, Mapping)
            else task
            for command, task in value.items()
        ]
    elif isinstance(value, list):
        entries = value
    else:
        raise _response_error("Internet Archive returned invalid item tasks")
    tasks: list[InternetArchiveItemTask] = []
    for entry in entries:
        if not isinstance(entry, Mapping):
            raise _response_error("Internet Archive returned an invalid item task")
        command = entry.get("cmd", entry.get("command"))
        if not isinstance(command, str) or not command:
            raise _response_error("Internet Archive returned an invalid item task")
        state, raw_state = _task_state(entry.get("state", entry.get("status")))
        wait_admin = entry.get("wait_admin")
        if (
            isinstance(wait_admin, str)
            and wait_admin.isascii()
            and wait_admin.isdecimal()
        ):
            wait_admin = int(wait_admin)
        if wait_admin is not None and (
            not isinstance(wait_admin, int)
            or isinstance(wait_admin, bool)
            or wait_admin < 0
        ):
            raise _response_error("Internet Archive returned an invalid item task")
        error = entry.get("error", entry.get("message"))
        if error is not None and not isinstance(error, str):
            raise _response_error("Internet Archive returned an invalid item task")
        if wait_admin == _ADMIN_BLOCKED:
            state = InternetArchiveItemTaskState.ERROR
        tasks.append(
            InternetArchiveItemTask(command, state, raw_state, wait_admin, error)
        )
    return tuple(tasks)


def _extended_code(data: Mapping[str, object]) -> str | None:
    if "errcode" not in data:
        return None
    value = data["errcode"]
    if not isinstance(value, int) or isinstance(value, bool):
        raise _response_error("Internet Archive returned an invalid extended error")
    return str(value)


def parse_item(
    data: Mapping[str, object], identifier: str, *, status_code: int = 200
) -> InternetArchiveItem:
    """Parse a metadata snapshot without interpreting absence as write permission."""
    _validate_identifier(identifier)
    if not isinstance(data, Mapping):
        raise _response_error("Internet Archive returned invalid item metadata")

    code = _extended_code(data)
    reason_value = data.get("error", data.get("message"))
    if reason_value is not None and not isinstance(reason_value, str):
        raise _response_error("Internet Archive returned an invalid item error")
    reason = reason_value if isinstance(reason_value, str) else None
    metadata_value = data.get("metadata")
    metadata = _metadata_values(metadata_value) if metadata_value is not None else {}
    if metadata_value is not None and metadata.get("identifier") != identifier:
        raise _response_error("Internet Archive returned metadata for another item")
    uploader = metadata.get("uploader")
    if uploader is not None and not isinstance(uploader, str):
        raise _response_error("Internet Archive returned an invalid item uploader")

    files_value = data.get("files")
    files = _parse_files(identifier, files_value) if files_value is not None else ()
    listing_complete = metadata_value is not None and files_value is not None
    count = data.get("files_count")
    if count is not None:
        count = _optional_size(count)
        listing_complete = listing_complete and count == len(files)

    workable_value = data.get("workable_servers", ())
    if not isinstance(workable_value, list | tuple) or any(
        not isinstance(server, str) or not server for server in workable_value
    ):
        raise _response_error("Internet Archive returned invalid workable servers")
    workable_servers = tuple(workable_value)
    server_unavailable = (
        _optional_boolean(data, "servers_unavailable")
        or code in _SERVER_CODES
        or status_code >= HTTPStatus.INTERNAL_SERVER_ERROR
    )
    pending_creation = _optional_boolean(data, "pending_creation")
    if code == _PENDING_CODE or pending_creation:
        availability = InternetArchiveItemAvailability.PENDING
    elif code in _UNAVAILABLE_CODES or status_code >= HTTPStatus.BAD_REQUEST:
        availability = InternetArchiveItemAvailability.UNAVAILABLE
    elif code is not None or reason is not None or metadata_value is None:
        # Secondary copies (106), inaccurate lookahead (400), and unknown errors
        # cannot establish authoritative state for writes, even with metadata.
        availability = InternetArchiveItemAvailability.UNCERTAIN
    else:
        availability = InternetArchiveItemAvailability.AVAILABLE
    return InternetArchiveItem(
        identifier=identifier,
        metadata=metadata,
        uploader=uploader,
        files=files,
        tasks=_parse_tasks(data.get("tasks")),
        availability=availability,
        listing_complete=listing_complete,
        has_redrow=_optional_boolean(data, "has_redrow"),
        is_dark=_optional_boolean(data, "is_dark"),
        nodownload=_optional_boolean(data, "nodownload"),
        server_unavailable=server_unavailable,
        workable_servers=workable_servers,
        extended_error_code=code,
        unavailable_reason=reason,
    )


def parse_identity(data: Mapping[str, object]) -> str:
    """Return the sole account identity authenticated by a pinned LOW key pair."""
    if not isinstance(data, Mapping) or data.get("success") is not True:
        raise _response_error("Internet Archive returned invalid account identity")
    value = data.get("value")
    if not isinstance(value, Mapping):
        raise _response_error("Internet Archive returned invalid account identity")
    supplied = [value.get(name) for name in ("username", "email")]
    if any(
        identity is not None and (not isinstance(identity, str) or not identity)
        for identity in supplied
    ):
        raise _response_error("Internet Archive returned ambiguous account identity")
    identities = {identity for identity in supplied if isinstance(identity, str)}
    if len(identities) != 1:
        raise _response_error("Internet Archive returned ambiguous account identity")
    return next(iter(identities))


def reconcile(  # noqa: PLR0912 - Conservative state checks stay explicit.
    item: InternetArchiveItem,
    identity: str,
    expected_metadata: Mapping[str, InternetArchiveMetadataValue],
    files: tuple[PreparedRecoveryFile, ...],
    *,
    catalog_complete: bool,
) -> Reconciliation:
    """Authorize and classify a complete batch from fresh read-only state."""
    names = tuple(file.name for file in files)
    if item.availability is not InternetArchiveItemAvailability.AVAILABLE:
        raise RecoveryDecisionError(
            "item visibility is unresolved",
            phase=InternetArchiveRecoveryPhase.RECONCILIATION,
            deferred=True,
            files=names,
        )
    if item.uploader is None or item.uploader != identity:
        raise RecoveryDecisionError(
            "the pinned API credentials do not own the item",
            phase=InternetArchiveRecoveryPhase.OWNERSHIP,
            files=names,
        )
    if any(item.metadata.get(key) != value for key, value in expected_metadata.items()):
        raise RecoveryDecisionError(
            "item provenance metadata does not match",
            phase=InternetArchiveRecoveryPhase.RECONCILIATION,
            files=names,
        )
    if item.is_dark or item.nodownload or item.has_redrow:
        raise RecoveryDecisionError(
            "item state does not permit file recovery",
            phase=InternetArchiveRecoveryPhase.RECONCILIATION,
            files=names,
        )
    if item.error_tasks:
        raise RecoveryDecisionError(
            "an item task requires operator intervention",
            phase=InternetArchiveRecoveryPhase.INGEST,
            files=names,
        )
    if (
        item.server_unavailable
        or not item.listing_complete
        or item.pending_tasks
        or not catalog_complete
    ):
        raise RecoveryDecisionError(
            "item reconciliation is not currently complete",
            phase=InternetArchiveRecoveryPhase.INGEST,
            deferred=True,
            files=names,
        )

    existing = {file.name: file for file in item.files}
    dispositions: dict[str, InternetArchiveFileDisposition] = {}
    conflicts: list[str] = []
    deferred: list[str] = []
    for file in files:
        remote = existing.get(file.name)
        if remote is None:
            dispositions[file.name] = InternetArchiveFileDisposition.UNATTEMPTED
        elif remote.size is None or remote.md5 is None or remote.sha1 is None:
            dispositions[file.name] = InternetArchiveFileDisposition.DEFERRED
            deferred.append(file.name)
        elif (
            remote.size == file.size
            and remote.md5 == file.md5
            and remote.sha1 == file.sha1
        ):
            dispositions[file.name] = InternetArchiveFileDisposition.ALREADY_MATCHING
        else:
            dispositions[file.name] = InternetArchiveFileDisposition.CONFLICTING
            conflicts.append(file.name)
    if conflicts:
        raise RecoveryDecisionError(
            "one or more existing item files conflict with the prepared batch",
            phase=InternetArchiveRecoveryPhase.RECONCILIATION,
            files=tuple(conflicts),
            checksum_mismatch=True,
        )
    if deferred:
        raise RecoveryDecisionError(
            "one or more existing item files lack complete checksums",
            phase=InternetArchiveRecoveryPhase.VERIFICATION,
            deferred=True,
            files=tuple(deferred),
        )
    return Reconciliation(dispositions)
