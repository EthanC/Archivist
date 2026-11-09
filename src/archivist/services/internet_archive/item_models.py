"""Models for Archive.org items, separate from Wayback page captures."""

from __future__ import annotations

import datetime
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path, PureWindowsPath
from types import MappingProxyType
from typing import Any, BinaryIO, TypeAlias, cast
from unicodedata import category

from archivist.core.errors import (
    InvalidOptionError,
    OptionCombinationError,
    ServiceError,
)

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*")
_METADATA_KEY = re.compile(r"[a-z][a-z0-9_-]*")
_COLLECTIONS = {
    "movies": "opensource_movies",
    "audio": "opensource_audio",
    "texts": "opensource",
    "software": "open_source_software",
    "image": "opensource_image",
    "data": "opensource_media",
}
_RESERVED_METADATA = frozenset(
    {
        "identifier",
        "uploader",
        "addeddate",
        "publicdate",
        "updatedate",
        "backup_location",
        "collection",
        "mediatype",
        "title",
        "description",
        "subject",
        "creator",
        "date",
        "language",
        "licenseurl",
        "subjects",
        "media_type",
        "test_item",
        "license",
        "metadata",
        "queue_derive",
    }
)

InternetArchiveMetadataValue: TypeAlias = str | tuple[str, ...]


class InternetArchiveItemAvailability(StrEnum):
    """The visibility state established by one metadata response."""

    AVAILABLE = "available"
    PENDING = "pending"
    UNAVAILABLE = "unavailable"
    UNCERTAIN = "uncertain"


class InternetArchiveItemFileSource(StrEnum):
    """The server-reported or filename-derived source of an item file.

    ``SPOOL`` marks the ``__ia_spool/`` part files IAS3 stores while a multipart
    upload is open. IA reports them as originals; they disappear on assembly.
    """

    ORIGINAL = "original"
    DERIVATIVE = "derivative"
    GENERATED = "generated"
    SPOOL = "spool"
    UNKNOWN = "unknown"


class InternetArchiveItemTaskState(StrEnum):
    """A normalized item task state without treating unknown states as success."""

    PENDING = "pending"
    COMPLETE = "complete"
    ERROR = "error"
    UNKNOWN = "unknown"


class InternetArchiveFileDisposition(StrEnum):
    """The outcome of one file in an explicit recovery call."""

    TRANSFERRED = "transferred"
    ALREADY_MATCHING = "already_matching"
    UNATTEMPTED = "unattempted"
    CONFLICTING = "conflicting"
    DEFERRED = "deferred"
    UNCERTAIN = "uncertain"


class InternetArchiveChecksumState(StrEnum):
    """Whether server checksums have verified the prepared source bytes."""

    UNVERIFIED = "unverified"
    PENDING = "pending"
    VERIFIED = "verified"
    MISMATCH = "mismatch"


class InternetArchiveRecoveryPhase(StrEnum):
    """The recovery phase in which an operation stopped."""

    OWNERSHIP = "ownership"
    RECONCILIATION = "reconciliation"
    TRANSFER = "transfer"
    INGEST = "ingest"
    VERIFICATION = "verification"


def _validate_text(value: object, name: str, *, multiline: bool = False) -> str:
    """Require nonblank Unicode text without controls or surrogate code points."""
    if not isinstance(value, str) or not value.strip():
        raise InvalidOptionError(f"{name} must be a nonblank string")
    if any(
        category(character) in {"Cc", "Cs"}
        and not (multiline and character in "\r\n\t")
        for character in value
    ):
        raise InvalidOptionError(f"{name} cannot contain controls or surrogates")
    return value


def _validate_identifier(value: object) -> str:
    """Validate an item identifier without imposing an unverified length limit."""
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise InvalidOptionError(
            "identifier must start with an ASCII letter or digit and contain only "
            "ASCII letters, digits, underscores, periods, or hyphens"
        )
    return value


def _validate_file_name(value: object) -> str:
    """Require a relative item path, preserving subpaths and Unicode spelling."""
    name = _validate_text(value, "file name")
    if (
        "\\" in name
        or PureWindowsPath(name).drive
        or any(part in {"", ".", ".."} for part in name.split("/"))
    ):
        raise InvalidOptionError("file name must be a relative path without traversal")
    return name


def _text_values(values: list[str], name: str, *, multiline: bool = False) -> list[str]:
    """Validate and copy text values, retaining order and repeated values."""
    result = list(values)
    if not result:
        raise InvalidOptionError(f"{name} cannot be empty")
    return [_validate_text(value, name, multiline=multiline) for value in result]


@dataclass(frozen=True, slots=True)
class InternetArchiveItemFile:
    """One exact filename and its optional server-maintained checksums."""

    name: str
    size: int | None = None
    md5: str | None = None
    sha1: str | None = None
    source: InternetArchiveItemFileSource = InternetArchiveItemFileSource.UNKNOWN
    source_value: str | None = None


@dataclass(frozen=True, slots=True)
class InternetArchiveItemTask:
    """One server task, retaining its raw state and operator-facing error."""

    command: str
    state: InternetArchiveItemTaskState
    raw_state: str | None = None
    wait_admin: int | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class InternetArchiveItem:
    """A read-only snapshot returned by the Archive.org Metadata API.

    Empty or incomplete responses are represented as uncertain snapshots. They
    are never interpreted as permission to create or mutate an item.
    """

    identifier: str
    metadata: Mapping[str, InternetArchiveMetadataValue] = field(
        default_factory=lambda: MappingProxyType({})
    )
    uploader: str | None = None
    files: tuple[InternetArchiveItemFile, ...] = ()
    tasks: tuple[InternetArchiveItemTask, ...] = ()
    availability: InternetArchiveItemAvailability = (
        InternetArchiveItemAvailability.UNCERTAIN
    )
    listing_complete: bool = False
    has_redrow: bool = False
    is_dark: bool = False
    nodownload: bool = False
    server_unavailable: bool = False
    workable_servers: tuple[str, ...] = ()
    extended_error_code: str | None = None
    unavailable_reason: str | None = None

    def __post_init__(self) -> None:
        """Freeze all response containers without inventing missing values."""
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))
        object.__setattr__(self, "files", tuple(self.files))
        object.__setattr__(self, "tasks", tuple(self.tasks))
        object.__setattr__(self, "workable_servers", tuple(self.workable_servers))

    @property
    def original_files(self) -> tuple[InternetArchiveItemFile, ...]:
        """Return original files while retaining every file in ``files``."""
        return tuple(
            file
            for file in self.files
            if file.source is InternetArchiveItemFileSource.ORIGINAL
        )

    @property
    def pending_tasks(self) -> tuple[InternetArchiveItemTask, ...]:
        """Return tasks that are pending or have an unrecognized state."""
        return tuple(
            task
            for task in self.tasks
            if task.state
            in {
                InternetArchiveItemTaskState.PENDING,
                InternetArchiveItemTaskState.UNKNOWN,
            }
        )

    @property
    def error_tasks(self) -> tuple[InternetArchiveItemTask, ...]:
        """Return tasks requiring operator intervention."""
        return tuple(
            task
            for task in self.tasks
            if task.state is InternetArchiveItemTaskState.ERROR
        )

    @property
    def available(self) -> bool:
        """Return whether this response establishes a usable item snapshot."""
        return self.availability is InternetArchiveItemAvailability.AVAILABLE


@dataclass(frozen=True, slots=True)
class InternetArchiveUploadFile:
    """A local path or explicitly named caller-owned seekable binary stream.

    Paths default to their basename. Streams are inspected by ``prepare_files``
    and uploaded from their current position; they are never closed by Archivist.
    """

    source: str | Path | BinaryIO = field(repr=False)
    name: str | None = None

    def __post_init__(self) -> None:
        """Normalize local paths and require a safe item-relative name."""
        source = self.source
        name = self.name
        if isinstance(source, (str, Path)):
            if isinstance(source, str):
                _validate_text(source, "source path")
            source = Path(source)
            object.__setattr__(self, "source", source)
            if name is None:
                name = source.name
        elif name is None:
            raise InvalidOptionError("binary streams require an explicit file name")
        object.__setattr__(self, "name", _validate_file_name(name))


@dataclass(frozen=True, slots=True)
class InternetArchiveUploadOptions:
    """Validated item metadata and uploader collection selection.

    Text is preserved, including Unicode, HTML, whitespace, and repeated values.
    Subjects and repeated metadata values are copied into lists. Custom metadata
    uses a read-only mapping. A missing collection selects the uploader default
    for the media type or test items.

    ``queue_derive=False`` sends ``x-archive-queue-derive: 0`` on every PUT,
    so the upload queues no derive task. The item then keeps only its original
    files: no derivatives, thumbnails, or streaming formats exist until a derive
    is queued separately. The default queues one derive after the last file.
    """

    identifier: str
    title: str
    description: str
    subjects: list[str]
    creator: str | None = None
    date: datetime.date | None = None
    collection: str | None = None
    language: str | None = None
    license: str | None = None
    media_type: str = "data"
    test_item: bool = False
    metadata: Mapping[str, str | list[str]] = field(
        default_factory=lambda: MappingProxyType({})
    )
    queue_derive: bool = True

    def __post_init__(self) -> None:
        """Validate typed fields, collection choices, and custom metadata keys."""
        _validate_identifier(self.identifier)
        _validate_text(self.title, "title")
        _validate_text(self.description, "description", multiline=True)
        if not isinstance(self.subjects, list):
            raise InvalidOptionError("subjects must be a list of strings")
        object.__setattr__(self, "subjects", _text_values(self.subjects, "subjects"))
        if self.date is not None and (
            not isinstance(self.date, datetime.date)
            or isinstance(self.date, datetime.datetime)
        ):
            raise InvalidOptionError(
                "date must be a date object, not a datetime or string"
            )
        for name in ("creator", "collection", "language", "license"):
            value = getattr(self, name)
            if value is not None:
                _validate_text(value, name)
        if not isinstance(self.test_item, bool):
            raise InvalidOptionError("test_item must be a boolean")
        if not isinstance(self.queue_derive, bool):
            raise InvalidOptionError("queue_derive must be a boolean")
        if not isinstance(self.media_type, str) or self.media_type not in _COLLECTIONS:
            raise InvalidOptionError(
                "media_type must be movies, audio, texts, software, image, or data"
            )
        if self.test_item and self.collection not in {None, "test_collection"}:
            raise OptionCombinationError("test items must use test_collection")
        collection = self.collection or (
            "test_collection" if self.test_item else _COLLECTIONS[self.media_type]
        )
        object.__setattr__(self, "collection", collection)
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))


def _normalize_metadata(
    metadata: Mapping[str, str | list[str]],
) -> Mapping[str, str | list[str]]:
    """Copy custom metadata and reject reserved or ambiguous wire keys."""
    if not isinstance(metadata, Mapping):
        raise InvalidOptionError("metadata must be a mapping")
    encoded_keys = {key.replace("_", "--") for key in _RESERVED_METADATA}
    normalized: dict[str, str | list[str]] = {}
    for key, value in metadata.items():
        if not isinstance(key, str) or _METADATA_KEY.fullmatch(key) is None:
            raise InvalidOptionError("metadata keys must match [a-z][a-z0-9_-]*")
        encoded = key.replace("_", "--")
        if encoded.replace("--", "_") != key:
            raise InvalidOptionError(
                "metadata keys must round-trip through underscore encoding"
            )
        if encoded in encoded_keys:
            raise InvalidOptionError(
                "metadata key is reserved or has an encoding collision"
            )
        encoded_keys.add(encoded)
        if isinstance(value, str):
            normalized[key] = _validate_text(value, "metadata value", multiline=True)
        elif isinstance(value, list):
            normalized[key] = _text_values(value, "metadata values", multiline=True)
        else:
            raise InvalidOptionError(
                "metadata values must be strings or lists of strings"
            )
    return MappingProxyType(normalized)


@dataclass(frozen=True, slots=True)
class InternetArchiveUploadFileResult:
    """One file outcome; false means failed or not yet attempted, not deleted."""

    name: str
    size: int
    transferred: bool = False
    etag: str | None = None


@dataclass(frozen=True, slots=True)
class InternetArchiveUploadResult:
    """Item transfer outcomes, separate from completion of uploader ingest.

    ``processing_complete`` does not promise derivative completion or public
    visibility. Clients can prepare all file outcomes before starting transfers.
    """

    identifier: str
    files: tuple[InternetArchiveUploadFileResult, ...]
    processing_complete: bool = False

    def __post_init__(self) -> None:
        """Copy file outcomes into an immutable tuple."""
        object.__setattr__(self, "files", tuple(self.files))

    @property
    def details_url(self) -> str:
        """Return the Archive.org item page URL, not a Wayback capture URL."""
        return f"https://archive.org/details/{self.identifier}"


@dataclass(frozen=True, slots=True)
class InternetArchiveRecoveryFileResult:
    """One explicit file-recovery outcome and its checksum verification state."""

    name: str
    size: int
    md5: str
    sha1: str
    disposition: InternetArchiveFileDisposition = (
        InternetArchiveFileDisposition.UNATTEMPTED
    )
    checksum_state: InternetArchiveChecksumState = (
        InternetArchiveChecksumState.UNVERIFIED
    )
    transferred: bool = False
    etag: str | None = None


@dataclass(frozen=True, slots=True)
class InternetArchiveRecoveryResult:
    """Per-file recovery outcomes, separate from ingest and verification."""

    identifier: str
    files: tuple[InternetArchiveRecoveryFileResult, ...]
    processing_complete: bool = False
    verification_complete: bool = False

    def __post_init__(self) -> None:
        """Copy file outcomes into an immutable tuple."""
        object.__setattr__(self, "files", tuple(self.files))

    @property
    def details_url(self) -> str:
        """Return the Archive.org item page URL."""
        return f"https://archive.org/details/{self.identifier}"


@dataclass(frozen=True, slots=True)
class InternetArchiveRemovalResult:
    """Queue acceptance for make_dark, not confirmation of darkness or erasure."""

    identifier: str
    accepted: bool
    task_id: str | None = None


@dataclass(frozen=True, slots=True)
class InternetArchiveUploadLimit:
    """One IAS3 ``check_limit`` reading, reported without a deferral policy.

    ``over_limit`` is ``True`` when IAS3 reports that writes are likely to get
    HTTP 503 SlowDown, ``False`` when the queue accepts more uploads or deletes,
    and ``None`` when the field was absent or not an integer. ``counters`` holds
    every integer field from the response's ``detail`` object, copied without
    interpretation. IAS3 documents ``detail`` as internal and subject to change.
    """

    over_limit: bool | None = None
    limit_reason: str | None = None
    counters: Mapping[str, int] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        """Copy counters into a read-only mapping."""
        object.__setattr__(self, "counters", MappingProxyType(dict(self.counters)))


class InternetArchiveUploadError(ServiceError):
    """An upload failure with partial outcomes and the underlying service error."""

    def __init__(
        self,
        result: InternetArchiveUploadResult,
        cause: ServiceError,
        *,
        failed_file: str | None = None,
    ) -> None:
        """Retain failure context without including server text in the message.

        ``failed_file`` identifies the failed transfer when known. Other files
        with ``transferred=False`` were not attempted; no rollback is implied.
        Inspect ``cause`` for structured response diagnostics and retry hints.
        """
        super().__init__(
            "Internet Archive item upload failed",
            service="Internet Archive",
            status_code=cause.status_code,
        )
        self.result = result
        self.cause = cause
        self.failed_file = failed_file


class InternetArchiveRecoveryError(ServiceError):
    """A recovery failure retaining exact per-file and service context."""

    def __init__(
        self,
        result: InternetArchiveRecoveryResult,
        cause: ServiceError,
        *,
        phase: InternetArchiveRecoveryPhase,
        failed_file: str | None = None,
    ) -> None:
        """Retain the cause (including diagnostics), failed filename, and phase."""
        super().__init__(
            "Internet Archive file recovery failed",
            service="Internet Archive",
            status_code=cause.status_code,
        )
        self.result = result
        self.cause = cause
        self.phase = phase
        self.failed_file = failed_file


class InternetArchiveRecoveryDeferredError(InternetArchiveRecoveryError):
    """A read-only deferral raised when current state cannot authorize writes."""

    def __init__(
        self,
        result: InternetArchiveRecoveryResult,
        cause: ServiceError,
        *,
        phase: InternetArchiveRecoveryPhase,
    ) -> None:
        """Retain the unresolved phase without implying that a write occurred."""
        super().__init__(result, cause, phase=phase)
        self.args = ("Internet Archive file recovery was deferred",)


class InternetArchiveMultipartPhase(StrEnum):
    """Where a multipart upload stands, as last persisted by the caller."""

    INITIATED = "initiated"
    COMPLETING = "completing"
    COMPLETED = "completed"


_MULTIPART_STATE_VERSION = 1
_MULTIPART_MAX_PARTS = 1000
_QUOTED_MD5 = re.compile(r'"[0-9a-f]{32}"')
_MD5_HEX = re.compile(r"[0-9a-f]{32}")
_SHA1_HEX = re.compile(r"[0-9a-f]{40}")


def _positive_int(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise InvalidOptionError(f"{name} must be a positive integer")
    return value


@dataclass(frozen=True, slots=True)
class InternetArchiveMultipartPart:
    """One part acknowledged by IAS3: its number, quoted-MD5 ETag, and size."""

    number: int
    etag: str
    size: int

    def __post_init__(self) -> None:
        """Require a positive part number and size and a quoted lowercase MD5."""
        _positive_int(self.number, "part number")
        _positive_int(self.size, "part size")
        if not isinstance(self.etag, str) or _QUOTED_MD5.fullmatch(self.etag) is None:
            raise InvalidOptionError("part etag must be a quoted lowercase MD5")


@dataclass(frozen=True, slots=True)
class InternetArchiveMultipartState:
    """Serializable resume state for one multipart upload of one file.

    Persist it each time ``on_state`` delivers a new value, before the upload
    continues. ``md5`` and ``sha1`` fingerprint the whole source; a resumed call
    rehashes the source and refuses to continue when they differ. ``parts``
    holds only parts whose ETag matched the local MD5 of their byte range.
    """

    identifier: str
    name: str
    upload_id: str
    size: int
    part_size: int
    md5: str
    sha1: str
    queue_derive: bool = True
    phase: InternetArchiveMultipartPhase = InternetArchiveMultipartPhase.INITIATED
    parts: tuple[InternetArchiveMultipartPart, ...] = ()

    def __post_init__(self) -> None:
        """Reject states that could not have come from a valid upload."""
        _validate_identifier(self.identifier)
        _validate_file_name(self.name)
        _validate_text(self.upload_id, "upload_id")
        _positive_int(self.size, "size")
        _positive_int(self.part_size, "part_size")
        if self.part_count > _MULTIPART_MAX_PARTS:
            raise InvalidOptionError(
                f"a multipart upload can have at most {_MULTIPART_MAX_PARTS} parts"
            )
        if not isinstance(self.md5, str) or _MD5_HEX.fullmatch(self.md5) is None:
            raise InvalidOptionError("md5 must be a lowercase hexadecimal MD5")
        if not isinstance(self.sha1, str) or _SHA1_HEX.fullmatch(self.sha1) is None:
            raise InvalidOptionError("sha1 must be a lowercase hexadecimal SHA-1")
        if not isinstance(self.queue_derive, bool):
            raise InvalidOptionError("queue_derive must be a boolean")
        if not isinstance(self.phase, InternetArchiveMultipartPhase):
            raise InvalidOptionError("phase must be an InternetArchiveMultipartPhase")
        parts = tuple(self.parts)
        if any(not isinstance(part, InternetArchiveMultipartPart) for part in parts):
            raise InvalidOptionError("parts must contain multipart part values")
        numbers = [part.number for part in parts]
        if numbers != sorted(set(numbers)):
            raise InvalidOptionError("parts must be unique and in ascending order")
        for part in parts:
            if part.number > self.part_count or part.size != self.part_length(
                part.number
            ):
                raise InvalidOptionError("part does not fit the upload's layout")
        if (
            self.phase is not InternetArchiveMultipartPhase.INITIATED
            and len(parts) != self.part_count
        ):
            raise InvalidOptionError("completion requires every part")
        object.__setattr__(self, "parts", parts)

    @property
    def part_count(self) -> int:
        """Return how many parts the source is divided into."""
        return -(-self.size // self.part_size)

    def part_length(self, number: int) -> int:
        """Return the byte length of one part; only the last part can be short."""
        if number == self.part_count:
            return self.size - self.part_size * (number - 1)
        return self.part_size

    @property
    def details_url(self) -> str:
        """Return the Archive.org item page URL."""
        return f"https://archive.org/details/{self.identifier}"

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-compatible mapping accepted by ``from_dict``."""
        return {
            "version": _MULTIPART_STATE_VERSION,
            "identifier": self.identifier,
            "name": self.name,
            "upload_id": self.upload_id,
            "size": self.size,
            "part_size": self.part_size,
            "md5": self.md5,
            "sha1": self.sha1,
            "queue_derive": self.queue_derive,
            "phase": self.phase.value,
            "parts": [[part.number, part.etag, part.size] for part in self.parts],
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> InternetArchiveMultipartState:
        """Rebuild a state saved by ``to_dict``, validating every field."""
        if not isinstance(data, Mapping):
            raise InvalidOptionError("multipart state must be a mapping")
        if data.get("version") != _MULTIPART_STATE_VERSION:
            raise InvalidOptionError("unsupported multipart state version")
        fields = {
            "identifier",
            "name",
            "upload_id",
            "size",
            "part_size",
            "md5",
            "sha1",
            "queue_derive",
            "phase",
            "parts",
        }
        if set(data) != fields | {"version"}:
            raise InvalidOptionError("multipart state has missing or unknown fields")
        values = cast("dict[str, Any]", data)
        raw_parts = values["parts"]
        if not isinstance(raw_parts, list) or any(
            not isinstance(entry, list) or len(entry) != 3  # noqa: PLR2004
            for entry in raw_parts
        ):
            raise InvalidOptionError(
                "multipart state parts must be [number, etag, size] lists"
            )
        phase = values["phase"]
        if not isinstance(phase, str) or phase not in {
            member.value for member in InternetArchiveMultipartPhase
        }:
            raise InvalidOptionError("multipart state has an unknown phase")
        return cls(
            identifier=values["identifier"],
            name=values["name"],
            upload_id=values["upload_id"],
            size=values["size"],
            part_size=values["part_size"],
            md5=values["md5"],
            sha1=values["sha1"],
            queue_derive=values["queue_derive"],
            phase=InternetArchiveMultipartPhase(phase),
            parts=tuple(InternetArchiveMultipartPart(*entry) for entry in raw_parts),
        )


@dataclass(frozen=True, slots=True)
class InternetArchiveMultipartResult:
    """The outcome of a multipart call that reached an accepted completion.

    ``state`` is ``None`` only when ``add_file_multipart`` found the exact file
    already present and uploaded nothing. ``verified`` is ``True`` when item
    metadata showed the file with the source's size and MD5.
    """

    identifier: str
    name: str
    size: int
    md5: str
    sha1: str
    state: InternetArchiveMultipartState | None = None
    verified: bool = False

    @property
    def details_url(self) -> str:
        """Return the Archive.org item page URL."""
        return f"https://archive.org/details/{self.identifier}"


class InternetArchiveMultipartError(ServiceError):
    """A multipart failure retaining the latest resume state and its cause.

    ``state`` is ``None`` when the failure came before IAS3 issued an upload ID.
    Otherwise pass it back as ``state=`` to resume. ``uncertain`` is ``True``
    when the upload ID is gone but no completion evidence was found: resuming
    cannot decide the outcome, and the caller must reconcile the item itself.
    """

    def __init__(
        self,
        state: InternetArchiveMultipartState | None,
        cause: ServiceError,
        *,
        uncertain: bool = False,
    ) -> None:
        """Retain the cause (including diagnostics) without server text."""
        super().__init__(
            "Internet Archive multipart upload failed",
            service="Internet Archive",
            status_code=cause.status_code,
        )
        self.state = state
        self.cause = cause
        self.uncertain = uncertain


# ``add_files`` aliases make the operation-specific names available while the
# recovery names remain useful when reconciling an interrupted earlier call.
InternetArchiveAddFilesFileResult = InternetArchiveRecoveryFileResult
InternetArchiveAddFilesResult = InternetArchiveRecoveryResult
InternetArchiveAddFilesError = InternetArchiveRecoveryError
