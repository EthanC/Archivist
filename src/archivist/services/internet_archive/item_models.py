"""Models for Archive.org items, separate from Wayback page captures."""

from __future__ import annotations

import datetime
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path, PureWindowsPath
from types import MappingProxyType
from typing import BinaryIO
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
    }
)


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
class InternetArchiveRemovalResult:
    """Queue acceptance for make_dark, not confirmation of darkness or erasure."""

    identifier: str
    accepted: bool
    task_id: str | None = None


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
        """
        super().__init__(
            "Internet Archive item upload failed",
            service="Internet Archive",
            status_code=cause.status_code,
        )
        self.result = result
        self.cause = cause
        self.failed_file = failed_file
