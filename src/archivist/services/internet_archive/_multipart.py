"""Pure IAS3 multipart helpers shared by the synchronous and async clients.

Protocol facts were probed live on 2026-11-09 (see the multipart plan):

- Initiating needs an existing item: ``NoSuchBucket`` until creation starts.
- Part PUTs return ``ETag: "<md5>"``; completion XML must repeat it quoted.
- Retrying a completion is not idempotent: the upload ID is then unknown.
- Every part PUT, the initiate, the completion, and an abort each queue a task.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping
from contextlib import ExitStack
from dataclasses import dataclass, replace
from pathlib import Path
from typing import cast
from urllib.parse import quote
from xml.etree import ElementTree

from archivist.core.errors import InvalidOptionError, InvalidServiceResponseError
from archivist.services.internet_archive import _common, _items, _recovery
from archivist.services.internet_archive.item_models import (
    _MULTIPART_MAX_PARTS,
    InternetArchiveItem,
    InternetArchiveMultipartPart,
    InternetArchiveMultipartPhase,
    InternetArchiveMultipartState,
    InternetArchiveUploadFile,
)

MIB = 1024 * 1024
DEFAULT_PART_SIZE = 512 * MIB
# Amazon S3's documented part ceiling. IA's own ceiling is unconfirmed.
MAX_PART_SIZE = 5 * 1024 * MIB
# One ListParts page holds 1000 parts; IA's paging was not verified live.
MAX_PARTS = _MULTIPART_MAX_PARTS
TASKS_URL = "https://archive.org/services/tasks.php"

PHASE_INITIATE = "multipart-initiate"
PHASE_PART = "multipart-part"
PHASE_LIST = "multipart-list"
PHASE_COMPLETE = "multipart-complete"
PHASE_ABORT = "multipart-abort"
PHASE_EVIDENCE = "multipart-evidence"

NO_SUCH_BUCKET = "NoSuchBucket"
NO_SUCH_UPLOAD = "NoSuchUpload"
COMPLETION_COMMENT = "s3-put-complete-multipart"

_SERVICE = _common.SERVICE
_READ_SIZE = 65536
_UNSAFE_XML = re.compile(r"<!\s*(?:DOCTYPE|ENTITY)", re.IGNORECASE)
_ERROR_TASK_STATES = frozenset({"error", "failed", "failure"})


def _response_error(message: str) -> InvalidServiceResponseError:
    return InvalidServiceResponseError(message, service=_SERVICE)


@dataclass(frozen=True, slots=True)
class SourceDigest:
    """Hashes of one source's whole range and of every part-sized slice."""

    file: _items.PreparedFile
    offset: int
    md5: str
    sha1: str
    part_size: int
    part_md5s: tuple[str, ...]

    @property
    def name(self) -> str:
        """Return the item-relative file name."""
        return self.file.name

    @property
    def size(self) -> int:
        """Return the prepared byte count."""
        return self.file.size

    def etag(self, number: int) -> str:
        """Return the quoted MD5 IAS3 must acknowledge for one part."""
        return f'"{self.part_md5s[number - 1]}"'

    def part_length(self, number: int) -> int:
        """Return one part's byte length; only the last part can be short."""
        if number == len(self.part_md5s):
            return self.size - self.part_size * (number - 1)
        return self.part_size

    def part_body(self, number: int) -> _items.PreparedFile:
        """Position the source at one part and return a body bounded to it."""
        try:
            self.file.stream.seek(self.offset + self.part_size * (number - 1))
        except (AttributeError, OSError, TypeError, ValueError):
            raise InvalidOptionError(
                "upload source could not be positioned for a multipart part"
            ) from None
        return _items.PreparedFile(
            self.name, self.part_length(number), self.file.stream
        )


def validate_part_size(part_size: object) -> int:
    """Require an integer part size within the client's enforced bounds."""
    if (
        not isinstance(part_size, int)
        or isinstance(part_size, bool)
        or not 1 <= part_size <= MAX_PART_SIZE
    ):
        raise InvalidOptionError(
            f"part_size must be an integer from 1 to {MAX_PART_SIZE} bytes"
        )
    return part_size


def validate_call(  # noqa: PLR0913 - Mirrors the public keyword arguments.
    *,
    state: object,
    part_size: object,
    on_state: object,
    wait: object,
    timeout: object,
    poll_interval: object,
) -> tuple[int, float, float]:
    """Validate shared multipart arguments before any source or network access."""
    if state is not None and not isinstance(state, InternetArchiveMultipartState):
        raise InvalidOptionError("state must be an InternetArchiveMultipartState")
    if on_state is not None and not callable(on_state):
        raise InvalidOptionError("on_state must be callable")
    if not isinstance(wait, bool):
        raise InvalidOptionError("wait must be a boolean")
    size = validate_part_size(part_size) if state is None else state.part_size
    return (
        size,
        _common.validate_duration(
            cast("float", timeout), name="timeout", allow_zero=False
        ),
        _common.validate_duration(
            cast("float", poll_interval), name="poll_interval", allow_zero=False
        ),
    )


def validate_file(file: object) -> None:
    """Require exactly one upload file or path, before opening anything."""
    if not isinstance(file, (InternetArchiveUploadFile, str, Path)):
        raise InvalidOptionError("file must be an upload file or a path")


def check_name(prepared: _items.PreparedFile) -> _items.PreparedFile:
    """Reject names inside the IAS3 multipart spool directory."""
    if prepared.name.startswith(_recovery.SPOOL_PREFIX):
        raise InvalidOptionError(
            "upload file name is reserved for IAS3 multipart spool files"
        )
    return prepared


def prepare_source(
    file: InternetArchiveUploadFile | str | Path, identifier: str, stack: ExitStack
) -> _items.PreparedFile:
    """Open and validate exactly one source, as ``upload`` validates a batch."""
    validate_file(file)
    return check_name(_items.prepare_files((file,), identifier, stack)[0])


def hash_source(file: _items.PreparedFile, part_size: int) -> SourceDigest:
    """Hash the source and each part with bounded reads, restoring the offset."""
    if -(-file.size // part_size) > MAX_PARTS:
        raise InvalidOptionError(
            f"a multipart upload can have at most {MAX_PARTS} parts; "
            "use a larger part_size"
        )
    stream = file.stream
    try:
        offset = stream.tell()
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise OSError
        whole_md5 = hashlib.md5(usedforsecurity=False)
        whole_sha1 = hashlib.sha1(usedforsecurity=False)
        part_md5s: list[str] = []
        try:
            remaining = file.size
            while remaining:
                part = hashlib.md5(usedforsecurity=False)
                part_remaining = min(part_size, remaining)
                while part_remaining:
                    requested = min(_READ_SIZE, part_remaining)
                    chunk = stream.read(requested)
                    if (
                        not isinstance(chunk, bytes)
                        or not chunk
                        or len(chunk) > requested
                    ):
                        raise OSError
                    whole_md5.update(chunk)
                    whole_sha1.update(chunk)
                    part.update(chunk)
                    part_remaining -= len(chunk)
                    remaining -= len(chunk)
                part_md5s.append(part.hexdigest())
        finally:
            stream.seek(offset)
    except (AttributeError, OSError, TypeError, ValueError):
        raise InvalidOptionError(
            "upload source could not be hashed as stable binary data"
        ) from None
    return SourceDigest(
        file,
        offset,
        whole_md5.hexdigest(),
        whole_sha1.hexdigest(),
        part_size,
        tuple(part_md5s),
    )


def check_resume(
    state: InternetArchiveMultipartState, digest: SourceDigest, identifier: str
) -> None:
    """Refuse to resume with a different item, file name, or source content."""
    if state.identifier != identifier or state.name != digest.name:
        raise InvalidOptionError("state belongs to a different item or file name")
    if (state.size, state.md5, state.sha1) != (digest.size, digest.md5, digest.sha1):
        raise InvalidOptionError("upload source changed since the state was saved")


def new_state(
    digest: SourceDigest,
    identifier: str,
    upload_id: str,
    *,
    queue_derive: bool,
    interactive_priority: bool,
) -> InternetArchiveMultipartState:
    """Return the first resume state for a freshly initiated upload."""
    return InternetArchiveMultipartState(
        identifier=identifier,
        name=digest.name,
        upload_id=upload_id,
        size=digest.size,
        part_size=digest.part_size,
        md5=digest.md5,
        sha1=digest.sha1,
        queue_derive=queue_derive,
        interactive_priority=interactive_priority,
    )


def object_url(identifier: str, name: str) -> str:
    """Return the IAS3 object URL for one item file."""
    return f"{_items.S3_URL}/{identifier}/{quote(name, safe='/')}"


def bucket_url(identifier: str) -> str:
    """Return the IAS3 bucket URL for one item."""
    return f"{_items.S3_URL}/{identifier}"


def initiate_headers(size: int, *, interactive_priority: bool) -> dict[str, str]:
    """Return headers for an empty-bodied initiate request."""
    return {
        "Content-Type": "application/octet-stream",
        "Content-Length": "0",
        "x-archive-size-hint": str(size),
        **_items.priority_headers(interactive_priority),
    }


def part_headers(state: InternetArchiveMultipartState, length: int) -> dict[str, str]:
    """Return headers for one part body."""
    return {
        "Content-Type": "application/octet-stream",
        "Content-Length": str(length),
        **_items.priority_headers(state.interactive_priority),
    }


def complete_headers(state: InternetArchiveMultipartState) -> dict[str, str]:
    """Return completion headers; derive settings take effect only here."""
    headers = {
        "Content-Type": "application/xml",
        **_items.priority_headers(state.interactive_priority),
    }
    if not state.queue_derive:
        headers["x-archive-queue-derive"] = "0"
    return headers


def abort_headers(state: InternetArchiveMultipartState) -> dict[str, str]:
    """Return abort headers, keeping the upload's priority setting."""
    return _items.priority_headers(state.interactive_priority)


def complete_body(state: InternetArchiveMultipartState) -> bytes:
    """Return CompleteMultipartUpload XML with the quoted ETags IAS3 compares."""
    parts = "".join(
        f"<Part><PartNumber>{part.number}</PartNumber><ETag>{part.etag}</ETag></Part>"
        for part in state.parts
    )
    return f"<CompleteMultipartUpload>{parts}</CompleteMultipartUpload>".encode()


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _parse_xml(text: str, root_name: str) -> ElementTree.Element:
    """Parse entity-free XML and require its namespace-free root element."""
    if _UNSAFE_XML.search(text):
        raise _response_error("Internet Archive returned unsupported multipart XML")
    try:
        root = ElementTree.fromstring(text.strip())
    except (ElementTree.ParseError, ValueError):
        raise _response_error(
            "Internet Archive returned malformed multipart XML"
        ) from None
    if _local(root.tag) != root_name:
        raise _response_error("Internet Archive returned unexpected multipart XML")
    return root


def _child(element: ElementTree.Element, name: str) -> str | None:
    values = [child.text or "" for child in element if _local(child.tag) == name]
    if len(values) > 1:
        raise _response_error("Internet Archive returned ambiguous multipart XML")
    return values[0] if values else None


def _check_target(
    root: ElementTree.Element, identifier: str, name: str, kind: str
) -> None:
    if _child(root, "Bucket") != identifier or _child(root, "Key") != name:
        raise _response_error(f"Internet Archive {kind} named another item or file")


def parse_initiate(text: str, identifier: str, name: str) -> str:
    """Return the upload ID from an InitiateMultipartUploadResult."""
    root = _parse_xml(text, "InitiateMultipartUploadResult")
    _check_target(root, identifier, name, "multipart initiation")
    upload_id = _child(root, "UploadId")
    if not upload_id or not upload_id.strip() or upload_id != upload_id.strip():
        raise _response_error("Internet Archive returned an invalid upload ID")
    return upload_id


def _decimal(value: str | None) -> int:
    if value is None or not value.isascii() or not value.isdecimal():
        raise _response_error("Internet Archive returned an invalid multipart part")
    return int(value)


def parse_list_parts(
    text: str, state: InternetArchiveMultipartState
) -> dict[int, tuple[str, int]]:
    """Return listed parts by number as ``(etag, size)``; refuse truncation."""
    root = _parse_xml(text, "ListPartsResult")
    _check_target(root, state.identifier, state.name, "part listing")
    if _child(root, "UploadId") not in {None, state.upload_id}:
        raise _response_error("Internet Archive listed parts of another upload")
    if (_child(root, "IsTruncated") or "false").strip().casefold() != "false":
        raise _response_error("Internet Archive returned a truncated part listing")
    parts: dict[int, tuple[str, int]] = {}
    for element in root:
        if _local(element.tag) != "Part":
            continue
        number = _decimal(_child(element, "PartNumber"))
        etag = _child(element, "ETag")
        size = _decimal(_child(element, "Size"))
        if number in parts or etag is None:
            raise _response_error("Internet Archive returned an invalid multipart part")
        parts[number] = (etag.strip().lower(), size)
    return parts


def parse_complete(text: str, identifier: str, name: str) -> None:
    """Require a CompleteMultipartUploadResult for the expected object."""
    _check_target(
        _parse_xml(text, "CompleteMultipartUploadResult"),
        identifier,
        name,
        "multipart completion",
    )


def acknowledged_part(
    etag: object, digest: SourceDigest, number: int
) -> InternetArchiveMultipartPart:
    """Accept a part only when its ETag equals the local MD5 of its bytes."""
    expected = digest.etag(number)
    if not isinstance(etag, str) or etag.strip().lower() != expected:
        raise _response_error(
            "Internet Archive acknowledged a multipart part with a different checksum"
        )
    return InternetArchiveMultipartPart(number, expected, digest.part_length(number))


def reconcile_parts(
    state: InternetArchiveMultipartState,
    listed: Mapping[int, tuple[str, int]],
    digest: SourceDigest,
) -> InternetArchiveMultipartState:
    """Keep only listed parts that match the source; others are sent again.

    A listed part absent from the state was sent before its acknowledgement
    was lost; it is kept too when it matches the source.
    """
    parts: list[InternetArchiveMultipartPart] = []
    for number in range(1, state.part_count + 1):
        expected = (digest.etag(number), digest.part_length(number))
        if listed.get(number) == expected:
            parts.append(InternetArchiveMultipartPart(number, *expected))
    return replace(
        state, phase=InternetArchiveMultipartPhase.INITIATED, parts=tuple(parts)
    )


def with_part(
    state: InternetArchiveMultipartState, part: InternetArchiveMultipartPart
) -> InternetArchiveMultipartState:
    """Return the state with one more acknowledged part, kept in order."""
    parts = {existing.number: existing for existing in state.parts}
    parts[part.number] = part
    return replace(state, parts=tuple(parts[number] for number in sorted(parts)))


def missing_parts(state: InternetArchiveMultipartState) -> tuple[int, ...]:
    """Return part numbers not yet acknowledged, in ascending order."""
    done = {part.number for part in state.parts}
    return tuple(
        number for number in range(1, state.part_count + 1) if number not in done
    )


def file_status(
    item: InternetArchiveItem, state: InternetArchiveMultipartState
) -> bool | None:
    """Compare the item's copy of the file with the source.

    Return ``True`` for a size and MD5 match, ``False`` for a conflicting copy,
    and ``None`` while the file or its checksum is not yet listed.
    """
    remote = next((file for file in item.files if file.name == state.name), None)
    if remote is None or remote.size is None or remote.md5 is None:
        return None
    return (
        remote.size == state.size
        and remote.md5 == state.md5
        and remote.sha1 in {None, state.sha1}
    )


def completion_task_found(data: Mapping[str, object], upload_id: str) -> bool:
    """Return whether the Tasks API lists a non-failed completion for an upload."""
    value = data.get("value")
    if data.get("success") is not True or not isinstance(value, Mapping):
        raise _response_error("Internet Archive returned invalid task data")
    for kind in ("catalog", "history"):
        rows = cast("Mapping[str, object]", value).get(kind) or []
        if not isinstance(rows, list):
            raise _response_error("Internet Archive returned invalid task data")
        for row in rows:
            if not isinstance(row, Mapping):
                raise _response_error("Internet Archive returned invalid task data")
            args = row.get("args")
            status = row.get("status")
            if (
                isinstance(args, Mapping)
                and args.get("comment") == COMPLETION_COMMENT
                and args.get("complete_multipart_upload_id") == upload_id
                and not (
                    isinstance(status, str) and status.casefold() in _ERROR_TASK_STATES
                )
            ):
                return True
    return False


def task_params(identifier: str) -> dict[str, str]:
    """Return Tasks API query values listing queued and finished item tasks."""
    return {"identifier": identifier, "catalog": "1", "history": "1"}


StateCallback = Callable[[InternetArchiveMultipartState], object]
