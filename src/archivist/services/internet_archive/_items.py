"""Shared local preparation and protocol parsing for Archive.org item clients.

No helper performs network I/O. The caller owns authentication, transport,
polling, and the ExitStack that closes locally opened upload files.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncIterator, Callable, Iterable, Iterator, Mapping
from contextlib import ExitStack, asynccontextmanager, suppress
from dataclasses import dataclass, field
from html.parser import HTMLParser
from http import HTTPStatus
from io import SEEK_END
from pathlib import Path
from typing import BinaryIO, ParamSpec, TypeVar
from urllib.parse import quote

from archivist.core._http import ResponseLike, parse_retry_after
from archivist.core.errors import (
    AuthenticationError,
    InvalidOptionError,
    InvalidServiceResponseError,
    RateLimitError,
    ServiceError,
)
from archivist.services.internet_archive.item_models import (
    InternetArchiveRemovalResult,
    InternetArchiveUploadFile,
    InternetArchiveUploadOptions,
    _normalize_metadata,
    _text_values,
    _validate_identifier,
    _validate_text,
)
from archivist.services.internet_archive.models import InternetArchiveApiKey

UPLOAD_URL = "https://archive.org/upload"
UPLOAD_API_URL = "https://archive.org/upload/app/upload_api.php"
S3_URL = "https://s3.us.archive.org"
MANAGE_URL = "https://archive.org/manage/"

_SERVICE = "Internet Archive"
_P = ParamSpec("_P")
_T = TypeVar("_T")
_ADMIN_BLOCKED = 2
_GENERATED_SUFFIXES = (
    "_meta.xml",
    "_files.xml",
    "_dc.xml",
    "_meta.sqlite",
    "_archive.torrent",
)
_REMOVAL_ACCEPTED = re.compile(
    r"\bItem\s*:\s*'([A-Za-z0-9][A-Za-z0-9_.-]*)'\s*"
    r'queued\s+for\s+"make_dark"\s+operation\s*-\s*task\s+ID\s*:\s*'
    r"([0-9]+)\b"
)


def raise_for_item_status(response: ResponseLike) -> None:
    """Reject redirects and HTTP failures without echoing secret-bearing HTML."""
    status = response.status_code
    if HTTPStatus.OK <= status < HTTPStatus.MULTIPLE_CHOICES:
        return
    message = f"Internet Archive item request returned HTTP {status}"
    if status in {HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN}:
        raise AuthenticationError(message, service=_SERVICE, status_code=status)
    if status == HTTPStatus.TOO_MANY_REQUESTS:
        retry_after, raw = parse_retry_after(response.headers)
        raise RateLimitError(
            message,
            service=_SERVICE,
            status_code=status,
            retry_after=retry_after,
            retry_after_raw=raw,
        )
    raise InvalidServiceResponseError(message, service=_SERVICE, status_code=status)


@dataclass(frozen=True, slots=True)
class PreparedFile:
    """A sized iterable body that streams only the validated remaining bytes.

    Pass this object directly to transport so its length prevents chunked
    encoding and its iterator bounds reads even for non-iterable sources.
    """

    name: str
    size: int
    stream: BinaryIO = field(repr=False)

    def __len__(self) -> int:
        """Return the prepared byte count for the transport's Content-Length."""
        return self.size

    def __iter__(self) -> Iterator[bytes]:
        """Yield exactly the prepared size in at most 64 KiB reads, without closing."""
        remaining = self.size
        while remaining:
            requested = min(65536, remaining)
            chunk = self.stream.read(requested)
            if not isinstance(chunk, bytes) or not chunk or len(chunk) > requested:
                raise OSError(
                    "upload source returned invalid or incomplete binary data"
                )
            remaining -= len(chunk)
            yield chunk


async def _source_io(
    function: Callable[_P, _T], *args: _P.args, **kwargs: _P.kwargs
) -> _T:
    """Run source I/O without leaving a worker using handles after cancellation."""
    worker = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(worker)
    except asyncio.CancelledError:
        # Cancellation waits for the in-flight, uninterruptible operation to finish.
        # Repeated cancellation must not let cleanup or the caller race the worker.
        while not worker.done():
            with suppress(BaseException):
                await asyncio.shield(worker)
        with suppress(BaseException):
            worker.result()
        raise


@dataclass(frozen=True, slots=True)
class AsyncPreparedFile:
    """Keep a sized async body separate from the synchronous transport body."""

    file: PreparedFile

    def __len__(self) -> int:
        """Preserve Content-Length rather than enabling chunked encoding."""
        return len(self.file)

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """Read each bounded chunk off the event loop, without closing the source."""
        iterator = iter(self.file)
        while (chunk := await _source_io(lambda: next(iterator, None))) is not None:
            yield chunk


@asynccontextmanager
async def async_prepare_files(
    files: Path | Iterable[InternetArchiveUploadFile | str | Path], identifier: str
) -> AsyncIterator[tuple[PreparedFile, ...]]:
    """Prepare sources and close owned handles off-loop, after active I/O finishes."""
    stack = ExitStack()
    try:
        yield await _source_io(prepare_files, files, identifier, stack)
    except BaseException:
        # Preserve the original failure, including cancellation during cleanup.
        with suppress(BaseException):
            await _source_io(stack.close)
        raise
    else:
        await _source_io(stack.close)


def prepare_files(
    files: Path | Iterable[InternetArchiveUploadFile | str | Path],
    identifier: str,
    stack: ExitStack,
) -> tuple[PreparedFile, ...]:
    """Open and validate the entire batch before the caller starts transport.

    A single Path is treated as a one-file batch.
    Only path handles are registered with ``stack``. Caller-owned streams remain
    open and at their original position, including when validation fails. Sizes
    cover remaining bytes, and probing reads at most one byte per stream.
    """
    _validate_identifier(identifier)
    if isinstance(files, Path):
        files = (files,)
    if isinstance(files, (str, bytes)):
        raise InvalidOptionError(
            "files must be a Path or an iterable of upload files or paths"
        )
    try:
        iterator = iter(files)
    except TypeError:
        raise InvalidOptionError(
            "files must be a Path or an iterable of upload files or paths"
        ) from None
    prepared: list[PreparedFile] = []
    names: set[str] = set()
    stream_ids: set[int] = set()
    generated = {identifier + suffix for suffix in _GENERATED_SUFFIXES}
    for value in iterator:
        if not isinstance(value, (InternetArchiveUploadFile, str, Path)):
            raise InvalidOptionError("files must contain upload files or paths")
        upload = (
            value
            if isinstance(value, InternetArchiveUploadFile)
            else InternetArchiveUploadFile(value)
        )
        name = upload.name
        assert name is not None  # Normalized by InternetArchiveUploadFile.
        if name in names:
            raise InvalidOptionError("duplicate upload file names are not allowed")
        if name in generated:
            raise InvalidOptionError(
                "upload file name is reserved for generated item files"
            )
        names.add(name)
        source = upload.source
        if isinstance(source, (str, Path)):
            try:
                # The caller's stack keeps path handles open through transport.
                stream = stack.enter_context(Path(source).open("rb"))  # noqa: SIM115
            except (OSError, ValueError):
                raise InvalidOptionError(
                    "upload source path could not be opened"
                ) from None
        else:
            stream = source
        if id(stream) in stream_ids:
            raise InvalidOptionError("duplicate upload source streams are not allowed")
        stream_ids.add(id(stream))
        prepared.append(PreparedFile(name, _remaining_size(stream), stream))
    if not prepared:
        raise InvalidOptionError("at least one upload file is required")
    return tuple(prepared)


def _remaining_size(stream: BinaryIO) -> int:
    """Probe a seekable binary stream and restore its original offset."""
    try:
        if not stream.readable() or not stream.seekable():
            raise InvalidOptionError("upload streams must be readable and seekable")
        offset = stream.tell()
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise InvalidOptionError("upload stream returned an invalid position")
        try:
            probe = stream.read(1)
            if not isinstance(probe, bytes):
                raise InvalidOptionError("upload streams must be binary")
            stream.seek(0, SEEK_END)
            end = stream.tell()
            if not isinstance(end, int) or isinstance(end, bool) or end < offset:
                raise InvalidOptionError("upload stream returned an invalid size")
        finally:
            stream.seek(offset)
    except (AttributeError, OSError, TypeError, ValueError) as exc:
        if isinstance(exc, InvalidOptionError):
            raise
        raise InvalidOptionError(
            "upload source must be a readable seekable binary stream"
        ) from None
    size = end - offset
    if size == 0 or not probe:
        raise InvalidOptionError("zero-byte upload files are not allowed")
    return size


def upload_headers(
    options: InternetArchiveUploadOptions, total_size: int
) -> dict[str, str]:
    """Encode uploader metadata as URI-wrapped values, numbering repeated keys."""
    if (
        not isinstance(total_size, int)
        or isinstance(total_size, bool)
        or total_size <= 0
    ):
        raise InvalidOptionError("total_size must be a positive integer")
    headers = {
        "x-amz-acl": "bucket-owner-full-control",
        "x-archive-size-hint": str(total_size),
        "x-archive-interactive-priority": "1",
        "Content-Type": "multipart/form-data; charset=UTF-8",
    }
    metadata: dict[str, str | list[str]] = {
        "title": options.title,
        "description": options.description,
        "subject": _text_values(options.subjects, "subjects"),
        "mediatype": options.media_type,
    }
    metadata.update(
        {
            key: value
            for key, value in (
                ("collection", options.collection),
                ("creator", options.creator),
                (
                    "date",
                    options.date.isoformat() if options.date is not None else None,
                ),
                ("language", options.language),
                ("licenseurl", options.license),
            )
            if value is not None
        }
    )
    metadata.update(_normalize_metadata(options.metadata))
    for key, value in metadata.items():
        encoded_key = key.replace("_", "--")
        if isinstance(value, str):
            headers[f"x-archive-meta-{encoded_key}"] = f"uri({quote(value, safe='')})"
        else:
            width = max(2, len(str(len(value))))
            for index, entry in enumerate(value, start=1):
                headers[f"x-archive-meta{index:0{width}d}-{encoded_key}"] = (
                    f"uri({quote(entry, safe='')})"
                )
    return headers


class _ItemHTMLParser(HTMLParser):
    """Collect visible response text and hidden uploader bootstrap attributes."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.args: list[str | None] = []
        self.text: list[str] = []
        self.login_form = False
        self._ignored_tag: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"script", "style"}:
            self._ignored_tag = tag
        if self._ignored_tag is not None:
            return
        if tag in {"br", "p", "div", "li", "tr"}:
            self.text.append(" ")
        attributes = dict(attrs)
        if tag == "input":
            input_type = (attributes.get("type") or "").lower()
            if input_type == "password":
                self.login_form = True
            classes = (attributes.get("class") or "").split()
            if input_type == "hidden" and "js-uploader-args" in classes:
                self.args.append(attributes.get("value"))

    def handle_endtag(self, tag: str) -> None:
        if tag == self._ignored_tag:
            self._ignored_tag = None
        if self._ignored_tag is None and tag in {"p", "div", "li", "tr"}:
            self.text.append(" ")

    def handle_data(self, data: str) -> None:
        if self._ignored_tag is None:
            self.text.append(data)


def _parse_html(html: str) -> _ItemHTMLParser:
    """Parse item HTML and reject explicit logged-out responses safely."""
    if not isinstance(html, str):
        raise InvalidServiceResponseError(
            "Internet Archive returned invalid item HTML", service=_SERVICE
        )
    parser = _ItemHTMLParser()
    try:
        parser.feed(html)
        parser.close()
    except (AssertionError, ValueError):
        raise InvalidServiceResponseError(
            "Internet Archive returned invalid item HTML", service=_SERVICE
        ) from None
    text = " ".join(" ".join(parser.text).split()).casefold()
    if "you must be logged in" in text or parser.login_form:
        raise AuthenticationError(
            "Internet Archive item operations require login", service=_SERVICE
        )
    return parser


def parse_upload_key(html: str) -> InternetArchiveApiKey:
    """Extract LOW credentials from the hidden HTML-escaped uploader JSON.

    Malformed responses never appear in exception messages or chained decoder
    errors, since the document can contain account secrets.
    """
    parser = _parse_html(html)
    try:
        if len(parser.args) != 1 or parser.args[0] is None:
            raise ValueError
        data = json.loads(parser.args[0])
        if not isinstance(data, dict) or not isinstance(data.get("s3user"), dict):
            raise ValueError
        user = data["s3user"]
        access = _validate_text(user.get("s3accesskey"), "access key")
        secret = _validate_text(user.get("s3secretkey"), "secret key")
        if any(character.isspace() for character in access + secret):
            raise ValueError
    except (ValueError, RecursionError):
        raise InvalidServiceResponseError(
            "Internet Archive returned invalid uploader credentials", service=_SERVICE
        ) from None
    return InternetArchiveApiKey(access, secret)


def validate_availability(data: Mapping[str, object], identifier: str) -> None:
    """Require explicit success for the exact requested upload identifier."""
    _validate_identifier(identifier)
    if isinstance(data, Mapping) and data.get("success") is False:
        raise InvalidOptionError("Internet Archive item identifier is unavailable")
    if (
        not isinstance(data, Mapping)
        or data.get("success") is not True
        or data.get("identifier") != identifier
    ):
        raise InvalidServiceResponseError(
            "Internet Archive returned invalid identifier availability",
            service=_SERVICE,
        )


def parse_catalog(data: Mapping[str, object]) -> bool:
    """Parse GET upload_api.php name=catalogRows responses.

    True means no archive.php ingest task remains. It does not establish that
    derivatives have finished or that the item is publicly visible.
    """
    if isinstance(data, Mapping) and data.get("success") is False:
        raise ServiceError("Internet Archive catalog query failed", service=_SERVICE)
    if not isinstance(data, Mapping) or data.get("success") is not True:
        raise InvalidServiceResponseError(
            "Internet Archive returned invalid catalog status", service=_SERVICE
        )
    rows = data.get("rows")
    if not isinstance(rows, list):
        raise InvalidServiceResponseError(
            "Internet Archive returned invalid catalog rows", service=_SERVICE
        )
    pending = False
    blocked = False
    for row in rows:
        if not isinstance(row, Mapping):
            raise InvalidServiceResponseError(
                "Internet Archive returned an invalid catalog row", service=_SERVICE
            )
        wait_admin = row.get("wait_admin")
        try:
            command = _validate_text(row.get("cmd"), "catalog command")
            if isinstance(wait_admin, str) and re.fullmatch(r"[0-9]+", wait_admin):
                wait_admin = int(wait_admin)
        except ValueError:
            raise InvalidServiceResponseError(
                "Internet Archive returned an invalid catalog row", service=_SERVICE
            ) from None
        if (
            command != command.strip()
            or not isinstance(wait_admin, int)
            or isinstance(wait_admin, bool)
            or wait_admin < 0
        ):
            raise InvalidServiceResponseError(
                "Internet Archive returned an invalid catalog row", service=_SERVICE
            )
        pending = pending or command == "archive.php"
        blocked = blocked or (command == "archive.php" and wait_admin == _ADMIN_BLOCKED)
    if blocked:
        raise ServiceError(
            "Internet Archive ingest requires administrator intervention",
            service=_SERVICE,
        )
    return not pending


def removal_form(ids: Iterable[str], comment: str) -> dict[str, str]:
    """Validate an explicit item batch and build the observed make_dark form."""
    if isinstance(ids, (str, bytes)):
        raise InvalidOptionError("ids must be an iterable of identifiers, not a string")
    try:
        identifiers = tuple(ids)
    except TypeError:
        raise InvalidOptionError("ids must be an iterable of identifiers") from None
    if not identifiers:
        raise InvalidOptionError("at least one removal identifier is required")
    for identifier in identifiers:
        _validate_identifier(identifier)
    if len(set(identifiers)) != len(identifiers):
        raise InvalidOptionError("removal identifiers must be unique")
    _validate_text(comment, "comment")
    return {
        "identifier": ",".join(identifiers),
        "admin": "make_dark",
        "curation[state]": "dark",
        "curation[comment]": comment,
    }


def parse_removal(
    html: str, ids: tuple[str, ...]
) -> tuple[InternetArchiveRemovalResult, ...]:
    """Report only explicit per-item make_dark queue acknowledgements.

    Missing acknowledgements in a partially accepted batch yield false results.
    No matching acknowledgement is an invalid response, never implied success.
    """
    parser = _parse_html(html)
    accepted: dict[str, str] = {}
    requested = set(ids)
    for match in _REMOVAL_ACCEPTED.finditer(" ".join(parser.text)):
        identifier, task_id = match.groups()
        if identifier in requested and task_id.strip("0"):
            if identifier in accepted and accepted[identifier] != task_id:
                raise InvalidServiceResponseError(
                    "Internet Archive returned conflicting removal tasks",
                    service=_SERVICE,
                )
            accepted[identifier] = task_id
    if not accepted:
        raise InvalidServiceResponseError(
            "Internet Archive did not acknowledge item removal", service=_SERVICE
        )
    return tuple(
        InternetArchiveRemovalResult(
            identifier, identifier in accepted, accepted.get(identifier)
        )
        for identifier in ids
    )
