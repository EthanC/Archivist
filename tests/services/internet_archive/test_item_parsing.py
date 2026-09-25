"""Exercise item preparation and protocol parsing without live network access."""

from __future__ import annotations

import json
from contextlib import ExitStack
from dataclasses import FrozenInstanceError
from datetime import date
from html import escape
from io import SEEK_END, BytesIO, StringIO
from pathlib import Path
from traceback import format_exception
from typing import Any, BinaryIO, cast
from unittest.mock import Mock, call
from urllib.parse import quote

import pytest
from niquests import Request

from archivist.core.errors import (
    AuthenticationError,
    InvalidOptionError,
    InvalidServiceResponseError,
    ServiceError,
)
from archivist.services.internet_archive import _items
from archivist.services.internet_archive.item_models import (
    InternetArchiveRemovalResult,
    InternetArchiveUploadFile,
    InternetArchiveUploadOptions,
)


def _options(**overrides: object) -> InternetArchiveUploadOptions:
    values: dict[str, object] = {
        "identifier": "item-1",
        "title": "Title",
        "description": "Description",
        "subjects": ["tag", "tag"],
    }
    values.update(overrides)
    return cast("Any", InternetArchiveUploadOptions)(**values)


def _bootstrap(payload: object) -> str:
    value = escape(json.dumps(payload), quote=True)
    return f'<input type="hidden" class="other js-uploader-args" value="{value}">'


def _ack(identifier: str = "item-1", task_id: str = "123") -> str:
    return (
        f"Item: '{identifier}' queued for \"make_dark\" operation - task ID: {task_id}"
    )


class _OneByteProbe(BytesIO):
    """Fail immediately if preparation requests more than a one-byte probe."""

    def __init__(self, data: bytes) -> None:
        super().__init__(data)
        self.read_sizes: list[int | None] = []

    def read(self, size: int | None = -1, /) -> bytes:
        self.read_sizes.append(size)
        assert size == 1
        return super().read(size)


def test_item_endpoint_constants_match_the_observed_protocol() -> None:
    """Keep upload, S3, catalog, and management requests on their exact endpoints."""
    assert _items.UPLOAD_URL == "https://archive.org/upload"
    assert _items.UPLOAD_API_URL == "https://archive.org/upload/app/upload_api.php"
    assert _items.S3_URL == "https://s3.us.archive.org"
    assert _items.MANAGE_URL == "https://archive.org/manage/"


def test_prepare_files_opens_all_paths_and_preserves_borrowed_streams(
    tmp_path: Path,
) -> None:
    """Prepare the whole batch, retaining offsets and closing only owned handles."""
    first = tmp_path / "first.bin"
    second = tmp_path / "second.bin"
    first.write_bytes(b"first")
    second.write_bytes(b"second")
    with _OneByteProbe(b"prefix-body") as borrowed:
        borrowed.seek(len(b"prefix-"))
        inputs = (
            value
            for value in (
                str(first),
                second,
                InternetArchiveUploadFile(borrowed, "sub/\u65e5\u672c.bin"),
            )
        )
        with ExitStack() as stack:
            prepared = _items.prepare_files(inputs, "item-1", stack)
            assert tuple((item.name, item.size) for item in prepared) == (
                ("first.bin", len(b"first")),
                ("second.bin", len(b"second")),
                ("sub/\u65e5\u672c.bin", len(b"body")),
            )
            assert all(not item.stream.closed for item in prepared)
            assert prepared[0].stream.read() == b"first"
            assert prepared[1].stream.read() == b"second"
            assert prepared[2].stream is borrowed
            assert borrowed.tell() == len(b"prefix-")
            assert borrowed.read_sizes == [1]
            assert not hasattr(prepared[2], "__dict__")
            assert "stream=" not in repr(prepared[2])
            with pytest.raises(FrozenInstanceError):
                cast("Any", prepared[2]).name = "changed"
        assert prepared[0].stream.closed and prepared[1].stream.closed
        assert not borrowed.closed
        assert borrowed.tell() == len(b"prefix-")


@pytest.mark.parametrize(
    "batch",
    [
        None,
        1,
        "file.bin",
        b"file.bin",
        InternetArchiveUploadFile(Path("file.bin")),
        [],
        [None],
        [1],
        [BytesIO(b"x")],
    ],
)
def test_prepare_files_rejects_invalid_batches(batch: object) -> None:
    """Reject invalid batches, including bare strings and upload descriptors."""
    with ExitStack() as stack, pytest.raises(InvalidOptionError):
        _items.prepare_files(cast("Any", batch), "item-1", stack)


def test_prepare_files_validates_identifier_before_opening_files() -> None:
    """Reject malformed item identifiers before looking up any local paths."""
    with ExitStack() as stack, pytest.raises(InvalidOptionError, match="identifier"):
        _items.prepare_files(["missing.bin"], "../bad", stack)


def test_duplicate_names_are_rejected_without_closing_borrowed_streams() -> None:
    """Reject duplicate item names even when the input sources differ."""
    with BytesIO(b"one") as first, BytesIO(b"two") as second:
        with ExitStack() as stack, pytest.raises(InvalidOptionError, match="duplicate"):
            _items.prepare_files(
                [
                    InternetArchiveUploadFile(first, "same"),
                    InternetArchiveUploadFile(second, "same"),
                ],
                "item-1",
                stack,
            )
        assert not first.closed and not second.closed
        assert first.tell() == second.tell() == 0


def test_duplicate_stream_identity_is_rejected_before_a_second_probe() -> None:
    """Reject one source under different item names without closing or consuming it."""
    with _OneByteProbe(b"payload") as source:
        source.seek(1)
        with (
            ExitStack() as stack,
            pytest.raises(InvalidOptionError, match=r"duplicate.*streams"),
        ):
            _items.prepare_files(
                [
                    InternetArchiveUploadFile(source, "one"),
                    InternetArchiveUploadFile(source, "two"),
                ],
                "item-1",
                stack,
            )
        assert not source.closed and source.tell() == 1
        assert source.read_sizes == [1]


def test_separate_path_handles_can_upload_the_same_local_file(tmp_path: Path) -> None:
    """Allow one local path under distinct names with independent handles."""
    path = tmp_path / "source.bin"
    path.write_bytes(b"payload")
    with ExitStack() as stack:
        first, second = _items.prepare_files(
            [
                InternetArchiveUploadFile(path, "one"),
                InternetArchiveUploadFile(path, "two"),
            ],
            "item-1",
            stack,
        )
        assert first.stream is not second.stream
        assert b"".join(first) == b"payload"
        assert second.stream.tell() == 0
        assert b"".join(second) == b"payload"
    assert first.stream.closed and second.stream.closed


@pytest.mark.parametrize(
    "suffix", ["_meta.xml", "_files.xml", "_dc.xml", "_meta.sqlite", "_archive.torrent"]
)
def test_generated_names_are_reserved_only_at_the_item_root(suffix: str) -> None:
    """Protect generated item files without forbidding matching names in subpaths."""
    with BytesIO(b"x") as stream, ExitStack() as stack:
        with pytest.raises(InvalidOptionError, match="reserved"):
            _items.prepare_files(
                [InternetArchiveUploadFile(stream, "item-1" + suffix)], "item-1", stack
            )
        files = _items.prepare_files(
            [InternetArchiveUploadFile(stream, "sub/item-1" + suffix)], "item-1", stack
        )
        assert files[0].size == 1
        assert not stream.closed and stream.tell() == 0


@pytest.mark.parametrize(
    "failure", ["missing", "directory", "empty", "invalid-later-entry"]
)
def test_owned_handles_close_after_late_batch_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    """Close every earlier path handle through the caller stack on batch failure."""
    first = tmp_path / "first.bin"
    empty = tmp_path / "empty.bin"
    first.write_bytes(b"first")
    empty.write_bytes(b"")
    original_open = Path.open
    handles: list[BinaryIO] = []

    def tracked_open(path: Path, mode: str) -> BinaryIO:
        stream = cast("BinaryIO", original_open(path, mode))
        handles.append(stream)
        return stream

    monkeypatch.setattr(Path, "open", tracked_open)
    later = {
        "missing": tmp_path / "missing.bin",
        "directory": tmp_path,
        "empty": empty,
        "invalid-later-entry": object(),
    }[failure]
    with BytesIO(b"borrowed") as borrowed:
        with pytest.raises(InvalidOptionError), ExitStack() as stack:
            _items.prepare_files(
                cast(
                    "Any",
                    [first, InternetArchiveUploadFile(borrowed, "borrowed"), later],
                ),
                "item-1",
                stack,
            )
        assert handles and all(stream.closed for stream in handles)
        assert not borrowed.closed and borrowed.tell() == 0


def test_path_open_value_errors_are_safe_option_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Wrap invalid path errors without exposing the local path or exception text."""
    monkeypatch.setattr(Path, "open", Mock(side_effect=ValueError("private path")))
    path = "private.bin"
    with ExitStack() as stack, pytest.raises(InvalidOptionError) as failure:
        _items.prepare_files([path], "item-1", stack)
    assert "private" not in "".join(format_exception(failure.value))


@pytest.mark.parametrize("offset", [0, 3, 4])
def test_empty_remaining_streams_are_rejected_without_moving_them(offset: int) -> None:
    """Reject empty streams and offsets at or beyond EOF without moving them."""
    data = b"" if offset == 0 else b"abc"
    with BytesIO(data) as stream, ExitStack() as stack:
        stream.seek(offset)
        with pytest.raises(InvalidOptionError):
            _items.prepare_files(
                [InternetArchiveUploadFile(stream, "empty")], "item-1", stack
            )
        assert stream.tell() == offset and not stream.closed


def test_text_streams_and_closed_streams_are_rejected() -> None:
    """Require open binary streams and restore text-stream positions on rejection."""
    with StringIO("text") as text, ExitStack() as stack:
        text.seek(1)
        with pytest.raises(InvalidOptionError, match="binary"):
            _items.prepare_files(
                [InternetArchiveUploadFile(cast("Any", text), "text")], "item-1", stack
            )
        assert text.tell() == 1 and not text.closed
    closed = BytesIO(b"x")
    closed.close()
    with ExitStack() as stack, pytest.raises(InvalidOptionError):
        _items.prepare_files(
            [InternetArchiveUploadFile(closed, "closed")], "item-1", stack
        )


@pytest.mark.parametrize(
    ("method", "value"),
    [
        ("readable", False),
        ("seekable", False),
        ("tell", None),
        ("tell", True),
        ("tell", -1),
        ("read", None),
        ("read", "text"),
        ("read", bytearray(b"x")),
        ("read", b""),
    ],
)
def test_stream_protocol_runtime_values_are_validated(
    method: str, value: object
) -> None:
    """Reject misleading stream capabilities, invalid offsets, and nonbinary probes."""
    with BytesIO(b"payload") as source, ExitStack() as stack:
        source.seek(1)
        stream = Mock(wraps=source)
        getattr(stream, method).return_value = value
        with pytest.raises(InvalidOptionError):
            _items.prepare_files(
                [InternetArchiveUploadFile(cast("BinaryIO", stream), "file")],
                "item-1",
                stack,
            )
        assert source.tell() == 1 and not source.closed
        stream.close.assert_not_called()


@pytest.mark.parametrize("end", [None, True, -1, 0])
def test_invalid_stream_end_positions_restore_the_initial_offset(end: object) -> None:
    """Validate end positions independently of the initial tell result."""
    with BytesIO(b"payload") as source, ExitStack() as stack:
        source.seek(1)
        stream = Mock(wraps=source)
        stream.tell.side_effect = [1, end]
        with pytest.raises(InvalidOptionError, match="size"):
            _items.prepare_files(
                [InternetArchiveUploadFile(cast("BinaryIO", stream), "file")],
                "item-1",
                stack,
            )
        assert source.tell() == 1 and not source.closed


@pytest.mark.parametrize("error_type", [AttributeError, OSError, TypeError, ValueError])
def test_probe_failures_restore_offsets_and_hide_exception_text(
    error_type: type[Exception],
) -> None:
    """Restore the current offset even when a bounded read raises an IO failure."""
    with BytesIO(b"payload") as source, ExitStack() as stack:
        source.seek(1)
        stream = Mock(wraps=source)
        stream.read.side_effect = error_type("private stream details")
        with pytest.raises(InvalidOptionError) as failure:
            _items.prepare_files(
                [InternetArchiveUploadFile(cast("BinaryIO", stream), "file")],
                "item-1",
                stack,
            )
        stream.read.assert_called_once_with(1)
        assert source.tell() == 1 and not source.closed
        assert "private" not in "".join(format_exception(failure.value))


def test_non_stream_objects_are_rejected_during_preparation() -> None:
    """Turn missing binary stream methods into a package validation error."""
    with ExitStack() as stack, pytest.raises(InvalidOptionError):
        _items.prepare_files(
            [InternetArchiveUploadFile(cast("Any", object()), "file")], "item-1", stack
        )


@pytest.mark.parametrize("stage", ["end", "restore"])
def test_stream_seek_failures_are_safe_and_do_not_close_borrowed_handles(
    stage: str,
) -> None:
    """Wrap end-seek and restoration failures without taking stream ownership."""
    with BytesIO(b"payload") as source, ExitStack() as stack:
        source.seek(1)
        stream = Mock(wraps=source)

        def seek(offset: int, whence: int = 0) -> int:
            if (stage == "end" and whence == SEEK_END) or (
                stage == "restore" and whence == 0
            ):
                raise OSError("private seek failure")
            return source.seek(offset, whence)

        stream.seek.side_effect = seek
        with pytest.raises(InvalidOptionError) as failure:
            _items.prepare_files(
                [InternetArchiveUploadFile(cast("BinaryIO", stream), "file")],
                "item-1",
                stack,
            )
        stream.seek.assert_called_with(1)
        stream.close.assert_not_called()
        assert not source.closed
        if stage == "end":
            assert source.tell() == 1
        assert "private" not in "".join(format_exception(failure.value))


def test_cancellation_closes_owned_handles_but_not_borrowed_streams(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preserve cancellation while the caller stack cleans up already-open paths."""
    with BytesIO(b"owned") as owned, BytesIO(b"borrowed") as borrowed:
        monkeypatch.setattr(Path, "open", Mock(return_value=owned))
        monkeypatch.setattr(owned, "read", Mock(side_effect=KeyboardInterrupt))
        with pytest.raises(KeyboardInterrupt), ExitStack() as stack:
            _items.prepare_files(
                [InternetArchiveUploadFile(borrowed, "borrowed"), "owned.bin"],
                "item-1",
                stack,
            )
        assert owned.closed
        assert not borrowed.closed and borrowed.tell() == 0


@pytest.mark.parametrize("explicit_length", [False, True])
def test_prepared_body_streams_noniterable_sources_with_a_known_length(
    explicit_length: bool,
) -> None:
    """Prepare niquests bodies without chunked encoding or reads beyond the snapshot."""
    payload = b"x" * (65536 * 2 + 17)
    prefix = b"prefix"
    with BytesIO(prefix + payload) as source, ExitStack() as stack:
        source.seek(len(prefix))
        stream = Mock(
            spec=["read", "seek", "tell", "readable", "seekable", "close"], wraps=source
        )
        with pytest.raises(TypeError):
            iter(stream)
        body = _items.prepare_files(
            [InternetArchiveUploadFile(cast("BinaryIO", stream), "file")],
            "item-1",
            stack,
        )[0]
        assert len(body) == len(payload)
        assert not hasattr(body, "read")
        source.seek(0, SEEK_END)
        source.write(b"later bytes")
        source.seek(len(prefix))
        headers = {"Content-Length": str(len(payload))} if explicit_length else {}
        request = Request(
            "PUT", "https://example.invalid/file", data=body, headers=headers
        ).prepare()
        assert request.body is body
        assert request.headers is not None
        assert request.headers["Content-Length"] == str(len(payload))
        assert "Transfer-Encoding" not in request.headers
        stream.read.assert_called_once_with(1)
        assert b"".join(body) == payload
        assert stream.read.call_args_list == [
            call(1),
            call(65536),
            call(65536),
            call(17),
        ]
        assert len(body) == len(payload)
        assert source.read() == b"later bytes"
        stream.close.assert_not_called()
        assert not source.closed


def test_prepared_body_accepts_partial_reads_until_the_exact_size_is_reached() -> None:
    """Accumulate nonempty bounded reads rather than mistaking them for EOF."""
    with BytesIO(b"abc") as source, ExitStack() as stack:
        stream = Mock(wraps=source)
        body = _items.prepare_files(
            [InternetArchiveUploadFile(cast("BinaryIO", stream), "file")],
            "item-1",
            stack,
        )[0]
        stream.read.reset_mock()
        stream.read.side_effect = [b"a", b"bc"]
        assert list(body) == [b"a", b"bc"]
        assert stream.read.call_args_list == [call(3), call(2)]
        stream.close.assert_not_called()


@pytest.mark.parametrize(
    "chunk", [None, "private text", bytearray(b"abc"), memoryview(b"abc"), b"", b"abcd"]
)
def test_prepared_body_rejects_empty_nonbinary_and_oversized_reads(
    chunk: object,
) -> None:
    """Reject invalid transport reads with an OSError and retain source ownership."""
    with BytesIO(b"abc") as source, ExitStack() as stack:
        stream = Mock(wraps=source)
        body = _items.prepare_files(
            [InternetArchiveUploadFile(cast("BinaryIO", stream), "file")],
            "item-1",
            stack,
        )[0]
        stream.read.return_value = chunk
        with pytest.raises(OSError, match="invalid or incomplete") as failure:
            list(body)
        assert "private" not in str(failure.value)
        stream.read.assert_called_with(len(b"abc"))
        stream.close.assert_not_called()
        assert not source.closed


def test_prepared_body_detects_truncation_after_preparation() -> None:
    """Raise on early EOF even after yielding valid bytes from a shortened source."""
    with BytesIO(b"original") as source, ExitStack() as stack:
        body = _items.prepare_files(
            [InternetArchiveUploadFile(source, "file")], "item-1", stack
        )[0]
        source.truncate(1)
        chunks = iter(body)
        assert next(chunks) == b"o"
        with pytest.raises(OSError, match="incomplete"):
            next(chunks)
        assert not source.closed


@pytest.mark.parametrize("error_type", [OSError, KeyboardInterrupt])
def test_prepared_body_propagates_read_failures_without_closing_the_source(
    error_type: type[BaseException],
) -> None:
    """Leave read error translation to transport and preserve caller-owned handles."""
    with BytesIO(b"abc") as source:
        stream = Mock(wraps=source)
        error = error_type("read failed")
        stream.read.side_effect = error
        body = _items.PreparedFile("file", len(b"abc"), cast("BinaryIO", stream))
        with pytest.raises(error_type) as failure:
            next(iter(body))
        assert failure.value is error
        stream.close.assert_not_called()
        assert not source.closed


def test_closing_a_partially_consumed_body_does_not_close_its_source() -> None:
    """Leave a borrowed source open if transport stops before consuming the body."""
    with BytesIO(b"x" * (65536 + 1)) as source:
        body = _items.PreparedFile("file", len(source.getvalue()), source)
        chunks = iter(body)
        assert next(chunks) == b"x" * 65536
        cast("Any", chunks).close()
        assert not source.closed
        assert source.read() == b"x"


def test_upload_headers_have_exact_defaults_and_one_based_repeated_fields() -> None:
    """Serialize defaults without requesting automatic creation of existing buckets."""
    assert _items.upload_headers(_options(), 12) == {
        "x-amz-acl": "bucket-owner-full-control",
        "x-archive-size-hint": "12",
        "x-archive-interactive-priority": "1",
        "Content-Type": "multipart/form-data; charset=UTF-8",
        "x-archive-meta-title": "uri(Title)",
        "x-archive-meta-description": "uri(Description)",
        "x-archive-meta01-subject": "uri(tag)",
        "x-archive-meta02-subject": "uri(tag)",
        "x-archive-meta-mediatype": "uri(data)",
        "x-archive-meta-collection": "uri(opensource_media)",
    }


def test_upload_headers_encode_every_metadata_value_and_escape_underscores() -> None:
    """Encode Unicode, markup, multiline values, and repeated strings without loss."""
    scalar = " Caf\u00e9 / ?#&='\" "
    multiline = "<p>\u65e5\u672c</p>\n\tline two"
    options = _options(
        title=scalar,
        description=multiline,
        subjects=[scalar, scalar],
        creator=scalar,
        date=date(2026, 9, 25),
        language=scalar,
        license=scalar,
        collection=scalar,
        metadata={"custom_key": [multiline, multiline], "source": scalar},
    )
    headers = _items.upload_headers(options, 123)
    encoded = f"uri({quote(scalar, safe='')})"
    for key in (
        "title",
        "creator",
        "language",
        "licenseurl",
        "collection",
        "source",
    ):
        assert headers[f"x-archive-meta-{key}"] == encoded
    assert (
        headers["x-archive-meta01-subject"]
        == headers["x-archive-meta02-subject"]
        == encoded
    )
    assert headers["x-archive-meta-description"] == f"uri({quote(multiline, safe='')})"
    assert headers["x-archive-meta-date"] == "uri(2026-09-25)"
    assert (
        headers["x-archive-meta01-custom--key"]
        == headers["x-archive-meta02-custom--key"]
        == f"uri({quote(multiline, safe='')})"
    )
    assert all(
        value.isascii() and "\n" not in value and "\t" not in value
        for value in headers.values()
    )
    assert "x-amz-auto-make-bucket" not in headers


@pytest.mark.parametrize(
    ("count", "width"),
    [(1, 2), (9, 2), (10, 2), (99, 2), (100, 3), (101, 3), (1000, 4)],
)
def test_repeated_header_numbers_preserve_lexical_order(count: int, width: int) -> None:
    """Use one consistent width per metadata list, including 100 or more values."""
    values = [f"value-{index}" for index in range(count)]
    options = _options(
        subjects=values,
        metadata={"repeated_key": values, "short_key": ["one", "two"]},
    )
    headers = _items.upload_headers(options, 1)
    for field_name in ("subject", "repeated--key"):
        names = sorted(name for name in headers if name.endswith(f"-{field_name}"))
        assert names[0] == f"x-archive-meta{1:0{width}d}-{field_name}"
        assert names[-1] == f"x-archive-meta{count:0{width}d}-{field_name}"
        assert [headers[name] for name in names] == [
            f"uri({value})" for value in values
        ]
    assert headers["x-archive-meta01-short--key"] == "uri(one)"
    assert headers["x-archive-meta02-short--key"] == "uri(two)"


def test_valid_custom_keys_round_trip_through_metadata_headers() -> None:
    """Encode both ordinary underscores and underscore-hyphen combinations exactly."""
    headers = _items.upload_headers(_options(metadata={"a_b": "one", "a_-b": "two"}), 1)
    assert headers["x-archive-meta-a--b"] == "uri(one)"
    assert headers["x-archive-meta-a---b"] == "uri(two)"


def test_upload_headers_use_edits_to_the_subjects_list() -> None:
    """Serialize an edited list without losing repeated values or order."""
    options = _options(subjects=["first"])
    options.subjects.extend(["second", "first"])
    headers = _items.upload_headers(options, 1)
    assert headers["x-archive-meta01-subject"] == "uri(first)"
    assert headers["x-archive-meta02-subject"] == "uri(second)"
    assert headers["x-archive-meta03-subject"] == "uri(first)"


@pytest.mark.parametrize("subjects", [[], [""], ["x\n"], [1]])
def test_upload_headers_revalidate_edited_subjects(subjects: list[object]) -> None:
    """Reject invalid list edits before headers reach the transport."""
    options = _options()
    options.subjects[:] = cast("Any", subjects)
    with pytest.raises(InvalidOptionError):
        _items.upload_headers(options, 1)


def test_upload_headers_use_edits_to_metadata_lists() -> None:
    """Serialize edited custom values in order, including repeated strings."""
    options = _options(metadata={"custom_key": ["first"]})
    values = options.metadata["custom_key"]
    assert isinstance(values, list)
    values.extend(["second", "first"])
    headers = _items.upload_headers(options, 1)
    assert headers["x-archive-meta01-custom--key"] == "uri(first)"
    assert headers["x-archive-meta02-custom--key"] == "uri(second)"
    assert headers["x-archive-meta03-custom--key"] == "uri(first)"


@pytest.mark.parametrize("values", [[], [""], ["x\x00"], [1]])
def test_upload_headers_revalidate_edited_metadata(values: list[object]) -> None:
    """Reject invalid custom list edits before headers reach the transport."""
    options = _options(metadata={"custom_key": ["first"]})
    repeated = options.metadata["custom_key"]
    assert isinstance(repeated, list)
    repeated[:] = cast("Any", values)
    with pytest.raises(InvalidOptionError):
        _items.upload_headers(options, 1)


@pytest.mark.parametrize("total_size", [None, "1", True, 0, -1, 1.5])
def test_upload_headers_reject_invalid_total_sizes(total_size: object) -> None:
    """Require a positive integer size hint rather than implicit coercion."""
    with pytest.raises(InvalidOptionError):
        _items.upload_headers(_options(), cast("Any", total_size))


def test_bootstrap_extracts_html_escaped_json_and_keeps_credentials_out_of_repr() -> (
    None
):
    """Find the class token on hidden inputs without depending on attribute order."""
    html = _bootstrap(
        {"s3user": {"s3accesskey": "dummy&access", "s3secretkey": "dummy<secret>"}}
    )
    html = (
        '<div><input><input type="text" class="js-uploader-args">'
        '<input type="hidden" class="unrelated"></div>' + html
    )
    key = _items.parse_upload_key(html)
    assert key.access_key == "dummy&access"
    assert key.secret_key == "dummy<secret>"
    assert "dummy" not in repr(key)


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        1,
        "text",
        {},
        {"s3user": None},
        {"s3user": []},
        {"s3user": {}},
        {"s3user": {"s3accesskey": "access"}},
        {"s3user": {"s3accesskey": None, "s3secretkey": "secret"}},
        {"s3user": {"s3accesskey": "", "s3secretkey": "secret"}},
        {"s3user": {"s3accesskey": "access", "s3secretkey": 1}},
        {"s3user": {"s3accesskey": "a b", "s3secretkey": "secret"}},
        {"s3user": {"s3accesskey": "access", "s3secretkey": "s ecret"}},
        {"s3user": {"s3accesskey": "access", "s3secretkey": "x\n"}},
        {"s3user": {"s3accesskey": "access", "s3secretkey": "x\ud800"}},
    ],
)
def test_bootstrap_rejects_malformed_runtime_shapes_and_keys(payload: object) -> None:
    """Require nonblank safe credentials in the documented nested JSON object."""
    with pytest.raises(InvalidServiceResponseError) as failure:
        _items.parse_upload_key(_bootstrap(payload))
    assert failure.value.service == "Internet Archive"
    assert failure.value.__cause__ is None


@pytest.mark.parametrize(
    "html",
    [
        None,
        b"html",
        [],
        "",
        "<input type=hidden class=js-uploader-args>",
        '<input type=hidden class=js-uploader-args value="private-secret{">',
        "<![bogus]>",
        '<input type=hidden class=js-uploader-args value="&#' + "9" * 5000 + ';">',
    ],
)
def test_bootstrap_rejects_missing_malformed_or_nontext_html_safely(
    html: object,
) -> None:
    """Convert HTML and JSON parser failures without leaking document contents."""
    with pytest.raises(InvalidServiceResponseError) as failure:
        _items.parse_upload_key(cast("Any", html))
    assert "private-secret" not in "".join(format_exception(failure.value))


def test_bootstrap_rejects_duplicate_inputs_and_json_recursion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject ambiguous bootstrap inputs and safely wrap JSON recursion errors."""
    html = _bootstrap({"s3user": {"s3accesskey": "access", "s3secretkey": "secret"}})
    with pytest.raises(InvalidServiceResponseError):
        _items.parse_upload_key(html + html)
    monkeypatch.setattr(
        _items.json, "loads", Mock(side_effect=RecursionError("private-secret"))
    )
    with pytest.raises(InvalidServiceResponseError) as failure:
        _items.parse_upload_key(html)
    assert "private-secret" not in "".join(format_exception(failure.value))


@pytest.mark.parametrize(
    "html",
    [
        "<p>You must be logged in</p>",
        "You <b>must</b><i>be</i> logged in",
        '<input TYPE="password">',
    ],
)
def test_item_html_parsers_recognize_logged_out_responses(html: str) -> None:
    """Raise authentication errors instead of treating login HTML as acceptance."""
    with pytest.raises(AuthenticationError):
        _items.parse_upload_key(html)
    with pytest.raises(AuthenticationError):
        _items.parse_removal(html, ("item-1",))


@pytest.mark.parametrize("tag", ["script", "style"])
def test_html_parsing_ignores_script_and_style_contents(tag: str) -> None:
    """Ignore credential-like markup, login text, and acknowledgements in raw tags."""
    valid = _bootstrap({"s3user": {"s3accesskey": "access", "s3secretkey": "secret"}})
    html = f"<{tag}>You must be logged in {_ack()} {valid}</{tag}>"
    assert _items.parse_upload_key(html + valid).access_key == "access"
    with pytest.raises(InvalidServiceResponseError):
        _items.parse_removal(html, ("item-1",))


def test_html_parser_callbacks_ignore_nested_events_while_in_raw_text() -> None:
    """Keep ignored state until its matching end tag even with nested callbacks."""
    parser = _items._ItemHTMLParser()
    parser.handle_starttag("style", [])
    parser.handle_starttag("div", [])
    parser.handle_endtag("span")
    parser.handle_data("ignored")
    parser.handle_endtag("style")
    parser.feed(
        "<div><p>visible</p><br/><li>item</li><table><tr><td>cell</td></tr></table></div>"
    )
    parser.close()
    assert "ignored" not in " ".join(parser.text)
    assert "visible" in parser.text and "item" in parser.text and "cell" in parser.text
    assert parser.args == []


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        1,
        {},
        {"success": 1, "identifier": "item-1"},
        {"success": "true", "identifier": "item-1"},
        {"success": True},
        {"success": True, "identifier": "other"},
        {"success": True, "identifier": None},
    ],
)
def test_availability_requires_exact_success_and_identifier(payload: object) -> None:
    """Reject truthy success substitutes and server-suggested alternate identifiers."""
    with pytest.raises(InvalidServiceResponseError):
        _items.validate_availability(cast("Any", payload), "item-1")


def test_availability_acceptance_conflicts_and_input_validation() -> None:
    """Require the same identifier and expose explicit conflicts as option errors."""
    assert (
        _items.validate_availability(
            {"success": True, "identifier": "item-1"}, "item-1"
        )
        is None
    )
    with pytest.raises(InvalidOptionError, match="unavailable"):
        _items.validate_availability({"success": False, "message": "private"}, "item-1")
    with pytest.raises(InvalidOptionError, match="identifier"):
        _items.validate_availability({}, "../bad")


@pytest.mark.parametrize(
    ("rows", "complete"),
    [
        ([], True),
        ([{"cmd": "derive.php", "wait_admin": 0}], True),
        ([{"cmd": "derive.php", "wait_admin": 2}], True),
        ([{"cmd": "archive.php", "wait_admin": 0}], False),
        ([{"cmd": "archive.php", "wait_admin": "1"}], False),
        (
            [
                {"cmd": "archive.php", "wait_admin": "0"},
                {"cmd": "derive.php", "wait_admin": 2},
            ],
            False,
        ),
    ],
)
def test_catalog_completion_tracks_only_uploader_ingest(
    rows: list[dict[str, object]], complete: bool
) -> None:
    """Ignore derivative completion and blocked non-ingest tasks when polling upload."""
    assert _items.parse_catalog({"success": True, "rows": rows}) is complete


@pytest.mark.parametrize("wait_admin", [2, "2", "02"])
def test_catalog_blocked_ingest_raises_a_service_error(wait_admin: object) -> None:
    """Report administrator intervention only for the uploader archive.php task."""
    with pytest.raises(ServiceError, match="administrator"):
        _items.parse_catalog(
            {
                "success": True,
                "rows": [
                    {"cmd": "archive.php", "wait_admin": wait_admin},
                    {"cmd": "archive.php", "wait_admin": 0},
                ],
            }
        )


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        1,
        {},
        {"success": 1, "rows": []},
        {"success": True},
        {"success": True, "rows": None},
        {"success": True, "rows": {}},
        {"success": True, "rows": ()},
        {"success": True, "rows": [None]},
        {"success": True, "rows": [[]]},
        {"success": True, "rows": [{}]},
    ],
)
def test_catalog_rejects_invalid_response_containers(payload: object) -> None:
    """Do not convert malformed or absent catalog rows into a completed upload."""
    with pytest.raises(InvalidServiceResponseError):
        _items.parse_catalog(cast("Any", payload))


@pytest.mark.parametrize(
    "command",
    [
        None,
        1,
        [],
        "",
        " ",
        " archive.php",
        "archive.php ",
        "archive.php\x00",
        "archive.php\n",
        "archive.php\ud800",
    ],
)
def test_catalog_rejects_invalid_commands(command: object) -> None:
    """Reject malformed command text rather than reporting the ingest task absent."""
    with pytest.raises(InvalidServiceResponseError):
        _items.parse_catalog(
            {"success": True, "rows": [{"cmd": command, "wait_admin": 0}]}
        )


@pytest.mark.parametrize(
    "wait_admin",
    [None, True, False, -1, 0.0, [], "", "-1", "bad", "\u0662", "9" * 5000],
)
def test_catalog_rejects_invalid_wait_admin_values(wait_admin: object) -> None:
    """Validate administrative flags and safely handle integer conversion failures."""
    with pytest.raises(InvalidServiceResponseError):
        _items.parse_catalog(
            {
                "success": True,
                "rows": [{"cmd": "archive.php", "wait_admin": wait_admin}],
            }
        )


def test_catalog_failures_and_late_malformed_rows_never_become_success() -> None:
    """Require a successful query and validate every row before reporting state."""
    with pytest.raises(ServiceError, match="query failed"):
        _items.parse_catalog({"success": False, "rows": []})
    with pytest.raises(InvalidServiceResponseError):
        _items.parse_catalog(
            {"success": True, "rows": [{"cmd": "archive.php", "wait_admin": 2}, {}]}
        )


def test_removal_form_preserves_explicit_identifiers_and_comment() -> None:
    """Serialize a caller-supplied batch without selecting additional account items."""
    ids = (identifier for identifier in ("one", "two_2"))
    comment = " requested: caf\u00e9 & <reason> "
    assert _items.removal_form(ids, comment) == {
        "identifier": "one,two_2",
        "admin": "make_dark",
        "curation[state]": "dark",
        "curation[comment]": comment,
    }


@pytest.mark.parametrize(
    "ids",
    [
        None,
        1,
        "one",
        b"one",
        (),
        [],
        ["one", "one"],
        [None],
        [1],
        [[]],
        [""],
        ["../bad"],
        ["one\n"],
    ],
)
def test_removal_form_rejects_invalid_batches(ids: object) -> None:
    """Require nonempty unique valid identifiers and reject bare string batches."""
    with pytest.raises(InvalidOptionError):
        _items.removal_form(cast("Any", ids), "reason")


@pytest.mark.parametrize(
    "comment", [None, 1, "", " ", "x\n", "x\t", "x\x00", "x\ud800"]
)
def test_removal_form_requires_a_valid_comment(comment: object) -> None:
    """Require a nonblank single-line provenance comment for management requests."""
    with pytest.raises(InvalidOptionError):
        _items.removal_form(("item-1",), cast("Any", comment))


def test_removal_parses_escaped_html_and_flexible_whitespace_in_request_order() -> None:
    """Recognize explicit task acknowledgements and preserve partial missing results."""
    html = (
        "<div>Item:\n &#39;two&#39; <b>queued</b><b>for</b> &quot;make_dark&quot; "
        "operation\t- task ID: <span>456</span></div>" + f"<p>{escape(_ack('one'))}</p>"
    )
    assert _items.parse_removal(html, ("one", "missing", "two")) == (
        InternetArchiveRemovalResult("one", True, "123"),
        InternetArchiveRemovalResult("missing", False),
        InternetArchiveRemovalResult("two", True, "456"),
    )


@pytest.mark.parametrize(
    "html",
    [
        "",
        "success",
        _ack("other"),
        _ack(task_id="0"),
        _ack(task_id="000"),
        _ack(task_id="-1"),
        _ack(task_id="abc"),
        _ack().replace("make_dark", "delete"),
        _ack().replace("queued", "failed"),
        "<!--" + _ack() + "-->",
        "<script>" + _ack() + "</script>",
    ],
)
def test_removal_requires_a_positive_explicit_requested_acknowledgement(
    html: str,
) -> None:
    """Do not infer acceptance from unrelated identifiers or generic success text."""
    with pytest.raises(InvalidServiceResponseError):
        _items.parse_removal(html, ("item-1",))


def test_removal_ignores_unrequested_and_zero_tasks_in_partial_responses() -> None:
    """Return false for absent requested acknowledgements even if other tasks exist."""
    html = "<br>".join((_ack("other"), _ack("zero", "0"), _ack("item-1")))
    assert _items.parse_removal(html, ("item-1", "zero")) == (
        InternetArchiveRemovalResult("item-1", True, "123"),
        InternetArchiveRemovalResult("zero", False),
    )


def test_removal_accepts_duplicate_identical_tasks_but_rejects_conflicts() -> None:
    """Permit repeated presentation of a task but reject ambiguous task IDs."""
    assert _items.parse_removal(_ack() + "<br>" + _ack(), ("item-1",)) == (
        InternetArchiveRemovalResult("item-1", True, "123"),
    )
    with pytest.raises(InvalidServiceResponseError, match="conflicting"):
        _items.parse_removal(_ack() + "<br>" + _ack(task_id="456"), ("item-1",))
