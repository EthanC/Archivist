"""Verify Archive.org item models, validation, and immutable normalization."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, fields
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast

import pytest

from archivist.core.errors import (
    InvalidOptionError,
    OptionCombinationError,
    ServiceError,
)
from archivist.services.internet_archive.item_models import (
    InternetArchiveRemovalResult,
    InternetArchiveUploadError,
    InternetArchiveUploadFile,
    InternetArchiveUploadFileResult,
    InternetArchiveUploadOptions,
    InternetArchiveUploadResult,
)


def _options(**overrides: object) -> InternetArchiveUploadOptions:
    values: dict[str, object] = {
        "identifier": "item-1",
        "title": "Title",
        "description": "Description",
        "subjects": ["tag"],
    }
    values.update(overrides)
    return cast("Any", InternetArchiveUploadOptions)(**values)


@pytest.mark.parametrize(
    ("media_type", "collection"),
    [
        ("movies", "opensource_movies"),
        ("audio", "opensource_audio"),
        ("texts", "opensource"),
        ("software", "open_source_software"),
        ("image", "opensource_image"),
        ("data", "opensource_media"),
    ],
)
def test_media_types_choose_the_uploader_default_collection(
    media_type: str, collection: str
) -> None:
    """Use the observed default for each supported media type."""
    assert _options(media_type=media_type).collection == collection
    assert (
        _options(media_type=media_type, test_item=True).collection == "test_collection"
    )


def test_option_defaults_and_explicit_collections() -> None:
    """Retain explicit collections and require test collection compatibility."""
    options = _options()
    assert options.media_type == "data"
    assert options.test_item is False
    assert options.collection == "opensource_media"
    assert (
        options.creator is options.date is options.language is options.license is None
    )
    assert options.metadata == {}
    assert isinstance(options.metadata, MappingProxyType)
    assert _options(collection="custom").collection == "custom"
    assert _options(collection="test_collection").collection == "test_collection"
    assert _options(test_item=True, collection="test_collection").test_item is True
    with pytest.raises(OptionCombinationError, match="test_collection"):
        _options(test_item=True, collection="custom")


@pytest.mark.parametrize("identifier", ["a", "0", "A_1.-z", "a" * 1000])
def test_identifiers_have_no_invented_length_limit(identifier: str) -> None:
    """Accept the ASCII identifier grammar without imposing an 80-character cap."""
    assert _options(identifier=identifier).identifier == identifier


@pytest.mark.parametrize(
    "identifier",
    [
        None,
        1,
        True,
        [],
        "",
        " ",
        "_a",
        ".a",
        "-a",
        "a/b",
        "a\\b",
        "a b",
        "a\n",
        "caf\u00e9",
        "a\ud800",
    ],
)
def test_identifiers_reject_invalid_runtime_shapes(identifier: object) -> None:
    """Reject invalid identifier types, characters, and first characters."""
    with pytest.raises(InvalidOptionError, match="identifier"):
        _options(identifier=identifier)


@pytest.mark.parametrize("name", ["title", "description"])
@pytest.mark.parametrize(
    "value",
    [
        None,
        1,
        True,
        [],
        "",
        " \t\n",
        "x\x00",
        "x\x1b",
        "x\x7f",
        "x\u0085",
        "x\ud800",
        "x\udfff",
    ],
)
def test_required_text_rejects_invalid_values(name: str, value: object) -> None:
    """Require nonblank text and reject controls and invalid surrogate values."""
    with pytest.raises(InvalidOptionError):
        _options(**{name: value})


@pytest.mark.parametrize("name", ["creator", "collection", "language", "license"])
@pytest.mark.parametrize(
    "value", [1, False, [], "", " ", "x\n", "x\t", "x\x00", "x\ud800"]
)
def test_optional_text_is_validated_when_present(name: str, value: object) -> None:
    """Treat None as absent, but validate every supplied optional text field."""
    with pytest.raises(InvalidOptionError):
        _options(**{name: value})


@pytest.mark.parametrize("value", [date.min, date(2024, 2, 29), date.max])
def test_upload_date_accepts_date_objects(value: date) -> None:
    """Retain calendar dates without imposing an arbitrary year restriction."""
    assert _options(date=value).date == value


@pytest.mark.parametrize(
    "value", ["2026-09-25", "", 1, False, [], datetime(2026, 9, 25)]
)
def test_upload_date_rejects_strings_datetimes_and_other_types(value: object) -> None:
    """Require a calendar date, not a timestamp or an unparsed string."""
    with pytest.raises(InvalidOptionError, match="date"):
        _options(date=value)


@pytest.mark.parametrize(
    "subjects",
    [
        None,
        1,
        "tag",
        b"tag",
        (),
        ("tag",),
        {"tag"},
        iter(["tag"]),
        [],
        [""],
        [" "],
        [1],
        ["tag", None],
        ["x\n"],
        ["x\t"],
    ],
)
def test_subjects_require_a_nonempty_list_of_text(subjects: object) -> None:
    """Reject nonlists, empty subjects, and malformed list elements."""
    with pytest.raises(InvalidOptionError):
        _options(subjects=subjects)


@pytest.mark.parametrize("test_item", [0, 1, None, "true", []])
def test_test_item_requires_a_boolean(test_item: object) -> None:
    """Reject truthy and falsey values that are not actual booleans."""
    with pytest.raises(InvalidOptionError, match="boolean"):
        _options(test_item=test_item)


@pytest.mark.parametrize("media_type", [None, [], 1, "", "video", "Movies", "data\n"])
def test_media_type_rejects_unknown_and_nonstring_values(media_type: object) -> None:
    """Restrict media types to the uploader choices, including runtime types."""
    with pytest.raises(InvalidOptionError, match="media_type"):
        _options(media_type=media_type)


@pytest.mark.parametrize(
    "key",
    [
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
    ],
)
def test_metadata_cannot_override_typed_or_server_owned_fields(key: str) -> None:
    """Reject reserved typed and server-owned metadata names."""
    with pytest.raises(InvalidOptionError, match="reserved"):
        _options(metadata={key: "value"})


@pytest.mark.parametrize(
    "key",
    [1, None, "", "Upper", "1key", "_key", "a b", "a:b", "a.b", "caf\u00e9", "a\n"],
)
def test_metadata_keys_follow_the_lowercase_ascii_grammar(key: object) -> None:
    """Reject malformed custom keys before they can become HTTP headers."""
    with pytest.raises(InvalidOptionError, match="metadata keys"):
        _options(metadata={key: "value"})


@pytest.mark.parametrize("metadata", [None, [], "source", 1])
def test_metadata_requires_a_mapping(metadata: object) -> None:
    """Reject nonmapping metadata containers at runtime."""
    with pytest.raises(InvalidOptionError, match="mapping"):
        _options(metadata=metadata)


@pytest.mark.parametrize(
    "value",
    [
        None,
        1,
        True,
        b"x",
        ("x",),
        {},
        "",
        " ",
        (),
        [],
        [1],
        [""],
        ["x", "y\x00"],
        "x\ud800",
    ],
)
def test_metadata_values_require_text_or_nonempty_text_lists(value: object) -> None:
    """Reject nonlist repeated values and invalid scalar or repeated text."""
    with pytest.raises(InvalidOptionError):
        _options(metadata={"source": value})


@pytest.mark.parametrize(
    "keys",
    [
        ("a_b", "a--b"),
        ("a--b", "a_b"),
        ("a__b", "a----b"),
        ("a_-b", "a-_b"),
        ("a-_b", "a_-b"),
    ],
)
def test_metadata_rejects_ambiguous_encodings_in_either_order(
    keys: tuple[str, str],
) -> None:
    """Reject each non-round-tripping alias regardless of the other supplied keys."""
    with pytest.raises(InvalidOptionError, match="round-trip"):
        _options(metadata=dict.fromkeys(keys, "value"))


@pytest.mark.parametrize(
    "key",
    [
        "a--b",
        "a-_b",
        "a---b",
        "a_-_b",
        "a--",
        "a-_",
        "a----b",
        "backup--location",
        "media--type",
        "test--item",
    ],
)
def test_metadata_keys_must_round_trip_even_without_another_key(key: str) -> None:
    """Reject aliases that IA would decode as a different metadata field name."""
    with pytest.raises(InvalidOptionError, match="round-trip"):
        _options(metadata={key: "value"})


@pytest.mark.parametrize(
    "key", ["a_b", "a_-b", "a__b", "a__-b", "a-b", "a_", "a_-", "a-b_c"]
)
def test_metadata_preserves_round_tripping_underscore_and_hyphen_keys(key: str) -> None:
    """Retain legitimate underscore escapes, including trailing single hyphens."""
    assert _options(metadata={key: "value"}).metadata == {key: "value"}


def test_options_copy_containers_and_preserve_unicode_html_and_repetitions() -> None:
    """Copy containers without trimming, deduplicating, or escaping text."""
    text = " <p>Caf\u00e9 \u65e5\u672c & source</p> "
    subjects = ["tag", "tag", "\u65e5\u672c"]
    repeated = [text, text]
    metadata: dict[str, str | list[str]] = {
        "source": text,
        "custom_key": repeated,
        "a_-b": "x",
    }
    options = _options(
        title=text,
        description=text,
        subjects=subjects,
        metadata=metadata,
        creator=" Author ",
        date=date(2026, 9, 25),
        language="eng",
        license="https://example.invalid/license",
    )
    subjects.append("later")
    metadata["source"] = "changed"
    repeated.append("later")
    metadata["custom_key"] = ["changed"]
    assert options.title == options.description == text
    assert options.subjects == ["tag", "tag", "\u65e5\u672c"]
    assert options.subjects is not subjects
    options.subjects.append("edited")
    assert subjects == ["tag", "tag", "\u65e5\u672c", "later"]
    assert options.metadata == {"source": text, "custom_key": [text, text], "a_-b": "x"}
    copied = options.metadata["custom_key"]
    assert isinstance(copied, list)
    assert copied is not repeated
    copied.append("edited")
    assert repeated == [text, text, "later"]
    assert options.creator == " Author "
    assert options.date == date(2026, 9, 25)
    assert options.language == "eng"
    assert options.license == "https://example.invalid/license"
    with pytest.raises(TypeError):
        cast("Any", options.metadata)["source"] = "changed"


def test_multiline_text_is_preserved_only_in_description_and_metadata() -> None:
    """Permit newline and tab text where URI encoding protects metadata values."""
    text = "<p>line one</p>\n\t<p>line two</p>"
    options = _options(
        description=text, metadata={"source": text, "notes": [text, text]}
    )
    assert options.description == text
    assert options.metadata == {"source": text, "notes": [text, text]}
    for field_name in ("title", "creator", "date", "language", "license", "collection"):
        with pytest.raises(InvalidOptionError):
            _options(**{field_name: text})


@pytest.mark.parametrize("source", ["folder/file.bin", Path("folder/file.bin")])
def test_upload_paths_default_to_basename_and_accept_explicit_names(
    source: str | Path,
) -> None:
    """Normalize strings to paths without requiring the local file to exist yet."""
    upload = InternetArchiveUploadFile(source)
    assert upload.source == Path(source)
    assert upload.name == "file.bin"
    renamed = InternetArchiveUploadFile(source, "sub/\u65e5\u672c.bin")
    assert renamed.source == Path(source)
    assert renamed.name == "sub/\u65e5\u672c.bin"


def test_upload_streams_require_names_and_remain_caller_owned() -> None:
    """Borrow named streams without reading or closing them in the model."""
    with BytesIO(b"payload") as stream:
        stream.seek(1)
        upload = InternetArchiveUploadFile(stream, "sub/file.bin")
        assert upload.source is stream
        assert upload.name == "sub/file.bin"
        assert stream.tell() == 1
        assert "BytesIO" not in repr(upload)
        with pytest.raises(InvalidOptionError, match="explicit"):
            InternetArchiveUploadFile(stream)
        assert not stream.closed


@pytest.mark.parametrize(
    "name",
    [
        None,
        1,
        "",
        " ",
        "/absolute",
        "//server/share",
        "C:/absolute",
        "C:relative",
        "a\\b",
        "../a",
        "a/../b",
        "a/./b",
        "a//b",
        "a/",
        ".",
        "..",
        "a\x00",
        "a\n",
        "a\t",
        "a\ud800",
    ],
)
def test_upload_names_reject_invalid_runtime_shapes_and_unsafe_paths(
    name: object,
) -> None:
    """Reject absolute paths, traversal, empty segments, and malformed text."""
    with BytesIO(b"payload") as stream, pytest.raises(InvalidOptionError):
        InternetArchiveUploadFile(stream, cast("Any", name))


@pytest.mark.parametrize("source", ["", " ", "file\x00", "file\ud800"])
def test_upload_source_strings_are_validated_before_path_normalization(
    source: str,
) -> None:
    """Reject invalid source text even when an explicit item name is supplied."""
    with pytest.raises(InvalidOptionError):
        InternetArchiveUploadFile(source, "valid.bin")


def test_upload_results_freeze_copies_and_separate_transfer_from_processing() -> None:
    """Keep failed or unattempted files separate from ingest completion."""
    transferred = InternetArchiveUploadFileResult("one.bin", 3, True, "etag")
    untouched = InternetArchiveUploadFileResult("two.bin", 4)
    files = [transferred, untouched]
    result = InternetArchiveUploadResult("item-1", cast("Any", files))
    files.clear()
    assert result.files == (transferred, untouched)
    assert result.processing_complete is False
    assert result.details_url == "https://archive.org/details/item-1"
    assert transferred.transferred is True
    assert transferred.etag == "etag"
    assert untouched.transferred is False
    assert untouched.etag is None
    assert InternetArchiveUploadResult("item-1", (), True).processing_complete is True


def test_removal_results_report_acceptance_without_asserting_dark_state() -> None:
    """Expose queue acceptance and optional task IDs, not eventual visibility."""
    accepted = InternetArchiveRemovalResult("one", True, "123")
    absent = InternetArchiveRemovalResult("two", False)
    assert accepted.accepted is True and accepted.task_id == "123"
    assert absent.accepted is False and absent.task_id is None
    assert not hasattr(accepted, "dark")


def test_all_item_dataclasses_are_frozen_and_slotted() -> None:
    """Prevent mutation and per-instance dictionaries on every item model."""
    models = (
        InternetArchiveUploadFile("file.bin"),
        _options(),
        InternetArchiveUploadFileResult("file.bin", 1),
        InternetArchiveUploadResult("item-1", ()),
        InternetArchiveRemovalResult("item-1", False),
    )
    for model in models:
        assert not hasattr(model, "__dict__")
        with pytest.raises(FrozenInstanceError):
            setattr(model, fields(model)[0].name, "changed")


def test_upload_errors_retain_safe_partial_failure_context() -> None:
    """Keep partial results and causes available without exposing server text."""
    result = InternetArchiveUploadResult(
        "item-1", (InternetArchiveUploadFileResult("private.bin", 1),)
    )
    cause = ServiceError("private server response", status_code=503)
    error = InternetArchiveUploadError(result, cause, failed_file="private.bin")
    assert isinstance(error, ServiceError)
    assert error.result is result
    assert error.cause is cause
    assert error.failed_file == "private.bin"
    assert error.service == "Internet Archive"
    assert error.status_code == cause.status_code
    assert str(error) == "Internet Archive item upload failed"
    assert "private" not in repr(error)
    assert InternetArchiveUploadError(result, cause).failed_file is None
