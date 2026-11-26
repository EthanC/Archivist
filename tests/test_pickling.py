"""Every public model survives pickle with its read-only mappings intact.

Models are discovered from the public packages and from the type hints of
every public client method, sync and async, so a new model is covered without
editing this file. Samples are built from field type hints with non-empty
mappings and populated nested tuples. A model whose validation rejects the
generated sample needs an entry in ``_FIELD_VALUES`` or ``_CLASS_OVERRIDES``.
"""

from __future__ import annotations

import copy
import dataclasses
import datetime
import enum
import inspect
import pickle
import types
import typing
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

import archivist
import archivist.core
import archivist.services.archive_today
import archivist.services.internet_archive
from archivist.services.internet_archive import _recovery
from archivist.services.internet_archive.item_models import (
    InternetArchiveFileDisposition,
    InternetArchiveMultipartPart,
)

_PUBLIC_MODULES = (
    archivist,
    archivist.core,
    archivist.services.archive_today,
    archivist.services.internet_archive,
)
_MD5 = "0123456789abcdef0123456789abcdef"
_ETAG = f'"{_MD5}"'

# Values for field names whose validation needs a specific shape.
_FIELD_VALUES: dict[str, object] = {
    "identifier": "sample-item",
    "name": "dir/sample.txt",
    "md5": _MD5,
    "sha1": "0123456789abcdef0123456789abcdef01234567",
    "etag": _ETAG,
    "relations": frozenset({"memento", "first"}),
}

# Whole-field overrides for models with cross-field validation.
_CLASS_OVERRIDES: dict[str, dict[str, object]] = {
    "InternetArchiveUploadOptions": {
        "collection": None,
        "test_item": False,
        "media_type": "texts",
    },
    "InternetArchiveMultipartState": {
        "size": 10,
        "part_size": 4,
        "parts": (
            InternetArchiveMultipartPart(1, _ETAG, 4),
            InternetArchiveMultipartPart(2, _ETAG, 4),
        ),
    },
    "InternetArchiveMultipartPart": {"number": 1, "size": 4},
    "InternetArchiveSaveOptions": {"if_not_archived_within": ("1d", 60)},
    "Reconciliation": {
        "dispositions": {
            "a.txt": InternetArchiveFileDisposition.TRANSFERRED,
            "b.txt": InternetArchiveFileDisposition.DEFERRED,
        }
    },
}

# Internal models that are still pickled as part of public state.
_INTERNAL_MODELS: tuple[type, ...] = (_recovery.Reconciliation,)

_SKIP_HINT_TYPES = (typing.TypeVar,)


def _resolve_hints(obj: object) -> dict[str, Any]:
    return typing.get_type_hints(obj, include_extras=False)


def _dataclasses_in(annotation: object, found: set[type]) -> None:
    """Add every dataclass referenced anywhere inside ``annotation``."""
    if isinstance(annotation, type) and dataclasses.is_dataclass(annotation):
        if annotation in found:
            return
        found.add(annotation)
        for hint in _resolve_hints(annotation).values():
            _dataclasses_in(hint, found)
        return
    origin = typing.get_origin(annotation)
    if origin is not None and dataclasses.is_dataclass(origin):
        _dataclasses_in(origin, found)
    for arg in typing.get_args(annotation):
        _dataclasses_in(arg, found)


def _public_models() -> set[type]:
    found: set[type] = set(_INTERNAL_MODELS)
    for module in _PUBLIC_MODULES:
        for name in module.__all__:
            value = getattr(module, name)
            if not isinstance(value, type):
                continue
            if dataclasses.is_dataclass(value):
                _dataclasses_in(value, found)
            elif name.endswith("Client"):
                for attr, member in inspect.getmembers(value, inspect.isfunction):
                    if attr.startswith("_"):
                        continue
                    for hint in _resolve_hints(member).values():
                        _dataclasses_in(hint, found)
    return found


def _client_classes() -> set[type]:
    return {
        getattr(module, name)
        for module in _PUBLIC_MODULES
        for name in module.__all__
        if name.endswith("Client")
    }


def _union_choice(args: tuple[object, ...]) -> object:
    """Prefer the richest union member so mappings and nesting get exercised."""

    def rank(arg: object) -> int:
        origin = typing.get_origin(arg) or arg
        if arg is type(None):
            return 99
        if origin is typing.BinaryIO or arg is Path:
            return 50
        if isinstance(origin, type) and issubclass(origin, Mapping):
            value_args = typing.get_args(arg)
            nested = bool(value_args) and dataclasses.is_dataclass(value_args[-1])
            return 0 if nested else 1
        if isinstance(origin, type) and dataclasses.is_dataclass(origin):
            return 2
        if origin is tuple:
            return 3
        return 10

    return min(args, key=rank)


def _sample_value(annotation: object, field_name: str) -> object:  # noqa: PLR0911, PLR0912
    if field_name in _FIELD_VALUES:
        return _FIELD_VALUES[field_name]
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin in {typing.Union, types.UnionType}:
        return _sample_value(_union_choice(args), field_name)
    if origin is typing.Literal:
        return args[0]
    if isinstance(annotation, typing.TypeVar):
        return _sample(archivist.ArchiveRecord)
    if origin is not None and dataclasses.is_dataclass(origin):
        return _sample(origin)
    if isinstance(annotation, type) and dataclasses.is_dataclass(annotation):
        return _sample(annotation)
    if origin is tuple:
        if len(args) == 2 and args[1] is Ellipsis:  # noqa: PLR2004
            return tuple(_sample_value(args[0], f"{field_name}[{i}]") for i in (0, 1))
        return tuple(_sample_value(arg, field_name) for arg in args)
    if origin is list:
        return [_sample_value(args[0], f"{field_name}[0]")]
    if origin is frozenset:
        return frozenset({_sample_value(args[0], field_name)})
    if isinstance(origin, type) and issubclass(origin, Mapping):
        key_type, value_type = args
        return {
            f"{_sample_value(key_type, 'key')}{suffix}": _sample_value(
                value_type, "value"
            )
            for suffix in ("a", "b")
        }
    if annotation is bool:
        return True
    if annotation is int:
        return 3
    if annotation is float:
        return 1.5
    if annotation is str:
        return "sample"
    if annotation is datetime.datetime:
        return datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.UTC)
    if annotation is datetime.date:
        return datetime.date(2024, 1, 2)
    if annotation is datetime.timedelta:
        return datetime.timedelta(seconds=60)
    if isinstance(annotation, type) and issubclass(annotation, enum.Enum):
        return next(iter(annotation))
    if annotation is Any or annotation is object:
        return "sample"
    raise TypeError(
        f"no sample for {field_name}: {annotation!r}; extend _sample_value "
        "or add an override in tests/test_pickling.py"
    )


def _sample(cls: Any) -> Any:  # noqa: ANN401 - samples are built for any model.
    hints = _resolve_hints(cls)
    overrides = _CLASS_OVERRIDES.get(cls.__name__, {})
    kwargs = {
        model_field.name: (
            overrides[model_field.name]
            if model_field.name in overrides
            else _sample_value(hints[model_field.name], model_field.name)
        )
        for model_field in dataclasses.fields(cls)
        if model_field.init
    }
    return cls(**kwargs)


def _proxies(value: object, path: str = "") -> dict[str, object]:
    """Map the path of every value reachable through models and containers."""
    found: dict[str, object] = {path: value}
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        for model_field in dataclasses.fields(value):
            found |= _proxies(
                getattr(value, model_field.name), f"{path}.{model_field.name}"
            )
    elif isinstance(value, Mapping):
        for key, item in value.items():
            found |= _proxies(item, f"{path}[{key!r}]")
    elif isinstance(value, (tuple, list, frozenset)):
        for index, item in enumerate(value):
            found |= _proxies(item, f"{path}[{index}]")
    return found


_MODELS = sorted(_public_models(), key=lambda cls: cls.__qualname__)


def test_discovery_finds_reported_models() -> None:
    """Discovery reaches the exported, nested, and client-returned models."""
    names = {cls.__name__ for cls in _MODELS}
    assert {
        "InternetArchiveItem",
        "InternetArchiveItemFile",
        "InternetArchiveItemTask",
        "InternetArchiveUploadOptions",
        "InternetArchiveUploadLimit",
        "InternetArchiveSystemStatus",
        "InternetArchiveCdxRecord",
        "InternetArchiveSuccessStatus",
        "InternetArchiveOutlinkAvailability",
        "InternetArchiveMultipartState",
        "ArchiveTodayTimeMap",
        "PagedSearchResult",
        "Reconciliation",
    } <= names


@pytest.mark.parametrize("client", sorted(_client_classes(), key=str), ids=str)
def test_client_return_models_are_covered(client: type) -> None:
    """Every model a sync or async client method returns is in the pickle set."""
    returned: set[type] = set()
    for name, member in inspect.getmembers(client, inspect.isfunction):
        if not name.startswith("_"):
            _dataclasses_in(_resolve_hints(member).get("return"), returned)
    assert returned
    assert returned <= set(_MODELS)


@pytest.mark.parametrize("cls", _MODELS, ids=lambda cls: cls.__name__)
@pytest.mark.parametrize("protocol", range(pickle.HIGHEST_PROTOCOL + 1))
def test_model_pickle_round_trip(cls: type, protocol: int) -> None:
    """Pickling returns an equal model whose mappings stay read-only."""
    original = _sample(cls)
    restored = pickle.loads(pickle.dumps(original, protocol=protocol))

    assert type(restored) is cls
    assert restored == original
    before = _proxies(original)
    after = _proxies(restored)
    assert before.keys() == after.keys()
    for path, value in before.items():
        if isinstance(value, Mapping):
            assert value, f"sample mapping {path} is empty"
        if isinstance(value, types.MappingProxyType):
            restored_mapping = after[path]
            assert isinstance(restored_mapping, types.MappingProxyType), path
            with pytest.raises(TypeError):
                typing.cast("dict[str, object]", restored_mapping)["new"] = "value"
        assert type(after[path]) is type(value), path


@pytest.mark.parametrize("cls", _MODELS, ids=lambda cls: cls.__name__)
def test_model_copy_keeps_read_only_mappings(cls: type) -> None:
    """``copy`` and ``deepcopy`` use the same state and keep the proxies."""
    original = _sample(cls)
    for duplicate in (copy.copy(original), copy.deepcopy(original)):
        assert duplicate == original
        for path, value in _proxies(original).items():
            if isinstance(value, types.MappingProxyType):
                assert isinstance(_proxies(duplicate)[path], types.MappingProxyType)


def test_samples_cover_nested_files_tasks_and_mappings() -> None:
    """The generated item exercises nested files, tasks, and metadata."""
    item = _sample(archivist.InternetArchiveItem)
    assert len(item.files) == 2  # noqa: PLR2004
    assert len(item.tasks) == 2  # noqa: PLR2004
    assert isinstance(item.metadata, types.MappingProxyType)
    assert len(item.metadata) == 2  # noqa: PLR2004
    status = _sample(archivist.InternetArchiveSuccessStatus)
    assert isinstance(status.outlinks, types.MappingProxyType)
    assert all(
        isinstance(
            value,
            archivist.services.internet_archive.InternetArchiveOutlinkAvailability,
        )
        for value in status.outlinks.values()
    )


def test_success_status_freezes_caller_outlinks() -> None:
    """A plain dict passed by a caller is stored read-only and still pickles."""
    status = archivist.InternetArchiveSuccessStatus(
        job_id="job",
        original_url="https://example.com/",
        timestamp=datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
        outlinks={"https://example.com/a": "job-a"},
    )
    assert isinstance(status.outlinks, types.MappingProxyType)
    assert pickle.loads(pickle.dumps(status)) == status


def test_errors_carrying_models_pickle() -> None:
    """Errors that wrap results and state survive a process boundary."""
    cause = archivist.ServiceError("boom", service="Internet Archive")
    errors: list[Exception] = [
        archivist.InternetArchiveUploadError(
            _sample(archivist.InternetArchiveUploadResult), cause, failed_file="a"
        ),
        archivist.InternetArchiveRecoveryDeferredError(
            _sample(archivist.InternetArchiveRecoveryResult),
            cause,
            phase=archivist.InternetArchiveRecoveryPhase.INGEST,
        ),
        archivist.InternetArchiveMultipartError(
            _sample(archivist.InternetArchiveMultipartState), cause
        ),
    ]
    for error in errors:
        restored = pickle.loads(pickle.dumps(error))
        assert type(restored) is type(error)
        assert restored.__dict__.keys() == error.__dict__.keys()
        for key in ("result", "state"):
            if hasattr(error, key):
                assert getattr(restored, key) == getattr(error, key)
