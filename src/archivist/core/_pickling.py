"""Pickle support for frozen, slotted models that hold read-only mappings.

``MappingProxyType`` cannot be pickled, so the default dataclass state of a
model storing one fails, for example when a result crosses a process boundary.
Models assign these functions as ``__getstate__`` and ``__setstate__``: each
read-only mapping is sent as a plain ``dict`` and wrapped again on load.
Classes using them must not store a plain ``dict`` in any field.
"""

from __future__ import annotations

from dataclasses import fields
from types import MappingProxyType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from _typeshed import DataclassInstance


def frozen_getstate(self: DataclassInstance) -> list[object]:
    """Return field values in order, with read-only mappings as plain dicts."""
    state: list[object] = []
    for model_field in fields(self):
        value = getattr(self, model_field.name)
        state.append(dict(value) if isinstance(value, MappingProxyType) else value)
    return state


def frozen_setstate(self: DataclassInstance, state: list[object]) -> None:
    """Restore field values without validation, re-wrapping each mapping."""
    for model_field, value in zip(fields(self), state, strict=True):
        object.__setattr__(
            self,
            model_field.name,
            MappingProxyType(value) if isinstance(value, dict) else value,
        )
