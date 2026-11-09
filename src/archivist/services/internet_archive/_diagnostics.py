"""Attribute failures to the active request in a task-local operation scope.

The response exists only during the operation. Exceptions retain copied scalar
diagnostics. Each new request clears the preceding response before transport.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from typing import ParamSpec, TypeVar, cast

from archivist.core._diagnostics import (
    async_response_diagnostics,
    parse_response_diagnostics,
    response_diagnostics,
)
from archivist.core._http import ResponseLike
from archivist.core.errors import (
    NetworkError,
    PollingTimeoutError,
    ServiceDiagnostics,
    ServiceError,
)
from archivist.services.internet_archive import _common, _items, _recovery

_P = ParamSpec("_P")
_T = TypeVar("_T")


@dataclass
class _RequestContext:
    phase: str = "request"
    response: object | None = None


_CONTEXT: ContextVar[_RequestContext | None] = ContextVar(
    "ia_diagnostics", default=None
)


def begin_request(method: str, url: str) -> None:
    """Clear stale response context and identify the step about to start."""
    context = _CONTEXT.get()
    if context is None:
        return
    context.response = None
    if url == _common.CSRF_URL:
        phase = "csrf"
    elif url == _common.LOGIN_URL:
        phase = "login"
    elif url == _items.UPLOAD_URL:
        phase = "upload-key"
    elif url == _items.UPLOAD_API_URL and method == "POST":
        phase = "identifier-check"
    elif url == _items.S3_URL:
        phase = "upload-limit"
    elif url.startswith(_items.S3_URL + "/"):
        phase = "file-upload" if "/" in url[len(_items.S3_URL) + 1 :] else "item-create"
    elif url.startswith(_recovery.METADATA_URL + "/"):
        phase = "item-metadata"
    elif url == _common.USER_INFO_URL:
        phase = "identity"
    else:
        phase = "request"
    context.phase = phase


def received_response(response: object) -> None:
    """Make only the current response available to the operation's failure handler."""
    context = _CONTEXT.get()
    if context is not None:
        context.response = response


def _cause(error: ServiceError) -> ServiceError:
    """Keep response diagnostics on the underlying upload or recovery error."""
    return cast("ServiceError", getattr(error, "cause", error))


def _enrich(
    error: ServiceError, context: _RequestContext, diagnostics: ServiceDiagnostics
) -> None:
    """Copy safe fields without replacing an already attributed nested failure."""
    for name, value in diagnostics.items():
        setattr(error, name, value)
    error.request_phase = context.phase
    if context.response is not None and not isinstance(
        error, (NetworkError, PollingTimeoutError)
    ):
        error.status_code = cast("ResponseLike", context.response).status_code


def _headers_only(error: ServiceError, context: _RequestContext) -> ServiceDiagnostics:
    """Never read a failed body again or extend an expired operation deadline."""
    if isinstance(error, PollingTimeoutError) or context.response is None:
        return ServiceDiagnostics()
    return parse_response_diagnostics(
        None, cast("ResponseLike", context.response).headers
    )


def diagnostic_operation(function: Callable[_P, _T]) -> Callable[_P, _T]:
    """Enrich synchronous service failures within an isolated operation scope."""

    @wraps(function)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        context = _RequestContext()
        token = _CONTEXT.set(context)
        try:
            return function(*args, **kwargs)
        except ServiceError as exc:
            error = _cause(exc)
            if error.request_phase is None:
                diagnostics = (
                    response_diagnostics(cast("ResponseLike", context.response))
                    if context.response is not None
                    and not isinstance(error, (NetworkError, PollingTimeoutError))
                    else _headers_only(error, context)
                )
                _enrich(error, context, diagnostics)
            exc.status_code = error.status_code
            raise
        finally:
            _CONTEXT.reset(token)

    return wrapped


def async_diagnostic_operation(
    function: Callable[_P, Awaitable[_T]],
) -> Callable[_P, Awaitable[_T]]:
    """Enrich async failures without sharing response context across tasks."""

    @wraps(function)
    async def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        context = _RequestContext()
        token = _CONTEXT.set(context)
        try:
            return await function(*args, **kwargs)
        except ServiceError as exc:
            error = _cause(exc)
            if error.request_phase is None:
                diagnostics = (
                    await async_response_diagnostics(context.response)
                    if context.response is not None
                    and not isinstance(error, (NetworkError, PollingTimeoutError))
                    else _headers_only(error, context)
                )
                _enrich(error, context, diagnostics)
            exc.status_code = error.status_code
            raise
        finally:
            _CONTEXT.reset(token)

    return wrapped
