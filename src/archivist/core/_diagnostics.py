"""Bounded, body-safe response diagnostics shared by both transports."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from html import unescape
from inspect import isawaitable, iscoroutine
from typing import cast
from unicodedata import category
from xml.etree import ElementTree

import niquests

from archivist.core._http import (
    ResponseLike,
    parse_retry_after,
    sanitize_service_reason,
)
from archivist.core.errors import ServiceDiagnostics

MAX_BODY_BYTES = 16 * 1024
MAX_FIELD_LENGTH = 256
MAX_MESSAGE_LENGTH = 1024
MAX_HEADER_LENGTH = 256
# x-amz-request-id is the S3 request ID header. No speculative Archive headers.
_HEADERS = frozenset({"content-type", "retry-after", "x-amz-request-id"})
_URL = re.compile(r"(?i)https?://[^\s<>\"']+")
_CREDENTIAL = re.compile(
    r"(?i)\b(?:authorization|cookies?|set-cookie|password|passwd|secret(?:[_ -]?key)?|"
    r"access[_ -]?key|s3accesskey|s3secretkey|logged-in-(?:sig|user)|"
    r"x-amz-(?:credential|signature|security-token)|token|signature)\b"
    r"[\"']?\s*[=:]\s*(?:\"[^\"]*\"|'[^']*'|[^\s,;]+)"
)
_AUTH_VALUE = re.compile(r"(?i)\b(?:LOW|Basic|Bearer)\s+[^\s,;]+")
_PRIVATE_HEADER = re.compile(r"(?i)\b(?:authorization|cookies?|set-cookie)\s*[:=].*")
_READ_ERRORS = (
    niquests.exceptions.RequestException,
    OSError,
    AttributeError,
    NotImplementedError,
    TypeError,
    ValueError,
)


def _safe_text(value: object, limit: int) -> str | None:
    """Remove URLs and labeled credentials before markup removal and truncation."""
    if not isinstance(value, str):
        return None
    value = _URL.sub("[redacted-url]", unescape(value))
    try:
        value = sanitize_service_reason(value, max_length=MAX_BODY_BYTES)
    except (AssertionError, ValueError):
        return None
    if value is None:
        return None
    value = _URL.sub("[redacted-url]", value)
    value = _AUTH_VALUE.sub("[redacted-credential]", value)
    value = _PRIVATE_HEADER.sub("[redacted-credential]", value)
    value = _CREDENTIAL.sub("[redacted-credential]", value)
    value = "".join(char for char in value if category(char) not in {"Cc", "Cs"})
    return value[:limit].rstrip() or None


def _body_fields(body: object) -> Mapping[str, object]:
    """Read only complete bounded JSON objects or entity-free S3 Error XML."""
    if not isinstance(body, str):
        return {}
    encoded = body[: MAX_BODY_BYTES + 1].encode("utf-8", errors="replace")
    if len(encoded) > MAX_BODY_BYTES:
        return {}
    text = body.strip()
    try:
        if text.startswith("{"):
            data = json.loads(text)
            return cast("Mapping[str, object]", data)
        if text.startswith("<") and not re.search(
            r"<!\s*(?:DOCTYPE|ENTITY)", text, re.I
        ):
            root = ElementTree.fromstring(text)
            if root.tag.rsplit("}", 1)[-1] == "Error":
                return {
                    child.tag.rsplit("}", 1)[-1]: child.text
                    for child in root
                    if child.tag.rsplit("}", 1)[-1] in {"Code", "Message", "RequestId"}
                }
    except (ValueError, RecursionError, ElementTree.ParseError):
        pass
    return {}


def parse_response_diagnostics(
    body: object, headers: Mapping[str, str], *, request_phase: str | None = None
) -> ServiceDiagnostics:
    """Extract recognized fields, never full bodies, arbitrary headers, or URLs.

    JSON recognizes top-level error/message/detail/reason strings and S3-style
    Code/Message/RequestId. HTML and plain text yield headers only.
    """
    safe_headers = {
        key.lower(): value
        for key, raw in headers.items()
        if key.lower() in _HEADERS
        and (value := _safe_text(raw[:MAX_BODY_BYTES], MAX_HEADER_LENGTH)) is not None
    }
    retry_after, _ = parse_retry_after(safe_headers)
    fields = _body_fields(body)
    message = next(
        (
            fields[key]
            for key in ("Message", "error", "message", "detail", "reason")
            if isinstance(fields.get(key), str) and fields[key]
        ),
        None,
    )
    return ServiceDiagnostics(
        service_code=_safe_text(fields.get("Code"), MAX_FIELD_LENGTH),
        service_message=_safe_text(message, MAX_MESSAGE_LENGTH),
        request_id=_safe_text(fields.get("RequestId"), MAX_FIELD_LENGTH)
        or safe_headers.get("x-amz-request-id"),
        request_phase=request_phase,
        retry_after=retry_after,
        retry_after_raw=safe_headers.get("retry-after"),
        diagnostic_headers=safe_headers,
    )


def response_diagnostics(
    response: ResponseLike, *, request_phase: str | None = None
) -> ServiceDiagnostics:
    """Read synchronous text without interpreting awaitable access as body data."""
    try:
        body = response.text
        if iscoroutine(body):
            body.close()
    except _READ_ERRORS:
        body = None
    return parse_response_diagnostics(
        body, response.headers, request_phase=request_phase
    )


async def async_response_diagnostics(
    response: object, *, request_phase: str | None = None
) -> ServiceDiagnostics:
    """Read ordinary or awaitable niquests text through the same pure parser."""
    typed = cast("ResponseLike", response)
    try:
        body = typed.text
        if isawaitable(body):
            body = await body
    except _READ_ERRORS:
        body = None
    return parse_response_diagnostics(body, typed.headers, request_phase=request_phase)
