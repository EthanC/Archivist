"""Test shared synchronous and asynchronous HTTP response handling."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import cast

import niquests
import pytest

from archivist.core._http import (
    ResponseLike,
    async_response_json,
    async_response_mapping,
    async_response_text,
    parse_retry_after,
    raise_for_common_status,
    response_json,
    response_mapping,
    response_text,
    translate_request_error,
)
from archivist.core.errors import (
    AuthenticationError,
    InvalidServiceResponseError,
    NetworkError,
    RateLimitError,
    TLSVerificationError,
)


class Response:
    """Provide a configurable synchronous response test double."""

    def __init__(
        self,
        payload: object = None,
        *,
        status_code: int = 200,
        headers: Mapping[str, str] | None = None,
        text: str = "body",
    ) -> None:
        """Initialize response attributes used by HTTP helpers."""
        self.payload = payload
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text
        self.url = "https://service.example/api"

    def json(self) -> object:
        """Return the payload or raise a configured transport failure."""
        if isinstance(self.payload, BaseException):
            raise self.payload
        return self.payload


class BrokenTextResponse:
    """Provide a response whose text property raises an error."""

    status_code = 200
    headers: Mapping[str, str] = {}
    url = "https://service.example/api"

    @property
    def text(self) -> str:
        """Raise the response text failure under test."""
        raise OSError("sensitive response details")

    def json(self) -> object:
        """Return a placeholder JSON value."""
        return None


class AsyncResponse:
    """Provide a configurable response test double for async helpers."""

    def __init__(self, payload: object, *, status_code: int = 200) -> None:
        """Initialize response attributes used by async HTTP helpers."""
        self.payload = payload
        self.status_code = status_code
        self.text: object = "body"

    def json(self) -> object:
        """Return the payload or raise a configured transport failure."""
        if isinstance(self.payload, BaseException):
            raise self.payload
        return self.payload


class BrokenAsyncTextResponse:
    """Provide an async response whose text property raises an error."""

    status_code = 200

    @property
    def text(self) -> str:
        """Raise the response text failure under test."""
        raise OSError("sensitive response details")


async def awaiting(value: object) -> object:
    """Return a value through an awaitable for decoder tests."""
    return value


def test_parse_retry_after_handles_missing_invalid_naive_and_negative_values() -> None:
    """Handle unsupported and boundary Retry-After values safely."""
    assert parse_retry_after({}) == (None, None)
    assert parse_retry_after({"retry-after": "not a date"}) == (None, "not a date")

    parsed, raw = parse_retry_after({"Retry-After": "Wed, 21 Oct 2015 07:28:00"})
    assert parsed == datetime(2015, 10, 21, 7, 28, tzinfo=UTC)
    assert raw == "Wed, 21 Oct 2015 07:28:00"
    assert parse_retry_after({"Retry-After": "-3"}) == (0.0, "-3")


def test_sync_response_decoders_accept_expected_values() -> None:
    """Decode valid synchronous JSON, mappings, and text."""
    assert response_json(Response([1, 2]), service="Test") == [1, 2]
    assert response_mapping(Response({"ok": True}), service="Test") == {"ok": True}
    assert response_text(Response(text="decoded"), service="Test") == "decoded"


def test_sync_response_decoders_translate_transport_and_shape_errors() -> None:
    """Translate synchronous transport and response-shape failures."""
    with pytest.raises(NetworkError, match="network request to Test failed") as raised:
        response_json(Response(OSError("secret")), service="Test")
    assert raised.value.__cause__ is None

    decode_error = niquests.exceptions.JSONDecodeError("invalid", "secret", 0)
    with pytest.raises(InvalidServiceResponseError, match="invalid JSON"):
        response_json(Response(decode_error), service="Test")

    with pytest.raises(InvalidServiceResponseError, match="unexpected shape"):
        response_mapping(Response(["not", "a", "mapping"]), service="Test")

    with pytest.raises(NetworkError, match="network request to Test failed") as raised:
        response_text(cast("ResponseLike", BrokenTextResponse()), service="Test")
    assert raised.value.__cause__ is None


@pytest.mark.asyncio
async def test_async_response_decoders_accept_regular_and_awaitable_values() -> None:
    """Decode regular and awaitable values from asynchronous responses."""
    assert await async_response_json(AsyncResponse([1, 2]), service="Test") == [1, 2]
    assert await async_response_json(
        AsyncResponse(awaiting({"ok": True})), service="Test"
    ) == {"ok": True}
    assert await async_response_mapping(
        AsyncResponse({"ok": True}), service="Test"
    ) == {"ok": True}

    response = AsyncResponse(None)
    response.text = "decoded"
    assert await async_response_text(response, service="Test") == "decoded"
    response.text = awaiting("awaited")
    assert await async_response_text(response, service="Test") == "awaited"


@pytest.mark.asyncio
async def test_async_response_decoders_translate_all_error_paths() -> None:
    """Translate asynchronous transport, shape, and text failures."""
    with pytest.raises(NetworkError, match="network request to Test failed") as raised:
        await async_response_json(AsyncResponse(OSError("secret")), service="Test")
    assert raised.value.__cause__ is None

    decode_error = niquests.exceptions.JSONDecodeError("invalid", "secret", 0)
    with pytest.raises(InvalidServiceResponseError, match="invalid JSON"):
        await async_response_json(AsyncResponse(decode_error), service="Test")

    with pytest.raises(InvalidServiceResponseError, match="invalid JSON"):
        await async_response_json(AsyncResponse(ValueError("secret")), service="Test")
    with pytest.raises(InvalidServiceResponseError, match="unexpected shape"):
        await async_response_mapping(
            AsyncResponse(["not", "a", "mapping"]), service="Test"
        )
    with pytest.raises(NetworkError, match="network request to Test failed") as raised:
        await async_response_text(BrokenAsyncTextResponse(), service="Test")
    assert raised.value.__cause__ is None

    response = AsyncResponse(None)
    response.text = b"not decoded"
    with pytest.raises(InvalidServiceResponseError, match="non-text"):
        await async_response_text(response, service="Test")


@pytest.mark.parametrize("status_code", [401, 403])
def test_common_status_translates_authentication_errors(status_code: int) -> None:
    """Translate authentication HTTP statuses into package errors."""
    with pytest.raises(AuthenticationError) as raised:
        raise_for_common_status(Response(status_code=status_code), service="Test")
    assert raised.value.status_code == status_code


def test_common_status_translates_rate_limits_and_other_failures() -> None:
    """Translate rate limits and unexpected HTTP failures."""
    expected_retry_seconds = 4.0
    with pytest.raises(RateLimitError) as raised:
        raise_for_common_status(
            Response(status_code=429, headers={"Retry-After": "4"}), service="Test"
        )
    assert raised.value.retry_after == expected_retry_seconds
    assert raised.value.retry_after_raw == "4"

    with pytest.raises(InvalidServiceResponseError, match="HTTP 500"):
        raise_for_common_status(Response(status_code=500), service="Test")
    assert raise_for_common_status(Response(status_code=399), service="Test") is None


def test_transport_error_translation_distinguishes_tls_failures() -> None:
    """Distinguish TLS failures from general transport errors."""

    class TLSHandshakeError(OSError):
        pass

    error = translate_request_error(TLSHandshakeError("secret"), service="Test")
    assert isinstance(error, TLSVerificationError)
    assert error.cause_type == "TLSHandshakeError"
