"""Shared HTTP fixtures for Archivist tests."""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import cast
from urllib.parse import parse_qs, urlsplit

import pytest

from archivist.services.internet_archive import _common as ia_common

FIXTURES = Path(__file__).parent / "fixtures"
IA_SUCCESS_STATUS = json.loads(
    (FIXTURES / "internet_archive" / "status_success.json").read_text(encoding="utf-8")
)


@dataclass(slots=True)
class RecordedRequest:
    """Store the relevant parts of a request received by the fixture server."""

    method: str
    path: str
    query: dict[str, list[str]]
    headers: dict[str, str]
    form: dict[str, list[str]]
    json: object | None


@dataclass(slots=True)
class ServerState:
    """Track fixture server requests and capture polling state."""

    base_url: str = ""
    requests: list[RecordedRequest] = field(default_factory=list)
    status_calls: dict[str, int] = field(default_factory=dict)

    def matching(self, path: str, method: str | None = None) -> list[RecordedRequest]:
        """Return requests matching a path and optional HTTP method."""
        return [
            request
            for request in self.requests
            if request.path == path and (method is None or request.method == method)
        ]


class FixtureServer(ThreadingHTTPServer):
    """Expose shared test state from the threaded fixture server."""

    state: ServerState


class FixtureHandler(BaseHTTPRequestHandler):
    """Serve deterministic responses for supported archive services."""

    protocol_version = "HTTP/1.1"

    @property
    def state(self) -> ServerState:
        """Return the fixture state attached to the server."""
        return cast("FixtureServer", self.server).state

    def log_message(self, *args: object, **kwargs: object) -> None:
        """Suppress fixture HTTP request logging."""
        return

    def _record(self) -> RecordedRequest:
        parsed = urlsplit(self.path)
        length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(length) if length else b""
        content_type = self.headers.get("Content-Type", "")
        form = (
            parse_qs(raw_body.decode("utf-8"), keep_blank_values=True)
            if "application/x-www-form-urlencoded" in content_type
            else {}
        )
        json_body: object | None = None
        if "application/json" in content_type and raw_body:
            json_body = json.loads(raw_body)
        request = RecordedRequest(
            method=self.command,
            path=parsed.path,
            query=parse_qs(parsed.query, keep_blank_values=True),
            headers=dict(self.headers.items()),
            form=form,
            json=json_body,
        )
        self.state.requests.append(request)
        return request

    def _send(
        self,
        status: int,
        body: str | bytes = b"",
        *,
        content_type: str = "text/html; charset=utf-8",
        headers: dict[str, str | tuple[str, ...]] | None = None,
    ) -> None:
        encoded = body.encode("utf-8") if isinstance(body, str) else body
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(encoded)))
        for key, value in (headers or {}).items():
            if isinstance(value, tuple):
                for item in value:
                    self.send_header(key, item)
            else:
                self.send_header(key, value)
        self.end_headers()
        if encoded:
            self.wfile.write(encoded)

    def _json(
        self,
        status: int,
        value: object,
        *,
        headers: dict[str, str | tuple[str, ...]] | None = None,
    ) -> None:
        self._send(
            status,
            json.dumps(value),
            content_type="application/json",
            headers=headers,
        )

    def do_GET(self) -> None:
        """Serve a fixture response for a GET request."""
        request = self._record()
        for handler in (
            self._handle_internet_archive_get,
            self._handle_archive_today_get,
        ):
            if handler(request):
                return
        self._send(404, "not found")

    def _handle_internet_archive_get(self, request: RecordedRequest) -> bool:
        path = request.path
        handled = True
        if path == "/ia/csrf":
            self._json(200, {"success": True, "value": {"token": "test-csrf-token"}})
        elif path == "/ia/user":
            self._json(
                200,
                {
                    "success": True,
                    "value": {
                        "username": "account@example.invalid",
                        "itemname": "@mxtive1",
                        "screenname": "Mxtive",
                    },
                },
            )
        elif path.startswith("/ia/status/") and path not in {
            "/ia/status/user",
            "/ia/status/system",
        }:
            self._send_capture_status(path.rsplit("/", 1)[-1])
        elif path == "/ia/status/user":
            self._json(200, {"available": 12, "processing": 3})
        elif path == "/ia/status/system":
            self._json(
                200,
                {"status": "ok", "recent_captures": 7, "queues": {"spn2-api": 1}},
            )
        elif path == "/ia/available":
            self._json(
                200,
                {
                    "url": request.query["url"][0],
                    "timestamp": request.query.get("timestamp", [None])[0],
                    "archived_snapshots": {
                        "closest": {
                            "available": True,
                            "status": "200",
                            "timestamp": "20200102030405",
                            "url": "https://web.archive.org/web/20200102030405/https://example.com/",
                        }
                    },
                },
            )
        elif path == "/ia/cdx":
            self._json(
                200,
                [
                    [
                        "timestamp",
                        "original",
                        "mimetype",
                        "statuscode",
                        "digest",
                        "length",
                    ],
                    [
                        "20200102030405",
                        "https://example.com/",
                        "text/html",
                        "200",
                        "ABC",
                        "42",
                    ],
                    [],
                    ["resume-token"],
                ],
            )
        else:
            handled = False
        return handled

    def _send_capture_status(self, job_id: str) -> None:
        calls = self.state.status_calls.get(job_id, 0)
        self.state.status_calls[job_id] = calls + 1
        if job_id == "failed":
            self._json(
                200,
                {
                    "status": "error",
                    "job_id": job_id,
                    "status_ext": "error:cannot-fetch",
                    "message": "capture failed",
                    "resources": [],
                },
            )
        elif job_id == "pending" or calls == 0:
            self._json(
                200,
                {"status": "pending", "job_id": job_id, "resources": []},
                headers={"Retry-After": "0"},
            )
        else:
            success_status = dict(IA_SUCCESS_STATUS)
            success_status["job_id"] = job_id
            self._json(200, success_status)

    def _handle_archive_today_get(self, request: RecordedRequest) -> bool:
        path = request.path
        original = "https://example.com/"
        encoded_original_path = "/https://example.com/"
        if path == f"/timemap{encoded_original_path}":
            body = "\n".join(
                (
                    f'<{original}>; rel="original",',
                    f'<{self.state.base_url}/timegate/{original}>; rel="timegate",',
                    (
                        f"<{self.state.base_url}/20200102030405/{original}>; "
                        'rel="first memento"; '
                        'datetime="Thu, 02 Jan 2020 03:04:05 GMT",'
                    ),
                    (
                        f"<{self.state.base_url}/20210102030405/{original}>; "
                        'rel="last memento"; '
                        'datetime="Sat, 02 Jan 2021 03:04:05 GMT",'
                    ),
                    (
                        f'<{self.state.base_url}/timemap/{original}>; rel="self"; '
                        'type="application/link-format"'
                    ),
                )
            )
            self._send(200, body, content_type="application/link-format")
        elif path in {
            f"/timegate{encoded_original_path}",
            f"/oldest{encoded_original_path}",
        }:
            self._send(
                302,
                headers={
                    "Location": f"{self.state.base_url}/20200102030405/{original}"
                },
            )
        elif path == f"/newest{encoded_original_path}":
            self._send(
                302,
                headers={
                    "Location": f"{self.state.base_url}/20210102030405/{original}"
                },
            )
        elif path == "/rss":
            self._send(
                200,
                (
                    '<?xml version="1.0"?><rss version="2.0"><channel>'
                    "<title>archive.test</title>"
                    "<lastBuildDate>Sat, 02 Jan 2021 03:04:05 GMT</lastBuildDate>"
                    "<item><title>Example</title>"
                    f"<link>{self.state.base_url}/abcde</link>"
                    "<pubDate>Thu, 02 Jan 2020 03:04:05 GMT</pubDate>"
                    "<description>Standard RSS description</description>"
                    "</item></channel></rss>"
                ),
                content_type="application/rss+xml",
            )
        else:
            return False
        return True

    def do_POST(self) -> None:
        """Serve a fixture response for a POST request."""
        request = self._record()
        for handler in (self._handle_internet_archive_post,):
            if handler(request):
                return
        self._send(404, "not found")

    def _handle_internet_archive_post(self, request: RecordedRequest) -> bool:
        path = request.path
        handled = True
        if path.startswith("/ia/save/"):
            self._send(
                200,
                '<html><script>watchJob("job-1", "/_static/", 6000, false);'
                "></script></html>",
            )
        elif path == "/ia/save":
            self._json(
                200,
                {"url": request.form["url"][0], "job_id": "job-1", "message": "queued"},
            )
        elif path == "/ia/status":
            if "job_ids" in request.form:
                self._json(
                    200,
                    {
                        "a": {"status": "pending", "job_id": "a", "resources": []},
                        "b": {
                            "status": "error",
                            "job_id": "b",
                            "status_ext": "error:blocked",
                            "resources": [],
                        },
                    },
                )
            else:
                self._json(
                    200,
                    [
                        {"status": "pending", "job_id": "child", "resources": []},
                    ],
                )
        elif path == "/ia/login":
            self._json(
                200,
                {"success": True},
                headers={
                    "Set-Cookie": (
                        "logged-in-user=test%40example.invalid; Path=/; HttpOnly",
                        "logged-in-sig=test-signature; Path=/; HttpOnly",
                    )
                },
            )
        elif path == "/ia/mwa":
            cookie = request.headers.get("Cookie", "")
            if "logged-in-user=" not in cookie or "logged-in-sig=" not in cookie:
                self._json(401, {"ok": False})
            else:
                self._json(200, {"ok": True})
        else:
            handled = False
        return handled


@pytest.fixture
def archive_server() -> Iterator[ServerState]:
    """Run a local HTTP server that supplies deterministic service responses."""
    state = ServerState()
    server = FixtureServer(("127.0.0.1", 0), FixtureHandler)
    server.state = state
    host, port = cast("tuple[str, int]", server.server_address)
    state.base_url = f"http://{host}:{port}"
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield state
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


@pytest.fixture
def ia_endpoints(
    monkeypatch: pytest.MonkeyPatch, archive_server: ServerState
) -> ServerState:
    """Point Internet Archive endpoints at the local fixture server."""
    base = archive_server.base_url
    monkeypatch.setattr(ia_common, "SAVE_URL", f"{base}/ia/save")
    monkeypatch.setattr(ia_common, "STATUS_URL", f"{base}/ia/status")
    monkeypatch.setattr(ia_common, "USER_STATUS_URL", f"{base}/ia/status/user")
    monkeypatch.setattr(ia_common, "SYSTEM_STATUS_URL", f"{base}/ia/status/system")
    monkeypatch.setattr(ia_common, "AVAILABILITY_URL", f"{base}/ia/available")
    monkeypatch.setattr(ia_common, "CDX_URL", f"{base}/ia/cdx")
    monkeypatch.setattr(ia_common, "CSRF_URL", f"{base}/ia/csrf")
    monkeypatch.setattr(ia_common, "LOGIN_URL", f"{base}/ia/login")
    monkeypatch.setattr(ia_common, "USER_INFO_URL", f"{base}/ia/user")
    monkeypatch.setattr(ia_common, "MY_WEB_ARCHIVE_URL", f"{base}/ia/mwa")
    return archive_server
