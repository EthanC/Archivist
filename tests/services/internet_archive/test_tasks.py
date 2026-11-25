"""Exercise the Tasks API summary, derive submission, and status reads."""

# ruff: noqa: PLR2004 - Assertions describe exact protocol values and request counts.

from __future__ import annotations

import inspect
import json
import pickle
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

import pytest
import pytest_asyncio

from archivist import (
    AsyncInternetArchiveClient,
    AuthenticationError,
    InternetArchiveAccount,
    InternetArchiveApiKey,
    InternetArchiveClient,
    InternetArchiveCookies,
    InternetArchiveTaskNotFoundError,
    InternetArchiveTaskState,
    InternetArchiveTaskStatus,
    InternetArchiveTaskSubmission,
    InternetArchiveTaskSummary,
    InvalidOptionError,
    InvalidServiceResponseError,
    NetworkError,
    RateLimitError,
)
from archivist.services.internet_archive import _tasks
from tests.services.internet_archive.test_client_edge_cases import (
    AsyncSession,
    StubResponse,
    SyncSession,
    as_async_session,
    as_sync_session,
)
from tests.services.internet_archive.test_item_recovery import Client, resolve

if TYPE_CHECKING:
    from tests.conftest import RecordedRequest, ServerState

IDENTIFIER = "derive-item"
PATH = "/ia/tasks"
AUTH = "LOW access:secret"
S3_SLOW_DOWN = (
    "<Error><Code>SlowDown</Code><Message>Please reduce your request rate."
    "</Message><RequestId>slow-id</RequestId></Error>"
)


@pytest_asyncio.fixture(
    params=[InternetArchiveClient, AsyncInternetArchiveClient], ids=["sync", "async"]
)
async def client(request: pytest.FixtureRequest) -> AsyncIterator[Client]:
    """Provide both transports with an S3 key and cookies that must not be sent."""
    instance = request.param(
        api_key=InternetArchiveApiKey("access", "secret"),
        cookies=InternetArchiveCookies("user%40example.com", "signature"),
    )
    try:
        yield instance
    finally:
        await resolve(instance.close())


def reply(
    state: ServerState,
    method: str,
    status: int,
    body: object,
    headers: dict[str, str] | None = None,
) -> None:
    """Queue one raw or JSON-encoded Tasks API reply."""
    text = body if isinstance(body, str) else json.dumps(body)
    state.queued_responses.setdefault((method, PATH), []).append(
        (status, text, {"Content-Type": "application/json", **(headers or {})})
    )


def listing(
    catalog: list[dict[str, object]] | None = None,
    history: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    """Build a documented listing reply."""
    return {
        "success": True,
        "value": {"catalog": catalog or [], "history": history or []},
    }


SUMMARY_ZEROS = {"queued": 0, "running": 0, "error": 0, "paused": 0}
# The shape of a finished derive history row, with placeholder identity values.
LIVE_HISTORY_ROW: dict[str, object] = {
    "identifier": "example-history-item",
    "task_id": 5740494761,
    "server": "iw902202",
    "cmd": "derive.php",
    "submittime": "2026-11-01 07:36:31.979818",
    "submitter": "user@example.com",
    "priority": -4,
    "finished": 4181123504,
}
# The shape of a paused live derive catalog row, with a placeholder identifier.
LIVE_PAUSED_ROW: dict[str, object] = {
    "task_id": 5765169292,
    "identifier": IDENTIFIER,
    "server": "iw902202",
    "cmd": "derive.php",
    "args": {},
    "submittime": "2026-11-17 10:58:35.107053",
    "submitter": "user@example.com",
    "priority": 0,
    "wait_admin": 9,
    "status": "paused",
}


def row(task_id: int = 101, wait_admin: object = 0, **extra: object) -> dict:
    """Build one catalog row in the documented field names."""
    return {
        "task_id": task_id,
        "identifier": IDENTIFIER,
        "cmd": "derive.php",
        "server": "ia800000.us.archive.org",
        "submittime": "2026-11-25 10:15:30",
        "wait_admin": wait_admin,
        "args": {},
        **extra,
    }


def assert_isolated(request: RecordedRequest) -> None:
    """Every task request carries the S3 key and no session cookies."""
    headers = {key.lower(): value for key, value in request.headers.items()}
    assert headers["authorization"] == AUTH
    assert "cookie" not in headers


# Summary


@pytest.mark.asyncio
async def test_summary_reads_counts(ia_endpoints: ServerState, client: Client) -> None:
    """Parse the production backlog reading and send only summary=1."""
    reply(
        ia_endpoints,
        "GET",
        200,
        {
            "success": True,
            "value": {
                "summary": {"queued": 423, "running": 3, "error": 1, "paused": 0}
            },
        },
    )
    summary = await resolve(client.task_summary())
    assert summary == InternetArchiveTaskSummary(423, 3, 1, 0)
    assert summary.outstanding == 426
    (request,) = ia_endpoints.requests
    assert (request.method, request.path) == ("GET", PATH)
    assert request.query == {"summary": ["1"]}
    assert_isolated(request)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        {"success": True, "value": {}},
        {"success": True, "value": {"summary": None}},
        {"success": True},
        {"success": True, "value": {"summary": {"queued": 1, "running": 0}}},
        {
            "success": True,
            "value": {
                "summary": {"queued": True, "running": 0, "error": 0, "paused": 0}
            },
        },
        {
            "success": True,
            "value": {
                "summary": {"queued": "1", "running": 0, "error": 0, "paused": 0}
            },
        },
        {
            "success": True,
            "value": {"summary": {"queued": -1, "running": 0, "error": 0, "paused": 0}},
        },
        "not json",
        "[1]",
    ],
    ids=[
        "missing-summary",
        "null-summary",
        "missing-value",
        "missing-count",
        "bool",
        "string",
        "negative",
        "non-json",
        "non-object",
    ],
)
async def test_summary_rejects_malformed_replies(
    ia_endpoints: ServerState, client: Client, body: object
) -> None:
    """Accept only real non-negative integers in a summary object."""
    reply(ia_endpoints, "GET", 200, body)
    with pytest.raises(InvalidServiceResponseError) as raised:
        await resolve(client.task_summary())
    assert raised.value.request_phase == "task-summary"
    assert raised.value.status_code == 200


@pytest.mark.asyncio
async def test_unsuccessful_reply_keeps_bounded_error_text(
    ia_endpoints: ServerState, client: Client
) -> None:
    """Report IA's error text after redaction, never the raw value."""
    reply(
        ia_endpoints,
        "GET",
        200,
        {"success": False, "error": "denied token=hunter2 " + "x" * 2000},
    )
    with pytest.raises(InvalidServiceResponseError) as raised:
        await resolve(client.task_summary())
    message = raised.value.service_message
    assert message is not None
    assert message.startswith("denied")
    assert "hunter2" not in message
    assert len(message) <= 1024
    assert "hunter2" not in str(raised.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "body", "headers", "error_type", "retry_after"),
    [
        (
            401,
            '{"success":false,"error":"Unauthorized"}',
            {},
            AuthenticationError,
            None,
        ),
        (403, '{"error":"forbidden"}', {}, AuthenticationError, None),
        (503, S3_SLOW_DOWN, {}, RateLimitError, None),
        (503, S3_SLOW_DOWN, {"Retry-After": "30"}, RateLimitError, 30.0),
        (503, "", {"Retry-After": "5"}, RateLimitError, 5.0),
        (429, "", {"Retry-After": "9"}, RateLimitError, 9.0),
        (503, "", {}, InvalidServiceResponseError, None),
        (302, "", {"Location": "https://example.invalid/"}, None, None),
    ],
    ids=[
        "401",
        "403",
        "slowdown",
        "slowdown-retry-after",
        "503-retry-after",
        "429",
        "503-bare",
        "redirect",
    ],
)
async def test_http_failures_are_classified(  # noqa: PLR0913,PLR0917 - Parametrized matrix.
    ia_endpoints: ServerState,
    client: Client,
    status: int,
    body: str,
    headers: dict[str, str],
    error_type: type[Exception] | None,
    retry_after: float | None,
) -> None:
    """Classify statuses with the task phase and never follow redirects."""
    reply(ia_endpoints, "GET", status, body, headers)
    with pytest.raises(error_type or InvalidServiceResponseError) as raised:
        await resolve(client.task_summary())
    error = cast("Any", raised.value)
    assert error.request_phase == "task-summary"
    assert error.status_code == status
    assert error.retry_after == retry_after
    if status == 401:
        assert error.service_message == "Unauthorized"
    if body == S3_SLOW_DOWN:
        assert error.service_code == "SlowDown"
        assert error.request_id == "slow-id"
    assert len(ia_endpoints.requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    ("operation", "phase"),
    [
        ("summary", "task-summary"),
        ("status", "task-status"),
        ("derive", "task-submit-check"),
    ],
)
async def test_network_failure_names_the_phase(
    asynchronous: bool, operation: str, phase: str
) -> None:
    """Translate transport failures without echoing exception text."""
    responses: list[StubResponse | BaseException] = [OSError("private secret")]
    if asynchronous:
        instance: Client = AsyncInternetArchiveClient(
            session=as_async_session(AsyncSession(responses)),
            api_key=InternetArchiveApiKey("access", "secret"),
        )
    else:
        instance = InternetArchiveClient(
            session=as_sync_session(SyncSession(responses)),
            api_key=InternetArchiveApiKey("access", "secret"),
        )
    call = {
        "summary": instance.task_summary,
        "status": lambda: instance.task_status(7),
        "derive": lambda: instance.queue_derive(IDENTIFIER),
    }[operation]
    with pytest.raises(NetworkError) as raised:
        await resolve(call())
    assert raised.value.request_phase == phase
    assert raised.value.status_code is None
    assert "private secret" not in str(raised.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(("client_timeout", "expected"), [(30.0, 15.0), (5.0, 5.0)])
async def test_reads_are_isolated_and_capped(
    asynchronous: bool, client_timeout: float, expected: float
) -> None:
    """Send explicit auth, no cookies, no redirects, and a 15 second cap."""
    response = StubResponse(
        {
            "success": True,
            "value": {"summary": {"queued": 0, "running": 0, "error": 0, "paused": 0}},
        }
    )
    session: SyncSession | AsyncSession
    if asynchronous:
        session = AsyncSession([response])
        instance: Client = AsyncInternetArchiveClient(
            session=as_async_session(session),
            api_key=InternetArchiveApiKey("access", "secret"),
            timeout=client_timeout,
        )
    else:
        session = SyncSession([response])
        instance = InternetArchiveClient(
            session=as_sync_session(session),
            api_key=InternetArchiveApiKey("access", "secret"),
            timeout=client_timeout,
        )
    await resolve(instance.task_summary())
    ((method, url, kwargs),) = session.requests
    assert (method, url) == ("GET", _tasks.TASKS_URL)
    assert kwargs["headers"] == {"Accept": "application/json", "Authorization": AUTH}
    assert kwargs["cookies"] == {}
    assert kwargs["auth"] is None
    assert kwargs["allow_redirects"] is False
    assert kwargs["timeout"] == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "client_type", [InternetArchiveClient, AsyncInternetArchiveClient]
)
async def test_account_clients_use_the_cached_upload_key(
    ia_endpoints: ServerState, client_type: type[Client]
) -> None:
    """Resolve the uploader key once, as uploads do, then send it as LOW auth."""
    summary = {
        "success": True,
        "value": {"summary": {"queued": 0, "running": 0, "error": 0, "paused": 0}},
    }
    reply(ia_endpoints, "GET", 200, summary)
    reply(ia_endpoints, "GET", 200, summary)
    instance = client_type(account=InternetArchiveAccount("user", "password"))
    try:
        await resolve(instance.task_summary())
        await resolve(instance.task_summary())
    finally:
        await resolve(instance.close())
    assert len(ia_endpoints.matching("/ia/upload", "GET")) == 1
    reads = ia_endpoints.matching(PATH, "GET")
    assert [r.headers["Authorization"] for r in reads] == [
        "LOW dummy-access:dummy-secret"
    ] * 2
    assert all("cookie" not in map(str.lower, r.headers) for r in reads)


# Submission


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({}, {"identifier": IDENTIFIER, "cmd": "derive.php", "args": {}}),
        (
            {"priority": -5, "args": {"remove_derived": "*.mp4"}},
            {
                "identifier": IDENTIFIER,
                "cmd": "derive.php",
                "args": {"remove_derived": "*.mp4"},
                "priority": -5,
            },
        ),
        (
            {"priority": 0},
            {"identifier": IDENTIFIER, "cmd": "derive.php", "args": {}, "priority": 0},
        ),
    ],
    ids=["default", "priority-and-args", "zero-priority"],
)
async def test_derive_posts_exact_json(
    ia_endpoints: ServerState,
    client: Client,
    kwargs: dict[str, Any],
    expected: dict[str, object],
) -> None:
    """Check for a pending derive, then post once with priority only when given."""
    reply(ia_endpoints, "GET", 200, listing())
    reply(
        ia_endpoints,
        "POST",
        200,
        {
            "success": True,
            "value": {"task_id": 1234567, "log": "https://archive.org/log/1234567"},
        },
    )
    result = await resolve(client.queue_derive(IDENTIFIER, **kwargs))
    assert result == InternetArchiveTaskSubmission(
        1234567, IDENTIFIER, "https://archive.org/log/1234567"
    )
    check, post = ia_endpoints.requests
    assert (check.method, check.path) == ("GET", PATH)
    assert check.query == {
        "identifier": [IDENTIFIER],
        "cmd": ["derive.php"],
        "catalog": ["1"],
        "summary": ["0"],
    }
    assert (post.method, post.path) == ("POST", PATH)
    assert post.headers["Content-Type"] == "application/json"
    assert post.json == expected
    assert_isolated(check)
    assert_isolated(post)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        ([row(300, 0)], 300),
        ([row(300, 1)], 300),
        ([row(301, 0), row(299, 1)], 299),
        ([LIVE_PAUSED_ROW], 5765169292),
        ([row(301, 0), row(299, 9)], 299),
        ([row(1, 2), row(300, 0)], 300),
    ],
    ids=["queued", "running", "oldest", "paused", "paused-and-queued", "skips-error"],
)
async def test_pending_derive_short_circuits(
    ia_endpoints: ServerState,
    client: Client,
    rows: list[dict[str, object]],
    expected: int,
) -> None:
    """Return the existing queued, running or paused derive without posting."""
    reply(ia_endpoints, "GET", 200, listing(rows))
    result = await resolve(client.queue_derive(IDENTIFIER))
    assert result == InternetArchiveTaskSubmission(
        expected, IDENTIFIER, already_pending=True
    )
    assert ia_endpoints.matching(PATH, "POST") == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows",
    [
        [row(1, 2)],
        [row(1, 5)],
        [row(1, 0, cmd="archive.php")],
        [row(1, 0, identifier="other-item")],
    ],
    ids=["error", "unknown", "other-command", "other-item"],
)
async def test_non_pending_rows_do_not_block_submission(
    ia_endpoints: ServerState, client: Client, rows: list[dict[str, object]]
) -> None:
    """Submit when no queued, running or paused derive exists for this item.

    A failed derive (``wait_admin`` 2) must never count as pending.
    """
    reply(ia_endpoints, "GET", 200, listing(rows))
    reply(ia_endpoints, "POST", 200, {"success": True, "value": {"task_id": 9}})
    result = await resolve(client.queue_derive(IDENTIFIER))
    assert result == InternetArchiveTaskSubmission(9, IDENTIFIER)
    assert len(ia_endpoints.matching(PATH, "POST")) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "body", "error_type"),
    [
        (
            200,
            {"success": False, "error": "item is locked"},
            InvalidServiceResponseError,
        ),
        (
            200,
            {"success": True, "value": {"task_id": "9"}},
            InvalidServiceResponseError,
        ),
        (
            200,
            {"success": True, "value": {"task_id": True}},
            InvalidServiceResponseError,
        ),
        (
            200,
            {"success": True, "value": {"task_id": 9, "log": 5}},
            InvalidServiceResponseError,
        ),
        (200, {"success": True, "value": None}, InvalidServiceResponseError),
        (429, {"success": False, "error": "limit"}, RateLimitError),
        (503, S3_SLOW_DOWN, RateLimitError),
        (403, {"success": False, "error": "not yours"}, AuthenticationError),
    ],
    ids=[
        "unsuccessful",
        "string-id",
        "bool-id",
        "bad-log",
        "null-value",
        "429",
        "slowdown",
        "403",
    ],
)
async def test_submission_failures_name_the_submit_phase(
    ia_endpoints: ServerState,
    client: Client,
    status: int,
    body: object,
    error_type: type[Exception],
) -> None:
    """Reject malformed or refused submissions after exactly one POST."""
    reply(ia_endpoints, "GET", 200, listing())
    reply(ia_endpoints, "POST", status, body)
    with pytest.raises(error_type) as raised:
        await resolve(client.queue_derive(IDENTIFIER))
    assert cast("Any", raised.value).request_phase == "task-submit"
    assert len(ia_endpoints.matching(PATH, "POST")) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs",
    [
        {"identifier": "-bad"},
        {"identifier": IDENTIFIER, "priority": 11},
        {"identifier": IDENTIFIER, "priority": -11},
        {"identifier": IDENTIFIER, "priority": True},
        {"identifier": IDENTIFIER, "priority": 1.0},
        {"identifier": IDENTIFIER, "args": {"a": 1}},
        {"identifier": IDENTIFIER, "args": {"": "x"}},
        {"identifier": IDENTIFIER, "args": [("a", "b")]},
    ],
)
async def test_invalid_derive_options_send_nothing(
    ia_endpoints: ServerState, client: Client, kwargs: dict[str, Any]
) -> None:
    """Validate locally before any request."""
    kwargs = dict(kwargs)
    identifier = kwargs.pop("identifier")
    with pytest.raises(InvalidOptionError):
        await resolve(client.queue_derive(identifier, **kwargs))
    assert ia_endpoints.requests == []


@pytest.mark.asyncio
async def test_only_derive_can_be_submitted(
    ia_endpoints: ServerState, client: Client
) -> None:
    """No parameter selects the command, and args cannot override it."""
    parameters = inspect.signature(client.queue_derive).parameters
    assert set(parameters) == {"identifier", "priority", "args"}
    with pytest.raises(TypeError):
        await resolve(cast("Any", client).queue_derive(IDENTIFIER, cmd="delete.php"))
    reply(ia_endpoints, "GET", 200, listing())
    reply(ia_endpoints, "POST", 200, {"success": True, "value": {"task_id": 9}})
    await resolve(client.queue_derive(IDENTIFIER, args={"cmd": "delete.php"}))
    (post,) = ia_endpoints.matching(PATH, "POST")
    assert cast("dict[str, object]", post.json)["cmd"] == "derive.php"


# Status


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("wait_admin", "state"),
    [
        (0, InternetArchiveTaskState.QUEUED),
        (1, InternetArchiveTaskState.RUNNING),
        (2, InternetArchiveTaskState.ERROR),
        (9, InternetArchiveTaskState.PAUSED),
    ],
)
async def test_status_maps_wait_admin(
    ia_endpoints: ServerState,
    client: Client,
    wait_admin: int,
    state: InternetArchiveTaskState,
) -> None:
    """Map each documented run state and query by task ID across both lists."""
    reply(ia_endpoints, "GET", 200, listing([row(55, wait_admin)]))
    status = await resolve(client.task_status(55))
    assert status == InternetArchiveTaskStatus(
        task_id=55,
        identifier=IDENTIFIER,
        cmd="derive.php",
        state=state,
        submitted_at=datetime(2026, 11, 25, 10, 15, 30, tzinfo=UTC),
        server="ia800000.us.archive.org",
        wait_admin=wait_admin,
        raw_state=str(wait_admin),
    )
    (request,) = ia_endpoints.requests
    assert request.query == {"task_id": ["55"], "history": ["1"], "catalog": ["1"]}
    assert_isolated(request)


@pytest.mark.asyncio
async def test_live_history_row_is_finished(
    ia_endpoints: ServerState, client: Client
) -> None:
    """Parse a live-shaped history row without reading ``finished`` as a time."""
    reply(
        ia_endpoints,
        "GET",
        200,
        {
            "success": True,
            "value": {
                "summary": SUMMARY_ZEROS,
                "catalog": [],
                "history": [LIVE_HISTORY_ROW],
            },
        },
    )
    status = await resolve(client.task_status(5740494761))
    assert status == InternetArchiveTaskStatus(
        task_id=5740494761,
        identifier=str(LIVE_HISTORY_ROW["identifier"]),
        cmd="derive.php",
        state=InternetArchiveTaskState.FINISHED,
        submitted_at=datetime(2026, 11, 1, 7, 36, 31, 979818, tzinfo=UTC),
        finished_at=None,
        server="iw902202",
        finished_marker=4181123504,
    )
    (request,) = ia_endpoints.requests
    assert request.query == {
        "task_id": ["5740494761"],
        "history": ["1"],
        "catalog": ["1"],
    }


@pytest.mark.asyncio
async def test_live_catalog_row_with_empty_history(
    ia_endpoints: ServerState, client: Client
) -> None:
    """Report a live row from the observed catalog-only reply."""
    live = {
        **row(77, 2),
        "submitter": "user@example.com",
        "priority": -4,
        "color": "red",
        "status": "error",
    }
    reply(
        ia_endpoints,
        "GET",
        200,
        {
            "success": True,
            "value": {"summary": SUMMARY_ZEROS, "catalog": [live], "history": []},
            "cursor": "c:77",
        },
    )
    status = await resolve(client.task_status(77))
    assert status.state is InternetArchiveTaskState.ERROR
    assert status.finished_marker is None
    assert status.finished_at is None


@pytest.mark.asyncio
async def test_history_row_wins_over_stale_live_row(
    ia_endpoints: ServerState, client: Client
) -> None:
    """A history row means finished; a non-integer marker is dropped."""
    reply(
        ia_endpoints,
        "GET",
        200,
        listing([row(55, 1)], [{**row(55), "finished": "soon", "server": None}]),
    )
    status = await resolve(client.task_status(55))
    assert status.state is InternetArchiveTaskState.FINISHED
    assert status.finished_marker is None
    assert status.finished_at is None
    assert status.server is None


@pytest.mark.asyncio
async def test_summary_only_reply_is_not_found(
    ia_endpoints: ServerState, client: Client
) -> None:
    """A reply without catalog or history keys means no row matched."""
    reply(
        ia_endpoints, "GET", 200, {"success": True, "value": {"summary": SUMMARY_ZEROS}}
    )
    with pytest.raises(InternetArchiveTaskNotFoundError):
        await resolve(client.task_status(55))


@pytest.mark.asyncio
async def test_history_is_never_requested_unfiltered(
    ia_endpoints: ServerState, client: Client
) -> None:
    """IA rejects history=1 without identifier or task_id with HTTP 400."""
    summary = {"success": True, "value": {"summary": SUMMARY_ZEROS}}
    reply(ia_endpoints, "GET", 200, summary)
    reply(ia_endpoints, "GET", 200, listing())
    reply(ia_endpoints, "POST", 200, {"success": True, "value": {"task_id": 9}})
    reply(ia_endpoints, "GET", 200, listing([row(9)]))
    await resolve(client.task_summary())
    await resolve(client.queue_derive(IDENTIFIER))
    await resolve(client.task_status(9))
    reads = ia_endpoints.matching(PATH, "GET")
    assert len(reads) == 3
    for request in reads:
        if request.query.get("history") == ["1"]:
            assert request.query.get("identifier") or request.query.get("task_id")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw", [5, "0", None, True], ids=["int", "string", "null", "bool"]
)
async def test_unknown_state_keeps_raw_value(
    ia_endpoints: ServerState, client: Client, raw: object
) -> None:
    """Never guess a state; preserve the value for diagnostics."""
    reply(ia_endpoints, "GET", 200, listing([row(55, raw)]))
    status = await resolve(client.task_status(55))
    assert status.state is InternetArchiveTaskState.UNKNOWN
    assert status.raw_state == (None if raw is None else str(raw))
    assert status.wait_admin == (raw if raw == 5 else None)


@pytest.mark.asyncio
async def test_missing_task_raises_not_found(
    ia_endpoints: ServerState, client: Client
) -> None:
    """Rows for other tasks do not satisfy the lookup."""
    reply(ia_endpoints, "GET", 200, listing([row(54)], [row(56)]))
    with pytest.raises(InternetArchiveTaskNotFoundError) as raised:
        await resolve(client.task_status(55))
    assert raised.value.task_id == 55
    assert raised.value.request_phase == "task-status"
    restored = pickle.loads(pickle.dumps(raised.value))
    assert restored.task_id == 55


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows",
    [
        "rows",
        ["row"],
        [{**row(), "task_id": "55"}],
        [{**row(), "submittime": None}],
        [{**row(), "submittime": "yesterday"}],
        [{**row(), "cmd": ""}],
        [{**row(), "server": 5}],
    ],
    ids=["not-list", "not-object", "string-id", "no-time", "bad-time", "cmd", "server"],
)
async def test_malformed_rows_are_rejected(
    ia_endpoints: ServerState, client: Client, rows: object
) -> None:
    """Strictly type every row field the result depends on."""
    reply(
        ia_endpoints,
        "GET",
        200,
        {"success": True, "value": {"catalog": rows, "history": []}},
    )
    with pytest.raises(InvalidServiceResponseError):
        await resolve(client.task_status(55))


@pytest.mark.asyncio
@pytest.mark.parametrize("task_id", [0, -1, True, "55", 5.0])
async def test_invalid_task_id_sends_nothing(
    ia_endpoints: ServerState, client: Client, task_id: object
) -> None:
    """Validate the task ID locally."""
    with pytest.raises(InvalidOptionError):
        await resolve(client.task_status(cast("Any", task_id)))
    assert ia_endpoints.requests == []


@pytest.mark.asyncio
async def test_closed_client_is_rejected(client: Client) -> None:
    """Refuse every task operation from a closed client."""
    await resolve(client.close())
    for call in (
        client.task_summary,
        lambda: client.task_status(1),
        lambda: client.queue_derive(IDENTIFIER),
    ):
        with pytest.raises(RuntimeError, match="closed"):
            await resolve(call())


def test_parsers_reject_non_objects_and_allow_absent_lists() -> None:
    """Guard direct parser use and treat an omitted list as empty."""
    with pytest.raises(InvalidServiceResponseError):
        _tasks.parse_summary(cast("Any", [1]))
    only_history = {"success": True, "value": {"history": [row(55)]}}
    assert _tasks.parse_status(only_history, 55).state is (
        InternetArchiveTaskState.FINISHED
    )
    assert _tasks.pending_derive({"success": True, "value": {}}, IDENTIFIER) is None


def test_zoned_submit_time_is_normalized_to_utc() -> None:
    """Convert an explicit offset rather than relabelling it as UTC."""
    data = listing([row(55, submittime="2026-11-25T11:00:00+01:00")])
    status = _tasks.parse_status(data, 55)
    assert status.submitted_at == datetime(2026, 11, 25, 10, tzinfo=UTC)
