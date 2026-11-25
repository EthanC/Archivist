"""Pure Tasks API helpers shared by the synchronous and async clients.

Response shapes follow https://archive.org/developers/tasks.html: every reply
is ``{"success": ..., "value": ..., "error": ...}``. ``value.summary`` holds
the account's ``queued``/``running``/``error``/``paused`` counts, listings put
live rows in ``value.catalog`` and finished rows in ``value.history``, and a
submission returns ``value.task_id`` and ``value.log``. No helper performs
network I/O.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import cast

from archivist.core._diagnostics import (
    MAX_FIELD_LENGTH,
    MAX_MESSAGE_LENGTH,
    _safe_text,
)
from archivist.core.errors import (
    InvalidOptionError,
    InvalidServiceResponseError,
)
from archivist.services.internet_archive import _common, _items
from archivist.services.internet_archive.item_models import (
    InternetArchiveTaskNotFoundError,
    InternetArchiveTaskState,
    InternetArchiveTaskStatus,
    InternetArchiveTaskSubmission,
    InternetArchiveTaskSummary,
    _validate_identifier,
)

TASKS_URL = "https://archive.org/services/tasks.php"
READ_TIMEOUT = _items.UPLOAD_LIMIT_TIMEOUT
DERIVE = "derive.php"

PHASE_SUMMARY = "task-summary"
PHASE_SUBMIT_CHECK = "task-submit-check"
PHASE_SUBMIT = "task-submit"
PHASE_STATUS = "task-status"

_SERVICE = _common.SERVICE
_MIN_PRIORITY = -10
_MAX_PRIORITY = 10
_WAIT_ADMIN_STATES = {
    0: InternetArchiveTaskState.QUEUED,
    1: InternetArchiveTaskState.RUNNING,
    2: InternetArchiveTaskState.ERROR,
    9: InternetArchiveTaskState.PAUSED,
}
# Paused derives count as pending: IA staff usually hold them on purpose, so a
# second derive must not be queued alongside one. Failed (ERROR) derives do not.
_PENDING = frozenset(
    {
        InternetArchiveTaskState.QUEUED,
        InternetArchiveTaskState.RUNNING,
        InternetArchiveTaskState.PAUSED,
    }
)
_SUMMARY_FIELDS = ("queued", "running", "error", "paused")


def _invalid(message: str) -> InvalidServiceResponseError:
    return InvalidServiceResponseError(message, service=_SERVICE)


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _validate_task_id(task_id: object) -> int:
    if not _is_int(task_id) or cast("int", task_id) <= 0:
        raise InvalidOptionError("task_id must be a positive integer")
    return cast("int", task_id)


def _value(data: Mapping[str, object]) -> Mapping[str, object]:
    """Require ``success: true`` and return the ``value`` object."""
    if not isinstance(data, Mapping):
        raise _invalid("Internet Archive returned invalid task data")
    if data.get("success") is not True:
        raise InvalidServiceResponseError(
            "Internet Archive rejected the task request",
            service=_SERVICE,
            service_message=_safe_text(data.get("error"), MAX_MESSAGE_LENGTH),
        )
    value = data.get("value")
    if not isinstance(value, Mapping):
        raise _invalid("Internet Archive returned invalid task data")
    return cast("Mapping[str, object]", value)


def summary_params() -> dict[str, str]:
    """Return the account summary query."""
    return {"summary": "1"}


def parse_summary(data: Mapping[str, object]) -> InternetArchiveTaskSummary:
    """Read the four documented counts; booleans and numeric strings are invalid."""
    summary = _value(data).get("summary")
    if not isinstance(summary, Mapping):
        raise _invalid("Internet Archive returned an invalid task summary")
    counts = [summary.get(name) for name in _SUMMARY_FIELDS]
    if not all(_is_int(count) and cast("int", count) >= 0 for count in counts):
        raise _invalid("Internet Archive returned an invalid task summary")
    return InternetArchiveTaskSummary(*cast("list[int]", counts))


def derive_body(
    identifier: str,
    priority: int | None,
    args: Mapping[str, str] | None,
) -> dict[str, object]:
    """Validate options and build the derive submission; the command is fixed."""
    _validate_identifier(identifier)
    if args is None:
        args = {}
    if not isinstance(args, Mapping) or not all(
        isinstance(key, str) and key and isinstance(value, str)
        for key, value in args.items()
    ):
        raise InvalidOptionError("args must map non-empty strings to strings")
    body: dict[str, object] = {
        "identifier": identifier,
        "cmd": DERIVE,
        "args": dict(args),
    }
    if priority is not None:
        if not _is_int(priority) or not _MIN_PRIORITY <= priority <= _MAX_PRIORITY:
            raise InvalidOptionError("priority must be an integer from -10 to 10")
        body["priority"] = priority
    return body


def encode_body(body: Mapping[str, object]) -> bytes:
    """Serialize a submission as compact UTF-8 JSON."""
    return json.dumps(body, separators=(",", ":")).encode()


def pending_derive_params(identifier: str) -> dict[str, str]:
    """List live derive tasks for one item."""
    _validate_identifier(identifier)
    return {"identifier": identifier, "cmd": DERIVE, "catalog": "1", "summary": "0"}


def status_params(task_id: int) -> dict[str, str]:
    """List live and finished rows for one task ID."""
    return {
        "task_id": str(_validate_task_id(task_id)),
        "history": "1",
        "catalog": "1",
    }


def _submitted_at(value: object) -> datetime:
    """Read ``submittime``, observed as zone-less UTC ``YYYY-MM-DD HH:MM:SS.ffffff``."""
    if not isinstance(value, str):
        raise _invalid("Internet Archive returned an invalid task timestamp")
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError:
        raise _invalid("Internet Archive returned an invalid task timestamp") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _rows(value: Mapping[str, object], kind: str) -> list[Mapping[str, object]]:
    rows = value.get(kind)
    if rows is None:
        return []
    if not isinstance(rows, list) or not all(isinstance(row, Mapping) for row in rows):
        raise _invalid("Internet Archive returned invalid task rows")
    return cast("list[Mapping[str, object]]", rows)


def _parse_row(
    row: Mapping[str, object], *, finished: bool
) -> InternetArchiveTaskStatus:
    task_id = row.get("task_id")
    identifier = row.get("identifier")
    command = row.get("cmd")
    server = row.get("server")
    if (
        not _is_int(task_id)
        or cast("int", task_id) <= 0
        or not isinstance(identifier, str)
        or not identifier
        or not isinstance(command, str)
        or not command
        or (server is not None and not isinstance(server, str))
    ):
        raise _invalid("Internet Archive returned an invalid task row")
    # History rows omit wait_admin, status, color, and args; only live rows
    # carry a run state.
    raw = row.get("wait_admin")
    marker = row.get("finished")
    wait_admin = cast("int", raw) if _is_int(raw) else None
    if finished:
        state = InternetArchiveTaskState.FINISHED
    elif wait_admin is None:
        state = InternetArchiveTaskState.UNKNOWN
    else:
        state = _WAIT_ADMIN_STATES.get(wait_admin, InternetArchiveTaskState.UNKNOWN)
    return InternetArchiveTaskStatus(
        task_id=cast("int", task_id),
        identifier=identifier,
        cmd=command,
        state=state,
        submitted_at=_submitted_at(row.get("submittime")),
        # History ``finished`` is an opaque integer, not a Unix time: a task
        # submitted 2026-11-01 reported 4181123504. Membership in history is
        # the finish signal, so no finish time is reported.
        finished_marker=cast("int", marker) if finished and _is_int(marker) else None,
        server=server or None,
        wait_admin=wait_admin,
        raw_state=None if raw is None else _safe_text(str(raw), MAX_FIELD_LENGTH),
    )


def parse_status(data: Mapping[str, object], task_id: int) -> InternetArchiveTaskStatus:
    """Return the matching row, preferring a finished history row over a live one."""
    value = _value(data)
    catalog = [_parse_row(row, finished=False) for row in _rows(value, "catalog")]
    history = [_parse_row(row, finished=True) for row in _rows(value, "history")]
    for status in (*history, *catalog):
        if status.task_id == task_id:
            return status
    raise InternetArchiveTaskNotFoundError(task_id)


def pending_derive(
    data: Mapping[str, object], identifier: str
) -> InternetArchiveTaskSubmission | None:
    """Return the oldest queued, running or paused derive for the item, if any."""
    value = _value(data)
    pending = [
        status
        for status in (
            _parse_row(row, finished=False) for row in _rows(value, "catalog")
        )
        if status.identifier == identifier
        and status.cmd == DERIVE
        and status.state in _PENDING
    ]
    if not pending:
        return None
    task_id = min(status.task_id for status in pending)
    return InternetArchiveTaskSubmission(task_id, identifier, already_pending=True)


def parse_submission(
    data: Mapping[str, object], identifier: str
) -> InternetArchiveTaskSubmission:
    """Read the new task ID and optional log URL from a submission reply."""
    value = _value(data)
    task_id = value.get("task_id")
    log_url = value.get("log")
    if not _is_int(task_id) or cast("int", task_id) <= 0:
        raise _invalid("Internet Archive returned an invalid task ID")
    if log_url is not None and not isinstance(log_url, str):
        raise _invalid("Internet Archive returned an invalid task log URL")
    return InternetArchiveTaskSubmission(
        cast("int", task_id), identifier, log_url or None
    )
