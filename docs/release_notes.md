# 1.6.0 release notes

## Internet Archive tasks

- Added `task_summary()`, `queue_derive()`, and `task_status()` to
  `InternetArchiveClient` and `AsyncInternetArchiveClient`, backed by the
  [Tasks API](https://archive.org/developers/tasks.html).
- `task_summary()` returns `InternetArchiveTaskSummary`, the account's queued,
  running, error, and paused catalog tasks. IAS3 `check_limit`, behind
  `upload_limit()`, does not count this backlog.
- `queue_derive()` submits only `derive.php`. It returns the existing task
  with `already_pending=True` when a derive is already queued or running for
  the item, so a retry after a lost response is safe.
- `task_status()` returns `InternetArchiveTaskStatus` with an
  `InternetArchiveTaskState`. Unrecognized `wait_admin` values map to `UNKNOWN`
  and keep the raw value. A history row means `FINISHED`; `finished_at` is
  always `None`, and the row's opaque integer `finished` value is exposed as
  `finished_marker`. A missing task raises `InternetArchiveTaskNotFoundError`.
- Task requests report `task-summary`, `task-submit-check`, `task-submit`, and
  `task-status` request phases. HTTP 503 `SlowDown` from these methods raises
  `RateLimitError`; other methods are unchanged.

No existing API changed.

## Known limitations

- `upload_limit()` and the upload paths raise `InvalidServiceResponseError` for
  an HTTP 503 `SlowDown`, while the new task methods raise `RateLimitError`.
  Unifying them is left for a future major version.
- The `already_pending` check in `queue_derive()` only sees derives submitted
  by the caller's own account.
- `queue_derive()` has not yet submitted a task against the live API. Its
  read-only checks and every other task method were verified live.
