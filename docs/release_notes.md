# 1.6.0 release notes

## Added

- `task_summary()`, `queue_derive()`, and `task_status()` on
  `InternetArchiveClient` and `AsyncInternetArchiveClient`, backed by the
  [Tasks API](https://archive.org/developers/tasks.html), with the models
  `InternetArchiveTaskSummary`, `InternetArchiveTaskSubmission`,
  `InternetArchiveTaskStatus`, `InternetArchiveTaskState`, and
  `InternetArchiveTaskNotFoundError`.
  - `task_summary()` returns the account's queued, running, error, and paused
    catalog tasks. IAS3 `check_limit`, behind `upload_limit()`, does not count
    this backlog.
  - `queue_derive()` submits only `derive.php`. It returns the existing task
    with `already_pending=True` when a derive is already queued, running or
    paused for the item, so a retry after a lost response is safe. Failed
    derives are not counted, and a new derive is submitted.
  - `task_status()` returns an `InternetArchiveTaskStatus`. Unrecognized
    `wait_admin` values map to `UNKNOWN` and keep the raw value. A history row
    means `FINISHED`; `finished_at` is always `None`, and the row's opaque
    integer `finished` value is exposed as `finished_marker`. A missing task
    raises `InternetArchiveTaskNotFoundError`.
  - Task requests report `task-summary`, `task-submit-check`, `task-submit`,
    and `task-status` request phases.
- `transfer_missing_file()` on `InternetArchiveClient` and
  `AsyncInternetArchiveClient`. It PUTs one missing original into an item the
  caller owns after a fresh `whoami` and metadata read, with no hashing,
  polling, checksum verification, or derive gating. The PUT carries
  `If-None-Match: *` and is sent once through the mounted session adapter. It
  returns `InternetArchiveUploadResult`; a failed or unacknowledged PUT raises
  `InternetArchiveUploadError`. Callers that subclassed the client to use its
  private recovery helpers should switch to it.
  - `InternetArchiveRecoveryConflictError`, a subclass of
    `InternetArchiveRecoveryError`, for refusals that need operator action.
  - `InternetArchiveRecoveryReason`, a stable enum of why a transfer was
    deferred or refused: `metadata_unresolved`, `slot_occupied`,
    `listing_not_authoritative`, `ownership_mismatch`, `item_restricted`,
    `size_changed`, and `provenance_missing`.
- `parse_retry_after` is exported from `archivist` (and `archivist.core`).

## Changed

- `InternetArchiveRecoveryError` and `InternetArchiveRecoveryDeferredError`
  gain a `reason` attribute, which is `None` outside `transfer_missing_file()`.

## Breaking changes

- 503 SlowDown and 429 responses now raise `RateLimitError` on every Internet
  Archive path. Before, only the task methods did, and every other path raised
  `InvalidServiceResponseError`. A 503 with only a `Retry-After` header is also
  a `RateLimitError`; a 503 with neither stays `InvalidServiceResponseError`.
  The error keeps `status_code`, `service_code`, `service_message`,
  `request_id`, `request_phase`, and `retry_after`.

## Known limitations

- The `already_pending` check in `queue_derive()` only sees derives submitted
  by the caller's own account.
- `queue_derive()` has not yet submitted a task against the live API. Its
  read-only checks and every other task method were verified live.
