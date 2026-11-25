# Archivist

<p align="center">
  <a href="https://pypi.org/project/archivist-py/"><img src="https://img.shields.io/pypi/v/archivist-py" alt="PyPI version"></a>
  <a href="https://pypi.org/project/archivist-py/"><img src="https://img.shields.io/pypi/pyversions/archivist-py" alt="Supported Python versions"></a>
  <a href="https://github.com/EthanC/Archivist/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/ethanc/archivist/ci.yml" alt="Build status"></a>
  <a href="https://codecov.io/gh/ethanc/archivist"><img src="https://codecov.io/gh/ethanc/archivist/branch/main/graph/badge.svg" alt="Coverage report"></a>
  <a href="https://pypi.org/project/archivist-py/"><img src="https://img.shields.io/pypi/dm/archivist-py" alt="PyPI downloads"></a>
</p>

**Archive webpages and files with a typed Python API.**

Python clients for [Internet Archive](https://archive.org/) and
[Archive.today](https://archive.is/), with synchronous and asynchronous APIs.

## Features

- Search Internet Archive's Wayback capture history
- Save pages through Internet Archive's Save Page Now service
- Upload files to Internet Archive, recover interrupted transfers, and request item removal
- Check capture progress, service availability, and account capacity
- Browse Archive.today history and find first, latest, or closest captures
- Use the same models from synchronous and asynchronous code
- Keep credentials and sensitive URL data out of representations and logs

## Installation

Archivist requires Python 3.11 or later.

```console
uv add archivist-py
```

Using pip:

```console
pip install archivist-py
```

## Internet Archive

Find the oldest Wayback Machine capture without an account:

```python
from archivist import (
    InternetArchiveCdxRecord,
    InternetArchiveCdxResult,
    InternetArchiveClient,
)

with InternetArchiveClient() as client:
    captures: InternetArchiveCdxResult = client.search("https://example.com/", limit=1)

oldest: InternetArchiveCdxRecord | None = next(iter(captures), None)
if oldest is not None:
    print(f"{oldest.timestamp:%Y-%m-%d}: {oldest.archive_url()}")
```

Save a page anonymously and wait for the finished capture in one call:

```python
from archivist import InternetArchiveClient, InternetArchiveSuccessStatus

with InternetArchiveClient() as client:
    capture: InternetArchiveSuccessStatus = client.save("https://example.com/")

print(capture.archive_url())
```

Credentials are optional for the Wayback examples above. Provide them for account
features and restricted save options such as screenshots, emailed results, and
WACZ files.

Upload a file to a new Internet Archive item using your account:

```python
from pathlib import Path

from archivist import (
    InternetArchiveAccount,
    InternetArchiveClient,
    InternetArchiveUploadOptions,
)

account = InternetArchiveAccount("you@example.com", "your-password")
options = InternetArchiveUploadOptions(
    identifier="your-unique-item-id",
    title="Project notes",
    description="Notes from my project.",
    subjects=["notes"],
)
with InternetArchiveClient(account=account) as client:
    result = client.upload(Path("notes.txt"), options)

print(result.details_url)
```

Uploads are create-only. Interrupted transfers can be reconciled with
`get_item()` and recovered explicitly with `add_files()` after exact ownership,
provenance, filename, size, MD5, and SHA-1 checks. For upload recovery, options,
and item removal, see the
[Internet Archive guide](https://archivist.e3n.im/internet_archive/#upload-items).

When you already hold acknowledged facts about one missing original (the item's
provenance metadata and the file's exact size), `transfer_missing_file()` sends
a single guarded PUT instead:

| | `add_files()` | `transfer_missing_file()` |
| --- | --- | --- |
| Files per call | Any number | Exactly one |
| Local hashing | MD5 and SHA-1 of every source | None; size must equal `expected_size` |
| Existing filename | Accepted when checksums match | Deferred (`slot_occupied`) |
| Waiting and polling | Optional readiness and checksum verification | Never; one read of identity and metadata |
| Derive gating | Derive queued only on the last missing file | `queue_derive` applies to the one PUT |
| PUT | Plain PUT | Create-only, with `If-None-Match: *` |
| Refusals | `InternetArchiveRecoveryError` and its deferred subclass | Deferred or conflict error with a stable `reason` |
| Returns | `InternetArchiveRecoveryResult` | `InternetArchiveUploadResult` |

See [Transfer one missing file](https://archivist.e3n.im/internet_archive/#transfer-one-missing-file).

## Internet Archive tasks

Read your account's catalog task backlog, queue a derive, and track it:

```python
from archivist import (
    InternetArchiveApiKey,
    InternetArchiveClient,
    InternetArchiveTaskState,
)

key = InternetArchiveApiKey("access", "secret")
with InternetArchiveClient(api_key=key) as client:
    summary = client.task_summary()
    print(summary.queued, summary.running, summary.error, summary.paused)
    if summary.outstanding > 100:
        print("catalog backlog is high; defer new items")

    submission = client.queue_derive("your-unique-item-id")
    status = client.task_status(submission.task_id)
    print(status.state is InternetArchiveTaskState.FINISHED)
```

`upload_limit()` (IAS3 `check_limit`) doesn't count the catalog task backlog,
so it can report room while IA rejects new items with `503 SlowDown`.
`task_summary()` reads the backlog from the
[Tasks API](https://archive.org/developers/tasks.html). `queue_derive()` returns
the existing task with `already_pending=True` instead of submitting a second
derive when your account already has one queued, running or paused for the
item, so a retry after a lost response is safe. A failed derive does not count. Only `derive.php` can be submitted. See the
[Internet Archive guide](https://archivist.e3n.im/internet_archive/#catalog-tasks).

## Archive.today

Find the oldest Archive.today capture through its Memento interface:

```python
from archivist import ArchiveTodayClient, ArchiveTodayMemento

with ArchiveTodayClient() as client:
    oldest: ArchiveTodayMemento | None = client.first("https://example.com/")

if oldest is not None:
    print(f"{oldest.archived_at:%Y-%m-%d}: {oldest.archive_url}")
```

Archivist can browse existing Archive.today captures, but cannot create new
ones because the service's bot protection prevents reliable automated access.

## Async Support

Async clients mirror the synchronous API. Use `AsyncInternetArchiveClient` or
`AsyncArchiveTodayClient` with `async with`, then await the same operations.

## Documentation

Read the [full documentation](https://archivist.e3n.im/) for authentication,
save options, async examples, error handling, and the complete API reference.
