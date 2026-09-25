## Search captures

```python
from archivist import InternetArchiveCdxResult, InternetArchiveClient

with InternetArchiveClient() as client:
    captures: InternetArchiveCdxResult = client.search("https://example.com/", limit=10)

for capture in captures:
    print(capture.archive_url())
```

## Save a page

```python
from archivist import InternetArchiveClient, InternetArchiveSuccessStatus

with InternetArchiveClient() as client:
    capture: InternetArchiveSuccessStatus = client.save("https://example.com/")

print(capture.archive_url())
print(f"First archive: {capture.first_archive}")
```

Get the authenticated account's public web archive:

```python
from archivist import InternetArchiveAccount, InternetArchiveClient

account = InternetArchiveAccount("account@example.com", "password")
with InternetArchiveClient(account=account) as client:
    print(client.my_web_archive_url())
```

## Upload items

`upload()` creates an Archive.org item at
`https://archive.org/details/<identifier>`. It does not save a webpage to the
Wayback Machine. The upload form's "Page URL" is the item identifier; put an
originating URL in `metadata["source"]`.

```python
import os
from datetime import date
from pathlib import Path
from uuid import uuid4

from archivist import (
    InternetArchiveAccount,
    InternetArchiveClient,
    InternetArchiveUploadOptions,
)

account = InternetArchiveAccount(
    os.environ["INTERNET_ARCHIVE_EMAIL"],
    os.environ["INTERNET_ARCHIVE_PASSWORD"],
)
options = InternetArchiveUploadOptions(
    identifier=f"archivist-example-{uuid4().hex}",
    title="Synthetic protocol example",
    description="<p>A <b>test</b> item with Unicode: caf\u00e9.</p>",
    subjects=["software testing", "example"],
    creator="Example author",
    date=date(2026, 9, 25),
    language="eng",
    license="https://creativecommons.org/publicdomain/zero/1.0/",
    media_type="data",
    test_item=True,
    metadata={
        "source": "https://example.com/original",
        "custom_key": ["first value", "second value"],
    },
)
with InternetArchiveClient(account=account) as client:
    result = client.upload(
        [Path("example.txt"), Path("supplement.bin")],
        options,
        wait=True,
        timeout=600,
        poll_interval=5,
    )

print(result.details_url)
for file in result.files:
    print(file.name, file.size, file.transferred, file.etag)
```

The examples use `test_collection`. IA's
[item documentation](https://archive.org/developers/items.html#collections)
states that these items are removed after approximately 30 days. Use this
collection only for synthetic tests, not permanent content. Explicit removal is
still preferable when a test is finished.

Each upload example is an alternative: create a fresh `options.identifier` for
each call. Reusing the same identifier is rejected, even for an item you own.

### Authentication and sessions

Upload accepts any of these client constructor arguments:

- `api_key=InternetArchiveApiKey(access_key, secret_key)` uses LOW authentication.
- `cookies=InternetArchiveCookies(logged_in_user, logged_in_sig)` obtains upload
  credentials from the authenticated upload form.
- `account=InternetArchiveAccount(username, password)` logs in, then obtains
  upload credentials from that form. An account email can be used as `username`.

Removal requires `cookies` or `account`; an API key alone is insufficient for
`remove_items()`. Do not log cookie values, keys, passwords, or the upload form's
credential-bearing bootstrap HTML. Archivist does not load `.env` files.

Clients create sessions with retries disabled. **An injected `niquests.Session`
or `niquests.AsyncSession` MUST use `retries=0`.** Item operations reject
retry-enabled sessions during preflight, before authentication or mutation;
transport retries could otherwise replay a request. Caller-supplied sessions
remain caller-owned and must be closed by the caller.

```python
import niquests

from archivist import InternetArchiveClient

with niquests.Session(retries=0) as session:
    with InternetArchiveClient(session=session, account=account, timeout=60) as client:
        result = client.upload(["example.txt"], options)
```

### Upload options

`InternetArchiveUploadOptions` is a frozen dataclass. It defensively copies
`subjects` and repeated custom metadata values into lists. The custom metadata
mapping is read-only, but its lists and the subjects list can be edited in place.
All list values are revalidated before upload.

| Option | Required/default | Meaning |
| --- | --- | --- |
| `identifier` | Required | Exact new item identifier. Starts with an ASCII letter or digit; remaining characters are ASCII letters, digits, `_`, `.`, or `-`. No automatic renaming. |
| `title` | Required | Nonblank item title. |
| `description` | Required | Nonblank description; HTML and Unicode are preserved. |
| `subjects` | Required | Nonempty list of nonblank strings. Pass separate tags, not a comma-separated string. |
| `creator` | `None` | Creator of the content. |
| `date` | `None` | Creation/publication date as a `datetime.date`, for example `date(2026, 9, 25)`. Serialized as `YYYY-MM-DD`; strings and `datetime` timestamps are rejected. |
| `collection` | Selected from media type | Collection identifier, subject to server permissions. |
| `language` | `None` | Language value, for example `"eng"`. |
| `license` | `None` | License URL, serialized as `licenseurl`; no license is assigned by default. |
| `media_type` | `"data"` | One of `movies`, `audio`, `texts`, `software`, `image`, or `data`. |
| `test_item` | `False` | Selects `test_collection`. Cannot be combined with another explicit collection. |
| `metadata` | Empty mapping | Additional metadata, including `source`; each value is a nonblank string or nonempty list of nonblank strings. |

Without `test_item` or an explicit collection, the defaults are:

| Media type | Collection |
| --- | --- |
| `movies` | `opensource_movies` |
| `audio` | `opensource_audio` |
| `texts` | `opensource` |
| `software` | `open_source_software` |
| `image` | `opensource_image` |
| `data` | `opensource_media` |

Text values reject Unicode surrogates and control characters, except that
`description` and custom metadata values preserve carriage returns, newlines,
and tabs. Custom keys must match `[a-z][a-z0-9_-]*`. Dedicated option names,
their aliases, and server-owned fields such as `identifier`, `uploader`,
`addeddate`, `publicdate`, `updatedate`, and `backup_location` are reserved.
Custom metadata cannot override them. Underscores are escaped as double hyphens
in header names. Each key must survive encoding and server decoding unchanged:
`custom_key` is accepted, but `custom--key` is ambiguous and rejected even when
used alone. Keys that collide after encoding are also rejected. Numbered headers
use consistent zero-padding so repeated values retain their order even when
indices reach 100 or more.

`upload(files, options, *, wait=False, timeout=300.0, poll_interval=2.0)` accepts
a single `Path`, or an iterable of local path strings, `Path` objects, or
`InternetArchiveUploadFile` objects. For one file, use
`client.upload(Path("example.txt"), options)` without wrapping the path in a list.
Bare path strings still require an iterable. `timeout` is the ingest-polling
budget after transfer, not an overall upload deadline. The client's constructor `timeout`
sets connection and read-inactivity timeouts for each request. Both timeout
values and `poll_interval` must be positive finite numbers.

### Streams and filenames

`InternetArchiveUploadFile(source, name=None)` accepts a local path or a
readable, seekable binary stream. Paths default to their basename; streams
require an explicit destination name.

```python
from io import BytesIO

from archivist import InternetArchiveUploadFile

with BytesIO(b"skip:payload to archive") as stream:
    stream.seek(5)
    file = InternetArchiveUploadFile(stream, name="folder/payload.txt")
    with InternetArchiveClient(account=account) as client:
        result = client.upload([file], options)
    assert not stream.closed
```

Only bytes from the stream's current offset to its end are uploaded. Validation
restores that offset after inspecting the source; transfer consumes the stream
and does not promise to restore it. Archivist never closes caller-owned streams,
including on failure or async cancellation. It closes the file handles it opens
for paths. Keep sources open and unchanged for the duration of the call.
Files are streamed sequentially in chunks of at most 64 KiB. Reusing the same
stream object for multiple files is rejected; use separate streams instead.

The async client prepares sources, reads chunks, and closes owned handles in
worker threads so slow source I/O does not block the event loop. Cancellation
waits for an in-flight source operation to finish before cleanup and returning
control to the caller. It cannot interrupt a blocking source operation, but
unrelated coroutines remain able to run during that wait.

The complete batch is validated before authentication or mutation. Client-side
restrictions are conservative safety checks, not claims about every filename
the server accepts:

- Names must be nonblank item-relative paths. Forward-slash subdirectories and
  Unicode are allowed; absolute paths, Windows drives, backslashes, empty path
  components, `.` and `..` components, controls, and surrogates are rejected.
- Duplicate destination names are rejected.
- Zero remaining bytes are rejected, matching the deployed upload form's
  zero-byte-file restriction.
- Exact root-level names `<identifier>_meta.xml`, `<identifier>_files.xml`,
  `<identifier>_dc.xml`, `<identifier>_meta.sqlite`, and
  `<identifier>_archive.torrent` are reserved for generated item files.

IA's help guide recommends simple ASCII names, but live protocol checks verified
a Unicode filename. The client does not invent identifier-length, file-size,
total-size, or file-count caps from published recommendations. Server capacity,
permissions, and quota limits still apply and can reject a request.

### Creation and failures

Availability lookup is advisory. Before any file transfer, Archivist performs a
standalone, zero-body bucket PUT with item metadata and without auto-create or
ignore-preexisting headers. Live verification confirmed that a duplicate bucket
PUT returns HTTP 409 `BucketAlreadyExists` before the item becomes visible in
the Metadata API, leaving the original metadata unchanged. File uploads start
only after the creation response is accepted.

The client does not append files to an existing item, silently rename an
identifier, retry a mutation, follow mutation redirects, or automatically delete
a partially uploaded item. Redirect responses are errors, not permission to
forward credentials or replay file bodies elsewhere.

Local validation and advisory availability failures can raise `InvalidOptionError`;
authentication and other pre-creation failures can raise service errors directly.
Service failures once bucket creation starts raise `InternetArchiveUploadError`:

```python
from archivist import InternetArchiveUploadError

with InternetArchiveClient(account=account) as client:
    try:
        result = client.upload(["example.txt"], options, wait=True)
    except InternetArchiveUploadError as error:
        print(error.result.identifier)
        print(error.failed_file)
        print(type(error.cause).__name__, error.cause.status_code)
        for file in error.result.files:
            print(file.name, file.transferred)
        raise
```

`error.result` retains all prepared file outcomes. `error.cause` is the underlying
service error, such as a rate limit, network failure, or polling timeout.
`error.failed_file` names the failing transfer, or is `None` for creation or
polling failures. `transferred=True` records an acknowledged transfer, not
finished server processing. `False` means no successful acknowledgement; the
file may have failed or not been attempted.

A lost response can leave creation or transfer outcome unknown even when the
server accepted the request. There is no confirmed-creation flag on the partial
result. Do not treat `error.result.identifier` as proof of ownership, retry the
upload blindly, or remove that identifier automatically. Resolve uncertainty
with read-only checks, for example a unique `source` marker supplied by this
operation. A creation conflict must never trigger cleanup of a preexisting item.
A later file-transfer conflict does not rule out creation by this operation;
cleanup still requires independent ownership proof.

### Ingest polling

With `wait=False`, the result records file transfers and has
`processing_complete=False`. With `wait=True`, the client polls
`/upload/app/upload_api.php?name=catalogRows&identifier=<identifier>` until no
`archive.php` ingest task remains, subject to the polling budget. It does not use
the legacy `/catalog_status.php` XML helper. Failed task states and malformed
responses are surfaced as errors.

The synchronous client checks its polling budget between requests and caps
each request's connection and read-inactivity timeouts by the remaining budget.
An in-flight request can exceed that budget while the server continues sending
data; this is not a hard wall-clock cutoff. The async client also applies an
`asyncio.timeout` deadline, which can interrupt awaited network work. Neither
polling deadline covers the preceding file transfers.

`processing_complete=True` does not promise completed derivatives, search
indexing, or public availability. Metadata and downloads can lag behind transfer
acceptance; verify them separately when the application requires those states.

## Remove items

```python
with InternetArchiveClient(account=account) as client:
    removals = client.remove_items(
        [result.identifier],
        comment="Finished synthetic upload example",
    )
for removal in removals:
    print(removal.identifier, removal.accepted, removal.task_id)
```

`remove_items(identifiers, *, comment="Removed with Archivist")` requires a
nonempty iterable of unique identifiers, not one comma-separated string, and a
nonblank comment. Supply only identifiers you intend to remove and are allowed
to manage. It submits the library's `/manage/` `make_dark` operation, not
individual-file deletion or My Web Archive removal.

Each `InternetArchiveRemovalResult` reports `identifier`, `accepted`, and an
optional `task_id`. Acceptance requires an explicit per-item acknowledgement in
the HTML response; HTTP 200 alone is insufficient. In a partially acknowledged
batch, unacknowledged items have `accepted=False`; this is not proof that the
server did nothing. Missing or malformed acknowledgements can raise an error.
Do not resubmit an ambiguous removal automatically.

An accepted task can wait behind other item processing. To confirm darkness,
poll `https://archive.org/metadata/<identifier>` separately for `is_dark: true`;
an empty object alone is not confirmation. Making an item dark removes public
access and is not a promise of permanent erasure. `remove_items()` returns queue
acceptance without waiting for that final state.

## Async item operations

The async client has the same options, results, ownership rules, and errors:

```python
from archivist import AsyncInternetArchiveClient


async def upload_example() -> None:
    async with AsyncInternetArchiveClient(account=account) as client:
        result = await client.upload(["example.txt"], options, wait=True)
        removals = await client.remove_items([result.identifier])
        print(removals[0].accepted)
```

## Live mutation tests

Normal tests do not load `.env` or contact Archive.org. Item lifecycle tests
require **both** `ARCHIVIST_RUN_LIVE=1` and `ARCHIVIST_RUN_LIVE_MUTATIONS=1`, plus
`INTERNET_ARCHIVE_EMAIL` and `INTERNET_ARCHIVE_PASSWORD` in the environment.
They cover single- and multi-file uploads with both clients, verify metadata and
downloaded bytes, then verify the unique `source` marker before requesting
removal and polling for darkness. Ownership recovery also runs after upload
failures, cancellation, and keyboard interruption. Cleanup is shielded from
cancellation and bounded to 65 minutes, including up to 60 minutes waiting for
darkness. Cleanup failures add sanitized notes with the identifier to the
original exception instead of replacing it. Without an original exception, a
cleanup failure fails the test. Unconfirmed ownership never authorizes deletion,
regardless of upload success or conflict status.

For PowerShell, after securely providing the account environment variables:

```powershell
$env:ARCHIVIST_RUN_LIVE = "1"
$env:ARCHIVIST_RUN_LIVE_MUTATIONS = "1"
uv run pytest tests/live/test_items.py -m live --no-cov
```

Do not enable these gates in ordinary CI or enable transport debug logs. These
tests mutate real `test_collection` items, even though their payloads are tiny.

## API reference

::: archivist.services.internet_archive.client

::: archivist.services.internet_archive.async_client

::: archivist.services.internet_archive.models

::: archivist.services.internet_archive.item_models
