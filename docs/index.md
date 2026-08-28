# Archivist

Preserve the web and retrieve archives with a typed Python API.

## Installation

**Archivist requires Python 3.11 or later.**

Add Archivist to a [uv](https://github.com/astral-sh/uv) project:

```console
uv add archivist-py
```

The package is also available through pip:

```console
pip install archivist-py
```

## Internet Archive

Capture pages anonymously or provide credentials when using account features
and restricted save options.

```py
from archivist import InternetArchiveClient

with InternetArchiveClient() as client:
    capture = client.save("https://example.com/")
    print(capture.wayback_timestamp)
```

## Archive.today

Archive.today access uses its Memento and RSS interfaces. HTML search and
capture submission are not part of the client.

```py
from datetime import UTC, datetime

from archivist import ArchiveTodayClient

with ArchiveTodayClient() as client:
    history = client.timemap("https://example.com/")
    closest = client.closest("https://example.com/", datetime(2025, 1, 1, tzinfo=UTC))
    recent = client.recent_captures()
```

## Asynchronous clients

Async methods return the same models and errors as synchronous methods.

```py
import asyncio

from archivist import AsyncArchiveTodayClient


async def main() -> None:
    async with AsyncArchiveTodayClient() as client:
        history = await client.timemap("https://example.com/")
        print(history.items)


asyncio.run(main())
```

## API reference

- [Core models, protocols, and exceptions](core.md)
- [Internet Archive](internet_archive.md)
- [Archive.today](archive_today.md)

## Logging

Archivist emits records under the `archivist` logger. The package does not set
an application log level or install an output handler.

```py
import logging

logging.basicConfig(level=logging.INFO)
logging.getLogger("archivist").setLevel(logging.INFO)
```

Logs cover submissions, job IDs, state changes, completion, rate limits, and
parsing failures. Target URL query strings and user information are removed
before logging.
