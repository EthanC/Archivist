# Archivist

<p align="center">
  <a href="https://pypi.org/project/archivist-py/"><img src="https://img.shields.io/pypi/v/archivist-py" alt="PyPI version"></a>
  <a href="https://pypi.org/project/archivist-py/"><img src="https://img.shields.io/pypi/pyversions/archivist-py" alt="Supported Python versions"></a>
  <a href="https://github.com/EthanC/Archivist/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/ethanc/archivist/ci.yml" alt="Build status"></a>
  <a href="https://codecov.io/gh/ethanc/archivist"><img src="https://codecov.io/gh/ethanc/archivist/branch/main/graph/badge.svg" alt="Coverage report"></a>
  <a href="https://pypi.org/project/archivist-py/"><img src="https://img.shields.io/pypi/dm/archivist-py" alt="PyPI downloads"></a>
</p>

**Preserve the web and retrieve archives with a typed Python API.**

Archivist gives Python applications a consistent interface to the stable APIs
published by [Internet Archive](https://archive.org/) and
[Archive.today](https://archive.is/). Search capture history, find the version
nearest a point in time, or submit a page to the Wayback Machine without
building around each service's response format.

Both synchronous and asynchronous clients are included, with matching models
and errors.

## Features

- Search Internet Archive's Wayback capture history
- Save pages through Internet Archive's Save Page Now service
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

Credentials are optional. Provide them for account features and restricted save
options such as screenshots, emailed results, and WACZ files.

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
