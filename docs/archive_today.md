Archive.today support is limited to its RFC 7089 Memento interface and standard
RSS feed. The package does not expose HTML search or capture submission.

## List captures

```python
from archivist import ArchiveTodayClient, ArchiveTodayTimeMap

with ArchiveTodayClient() as client:
    captures: ArchiveTodayTimeMap = client.timemap("https://example.com/")

for capture in captures:
    print(capture.archive_url)
```

## Find the closest capture

```python
from datetime import UTC, datetime

from archivist import ArchiveTodayClient, ArchiveTodayMemento

with ArchiveTodayClient() as client:
    capture: ArchiveTodayMemento | None = client.closest(
        "https://example.com/", datetime(2025, 1, 1, tzinfo=UTC)
    )

if capture is not None:
    print(capture.archive_url)
```

::: archivist.services.archive_today.client

::: archivist.services.archive_today.async_client

::: archivist.services.archive_today.models
