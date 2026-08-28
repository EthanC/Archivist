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
```

::: archivist.services.internet_archive.client

::: archivist.services.internet_archive.async_client

::: archivist.services.internet_archive.models
