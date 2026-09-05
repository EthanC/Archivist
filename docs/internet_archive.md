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

::: archivist.services.internet_archive.client

::: archivist.services.internet_archive.async_client

::: archivist.services.internet_archive.models
