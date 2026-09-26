# Internet Archive File-Level Upload Recovery

## Proposed Contract

Add explicit `get_item()` and `add_files()` operations to both Internet Archive
clients. `upload()` remains create-only, including when the existing item belongs
to the caller and has matching provenance.

Agreed policies:

- `wait=True` uses read-only polling until reconciliation is safe, then waits for
  ingest and checksum verification under one shared deadline.
- An existing file matches only when its exact name, size, MD5, and SHA-1 match.
  Either missing digest defers recovery.

## Implementation Plan

### 1. Typed Item Snapshots

Extend `src/archivist/services/internet_archive/item_models.py` with
`InternetArchiveItem`, file records, and task records. Include metadata,
server-maintained uploader, exact filenames, optional sizes/checksums, file source
classification, task states, `pending_tasks`, `has_redrow`, `is_dark`, `nodownload`,
and server-unavailability information. Retain all filenames for collision
detection, including derivatives and generated files; expose original files
separately.

Implement `get_item(identifier)` using
`/metadata/{identifier}?extended_err=1`. Distinguish pending creation,
unavailable/deleted items, uncertain visibility, and malformed responses. Preserve
IA's extended error code. Empty responses mean visibility is unresolved, never
permission to create or write. Missing checksums remain missing rather than
receiving invented defaults.

### 2. Credential-Bound Ownership

Reuse the current API-key/bootstrap logic, then pin the resolved LOW key pair for
the recovery operation. Authenticate `services/user.php?op=whoami` with that exact
pair and no cookies, and compare its returned account identity with
`metadata.uploader`.

Reject missing, malformed, ambiguous, or mismatched ownership evidence. Do not
use the configured account email, cookie identity, operation marker, or general
write privileges as substitutes. Mixed credentials must not let one account's
cookies authorize another account's keys.

### 3. Provenance and Validation

Add the following API, with the same source forms as `upload()`:

```python
add_files(
    identifier,
    files,
    *,
    expected_metadata,
    wait=False,
    timeout=300.0,
    poll_interval=2.0,
)
```

`expected_metadata` will be a required, nonempty typed mapping of exact metadata
expectations, not metadata to write. Saga's documented example will supply
`source` and a persisted operation-marker field. Compare complete values, not
substring or list-membership matches; do not silently canonicalize URLs.

Reuse source/name validation and handle ownership. Hash every source with bounded
reads before any PUT, preserving its starting position. Keep async source I/O off
the event loop and drain active workers before cleanup on cancellation. Document
that source contents must remain unchanged throughout the operation.

### 4. Reconciliation and Transfers

Inspect fresh metadata and ingest/catalog state before the first write. Reject
any known conflict across the entire batch before transferring otherwise-missing
files. Missing checksums, incomplete listings, pending ingestion, unknown task
states, unavailable items, and inconsistent metadata cannot authorize writes.
Error tasks produce an operator-visible failure, not automatic task resubmission.

With `wait=False`, unresolved preflight raises a typed deferral with zero PUTs.
With `wait=True`, poll only reads and reconsider the complete batch once state
becomes usable.

Before each transfer, refresh ownership/provenance-relevant item state, task
state, and filename reconciliation. If state becomes unresolved after an earlier
transfer, stop or poll according to `wait`; retain the earlier acknowledgment and
leave later files unattempted.

PUT only missing file paths. Never call identifier availability, bucket creation,
metadata mutation, removal, or rollback. Do not send auto-create or item-metadata
headers. Preserve derive scheduling without introducing a dummy PUT or task
submission.

### 5. Results and Errors

Keep acknowledgment, ingest, and verification separate. Extend the existing
result/error family additively with explicit per-file dispositions,
checksum-verification state, and failure phase. Preserve `transferred` as
acknowledgment of this call's transfer, not "already exists," and preserve the
narrow meaning of `processing_complete`.

Results and errors will identify transferred, already-matching, unattempted,
conflicting, deferred, and uncertain files. Errors retain the underlying
`ServiceError`, HTTP information, failed filename, and phase such as ownership,
reconciliation, transfer, ingest, or verification.

A lost response stops the operation with an uncertain file; it does not trigger
retransmission. An ETag is retained only as acknowledgment metadata. External
async cancellation continues to propagate after cleanup; documentation will
require reconciliation after any interrupted mutation.

### 6. Recovery Transport

Isolate recovery transport without changing Wayback behavior. Build recovery
requests from explicit headers and parameters so injected session cookies,
authentication, hooks, or defaults cannot introduce another identity, metadata
changes, auto-creation, or replay.

Preserve session ownership, proxy/TLS settings, explicit request timeouts, and
bounded streaming. Keep redirects and transport retries disabled. Address the
installed niquests synchronous `TypeError` resend fallback with a
recovery-specific one-shot mutation path, backed by a regression test rather than
assuming `retries=0` covers it.

Use one monotonic deadline across readiness checks, inter-file waiting, and final
verification; never reset it per poll or file. Preserve `upload()`'s existing
timeout contract and document the synchronous client's existing wall-clock
limitations.

### 7. Exports and Documentation

Add public models/errors to the service and top-level exports. Update
`docs/internet_archive.md`, with shorter pointers in `README.md` and the
documentation landing page.

Include synchronous and asynchronous examples covering persisted provenance
before initial upload, read-only `get_item()` reconciliation after uncertainty,
explicit recovery, deferrals, and operator handling. State the single-writer
requirement across processes, restarts, and manual edits. Do not claim atomic
no-overwrite protection: the reviewed IAS3 documentation does not establish
conditional-write guarantees.

## Verification

Extend the shared sync/async tests and loopback server with changing metadata,
stored files, checksums, identity responses, task transitions, and complete
request recording.

Coverage will include:

- Unchanged create-only uploads, including same-owner existing identifiers.
- Missing, matching, conflicting, and mixed batches; derivative/generated-name
  collisions.
- Wrong provenance/uploader, unavailable identity, mixed credentials, and
  contaminated session defaults.
- Empty/delayed metadata, either missing digest, pending/error tasks, and changes
  between preflight and transfer.
- Lost responses followed by a new client instance and explicit reconciliation.
- Repeated completed recovery calls that perform reads but no PUTs.
- Deadline exhaustion, cancellation during hashing/transfers/polling, and owned
  versus borrowed resource cleanup.
- Unchanged user metadata and exact assertions against bucket creation, deletion,
  rollback, redirects, and mutation retries.

Run focused offline tests, then the full coverage suite, Ruff checks without
autofix, type checking, documentation build, and package checks. Live mutations
are not needed for this test plan.

## Scope

Overwrite mode, job orchestration, and unrelated Wayback changes remain out of
scope. Recovery is file-level, not byte-offset or multipart resumability.
Conflicting or unresolved content stays untouched for operator recovery.

Implementation and offline verification are complete. Live mutations remain
excluded from the verification plan.
