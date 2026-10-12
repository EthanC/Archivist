# 1.4.0 release notes

## Service response diagnostics

- Added optional `ServiceError` fields: `service_code`, `service_message`,
  `request_id`, `request_phase`, `retry_after`, `retry_after_raw`, and
  `diagnostic_headers`. Existing constructors remain supported.
- Preserved S3 XML error fields, including unknown codes, and bounded JSON
  explanations across synchronous and asynchronous failure paths.
- Retained retry hints on HTTP 503 and authentication/permission failures as
  well as HTTP 429. Extraction failures preserve the original service error.
- Preserved diagnostics, partial transfer results, and failed-file context when
  service exceptions and upload/recovery wrappers are pickled.
- Corrected generic HTML HTTP 400 login failures, malformed login/CSRF replies,
  and public CSRF lookup failures to service-response errors. Explicit login
  rejection remains an authentication error.
- Metadata HTTP 5xx now raises a service-response error, including during
  recovery, instead of returning an unavailable-item snapshot for that HTTP
  failure.

Create and file PUT requests remain one-shot. Transfer acknowledgement,
identity checks, provenance checks, and per-file outcomes retain their existing
rules. The client supplies diagnostic fields without deciding whether an
application should retry.

## Saga integration

The prepared package version is `archivist-py==1.4.0`. Publishing requires a
PyPI publishing credential; this document does not assert that the version has
been published. After publication, replace Saga's `archivist-py==1.3.0` pin with
the published version and remove the compatibility subclass for the login/CSRF
classification corrections above.

Read the [field names and types](core.md#service-error-diagnostics) and
[request phases and compatibility changes](internet_archive.md#failure-diagnostics).
Wrapper diagnostics are available on `.cause`, alongside `.result` and
`.failed_file`. Recovery wrapper `.phase` remains the recovery state-machine
phase.

Sanitized integration fixture: `tests/fixtures/internet_archive/s3_slow_down.xml`.
For an HTTP 503 item-creation response using that fixture and `Retry-After: 15`,
the underlying exception exposes:

```python
status_code = 503
service_code = "SlowDown"
service_message = "Please reduce your request rate."
request_id = "fixture-request-id"
request_phase = "item-create"
retry_after = 15.0
retry_after_raw = "15"
diagnostic_headers = {"retry-after": "15"}
```

The fixture was captured using Archive.org's documented public error simulation
and has a substituted request ID. Regression tests also cover namespaced XML,
unknown codes, JSON rejection, malformed bodies, numeric and HTTP-date retry
hints, redaction, and partial upload/recovery failures.
