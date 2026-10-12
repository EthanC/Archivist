## Service error diagnostics

`ServiceError` accepts the following optional keyword-only fields. Existing
constructor calls, `service`, and `status_code` remain supported. Subclasses
share these fields, including `RateLimitError.retry_after` and
`CaptureFailedError.service_code`.

| Field | Type | Response-derived value |
| --- | --- | --- |
| `service_code` | `str \| None` | Recognized server code, including unknown S3 codes, bounded to 256 characters. |
| `service_message` | `str \| None` | Sanitized server explanation, bounded to 1,024 characters. |
| `request_id` | `str \| None` | XML/JSON `RequestId`, otherwise `x-amz-request-id`, bounded to 256 characters. |
| `request_phase` | `str \| None` | The failed request step. |
| `retry_after` | `float \| datetime \| None` | Finite nonnegative relative delay or timezone-aware UTC HTTP date. Malformed values yield `None`. |
| `retry_after_raw` | `str \| None` | Sanitized, whitespace-normalized representation of `Retry-After`, bounded to 256 characters. |
| `diagnostic_headers` | `dict[str, str]` | Copied response headers with lowercase names: `content-type`, `retry-after`, and `x-amz-request-id`. Each value is bounded to 256 characters. Defaults to a new empty dictionary. |

The parser accepts complete bodies of at most 16 KiB measured as UTF-8. Larger
bodies are discarded for diagnostics rather than parsed from a truncated prefix.
It recognizes S3 `<Error>` XML, with or without a namespace, containing `Code`,
`Message`, and `RequestId`. JSON objects support those same case-sensitive fields
and the existing string explanation fields `error`, `message`, `detail`, and
`reason`. It ignores unknown fields and nested objects. HTML, plain text, empty
bodies, malformed JSON/XML, and XML containing entity or DTD declarations produce
header-only diagnostics.

Response-derived strings have HTML markup, email addresses, HTTP(S) URLs,
labeled passwords/keys/tokens/signatures, authorization values, and cookie-header
text removed or replaced with redaction markers before the final character
limit. Control characters and Unicode surrogates are removed. These rules also
apply to codes, request IDs, and allowlisted header values. No response body,
response object, session, arbitrary header, `Authorization`, `Cookie`,
`Set-Cookie`, or signed resource URL is stored in the diagnostic fields. Values
supplied directly to exception constructors are caller-owned and are copied
without response-parser sanitization.

Synchronous and asynchronous response adapters use the same parser. Optional
awaitable diagnostic body reads have a one-second cap. A diagnostic read failure
falls back to headers and preserves the original exception. Timed-out operations
do not start another diagnostic body read; failed network/body reads are not
repeated. Pickling a service exception retains its diagnostic fields. Upload and
recovery wrappers also retain their `.cause`, `.result`, and failure context.

See [Internet Archive failure diagnostics](internet_archive.md#failure-diagnostics)
for request phases, classification changes, and caller examples.

## API reference

::: archivist.core.models

::: archivist.core.protocols

::: archivist.core.errors
