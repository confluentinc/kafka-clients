# Critic 65 — RESOLVED findings (Milestone 14 Phase 1)

## Issue 1: `message` JSON field carried a class-name prefix, diverging from Java `getMessage()` — RESOLVED
- **File**: `tools/verifiable-clients/src/verifiable_producer.rs` (`FailedSend::from_error`, `message` field)
- **Severity**: Behavior Mismatch (LOW-MEDIUM)
- **Java Reference**: `VerifiableProducer.java:442-444` (`message()` returns `exception.getMessage()`)
- **Fix**: `from_error` now derives `message` from `Error::message()` (the faithful
  translation of Java `Throwable.getMessage()` — bare text, no class-name prefix),
  instead of `error.to_string()` (which is `Throwable.toString()` =
  `getClass().getName() + ": " + getLocalizedMessage()`). The class/variant already
  lives in the sibling `exception` field, so `message` no longer duplicates that
  prefix. The send-path callback and both synchronous FailedSend prints all route
  through `from_error`, so the fix applies everywhere.

## Issue 2: `message` field could never serialize as `null` — RESOLVED
- **File**: `tools/verifiable-clients/src/verifiable_producer.rs` (`FailedSend::from_error`, `message` field)
- **Severity**: Behavior Mismatch (LOW)
- **Java Reference**: `VerifiableProducer.java:442-444` — `exception.getMessage()` may be
  `null`, and the field has no `NON_NULL` filter, so Java can emit `"message":null`.
- **Fix**: `from_error` maps an empty `Error::message()` (Rust's representation of
  "no message") to `None`, so serde renders the JSON `null` Java produces. The field
  is kept as `Option<String>` WITHOUT skip-serialization, matching Java's `FailedSend`
  which has no `@JsonInclude(NON_NULL)` (always present as a string or `null`, never
  omitted). Two new tests (`failed_send_from_error_present_message_json` and
  `failed_send_from_error_null_message_json`) exercise the real `from_error`
  derivation path (not a hand-built fixture) and pin the exact wire strings for BOTH
  a present message and a `null` message (DoD §12 — pins the mechanism, closing the
  Critic's note that the old null test only proved the fixture).

Field order and names in `producer_send_error` are unchanged and remain exactly Java's
`FailedSend` `@JsonProperty` order (key, value, topic, exception, message) under the
base `@JsonPropertyOrder({"timestamp","name"})`.
