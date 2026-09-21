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

---

# Phase 2 — resolved items (Actor 65)

## Issue P2-1: Clean shutdown via SIGTERM is not handled — RESOLVED
- **Files**: `tools/verifiable-clients/src/lib.rs` (new `wait_for_shutdown_signal`),
  `tools/verifiable-clients/src/bin/verifiable_consumer.rs`,
  `tools/verifiable-clients/src/bin/verifiable_producer.rs`
- **Severity**: Behavior Mismatch (MEDIUM)
- **Java Reference**: `VerifiableConsumer.java:730` / `VerifiableProducer.java` shutdown
  hooks (JVM hook fires on both SIGINT and SIGTERM) vs.
  `kafka/tests/kafkatest/services/verifiable_client.py:232`
  (`kill_signal` defaults to `signal.SIGTERM` for a clean shutdown).
- **Fix**: Added a shared `pub async fn wait_for_shutdown_signal()` to `lib.rs` that
  resolves on the FIRST of SIGINT (`tokio::signal::ctrl_c()`) or, on Unix, SIGTERM
  (`tokio::signal::unix::signal(SignalKind::terminate())`), racing the two in a
  `tokio::select!`. The SIGTERM arm is compiled only under `#[cfg(unix)]`; on non-Unix
  the fn falls back to Ctrl-C alone so the crate still builds. If SIGTERM can't be
  registered, it falls back to Ctrl-C rather than installing no handler. The select
  only *detects* the signal — no side effects live inside the arms — so it is
  cancellation-safe (`consumer-threading.md` §10 spirit). Both bins now
  `wait_for_shutdown_signal().await` and then run their EXISTING, unchanged clean-
  shutdown routine: consumer prints `shutdown_requested` + `handle.wakeup()`; producer
  sets the stop `AtomicBool`. The shutdown routines themselves were not touched — only
  what triggers them. Each bin's shutdown handler carries a rustdoc note citing that
  ducktape sends SIGTERM by default and that Java's hook covers both signals.

## Issue P2-3: no test pins `records_consumed.count` = FULL poll count when truncated — RESOLVED
- **File**: `tools/verifiable-clients/src/verifiable_consumer.rs`
- **Severity**: Missing test coverage (LOW — DoD §3/§12)
- **Java Reference**: `VerifiableConsumer.java:176` —
  `new RecordsConsumed(records.count(), summaries)` uses the FULL `records.count()`.
- **Fix**: Split `on_records_received`'s body into a testable core
  (`collect_records_consumed`) that returns the offsets to commit together with the
  exact `RecordsConsumed` event `on_records_received` prints; `on_records_received`
  now calls it and prints the returned event, so its Java-faithful signature and
  observable behavior are unchanged (no new struct/trait — DoD §7). New test
  `records_consumed_count_is_full_poll_count_when_truncated` runs the real path with
  `max_messages=2` against a 5-record batch and asserts the emitted
  `records_consumed.count` == 5 (full) while the per-partition summary count == 2,
  min/max offsets reflect the truncation (100/101), the committed offset is 102, and
  `consumed_messages` == 2. It also serializes the event and pins the exact wire
  string, so a regression emitting the truncated count fails. (+1 test → 57 passing.)
