---
name: review-m8-phase7a-patterns
description: Phase 7a receive-path patterns — §27 zero-copy violations, transactional code gaps, Notify race
metadata:
  type: feedback
---

# M8 Phase 7a (receive-path foundation) review patterns

Lessons learned reviewing `CompletedFetch`, `FetchBuffer`,
`FetchSessionHandler`, `AbstractFetch`, `BufferSupplier`, `FetchRequest`,
`FetchResponse`, `FetchConfig`, `FetchMetadata`, `TopicIdPartition`.

## §27 zero-copy audit checklist (receive path)

The §27 contract requires:
- Topic name `Arc<str>` cloned cheaply per record, NOT freshly
  allocated. **Look for `Arc::from(topic_str)` inside per-record loops
  — that allocates fresh.** The fix is to cache one `Arc<str>` per
  `CompletedFetch` (or per batch cursor) at construction time and
  `Arc::clone` it per record.
- Per-record decoded record (`DefaultRecord`-equivalent) should be
  borrowed, NOT cloned. If `DefaultRecord` owns `Vec<u8>` for key/value,
  cloning it deep-copies the bytes. **Search for `.clone()` calls in
  any per-record path** — they need to be either `Arc::clone` or
  removed entirely.
- Iteration should be lazy at the record granularity, not just batch
  granularity. If `iter_records()` returns `Vec<DefaultRecord>`, it's
  not really lazy — every record's key+value is materialized up front
  at batch entry.

**Why:** §27's per-record allocation-budget test (introduced in Phase
7b) will fail if any of these are violated. The Actor's Phase 7a
docstring may claim "single clone per record" but verify by reading
the actual code, not the docstring.

**How to apply:** When reviewing any consumer receive-path code,
mechanically grep for:
- `Arc::from(` inside `for`/`loop` blocks → ALWAYS a violation
- `.clone()` on `DefaultRecord` / `Record` types → almost always a violation
- `String::from_utf8(.*topic` → §27 violation (topic should be `Arc<str>`)
- `to_vec()` / `Vec::clone` on fetch payload → §27 violation

## Transactional code path partial-translation pattern

`CompletedFetch.fetchRecords` has subtle READ_COMMITTED logic that's
easy to under-translate:
- Java checks `batch.isTransactional() && abortedProducerIds.contains(...)`.
- Rust often translates this as `!is_control_batch &&
  aborted_producer_ids.contains(...)` — the conditions are NOT
  equivalent. Non-transactional batches with assigned producer IDs would
  be mis-skipped.
- Java has a `containsAbortMarker(batch)` path that REMOVES the
  producer ID from the aborted set when an abort marker control batch
  is encountered. Without `ControlRecordType` support in Rust, this
  path is silently dropped — leading to stale `aborted_producer_ids`
  entries that mis-skip subsequent transactions.

**Why:** Transactional tests are typically skipped (no
`ControlRecordType`/`EndTransactionMarker` in Rust). The Actor
documents this as "tests deferred" but the production code path is
also partially missing, with no error/warning. Long-running
READ_COMMITTED consumers will silently lose records.

**How to apply:** When the Actor says "transactional tests deferred,"
specifically audit the READ_COMMITTED CODE PATH for:
1. `isTransactional()` check is preserved.
2. `containsAbortMarker` path is either implemented OR returns a
   `KafkaError::unsupported_version` so the divergence is loud.
3. Any "we only translated half the code path" pattern.

## `tokio::sync::Notify::notified()` race window

Translating Java's `Condition.await(timer)` to tokio's `Notify`:
- Java: lock + signal + await is atomic via `synchronized`.
- Rust: `Notify::notified()` returns a future that registers as a
  waiter ON FIRST POLL (or explicit `.enable()`). If
  `notify_waiters` fires between the flag-check and the first poll,
  the signal is LOST (no permit stored).

**Pattern to flag:**
```rust
if flag.swap(false) { return; }
let notified = notify.notified();      // <-- NOT YET REGISTERED
timeout(t, notified).await;            // <-- racing window
```

**Correct pattern:**
```rust
let notified = self.notify.notified();
tokio::pin!(notified);
notified.as_mut().enable();             // <-- explicit register
if flag.swap(false) { return; }         // <-- re-check after register
timeout(t, notified).await;
```

**Why:** Without `.enable()`, await_wakeup spuriously waits the full
timeout when wakeup races. Not a correctness bug (flag fallback
ensures eventual return) but defeats the latency promise.

## Parameterized test under-translation pattern

Java `@ParameterizedTest` over N parameter combinations becomes N
tests in Rust. Common shortcut: Actor translates ONE combination and
adds suffix to the test name to suggest others. The plan's DoD §3
says: "Are all test using those classes translated?" — partial
parameter coverage is a failure.

**Examples from Phase 7a:**
- `testTopicIdReplaced(boolean, boolean)` — 4 combos, 1 translated.
- `testIdUsageRevokedOnIdDowngrade` — `forEach([0, 1])`, only one
  iteration translated.
- `testVerifyFullFetchResponsePartitionsWithTopicIds` — entirely
  missing as a sibling of `testVerifyFullFetchResponsePartitions`.

**How to apply:** For each Java `@ParameterizedTest` or
`@MethodSource`, count parameter combinations vs Rust test count.
Demand all combinations or explicit rationale per skipped one.

## Inner-class rename + visibility pattern

Java's package-private inner classes (like
`FetchSessionHandler.FetchRequestData`) often get renamed in Rust to
avoid collision with auto-generated `*Data` types. The rename is
fine, BUT:
- Java's package-private becomes `pub(crate)` in Rust.
- A `pub` rename leaks the type to library users.
- The rename should be in rustdoc with a back-reference to the Java
  name.

**Pattern to flag:** `pub struct <RenamedInner>` inside a `pub` module
that translates a package-private Java inner class.

## Behavior-changing helper-parameter pattern

When Java has `newBuilder(int initial_size, boolean copySession)` and
the Rust translation says "the bool is ignored":
- This is a silent semantic change.
- If a caller passes `true` expecting behavior, they get a no-op.
- Either implement both branches OR drop the parameter from the Rust
  API. Don't accept-and-ignore.

## Abstract-class translation gotcha

Java abstract classes translated as concrete Rust structs:
- The Phase-7a plan accepts this for `AbstractFetch`.
- Watch for missed methods — Java's concrete (non-abstract) methods
  must all be translated. `handleCloseFetchSessionSuccess` and
  `handleCloseFetchSessionFailure` are concrete protected methods that
  must NOT be deferred to subclass translation.
- Java's `protected` → Rust `pub(crate)` is the right mapping.

## Constructor parameter dropping pattern

Plan: "drop metrics parameters" is OK. But "BufferSupplier" is NOT a
metrics parameter — it's a real injection point that allows the
caller to share state. Dropping it (or always `Arc::new(...)` inside
the constructor) breaks the sharing pattern.

**How to apply:** When the Actor drops a Java constructor parameter,
verify it's actually metrics-related, not a legitimate dependency
injection point.
