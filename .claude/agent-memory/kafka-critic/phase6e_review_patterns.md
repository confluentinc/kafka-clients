---
name: Phase-6e Sender review patterns
description: Recurring review-axis findings for Sender/MockClient translations — Java exception-class-based predicate translations, skip-rationale audit for non-tx subset, MockClient state-machine divergence, hot-path String allocations
type: feedback
---

Round 1 review of Phase 6e (`Sender<C: KafkaClient>`, ~2615 LOC + 29
tests, MockClient subset). Carry-forward patterns for Phase 7+:

## High-yield audit axes (these found real bugs / gaps)

### 1. Java `instanceof InvalidMetadataException` → hardcoded Rust enum list

**Pattern**: When Java code does `error.exception() instanceof
InvalidMetadataException` (or any superclass-checking `instanceof`),
Rust must translate to a `match` over the `Errors` enum that enumerates
**every** subclass. Easy to miss subclasses.

**How I found it**: Java `Sender.java:712` checks
`InvalidMetadataException`. I greped
`extends InvalidMetadataException` over the Java tree, got 15
subclasses, cross-referenced against the Rust `is_invalid_metadata`
list, and found `KafkaStorageException` and `InconsistentTopicIdException`
(both produce-path-reachable, both in the Rust `Errors` enum) missing.

**Why it matters**: The user-visible effect is
`metadata.requestUpdate(false)` not being called for these errors,
leaving stale leadership/topic-id state until something else forces a
refresh.

**For future review**: Whenever you see a Rust `is_X_metadata` /
`is_X_error` / similar predicate that's a hardcoded `matches!`, grep
the Java tree for `extends XException` and cross-reference. Filter by
"can this error appear in this API?" using the Errors-enum doc
comments — some subclasses are admin- or consumer-only.

### 2. Skipped Java tests audit — beware "metrics" labels hiding behavioral invariants

**Pattern**: When the actor classifies a Java test as "metrics
infrastructure out of scope", verify by reading the test body, not the
test name. `testNodeLatencyStats` was classified as metrics but
actually exercises the Sender's `update_node_latency_stats(can_drain)`
dispatch — a behavioral invariant, not metric registration.

**Heuristic**: Look for tests that ONLY query `metrics.metrics()` or
register sensors → real metrics test. Tests that interact with
`accumulator.getNodeLatencyStats(...)` or assert on field values
inside the Sender's behavioral state → behavioral.

### 3. Skipped Java tests audit — verify "idempotent" classification by reading the test

**Pattern**: When the actor lists a case under "idempotent producer
path", verify by greping for `TransactionManager`, `producerId`,
`epoch`, `sequenceNumber`, `hasIdempotentRecords` in the test body.
Also check the `setupWithTransactionState(...)` arg — `null` means
non-tx.

**Tests that LOOK idempotent but aren't**: cases that use
`assertSendFailure(...)` with a non-tx exception class, or that
exercise terminal/retry paths without ever reading
`transactionManager.{hasFatalError, sequenceNumber, ...}`.

**For Phase 6e specifically**: 6 Java cases were genuinely non-tx and
NOT in the actor's skip list:
- `testSendInOrder` — multi-broker ordering on metadata change
- `testAppendInExpiryCallback` — re-append from inside expiry callback
- `testMetadataTopicExpiry` — metadata.containsTopic interaction
- `testResetNextBatchExpiry` — poll timeout sequence verification
- `testWhenProduceResponseReturnsWithALeaderShipChange*` (×2) — KIP-951
- `testNoBufferReuseWhenBatchExpires` — KAFKA-19012 invariant

Plus 1 misclassified-as-metrics: `testNodeLatencyStats`.

Plus 1 partial: `testRetries` second loop (retry exhaustion path).

### 4. MockClient state-machine divergence — "extra runOnce" smell

**Pattern**: Java's `MockClient.ConnectionState.ready()` is a recursive
state machine: DISCONNECTED → CONNECTING → CONNECTED in a single call.
Rust translations sometimes implement it as a 2-step process where
first call clears `disconnected` and returns false, second call sets
`ready=true` and returns true.

**Smell**: Test code that has 2× or 3× `runOnce()` calls labeled
`// resend` / `// reconnect` (Java has 1× labeled `// reconnect; //
resend`). Each extra `runOnce()` is a sign the MockClient takes more
ticks than Java's to transition to CONNECTED.

**For Phase 6e**: the actor's MockClient is consistent (always 2 calls
to clear-then-ready) and the tests compensate. Not a bug, but worth
documenting in the MockClient rustdoc so future test authors know
about the divergence.

### 5. `.to_string()` per-batch on the send path

**Pattern**: Hot-path predicates that allocate `String` from `&str`
that's already backed by `Arc<str>`. Per-batch on the send path
violates CLAUDE.md rule 11 even if it's not per-record.

**For Phase 6e**: `topic_ids_for_batches` allocates `tp.topic().to_string()`
per batch to build a throwaway `HashMap<String, Uuid>` that's used for
exactly one lookup per batch. Inline the lookup instead.

**Heuristic for future review**: grep `\.to_string()` in any function
in the producer hot path. Each one needs justification (does it cross
an FFI / wire-format boundary? Is it a one-time setup?).

### 6. Test name vs. assertion behavior mismatch

**Pattern**: Test named `test_X_treats_Y_as_Z` but the body actually
exercises a different code path (because translating the original test
faithfully would require infrastructure not yet built — e.g. acks=0
sender setup).

**Smell**: A long defensive comment in the test body explaining
"this isn't quite the original test". The actor knows it's
divergent — easy to spot.

**For Phase 6e**: `test_no_response_body_treats_all_records_as_success`
exercises the disconnect path, NOT the acks=0 / no-body branch. The
acks=0 short-circuit at `sender.rs:886-892` is genuinely untested.

## Verified-safe patterns (no issue filed, useful precedent)

### Sender<C: KafkaClient> generic-over-client

`async fn poll(&mut self, ...) -> Vec<ClientResponse>` in the trait
makes `dyn KafkaClient` painful (BoxFuture everywhere). Generic
monomorphization is the only clean choice. Java's `KafkaClient`
interface is the equivalent abstraction; the dyn-vs-generic split is a
Rust idiom decision, not a CLAUDE.md DoD-line-7 deviation. Don't file.

### ResponseContext correlation map

Java's `RequestCompletionHandler` lambda captures
`(records, topicNames)` and runs synchronously inside `client.poll`.
Rust's `Mutex<HashMap<i32, ResponseContext>>` populated at send-time
and drained after `poll().await` is **behaviorally equivalent**.
Verify by checking that Java's `handleProduceResponse` doesn't read or
mutate state owned by *other* in-flight requests — if it did,
order-of-effects would matter. For Phase 6e: it doesn't (the body is
purely local to `(response, batches, topicNames, now)`).

### MutexGuard across .await audit method

For each `.await` in production code, scroll up until a `let _ = ...lock()...`
or before the function start. If a guard is alive at the await, the
runtime can deadlock. For Sender: 5 awaits, 0 guards. Clean.

### Mute/unmute pairing

Mute is conditional on `guarantee_message_order=true` — verify unmute
has the SAME condition. For Phase 6e: both are gated on the flag at
`sender.rs:448` (mute) and `sender.rs:984` (unmute). Symmetric. ✓

## Anti-patterns to flag

- **`as_any` trait pollution**: when Java uses runtime cast on a
  polymorphic field, Rust needs `&dyn Any`. The trait must add `as_any`
  AND every impl needs to `return self`. If any impl is missed, the
  build fails — but in `pub` traits this is API-breaking. Phase 6e:
  3 impls, all updated. Acceptable.
- **`unwrap` after `contains_key`**: at `sender.rs:844-846`, the actor
  writes `if ctx.topic_names.contains_key(&id) { ctx.topic_names.get(&id).expect(...) }`
  — two lookups. Could be one `match`. Code-quality, not a bug.
- **`if let Some(_) = ... { unreachable!(...) }`**: the actor uses this
  for the never-set `transaction_manager` field. Compiler can't prove
  unreachable so the path stays in. Acceptable per Phase 6 plug-in
  contract.

## Things to remember about Sender translation

- The `pending_responses` map's lifecycle: populated in
  `send_produce_request`, drained in `handle_responses`. If a request
  never gets a response, the entry leaks until Sender is dropped. For
  produce, this only happens on Sender shutdown (where it's fine).
- `complete_batch_with_response` calls `set_inflight(false)` BEFORE the
  if/else — keep this order, it matches Java's
  `Sender.java:672`.
- The retry path (`reenqueue_batch`) and the success path
  (`complete_batch_success`) both unmute the partition. Don't refactor
  to "only unmute on terminal", that's wrong.
- `accumulator.deallocate(batch)` is idempotent — checks
  `is_buffer_deallocated`. The "double-deallocation" defense at
  `sender.rs:1147-1156` is the ASYMMETRY between
  `complete_and_deallocate` (immediate) and `complete_batch` (deferred);
  the latter is for KAFKA-19012.
