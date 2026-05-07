---
name: Phase 6e Round 1 patterns
description: Patterns settled in Phase 6e Round 1 — Java instanceof→Rust enum-list audit, async catch_unwind, MockClient panic injection, KIP-951 leader-info test shape
type: project
---

Patterns from addressing Phase 6e Round 1 (1 Blocking + 7 Suggestion).

## Java `instanceof X` → Rust enum-list audit

When Java does `error.exception() instanceof InvalidMetadataException`,
the Rust translation typically hardcodes a list of `Errors` variants in
a `matches!` arm. The full set of relevant variants is the wire-coded
subset of Java's exception hierarchy — derive it by:

1. `find kafka/.../errors -name "FooException.java"` and grep for
   `extends X` to enumerate all subclasses.
2. Cross-reference each subclass's wire code via the Java's `code()` /
   `Errors` enum.
3. Drop client-internal subclasses (no wire code) — they cannot arrive
   on a broker response.
4. Include ALL wire-coded subclasses regardless of "produce-path
   reachability" — the Java `instanceof` check matches them all.

Example: `InvalidMetadataException` has 15 subclasses. Two are
client-internal (`StaleMetadataException`, `NoAvailableBrokersException`).
Of the 13 wire-coded ones, the original Rust list had only 6 (because
the actor wrote what they thought was "produce-path reachable"). Java
treats all 13 the same.

**Regression test pattern**: assert every wire-coded subclass in the
list returns `true` from the predicate, plus a sanity slice of
non-subclasses returning `false`. This catches both omissions and
later additions to the enum.

## Hot-path inlining: avoid intermediate `HashMap<String, V>`

`topic_ids_for_batches(batches) -> HashMap<String, Uuid>` was a
"helper" that allocated a per-batch `String` key and an intermediate
HashMap, both used for one lookup per batch and dropped. Inline:

```rust
let topic_ids = self.metadata.metadata().topic_ids();  // HashMap<String, Uuid> snapshot
for batch in &batches {
    let topic_id = topic_ids.get(tp.topic()).copied().unwrap_or(Uuid::zero());
    // …
}
```

The `Borrow<str>` blanket on `String` lets `topic_ids.get(<&str>)`
work without allocation. Java's GC hides this cost; Rust makes it
explicit. Per-batch on the send path = O(batches) avoidable allocs
per `runOnce()`.

## Async `catch_unwind` for Java `try/catch (Exception)` parity

`std::panic::catch_unwind` doesn't work across `.await` points the way
the synchronous version does. Use
`futures_util::FutureExt::catch_unwind` with
`std::panic::AssertUnwindSafe`:

```rust
use futures_util::FutureExt;
use std::panic::AssertUnwindSafe;

let outcome = AssertUnwindSafe(self.run_once()).catch_unwind().await;
if let Err(payload) = outcome {
    error!("{}: {}", prefix, panic_payload_message(&payload));
}
```

`AssertUnwindSafe` is required because `&mut self` is not `UnwindSafe`
by default. The contract is that the caller tolerates post-panic
inconsistent state — Java has the same hazard ("thread keeps going
with corrupt state").

`panic_payload_message(&Box<dyn Any + Send>)`: downcasts to
`&'static str` then `String`, falls back to a marker.

## MockClient panic injection for run-loop tests

To exercise the panic-swallow path in `run_loop`, add a `Option<String>`
field to MockClient and panic from `poll()` on first call:

```rust
struct MockClientImpl {
    // …
    panic_on_next_poll: Option<String>,
}

impl MockClientImpl {
    pub(super) fn set_panic_on_next_poll(&mut self, msg: &str) {
        self.panic_on_next_poll = Some(msg.to_string());
    }
}

// In KafkaClient impl:
async fn poll(&mut self, _timeout_ms: i64, now: i64) -> Vec<ClientResponse> {
    if let Some(msg) = self.panic_on_next_poll.take() {
        panic!("{msg}");
    }
    // …
}
```

Test shape: `make_test_setup`, arm panic, `initiate_close`, then
`tokio::time::timeout(2s, sender.run_loop()).await` — assert `Ok(_)`.
Without catch-unwind the test would propagate the panic and fail.

## KIP-951 produce response builder: per-partition `current_leader`

`build_produce_response_with_leader_info(topic, topic_id,
partition_responses: Vec<PartitionResponseRow>, node_endpoints: Vec<Node>)`
mirrors Java `produceResponse(responses, partitionLeaderInfo, nodes)`.

`PartitionResponseRow = (i32, i64, Errors, Option<(i32, i32)>)` —
clippy `type_complexity` requires a `type` alias for the 4-tuple. The
inner `Option<(leader_id, leader_epoch)>` is `Some` for the
"new-leader" path and `None` for default-constructed `LeaderIdAndEpoch`
(-1/-1).

`Cluster::current_leader(tp)` returns `LeaderAndEpoch { leader: Option<Node>, epoch: Option<i32> }`.
After the response is handled, assert `metadata.current_leader(tp).epoch == Some(101)` for the
new-leader case and `epoch == None` for the default.

## Skip-list bucket structure (Phase 6d Round 1 lesson re-applied)

The rustdoc skip list now has FOUR buckets:

1. **Translated** (Java case → Rust test by name) — eliminates "is it
   covered or skipped?" ambiguity.
2. **Skipped — idempotent / transactional** — out of milestone scope.
3. **Skipped — metrics / mock infra** — covered by stubbed metrics.
4. **Skipped — deferred non-tx with explicit rationale** — for each
   case: which path it exercises, why the Rust test is missing today
   (typically a harness gap: Java uses `Mockito.spy/InOrder` which
   Rust doesn't have without a mock framework), and which alternate
   Rust test covers the closest invariant.

If you find yourself filing "metrics infra" for a test that's actually
behavioral, audit the test body — the Java test name may mislead.
`testNodeLatencyStats` was misclassified for this reason; the test
actually drives `accumulator.update_node_latency_stats(can_drain={false,true})`
dispatch from the Sender, which is a behavioral invariant.

## Test name = test body discipline

If a test's body diverges from its name (e.g. `test_no_response_body_*`
that actually triggers a disconnect), prefer to:
1. Rename the existing test to match what it actually exercises.
2. Add a new test with the original name that exercises the original
   intent (often discoverable by reading the Java source the test was
   ostensibly translating).

This way a future reviewer browsing test names gets accurate signal
about what's covered.

## MockClient connection state machine: 3-tick disconnect → resend

After `disconnect_node(id)`, the connection state machine for that
node requires 3 `run_once()` ticks to reach "ready" again:

1. `run_once` after `disconnect_node`: receives the disconnect → reenqueue.
2. `run_once` after `time.sleep(RETRY_BACKOFF_MS + 1)`: `ready()` resets
   the disconnected flag, returns false (not yet ready).
3. `run_once`: `ready()` returns true → drain & send.

Forgetting tick 2 is a common source of "in-flight count is 0 not 1"
test failures. The pattern shows up in `test_retries_then_success`
(`run_once × 3` after disconnect) and is required for any retry test.
