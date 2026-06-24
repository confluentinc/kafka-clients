# Phase 26 — Poll-loop CPU: don't return on send-only + skip fetch-prep when all nodes pending

**Milestone-8 / Phase-26** · Agent number **N = 26**

Two CPU micro-optimizations ported from the GraalVM Java-consumer spike
(`example-confluent-kafka-native-java` branch `test_consumer_benchmark_c_sync`,
the `*NoUse` reference files). Both restore/port **stock Apache Kafka** behavior the
Rust port either diverged from or omitted. **Neither changes latency** (that is
broker-side `fetch.min.bytes` accumulation, separately measured); both reduce
wasted CPU in the steady-state poll loop. Implement as **two independent commits**
so each can be reviewed / reverted alone.

## Fix #1 — `Selector::poll` must not return on a send-only round

**Where:** `src/common/network/selector.rs`, the `made_progress` break in `poll`:
```rust
let made_progress = !self.completed_sends.is_empty()
    || !self.completed_receives.is_empty()
    || !self.connected.is_empty()
    || !self.disconnected.is_empty();
if made_progress { break; }
```
**Problem:** stock Java `NetworkClient.poll` loops
`do { selector.poll(t) } while (completedReceives().isEmpty() && disconnected().isEmpty())`
— it does NOT return on completed *sends*. Rust returns as soon as fetch requests
are written (`completed_sends` non-empty) with no response yet, so `run_once`
spins an extra full iteration (drain events + poll every manager) before
re-entering to await the response.

**Change:** drop `completed_sends` from the `made_progress` break condition so a
send-only poll keeps waiting (in the existing `select!` on readiness / wakeup /
deadline) for the actual response.

**CRITICAL — keep `connected` in the break.** Unlike Java's condition, Rust MUST
still break on `connected`: `poll_channel_reads` pushes onto `self.connected` on
the post-handshake `was_ready -> ready` transition specifically so `poll()` exits
and `handle_initiate_api_version_requests` fires (the §10 join path). Removing
`connected` re-introduces the join stall. So the ONLY removal is `completed_sends`;
`connected`, `disconnected`, `completed_receives` all stay.

**Why safe (functionality unchanged):**
  - Every consumer request (fetch / heartbeat / commit / offset / metadata /
    api-version) expects a response, so there is no fire-and-forget send that
    would block forever waiting for a receive.
  - `completed_sends` still accumulate and are returned to the caller when the
    poll next breaks (on receive / connect / disconnect / deadline) — just one
    cycle later. `handle_completed_sends` does nothing response-bearing for the
    consumer.
  - The poll `deadline` (= `maximum_time_to_wait_ms`, the min of all manager
    timers) still bounds the wait, so heartbeat / commit / poll-timeout fire on
    schedule even if no fetch response arrives.
  - `made_read_progress_last_poll`, the `effective_timeout == 0` buffered-data
    fast path, `deferred_wakeup`, `any_channel_mid_receive`, and the timeout-0
    `_ =>` arm are UNCHANGED.

**§10 / §11 invariants (Critic must verify):** network poll still side-effect-safe;
wakeup (`notify`) arm + ordering unchanged; join works (cloud + local) — `connected`
break preserved; no busy-spin introduced (a send-only round now parks on readiness
instead of returning, which is the intended behavior, not a spin).

## Fix #2 — `prepare_fetch_requests`: up-front skip when no node is fetchable

**Where:** `src/consumer/internals/abstract_fetch.rs`, top of `prepare_fetch_requests`
(before the `fetchable_partitions` SubscriptionState lock + `compute_buffered_nodes`
+ per-partition loop).

**Problem:** the Rust poll loop re-issues `create_fetch_requests` every `run_once`
iteration (see fetch_request_manager.rs:323 comment), so `prepare_fetch_requests`
runs every loop. In steady state every broker has an in-flight fetch
(1-fetch-in-flight per broker), so the full computation (lock + fetchable scan +
buffered-nodes + per-partition node resolution) runs only to return an empty map
because every partition's node is skipped (`nodes_with_pending_fetch_requests`).
This is the ~3.5% `prepare_fetch_requests` self-time in the CPU profile.

**Change (port stock Java's `if (unfetchableNodes == nodes.size()) return emptyMap`):**
at the very top of `prepare_fetch_requests`, iterate the cluster nodes; if EVERY
node is in `nodes_with_pending_fetch_requests` OR `is_unavailable(node)`, return
`Ok(HashMap::new())` immediately — before taking the SubscriptionState lock,
before `fetchable_partitions`, before `compute_buffered_nodes`.

**Implement as a STATELESS short-circuit, NOT a cache.** Do NOT add a memoized /
invalidated cache of the fetchable set (stale-cache risk = a partition that becomes
fetchable is never fetched -> stall). The stateless check returns empty only when
it is genuinely true that no node can be fetched this instant; the moment a fetch
response frees a node (removes it from `nodes_with_pending_fetch_requests`), the
next call passes the check and issues normally.

**Why safe (functionality unchanged):**
  - It is a pure short-circuit of a result that would have been empty anyway:
    if all nodes are pending/unavailable, no partition can be fetched -> empty map.
  - Exactly mirrors stock Java's first early-return.
  - The per-partition loop's existing skip conditions (pending / unavailable /
    buffered) are unchanged for the case where SOME node is free.
  - Edge: if the cluster node list is empty (no metadata yet), the "all nodes
    unfetchable" check must NOT short-circuit incorrectly — match Java: with zero
    nodes the loop count is 0, `0 == 0` is true so Java returns empty; preserve
    that (empty cluster -> empty map is correct, nothing to fetch).

## Out of scope
  - Removing `connected` from the break (would break §10 join).
  - A stateful fetchable-set cache (invalidation hazard).
  - Fetch pipelining depth / latency changes (separate; latency is broker-side).

## Tests (DoD)
  - All existing tests pass: `cargo test` (esp. `common::network::selector`,
    `network_client`, `consumer::internals::{abstract_fetch, fetch_request_manager,
    fetch_collector}`).
  - Fix #1: a selector test that a poll which only completes a send (no receive)
    does NOT return until a receive arrives OR the deadline (and still returns
    promptly on connect / disconnect / wakeup). The Phase-23/24 selector tests
    (`test_readiness_wait_path`, `test_ready_set_sweep_*`) must still pass.
  - Fix #2: a `prepare_fetch_requests` test that when all nodes are in
    `nodes_with_pending_fetch_requests`, it returns empty WITHOUT touching
    SubscriptionState (and that freeing one node makes the next call issue a fetch
    for it — no stall). Assert empty-cluster -> empty map.
  - `cargo build`, `cargo test`, `cargo xtask lint`, `cargo xtask format-check` green.
  - Docker-gated integration tests at least compile; run `plaintext_consumer_*` if
    Docker (these exercise the real join path that Fix #1 touches).

## Validation (Manager, post-review)
Re-run the EC2 cloud SASL_SSL comparison (small-batch 200k default-wait AND
big-batch). Expect lower CPU (fewer run_once iterations from #1; cheaper per-call
prepare from #2), unchanged throughput and latency, and the KIP-848 join still
working (cloud + local).

## Critic 26 focus
  - **Fix #1 is §10-critical**: `connected` MUST remain in the break (join path);
    only `completed_sends` removed. Verify no fire-and-forget send can block on a
    never-arriving receive; deadline still bounds the wait; wakeup/`deferred_wakeup`
    /timeout-0 arm intact; no busy-spin. Diff the poll loop tail carefully.
  - **Fix #2**: stateless short-circuit only (no cache); the early-return condition
    is logically equivalent to "all nodes pending/unavailable -> would-be-empty";
    no partition can be stranded (freeing a node re-enables fetch next call);
    empty-cluster handled.
  - Both behavior-identical except lower CPU; no latency change.
