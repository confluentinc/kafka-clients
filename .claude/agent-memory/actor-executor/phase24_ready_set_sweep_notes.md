---
name: phase24-ready-set-sweep-notes
description: Milestone-8 Phase 24 — Selector poll-loop ready-set sweep; process only ready∪buffered∪immediately_connected channels; test scaffolding gotchas
metadata:
  type: project
---

Phase 24 made `Selector::poll` pass-1 process only the channels the reactor
flagged ready (ready_set ∪ buffered ∪ immediately_connected) instead of
sweeping every channel + issuing a `recv` syscall on each. Mirrors Java
`selector.select()` → `selectedKeys()`. Commits bd7fc0d (impl) + 764b62c (tests).

**Why:** Cloud profile showed pass-1 `try_read` syscalls on all N broker
connections every iteration were the dominant remaining poll-loop CPU cost.

**Design that landed (src/common/network/selector.rs):**
- `poll_channel_readiness(&self, cx, ready_out: &mut FxHashSet<Arc<str>>)`:
  clears `ready_out`, records ready ids, returns Pending iff empty. Still polls
  BOTH interests on EVERY interested channel (waker-registration invariant —
  Phase 23) before returning; do not short-circuit.
- `ready_scratch: FxHashSet<Arc<str>>` field, taken via `mem::take` /
  refilled / restored (mirrors `poll_id_scratch`, no per-poll alloc).
- poll loop: `ready_ids` taken before loop; pass-1 channel set =
  `process_all ? all : immediately_connected ∪ ready_ids`. `ready_ids.clear()`
  after pass-1 consumes it (the no-interest select! branch never runs the
  poll_fn, so a stale set would otherwise be reprocessed).
- `process_all = deadline.is_none()` initially (timeout-0 / immediately-
  connected / made_progress+buffered path has no WAIT → no ready set → fall
  back to all-sweep ONCE, the original behavior; rare). Also set true in the
  deferred-wakeup mid-receive drain pass (cancelled WAIT produced no snapshot).

**Critical correctness reasoning (the data-stall risk #1):**
- A mid-receive channel has `want_read=true` (incomplete receive ⇒ no
  has_completed_receive), so when its next segment arrives it shows up in
  ready_ids. The deferred-wakeup re-drain forces process_all anyway.
- ready_set membership ⇒ processed-and-drained (no busy-spin): a
  `poll_transport_readable`-Ready channel is in ready_ids ⇒ pass-1 try_reads
  it ⇒ WouldBlock clears tokio reactor readiness ⇒ next WAIT parks.

**TEST SCAFFOLDING GOTCHAS (cost the most time):**
1. There is NO mock/counting transport in the selector tests — the PLAN's
   phrasing was aspirational. Built one: `CountingTransportLayer` wraps
   `PlaintextTransportLayer`, delegates everything, counts only `try_read`
   into `Arc<StdMutex<HashMap<String,usize>>>`. `CountingChannelBuilder`
   produces it. `TransportLayer`/`InterestOps` are NOT in selector.rs's
   top-level `use super::...`, so the test module must import them explicitly
   (`crate::common::network::transport_layer::{InterestOps, TransportLayer}`);
   `ChannelBuilder`/`KafkaChannel` ARE in scope via `super::*`.
2. Plaintext channels are `ready()==true` immediately after `connect()`, so
   `wait_for_channel_ready`'s `while !is_channel_ready` loop NEVER polls →
   channels stay in `immediately_connected_keys` until the FIRST real poll.
   That first poll then takes the timeout-0 process-all path (effective_timeout
   =0 because immediately_connected non-empty). So a test measuring "idle
   channels not swept" MUST do a few settling polls (`poll(20)` ×3) first to
   drain the immediately-connected state + spurious post-connect readiness
   before baselining counters. Without settling, the first poll legitimately
   process-alls and the assertion (0 try_reads on idle) fails.
3. Mutation-check confirmed: commenting out the `ready_ids` → channel_ids loop
   makes the ready channel's echo never drain; the 5s `tokio::time::timeout`
   in test_ready_set_sweep_skips_idle_channels fails. The test has real power.

**Verify:** all 1761 lib tests pass; lint + format-check clean; all test
targets (incl Docker-gated integration) compile. Tests stable over 5 reruns.
