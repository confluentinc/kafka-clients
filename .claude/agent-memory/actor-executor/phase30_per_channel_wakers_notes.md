---
name: phase30-per-channel-wakers
description: Milestone-8 Phase 30 — replace Selector per-WAIT readiness sweep with per-channel wakers + fired-queue (selectedKeys() on tokio); arming discipline, dirty sites, cancel-safety
metadata:
  type: project
---

Phase 30 (N=30): replaced the Phase-24 per-WAIT sweep (`poll_channel_readiness`,
which polled every interested channel × 2 directions on every WAIT and re-swept
on every wake) with per-channel wakers feeding a shared fired-queue — Java NIO
`selector.select()` → `selectedKeys()` semantics on pure tokio. Two commits.
All in src/common/network/selector.rs only. 1770 lib tests (1767 + 3 new).

**Mechanism (the structures):**
- `ReadyQueue { fired: StdMutex<Vec<(u32,bool)>>, root: StdMutex<Option<Waker>> }`
  shared `Arc` between Selector and every `ChannelWaker`. `set_root` (will_wake
  guard), `wake_root` (take-and-wake OUTSIDE the lock).
- `ChannelWaker { token, is_write, queue }` impl `std::task::Wake`: push
  `(token,is_write)` to fired, then `wake_root()`.
- `ChannelArming { token, read_waker, write_waker, armed_read, armed_write }` in
  a Selector-owned `FxHashMap<Arc<str>, ChannelArming>` (NOT inside KafkaChannel
  — keep channel transport-focused). Plus `token_to_id: FxHashMap<u32,Arc<str>>`,
  `next_token: u32`, `interest_dirty: FxHashSet<Arc<str>>`, `armed_count: usize`.

**Arming discipline (correctness core):**
- `arm_channel(id, ready_out)`: computes `channel_interest` (EXACT Phase-23
  predicate, reused verbatim). Buffered-plaintext short-circuit → ready_out. For
  each interested+not-armed direction: poll transport with cached waker; Ready →
  ready_out (NOT armed); Pending → set armed flag + `armed_count += 1`.
- `arm_rearm_set(processed, ready_out)`: drains `interest_dirty` first (into a
  local Vec to avoid borrow conflict), arms those, then arms `processed`
  (pass-1 channel_ids) skipping dups. Called AFTER pass-1/pass-2, BEFORE the
  scratch restore (channel_ids still live), only when no made_progress break.
- `drain_fired_queue(ready_out)`: mem::take fired vec, translate token→id via
  token_to_id, clear matching armed flag (`armed_count -= 1`), insert id into
  ready_out. Tokens for removed channels are absent → dropped.
- WAIT future is now `poll_fn(move |cx| { selector.ready_queue.set_root(cx.waker());
  selector.drain_fired_queue(ready_ids_ref); Ready iff non-empty })`. Borrow
  trick: `let selector = &mut *self;` reborrow; clone `notify` BEFORE the
  reborrow or you get a borrow conflict in the select! arms.
- After arming, if `ready_ids` non-empty → `continue` (process now, no park).
  Mirrors old poll_channel_readiness returning Ready immediately.

**Dirty sites (every mutation that flips channel_interest false→true):**
1. `send()` Ok arm — queued write turns want_write on.
2. `unmute()` — want_read on; mark dirty even with no buffered bytes (a fire
   during the muted window may be stale/consumed; re-arm relies on tokio
   LEVEL-triggered readiness to re-fire on the still-readable socket).
3. `clear()` / `clear_completed_receives()` / `drain_completed_receives()` —
   clearing has_completed_receive turns want_read on. ALL THREE need it:
   drain/clear_completed leave the list empty so the next poll's clear() finds
   nothing to dirty — each entry point must dirty its own sources.
4. `connect()` registration via `register_arming` (also marks dirty).
- mute() and handshake transitions need NO hook (mute = interest OFF, stale fire
  harmless; handshake channels are in the pass-1 processed set → re-armed).

**Cancel-safety (the subtlest point — PLAN flagged):** WAIT drains only into
selector-owned `ready_scratch`/`ready_ids`, never a future-local. A wakeup/
deadline that cancels the WAIT after a drain would leave drained-but-unprocessed
ids with their armed flags CLEARED and not in any re-arm set → data stall. Fix:
at loop EXIT (after the loop, before `ready_ids.clear()`), re-mark any leftover
`ready_ids` (channels still in self.channels) as `interest_dirty` so the next
poll re-arms them (level-triggered → re-fires). The mid-receive deferred-wakeup
arm already handles its case via `process_all=true`.

**has_interested_channel is now O(1):** `armed_count > 0 || !interest_dirty
.is_empty()`. At the WAIT it's effectively `armed_count > 0` because
arm_rearm_set already drained interest_dirty.

**Channel-removal accounting:** unregister_arming at the two PERMANENT
`self.channels.remove` sites (close_channel_internal, send error path), guarded
decrements of armed_count. Do NOT unregister in poll_channels_write_concurrent
(temporary remove+reinsert — arming entry must survive).

**Test gotchas:**
- 3 new tests reuse existing EchoServer + CountingTransportLayer/
  create_counting_selector scaffolding (Phase 24). Same settle-3×poll(20) rule.
- Multi-channel exactness: completed_receives is wiped by each poll's clear()
  and a poll usually returns ONE receive — accumulate observed sources into a
  HashSet across polls, don't expect all K in one snapshot.
- Mutation-check both directions: drop-fired-id in drain_fired_queue = clean
  subset stall (drain timeout). The "always-ready arm" superset mutation
  BUSY-SPINS (idle channels never park) → test hangs/times out rather than a
  clean assert; #[deny(warnings)] also bites (unused vars) — use `let _ = ...`.
- send()/unmute() dirty-mark removal mutations fail cleanly via inner 5s timeout.

**What the Critic should scrutinize:** (1) the leftover-ready_ids→dirty salvage
at loop exit — is it truly the only stranding path? (2) the race where a fire
lands between arm and the WAIT's first set_root: handled because set_root runs
before drain on first poll, and the fire is still in the queue. (3) armed_count
underflow safety. (4) whether any OTHER mutation flips channel_interest ON that
I missed (re-derive from channel_interest inputs).
