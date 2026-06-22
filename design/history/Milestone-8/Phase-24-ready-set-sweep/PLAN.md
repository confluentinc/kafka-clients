# Phase 24 — Process only *ready* channels in the Selector poll loop (ready-set sweep)

**Milestone-8 / Phase-24** · Agent number **N = 24**

## Why

Post-Phase-23 `perf` profiling (EC2, Confluent Cloud SASL_SSL, 200k, KIP-848) shows
the dominant remaining cost is the Selector poll loop. Specifically, `Selector::poll`
pass-1 (`selector.rs:966-984`) sweeps **every** channel and calls `poll_channel_reads`
→ `attempt_read` → `try_read` (a `recv`/`read_tls` **syscall**) on **each channel,
every loop iteration**. With ~9–24 broker connections on cloud, when data arrives on
*one* channel we still issue a `recv` on all N — N−1 return `WouldBlock`. This is the
bulk of the profile's syscall cost (`TcpStream::try_read` + `__arch_copy_to_user`
~10%) plus the redundant per-channel bookkeeping.

**Java does not do this.** `Selector.poll(timeout)` → `nioSelector.select()` returns
`selectedKeys()` — only the channels the OS flagged ready — and the consumer processes
*only those*. We sweep all registered channels. Processing only ready channels is the
single biggest remaining **code-level**, **Java-faithful** structural win: O(ready)
syscalls per poll instead of O(all).

Phase 23 already added a non-allocating readiness sweep (`poll_channel_readiness`,
`channel_interest`) that polls each channel's reactor readiness (no syscall — it
checks tokio's cached readiness state via `TcpStream::poll_read_ready`). We extend it
to **record which channels are ready** and feed that set to pass-1.

## Goal

Pass-1 processes only the channels that are actually ready this iteration —
**ready_set ∪ buffered ∪ immediately_connected ∪ mid-receive** — instead of all
channels. **Behavior must be identical**: no records dropped or duplicated, join works
(cloud + local), `wakeup()` works, no busy-spin, idle expiry unchanged. The only
observable change is fewer `recv` syscalls and lower CPU.

**Correctness strictly trumps the optimization.** If any case is ambiguous, process the
channel (a redundant `try_read` is cheap and safe; *skipping* a channel that has data
risks stalling that partition). Document any case where you deliberately fall back to
processing all channels.

## Design

### 1. `poll_channel_readiness` records the ready set
Change the Phase-23 readiness sweep to also record which channel ids it found ready:

```rust
fn poll_channel_readiness(&self, cx, ready_out: &mut FxHashSet<Arc<str>>) -> Poll<()> {
    ready_out.clear();
    for (id, channel) in &self.channels {
        let (want_read, want_write) = self.channel_interest(id, channel);
        if want_read && channel.has_bytes_buffered() { ready_out.insert(id.clone()); continue; }
        if want_read && channel.poll_transport_readable(cx).is_ready() { ready_out.insert(id.clone()); }
        else if want_write && channel.poll_transport_writable(cx).is_ready() { ready_out.insert(id.clone()); }
        // ... still poll BOTH interests to register wakers for all Pending channels ...
    }
    if ready_out.is_empty() { Poll::Pending } else { Poll::Ready(()) }
}
```
Keep the **waker-registration invariant** (Phase 23): poll every interested channel's
relevant readiness even after one is found ready, so the task wakes when ANY becomes
ready. (Returning `Ready` early is fine, but make sure all Pending channels got a
`cx`-registered waker — i.e. poll them before returning, as Phase 23 does.)

Use a reusable scratch `FxHashSet<Arc<str>>` field (e.g. `ready_scratch`) — no per-poll
allocation (Phase 22 precedent).

### 2. Restructure `Selector::poll` to process the recorded ready set
The loop becomes (preserving every existing piece — see invariants):

```text
let mut ready_ids = take(self.ready_scratch);   // empty on first iteration
loop {
    // PROCESS: buffered ∪ immediately_connected ∪ ready_ids (from the prior wait)
    //   - drain channels_with_buffered_read (as today)
    //   - for id in (immediately_connected ∪ ready_ids): poll_channel_reads(id, is_immediately, ...)
    //   - poll_channels_write_concurrent over the same set (+ any with pending writes)
    // made_progress check → break  (unchanged)
    // deferred_wakeup check → break (unchanged)
    // WAIT: select! { notify => wakeup; readiness_wait(records into ready_ids); sleep(dl) }
    //   - readiness_wait = poll_fn(|cx| self.poll_channel_readiness(cx, &mut ready_ids))
    //   - the no-interest branch, woke_by_wakeup / any_channel_mid_receive /
    //     deferred_wakeup handling, and the timeout-0/deadline-None `_ =>` arm:
    //     ALL UNCHANGED
}
restore self.ready_scratch
```

Key points:
  - On the **first iteration**, `ready_ids` is empty, so only buffered/immediately-
    connected channels are processed; if none, no progress → the WAIT runs, its
    `poll_fn` polls all channels' (cheap, syscall-free) reactor readiness and, in the
    common steady-state case where data is already pending, returns `Ready`
    *immediately* with `ready_ids` populated — loop back, process only those. So the
    common path still benefits (no real blocking) without an "optimistic read-all".
  - **`timeout-0` / `deadline = None` path** (`immediately_connected` non-empty, or
    `made_progress_last && data_in_buffers`): there is no WAIT, so no socket-readiness
    set is produced. In this path it is acceptable and safest to **fall back to
    processing all channels once** (current behavior) — it is rare (join / partial-read
    drain) and the channels there are known to have work. Do NOT try to be clever here.
  - **mid-receive channels**: a channel mid-frame must continue to be drained. A
    mid-receive channel becomes readable (reactor-flagged) when its next segment
    arrives → it appears in `ready_ids`; and if it has transport-buffered plaintext,
    `channel_interest`'s `has_bytes_buffered` path already marks it ready. Verify a
    partial receive spanning multiple `poll()` calls still completes (no stall).

### 3. Invariants the refactor MUST preserve (Critic focus)
  - **No dropped/stalled data**: every channel with pending data (socket-readable,
    transport-buffered, or mid-receive) is eventually processed. A partition must never
    stall because its channel was excluded from the ready set. This is the #1 risk.
  - **No busy-spin**: if `poll_transport_readable` reports a channel ready, that channel
    MUST be processed this iteration (drained to `WouldBlock`, clearing reactor
    readiness) — otherwise readiness stays set and the next WAIT returns immediately,
    spinning the loop at 100% CPU. (ready_set membership ⇒ processed.)
  - **§10 cancel-safety**: the WAIT (readiness `poll_fn`) stays side-effect-free except
    recording ids + registering wakers; the non-cancel-safe network poll
    (`poll_channel_reads`/`try_read`/connect) stays OUTSIDE the `select!`, in the
    PROCESS step. Recording ids into the set is not a network side effect — safe.
  - **§11 wakeup**: `notify.notified()` first `biased;` arm unchanged; pre-stored-permit
    behavior, `deferred_wakeup`, `any_channel_mid_receive()`, and the timeout-0 `_ =>`
    `yield_now().await; break;` arm all unchanged.
  - **immediately_connected**: still processed (connect/prepare) and cleared each
    iteration exactly as today.
  - **made_read_progress_last_poll**, idle-expiry (`maybe_close_oldest_connection`),
    `clear()` semantics — unchanged.
  - **No per-poll allocation**: reuse a scratch `FxHashSet` for `ready_ids` (mem::take /
    refill / restore), mirroring `poll_id_scratch`.

### 4. Out of scope
  - Integer node-id keys (separate phase), buffer pooling (separate), UnbufferedConnection.
  - Any threading-model change.

## Tests (DoD)
  - All existing tests pass: `cargo test` (esp. `common::network::selector`,
    `network_client`, `network_receive`, `ssl_transport_layer`, `kafka_channel`).
  - The Phase-23 `test_readiness_wait_path` still passes.
  - **New test**: with multiple connected channels where only a subset have data,
    `poll` returns the data from the ready ones, processes only those (assert via a
    counting/mock transport that `try_read` is NOT called on the idle channels, or that
    idle channels are untouched), and a previously-idle channel that later becomes
    readable IS then processed (no permanent exclusion / no stall).
  - **No busy-spin test**: a ready channel left unprocessed would spin — assert the loop
    parks to the deadline when all ready channels have been drained.
  - `cargo build`, `cargo test`, `cargo xtask lint`, `cargo xtask format-check` green.
  - Docker-gated integration tests at least compile; run `plaintext_consumer_*` if Docker.

## Validation (Manager, post-review)
Re-run the EC2 cloud SASL_SSL 200k comparison (default + 64KB) and a local plaintext
run. Expect: **fewer `recv` syscalls** (lower voluntary-ctxt-switch / syscall count,
optionally confirmed via `strace -c -f` on a short window), **lower CPU**, with
**identical throughput and latency**, and the KIP-848 join still working (cloud + local).

## Critic 24 focus
  - **No data stall**: prove no channel with pending data can be permanently excluded
    from processing (the ready set + buffered + immediately_connected + mid-receive
    cover all "has work" cases). This is the highest-stakes check.
  - **No busy-spin**: ready_set membership ⇒ processed-and-drained this iteration.
  - §10 / §11 / join parity preserved (diff the loop tail carefully — timeout-0 arm,
    deferred_wakeup, mid-receive, wakeup ordering all intact).
  - Interest predicate (`channel_interest`) unchanged from Phase 23.
  - No per-poll allocation (scratch reuse).
