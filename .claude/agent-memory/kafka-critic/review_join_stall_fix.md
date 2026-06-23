---
name: review-join-stall-fix
description: Critic patterns for the consumer KIP-848 join-stall fix (cancel-safe network poll, selector wakeup-return)
metadata:
  type: project
---

Consumer join-stall root cause (`design/current/consumer-join-stall-rootcause.md`):
the bg `run_once` Phase 4 wrapped `delegate.poll_default()` in a cancelling
`tokio::select!`. `poll_default → NetworkClient::poll → maybe_update → ready →
initiate_connect` sets `connection_states.connecting()` (persisted side effect)
THEN `.await`s `current_address`/`selector.connect()`. select! cancellation
dropped that future mid-await, stranding the node in `Connecting` (can_connect
only true when Disconnected) until the ~10s setup timeout. CLAUDE.md §9.6.1.

Option-A fix shape (review heuristics for any similar poll-cancel site):
- Correct fix = pin the poll future (`tokio::pin!`) + drive with `&mut poll_fut`;
  signal arms (guarded `if !poked`) POKE a lock-free `Arc<Notify>` selector
  wakeup handle instead of cancelling. `Selector::poll` must RETURN on its
  `notify` (Java `Selector.wakeup()` semantics), not do one more pass.
- `initiate_connect` runs in `maybe_update`, BEFORE `selector.poll` inside
  `NetworkClient::poll` — the fix must protect the WHOLE `NetworkClient::poll`
  future, not just the selector poll. Verify the protected boundary covers
  maybe_update.

**Recurring blind spots to check on this class of fix:**
1. SYMMETRIC PRODUCER BUG: `src/producer/internals/sender.rs:213` `Sender::run_once`
   still wraps `self.client.poll()` in a cancelling `select!` against `self.wakeup`
   (fired per-batch from kafka_producer.rs:536/629/751/820). Same root cause,
   left unfixed by the consumer-only fix. Always check the producer Sender when
   reviewing a consumer poll-cancellation fix (and vice versa).
2. RULE CONTRADICTION: `consumer-threading.md` §10/§11 still says "wrap network
   poll in cancelling tokio::select!, must be cancel-safe". The fix contradicts
   it verbatim; an Actor reading the rule can revert the fix. The rule MUST be
   amended in the same change-set. Critic flags but cannot edit the rule.
3. STALE Notify PERMIT: `notify_one()` stores one permit if no waiter parked;
   `Selector::clear()` does NOT drain it. After the "return-on-wakeup" change, a
   permit fired outside a poll (e.g. `Self::wakeup` → delegate.wakeup from outside
   Phase 4) makes the NEXT poll return immediately. Bounded (consumed once, one
   extra loop), and arguably Java-faithful (wakeup-before-select returns next
   select). Lean: document as intentional + add test, not a blocker.

**Test-gap pattern (high value):** a "preempt the blocking poll" test
(`application_event_notify_preempts_blocking_network_poll`) asserts the poll
RETURNS promptly but NOT that side effects (Connecting state) are PRESERVED. A
revert of the cancelling select! would still pass it. The missing regression
test must transition a node to Connecting mid-poll, fire wakeup, and assert the
node is not stranded. Also: shared `Selector::poll` wakeup-return behavior change
had ZERO direct selector unit test (CountingClient is a fake, bypasses real poll).

**Read-bound throughput fix (same commit 9966df1; review heuristics):**
- New `TransportLayer::try_read()`/`supports_try_read()` (default WouldBlock/false);
  Plaintext overrides (true), SSL/mocks keep default → async path. `NetworkReceive::
  read_from` drains payload in a tight `try_read` loop (break at `>= buf.len()`,
  return-partial on WouldBlock, UnexpectedEof on Ok(0)). Correct: no over-read past
  message boundary (slice is `buf[read..]`), no false-EOF (size slice always
  non-empty under guard, payload loop skipped when buf.len()==0).
- ISSUE FOUND — `any_channel_mid_receive()` gate in `Selector::poll` (selector.rs
  ~810-829): the no-progress select sets `woke_by_wakeup=true` by CONSUMING the
  notify permit, then does NOT break if a channel is mid-receive. If that channel
  then stalls (WouldBlock, no progress), the re-entered select has no permit → the
  wakeup is swallowed until the poll deadline. Bounded (run() re-checks is_running
  each run_once; not a hang) but a latency regression vs Java Selector.wakeup()
  which always returns regardless of mid-read. Reported as Behavior Mismatch.
  Fix: re-arm permit or track pending_wakeup and break on next no-progress pass.
- TEST GAP — `MockTransportLayer` in network_receive tests does NOT override
  try_read, so the entire tight-drain branch has zero direct unit coverage.
  WouldBlock-mid-payload and EOF-mid-payload sub-cases untested (EchoServer tests
  cover happy-path large reads over TCP but can't force those deterministically;
  test_server_disconnect only tests EOF while idle, not mid-payload).
- yield_now() on immediate/deadline-passed arm: correct, not a busy-spin mask.

Plumbing added (not in Java; justified): `wakeup_handle() -> Arc<Notify>` on
`Selectable`/`KafkaClient`/`NetworkClientDelegate`. Java calls `Selector.wakeup()`
cross-thread directly (nioSelector.wakeup is thread-safe); Rust selector is behind
the delegate's async Mutex held for the whole poll, so a lock-free Arc<Notify>
handle is the equivalent. Mock impls returning a throwaway Notify is fine (never
awaited).
