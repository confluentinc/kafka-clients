# Resolved Critic-3 comments (Actor 3)

Comments moved here from `COMMENTS.3.md` once the issue was fixed and verified.
Each entry keeps the Critic's original text verbatim, followed by a
`### Resolution` block naming the commit and what was actually done.


---

## Issue 1: `close()`'s background-task shutdown wake is a no-op — `wakeup_fn` fires the (already disabled) `WakeupTrigger`, not a bg-task poke

- **File**: `src/consumer/async_kafka_consumer.rs:2269-2276`, `:5050`, `:5108-5110`
- **Severity**: Bug
- **Java Reference**: `clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerNetworkThread.java:322-326` and `:375-383`; `AsyncKafkaConsumer.java:1545`
- **Description**:
  This is exactly the defect `bb9ebb0` just fixed, still live on the close path.

  `close_internal` step 1 calls `self.wakeup_trigger.disable()` (line 5050,
  translating Java's `wakeupTrigger.disableWakeups()`), after which
  `WakeupTrigger::wakeup()` returns early without cancelling the token
  (`wakeup_trigger.rs:113-120`).

  Steps 7/8 then shut the bg loop down with (lines 5108-5110):

  ```rust
  self.network_thread_close.signal_close();
  self.network_thread_close.wakeup();
  if let Err(err) = self.network_thread_close.await_join().await { ... }
  ```

  Both of those closures are built at lines 2269-2276 and their only wake
  action is `WakeupTrigger::wakeup()`:

  ```rust
  let signal_close_fn = Box::new(move || {
      signal_close_running.store(false, Ordering::Release);
      signal_close_wakeup.wakeup();        // disabled -> no-op
  });
  let wakeup_fn = Box::new(move || {
      wakeup_for_fn.wakeup();              // disabled -> no-op
  });
  ```

  `signal_close_wakeup` / `wakeup_for_fn` are clones of the *same*
  `WakeupTrigger` instance created at line 2212 and stored on the consumer
  (2357/2440); `WakeupTrigger` clones share `Arc` state, so `disable()` at 5050
  silences both. Neither closure touches the selector wakeup handle or the
  application-event `Notify`.

  Consequently, at close nothing wakes the bg task. The loop
  (`while thread.is_running() { thread.run_once().await }`, lines 2321-2323) is
  parked in Phase 4's `delegate_guard.poll_default(poll_wait_time_ms, ...)`
  (`consumer_network_thread.rs:707`) with `token.cancelled()` never firing, so it
  only observes `running == false` once that poll times out —
  `poll_wait_time_ms`, bounded by `MAX_POLL_TIMEOUT_MS = 5_000`
  (`consumer_network_thread.rs:122`). `await_join()` has no timeout
  (`async_kafka_consumer.rs:1040-1080`), so `close()` blocks for that whole
  interval.

  Java does not have this coupling: `disableWakeups()` acts on
  `AsyncKafkaConsumer.wakeupTrigger`, while the shutdown wake goes through
  `ConsumerNetworkThread.close()` → `wakeup()` →
  `networkClientDelegate.wakeup()` → `Selector.wakeup()`, which is a completely
  separate primitive and always effective.

  **Why no test catches it:** the only tests that exercise this handle build
  their own fixture whose `wakeup_fn` *is* a real bg poke —
  `spawn_dedicated_bg` at `async_kafka_consumer.rs:9967-9970` uses
  `wakeup_wake.notify_one()` on the `Notify` the fake loop parks on. So
  `dedicated_close_handle_joins_cleanly` (:9979) and
  `dedicated_await_join_is_idempotent` (:9995) prove a prompt join for a wake
  path production does not have. (`bb9ebb0` fixed the analogous fixture
  divergence for `wakeup_fn` in `make_test_consumer_with_channels` — the same
  audit was not extended here.)
- **Expected**: `signal_close()` / `wakeup()` must poke a primitive that is
  independent of the user-facing `WakeupTrigger`, exactly as Java's
  `ConsumerNetworkThread.wakeup()` is. Simplest fix consistent with `bb9ebb0`:
  have `signal_close_fn` (and `wakeup_fn`) also fire the application-event
  `Notify` — i.e. capture the same `Arc<Notify>` and call `notify_one()`, the
  closure form of the new `ApplicationEventHandler::wake_background_task()` —
  or capture the delegate's `wakeup_handle()` and fire that. Then add a
  regression test whose fixture `wakeup_fn` mirrors production (fires only the
  `WakeupTrigger`) and asserts the bg loop still exits promptly after
  `disable()`.
- **Actual**: after `close_internal` step 1, both shutdown-wake closures are
  inert; the bg loop exits only when its in-flight network poll times out, so
  `close()` can take up to ~5 s.

### Resolution — FIXED (commit `af60796`, fixup of `bb9ebb0`)

Both closures now poke the application-event `Notify` instead of the
`WakeupTrigger`, and they are built by a single new shared helper:

```rust
pub(crate) fn build_network_thread_close_fns(
    running: Arc<AtomicBool>,
    event_notify: Arc<tokio::sync::Notify>,
) -> NetworkThreadCloseFns
```

(`src/consumer/async_kafka_consumer.rs`, with a rustdoc that states both
reasons the trigger is wrong here: it is user-facing, *and* it is inert after
`close_internal` step 1's `disable()`.) The production ctor and the unit-test
fixture both call it, so the divergence the Critic identified as the reason no
test caught this cannot recur — that was the deeper defect, not the closure
body.

The `Notify` is the right primitive because `run_once`'s network-poll `select!`
has a `notified()` arm that fires the selector's wakeup handle, so the
in-flight poll returns at a safe boundary (§10 — the poll is not cancel-safe
and must not be dropped). Java analog: `ConsumerNetworkThread.close()` →
`wakeup()` → `networkClientDelegate.wakeup()` → `Selector.wakeup()`.

**New regression test** —
`consumer_network_thread::tests::close_handle_wakes_bg_task_after_wakeup_trigger_disabled`.
It models production's shutdown path rather than a stand-in loop: a real
`ConsumerNetworkThread` looping on its own `std::thread` hosting a
`current_thread` runtime (Phase-21 shape), a `NetworkThreadCloseHandle` whose
closures come from `build_network_thread_close_fns`, a client whose `poll()`
parks until its selector wakeup handle fires (`CountingClient::poll_block`, and
its `wakeup_handle()` hands out the very `Notify` the parked poll awaits), and
`wakeup_trigger.disable()` called first exactly as `close_internal` does. It
asserts `signal_close()` + `wakeup()` + `await_join()` complete in well under
`MAX_POLL_TIMEOUT_MS`.

**Fixture fixes** (the Critic's "CRITICAL test guidance"):
  * `spawn_dedicated_bg` builds its closures with `build_network_thread_close_fns`.
  * It also now reports when the loop has actually *parked*, and both
    `dedicated_*` tests wait for that before calling `signal_close()`. Without
    it, `signal_close()` could be observed by the loop's `while` check before
    the loop ever waited, so those tests passed even with a completely inert
    wake — verified: under the pre-fix mutation they passed until this was
    added.
  * `make_test_consumer_with_channels` wraps the production closures (adding
    only the observability flag) instead of hand-rolling bodies.

**Mutation check performed**: restoring the pre-fix trigger-only closure bodies
fails `close_handle_wakes_bg_task_after_wakeup_trigger_disabled` (Elapsed after
5 s), `dedicated_close_handle_joins_cleanly`, and
`dedicated_await_join_is_idempotent`.

**Measured suite-level impact** (`close()` runs in every consumer test's
teardown, so this is a direct read of the defect): the 72 native
`plaintext_consumer*` integration tests take **12.59 s** with the fix and
**20.32 s** with the pre-fix wake behaviour re-applied — a 38 % reduction on the
same tests, same broker pool, back-to-back runs. The full 227-test integration
binary is 41.19 s (previously recorded at ~50 s).


---

## Issue 2: `ConsumerNetworkThread::wakeup()` fires the user-facing `WakeupTrigger`; Java's fires only the selector

- **File**: `src/consumer/internals/consumer_network_thread.rs:359-372`
- **Severity**: Behavior Mismatch (latent — currently no production caller)
- **Java Reference**: `ConsumerNetworkThread.java:322-326`
- **Description**:
  ```rust
  /// Mirror of Java's `wakeup()`. Cancels the current token via the
  /// shared [`WakeupTrigger`] and also calls `network_client_delegate.wakeup()` ...
  pub(crate) async fn wakeup(&self) {
      self.wakeup.wakeup();
      let delegate = self.network_client_delegate.lock().await;
      delegate.wakeup();
  }
  ```
  Java's `ConsumerNetworkThread.wakeup()` is *only*
  `if (networkClientDelegate != null) networkClientDelegate.wakeup();` — it does
  not touch `wakeupTrigger`. The rustdoc's "Mirror of Java's `wakeup()`" is
  therefore wrong, and the method is the exact trap `bb9ebb0` fixed: Java calls
  this method internally from `wakeupNetworkThread()` (every
  `ApplicationEventHandler.add`) and from `close()`. An Actor wiring this up as
  the "wake the bg task" helper — which its name and doc invite — would
  reintroduce the spurious `KafkaError::Wakeup` on the caller's `poll()`.

  It currently has no callers (grepped: no `thread.wakeup()` /
  `network_thread.wakeup().await` anywhere), so this is latent, not live.
- **Expected**: drop `self.wakeup.wakeup()` so the method is the delegate poke
  only (Java-faithful), and correct the rustdoc. Note `signal_close()`
  (`:381-384`) has the same shape — Java's `close()` sets `running = false` then
  calls the selector-only `wakeup()`; see Issue 1 for why the `WakeupTrigger`
  there is not merely redundant but inert.
- **Actual**: cancels the user-facing cancellation token in addition to poking
  the selector.

### Resolution — FIXED (commit `af60796`)

`ConsumerNetworkThread::wakeup()` is now the delegate poke only, matching
`ConsumerNetworkThread.java:322-326`, and the rustdoc says so explicitly
(including *why* the trigger must not be fired, so the next Actor reading the
method name is not invited into the trap).

`signal_close()` likewise dropped the trigger fire and now pokes
`self.event_notify` — Java's `close()` sets `running = false` then calls the
selector-only `wakeup()`.

With both fires gone, the `wakeup: WakeupTrigger` field was dead, so it is
removed: the bg task keeps only the `watch::Receiver<CancellationToken>` it
needs to *observe* the token. That is also closer to Java, where
`wakeupTrigger` is an `AsyncKafkaConsumer` field the network thread has no
access to. The one test that fired it
(`run_once_returns_when_wakeup_fires_during_poll`) now cancels the observed
token directly, which is exactly what the app-side `WakeupTrigger::wakeup()`
does.


---

## Issue 3: `AsyncKafkaConsumer::wakeup()`'s second call does not do what its comment says

- **File**: `src/consumer/async_kafka_consumer.rs:2655-2661`
- **Severity**: Design Flaw (dead call + incorrect comment)
- **Java Reference**: `AsyncKafkaConsumer.java:1682` (`wakeupTrigger.wakeup();` — one statement)
- **Description**:
  ```rust
  pub fn wakeup(&self) {
      self.wakeup_trigger.wakeup();
      // Also wake the bg task's `select!` directly so the underlying
      // `KafkaClient::poll` is unblocked even if the wakeup token was
      // already cancelled.
      self.network_thread_close.wakeup();
  }
  ```
  The stated purpose is unachievable as written: `network_thread_close.wakeup()`
  runs `wakeup_fn`, which is `wakeup_trigger.wakeup()` (line 2274-2276) — the
  same trigger just fired on the line above. `CancellationToken::cancel()` is
  idempotent, so in the "token was already cancelled" case this second call is a
  no-op and the underlying `KafkaClient::poll` is *not* unblocked directly. Same
  root cause as Issues 1 and 2: `wakeup_fn` is documented at :965-970 as "Wakes
  the bg-task's `select!`" but is not a bg-task poke.
- **Expected**: either make `wakeup_fn` an actual bg poke (which also fixes
  Issue 1) so the comment becomes true, or delete the second call and match
  Java's single statement.
- **Actual**: a no-op second call carrying a comment that describes behavior the
  code does not have.

### Resolution — FIXED (commit `af60796`)

Kept the second call and made it true, rather than deleting it: with Issue 1
fixed, `network_thread_close.wakeup()` *is* a bg-task poke (the application-event
`Notify`), so it does unblock the in-flight `KafkaClient::poll`.

Deleting it and matching Java's single statement would have been wrong, not
merely different: Java's `WakeupTrigger` completes the `CompletableFuture` a
blocked API call waits on and never has to interrupt a socket poll, whereas the
Rust `poll()` can be parked inside `NetworkClientDelegate::poll_default` on the
bg task. Without the poke a `wakeup()` arriving mid-poll is only acted on up to
`MAX_POLL_TIMEOUT_MS` later. The comment is rewritten to say that, and to note
that re-firing the (idempotent) token would unblock nothing — which was the
factually wrong part.


---

## Issue 4: the close-path `ConsumerRebalanceListenerCallbackNeeded` arm sends the ack without the poke, and its comment describes pre-Phase-41 behavior

- **File**: `src/consumer/async_kafka_consumer.rs:3153-3183`
- **Severity**: Design Flaw (minor; comment is factually wrong)
- **Description**:
  The `skip_rebalance_callback` arm ends at `let _ = ack.send(Ok(()));` with no
  `wake_background_task()`, while the sibling arm 20 lines below now pokes. The
  bg loop therefore only `try_recv`s this ack after its in-flight poll times
  out, delaying the gated membership transition during `close()` by up to
  `poll_wait_time_ms` — the very latency `bb9ebb0`'s poke exists to remove. (Not
  a hang, and bounded by the close deadline, hence "minor".)

  Separately, the arm's rationale is stale (pre-dates Phase 41; introduced
  before this commit):

  > We still must send the §31 ack so the bg task's `invoke_rebalance_callback`
  > (parked on `ack_rx.await`) unblocks ... without it the bg task never makes
  > progress and `network_thread_close.await_join()` (Step 8) hangs.

  Per `consumer-threading.md` §31 step 2 and `run_once` Phases 2.4/2.5, the bg
  loop no longer parks on `ack_rx.await` — it stores the receiver and
  `try_recv`s it each iteration, so a missing ack gates only the membership
  transition and cannot hang `await_join`. The final sentence ("Java has no
  equivalent dependency because ... never parks the bg thread on the ack") is
  now describing the Rust behavior too.
- **Expected**: add `self.application_event_handler.wake_background_task();`
  after the ack for symmetry, and rewrite the comment to the Phase-41 model
  (ack unblocks the *stored* receiver `try_recv`, gating the state transition —
  not a parked bg task).
- **Actual**: no poke; comment describes a blocking handshake that no longer
  exists.

### Resolution — FIXED (commit `af60796`)

Added `self.application_event_handler.wake_background_task();` after the ack in
the `skip_rebalance_callback` arm, with a comment noting it matters *more* on
the close path than off it, since every close step runs under the close
deadline.

The stale rationale is rewritten to the Phase-41 model: the bg loop stores the
receiver and `try_recv`s it on every `reconcile` / `drive_pending_release`
entry, so a missing ack cannot hang `await_join()` — what it gates is the
membership state transition, leaving the member stuck in
`RECONCILING` / `FENCED` / `STALE` for the rest of the close path. The final
sentence about Java "never parking the bg thread on the ack" is dropped, since
that is now true of the Rust translation too.


---

## Issue 5: `test_delivery_callback_logs_metadata` claims "invoked exactly once" but cannot detect a double invocation

- **File**: `tests/integration/producer_test.rs:791-793` and `:812-819`; `tests/common/callback_log.rs:335-344`
- **Severity**: Design Flaw (test strength)
- **Description**:
  The test's docstring is "a delivery callback ... is invoked **exactly once**",
  and the assertion is
  `assert_eq!(deliveries.len(), 1, "... callbacks fire once per record")`.
  But the snapshot it asserts on comes from `wait_for_kind`, which returns as
  soon as the *first* matching entry is visible:

  ```rust
  let entries = self.entries().await...;
  if entries.iter().any(|e| e.kind == kind) || start.elapsed() >= deadline {
      return entries;
  }
  ```

  A backend that fires the callback twice will, in the common case, be sampled
  between the two appends and pass. The "at least once" half of the contract is
  pinned; the "at most once" half — which is the interesting half, since
  double-firing is the classic FFI/binding callback bug and CLAUDE.md §9.5 calls
  out the exactly-once obligation explicitly — is not.

  The consumer-side `poll_until_kind` has the same shape, but its assertions are
  `any(...)`-based, so only this test over-claims.
- **Expected**: after the first `KIND_DELIVERY` entry appears, wait a bounded
  grace period (e.g. re-read after 500 ms–1 s, or poll until two consecutive
  reads agree) and *then* assert `len() == 1`. Alternatively weaken the
  assertion and the docstring to "at least one" so the test no longer claims
  what it does not check.
- **Actual**: `len() == 1` is asserted against a snapshot deliberately taken at
  the earliest moment one entry exists.

### Resolution — FIXED (commit `bdb8b77`)

Added `ProducerCallbackLog::wait_for_kind_settled(kind, deadline, grace)`
(`tests/common/callback_log.rs`): `wait_for_kind` followed by a bounded settle
window that keeps re-reading the log for `grace` and returns the last snapshot.
`test_delivery_callback_logs_metadata` uses it with a 750 ms grace, so the
`assert_eq!(deliveries.len(), 1)` is now evaluated against a snapshot taken
after the log has stopped moving rather than at the earliest instant one entry
exists.

Chose the strengthen-the-test option over weakening the docstring, because the
"at most once" half is the one worth having: double-firing is the classic
FFI/binding callback bug and CLAUDE.md §9.5 makes exactly-once invocation an
explicit obligation. A stray second append lands within milliseconds of the
first (same completion path, same dispatcher thread), so a sub-second window
catches it; the window is a flat cost per backend and never retries or extends.

The helper's rustdoc states the requirement so future count assertions do not
reach for `wait_for_kind`. The consumer-side `poll_until_kind` is deliberately
left alone — as the Critic noted, its call sites all assert with `any(...)`, so
it does not over-claim.


---

## Issue 6: `server.cc` `delete state` in `Close` is a use-after-free — `..._destroy()` detaches the FFI dispatcher instead of joining it

- **File**: `bindings/c/grpc_server/server.cc:255-257` (the design comment), `:562-571` (producer `Close`), `:1154-1162` (consumer `Close`)
- **Severity**: Bug (memory safety; not reachable from the current test flow, see below)
- **Description**:
  The `LogState` design comment asserts the safety argument:

  > The state is deleted in Close, after `..._destroy(client)` returns: destroying
  > the client drops the listener / commit adapters, so no callback can still
  > reference it.

  and `Close` implements it literally:

  ```cpp
  kafka_producer_Producer_close(producer, &err);
  kafka_producer_Producer_destroy(producer);
  delete state;                                  // :568-571
  ```
  ```cpp
  kafka_consumer_Consumer_close(consumer);
  kafka_consumer_Consumer_destroy(consumer);
  delete state;                                  // :1156-1162
  ```

  The premise is false: **neither `destroy` joins the dispatcher thread that
  actually runs the C callback.** Both deliberately detach it:

  - `src/ffi/producer.rs:899-909` — *"NOTE: … joining here would hang if the
    caller destroys the producer while futures/callbacks are still outstanding …
    We therefore detach the dispatcher"*, then `drop(completion_tx); drop(dispatcher…)`.
  - `src/ffi/consumer.rs:532-536` — *"Close the completion channel and detach the
    dispatcher (do NOT join …)"*.

  And the C callback does not run at the point the Rust-side callback fires — it
  is only *enqueued*. `make_record_callback` (`src/ffi/producer.rs:502-523`)
  boxes a `RecordCompletion` and calls `enqueue_or_run_inline`
  (`src/ffi/common.rs:312-316`); the dispatcher drains the queue on its own
  thread (`src/ffi/common.rs:294-307`). So after `Producer_destroy` returns, a
  `log_delivery(…, state)` job can still be sitting in the queue, and the
  detached dispatcher then dereferences `state->log` / `state->client_id`
  (`server.cc:356-357, 374`) on memory freed by `delete state`.

  The consumer side has the same hole through a narrower window: its callbacks
  do wait (`dispatch_and_wait`, `src/ffi/common.rs:348-367`, used by
  `FfiRebalanceListener::invoke` and `FfiOffsetCommitCallback::on_complete`), but
  `dispatch_and_wait` *sends the job before awaiting*, and
  `Consumer_destroy` step 1 is `runtime.shutdown_background()`
  (`src/ffi/consumer.rs:529`), which cancels the awaiting task and leaves the
  already-queued job to run later — after `delete state` at `:1162`.

  **Second, independently wrong consequence.** The same false premise backs the
  advertised post-close read guarantee at `server.cc:223-226` and `:562-563`
  (*"close() flushes, so any outstanding delivery callback fires (and appends to
  the log) before this returns"*). `flush()` guarantees the *Rust* callback ran,
  i.e. that the job was enqueued — not that the dispatcher ran it. A
  `Close` immediately followed by `GetCallbackLog` can therefore legitimately
  miss entries on the `c` backend, while the two Python servers (whose callbacks
  append synchronously in-process) do deliver it. That is a real cross-backend
  semantic divergence in a property the proto/servers explicitly promise.

  **Not currently triggered**: all three new tests read the log to completion
  (`wait_for_kind` / `poll_until_kind`) *before* calling `close()`, so by the
  time `Close` runs the job has already been drained. This is latent, and it is
  latent by luck of test ordering, not by construction.
- **Expected**: stop freeing `LogState` at all — the design already treats it as
  session-lifetime (that is the stated reason every `user_data_destroy` is
  `nullptr`). Keep the allocations in a `std::vector<std::unique_ptr<LogState>>`
  (or simply leak them, as `CallbackLog` effectively does) and drop both
  `delete state` calls plus the `log_states_.erase(...)`. That also removes the
  smaller `log_state_for()`-returns-`nullptr`-after-`Close` race in `Send` /
  `CommitAsync`, and the `LogState` leak for a client that is never `Close`d
  (neither service impl has a destructor). Then correct the two comments: `flush`
  /`close` do not guarantee the dispatcher job has run, so post-close
  `GetCallbackLog` is eventually-consistent on the `c` backend — or add an
  explicit drain/settle step if the guarantee is wanted.
- **Actual**: `delete state` runs while a queued dispatcher job may still hold
  the pointer, justified by a comment that misdescribes `destroy`'s teardown
  contract.

### Resolution — FIXED (commit `bdb8b77`)

Took the Critic's recommended direction: `LogState` is now session-lifetime and
never freed before the service is destroyed.

  * `log_states_` in both services is `std::unordered_map<uint64_t,
    std::unique_ptr<LogState>>`; `log_state_for()` returns `it->second.get()`.
  * Both `Close` handlers no longer look up, erase, or `delete` the state — the
    two `delete state` calls and both `log_states_.erase(...)` are gone. The
    `unique_ptr` means the states are released with the service (no leak) rather
    than leaked outright, which the Critic offered as the alternative.
  * This also removes the smaller `log_state_for()`-returns-`nullptr`-after-Close
    race in `Send` / `CommitAsync`, and the leak for a client that is never
    Closed (neither service impl has a destructor).

Corrected the three comments the false premise had produced:

  * `LogState`'s design comment now records *why* the old argument was wrong —
    neither `..._destroy` joins the dispatcher, both detach it, and the Rust
    callback only *enqueues* the C callback as a dispatcher job.
  * both `Close` comments now say `close()`/`flush()` guarantee the *Rust* side
    of the callback ran, i.e. the C callback was enqueued — not that the
    dispatcher has run it.
  * `CallbackLog`'s comment documents the cross-backend semantics explicitly:
    post-close `GetCallbackLog` is *eventually* consistent on the `c` backend,
    the two Python servers append synchronously in-process and have no such
    window, and callers asserting on counts must poll (`wait_for_kind` /
    `wait_for_kind_settled` / `poll_until_kind`) rather than read once.

Chose documenting eventual consistency over adding a drain/settle step in the
server: the C FFI exposes no way to join a detached dispatcher, so any
server-side "drain" would be a sleep. The Rust harness already polls with a
deadline on every callback-log read, which is the honest place for it.

The three servers' post-close `GetCallbackLog` semantics are aligned in the
respect that matters: all three keep entries after `Close`
(`grpc_server.py`'s service-level `CallbackLog` is never popped either, and its
`GetCallbackLog` docstring already says post-close reads are intentional). The
remaining difference is timing-only and is now written down on both sides.

