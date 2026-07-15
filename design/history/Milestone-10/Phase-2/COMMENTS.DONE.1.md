# Resolved Critic (N=1) findings — Milestone-10 Phase 2

All three findings from `COMMENTS.1.md` are resolved. The Critic's overall
verdict was NOT BLOCKING and the teardown rewrite was judged sound; these were
test-teeth / coverage gaps.

---

## Issue 1 (should-fix): SIGBUS regression test had weak teeth — RESOLVED

**Was:** `test_destroy_after_in_flight_async_is_safe` fired one `subscribe_async`
on a near-instantaneous mock then immediately called `destroy`. The op almost
always completed before `destroy` ran, so the protected window (a worker still
touching `consumer_mut(hs)` while the box is freed) was practically never hit.
A revert of the blocking `drop(runtime)` back to `shutdown_background()` would
keep the test green ~999/1000 runs.

**Fix:** Replaced it with `test_destroy_blocks_until_in_flight_async_completes`
in `src/ffi/share_consumer.rs`, which drives the in-flight window
*deterministically*:

- A test-only `BarrierConsumer` (wrapped as `ShareConsumerKind::Kafka`, so it
  also exercises the previously-untested production arm of `consumer_mut`) parks
  its worker thread on a two-party `std::sync::Barrier` *inside* `poll`. Because
  the wait is a synchronous block — not a tokio `await` — the runtime cannot
  cancel the worker; it must join it.
- Sequence: `poll_async` → wait until `poll` has entered and borrowed the
  consumer (a `started` channel) → call `ShareConsumer_destroy` from a second
  thread → assert `destroy` has NOT returned while the op is still parked (a
  correct blocking runtime-drop must wait) → release the barrier → assert
  `destroy` then returns and the destroyer thread joins cleanly. The completion
  job runs on the detached dispatcher after the box is freed, touching only its
  own `Arc<AtomicU64>` + owned result handles.

The teeth are an ordering invariant ("destroy must block until the in-flight op
completes"), which is a deterministic proxy for the exact UAF protection
mechanism #2 provides — strictly better than relying on a nondeterministic
crash.

**Teeth proof (performed as requested):** temporarily reverted the fix to
`if let Some(rt) = handle.runtime.take() { rt.shutdown_background(); }` and ran
the new test. It FAILED deterministically:

```
thread '...test_destroy_blocks_until_in_flight_async_completes' panicked at
src/ffi/share_consumer.rs: destroy returned while an async op was still in
flight — the teardown guard's blocking runtime-drop has regressed to a
non-blocking shutdown
```

Restored the blocking `drop(handle.runtime.take())` and confirmed the test
passes; ran it 12× (each ~0.5s) with no flakiness and prompt termination.

---

## Issue 2 (minor): acknowledge error-forwarding untested at the FFI boundary — RESOLVED (documented deferral)

**Was:** `MockShareConsumer::acknowledge*` unconditionally returns `Ok`, so the
FFI acknowledge test cannot assert the non-in-flight
`IllegalState "The record cannot be acknowledged."` message at the FFI boundary.

**Resolution: documented deferral (option b), no fake assertion.** The
`"The record cannot be acknowledged."` contract is genuinely asserted by the
Milestone-9 unit tests that own it:

- `src/consumer/internals/share_fetch.rs` — `acknowledge` and
  `acknowledge_on_exception` reject with that message; asserted at
  `share_fetch.rs:416` and `share_fetch.rs:463`.
- `src/consumer/internals/share_in_flight_batch.rs` — same rejection asserted at
  `share_in_flight_batch.rs:475`.

The FFI mock cannot reach it because the rejection is a property of the
production `ShareInFlightBatch` offset-tracking, which the broker-less mock does
not implement. Reaching it at the FFI boundary was considered via a production
`KafkaShareConsumer` + `acknowledge_by_offset` on an empty `current_fetch`
(which *does* return the error synchronously with no network). It was rejected:
Phase 2 exposes no `close()` FFI entry point (commit/close are Phase 3), so
`ShareConsumer_destroy` on a production consumer would detach — not join — its
dedicated background IO thread, leaving it spinning on connection retries for
the rest of the test-binary lifetime. Trading a suite-wide leaked/spinning
thread for a message already asserted at M9 is a poor bargain.

**Tracking note (so it is not lost):** the acknowledge error message remains
unasserted specifically *at the FFI ABI boundary* until the broker-driven
integration layer (Phase 3 / integration) lands. The existing
`test_acknowledge_polled_record` doc comment already records this deferral.

---

## Issue 3 (minor): production constructor error path unexercised by FFI tests — RESOLVED

**Was:** No FFI test drove `kafka_consumer_KafkaShareConsumer_new`; the
`out_error` write on a config-parse/blank-`group.id` failure was unchecked
through the ABI.

**Fix:** Added `test_kafka_share_consumer_new_blank_group_id_sets_out_error` in
`src/ffi/share_consumer.rs`. It builds a properties handle with a blank
(whitespace) `group.id`, calls `kafka_consumer_KafkaShareConsumer_new`, asserts
the returned handle is null, and asserts `out_error` carries the precise,
unwrapped cause via message content: `"You must provide a valid group.id"`
(DoD §3 — error-message content is part of the contract). This confirms the
FFI surfaces the raw build error (not the generic
`"Failed to construct Kafka share consumer"` wrapper), consistent with the
`new_share_consumer_with_wakeup` path used by the FFI.
