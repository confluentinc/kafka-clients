---
name: ffi-callback-bridging-phase5
description: FFI callback-bridging Phase 5 — unsubscribe/close do NOT release the rebalance listener (Java-faithful), take_error as box_error's inverse, negative-assert mutation check for the §31 blocking test
metadata:
  type: project
---

Phase 5 of the FFI/Python callback-bridging plan (`~/.claude/plans/wiggly-bouncing-babbage.md`)
landed as `305cb49` on `ffi-callback-bridging`:
`kafka_consumer_ConsumerRebalanceListener_t`,
`kafka_consumer_Consumer_subscribe_with_listener{,_async}`,
`kafka_consumer_MockConsumer_rebalance`, and `common::take_error`.

**Why:** C had no `ConsumerRebalanceListener` equivalent, and no way to drive one
deterministically without a broker.

**How to apply** — findings that carry into Phases 6-7:

1. **`unsubscribe()` / `close()` do NOT release a registered rebalance listener.**
   `SubscriptionState::unsubscribe` clears the subscription, group subscription,
   assignment, pattern and subscription type but leaves `rebalance_listener`
   untouched — faithful to Java's `SubscriptionState.unsubscribe()`. The
   registration is released only by a **subsequent `subscribe*` call** (which
   calls `register_rebalance_listener(None)` / replaces it) or by dropping the
   consumer. The plan's phase-5 bullet said "user_data_destroy fires on
   unsubscribe/close" — that is wrong; the docs and the C test were written
   against the real behavior instead. Python's `_ListenerAdapter` DECREF timing
   in Phase 6 must follow the same rule.

2. **`take_error(ptr) -> Option<KafkaError>` (added to `src/ffi/common.rs`) is
   the inbound counterpart of `box_error`** — `Box::from_raw` back to
   `KafkaErrorInner` and hand out the inner `KafkaError`. Needed because the
   rebalance callbacks are the first FFI callbacks whose *return value* Rust must
   interpret. `kafka_common_KafkaError_new` (Phase 1) produces exactly such a
   handle, so a C/Python listener can build one; note the C side must NOT destroy
   a handle it returns.

3. **The §31 "rebalance blocks until the listener returns" test needed a
   mutation check to prove it isn't vacuous.** Flipping the listener's condvar
   loop to a no-op (`while (0 && !p->release)`) in a scratch copy of the C file
   made `TEST_ASSERT_EQUAL_INT(0, rebalance_returned)` fail as required. Cheap
   (C-only recompile, no cargo rebuild) and worth doing for any
   "operation has NOT completed yet" assertion — those pass trivially if the
   observed flag never gets set for an unrelated reason.

4. **`MockConsumer::rebalance` is the whole test surface and it constrains the
   fixtures**: it needs an `AutoTopics` subscription (a manually-assigned
   consumer fails with "manual assignment in use"), it fires
   `on_partitions_revoked` only when something was removed, it fires
   `on_partitions_assigned` **unconditionally when a listener is registered**
   (with a possibly-empty *added* list, not the full assignment), and it never
   fires `on_partitions_lost`. So the Java-default `lost → revoked` delegation is
   unreachable from C and had to be asserted in a Rust unit test against the
   adapter directly. `src/ffi/consumer.rs` had **no** `#[cfg(test)] mod tests`
   before this phase — it does now.

5. **Phase 4's guard-probe trick generalizes to "prove an FFI call consumed its
   argument on the error path".** Calling `subscribe_with_listener` from inside a
   Phase-3 commit callback (app thread still holds the owner guard) yields a
   deterministic `ConcurrentModification`, and the destroy hook still fires
   exactly once — no threads, sleeps or fault injection needed.

6. Only **nullable** fn-pointer params need the inline-`Option<unsafe extern "C"
   fn(...)>` spelling for cbindgen (Phase 3 finding); required ones render fine
   from the `_t` alias. Keeping the aliases "used" (so `#![deny(warnings)]`
   doesn't trip on `dead_code`) was done by typing the private
   `RebalanceListenerInner` fields and a private
   `new_rebalance_listener_inner(...)` helper with them.

Environment notes from Phases 1-4 all still held (no cmake — compile the Unity
suites directly against `target/release/libconfluent_kafka.a`; no venv;
`cargo xtask lint` skips the `ffi` feature so run
`cargo clippy --all-targets --features ffi -- -D warnings` separately). One
addition: `cargo doc` on this repo already fails with ~6 pre-existing
`broken_intra_doc_links` errors, so it cannot be used as a pass/fail gate —
grep its output for *your* item names instead. In particular a
`[`X`](crate::ffi::some_module)` link to a `pub(crate)` module is a
`private_intra_doc_links` error.

See [[ffi-callback-bridging-phase1]], [[ffi-callback-bridging-phase2]],
[[ffi-callback-bridging-phase3]], [[ffi-callback-bridging-phase4]].
