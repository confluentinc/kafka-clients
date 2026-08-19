---
name: review-fix-commit-rereview
description: Heuristics for re-reviewing fix commits (Manager loop round 2) — the four failure modes that survive a "fixed and verified" claim, from the ffi-callback-bridging re-review
metadata:
  type: project
---

Re-review pass over 7 fix commits (`af60796`, `bdb8b77`, `8a30e27`, `3002047`,
`7b913ea`, `9957c2b`, `c824fc6`) closing 13 findings from three critics. Every
original finding was genuinely fixed; all 5 new issues were **fix-adjacent**, not
fix-wrong. That distribution is the lesson: on a re-review, do not re-litigate
the original finding — audit the *neighbourhood* the fix defined.

**The four re-review failure modes, in the order they paid off:**

1. **Enumerate ALL exits against the Java construct the fix cited.** `7b913ea`
   cited Java's `finally { free.deallocate(buffer); }` and fixed the two *error*
   exits. A table of all six exits of `append_inner` showed a **success** exit
   (`try_append` block 1 returning `Ok(Some)` after a `continue` from block 2)
   still drops a `Some` buffer. When a fix quotes a `finally` / `try-with-resources`
   / RAII construct, build the exit table — Java's `finally` covers success exits
   too, and Rust translations reliably miss those.

2. **A newly-added invocation inherits the error *classification*, which may be
   wrong.** `7b913ea` routed the accumulator-error arm through
   `handle_api_exception`. Correct for `BufferExhaustedException` (a real
   `ApiException`), wrong for "Producer closed while send in progress" — Java
   throws a **bare `KafkaException`** there, so `catch (ApiException)` misses it
   and the callback never fires. `KafkaError::is_api_exception()` returns `true`
   for everything except `IllegalArgument|IllegalState|Serialization|Wakeup|
   ConcurrentModification`, so **Rust cannot represent "KafkaException but not
   ApiException"**. Any fix that starts firing callbacks / returning
   `Ok(failed-future)` on an `is_api_exception()` predicate needs each concrete
   error checked against its Java superclass chain. Firing where Java does not is
   as much a §9.5 break as dropping where Java does.

3. **A fix that establishes an invariant across N sites: find site N+1.**
   `3002047` made the app-side `rebalance_listener` mirror track Java's single
   `SubscriptionState.rebalanceListener` slot at all three `subscribe_*` sites
   — but `AsyncKafkaConsumer::unsubscribe()` still does
   `*self.rebalance_listener.lock().unwrap() = None`, while Java's *and Rust's*
   `SubscriptionState::unsubscribe()` deliberately keep it. The commit's own
   shipped FFI doc says "unsubscribe/close keep the registered listener". Grep
   every write to the field, not just the ones the fix touched.
   Why no test caught it: the bg-side `Arc` keeps the FFI object alive, so the
   `user_data_destroy` timing (what the C tests assert) is unchanged — only the
   *invocation* diverges. **Release-timing tests do not cover invocation.**

4. **A fix can make its own regression guard vacuous.** `af60796` made
   `NetworkThreadCloseHandle::wakeup()` poke the `Notify` instead of the
   `WakeupTrigger`. That silently defanged the last assertion of
   `process_background_events_ack_pokes_bg_wakeup`
   (`maybe_trigger_wakeup().is_ok()`) for the exact mutation its docstring
   names, and falsified two comments ("the latter fires the `WakeupTrigger`",
   "Both `Notify`s wake the poll"). The test still holds via a *different*
   assertion (`bg_wakeup_called`, set by the fixture wrapper) — worth saying so
   in the comment, or the next Actor deletes the one that still works. **After
   any primitive swap, re-run every mutation the affected tests claim to catch.**

**Also: a "made the comment true" resolution can still be false.** Critic-3's
Issue 3 said `AsyncKafkaConsumer::wakeup`'s second statement was a no-op with a
lying comment; the Actor kept the call and rewrote the comment to claim it is
needed to unblock an in-flight poll. But `run_once`'s poll `select!` already has
a `token.cancelled()` arm, so statement 1 (`wakeup_trigger.wakeup()`) already
returns the poll — the second is load-bearing **only** after
`wakeup_trigger.disable()` (close-path race via `ConsumerHandle::wakeup`). Verify
the *new* justification against the code, not just that the old one is gone.

**Non-issues verified — do not re-report:**

- `AppendError`'s `Some`/`None` split: only two construction sites, both before
  any batch takes the callback, so `None` is unreachable. No double-fire (FFI
  attaches `make_record_callback` as the sole callback; the `Ok(failed-future)`
  path fires nothing extra). No double-free (`buffer.take()` before the move).
- `af60796` fixture/production parity is real: production ctor,
  `spawn_dedicated_bg` and `make_test_consumer_with_channels` all call
  `build_network_thread_close_fns`; fixtures add only observability flags.
  Removed `wakeup` field is genuinely dead (only the ctor param survives).
- The empty-topics listener release asymmetry (real consumer releases while
  returning success; `MockConsumer` keeps) is Java's, both directions verified.
- `bdb8b77`'s session-lifetime `LogState`: all three `log_state_for` callers sit
  behind a client-exists check, and `next_id_` is monotonic, so a retained entry
  can never be reused by a new client.
- `c824fc6`'s two `Py_BEGIN_ALLOW_THREADS` regions contain no Python C-API call
  (`py_Producer_partitions_for` was restructured to hoist `err` for exactly that
  reason).

**Environment that worked:** `cargo test --features ffi --lib` (2228 tests, ~5 s
after build). C suites: `cargo build --release --features ffi` then the direct
`cc` recipe from `actor-executor/ffi_callback_bridging_phase1_notes.md` — the
23-test `test_consumer_callbacks.c` runs in seconds and its two
`make_real_consumer` fixtures need no broker. Docker/multilanguage was not needed
for any finding.

Related: [[review_ffi_callback_bridging]], [[review_wakeup_primitive_confusion]],
[[review_python_bindings_callbacks]].
