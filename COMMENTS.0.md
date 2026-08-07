# Critic 0 — re-review of the fix commits (`af60796`, `bdb8b77`, `8a30e27`, `3002047`, `7b913ea`, `9957c2b`, `c824fc6`)

Scope: correctness of the fixes themselves and regressions they may have
introduced. 5 issues found (2 Bug / Behavior Mismatch worth fixing, 3 lower
severity). The verification list of everything that checked out clean is at the
bottom.

---

## Issue 2: `7b913ea`'s new `handle_api_exception` routing fires the user callback for "Producer closed while send in progress", where Java rethrows and never invokes it

- **File**: `src/producer/kafka_producer.rs:614-627`; error constructed at `src/producer/internals/record_accumulator.rs:630-637`; contract documented at `src/ffi/producer.rs:399-408`
- **Severity**: Behavior Mismatch
- **Java Reference**: `RecordAccumulator.java:427-428` — `if (closed) throw new KafkaException("Producer closed while send in progress");` (a **bare** `KafkaException`, not an `ApiException`). `KafkaProducer.java:1056-1068` catches `ApiException` (fires the callback, returns `FutureFailure`) and `KafkaProducer.java:1073-1077` catches `KafkaException` (`errors.record()`, `interceptors.onSendError(...)`, **`throw e`** — the callback is not invoked).
- **Description**:
  The Rust translation of that throw is
  `KafkaError::with_message(Errors::UnknownServerError, "Producer closed while
  send in progress")`, i.e. `KafkaError::Generic(..)`, and
  `KafkaError::is_api_exception()` (`kafka_error.rs:513-522`) returns `true` for
  everything except `IllegalArgument | IllegalState | Serialization | Wakeup |
  ConcurrentModification`. So the closed-mid-send error takes the **new**
  `is_api_exception` arm and now calls
  `handle_api_exception(error, topic, partition, callback)`.

  Before `7b913ea` this path returned `Ok(failed-future)` without firing —
  which was already a divergence from Java's `throw`, but at least honoured
  "Java does not invoke the callback here". The fix adds the callback
  invocation, so a user callback now fires with `(null-metadata,
  UnknownServerError)` on a code path where Java guarantees it does not fire at
  all. CLAUDE.md §9.5's exactly-once obligation cuts both ways: firing where
  Java does not is as much a contract break as dropping where Java does.

  The commit also writes the divergence into the public C contract
  (`src/ffi/producer.rs:406`: "...as well as rejections *inside* it (buffer
  exhaustion / `max.block.ms` expiry, **producer closed mid-send**)"), so a C
  binding will now be told to expect both handles for a case Java never
  reports through the callback.

  The buffer-exhaustion half of the fix is correct and unaffected —
  `BufferExhaustedException extends TimeoutException extends
  RetriableException extends ApiException`, so Java *does* fire there. Only the
  `closed` error is misclassified.
- **Expected**: the closed-mid-send error should not satisfy
  `is_api_exception()`, so `do_send_bytes` takes the non-`ApiException` arm,
  returns `Err(error)` and drops the callback unfired — which is what that arm's
  own comment (`kafka_producer.rs:623-626`) says it is for. Then correct
  `src/ffi/producer.rs:406` to drop "producer closed mid-send" from the
  fires-with-both-handles list. (Rust currently has no variant that is a
  Java `KafkaException` but not an `ApiException`; introducing one, or
  special-casing this error, is the fix direction. A producer-level test for the
  closed path is missing either way — `test_append_returns_the_callback_when_closed`
  only pins the accumulator hand-back.)
- **Actual**: the callback fires once with placeholder metadata and the error,
  and `Ok(failed-future)` is returned.

### Related minor point on the same commit

`impl From<AppendError> for KafkaError` (`record_accumulator.rs:127-131`) has no
call site. It re-enables exactly the silent drop `AppendError` was introduced to
make unrepresentable: any future `accumulator.append(...)?` inside a function
returning `Result<_, KafkaError>` compiles and discards the handed-back callback
without a diagnostic. Consider removing it (nothing uses it) or documenting the
hazard next to it.

---

## Issue 3: `3002047` restored the single-slot invariant on the three subscribe paths but `unsubscribe()` still clears the app-side mirror, contradicting the contract shipped in the same commit

- **File**: `src/consumer/async_kafka_consumer.rs:3058-3059`
- **Severity**: Behavior Mismatch
- **Java Reference**: `SubscriptionState.java:347-355` — `unsubscribe()` clears `subscription`, `groupSubscription`, `assignment`, `assignedTopicIds`, `subscribedPattern`, `subscriptionType`, bumps `assignmentId`, and does **not** touch `rebalanceListener` (`registerRebalanceListener` is only called from `:193`, `:199`, `:205`, `:219`). `AsyncKafkaConsumer.java:1830-1855` (`unsubscribe`) does not clear it either.
- **Description**:
  `3002047`'s stated invariant is "Java has ONE slot and the Rust app-side
  mirror must track it", and the FFI contract it shipped in the same commit
  states this explicitly
  (`src/ffi/consumer.rs`, `..._user_data_destroy_t` → "# When it does NOT fire":
  "`unsubscribe` / `close` — they clear the subscription but keep the registered
  listener, matching Java's `SubscriptionState.unsubscribe()`").

  But `AsyncKafkaConsumer::unsubscribe()` ends with

  ```rust
  // Reset the listener field — the previous subscription is gone.
  *self.rebalance_listener.lock().unwrap() = None;
  ```

  while `SubscriptionState::unsubscribe()`
  (`subscription_state.rs:805-813`) correctly leaves
  `self.rebalance_listener` untouched. The two Rust slots therefore disagree
  after `unsubscribe()`, and the one that actually invokes the callback is the
  app-side mirror (`process_background_events`, `:3274`). This is the same
  defect class `3002047` fixed, in the opposite direction: the mirror now
  clears where Java keeps.

  Observable consequence: a `ConsumerRebalanceListenerCallbackNeeded` that the
  bg task enqueues while `SubscriptionState.rebalance_listener()` is still
  `Some` but which the app drains *after* `unsubscribe()` returns takes the
  `None => Ok(())` arm (`:3300-3304`) and silently skips the user's
  `on_partitions_revoked` / `on_partitions_lost`, where Java would invoke it.
  `leave_group_on_close` (`:5308`) reads the same mirror.

  The C release-timing tests do not catch it because the bg-side
  `SubscriptionState` registration still holds an `Arc`, so the FFI
  `user_data_destroy` hook timing is unchanged — the shipped doc happens to
  stay accurate about *release*, and is wrong only about *invocation*. No Rust
  test asserts the mirror is `None` after `unsubscribe()`, so removing the two
  lines does not break anything.
- **Expected**: delete the `= None` write (and its comment) so `unsubscribe()`
  leaves the mirror alone, matching `SubscriptionState::unsubscribe()`, Java,
  and the FFI doc. Add a Rust test asserting the mirror survives
  `unsubscribe()` — the mirror of the assertion `3002047` added to
  `subscribe_with_listener_stores_listener`.
- **Actual**: `unsubscribe()` clears the app-side listener while the bg-side
  registration keeps it.

---

## Issue 4: `af60796` invalidated the rationale of the Phase-41b regression guard it did not update — two comments and a test docstring now assert behavior the code no longer has, and one assertion is vacuous

- **File**: `src/consumer/async_kafka_consumer.rs:3339-3348` (comment), `:7089-7097` (test docstring), `:7159-7169` (the two guard assertions); also `src/consumer/internals/consumer_network_thread.rs:1108-1109`
- **Severity**: Design Flaw (documentation accuracy + test strength)
- **Description**:
  `af60796` correctly went around rewriting the comments its change falsified
  — but missed the three that matter most, because they are the ones justifying
  the guard for the original Phase-41b bug.

  `:3339-3348` still reads:

  > It must be the application-event `Notify` and NOT
  > `network_thread_close.wakeup()`: **the latter fires the `WakeupTrigger`**,
  > which is the *user-facing* `Consumer::wakeup()` cancellation token. […]
  > Both `Notify`s wake the bg loop's network poll (`run_once` `select!` has an
  > arm for each)

  After `af60796`, `network_thread_close.wakeup()` fires the *same*
  `event_notify` (`build_network_thread_close_fns`), not the trigger, and there
  is only one `Notify` — the other `select!` arm is `token.cancelled()`. Both
  sentences are now false.

  `process_background_events_ack_pokes_bg_wakeup` (`:7089-7097`) repeats the
  same claim in its docstring, and its last assertion

  ```rust
  assert!(consumer.wakeup_trigger.maybe_trigger_wakeup().is_ok(), ...)   // :7166
  ```

  is now **vacuous for the mutation the docstring describes**: substituting
  `network_thread_close.wakeup()` for `wake_background_task()` no longer
  cancels the token, so this assertion passes under the mutation. The test still
  holds the line, but only through the preceding
  `!handles.bg_wakeup_called.load(...)` assertion (`:7160-7164`), which works
  because the fixture wraps the production closure with an observability flag.
  Worth stating explicitly so the next Actor does not "simplify" the
  `bg_wakeup_called` assertion away as redundant with the (now toothless) one
  below it.

  `consumer_network_thread.rs:1108-1109` has the same residue: "so
  `signal_close()` followed by `wakeup.wakeup()` exits immediately" names the
  field `af60796` removed.
- **Expected**: rewrite `:3339-3348` and the `:7089-7097` docstring to the
  post-`af60796` mechanism (both wakes go through the one `event_notify`; the
  reason to call `wake_background_task()` rather than
  `NetworkThreadCloseHandle::wakeup()` here is now layering/clarity, not the
  trigger); either drop `:7166` or replace it with an assertion that still
  discriminates; fix the `consumer_network_thread.rs:1108` comment to name the
  `Notify`.
- **Actual**: three comments describe the pre-`af60796` primitive, and the
  documented mutation no longer trips the assertion written for it.

---

## Issue 5: `af60796`'s rewrite of `AsyncKafkaConsumer::wakeup`'s comment (Critic-3 Issue 3) still overstates why the second statement is needed

- **File**: `src/consumer/async_kafka_consumer.rs:2714-2731`
- **Severity**: Design Flaw (minor — comment accuracy on a redundant call)
- **Description**:
  Critic-3 Issue 3 was "a no-op second call carrying a comment that describes
  behavior the code does not have", and the resolution was "kept the second
  call and made it true". The call is now a real bg poke, but the *necessity*
  claim in the new comment is still wrong:

  > Without it a `wakeup()` arriving while the bg loop is in its network poll is
  > only acted on up to `MAX_POLL_TIMEOUT_MS` later.

  `self.wakeup_trigger.wakeup()` on the line above cancels the token the bg task
  observes (`WakeupTrigger::wakeup` → `sender.borrow().cancel()`,
  `wakeup_trigger.rs:113-121`), and `run_once`'s poll `select!` has a
  `token.cancelled()` arm that fires `network_wakeup.notify_one()`
  (`consumer_network_thread.rs:727-731`). So the in-flight poll is already
  returned at a safe boundary by the first statement whenever the trigger is
  enabled. The second statement is genuinely load-bearing only after
  `close_internal` step 1 has called `wakeup_trigger.disable()` (so the token is
  never cancelled) — e.g. a `ConsumerHandle::wakeup()` racing a `close()` — and
  is otherwise a redundant stored `Notify` permit that causes one spurious early
  poll return.
- **Expected**: say what is actually true — the token arm already covers the
  enabled case; the poke is the fallback for the disabled-trigger window (and
  keeps `ConsumerHandle::wakeup` uniform). Or drop the call and note the
  disabled-trigger case. Either way the current justification should not survive
  a third round.
- **Actual**: the comment asserts a `MAX_POLL_TIMEOUT_MS` delay that the
  `token.cancelled()` arm prevents.

---

## Verified clean (no issue raised)

**`7b913ea` — `AppendError` / callback hand-back**
- Callback-`Some`/`None` split is sound: `AppendError` is constructed at exactly
  two places (`try_append`'s `closed` check, before any batch is touched; the
  `free.allocate` failure, where the callback was just handed back by the
  previous `try_append`). No path exists where a batch took ownership and an
  `AppendError` is still returned, so no `None`-with-lost-callback case.
- **No double-fire**: `append` never invokes the callback itself (pinned by both
  new accumulator tests); `do_send_bytes` fires it once via
  `handle_api_exception`; the FFI `send_with_callback` (`ffi/producer.rs:1148-1163`)
  and `send_batch_async` (`:1501`) attach `make_record_callback` as the *only*
  callback and fire nothing extra on the `Ok(failed-future)` path, so the C
  callback runs exactly once.
- **No double-free**: the `append_new_batch` exit `buffer.take()`s before the
  move, and both new `if let Some(b) = buffer.take()` guards are no-ops once the
  buffer has been handed to a batch or already deallocated.
- Manual `Debug` (error + `callback_returned: bool`) keeps all `.unwrap()` /
  `.expect()` call sites compiling and hides nothing asserted on; `RecordAppendResult`
  is still not `Debug`, which is why the new tests `match` instead of `unwrap_err`.
- **All `append` callers updated**: the only production caller is
  `kafka_producer.rs:589`; the remaining 40+ sites are `#[cfg(test)]` in
  `record_accumulator.rs` / `sender.rs`.
- `test_callback_invoked_on_buffer_exhaustion` asserts both handles, the
  `INVALID_OFFSET` placeholder, and the future's `BufferExhausted` — a real
  exactly-once assertion, not `>= 1`.

**`af60796` — shared close-fn builder**
- Production ctor (`:2328-2331`), `spawn_dedicated_bg` (`:10083`) and
  `make_test_consumer_with_channels` (`:5774-5786`) all obtain both closures
  from `build_network_thread_close_fns`; the fixture's only additions are the
  observability flags (and a fixture-local `running` flag, which is inert
  because that fixture has no real bg loop). The divergence that hid Critic-3
  Issue 1 cannot recur.
- No production site fires the `WakeupTrigger` for an internal wake any more:
  the only two remaining `wakeup_trigger.wakeup()` calls are
  `AsyncKafkaConsumer::wakeup` (`:2715`) and `ConsumerHandle::wakeup` (`:233`),
  both user-facing. `ConsumerNetworkThread::{wakeup, signal_close}` are now
  delegate-/Notify-only and still have no production callers.
- The removed `wakeup: WakeupTrigger` field is genuinely dead: `wakeup` now
  appears only as the ctor parameter (`consumer_network_thread.rs:289`) used to
  derive `wakeup_rx`.
- `close_handle_wakes_bg_task_after_wakeup_trigger_disabled` is bounded on both
  sides (5 s `tokio::time::timeout` + `elapsed < 2 s` against
  `MAX_POLL_TIMEOUT_MS = 5000`) and the 200 ms pre-sleep only makes the test
  stricter, not racier — `Notify::notify_one` stores a permit, so a poke landing
  before the park is not lost. `await_bg_parked` is a bounded 1 s spin with an
  explicit panic message. Not flaky.
- Poking `event_notify` has no side effect beyond waking: the `run_once` arm
  only calls `network_wakeup.notify_one()` and sets `poked`; a spurious permit
  costs one early poll return.
- No test asserts "`app_event_notify` was not poked" after a path that now pokes
  it (only the pre-condition check at `:7123-7129`).

**`3002047` / `8a30e27` — listener contract**
- The `None`-overwrite is Java-faithful: `AsyncKafkaConsumer.java:2022-2023`
  → `subscribeInternal(topics, Optional.empty())` →
  `SubscriptionState.java:192-196` `registerRebalanceListener(Optional.empty())`.
  `SubscriptionState::register_rebalance_listener` (`:607-614`) assigns the
  `Option` verbatim, so app-side mirror and bg-side slot agree on all three
  subscribe paths.
- `subscribe_internal_topics` / `_pattern` / `subscribe_to_regex` are reached
  only from the six public subscribe methods — no internal caller can
  accidentally clear the listener.
- The empty-topics arm returns before `listener_for_app_side` is built, so the
  Arc really is dropped (release observed) — consistent with the new C test.
- `MockConsumer::subscribe_with_listener` (`mock_consumer.rs:483-493`) matches
  `MockConsumer.java:196-200` (no empty short-circuit, registers before the
  type check), so the mock-vs-real asymmetry the docs describe is Java's.
- All 23 C tests in `test_consumer_callbacks.c` compiled and passed here
  (direct `cc` recipe, `libconfluent_kafka.a` from `--release --features ffi`),
  including the three new arms and the two real-consumer fixtures — no broker
  contacted, no hang.

**`bdb8b77` — harness**
- `LogState` is now `unique_ptr` in both services, `Close` neither erases nor
  frees, and both `log_state_for` callers (`server.cc:474` Send, `:894`
  CommitAsync, `:815` Subscribe) are already behind a client-exists check, so
  the now-non-null-after-Close return cannot be used on a destroyed client. Ids
  are monotonic (`next_id_`), so a retained entry cannot collide with a new
  client.
- `wait_for_kind_settled` is correct and bounded (first-match wait, then a flat
  `grace` of re-reads, returning the last snapshot); it does not extend on
  activity, and the zero-match case still fails the `== 1` assertion loudly.
  `poll_until_kind` correctly left alone.

**`9957c2b` / `c824fc6` — Python**
- `kafka_consumer_Consumer_seek_with_metadata_async` is a faithful mirror of its
  sync sibling (same `metadata == NULL → String::new()`, same `leader_epoch < 0
  → None`, same inline-callback error path for a
  `OffsetAndMetadata::with_leader_epoch` failure) and error content is identical
  to the removed sync path — both surface whatever `c.seek_with_metadata(...)`
  returns, so no message-parity regression.
- The removed `py_Consumer_seek` / `py_Consumer_seek_with_metadata` have no
  remaining references: method table updated, `grpc_server.py:362-364` uses the
  sync `Consumer.seek`, `grpc_server_async.py` now awaits, `ConsumerHandle.seek`
  is a separate FFI family. The new `Consumer.seek` additionally calls
  `_check_closed()`, which is *more* Java-faithful (Java's `seek` on a closed
  consumer throws), and no test asserted the old behavior.
- Coroutine rejection happens in `_CommitCallbackAdapter.__init__`, i.e. before
  `_lib.Consumer_commit_async(...)` is called — no half-committed state, and
  `test_coroutine_commit_callback_is_rejected_on_the_sync_consumer` asserts
  `committed([tp]) == {}` for both overloads.
- `offsets_to_arrays`: the `PyUnicode_Check` pre-check runs before any pointer
  is stored, `ok = 0` reaches the existing `if (!ok) { offset_arrays_free(out);
  return -1; }`, and all four callers already bail on `n < 0` before taking a
  reference. No leak, no leftover exception indicator.
- `c824fc6`: no Python C-API call sits inside either
  `Py_BEGIN_ALLOW_THREADS` region — `py_Producer_flush` releases around the
  bare `kafka_producer_Producer_flush` and builds its `PyLong` after
  `Py_END_ALLOW_THREADS`; `py_Producer_partitions_for` was restructured to
  declare `err` first precisely so `Py_BuildValue` stays outside. Correct.
- `AsyncConsumer._loop` caching: `_run_async` refreshes it on every awaited op
  and `_listener_loop` refreshes it whenever a loop is running, so the cache is
  only consulted from a worker thread — the documented usage. A consumer driven
  from two different event loops could observe a stale loop, but that requires
  reusing one consumer across `asyncio.run(...)` calls; not raised.
- No contradictory assertions across the three actors' test additions (Actor 1's
  C suite, Actor 2's Python suite and Actor 3's server.cc/harness changes touch
  disjoint surfaces).

**Build/test**: `cargo test --features ffi --lib` → 2228 passed / 0 failed / 1
ignored, including `close_handle_wakes_bg_task_after_wakeup_trigger_disabled`
and the three new producer/accumulator tests.
