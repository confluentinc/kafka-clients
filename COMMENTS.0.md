# Critic 0 — re-review of the fix commits (`af60796`, `bdb8b77`, `8a30e27`, `3002047`, `7b913ea`, `9957c2b`, `c824fc6`)

Scope: correctness of the fixes themselves and regressions they may have
introduced. 5 issues found (2 Bug / Behavior Mismatch worth fixing, 3 lower
severity). The verification list of everything that checked out clean is at the
bottom.

**All 5 issues are resolved** — each original comment plus its resolution is in
`COMMENTS.DONE.0.md`. Fix commits: `f7e5eff` (issue 1) and `b53c941` (issue 2),
both `fixup! 7b913ea`; `62ea49e` (issue 3), `fixup! 3002047`; and the
`fixup! af60796` commit for issues 4-5.

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
