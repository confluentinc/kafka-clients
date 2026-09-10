# P5 — Actor 68 session analysis

Phase **P5** of `PLAN-python-interface-implementation.md`: the consumer client family
of the new `confluent_kafka` package — `Consumer` guard base, `KafkaConsumer`,
`MockConsumer`, the `Async*` peers, `ConsumerRebalanceListener` / `CommitCallback`,
and the caller-thread rebalance/commit callback contract (§31/§41, D25 gaps 1/8).
Branch `p5-consumer-wip` (from `dev_python-interface-implementation`). Worktree
`../ecfk-p5`. Committed, not pushed, not merged.

## What was built

### 1. `confluent_kafka/consumer/` client family (one Java class per file)
- `consumer_rebalance_listener.py` — `ConsumerRebalanceListener` class (Java's three
  methods, `on_partitions_lost` defaulting to `on_partitions_revoked`; positional) +
  the `CommitCallback = Callable[[dict|None, KafkaError|None], None]` alias.
- `consumer.py` — `Consumer(Generic[K, V])` non-instantiable base (guard →
  `TypeError` naming `KafkaConsumer`/`MockConsumer`), every method per spec §6.2 with
  the `@overload` stubs (`subscribe` topics/pattern, `seek` offset/offset_and_metadata,
  `close` timeout/option); `commit`/`commit_nowait`, `committed → dict[..,
  OffsetAndMetadata|None]`, `client_instance_id`/metric-registration → rule-10 error.
- `kafka_consumer.py` — `KafkaConsumer` (constructor only; `config` dict, typed
  deserializers infer K/V).
- `mock_consumer.py` + `_mock_driver.py` — `MockConsumer` two-stub ctor + the FULL Java
  mock surface (`add_record`, `update_{beginning,end,duration}_offsets`,
  `update_partitions`, `set_{poll,offsets}_exception`, `set_max_poll_records`,
  `rebalance`, `should_rebalance`, `reset_should_rebalance`, `schedule_nop_poll_task`,
  `last_poll_timeout`, `closed`; KIP-714 telemetry methods → rule-10 error).
- `async_consumer.py`, `async_kafka_consumer.py`, `async_mock_consumer.py` — the `Async*`
  peers: `async def` where the spec says (incl. `seek` per D21; `commit_nowait`,
  `assignment`/`subscription`/`paused`/`current_lag`/`group_metadata`/`metrics`/`wakeup`
  plain `def`).
- `_engine.py` — the FFI engine: native handle ownership, `_run_sync`/`_run_async`, the
  caller-thread pending-callback drain, commit-callback adapter.
- `_client_base.py` — `(submit, resolve, free)` op-spec builders + the synchronous state
  reads shared by sync/async.
- `_poll.py` — zero-copy poll deserialization (§27) + `RecordDeserializationError`
  payload attach (C32).
- `_conversions.py` — FFI-tuple ↔ P2 value-type conversions + `MetricValue`.
- `_config_resolve.py` — construction: config validation, serde resolution, typed
  construction error.
- `_unsupported.py` — the rule-10 `UnsupportedVersionError` helper.
- `__init__.py` — re-exports the family alongside the P2 value types.

Dropped per spec: `client_id()`, `enforce_rebalance()`.

### 2. FFI additions (`src/ffi/consumer.rs`, all with C tests)
- `subscribe(SubscriptionPattern)`: `Consumer_subscribe_pattern_async` (+listener,
  +caller-thread-listener) — core had `subscribe_subscription_pattern`, no FFI.
- `close(CloseOptions)`: `Consumer_close_options[_async]` (timeout_ms + group-membership
  operation code) — core had `close_options`, FFI only had `close_with_timeout`.
- Mock: `MockConsumer_{rebalance_async,set_poll_exception,set_offsets_exception,
  update_duration_offsets,set_max_poll_records,schedule_nop_poll_task,should_rebalance,
  reset_should_rebalance,closed,last_poll_timeout}`.
- **Caller-thread rebalance-callback delivery (the §31/D25-gap-1/8 fix):**
  `CallerThreadRebalanceListener` (a core `ConsumerRebalanceListener`) enqueues each
  callback + an ack `oneshot` on the consumer handle and parks the driving op on it; a
  one-shot C notify (`Consumer_set_pending_callback_notify`) wakes the embedder's
  blocking-op loop, which drains via `Consumer_next_pending_callback` /
  `PendingCallback_method` / `_partitions` / `Consumer_ack_pending_callback` on ITS OWN
  thread, runs the user listener, and acks. Subscribe installs it via
  `Consumer_subscribe_caller_thread_listener_async`. `kafka_common_Error_new` exposed as
  `KafkaError_new` for building a listener-throw error.
- `Consumer_KafkaConsumer_new_typed` → `(handle, error_int)` so the package raises the
  typed hierarchy (legacy `Consumer_KafkaConsumer_new` raises `RuntimeError`, kept for
  grpc/admin/producer which still import the legacy `consumer.py`).
- cbindgen: added `kafka_consumer_PendingCallback_t` +
  `kafka_consumer_Consumer_pending_callback_notify_t` to the export list; the destroy
  param is spelled inline `Option<unsafe extern "C" fn(*mut c_void)>` (cbindgen breaks on
  `Option<typedef_alias>`).

### 3. Callback-thread contract (deliverable 3 — the crux)
The legacy binding ran the listener on the Rust dispatcher thread and deadlocked a
reentrant `await consumer.commit()` (D25 gaps 1/8). P5 delivers the listener on the
**caller's thread** and the rebalance blocks until it returns. Async coroutine listeners
are awaited on the loop; `commit_nowait`/`on_commit` follow the same drain. The stale-
notify-snapshot bug (async re-registers the notify per op via a loop-hop closure; a
subscribe-time snapshot notified a dead Event → hang) is fixed by reading the notify slot
(`Arc<Mutex<Option<..>>>`) at callback time. Verified by the two §31 regression tests
(commit/reenter from `on_partitions_revoked`; rebalance blocked until the listener
resolves) for both sync and async.

### 4. Deserialization (§5.4/§27)
Native `ConsumerRecords` batch owns the fetched bytes; `deserialize_batch` runs the
key/value deserializers on the batch's `memoryview`s (zero-copy) on the caller's thread;
`bytes_deserializer` copies, `memoryview_deserializer` borrows (the memoryview pins the
batch via the C `_BorrowedBytes` buffer protocol — no extra retention needed). A failing
deserializer raises `RecordDeserializationError` with `topic_partition()`/`offset()`/
`key_buffer()`/`value_buffer()`/`origin()` + `__cause__`; the position does not move.

## Java tests translated / skipped

| Java test | Status |
|---|---|
| `MockConsumerTest` (all 8) | translated: testSimpleMock, testConsumerRecordsIsEmptyWhenReturningNoRecords, shouldNotClearRecordsForPausedPartitions, endOffsetsShouldBeIdempotent, testDurationBasedOffsetReset, testRebalanceListener, testRe2JPatternSubscription, shouldReturnMaxPollRecords |
| `KafkaConsumerTest` config/arg slice | translated: testEmptyGroupId (outer wrapper — C34), group.id optional/assign, testClientInstanceIdInvalidTimeout, config-must-be-dict, positional→TypeError, subscribe/seek combination errors |
| `KafkaConsumerTest` testGroupIdWithWhitespace | **skipped** (C35 — core does not trim group.id) |
| `KafkaConsumerTest` ~65 MockClient/mock-broker tests | **skipped** (poll/fetch/commit/heartbeat/close-with-broker/auth/timeout/lag need Java's internal MockClient — no mock broker on this surface) |
| `KafkaConsumerTest` metric-reporter/SASL/JMX/plugin/log tests | **skipped** (need a metrics-reporter/JMX/SASL/log double not present) |

Plus the two §31 regression tests, and legacy `test_consumer.py`/`test_consumer_callbacks.py`
behaviors migrated (seek/position/pause-resume/beginning-end-offsets/commit-committed/
zero-copy memoryview/group-metadata/async poll-commit-seek).

## Test counts
- `test/unit/test_consumer_family.py`: **48 passed, 1 skipped**.
- Full unit suite: **734 passed, 3 skipped** (686 P1–P4 baseline incl. legacy consumer
  tests + 48 new).
- `mypy --strict` clean over 66 files (incl. `test_typing.py` consumer-family additions).
- C tests: new `test_mock_consumer` cases (p5 mock helpers, close_options, caller-thread
  rebalance drain/ack) pass.

## Core gaps / clarifications logged (C26–C35)
- **C26** legacy `consumer.py` NOT retired (grpc/admin/producer import it).
- **C27** caller-thread callback FFI mechanism (the §31/gap-1/8 fix).
- **C28** `MockConsumer.add_record` carries serialized bytes (mock applies serdes on poll).
- **C29** `schedule_poll_task` general form → rule-10 (core task takes `&mut MockConsumer`);
  `schedule_nop_poll_task` wired.
- **C30** KIP-714 telemetry → `UnsupportedVersionError`.
- **C31** mock exception setters inject via wire-code round-trip (4-class collapse trap).
- **C32** `RecordDeserializationError` payload attached at raise time (closes P1 C4 for the
  deserialize path).
- **C33** `metrics()` values are a `MetricValue` (Metric protocol has no concrete class).
- **C34** construction error surfaces the outer `KafkaError` wrapper, not the buried
  `InvalidGroupIdException` cause (FFI has no cause-chain accessor).
- **C35** core does not trim `group.id` before the empty check (whitespace test skipped).

## Verification
- `cargo xtask format-check` clean; `cargo xtask lint` (clippy) clean.
- `cargo build --features ffi` clean (cbindgen header regenerated with the new types).
- `make devel-build-python` builds the C extension + editable install.
- `mypy --strict` clean; `pytest test/unit` 734 passed / 3 skipped; C tests pass.
- Worktree note: the `kafka` and `bindings/c/tests/unity` submodules must be
  `git submodule update --init`'d (worktrees don't auto-checkout submodules) — else
  `test_errors.py` (reads Java sources) and the C test build fail. The fresh worktree venv
  needed `pip install pytest mypy pytest-asyncio`.

## Concurrency
Actor 67 (producer) works in `../ecfk-p4`. Per the task I did NOT edit `src/ffi/producer.rs`,
`src/ffi/common.rs`, or `confluent_kafka/producer/*`. `_confluentkafka.c` edits are confined
to consumer natives + the shared module table. `cbindgen.toml` gained two consumer types.

## Commits
1. `6f6bf40c` — P5: consumer FFI — caller-thread rebalance callbacks, mock methods,
   close(CloseOptions), subscribe(pattern) (+ C-ext natives, C tests, shortened pre-commit hook).
2. `48b3ad86` — P5: confluent_kafka.consumer client family (+ tests, clarifications, this report).

Branch `p5-consumer-wip` (from `dev_python-interface-implementation`). Not pushed, not merged —
the Manager runs the full `make verify-sandbox` before merging.

`COMMENTS.68.md` created empty in the repo root; empty at start and end.
