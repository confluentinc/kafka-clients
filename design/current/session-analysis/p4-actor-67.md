# P4 — Actor 67 session analysis

Phase **P4** of `PLAN-python-interface-implementation.md`: the producer family
(`confluent_kafka.producer`) — `Producer` / `KafkaProducer` / `MockProducer` and
their `Async` peers, built on the Rust core's C FFI. Branch `p4-producer-wip`
(worktree `../ecfk-p4`, from `dev_python-interface-implementation` HEAD
`6af2cbb6`). Committed there, NOT merged (Manager merges).

## Files

New (`bindings/python/confluent_kafka/producer/`):
- `producer.py` — `Producer` non-instantiable base (all methods).
- `async_producer.py` — `AsyncProducer` base (async peers).
- `kafka_producer.py` — `KafkaProducer` / `AsyncKafkaProducer` + serde/config helpers.
- `mock_producer.py` — `MockProducer` / `AsyncMockProducer` (pure-Python translation of `MockProducer.java`).
- `_base.py` — shared `_ProducerState`, native-record build (zero-copy), metrics/partition-info converters.
- `_send.py` — completion marshaling (`_metadata_from_ffi` via `RecordMetadata_copy_full`, `_completion_to_python`, `_invoke_on_delivery`).

Modified:
- `producer/__init__.py` — exports the family + `DeliveryCallback` alias.
- `producer/record_metadata.py` — added `RecordMetadata._from_ffi` classmethod.
- `bindings/python/_confluentkafka.c` — 3 new natives (see FFI).
- `src/ffi/producer.rs` — 3 new FFI functions + 3 Rust tests.
- `design/current/implementation-clarifications.md` — C22-C25.

New tests: `test/unit/test_producer_family.py` (71 cases).

## FFI added (each: Rust + C ext native + Rust unit test)

1. `kafka_producer_RecordMetadata_copy_full` — extracts all 6 RecordMetadata
   fields (topic/partition/offset/timestamp/serialized_key_size/
   serialized_value_size) in one call and destroys the handle. Used by the real
   producer's send-completion marshaling. Rust test `test_record_metadata_copy_full`.
2. `kafka_producer_Producer_close_timeout_async` — Java `close(Duration)`;
   `timeout_ms == -1` selects the default untimed flushing close (CLAUDE.md §2
   presence-rule, ONE entry point for `close()`/`close(Duration)`). Refactored
   `flush_or_close_async` → `flush_or_close_async_timeout`. Rust tests
   `test_close_timeout_async_succeeds` / `_default_sentinel`.
3. `Producer_send_offsets_to_transaction_fields_async` (C-ext native only) —
   the new-package `ConsumerGroupMetadata` is pure-Python (no native handle), so
   this builds a temp native CGM handle from its 4 fields via
   `kafka_consumer_ConsumerGroupMetadata_new`, calls the existing async FFI
   (which marshals gm synchronously), and destroys it.

**Reverted mid-phase (no Python caller → would violate DoD):** an FFI-backed
MockProducer surface (`error_from_ffi_code` inverse in `src/ffi/common.rs`;
`MockProducer_error_next_with_code` + ~19 scalar accessors / setters /
`check_send` in `src/ffi/producer.rs`; `check_send_precondition` in
`src/producer/mock_producer.rs`). See "Core gaps / MockProducer pivot" below —
`src/ffi/common.rs` and `src/producer/mock_producer.rs` end this phase unchanged.

## Zero-copy argument (§12 / DoD #10 / C10)

The P2 `ProducerRecord[K, V]` is a pure-Python value type holding the user's
`K`/`V` objects. At `send()` the serializers run on the caller thread (§5.4),
producing `bytes` for key and value. Those `bytes` are passed straight to the
native `_confluentkafka.ProducerRecord(topic, value_bytes, key_bytes, …)`, whose
`tp_init` INCREFs the `bytes` and stores raw pointers into them
(`record_struct.key/value` = `PyBytes_AsString(...)`), with NO copy. The C
extension's existing batching engine writes those pointers into the batch buffer.
So there is exactly one allocation on the send path — the serializer's own output
`bytes`, which is unavoidable (the user's chosen encoding) — and no intermediate
copy of key/value bytes. The pure-Python record and the native record are
reconciled by constructing the native one from the serialized bytes at send time
(C10 option "pass its fields straight into the native send").

## Core gaps / MockProducer pivot (C22, C25)

The Rust core is fundamentally async (batching send task + dispatcher-thread
completions), so a MockProducer wrapped over it CANNOT reproduce Java's
`MockProducer` synchronous completion (`md.isDone()` immediately after `send`,
`completeNext` right after `send`), and its serialized-`Vec<u8>` history would not
compare equal to the original `ProducerRecord` a test constructs. Java's
`MockProducer`, by contrast, is a self-contained synchronous in-memory mock with
no I/O thread. So the faithful translation (DoD #2, one Java class → one Python
class) is a **pure-Python** `MockProducer` that mirrors `MockProducer.java`
field-for-field and method-for-method — which is what shipped. It touches neither
the FFI nor the core mock; the mock-surface FFI drafted earlier was reverted.

Real-client core gaps (rule 10 → raise mapped `KafkaError`): `client_instance_id`,
`register_metric_for_subscription`, `unregister_metric_from_subscription` (the
core `Producer` trait has no KIP-714 telemetry). `client_instance_id`'s negative
timeout is validated Python-side first with Java's exact message. The MockProducer
implements the full telemetry surface itself (in-memory, Java-faithful).

## Java tests translated / skipped

`MockProducerTest.java` (55 @Test, 0 parameterized) → 51 translated + 2
transaction-flag-reset variants folded; **skipped**:
- `testPartitioner` — needs `Cluster` / `RoundRobinPartitioner` / `PartitionInfo`
  seeding, none of which exist on the Python surface (custom partitioner deferred
  to the plugin pass, spec §6.1).
- `shouldThrowClassCastException` — relies on Java generics letting a `String` key
  past an `IntegerSerializer` to throw `ClassCastException` at serialize time;
  Python has no generics enforcement and the default `bytes` serializer accepts
  any bytes, so there is no faithful equivalent.

`KafkaProducerTest.java` (82 methods) → translated the broker-independent slice
(construction/config validation, close idempotence + negative-timeout,
`partitionsFor(null)`, `client_instance_id` negative-timeout message,
`ProducerRecord` null-topic); **skipped** the mock-dependent majority (MockClient
/ mocked ProducerMetadata / Sender / KafkaProducerTestContext / mockStatic /
metrics internals / JMX / thread-interrupt). Full per-method triage is in the P4
task's KafkaProducerTest analysis; the transaction happy-path and
send/metadata/interceptor tests all require a broker mock the Python surface
cannot reach.

## Test counts

- `test/unit/test_producer_family.py`: 71 (51 MockProducerTest + ~9
  KafkaProducer construction/config/close + ProducerRecord null-topic + rule-11
  surface + 4 async).
- Rust FFI: `test_record_metadata_copy_full`, `test_close_timeout_async_succeeds`,
  `test_close_timeout_async_default_sentinel` (all pass).
- Full `pytest test/unit`: 770 passed / 2 skipped baseline + the 71 new = 772
  collected, all pass.
- `mypy --strict confluent_kafka`: clean (57 source files).
- `cargo xtask format-check` + `cargo xtask lint`: clean.

## Coverage boundary (noted for the Critic)

The real `KafkaProducer` send path (batching engine + `RecordMetadata_copy_full`
+ dispatcher completion) needs a broker; it is not exercised by a Python unit
test (the mock is pure-Python and does not touch the FFI). The FFI functions are
covered by the Rust unit tests directly. End-to-end broker coverage arrives when
the gRPC multilanguage harness moves to the new package (P7).

## Clarifications logged (C22-C25)

- **C22** — MockProducer is a pure-Python synchronous translation of
  `MockProducer.java`, not FFI-backed (with the reverted-FFI consequence).
- **C23** — mock `error_next(*, error)` accepts any `BaseException` and delivers
  the exact instance (Java identity preserved); the exception setters likewise
  widen the spec's `KafkaError | None` to `BaseException | None` (Java's fields
  are `RuntimeException`; the canonical test injects `IllegalArgumentException`).
- **C24** — legacy `bindings/python/producer.py` kept unmodified; dependency list
  (admin/consumer/grpc + tests) recorded for P5/P7. Extends C5.
- **C25** — real-client telemetry/metric-subscription raise (core gap); the mock
  implements the full Java surface.

## Legacy module

`bindings/python/producer.py` is kept unmodified (still imported by `admin.py`,
`consumer.py`, `grpc_server*.py`, `grpc_translate.py`, and several tests — C24).
The legacy `test/unit/test_producer.py` (111 cases) stays alongside the new
`test_producer_family.py`. Retire both when P5 (consumer) and P7 (admin/gRPC)
move into the package.

## Concurrency / worktree

Per the Manager's mid-phase instruction, all P4 work moved to an isolated git
worktree `../ecfk-p4` (branch `p4-producer-wip`) so the shared pre-commit hook
(`make verify-sandbox`, whole-tree format/lint/build) does not fail other actors'
commits on the main tree. The main tree was restored of my Rust/C files and my
new Python files removed; `cargo xtask format-check` there is clean. Two worktree
setup notes: (1) the `kafka/` and `bindings/c/tests/unity` submodules were not
initialised in the fresh worktree, so they are symlinked to the main tree's
checkout (read-only Java reference / Unity headers) — these show as type-change
(`T`) in the worktree and are NOT committed (path-limited commits). (2) The shared
root `venv/` editable install is repointed by `pip install -e .` from the worktree
— acceptable per the Manager's note.

## Commits

- `21b7a328` — P4: producer family (branch `p4-producer-wip`).

`COMMENTS.67.md` created empty; empty at start and end.
