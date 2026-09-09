---
name: phaseP4-python-producer
description: P4 Python producer family binding — FFI gaps, error-injection inverse, CGM handle, zero-copy send reconciliation
metadata:
  type: project
---

# P4 — Python producer family (`confluent_kafka/producer/`)

Branch `dev_python-interface-implementation`. Actor 67. Builds the new-package producer
family on the Rust core C FFI, replacing legacy `bindings/python/producer.py` (kept while
`admin.py`/`grpc_server*.py`/`consumer.py` still import it — verify with grep).

## Key architecture facts (verified 2026-09-09)

- **C ext `_confluentkafka.c` has its OWN hand-rolled C batching engine** (`BatchNode`,
  `record_batches_mutex`, background poll thread calling `kafka_producer_Producer_send_batch`),
  NOT the `src/ffi/common.rs` dispatcher. `Producer_send(producer, native_ProducerRecord, cb)`.
  Reuse it as-is; do not rewrite.
- **Zero-copy reconciliation (C10):** P2 `ProducerRecord[K,V]` is pure-Python (user objects).
  At `send()`: serialize key/value → `bytes`, build native `_confluentkafka.ProducerRecord(
  topic, value_bytes, key=..., partition=-1, timestamp=-1)` (its `tp_init` INCREFs the bytes
  and holds pointers = zero-copy), pass to `Producer_send`. Serialization eager on caller thread.
- **Completion callback** `cb(result:int, error:int)` = raw `kafka_producer_RecordMetadata_t*` /
  `kafka_common_Error_t*` as ints. Convert via `_send._completion_to_python`: metadata via new
  native `RecordMetadata_copy_full` (added — returns topic/partition/offset/timestamp/ser_key_size/
  ser_value_size, destroys handle); error via `from_ffi_error(handle)` (typed hierarchy, consumes).

## FFI additions needed (each: Rust + C ext native + C test + Python test)

1. `kafka_producer_RecordMetadata_copy_full` — 6-field extract (adds serialized_key_size,
   serialized_value_size, has_offset/has_timestamp via -1 sentinel) + destroy. Core RecordMetadata
   already has serialized_key_size()/serialized_value_size()/has_offset()/has_timestamp().
2. **error injection inverse** — `error_next(*, error: KafkaError)` needs class-enum→Error.
   THE TRAP: existing `error_next(error_code: i16)` uses `Errors::for_code` (WIRE codes);
   Python `_ffi_id`/`to_ffi_id` and `kafka_common_Error_code` use `kafka_common_ErrorCode_t`
   (CLASS enum). They DIVERGE for 4 inheritance classes (BufferExhausted -28, Authentication -7,
   Authorization -9, SslAuthentication -16 → for_code collapses all to UnknownServerError).
   FIX: add core `error_from_ffi_code(code, msg)` = inverse of `error_code_of` (src/ffi/common.rs),
   + FFI `kafka_common_Error_new_from_ffi_code` OR a new mock-error-next taking the class-enum.
   Then Python passes `to_ffi_id(error)` + `str(error)`.
3. Full MockProducer surface — FFI only has complete_next/error_next/history_count/clear/
   set_commit_transaction_error/sent_offsets/committed_offset. MISSING (core HAS all, see
   src/producer/mock_producer.rs): flushed, closed, history (records not count), transaction_*(4),
   commit_count, uncommitted_records, uncommitted_offsets, consumer_group_offsets_history,
   fence_producer, all set_*_error setters (init/begin/send_offsets/commit/abort/send/flush/
   partitions_for/close), set_mock_metrics. Core LACKS: inject_timeout, disable_telemetry,
   set_client_instance_id, added_metrics, register/unregister_metric → raise mapped error + log gap.
4. `client_instance_id`, `register/unregister_metric_for_subscription` — NOT in core Producer
   trait → Python raises mapped Java error (rule 10) + clarification. BUT negative-timeout on
   client_instance_id validated Python-side FIRST (IllegalArgumentError "The timeout cannot be
   negative.") so KafkaProducerTest #58 translatable.
5. `send_offsets_to_transaction` FFI needs native `kafka_consumer_ConsumerGroupMetadata_t*`.
   P2 CGM is pure-Python. `kafka_consumer_ConsumerGroupMetadata_new(gid, gen, mid, gii)` exists →
   add native that builds temp handle from the 4 CGM fields, calls FFI, destroys.
6. close-timeout: core has `close_timeout(Duration)`, FFI `Producer_close`/`_close_async` use
   non-timeout `close()`. Add timed form (`kafka_producer_Producer_close_timeout` +/async).

## Java test analysis (from sub-agents)

- **MockProducerTest**: 55 @Test, 0 parameterized, 3 ctor overloads, helper buildMockProducer +
  isError. SKIP #2 testPartitioner (needs Cluster/PartitionInfo/RoundRobinPartitioner) and #50
  shouldThrowClassCastException (Java generics+IntegerSerializer). #55 testMetadataOnException:
  errorNext delivers callback with non-null RecordMetadata all fields -1 AND the error.
  error_next identity (#3,#55): FFI reconstructs by code+msg → assert type+message, not identity.
- **KafkaProducerTest**: ~17 translatable (config validation #1-4,#19-21,#75; close #32,#33;
  ProducerRecord null topic #70; adapt #10,#38,#58,#63). REST SKIP (MockClient/mocks/metadata/
  KafkaProducerTestContext). Config-validation tests depend on core validating config at ctor —
  verify per-case; adapt/skip where core diverges. #58 exact msg "The timeout cannot be negative.".

## Test skips to document (reasons)
- MockProducerTest #2 (partitioner+Cluster out of scope), #50 (Java generics/ClassCast).
- KafkaProducerTest: all MockClient/mock-dependent (list per method).

## RESOLVED architecture (final, differs from initial plan above)

- **MockProducer is PURE-PYTHON, not FFI-backed.** The Rust core is async
  (batching send task + dispatcher-thread completions) and CANNOT reproduce
  Java's MockProducer synchronous completion (md.isDone() right after send,
  completeNext right after send) or its original-record history (core stores
  serialized Vec<u8>). Java's MockProducer IS a self-contained synchronous
  in-memory mock, so the faithful translation (DoD #2) is a direct pure-Python
  translation of MockProducer.java (mock_producer.py: _MockCore mirrors every
  field/method). It touches NEITHER the FFI NOR the core mock. All the mock-surface
  FFI I drafted (error_from_ffi_code inverse, MockProducer_* scalars/setters/
  error_next_with_code/check_send) was REVERTED — no Python caller → DoD violation.
  So src/ffi/common.rs and src/producer/mock_producer.rs ended UNCHANGED.
- **Kept FFI (real KafkaProducer only):** RecordMetadata_copy_full,
  Producer_close_timeout_async, Producer_send_offsets_to_transaction_fields_async.
  Each has a Rust unit test + a C test in test_mock_producer.c.
- **mypy --strict is enforced** (bindings/python/Makefile typecheck). _confluentkafka
  has no stub → per-import `# type: ignore[import-not-found]` (Actor 64's pattern;
  do NOT add a [tool.mypy] override to pyproject — it makes their existing ignore
  unused and breaks their file). Serializer[K] defaults trip variance → type ctor
  params Serializer[Any]. resolve_serde returns Callable[...,object] → cast to
  Serializer[Any]. AsyncMockProducer.send wraps the concurrent Future via
  asyncio.wrap_future (base declares asyncio.Future).
- **error_next / set_*_exception typed BaseException (not spec's KafkaError)** —
  Java's errorNext takes RuntimeException; MockProducerTest injects
  IllegalArgumentException (a JDK analog). C23.

## Worktree + pre-commit-hook contention (Manager process change)

- P4 moved to worktree ../ecfk-p4 (branch p4-producer-wip). Worktree has SEPARATE
  index (.git/worktrees/ecfk-p4/index) — main-tree commits don't lock it.
- kafka/ and bindings/c/tests/unity submodules NOT init'd in a fresh worktree →
  SYMLINK them to the main tree (`ln -s /home/prathi/work/example-confluent-kafka-rust/kafka kafka`).
  They show as type-change (T); never commit them (path-limited commits).
- The pre-commit hook (make verify-sandbox: release build + all-features release +
  C build + docker integration) is ~15+ min and HEAVY. Under many concurrent actors
  (load 36+, 7 hooks) it gets SIGTERM (signal 15, OOM/reaper). Retry commit in a
  detached loop that waits for a low-contention window (≤1 verify-sandbox, load<20)
  before each attempt.
