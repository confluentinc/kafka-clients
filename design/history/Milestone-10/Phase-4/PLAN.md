# Phase 4: Python binding — CPython extension glue + wrapper + unit tests

*(Proposed merge: original milestone phases 4 (extension) + 5 (wrapper) + the
unit-test part of 6, into one cohesive "Python binding" phase. The Docker/gRPC
multilanguage harness becomes the final phase (5). Rationale: the extension, the
Pythonic wrapper, and its pytest tests are co-developed and can't be meaningfully
verified apart — the same read/write-seam logic that justified the C-FFI 2-phase
merge you approved.)*

## Goal

A working, **mock-backed, tested** Python share-consumer API:
1. Extend `bindings/python/_confluentkafka.c` with the `ShareConsumer_*` surface.
2. Add `bindings/python/share_consumer.py` — the Pythonic wrapper
   (`KafkaShareConsumer` + `MockShareConsumer`).
3. Add `bindings/python/test/unit/test_share_consumer.py` (pytest, mock-backed).

No broker needed — drives `MockShareConsumer` end to end (subscribe → poll →
acknowledge → commit → close), exactly as the sibling `test_consumer.py` does.

## Branch

`milestone9-share-consumer-python`. Verify `git branch --show-current` before each
commit (shared-workdir agents have moved the branch before).

## Sources / template

- **This branch (local pattern):** `bindings/python/_confluentkafka.c` (producer
  extension — module init, `PyMethodDef`, `Py_BEGIN_ALLOW_THREADS`, the
  `KafkaError_*` accessors + `py_KafkaError_*` — the shared error machinery is
  ALREADY here), `producer.py` (`KafkaError` Python exception; wrapper style),
  `setup.py`/`pyproject.toml`/`Makefile`.
- **Sibling template (studied, staged in scratchpad — must be PORTED to this branch,
  since this branch's `_confluentkafka.c`/py-modules are producer-only):**
  `ref_py__confluentkafka.c` (the consumer extension machinery), `ref_py_consumer.py`
  (the `_run_sync`/`_run_async` orchestration + `(submit, resolve, free)` spec
  pattern + value types), `ref_py_test_consumer.py` (test shape).
- **The FFI surface is already generated** in `target/include/confluent_kafka.h`
  (Phases 1–3): every `ShareConsumer_*`/`ShareCommitResult_t`/
  `ShareAcknowledgeOffsets_t`/`TopicIdPartition_t`/`AcknowledgeType_t` symbol exists.

## Design decisions (baked in — override in review)

- **Sync-first API.** Ship a blocking `KafkaShareConsumer` + `MockShareConsumer`
  (mirrors Java's blocking `KafkaShareConsumer` and confluent-kafka-python). An
  `async` variant (`AsyncKafkaShareConsumer` via the reusable `_run_async`) is an
  easy additive follow-on — say if you want both now (the sibling consumer ships
  both).
- **Extension exposes only the `*_async` FFI variants; the wait lives in Python**
  (reuse `_run_sync`: `threading.Event` + 100 ms slices for `KeyboardInterrupt` +
  `wakeup()` on interrupt). This is the sibling's proven design; the sync C FFI
  variants (Phase 2/3) stay used by the C bindings, not Python.
- **Reuse, don't reinvent:** port the consumer extension's trampolines
  (`consumer_op_trampoline`, `consumer_poll_trampoline`, `fire_handle_cb`),
  `_BorrowedBytes`/`borrowed_memoryview`, the `ConsumerRecord(s)` PyTypes,
  `wrap_records`, `topics_to_array`, and `KafkaError._from_c` — the share poll
  returns the **same** `kafka_consumer_ConsumerRecords_t*`, so records are
  zero-copy memoryviews with the batch-keepalive chain, unchanged. Only add the
  `delivery_count` getter.

## The share-specific delta (the real work)

1. **Config + lifecycle** (`METH_VARARGS`, handle-as-int): `ShareConsumerProperties_*`,
   `KafkaShareConsumer_new`, `MockShareConsumer_new`, `ShareConsumer_destroy`
   (`Py_BEGIN_ALLOW_THREADS`), `ShareConsumer_wakeup`.
2. **subscribe / unsubscribe / subscription / poll / close** — 1:1 with the consumer
   (`consumer_op_trampoline` + `consumer_poll_trampoline`, reused as-is; the share
   callback typedefs are byte-identical).
3. **Acknowledge (sync-local, returns error-int → raise `KafkaError`):** `AcknowledgeType`
   (module int constants ACCEPT=1/RELEASE=2/REJECT=3/RENEW=4), `acknowledge`,
   `acknowledge_with_type`, `acknowledge_by_offset` — map onto the existing
   sync-local-op pattern (like `py_Consumer_seek`). Record-pointer variants borrow
   the live poll batch — the existing batch-keepalive already guarantees validity.
4. **Commit (async):** a **new** `share_commit_trampoline` for
   `ShareConsumer_commit_callback_t(ShareCommitResult_t*, KafkaError*, void*)`
   (like `fire_handle_cb` but two owned handles); a **new drain** for
   `ShareCommitResult_t` → `dict[TopicIdPartition → KafkaError|None]`; wire
   `commit_sync_async`, `commit_sync_timeout_async`, `commit_async` (fire-and-forget
   error-int), `commit_async_async`.
5. **`TopicIdPartition` marshaling (new):** `topic:str`, `topic_id:bytes` (16 raw
   UUID bytes — share-specific), `partition:int`. A small Python value type +
   accessors over the borrowed `TopicIdPartition_t`.
6. **Registered ack-commit callback (the ONE novel lifecycle):**
   `set_acknowledgement_commit_callback(consumer, cb_or_None, user_data)` with
   `AcknowledgementCommitCallback_t(const ShareAcknowledgeOffsets_t*, const KafkaError*, void*)`.
   **Persistent** — fires on every ack-commit completion — so store the PyObject in
   `user_data`, `Py_INCREF` on set and `Py_DECREF` only on clear (NULL clears);
   do **NOT** `Py_DECREF` per call (unlike every one-shot trampoline). Its own
   trampoline marshals `ShareAcknowledgeOffsets_t` (**owned-by-callback → must
   `destroy`**) into `dict[TopicIdPartition → set[int]]` + `KafkaError|None`, then
   calls the stored Python callable under `PyGILState_Ensure`.
7. **`acquisition_lock_timeout_ms`** (sync getter → `int|None`).
8. **Mock drivers:** `MockShareConsumer_add_record` (⚠ arg order: key/value pairs
   **before** offset — differs from the regular `MockConsumer_add_record`),
   `MockShareConsumer_set_client_instance_id`.

## Wrapper (`share_consumer.py`)

Port `_ConsumerBase`/`_run_sync` + the `(submit, resolve, free)` spec pattern from
`ref_py_consumer.py`. `KafkaShareConsumer` (blocking) + `MockShareConsumer` (adds
`add_record`, `set_client_instance_id`). `poll(timeout)` → the `ConsumerRecords`
iterable (zero-copy). `acknowledge(record[, type])` / `acknowledge(topic, partition,
offset, type)`. `commit_sync[_timeout]()` → `dict`, `commit_async()`.
`set_acknowledgement_commit_callback(cb)`. `close()` reaps the handle in `finally`.
Import the shared `KafkaError` from `producer` (as the sibling does). Register the
share modules in `pyproject.toml` `py-modules`.

## Implementation steps (each an independent, green commit)

- **4a — extension glue.** Port the reusable consumer machinery + add the
  `ShareConsumer_*` methods, the two new drains (`ShareCommitResult_t`,
  `ShareAcknowledgeOffsets_t` + `TopicIdPartition`), the commit trampoline, and the
  persistent ack-callback, into `_confluentkafka.c`; register in the `PyMethodDef`
  table + `PyInit`. **Build:** `make build` (pip install -e .). **Commit.**
- **4b — wrapper.** `share_consumer.py` (`KafkaShareConsumer` + `MockShareConsumer`),
  reusing `_run_sync` + the spec pattern; `pyproject.toml` py-modules. **Commit.**
- **4c — unit tests.** `test/unit/test_share_consumer.py` (pytest, mock-backed):
  subscribe→add_record→poll→read (incl. `delivery_count` + zero-copy memoryview
  lifetime)→acknowledge→commit_sync (assert the `dict`)→registered ack callback
  fires→close. **Commit.**

## Tests / DoD

- `make build` clean (extension compiles + links `libconfluent_kafka` against the
  generated header; requires `cargo build --features ffi --release` first).
- `make test` green — `pytest test/unit` incl. `test_share_consumer.py`; assert
  error message content and the ack-commit callback delivery (mock-backed).
- The Rust/C layers stay green (this phase is Python-only; no `src/`/`cbindgen`
  change) — spot-check `cargo build --features ffi` + the C smoke test unaffected.
- Zero-copy memoryview lifetime test (mirror the sibling's
  `test_memoryview_zero_copy_lifetime`).

## Risks / watch-items

- **Persistent ack-callback refcount** (the one novel lifecycle): INCREF once on
  set, DECREF once on clear/`destroy`-of-consumer — a per-call DECREF (the one-shot
  pattern) would use-after-free the Python callable. Guard set/replace/clear.
- **`ShareAcknowledgeOffsets_t` ownership:** owned-by-callback → the trampoline must
  `destroy` it after marshaling (leak otherwise).
- **`MockShareConsumer_add_record` arg order** (key/value before offset) — easy to
  transpose vs the regular consumer.
- **Signal/cancel:** reuse `_run_sync` exactly so `KeyboardInterrupt` → `wakeup()` →
  drain works (don't hand-roll the wait).
- **Broker-less scope:** the mock never fires the ack-commit callback end-to-end
  (same limitation as the C-FFI mock) — the callback bridge is unit-tested by
  invoking the trampoline directly / via a mock that does fire, or documented as
  covered at the FFI + M9 layers. Decide during 4c.
- **Build ordering:** `make build` needs `libconfluent_kafka` (release) + the header
  present first.

## Not in this phase (→ Phase 5)

The Docker/gRPC multilanguage harness (`grpc_server*.py`, `grpc_translate.py`,
`Dockerfile.grpc*`) and any live-broker integration. `client_instance_id` stays
omitted (KIP-714).
