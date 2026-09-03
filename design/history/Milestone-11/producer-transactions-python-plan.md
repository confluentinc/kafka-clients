# Producer transactions — Python bindings plan (Milestone 11)

**Status:** planned, not started. Sibling to `producer-transactions-ffi-plan.md` (the
C-FFI layer, already landed in `203c2b9b`). This document is the contract the
Actor implements and the Critic reviews.

**Branch:** `milestone11-producer-transactions-python` (based on the CFFI HEAD
`68da7620`).

**Assigned agents:** Actor **54** / Critic **54**.

---

## 1. Goal

Expose the producer transaction API in the Python binding, faithful to the Java
`KafkaProducer` transaction contract and to the already-landed Rust FFI. Deliver
**all five** control ops —

```
init_transactions   begin_transaction   commit_transaction
abort_transaction   send_offsets_to_transaction
```

— on **both** the synchronous `Producer` family (`Producer`, `KafkaProducer`,
`MockProducer`) and the `AsyncProducer` family (`AsyncProducer`,
`AsyncKafkaProducer`, `AsyncMockProducer`), plus the `txn_requires_abort` error
accessor on Python's `KafkaError`, unit tests, and the Python gRPC-harness
handlers so the already-registered `multilanguage_test!(python)` scenarios pass.

This discharges the remaining part of PLAN.md **§9.6** ("C FFI / Python / gRPC
multilanguage harness for transactions") and the `producer.py`/gRPC-server steps
of the §7.2 ordering (*C FFI → `producer.py` → gRPC servers → multilanguage
scenarios*).

---

## 2. Sources & template

- **Java contract:** `kafka/clients/.../producer/KafkaProducer.java` transaction
  methods (`initTransactions`, `beginTransaction`, `commitTransaction`,
  `abortTransaction`, `sendOffsetsToTransaction`). None take a timeout.
- **Rust FFI (the ABI to wrap):** `src/ffi/producer.rs`, `src/ffi/common.rs`
  (see §4 for exact symbols). Regenerated C header at
  `target/include/confluent_kafka.h` (build artifact; `cargo build --features
  ffi` regenerates it — it was stale before this phase and has been refreshed).
- **Structural template:** `design/history/Milestone-10/Phase-4/PLAN.md` (on
  branch `milestone9-share-consumer-python`) — the share-consumer Python binding,
  delivered as one merged phase in independently-green commits
  (extension glue → wrapper → unit tests), gRPC harness split out. Read via
  `git show milestone9-share-consumer-python:design/history/Milestone-10/Phase-4/PLAN.md`.
- **In-tree code templates** (reuse, do not reinvent):
  - direct-error-return C wrapper: `py_Producer_flush` (`_confluentkafka.c:930`).
  - async trampoline: `producer_op_trampoline` (`_confluentkafka.c:770`) + `py_Producer_flush_async` (`:965`).
  - offsets list marshaling: `py_Consumer_commit_sync_offsets_async` (`_confluentkafka.c:1582-1615`, format `"siL|iO"`).
  - group-metadata: `py_Consumer_group_metadata` (`_confluentkafka.c:1808`), `consumer.py:270`.
  - sync wait helper: `_ProducerBase._run_sync` / `_resolve_void` (`producer.py:272`, `:145`).
  - unit-test shape: `test_flush` and the mock error path in `test/unit/test_producer.py`.

---

## 3. State entering this phase

| Layer | State |
|---|---|
| Rust FFI: 5 txn ops + `txn_requires_abort` + mock hooks | done (`203c2b9b`) |
| gRPC harness: proto RPCs, Rust client, C++ server, registered `multilanguage_test!(python)` | done (`68da7620`); Python scenarios **registered but failing** — no Python handler |
| C header with txn prototypes | regenerated this phase |
| `_confluentkafka.c`, `producer.py`, `consumer.py` (handle), `grpc_server*.py` | **nothing** — all txn work is this phase |

---

## 4. Rust FFI symbols to wrap (exact)

All in `src/ffi/producer.rs`; each returns `*mut kafka_common_KafkaError_t`
(**null = success**; non-null is an owned error the caller frees with
`kafka_common_KafkaError_destroy`).

| FFI symbol | Blocking? | Notes |
|---|---|---|
| `kafka_producer_Producer_init_transactions(producer)` (`:2824`) | yes (`block_on`) | |
| `kafka_producer_Producer_begin_transaction(producer)` (`:2858`) | **no** — pure state transition | |
| `kafka_producer_Producer_commit_transaction(producer)` (`:3043`) | yes | |
| `kafka_producer_Producer_abort_transaction(producer)` (`:3088`) | yes | |
| `kafka_producer_Producer_send_offsets_to_transaction(producer, topics, partitions, offsets, leader_epochs, metadata, count, group_metadata)` (`:2933`) | yes | parallel arrays + a **borrowed** `ConsumerGroupMetadata*`; **panics if `count < 0`**; rejects null `group_metadata` with `IllegalArgument` |
| `kafka_common_KafkaError_txn_requires_abort(error) -> bool` (`common.rs:204`) | — | error accessor |

Mock test hooks (for unit tests): `kafka_producer_MockProducer_set_commit_transaction_error`
(`:3255`), `MockProducer_sent_offsets` (`:3300`), `MockProducer_committed_offset` (`:3348`).

All five control ops share a lock-free mutual-exclusion flag `txn_control_busy`
(atomic CAS): an overlapping call returns a **`ConcurrentModification`** error,
which is a caller-sequencing bug — **not** a reason to abort or retry, and the
transaction is untouched.

---

## 5. Design decisions (baked in — Critic may override with recorded rationale)

1. **No `timeout=` parameter.** Faithful to Java and to the FFI (neither takes
   one; blocking is bounded by `max.block.ms` / `transaction.timeout.ms`).
   confluent-kafka-python's timeouts are a librdkafka artifact we do not inherit.

2. **Direct-error-return bridging.** The FFI ops return the error pointer
   directly, so the C wrapper is *simpler* than `py_Producer_flush`'s out-param
   form: call the FFI fn, return the error handle as a Python int (0 on null),
   and let Python raise `KafkaError._from_c(err)`. Do **not** invent an
   async/callback form for these — they are inherently blocking control calls.

3. **Release the GIL around every blocking FFI txn call.** Wrap the `block_on`
   ops (`init`, `commit`, `abort`, `send_offsets`) in
   `Py_BEGIN_ALLOW_THREADS` / `Py_END_ALLOW_THREADS` in the C wrapper, so a
   blocking transaction call does not freeze the interpreter — this is what makes
   the `AsyncProducer` executor variant (§6) and any multi-threaded sync use
   correct. `begin_transaction` is non-blocking and needs no GIL release.

4. **Sync family = direct calls; Async family = `run_in_executor`.** The sync
   `Producer` methods call the C wrapper directly and raise on a non-null error
   (mirror `_resolve_void`). The `AsyncProducer` methods `await
   loop.run_in_executor(None, <blocking C call>)` for `init`/`commit`/`abort`/
   `send_offsets`; `begin_transaction` may be called directly (non-blocking).
   The `txn_control_busy` flag makes sequential awaits safe.

5. **§13 verbatim in docstrings.** Every txn method's docstring (sync and async)
   must state plainly that **async / outbox `send()` inside a transaction is
   unsupported and undefined** — a record queued between `begin_transaction` and
   `commit`/`abort` may be published despite an abort or lost/rejected despite a
   commit — and steer callers to the **synchronous** `send()`. Do **not** soften
   to "discouraged". Do **not** add a runtime guard rejecting async send in a txn
   (the decision is document-not-enforce). Every transactional test/example uses
   synchronous `send()` between `begin` and `commit`/`abort`.

6. **`ConcurrentModification` is not abort-worthy.** Docstrings and error handling
   must not tell users to abort or retry differently on it.

7. **Flat-error caveat, do not "fix".** A `FATAL_ERROR` whose last error is a
   string-payload variant (`IllegalState`, `Timeout`, `ConcurrentModification`,
   `TransactionAborted`) reports `is_fatal()==false` / `txn_requires_abort()==false`
   — a known flat-error limitation documented in `cbceb9fc`. The binding surfaces
   whatever the FFI reports; it must not paper over this.

---

## 6. The delta — the real work, and its one sharp edge

### 6.1 `send_offsets_to_transaction` needs a live group-metadata handle — the consumer binding must retain it

**Problem.** `kafka_producer_Producer_send_offsets_to_transaction` requires a live
`kafka_consumer_ConsumerGroupMetadata_t*`. But today the Python consumer
**throws that handle away**: `py_Consumer_group_metadata`
(`_confluentkafka.c:1808-1820`) reads the four fields and calls
`kafka_consumer_ConsumerGroupMetadata_destroy(m)` before returning a tuple;
`consumer.py:270-274` wraps that tuple in the pure-Python `ConsumerGroupMetadata`
dataclass (`consumer.py:99`). There is **no FFI constructor** to rebuild a handle
from the four fields (the header exposes only accessors + `destroy`), so
reconstruction is impossible without a Rust change.

**Chosen approach — mirror the existing "C-handle-carrying value object"
precedent, `ProducerRecord` / `ConsumerRecords` (Python/C-extension only, no
`src/` change).** Those are the codebase's answer to exactly this shape — a value
that carries a C handle, is passed back into a call, and must be freed later: a
**proper CPython extension type whose `tp_dealloc` frees the handle on GC**, NOT a
Python `__del__` (the one lifetime style this binding deliberately avoids —
`__del__` is less reliable than a C destructor: interpreter-shutdown ordering,
non-prompt collection, swallowed exceptions).

  - `_confluentkafka.c`: make `ConsumerGroupMetadata` a real extension type,
    modelled on `ProducerRecordType` (`:208`, a value object with field getters
    that also carries C state and is passed into a call) and
    `ConsumerRecordsType` (`:1307`, a raw-handle owner):
    `struct { PyObject_HEAD; kafka_consumer_ConsumerGroupMetadata_t* handle; }`,
    with getters for the four fields (like `ProducerRecord_getsetters` `:199`) and
    a `tp_dealloc` doing `if (handle) { kafka_consumer_ConsumerGroupMetadata_destroy(handle); handle = NULL; }`
    — the double-free guard from `ConsumerRecords_dealloc` (`:1147`). Change
    `py_Consumer_group_metadata` (`:1808`) to **stop** destroying the handle and
    instead construct and return one of these objects. `PyType_Ready` +
    `PyModule_AddObject` it in module init alongside the existing types.
  - `consumer.py`: `group_metadata()` (`:270`) returns that object directly, or a
    thin wrapper exposing the same `.group_id` / `.generation_id` / `.member_id` /
    `.group_instance_id` surface + `__repr__` so existing `test_consumer.py` stays
    green. **No Python `__del__`** — freeing lives in the type's `tp_dealloc`.
  - `producer.py`: `send_offsets_to_transaction(offsets, group_metadata)` reads
    the handle off that object, marshals `offsets` (§6.2), calls the C wrapper.

  *Lighter fallback (only if the extension type proves disproportionate):* keep
  the pure-Python dataclass but hold the handle int and free it in `__del__` with
  an "already freed" sentinel. Record the deviation and reason — it departs from
  the house pattern, so the Critic must scrutinise the `__del__` semantics
  (shutdown ordering, non-prompt collection) much harder.

**Lifecycle contract the Critic must verify:** the handle is owned by exactly one
Python `ConsumerGroupMetadata`; freed exactly once, in `tp_dealloc` on GC
(`__del__` only in the fallback); not freed
while a `send_offsets_to_transaction` call is in flight (it is — the object is a
live argument to a synchronous call, so it outlives the call); each
`group_metadata()` call returns a fresh owned handle (the FFI clones internally),
so two calls do not alias one handle. No use-after-free, no double-free.

**Rejected alternative:** add a Rust FFI `ConsumerGroupMetadata_new(group_id,
generation_id, member_id, group_instance_id)` and reconstruct in the producer.
Rejected because it is a `src/` + header change (breaks the "Python-only" nature
of the phase, needs Rust review) for no behavioural gain — Java's
`ConsumerGroupMetadata` carries exactly those four fields, so retaining the real
handle is strictly at least as faithful. If the Actor hits a blocker with the
retain approach, escalate to the Manager before switching — do not silently add
a Rust FFI function.

### 6.2 `send_offsets` offsets marshaling

Accept the same offsets shape the Python consumer's commit path already uses
(a list of `(topic, partition, offset[, leader_epoch, metadata])`; mirror
whatever `test_consumer.py` / the commit API expose so the two are consistent).
The C wrapper marshals it into the parallel `topics/partitions/offsets/
leader_epochs/metadata` arrays + `count`, following
`py_Consumer_commit_sync_offsets_async` (`:1582-1615`) as the template. Python
must never pass `count < 0` (the FFI panics); an empty offsets list is a
legitimate `count == 0`.

---

## 7. Implementation steps (each an independent green commit)

### 9a — C extension glue (`bindings/python/_confluentkafka.c`)

  - Add `py_Producer_init_transaction`, `py_Producer_begin_transaction`,
    `py_Producer_commit_transaction`, `py_Producer_abort_transaction` — each
    parses `"K"` (producer handle), calls the FFI op (blocking ones wrapped in
    `Py_BEGIN_ALLOW_THREADS`/`Py_END_ALLOW_THREADS` per §5.3), returns the error
    handle int (0 on success). Template: `py_Producer_flush` (`:930`).
  - Add `py_Producer_send_offsets_to_transaction` — parses the offsets list +
    group-metadata handle, marshals arrays (§6.2), GIL released around the
    blocking FFI call, returns the error int.
  - Add `py_KafkaError_txn_requires_abort` wrapping
    `kafka_common_KafkaError_txn_requires_abort`.
  - Add `py_ConsumerGroupMetadata_destroy`, and change `py_Consumer_group_metadata`
    to retain + return the handle (§6.1).
  - Register every new function in `ProducerNativeMethods[]` (`:2091`) /
    the consumer table (`:2135+`).
  - **Green:** `make devel-build-python` (or `make build-python`) compiles and
    links against the refreshed lib+header; `pytest bindings/python/test/unit -v`
    still green (no behaviour change yet).

### 9b — Python API (`producer.py`, `consumer.py`)

  - `producer.py`: add the five txn methods to the sync `Producer` (`:211`) and
    the async `AsyncProducer` (`:326`) per §5.4. Add a `txn_requires_abort`
    read-only `@property` to `KafkaError` (`:12`), fed by
    `_lib.KafkaError_txn_requires_abort` in `_from_c` (`:20`) — mirror the
    existing `is_fatal`/`is_retriable` caching. §13 docstrings on every method
    (§5.5).
  - `consumer.py`: retain the handle in `ConsumerGroupMetadata` + `__del__`
    (§6.1).
  - **Green:** import works; `pytest bindings/python/test/unit -v` green
    (existing tests unaffected).

### 9c — Unit tests (`bindings/python/test/unit/test_producer.py`)

  Mock-backed, no broker, mirroring `test_flush` and the existing mock error path;
  cover both sync and async producers:
  - happy path: `init → begin → send (synchronous) → commit`, assert no raise and
    (via `MockProducer_sent_offsets` / `committed_offset`) that `send_offsets`
    recorded offsets;
  - abort path;
  - failure path: seed `MockProducer_set_commit_transaction_error`, assert
    `pytest.raises(KafkaError)` and that `err.txn_requires_abort` /
    `err.is_fatal` reflect the seeded error;
  - `send_offsets_to_transaction` with a real retained `group_metadata` object
    from a `MockConsumer` (or the mock hook), asserting the handle lifecycle
    does not leak/double-free (a test that creates and drops many
    `group_metadata()` objects).
  - **Green:** `pytest bindings/python/test/unit -v`.

  Translate any Java `TransactionManager`/producer transaction unit assertions
  that map onto the mock surface; where a Java test needs a real broker it is
  covered by 9d instead — note it, do not silently skip (DoD #3).

### 9d — Python gRPC handlers (`grpc_server.py`, `grpc_server_async.py`)

  - Add `InitTransactions` / `BeginTransaction` / `CommitTransaction` /
    `AbortTransaction` handlers to the `ProducerService` servicer in **both**
    servers, mirroring the existing `Flush` handler (`grpc_server.py:148-159`;
    async `grpc_server_async.py:144`): take the producer by `producer_id`, call
    the `producer.py` method, translate a raised `KafkaError` via
    `_kafka_error_to_proto` (`grpc_translate.py:80`) into `StatusResponse`.
    (`send_offsets` is **not** in the proto — do not add a handler for it here.)
  - Regenerate `producer_service_pb2` (the `.proto` already carries the four RPCs
    from `68da7620`; the Python Dockerfile regenerates stubs).
  - **Green:** the registered `multilanguage_test!(python)` transaction scenarios
    pass. This needs the **Docker multilanguage harness** — see §9 risk.

---

## 8. Definition of Done / green gate

Python-only change (no `src/` edit → Rust and clippy untouched; matches M10's
"Rust/C layers stay green"). Apply the full DoD in `definition-of-done.md`, with:

  - **Primary tight loop (per commit):** `make devel-build-python` +
    `python -m pytest bindings/python/test/unit -v`.
  - **DoD #3:** translate the Java transaction tests that map onto the mock
    surface; assert error-message content, not just that an error was raised.
  - **DoD #10 (hot-path allocation): N/A** — transaction control calls are
    per-transaction, not per-record. State this in the self-review.
  - **DoD #11 (consumer trait surface): N/A** to Python, but keep the async
    methods genuinely async (executor-backed), not a `block_on` façade.
  - **`make verify`** (full: build + format-check + lint + C/Python/gRPC tests)
    is the final gate, but its Docker multilanguage harness is heavy — run it
    deliberately, not in a hot loop (§9).

---

## 9. Risks & watch-items

  - **Docker harness leak (OPEN-BUGS §9.17).** The multilanguage Docker brokers
    have leaked and wedged this machine. 9d's validation needs the harness — run
    it once, deliberately, with the documented cleanup; do not loop on it. Unit
    tests (mock-backed, 9a-9c) are the primary green signal.
  - **Group-metadata handle lifecycle (§6.1).** The one genuinely new-thinking
    part. Critic: verify single-owner, freed-exactly-once, no use-after-free.
  - **GIL release (§5.3).** A blocking txn call that keeps the GIL will freeze
    the interpreter and break the async executor variant. Critic: verify
    `Py_BEGIN_ALLOW_THREADS` wraps every `block_on` op.
  - **Async server target.** Confirm which Python server the
    `multilanguage_test!(python)` txn scenarios launch (sync vs async). If sync
    only, the async handlers are still delivered for API completeness but are not
    on the harness path.
  - **`begin_transaction` is synchronous** even in the FFI — do not force it onto
    a callback/await path that implies network I/O.

---

## 10. Not in this phase (→ next / other tracks)

  - Write-path / generator open bugs: `buffer.memory` (OPEN-BUGS #3),
    `Enable2Pc`/§9.1 (#4). These touch `src/` and belong on their own review.
  - Any `.proto` extension for `send_offsets_to_transaction` over gRPC (it needs
    `ConsumerGroupMetadata` on the wire — proto comments `:51-52`, `:146-148`).
    `send_offsets` ships in the Python API + unit tests only this phase.
  - Adding a Rust FFI `ConsumerGroupMetadata` constructor (§6.1 rejected
    alternative).
