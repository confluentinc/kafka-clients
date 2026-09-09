# P6 Actor 69 — tests, typing gate, legacy retirement

Branch `dev_python-interface-implementation` (from tip `4d97e9d0`). Five commits,
all with the pre-commit hook (format-check + lint + Python typecheck) passing.
Final `make test-python`: mypy --strict green (73 files), unit 676 passed /
1 skipped, integration 232 passed / 0 failed (sync + async gRPC backends).

## Commits

| Hash | What |
|---|---|
| `467578f1` | Retire legacy modules; port gRPC servers + admin to the package |
| `e5bdb235` | Migrate legacy tests to the new API; delete the legacy test files |
| `da5b60d2` | Extend the typing gate; add `bindings/python/README.md` |
| `fd7685a5` | gRPC image Dockerfiles copy the package (Linux) |
| `9f01760f` | macOS gRPC Dockerfiles; log clarifications C40-C43 |

## 1. Legacy modules retired

Deleted `bindings/python/{producer,consumer,_error_code}.py`.

Former importers and how they were repointed (`grep` per the task):
- `admin.py` (PAUSED, frozen public surface) imported `Node` / `OffsetAndMetadata`
  from `consumer` and `KafkaError` from `producer`. Those three admin-legacy types
  (flat `KafkaError` with `code()`/`is_retriable()`; positional `Node` /
  `OffsetAndMetadata`) moved to a new **private** `confluent_kafka/_legacy_compat.py`
  shim; `admin.py` imports the shim. Admin's public surface is unchanged.
- `grpc_translate.py` / `grpc_server.py` / `grpc_server_async.py`: ported to the
  new package (see §2).
- `test_admin.py`: imports the compat `KafkaError`.
- `test_errors.py`: two tests cross-checked `_ffi_id` against `_error_code.py`;
  re-expressed to parse the surviving Rust mirror `tests/common/error_code.rs`.
- `test/performance/*`: **not** ported — blocked by a package-name collision, an
  owner item (C43); not in `make verify`.

`pyproject.toml` `py-modules` is now `["admin", "grpc_server", "grpc_server_async",
"grpc_translate"]`. `xtask generate-error-codes` no longer emits `_error_code.py`
(the Rust mirror + the generated hierarchy stay); `check-generated` drops that leg.
`cargo test -p xtask` (38 passed) and `cargo xtask check-generated` pass. The flat
`KafkaError` is no longer importable from the top level.

## 2. gRPC integration servers ported

`grpc_server.py` / `grpc_server_async.py` / `grpc_translate.py` now speak the new
package: keyword-only calls (`send(record=…)`, `poll(timeout=…)`,
`commit()`/`commit_nowait(on_commit=…)`, `subscribe(topics=…)`,
`seek(partition=…, offset=|offset_and_metadata=)`), typed errors via `to_ffi_id`
and the `_ffi_id` constants (replacing `_error_code`), method value-type accessors
(`tp.topic()`, `r.key()`, `metric.metric_name()`), the `close` timeouts wired
through the new timed FFI forms (D7), and `OffsetsForTimes` handling `None` entries.
`metrics()` now returns `dict[MetricName, Metric]`; `_metric_to_proto` picks the
proto value oneof from the Python value type (metric assertions are structural, so
the lost Long/Int distinction is not observable — C42). The Node/PartitionInfo/OAM
proto helpers read through a `_member` shim, shared by the new (method-accessor)
producer/consumer path and the paused-admin (attribute-accessor) path. The async
server drops `await` from `begin_transaction` / `metrics` (plain `def` on the async
class now).

Dockerfiles (`Dockerfile.grpc{,.async}` and the two `.macos` variants) copy the
`confluent_kafka` package instead of the retired modules. `make build-grpc-images-python`
+ `make test-integration-python`: **232 passed, 0 failed**.

## 3. Legacy tests → new API

Deleted `test_producer.py` (111) / `test_consumer.py` (29) /
`test_consumer_callbacks.py` (39). Re-expressed every surviving-subject case:

- `test_producer_family.py` (+10): on_delivery cancelled-future / raising / None,
  `partitions_for`, close-with-send-in-flight, abortable-vs-non-abortable commit
  failure (typed `TransactionAbortableError` vs `TimeoutError`), async on-delivery
  loop-thread + close-in-flight.
- `test_consumer_family.py` (+2): wakeup-consumed-by-next-poll, seek-after-close.
- `test_consumer_callbacks_family.py` (new, 15): `commit_nowait`/`on_commit`
  payload (sync+async), raising-callback swallowed, must-be-callable, coroutine-on-
  sync reported (logged), non-str metadata reject (commit+commit_nowait), default
  metadata accepted, `on_partitions_lost` default → revoked, listener registration
  lifetime (retained across unsubscribe; released by replacing/listenerless
  subscribe or close).

**Dropped, with reason:**
- Every flat-error assertion (`err.code`/`is_retriable`/`is_fatal`/
  `txn_requires_abort`), `client_id()`, `enforce_rebalance()` — spec removed the
  subject.
- The public `ConsumerHandle`/`c.handle()` object-API tests — spec removed the
  public handle; the reentrancy *behavior* is covered by
  `test_consumer_family.py`'s reentrancy suite.
- The producer backpressure suite + the async-FFI-routing / GIL / handle-lifecycle
  regressions — they targeted the retired FFI-backed mock's `_c_producer` /
  `_lib.Producer_*_async` plumbing; the pure-Python `MockProducer` (C22/C38) has no
  such surface.
- The two `@skip`ped broker-only consumer cases (poll does not block on the mock)
  and the two seek FFI-routing/liveness plumbing tests — integration-covered.
- The listener-missing-a-method rejection — the new `ConsumerRebalanceListener` is
  a subclassable class with no-op defaults, not a Java abstract interface, so a
  partial subclass is legal.

## 4. Remaining Java test translations

Per the P2-P5 critic reports and COMMENTS.DONE files, the in-scope Java test
classes were already translated in earlier phases (MockProducerTest 51/55 + 2
folded + 2 Java-only skipped; MockConsumerTest 8/8; ConsumerRecordTest,
ConsumerRecordsTest, OffsetAndMetadataTest, ProducerRecordTest, RecordMetadataTest,
CloseOptionsTest, ConsumerGroupMetadataTest, SerializationTest, and the
broker-independent slices of KafkaProducerTest/KafkaConsumerTest). No Java test was
left entirely un-translated for the binding surface.

Not translated, with reason:
- `KafkaProducerTest` / `KafkaConsumerTest` broker-dependent majority — need a
  broker / Java's internal `MockClient`; covered by the gRPC/multilanguage
  integration arm (232 passed).
- `RecordSendTest` — tests the internal `FutureRecordMetadata`; the binding's
  `send()` returns a plain `concurrent.futures.Future`, so the get/timeout/error
  behavior is Python's own `Future`, not binding code.
- `PreparedTxnStateTest` — `PreparedTxnState` (2PC) is not on the Python surface
  (M13 P2 reverted 2PC).
- Two mock/core gaps recorded earlier as owner items: a reentrant blocking
  `commit()` from a listener on `MockConsumer` (C37) and `wakeup()`→`WakeupError`
  on an in-flight blocking call (needs a broker harness) — unchanged in P6.

## 5. Typing gate

`make verify` reaches `mypy --strict` via root `verify` → `test` → `test-python`
→ `bindings/python/Makefile` `test` → `typecheck`. The Makefile `typecheck`
comment now documents the venv-python requirement. `test_typing.py` gained
`assert_type` coverage for the consumer `close` @overloads (timeout vs option), the
`MockConsumer` `str | OffsetResetStrategy` constructor @overloads, the producer
`send()` → `Future[RecordMetadata]` + generic inference, and the `json_*` serde
factories (existing coverage already had TopicIdPartition, ConsumerRecords.records,
subscribe/seek, the other serde factories, KafkaConsumer/MockConsumer generic
inference). mypy --strict runs over `confluent_kafka` **and** `test_typing.py`.

## 6. Docs

Package `__init__` docstrings already stated the import paths / package mirroring.
New `bindings/python/README.md`: import paths (no root re-exports, spec §4), the
async-peer rule, the typed error model, and the spec §11 migration pointers from
`confluent-kafka-python`.

## 7. Verify

- `make test-python` (final): mypy --strict green (73 files); unit 676 passed / 1
  skipped; integration 232 passed / 0 failed.
- `cargo test -p xtask`: 38 passed. `cargo xtask check-generated`: green.
- No TODO/FIXME in the changed files. COMMENTS.69.md empty.

## Test counts

| | Before P6 | After P6 |
|---|---|---|
| `test_producer_family.py` | 73 | 82 |
| `test_consumer_family.py` | 52 | 54 |
| `test_consumer_callbacks_family.py` | — | 15 |
| legacy `test_producer/consumer/consumer_callbacks.py` | 179 | deleted |
| `test/unit` total (pytest functions) | — | 536 defs, 676 collected+parametrized |

## Clarifications logged

C40 (legacy retirement + admin compat shim), C41 (`_error_code.py` generator
change), C42 (gRPC port + metric-kind relaxation), C43 (performance tests
un-ported — `confluent_kafka` package-name collision with the reference PyPI
library, owner item, not in `make verify`).

## Repo-state note

A stale `core.worktree` in the `bindings/c/tests/unity` submodule config pointed
at a removed `../ecfk-p4` worktree, which aborted `git commit`'s post-hook
submodule status. Fixed the local `.git/modules/.../config` `core.worktree` to
point back at the main tree's submodule path (local git state only, not tracked).
