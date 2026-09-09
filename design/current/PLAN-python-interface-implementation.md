# Plan — implement the Python interface spec (producer + consumer)

**Branch:** `dev_python-interface-implementation` (from `origin/dev_interface_consolidation`)
**Manager:** this session · **Rules:** `.claude/rules/python-binding-interface.md` (new) + all existing rules
**Contract:** `design/current/python-client-interface-spec.md` (§4–§8, §10; §9 share consumer excluded — C1)
**Owner instruction (2026-09-09):** add the rules first, then implement with Actor/Critic loops over
*all* rules; never stop for input — log to `design/current/implementation-clarifications.md`; ask
the owner once at the end.

## Starting state (surveyed 2026-09-09)

| Layer | Present | Gap vs spec |
|---|---|---|
| `bindings/python/producer.py` (947 l) | `Producer`/`AsyncProducer`/`KafkaProducer`/`MockProducer`, flat `KafkaError` with `code()`/`is_retriable()`, `RecordMetadata` (4 accessors), positional signatures, `send(producer_record, on_delivery)` | package layout, keyword-only, typed error hierarchy, `has_offset()`/`has_timestamp()`/sizes, `send(*, record=…)`, serde kwargs, `Producer` non-instantiable guard, full `MockProducer` surface, context managers per spec |
| `bindings/python/consumer.py` (1258 l) | `Consumer`/`AsyncConsumer`/`KafkaConsumer`/`MockConsumer` + `Async*`, `ConsumerHandle`, `_ListenerAdapter` (listener on dispatcher thread), `commit(offsets, timeout)`, `commit_async` on handle only, `seek(partition, offset)`, `close(timeout)`, `client_id()`, `enforce_rebalance()` | keyword-only, `commit_nowait`, `seek` two params + stubs, `close(*, timeout, option: CloseOptions)`, `subscribe(*, topics|pattern, listener)`, `ConsumerRecords.records(*, partition|topic)` + `partitions()`/`next_offsets()`, listener on caller's task, drop `client_id`/`enforce_rebalance`, `MockConsumer(*, offset_reset_strategy: str|OffsetResetStrategy)`, full mock surface, value types keyword-only with accessor methods |
| `bindings/python/_confluentkafka.c` (7525 l) | C extension: native dispatch, `ConsumerRecords` type, producer natives | must follow the new signatures; stays private (`_confluentkafka`) |
| `src/ffi/{producer,consumer,common}.rs` | ~60 producer, ~160 consumer, ~60 common exports; `Error` handle + hierarchy predicates + typed payload accessors; async dispatcher | missing: `Consumer_close` with `CloseOptions`, `MockConsumer.set_offsets_exception`, `schedule_poll_task`, `should_rebalance`, `last_poll_timeout`, `update_duration_offsets`, `set_max_poll_records`, `MockProducer` failure-injection setters / txn observation, `client_instance_id`, metric (un)registration — add where the Rust core has the capability (rule 10) |
| `xtask generate-error-codes` | emits `_error_code.py` constants + Rust mirror from `kafka_common_ErrorCode_t` | extend to emit the Python class hierarchy (+ `.pyi`) from the Java exception sources cross-checked with the enum (rule 5) |
| tests | 179 pytest functions (producer 111, consumer 29, callbacks 39) | rewrite to the new surface + translate Java tests (rule 11); mypy in `make verify` |

## Phases (each = Actor N → Critic N → Actor fixes → … until `COMMENTS.N.md` is empty)

| # | Phase | Deliverable | Actor/Critic |
|---|---|---|---|
| P0 | Rules | `.claude/rules/python-binding-interface.md` reviewed against every existing rule | Critic 63 (rules review) → Manager fixes |
| P1 | Errors + package skeleton | `bindings/python/confluent_kafka/` package; `xtask generate-error-codes` extended → `common/errors/` generated hierarchy + `.pyi`, root JDK analogs, `id→class` table, cause chaining helper; `_confluentkafka` import path. **Done** (bccfc91d, 911d10b2, fixes 6692ef41, C19 typed re-export stub af64f41f; Critic 64: 0 blockers, F1/F2 low → fixed). Deviation accepted: `producer.py`/`consumer.py`/`_error_code.py` are NOT retired in P1 — they stay until P4/P5 replace them (C5), so the existing 365 tests keep passing at every phase boundary. | 64 |
| P2 | Common + consumer value types | `common/`: `TopicPartition`, `TopicIdPartition` (2 stubs), `Node` (+`has_rack`), `PartitionInfo`, `Uuid`, `MetricName`/`Metric`/`KafkaMetric`, `TimestampType`, `Headers`, `Duration`; `consumer/`: `OffsetAndMetadata`, `OffsetAndTimestamp`, `ConsumerGroupMetadata` (deprecated ctor, defaults), `CloseOptions` (+ nested enum, builder), `SubscriptionPattern`, `OffsetResetStrategy` (deprecated), `ConsumerRecord`, `ConsumerRecords` (`records(*, partition|topic)`, `partitions()`, `next_offsets()`, `empty()`), `ProducerRecord`, `RecordMetadata` (Java's 8 methods). **Actor done** (7049b57e, 3d790a2d, 9da263bf, 7551bc68; 175 tests; C6–C12 logged — C6: Java 4.3.1 `nextOffsets()` on the records-only ctor logs + returns `{}`, does not throw; C10: `ProducerRecord` pure-Python, native reconciliation deferred to P4). Critic 65: 0 blockers; F1–F3 fixed in c6fa796c, F4/F5 deferred to P4. **Done** (pending Critic 65 round-2 confirmation). | 65 |
| P3 | Serialization + config | `common/serialization/`: protocols, `Configurable`/`Closable`/`SerdeBase`, typed built-in factories (native execution where the FFI allows), kwarg + config routes; config coercion, unknown-key warning, `group.id` optional. **Actor done** (4a1ad452, f41b2963, 6af2cbb6; 130 tests; C15–C19 logged — C15 Uuid base64 wire form, C18 built-ins in Python for now, C19 errors `__init__.pyi` gap → Actor 64). Critic 66: 0 blockers, F1 Java timeout message, F2 double-NaN canonicalization → fixed in d84bde54. **Done.** | 66 |
| P4 | Producer family | `Producer` guard base, `KafkaProducer(*, config, key_serializer, value_serializer, partitioner)`, `AsyncProducer`/`AsyncKafkaProducer`, `MockProducer` full Java surface (FFI additions as needed), `send(*, record, on_delivery)` → `Future[RecordMetadata]` / `async def` returning `asyncio.Future`, txn methods without `timeout`, `partitions_for`, `metrics`, `client_instance_id`, `register/unregister_metric_for_subscription`, `close`, context managers. **Actor done** on `p4-producer-wip` (21b7a328, a53ec4eb, fixup 27516547; 73 family tests; C22–C25 + C36 — pure-Python `MockProducer`, owner to confirm). Critic 67: 0 blockers, F1–F4 fixed, round 2 PASS. Full verify-sandbox on 27516547: Rust 205/205 + C tests pass, but `bindings/c/grpc_server/server.cc:954` still calls the 2-arg `Producer_close` (F4 fold) → build-grpc-images-c fails → fixup round; re-verify → merge. | 67 |
| P5 | Consumer family | `Consumer` guard base, `KafkaConsumer`, `AsyncConsumer`/`AsyncKafkaConsumer`, `MockConsumer` full Java surface (FFI additions), every method per spec §6.2 incl. `@overload` stubs, `commit`/`commit_nowait`, `seek`, `close(*, timeout, option)`, `subscribe(*, topics|pattern, listener)`, listener + `on_commit` on the caller's task (async listener awaited), `ConsumerHandle` (§41) re-shaped, `wakeup`, drop `client_id`/`enforce_rebalance`. **Actor done** on `p5-consumer-wip` (6f6bf40c, 48b3ad86, 359116e7; C26–C35). Critic 68: 1 BLOCKER (reentrant listener op rejected by the FFI owner guard) + F2 cause-chain accessor, F3 unwired timeouts list, F4 `client_instance_id -> Uuid` → fixed in 274955ce; Critic 68 round 2: F2–F4 PASS, B1 resolved for the real consumer (reentrant ops via `ConsumerHandle`, other threads still rejected) with a documented CORE gap: the mock's `ConsumerHandle` has no blocking-op path, so a reentrant `commit()` inside a listener on `MockConsumer` raises `UnsupportedVersionError` where Java's mock allows it (C37, owner). Awaiting P4 merge → rebase → verify → merge. | 68 |
| P6 | Tests + typing gate | Java test translations (`MockProducerTest`, `MockConsumerTest`, `ConsumerRecordsTest`, `TopicPartitionTest`, `ProducerRecordTest`, `RecordMetadataTest`, …), keyword-only / combination / stub tests, error-hierarchy test, `mypy --strict` in `make verify`, migration of the existing 179 tests | 69 |
| P7 | Final audit | Critic R5 pass of the whole binding vs Java + spec; `make verify` green; clarifications file complete | 70 |

## Process (added 2026-09-09 after the C21 blocker)
The pre-commit hook runs `make verify-sandbox` over the WHOLE tree, so actors that touch Rust/C work in
their own worktree: P4 → `../ecfk-p4` branch `p4-producer-wip`; P5 → `../ecfk-p5` branch
`p5-consumer-wip` (both from `6af2cbb6`). Python-only fixes (Actors 64/65/66) commit in the main tree,
serialised by a `pgrep`-wait inside the same command as the commit. The Manager merges the worktree
branches into `dev_python-interface-implementation` (P4 first, then P5 rebased) before P6/P7.

**Hook (owner, 2026-09-10, option 2):** `.githooks/pre-commit` temporarily runs only `format-check` +
`lint` + Python `typecheck`. Full `make verify-sandbox` is run by the Manager (a) on each phase branch
before merging it, (b) at P7. **P7 exit step: restore `.githooks/pre-commit` to `exec make
verify-sandbox` and commit it.**

## Out of scope (logged as clarifications)
C1 share consumer (no core). Admin binding (paused; `admin.py` left as is). Schema Registry serde
(separate package). Custom partitioner plumbing (plugin pass; `partitioner=` accepted, declared).

## Done when
`make verify` passes; every `COMMENTS.<N>.md` for 63–70 is empty; the Critic 70 report lists zero
blockers/findings; `implementation-clarifications.md` holds every open question for the owner.
