# Milestone 11 — Producer Idempotence and Transactions

**Status:** APPROVED (2026-07-31). Phase 1 in progress; Phases 2-8 not started.

Decisions §7.1-§7.4 all approved as recommended: faithful idempotence-config
translation plus a temporary `KafkaProducer` guard (§7.1), bindings deferred to a
follow-up milestone (§7.2), `.claude/rules/producer-transactions.md` created
(§7.3), KIP-939 2PC kept in Phase 5 (§7.4).

**Corrections applied during Phase 1** — two items in §6.8 did not survive
contact with the Rust source and are superseded by
`.claude/rules/producer-transactions.md` §7 and §8:

  - `BTreeMap<(i64, i16, i32), ProducerBatch>` is **not implementable**.
    `ProducerBatch` is not `Clone`, and `Sender::in_flight_batches`
    (`sender.rs:122`) takes ownership after `drain()` moves it out of
    `RecordAccumulator` (`record_accumulator.rs:128`). Java relies on GC-shared
    references. `TxnPartitionEntry` therefore stores ordering *keys* only — see
    rules file §7. (Ownership **alternates**: on the retry path it returns to the
    accumulator's deque while the batch stays tracked. Corrected in rules §7 after
    Critic 41's second pass; the conclusion above is unaffected.)
  - "Phase 1 reuses `decrement_sequence`" is **wrong**. Java's
    `TxnPartitionEntry.decrementSequence` (163-173) does plain subtraction and
    throws on negative; it does not call `DefaultRecordBatch.decrementSequence`.
    Only `incrementSequence` delegates to the wrapping helper — see rules
    file §8.
**Scope:** Rust only. C FFI / Python / gRPC multilanguage harness deferred (see §7.2).
**Java source:** Apache Kafka 4.2.0 (`kafka/` submodule at `a18251b`).
**Agent numbers:** 41–48 (highest previously used is 40).

---

## 1. What this milestone delivers

The Java `TransactionManager` and its integration into the producer send path,
giving the Rust client:

- **Idempotent produce** — `enable.idempotence=true` actually acquires a producer
  ID via `InitProducerId`, assigns per-partition sequence numbers, tracks
  in-flight batches by sequence, and recovers from
  `OUT_OF_ORDER_SEQUENCE_NUMBER` / `UNKNOWN_PRODUCER_ID` via epoch bump.
- **Transactions** — the 9-state transaction state machine, the five coordinator
  RPCs, transaction-coordinator discovery, and the public
  `init_transactions` / `begin_transaction` / `commit_transaction` /
  `abort_transaction` / `send_offsets_to_transaction` API on both
  `KafkaProducer` and `MockProducer`.
- **KIP-890 Transaction V2** and **KIP-939 two-phase commit**, both of which are
  present in Kafka 4.2's `TransactionManager` and are not optional if we want
  behavioral parity (see §6.1).

### 1.1 Corrections to the pre-milestone gap analysis

Verified by reading the source. Three items in the briefing were wrong or
overstated, and they change the plan:

| Briefing claim | Reality |
|---|---|
| "Typed errors missing: `TransactionAbortedError`, `TransactionAbortableError`, `TransactionalIdAuthorizationError`, `InvalidTxnStateError`, `UnknownProducerIdError`" | **All the wire error codes already exist** in `src/common/protocol/errors.rs`: `OutOfOrderSequenceNumber=45`, `DuplicateSequenceNumber=46`, `InvalidProducerEpoch=47`, `InvalidTxnState=48`, `InvalidProducerIdMapping=49`, `InvalidTransactionTimeout=50`, `ConcurrentTransactions=51`, `TransactionCoordinatorFenced=52`, `TransactionalIdAuthorizationFailed=53`, `UnknownProducerId=59`, `ProducerFenced=90`, `TransactionalIdNotFound=105`, `TransactionAbortable=120`. This codebase only creates a *typed error struct* when the Java subclass carries extra payload (cf. `TopicAuthorizationError`'s `unauthorized_topics`). None of these do. They become `KafkaError::with_message(Errors::X, ..)` / `KafkaError::fatal(..)`. **Only one new enum variant is needed:** `TransactionAborted`, because Java's `TransactionAbortedException` has *no wire code* (verified absent from `Errors.java`) — it is client-side only, like the existing `Wakeup` variant. |
| "Request/response wrappers for all 5 txn RPC pairs ... `WriteTxnMarkers`" | `WriteTxnMarkers` is **not producer-side**. Its only `clients/src/main` users are `Admin.java` and `admin/internals/AbortTransactionHandler.java`; `grep` over `producer/**` returns nothing. It is a coordinator→broker RPC. **Out of scope** — it belongs to a future Admin milestone. Producer needs exactly 5 pairs. |
| "`EndTransactionMarker` missing" | Also **not producer-side**. Its users are `MemoryRecordsBuilder.writeEndTransactionalMarker` and `MemoryRecords.withEndTransactionMarker` — the *broker* write path for control records. The producer never constructs one. **Out of scope**; leave in `remaining_classes.txt`. |

Two further findings that *reduce* risk:

- **`ApiVersions` finalized-features support already exists** —
  `src/api_versions.rs` has `FinalizedFeaturesInfo { finalized_features_epoch,
  finalized_features }`, `max_finalized_features_epoch()`, and
  `finalized_features_info()`. There is even a test at `api_versions.rs:186`
  asserting on the `"transaction.version"` key. This is the exact prerequisite
  for KIP-890 TV2 detection (`TransactionManager.maybeUpdateTransactionV2Enabled`,
  Java lines 492–504), and it is already done.
- **`ProduceRequest` TV1/TV2 plumbing already exists but is unused** —
  `src/common/requests/produce_request.rs` has
  `LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2 = 11` (line 37), a
  `transactional_id` cache field (54), `transactional_id()` (97),
  `is_transaction_v2_requested(version)` (102), and
  `ProduceRequestBuilder::builder(data, use_transaction_v1_version)` (260).
  `Sender` never calls the two-arg builder and never sets `transactional_id`.
  Phase 4/6 wires up what is already there.
- **`CoordinatorType::Transaction`** already exists at
  `src/common/requests/find_coordinator_request.rs:50`, and
  `FindCoordinatorRequestBuilder::build_version` (line 215) already rejects
  `version < 1` for the transaction key type. Coordinator discovery reuses the
  consumer's wrapper unchanged.

One finding that *adds* scope: **`MockProducer`'s transactional surface is an
explicitly-deferred debt with 44 named skipped tests**, listed in a comment
block at `src/producer/mock_producer.rs:525–577`. This is not in the briefing's
gap analysis but is squarely in scope for this milestone (DoD §3).

---

## 2. Sequencing decision: idempotence first — validated against the source

The briefing asked us to validate the "idempotence first" recommendation against
the actual Java class decomposition rather than assume it. **It holds, and the
Java code is unusually well-fenced for it.**

Every transactional code path in `TransactionManager.java` is behind either
`ensureTransactional()` (line 1147, throws when `transactionalId == null`) or an
`if (isTransactional())` guard. Nothing on the idempotent path calls a
transaction-only method. Concretely, an idempotence-only slice needs:

- **4 of 9 states**: `UNINITIALIZED`, `INITIALIZING`, `READY`, `FATAL_ERROR`.
  `ABORTABLE_ERROR` is unreachable without a `transactionalId`.
  (`PREPARED_TRANSACTION`, `COMMITTING_*`, `ABORTING_*` likewise.)
- **1 of 6 request handlers**: `InitProducerIdHandler` (Java 1461–1539) plus the
  `TxnRequestHandler` base (1345–1459). `InitProducerIdHandler.coordinatorType()`
  (1482–1488) returns `null` when non-transactional, so **the whole
  FindCoordinator subsystem and the `Priority` priority-queue are unnecessary**
  for idempotence — a plain FIFO holding a single `InitProducerId` suffices.
- **`TxnPartitionMap` + `TxnPartitionEntry` in full.** Despite the `Txn` names
  these two files are *pure idempotence machinery*: sequence numbers, in-flight
  batches by sequence, last-acked sequence/offset. Their only transaction
  awareness is a `boolean isTransactional` parameter threaded into
  `TxnPartitionMap.updateLastAckedOffset` (Java 88–99) that decides whether to
  lazily create a missing entry.

Five methods are *shared but internally forked* on `isTransactional()` and so get
their transactional arms added in Phase 5 rather than being separable functions:
`maybeTransitionToErrorState` (764–786), `handleFailedBatch` (788–822),
`maybeResolveSequences` (850–883), `nextRequest` (894–932), `canRetry`
(1015–1082). All five are strictly shorter in the idempotent-only form, so this
is additive growth, not rework.

**Caveat that the plan must absorb (this is the one place the clean carve
breaks):** the *implementation* carves cleanly but the *test suite does not*.
`TransactionManagerTest.setup()` (line 163) constructs a **transactional**
manager by default; only ~15 call sites re-initialize with `Optional.empty()`
for the idempotent-only case. So most sequence-number tests run through a
transactional manager. Consequence: Phase 3 can only land ~15–20 of the 140
`TransactionManagerTest` methods; the rest are **deferred within this
milestone** to Phases 5 and 8 — not skipped. Phase 3's DoD explicitly does *not*
claim `TransactionManagerTest` parity.

---

## 3. Phase breakdown

| Phase | N | Title | Java main | Java tests |
|---|---|---|---|---|
| 1 | 41 | Support types, error variant, config validation + honesty guard | 4 files (~350 LoC) | `ProducerConfigTest` (idempotence subset) |
| 2 | 42 | Five txn request/response wrapper pairs | 10 files (~1 030 LoC) | 6 dedicated `*Test.java` (695 L) + `RequestResponseTest` txn cases |
| 3 | 43 | `TransactionManager` — idempotence core | `TransactionManager.java` (idempotent slice) | ~15–20 `TransactionManagerTest` methods |
| 4 | 44 | Idempotent send-path integration | `Sender.java`, `RecordAccumulator.java` (deltas) | `SenderTest` + `RecordAccumulatorTest` idempotence subsets |
| 5 | 45 | `TransactionManager` — transactional state machine, 5 handlers, TV2, 2PC | `TransactionManager.java` (remainder) | bulk of `TransactionManagerTest` |
| 6 | 46 | `Producer` trait + `KafkaProducer` txn API + Sender txn request loop | `KafkaProducer.java`, `Producer.java`, `Sender.java` (txn loop) | `KafkaProducerTest` (27 txn tests) |
| 7 | 47 | `MockProducer` transactional surface | `MockProducer.java` (txn methods) | 44 currently-skipped `MockProducerTest` methods |
| 8 | 48 | `TransactionManagerTest` parity sweep + broker integration | — | remainder of `TransactionManagerTest`; new integration tests |

Dependency order is strict: 1 → 2 → 3 → 4 → 5 → 6 → {7, 8}. Phases 7 and 8 are
independent of each other and may run in parallel if desired (different agent
numbers, disjoint files).

---

### Phase 1 (N=41) — Support types, error variant, config validation + honesty guard

| Java source | Lines | Target Rust path |
|---|---|---|
| `common/utils/ProducerIdAndEpoch.java` | 59 | `src/common/utils/producer_id_and_epoch.rs` |
| `common/requests/TransactionResult.java` | 34 | `src/common/requests/transaction_result.rs` |
| `producer/internals/TransactionalRequestResult.java` | 87 | `src/producer/internals/transactional_request_result.rs` |
| `producer/internals/TxnPartitionEntry.java` | 174 | `src/producer/internals/txn_partition_entry.rs` |
| `producer/internals/TxnPartitionMap.java` | 131 | `src/producer/internals/txn_partition_map.rs` |
| `producer/ProducerConfig.java` 591–651 | 60 | `src/producer/producer_config.rs` (extend) |
| `common/errors/TransactionAbortedException.java` | 38 | `src/common/kafka_error.rs` (new enum variant) |

Full per-commit detail in `Phase-1/PLAN.md`.

---

### Phase 2 (N=42) — Five txn request/response wrapper pairs

| Java source | Lines | Target Rust path |
|---|---|---|
| `requests/InitProducerIdRequest.java` | 85 | `src/common/requests/init_producer_id_request.rs` |
| `requests/InitProducerIdResponse.java` | 82 | `src/common/requests/init_producer_id_response.rs` |
| `requests/AddPartitionsToTxnRequest.java` | 203 | `src/common/requests/add_partitions_to_txn_request.rs` |
| `requests/AddPartitionsToTxnResponse.java` | 165 | `src/common/requests/add_partitions_to_txn_response.rs` |
| `requests/AddOffsetsToTxnRequest.java` | 68 | `src/common/requests/add_offsets_to_txn_request.rs` |
| `requests/AddOffsetsToTxnResponse.java` | 81 | `src/common/requests/add_offsets_to_txn_response.rs` |
| `requests/EndTxnRequest.java` | 85 | `src/common/requests/end_txn_request.rs` |
| `requests/EndTxnResponse.java` | 84 | `src/common/requests/end_txn_response.rs` |
| `requests/TxnOffsetCommitRequest.java` | 260 | `src/common/requests/txn_offset_commit_request.rs` |
| `requests/TxnOffsetCommitResponse.java` | 206 | `src/common/requests/txn_offset_commit_response.rs` |

**Tests** (all translatable, all self-contained — this is why Phase 2 carries its
own DoD value independent of `TransactionManager`):
`AddPartitionsToTxnRequestTest` (161), `AddPartitionsToTxnResponseTest` (146),
`EndTxnRequestTest` (95), `EndTxnResponseTest` (54),
`TxnOffsetCommitRequestTest` (172), `TxnOffsetCommitResponseTest` (67).
`InitProducerId` and `AddOffsetsToTxn` have no dedicated test file; their
coverage lives in `RequestResponseTest.java` (21 matching sites) — translate
those cases into the per-wrapper Rust test modules, matching the convention the
existing wrappers use.

**Wiring cost, measured:** adding one variant to `ConcreteRequest`
(`abstract_request.rs:97–123`) requires editing **8 exhaustive `match` sites**
(`version` 127, `api_key` 145, `to_send` 169, `serialize_with_header` 194,
`serialize` 263, `get_error_response` 296, `do_parse_request` 332, `Display`
396). `ConcreteResponse` (`abstract_response.rs:56–82`) requires **10**
(`api_key` 86, `to_send` 110, `serialize_with_header` 134, `serialize` 182,
`error_counts` 210, `throttle_time_ms` 230, `maybe_set_throttle_time_ms` 249,
`should_client_throttle` 267, `parse` 322, `Display` 385). Plus `mod.rs`
declarations (21–53) and re-exports (55–90), alphabetical.

**Design note — no `AbstractRequest` trait exists.** The pattern is: a
`{Name}Request` struct holding `data` + `version` with a fixed inherent surface
(`new`, `data`, `pub(crate) data_mut`, `version`, `api_key`,
`get_error_response`, `parse`), plus a `{Name}RequestBuilder` implementing
`pub trait RequestBuilder` (`abstract_request.rs:64–89`). Responses are plain
structs, duck-typed by the enum arms; the required surface is `new`, `api_key`,
`data`, `pub(crate) data_mut`, `throttle_time_ms`,
`maybe_set_throttle_time_ms`, `should_client_throttle`, `error_counts`, `parse`.
Copy `find_coordinator_{request,response}.rs` as the reference — it is the
closest analogue (version-gated builder, coordinator-typed, `Node` extraction).

**Scope note.** All 5 pairs land in one phase rather than splitting
`InitProducerId` (the only one idempotence needs) into Phase 2 and the other 4
into Phase 5. Rationale: one pass through the 18 exhaustive match sites instead
of two, and the 695 lines of dedicated Java wrapper tests give the phase
independent verification value. Cost: 4 wrapper pairs sit unused until Phase 5
(`internals` modules are already `#![allow(dead_code)]`-tolerant, so this does
not fight `#![deny(warnings)]`).

---

### Phase 3 (N=43) — `TransactionManager`, idempotence core

`src/producer/internals/transaction_manager.rs` — created here, grown in Phase 5.
Per CLAUDE.md §2 (`internal` packages), everything is `pub(crate)`.

Translate from `TransactionManager.java`:

- `State` enum, but only the 4 reachable states, **with the full 9-variant
  `is_transition_valid` table from Java 162–188 written correctly from the
  start** so Phase 5 adds no transition logic. (Target-first table; note the
  `ABORTABLE_ERROR` self-loop and that `READY → READY` is illegal.)
- `transition_to(target, error, caller)` (Java 1114–1145) — see §6.2 for the
  `Caller` parameter that replaces Java's `Thread.currentThread() instanceof
  Sender.SenderThread`.
- Producer-ID lifecycle: `producer_id_and_epoch` (581), `maybe_update_producer_id_and_epoch`
  (585), `set_producer_id_and_epoch` (603), `reset_idempotent_producer_id` (618),
  `request_idempotent_epoch_bump_for_partition` (640), `bump_idempotent_producer_epoch`
  (645), `bump_idempotent_epoch_and_reset_id_if_needed` (663).
- Sequence machinery: `sequence_number` (682), `increment_sequence_number` (693),
  `first_in_flight_sequence` (710), `next_batch_by_sequence` (717),
  `last_acked_sequence` (730), `last_acked_offset` (734),
  `reset_sequence_for_partition` (627), `reset_sequence_numbers` (632),
  `is_next_sequence` (885), `is_next_sequence_for_unresolved_partition` (889),
  `has_stale_producer_id_and_epoch` (828), `has_unresolved_sequences` (832),
  `has_unresolved_sequence` (836), `mark_sequence_unresolved` (840),
  `maybe_resolve_sequences` (850, idempotent arm only).
- Batch bookkeeping: `add_in_flight_batch` (697), `remove_in_flight_batch` (721),
  `has_inflight_batches` (824), `update_last_acked_offset` (738),
  `handle_completed_batch` (745), `handle_failed_batch` (788, idempotent arm),
  `can_retry` (1015, idempotent arms).
- `InitProducerIdHandler` (1461–1539) + the `TxnRequestHandler` base (1345–1459),
  non-transactional path only (`coordinator_type()` → `None`).
- `maybe_fail_with_error` (1152), `transition_to_fatal_error` (541),
  `has_fatal_error` (986), `last_error` (462), `hasProducerId` (476),
  `is_transactional` (480).

**Tests:** the ~15 `TransactionManagerTest` methods that call
`initializeTransactionManager(Optional.empty(), ..)` (Java lines 270, 277, 626,
635, 673, 713, 750, 852, 865, 3041, 3085, +). Phase 3's DoD does **not** claim
`TransactionManagerTest` parity — see §2.

---

### Phase 4 (N=44) — Idempotent send-path integration

No new Java classes; deltas to two existing Rust files. Insertion points are
already scaffolded with deferral comments.

| Concern | Java reference | Rust insertion point |
|---|---|---|
| `TransactionManager` field + ctor arg | `Sender.java:123,140,154` | `src/producer/internals/sender.rs:97–131` (struct), `136–168` (`new`) |
| `maybe_resolve_sequences` / fatal check / `bump_idempotent_epoch_and_reset_id_if_needed` | `Sender.java:310–340` | `sender.rs:213–216` — **replaces the existing `// No transaction manager in this phase` comment at line 214**, must run before `send_producer_data` |
| **Per-batch sequence assignment** + `add_in_flight_batch` | `RecordAccumulator.java:900–925` | `record_accumulator.rs` between `deque.pop_front()` (919) and `batch.close()` (923). Must be here, not in `Sender`: `sender.rs:396` (`batch.records()` → `take_built_records()`) serializes the v2 batch header, so producer id / epoch / base sequence must already be set. |
| `should_stop_drain_batches_for_partition` | `RecordAccumulator.java:815–850` | `record_accumulator.rs:858`, alongside the existing `is_muted` check |
| `handle_completed_batch` | `Sender.java:758` | `sender.rs:766` — replaces `// No transaction manager in this phase` |
| `handle_failed_batch(.., adjust_sequence_numbers)` | `Sender.java:848` | `sender.rs:878` — replaces `// No transaction manager handling in this phase`; **wire up the already-present-but-underscore-ignored `_adjust_sequence_numbers` parameter** at `sender.rs:867` |
| `transaction_manager.can_retry` | `Sender.java:881` | `sender.rs:892–897` |
| `remove_in_flight_batch` on `MESSAGE_TOO_LARGE` split | `Sender.java:686` | `sender.rs:658` |
| `mark_sequence_unresolved` on expiry | `Sender.java:374` | `sender.rs:435` (`fail_expired_batches`) |
| `RecordAccumulator` `transaction_manager` field | `RecordAccumulator.java` ctor | `record_accumulator.rs:142–166` (struct), `184`/`223` (ctors) |
| Remove the Phase-1 idempotence guard | — | `kafka_producer.rs` (see §7.1) |

Also in this phase: `KafkaProducer` must gain `configure_transaction_state`
(Java `KafkaProducer.java:592–620`) and share the `TransactionManager` with the
Sender task. **This is a real structural obstacle** — see §6.3.

**Tests:** the idempotence subset of `SenderTest.java` (43 of its 76 tests are
idempotence/txn-related; the idempotence-only ones land here, the transactional
ones in Phase 6) and the idempotence subset of `RecordAccumulatorTest.java`
(which references `TransactionManager` directly).

---

### Phase 5 (N=45) — `TransactionManager`, transactional state machine

Grows `transaction_manager.rs` to full parity. From `TransactionManager.java`:

- All 9 states reachable; `Priority` enum (195–207) and the priority-ordered
  pending-request queue (224). Note `InitProducerIdHandler.priority()` (1477) is
  *dynamic*: `EPOCH_BUMP` when bumping, else `INIT_PRODUCER_ID`. Java's
  `PriorityQueue` is unstable; a Rust `BinaryHeap` matches, but add an
  insertion-sequence tiebreaker if determinism is wanted for tests.
- Entry points: `initialize_transactions` (291/295/299), `begin_transaction`
  (330), `prepare_transaction` (342, 2PC), `begin_commit` (353), `begin_abort`
  (361), `begin_completing_transaction` (373), `send_offsets_to_transaction` (404),
  `maybe_add_partition` (437), `is_send_to_partition_allowed` (466),
  `transition_to_uninitialized` (756), `reset_transaction_state` (1330).
- `PendingStateTransition` (1953–1967) + `handle_cached_transaction_request_result`
  (1261–1283) + `throw_if_pending_state` (1249). See §6.4 — this is why
  `TransactionalRequestResult` cannot be a `oneshot`.
- Error machine: `transition_to_abortable_error` (530),
  `transition_to_abortable_error_or_fatal_error` (557), `has_abortable_error` (991),
  `fail_pending_requests` (944), `need_to_trigger_epoch_bump_from_client` (1309),
  `can_handle_abortable_error` (1326), `maybe_transition_to_error_state` (764, txn arm).
- The 5 remaining handlers: `FindCoordinatorHandler` (1651–1721),
  `AddPartitionsToTxnHandler` (1541–1649, incl. the
  `ADD_PARTITIONS_RETRY_BACKOFF_MS = 20` override on first
  `CONCURRENT_TRANSACTIONS`), `EndTxnHandler` (1723–1794),
  `AddOffsetsToTxnHandler` (1796–1854), `TxnOffsetCommitHandler` (1856–1951).
- Coordinator state: `coordinator` (958), `lookup_coordinator` (969/1191),
  `handle_coordinator_ready` (1103), `coordinator_supports_bumping_epoch` (1110).
  See §6.5 for where this state must live in Rust.
- **KIP-890 TV2**: `maybe_update_transaction_v2_enabled` (492–504),
  `is_transaction_v2_enabled` (506). Reads
  `ApiVersions::finalized_features_info()` for `"transaction.version" >= 2`.
  TV2 changes behavior materially: `maybe_add_partition` (448) skips
  `AddPartitionsToTxn` entirely, `send_offsets_to_transaction` (417) skips
  `AddOffsetsToTxn`, and `EndTxnHandler` absorbs a server-returned pid/epoch.
- **KIP-939 2PC**: `enable2PC` (148), `PREPARED_TRANSACTION` state,
  `prepare_transaction` (342), `is_prepared` (1099), `prepared_transaction_state`
  (1976), and the `keep_prepared_txn` branch of `InitProducerIdHandler`
  (1501–1511).

**Tests:** the bulk of `TransactionManagerTest.java` (140 methods total: 122
`@Test` + 18 `@ParameterizedTest`; the `@ParameterizedTest` ones are mostly
`transactionV2Enabled` true/false matrices). Expect this phase's test volume to
exceed its implementation volume. If the phase proves too large in practice,
split at plan time into 5a (state machine + `FindCoordinator` +
`InitProducerId` epoch bump) and 5b (AddPartitions / AddOffsets /
TxnOffsetCommit / EndTxn + TV2 + 2PC).

---

### Phase 6 (N=46) — Public producer API + Sender transactional loop

| Java source | Target Rust path |
|---|---|
| `producer/Producer.java` 43–66 | `src/producer/producer_trait.rs` — add 5 methods, delete the "not included in this phase" note at lines 34–36 |
| `producer/KafkaProducer.java` 592–620, 648–860, 956–990, 1507 | `src/producer/kafka_producer.rs` |
| `producer/internals/Sender.java` 234, 267–292, 459–530, 564–569, 917–926 | `src/producer/internals/sender.rs` |

Translations of note:
- `init_transactions` / `commit_transaction` / `abort_transaction` /
  `send_offsets_to_transaction` are **blocking in Java** (`result.await(maxBlockTimeMs, ..)`
  at `KafkaProducer.java:654, 742, 785, 820`) → `async fn` returning
  `Result<(), KafkaError>`, with `tokio::time::timeout(max_block_ms, ..)`
  mapping expiry to `KafkaError::timeout` (CLAUDE.md §9.1).
- `begin_transaction` does **not** block in Java (line 674–681, pure state
  transition) → stays a **sync** `fn`. Do not make it `async` for symmetry.
- `prepare_transaction` (2PC) is on `KafkaProducer` only, **not** on the
  `Producer` interface — verified: `grep prepareTransaction Producer.java`
  returns nothing. Mirror that: inherent method on `KafkaProducer`, absent from
  the `Producer` trait.
- `maybe_send_and_poll_transactional_request` (`Sender.java:459–518`) is the
  riskiest single method in the milestone — see §6.6.
- `sendProduceRequest` (924–926) sets `transactional_id` and
  `use_transaction_v1_version` on the produce request → wire
  `ProduceRequestBuilder::builder(data, !is_transaction_v2_enabled)` at
  `sender.rs:977–982`.
- Remove the Phase-1 transactions guard (§7.1).

**Tests:** the 27 transactional tests in `KafkaProducerTest.java`, plus the
transactional subset of `SenderTest.java`.

---

### Phase 7 (N=47) — `MockProducer` transactional surface

`src/producer/mock_producer.rs`. Java `MockProducer.java` fields
`transactionInitialized` (70), `transactionInFlight` (71), `sentOffsets` (75),
plus `initTransactions` (145), `beginTransaction` (162),
`sendOffsetsToTransaction` (182), `commitTransaction` (204),
`abortTransaction` (230), `fenceProducer`, and the uncommitted-record /
uncommitted-offset staging that makes
`shouldPublishMessagesOnlyAfterCommitIfTransactionsAreEnabled` pass.

**Tests:** the **44 named skipped tests** enumerated at
`src/producer/mock_producer.rs:525–577`. Delete that comment block as the tests
land. Pure in-memory state, no network — the cheapest full-parity phase in the
milestone, and it independently validates the Phase-6 trait shape.

One test stays skipped with justification already written and still valid:
`shouldThrowClassCastException` (`mock_producer.rs:579–588`) — tests Java type
erasure, which has no Rust analogue.

---

### Phase 8 (N=48) — `TransactionManagerTest` parity sweep + broker integration

- Close out any `TransactionManagerTest` method not landed in Phases 3/5, so the
  full 140 are accounted for (translated, or named-and-justified per DoD §3).
- New integration tests under `tests/integration/` against a testcontainers
  broker: idempotent produce with a forced epoch bump; transactional
  commit visible only after commit; abort discards; consume-transform-produce
  with `send_offsets_to_transaction` against the Milestone-8 consumer with
  `isolation.level=read_committed`.
- Final DoD closure: `make verify`, `design/current/` refresh,
  `marked_classes.txt` / `remaining_classes.txt` updates.

---

## 4. `marked_classes.txt` / `remaining_classes.txt` deltas

Move from `remaining_classes.txt` to `marked_classes.txt` as each phase lands:

```
org.apache.kafka.common.utils.ProducerIdAndEpoch                       (line 450) → Phase 1
org.apache.kafka.common.requests.TransactionResult                     (line 151) → Phase 1
org.apache.kafka.clients.producer.internals.TransactionalRequestResult (line 457) → Phase 1
org.apache.kafka.clients.producer.internals.TxnPartitionEntry          (line 459) → Phase 1
org.apache.kafka.clients.producer.internals.TxnPartitionMap            (line 460) → Phase 1
org.apache.kafka.clients.producer.internals.TransactionManager         (line 471) → Phase 5
```

Plus the 10 new `common.requests.*` wrapper classes in Phase 2. **Leave
`org.apache.kafka.common.record.EndTransactionMarker` (line 70) in
`remaining_classes.txt`** — out of scope per §1.1.

---

## 5. New rules file: **recommended — create `.claude/rules/producer-transactions.md`**

Recommendation: **yes**, in Phase 1, governed the same way
`consumer-threading.md` governs Milestone 8. There are six load-bearing design
calls with no mechanical Java→Rust mapping, and without a written rule a Critic
cannot tell an intentional deviation from a bug:

1. **`Caller` enum replacing thread identity** (§6.2) — correctness-critical,
   zero Java analogue.
2. **Lock topology**: which `TransactionManager` state is shared behind
   `Arc<Mutex<..>>` vs. owned by the Sender task (§6.5). Java's four unguarded
   non-volatile fields are safe *only* by single-thread confinement; naively
   putting them behind the shared lock is wrong, and naively leaving them
   unsynchronized is also wrong.
3. **Lock ordering `deque` → `TransactionManager`** (§6.7), from
   `RecordAccumulator.java:877–926`.
4. **Never hold the `TransactionManager` guard across `.await`**, and **never
   race the network poll in `select!`** — extends `consumer-threading.md` §10's
   hard-won cancellation rule to the producer Sender (§6.6).
5. **`TransactionalRequestResult` keeps latch semantics** — re-awaitable, with
   `is_acked` distinct from `is_completed`. A `oneshot` is *wrong* (§6.4).
6. **`BTreeSet`/`BTreeMap` must not be keyed on interior-mutable sort keys**
   (§6.8).

Items 1, 2, 5 and 6 are exactly the class of decision that produced repeated
Critic churn in Milestone 8 before `consumer-threading.md` existed.

---

## 6. Risks and blockers

### 6.1 KIP-890 (TV2) and KIP-939 (2PC) are in the 4.2 source — not optional

Kafka 4.2's `TransactionManager` carries both. `isTransactionV2Enabled` (line
147) gates *behavioral* forks, not cosmetics: under TV2 the producer sends **no**
`AddPartitionsToTxn` (line 448) and **no** `AddOffsetsToTxn` (line 417), and
`EndTxn` returns a fresh pid/epoch. Implementing only the V1 path would look
correct against a V1 broker and silently misbehave against a modern one.
Mitigation: TV2 detection is *already unblocked* (`ApiVersions` finalized
features exist), and `ProduceRequest` already has the version cap. Both forks
land in Phase 5, tested via the `transactionV2Enabled` parameterized matrix that
`TransactionManagerTest` already provides.

2PC (`enable2PC`, `PREPARED_TRANSACTION`, `prepareTransaction`,
`keepPreparedTxn`) is smaller and self-contained. **Open decision:** could be
deferred (§7.4).

### 6.2 `shouldPoisonStateOnInvalidTransition` has no Rust analogue — highest-risk mapping

`TransactionManager.java:287–289`:

```java
protected boolean shouldPoisonStateOnInvalidTransition() {
    return Thread.currentThread() instanceof Sender.SenderThread;
}
```

On an invalid state transition Java behaves **differently depending on which
thread it is on** (documented at length in Java 234–286, KAFKA-14831):
application thread → throw, state unchanged, user can recover; Sender thread →
set `FATAL_ERROR`, store `lastError`, then throw ("poison"). Rust has no
equivalent of `instanceof SenderThread`, and `tokio::task::id()` is not a stable
substitute (the Sender is a spawned task whose id is not known to the app side
at call time).

Proposed mapping: thread an explicit `Caller { App, Sender }` parameter into
`transition_to`. Every call site must pass its origin literally. Rationale to
record in the rules file: the poison-vs-throw distinction is load-bearing for
the transactional guarantee, so it must be *explicit and reviewable* rather than
inferred. Risk: ~20 call sites, each an opportunity to pass the wrong value; a
Critic must check every one against the Java call chain.

### 6.3 `KafkaProducer` holds no client handle — structural obstacle

At `kafka_producer.rs:392–395` the `Sender` (and therefore the `KafkaClient`) is
**moved into** `tokio::task::spawn`. `KafkaProducer` retains only `accumulator`,
`metadata`, `running`, `force_close`, `wakeup`, `sender_handle`. But
`TransactionManager` is written and read by **both** sides: `KafkaProducer` calls
`initialize_transactions` / `begin_transaction` / `begin_commit` / `begin_abort`
/ `send_offsets_to_transaction` / `maybe_add_partition` /
`maybe_transition_to_error_state`, while the Sender task calls ~25 other
methods, and `RecordAccumulator` calls 12 more.

So `TransactionManager` must be `Arc<...>` shared three ways
(`KafkaProducer`, `Sender`, `RecordAccumulator`) with interior mutability. That
is fine, but it means Phase 4 changes the construction order in
`from_config`/`with_client`: the manager must be built **before** the accumulator
(which needs it) and before the Sender (which needs it), then cloned into all
three. Mitigation: this is a mechanical refactor, but it touches the most
load-bearing constructor in the crate, so Phase 4 should do it as its own commit
with the existing producer test suite green before any sequence logic is added.

### 6.4 `TransactionalRequestResult` is not a `oneshot`

Java uses `CountDownLatch(1)` plus a **separate `volatile boolean isAcked`** that
is set **only inside `await()`** (`TransactionalRequestResult.java:62`).
`handle_cached_transaction_request_result` (Java 1261–1283) keys off
`isAcked()`, not `isCompleted()`: a `commitTransaction` that *completed* but was
never *awaited* must return the **same** result object on retry, and a
`commitTransaction` that timed out must be retryable while rejecting a
*different* operation with `IllegalStateException`.

A `tokio::sync::oneshot` cannot express this (single-consumer, not
re-awaitable). Required shape: a struct holding `Arc<Notify>` (or a
`watch` channel) + `Mutex<Option<KafkaError>>` + `AtomicBool` completed +
`AtomicBool` acked, with `await_result(timeout)`, `done()`, `fail(err)`,
`is_completed()`, `is_acked()`, `is_successful()`. Getting this wrong produces
a hang or a double-commit that no unit test in Phase 1 would catch — the
covering tests are in `TransactionManagerTest` (Phase 5).

### 6.5 Four Java fields are safe only by single-thread confinement

`inFlightRequestCorrelationId` (136), `transactionCoordinator` (137),
`consumerGroupCoordinator` (138), `coordinatorSupportsBumpingEpoch` (139) are
**non-volatile and not consistently guarded**. E.g. `clearInFlightCorrelationId`
is called from `TxnRequestHandler.onComplete` at line 1410 — *outside* the
`synchronized` block that starts at 1421. Likewise `pendingRequests` (a plain
`PriorityQueue`) is mutated via the **unsynchronized** `lookupCoordinator(TxnRequestHandler)`
(969) that `Sender.java:522` calls directly.

These are not bugs in Java: they are touched exclusively by the Sender thread.
The Rust translation must decide deliberately: **coordinator nodes, in-flight
correlation id, `coordinatorSupportsBumpingEpoch`, and the pending-request queue
belong to the Sender task's own (unshared) state**, while the `volatile` fields
and the partition maps go behind the shared lock. Putting everything behind one
mutex would be *safe but slower and less faithful*; leaving them unsynchronized
in Rust would not compile. A Critic reading only the Java `synchronized`
keywords will get this wrong in both directions — hence the rules file.

### 6.6 `maybeSendAndPollTransactionalRequest` — cancellation and blocking

`Sender.java:459–518` contains, on the Sender thread: `client.poll(...)` at 462,
496, 509; `awaitNodeReady(...)` at 484 (which is `NetworkClientUtils.awaitReady`,
blocking); `time.sleep(retryBackoffMs)` at 501 and 525.

In Rust all four become `.await` points inside the Sender task. Two hazards:

- **The network poll is not cancel-safe in this codebase.** This is the exact
  failure documented in `consumer-threading.md` §10 and
  `design/current/consumer-join-stall-rootcause.md`: `initiate_connect` calls
  `connection_states.connecting()` (a persisted side effect) and *then* awaits
  the TCP handshake, so a dropped poll strands the node in `Connecting` until the
  ~10 s connection-setup timeout. The producer Sender must therefore **not**
  race the poll in a `tokio::select!` either. Rule 4 in §5 exists to carry this
  forward; Milestone 8 learned it the hard way.
- **No `MutexGuard` may be held across any of these awaits** (CLAUDE.md §9.6.2).
  Since this method calls ~10 `TransactionManager` methods interleaved with 3
  polls and 2 sleeps, the guard must be acquired and dropped repeatedly. That
  changes atomicity relative to Java, where the Sender thread's view is stable
  simply because it *is* the only writer. Each re-acquire is a place where Java's
  implicit consistency can break. This is the second-highest-risk item after
  §6.2 and should be reviewed method-by-method in Phase 6.

### 6.7 Lock ordering: `deque` → `TransactionManager`

`RecordAccumulator.java:877–926` assigns sequences **inside**
`synchronized (deque)` while calling into `synchronized` `TransactionManager`
methods (`maybeUpdateProducerIdAndEpoch` 908, `sequenceNumber` 918,
`incrementSequenceNumber` 919, `addInFlightBatch` 924). So Java's lock order is
deque → TransactionManager, and it is never inverted.

Rust must preserve that order or deadlock. Good news: the whole block is
CPU-bound with no `await`, so `std::sync::Mutex` is correct (same reasoning as
`consumer-threading.md` §16 for `SubscriptionState`), and the Rust accumulator
already holds per-partition deques as
`DashMap<i32, Mutex<VecDeque<ProducerBatch>>>` (`record_accumulator.rs:126–131`).

### 6.8 `TreeSet` keyed on a mutable sort key

`TxnPartitionEntry.inflightBatchesBySequence` is a `TreeSet<ProducerBatch>`
ordered by `(producerId, producerEpoch, baseSequence)`
(`TxnPartitionEntry.java:62–65`) — **all three of which mutate** via
`resetProducerState`. Java copes by rebuilding the set in `resetSequenceNumbers`
(154–161); the comment at 58–61 records the bug that motivated the 3-key
comparator.

A direct Rust `BTreeSet<ProducerBatch>` whose `Ord` reads interior-mutable
fields violates `BTreeSet`'s invariants (logic corruption, not UB, but
unpredictable). Required: key on an **explicit snapshot tuple** —
`BTreeMap<(i64, i16, i32), ProducerBatch>` — and rebuild the map wherever Java
rebuilds the set.

Related, and already handled: `TxnPartitionEntry.incrementSequence` (104)
delegates to `DefaultRecordBatch.incrementSequence`
(`DefaultRecordBatch.java:557`), which **wraps at `Integer.MAX_VALUE`** rather
than using plain `+`. The Rust equivalents already exist —
`increment_sequence` at `src/common/record/default_record_batch.rs:887` and
`decrement_sequence` at 898 — so Phase 1 reuses them rather than adding them.

### 6.9 Test volume is the dominant cost

Implementation is ~3 400 Java LoC. Tests are ~**8 100** Java LoC across
`TransactionManagerTest` (4 487), the idempotence/txn share of `SenderTest`
(43/76 tests of 4 002 lines), `KafkaProducerTest` txn (27 tests of 2 952),
`MockProducerTest` txn (44 tests of 751), plus 695 lines of wrapper tests.
Roughly **70% of this milestone is test translation.** Any schedule that budgets
implementation only will be wrong by a factor of ~3.

### 6.10 `enable.idempotence` default is currently dishonest

See §7.1 — this is an open decision, not just a risk.

---

## 7. Open decisions for the user

### 7.1 The `enable.idempotence` honesty gap — **recommend: faithful config validation now + explicit failure for the unimplemented path, removed as each phase lands**

Facts established by reading both sides:

- `ProducerConfig::enable_idempotence` defaults to **`true`**
  (`producer_config.rs:204`) but is **never read** by `KafkaProducer`, `Sender`,
  or `RecordAccumulator` (zero reads confirmed). `transactional_id` (line 181)
  likewise. `INIT_PRODUCER_ID` has no use outside `api_keys.rs`. So the producer
  advertises idempotence and delivers at-least-once.
- Java's own behavior is subtler than "default true":
  `postProcessAndValidateIdempotenceConfigs` (`ProducerConfig.java:591–651`)
  **silently disables** idempotence when `retries == 0` or `acks != all`
  *if the user did not explicitly set `enable.idempotence`* (line 628), and
  raises `ConfigException` when the user *did* set it explicitly and the configs
  conflict. It also always rejects `max.in.flight > 5` with idempotence on,
  rejects `transactional.id` without idempotence, and rejects
  `transaction.timeout.ms` together with 2PC. **None of this validation exists in
  Rust** — `ProducerConfig::MAX_IN_FLIGHT_REQUESTS_FOR_IDEMPOTENCE` is declared
  at line 37/235 and never read.

Three options considered:

- **(A) Leave as-is until Phase 4.** Rejected: violates CLAUDE.md §5 for the
  several phases in between, and the config validation is a missing translation
  regardless.
- **(B) Honor the `true` default immediately by rejecting construction.** Rejected:
  the default is `true`, so *every* existing producer user and every existing
  producer test would fail at construction mid-milestone.
- **(C) Recommended.** In Phase 1: translate
  `postProcessAndValidateIdempotenceConfigs` **faithfully** into
  `ProducerConfig` (including the silent-disable arm, which is what keeps
  today's default-config users working and is what Java does), and add the
  missing `transaction.two.phase.commit.enable` key. Then, in
  `KafkaProducer::from_config`/`with_client` — *not* in the config layer — add a
  temporary guard: if the effective config requests idempotence **explicitly**
  or sets `transactional.id`, return
  `KafkaError::with_message(Errors::UnsupportedVersion, ..)` naming the
  unimplemented feature. Users who never touched the config get today's
  behavior (now with Java's silent-disable semantics and a `warn!`); users who
  explicitly ask get an honest error instead of silent non-compliance.

Why the guard belongs in `KafkaProducer` and not `ProducerConfig`: the config
translation should mirror Java exactly, so "not yet implemented" must not
pollute it. Removal is tracked as an explicit deliverable — the idempotence arm
in Phase 4, the transactional arm in Phase 6 — so nothing is left behind
(CLAUDE.md §5 forbids lingering TODOs).

**Decision needed:** confirm (C), or pick (A)/(B).

### 7.2 Bindings deferral — **recommend: defer, as the briefing proposes**

Concur. Rationale to record: it roughly doubles the phase count, and
transactions are the one producer surface where the Milestone-9 FFI access-guard
model needs genuinely new thinking. The guard is "one operation in flight" and
`wakeup()` bypasses it — but a *transaction* spans multiple FFI calls
(`begin` → N× `send` → `commit`). So the guard needs a story for an open
transaction: does it stay held from `begin_transaction` to
`commit_transaction` (which would block the `send` calls that must happen in
between), or does the FFI need transaction-state-aware admission control? That
is a design question, not a translation question, and it deserves its own
milestone rather than being improvised at the tail of this one.

**Decision needed:** confirm deferral. If overturned, add ~6 phases (producer
txn FFI, `producer.py` txn API, both gRPC servers, multilanguage scenarios).

### 7.3 New rules file — **recommend: yes, `.claude/rules/producer-transactions.md` in Phase 1**

See §5 for the six rules and the reasoning. It is cheap (one file, written once)
and it is the difference between a Critic being able to review §6.2/§6.5
correctly or not.

**Decision needed:** confirm, or say to fold these notes into the per-phase
`PLAN.md` files instead (weaker: phase plans are not loaded into agent context
the way `.claude/rules/` files are).

### 7.4 KIP-939 two-phase commit — **recommend: keep in scope, Phase 5, but split it out if Phase 5 overruns**

It is small (one state, `prepareTransaction`, `keepPreparedTxn`,
`preparedTxnState`, `isPrepared`, `is2PCEnabled`, plus the
`throwIfInPreparedState` guards at `KafkaProducer.java:968/989`) and it is
entangled with the state table we are writing anyway (`PREPARED_TRANSACTION`
appears in three transition arms). Deferring it means editing the state machine
twice. Keep it — but it is the natural thing to cut if Phase 5 needs to shrink.

**Decision needed:** confirm in-scope, or defer to a later milestone.

### 7.5 Scope calls found while reading the source — **recommend: accept all three**

Discovered during planning, not in the briefing:

1. **`WriteTxnMarkers` request/response: out of scope.** Admin/coordinator RPC,
   not producer (§1.1). Its two Java test files
   (`WriteTxnMarkersRequestTest`, `WriteTxnMarkersResponseTest`) go out of scope
   with it — justified under DoD §3 as belonging to an untranslated Admin
   surface, not as "irrelevant to Rust."
2. **`EndTransactionMarker`: out of scope.** Broker-side control-record write
   path (§1.1).
3. **`MockProducer` transactional surface: *in* scope**, Phase 7 — 44 named
   skipped tests at `mock_producer.rs:525–577`. This is additional scope beyond
   the briefing and is required by DoD §3.

**Decision needed:** confirm.

---

## 8. Effort estimate

Relative sizing, based on Java LoC actually counted, Java test-method counts
actually counted, and the number of Rust insertion points actually located.
"Units" are comparable to one another, not to wall-clock.

| Phase | N | Impl | Test | Total | Basis |
|---|---|---|---|---|---|
| 1 | 41 | 2 | 1 | **3** | 350 Java LoC, mostly mechanical. `TransactionalRequestResult` is the only subtle part (§6.4). `TxnPartitionEntry`/`Map` have **no dedicated Java test file** — covered indirectly via `TransactionManagerTest`, so Phase 1's own tests are `ProducerConfigTest`'s idempotence cases plus Rust-side unit tests for the two maps. |
| 2 | 42 | 4 | 3 | **7** | 1 030 Java LoC across 10 files + 18 exhaustive match sites + 695 lines of wrapper tests. High volume, low risk — the pattern is fully established by 12 existing pairs. |
| 3 | 43 | 5 | 2 | **7** | The idempotence slice: 4 states, 1 handler, ~25 methods. Only ~15–20 test methods available (§2). Risk concentrated in the `Caller` enum (§6.2). |
| 4 | 44 | 6 | 5 | **11** | Small diff, high blast radius: 10 insertion points across `sender.rs`/`record_accumulator.rs`, the constructor re-ordering of §6.3, plus lock-ordering (§6.7) and hot-path allocation (CLAUDE.md §11 — sequence assignment runs per batch on the drain path). Test side is the idempotence share of `SenderTest` + `RecordAccumulatorTest`. |
| 5 | 45 | 9 | 12 | **21** | **Largest phase.** Remaining 5 states, 5 handlers, priority queue, `PendingStateTransition`, TV2 forks, 2PC — against the bulk of a 4 487-line test file with a `transactionV2Enabled` matrix. Candidate for a 5a/5b split. |
| 6 | 46 | 5 | 4 | **9** | Public API is small, but `maybeSendAndPollTransactionalRequest` is the milestone's hardest async translation (§6.6), and 27 `KafkaProducerTest` tests. |
| 7 | 47 | 2 | 4 | **6** | Pure in-memory state; 44 test methods. Low risk, high test count. |
| 8 | 48 | 1 | 6 | **7** | Test-only sweep plus new broker integration tests. |
| | | **34** | **37** | **71** | |

Two things this estimate is asserting, both worth challenging at approval:

- **Testing is ~52% of total effort** (37/71), and Phase 5 alone is ~30% of the
  milestone. If Phase 5's estimate is wrong, the milestone's estimate is wrong.
- **Phase 4 is under-weighted by LoC and over-weighted by risk.** It is the
  smallest diff of any implementation phase but touches the producer's hottest
  path and its most load-bearing constructor. Historically (Milestone 8) this
  shape of phase generated the most Critic cycles.

Comparison anchor: Milestone 8 required 40 phase-numbers for the consumer
(~1 000 Java test methods). This milestone is roughly one-fifth that scale.

---

## 9. Follow-ups (deferred work, tracked)

Items discovered or decided during Milestone 11 that are deliberately **not**
done inside it. Each records what, why deferred, and how to verify the fix.

### 9.1 Code generator omits Java's non-default-at-unsupported-version guard

**Status:** open. Found in Phase 2 while translating
`RequestResponseTest.testInitProducerIdRequestVersions`.

For a version-gated non-tagged field, Java's generated `write` emits two halves:

```java
if (_version >= 3) { _writable.writeLong(producerId); }
else if (producerId != -1) {
    throw new UnsupportedVersionException(
        "Attempted to write a non-default producerId at version " + _version);
}
```

This project's generator emits only the first. A non-default value at an
unsupported version is therefore **silently dropped** rather than rejected: Java
refuses to encode a message it cannot represent faithfully, while this port
encodes a valid-but-different message and reports nothing. The wire result is a
well-formed *older* request missing the caller's value, so the broker accepts it
— there is no error anywhere in the path.

**Scope:** systemic. Affects every version-gated non-tagged field across all 197
generated message types, not only `InitProducerIdRequest`.

**Severity:** a missing safety net rather than a live fault. The bad branch is
only reached when client code sets a field without checking the negotiated
version — itself a programming error. Java converts that error into an
exception; here it becomes silent wire divergence. Worth fixing precisely
because this milestone's guarantee (no duplicate records) depends on
`producerId` reaching the broker.

**Fix location:** `generator/src/lib.rs`, the field-write emission function
(the `has_version_check` block around lines 1176-1187). Three pieces needed:

  1. The "is this field at its default?" condition. `get_default_check`
     (line ~1645) is close but was written for *tagged*-field semantics
     ("should this be written?"), so it needs adapting rather than reusing
     as-is.
  2. The original spec field name for the message text — Java's wording is
     `producerId`, not `producer_id`.
  3. Emission of the `else if` branch where the code currently just closes the
     version `if`.

Estimated ~20-30 lines in one function.

**Unknown, and the reason this is deferred:** enabling the check regenerates all
197 message types with a new error path. Any existing code that sets a field and
then serializes at a lower version starts failing. That count cannot be derived
by reading — it must be measured by making the change locally and running the
suite (~1 hour).

**Test coverage:** exactly **one** Java test in the whole `clients` module
asserts this behaviour (`RequestResponseTest.testInitProducerIdRequestVersions`),
and it covers `InitProducerId`. Verified: none of the other Phase 2 pairs'
dedicated test files contain such an assertion, so no further phases will
surface additional skips from this gap.

#### Refinement (found in Phase 2, `TxnOffsetCommit`): the check applies only to
#### **non-ignorable** fields

Java's generator emits the non-default-at-unsupported-version check **only** when
the field is not marked ignorable — `MessageDataGenerator.java:792`:

    if (!field.ignorable()) {
        cond.ifNotMember(__ -> {
            field.generateNonIgnorableFieldCheck(...);
        });
    }

So the spec's `"ignorable": true` flag is load-bearing, and the two cases differ:

| Field | `ignorable` | Java | Rust today |
|---|---|---|---|
| `InitProducerIdRequest.ProducerId` (v3+) | absent | **throws** | silently drops ← the gap |
| `TxnOffsetCommitRequest.CommittedLeaderEpoch` (v2+) | `true` | silently drops | silently drops ← **correct** |

This materially narrows the fix: **the generator must consult the `ignorable`
flag, not add the check unconditionally.** Adding it everywhere would start
rejecting legitimate ignorable-field drops that Java accepts — turning a
missing-error bug into a spurious-error bug, across all 197 message types.

Discovered by an all-versions round-trip test in
`txn_offset_commit_request.rs`, which initially failed because it asserted the
leader epoch survived at v0/v1. The expectation was wrong, not the code; the test
is now version-aware and documents why.

**How to verify the fix:** remove the `#[ignore]` from
`test_init_producer_id_request_versions` in
`src/common/requests/init_producer_id_request.rs`. The assertion is already
correct and will pass once the generator is fixed. Do **not** weaken it to match
current behaviour.

**Do NOT bundle this into a transactions phase.** It touches every message type;
mixing it with transaction work makes both changes hard to review and a failure
ambiguous between the two.

### 9.2 Migrate the Java base from 4.2.0 to 4.3.1

**Status:** decided — do it **after** Milestone 11 completes, as its own work.

Evaluated in full during this milestone. There are **no functional changes to
transactions or idempotence** in 4.3.1: the wire protocol, the sequence-number
logic, the state machine table, the class inventory, and the test counts
(122/75/77/55) are all identical. What changed:

  - `TransactionalRequestResult.await()` loses its no-arg overload; the timed
    overload gains a third `expectedTimeoutReason` parameter appended to the
    timeout message. **44 call sites** across `TransactionManagerTest`,
    `SenderTest`, `KafkaProducerTest`, and `KafkaProducer`.
  - `KafkaProducer.throwIfInPreparedState()` deleted as redundant — the manager
    already rejected both guarded operations (invalid `PREPARED_TRANSACTION →
    IN_TRANSACTION` transition; `maybeAddPartition`'s `currentState !=
    IN_TRANSACTION` check). Operations stay rejected; only the message differs.
  - `throwIfPendingState(String)` → `throwIfPendingState(TransactionOperation)`,
    a new 4-value private enum. Messages byte-identical.
  - Four timeout-reason constants added to `KafkaProducer`; one to `Sender` for
    expired batches.
  - `MockProducer`: javadoc only.
  - `ProducerConfig`: import + javadoc only — **Phase 1 commit 8 is unaffected**.

**Cost of deferring: ~1-1.5 days**, roughly 2% of the milestone. Low risk:
deleting the no-arg method and adding a parameter makes **every** call site a
compile error, so none can be missed.

**Why after, not before:** test parity gets proven against one stable base
first. If something breaks post-migration, the cause is unambiguous.

**Accept:** Java line-number citations in commits and
`.claude/rules/producer-transactions.md` are against 4.2.0. `TransactionManager`
references shift by **+18** at 4.3.1 (`shouldPoisonStateOnInvalidTransition`
287→305, `handleFailedBatch` 788→806). `TxnPartitionEntry` is unaffected (163
unchanged). The rules file records its base version to keep citations
unambiguous.

### 9.3 `record` → `record.internal` visibility demotion

**Status:** open, and **independent of transactions** — do not fold into 9.2.

4.3.1 moved the entire `org.apache.kafka.common.record` package to
`record.internal` (35 classes, ~7 500 lines; 83 client files re-imported). Only
`TimestampType` remained public.

CLAUDE.md §2 requires classes in an `internal` package to be `pub(crate)`.
`src/common/record/` is currently `pub mod record` with **13 public
re-exports**, so a faithful 4.3.1 base makes this a **breaking change to this
crate's public API**. Blast radius in Rust today: 32 files, 100 reference lines.

Verified safe: the C FFI does **not** expose these types. The `RecordBatch`
matches in `src/ffi/producer.rs` are its own unrelated local structs
(`RecordBatchCompletion`, `RecordBatchCallbackTarget`), so the C and Python
bindings are unaffected.

Deserves its own review because it is a public-API break unrelated to either
transactions or the 4.3.1 semantic changes.

### 9.4 Remove the `MILESTONE-11 GUARD`

**Status:** open, already scheduled inside this milestone.

`KafkaProducer::from_config` rejects explicit `enable.idempotence=true` and any
`transactional.id` (`src/producer/kafka_producer.rs`, marked `MILESTONE-11
GUARD:`). Remove the idempotence arm in **Phase 4** and the transactional arm in
**Phase 6**, deleting the corresponding `test_guard_*` tests in the same commit.

### 9.5 Critic review of Phase 1

**Status:** DONE — review loop **closed** 2026-08-04. Three Critic passes per
`agent-roles.md` steps 2-6, converged clean on pass 3. Archived at
`design/history/Milestone-11/Phase-1/COMMENTS.DONE.41.md`; one partial false
positive in `COMMENTS.FP.md`.

| Pass | Findings | Fixes |
|---|---|---|
| 1 | 8 | `0a8612e` (7 fixed) + `6aeb30c` (validation); 1 partially rejected |
| 2 | 1 | `363a7c7` |
| 3 | **0** | — signed off |

**Pass 1's material finding:** `TxnPartitionEntry::reset_sequence_numbers` took
its tracked in-flight membership from the caller's slice rather than its own set,
so a short slice silently cleared tracking and rewound the partition's sequence
counter.

**Pass 2 caught the fix being wrong twice over.** The pass-1 fix added a
missing-batch error justified by an audit claiming the state was unreachable. The
audit was wrong: `Sender.reenqueueBatch` (`Sender.java:750-752`) does not call
`transactionManager.removeInFlightBatch` — unlike the split path at `:685` — so a
retried batch leaves the Sender's map while **staying tracked**, and Java asserts
this at `RecordAccumulator.java:558-560`. Erroring there would have broken
idempotent recovery, which is the very path `bumpIdempotentProducerEpoch` →
`startSequencesAtBeginning` (`:655`) serves. Rules §7's "sole owner" claim was
therefore false and is corrected: **ownership alternates**, and Phase 4 must
assemble the batch pool from both the Sender's map and the accumulator's deque.

Writing that regression test then exposed a *second* defect in the pass-1 fix —
it resolved and mutated in one pass, leaving already-visited batches rewritten
when a later key was missing. Now resolves every key before mutating anything.

**Two rules-file additions came out of the loop:** §9 (the
`UnknownProducerId <: OutOfOrderSequence` subtype relation that flat error codes
lose) and §11 (version-gated field checks apply only to non-`ignorable` fields).

**Still open from this item, and inherent rather than a gap:** four of the five
Phase 1 types have no *Java-parity* test coverage, because Kafka has no test file
for any of them. Fidelity rests on source reading plus Rust-authored tests until
`TransactionManagerTest` lands in Phases 3/5. The six translated `ProducerConfig`
tests are the exception and pass. The Critic verified both Phase 1 plan overrides
(§6.8) as correct with the plan wrong.

**Lesson for later phases:** two of the three passes found a real defect, and the
second one was in the *fix* for the first. Do not treat a single Critic pass as
sufficient — `agent-roles.md` step 6 loops back to step 2 for a reason.

### 9.6 C FFI / Python / gRPC multilanguage harness for transactions

**Status:** deferred to a follow-up milestone (§7.2).

Beyond the phase-count cost, the Milestone 9 FFI access-guard model has no story
for an *open* transaction: a transaction spans `begin` → N×`send` → `commit`,
while the binding is one-operation-in-flight. Needs design, not a mechanical
extension.

---

## 10. Recorded translation deviations

`definition-of-done.md` §7 requires every deviation from the Java source to be
justified. Gathered here so a reviewer has one list to check rather than hunting
through commit messages. Each is also documented at its own call site.

### 10.1 `HashMap` iteration → sorted output, wherever Java's order is unspecified

**Rule: where Java groups wire data through a `HashMap` and then serialises it,
the Rust translation sorts before emitting.**

Java's `HashMap` has unspecified iteration order, so the *byte encoding* it
produces for a given logical value is not stable across runs. That is invisible
in Java because the broker treats these collections as sets. It is a problem
here for two reasons:

  1. `definition-of-done.md` §3 requires byte-level wire tests against known
     vectors, not only round-trips. A test cannot assert on bytes whose order is
     nondeterministic.
  2. A nondeterministic encoding makes any future byte-diff against the Java
     client — the most direct way to prove wire compatibility — impossible.

Sorting produces the same logical value with a stable encoding, so it is
behaviour-preserving on the wire while being strictly more testable.

**Applied so far (Phase 2):**

| Site | Sorted by |
|---|---|
| `AddPartitionsToTxnRequestBuilder::build_txn_topic_collection` | topic name |
| `AddPartitionsToTxnResponse::topic_collection_for_errors` | topic name, then partition index |

**How to apply going forward:** any `HashMap`/`HashSet`-grouped collection that
reaches a `write()` gets a deterministic order, and the reason is noted at the
site. `TxnOffsetCommitRequest` (Phase 2) groups offsets by topic the same way and
must follow this. Do NOT sort collections that are *not* serialised — there the
extra work buys nothing.

**Not applicable to** collections whose order Java specifies, or where order is
semantically meaningful (in-flight batch ordering by sequence, for instance,
which is already a `BTreeSet` for exactly that reason).

### 10.2 Broker-side members omitted from translated request classes

`AddPartitionsToTxnRequest` in Java carries `Builder.forBroker`,
`normalizeRequest`, `allVerifyOnlyRequest`, `partitionsByTransaction`, and
`errorResponseForTransaction`. All five construct or inspect a *received* v4+
request, which only a broker does — verified by caller analysis over the whole
Kafka tree: `core/.../KafkaApis.scala` and
`server/.../AddPartitionsToTxnManager.java` only, nothing under
`clients/src/main`.

Verified callers, none under `clients/src/main`: `forBroker` →
`AddPartitionsToTxnManager.java:343`; `normalizeRequest` → `KafkaApis.scala:1852`;
`partitionsByTransaction` → `:1857`, `:1895`; `errorResponseForTransaction` →
`:1899`, `:1938`; `allVerifyOnlyRequest` → `RequestChannel.scala:228` — a third
file the original write-up omitted (Critic 42 finding 5).

Omitted, consistent with §1.1's exclusion of `WriteTxnMarkers` and
`EndTransactionMarker` from this client-only port. Translating them would add
permanently unreachable code that `#![deny(warnings)]` forces us to mask with
`#[allow(dead_code)]`, which then hides genuinely dead code later.

The corresponding Java tests (`testBatchedRequests`, `testNormalizeRequest`, and
the `version >= 4` half of `testConstructor`) are skipped for the same reason,
recorded in a comment block in the Rust test module per DoD §3.

### 10.3 Java `throws` on a missing lookup → `Option` / `Result`

`AddPartitionsToTxnResponse::get_transaction_topic_results` returns
`Option<&[..]>`. Java calls `find(..)` and dereferences the result, throwing on a
missing transactional id; a Rust caller cannot catch that, so the absence is made
explicit in the return type. Same treatment as `TxnPartitionMap::get`, which
returns `Result` where Java throws `IllegalStateException` (CLAUDE.md §10.2).

### 10.4 Phase 1 deviations (cross-reference)

Recorded in full elsewhere; listed here for completeness:

  - `TxnPartitionEntry` stores ordering keys rather than owning `ProducerBatch` —
    this document's header and `.claude/rules/producer-transactions.md` §7.
  - `TxnPartitionEntry::decrement_sequence` does not wrap — rules §8.
  - `TransactionalRequestResult` composes `Notify` + `AtomicBool` in place of
    `CountDownLatch` — rules §5.
  - `TxnPartitionMap::get_mut` has no Java counterpart (Java references are
    implicitly mutable) — commit `90732c4`.
  - `ProducerConfig::explicitly_set` replaces Java's
    `AbstractConfig.originals()` — commit `d14d1ec`.
  - The five typed txn error structs deliberately not created — §1.1 and rules §9.
