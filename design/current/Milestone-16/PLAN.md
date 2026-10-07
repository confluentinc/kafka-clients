# Milestone 16 — Bring the Rust client up to Apache Kafka 4.4

**Status:** DRAFT 2026-10-07 — awaiting user approval. Phases run through the Manager → Actor → Critic loop.

**Branch:** `milestone-16-ak-4.4` (off `master`). If tracks run in parallel (§4), each track gets its own
branch/worktree off the milestone branch and is merged back with a merge commit — never rebase + force-push.

**Java source:** Apache Kafka **4.4.0-rc4** (`1156b2752a`, tagged 2026-10-06). `4.4.0-rc3..4.4.0-rc4` has
**no** change under `clients/src/main`, so the repo's existing `AUDIENCE_REF` / `DEPRECATION_REFS`
(`4.4.0-rc3`, `rust/xtask/src/java.rs`) stay valid. Phase 13 re-diffs against `4.4.0` final once it is tagged.

**Agent numbers:** 90–103 (Phase N → agent 90+N). The highest number used so far is 86.

## 1. What this milestone delivers

It translates the 4.3.1 → 4.4 Java clients delta for every Java file that has a Rust counterpart. It also
syncs `rust/generator/messages/` to the 4.4 specs, adds the new public API to the C and Python bindings,
and moves the `kafka/` submodule and the docs' source-reference line to 4.4.

**Measured delta** (`git -C kafka log --no-merges --right-only --cherry-pick 4.3.1...4.4.0-rc4`):

- 848 unique commits overall. **190 touch `clients/src`** (168 touch main code). On the M13 metric
  (`-- clients/src/main/java/org/apache/kafka/clients`) there are 116 commits, against 81 for 4.2.0 → 4.3.1.
- **68 commits to port**: 55 behaviour/API/wire (P) and 13 test-only (T). One more, the trunk-only
  KAFKA-20864, is added by decision D3.
- The rest:
  - **56 out of scope:** Streams 24, broker/server 13, Share 11, Classic 5, untranslated areas 3.
  - **37 no-op for Rust.**
  - **21 doc/log-only.**
  - **6 reverted inside the range:** KAFKA-20385/20684 RebalanceListener (it returns in 4.5), and 2PC.
  - **2 already ported in M13**, because they were backported to 4.3.1 under different hashes:
    KAFKA-20426 `44bafc60e7` and KAFKA-20428 `4b9eddc132`.
- `rust/generator/messages/` currently matches 4.3.1 **exactly**: 18 spec files differ from 4.4, 5 of them new.

The per-commit classification table is §6. Phase 13 completes it into the formal audit.

### 1.1 Scope decisions

- **Skip untranslated areas**, as M13 did: Share (KIP-932), Streams-protocol consumer internals and Streams
  admin (`describeStreamsGroups` topology description), the Classic consumer, broker/server code
  (authorizer, quota callback, policy, OAuth broker validator, `FileRecords`), the Raft-voter admin API
  (`RaftVoterEndpoint`), `ListDeserializer`, config providers (`AllowedPaths`), `TopicConfig` constants, and
  telemetry.
- **Include KIP-1332 incremental buffer allocation** (KAFKA-20578), user-confirmed 2026-10-07, with the type
  mapping in §2.3.
- **The KIP-1265 `@InterfaceAudience` annotations are already applied** (`AUDIENCE_REF = 4.4.0-rc3`). This
  milestone adds no audience work beyond keeping the lint green for the new public types.
- **The CLAUDE.md "Source Reference" line edit (4.3.1 → 4.4.0) is human-approved through this plan**, as in
  M13. Only that line changes. Any other CLAUDE.md or rules change goes through the suggestion process
  (drafts go in `design/current/Milestone-16/rules-errata.md`).

### 1.2 Method: per-subsystem tree diff, with the commit list as a checklist

As in M13, Actors translate from the tree diff (`git -C kafka diff 4.3.1 4.4.0-rc4 -- <files>`), not by
replaying commits one by one. Each phase lists its KAFKA-ids so the Actor has the rationale and the Critic
can audit completeness. Every Java hunk in a phase's files is either applied or recorded as a skip with a
reason in that phase's completion notes.

### 1.3 Open decisions (defaults used by this plan unless the user overrides)

| # | Decision | Default |
|---|---|---|
| D1 | Target | Start on **4.4.0-rc4** now; Phase 13 re-diffs and bumps refs to **4.4.0 final**. |
| D2 | Java's `common.utils` → `common.utils.internals` package moves (KAFKA-20297) | **Mirror them** (M13 precedent: `record` → `record::internal`). In Rust this affects only `byte_utils`, `exponential_backoff`, `log_context`, `producer_id_and_epoch`. |
| D3 | KAFKA-20864 (trunk-only fix to the 4.4 incremental-allocation code: closing the wrong batch on extension failure, unbounded retries) | **Include it** as a recorded deviation ahead of 4.4. Otherwise the 4.4 bug would be ported faithfully. |
| D4 | Representing `UnsupportedProtocolFieldException` (crate-private, `extends UnsupportedVersionException`) | Model it as a crate-private kind on the existing `UnsupportedVersion` variant, so the public `Error` surface still matches Java's public view (§2.4). |

## 2. Key findings that shape the phases

### 2.1 KIP-909 (async bootstrap DNS) is the riskiest change

KAFKA-14648 with its follow-ups (KAFKA-20939, the KIP-909 follow-ups) adds:
- `bootstrap.resolve.timeout.ms` and `BootstrapConfiguration`;
- `BootstrapResolutionException`;
- lazy bootstrap in `NetworkClient` / `MetadataUpdater` (`bootstrap`, `isBootstrapped`, `bootstrapFailed`);
- `Metadata.bootstrapFatalError` / `maybeThrowBootstrapFatalException`;
- `AdminMetadataManager` changes, and the removal of `AdminBootstrapAddresses`.

Java needed three follow-up fixes for consumer busy loops it caused (KAFKA-20854/21010/20970).

- **Default:** `0` keeps today's synchronous resolve-at-construction behaviour. A positive value switches
  to asynchronous resolution, and the setting is marked experimental.
- **Rust specifics:** `ClientUtils::resolve` is already `async`. The new risk is Rust-only: async resolution
  must never sit inside a `select!` arm (CLAUDE.md §11.6, consumer-threading §10). The consumer busy-loop
  fixes must be checked against the Rust `maximum_time_to_wait` / poll-timer logic, not just transcribed.

### 2.2 TxnOffsetCommit v6 must not go live before the producer can fill it

KIP-1319 v6 swaps topic **names** for topic **IDs** in TxnOffsetCommit. If the v6 spec were synced in
Phase 0 while the builder still populated names, the client would negotiate v6 and send requests without
topic IDs.

**Rule:** Phase 0 syncs every spec **except** `TxnOffsetCommitRequest.json` / `TxnOffsetCommitResponse.json`.
Those are synced in Phase 5, together with the builder and `TransactionManager` changes. The builder's
`latest_allowed_version` follows Java's `super(...)` call (producer-transactions.md §12).

`DeleteGroups` v3, `ApiVersions` v5 and `DescribeProducers` (`mapKey`) are safe to sync in Phase 0: their
new fields are ignorable, or default to null/-1.

### 2.3 KIP-1332: how Java's subclasses map to Rust

Java adds `ChunkedRecordAccumulator extends RecordAccumulator` and `ChunkedProducerBatch extends ProducerBatch`.
Rust has no inheritance, so each is mapped separately:

- **`ChunkedRecordAccumulator`: a real struct, by composition.** This follows the existing precedent
  `ConsumerHeartbeatRequestManager { inner: AbstractHeartbeatRequestManager }`.
  - The struct is `ChunkedRecordAccumulator { base: Arc<RecordAccumulator>, .. }` in
    `chunked_record_accumulator.rs`, with its own `append` / `try_append`.
  - `Sender` keeps `Arc<RecordAccumulator>` and does not change. Its accumulator calls (`ready`, `drain`,
    expiry, `reenqueue`, `deallocate`) are all inherited unchanged in Java. In production, `append` is
    called only from `kafka_producer.rs` (around `:1861`), and that one site dispatches on the strategy.
  - Java's virtual `tryAppend` / `createProducerBatch` inside the base `appendNewBatch` become parameters
    of the base `append_new_batch`.
- **`ChunkedProducerBatch`: folded into `ProducerBatch` (a justified deviation, recorded per DoD #7).**
  - One partition deque holds both kinds of batch: split batches stay plain even in incremental mode.
  - `ProducerBatch` is not `Clone` and moves by value between the accumulator, `Sender` and
    `TransactionManager` (producer-transactions.md §7).
  - So `ProducerBatch` carries a single-or-chunked buffer. The Java methods live in
    `chunked_producer_batch.rs` as an `impl ProducerBatch` block, and `instanceof ChunkedProducerBatch`
    becomes `batch.is_chunked()`.
- **The builder's buffer abstraction** (a single `Vec<u8>` or a `ChunkedByteBufferOutputStream`) is the one
  new Rust type. It matches Java 4.5's abstract `ByteBufferOutputStream` / `SingleByteBufferOutputStream`
  (KAFKA-20807), so the shape is already right for the next bump.
- **Flatten copy:** the Rust builder already copies once at close (`take_batch_data`, a documented §14
  deviation). The chunked path makes that same single copy, so it is no regression. Do **not** send
  straight from the chunks with `IoSlice` yet: that is Java's future KAFKA-20580, and doing it now would
  change when chunks can be returned to the pool.
- **Rust-only hazards:**
  - A chunk `Vec` must never grow. Java's fixed-capacity `ByteBuffer` throws on overflow, whereas a Rust
    `Vec` would silently break the memory accounting.
  - A cancelled `append` future must refund new-batch and extension chunks (extend `AppendGuard`).
  - `reopen_and_rewrite_producer_state` must rewrite the header of the flattened buffer.
- **Draft a rules note** (in `rules-errata.md`, applied by a human) recording the batch fold, so Critics
  don't flag the missing type.

### 2.4 `UnsupportedVersionException` gains a subclass

KAFKA-18157 adds `common.internals.UnsupportedProtocolFieldException extends UnsupportedVersionException`.
About 15 request builders throw it, and `ConsumerHeartbeatRequestManager` tells it apart from its parent.

- The class is not public. Under D4 the public `Error` keeps one `UnsupportedVersion` variant, with a
  crate-private kind that the heartbeat manager checks.
- The Phase 1 Critic must confirm whether CLAUDE.md §12.4 then requires an `is_unsupported_version_error()`
  predicate (plus a test in both directions and the C FFI twin). If the hidden-kind design is rejected,
  the predicate becomes mandatory.

### 2.5 New public API → bindings

| Item | Rust | C FFI | Python |
|---|---|---|---|
| `Admin.unregisterController(int[, options])` + `UnregisterControllerOptions/Result` | `admin` | sync + async | `admin.py` sync + asyncio |
| `MockConsumer.losePartitions` | `consumer::MockConsumer` | `kafka_consumer_MockConsumer_lose_partitions` | `consumer.py` mock |
| Errors 134 `GROUP_DELETION_FAILED`, 135 `STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED`, 136 `CONTROLLER_ID_NOT_REGISTERED`; `BootstrapResolutionException` (non-wire) | `common::errors` | error code / predicates as applicable | `_error_code.py` |
| Configs: `bootstrap.resolve.timeout.ms`, `metadata.cluster.check.enable`, `partitioner.rack.aware`, producer `client.rack`, internal `buffer.memory.allocation.strategy` | config structs | pass-through | pass-through |

Each binding change lands in the phase that adds the Rust API.

The configs must be wired end to end, not just parsed (see the 2026-09-30 config audit,
`design/current/config-audit-master-7ac1391b.md`: silently ignored configs were the main finding class).

## 3. Phases

Every phase works the same way:
- The Actor (agent 90+N) implements and commits step by step.
- The Critic (agent 90+N) reviews into `COMMENTS.9N.md` (`COMMENTS.10N.md` for N ≥ 10).
- The Actor fixes, moves the items to `COMMENTS.DONE.*`, and the loop repeats until the review is clean.
- Gates for every phase: `cargo build`, `cargo test`, `cargo xtask format-check`, `cargo xtask lint`; then
  `make verify` before the phase closes.

### Phase 0 — Reference bump, spec corpus, errors, bookkeeping (agent 90)

- Move the `kafka/` submodule to `4.4.0-rc4` (`1156b2752a`) and commit the gitlink.
- Sync `rust/generator/messages/` from `kafka/clients/src/main/resources/common/message/`: 16 of the 18
  changed files, holding back the TxnOffsetCommit pair (§2.2).
  - New: `UnregisterController{Request,Response}`, `StreamsGroupTopologyDescriptionUpdate{Request,Response}`,
    `AbortedTxn`.
  - Modified: `ApiVersions` v5, `DeleteGroups` v3, `DescribeProducersRequest` `mapKey`,
    `ConsumerGroupHeartbeatResponse`, `ShareGroupHeartbeatResponse`, and the Streams specs.
  - Regenerate, then fix compile fallout. Expected: `DescribeProducersHandler` (keyed collection) and the
    `ApiKeys` / `ConcreteRequest` / `ConcreteResponse` match arms for the two new APIs (admin-client.md §7).
    Only UnregisterController gets a real wrapper, in Phase 12. StreamsGroupTopologyDescriptionUpdate gets
    only whatever minimal wiring the existing Streams APIs have, and that has to be checked.
- `Errors`:
  - add codes 134–136 and the three error types (`GroupDeletionFailedError`,
    `StreamsTopologyDescriptionUpdateFailedError`, `ControllerIdNotRegisteredError`);
  - add `BootstrapResolutionError` (non-wire, needed by Phase 2);
  - change the `FENCED_STATE_EPOCH` message text (8ba75d04cb);
  - update `python/_error_code.py` and any C-side code list.
  - The full-error-code hierarchy tests (`test_retriable_errors_match_java_hierarchy` and its siblings)
    must cover the new codes.
- Update `README.md` and the `CLAUDE.md:93` source line "(Apache Kafka 4.3.1)" → "(Apache Kafka 4.4.0)".
- Rules errata survey: re-check the Java `file:line` citations in `.claude/rules/*.md` against 4.4, chiefly
  `producer-transactions.md` (TransactionManager moves under KIP-1319) and `consumer-threading.md`. Write
  the list to `design/current/Milestone-16/rules-errata.md`.
- Commits covered: 7997c9ebe0 (spec + errors portion only; the Streams RPC itself is out of scope),
  c274a7348f (spec + error portion), 8ba75d04cb, 04c3c00f25, 89ccd6a126 / 6be48c6e54 / 70dfc4236c
  (spec-only syncs).

Phase 0 completion notes (agent 90):

- **Submodule:** `kafka/` gitlink moved to `4.4.0-rc4` (`1156b2752a`).
- **Spec sync:** 16 of the 18 differing specs copied into `rust/generator/messages/`; the corpus now
  differs from 4.4.0-rc4 only in `TxnOffsetCommitRequest.json` / `TxnOffsetCommitResponse.json`, held
  back for Phase 5 (§2.2).
  - **No hand-written compile fallout.** The Rust generator emits every collection as a `Vec` (it parses
    `mapKey` into `StructSpec::has_keys` but never emits a keyed collection), so the
    `DescribeProducersRequest` `mapKey` change does not alter the generated shape and
    `DescribeProducersHandler` is untouched. Java's handler change there is the `CollectionUtils`
    removal (a1c155ee28), a refactor the Rust `HashMap` grouping already matches.
  - **New APIs:** `UnregisterController` (94) and `StreamsGroupTopologyDescriptionUpdate` (93) get the
    wiring the untranslated Streams/Share/broker APIs already have: `ApiKeys` constants
    (`UNREGISTER_CONTROLLER` is `forwardable`, per `ApiKeys.java:142`), entries in `ApiKeys::ALL`, and
    `assert_message_version!` rows in `message_test.rs`. No `ConcreteRequest`/`ConcreteResponse`
    variant: both fall through to the existing "not currently handled" arm, as `STREAMS_GROUP_HEARTBEAT`
    does. **Phase 12** adds the UnregisterController variants and wrappers.
  - `AbortedTxn` is a broker-side data spec; the generated `AbortedTxnData` has no client caller.
  - **Byte-level tests** (DoD #3): ApiVersions request v5 (defaults, both fields set, v4 drops them,
    v5 parse), response v4 == v5; DeleteGroups request v3 == v2, response v3 (message set, null, and
    dropped at v2). Also translated `RequestResponseTest.testDeleteGroupsResponseV3PreservesErrorMessage`.
- **Errors:** 134 `GroupDeletionFailedError`, 135 `StreamsTopologyDescriptionUpdateFailedError`,
  136 `ControllerIdNotRegisteredError` (all `extends ApiException`, not retriable, not fatal) and the
  non-wire `BootstrapResolutionError` (`extends KafkaException`, so only `is_kafka_error`). Each has its
  own file and an `Error` variant. `FENCED_STATE_EPOCH` now carries Java's generic text (8ba75d04cb).
  No new intermediate Java class, so no new hierarchy predicate.
  - **Tests:** every full-code walk now covers `-1..=136` (138 constants), and the FFI tables cover 166
    codes. `test_errors_added_or_reworded_in_kafka_4_4` pins the exact messages.
  - **Bindings:** `kafka_common_ErrorCode_t` gains 134-136 and `BOOTSTRAP_RESOLUTION = -29`, appended at
    the most-negative end because the negatives are ABI. `python/_error_code.py` and
    `rust/tests/common/error_code.rs` are regenerated from it, and the multilanguage decoder maps -29.
- **Docs:** `CLAUDE.md` Source Reference line and `rust/README.md` now say 4.4.0. The repository-root
  `README.md` names no Kafka version, so it has nothing to change.
- **Rules errata:** `design/current/Milestone-16/rules-errata.md`. 46 citations: 36 drifted, 10
  unchanged, and none whose rule claim breaks. It also lists 4.4 changes to code the rules reason about
  without a line cite: KIP-1319 vs producer-transactions §10-§12 (relevant to **Phase 5**), the
  MockAdminClient move and its new in-memory methods vs admin-client §9 (**Phase 12**), and KIP-909
  AdminMetadataManager state vs admin-client §3 (**Phase 2**). It reserves a section for the §2.3
  KIP-1332 note (**Phases 7/8**).
- **Unplanned fallout of the submodule bump: the custom lint.** `cargo xtask lint-custom` resolves every
  `#[doc(alias = "org.apache.kafka...")]` marker against the `kafka/` working tree, so the bump produced
  158 findings. Phase 0 fixed the 113 that are pure relocations, each true at 4.4 on its own:
  - 101 `KafkaAdminClientTest` markers re-pointed to the per-domain classes of 1a443b2d23. **Phase 12:**
    the mapping is done.
  - xtask now also indexes `clients/src/testFixtures` (3b6c8385ca moved `MockTime` and
    `MockAdminClient` there).
  - `Node`'s public constructors are marked with explicit overloads. 7be741d08b added a protected
    `idString` overload; translating it is **Phase 4** work.
  - `SubscriptionState::has_partitions_needing_validation` is removed, mirroring 455cbdfea2.

  **45 findings remain**, each tied to a behavioural or module change that a later phase owns:

  | Findings | Change | Owner |
  |---|---|---|
  | 25 | utils → `utils.internals` moves: `ByteUtils`, `ExponentialBackoff` (+ its test), `LogContext`, `ProducerIdAndEpoch` (D2) | Phase 1 |
  | 5 | `shouldClientThrottle` overrides removed (KAFKA-20828) | Phase 1 |
  | 2 | `TxnOffsetCommitRequest.getErrorResponseTopics` (baa064e422) and `TxnOffsetCommitResponse.errors` (89f3888c87) removed, held back by §2.2 | Phase 5 |
  | 2 | `ProducerBatch.isWritable` and `RecordAccumulator.recordsBuilder` (KAFKA-20578) | Phases 7/8 |
  | 1 | `ConsumerMembershipManager.onHeartbeatSuccess` (KAFKA-20681) | Phase 9 |
  | 10 | `SensorBuilder` → `consumer.internals.metrics` (KAFKA-19542) | Phase 11 |

  So `cargo xtask lint`, and with it `make verify`, cannot pass at the end of Phase 0 without pulling
  in other phases' work, which for the TxnOffsetCommit pair §2.2 forbids. Each owning phase must clear
  its rows. The user's gate decision and the per-finding table are in §5.1.
  - The `ApiVersionsRequest` changes that go with v5 (`setClusterId`/`setNodeId` and the both-or-neither
    check in `isValid`, 0ef4a4c80e) are left to **Phase 4**. Until then the client sends v5 with the
    defaults (`null` / -1), which the check accepts.
  - **Spec portions already applied (Phase 4 Critic, Phase 13 audit: do not count as missing).** The
    corpus sync landed the `ApiVersionsRequest.json` / `ApiVersionsResponse.json` parts of all three
    KAFKA-20246 commits: ede01b871e (v5 with the `ClusterId` / `NodeId` fields, marked unstable, 1/N),
    0ef4a4c80e (the fields' "provide both" docs, 2/N) and 7be741d08b (v5 marked stable, 3/N). What Phase 4 still owes
    from them is the Java code only, exactly as listed in the Phase 4 section below; Phase 0 applied
    none of it.
- **Skips:**
  - Streams-only spec content (StreamsGroupDescribe/Heartbeat v1, the topology description RPC) is
    synced but has no wrapper: out of scope (§1.1).
  - Java `MessageTest` / `RequestResponseTest` deltas for TxnOffsetCommit v6 (**Phase 5**), the
    tagged-field/array caps (**Phase 1**), `shouldClientThrottle` (**Phase 1**), and UnregisterController
    / Streams fixtures (**Phase 12** / out of scope) are left to their phases.
- **Verification:**
  - **Passing:** `cargo build`; `cargo test` (4285 passed, 0 failed, 10 ignored); `cargo xtask
    format-check`; `check-generated`; the three clippy passes and `doc-hygiene`, run one by one because
    `xtask lint` stops at the custom lint.
  - **`make -k verify`** (2026-10-07, macOS):
    - `build-c` fails with `cmake: command not found` (host limitation).
    - `lint` fails with exactly the 45 findings in the table above.
    - `test-rust-all-features`: 4462 passed, 17 failed. All 17 are Docker-backed `integration_tests::*`
      (the Docker daemon was not running), and cargo then skipped the remaining test targets.
    - Python unit tests: 363 passed, 2 skipped. `check-bindings` / format-arity: 29 passed. Soak tests:
      156 passed.
  - **Owed:** a real-broker run of the integration suite, in particular `api_versions_test` /
    `connection_test`, which now negotiate ApiVersions v5 (a pre-4.4 broker answers v5 with
    UNSUPPORTED_VERSION and the client retries at the broker's highest version).

### Phase 1 — Common and wire foundations (agent 91)

- **D2 module moves:** `common::utils::{byte_utils, exponential_backoff, log_context, producer_id_and_epoch}`
  → `common::utils::internals`, crate-private, with the Java package docs carried over. Fix imports across
  the crate. This goes first because it renames imports everywhere.
- **Generated-reader hardening** (f66a67fcef, 1a770734fe):
  - `MessageUtil` array-length limits, plus bounded array and tagged-field allocation in generated readers
    (`rust/generator/src/lib.rs`, `common/protocol/message_util.rs`, schema `ArrayOf` / `CompactArrayOf` if
    a counterpart exists).
  - Tests: `MessageTest` / `SimpleArraysMessageTest` additions, `SimpleKeyedArraysMessage.json`
    (dedicated per-message test file — DoD #3), `RequestHeaderTest`, `RequestContextTest`,
    `ProtocolSerializationTest`.
- **KAFKA-18157** (239a3e4990): `UnsupportedProtocolField` per D4 / §2.4, the ~15 request builders, and
  the `ConsumerHeartbeatRequestManager` branch. Tests: `NetworkClientTest` and
  `ConsumerHeartbeatRequestManagerTest` hunks, with exact error messages asserted.
- **KAFKA-20828** (63f445aaa9): `AbstractResponse` throttle time is derived from the response schema;
  remove the per-response overrides in the Rust wrappers that have counterparts. Test: `RequestResponseTest`
  (+55).
- **KAFKA-20072** (474798afbe): `Uuid::random` must never produce an ID that starts with `-` (or contains
  hyphens, per the Java change). Test: `UuidTest`.
- `KafkaMetric` `Display` (46ad599a6e) + `KafkaMetricTest`; `ByteUtilsTest` split (8b6d31f00c).
- Hot-path note (DoD #10): the reader bounds sit on the receive / response path; confirm there is no new
  per-record allocation.

Phase 1 completion notes (agent 91):

- **D2 moves (KAFKA-20297).** `byte_utils`, `exponential_backoff`, `log_context` and
  `producer_id_and_epoch` now live in `rust/src/common/utils/internals/`, a `pub(crate)` module whose
  docs carry the parent package's description of these classes (Java ships no `package-info.java` for
  `utils.internals`). Every import, the generator's emitted paths and the doc-alias markers use the new
  package. The 25 §5.1 rows are gone.
- **Generated-reader hardening (1a770734fe, f66a67fcef).** `MessageUtil` gains
  `MAX_PREALLOCATED_ARRAY_CAPACITY` / `MAX_ARRAY_LENGTH` / `MAX_TAGGED_FIELD_COUNT`. Every generated array
  reader (plain and tagged, nullable or not) rejects a length above `MAX_ARRAY_LENGTH` after the
  remaining-bytes check and pre-allocates at most 1000 elements. Every tagged-field section read (the
  known-tag path and both unknown-only paths) rejects a negative count, a count above the remaining bytes
  and a count above `MAX_TAGGED_FIELD_COUNT`, with Java's messages. The count is read as `i32`, as Java's
  `int`, so the negative guard is reachable.
  - Tests: `MessageDataGeneratorTest` (3, on emitted source), `MessageTest` (2),
    `SimpleArraysMessageTest` (4) with the new `generator/test-messages/SimpleKeyedArraysMessage.json`,
    `RequestHeaderTest` (1), `RequestContextTest` (3).
  - **DoD #10:** no new allocation. The reader gains two integer compares per array and three per tagged
    section, and the capacity is `min(length, 1000)`. An array of more than 1000 elements now regrows
    while it is read; that is per response, not per record (record payloads are a `records` byte field,
    not an array).
- **KAFKA-18157 (239a3e4990), PLAN D4.** `UnsupportedProtocolFieldException` is a crate-private
  `UnsupportedVersionKind` on the existing `UnsupportedVersionError` (`kafka_error_type!` gains an optional
  `kind:`). Java's class is `common::internals::UnsupportedProtocolFieldError`: its two constructors
  (`with_options`, with an Options builder per §2's three-parameter cap, and `with_message`) and Java's
  `instanceof` (`is_unsupported_protocol_field_error`).
  - **Throw sites ported (14 of Java's 20):** CreateTopics ×2, ConsumerGroupHeartbeat, ElectLeaders,
    FindCoordinator, ListGroups ×2, Metadata ×3, OffsetCommit, OffsetFetch, plus ListTransactions ×2. The
    ListTransactions pair is new code: its 4.3.1 `DurationFilter` / `TransactionalIdPattern` checks had
    never been ported. The 6 skipped sites are listed under the skips below.
  - **Plumbing.** For `instanceof` to work, the error object has to travel as it does in Java. A builder
    carries it inside its `io::Error` (`UnsupportedVersionError::into_io_error` / `from_io_error`, the
    `CorrelationIdMismatchError` precedent). `ClientResponse.version_mismatch` holds an
    `UnsupportedVersionError` instead of a `String`. `NetworkClient`, `NetworkClientDelegate`, `Sender`
    and the admin runnable pass it on unchanged. `ConsumerHeartbeatRequestManager.handleSpecificFailure`
    dispatches on the class, keeps the subclass's message, and wraps the cause as Java does.
  - **CLAUDE.md §12.4: predicate added.** §12.4 says "every **intermediate** (non-leaf) class in Java's
    error hierarchy MUST be recoverable as a predicate on `Error`". It makes no exception for a class whose
    subclasses are not public. `UnsupportedVersionException` has subclasses: the new one, and the
    `NoBatchedFindCoordinatorsException` / `NoBatchedOffsetFetchRequestException` nested in the request
    classes, so it was already intermediate in 4.3.1. D4 only decides how the *subclass* is represented,
    and the hidden kind does not make the parent a leaf. So `Error::is_unsupported_version_error()` exists
    now. It covers exactly the `UnsupportedVersion` variant, every kind included, and nests inside
    `is_invalid_configuration_error`. It is pinned in both directions:
    - over every code in `errors.rs`'s `test_hierarchy_predicates_match_java`;
    - in the nesting tests;
    - as a new column, with rows for both kinds, in `error.rs`'s `variant_predicates_match_java_hierarchy`.

    Its C twin is `kafka_common_Error_is_unsupported_version_error`. It has a Rust test over every code
    and lines in `c/tests/test_mock_producer.c`. Python exposes only retriable / fatal /
    transaction-abortable, so Python has no counterpart to add.
  - Tests: `UnsupportedProtocolFieldExceptionTest` (3); `NetworkClientTest`
    `testUnsupportedVersionDuringInternalMetadataRequest`, which now asserts the recorded failure's kind
    (`TestMetadataUpdater` got a shared failure handle); `ConsumerHeartbeatRequestManagerTest`
    `testUnsupportedVersionFromClient`, both cases plus two Rust-side ones showing the dispatch is on the
    class, not the text; `RequestResponseTest.testCreateTopicRequestV3FailsIfNoPartitionsOrReplicas`; a
    `NetworkClientDelegate` test; exact messages at every ported site.
- **KAFKA-20828 (63f445aaa9).** `ConcreteResponse::should_client_throttle`, the `AbstractResponse`
  dispatch, delegates for the 33 types whose Java class overrides the method. For the other 19 it
  computes the schema default (`throttle_time_ms` present at the version). Those 19 lose their per-type
  methods: the five Java removed and fourteen Rust stand-ins for the old inherited `false`.
  The ones that change from `false` to `true` are ConsumerGroupHeartbeat, ConsumerGroupDescribe,
  OffsetsForLeaderEpoch (v2+), UpdateFeatures, Describe/AlterClientQuotas, DescribeProducers,
  DescribeTransactions and ListTransactions; all match Java 4.4. (DescribeCluster already answered
  `true`, which matches its schema.) The 5 §5.1 rows are gone.
  - Test: `RequestResponseTest.testClientThrottlesResponsesWithThrottleTime` over every `ConcreteResponse`
    type and version (each parsed from its default data). It found a **pre-existing divergence**:
    `DeleteTopicsResponse` throttled from v1, Java from v2 (`DeleteTopicsResponse.java`, unchanged since
    4.3.1). Fixed.
- **KAFKA-20072 (474798afbe).** `Uuid::random_uuid` rejects any base64 string containing `-`;
  `testRandomUuid` keeps its 100 repetitions.
- **KafkaMetric `Display` (46ad599a6e).**
  - Rust has no reflection, so `Measurable` and `Gauge` gain a defaulted `type_name()` (DoD #7).
    It returns `std::any::type_name` of the implementor. The closure adapters answer `None`, as Java
    omits lambdas.
  - Sensor's internal `MeasurableArc` forwards to the stat it shares.
  - `MetricName`'s `Display` now renders tags as Java's `Map.toString` does (`{k=v}`), not as Rust
    `Debug`.
  - Tests: the three `KafkaMetricTest` cases, plus a sensor-registered stat.
- **ByteUtilsTest split (8b6d31f00c).** `Bytes.increment` / `BYTES_LEXICO_COMPARATOR` moved into the
  translated `ByteUtils`, so they are translated with it (DoD #2). `Bytes` has no Rust counterpart, so
  the API takes slices and returns `Vec<u8>`, whose `Ord` is `Bytes`' unsigned lexicographic order.
  - Tests: `testIncrement`, `testIncrementUpperBoundary`, `testIncrementWithSubmap`,
    `testBytesLexicographicCases`.
- **Recorded skips and deviations:**
  - `ProtocolSerializationTest.testReadArrayLengthAboveMaxIsRejected` /
    `testReadCompactArrayLengthAboveMaxIsRejected`, and the `ArrayOf` / `CompactArrayOf` half of
    f66a67fcef: there is no Rust schema reader. `protocol::types::Schema` only describes fields; the
    generated readers are the only decoders, and they carry the same limits.
  - `MessageDataGeneratorTest`: Java asserts the `ArrayList`, `BazCollection` and `BamCollection` forms
    separately. The Rust generator emits every collection as a `Vec`, so the test asserts the one capped
    form three times.
  - `RequestContextTest`: `RequestContext` is the broker's view of a request and has no client
    counterpart. The three cases run against the `ProduceRequestData` reader that
    `RequestContext.parseRequest` drives. Where Java asserts the `InvalidRequestException`'s cause, they
    assert the reader's error; where Java asserts only the class, they assert that the read fails.
  - KAFKA-18157, not ported:
    - the CreateAcls / DeleteAcls / DescribeAcls v0 guards: v0 was removed in Kafka 4.0
      (`validVersions` 1-3), the Rust builders already omitted these unreachable guards, and 4.4
      changes only their exception class;
    - the Heartbeat / JoinGroup / SyncGroup builders: classic protocol, no Rust builder
      (consumer-threading §20);
    - Streams' `InternalTopicManager`: out of scope.
  - `KafkaMetricTest`:
    - `testToStringWithStatProvider` asserts the Rust type path
      (`confluent_kafka::common::metrics::stats::avg::Avg`) where Java names the Java class;
    - `testToStringWithAnonymousClassProvider` uses `ClosureGauge`: Rust has no anonymous classes, and
      an inline provider is a closure adapter;
    - `testConstructorWithNullProvider` stays untranslated: a null provider is unrepresentable.
  - `ByteUtils`:
    - `increment` returns `LocalIllegalArgument` where Java throws `IndexOutOfBoundsException`, the
      crate's precedent for that class (`MockAdminClient`);
    - the comparator's offset/length overload is the slice method called with sub-slices.
- **Review round 1 fixes (COMMENTS.91, all moved to `COMMENTS.DONE.91.md`):**
  - **B1:** `generator/test-messages/SimpleKeyedArraysMessage.json` was git-ignored (`*.json`) and never
    committed. It is now force-added, as its siblings are. No other Phase 1 file is ignored.
    Re-verified from a clean `git archive HEAD rust` export with a fresh `CARGO_TARGET_DIR`:
    `cargo test --lib` builds and passes there (4261 passed, 0 failed, 3 ignored).
  - **L1:** a produce batch failed by a version mismatch now gets Java's message-less
    `PartitionResponse(UNSUPPORTED_VERSION)` (`Sender.java:599`), so the callback sees the default text.
    `testUnsupportedVersionInProduceRequest` asserts it.
  - **L2:** all 15 generated "zero-fill then `read_bytes`" sites now go through `read_array`, which checks
    the remaining bytes before it allocates. Compact string readers gain Java's `> 0x7fff` guard
    ("string field <camelCaseName>[ element] had invalid length <n>", Java's order). There is no new
    allocation (DoD #10): one allocation and one copy where there used to be an allocation, a memset and
    a copy. Covered by a generator test and two runtime tests.
  - **L3:** the stale `utils.ProducerIdAndEpoch` / `common/utils/byte_utils.rs` references are fixed in
    `python/admin.py`, `admin_service.proto`, `status.md` and an integration-test comment. All are
    hand-written; none is generated.
  - **Manager (a):** b69c07c816 is reclassified to Consumer/P (partial) and added to Phase 10, not
    implemented.
  - **Manager (b):** `cargo xtask lint --keep-going` runs every lint step even after one fails, reports
    every failure, and fails at the end if any step did. Its first run on Phase 1 found 3
    `module_path_hygiene` findings: literal `stats::avg::Avg` type paths in the `KafkaMetric` `Display`
    doc and tests. They are fixed. With `--keep-going` the result is: lint-custom fails with exactly the
    15 remaining §5.1 rows; doc-hygiene, module-path-hygiene and all three clippy passes are clean.
    **Later phases should gate on `cargo xtask lint --keep-going`.**
- **Timing log** (2026-10-07, IST):

  | Step | Start | End | Minutes |
  |---|---|---|---|
  | 1 D2 moves | 16:49 | 16:55 | 6 |
  | 2 reader hardening | 16:55 | 17:03 | 8 |
  | 3 KAFKA-18157 + predicate + FFI | 17:03 | 17:27 | 24 |
  | 4 KAFKA-20828 (+ clippy fixup) | 17:27 | 17:32 | 5 |
  | 5 KAFKA-20072 | 17:32 | 17:33 | 1 |
  | 6 KafkaMetric Display | 17:33 | 17:36 | 3 |
  | 7 ByteUtilsTest split | 17:36 | 17:38 | 2 |
  | 8 gates, notes, `make verify` (11.5 min of it) | 17:38 | 17:56 | 18 |

- **Verification:**
  - `cargo build` passes. `cargo xtask format-check` passes.
  - `cargo test`: 4308 passed, 0 failed, 10 ignored (lib 4259 / 3 ignored, plus 36, 8 and 5 / 7
    ignored).
  - `cargo xtask lint`: the custom lint reports exactly the 15 remaining §5.1 rows (Phase 5 2,
    Phases 7/8 2, Phase 9 1, Phase 11 10), nothing new. The step stops there, so the rest was run one by
    one, and all pass: `doc-hygiene`, both workspace clippy passes (default and `--all-features`), and
    the xtask clippy pass.
  - `make -k verify` (2026-10-07, macOS, 17:43–17:54) fails in three targets:
    - `build-c`: `cmake: command not found`. Environment: cmake is not installed on this host.
    - `lint`: the 15 remaining §5.1 rows only, as above. Expected under the §5.1 gate rule.
    - `test-rust-all-features`: 4486 passed, 17 failed, 3 ignored. All 17 failures are Docker-backed
      `integration_tests::*` ("failed to list networks": the Docker daemon is not running), an
      environment issue.

    Everything else passes: Python unit tests (363 passed, 2 skipped), `check-bindings` / format-arity
    (29 passed), and the soak tests (156 passed).
  - **Owed:** a Docker-backed run of the integration suite, and a `build-c` / C-test run on a host with
    cmake. The C test changes (`kafka_common_Error_is_unsupported_version_error`) are therefore
    compile-checked only through the Rust FFI test.

### Phase 2 — KIP-909 core: bootstrap DNS resolution (agent 92)

- Translate in tree-diff order:
  - `BootstrapConfiguration` (new);
  - `ClientUtils` (`bootstrapConfiguration`, `maybeBootstrapMetadataSynchronously`, `parseAddresses`);
  - `CommonClientConfigs` `BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG` (default 0, experimental doc);
  - `MetadataUpdater` defaults (`bootstrap`, `isBootstrapped`, `bootstrapFailed`, `clusterId`);
  - `NetworkClient` (+216) and `Metadata` (`bootstrapFatalError`, `maybeThrowBootstrapFatalException`);
  - `AdminMetadataManager` (+42), `KafkaAdminClient` (+90/−6), `AdminClientConfig`;
  - `KafkaProducer` / `ProducerConfig` construction;
  - consumer construction and config wiring (`ConsumerConfig`, `KafkaConsumer`, `AsyncKafkaConsumer`,
    `NetworkClientDelegate`, `ConsumerUtils`).
  - Java deletes `AdminBootstrapAddresses`; confirm Rust has no counterpart to delete.
- KAFKA-20939 (0df48ff5c5, 87943b2ff8): a DNS-failure regression fix and the "experimental" marking.
- MINOR 0720ba1141: port validation, dead code.
- Tests: `NetworkClientTest`, `KafkaAdminClientTest`, `KafkaProducerTest`, `KafkaConsumerTest` (non-share,
  non-classic slices), `ClientUtilsTest`, `MetadataTest` hunks. The behaviour of both modes (0 and
  positive) is the contract.
- Integration: a test with an unresolvable bootstrap host under a positive timeout, asserting the
  `BootstrapResolutionError` message.
- Commits covered: 507d01da42 (minus its Share/Classic files), 0df48ff5c5, 87943b2ff8, 0720ba1141.
  Recorded skip: d1c0bd82c0 (ShareConsumerImpl only).

Phase 2 completion notes (agent 92):

- **Commits:** b3d85ec1 (core + admin + producer), 5b2eef8d (consumer), 37617f3c (integration test),
  c81ffa34 (lint), plus this notes commit. All four Java commits are covered: 507d01da42 (minus Share /
  Classic), 0df48ff5c5, 87943b2ff8, 0720ba1141.
- **What landed, in tree-diff order:**
  - `CommonClientConfigs::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG` and `DEFAULT_BOOTSTRAP_RESOLVE_TIMEOUT_MS = 0`.
    The `_DOC` text (with 87943b2ff8's "evolving feature" sentence) is rustdoc, following the
    `CLIENT_DNS_LOOKUP_CONFIG` precedent: the crate has no `ConfigDef` docs.
  - `BootstrapConfiguration` (new, crate-private): `DISABLED`, `enabled()` (validates without resolving;
    "Invalid url" / "Invalid port" per 0720ba1141), `is_disabled()`. Java compares against `DISABLED` by
    identity; Rust compares by value, which is equivalent because only `DISABLED` has no `client_dns_lookup`.
  - `ClientUtils`: `resolveAddress` extracted and shared, as in Java, with its two exceptions as the
    private `ResolveAddressError`; `parse_addresses`, `bootstrap_configuration` (with the info log) and
    `maybe_bootstrap_metadata_synchronously`. 0720ba1141's removal of the `(AbstractConfig)` overload has no
    Rust counterpart. Rust configs share no `AbstractConfig`, so the two new functions take the values
    Java reads from it.
  - `Metadata`: `bootstrap_fatal_error`, `maybe_return_bootstrap_fatal_error`. The failure is checked first
    by `maybe_return_fatal_error` and by `clear_errors_and_maybe_return_error`, is never cleared, and wakes
    `await_update` waiters (Java's `notifyAll`).
  - `MetadataUpdater`: `bootstrap_failed` (default no-op), `is_bootstrapped`, `bootstrap`. The test-only
    `ManualMetadataUpdater` / `TestMetadataUpdater` implement Java's `ManualMetadataUpdater` semantics.
  - `NetworkClient`: `ensure_bootstrapped` and its helpers, `handle_empty_node_list`, the
    `&& is_bootstrapped()` rebootstrap gate in `default_maybe_update`, the `DefaultMetadataUpdater`
    `isBootstrapped` / `bootstrap` / `bootstrapFailed` delegation, and `cancel_bootstrap_resolution` on close.
  - Admin: `AdminClientConfig` key (`atLeast(0)`) and getter; `KafkaAdminClient::determine_bootstrap_type`;
    `AdminMetadataManager` `is_bootstrapped` / `bootstrap_fatal_error` / `record_bootstrap_fatal_error` plus
    the updater hooks; `AdminClientRunnable::fail_all_pending_calls` and the loop exit; and the enqueue-side
    check in `CallSender::call` (Java's `runnable.call` → `enqueue`), which `submit`, the
    `AdminApiDriver` path and the per-broker `listGroups` sends all use (review round 1).
  - Producer: `ProducerConfig` key; construction through `maybe_bootstrap_metadata_synchronously` and
    `set_bootstrap_configuration`.
  - Consumer: `ConsumerConfig` key; `AsyncKafkaConsumer` construction as for the producer; `ensure_open`
    (Java's `acquireAndEnsureOpen`) returns the bootstrap failure from every `Result`-returning API (the
    sync accessors cannot; see the deviations); `NetworkClientDelegate`
    `bootstrap_error_propagated` / `propagate_metadata_error`.
  - Rustdoc: the `@throws BootstrapResolutionException` lines on the `Consumer` trait (`poll`, `commit_sync`,
    `committed`, `partitions_for`, `list_topics`, `offsets_for_times`) and the `Producer` trait (`send`,
    `partitions_for`).
  - `AdminBootstrapAddresses` has no Rust class to delete: its `fromConfig` logic lived inline in
    `AdminClientConfig::new`, which now calls `determine_bootstrap_type`.
- **Mode 0 is unchanged, and how that is shown:**
  - Every client's `bootstrap.resolve.timeout.ms` defaults to 0. `bootstrap_configuration(0, ..)` returns
    `DISABLED`, and every new `NetworkClient` path returns at once on `is_disabled()`. The two shared-path
    edits are inert in mode 0:
    - `handle_empty_node_list` still panics with the old message;
    - the rebootstrap gate's `is_bootstrapped()` can only be false when the node list is empty, and then
      `least_loaded_node` has already panicked.
  - `maybe_bootstrap_metadata_synchronously(0, ..)` is the old `parse_and_validate_addresses` +
    `metadata.bootstrap`, with the same error. The admin path keeps its synchronous
    `parse_and_validate_addresses` + `update(Cluster::bootstrap)`.
  - Tests:
    - `test_disabled_bootstrap_configuration_never_resolves` polls a `DISABLED` client at three far-apart
      times and asserts no resolution, no timer and no failure;
    - `test_least_loaded_node_while_bootstrapping` asserts the mode-0 empty-cluster panic message;
    - `test_bootstrap_configuration` and `test_maybe_bootstrap_metadata_synchronously` cover the 0 branch;
    - the three `*ConstructorFailsWithConfigException*WhenTimeoutZero` translations (producer, consumer,
      admin) pin the construction-failure contract;
    - every pre-existing `NetworkClient` / client test still passes unchanged on the `DISABLED` default.
- **Async path (mode > 0) design** (CLAUDE.md §11.6, consumer-threading §10):
  - The resolution runs `ClientUtils::parse_addresses` on a detached OS thread named
    `kafka-bootstrap-dns-resolver` (Java's daemon executor thread, `NetworkClient.java:399`), and its result
    comes back over a `oneshot`. `poll()` only `try_recv`s that result, so the resolution is never awaited,
    is never in a `select!` arm, and never holds up or races the network poll. No runtime owns the thread,
    so no runtime drop waits for it (review round 1, Issue 2).
  - `close()` cancels the attempt (drops the receiver; the thread's `is_interrupted` check, Java's
    `isInterrupted()`, stops it at the next URL) and then waits for the thread as Java's
    `shutdownExecutorServiceQuietly(.., 1, SECONDS)` does: at most 1 s, then at most 1 s more, then it logs
    and returns.
  - When the result lands, the thread pokes the selector's wakeup handle so a blocked poll picks it up
    promptly. That wake is Rust-only and changes latency only.
  - No lock is held across an `.await`: `Metadata` / `AdminMetadataManager` take their std `Mutex` briefly
    and synchronously.
  - The timer starts on the first poll, and an expiry at the same poll as a result loses to the result, as
    in Java.
- **Recorded skips:**
  - d1c0bd82c0 and the Share / Classic hunks of 507d01da42 / 0df48ff5c5 (`ShareConsumerImpl`,
    `KafkaShareConsumer`, `ClassicKafkaConsumer`, `KafkaShareConsumerTest`), plus the `CLASSIC` parameter of
    the two `KafkaConsumerTest` translations: out of scope (§1.1, consumer-threading §20).
  - Connect, core, raft, server, streams, tools and trogdor hunks of 507d01da42 / 0df48ff5c5: not client code.
  - `MetadataUpdater.clusterId`: it comes from 0ef4a4c80e (KAFKA-20246 2/N), which Phase 4 owns. Its only
    callers are Phase 4's.
  - `ClientUtils.createNetworkClient(config, bootstrapServers, ..)` and the `metadata.cluster.check.enable`
    argument: Rust has no `createNetworkClient`, since each client builds its `NetworkClient` itself, and
    the cluster-check flag is Phase 4.
  - `KafkaAdminClient.java:790-797` (`MetadataUpdateNodeIdProvider.provide()`), the `isBootstrapped()` gate on the admin rebootstrap: the Rust admin client
    runs with `MetadataRecoveryStrategy::None` and has no rebootstrap path. `AdminMetadataManager::is_bootstrapped`
    is translated (DoD #2) and tested, and is `cfg_attr(not(test), expect(dead_code))`.
  - `KafkaProducer.awaitTopicMetadata`'s `maybeThrowBootstrapFatalException`, and
    `KafkaProducerTest.testProducerSendOffsetsToTransactionBootstrapResolutionExceptionPropagated` (including
    0720ba1141's mock tweak to it): `awaitTopicMetadata` is KIP-1319, so this is **Phase 5's**. Java's own
    comment says `sendOffsetsToTransaction` "became metadata-aware in KIP-1319".
  - Java's `Thread.interrupted()` → `InterruptException` branch in `ensureBootstrapped`: it tests the
    *application* thread's interrupt flag, which a tokio task does not have. (`parseAddresses`'
    `isInterrupted()` exit **is** translated, as the `is_interrupted` argument; review round 1 corrected an
    earlier note that called it untranslatable.)
  - `NetworkClientTest` fixture plumbing: the `bootstrapConfiguration` argument on every helper, the
    `bootstrapMetadataUpdater` calls, and the extra `client.poll(0, ..)` and metadata re-update in
    `testRebootstrap` / `testRequestTimeout` / `testDefaultRequestTimeout`. Java's tests needed these because
    their fixtures switched to an *enabled* configuration; the Rust fixtures keep the `DISABLED` default,
    which is what the production clients use in mode 0.
  - `KafkaProducerTest.testConstructorFailureCloseResource` / `KafkaConsumerTest.testConstructorClose`
    (bootstrap host and interceptor-class changes) and `doTestHeaders` (bootstrapping the injected metadata):
    no Rust counterpart test. Reflective metric reporters and interceptor classes are not translated.
  - `KafkaAdminClientTest.mockBootstrapCluster` (`createUnresolved`): a fixture with no Rust counterpart.
  - `MetadataTest` (one import-line change) and `ClientUtilsTest` (unchanged): nothing to translate. New
    Rust tests cover `parse_addresses`, `bootstrap_configuration` and `maybe_bootstrap_metadata_synchronously`.
  - `PlaintextConsumerTest.testListTopics` now polls inside its wait. The Rust
    `test_async_consumer_list_topics` already does.
- **Deviations (DoD #7):**
  - `PendingBootstrapResolution` (a `oneshot::Receiver`) replaces Java's `CompletableFuture`, and one
    detached, named OS thread per attempt replaces the `kafka-bootstrap-dns-resolver` single-thread
    executor (attempts never overlap, so it is one thread at a time; `bootstrap_resolver_exit` is the
    executor's `awaitTermination`). Dropping the receiver is `cancel(true)` plus the `shutdownNow()`
    interrupt. A `getaddrinfo` already running cannot be interrupted in either language; the thread exits
    after it. (The first version ran on tokio's blocking pool. Critic 92 showed a running blocking task
    holds a runtime's drop without bound — `close_with_options(1 s)` took 14 s — so that justification was
    withdrawn.)
  - `CallSender` (Rust-only) bundles the admin channel with the state `call` / `enqueue` read, because
    Java's driver closures capture the runnable itself and the Rust runnable is owned by its task.
  - The consumer's sync accessors (`assignment`, `subscription`, `paused`, `group_metadata`,
    `current_lag`) keep returning their value after a permanent bootstrap failure, where Java's
    `acquireAndEnsureOpen()` throws `BootstrapResolutionException`: they have no error channel
    (consumer-threading §1), as for the existing closed-consumer divergence. Documented on each accessor.
  - The bootstrap `Timer` is a deadline (`org.apache.kafka.common.utils.Timer` has no translation).
  - The configuration is a setter rather than a constructor argument.
  - `determineBootstrapType` runs inside `AdminClientConfig::new`, where the old `fromConfig` logic already
    ran, because the Rust config does not keep `bootstrap.controllers`.
  - `ResolveAddressError` stands for the two Java exceptions of `resolveAddress`.
  - `CallSender::call` follows Java's order. It rejects with `IllegalState` only when `close()` has set a
    hard-shutdown time (Java's `hardShutdownTimeMs` check in `call()`), not on `shutdown.closing`, which the
    I/O task also sets when it exits on its own. After that come the `bootstrap.controllers` endpoint check,
    the bootstrap failure, and finally the closed channel, which gives Java's `enqueue` `TimeoutException`.
- **DoD #10: N/A.** The `NetworkClient`, `Metadata` and admin bootstrap paths are per poll and per
  construction, not per record. The only addition to `poll()` in mode 0 is the `is_disabled()` comparison.
- **For Phase 3 (consumer busy loops):** the consumer's async path works end to end
  (`test_consumer_bootstrap_resolution_error_propagated_to_poll`, ~3 s), but Phase 2 did not audit the
  pre-bootstrap poll-timer / `maximum_time_to_wait` behaviour that KAFKA-20854/21010/20970 fix. While
  bootstrapping:
  - `NetworkClient::least_loaded_node` returns an empty node instead of panicking, so request managers
    see `None` there;
  - the default updater's `maybe_update` returns `reconnect_backoff_ms`;
  - the resolver's wake pokes the selector.
  Phase 3's `AbstractFetch` / heartbeat / commit fixes plug in on top. `ensure_open` is the
  `acquireAndEnsureOpen` hook if Phase 3 needs more call sites.
- **For Phase 4 (shared `NetworkClient`):**
  - Follow the `set_bootstrap_configuration` setter precedent for `metadataClusterCheckEnable`.
  - `MetadataUpdater::cluster_id` is still to add, with Java's default `None`.
  - `poll()` now calls `ensure_bootstrapped(now)` right after the poll-time stores, as Java does right after
    `ensureActive()`.
  - `least_loaded_node` goes through `handle_empty_node_list`, so the KAFKA-20393 `stickyNode` work should
    keep that path.
- **For Phase 12 (shared `KafkaAdminClient`):**
  - `AdminClientRunnable` has a `bootstrap_failure_handled` flag. `run_once` returns right after
    `fail_all_pending_calls`, and `process_requests` breaks on the flag.
  - Every enqueue goes through `CallSender::call`, which reads `metadata_manager.bootstrap_fatal_error()`
    once at the top; keep that order relative to the closing and controllers checks when adding
    `unregister_controller`. Java's `enqueue` also starts with a `tries > maxRetries` check that the Rust
    enqueue never had (a pre-existing gap, not KIP-909).
  - `fail_all_remaining` closes the call channel before its final drain, so no accepted call is dropped.
  - `bootstrap.controllers` is still rejected in the config, so `determine_bootstrap_type` returning `true`
    becomes the unsupported-version error.
- **For the producer-track merge (Phase 5):** in `KafkaProducer::new_inner` this phase touched only steps 1
  and 5 and added the `set_bootstrap_configuration` call after the `NetworkClient` is built. In
  `ProducerConfig` it added the field, constant, parse arm and test next to `retry.backoff.max.ms`, and the
  `Producer` trait docs gained one line each on `send` / `partitions_for`.
- **Timing log** (2026-10-07, IST):

  | Step | Start | End | Minutes |
  |---|---|---|---|
  | 0 reading (rules, plan, Java diffs, Rust counterparts) | 20:29 | 20:45 | 16 |
  | 1 core + NetworkClient + admin + producer, with tests (b3d85ec1) | 20:45 | 20:59 | 14 |
  | 2 consumer, with tests (5b2eef8d) | 20:59 | 21:04 | 5 |
  | 3 integration test (37617f3c) | 21:04 | 21:08 | 4 |
  | 4 gates: format-check, `lint --keep-going` + fixes (c81ffa34), full `cargo test` | 21:08 | 21:18 | 10 |
  | 5 `make -k verify` (10 min) and these notes | 21:18 | 21:30 | 12 |

- **Verification:**
  - `cargo build` passes. `cargo xtask format-check` passes.
  - `cargo test`: 4336 passed, 0 failed, 10 ignored (lib 4287 / 3 ignored, plus 36, 8 and 5 / 7 ignored).
  - `cargo xtask lint --keep-going`: lint-custom reports exactly the 15 remaining §5.1 rows (Phase 5 2,
    Phases 7/8 2, Phase 9 1, Phase 11 10), nothing new. Every other step is clean: doc-hygiene,
    module-path-hygiene, and the default, `--all-features` and xtask clippy passes.
  - `make -k verify` (macOS, 21:18–21:28) fails in three targets, for the same reasons as in Phase 1:
    - `build-c`: `cmake: command not found` (environment);
    - `lint`: the 15 §5.1 rows only;
    - `test-rust-all-features`: the `cargo test --all-features` lib run appears twice in the log, at
      4508 and 4514 passed. Each has 17 failed and 3 ignored, and all 17 are Docker-backed
      `integration_tests::*`; the Docker daemon is not running (environment).

    Everything else passes: Python unit tests (363 passed, 2 skipped), `check-bindings` / format-arity
    (29 passed), and the soak tests (156 passed).
  - The three broker-less `bootstrap_resolution_test` integration tests (producer, consumer, admin with an
    unresolvable host) were run directly and pass.
  - **Owed:** a Docker-backed run of `bootstrap_resolution_test::test_clients_bootstrap_asynchronously_with_positive_timeout`
    and the rest of the integration suite, and a `build-c` / C-test run on a host with cmake. No C or Python
    source changed in this phase.
- **Review round 1 (COMMENTS.92, all moved to `COMMENTS.DONE.92.md`, 2026-10-07 22:02–22:30 IST):**
  - **Issue 1 (Medium), fixup 20185de8:** one shared enqueue, `CallSender::call`, so the 14 driver-backed
    RPCs fail with `BootstrapResolutionError` instead of the retriable "thread has exited" timeout. The
    channel is closed before the final drain, so every accepted call's future completes. Tests: three
    driver RPCs in `test_admin_bootstrap_resolution_error_propagated` (each bounded at 5 s, exact message)
    and `test_call_sender_applies_the_enqueue_checks`.
  - **Issue 2 (Medium), fixup 253f968d:** the detached resolver thread, the `is_interrupted` check and the
    bounded `close()` described above. Tests:
    - `NetworkClient` `close()` with three 5 s lookups in flight: under 3 s;
    - a runtime drop with a lookup in flight: under 2 s;
    - the consumer built outside any runtime, as the bindings build it, with `close_with_options(1 s)`:
      under 5 s (the whole filtered run took 3.2 s; the Critic's probe measured 14 s before);
    - the producer close plus runtime drop, the C / Python destroy path: under 4 s;
    - `parse_addresses` stops once interrupted.

    The slow lookups come from a `cfg(test)` host-name seam (`*.slow-dns.kafka.test`), not real DNS.
  - **Issue 3 (Low), fixup 6b929c66:** the sync-accessor divergence is documented; the `ensure_open` doc is
    narrowed.
  - **Issue 4 (Low), fixup fbfe06c7 plus this file:** citations `:1621-1629` and `:790-797` re-checked at
    4.4.0-rc4; the stale `(AbstractConfig)` sentence and the misplaced `consumer_config.rs` doc comment are
    fixed.
  - **Harness, bc66e31e:** `INTEGRATION_TEST_BROKER_TAG` selects the `apache/kafka` tag (default `4.2.0`).
    Broker runs, against both the default and `4.4.0-rc4` (confirmed via `docker ps`: only
    `apache/kafka:4.4.0-rc4` containers):

    | Suite | 4.2.0 | 4.4.0-rc4 |
    |---|---|---|
    | `--test integration -- bootstrap_resolution_test` | 4 / 4 | 4 / 4 |
    | `--test integration -- client_rebootstrap_test` | 4 / 4 | 4 / 4 |
    | `--lib -- integration_tests::` | 31 / 31 | 31 / 31 |
  - **Round 2 (Issues 5, 6):** `CallSender::call` now rejects with `IllegalState` only when a hard-shutdown
    time is set, as Java's `call()` does. A task that exited on its own (a panic caught in `run`) therefore
    yields Java's `TimeoutException("The AdminClient thread has exited.")` at the closed channel, and a
    bootstrap failure is caught by the next check. Covered in `test_call_sender_applies_the_enqueue_checks`.
    These review notes now sit inside the Phase 2 section, where they belong; they had been appended after §6.

### Phase 3 — KIP-909 consumer follow-ups (agent 93)

- KAFKA-20854 (642e0a5db0): `AbstractFetch` (+61), `FetchRequestManager`, `Fetcher` (classic → skip),
  `RequestManagers`, `SubscriptionState`, `AsyncKafkaConsumer`.
- KAFKA-21010 (d12e95da90): `AbstractHeartbeatRequestManager`, `HeartbeatRequestState`,
  `CommitRequestManager`.
- KAFKA-20970 (cd44c5de0f): `CommitRequestManager`.
- Tests: `FetchRequestManagerTest` (+158), `ConsumerHeartbeatRequestManagerTest` (+121),
  `CommitRequestManagerTest` (+95), `AbstractHeartbeatRequestManagerTest`. Skip the Share / Streams test
  hunks with reasons.
- Busy-loop fixes must be shown not to spin in Rust: assert poll-timer / `maximum_time_to_wait` values;
  don't rely on wall-clock behaviour alone.

Phase 3 completion notes (agent 93):

- **Commits:**
  - 91dcc787 (KAFKA-20854) + fixup 73d04f5f (lint markers);
  - 02ac689c (KAFKA-20253, pulled forward: coordinator + auto-commit hunks) + fixup 150f4170 (coordinator
    forwarder wakes the bg task);
  - e4bf1523 (KAFKA-20970);
  - 98e6c919 (KAFKA-21010 + the KAFKA-20253 heartbeat hunk it rewrites);
  - 7ffcfa5b (the bg loop consults the commit manager);
  - a269808f (selector drains fired readiness every poll iteration);
  - plus this notes commit.
- **What landed:**
  - KAFKA-20854:
    - `SubscriptionState::has_fetchable_partitions`.
    - `AbstractFetch`'s nested `FetchRequestPreparationResult`, returned by `prepare_fetch_requests` and
      `prepare_close_fetch_session_requests`. Close may always wake the buffer.
    - The "wake on any response" moved from `handle_fetch_success` to `remove_pending_fetch_request`.
    - The Phase-26 all-nodes-unfetchable short-circuit computes the same `can_wake` flag as the full path:
      false with nothing buffered, which is the KIP-909 no-nodes window.
    - `FetchRequestManager` takes `retry_backoff_ms` and overrides `maximum_time_to_wait`
      (`retry_backoff_ms` with nothing in flight, else `i64::MAX`).
    - An empty poll wakes the buffer only on `can_wake`. This replaces the Rust-only "wake when nothing is
      in flight" guard, which still woke the buffer when no partition was fetchable.
    - `AsyncKafkaConsumer::poll_for_fetches_timeout_ms` (extracted, Rust-only) gains the "fetchable but
      unbuffered → retry backoff" branch.
  - KAFKA-20970 / KAFKA-21010:
    - `CommitRequestManager::maximum_time_to_wait` returns `retry_backoff_ms` while the coordinator is
      unknown. KAFKA-20970's interval accessor is added and then removed again, as in Java.
    - `ConsumerHeartbeatRequestManager::maximum_time_to_wait` has its 4.4 shape:
      - UNSUBSCRIBED or FATAL → `i64::MAX`;
      - poll timer expired → 0;
      - coordinator unknown or `should_skip_heartbeat()` → `HeartbeatRequestState::retry_backoff_ms()`.
    - `HeartbeatRequestState::retry_backoff_ms()` is new. It reads a new `RequestState::exponential_backoff`
      accessor, which stands in for Java's protected field.
  - Bg loop (`ConsumerNetworkThread::run_once` Phase 5):
    - It now also consults the coordinator and commit handles, which `RequestManagers::entries()` skips.
    - Java's `entries()` includes both. Before this phase the auto-commit timer never bounded the
      application's wait, so the 20970/21010 commit fixes would have been unreachable.
- **Regressions found by `make -k verify` and fixed in this phase:**
  - `consumer_bounce_test::test_async_close` failed 3/3 (baseline ed2e3ce3: 2/2 passed): "Close took too
    long 3001".
    - Cause: the KAFKA-20253 coordinator guard made the bg loop sleep (`i64::MAX`) while FindCoordinator was
      in flight. The FindCoordinator response is applied by a spawned forwarder *after* the network poll
      returns (Java applies it inside the poll), and nothing woke the loop afterwards.
    - Before the guard, the loop's 0 timeout hid this.
    - Fixed in 150f4170: `CoordinatorRequestManager::completion_notify`, wired to `event_notify` as the
      commit and fetch managers already are.
  - Selector stranded fire (a269808f):
    - `drain_fired_queue` ran only inside the WAIT's `readiness_wait`. Its `select!` is `biased` towards the
      wakeup `Notify`, so a poll entered with a stored permit returned without draining.
    - The fired channel then kept `armed_read = true` and was never re-armed or processed. A trace showed a
      coordinator-channel read fire drained 30 s late, after the channel had closed.
    - This phase makes it reachable: the application task now pokes the bg task once per `retry.backoff.ms`
      instead of busy-looping, and the loop no longer polls with timeout 0, whose `process_all` sweep used
      to read every channel anyway.
    - `test_fire_not_stranded_when_every_poll_starts_woken` fails at its 5 s timeout without the new drain.
- **Remaining timing difference (for the Critic):** `test_async_close` now passes but takes ~50 s, against
  ~20 s at baseline.
  - Close timings are identical. The extra ~30 s is the group's first ConsumerGroupHeartbeat going
    unanswered until the 30 s request timeout; the retry succeeds at once.
  - Broker logs (4.2.0) show the coordinator receiving that heartbeat about 190 ms *before* its
    `__consumer_offsets-0` followers make their first fetch. The member-join write therefore does not commit
    in time, and no response comes.
  - The baseline client reaches the coordinator slightly later and misses that window. The heartbeat bytes
    are identical.
  - So this is a broker-side race with the brand-new offsets partition, which the client's new timing hits;
    it is not a client defect. Under full-suite load the extra 30 s can push `test_async_close`'s 60 s
    receive bound over (seen once in verify run 2).
- **How each busy loop is shown fixed, by value:**
  - Background task, while bootstrapping:
    - `run_once_does_not_spin_while_bootstrap_resolution_is_pending` runs 40 iterations (50 ms model steps)
      of a group consumer over a client with no nodes: real coordinator, commit (100 ms auto-commit
      interval), heartbeat (a JOINING member with zero heartbeat interval), offsets and fetch.
    - It asserts every network poll timeout == `retry.backoff.ms` (100) and the published
      `cachedMaximumTimeToWait` == 100.
    - Teeth-checked: without the KAFKA-20253 coordinator guard the poll timeout is 0 from iteration 2. With
      the heartbeat returning its (zero) interval, or the commit manager ignoring the coordinator, the bound
      becomes 0 or the timer's remainder.
  - Application task:
    - `test_poll_for_fetches_timeout_bounded_by_retry_backoff` asserts the wait in each state:
      - nothing assigned (the bootstrapping case), no position, or unbuffered fetchable → 100;
      - everything buffered → the full timeout;
      - `maximumTimeToWait` 0 → 0, which the caller turns into an immediate return.
    - `test_poll_with_manual_assignment_does_not_busy_loop` asserts Java's 500.
  - Per manager, at Java's values:
    - FRM: the 8 new tests, plus the pre-4.4 `testEmptyFetchResponseWakesUpBuffer`. They assert
      `maximum_time_to_wait` (`i64::MAX` / 100) and the buffer's pending-wakeup flag
      (`FetchBuffer::is_woken_up_for_test`, cfg(test)) instead of Java's blocked-thread joins.
    - Commit and heartbeat assert `DEFAULT_RETRY_BACKOFF_MS` / `Long.MAX_VALUE` exactly.
    - Both `...DoesNotSpinDuringRealBootstrapDnsResolution` tests drive a real `NetworkClient` that resolves
      `unresolvable.invalid` asynchronously until the `BootstrapResolutionError`, asserting `> 0` at each
      step.
- **Audit of the bootstrapping wait computation (mode > 0):**
  - Besides the three fixed managers:
    - the offsets manager returns `EMPTY` / `with_requests` (`i64::MAX`);
    - the topic-metadata manager returns Java's one-shot `0` only when it emits a request;
    - the membership manager returns `EMPTY`;
    - the real `NetworkClient` caps its selector wait at `maybe_update`'s `reconnect_backoff_ms` (50 ms
      default) while no node is known.
  - The network poll is never raced in a `select!`, and no guard is held across an `.await`: the new reads
    take each std `Mutex` briefly, and the commit manager reads its coordinator before taking its state
    lock.
  - **Mode 0:** the bootstrap path is untouched. The wait changes apply in both modes, as in Java.
- **Recorded skips:**
  - the `Fetcher.java` hunk (classic consumer);
  - the `StreamsGroupHeartbeatRequestManager` hunks of 20970/21010 and their tests, and the
    `ShareHeartbeatRequestManagerTest` hunks of 21010/20253 (consumer-threading §20);
  - checkstyle suppressions;
  - the fixture-only `AsyncKafkaConsumerTest` hunks: the `retryBackoffMs` local in `newConsumer`, and the
    Mockito stub reordering in `testProcessBackgroundEventsWithInitialDelay`, which has no Rust counterpart.
- **Deviations (DoD #7):**
  - `FetchRequestPreparationResult` is keyed by node id, with the `Node` alongside.
  - `poll_for_fetches_timeout_ms` is an extracted helper (Java computes it inline).
  - The FRM "in flight" set can lag Java's by one iteration: completions are drained by the next
    `poll(now)`, which the forwarder's `completion_notify` schedules at once.
  - `CoordinatorRequestManager::completion_notify` exists because the Rust forwarder runs outside the
    network poll.
  - Two Rust tests were corrected:
    - the heartbeat "poll timer expired" test answered through `should_heartbeat_now()`, because the Rust
      poll timer is unarmed until the first reset (Issue 9); it now arms the timer;
    - several commit timer tests now wire a known coordinator, as Java's mocks do.
- **DoD #10: N/A** (per-poll / per-iteration paths, not per record). The extra `buffered_partitions()` in the
  wait bound and in the short-circuit allocates only when the buffer holds data. The selector drain is one
  uncontended lock per loop iteration.
- **For Phase 9:**
  - KAFKA-20253 (28de22de34) is **done here in full**: its three main hunks and its
    `ConsumerHeartbeatRequestManagerTest` / `CoordinatorRequestManagerTest` tests (the Share test is
    skipped). Record it as covered by Phase 3.
  - The heartbeat `maximum_time_to_wait` is the final 4.4 version.
  - `AbstractHeartbeatRequestManagerTest.testNoCoordinator`'s 21010 assertion lives in
    `poll_returns_empty_when_no_coordinator`; the e7b0cb7908 file mapping is still Phase 9's decision.
  - The heartbeat file lost its stale `cfg_attr(test, expect(dead_code))`.
  - The heartbeat forwarder has no bg-task wake, unlike the coordinator, commit and fetch forwarders.
    Phase 9 should check whether its responses wait out the poll timeout.
- **For Phase 4:**
  - `NetworkClient::set_mock_time` is now `pub(crate)` (cfg(test)) for the shared fixture
    `network_client_delegate::bootstrapping_network_client_delegate_for_test`.
  - The selector's loop now drains fired readiness at its top (a269808f).
- **For Phase 10:**
  - The FRM wake semantics and `poll_for_fetches_timeout_ms` changed as described above.
  - Production's `is_unavailable` closure is still the constant `false` under the pre-existing
    `FIXME(phase-9-sasl)` in `async_kafka_consumer.rs`, so KAFKA-20854's reconnect-backoff skip is
    reachable only in tests.
- **Timing log** (2026-10-07/08, IST):

  | Step | Start | End | Minutes |
  |---|---|---|---|
  | 0 reading (rules, plan, Java diffs, Rust counterparts) | 22:44 | 22:52 | 8 |
  | 1 KAFKA-20854 with tests (91dcc787) | 22:52 | 23:11 | 19 |
  | 2 KAFKA-20253 coordinator + auto-commit (02ac689c) | 23:11 | 23:15 | 4 |
  | 3 KAFKA-20970 with tests (e4bf1523) | 23:15 | 23:20 | 5 |
  | 4 KAFKA-21010 + 20253 heartbeat, with tests (98e6c919) | 23:20 | 23:25 | 5 |
  | 5 bg-loop consult + proof test + teeth checks (7ffcfa5b) | 23:25 | 23:31 | 6 |
  | 6 integration (default + 4.4.0-rc4), format-check, full test, lint + fixup (73d04f5f) | 23:31 | 23:38 | 7 |
  | 7 `make -k verify` #1 | 23:38 | 23:49 | 11 |
  | 8 regression bisect + root causes (baseline worktree, tracing, broker logs) | 23:49 | 01:17 | 88 |
  | 9 fixes 150f4170, a269808f, gates, `make -k verify` #2, re-runs, notes | 01:17 | 01:40 | 23 |

- **Verification (HEAD a269808f):**
  - `cargo build` passes, and so does `cargo xtask format-check`.
  - `cargo test`: 4369 passed, 0 failed, 10 ignored (lib 4320 / 3 ignored, plus 36, 8, and 5 / 7 ignored).
  - `cargo xtask lint --keep-going`: exactly the 15 §5.1 rows; doc-hygiene, module-path and clippy are clean.
  - Broker-backed tests:
    - default 4.2.0 broker, `--test integration -- plaintext_consumer consumer_test base_consumer_test
      bootstrap_resolution_test consumer_topic_creation_test`: 101 / 101 (run before the two late fixes;
      the consumer subset was re-run after them inside verify);
    - `INTEGRATION_TEST_BROKER_TAG=4.4.0-rc4 -- bootstrap_resolution_test`: 4 / 4.
  - `make -k verify` #2 (macOS) fails in three targets:
    - `build-c`: `cmake: command not found` (environment);
    - `lint`: the 15 §5.1 rows only;
    - `test-rust-all-features`: lib 4564 passed. The integration suite had 300 passed and 5 failed, all five
      under full-suite load: `admin_scram_test::…round_trips__rust`, `consumer_bounce_test::test_async_close`,
      `consumer_bounce_test::test_async_subscribe_when_topic_unavailable` ("TopicExists"),
      `consumer_test::test_leader_epoch`, `producer_transactions_test::test_fencing_on_transaction_expiration`.
      Re-run alone they all pass: 8 / 8 including the whole `consumer_bounce_test` module, 2 ignored.

    Verify #1 had 17 integration failures: 15 in `producer_transactions_test` ("Connection refused" to a dead
    cluster; a solo re-run still failed its `grpc_*` variants and three Rust tests on timeouts, none of
    them in code this phase touched), and 2 in `consumer_bounce_test`, one of them the then-real
    `test_async_close` regression.
  - Python unit tests: 363 passed, 2 skipped. `check-bindings`: 29 passed. Soak: 156 passed.

### Phase 4 — KIP-1242 misrouted-connection detection + NetworkClient fixes (agent 94)

- ede01b871e: the `metadata.cluster.check.enable` config in `CommonClientConfigs` and the
  producer / consumer / admin configs.
- 0ef4a4c80e:
  - ApiVersions v5 carries the `ClusterId` / `NodeId` the client expects (`ApiVersionsRequest` setters).
  - `NetworkClient` (+58): send them, and handle a mismatch or `REBOOTSTRAP_REQUIRED` by disconnecting
    and rebootstrapping.
  - `MetadataUpdater.clusterId`.
- 7be741d08b: a new `GroupCoordinatorNode` (`consumer::internals`) so coordinator connections carry the
  real broker id; `Node` changes; `CoordinatorRequestManager`. Skip `AbstractCoordinator` (classic).
- KAFKA-20393 (123ee9e45d): the `stickyNode` stale-IP fix in `NetworkClient`.
- Tests: `NetworkClientTest` (+164 across commits), `GroupCoordinatorNodeTest` (+54),
  `CoordinatorRequestManagerTest`, `KafkaAdminClientTest` / `KafkaConsumerTest` hunks. Byte-level
  ApiVersions v5 encoding test (DoD #3).
- Runs after Phase 2 (both edit `NetworkClient`).

### Phase 5 — Producer: KIP-1319 TxnOffsetCommit v6 with topic IDs (agent 95)

- Sync the held-back `TxnOffsetCommit{Request,Response}.json` (§2.2): v6, `GenerationIdOrMemberEpoch`
  rename, `TopicId`, stable.
- `TxnOffsetCommitRequest`:
  - reshaped Builder: `forTopicNames` / `forTopicIdsOrNames` (7340eefc48, 2342c80dca);
  - `supportsGroupIdNotFoundError` / `supportsStaleMemberEpochError` (723847904b);
  - `getTopics`.
- `TxnOffsetCommitResponse`:
  - reshaped Builder and `newBuilder(useTopicIds)` (20c2450e5b, baa064e422);
  - `useTopicIds`;
  - topic-level structure preserved (89f3888c87);
  - 319dd61cb3's client-side hunk.
- `TransactionManager`:
  - topic IDs wired through (83976543fe);
  - `GROUP_ID_NOT_FOUND` / `STALE_MEMBER_EPOCH` handling (7f5861817d);
  - v6 marked stable (b9945c8e84).
  - Every new call site must keep the explicit `Caller` (producer-transactions.md §1).
- `KafkaProducer` + `ProducerMetadata`: refresh metadata before TxnOffsetCommit, so topic IDs are known
  (6208dfc014). The await must not hold a `TransactionManager` guard (§4).
- Deterministic topic order before encoding (producer-transactions.md §10).
- Tests: `TxnOffsetCommitRequestTest` / `TxnOffsetCommitResponseTest`, the `RequestResponseTest` hunks,
  `TransactionManagerTest` (+157), `KafkaProducerTest` (+160, incl. the 930ebc5608 deflake), `SenderTest`,
  `MessageTest`. Byte-level v6 encoding test.

### Phase 6 — Producer: rack-aware partitioning (agent 96)

- KAFKA-19193 (a3f17327de, 88b48794ea, 165d7ec933, fc18c47efd docs):
  - `partitioner.rack.aware`, and producer `client.rack` in `ProducerConfig`;
  - `BuiltInPartitioner` (+98) with rack-local load stats and trace logging;
  - `RecordAccumulator` `partitionerRackAware` / `rack` and `ConfigException` on an empty rack;
  - `KafkaProducer` wiring.
- Tests: `BuiltInPartitionerTest` (+228), and the `RecordAccumulatorTest` / `SenderTest` hunks.
- DoD #10 applies: the partitioner is on the send path.
- Runs before Phases 7–8 (they also edit `RecordAccumulator`).

### Phase 7 — Producer: KIP-1332 part A — chunked pool, stream, builder (agent 97)

- `BufferPool`:
  - `AllocationMode { Full, Incremental }`;
  - `allocate_chunks` (async, FIFO single-waiter fairness, refund on timeout / close / error);
  - the extraction of `await_memory`, `signal_next_waiter_if_memory_available`, `release_reserved_bytes`
    and `record_buffer_exhausted`;
  - the mode guards on `allocate` / `allocate_chunks`;
  - the non-blocking (0 ms) path used by extension.
- `ChunkedByteBufferOutputStream` (new file): `io::Write` across fixed 16 KiB chunks that **never grow**,
  `position()` / `set_position`, `add_buffers`, release of unused chunks on close, flatten on `buffer()`.
- `MemoryRecordsBuilder`: the single/chunked buffer type (§2.3), `estimated_bytes_written_after`, and
  `buffer_stream()`. The reopen/rewrite path must work with the flattened buffer.
- Tests: `BufferPoolChunkAllocationTest` (+402), `ChunkedByteBufferOutputStreamTest` (+312). Rust-only
  tests: a cancelled `allocate_chunks` neither leaks waiters nor memory; no chunk grows past its capacity.
- DoD #10: re-run the hot-path allocation test, and do a throughput A/B on the default `full` path to show
  no regression.

### Phase 8 — Producer: KIP-1332 part B — accumulator, batch, producer wiring (agent 98)

- `RecordAccumulator` refactor (+164/−64): `topic_info_for`, `partition_changed`, `set_partition`,
  `update_partition_info_on_append`, `append_new_batch` taking the try-append / create-batch steps,
  `RecordAppendResult::needs_extension`, and batch-level deallocate hooks.
- `ChunkedRecordAccumulator` (new file, composition per §2.3):
  - `append` with the extension path and the new-batch path;
  - a shared `max.block.ms` budget;
  - refund of chunks when the partition changes;
  - the `AppendGuard` extended to new-batch and extension chunks.
- `ProducerBatch` + `chunked_producer_batch.rs`: `extension_bytes_needed`, `add_buffers`, chunk-aware
  `deallocate_buffer` / `deallocate_inflight_buffer`, `is_chunked`. Remove `is_writable` if Rust has it.
- `ProducerConfig` `buffer.memory.allocation.strategy` (internal, default `full`, case-insensitive), plus
  `KafkaProducer` wiring:
  - fall back with a warning when `batch.size` < 16 KiB;
  - `ConfigException` when compression ≠ none.
- **D3:** KAFKA-20864 (`cc6d42206f`, trunk): close the batch only if it is the one being extended; bound
  retries by the remaining `max.block.ms`. This also covers its `RecordAccumulatorTest` (+51) and
  `ChunkedRecordAccumulatorTest` (+567) additions. Record it as ahead-of-4.4.
- Tests: `ChunkedRecordAccumulatorTest` (+716, plus the D3 additions), `ProducerConfigTest` (+35), and the
  `KafkaProducerTest` / `RecordAccumulatorTest` hunks. Integration: rerun the producer send integration
  tests with `buffer.memory.allocation.strategy=incremental`
  (`IncrementalAllocationProducerSendTest` / `BaseProducerSendTest` analog).
- Deliverable: draft the `ProducerBatch`-fold rules note into `rules-errata.md` (§2.3).
- DoD #10 applies.

### Phase 9 — Consumer: heartbeat, membership, commit fixes (agent 99)

- KAFKA-20253 (28de22de34): heartbeat CPU spin, in `AbstractHeartbeatRequestManager`,
  `CommitRequestManager` and `CoordinatorRequestManager`.
- KAFKA-20761 (56410c311b): log the group configs defined on the broker.
- b8429b93a7: no-op call removed.
- KAFKA-20681 (6a6b536fbc): heartbeat-success handling consolidated into `AbstractMembershipManager`
  (consumer half only).
- KAFKA-20145 (e320142b7c): no redundant partial heartbeat acks from network-thread reconcile.
- KAFKA-20765 (fe88647935): OffsetFetch stale-epoch retry spinning.
- Test consolidation e7b0cb7908 (`AbstractHeartbeatRequestManagerTest` +330): mirror it if Rust keeps
  separate files, otherwise record how it maps.
- Tests: `ConsumerHeartbeatRequestManagerTest`, `AbstractHeartbeatRequestManagerTest`,
  `ConsumerMembershipManagerTest` (+44), `CommitRequestManagerTest` (+60), `CoordinatorRequestManagerTest`.
- Check every change against the consumer-threading §31 reconcile / ack machinery (Phase 41 design).

### Phase 10 — Consumer: fetch, offsets, poll, MockConsumer (agent 100)

- KAFKA-20187 (65ffe10e3b): the `endOffsetRequested` flag in `OffsetsRequestManager` (+83) and
  `ApplicationEventProcessor` (−33).
- KAFKA-20312 (d0e0ec478c): a null leader during regroup, in `OffsetFetcherUtils` and
  `OffsetsRequestManager`.
- KAFKA-20780 (20e952c783): clear a completed in-flight poll on an empty fetch.
- KAFKA-15529 (5d03ccff57): the `isConsumed` / position race in `CompletedFetch` / `FetchCollector`.
- KAFKA-18812 (8c0ca4ae35): API errors after a background task failure, in `ApplicationEventHandler` and
  `ConsumerNetworkThread`.
- KAFKA-20570 (f7dbf0bf3b): `ConsumerProtocol` deserialization errors become `SchemaError` / `KafkaError`.
- KAFKA-20575 (2768948823): `MockConsumer::lose_partitions` (public), plus the FFI
  `kafka_consumer_MockConsumer_lose_partitions` and the Python mock binding.
- b69c07c816 ("Bound decompressed record size"), **partial**: consumer-side `DefaultRecord.readFrom` checks
  only (the new "Invalid record size: N is negative." message and the `SOFT_MAX_ARRAY_LENGTH` upper bound;
  Rust already rejects a negative size with different text, `default_record.rs`); the broker-side
  `maxRecordBodySize` iterators stay out of scope. Reclassified from Broker/O by the Manager after the
  Phase 1 review (COMMENTS.91 Q2).
- Test-only commits:
  - a5137f7c38, 624ca392ef, 7c010c7583, 1a46339e90, 16e976ac8e, 159d696005, c75e10d229 (KafkaConsumerTest
    hunks);
  - 36aab4fddd and 40e9fcd742 (pause/re-assign and unsubscribe-no-autocommit tests, plus rustdoc);
  - 0fd8327920 (single in-flight poll event).
- Tests: `OffsetsRequestManagerTest` (+60), `FetchCollectorTest` (+32), `ConsumerProtocolTest` (+85, unit
  tests in `consumer_protocol.rs` per the consumer-threading §20 carve-out), `MockConsumerTest` (+105),
  `ApplicationEventHandlerTest`, `ConsumerNetworkThreadTest`, and the `AsyncKafkaConsumerTest` /
  `KafkaConsumerTest` / `FetchRequestManagerTest` hunks.
- DoD #10 applies (`CompletedFetch` / `FetchCollector` are on the per-record receive path; §27).

### Phase 11 — Consumer metrics: sensor lifecycle (agent 101)

- KAFKA-19542 (9a28bd23ad):
  - `MetricsLedger` (new);
  - `AbstractConsumerMetricsManager` (new);
  - `SensorBuilder` moved to the `consumer.internals.metrics` package (mirror it if the Rust layout
    mirrors that package);
  - every metrics manager removes its sensors on close;
  - `AsyncKafkaConsumer` / `FetchMetricsManager` wiring.
- KAFKA-20750 (02c0ce9707): divide-by-zero guard in `KafkaConsumerMetrics`.
- c39e2af92c (`ConsumerMetrics` wrapper removed): record N/A if Rust never had it; apply the
  `FetchMetricsRegistry` hunk.
- Tests: the metrics-manager tests, a `KafkaConsumerTest` close-removes-all-sensors assertion, and the
  divide-by-zero test.

### Phase 12 — Admin (agent 102)

- KAFKA-20395 (c274a7348f): `unregister_controller` / `unregister_controller_with_options`
  (admin-client.md §1 shape), `UnregisterControllerOptions/Result`, the
  `UnregisterControllerRequest/Response` wrappers with byte-level tests, the `MockAdminClient` behaviour
  per Java's mock, and C FFI sync + async plus Python `admin.py` sync + asyncio.
- 58f63f448e: complete the `nodeApiVersions` future when `describeFeatures` fails.
- KAFKA-19663 (3b849ff2bd): `InternalDescribeFeaturesResult` (crate-private) and the
  `DescribeFeaturesResult` delta.
- KAFKA-20673 follow-up (cb2f143b0d): skip the stale-leader lookup retry while closing.
- Tests: the `KafkaAdminClientTest` slices (Java split it into per-domain classes in 1a443b2d23; map them
  to the Rust test layout), `RequestResponseTest` hunks, and FFI / Python tests.
- Can run as soon as Phase 2 lands (both edit `KafkaAdminClient`).

### Phase 13 — Close-out (agent 103)

- **D1:** if `4.4.0` final is tagged, re-diff `4.4.0-rc4..4.4.0` across clients and specs. Port any delta,
  then bump the submodule, `AUDIENCE_REF`, `DEPRECATION_REFS` and the refreshed audience/deprecation lists
  (`cargo xtask fetch-java-refs`).
- Rustdoc sync for the 21 doc-only commits that touch translated types. The largest is 67850b0b68
  (+427 consumer javadoc); also 6ce2682d7b `auto.offset.reset` / `by_duration`, and 05185c1ef8 return
  semantics.
- **Completeness audit:** a table of every commit in §6 mapped to its phase or skip reason, appended here.
- `make verify` (macOS caveats: PIP_INDEX_URL override, Docker arms are Linux-CI-only), and updates to
  `MILESTONES.md` and `design/current/status.md`.
- Hand the drafted `rules-errata.md` amendments to the user.

## 4. Ordering and parallelism

```
0 → 1 ─┬─ network/consumer:  2 → 3 → 4 → 9 → 10 → 11
       ├─ producer:          5 → 6 → 7 → 8
       └─ admin:             (after 2) 12
                                          → 13
```

- **Phase 1 goes first:** its module moves rename imports crate-wide.
- **Network/consumer track (2 → 3 → 4 → 9 → 10 → 11):** this is the critical path. Phase 2 precedes 3 and
  4 because all three edit `NetworkClient` and the request managers. Phases 9 → 10 → 11 run in that order
  because they share `AbstractHeartbeatRequestManager`, `AsyncKafkaConsumer` and the metrics managers.
- **Producer track (5 → 6 → 7 → 8):** independent of the network work, apart from a small `KafkaProducer`
  constructor overlap with Phase 2, which resolves at merge time.
- **Admin (12):** needs only Phase 2.
- **Run in parallel**, the critical path is 0 → 1 → 2 → 3 → 4 → 9 → 10 → 11 → 13: nine loops instead of
  fourteen. Each track uses its own worktree (beware the macOS `/tmp` reaper for scratchpad worktrees) and
  merges into the milestone branch with merge commits.

## 5. DoD notes

- DoD applies in full in every phase:
  - Java tests are translated, with exact error messages asserted.
  - `@RepeatedTest` becomes a loop.
  - Changed encodings get byte-level tests (ApiVersions v5, DeleteGroups v3, TxnOffsetCommit v6,
    UnregisterController).
  - No TODO / FIXME, even where Java leaves TODOs (e.g. KIP-1332's compression TODO becomes an explicit
    `ConfigException` / error path, as Java's constructor does).
- **DoD #10** (hot-path allocation audit) applies to Phases 1 (reader bounds), 6, 7, 8 and 10. State N/A
  explicitly elsewhere.
- **DoD #7:** two justified deviations are recorded up front: the `ProducerBatch` fold and the builder
  buffer type (§2.3). D4 adds a crate-private error kind.
- **Out-of-scope tests** are skipped with the standing reasons. Each phase lists which tests it skipped.
  These include `ShareHeartbeatRequestManagerTest`, `StreamsGroupHeartbeatRequestManagerTest`,
  `KafkaShareConsumerTest`, `ConsumerCoordinatorTest`, `AbstractCoordinatorTest`, `OffsetFetcherTest`
  (classic), `RaftVoterEndpointTest`, the Streams admin tests, and the 1a443b2d23 test-class split
  (a reorganisation).

### 5.1 Known lint findings from the 4.4 submodule bump

`cargo xtask lint-custom` (`check-java-name`) resolves every `#[doc(alias = "org.apache.kafka...")]`
marker against the `kafka/` working tree. Phase 0's bump to 4.4.0-rc4 left the 45 findings below. Each
names a Java class or method that 4.4 moved or deleted as part of a change a later phase ports, so none
can be fixed in Phase 0 (the TxnOffsetCommit pair is barred by §2.2). The other 113 findings the bump
produced were pure relocations and were fixed in Phase 0.

**Gate rule (user decision, 2026-10-07; no tooling change):**
- A phase passes lint if `cargo xtask lint` reports nothing outside this table **and** that phase's own
  rows are gone.
- Each phase deletes its rows here in the commit that fixes them.
- Phase 13 requires this table to be empty and `cargo xtask lint` fully green.

Count by owner: Phase 5 2, Phases 7/8 2, Phase 9 1, Phase 11 10. (Phase 1 cleared its 30: 25 D2 moves + 5 throttle.)

| Rust item | Java marker | Cause | Owner |
|---|---|---|---|
| `rust/src/common/requests/txn_offset_commit_request.rs` `get_error_response_topics` | `common.requests.TxnOffsetCommitRequest#getErrorResponseTopics` | Removed by KIP-1319 (baa064e422); TxnOffsetCommit held back by §2.2 | Phase 5 |
| `rust/src/common/requests/txn_offset_commit_response.rs` `errors` | `common.requests.TxnOffsetCommitResponse#errors` | Removed by KIP-1319 (89f3888c87); TxnOffsetCommit held back by §2.2 | Phase 5 |
| `rust/src/producer/internals/producer_batch.rs` `is_writable` | `clients.producer.internals.ProducerBatch#isWritable` | Removed by KIP-1332 incremental allocation (KAFKA-20578, 1aed299b3e) | Phases 7/8 |
| `rust/src/producer/internals/record_accumulator.rs` `records_builder` | `clients.producer.internals.RecordAccumulator#recordsBuilder` | Removed by KIP-1332 incremental allocation (KAFKA-20578, 1aed299b3e) | Phases 7/8 |
| `rust/src/consumer/internals/consumer_membership_manager.rs` `on_heartbeat_success` | `clients.consumer.internals.ConsumerMembershipManager#onHeartbeatSuccess` | Consolidated into `AbstractMembershipManager` (KAFKA-20681, 6a6b536fbc) | Phase 9 |
| `rust/src/consumer/internals/sensor_builder.rs` `SensorBuilder` | `clients.consumer.internals.SensorBuilder` | Moved to `consumer.internals.metrics` (KAFKA-19542, 9a28bd23ad) | Phase 11 |
| `rust/src/consumer/internals/sensor_builder.rs` `new` | `clients.consumer.internals.SensorBuilder#SensorBuilder` | Moved to `consumer.internals.metrics` (KAFKA-19542, 9a28bd23ad) | Phase 11 |
| `rust/src/consumer/internals/sensor_builder.rs` `with_tags` | `clients.consumer.internals.SensorBuilder#SensorBuilder` | Moved to `consumer.internals.metrics` (KAFKA-19542, 9a28bd23ad) | Phase 11 |
| `rust/src/consumer/internals/sensor_builder.rs` `with_avg` | `clients.consumer.internals.SensorBuilder#withAvg` | Moved to `consumer.internals.metrics` (KAFKA-19542, 9a28bd23ad) | Phase 11 |
| `rust/src/consumer/internals/sensor_builder.rs` `with_min` | `clients.consumer.internals.SensorBuilder#withMin` | Moved to `consumer.internals.metrics` (KAFKA-19542, 9a28bd23ad) | Phase 11 |
| `rust/src/consumer/internals/sensor_builder.rs` `with_max` | `clients.consumer.internals.SensorBuilder#withMax` | Moved to `consumer.internals.metrics` (KAFKA-19542, 9a28bd23ad) | Phase 11 |
| `rust/src/consumer/internals/sensor_builder.rs` `with_value` | `clients.consumer.internals.SensorBuilder#withValue` | Moved to `consumer.internals.metrics` (KAFKA-19542, 9a28bd23ad) | Phase 11 |
| `rust/src/consumer/internals/sensor_builder.rs` `with_meter` | `clients.consumer.internals.SensorBuilder#withMeter` | Moved to `consumer.internals.metrics` (KAFKA-19542, 9a28bd23ad) | Phase 11 |
| `rust/src/consumer/internals/sensor_builder.rs` `with_meter_stat` | `clients.consumer.internals.SensorBuilder#SensorBuilder` | Moved to `consumer.internals.metrics` (KAFKA-19542, 9a28bd23ad) | Phase 11 |
| `rust/src/consumer/internals/sensor_builder.rs` `build` | `clients.consumer.internals.SensorBuilder#build` | Moved to `consumer.internals.metrics` (KAFKA-19542, 9a28bd23ad) | Phase 11 |

## 6. Commit classification (input to the Phase 13 audit)

The full 190-row table (SHA, component, classification, subject, line counts) is in
[`commits.md`](commits.md), generated from
`git -C kafka log --no-merges --right-only --cherry-pick 4.3.1...4.4.0-rc4 -- clients/src`.
The summary by component:

| Component | Total | Port (P) | Test-only (T) | Doc | No-op | Out of scope (O) / reverted (R) / already in 4.3.1 (A) |
|---|---|---|---|---|---|---|
| Consumer | 50 | 19 | 11 | 6 | 7 | R 5, A 2 |
| Common | 33 | 3 | 1 | 6 | 21 | O 2 |
| Streams | 24 | – | – | – | – | O 24 |
| Wire | 16 | 12 | – | 1 | 3 | – |
| Producer | 14 | 9 | 1 | 1 | 2 | R 1 |
| Broker | 12 | – | – | – | – | O 12 |
| Network | 12 | 8 | – | 3 | 1 | – |
| Share | 11 | – | – | – | – | O 11 |
| Admin | 11 | 4 | – | 4 | 2 | O 1 (Raft voter) |
| Classic | 5 | – | – | – | – | O 5 |
| Security | 2 | – | – | – | 1 | O 1 |
| **Total** | **190** | **55** | **13** | **21** | **37** | **O 56, R 6, A 2** |

Phase → P/T commit counts (sum 68): P0 2 (plus the spec-only syncs), P1 7, P2 4, P3 3, P4 4, P5 13, P6 3,
P7+P8 1 (+1 trunk commit for D3), P9 7, P10 18, P11 2, P12 4. (b69c07c816 moved from Broker/O to Consumer/P for Phase 10 after the
Phase 1 review.)
