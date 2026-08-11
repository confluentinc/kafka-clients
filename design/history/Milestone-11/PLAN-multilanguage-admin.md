# Milestone 11 — Admin multilanguage gRPC harness

Producer and consumer scenarios are written once in Rust and executed against
every language binding over gRPC (`RustNative` / `PythonGrpc` /
`PythonAsyncGrpc` / `CGrpc`). Producer has 70 such test entries and consumer
44. **Admin has none**, so the Rust core, the C FFI and the Python binding
have never been compared against each other. This plan closes that.

The gap is not theoretical: `ConsumerGroupDescription.coordinator()` returned a
fabricated `Node` (`host=""`, `port=-1`) and passed 3389 Rust, 249 C and 261
Python tests, because every one of those suites checks a single language
against its own expectations. A differential harness catches that class of
defect without anyone having to guess the right answer in advance — three
backends agree and one does not.

## 0. Scope

In scope: all 46 in-scope Admin RPCs (the same set enumerated in
`PLAN-bindings.md`), exposed over a new `AdminService`, driven from Rust
scenarios against four backends.

Out of scope: new Admin RPCs, changes to `src/admin` behaviour, and changes to
the production `Admin` trait or the `*Result` types. If a scenario reveals a
core defect, it is reported, not silently fixed inside this work.

## 1. Existing architecture this must follow

| Piece | Producer/consumer precedent |
|---|---|
| Proto | `multilanguage-test-server/proto/{producer,consumer}_service.proto`, compiled by `build.rs` via `tonic_build` with `protoc_bin_vendored` |
| Rust client | `tests/common/multilanguage_{producer,consumer}.rs` |
| Factory | `tests/common/backend_factory.rs` — `ProducerBackendFactory` / `ConsumerBackendFactory`, gRPC impls behind `#[cfg(feature = "multilanguage-tests")]` |
| Backend pool | `tests/common/backend_pool.rs` — `BackendKind`, one container per `(kind, broker_network)`, testcontainers-mapped ports |
| Macro | `tests/common/multilanguage_{test,consumer_test}_macro.rs` — one scenario name expands to four `#[tokio::test]`s with a `__grpc_` infix |
| Python server | `bindings/python/grpc_server.py` (sync) + `grpc_server_async.py` (asyncio), sharing `grpc_translate.py` |
| C++ server | `bindings/c/grpc_server/server.cc`, driving the **bare sync** C entry points |

Two properties of the precedent are load-bearing and must be preserved:

- **Blocking methods are awaited server-side.** The unary response *is* the
  resolved result; no futures or streaming cross the wire. Stated explicitly at
  `consumer_service.proto:33`.
- **The shared `__grpc_` infix** is what lets `make test-rust-all-features`
  exclude every container-backed arm with one `--skip __grpc`.

## 2. Decisions

### D1. The harness gets its own `AdminBackend` trait — the production `Admin` trait is not implementable from `tests/`

`MultilanguageConsumer` implements the production `Consumer` trait, so consumer
scenarios are generic over the real trait. **Admin cannot do this**, for two
independent reasons found by inspection:

- 33 of the 46 `*Result` types declare `pub(crate) fn new` (e.g.
  `list_topics_result.rs:34`, `list_offsets_result.rs:33`). Integration tests
  in `tests/` are a separate crate and cannot construct them.
- Completing a future later requires `KafkaFutureImpl`
  (`src/common/kafka_future.rs:377`), also `pub(crate)`. The only public
  constructor is `KafkaFuture::completed`.

Both visibilities are **correct** and must not be widened: Java's
`CreateTopicsResult` constructor is package-private and `KafkaFutureImpl` lives
in `org.apache.kafka.common.internals`, which CLAUDE.md maps to `pub(crate)`.
Widening them to make a test helper compile would diverge from Java.

So define a harness-local trait in `tests/common/admin_backend.rs`:

```rust
#[allow(async_fn_in_trait)]
pub trait AdminBackend {
    async fn create_topics(&self, topics: Vec<NewTopicSpec>, opts: CreateTopicsOpts)
        -> Result<HashMap<String, Result<(), KafkaError>>, KafkaError>;
    // ... one method per RPC, returning already-resolved plain data ...
    async fn close(&self, timeout: Option<Duration>) -> Result<(), KafkaError>;
    fn name(&self) -> &'static str;
}
```

Two implementations: `RustNativeAdmin` (owns a `Box<dyn Admin>`, calls the real
sync method then awaits the returned `KafkaFuture`s) and `MultilanguageAdmin`
(one gRPC round-trip per call).

**DoD #7 justification** (structs not present in Java): `AdminBackend` is test
scaffolding, exactly as `ConsumerBackendFactory` and `ProducerBackendFactory`
already are — neither exists in Java either. It models what actually crosses a
language boundary: both bindings already collapse per-key futures before
returning (Python `_run_sync` hands back a resolved dict; the C `_async` entry
points fire their callback with a fully-built result struct), so resolved plain
data is the honest wire shape, not a simplification. Per-key granularity is
still asserted — it is carried as a `HashMap<K, Result<V, KafkaError>>` rather
than as futures.

**Consequence to accept knowingly:** the harness cannot assert that an Admin
method *returns before* its future resolves. That property is untestable
through any binding (both are eager at the boundary) and stays covered by the
Rust unit tests in `src/admin`, which use the real trait.

### D2. Slicing mirrors the bindings slices, but a thin vertical goes first

The RPC grouping from `PLAN-bindings.md` (B0–B6) is reused verbatim as G0–G6 so
each slice is traceable to the binding slice it exercises. Ordering deviates in
one way: **G1 carries one scenario end-to-end through all four backends before
any further RPCs are added.** The proto shape for per-key results is the one
decision that is expensive to revisit once 46 RPCs depend on it, so it gets
validated against three independently-written servers first.

| Slice | Content |
|---|---|
| G0 | `admin_service.proto` skeleton + shared messages, `build.rs` wiring, `AdminService` stubs in both servers, `MultilanguageAdmin`, `AdminBackendFactory`, `multilanguage_admin_test!`, `BackendKind` reuse, `create`/`close` only |
| G1 | Topics & partitions (6): `createTopics`, `deleteTopics`, `listTopics`, `describeTopics` (names **and** ids), `createPartitions`, `deleteRecords` — **plus proving one scenario green on all four backends** |
| G2 | Cluster, configs, log dirs (8) |
| G3 | Elections, reassignments, offsets (4) |
| G4 | Groups & offsets (9) |
| G5 | ACLs, quotas, SCRAM, tokens, features (13) |
| G6 | Producers & transactions (6) + scenario sweep and the README/plan updates |

### D3. Scenario inventory is converted, not invented

The **primary** source is the 38 already-committed real-broker admin
integration tests (`tests/integration/admin_*_test.rs`, 3,235 lines across 12
files). They are Rust, they already use `TestContext`, and they currently run
against `RustNative` only. Converting each into a scenario body generic over
`AdminBackend` and registering it via `multilanguage_admin_test!` turns 38
single-backend tests into ~152 test entries — which is exactly how producer
reached 70 and consumer 44.

Conversion is mechanical because these tests only ever *read* `*Result` types:

```rust
// today
let admin = admin_for(ctx.bootstrap_servers());
let names = admin.list_topics(ListTopicsOptions::new()).names().get().await?;

// converted
let admin = factory.create(admin_config(&bootstrap_for(factory, ctx))).await?;
let names = admin.list_topics().await?;
```

This is also the empirical proof of D1: those 38 tests read results through
public accessors, and none of them constructs a `*Result` — construction is the
part that is `pub(crate)`, and it is exactly what a gRPC proxy would need.

The **secondary** source is `~/Desktop/ckr-apitest` (836 C checks, 718 Python
checks, all 46 RPCs), which covers edge cases the committed tests do not:
partial-failure batches, `validate_only` proven non-mutating, duplicate keys,
unicode byte-exactness through values *and* error messages, `None` vs `""` vs
absent, empty batches, boundary numerics. Those are ad-hoc and uncommitted;
this harness is where the ones worth keeping become permanent.

Where a committed test and an ad-hoc probe disagree, the committed test wins
unless the Java source says otherwise.

Cases that a single PLAINTEXT node cannot reach are recorded, not silently
skipped — the four delegation-token RPCs are error-path only, ACL denial is
unreachable (`User:ANONYMOUS` is a super user), and `electLeaders` reaches only
`ELECTION_NOT_NEEDED(84)`.

> **Correction (written at G6, after the slices ran).** Two of those three
> parenthetical claims were **wrong**, and so were three more recorded later. The
> pattern is consistent enough to be worth stating as a rule rather than as five
> separate corrections: *an "unreachable" verdict reached by reasoning about the
> fixture, rather than by running a purpose-built one, was wrong more often than
> it was right.* Six of them were tested at G6 and five fell:
>
>   - **ACL denial is reachable.** Not by resolving super-user status alone: with
>     `super.users` not naming `User:ANONYMOUS` the broker refuses to *start*,
>     because it authenticates to itself as `User:ANONYMOUS` over its PLAINTEXT
>     controller listener. Adding `allow.everyone.if.no.acl.found=true` fixes that
>     and an explicit DENY still binds (`StandardAuthorizer` gives a matching DENY
>     precedence over the implicit allow). Closed in G5 by
>     `authorizer_deny_reachable_single_broker`.
>   - **`PartitionReassignment{replicas, adding, removing}` is reachable**, and
>     deterministically: three brokers plus replication throttled to 1 KiB/s over
>     ~2 MiB stretches a move to ~30 minutes. Closed in G3.
>   - **`describeCluster`'s `authorizedOperations` null branch is reachable** — by
>     *not asking for it*, which the same scenario already did while its comment
>     called the branch unreachable. Comment corrected in G6.
>   - **`describeClassicGroups`' value arm is reachable**: an
>     `alterConsumerGroupOffsets` call on a never-consumed group id creates a
>     simple classic group. Closed in G6 by `describe_a_simple_classic_group`,
>     which is also what finally pointed `assert_real_coordinator` at the *second*
>     coordinator decode site.
>   - **`updateFeatures`' success arm and three of four `UpgradeType`s are
>     reachable** — on `MockAdminClient`, which the features slice never asked even
>     though the delegation-token slice in the same commit asked exactly that
>     question of itself. Closed in G6.
>   - **A real delegation token is genuinely unreachable**, and this one was
>     re-measured rather than assumed: with and without
>     `delegation.token.secret.key` all four RPCs answer
>     `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED(64)` byte-identically, because
>     `KafkaApis.allowTokenRequests` (`KafkaApis.scala:2345-2354`) gates on the
>     *client's* security protocol and no broker configuration moves it. The
>     blocker is on the client side (`AdminClientConfig` has no
>     `security.protocol`), so the marshaling was closed against the mock instead.
>
> The four questions that actually decide such a verdict, in the order they
> should be asked, are in §5 below.

### D4. Each server drives the same surface its own binding exposes

- Python sync server → `admin.py` `AdminClient` / `MockAdminClient`.
- Python async server → `AsyncAdminClient` / `AsyncMockAdminClient`.
- C++ server → the **bare sync** C entry points, as `server.cc` already does
  for the consumer. The `_async` variants are covered by the committed C unit
  tests; re-testing them here would trade differential coverage for redundancy.

## 3. Environment notes

- `docker pull` on the dev machine fails with a credential-helper timeout.
  Bypass with a scratch `DOCKER_CONFIG` (`{"auths":{}}`, no `currentContext`)
  **plus** an explicit `DOCKER_HOST=unix:///Users/pratyush/.docker/run/docker.sock`.
  Both base images (`python:3.11-slim`, `debian:trixie-slim`) are now cached.
- Point `CARGO_TARGET_DIR` outside the repo for container builds, or Linux
  artifacts land in the working tree.
- `cargo test` accepts only one positional filter.
- Host has no `cmake`, so `make build-c` / `test-c` / `verify` and the
  `.githooks/pre-commit` hook cannot pass locally; commit with `--no-verify`
  and leave `cargo xtask lint` to a Linux runner.

### Finding: the gRPC images are not buildable on macOS as committed

All three Dockerfiles consume **host-built** Rust artifacts —
`bindings/c/Dockerfile.grpc:43` copies `target/release/libconfluent_kafka.a`
and `bindings/python/Dockerfile.grpc:36` copies
`target/release/libconfluent_kafka.so`. On macOS the archive is Mach-O, so the
in-container GNU `ld` fails with `archive has no index; run ranlib to add one`,
and the `.so` does not exist at all because cargo emits
`libconfluent_kafka.dylib`. Architecture is **not** the cause: host and
container are both arm64/aarch64. CI is Linux, where the host artifact is
already ELF, which is why this has never surfaced.

Worked around locally by building the Rust artifacts for Linux in a container
and assembling a scratch build context (the repo tree is only read, so it is
safe to run alongside an editing agent). Making the committed Dockerfiles
host-independent — e.g. a builder stage that runs cargo in-container — is a
deliberate infra change and is **out of scope here**; it is recorded as a
finding for a separate decision.

## 4. Definition of Done

Per slice: `cargo build`, the new tests green on **all four** backends,
`cargo xtask format-check`, `cargo xtask lint`, `cargo xtask check-bindings`,
and no regression in the existing producer/consumer multilanguage arms. A slice
is not done while any backend is skipped for convenience — a backend that
cannot run must be reported as a blocker, not quietly dropped.

---

# 5. Final state (written at G6, when the plan was complete)

The harness is finished: **46 of 46 in-scope Admin RPCs**, driven from Rust
scenarios against four backends. This section is the permanent record — the
counts, the envelope vocabulary that 46 RPCs settled on, and every state proven
unreachable with its citation.

## 5.1 RPC coverage: 46 / 46

Counted against the slice table in §D2, which reuses `PLAN-bindings.md`'s
grouping verbatim.

| Slice | RPCs | Names |
|---|---|---|
| G0 | 0 | lifecycle only (`CreateAdmin` / `Close`) |
| G1 | 6 | `createTopics`, `deleteTopics`, `listTopics`, `describeTopics`, `createPartitions`, `deleteRecords` |
| G2 | 8 | `describeCluster`, `describeConfigs`, `incrementalAlterConfigs`, `listConfigResources`, `listClientMetricsResources`, `describeLogDirs`, `alterReplicaLogDirs`, `describeReplicaLogDirs` |
| G3 | 4 | `electLeaders`, `alterPartitionReassignments`, `listPartitionReassignments`, `listOffsets` |
| G4 | 9 | `listGroups`, `listConsumerGroups`, `describeConsumerGroups`, `describeClassicGroups`, `listConsumerGroupOffsets`, `alterConsumerGroupOffsets`, `deleteConsumerGroupOffsets`, `deleteConsumerGroups`, `removeMembersFromConsumerGroup` |
| G5 | 13 | `createAcls`, `describeAcls`, `deleteAcls`, `describeClientQuotas`, `alterClientQuotas`, `describeUserScramCredentials`, `alterUserScramCredentials`, `createDelegationToken`, `renewDelegationToken`, `expireDelegationToken`, `describeDelegationToken`, `describeFeatures`, `updateFeatures` |
| G6 | 6 | `describeProducers`, `describeTransactions`, `abortTransaction`, `forceTerminateTransaction`, `listTransactions`, `fenceProducers` |
| | **46** | |

`deleteTopics` and `describeTopics` each have **two** `AdminBackend` methods
(by names / by ids) rather than one taking a `TopicCollection`, because both
bindings split them and the result's key type changes with the collection kind —
counted as one RPC each, as Java does.

## 5.2 Entry counts

Measured on the branch at G6, not estimated:

| Invocation | Admin entries | Note |
|---|---|---|
| `cargo test --features integration-tests --test integration -- --list` | **77** `__rust` | 0 contain `__grpc`; the committed single-backend coverage the scenarios replaced |
| `cargo test --features integration-tests,multilanguage-tests --test integration -- --list` | **312** | 78 `__rust` + 234 `__grpc`, i.e. 78 scenarios × 4 backends |

312 = 308 in the thirteen `tests/integration/admin_*_test.rs` files + 4 in
`multilanguage_admin_test.rs` (G0's create/close vertical, which is
`multilanguage-tests`-only because it has no single-backend value). For scale,
producer has 70 entries and consumer 44 in the same binary.

The reason the two rows differ by more than a factor of four is §D2's
deviation: the three container arms are individually
`#[cfg(feature = "multilanguage-tests")]` *inside* `multilanguage_admin_test!`,
so an invocation still expands to `__rust` without the feature. That is what
made replacing the committed tests lossless rather than moving their coverage
behind a feature flag (which is what the producer suite did).

## 5.3 The envelope: four shapes, and why exactly four

G0 designed one shape. 46 RPCs needed four. Each was added because a *Java*
result shape could not be expressed by the ones already there, and the choice for
every RPC was made by reading its `src/admin/*_result.rs` future shape rather
than by analogy with a similar-sounding RPC.

**(1) Ordinary per-key `oneof`** — `Map<K, KafkaFuture<V>>`.

```
message <Rpc>Response { repeated <Rpc>Entry entries = 1; optional KafkaError error = 2; }
message <Rpc>Entry { ResultKey key = 1; oneof outcome { KafkaError error = 2; <V> value = 3; } }
```

`oneof` rather than two independent `optional` fields because one `KafkaFuture`
resolves to exactly one of value/error, so "both set" and "neither set" should not
be representable. The top-level `error` is not redundant: it carries failures that
precede any per-key future — a synchronous throw, an unknown `admin_id`, a
transport failure — and `entries` is then empty.

**(2) `VoidKeyedResponse`** — `Map<K, KafkaFuture<Void>>`, ~20 of the 46. There is
no value to carry, so an *absent* per-key error is the success signal, and the
shared `ResultKey` (nine variants by G6) lets all of them reuse one message
instead of needing one near-identical `<Rpc>Entry` each.

**(3) Whole-value** — the `*Result` holds a *single* future (or several
independent ones over attributes of one thing), so a per-key error arm would be
permanently dead. Twelve RPCs: `listTopics`, `describeCluster`,
`listConfigResources`, `listClientMetricsResources`,
`listPartitionReassignments`, `describeAcls`, `describeClientQuotas`,
`createDelegationToken`, `renew`/`expireDelegationToken` (which share
`DelegationTokenExpiryResponse`), `describeDelegationToken`, `describeFeatures`.
Two degenerate to *void* and therefore reuse the shared `StatusResponse`:
`abortTransaction` and `forceTerminateTransaction`.
`listGroups` / `listConsumerGroups` are this shape carrying Java's
`valid()`/`errors()` split, whose `errors()` is an **unkeyed**
`Collection<Throwable>` — which is precisely why they cannot be shape (1).

**(4) Value carries its own error** — the per-key future resolves *successfully*
while the value reports a failure inside itself. Exactly three of the 46, and the
set is now closed: `createTopics` (`TopicMetadataAndConfig`), `describeLogDirs`
(`LogDirDescription.error`), `deleteAcls` (`FilterResult { binding, exception }`).
A consumer must inspect the value's error as well as the entry's outcome. The
`deleteAcls` case is also the one where a *conversion* silently dropped the inner
check (round-16 item 1), which is why `deleted_bindings` now performs Java's fold
so the next caller cannot forget it.

**Rejected**: one mega `KeyedResult` unioning all 46 value types. It would be a
single envelope, but it deletes the compile-time guarantee that each RPC returns
its own value shape, and hands each of the three independently written servers a
wrong-variant path that only fails at runtime — in a harness whose entire purpose
is catching cross-language disagreement.

**Two-level values need no flattening.** `describeLogDirs`
(broker → log dir → `ReplicaInfo`), `listConsumerGroupOffsets`
(group → partition → nullable offset) and `describeProducers`
(partition → list of `ProducerState`) all keep their nesting inside the entry's
`oneof`; a `map` field cannot sit directly in a `oneof`, hence the one-field
wrapper messages (`LogDirDescriptionMap`, `GroupOffsets`,
`PartitionProducerState`, `TransactionListingList`), following `NodeList`.

**Enum encoding follows what the bindings already own.** An enum with a real wire
number crosses as that number (`AclOperation.code()`, `ScramMechanism.type()`,
`FeatureUpdate.UpgradeType.code()`); an enum with no numeric id crosses as its
Java constant name (`ConfigSource`, `ConfigType`, `GroupState`, `GroupType`,
`ClassicGroupState`, `TransactionState`). For the name-encoded ones, a name that
`parse`s to `Unknown` without spelling `"Unknown"` is a protocol error, because
`parse` is permissive by design and would otherwise absorb a garbled field into a
valid value. `OffsetSpec` crosses as a **named kind** rather than as the
`(is_timestamp, sentinel)` pair both bindings take, so that the six-sentinel table
is stated independently by each server instead of forwarded unexamined.

## 5.4 Every state proven unreachable, consolidated

The rule this table exists to enforce: a state recorded here must have a
*citation*, and the citation must say why no fixture reaches it — not merely that
the current one does not. Anything that fails that test was covered instead (see
the correction at the end of §D3 for the five that did).

| State | Why unreachable | Citation |
|---|---|---|
| `TopicMetadataAndConfig`'s error arm (`createTopics`) | Needs a caller without DESCRIBE_CONFIGS on the topic, or a broker older than CreateTopics v5. `validateOnly` does *not* reach it — the controller still fills the successes map. | `ReplicationControlManager.java` sets `topicConfigErrorCode` only in the `!authorizedToReturnConfigs` branch |
| `LogDirDescription.error` | The broker sets it only for a directory it marked offline via `LogDirFailureChannel` after an I/O failure. Needs a disk to fail mid-run. | `ReplicaManager.describeLogDirs` |
| A per-broker error entry in `describeLogDirs` | **Fixture limit, not unreachable by construction — and the fan-out half is an open gap, not covered.** The entry is completed exceptionally only when a broker answers with an empty result list, which the broker does in exactly one branch: `CLUSTER_AUTHORIZATION_FAILED` when the caller lacks DESCRIBE on CLUSTER. The fixture that would probe it already exists — `cluster_config.rs`'s `authorizer_deny_reachable_single_broker()`, where `User:ANONYMOUS` is deliberately not a super user — with a DENY of DESCRIBE on CLUSTER added; that was **not attempted**, so §5.5 question 2 is unanswered for this row and "unreachable" is not established. Separately, no `describeLogDirs` scenario runs on a multi-broker fixture at all — all four registrations (`admin_log_dirs_test.rs:350-366`) take the default single-broker config, and the only override is `alter_replica_log_dirs_cross_dir_move` → `two_log_dir_cluster()`, which is two log **dirs** on one broker. So the outer per-broker keying is exercised only at cardinality 1, and Critic **round-14 Issue 2 stays open** — including the two assertions Java's own multi-broker test makes and this suite makes nowhere (`totalBytes.isPresent`, `usableBytes.isPresent`). *Erratum:* the earlier text here claimed the fan-out "*is* covered (G3 correction)" with no citation at all. That correction is about `PartitionReassignment`'s `{replicas, adding, removing}` on a 3-broker cluster — a different RPC — and discharges nothing here. | `KafkaApis.scala:2233`, `:2244` (the sole branch setting the top-level error); `src/admin/kafka_admin_client.rs:2100-2112` (empty results → per-broker `complete_exceptionally`); Java's fan-out test `PlaintextAdminIntegrationTest.scala:847-875`, asserting per-broker replica sets at `:866` and the two byte columns at `:868-869` |
| A cross-*broker* replica move via `alterReplicaLogDirs` | Not a state that RPC has: `AlterReplicaLogDirsRequest` is per-broker. Reassignment does this. Not a coverage gap. | `AlterReplicaLogDirsRequest.json` |
| `electLeaders`: absent vs `Some(empty)` partition set | Both yield 0 entries on a healthy cluster. The *dangerous* direction (an explicit set widened to cluster-wide) **is** observable, because the null branch omits every `ELECTION_NOT_NEEDED`. Half-observable, and which half is stated. | `ReplicationControlManager.java:1507` |
| `OffsetSpec::for_timestamp(-2)` vs `earliest()` | `getOffsetFromSpec` is not injective and the broker answers identically (probed). Observable only against `MockAdminClient`. What *is* observable: `for_timestamp(0)` returns a real timestamp where `earliest()` returns -1. | probed on a live 4.2 broker |
| `ConsumerGroupDescription`'s `state()` / `group_state()` **transposition** | Invisible *by construction*, not merely unreached: the two enums' constant names coincide for all eight `ConsumerGroupState` values, and the only separating state `NOT_READY` is STREAMS-only, which `describeConsumerGroups` cannot return. A *dropped* field is still caught by `check_derived_state`. | `GroupState.java:52-61`, `:78-89`; `ConsumerGroupState.java:31-40` |
| `removeMembersFromConsumerGroup` with a present-but-empty member list | The harness's own input type refuses to build it: `RemoveMembersFromConsumerGroupOptions::new(empty)` returns `Err`, mirroring Java's throw. Reaching it needs a raw-proto escape hatch the native arm could not join. | `RemoveMembersFromConsumerGroupOptions.java:33-38`, `:59-61` |
| `ConsumerProtocol::deserialize_assignment` | Needs a classic group with a **joined member**, which only the `isInState(STABLE)` branch populates. An *empty* classic group is now covered; a stable one is not creatable without a classic consumer (out of scope per `consumer-threading.md` §20). Covered by unit tests. | `GroupMetadataManager.java:744-757` |
| A real delegation token | `KafkaApis.allowTokenRequests` gates on the **client's** security protocol and is tested before `tokenManager.isEnabled`, so PLAINTEXT always answers `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED(64)`. Re-measured with and without `delegation.token.secret.key`: byte-identical. The client cannot authenticate (`AdminClientConfig` has no `security.protocol`). Marshaling closed against the mock instead. | `KafkaApis.scala:2320-2323`, `:2345-2354`; `design/current/status.md:606-609` |
| A successful `updateFeatures` **against a real broker** | Every finalized feature is already at the maximum its own `supported_features` range allows, an `UPGRADE` to the same level is rejected, and `SAFE_DOWNGRADE` mutates cluster-wide persistent metadata. Closed against the mock instead. | `FeatureUpdate`/`validateFeatureUpdate` |
| `UpgradeType::UnsafeDowngrade` distinguished from `SafeDowngrade` | On an unseeded mock both reject a higher level with the same message and both accept level 0 (the extra `while next != cur` walk does nothing when they are equal). Separating them needs the mock's version bounds seeded across the wire. | `mock_admin_client.rs:1953-1968` |
| `ListTransactionsOptions`' pattern: absent vs present-but-empty | **Java's own client** drops an empty pattern before it reaches the wire, so the whole stack agrees to erase the distinction; the broker would too. Not a backend could get it wrong. | `ListTransactionsHandler.java:78-80`; `TransactionStateManager.scala:359-368` |
| `ProducerState.coordinatorEpoch` / `.currentTransactionStartOffset`; `TransactionDescription.transactionStartTimeMs` / `.topicPartitions`; every `TransactionState` but `Empty` | All are populated only while a transaction is **in progress**, which needs the transactional producer API this client does not implement. Each is asserted on its `None` / empty side, so a backend defaulting one to 0 still fails. | `src/producer/producer_trait.rs`; `ProducerStateEntry.currentTxnFirstOffset` |
| `describeProducers`' per-partition **error** arm | A nonexistent partition does not produce one: the `PartitionLeaderStrategy` lookup retries metadata until the API timeout and the call fails as a whole. Measured at ~145 000 metadata attempts in 30 s (~4 800/s). **This is DEFERRED 1, re-measured on a fourth trigger — not a new observation.** DEFERRED 1 already names `describeProducers` on an unknown topic as one of its four triggers and already adjudicates it as a self-inflicted broker DoS needing its own change; G6 rediscovered it and independently reproduced its 5 400–6 000/s figure. It is now in §5.6. The arm becomes reachable once DEFERRED 1 is fixed. | measured; `COMMENTS.DONE.1.md:2479-2504` (DEFERRED 1) |
| A SCRAM salted password / salt in the response direction | Write-only at the broker: `DescribeUserScramCredentialsResponse` carries only the mechanism and the iteration count. | `DescribeUserScramCredentialsResponse.json` |

## 5.5 The four questions that decide an "unreachable" verdict

In the order they should be asked. Five of the six claims re-tested at G6 fell to
question 2 or 3, both of which cost minutes.

1. **Is it unreachable *by construction*, or only on this fixture?** Say which.
   The `state()`/`group_state()` pair is the former (no cluster configuration
   reaches it); everything else in §5.4 is a fixture or scope limit.
2. **Have you tried a purpose-built `ClusterConfig`?** `cluster_pool` keys
   containers by config, so a distinct one simply starts its own — it is cheap.
   This is what closed the reassignment and ACL-denial claims.
3. **Can another RPC *in the same slice* produce the required server state?**
   This is the question the earlier clause missed: `describeClassicGroups`' value
   arm was reachable on the *same cluster*, by calling an RPC the slice already
   implemented.
4. **Have you asked `MockAdminClient`?** Grep `src/admin/mock_admin_client.rs`
   for the method. The mock is uniformly "unsupported" for some families and
   fully implemented for others — delegation tokens and both feature RPCs are
   implemented, and that is what closed their marshaling. "The real broker cannot
   reach it" is not the same claim as "nothing can".

And when the answer really is unreachable: prefer asserting the *empty* side over
skipping the field. `None` is an assertion a backend that defaults to `0` fails.

## 5.6 Defects found and not fixed (PLAN §0: report, don't fix)

Carried into the PR description rather than resolved here.

Two rows below were missing from earlier revisions of this table, which is the
list the PR description carries verbatim — and they are its two most
operationally significant items. Both were adjudicated during the earlier
real-broker probe (`COMMENTS.DONE.1.md`'s "Real-broker findings, deferred"
section), not discovered at G6.

| Defect | Location | Impact |
|---|---|---|
| DEFERRED 1: lookup-stage metadata retries are a CPU-bound busy spin | `src/admin/internals/admin_api_driver.rs` (`clear_inflight_request` / the lookup-scope retry path), reached through `maybe_send_requests` in `src/admin/kafka_admin_client.rs` | A denial-of-service against the broker the client is talking to, and self-inflicted: one unknown topic name is enough. Measured at 54 000–108 000 `Metadata` attempts in 10–20 s windows (~5 400–6 000 req/s) across five call sites before the broker dropped the connection, and re-measured at G6 on a fourth trigger (~145 000 in 30 s on `describeProducers`). Four known triggers: `deleteRecords` with `partition=-1`, and `listOffsets` / `describeProducers` / `abortTransaction` on an unknown topic. The *decision* not to back off is faithful — `AdminApiDriver.clearInflightRequest` sets a lookup scope's next-allowed-try to `now` — but in Java each retry costs a round trip, so the loop is RTT-bound; here the retry is re-sent inside the same `run_once` sweep, so it is CPU-bound. Same decision, three orders of magnitude apart. Needs its own change (it is core driver pacing, shared by every driver-backed RPC), which is why it is not a rider on a harness slice. It is also why §5.4's `describeProducers` per-partition error arm is out of reach |
| DEFERRED 2: `enable.idempotence` defaults to `true` but is unimplemented | `src/producer/internals/sender.rs` — nothing on the send path calls `ProducerBatch::set_producer_state`, and `InitProducerId` appears nowhere under `src/producer/` | The config advertises a guarantee (no duplicates on retry) that the send path does not provide; every record carries `RecordBatch::NO_PRODUCER_ID`, so no producer ever registers as idempotent. Load-bearing for this slice: it is the only reason `describe_producers_reports_an_idempotent_producer` has to obtain a `ProducerState` out of process, via `docker exec … kafka-console-producer.sh` (`admin_transactions_test.rs:193-230`), and it is the same gap that keeps Tier 3 Phase 6's ongoing-transaction test unreachable |
| `MockAdminClient::create(0)` fabricates a controller | `src/admin/mock_admin_client.rs:183` — `brokers.first().cloned().unwrap_or_else(...)` | More permissive than Java (`Builder.build()` reads `brokers.get(0)` and throws) *and* than its own C boundary (`kafka_admin_MockAdminClient_new` returns null for `num_brokers < 1`) |
| `NewPartitions.newAssignments` absent-vs-empty collapse | `src/ffi/admin.rs:1204-1210` — `NewPartitionsBuilder::build` decides by `is_empty()` | The C boundary cannot express `increaseTo(n, emptyList())`; a scenario building it would show three backends agreeing and the Java-faithful one disagreeing. `CreateTopicsRequest.json:45` has no `nullableVersions`, so `NewTopic`'s analogous caveat is harmless — this one is not |
| SCRAM salt absent-vs-empty collapse | `src/ffi/admin.rs:15600` — `read_scram_alterations` decides by `salt.is_empty()` | Same class as the row above, fifth instance found. An explicitly empty salt selects the salt-*generating* constructor. Needs a `bool has_salt` column |
| `alterClientQuotas` rejects a duplicate entity Java accepts | `src/ffi/admin.rs` (pre-existing B5a; Critic round-9 LOW 2) | Three-way divergence: Java accepts, the native client mirrors it, the FFI rejects |
| DEFERRED 3: argument-validation failures surface as `UNSUPPORTED_VERSION(35)` | `src/network_client.rs:507` stamps every `RequestBuilder::build_version` `io::Error` | Java answers `IllegalArgumentException`. One scenario had to be written around it (`remove_members_rejects_an_explicitly_empty_selection` discriminates by message so it survives the fix) |
| The producer / consumer gRPC services' mock-selection rule diverges from Admin's | `bindings/python/grpc_server.py`, `bindings/c/grpc_server/server.cc` | Admin's rule is normative and stated at `CreateAdminRequest`; the other two services predate it |
| The three gRPC Dockerfiles are not buildable on macOS as committed | `bindings/c/Dockerfile.grpc:43`, `bindings/python/Dockerfile.grpc:36` | They copy **host-built** Rust artifacts, which are Mach-O / `.dylib` on macOS. CI is Linux, so this has never surfaced there. Worked around locally by building the artifacts in a container |

## 5.7 Standing limitation: the guessed error variant, and what is left of it

The C FFI does not expose the Rust `KafkaError` discriminator, so both gRPC
servers infer `variant` from the message text — one shared algorithm, byte-for-byte
identical between `server.cc` and `grpc_translate.py`, because two independent
guesses manufacture disagreements the harness would report as defects.

G6 closed the half of this that was a real 1-vs-3 divergence:
`kafka_error_from_proto` now prefers the transported **code** whenever the guessed
variant is one of the five `KafkaError` cases that have no `Errors` slot
(`IllegalArgument`, `IllegalState`, `Timeout`, `RecordTooLarge`, `Serialization`)
*and* the wire carried a real broker code. The two cases separate without
judgement, because a genuinely code-less error always transports `-1`.

What remains, stated exactly: the three payload-carrying variants
(`TopicAuthorization`, `InvalidTopic`, `GroupAuthorization`) are rebuilt from
payloads the wire does not have, so on the gRPC backends their `message` is empty
and their topic / group sets are empty. `error()` and `code()` are correct for
them. No scenario asserts either. Preferring the code there would trade a wrong
payload for a wrong *variant*, breaking assertions that hold today.

## 5.8 Definition of Done, as actually run

Per slice, and re-run whole at G6: `cargo build`, the scenarios green on **all
four** backends, and no regression in the producer (70) or consumer (44)
multilanguage arms. Environment caveats that a Linux runner does not have, and
which every slice has disclosed:

  - `cargo fmt` and `cargo clippy` are absent from the pinned 1.95.0 toolchain
    (`rust-toolchain.toml` lists components that are not installed), so
    `cargo xtask format-check` and `cargo xtask lint` run only under the nix
    1.97.1 triple. Neither is unconditionally clean-by-assumption.
  - `cmake` and `protoc` are absent on the host, so `make verify` / `make test-c`
    cannot run locally and the C unit suite rests on CI.
  - `cargo xtask check-generated` **passes** — 199 generated files, exit 0 —
    under the nix 1.97.1 triple. *Erratum, and it matters because the per-slice
    self-reviews carried the opposite from G1 onward — five slices (Critic round
    17, Issue 6a; the wording survives in the G2 actor-memory note):* they said
    the command "fails on a pre-existing blank-line diff in generated
    `join_group_response_data.rs`", adjudicated pre-existing in Critic round 4.
    That diff does not reproduce at HEAD. What was actually being observed on
    this host is that the command exits **1** with `Error: No such file or
    directory (os error 2)` because `rustfmt` is absent from the pinned 1.95.0
    toolchain — and an exit code of 1 is *indistinguishable* between "a
    generated file is misformatted" and "the formatter could not be run at all".
    So the honest caveat replacing the old one is: **`check-generated` cannot
    run without `rustfmt` and reports the same exit status either way**; with
    `rustfmt` on `PATH` it is clean. The Critic reproduced this in both
    directions (round 17, "Chased and refuted" #7).

## 5.9 Ledger corrections (the commit messages are immutable; this is the errata)

Each slice's commit message carries a predicate-strength ledger for the tests it
converted. **Nine** claims were wrong, and are corrected here rather than left to
be read as the record — one row per claim, and the count is the row count of the
table below. (An earlier revision said "five" over a six-row table and omitted
three claims: Critic round-16 Issue 4, and two of G6's own six entries.) A
claimed strengthening must name an input the old assertions accept and the new
ones reject; where no such input exists the change is either cosmetic
(UNCHANGED), **non-discriminating** — a third verdict worth naming — or **MIXED**,
strengthened on some backends and unchanged on others. A strengthening that can
only fail on the gRPC arms must say so: `all_of_exactly` over a
`RustNativeAdmin` map built from the request's own keys cannot fail natively
(Critic round-15 LOW 4).

| Slice / commit | Claim | Correction |
|---|---|---|
| G4 `2c24a674` | `list_consumer_group_offsets_matches_committed` STRENGTHENED | **UNCHANGED in strength.** The added `assert_ne!` was entailed by the two `assert_eq!`s above it (`Some(5) != Some(3)`) and could never fire alone; the original already committed those distinct values and pinned each by key. Fixed in G6: the tautology is gone and replaced by an exact-key-set assertion, which *is* independent |
| G4 `2c24a674` | ledger covers the conversion | **Incomplete: 9 of 10.** `list_groups_and_list_consumer_groups_show_live_group` (unchanged) and — more importantly — `describe_consumer_groups_live_group` were omitted. The second is the **largest strengthening in that slice**: member identity, `assert_real_coordinator`, and `authorized_operations().is_none()` |
| G4 `2c24a674` | `delete_consumer_group_offsets_on_active_group_errors` "unchanged verbatim" | **Stricter.** The original folded the error through `.all()`; the conversion requires the whole call to be `Ok` and the error to arrive in tp0's per-partition slot |
| G5 `16bdbcf3` | `entity_type_filter_returns_only_matching_entities` STRENGTHENED by a `contains_only` read-back | **Non-discriminating as written.** `strict` narrows the result to a subset, so dropping it yields a superset and a pure single-component entity is reported either way; the companion `entries().len() == 1` loop needed a multi-component entity that nothing created. Fixed in G6 by creating a `<user, client-id>` entity, which the non-strict filter must report and the strict one must not — so dropping `strict` now fails the exclusion and inverting it fails the inclusion |
| G5 `16bdbcf3` | `create_then_describe_acls` ledger sentence | Describes **scenario (c)**'s code, not (a)'s. And (a) was in fact *weakened* by the conversion: it dropped the per-ACL `exception()` check the original got from `DeleteAclsResult::all()`. Restored in G6 |
| G5 `16bdbcf3` | `update_features_above_max_is_rejected` UNCHANGED | **Stronger**, and the supporting sentence was factually wrong |
| G5 `16bdbcf3` | `describe_features_reports_metadata_version` "asserts `finalized_features_epoch` is present, so a backend decoding an absent `Optional<Long>` as epoch 0 is visible" | **The assertion did not do that.** It was `epoch >= 0`, which admits exactly the `Some(0)` the sentence claimed to catch. Fixed in G6 to `> 0` with a corrected rationale (`admin_features_test.rs:125-142`) — a bootstrapped KRaft cluster has written records before features are finalized, so a genuine epoch is ≥ 1. The code is right; this row exists because the commit message is not, and correcting it is what this section is for (Critic round-16 Issue 4) |
| G6 `006f7ba7` | `list_transactions_returns_empty_when_none_active` STRONGER, because "a response with no entries fails instead of folding to 'no transactions'" | **STRONGER, but not for that reason.** The original already asserted `by_broker.len() == 1`, which rejects a response with no entries. The real gain is key **identity**: `all_of_exactly` over the cluster's actual broker ids rejects a backend that keys entries by `0` (or by loop index) where the cardinality check could not — and, per round-15 LOW 4, that gain is on the three gRPC arms. It also introduced one narrow vacuity: `broker_ids()` has no non-empty guard, so a backend answering both an empty node list *and* an empty map now passes where `len() == 1` failed (ticketed; one `assert!(!ids.is_empty())` closes it) |
| G6 `006f7ba7` | `fence_producers_allocates_producer_id_for_fresh_id` STRONGER, "plus `is_valid()` and `all_of_exactly`" | **MIXED**, and neither named addition was discriminating. `is_valid()` is `RecordBatch::NO_PRODUCER_ID < producer_id` (`src/common/utils/producer_id_and_epoch.rs:50-52`), i.e. `producer_id > -1` — entailed by the `producer_id >= 0` assertion two lines above it. That is round-15 Issue 3's defect class, re-introduced two commits after `be04e143` removed it. `all_of_exactly` cannot fail on the `__rust` arm (round-15 LOW 4), so it is STRONGER on the gRPC arms via key-set identity and UNCHANGED natively. The unique-id change is a flakiness fix that keeps `epoch == 0` exact; it rejects no input a single run accepted. Fixed here: `is_valid()` is replaced by a `describe_transactions` read-back requiring the coordinator to report the allocated `producer_id` and `epoch`, which a backend that resolved without sending `InitProducerId` cannot fake |
