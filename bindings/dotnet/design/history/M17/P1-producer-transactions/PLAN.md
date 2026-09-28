# M17/P1 — .NET producer transactions and idempotency (PLAN)

> **Status: APPROVED by the user on 2026-09-28.** Moved here from
> `design/current/PLAN-M17-producer-transactions.md` on approval (§10.3 step 1).
> Execution starts at CP0.
>
> **The user's answers to §9.** Every default is accepted, and the two
> go-aheads were given explicitly:
>
> - **Q4 — none** of B1-B7 in this phase (the default).
> - **Q18 — yes.** The Actor may run `.semaphore/install-dotnet.sh` into
>   `$HOME/.dotnet` at CP0 and run the net8.0 legs locally. It sets
>   `DOTNET_ROOT=$HOME/.dotnet`, `PATH=$HOME/.dotnet:$PATH` and
>   `DOTNET_MULTILEVEL_LOOKUP=0` in its own shell only. `DOTNET_ROLL_FORWARD`
>   is never set.
> - **Q21 — the default:** document and file forward, no fix in this phase.
> - **Q26 — yes.** The local Docker gate (G10, §4.5) runs at CP0 (baseline)
>   and CP7 (gate).
> - **Every other question** takes its stated default.
>
> Each of the four answers above is also recorded inline in §9.
>
> **Amended after approval, at the CP0 review (2026-09-28).** Five corrections,
> each marked in place: §4.5's isolated-run form (Critic finding 84.1), G7's
> never-stage list, the §10.2 execution note (agents cannot commit, so the user
> makes every commit; its gate commands per finding 84.2), a new RULE-DRAFT, RD10
> (with finding 84.3's count fix), and a note under §4.2's table on copying
> commands out of it.
>
> **Amended at the CP1 review (2026-09-28).** Four corrections and two
> additions, each marked in place. The corrections: CP7's two `Makefile` checks
> move out of §4.3's table into a list with before-values (Critic finding 84.6);
> §4.2's copy note lists every escaped command (84.6); D8's remarks instruction
> carries B5's and B6's qualifiers (84.5); and three new RULE-DRAFTs, RD11-RD13,
> come from Critic 84's CP1 suggestions. The additions: two watch items for CP4
> in D12, and a Rust ABI doc item in D15's file-forward list. That item made
> §7's count of the list stale, so the count is deleted; a deletion carries no
> marker.

**Requirement (verbatim):** "Make sure we implement all the public producer
transaction and idempotency apis for .NET."

**One-line summary.** Bind the five Java `Producer` transaction methods on both
producer surfaces (sync `IProducer` + async `IAsyncProducer`, real + mock) over
the C ABI that already exists (Mode A, +18 P/Invokes), add the public
`ConsumerGroupMetadata` constructors, the five hierarchy predicates on
`KafkaException`, the three ABI-backed `MockProducer` transaction helpers, the
five gRPC RPCs in both servicers, and un-skip the three cross-language transaction
tests. Idempotency has no Java methods: it is configuration plus error
classification, delivered as a predicate and pinned by tests.

---

## §0 Header

| Field | Value |
|---|---|
| Milestone / phase | **M17 / P1** — ".NET producer transactions + idempotency" (binding-local numbering, `bindings/dotnet/CLAUDE.md` §8.4). |
| Agent number | **N = 84** (`dotnet-actor` 84 / `dotnet-critic` 84). Highest number used so far in `bindings/dotnet/design/` is 83 (M15/P12, newest `design/current/STATUS.md` entry); M16/P1 (soak client) used 74. |
| Not to be confused with | `design/history/M15/P8-producers-and-transactions` — that phase bound the **admin** RPCs `describeProducers` / `describeTransactions` / `listTransactions`. This phase is producer-side transaction **control**. |
| Base branch / commit | `prashah_dev_dotnet_binding` @ `76629aea` ("Dummy commit for CI"); merge-base with `master` = `b76de2e1`. |
| Working branch | `prashah_dev_dotnet_producer_transactions`, stacked on `76629aea`, created by the Actor at CP0 (Q22). PR target: `prashah_dev_dotnet_binding`. |
| Mode | **A** — .NET-only over the existing C ABI. All 18 symbols bound here already exist (verified, §2). No Rust, `src/ffi`, `cbindgen.toml` or generator change. |
| Mode A proof | `git diff 76629aea..HEAD -- src/ src/ffi/ cbindgen.toml generator/ tests/` must print **nothing** at every checkpoint, and the generated header's SHA-256 must equal the value recorded at CP0 (gate G1, §4.2). (At plan time HEAD == base so it is trivially empty; the gate is meaningful on the working branch.) Control positive: `git diff --stat 76629aea..HEAD -- bindings/dotnet/` is non-empty from CP1 on. |
| Non-binding edits | Exactly **one**: the root `Makefile` at CP7 — delete the comment block `Makefile:272-291` and the three `--skip` lines `Makefile:309-311`, re-terminating `Makefile:308` with `-- __grpc_dotnet; \`. Nothing else outside `bindings/dotnet/` changes. |
| Principle | **Java shape, Rust logic.** The binding restores Java's `Producer` transaction shape (`Producer.java:45-66`). The transaction state machine, coordinator discovery, fencing, epoch bump, sequence numbers and every precondition beyond null / closed / cancelled stay in the Rust core. The binding marshals, drains its **own** send buffer (D3), orders its **own** completions (D4), and forwards core errors verbatim — it never branches on an error code (ffi §B5). |
| Worktree hygiene | The planning snapshot carries pre-existing noise that must **never** be staged: ` M kafka` (submodule), ` D bindings/dotnet/.claude/agents/dotnet-{actor,critic}.md`, `?? .claude/agents/dotnet-{actor,critic}.md` (discovery copies; `bindings/dotnet/CLAUDE.md` §8.4 forbids committing them). The Actor stages with explicit `git add <paths>` only and checks `git diff --cached --name-only` before every commit. `COMMENTS.84.md` / `COMMENTS.DONE.84.md` are never committed at the binding root (§8.4). |
| Java reference | `kafka/gradle.properties:17` reads `4.2.2-SNAPSHOT`; root `CLAUDE.md` says Apache Kafka 4.3.1, and the core cites "AK 4.3.1" (`src/producer/internals/transactional_request_result.rs:122-124`). Every Java line cited here is from the checked-out `kafka/` tree (Q17). |
| ABI header | `target/include/confluent_kafka.h` is generated and gitignored. Header line numbers (`h:NNNNN`) are from the locally generated copy at planning time and are convenience only; the authoritative citation for each symbol is its `src/ffi/*.rs` definition line. |
| Archive | On approval the Manager copies this file to `bindings/dotnet/design/history/M17/P1-producer-transactions/PLAN.md`. At close (Manager Step 7): `COMMENTS.DONE.84.md` is archived there, `design/current/STATUS.md` gains the M17/P1 entry, `COMMENTS.84.md` is reset. `marked_classes.txt` is not expected to change (no Rust translation in Mode A); the Manager confirms at Step 7. |

---

## §1 Goal and non-goals

### Goal

1. **The five Java transaction methods on both surfaces** — `initTransactions`
   (`Producer.java:45`), `beginTransaction` (`:50`), `sendOffsetsToTransaction`
   (`:55`), `commitTransaction` (`:61`), `abortTransaction` (`:66`) — on
   `IProducer<TKey,TValue>` (`KafkaProducer` / `MockProducer`) and
   `IAsyncProducer<TKey,TValue>` (`AsyncKafkaProducer` / `AsyncMockProducer`).
2. **`ConsumerGroupMetadata` public constructors** (`ConsumerGroupMetadata.java:37-46`,
   `:51-56`), needed to call `sendOffsetsToTransaction` without a live consumer
   (gRPC servicers, tests, consume-transform-produce against an externally
   coordinated group).
3. **Error classification** sufficient for Java's documented recovery pattern
   (`KafkaProducer.java:209-216`: `catch (ProducerFencedException |
   OutOfOrderSequenceException | AuthorizationException)` -> close; `catch
   (KafkaException)` -> abort) and for KIP-1050's `TransactionAbortableException`
   / `ApplicationRecoverableException` — five `KafkaException` predicates over the
   hierarchy predicates the ABI already exports.
4. **The ABI-backed Java `MockProducer` transaction helpers**:
   `commitTransactionException` (via `set_commit_transaction_error`),
   `sentOffsets()`, and the single-entry projection of
   `consumerGroupOffsetsHistory()` (`committed_offset`).
5. **Five transaction RPCs in both gRPC servicers**, so the three cross-language
   transaction tests stop being skipped for .NET (`Makefile:309-311`).
6. **Idempotency.** Java has no idempotency *methods*: idempotence is
   configuration (`enable.idempotence`, default true; `transactional.id` requires
   it) plus error semantics (`OutOfOrderSequenceException` and its subclass
   `UnknownProducerIdException`). Deliverable: config pass-through (already
   works — the config is a string map), the `IsOutOfOrderSequenceError`
   predicate, and tests pinning the core's idempotence / transaction config
   validation and its messages (D9).

### Non-goals

- No ABI change, no Rust edit (Mode A).
- **No KIP-939 two-phase commit** (`prepareTransaction`, `completeTransaction`,
  `initTransactions(boolean keepPreparedTxn)`): absent from the checked-out
  `Producer.java` (only the five methods at `:45-66`; `KafkaProducer.java:974`
  mentions `completeTransaction` inside a message string only) and absent from
  the ABI. (O1, Q17)
- No `clientInstanceId` (`Producer.java:106`), `registerMetricForSubscription`
  (`:71`) / unregister, or `MockProducer.injectTimeoutException`
  (`MockProducer.java:373-384`): not transaction or idempotency APIs. (O2)
- No binding of `kafka_producer_Producer_begin_transaction_async` (B8, D1, Q5).
- No typed exception hierarchy (flat `KafkaException` + predicates —
  `bindings/dotnet/CLAUDE.md` §4 "Error granularity", ffi §A5).
- No public error-code constants type (file-forward item 12, D15).
- No Python binding change (file-forward list, D15).
- No completion barrier for `Flush` (file forward; D4 scopes the barrier to
  commit / abort, Q3).
- The stale Rust test comment `tests/integration/producer_transactions_test.rs:674-678`
  ("Python is deferred") is **reported, not edited** — it is outside the binding
  (Q10).
- The Mode-B rows B1-B7 (§2) are **not** implemented; each is a decision gate
  (Q4). Nothing is silently dropped: every Java member appears in §2 with a mode.

---

## §2 Inventory

Legend. ID namespaces: T / B / O / V / M rows here, D decisions (§3), CP checkpoints and G
gates (§4), S test groups (§5), R1-R15 risks (§8; R-a / R-b / R-c are D4's residuals),
Q open questions (§9), RD rule drafts (D15). **Mode A** = bound in this phase over an
existing ABI symbol. **Mode B** = needs a new ABI symbol (a Rust-core task for `actor-executor` / `kafka-critic`,
`bindings/dotnet/CLAUDE.md` §6.3) — gated, not implemented (Q4). **Out** = out of
scope with the reason stated. "Borrowed" / "owned" follow ffi §A2's
classify-by-the-accessor rule. `MPT` = Java `MockProducerTest.java`.

### 2.1 Transaction control, group metadata, classification, config, mock helpers (Mode A)

| ID | Java | Python | Rust core | C ABI symbol (src def; header; ownership) | .NET sync | .NET async | Mock | Test source | Mode |
|---|---|---|---|---|---|---|---|---|---|
| T1 | `initTransactions()` — `Producer.java:45`; `KafkaProducer.java:648` (retry-safe after timeout, `:635`) | `init_transactions` via `_async` (`producer.py`) | `KafkaProducer::init_transactions` (timeout reason `kafka_producer.rs:661`); mock `mock_producer.rs:988` | `kafka_producer_Producer_init_transactions` (`producer.rs:3260`; h:16090) -> **owned** `Error_t*` or NULL. `..._init_transactions_async` (`producer.rs:3712`; h:16300) -> the callback **owns** the delivered `Error_t*` and frees it (typedef doc h:2331-2339: "owned by the callee"; the shipped `OperationCompletionSource.Complete` already frees it via `KafkaException.FromHandle`, `OperationCompletionSource.cs:183-194`). All five async typedefs alias the flush / close `OperationCallbackFn` shape, so the rooted `ProducerCallbacks.Operation` (`ProducerCallbacks.cs:74`) is reused | `void InitTransactions()` | `Task InitTransactions(CancellationToken = default)` | same members on both mocks | MPT 134, 141, 617 | A |
| T2 | `beginTransaction()` — `Producer.java:50`; `KafkaProducer.java:674` (local transition, does not block) | `begin_transaction` via `_async` | `KafkaProducer::begin_transaction` (sync); mock `:1018` | `kafka_producer_Producer_begin_transaction` (`producer.rs:3294`; h:16117) -> owned `Error_t*` or NULL. `_async` exists (`producer.rs:3752`; h:16329) and is **deliberately not imported** (B8) | `void BeginTransaction()` | `void BeginTransaction()` — stays sync (`bindings/dotnet/CLAUDE.md:487`, `:575`) | both | MPT 148, 154, 162, 631 | A |
| T3 | `sendOffsetsToTransaction(Map<TopicPartition,OffsetAndMetadata>, ConsumerGroupMetadata)` — `Producer.java:55`; `KafkaProducer.java:732-745` (empty-map early return `:738`; null metadata `:1498-1500`) | `send_offsets_to_transaction` | `KafkaProducer::send_offsets_to_transaction` (group-metadata check `kafka_producer.rs:1666-1676`); mock `:1055` | `kafka_producer_Producer_send_offsets_to_transaction` (`producer.rs:3369`; h:16185) — five parallel arrays + `count` + `const ConsumerGroupMetadata_t*`, all **borrowed for the call**; `count == 0` reads no array (h:16139-16149). `..._async` (`producer.rs:3819`; h:16384) marshals everything on the calling thread before returning (h:16346-16349) | `void SendOffsetsToTransaction(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata)` | `Task SendOffsetsToTransaction(offsets, groupMetadata, CancellationToken = default)` | both | MPT 170, 176, 430, 438, 447, 464, 638, 645 | A |
| T4 | `commitTransaction()` — `Producer.java:61`; `KafkaProducer.java:779` (flushes first `:748`; callback guarantee `:754-755`; timeout retry `:763-764`) | `commit_transaction` | `KafkaProducer::commit_transaction`; mock `:1096` (flushes first) | `kafka_producer_Producer_commit_transaction` (`producer.rs:3480`; h:16232). `..._async` (`producer.rs:3940`; h:16427) | `void CommitTransaction()` | `Task CommitTransaction(CancellationToken = default)` | both | MPT 183, 189, 196, 310, 330, 652 | A |
| T5 | `abortTransaction()` — `Producer.java:66`; `KafkaProducer.java:813` (timeout retry `:798-799`) | `abort_transaction` | `KafkaProducer::abort_transaction`; mock `:1141` (flushes first) | `kafka_producer_Producer_abort_transaction` (`producer.rs:3527`; h:16272). `..._async` (`producer.rs:3987`; h:16463) | `void AbortTransaction()` | `Task AbortTransaction(CancellationToken = default)` | both | MPT 231, 237, 244, 348, 364, 377, 659 | A |
| T6 | `ConsumerGroupMetadata(String, int, String, Optional<String>)` — `ConsumerGroupMetadata.java:37-46`, `@Deprecated(since = "4.2", forRemoval = true)` | built from proto in `grpc_translate.py:222-261` | core `ConsumerGroupMetadata` | managed-only value ctor. `kafka_consumer_ConsumerGroupMetadata_new` (`consumer.rs:1720`; h:13216) is imported for T3's marshalling: returns an **owned** handle (freed by the already-bound `kafka_consumer_ConsumerGroupMetadata_destroy`, `NativeMethods.cs:366`); `group_id` / `member_id` must be non-NULL (`CStr::from_ptr`), NULL `group_instance_id` = none | `[Obsolete] public ConsumerGroupMetadata(string groupId, int generationId, string memberId, string? groupInstanceId)` | same type | n/a | new `ConsumerGroupMetadataTests` | A |
| T7 | `ConsumerGroupMetadata(String groupId)` — `ConsumerGroupMetadata.java:51-56` (generation -1, member "", no instance id) | — | — | none (chains to T6's managed ctor) | `[Obsolete] public ConsumerGroupMetadata(string groupId)` | same type | n/a | same | A |
| T8 | classification by `instanceof` — `KafkaProducer.java:209-216`; KIP-1050 `TransactionAbortableException`, `ApplicationRecoverableException` (`ProducerFencedException`, `InvalidProducerEpochException`), `InvalidConfigurationException` (parent of `AuthorizationException`), `AuthorizationException`, `OutOfOrderSequenceException` (parent of `UnknownProducerIdException`) | `KafkaError.txn_requires_abort` (`_confluentkafka.c:1499`) + composed `is_fatal` (`:1463-1487`) | `Error::is_*_error` (docs `error.rs:170-296`; e.g. `is_transaction_abortable_error` `:1625`) | `kafka_common_Error_is_transaction_abortable_error` (`common.rs:1109`; h:12074), `_is_application_recoverable_error` (`common.rs:927`; h:11919), `_is_invalid_configuration_error` (`common.rs:901`; h:11896), `_is_authorization_error` (`common.rs:874`; h:11872), `_is_out_of_order_sequence_error` (`common.rs:979`; h:11963) — each `bool(const Error_t*)`, **borrowed**, false for NULL | `KafkaException.IsTransactionAbortableError` / `IsApplicationRecoverableError` / `IsInvalidConfigurationError` / `IsAuthorizationError` / `IsOutOfOrderSequenceError` | same type | driven via `ErrorNext` | `KafkaExceptionTests` truth table | A |
| T9 | idempotence config — `enable.idempotence`, `transactional.id` (Java `ProducerConfig`) | config dict | validation `producer_config.rs:764-767` | existing `ProducerProperties_put` + `KafkaProducer_new` | config dictionary | config dictionary | n/a | `PublicProducerIdempotenceTests` | A (tests only) |
| T10 | `public RuntimeException commitTransactionException` — `MockProducer.java:82` (sticky until cleared; read in `commitTransaction`, `:204`) | `_lib` only | `MockProducer::set_commit_transaction_error` (`mock_producer.rs:951`) | `kafka_producer_MockProducer_set_commit_transaction_error` (`producer.rs:4160`; h:16571) — `bool (producer, bool clear, int32_t code, const char* msg)`; false for NULL / non-mock / code 0 / code outside i16; setup-only (must not overlap a control call) | `MockProducer.SetCommitTransactionError(int code, string? message = null)` + `ClearCommitTransactionError()` | `AsyncMockProducer`: same | — | new `PublicProducerTransactionMockControlTests` | A |
| T11 | `sentOffsets()` — `MockProducer.java:456` | `_lib` | `mock_producer.rs:827` | `kafka_producer_MockProducer_sent_offsets` (`producer.rs:4205`; h:16588) | `bool SentOffsets()` | same on `AsyncMockProducer` | — | MPT 438, 447, 464 | A |
| T12 | single-entry projection of `consumerGroupOffsetsHistory()` — `MockProducer.java:479` (Java has no single-entry accessor; the Rust core added one) | `_lib` (512-byte buffer, `_confluentkafka.c:1099-1117`) | `mock_producer.rs:701` `committed_offset` | `kafka_producer_MockProducer_committed_offset` (`producer.rs:4253`; h:16628) — newest entry wins; NULL group/topic -> false; epoch -1 = none; metadata written NUL-terminated into a caller buffer, **truncated at a UTF-8 boundary without report** | `OffsetAndMetadata? CommittedOffset(string groupId, TopicPartition partition)` | same on `AsyncMockProducer` | — | MPT 397, 492, 529, 558, 583 (single-entry projection) | A |

### 2.2 Java members with no ABI symbol (Mode B — gated, not implemented)

Each is a thin ABI shim over a core function that **already exists**; none needs
new core logic. Implementing any of them is a Rust-core task first (Q4).

| ID | Java | Rust core (exists) | C ABI today | MPT tests affected | .NET in this phase |
|---|---|---|---|---|---|
| B1 | `fenceProducer()` — `MockProducer.java:429` | `mock_producer.rs:773` | none | 255, 261, 269, 278, 286, 294, 302, 666, 680 | skipped with reason |
| B2 | `transactionInitialized()` / `transactionInFlight()` / `transactionCommitted()` / `transactionAborted()` — `:436` / `:440` / `:444` / `:448` | `:787-815` | none | 134, 154, 196, 244 | proxied by observable consequences (§5) |
| B3 | `commitCount()` — `:460` | `:837` | none | 207, 218 | skipped with reason |
| B4 | `flushed()` — `:452` | `:853` | none | 696-716 (non-transactional) | n/a |
| B5 | `history()` / `uncommittedRecords()` — `:467` / `:471` | `:654` / `:670` | only `history_count` = committed `sent` count (`producer.rs:4096-4111`) | 310, 348, 377 | proxied via `HistoryCount()` |
| B6 | `consumerGroupOffsetsHistory()` (full list) / `uncommittedOffsets()` — `:479` / `:483` | `:685` / `:717` | only single-entry `committed_offset` (T12) | 397, 492, 529, 558, 583 | projected via `CommittedOffset` |
| B7 | `initTransactionException` / `beginTransactionException` / `sendOffsetsToTransactionException` / `abortTransactionException` — `:79`, `:80`, `:81`, `:83` | `set_*_transaction_error` `:920` / `:930` / `:941` / `:961` | none — the header states the sibling hooks have no C caller | (Rust-core tests only) | not exposed |
| B8 | (decision, not a gap) | — | `kafka_producer_Producer_begin_transaction_async` exists (`producer.rs:3752`) | — | **deliberately unbound**: Java's `beginTransaction` does not block, so there is no async member to back (D1, Q5) |

### 2.3 Out of scope

| ID | Item | Reason |
|---|---|---|
| O1 | KIP-939 2PC (`prepareTransaction`, `completeTransaction`, `initTransactions(boolean)`) | absent from the checked-out `Producer.java` and from the ABI (Q17) |
| O2 | `clientInstanceId` (`Producer.java:106`), `register`/`unregisterMetricForSubscription` (`:71` ff.), `MockProducer.injectTimeoutException` (`:373-384`) | not transaction / idempotency APIs |

### 2.4 gRPC servicers (Mode A, `grpc-server/`)

Proto: `multilanguage-test-server/proto/producer_service.proto`. Reference
implementation: `bindings/python/grpc_server.py:245-307`, translation helpers
`bindings/python/grpc_translate.py:222-261`.

| ID | RPC (proto line) | Request -> Response | `ProducerServiceImpl` (sync) | `AsyncProducerServiceImpl` (async) |
|---|---|---|---|---|
| V1 | `InitTransactions` (`:61`) | `TransactionRequest` (`:168`) -> `StatusResponse` (`:225`) | new override | new override (awaits) |
| V2 | `BeginTransaction` (`:64`) | same | new override | new override (sync call) |
| V3 | `CommitTransaction` (`:67`) | same | new override | new override (awaits) |
| V4 | `AbortTransaction` (`:70`) | same | new override | new override (awaits) |
| V5 | `SendOffsetsToTransaction` (`:76`) | `SendOffsetsToTransactionRequest` (`:202`: `producer_id`, repeated `OffsetEntry` `:180`, `ConsumerGroupMetadata` `:192`) -> `StatusResponse` | new override | new override (awaits) |
| M1 | root `Makefile` | comment `:272-291`, skips `:309-311` | removed at CP7 | — |

### 2.5 Symbol verification

All 18 symbols bound by this phase were verified present as
`pub unsafe extern "C" fn` at the `src/ffi/*.rs` lines cited above (planning-time
grep), and their prototypes were read in the generated header. The locally built
`target/release/libconfluent_kafka.dylib` exports all 18 (`nm -gU` plus `grep -cw "_<symbol>"`
returned 1 for each, at planning time). The Actor
re-verifies at CP1 (`grep -n` on `src/ffi/` and the header, plus an export check
against the built native — §4 CP1).

---

## §3 Decisions

Each decision: **Rule** (what the Actor builds), **Why**, **Alternatives**,
**Recommendation**. Where a decision needs the user, it points at a §9 question.

### D1 — Surface: five members per interface; `BeginTransaction` stays sync on both

**Rule.**

```csharp
// IProducer<TKey, TValue>  (KafkaProducer, MockProducer)
void InitTransactions();
void BeginTransaction();
void SendOffsetsToTransaction(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
    ConsumerGroupMetadata groupMetadata);
void CommitTransaction();
void AbortTransaction();

// IAsyncProducer<TKey, TValue>  (AsyncKafkaProducer, AsyncMockProducer)
Task InitTransactions(CancellationToken cancellationToken = default);
void BeginTransaction();
Task SendOffsetsToTransaction(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
    ConsumerGroupMetadata groupMetadata, CancellationToken cancellationToken = default);
Task CommitTransaction(CancellationToken cancellationToken = default);
Task AbortTransaction(CancellationToken cancellationToken = default);
```

Declared on each interface separately, no shared base (the M11/P8 D-6 precedent
for `Metrics()` and `Send(record, callback)`). All four types implement them as
one-line forwarders into new `NativeProducer` members (the forwarding pattern of
`MockProducer.cs:171-181`).

**Why.** `bindings/dotnet/CLAUDE.md` §4 decides sync vs async from the Java
implementation. `initTransactions`, `sendOffsetsToTransaction`,
`commitTransaction` and `abortTransaction` each end in `result.await(maxBlockTimeMs,
...)` (`KafkaProducer.java:648`, `:732-745`, `:779`, `:813`) -> `Task` +
`CancellationToken` on the async interface. `beginTransaction` is a local state
transition (`:674`) and is already on the stays-sync list (`CLAUDE.md:487`,
`:575`). The sync interface is Java 1:1. The offsets parameter type matches the
consumer's shipped `Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>)`
(`IConsumer.cs:258`) — the idiom map's `Map` row.

**Alternatives.** (a) `Task BeginTransaction(ct)` on the async surface for Python
parity (`producer.py` routes begin through `begin_transaction_async`) — rejected:
contradicts §4, Java does not block. (b) A separate `ITransactionalProducer` —
rejected: Java declares the methods on `Producer`. (c) Default interface members —
unavailable on netstandard2.0.

**Recommendation.** As Rule. Adding interface members breaks only external
implementers. The tree has exactly four implementers — verified by searching
`src/`, `tests/`, `soak/`, `grpc-server/` and `tests/Performance/`:
`MockProducer.cs:70`, `AsyncMockProducer.cs:92`, `KafkaProducer.cs:70`,
`AsyncKafkaProducer.cs:89`; the soak client, perf suite and servicers only consume
the interfaces. The package is pre-publish, the same acceptance `CLAUDE.md` §3
records for `Metrics()` and `Send(record, callback)`.

### D2 — `ConsumerGroupMetadata` constructors; `SendOffsetsToTransaction` precondition order

**Rule — constructors (both `[Obsolete]`).**

- `ConsumerGroupMetadata(string groupId, int generationId, string memberId, string? groupInstanceId)`:
  `groupId` null -> `ArgumentNullException(nameof(groupId), "group.id can't be null")`;
  `memberId` null -> `ArgumentNullException(nameof(memberId), "member.id can't be null")`
  (`ConsumerGroupMetadata.java:37-46`). `groupInstanceId == null` is Java's
  `Optional.empty()`. Java's third check ("group.instance.id can't be null") guards
  the `Optional` reference itself and has no .NET analogue.
- `ConsumerGroupMetadata(string groupId)` -> `(groupId, -1, "", null)`
  (`:51-56`: `UNKNOWN_GENERATION_ID`, `UNKNOWN_MEMBER_ID`, `Optional.empty()`).
- `[Obsolete]` message, adapted from Java's javadoc ("Since 4.2, please use
  KafkaConsumer#groupMetadata() instead. This class will be an interface in Kafka
  5.0."): *"Deprecated since Kafka 4.2: use IConsumerCommon.GroupMetadata()
  instead. ConsumerGroupMetadata becomes an interface in Kafka 5.0."* (Q7).
- The existing internal 4-arg ctor (`ConsumerGroupMetadata.cs:37`) has the same
  signature, so it becomes the public obsolete one. The ABI marshal
  (`ConsumerGroupMetadataMarshal.cs:50` — the only construction site; it already
  coalesces null ids to `""`, `:37-57`) switches to a non-obsolete internal
  factory over a private ctor. Prefer zero `CS0618` suppressions in `src/` (at
  most one). Tests and `grpc-server` suppress it locally (precedent
  `grpc-server/AdminServiceImpl.cs:580`).

**Rule — `SendOffsetsToTransaction` order (both surfaces).**

1. `groupMetadata == null` -> `ArgumentNullException(nameof(groupMetadata),
   "Consumer group metadata could not be null")`. This is Java's own first check
   (`KafkaProducer.java:1498-1500`, an `IllegalArgumentException`, i.e. the
   idiom map's `ArgumentException` family).
2. `offsets` null and per-entry validation by reusing
   `NativeConsumer.SnapshotCommitOffsets` (`NativeConsumer.cs:3750`) verbatim —
   the same exception types and messages as the consumer's `Commit(offsets)`
   ("Topic names must not be null.", "Partition must not be negative.", "Offset
   value must not be null."; null leader epoch -> -1; null metadata -> `""`). The
   snapshot copies the caller's dictionary, so mutating it after the call returns
   cannot affect the operation.
3. `ThrowIfClosed()` -> `ObjectDisposedException`.
4. (async) an already-cancelled token -> `OperationCanceledException`.
5. Drain (D3).
6. Transient native group metadata: pin the three strings,
   `ConsumerGroupMetadata_new`, submit, `ConsumerGroupMetadata_destroy` in a
   `finally` (D12). The header documents an owned handle with no NULL-return case;
   guard a NULL return defensively (throw before any submit) and document the
   guard as unreachable.
7. `NativeConsumer.WithPinnedCommitOffsets` (`NativeConsumer.cs:3826`) -> the
   native call.

Semantic checks stay in the core and are **not** duplicated: generation > 0 with
an unknown member id -> Code -3 `LOCAL_ILLEGAL_ARGUMENT`, "Passed in group
metadata GroupMetadata(groupId = ..., generationId = ..., memberId = ,
groupInstanceId = ) has generationId > 0 but the member.id is unknown"
(`kafka_producer.rs:1666-1676`; exact rendering pinned by measurement at CP5);
non-transactional; not in a transaction; fenced.

**Why.** The null checks are Java's own (`IllegalArgumentException` /
`NullPointerException` -> `ArgumentNullException`: idiom map + ffi §A5). Public
constructors are needed because `SendOffsetsToTransaction` must be callable
without a live consumer — the gRPC servicers build the metadata from proto (Python
does the same, `grpc_translate.py:222-261`), the tests need it, and so does
consume-transform-produce against an externally coordinated group. `[Obsolete]` is
the faithful projection of Java's `@Deprecated(forRemoval = true)`. Reusing the
consumer's shipped snapshot / pin helpers avoids a second implementation of the
same marshalling (DoD #6).

**Documented divergences** (stated once, in the `IProducer` / `IAsyncProducer`
remarks, Q24):

- Java orders `throwIfInvalidGroupMetadata` -> `throwIfNoTransactionManager` ->
  `throwIfProducerClosed` (`KafkaProducer.java:732-737`). The binding's closed
  check is managed and precedes the native call (ffi §A5), while the other two
  live in the core. So a *closed* producer that is also non-transactional, or that
  is given generation>0 / unknown-member metadata, reports
  `ObjectDisposedException` where Java reports ISE / IAE. This is the same class
  of divergence already accepted for `Send`.
- Java's `MockProducer` fails a null metadata with `NullPointerException`
  (`Objects.requireNonNull`, `MockProducer.java:184`). .NET throws
  `ArgumentNullException` on both flavours.

**Alternatives.** (a) No public ctor (only `IConsumerCommon.GroupMetadata()`) —
rejected: blocks the servicers and the tests, and Python can construct one. (b)
Public but not obsolete — rejected: Java deprecates the constructors. (c) Validate
generation / member in .NET — rejected: Kafka logic in the binding
(`bindings/CLAUDE.md` §2.6).

**Recommendation.** As Rule (Q7, Q24).

### D3 — Drain the binding-side send accumulator before every control operation

**Rule.** Each of the five control entry points first drains the managed
`SendAccumulator`, so every `Send` that has *returned* to its caller has been
handed to the core (via `send_batch`) before the control call reaches the core.

- **Task-returning ops** (async `InitTransactions` / `SendOffsetsToTransaction` /
  `CommitTransaction` / `AbortTransaction`): `await accumulator.DrainPendingAsync(ct)`
  (`SendAccumulator.cs:595-640`) before submit — the `FlushAfterDrain` precedent
  (`NativeProducer.cs:326-329`). The drain is unbounded except by the token: the
  same deliberate asymmetry that `FlushWithAccumulatorDrainBound`'s comment records
  (`NativeProducer.cs:815-848`).
- **Blocking ops** — the five sync ops (which never have an accumulator) and
  `IAsyncProducer.BeginTransaction()` (which can). They use one bounded helper
  extracted from `FlushWithAccumulatorDrainBound` (`NativeProducer.cs:815-848`),
  bound `s_accumulatorDrainTimeout` = 30 s (`:132`). On expiry:
  `throw new KafkaException("The producer's send accumulator did not drain within
  {N:0} seconds, so records buffered in the binding have not reached the core and
  {op} was not attempted.")` with **no** native control call. This is the public
  message-only ctor, so `Code == 0` and `IsRetriable == false`, exactly like the
  flush precedent (`KafkaException.cs:74`). `{op}` is the Java method name:
  `initTransactions()`, `beginTransaction()`, `sendOffsetsToTransaction()`,
  `commitTransaction()`, `abortTransaction()`. The flush message stays
  byte-identical (it is pinned by the test at `SendAccumulatorTests.cs:638-697`).
- **No accumulator** (`AccumulatorToStop()` null, `NativeProducer.cs:1072` —
  always the case on the sync types, which never call `EnsureAccumulator`,
  `:984-1001`) -> no-op.
- **Implementation constraint (keeps D10's test deterministic).** When no
  accumulator exists, the async ops must reach `SubmitVoidOperation`
  synchronously in the calling frame — no unconditional `await` / `Task.Yield`
  before submit. On both paths the preconditions (D2 null checks, closed, an
  already-cancelled token) run in a non-`async` wrapper, so they throw
  synchronously rather than being captured into the returned `Task` — the
  `FlushWithCallback` shape (`NativeProducer.cs:297-311`: no accumulator -> straight
  to the core submit; accumulator -> synchronous preconditions, then an `async`
  drain-then-submit continuation).
- **Test seams.**
  - `internal void XxxWithAccumulatorDrainBound(TimeSpan)` for each blocking form.
    The public members pass `s_accumulatorDrainTimeout`.
  - `NativeProducer.CreateMock(bool autoComplete, SendAccumulatorSettings settings)`
    plus an override field that `EnsureAccumulator` consults instead of
    `SendAccumulatorSettings.FromEnvironment()` (`SendAccumulatorSettings.cs:207`).
  - An `internal` `AsyncMockProducer` ctor overload taking the settings, via the
    existing internal ctor `SendAccumulatorSettings(int slotThreshold, int
    batchWindowMs, int batchChunk, int maxAdmittedRecords)` (`:136-140`).
  - `NativeSubmit` (`NativeProducer.cs:265`, today `private`) widened to `internal`, and each
    of the four async control operations given an `internal` overload that takes the submit
    delegate; the public path passes the real P/Invoke. This is the admin precedent
    (`NativeAdminClient`'s `Native*Submit` delegates, exercised by the `AdminP*SubmitArgumentTests`
    files). It lets the D5 tests assert "the control operation was never submitted" directly
    instead of inferring it from core state.

  The two settings seams are needed because the tests run in parallel and `FromEnvironment`
  reads process-wide environment variables (`:117-122`). A 60 s batch window with a
  high slot threshold makes "records are still buffered when the control call is
  made" deterministic (Q16).

**Why.**

- The FFI's `with_txn_control` (`producer.rs:3172-3232`) drains only the FFI's own
  `send_async` outbox (`producer.rs:3204-3219`; producer-transactions.md §13).
  .NET never uses that outbox. Its async `Send` buffers records in the managed
  accumulator (default 10 ms window, `SendAccumulatorSettings.cs:74`), and a batch
  thread hands them to the core through the synchronous `send_batch`
  (`SendAccumulator.cs:925-980`, `:1302`). So records whose `Send` has already
  returned can still be in managed memory when a control call reaches the core.
- Java registers a record with the transaction synchronously inside `send()`
  (`doSend` -> `TransactionManager.maybeAddPartition`). This gives three
  guarantees:
  - (i) commit includes every returned send (`KafkaProducer.java:748`, "This method
    will flush any unsent records before actually committing the transaction");
  - (ii) abort discards them;
  - (iii) a send before `beginTransaction` is never swept into the transaction
    that begin opens.

  Without the drain, (i) and (ii) fail for records buffered at the time of the
  call. (iii) inverts: a send issued before `BeginTransaction` reaches the core
  after begin and silently joins the transaction.
- producer-transactions.md §13 establishes exactly this contract for the FFI
  outbox ("every send that had returned — not merely been called — is included in
  the operation"). D3 is the binding-level mirror for the binding's own buffer.

**Alternatives.**

- (a) No drain — rejected, for the three reasons above.
- (b) Disable the accumulator for transactional producers — rejected: forks the
  async send path on configuration (M11/P3.1 design, DoD #10).
- (c) Drain only commit / abort — rejected: (iii) needs begin and init, and §13
  drains all five.
- (d) Unbounded blocking drain in `BeginTransaction` — rejected: no token and no
  escape (the flush precedent's own argument).
- (e) Make async `BeginTransaction` `Task`-returning because it may now wait —
  rejected by default: `CLAUDE.md` §4 keeps it sync, and the wait is bounded and
  normally one batch window (Q6).

**Recommendation.** As Rule (Q6, Q16).

### D4 — Completion barrier for `CommitTransaction` / `AbortTransaction` on the async surface

**Rule.** After the D3 drain, and **before** the native commit or abort is
submitted, enqueue a barrier on the producer's `SendCompletionPump` (new `internal
Task EnqueueBarrier()`). After the native operation **succeeds**, await the barrier
before completing the user's `Task`.

- **Invariant:** a barrier completes only after every group enqueued before it has
  had its completions read, its delivery callbacks invoked, and its
  `TaskCompletionSource`s set (or faulted). If `RunLoop` coalesces several groups
  into one `get_all` pass, the barrier is a coalescing boundary.
- **Shape:** `PendingSendBatch` (`SendCompletionPump.cs:931-958`) gains a barrier
  form — `Count == 0`, empty arrays, a `TaskCompletionSource<bool>` created with
  `RunContinuationsAsynchronously`, and an `IsBarrier` read.
- **`RunLoop`** (`:473-540`): on a barrier, complete it and continue. No
  `ProcessGroup`, no `get_all` with count 0. `DequeueGroup`'s
  `Interlocked.Add(ref _drainedSends, group.Count)` (`:553-561`) adds 0, and
  `ProcessedBatchCount` is not incremented. State both explicitly in code comments.
- **`DrainAndFaultRemaining`** (`:828-844`, stop path): completes a barrier with
  **success** — it represents ordering, not a record — and skips `DestroyFutures`
  for it.
- **Gate / stop:** `EnqueueBarrier` follows `Enqueue`'s gate / stop protocol
  (`:277-300`, `:334-341`). Wherever `Enqueue` would fault a group in place,
  `EnqueueBarrier` completes the barrier successfully instead. This is a benign
  teardown race: the core then reports the closed producer on the control call
  itself.
- **No pump** (sync types, or an async producer that never sent): no barrier. It
  is trivially satisfied because the sync `Send` returns only after its completion
  was read and its callback ran. `_pump` is read under `_pumpLock`
  (`NativeProducer.cs:112-113`).
- **On native failure:** return the failure immediately and do not await the
  barrier. Java throws without the callback guarantee on failure.
- **Waiting with a token:** netstandard2.0 has no `Task.WaitAsync`. The barrier
  wait uses a registration-based helper; D5 defines what cancellation means here.

**Why.**

- `KafkaProducer.java:754-755`: "If the transaction is committed successfully and
  this method returns without throwing an exception, it is guaranteed that all
  Callbacks for records in the transaction will have been invoked and completed."
- On the async surface, per-record callbacks and `Task` completion happen on the
  pump thread, one FIFO group at a time. Without a barrier the commit `Task` can
  complete while the pump still holds already-resolved groups. A delivery-callback
  count or a `send.IsCompleted` check right after `await CommitTransaction()` then
  fails.
- `MockProducerTest` 330 (`shouldFlushOnCommitForNonAutoCompleteIfTransactionsAreEnabled`
  — `assertTrue(md1.isDone())` right after `commitTransaction()`) and 364
  (`shouldThrowOnAbortForNonAutoCompleteIfTransactionsAreEnabled` — `md1.isDone()`
  after `abortTransaction()`, because the mock's abort flushes,
  `MockProducer.java:230-246`, `:347-355`) encode the same guarantee. For a real
  abort, Java's Sender fails incomplete batches before it sends `EndTxn(abort)`.
- FIFO plus drain is sufficient. After D3's drain, every returned send's group is
  already in the pump queue:
  - the accumulator clears `_draining` in a `finally` only after `SendChain()`
    returns (`SendAccumulator.cs:933`, `:979`), and `SendChain()` includes
    `_pump.Enqueue`;
  - `IsEmptyAndIdleLocked` requires `!_draining` (`:652`).

  A barrier enqueued afterwards therefore completes only after all of them.

**Residuals** (documented in xmldoc, not fixed):

- **R-a.** `Clear()` on a manual (`autoComplete: false`) mock drops pending
  completions. A pump group that waits on them never resolves, so a later barrier
  never completes. `CommitTransaction` then waits until its token is cancelled
  (Q21). The same stranding already makes that producer's `Dispose` hang today,
  independently of this phase (file-forward item 10).
- **R-b.** A delivery callback (it runs on the pump thread) that synchronously
  blocks on `CommitTransaction` / `AbortTransaction` deadlocks, because the barrier
  sits behind the callback's own group. Java has the analogous hazard (callbacks
  run on the I/O thread that must complete the commit). Document only (Q14).
- **R-c.** The sync surface has no pump and no barrier. Concurrent sync sends on
  other threads are not ordered against the control call — the same ambiguity Java
  has for a `send` racing `commitTransaction` (producer-transactions.md §13 states
  the same boundary).

**Alternatives.**

- (a) No barrier — rejected (Java `:754-755`, MPT 330 / 364).
- (b) Wait for the pump to go idle — rejected: sends made after the commit call
  could extend the wait indefinitely; the barrier waits for exactly the groups
  ahead of it.
- (c) Also barrier `Flush` — Java's flush post-condition has the same shape — out
  of scope, filed forward (Q3).

**Recommendation.** As Rule (Q3, Q14, Q21).

### D5 — Cancellation semantics of the `Task` control operations

**Rule.**

| Point at which the token fires | Outcome |
|---|---|
| Already cancelled at entry | `OperationCanceledException` thrown synchronously, before any drain or native call — the `FlushWithCallback` order (`NativeProducer.cs:297-311`: `ThrowIfClosed` -> `ThrowIfCancellationRequested`). |
| During the D3 drain | The returned `Task` is cancelled and the control operation is **not** submitted. The drain itself continues (per-waiter cancellation, `SendAccumulator.cs:595-640`). |
| After submit | The awaiter is cancelled (`RegisterCancellation(ct, context.CancelAwaiter)` inside `SubmitVoidOperation`, `NativeProducer.cs:1340-1392`). The native operation continues to completion and the core acks its own result. A retry while it still runs -> Code -2, "Transactional methods of KafkaProducer are not safe for concurrent access." (`producer.rs:3628`). A retry after it finished sees the state it left behind (e.g. a completed commit -> the core's invalid-transition error). |
| While awaiting the D4 barrier (commit / abort only) | The `Task` completes **successfully**: the commit or abort did happen, and only that call's callback-ordering guarantee is lost. |

**Why.**

- ffi §A7 — "cancels the wait, never aborts an enqueued send" — applied to control
  operations: a native control operation cannot be aborted.
- Java has no cancellation. Its nearest analogue is the `max.block.ms` timeout,
  after which retrying the same operation is safe (`KafkaProducer.java:635`,
  `:763-764`, `:798-799`). That path remains available natively (the core returns
  Code 7, retriable).
- Reporting cancellation after a successful commit would invite an abort of a
  committed transaction — the misuse Java's javadoc warns against ("it is not
  possible to attempt a different operation (such as abortTransaction) since the
  commit may already be in the progress of completing", `:763-765`).

**Alternatives.** (a) Fault with `OperationCanceledException` after a barrier-wait
cancellation — rejected, for the reason above. (b) Ignore the token after submit —
rejected: inconsistent with every other async member.

**Recommendation.** As Rule (Q2). The xmldoc must say "cancellation abandons the
wait, not the operation" and carry the -2 retry caveat.

### D6 — Error classification: five hierarchy predicates on `KafkaException`

**Rule.** Add five public read-only `bool` properties:

- `IsTransactionAbortableError`
- `IsApplicationRecoverableError`
- `IsInvalidConfigurationError`
- `IsAuthorizationError`
- `IsOutOfOrderSequenceError`

How they are populated:

- **Classified construction.** `FromBorrowedHandle` (`KafkaException.cs:197-209`,
  the only classified construction site) reads them eagerly through five new
  P/Invokes, alongside `Code` / `Message` / `IsRetriable`, and passes them to a new
  internal ctor.
- **Existing internal ctor.** The 3-arg internal ctor (`:101`) chains to it with
  all five false.
- **Public ctors** (`:64`, `:74`, `:89`) leave all five false, as they already
  leave `Code == 0` and `IsRetriable == false`.

Additional rules:

- **Fencing.** There is no `IsFatal` (`KafkaException.cs:47-54` stays true).
  `ProducerFencedException` is `Code == 90`, documented rather than a property —
  it is a leaf class (root `CLAUDE.md` §10.4, "Leaf classes need no predicate").
- **Verbatim forwarding.** The binding forwards -2 / -3 / -4 and every other code
  verbatim and never branches on a code (ffi §B5).

**Polarity** (documented on each property, citing the Java class it translates).
The predicates are **not** complements:

- `AuthorizationException` is a subclass of `InvalidConfigurationException`, so
  codes 29 and 53 answer true to both.
- `UnknownProducerIdException` is a subclass of `OutOfOrderSequenceException`, so
  codes 45 and 59 both answer true to `IsOutOfOrderSequenceError`.
- `INVALID_TXN_STATE` (48) answers false to all of them.

**Truth table** — the contract the CP5 test asserts. Each row is produced through
`ErrorNext(code, msg)` on both mocks; the core builds the typed variant via
`Errors::error_with_message`.

| Code | Name | TxnAbortable | AppRecoverable | InvalidConfig | Authorization | OutOfOrder | IsRetriable |
|---|---|---|---|---|---|---|---|
| 120 | TRANSACTION_ABORTABLE | **T** | F | F | F | F | F |
| 90 | PRODUCER_FENCED | F | **T** | F | F | F | F |
| 47 | INVALID_PRODUCER_EPOCH | F | **T** | F | F | F | F |
| 29 | TOPIC_AUTHORIZATION_FAILED | F | F | **T** | **T** | F | F |
| 53 | TRANSACTIONAL_ID_AUTHORIZATION_FAILED | F | F | **T** | **T** | F | F |
| 35 | UNSUPPORTED_VERSION | F | F | **T** | F | F | F |
| 45 | OUT_OF_ORDER_SEQUENCE_NUMBER | F | F | F | F | **T** | F |
| 59 | UNKNOWN_PRODUCER_ID | F | F | F | F | **T** | F |
| 7 | REQUEST_TIMED_OUT | F | F | F | F | F | **T** |
| 48 | INVALID_TXN_STATE | F | F | F | F | F | F |

If the measured result differs from a row, that is a **core finding**: the Manager
routes it to `kafka-critic`, and the binding does not patch around it.

**Recovery-pattern mapping** (goes in the `IProducer` / `IAsyncProducer`
remarks). Java's `catch (ProducerFencedException | OutOfOrderSequenceException |
AuthorizationException)` -> `ex.Code == 90 || ex.IsOutOfOrderSequenceError ||
ex.IsAuthorizationError` -> close (the literal translation). Everything else ->
abort. `IsTransactionAbortableError` -> abort and retry (KIP-1050). The KIP-1050
variant replaces `ex.Code == 90` with `ex.IsApplicationRecoverableError`.

**Honesty note on the KIP-1050 variant (R15).** It is a **superset** of Java's
example, not a literal translation. The core's application-recoverable set is six
codes — FENCED_INSTANCE_ID 82, ILLEGAL_GENERATION 22, INVALID_PRODUCER_EPOCH 47,
INVALID_PRODUCER_ID_MAPPING 49, PRODUCER_FENCED 90, UNKNOWN_MEMBER_ID 25
(`src/common/error.rs:1806-1817`) — so five codes beyond PRODUCER_FENCED route to
"close" where Java's `catch (KafkaException)` arm would abort. `ex.Code == 90` is
the faithful form because `ProducerFencedException` is a leaf, and root `CLAUDE.md`
§10.4 sanctions a code comparison for leaves. The ABI exports no ProducerFenced
predicate (the header's `kafka_common_Error_is_*` set was listed at planning time) —
unlike TransactionAbortable, which is bound precisely because the control-op headers
name it as the must-abort signal — so for this leaf the code is the only handle, and
user code comparing it is not the binding branching on a code. The remarks lead with
the literal form and state the difference in one sentence (§7.1).

**Cost.** Five extra P/Invokes per error materialization — on the error path only,
never on the send path.

**Why.** Java expresses the recovery decision with `instanceof` on intermediate
classes. The flat `KafkaException` (ffi §A5) can express it only through
predicates, and the ABI already exports exactly these hierarchy predicates under
the C-FFI convention (root `CLAUDE.md` §3: "a predicate added on the Rust side is
expected on the C side too — C cannot see enum variants, so these are the only way
a C caller can classify an error"). A .NET caller is in the same position as that
C caller.
`TransactionAbortableException` is the one leaf in the set (`public class
TransactionAbortableException extends ApiException`; the core keeps its test an inherent method
for exactly that reason, `error.rs:1613-1625`). It is bound anyway because the control-op
headers name `kafka_common_Error_is_transaction_abortable_error` as *the* "must abort" signal
(e.g. the `send_offsets_to_transaction` Returns block, h:16155-16163), so without it a .NET
caller would have to compare `Code == 120` — branching on a code, which the binding exists to
spare its users.

**Alternatives.**

- (a) The minimal three (TxnAbortable, AppRecoverable, InvalidConfig).
- (b) Python's `TxnRequiresAbort` naming — rejected: the naming rule derives the
  name from the Java class (root `CLAUDE.md` §10.4).
- (c) Lazy predicates that keep the native handle alive — rejected: the exception
  owns copied values only (ffi §A5).
- (d) `IsFatal` — rejected: the core deliberately does not expose fatality
  (`KafkaException.cs:47-54`; `is_fatal_error` is not public and not in the C FFI).
- (e) `IsProducerFencedError` sugar — rejected: it is a leaf.

**Recommendation.** The five, as Rule (Q1).

### D7 — Per-record outcomes inside a transaction belong to the core

**Rule.** The send path gains **no** transaction-aware logic. Every record's
`Task<RecordMetadata>`, sync return value and delivery callback is settled only from
the core's completion, exactly as for a non-transactional send (ffi §A6 form C,
§A7). What the tests observe, and who decides it:

- **Real producer, abort.** The core fails the batches that were not yet
  acknowledged with Code -17 `TRANSACTION_ABORTED`, "Failing batch since
  transaction was aborted" (`error.rs:1523-1524`; `src/ffi/common.rs:406`, mapped at `:610`).
  Batches the broker had already acknowledged complete successfully — they are
  aborted on the log, and a `read_committed` consumer never sees them. The binding
  does **not** fault "every send since `BeginTransaction`".
- **Mock, commit or abort.** The mock flushes first (`mock_producer.rs:1107`,
  `:1152`), and its flush completes every pending completion **successfully**
  (`:301-310`, `while self.complete_next() {}`). So manual-mode pending sends
  resolve with metadata on **both** commit and abort, as in Java
  (`MockProducer.java:230-246`, `:347-355`; MPT 330, 364).
- **A failed commit** (installed hook, fenced, not in a transaction) returns before
  the mock's flush (`mock_producer.rs:1098-1105`). Pending manual sends therefore
  stay pending, and the binding does not complete them. On the async surface the
  D4 barrier is not awaited on this path.

**Why.** `bindings/CLAUDE.md` §2.6 (no Kafka logic in the binding). Only the core
knows which batches were acknowledged.

**Alternatives.** (a) Fault every pending async send with -17 on
`AbortTransaction` — rejected: it duplicates core logic, and it is wrong for
batches that were already acknowledged. (b) Complete pending mock sends in the
binding on commit — rejected: the core already does it.

**Recommendation.** As Rule. The tests assert outcomes (code, message, metadata),
never the mechanism.

### D8 — The three ABI-backed `MockProducer` transaction helpers

**Rule.** These are inherent **public** members on both `MockProducer` and
`AsyncMockProducer` (Q15), never on the interfaces — the mock-only precedent at
`MockProducer.cs:183-225`. They forward to new `NativeProducer` members modelled on
`MockErrorNext` / `MockHistoryCount` (`NativeProducer.cs:1112-1157`). All are
synchronous, and all take the `SafeProducerHandle` as the P/Invoke parameter
(ffi §A2 sync convention). Precondition order is argument checks, then
`ThrowIfClosed()`, then native.

| Java | .NET | Contract |
|---|---|---|
| `public RuntimeException commitTransactionException` (`MockProducer.java:82`); `commitTransaction` throws it while it is set (`:204-213`) | `void SetCommitTransactionError(int code, string? message = null)` and `void ClearCommitTransactionError()` | A Java public mutable field becomes a setter plus a clearer (Q8). Pre-validate what the ABI would reject (h:16541-16556): `code == 0` -> `ArgumentOutOfRangeException(nameof(code), code, "The commit-transaction error code must be non-zero; 0 is Errors.NONE.")`; `code` outside `short` -> `ArgumentOutOfRangeException(nameof(code), code, "The commit-transaction error code must fit in a 16-bit signed integer.")`. A `false` return after validation is unreachable on a mock handle; guard it with `InvalidOperationException` and document it as unreachable. A `null` message means the code's default message (the ABI's null convention; pinned call-scoped as in `MockErrorNext`). The hook stays installed until cleared, as the Java field does. The xmldoc states the ABI's **setup-only** rule verbatim in substance: it must not overlap a control call on the same producer (h:16562-16569). The binding adds no guard for it (R11). |
| `sentOffsets()` (`:456`) | `bool SentOffsets()` | A **method**, not a property — the `HistoryCount()` FDG precedent (`MockProducer.cs:208-218`): it P/Invokes and can throw `ObjectDisposedException`. Semantics are the core's: reset only by `BeginTransaction`, not by commit (MPT 464). |
| `consumerGroupOffsetsHistory()` (`:479`), single-entry projection only (T12, B6) | `OffsetAndMetadata? CommittedOffset(string groupId, TopicPartition partition)` | `groupId` null -> `ArgumentNullException`; `partition.Topic` null (a `default` `TopicPartition`, `TopicPartition.cs:37` is a struct) -> `ArgumentException("Topic names must not be null.", nameof(partition))`. Not found -> `null`. Leader epoch `-1` -> `null` (h:16606-16607). The metadata grow rule is below. |

**The metadata grow rule (Q9).** The ABI truncates metadata at a UTF-8 character
boundary and does not report it (h:16608-16612). A truncated write is therefore
between `cap - 4` and `cap - 1` bytes long.

1. Call with `cap = 4101` — Kafka's default `offset.metadata.max.bytes` (4096)
   plus the NUL plus the 4-byte ambiguity window below, so any metadata within
   the broker default resolves in **one** call.
2. Let `len` be the byte length up to the first NUL. The ABI leaves `cap - 1`
   bytes of room and cuts at the last character boundary that fits, and a UTF-8
   character is at most 4 bytes, so `len < cap - 4` proves the value was **not**
   truncated, while `len >= cap - 4` is ambiguous (truncated, or genuinely that
   long). While ambiguous, retry with `cap *= 4`, clamped to a ceiling of
   `1,048,581` (1 MiB + 5). The sequence is 4101 -> 16404 -> 65616 -> 262464 ->
   1048581: at most five calls.
3. Still ambiguous at the ceiling means the metadata is at least 1,048,577 bytes
   long either way, so throw `InvalidOperationException("The committed offset
   metadata is larger than 1048576 bytes and cannot be returned untruncated.")` —
   the "+ 5" is what makes that sentence exact. Returning a silently truncated
   string is the worse failure. A false-positive retry (a value of exactly
   `cap - 4` .. `cap - 1` bytes) costs one extra lookup and is otherwise harmless.
4. The loop is an internal static helper that takes the native call as a
   delegate (the D3 `NativeSubmit` seam pattern), so S7 counts attempts with a
   fake as well as running the real P/Invoke end to end.

Each retry is a fresh lookup. That is sound because the helper is documented as
test-setup-only, like the hook. An embedded NUL in the metadata ends the string —
a limitation of the NUL-terminated ABI, documented, not worked around.

**Remarks updates.** `MockProducer.cs:36-43` and the `AsyncMockProducer` remarks
list the new helpers. They also state which Java helpers are **absent** and why,
per §2.2's C ABI column (⚠ B5's and B6's qualifiers added at the CP1 review,
finding 84.5):

- B1-B4 and B7 are not exported at the C ABI.
- B5: the `history()` list and `uncommittedRecords()` are not exported. Only
  `history()`'s count is, as the existing `HistoryCount()`.
- B6: the full `consumerGroupOffsetsHistory()` list and `uncommittedOffsets()` are
  not exported. Only a single-entry projection is, as `CommittedOffset`.

That keeps the mock's surface honest about `fenceProducer`,
`transactionInitialized()` and the others, per the "never silently drop"
requirement, without saying that `history()` is missing from a type that exposes
`HistoryCount()`.

**Why.** These are exactly the three mock controls the ABI exports (T10-T12). The
Makefile comment that scopes this phase names them too (`Makefile:287-290`).

**Alternatives.** (a) `internal` helpers exposed only to tests — rejected: Java's
are public, and the existing mock helpers are public (Q15). (b) A property
`CommitTransactionError { set; }` mirroring the Java field — rejected: a
write-only property is an FDG anti-pattern, and the setter needs two inputs. (c) A
fixed 512-byte buffer as in Python (`_confluentkafka.c:1099-1117`) — rejected: it
truncates silently at 511 bytes. Filed forward for Python (D15).

**Recommendation.** As Rule (Q8, Q9, Q15).

### D9 — Idempotence: configuration pass-through, pinned by tests

**Rule.** No new API — Java has none. Idempotence reaches the core through the
existing config dictionary. One new test file pins the core's validation through
**both** real producer constructors (`KafkaProducer`, `AsyncKafkaProducer`). No
broker is needed: every case fails in the constructor or before any network I/O,
with `bootstrap.servers=localhost:9092` as the unreachable address (precedent
`PublicProducerSendClosedCheckTests.cs:53`).

The table gives the expected text for each case. Every row asserts `Code` **and**
the exact `Message` (DoD §3). Codes are measured at CP5 and pinned from the
measurement, not assumed.

| Config (plus bootstrap) | Where it fails | Expected message (core source) | Code (expected, measured at CP5) |
|---|---|---|---|
| `transactional.id=t`, `enable.idempotence=false` | ctor | "Cannot set a transactional.id without also enabling idempotence." (`producer_config.rs:764-767`) | -10 `CONFIG` |
| `transactional.id=t`, `acks=1` (idempotence **not** set explicitly, so it is silently disabled, `:731-744`) | ctor | same as above — Java's deliberate asymmetry | -10 |
| `enable.idempotence=true`, `acks=1` | ctor | "Must set acks to all in order to use the idempotent producer. Otherwise we cannot guarantee idempotence." (`:733-735`) | -10 |
| `enable.idempotence=true`, `retries=0` | ctor | "Must set retries to non-zero when using the idempotent producer." (`:720-724`) | -10 |
| `max.in.flight.requests.per.connection=6` (default idempotence) | ctor | "To use the idempotent producer, max.in.flight.requests.per.connection must be set to at most 5. Current value is 6." (`:748-752`) | -10 |
| `enable.idempotence=false`, no `transactional.id` | each of the five control methods | "Cannot use transactional methods without enabling transactions by setting the transactional.id configuration property" (`kafka_producer.rs:1645-1653`, core test `:7247-7291`) | -4 `LOCAL_ILLEGAL_STATE` |
| default (idempotent), no `transactional.id` | `InitTransactions`, `BeginTransaction`, `CommitTransaction`, `AbortTransaction` | "Transactional method invoked on a non-transactional producer." (`transaction_manager.rs:2530-2536`, core test `kafka_producer.rs:7293-7327`) | -4 |
| default (idempotent), no `transactional.id` | `SendOffsetsToTransaction` with a **non-empty** map | pinned by measurement (the core test does not cover it) | measured |
| default (idempotent), no `transactional.id` | `SendOffsetsToTransaction` with an **empty** map | **success** — Java returns before any transaction check (`KafkaProducer.java:738`; h:16135-16140) | — |
| `transactional.id=t`, `max.block.ms=2000`, unreachable bootstrap | `InitTransactions` | the core's timeout text, `"Timeout expired after 2000ms while awaiting InitProducerId. ..."` (`transactional_request_result.rs:128-142`, reason at `kafka_producer.rs:661`); `IsRetriable == true` | 7 `REQUEST_TIMED_OUT` (the `Error::Timeout` variant, `error.rs:1428`, distinct from `LOCAL_TIMEOUT` -5) |

The last row is the Java "retry after timeout" contract (`KafkaProducer.java:635`).
The test calls `InitTransactions()` a second time and asserts it is **accepted**:
it times out again with the same code, rather than being rejected as an invalid
transition.

**Why.** DoD §3 (error messages are contract) and requirement item 6. The
idempotence rules are the core's; the binding's obligation is that they reach the
user unaltered on both flavours.

**Alternatives.** (a) `ProducerConfig` string constants
(`EnableIdempotence`, `TransactionalId`) — deferred (Q11): no Java-shaped constants
type exists in the binding yet, and adding one is a surface decision of its own.
(b) Validate in .NET — rejected (no logic in the binding).

**Recommendation.** As Rule (Q11).

### D10 — Concurrent control calls: the core's `-2`, no managed guard

**Rule.** The binding adds **no** lock or flag around control operations. The
core's `txn_control_busy` CAS rejects an overlapping control call with Code -2
`LOCAL_CONCURRENT_MODIFICATION`, "Transactional methods of KafkaProducer are not
safe for concurrent access." (`producer.rs:3191` sync, `:3628` async). The binding
surfaces it verbatim:

- **Sync:** a thrown `KafkaException`.
- **Async:** the core fires the callback **inline on the caller** during submit
  (`producer.rs:3615-3632`), so the member returns an **already-faulted `Task`** —
  not a synchronous throw. This is the consumer's concurrent-op precedent (ffi
  §B1, §B5).
- The xmldoc repeats the header: -2 is **not** a reason to abort (h:16161-16163).

**Test (deterministic).** An `AsyncKafkaProducer` with `transactional.id`,
`max.block.ms=5000` and an unreachable bootstrap.

1. `t1 = InitTransactions(cts.Token)`. No `Send` has happened, so no accumulator
   exists, and D3's implementation constraint puts the CAS inside this call.
2. `t2 = CommitTransaction()`: assert it is faulted, with `Code == -2` and the
   exact message.
3. Cancel `cts`. `t1` becomes cancelled (D5 row 3).
4. Dispose, bounded (D11).

A sync twin runs `InitTransactions()` on a worker thread while the test thread
retries `CommitTransaction()` every 50 ms until -2 is observed or the worker
returns. It asserts -2 was observed. The retry loop exists because a sync call
cannot signal that it has entered native; the 5 s window makes flakiness
negligible (R10).

**Why.** Managed serialization would mirror core logic (ffi §B1 anti-pattern) and
would turn Java's documented `ConcurrentModification`-style rejection into a
.NET-only wait.

**Alternatives.** A managed `SemaphoreSlim` — rejected, same reasoning as the
consumer's M3/P2 removal of its managed guard.

**Recommendation.** As Rule.

### D11 — Dispose while a control operation is in flight

**Rule.** No new teardown machinery. The contract asserted is:

- (a) the in-flight `Task` completes **exactly once** — success, a core error, or
  cancellation of the awaiter — and is never stranded;
- (b) there is no use-after-free. The span-the-op reference taken in
  `SubmitVoidOperation` (`NativeProducer.cs:1340-1392`) defers
  `Producer_destroy` until the completion callback has run. On the native side,
  the async task is registered and `destroy` joins it before dropping the producer
  (`producer.rs:3660-3662`, registration at `:3685`);
- (c) `Dispose` returns within a bound: the teardown path's existing bounds plus
  the operation's own `max.block.ms`. With no broker, `InitTransactions` cannot
  complete before `max.block.ms`, so the test sets it to 3000.

The sync surface has the same shape. The blocked call holds the marshaller's
call-scoped reference, so the destroy is deferred until it returns.

**Test.** For both flavours: start `InitTransactions` (a worker thread for sync),
then `Dispose()` from the test thread. Assert that `Dispose` returns and the
operation completes within 3 s + 30 s + slack. The operation's outcome — code and
message — is **measured** at CP5 (when S12 is written) and pinned. The pin is on the measured outcome,
not on a guess about how the core races close against init.

**Why.** ffi §A2 / §A7: the producer's teardown ordering is already correct for
`SubmitVoidOperation` users (`FlushCore` is the precedent). This decision only
proves that the new operations inherit it.

**Alternatives.** Cancel in-flight control operations at `Dispose` — rejected:
there is no native abort, and cancelling the awaiter would hide the core's outcome.

**Recommendation.** As Rule.

### D12 — Handle and pin hygiene for the new P/Invokes

**Rule.**

- **Sync control operations and mock helpers** pass `SafeProducerHandle` as the
  P/Invoke parameter (ffi §A2 sync convention, the `ProducerSend` precedent). A
  raw `DangerousGetHandle()` is forbidden on these.
- **Async control operations** go through `SubmitVoidOperation` unchanged: manual
  `DangerousAddRef` inside the `try`, `AbandonBeforeSubmit` when submit throws,
  release in `FreeGcHandle`, and the rooted `ProducerCallbacks.Operation`. There is
  no new delegate, no new completion source type and no new free site.
- **`SendOffsetsToTransaction` inputs are call-scoped to the submit P/Invoke.**
  This covers the pinned topic and metadata strings (`WithPinnedCommitOffsets`)
  and the transient `ConsumerGroupMetadata_t`. The async ABI marshals all of them
  on the calling thread before it returns (h:16346-16349), so they are released
  as soon as the submit P/Invoke returns — in `finally` blocks around it — and are
  never held until the callback. The three `ConsumerGroupMetadata_new` strings are
  pinned only for that call: the core copies them (`consumer.rs:1727-1733`,
  `to_string_lossy().to_string()`).
- **The error handle delivered to the async callback is owned** (T1 row) and is
  freed by the shipped `OperationCompletionSource.Complete` ->
  `KafkaException.FromHandle` path. D6's predicate reads happen inside
  `FromBorrowedHandle`, **before** that free.
- **New handle category, documented in ffi §A2 (RD7):** a *transient owned input
  handle*. `ConsumerGroupMetadata_t` is built by the binding from managed values,
  lent to exactly one call, and destroyed in the same frame.
- ⚠ **Watch items for CP4, recorded at the CP1 review** (Critic 84's notes, not
  findings):
  - `SubmitVoidOperation`'s `catch` assumes native never ran
    (`NativeProducer.cs:1384-1389`). So the send-offsets submit delegate must not
    throw once its P/Invoke has returned. The transient `ConsumerGroupMetadata_t`
    destroy and the array unpins go in `finally` blocks, and nothing that can throw
    runs after the call. A throw there would run `AbandonBeforeSubmit` for an
    operation native already owns: it releases the span-the-op ref and frees the
    `GCHandle` while the callback can still fire.
  - The `CommittedOffset` grow helper always passes `metadataCap == buffer.Length`.
    A larger cap lets native write past the pinned array (h:16625).

**Why.** It reuses every shipped lifetime mechanism, and adds only the transient
input handle, whose whole lifetime sits inside one method.

**Alternatives.** Cache a native handle inside the managed `ConsumerGroupMetadata`
— rejected: it would make a plain value type own a native resource (and need
`IDisposable`) for a non-hot call.

**Recommendation.** As Rule.

### D13 — No managed short-circuit for an empty offsets map

**Rule.** An empty dictionary is forwarded with `count == 0`, after the null,
closed and cancel preconditions. The binding never returns early.

**Why.** The two backends disagree **by design**, and each is faithful to its Java
counterpart (h:16135-16146):

- `KafkaProducer` short-circuits before consulting transaction state
  (`KafkaProducer.java:738`).
- `MockProducer` checks state first (`MockProducer.java:184-196`: `requireNonNull` at `:184`, the `verify*` calls at `:185-188`, the empty check at `:194-196`), so an empty map
  outside a transaction is an error.

A managed early return would erase the mock's error and duplicate core logic. The
MPT 170/176 translations rely on the mock's error: they pass an **empty** map
where Java passes `null`, because the .NET null-offsets precondition fires before
the state check (Q24).

**Alternatives.** Short-circuit like `KafkaProducer` — rejected, for the reason
above.

**Recommendation.** As Rule.

### D14 — gRPC servicers: five RPCs in each, Python-parity translation

**Rule.**

- `ProducerServiceImpl` and `AsyncProducerServiceImpl` each gain five overrides
  (V1-V5, §2.4). Each copies the `Flush` template exactly
  (`ProducerServiceImpl.cs:200-217`, `AsyncProducerServiceImpl.cs:213-230`): an
  unknown id -> `Translate.UnknownProducer`; **everything else inside the `try`**;
  `catch (Exception ex)` -> `Translate.ToProto(ex)`.
  - The sync servicer calls the sync members on the handler thread (Python's
    model).
  - The async servicer awaits the four `Task` members and calls
    `BeginTransaction()` synchronously.
- `Translate` gains two helpers:
  - `ProtoOffsetEntriesToDictionary(IEnumerable<Proto.OffsetEntry>)` — a port of
    `_proto_offset_entries_to_dict` (`grpc_translate.py:222-242`). Absent
    `metadata` -> `""`; absent `leader_epoch` -> `null`, forwarded as-is when
    present, as Python does. An empty list -> an empty dictionary (a legitimate
    "stage nothing").
  - `ProtoToGroupMetadata(Proto.ConsumerGroupMetadata?)` — a port of
    `_proto_to_group_metadata` (`:245-261`). An absent `group_instance_id` ->
    `null`. An **absent message** (C#'s generated property is `null` where
    Python's attribute access yields a default instance) is mapped to
    `new Proto.ConsumerGroupMetadata()`, giving `("", 0, "", null)` exactly as
    Python builds it. It uses the obsolete constructor under a local
    `#pragma warning disable CS0618` / `restore` pair (precedent
    `AdminServiceImpl.cs:580`, `:603`).
- **Translation happens inside the `try`.** This is a deliberate divergence from
  `grpc_server.py:302-303`, where both translations run **outside** the `try`, so
  a malformed request escapes there as a gRPC error instead of a `StatusResponse`.
  In .NET, every failure is a `StatusResponse`, matching the servicer's `Flush`
  template. The Python side is filed forward (D15).
- The class summaries change from "Maps the 8" to "Maps the 13"
  (`ProducerServiceImpl.cs:28`, `AsyncProducerServiceImpl.cs:28`). Each servicer
  then has 13 `public override` members; CP6 counts them.
- The multilanguage harness gains **6 arms**: 3 tests x (`__grpc_dotnet`,
  `__grpc_dotnet_async`). They start running once CP7 deletes the skips.

**Why.** It is the only way the cross-language transaction tests can cover .NET,
and it is the stated exit condition of the Makefile skip (`Makefile:287-291`).

**Alternatives.** (a) Leave the servicers for a later phase — rejected: the
Makefile comment makes their removal "the last step of that phase, not a follow-up
to it" (`:290-291`). (b) Mirror Python's translate-outside-`try` — rejected, for
the reason above.

**Recommendation.** As Rule.

### D15 — Documentation, RULE-DRAFTs, and the file-forward list

**Rule.** Xmldoc on every new member (§7), plus these edits to
Manager-drafted rules, which the **user** applies or signs off (Q12):

| ID | File:line | Change |
|---|---|---|
| RD1 | `bindings/dotnet/CLAUDE.md:58-59` | "Admin / transactions are **not** exposed yet." -> "Admin (M15) and producer transactions (M17/P1) are exposed; `begin_transaction_async` is deliberately unbound (§4 **Stays sync**)." |
| RD2 | `CLAUDE.md:154-158` (§3 sketch) | The `KafkaException` sketch lists `IsFatal`, which has never shipped (`KafkaException.cs:47-54`). Replace it with the five predicates and a note that fatality is not exposed. This corrects **pre-existing** drift and needs explicit sign-off. |
| RD3 | `CLAUDE.md:478` and `:480` (idiom map) | Row 478: add the four `Task` transaction methods to the producer examples. Row 480: `Code`/`IsRetriable` plus the five hierarchy predicates, citing root `CLAUDE.md` §10.4 and D6. |
| RD4 | `CLAUDE.md` §3 idiom map, new row | Java `@Deprecated(forRemoval = true)` -> C# `[Obsolete("<Java's deprecation text, adapted>")]`, warning level (never `error: true`). Internal construction goes through a non-obsolete factory, and tests and servicers suppress `CS0618` locally. First adopter: `ConsumerGroupMetadata` (D2). |
| RD5 | `CLAUDE.md:575` (§4 **Stays sync**, producer) | `BeginTransaction()` -> "(**shipped M17/P1** on both interfaces; drains the binding's send accumulator with a bounded wait first, D3)". Also list the four mock helpers. |
| RD6 | `CLAUDE.md` §4, new divergence note | A note carrying the file's existing warning marker: "§4 divergence — transaction control drains the send accumulator and orders completions (M17/P1)", covering the D3 drain, the D4 barrier and the D5 cancellation table, with pointers here. |
| RD7 | `.claude/rules/ffi-marshalling.md` Part A | §A2: the transient owned input handle category (D12). §A5 table (`:754`): the five predicate rows, plus correcting `_is_fatal`/`IsFatal` for the producer part. §A7: control operations over `SubmitVoidOperation`, the drain-before-control rule and the completion barrier (D3, D4). |
| RD8 | `.claude/rules/ffi-marshalling.md:115` (§0.1 tests) | Add the five predicates to the "each `bool`-returning fn is correct (`I1`)" list. |
| RD9 | `bindings/dotnet/CLAUDE.md` §8.4 or the STATUS header | No change unless the user wants the per-checkpoint gate-log location (Q20) recorded as a standing convention. |
| RD10 | `bindings/dotnet/CLAUDE.md` §7.5 (DoD) | New rule: a gate step that runs a filtered test selection passes only when the reported selected count is asserted (`running N tests`, `N passed`, or VSTest's `Total: N`), not on its exit code alone. It covers a libtest `-- <filter>`, `--exact` or `--skip` and `dotnet test --filter`; a libtest filter that selects nothing exits 0. Proposed by Critic 84 at the CP0 review (finding 84.1), added after approval. |
| RD11 | `.claude/rules/ffi-marshalling.md` §0.1 (tests) | New rule: a structural P/Invoke sweep pins each parameter's **name** as well as its type and its `out` / `[Out]` mark, and its mutation list includes a swap of two same-typed names. P/Invoke passes arguments by position, so where parameters share a type the names are the declaration's only statement of which header parameter sits where. Proposed by Critic 84 at the CP1 review (finding 84.4), added after approval. |
| RD12 | `bindings/dotnet/CLAUDE.md` §7.5 (DoD), next to RD10 | New rule for phase plans: a gate command lives in a list item or a code block, never in a table cell, and every "= 0" count check records its before-value as the control positive. Where a table cell cannot be avoided, the plan says for each escaped pipe whether it is markdown's escape or regex alternation. Proposed by Critic 84 at the CP1 review (finding 84.6), added after approval. |
| RD13 | `bindings/dotnet/CLAUDE.md` §7.4 (test conventions) for the test half; §7.5 (DoD), next to RD12, for the record half | New rule: a test's doc comment claims only what its assertions can detect; mutation-check the claim or drop it. It is the test-side twin of ffi §A6's round-5 rule that a claim which is not made cannot go stale. The same holds for gate records, commit messages and plan hand-offs: a count or cause they state is measured when it is written, at the base and at the worktree with both values quoted, and a claim about the ABI is counted from the header rather than from the binding's bound subset. A claim that something is absent is checked by a search that does not depend on one phrasing, such as reading every matching block whole, or it is not made. Proposed by Critic 84 at the CP1 review (findings 84.4, 84.7, 84.8 and 84.9), added after approval. |

**Stale in-code sentences removed as part of the work** (not RULE-DRAFTs):
`IAsyncProducer.cs:50` ("Transactions remain deferred."), `AsyncKafkaProducer.cs:37`,
and the `MockProducer` / `AsyncMockProducer` remarks (D8). This is the ffi §A6
round-5 rule: delete a stale claim, do not re-word it. The grep gate is in §4 CP5.

**File-forward list** (reported in the STATUS entry, not acted on):

1. Python: `grpc_server.py:302-303` translates outside the `try` (D14).
2. Python: `_confluentkafka.c:1099-1117` fixed 512-byte `committed_offset` buffer
   truncates silently (D8).
3. Python: `txn_requires_abort` naming (`_confluentkafka.c:1499`) has no Java
   counterpart (`error.rs:1616-1624` records the rename). The composed `is_fatal`
   (`:1463-1487`) has no core counterpart.
4. Rust: the stale comment `tests/integration/producer_transactions_test.rs:674-678`
   ("Python is deferred") (Q10).
5. ffi §B5 (`ffi-marshalling.md:1871`, `:1890`) says the core's `illegal_state`
   carries code `-1`. The core today has only `Error::local_illegal_state`
   (`error.rs:1421`) -> `LOCAL_ILLEGAL_STATE = -4` (`src/ffi/common.rs:392`, mapped at `:474`).
   Re-measure in a consumer phase.
6. ffi §B5 table (`:1782`) still maps `_is_fatal` -> `IsFatal`, which does not
   exist.
7. A completion barrier for `Flush` (Q3): Java's flush post-condition has D4's
   shape.
8. Mode-B rows B1-B7 (Q4), if the user wants any of them.
9. KIP-939 two-phase commit, once the Java reference moves past the checked-out
   tree (Q17).
10. `AsyncMockProducer.Clear()` on a manual (`autoComplete: false`) mock with sends
    still pending. The core drops the pending completions (`mock_producer.rs:730-738`,
    `completions.clear()`), which strands those futures exactly as Java's `clear()`
    does. The binding **amplifies** it: the pump's batched read of the group that
    holds a stranded future never returns, so every **later** send's `Task` — and a
    later D4 barrier — is stranded behind it (head-of-line), where Java would strand
    only the cleared futures. It also makes that producer's `Dispose` hang: teardown
    unblocks an in-flight `get_all` only by flushing (`SendCompletionPump.cs:84-96`),
    and the core's flush (`while self.complete_next() {}`) cannot resolve a completion
    `clear()` already dropped — the completion held the other `Arc` of the future's
    `ProduceRequestResult` (`mock_producer.rs:377-383`), so nothing ever marks it done
    and `Stop()`'s `_thread.Join()` (`:456-460`) never returns. Established by reading;
    no test exercises it today. Pre-existing (not introduced here); D4 inherits it as
    R-a. Documented in the `Clear()` and async commit / abort xmldoc (§7.1), not fixed
    (Q21, R13).
11. `ConsumerGroupMetadata` lacks Java's `equals` / `hashCode` / `toString`
    (`ConsumerGroupMetadata.java`: `toString` renders `GroupMetadata(groupId = %s,
    generationId = %d, memberId = %s, groupInstanceId = %s)`). Pre-existing on the
    consumer-side type; making it constructible raises its visibility. Filed forward
    by default; the cost if taken now is recorded in Q27.
12. A public error-code constants type. The binding has none (no `ErrorCode` type
    under `bindings/dotnet/src`, planning-time grep), so X2's literal recovery form
    and every test compare `Code` with a number (`90` for PRODUCER_FENCED). The ABI's
    `kafka_common_ErrorCode_t` enum (h:60 ff.) is the natural source; adding it is a
    public-surface decision of its own, like Q11's config constants.
13. Rust ABI doc wording, for `kafka-critic` (trivial; recorded at the CP1 review,
    corrected by findings 84.8 and 84.9). Eight async producer entry points say
    their callback "fires on the producer's dispatcher thread", yet also route a
    null handle through `callback`, and a null handle names no producer whose
    dispatcher could fire it:
    - the five transaction entry points, including `begin_transaction_async`,
      which .NET leaves unbound (B8). Their dispatcher sentences are at h:16278,
      h:16313, h:16339, h:16406 and h:16445, their null-producer clauses ("is
      reported through `callback`") at h:16291, h:16320, h:16353, h:16418 and
      h:16454, and their prototypes at h:16300, h:16329, h:16384, h:16427 and
      h:16463;
    - `flush_async` (h:16009) and `partitions_for_async` (h:16044), whose null
      producer is "reported via `callback`" (h:16015, h:16051);
    - `FutureRecordMetadata_get_async` (h:15579), whose null future "is reported
      as an error through `callback`" (h:15586-15587).

    Two neighbours word it differently and belong in the same fix. `close_async`
    (h:16026) calls a null producer "a no-op success" (h:16032) without saying
    whether `callback` fires. `FutureRecordMetadata_get_all_async` says only "the
    dispatcher thread" (h:15599) and turns a null entry into a per-index
    `InvalidRequest` error (h:15600-15601), without saying which dispatcher fires
    when no entry is non-null. Read in full, the producer's doc blocks that mention
    a dispatcher, in any wording, give these eight; a second sweep, of the producer
    blocks whose null clauses mention `callback`, gives the same eight. Both ran on
    header SHA-256 `e8d39f09…d111`, which this phase does not change. A grep for
    one phrase is how 84.8 missed "reported via `callback`".

**Recommendation.** As Rule (Q12, Q19, Q20, Q21, Q27).

### D16 — One phase, eight checkpoints

**Rule.** M17/P1 is a single phase, N=84, with checkpoints CP0-CP7 (§4). Each
checkpoint is one or more commits. `dotnet-critic` 84 reviews after every
checkpoint (Q23). The user is needed only at plan approval (which is also where the
go-aheads of Q18 and Q26 are given) and for the push to CI; the RULE-DRAFTs are handed
over at CP7 without blocking the close (Q12).

**Why.** Size is about 6,000-8,000 changed lines, roughly two thirds tests (§10.1); for
scale, M15/P12 closed at +5116 / -32 (`design/current/STATUS.md:10`). The surface is
cohesive: splitting sync from async would ship an interface change twice, and
splitting the servicers out would leave the Makefile skip in place past the phase
that is supposed to delete it (`Makefile:290-291`).

**Alternatives.** Three phases (surface / mock and tests / gRPC) — rejected, for
the reasons above (Q13).

**Recommendation.** As Rule (Q13, Q23).

---

## §4 Checkpoints and verification

### 4.1 Environment (planning-time facts, this machine)

- **Host:** macOS arm64 (Darwin 25.5.0). `dotnet --list-runtimes` shows
  `Microsoft.NETCore.App 10.0.12` **only** (SDK 10.0.401 under
  `/usr/local/share/dotnet`); `~/.dotnet` holds only first-use sentinels.
  - **net10.0** tests run locally.
  - **net8.0** tests do **not** run locally as-is: a net8.0 test assembly does not
    roll forward to the net10 runtime (`.semaphore/install-dotnet.sh` header). Two
    routes: (a) with the user's OK, run `.semaphore/install-dotnet.sh` (it installs
    SDK 10 plus the .NET 8 base runtime into `$HOME/.dotnet` and exports nothing, so
    the Actor sets `DOTNET_ROOT=$HOME/.dotnet`, `PATH=$HOME/.dotnet:$PATH` and
    `DOTNET_MULTILEVEL_LOOKUP=0` in its own shell for those runs); (b) otherwise
    CI's `verify-dotnet` jobs (Linux amd64 and macOS arm64) are the net8.0 verifier
    and every gate record says so. **Never** set `DOTNET_ROLL_FORWARD`: it would run
    the net8.0 assembly on the net10 runtime and prove nothing about net8.0 (Q18).
  - WARNING: `design/current/STATUS.md:285` and `design/history/M15/P12-admin-gaps/PLAN.md:56`
    both record "net8.0 IS executable locally (`~/.dotnet` has 8.0.30 + 10.0.11)". That
    describes a different machine state and is false on this host today; do not copy it
    into any M17 record.
  - **net462** is build-only everywhere (no Mono host); its TFM smoke tests are
    compiled, never run (`bindings/dotnet/Makefile:51-56`, `:67-69`).
    `bindings/dotnet/CLAUDE.md` §7.4 still lists a net462 smoke *round-trip*; the
    Makefile is what actually runs, so the plan follows it.
- **Docker:** server 29.6.1, aarch64 — available. `make test-integration-dotnet`
  prints SKIP on any non-Linux host (`Makefile:292-305`), so the local integration
  gate uses the recipe in §4.5, and CI's amd64 Linux `verify-dotnet` job remains the
  authoritative integration verifier (§4.6).
- **grpc-server** (`bindings/dotnet/grpc-server/Confluent.Kafka.GrpcServer.csproj`,
  net8.0, not in the sln) runs Grpc.Tools codegen, and the arm64 protoc is recorded
  as crashing during C# codegen (`Makefile:267-271`). M14/P2 formatted it on this kind
  of host (`design/current/STATUS.md:265`: "`dotnet format --verify-no-changes` clean on the
  **solution and on `grpc-server`**"), so CP0 **measures** which route works here: a host
  `dotnet build`, or only the amd64 image build (§4.5 step 3). Whichever route is
  unavailable is reported as "not run", never as passed.

### 4.2 Standing gates

Run from the repo root unless stated. Every checkpoint runs G1-G7; the extra gates
run where the table says. Each checkpoint's commands, exit codes and counts are
recorded in `bindings/dotnet/design/history/M17/P1-producer-transactions/gate/CP<n>.txt`
(a short text summary, not raw logs — raw logs stay in the Actor's scratch
directory; Q20).

| Gate | Command | Pass condition | When |
|---|---|---|---|
| G1 Mode A | `git diff 76629aea..HEAD -- src/ src/ffi/ cbindgen.toml generator/ tests/`; `shasum -a 256 target/include/confluent_kafka.h` | the diff prints **nothing**; the header hash equals the one CP0 recorded (the M14/P1 and M14/P2 precedent). Control positive: `git diff --stat 76629aea..HEAD -- bindings/dotnet/` non-empty from CP1 on. `tests/` is included because the Rust harness needs no change (§5.5). | every CP |
| G2 Build | `cargo build --features ffi --release`, then `dotnet build -c Release bindings/dotnet/Confluent.Kafka.sln` (`--no-incremental` at CP5 and CP7) | exit 0, `0 Warning(s)`, `0 Error(s)`, all six TFM outputs (library `netstandard2.0`/`net8.0`/`net10.0`, tests `net462`/`net8.0`/`net10.0`). `GenerateDocumentationFile` (`Confluent.Kafka.csproj:19`) plus `TreatWarningsAsErrors` (`Directory.Build.props`) make a missing xmldoc (CS1591), a dangling `cref` (CS1574) and an unsuppressed `[Obsolete]` use (CS0618) build errors. | every CP |
| G3 Format | `dotnet format bindings/dotnet/Confluent.Kafka.sln --verify-no-changes` | exit 0 | every CP; grpc-server added at CP6-CP7, soak via G8 |
| G4 net10 | `dotnet test -c Release -f net10.0 bindings/dotnet/Confluent.Kafka.sln` | all pass; total = previous CP's total + the tests this CP adds; never lower | every CP |
| G5 net8 | `dotnet test -c Release -f net8.0 bindings/dotnet/Confluent.Kafka.sln` | same total as G4, or recorded as "CI-verified (Q18)" | every CP |
| G6 P/Invoke count | `grep -r --include='*.cs' 'internal static extern' bindings/dotnet/src \| wc -l` | **697** at CP0 (`NativeMethods.cs` 218 + `NativeMethods.Admin.cs` 479); **715** from CP1 on — +18 at CP1, +0 at every other CP | every CP |
| G7 Hygiene | `git status --short`; `git diff --cached --name-only` before each commit; `git diff 76629aea..HEAD -- bindings/dotnet/src bindings/dotnet/tests bindings/dotnet/grpc-server Makefile \| grep -nE '^\+.*\b(TODO\|FIXME\|XXX\|HACK)\b'` (DoD #8) | staged paths are exactly the checkpoint's files; never `kafka`, `.claude/agents/*`, `bindings/dotnet/.claude/agents/*`, `COMMENTS.84.md`, `COMMENTS.DONE.84.md`, anything under `target/`, or the personas' local memory — `bindings/dotnet/.claude/agent-memory/*` and the repo-root `.claude/agent-memory/dotnet-{actor,critic}/`, which are untracked and not gitignored while their `.claude/agent-memory/` neighbours are tracked, so a broad `git add .claude/agent-memory/` would sweep them in (⚠ added at the CP0 review); the marker grep prints nothing | every commit |
| G8 Outside-sln consumers | `make -C bindings/dotnet test-soak-dotnet` (builds, formats and tests the soak client, which consumes `IProducer`/`IAsyncProducer`; G3 does not reach it — `bindings/dotnet/Makefile:76-106`); `dotnet build -c Release bindings/dotnet/tests/Performance/PerfV3` and `dotnet test -c Release -f net10.0 bindings/dotnet/tests/Performance/Confluent.Kafka.PerformanceTests/Confluent.Kafka.PerformanceTests.csproj --filter 'FullyQualifiedName!~PerfV3SmokeTests'` (the Docker-free perf gate, `bindings/dotnet/Makefile:131-151`; the soak client and the perf suite only *consume* the producer types, D1, so this gate guards source compatibility — overload resolution and the `[Obsolete]` constructors of D2 under `TreatWarningsAsErrors` — rather than interface conformance) | exit 0 (soak's net8.0 leg is subject to Q18) | CP4, CP5, CP7 |
| G9 grpc-server | host route: `dotnet build -c Release bindings/dotnet/grpc-server/Confluent.Kafka.GrpcServer.csproj` then `dotnet format bindings/dotnet/grpc-server/Confluent.Kafka.GrpcServer.csproj --verify-no-changes`; image route: §4.5 step 3 | 0W/0E (the same `Directory.Build.props` applies); override count `grep -c 'public override' bindings/dotnet/grpc-server/{ProducerServiceImpl,AsyncProducerServiceImpl}.cs` = **13** each (8 today) | CP6, CP7 |
| G10 Docker | §4.5 | §4.5 | CP0 (baseline), CP7 (gate) |
| G11 Rust hygiene | `cargo xtask format-check` and `cargo xtask lint` from the **repo root** (they false-fail from `bindings/dotnet/`, M15/P12 PLAN §0) | exit 0 — a no-op proof for a Mode A phase whose only non-binding edit is the `Makefile` | CP7 |

⚠ **Copying a command out of a table (added at the CP0 review; completed at the CP1
review, finding 84.6).** Markdown needs each `|` inside a table's code span escaped
as `\|`, and the raw file keeps the backslash. These are the escapes in this plan's
tables. In each of them the backslash is markdown's, so remove it when copying from
the raw file:

- G6's `\| wc -l`. Left in, the command is not a pipeline.
- G7's `Makefile \| grep`. Left in, everything after `--` is a pathspec, so the
  command prints the diff itself: a loud false fail.
- G7's pattern `(TODO\|FIXME\|XXX\|HACK)`, which runs under `-E`. Left in, it
  searches for a literal `|` and can never match, so the gate passes without being
  able to fail.
- The CP1 row's `nm -gU … \| grep -cw _<symbol>`. Left in, the command is not a
  pipeline.
- §7.1 X2's `\|\|`, which is C#'s `||`.

CP7's two `Makefile` checks used to sit in §4.3's table as well. The first of them
used `\|` as basic-regex alternation, where the backslash must stay, so the advice
above would have turned a live check into one that cannot fail. Both now sit in a
list under §4.3's table, written with `-E` and bare pipes. While work is
uncommitted, use the list forms in §10.2's execution note, which need no escaping.
Run gate greps as `command grep`: in the agents' shell `grep` is a wrapper function,
and a wrapper must not decide what a gate counts.

### 4.3 Checkpoints

Each checkpoint is one or more commits, and `dotnet-critic` 84 reviews it before the
next one starts (D16, Q23). File lists are expectations; a file outside the list is
explained in the commit message.

| CP | Content | Expected files | Tests added (§5) |
|---|---|---|---|
| **CP0** | Create `prashah_dev_dotnet_producer_transactions` off `76629aea` (Q22). Record the baselines: G1-G6 at the base, the grpc-server build route (§4.1), the integration roster, and the local Docker baseline (§4.5). No code. | `design/history/M17/P1-producer-transactions/gate/CP0.txt` (Q20) | none |
| **CP1** | The 18 `[DllImport]` declarations (§4.4) with ownership xmldoc copied in substance from the header, plus a structural sweep test. Before writing: `grep -n` each symbol in `src/ffi/*.rs` and in the header, and `nm -gU target/release/libconfluent_kafka.dylib \| grep -cw _<symbol>` = 1 for each (on Linux: `nm -D --defined-only ...so`). | `Internal/Interop/NativeMethods.cs`; new test `Interop/ProducerTransactionNativeMethodsTests.cs` | S1 |
| **CP2** | D6: the five `KafkaException` properties, `FromBorrowedHandle` reading them, the internal ctor chain. D2: the two public `[Obsolete]` `ConsumerGroupMetadata` ctors, the non-obsolete internal factory over a private ctor, `ConsumerGroupMetadataMarshal.cs:50` switched to it. | `KafkaException.cs`, `ConsumerGroupMetadata.cs`, `Internal/Interop/ConsumerGroupMetadataMarshal.cs`; tests: `KafkaExceptionTests.cs` (extended), new `ConsumerGroupMetadataTests.cs` | S2, S3 (constructor half) |
| **CP3** | D4: `SendCompletionPump.EnqueueBarrier()` and the barrier form of `PendingSendBatch`, with `RunLoop`, `DequeueGroup`, `DrainAndFaultRemaining` and the gate / stop protocol handling it. Not yet called by production code. | `Internal/SendCompletionPump.cs`; new test `Interop/SendCompletionPumpBarrierTests.cs` | S4 |
| **CP4** | `NativeProducer`: the five sync and four async control operations, the bounded blocking drain helper extracted from `FlushWithAccumulatorDrainBound` (the flush message byte-identical), the D3 seams (including the `NativeSubmit` widening), D4 wiring (barrier after the drain, awaited only on native success), D5, the three mock helpers' internal plumbing (grow rule, D8), the transient `ConsumerGroupMetadata_t` (D12). | `Internal/NativeProducer.cs`, `AsyncMockProducer.cs` (the internal settings-seam ctor overload only, D3); new tests `Interop/ProducerTransactionDrainTests.cs`, `Interop/ProducerTransactionCancellationTests.cs`, `Interop/MockProducerTransactionHelperTests.cs` | S5, S6, S7 |
| **CP5** | The public surface: five members on each interface with the xmldoc of §7, one-line forwarders in the four types, the mock helpers public on both mocks, remarks updates, and removal of the stale sentences. Stale-sentence gate: `grep -rn 'Transactions remain deferred' bindings/dotnet/src bindings/dotnet/grpc-server` prints nothing (today: `IAsyncProducer.cs:50`, `AsyncKafkaProducer.cs:37`). xmldoc non-vacuity: each new member's doc ID (e.g. `M:Confluent.Kafka.IProducer`2.InitTransactions`) appears in all three emitted `Confluent.Kafka.xml` files. | `IProducer.cs`, `IAsyncProducer.cs`, `KafkaProducer.cs`, `AsyncKafkaProducer.cs`, `MockProducer.cs`, `AsyncMockProducer.cs`; new tests `PublicProducerTransactionTests.cs`, `PublicSyncProducerTransactionTests.cs`, `PublicProducerTransactionMockControlTests.cs`, `PublicProducerTransactionErrorTests.cs`, `PublicProducerIdempotenceTests.cs`, `PublicProducerTransactionConcurrencyTests.cs`, `PublicProducerTransactionTeardownTests.cs`, `PublicProducerSendOffsetsToTransactionTests.cs` (S3's control-path half) | S3 (control-path half), S8, S9, S10, S11, S12 |
| **CP6** | D14: five overrides in each servicer, the two `Translate` helpers, the class-summary counts. G9 by the route CP0 recorded. | `grpc-server/ProducerServiceImpl.cs`, `grpc-server/AsyncProducerServiceImpl.cs`, `grpc-server/Translate.cs` | none in the unit suite (grpc-server is outside the sln and has no test project — the recorded M14/P2 constraint); covered by G10 at CP7 |
| **CP7** | Root `Makefile`: delete the comment block `:272-291` and the three `--skip` lines `:309-311`, re-terminate `:308` with `-- __grpc_dotnet; \`. Checks: the two `Makefile` checks in the list under this table, each recorded before and after the edit (⚠ moved out of the table at the CP1 review). Then the full G1-G11 with `--no-incremental`, the G10 gate, and the hand-offs: the RULE-DRAFTs to the user (§7.2, Q12; non-blocking), STATUS entry drafted for the Manager's Step 7, the user pushes for CI (Q25). | `Makefile` | none |

⚠ **CP7's `Makefile` checks (moved out of the table at the CP1 review, finding
84.6).** Run both from the repo root, and record each value before and after the
edit. The before-value is the control positive: it proves the check can fail.

- `command grep -cE 'producer_transactions|THREE TESTS ARE SKIPPED|--skip test_' Makefile`
  prints 5 before the edit, all inside what CP7 deletes, and must print 0 after it.
- `make -n test-integration-dotnet | command grep -c -- '--skip'` prints 3 before
  the edit and must print 0 after it. The count is scoped to this one target,
  because other targets and comments in the `Makefile` use `--skip` legitimately.
  Run it on the macOS host only. The recipe line calls `$(MAKE)`, so GNU make
  executes that line even under `-n`. Here it only prints the non-Linux SKIP
  banner, but on a Linux host the same command runs the integration suite for real.

### 4.4 The P/Invoke set (CP1)

All go in `NativeMethods.cs` as `[DllImport(DllName, EntryPoint = "<symbol>",
CallingConvention = CallingConvention.Cdecl)]`. C# names follow the file's existing
short-name convention (`ProducerFlushAsync`, `MockProducerErrorNext`,
`IsRetriable`). Header prototypes were read at planning time (h:16090-16628,
h:13216, h:11872-12074).

| # | C# name | Symbol | C# signature | Ownership / note |
|---|---|---|---|---|
| 1 | `ProducerInitTransactions` | `kafka_producer_Producer_init_transactions` | `IntPtr (SafeProducerHandle producer)` | returns an **owned** `Error_t*` or `IntPtr.Zero` -> `KafkaException.FromHandle` |
| 2 | `ProducerBeginTransaction` | `kafka_producer_Producer_begin_transaction` | same as 1 | same |
| 3 | `ProducerSendOffsetsToTransaction` | `kafka_producer_Producer_send_offsets_to_transaction` | `IntPtr (SafeProducerHandle producer, IntPtr[] topics, int[] partitions, long[] offsets, int[] leaderEpochs, IntPtr[] metadata, int count, IntPtr groupMetadata)` | arrays from `WithPinnedCommitOffsets`, group metadata from #13; all borrowed for the call |
| 4 | `ProducerCommitTransaction` | `kafka_producer_Producer_commit_transaction` | same as 1 | same |
| 5 | `ProducerAbortTransaction` | `kafka_producer_Producer_abort_transaction` | same as 1 | same |
| 6 | `ProducerInitTransactionsAsync` | `kafka_producer_Producer_init_transactions_async` | `void (IntPtr producer, ProducerCallbacks.OperationCallback callback, IntPtr userData)` | raw pointer under `SubmitVoidOperation`'s span-the-op ref (ffi §A2 async convention); the callback **owns** the delivered error |
| 7 | `ProducerSendOffsetsToTransactionAsync` | `kafka_producer_Producer_send_offsets_to_transaction_async` | `void (IntPtr producer, IntPtr[] topics, int[] partitions, long[] offsets, int[] leaderEpochs, IntPtr[] metadata, int count, IntPtr groupMetadata, ProducerCallbacks.OperationCallback callback, IntPtr userData)` | inputs marshalled on the calling thread before return (h:16346-16349), so the pins and the transient handle end with the submit P/Invoke |
| 8 | `ProducerCommitTransactionAsync` | `kafka_producer_Producer_commit_transaction_async` | same as 6 | same as 6 |
| 9 | `ProducerAbortTransactionAsync` | `kafka_producer_Producer_abort_transaction_async` | same as 6 | same as 6 |
| 10 | `MockProducerSetCommitTransactionError` | `kafka_producer_MockProducer_set_commit_transaction_error` | `[return: MarshalAs(I1)] bool (SafeProducerHandle producer, [MarshalAs(I1)] bool clear, int errorCode, IntPtr errorMessage)` | message pinned call-scoped (`Utf8Marshal.Pin`); `IntPtr.Zero` = the code's default message |
| 11 | `MockProducerSentOffsets` | `kafka_producer_MockProducer_sent_offsets` | `[return: MarshalAs(I1)] bool (SafeProducerHandle producer)` | — |
| 12 | `MockProducerCommittedOffset` | `kafka_producer_MockProducer_committed_offset` | `[return: MarshalAs(I1)] bool (SafeProducerHandle producer, IntPtr groupId, IntPtr topic, int partition, out long offset, out int leaderEpoch, [Out] byte[] metadata, int metadataCap)` | caller-owned buffer; the D8 grow rule |
| 13 | `ConsumerGroupMetadataNew` | `kafka_consumer_ConsumerGroupMetadata_new` | `IntPtr (IntPtr groupId, int generationId, IntPtr memberId, IntPtr groupInstanceId)` | **owned** result, destroyed in the same frame by the already-bound `ConsumerGroupMetadataDestroy` (`NativeMethods.cs:366-367`); `groupId` / `memberId` never NULL (D2 guarantees non-null values; pass `""` defensively), `groupInstanceId` `IntPtr.Zero` for none |
| 14-18 | `IsTransactionAbortableError`, `IsApplicationRecoverableError`, `IsInvalidConfigurationError`, `IsAuthorizationError`, `IsOutOfOrderSequenceError` | `kafka_common_Error_is_{transaction_abortable,application_recoverable,invalid_configuration,authorization,out_of_order_sequence}_error` | `[return: MarshalAs(I1)] bool (IntPtr error)` | **borrowed**; read inside `FromBorrowedHandle` before any free; false for NULL |

Sync operations take the `SafeProducerHandle` (ffi §A2 sync convention — the
marshaller's call-scoped ref); the async ones take a raw `IntPtr` because
`SubmitVoidOperation` already holds the span-the-op ref (`NativeProducer.cs:1340-1392`).
`kafka_producer_Producer_begin_transaction_async` is **not** declared (B8, Q5).

### 4.5 The local Docker gate (G10)

The recipe is the one M14/P2 ran (`design/current/STATUS.md:257`) and M15/P12 restated
(`design/history/M15/P12-admin-gaps/PLAN.md:58-66`), with one change: the cross-build
target directory moves inside the ignored `/target`. **Both** images are built — M14/P2's
shape; M15/P12 needed only the sync one — because this phase changes the sync *and* the
async servicer.

0. `docker info` must succeed. If it does not, G10 is recorded as **not run**.
1. Cross-build the Linux native (the host is arm64, the images are amd64):
   `docker run --rm --platform linux/amd64 -v "$PWD":/work -w /work rust:1-bookworm cargo build --features ffi --release --target-dir target/linux-amd64`.
   WARNING: M14/P2 and M15/P12 used `--target-dir target-linux-amd64`, which `.gitignore:1`
   (`/target`) does not cover — it appears as untracked worktree noise. Use the path
   inside `/target`.
   `build.rs:110` writes the header to `target/include/confluent_kafka.h` under the
   crate root whatever `--target-dir` says, so this step rewrites the host header: re-run
   G1's header hash afterwards (the content is expected to be identical).
2. Stage it where the Dockerfiles copy from (`Dockerfile.grpc:40`):
   `cp target/linux-amd64/release/libconfluent_kafka.so target/release/libconfluent_kafka.so`.
   The header comes from `target/include/` (`Dockerfile.grpc:41`). The staged `.so` does not
   disturb the host build: the csproj picks the per-OS file name
   (`Confluent.Kafka.csproj:52-54`), so macOS keeps loading the `.dylib`.
   **Verify the exports inside a Linux container**, e.g.
   `docker run --rm --platform linux/amd64 -v "$PWD":/work -w /work rust:1-bookworm sh -c "nm -D --defined-only target/release/libconfluent_kafka.so | grep -cE ' T kafka_(producer_Producer_(init_transactions|begin_transaction|send_offsets_to_transaction|commit_transaction|abort_transaction)|producer_Producer_(init_transactions|send_offsets_to_transaction|commit_transaction|abort_transaction)_async|producer_MockProducer_(set_commit_transaction_error|sent_offsets|committed_offset)|consumer_ConsumerGroupMetadata_new|common_Error_is_(transaction_abortable|application_recoverable|invalid_configuration|authorization|out_of_order_sequence)_error)$'"`
   — WARNING: macOS `nm -D` reads **zero** symbols from a Linux ELF, and a `debian:*-slim`
   image has no `nm` at all; both are false negatives (`design/current/STATUS.md:257`, M14/P2).
   The end-anchored selector names exactly the 18 bound symbols (5 sync + 4 async control
   operations, 3 mock helpers, `ConsumerGroupMetadata_new`, 5 predicates), so it must print
   **18** — an unanchored pattern would also count the `_async` twins.
3. `DOCKER_DEFAULT_PLATFORM=linux/amd64 make -C bindings/dotnet grpc-image grpc-image-async`.
   The sync image hosts `ProducerServiceImpl`, the async image
   `AsyncProducerServiceImpl` (`grpc-server/Program.cs:88-95`, `CONSUMER_FLAVOR`).
   This step compiles grpc-server with `TreatWarningsAsErrors`, so it doubles as
   G9's image route.
4. **Unset** `DOCKER_DEFAULT_PLATFORM` (the broker must stay native arm64), then:
   `cargo test --features integration-tests,multilanguage-tests --test integration -- __grpc_dotnet`
   with **no** `--skip`, at CP0 and at CP7 alike, so the six transaction arms are
   measured rather than hidden.
5. Roster: `cargo test --features integration-tests,multilanguage-tests --test integration -- __grpc_dotnet --list`.
   It must be identical at CP0 and CP7 (this phase adds no Rust test) and must
   contain the six arms
   `test_{transactional_records_are_visible_only_after_commit,aborted_transaction_records_are_discarded,consume_transform_produce_with_offsets}__grpc_dotnet{,_async}`
   (generated by `tests/integration/producer_transactions_test.rs:685-701` through
   `tests/common/multilanguage_test_macro.rs:56`).
   ⚠ **Corrected at the CP0 review (finding 84.1).** In the roster and the run log
   each arm carries its module path, `producer_transactions_test::test_...`. libtest's
   `--exact` compares the filter with that full path, so the short spelling above
   selects **nothing** under `--exact` and still exits 0 (`running 0 tests`). Without
   `--exact`, the short spelling also selects the `_async` twin. Every isolated run in
   the gate below therefore names the test by its full path, exactly as
   `gate/CP0-failed.txt` spells it.

**Expected at CP0:** the six transaction arms fail — the servicer base answers
`Unimplemented` (`Makefile:277-281`; recorded as residual (b) at `design/current/STATUS.md:15`) —
and every other arm passes, apart from known shared-broker flakes (the M14/P2 note (v)). This
is an expectation to be **measured**, not assumed: the gate below compares the two measured
failure sets, so it holds whatever the baseline turns out to be.

**Gate at CP7:**

- Sorted failing-name lists from both runs (`grep -E '^test .* \.\.\. FAILED$'`).
  `comm -13 CP0-failed CP7-failed` must be **empty**: no new failure. A new failure
  is re-run alone twice, by its full path with `--exact`. A re-run counts only if it
  prints `running 1 test` and `test result: ok. 1 passed`. If it passes both times it
  is recorded by name as a flake, otherwise it is a regression.
- `comm -23 CP0-failed CP7-failed` must contain **exactly** the six transaction arms,
  and all six must appear in the CP7 `ok` list.
- WARNING: a libtest run whose filter matches nothing prints `test result: ok. 0
  passed` and exits 0. The gate therefore also requires the CP7 run's total to equal
  the roster count from step 5, and `grep -c` over the six names in the CP7 `ok`
  list to return 6.
- The six arms also pass when run alone, each once, named by the full path from
  `gate/CP0-failed.txt`, for example
  `cargo test --features integration-tests,multilanguage-tests --test integration -- producer_transactions_test::test_transactional_records_are_visible_only_after_commit__grpc_dotnet --exact`.
  A run counts only if it prints `running 1 test` and `test result: ok. 1 passed`;
  its exit code alone is not evidence. The WARNING above applies to every filtered
  run, not only the full one. (⚠ Corrected at the CP0 review, finding 84.1.)

### 4.6 Local evidence versus the CI verifier

| Check | Local | CI |
|---|---|---|
| 0W/0E, six TFM outputs | yes (G2) | yes |
| net10.0 unit tests | yes (G4) | yes, Linux amd64 and macOS arm64 |
| net8.0 unit tests | only via Q18's install | yes — the verifier when Q18 is declined |
| net462 | build only | build only |
| grpc-server compile and format | host route if protoc works, else image route (G9) | inside the images |
| multilanguage `__grpc_dotnet` / `__grpc_dotnet_async` | §4.5 recipe (G10) | **authoritative**: `make verify-dotnet` -> `test-integration-dotnet` (`Makefile:449-451`), the only place the target runs as written, now without the three `--skip` lines |
| perf p99 smoke | not required (no per-record change, §6 DoD #10) | runs inside `verify-dotnet` |

`make test-integration-dotnet` cannot be the local verifier on this host: it skips
on non-Linux (`Makefile:292-305`), and the amd64-only image constraint
(`Makefile:267-271`) means only CI runs it end to end. The close-out states both
halves, as M15/P12 did: the local run as evidence, CI as the verifier. The phase is
not reported as CI-verified until the user has pushed and the `verify-dotnet` jobs
are green (Q25).

---

## §5 Test plan

All paths below are relative to `bindings/dotnet/tests/Confluent.Kafka.UnitTests/`.
"Both mocks" means `MockProducer` and `AsyncMockProducer`; "both real flavours" means
`KafkaProducer` and `AsyncKafkaProducer`; "all four" means all four implementers.

### 5.1 Principles

1. **Every flavour the behaviour exists on.** The sync and async control paths are
   separate code (D1, D3), so a test on one flavour cannot catch the natural
   one-flavour bug (the ffi §A6 form-C precedent). Control-path behaviour runs on all
   four; configuration and core-error behaviour on both real flavours; mock helpers on
   both mocks.
2. **`Code` and the exact `Message`.** Every error assertion checks `Code` and the
   full `Message` with ordinal equality (DoD §3). Values that come from the core are
   **measured** at the checkpoint that writes the test and recorded in that
   checkpoint's gate log (Q20). The plan's "expected" values are predictions: a
   measurement that contradicts one is a core finding, routed to `kafka-critic` (R3),
   and the test pins it only after the Manager has ruled it is not a core defect.
   Managed precondition exceptions are asserted by type, `ParamName`, and — where the
   binding authors the text — the message, built TFM-agnostically as
   `new ArgumentNullException("groupId", "group.id can't be null").Message` (the
   framework appends the parameter-name suffix in a TFM-specific format).
3. **Bounded and sleep-free.** Every wait that can hang goes through `TestTimeout.Run`
   (`TestTimeout.cs:33`, `:54`, `:74`). No `Thread.Sleep` / `Task.Delay` is used as
   synchronization. Two timing windows are sanctioned and named: D10's sync retry loop
   (a 50 ms poll that stops at the first observation, R10) and S5's barrier hold (a
   bounded window that gives a mutation room to show; the pass condition itself is a
   deterministic probe).
4. **Seams, not the environment.** Determinism comes from D3's internal seams —
   `NativeProducer.CreateMock(bool, SendAccumulatorSettings)`, the internal
   `AsyncMockProducer` settings overload, the `XxxWithAccumulatorDrainBound(TimeSpan)`
   forms, the `NativeSubmit` overloads — from D8's grow-loop delegate, and from the
   existing `WaitForSendsToReachCore` hook (`AsyncMockProducer.cs:271`). Never
   `SendAccumulatorSettings.FromEnvironment`: the suite runs in parallel and those are
   process-wide variables.
5. **A seam substitutes one thing** (DoD #12). The submit delegate, the settings or the
   native read is replaced; the code under test is production's path around it. A
   test that re-implements the path it claims to test is rejected in review.
6. **Memory witnesses run serially.** Any `WeakReference` / `GC.GetTotalMemory`
   assertion joins `SerialMemoryMeasurementCollection`
   (`Interop/ProducerSubmitHandleRefTests.cs:56-64`).
7. **Mutation checks are recorded, not committed.** Each assertion that exists to pin
   one production line — the drain call (D3), the barrier enqueue and await (D4), the
   coalescing boundary, the barrier branch in `DrainAndFaultRemaining`, the five
   predicate reads (D6), the grow condition (D8), the span-the-op release (D12) — is
   proven by mutating that line, observing red, restoring, and recording
   `mutation / test / red-then-green` in the gate log. Fixture and production are
   mutated separately (ffi §A1).
8. **The unit suite is broker-free.** Real-flavour tests use
   `bootstrap.servers=localhost:9092` (unreachable, `PublicProducerSendClosedCheckTests.cs:53`)
   and a short `max.block.ms`. Anything that needs a broker is G10 (§4.5).
9. **`CS0618` is suppressed locally** around each obsolete-constructor use in tests
   (`#pragma warning disable CS0618` / `restore`); G2 fails otherwise.
10. **Tables.** The D6 and D9 tables may be xUnit `[Theory]` rows (the admin suite is
    the precedent); each row asserts its whole row. None of the mapped Java tests is a
    `@RepeatedTest` or `@ParameterizedTest` — checked for every MPT, CGMT and KPT test
    in §5.3 — so no loop bound needs carrying over (DoD §3).

### 5.2 Test inventory

| ID | CP | File | Decisions | Headline |
|---|---|---|---|---|
| S1 | CP1 | `Interop/ProducerTransactionNativeMethodsTests.cs` (new) | §4.4 | the 18 declarations are exactly right, and resolve |
| S2 | CP2 | `KafkaExceptionTests.cs` (extended) | D6 | the five predicates: the ten-row table plus a both-directions sweep over every code |
| S3 | CP2 + CP5 | `ConsumerGroupMetadataTests.cs` (new, CP2); `PublicProducerSendOffsetsToTransactionTests.cs` (new, CP5) | D2, D12, D13 | constructors; `SendOffsetsToTransaction` preconditions and snapshot |
| S4 | CP3 | `Interop/SendCompletionPumpBarrierTests.cs` (new) | D4 | the barrier in isolation |
| S5 | CP4 | `Interop/ProducerTransactionDrainTests.cs` (new) | D3, D4 | the drain and the barrier wiring |
| S6 | CP4 | `Interop/ProducerTransactionCancellationTests.cs` (new) | D5, D12 | the cancellation table and handle lifetime |
| S7 | CP4 | `Interop/MockProducerTransactionHelperTests.cs` (new) | D8 | helper plumbing and the grow rule |
| S8 | CP5 | `PublicProducerTransactionTests.cs` (async), `PublicSyncProducerTransactionTests.cs` (sync), `PublicProducerTransactionMockControlTests.cs` | D1, D7, D8 | the public surface and the MockProducerTest translations |
| S9 | CP5 | `PublicProducerTransactionErrorTests.cs` | D6 | classification end to end |
| S10 | CP5 | `PublicProducerIdempotenceTests.cs` | D9, D13 | idempotence configuration and the KafkaProducerTest subset |
| S11 | CP5 | `PublicProducerTransactionConcurrencyTests.cs` | D10 | the core's -2 on overlapping control calls |
| S12 | CP5 | `PublicProducerTransactionTeardownTests.cs` | D11 | dispose while a control operation is in flight |

**S1 — the declarations (CP1).**

- Reflection over `NativeMethods`: the 18 methods of §4.4 exist, each with
  `DllImport.EntryPoint` equal to its symbol, `CallingConvention.Cdecl`, and
  `[MarshalAs(UnmanagedType.I1)]` on every `bool` return and `bool` parameter (#10-#12
  returns, #10's `clear`, #14-#18 returns). A behavioural test cannot see a missing
  `I1` on a little-endian host, which is why the admin precedent sweeps structurally
  (`Interop/AdminNativeMethodsMarshallingTests.cs:33`).
- Parameter shape: #1-#5 and #10-#12 take `SafeProducerHandle` first; #6-#9 take
  `IntPtr` first and end with `ProducerCallbacks.OperationCallback` plus
  `IntPtr userData`; #13-#18 take `IntPtr`.
- Negative: no declaration has `EntryPoint == "kafka_producer_Producer_begin_transaction_async"`
  (B8, Q5).
- Resolution smoke for 14 symbols on a `NativeProducer.CreateMock` handle: the five
  sync operations in a valid order (init; begin; send-offsets with `count == 0` and a
  transient group metadata; commit; begin + abort), the three helpers,
  `ConsumerGroupMetadataNew` + destroy, and the five predicates on `IntPtr.Zero`
  (false) and on a `KafkaErrorNew(120, ...)` handle (only `IsTransactionAbortableError`
  true; the handle destroyed afterwards). A wrong entry point throws only when called,
  so the call is the proof. The four async symbols are resolved by S6's real-P/Invoke
  pass.
- Mutation: one misspelled `EntryPoint` turns S1 red.

**S2 — `KafkaException` predicates (CP2).**

- The public ctors (`KafkaException.cs:64`, `:74`, `:89`) and the internal 3-arg ctor
  (`:101`) leave all five properties false; so does every `SerializationException`
  ctor.
- The ten D6 rows through `KafkaErrorNew(code, msg)` -> `KafkaException.FromHandle`:
  each row asserts `Code`, `Message`, `IsRetriable` and all five properties, which also
  proves the construction site reads them before the handle is freed.
- One extra row records how the ABI resolves a code with no assigned error:
  `kafka_common_Error_new` maps an unassigned or out-of-range code to
  UNKNOWN_SERVER_ERROR, which reads back as `Code == -1` (measured). Every table row
  uses an assigned code, so this is recorded rather than rediscovered.
- Both-directions sweep (root `CLAUDE.md` §10.4: "a sampled test cannot catch a code
  wrongly added to, or missing from, the set"). For every code in -1..200 except 0,
  build the error with `KafkaErrorNew`, skip it when it reads back as -1 from a code
  other than -1 (unassigned — the core test's own rule, `errors.rs:2016-2020`), and
  assert each of the five properties equals membership in a set derived from the Java
  `extends` chain: TransactionAbortable {120}; ApplicationRecoverable {22, 25, 47, 49,
  82, 90}; Authorization {29, 30, 31, 53, 65}; OutOfOrderSequence {45, 59};
  InvalidConfiguration = the Authorization set plus the core test's other members
  (`errors.rs:1957-1981`). The expected sets are written in the test from the Java
  sources with that citation, not generated from the core, and a disagreement is a
  core finding routed to `kafka-critic` (as for the ten rows). The ten-row table stays
  as the readable contract; the sweep is what catches a swapped or mis-bound
  P/Invoke on a code the table does not sample.
- Mutation: removing each of the five reads in turn reddens exactly its column.

**S3 — `ConsumerGroupMetadata` and the `SendOffsetsToTransaction` preconditions.**

CP2 half (`ConsumerGroupMetadataTests.cs`):

- CGMT 36 / 52 / 62 / 72 translated (§5.3.2). The null-argument messages are asserted
  as described in §5.1 item 2.
- MPT 430 maps here: Java's `NullPointerException` comes from
  `new ConsumerGroupMetadata(null)` — the constructor — not from the producer.
- Both public constructors carry `[Obsolete]` with D2's exact text and
  `IsError == false`; the internal factory is not obsolete (reflection).
- Any `generationId` is accepted (0 and negative included); empty and non-ASCII
  strings are preserved verbatim.
- The consumer's existing `GroupMetadata()` tests stay green unchanged — the marshal
  now goes through the factory (`ConsumerGroupMetadataMarshal.cs:50`).

CP5 half (`PublicProducerSendOffsetsToTransactionTests.cs`, all four unless noted):

- `groupMetadata == null` -> `ArgumentNullException` (`ParamName` `groupMetadata`,
  "Consumer group metadata could not be null"), also when `offsets` is null too — it
  is Java's first check (D2 step 1). On the real flavours this is KPT 1944.
- `offsets == null` -> `ArgumentNullException` (`offsets`). A `default(TopicPartition)`
  key -> `ArgumentException` "Topic names must not be null."; a null value ->
  `ArgumentException` "Offset value must not be null." (both `ParamName` `offsets`).
  The snapshot's negative-partition branch cannot be reached through the public
  `TopicPartition` ctor (`TopicPartition.cs:46-57` rejects it first), so it is not
  asserted.
- Closed -> `ObjectDisposedException`, after the argument checks (a closed producer
  given null metadata still reports `ArgumentNullException`). Async with an
  already-cancelled token -> `OperationCanceledException`, thrown synchronously.
- KPT 1950 on the real flavours, broker-free: `transactional.id` set, **no**
  `InitTransactions`, then `SendOffsetsToTransaction(empty, new
  ConsumerGroupMetadata("group", 2, "", null))`. Java checks the group metadata before
  the transaction manager (`KafkaProducer.java:734-735`) and so does the core
  (`kafka_producer.rs:1515-1516`), so the result is the core's -3 with the
  `kafka_producer.rs:1666-1676` message, rendering measured.
- On both mocks the same metadata is **accepted** inside a transaction — Java's
  `MockProducer.sendOffsetsToTransaction` has no generation check
  (`MockProducer.java:184-196`), and the core mock mirrors it.
- Snapshot semantics: mutating the caller's dictionary after the call returns —
  including while the async call is still held in a D3 drain — does not change what
  `CommittedOffset` reports after commit (42 stays 42).
- Python 1395 analogue (`test_group_metadata_handle_lifecycle`): 5000
  `SendOffsetsToTransaction` calls inside one transaction on each mock, each building
  and destroying a transient native `ConsumerGroupMetadata_t` (D12). No crash, and
  `CommittedOffset` after commit is the last value sent.

**S4 — the barrier in isolation (CP3).** Built on the existing pump-test scaffolding
(`Interop/SendCompletionPumpGateTests.cs` and its siblings), with futures from a
manual mock so completion order is chosen by the test.

- On an idle pump, `EnqueueBarrier()` completes; `ProcessedBatchCount` and the
  drained-send count are unchanged.
- Behind N groups, the barrier completes only after every group's
  `TaskCompletionSource`s are set and their delivery callbacks have returned — asserted
  by a probe inside the barrier's continuation, not by timing.
- Coalescing boundary: group A (completed), barrier, group B (withheld). The barrier
  completes while group B is still pending, which is impossible if one `get_all` pass
  spanned A and B. Group B is completed afterwards for cleanup.
- Stop path: a queued barrier completes **successfully** in `DrainAndFaultRemaining`,
  and `DestroyFutures` is not called for it.
- Gate / stop: `EnqueueBarrier` after the gate has closed, or after stop, returns a
  successfully completed task where `Enqueue` would fault a group.
- A continuation on the barrier that blocks does not stall the pump — a later group
  still completes (`RunContinuationsAsynchronously`).
- Mutations: drop the barrier branch in `RunLoop`; complete the barrier before the
  preceding group's callbacks run; fault instead of succeed in
  `DrainAndFaultRemaining`. Each is red.

**S5 — the drain and the barrier wiring (CP4).** The settings seam gives a 60 s batch
window and a slot threshold above the test's record count, so records stay buffered
until something drains them. The witness that an operation drained is that,
immediately after it returns, the accumulator is empty and idle:
`WaitForSendsToReachCore(TimeSpan.Zero)` succeeds (with the drain mutated out it throws
`TimeoutException`).

- (i) Commit includes returned sends: init, begin, three `Send`s (buffered),
  `CommitTransaction` -> `HistoryCount() == 3` as soon as it returns.
- (ii) Abort discards them: the same with `AbortTransaction`, then `Flush` ->
  `HistoryCount() == 0`, and the three send `Task`s complete (the mock's abort
  flushes, D7).
- (iii) A send issued before `BeginTransaction` is not swept into the transaction:
  after init, `Send(r0)` (buffered), `BeginTransaction` (drains), `AbortTransaction` ->
  `HistoryCount() == 1`. Without the drain, r0 would reach the core after begin, be
  staged, and be discarded (0).
- Each of the five operations passes the empty-and-idle witness above, on both
  surfaces where an accumulator can exist (the async types).
- The sync types never create an accumulator: the internal accessor stays null across a
  full sync transaction.
- Async `BeginTransaction`'s bounded drain expires deterministically: reuse the hold of
  `Interop/SendAccumulatorTests.cs:638-697` (close the core, park the batch thread in a
  blocking delivery callback), then `BeginTransactionWithAccumulatorDrainBound(TimeSpan.Zero)`
  -> `KafkaException` with `Code == 0` and `Message ==` "The producer's send
  accumulator did not drain within 0 seconds, so records buffered in the binding have
  not reached the core and beginTransaction() was not attempted." Because the core is
  closed, a native call would have produced the core's closed-producer error instead,
  so the message itself proves no native call was made. The flush expiry test stays
  byte-identical and green.
- D4 wiring. An auto-complete mock, and N sends whose delivery callback blocks on a
  test-held event, so the pump is parked on their group. `CommitTransaction()` is
  called; the native commit is observed done (`HistoryCount() == N`); the commit
  `Task` is held for a bounded window and must not complete; the event is released;
  the commit `Task` completes, and a probe in its continuation sees all N callbacks
  returned. The probe is the pass condition, and it cannot flake; the window only
  gives the "no barrier" mutation room to show. With `SetCommitTransactionError`
  installed and the pump still parked, `CommitTransaction()` faults within the bound —
  no barrier wait on failure. The same pair runs for `AbortTransaction`.
- Mutations: drop the drain in each of the five operations; drop the barrier await;
  await the barrier on the failure path too.

**S6 — cancellation and lifetime (CP4).** Uses the `NativeSubmit` seam: a counting fake
that either forwards to the real P/Invoke or captures the callback without firing it.

- D5 row 1: an already-cancelled token -> `OperationCanceledException` thrown
  synchronously by each of the four `Task` operations (not a cancelled `Task`); submit
  count 0.
- D5 row 2: cancel while the D3 drain is held -> the `Task` is `Canceled`; submit
  count 0; the drain continues (the buffered sends still reach the core:
  `WaitForSendsToReachCore` succeeds afterwards).
- D5 row 3: the fake captures the callback; cancel -> the awaiter is `Canceled`. Then
  fire the late callback — once with `IntPtr.Zero`, once (second run) with a live
  `KafkaErrorNew(120, ...)` handle -> no throw, and the `Task` stays `Canceled`.
  Differential reference witness: `Dispose()` before the late callback leaves
  `Handle.IsClosed == false` (the span-the-op reference is outstanding, so the native
  destroy is deferred); after the late callback `Handle.IsClosed == true`. The
  completion context becomes collectable (`WeakReference`, serial collection). The late
  error handle is freed by `OperationCompletionSource.Complete` ->
  `KafkaException.FromHandle` **before** the cancellation check
  (`OperationCompletionSource.cs:183-194`). That free is reviewed, not measured: Python
  1587 counts frees by swapping `_lib.KafkaError_destroy`, and a `DllImport` cannot be
  swapped.
- D5 row 4: pump parked; the fake captures the callback; fire it with `IntPtr.Zero`
  (native success), **then** cancel -> the `Task` is `RanToCompletion` although the
  barrier never completed. Firing first makes the order deterministic: `Complete`
  disposes the token registration before setting the result, so the later cancel can
  only reach the barrier wait.
- Python 1557 analogue (`test_async_txn_ops_route_through_async_ffi`): each of the four
  async operations submits exactly once through the seam (count 1), and once through
  the real P/Invoke (the operation completes against the mock) — the resolution proof
  S1 defers here.
- D12 marshalling: inside the fake submit for `SendOffsetsToTransaction`, the five
  arrays decode to the snapshot's values, and the group-metadata pointer reads back
  through the already-bound accessors (`ConsumerGroupMetadataGroupId`,
  `ConsumerGroupMetadataGenerationId`, and the member / instance-id twins,
  `NativeMethods.cs:332-341` ff.), proving the transient handle carries the managed
  values. That the handle and the pins end with the submit P/Invoke is a review item
  (same-frame `finally`), not a measurement.

**S7 — the mock helpers' plumbing (CP4).**

- `SetCommitTransactionError`: code 0, 32768 and -32769 -> `ArgumentOutOfRangeException`
  (`ParamName` `code`) with the two D8 messages; `short.MinValue` and `short.MaxValue`
  are accepted. The hook is sticky: commits in two successive transactions both fail
  until `ClearCommitTransactionError()`, after which commit succeeds. A null message
  yields the code's default message (measured and pinned). The installed error is the
  core's typed error (120 -> `IsTransactionAbortableError`).
- `SentOffsets()`: false after begin, true after a non-empty send-offsets, still true
  after commit, false after the next begin.
- `CommittedOffset`: null `groupId` -> `ArgumentNullException` (`groupId`);
  `default(TopicPartition)` -> `ArgumentException` "Topic names must not be null."
  (`partition`); not found -> null; an absent leader epoch -> `LeaderEpoch == null`; a
  present epoch round-trips.
- The grow rule, counted with the delegate fake and repeated end to end: 4096-byte
  metadata resolves in one attempt; 5000 bytes is exact after one retry; exactly 4097
  bytes (inside the false-positive window) is exact after one harmless retry; a 3-byte
  and a 4-byte UTF-8 character straddling the first cap are exact; more than 1,048,576
  bytes -> `InvalidOperationException` with D8's message after at most five attempts.
- Mutation: the naive condition `len >= cap - 1` reddens the 4-byte-straddle case.

**S8 — the public surface and the MockProducerTest translations (CP5).**

- D1 shape pin by reflection: each interface declares exactly the five members with
  D1's signatures (names, parameter names, defaults, return types);
  `IAsyncProducer.BeginTransaction` returns `void`; all four types implement them.
- The MPT rows of §5.3.1 on both mocks unless a row says otherwise.
- D7 on the async mock: manual-mode sends pending across a commit or abort resolve with
  metadata (MPT 330, 364). A **failed** commit leaves them pending — witnessed at the
  core, deterministically: after the failed commit `CompleteNext()` returns `true` (the
  core still held the completion), and only then does the send's `Task` complete.
- The public helpers forward correctly: one smoke per helper per mock (the edge cases
  are S7's).
- KPT 2163 on both real flavours: a closed producer -> `ObjectDisposedException` from
  `InitTransactions` (the ffi §A5 closed-producer idiom; Java throws ISE).

**S9 — classification end to end (CP5).**

- The D6 truth table through `ErrorNext(code, msg)` on both mocks. Async: a pending
  manual send is faulted and its `Task`'s `KafkaException` asserted. Sync: a
  worker-thread `Send` driven by the `DriveUntilResolved` pattern
  (`PublicSyncProducerMockControlTests.cs:151`).
- The commit-hook path (Python 995 / 1011), both mocks, both surfaces:
  `SetCommitTransactionError(120, "commit failed abortably")` -> `CommitTransaction`
  throws / faults with `Code == 120`, that message, `IsTransactionAbortableError`;
  `AbortTransaction` then succeeds and a fresh transaction commits. With
  `(7, "commit timed out")` -> `IsTransactionAbortableError == false`,
  `IsRetriable == true`.
- Recovery composition: D6's literal and KIP-1050 compositions, evaluated over the ten
  rows, select exactly the "close" sets D6 states — so the remarks' example cannot drift
  from the predicates (R15).

**S10 — idempotence configuration and the KafkaProducerTest subset (CP5).** Both real
flavours.

- Every row of the D9 table, `Code` and exact `Message`.
- KPT 238 / 341 / 413, construction halves: every `validProps*` combination
  constructs, and every `invalidProps*` combination fails in the constructor with the
  message the core's validation produces **for that combination**. Java's third
  `assertThrows` argument is a failure message, not the expected text — so KPT 238's
  `invalidProps3` (`acks=0` + `transactional.id`) expects "Cannot set a transactional.id
  without also enabling idempotence.", and KPT 413's `invalidProps3` / `invalidProps4`
  expect the max-in-flight message.
- KPT 238 / 341 / 413, readback halves: Java reads the effective config back. The
  binding has no config introspection, but the effective `enable.idempotence` **is**
  observable: with idempotence off there is no transaction manager
  (`kafka_producer.rs:1128-1135`), so `InitTransactions()` reports "Cannot use
  transactional methods without enabling transactions by setting the transactional.id
  configuration property" (`:1645-1653`), whereas an idempotent non-transactional
  producer reports "Transactional method invoked on a non-transactional producer."
  (`transaction_manager.rs:2530-2536`). Each valid combination's idempotence
  expectation is asserted through that probe (`acks=0` alone -> the first message;
  defaults -> the second). The `acks` / `retries` readbacks have no observable: N/A.
- KPT 222, partial: with only `transactional.id` set the producer constructs
  (idempotence defaulted on), and the effective `client.id` is
  `producer-<transactional.id>`, observable as the `client-id` tag on `Metrics()`
  (`kafka_producer.rs:1279-1282`, default at `producer_config.rs:797`). The `acks` /
  `retries` readbacks: N/A.
- KPT 1329: the D9 timeout row, then the second `InitTransactions()` is accepted (it
  times out again with the same code).
- KPT 2054: after the timeout, `BeginTransaction()` -> the core's -4 "Cannot attempt
  operation `beginTransaction` because the previous call to `initTransactions` timed out
  and must be retried" (measured; the core's own translation asserts that text,
  `kafka_producer.rs:5463-5494`), then `Dispose()` returns within D11's bound. Java's
  `close(Duration.ofMillis(0))` has no .NET form — `IProducer.cs:226-232` records that
  the ABI has no timed close — so the bounded `Dispose` stands in.
- D13 on the real flavours: an empty map on an **idempotent** non-transactional
  producer -> success (Java returns before the transaction-state check); with
  idempotence **off** -> the -4 "Cannot use transactional methods ..." error, because
  Java's and the core's transaction-manager check precedes the empty check
  (`KafkaProducer.java:735-738`, `kafka_producer.rs:1516-1521`).

**S11 — overlapping control calls (CP5).** D10's two tests as written there: the
deterministic async one on `AsyncKafkaProducer` and the sync retry loop on
`KafkaProducer`, asserting `Code == -2` and the exact message, `t1` ending `Canceled`,
and a bounded `Dispose`. The mocks are excluded: their control calls finish too fast to
open a window, so a mock test could only pass by luck.

**S12 — dispose while a control operation is in flight (CP5).** D11 on both real
flavours with `max.block.ms=3000`: start `InitTransactions` (a worker thread for sync,
the `Task` for async), `Dispose()` from the test thread, and assert that `Dispose`
returns and the operation completes **exactly once** within 3 s + 30 s + slack — the
`Task` ends in exactly one terminal state and is never stranded. The outcome (code and
message) is measured at CP5 and pinned. Afterwards `Handle.IsClosed == true` (the
references balanced).

### 5.3 Java test mapping

Status legend: **translated**; **proxied (B2)** — the Java getter has no ABI symbol,
so the test asserts the getter's observable consequences; **projected (B5 / B6)** —
asserted through `HistoryCount()` / `CommittedOffset`, which carry the count / the
single-entry value but not full-list equality; **N/A (Bn)** — needs Mode-B row Bn;
**Q24** — translated with a documented divergence. Core messages below are the core
mock's (`mock_producer.rs:257-288`, `:993-994`, `:1029`), with the code expected to be
-4 `LOCAL_ILLEGAL_STATE` and measured.

#### 5.3.1 `MockProducerTest` (transaction tests, `:134-680`)

| Java | Test | .NET assertion | Status |
|---|---|---|---|
| 134 | `shouldInitTransactions` | after init, `BeginTransaction` succeeds and a second `InitTransactions` fails | proxied (B2) |
| 141 | `shouldThrowOnInitTransactionIfProducerAlreadyInitializedForTransactions` | "MockProducer has already been initialized for transactions." | translated |
| 148 | `shouldThrowOnBeginTransactionIfTransactionsNotInitialized` | "MockProducer hasn't been initialized for transactions." | translated |
| 154 | `shouldBeginTransactions` | after begin, a second begin fails and commit succeeds | proxied (B2) |
| 162 | `shouldThrowOnBeginTransactionsIfTransactionInflight` | "Transaction already started" | translated |
| 170 | `shouldThrowOnSendOffsetsToTransactionIfTransactionsNotInitialized` | not-initialized message, with an **empty** map where Java passes `null` (the .NET null-offsets precondition would fire first) | Q24 |
| 176 | `shouldThrowOnSendOffsetsToTransactionTransactionIfNoTransactionGotStarted` | "There is no open transaction.", empty map | Q24 |
| 183 | `shouldThrowOnCommitIfTransactionsNotInitialized` | not-initialized message | translated |
| 189 | `shouldThrowOnCommitTransactionIfNoTransactionGotStarted` | "There is no open transaction." | translated |
| 196 | `shouldCommitEmptyTransaction` | after commit, a second commit fails ("There is no open transaction.") and begin succeeds; committed-versus-aborted is not observable for an empty transaction | proxied (B2) |
| 207 | `shouldCountCommittedTransaction` | `commitCount()` has no ABI symbol | N/A (B3) |
| 218 | `shouldNotCountAbortedTransaction` | same | N/A (B3) |
| 231 | `shouldThrowOnAbortIfTransactionsNotInitialized` | not-initialized message | translated |
| 237 | `shouldThrowOnAbortTransactionIfNoTransactionGotStarted` | "There is no open transaction." | translated |
| 244 | `shouldAbortEmptyTransaction` | the mirror of 196 | proxied (B2) |
| 255, 261, 269, 278, 286, 294, 302 | `shouldThrowFenceProducerIfTransactionsNotInitialized` and the six `...IfProducerGotFenced` tests | `fenceProducer()` has no ABI symbol | N/A (B1) |
| 310 | `shouldPublishMessagesOnlyAfterCommitIfTransactionsAreEnabled` | `HistoryCount()` 0 before commit, 2 after | projected (B5) |
| 330 | `shouldFlushOnCommitForNonAutoCompleteIfTransactionsAreEnabled` | async manual mock: both send `Task`s pending before commit and completed the moment `await CommitTransaction()` returns (D4) | translated (async). Sync N/A: a manual sync `Send` blocks its caller and exposes no non-destructive "pending" signal, so "not done before commit" cannot be established without a race (R-c) |
| 348 | `shouldDropMessagesOnAbortIfTransactionsAreEnabled` | `HistoryCount()` 0 after the abort and after a later empty commit | projected (B5) |
| 364 | `shouldThrowOnAbortForNonAutoCompleteIfTransactionsAreEnabled` | as 330, with abort | translated (async); sync N/A as 330 |
| 377 | `shouldPreserveCommittedMessagesOnAbortIfTransactionsAreEnabled` | `HistoryCount()` stays 2 across a later abort | projected (B5) |
| 397 | `shouldPublishConsumerGroupOffsetsOnlyAfterCommitIfTransactionsAreEnabled` | `CommittedOffset` null for every pair before commit; after it g1/p0 42, g1/p1 73, g2/p0 101, g2/p1 21 | projected (B6) |
| 430 | `shouldThrowOnNullConsumerGroupMetadataWhenSendOffsetsToTransaction` | the NPE is the constructor's | S3 |
| 438 | `shouldIgnoreEmptyOffsetsWhenSendOffsetsToTransactionByGroupMetadata` | `SentOffsets()` false after an empty send | translated |
| 447 | `shouldAddOffsetsWhenSendOffsetsToTransactionByGroupMetadata` | `SentOffsets()` false, then true | translated |
| 464 | `shouldResetSentOffsetsFlagOnlyWhenBeginningNewTransaction` | the full sequence | translated |
| 492 | `shouldPublishLatestAndCumulativeConsumerGroupOffsetsOnlyAfterCommitIfTransactionsAreEnabled` | null before commit; after it g/p0 42, g/p1 101 (the later send wins), g/p2 21 | projected (B6) |
| 529 | `shouldDropConsumerGroupOffsetsOnAbortIfTransactionsAreEnabled` | null after each abort and the empty commit that follows it | projected (B6) |
| 558 | `shouldPreserveOffsetsFromCommitByGroupIdOnAbortIfTransactionsAreEnabled` | g/p0 42 and g/p1 73 survive a later empty abort | projected (B6) |
| 583 | `shouldPreserveOffsetsFromCommitByGroupMetadataOnAbortIfTransactionsAreEnabled` | g survives; g2/p2 and g2/p3 null after the abort (the second group is kept, unlike Python 1202) | projected (B6) |
| 617 | `shouldThrowOnInitTransactionIfProducerIsClosed` | `ObjectDisposedException` (Java ISE; the ffi §A5 closed-producer idiom) | translated |
| 624 | `shouldThrowOnSendIfProducerIsClosed` | not a transaction API; already covered (`PublicProducerSendClosedCheckTests.cs`) | existing |
| 631 | `shouldThrowOnBeginTransactionIfProducerIsClosed` | `ObjectDisposedException` | translated |
| 638, 645 | `shouldThrowSendOffsetsToTransactionBy{GroupId,GroupMetadata}IfProducerIsClosed` | `ObjectDisposedException`, with an empty map where Java passes `null` | Q24 |
| 652 | `shouldThrowOnCommitTransactionIfProducerIsClosed` | `ObjectDisposedException` | translated |
| 659 | `shouldThrowOnAbortTransactionIfProducerIsClosed` | `ObjectDisposedException` | translated |
| 666 | `shouldThrowOnFenceProducerIfProducerIsClosed` | no `fenceProducer` | N/A (B1) |
| 673 | `shouldThrowOnFlushProducerIfProducerIsClosed` | not a transaction API; already covered | existing |
| 680 | `shouldNotThrowOnFlushProducerIfProducerIsFenced` | needs `fenceProducer` | N/A (B1) |

The non-transactional MPT tests (`:73-107`, `:689-724`) are outside this phase. Tally
of the 46 rows above, counting each Java method once: translated 16 (the two async-only
rows 330 / 364 included), proxied 4, projected 8, Q24 4, S3 1, existing 2, N/A 11 (B1 9,
B3 2). The core mock does implement `fence_producer` (`mock_producer.rs:760-778`) and
`commit_count()` (`:837`); what is missing is only their ABI export, which is why B1 / B3 are
Mode-B gates rather than core work (Q4).

#### 5.3.2 `ConsumerGroupMetadataTest`

| Java | Test | .NET | Status |
|---|---|---|---|
| 36 | `testAssignmentConstructor` | the 4-arg ctor round-trips all four values, including an instance id | translated |
| 52 | `testGroupIdConstructor` | generation -1, member `""`, instance id null | translated |
| 62 | `testInvalidGroupId` | `ArgumentNullException`, `ParamName` `groupId`, "group.id can't be null" | translated |
| 72 | `testInvalidMemberId` | `ArgumentNullException`, `ParamName` `memberId`, "member.id can't be null" | translated |
| 81 | `testInvalidInstanceId` | passes a null `Optional<String>` reference; `string?` has no separate absent-versus-null-reference state (D2) | N/A |

#### 5.3.3 `KafkaProducerTest`

| Java | Test | .NET | Test |
|---|---|---|---|
| 222 | `testOverwriteAcksAndRetriesForIdempotentProducers` | idempotence defaulted on and `client.id` via `Metrics()` tags; `acks` / `retries` readbacks N/A | S10 (partial) |
| 238 | `testAcksAndIdempotenceForIdempotentProducers` | construction halves; idempotence readback via the probe | S10 |
| 341 | `testRetriesAndIdempotenceForIdempotentProducers` | same | S10 |
| 413 | `testInflightRequestsAndIdempotenceForIdempotentProducers` | same, including the exact max-in-flight message (`:436-437`) | S10 |
| 1329 | `testInitTransactionTimeout` | timeout, then the retry is accepted | S10 |
| 1944 | `testNullGroupMetadataInSendOffsets` | managed `ArgumentNullException`, broker-free | S3 |
| 1950 | `testInvalidGenerationIdAndMemberIdCombinedInSendOffsets` | the core's -3, broker-free (no init needed — the metadata check comes first) | S3 |
| 2054 | `testOnlyCanExecuteCloseAfterInitTransactionsTimeout` | begin after the timeout -> the core's -4; bounded `Dispose` | S10 |
| 2163 | `testTransactionalMethodThrowsWhenSenderClosed` | `ObjectDisposedException` | S8 |

Core-owned, N/A at the binding: `:787-916` (the five `@ParameterizedTest` metadata
tests — not transaction APIs), and `:1290`, `:1364`, `:1419`, `:1444`, `:1503`, `:1533`,
`:1563`, `:1600`, `:1637`, `:1677`, `:1715`, `:1772`, `:1842`, `:1895`, `:2210`, `:2423`.
Each scripts broker responses through Java's `MockClient`, which the binding has no
seam for, and each pins core logic that the core translates itself (e.g.
`kafka_producer.rs:5311` `test_init_transactions_response_after_timeout`, `:5979`
`test_commit_transaction_with_record_too_large_error`, `:6176` / `:6215` / `:6436` the
send-txn-offsets tests). End to end they are exercised by the six G10 arms.

### 5.4 Python parity (`bindings/python/test/unit/test_producer.py`)

| Python | Covered by |
|---|---|
| 922 `test_txn_init_begin_send_commit`, 952 `test_txn_committed_records_in_history`, 965 `test_txn_aborted_records_discarded`, 982 `test_txn_abort_then_reuse` | S8 (MPT 310, 348, 377) |
| 934 `test_txn_commit_empty`, 944 `test_txn_abort_empty` | S8 (MPT 196, 244) |
| 995 `test_txn_commit_failure_requires_abort`, 1011 `test_txn_commit_failure_non_abortable` | S9 (same codes 120 / 7 and messages) |
| 1031 `test_txn_send_offsets_to_transaction` | S8 (MPT 397) |
| 1056 `test_txn_send_offsets_non_str_metadata_raises_type_error` | N/A — `string?` metadata is statically typed |
| 1078 `test_txn_send_offsets_empty_stages_nothing` | S8 (MPT 438) |
| 1098 `test_txn_reset_sent_offsets_flag_only_when_beginning_new_transaction` | S8 (MPT 464) |
| 1125 `test_txn_publish_latest_and_cumulative_offsets_only_after_commit` | S8 (MPT 492) |
| 1162 `test_txn_drop_consumer_group_offsets_on_abort` | S8 (MPT 529) |
| 1202 `test_txn_preserve_committed_offsets_on_later_abort` | S8 (MPT 583) — .NET keeps the second group, which Python drops for lack of a metadata ctor |
| 1246-1325 (nine illegal-state tests) | S8 (MPT 141-237 rows) |
| 1348-1380 (five after-close tests, `RuntimeError`) | S8 (MPT 617-659 rows, `ObjectDisposedException`) |
| 1395 `test_group_metadata_handle_lifecycle` | S3 (the 5000-iteration loop) |
| 1417-1480 (six async tests) | S8 / S9 on the async mock |
| 1526 `test_txn_sync_ops_route_through_async_ffi_and_release_gil` | N/A — no GIL; the .NET sync surface deliberately calls the sync ABI (D1). S1 pins the sync declarations' shapes and resolves them; the routing itself is a review item (§6 DoD #11) |
| 1557 `test_async_txn_ops_route_through_async_ffi` | S6 |
| 1587 `test_async_txn_cancelled_await_frees_late_error_handle` | S6 row 3 (the free is reviewed, not measured) |

### 5.5 The Rust harness is unchanged

- `DotnetGrpcFactory` (`tests/common/backend_factory.rs:496-527`) already talks to the
  servicers through the proto. The harness's transaction methods exist
  (`tests/common/multilanguage_producer.rs:132`, `:146`, `:169`, `:212`, `:224`) and map
  a gRPC status to a core `Error` (`:530`).
- The three scenarios (`tests/integration/producer_transactions_test.rs:685-701`)
  already expand to the six .NET arms through `multilanguage_test!`
  (`tests/common/multilanguage_test_macro.rs:56`). They fail today only because the
  servicers answer `Unimplemented`.
- So G1's `tests/` clause stays empty. The stale comment at
  `producer_transactions_test.rs:674-678` is reported, not edited (Q10). Running G10
  locally needs Docker and the user's go-ahead (Q26).

---

## §6 Definition-of-Done mapping

The DoD is `.claude/rules/definition-of-done.md`. For a binding phase it is read
through `bindings/dotnet/CLAUDE.md` §7.5 (TFM matrix, mock-driven unit tests,
lint/format, the ffi anti-patterns). Each row states how M17/P1 meets the item and
where the Critic finds the evidence.

| # | DoD item | How M17/P1 meets it | Evidence |
|---|---|---|---|
| 1 | Consistent with every rule | Java shape, Rust logic: the binding adds no Kafka behaviour (`bindings/CLAUDE.md` §1.2, §2.6). Each decision cites the rule it applies: sync versus async per `bindings/dotnet/CLAUDE.md` §4 (**Stays sync**, `:575`); ffi §A2's sync-`SafeHandle` / async span-the-op split (D12); ffi §A5's two surfaces with verbatim codes (D6, D10); ffi §A6 / §A7 for the pump and callbacks (D4); `producer-transactions.md` §13's drain contract mirrored for the binding's own buffer (D3); root `CLAUDE.md` §10.4's predicate naming, polarity and both-directions test (D6, S2). Rule changes the phase needs are RULE-DRAFTs the user applies (RD1-RD13, Q12); the phase edits no rule file. | Critic 84 at each CP; G2 (missing xmldoc, dangling `cref` and unsuppressed CS0618 are build errors) |
| 2 | Every method implemented | T1-T12 on every flavour they exist on (§2.1): five members on each interface and its two implementers, the two `ConsumerGroupMetadata` constructors, five predicates, four mock-helper members on each mock, five RPCs in each servicer. Every Java member without an ABI symbol is listed (B1-B7), named as absent in the mock remarks (D8) and gated to the user (Q4); `begin_transaction_async` is unbound by decision (B8). Nothing is dropped silently. | S1 reflection sweep; G6 = 715; G9 override count 13 |
| 3 | Every test translated | §5.3 maps every `MockProducerTest` transaction test, every `ConsumerGroupMetadataTest` test and the in-scope `KafkaProducerTest` tests, with a status and a reason for each N/A. Every negative test asserts `Code` and the exact `Message` (§5.1 principle 2). No mapped Java test is `@RepeatedTest` / `@ParameterizedTest` (the five parameterized `KafkaProducerTest` metadata tests are out of scope, §5.3.3). Per-message-type files and byte-level wire vectors: N/A — the phase adds no wire type; the encoding is the core's, exercised end to end by G10. | §5.3, §5.4 |
| 4 | Blockers implemented | B1-B7 block only full `MockProducerTest` coverage, not the requirement. Each needs an ABI shim over an existing core function (Mode B, `bindings/dotnet/CLAUDE.md` §6.3), which the .NET personas may not author (§8.1), so each is a user decision (Q4), neither implemented nor invented. Every bound symbol is exported today (§2.5). | §2.2, Q4 |
| 5 | Unit and integration tests pass | G4 / G5 at every CP; G8 at CP4, CP5 and CP7; G10 at CP7 (the six `__grpc_dotnet*` arms of the three transaction scenarios plus the existing roster); CI `verify-dotnet` is authoritative (§4.6). | `gate/CP<n>.txt` |
| 6 | No duplicated code | No second copy of any mechanism: the bounded blocking drain is **extracted** from `FlushWithAccumulatorDrainBound` and shared with `Flush` (D3); the barrier is a form of `PendingSendBatch` (D4); async control operations reuse `SubmitVoidOperation`, `OperationCompletionSource` and `ProducerCallbacks.Operation` (D12); offsets reuse `WithPinnedCommitOffsets` (§4.4 row 3); the servicers reuse the `Flush` template and `Translate` (D14). | Critic 84 |
| 7 | No types absent from Java | No new public type. Internal additions, each justified where introduced: `EnqueueBarrier` and the barrier form of `PendingSendBatch` (D4 — the only way to give Java's callback guarantee over a batched pump); the metadata grow helper (D8 — the ABI truncates without reporting); the settings and submit seams (D3, Q16 — determinism); the internal constructor and factory overloads (D2, D3, D6), which carry no behaviour. | Critic 84 |
| 8 | No TODO / FIXME | G7's marker grep over the code paths prints nothing at every commit. | gate records |
| 9 | `make verify` | Root `make verify` (`Makefile:434`) is Rust + C + Python + `check-bindings`; it does not reach .NET, and G1 proves the phase changes nothing it builds (G11 re-runs its format and lint legs as a no-op proof after the CP7 `Makefile` edit). The .NET leg is `make verify-dotnet` (`Makefile:449-451`: unit, integration, perf), which runs as written only on CI's Linux amd64 job, after the user pushes (Q25); locally its pieces are G2-G5, G8 and G10. | CI; G1, G11 |
| 10 | Hot-path allocation audit | Control operations are per transaction, not per record, but D3 / D4 sit beside the send path, so it is audited. (a) `Send` is unchanged: no new branch or field on the send path. (b) The pump's per-group loop gains one `IsBarrier` read — a field load, no allocation. (c) One barrier object plus its `TaskCompletionSource<bool>` per async commit or abort — per transaction. (d) The five predicate reads are non-allocating `bool` P/Invokes that run only when a core error is materialized as a `KafkaException` — the failure path. The existing budgets stay green **unchanged**: `PublicProducerSendAllocationBudgetTests.cs:58` and `PublicSyncProducerSendAllocationBudgetTests.cs:58` (512 B per send), `PublicProducerDeliveryCallbackAllocationBudgetTests.cs:85` (512 B, plain path). No budget is widened. | G4 |
| 11 | Consumer trait surface | N/A (no consumer file changes). Its spirit, applied: the sync members call the sync ABI — no managed `block_on` / `Task.Run` façade — and the async members return the `Task` of `SubmitVoidOperation`. S1 pins the declarations' shapes (sync `SafeProducerHandle` forms, async callback forms) and that `begin_transaction_async` is not bound; S6 pins that each async member submits exactly once through its async symbol; that each sync member calls its sync symbol directly is a review item (there is no sync seam to count through). | S1, S6, Critic 84 |
| 12 | Test-fixture fidelity | Each seam substitutes exactly one thing and is reached through production's own entry points: the settings seam feeds production's `EnsureAccumulator`; the submit seam defaults to the real P/Invoke, which the public path passes; the grow helper is production's helper with the native call as a delegate; the barrier tests drive the production pump. Each seam test has a mutation check recorded in the gate log (§5.1 principles 5 and 7). | S4-S7 mutation records |

---

## §7 Documentation

### 7.1 Xmldoc (CP5; content, not wording)

Every comparative or residual claim is stated **once**, at a canonical home, and every
other site points there without restating it (ffi §A6 round-5 amendment: delete a
comparison outside its home rather than re-scope it). G2 makes a missing xmldoc or a
dangling `cref` a build error; CP5's doc-ID check proves the new members' docs are
emitted.

| # | Point | Canonical home | Pointed to from |
|---|---|---|---|
| X1 | The lifecycle — `InitTransactions` once per producer (and retry-safe after a timeout, `KafkaProducer.java:635`), then `BeginTransaction`, sends and `SendOffsetsToTransaction`, then `CommitTransaction` or `AbortTransaction` — and that it requires `transactional.id`. | `IProducer` / `IAsyncProducer` remarks | each member's `<summary>` |
| X2 | The recovery mapping, **literal form first**: `ex.Code == 90 \|\| ex.IsOutOfOrderSequenceError \|\| ex.IsAuthorizationError` -> close the producer; any other `KafkaException` -> abort; `IsTransactionAbortableError` -> abort and retry. Then one sentence: the KIP-1050 variant (`IsApplicationRecoverableError` in place of `Code == 90`) is a **superset** — five more codes route to close (D6, R15). Code -2 is never a reason to abort (h:16161-16163). | interface remarks | the five predicates; `CommitTransaction` |
| X3 | The drain contract, mirroring `producer-transactions.md` §13: every `Send` that had **returned** before the control call is included — committed by a commit, discarded by an abort, and never swept into a transaction by a later `BeginTransaction`. Sends racing on other threads are not ordered against the call (R-c). It must **not** say sends inside a transaction are unsupported. On the sync interface: one sentence that a synchronous `Send` returns only after the core has the record, so nothing is buffered. | `IAsyncProducer` remarks | each async control member |
| X4 | The barrier guarantee (Java `KafkaProducer.java:754-755`): when `CommitTransaction` / `AbortTransaction` completes successfully, every send `Task` and delivery callback of the transaction's records has completed; nothing is promised when it fails (D4). | `IAsyncProducer.CommitTransaction` / `AbortTransaction` remarks | — |
| X5 | The residuals, enumerated once: R-a (`Clear()` on a manual mock strands a later barrier), R-b (a delivery callback that blocks on commit / abort deadlocks), R-c (the sync surface has no barrier; concurrent sends are unordered). | `IAsyncProducer` remarks | commit / abort; `IDeliveryCallback` remarks (R-b, pointer only — its own at-most-once enumeration is a different axis and is not touched); `AsyncMockProducer.Clear()` (R-a) |
| X6 | Cancellation "abandons the wait, not the operation": D5's four cases in prose, including success when the token fires during the barrier wait, and the caveat that a retry while the abandoned operation still runs fails with -2. | `IAsyncProducer` remarks | each `cancellationToken` parameter |
| X7 | Async `BeginTransaction` stays synchronous (Java's does not block), first drains the send accumulator with a bounded 30 s wait, and on expiry throws `KafkaException` (Code 0) with D3's message, the transaction not begun. | `IAsyncProducer.BeginTransaction` | — |
| X8 | The Q24 divergences of D2 (the managed closed check precedes the core's checks; `ArgumentNullException` where Java's mock throws NPE). | interface remarks | the `SendOffsetsToTransaction` exception lists |
| X9 | Each predicate cites the Java class it translates, lists the codes it covers (root `CLAUDE.md` §10.4), and states its polarity (not complements: 29 / 53 are both Authorization and InvalidConfiguration; 45 / 59 both OutOfOrderSequence; 48 none). The class remarks state that fatality is not exposed (no `IsFatal`) and that `ProducerFencedException` is `Code == 90` (a leaf). | each property; `KafkaException` remarks | X2 |
| X10 | `ConsumerGroupMetadata` constructors: `[Obsolete]` with D2's message, the defaults of the one-argument form, and the null rules. | the constructors | — |
| X11 | The mock helpers: setup-only (must not overlap a control call — the ABI's rule, h:16562-16569), the hook sticky until cleared, `SentOffsets` reset only by `BeginTransaction`, `CommittedOffset`'s newest-wins / epoch -1 -> `null` / embedded-NUL / over-1-MiB rules; and which Java helpers are **absent** (B1-B7, "not exported at the C ABI"). | `MockProducer` / `AsyncMockProducer` remarks and members | — |
| X12 | `AsyncMockProducer.Clear()` on a manual mock with sends pending strands those sends (Java-faithful), **and** every later send and barrier behind them, **and** makes `Dispose` hang (the binding's amplification, file-forward item 10). Stated as a precondition: complete or fail pending sends before `Clear()`. | `AsyncMockProducer.Clear()` | X5 (R-a) |
| X13 | Idempotence is configured through the config dictionary (Java has no API): `enable.idempotence` defaults on; `transactional.id` requires it. | `KafkaProducer` / `AsyncKafkaProducer` class remarks | — |
| X14 | The servicers' class-summary RPC counts. | `ProducerServiceImpl` / `AsyncProducerServiceImpl` | — |

**Deleted, not re-worded:** "Transactions remain deferred." (`IAsyncProducer.cs:50`,
`AsyncKafkaProducer.cs:37`), and any mock-remarks sentence claiming transactions are
unavailable. The CP5 grep gate proves the deletion.

### 7.2 RULE-DRAFTs (CP7)

The Manager writes two drafts into
`bindings/dotnet/design/history/M17/P1-producer-transactions/`, in the M15/P3
precedent's format (`design/history/M15/P3-cluster-configs-logdirs/RULE-DRAFT-D20-ffi-marshalling.md`:
a "**Status: NOT APPLIED. Handed to the maintainer <date>.**" header, the reason, the
exact insertion point, and verbatim proposed text):

- `RULE-DRAFT-RD1-RD6-RD9-RD10-RD12-RD13-claude-md.md` — RD1-RD6, RD9, RD10, RD12
  and RD13 against `bindings/dotnet/CLAUDE.md` (RD10 was added at the CP0 review,
  RD12 and RD13 at the CP1 review). RD2 is flagged separately: it corrects
  **pre-existing** drift (the never-shipped `IsFatal`) and needs its own sign-off.
- `RULE-DRAFT-RD7-RD8-RD11-ffi-marshalling.md` — RD7, RD8 and RD11 against
  `bindings/dotnet/.claude/rules/ffi-marshalling.md` (RD11 was added at the CP1
  review).

No agent edits either rule file. The user applies, edits or declines each draft; the
phase close does not wait on that (Q12).

### 7.3 STATUS entry (Manager, Step 7)

A newest-first entry in `bindings/dotnet/design/current/STATUS.md`, in the M15/P12
entry's shape:

- phase, N=84, Mode A, branch and base, commits (not squashed), and the plan's
  archived path (`design/history/M17/P1-producer-transactions/PLAN.md`);
- the Mode-A proof with its control positive, `internal static extern` **697 -> 715**,
  test totals per TFM, and which gates ran locally versus in CI (net8.0 per Q18, the
  integration gate per §4.6);
- the values measured at CP5 and pinned (D9's codes and the non-empty-map
  non-transactional `SendOffsetsToTransaction` message; KPT 1950's rendering; D11's
  outcome; the unassigned-code readback);
- the Critic 84 record, archived as
  `design/history/M17/P1-producer-transactions/COMMENTS.DONE.84.md` (the
  binding-root file is never committed, `bindings/dotnet/CLAUDE.md` §8.4);
- the file-forward list (D15), the Mode-B rows B1-B7 with Q4's outcome, the
  RULE-DRAFT status, and the local gate logs' location (Q20).

The root `marked_classes.txt` is **not** touched: it tracks Java classes translated
into the Rust core (last edited by core commits, e.g. `47796aae`), and this phase
translates none.

On approval the Manager saves this plan as
`design/history/M17/P1-producer-transactions/PLAN.md` (the M15/P12 precedent); the
planning run was permitted to write only `design/current/PLAN-M17-producer-transactions.md`.

---

## §8 Risks

| # | Risk | Likelihood / impact | Mitigation |
|---|---|---|---|
| R1 | **Barrier defect.** A barrier that never completes hangs every async commit and abort; one that completes early silently drops Java's callback guarantee. | Medium / high | CP3 lands the barrier alone, unused by production, with S4 (idle, behind N groups, the coalescing boundary, the stop path, the gate) and its mutations; Critic 84 reviews CP3 before CP4 wires it; production awaits it only after native success (D4). |
| R2 | **Hangs.** New waits: the async drain, the bounded blocking drain, the barrier, native control operations. | Medium / high | Production: the blocking drain is bounded (30 s, D3), the async waits take a token (D5), native operations are bounded by `max.block.ms`. Tests: every wait goes through `TestTimeout.Run` (§5.1 principle 3), so a hang fails its test instead of stalling the suite. |
| R3 | **Measurement drift.** Several expectations are measured at CP5 and pinned (D9 codes, the non-empty-map non-transactional message, KPT 1950's rendering, D11's outcome, the unassigned-code readback). A measurement can disagree with the plan's expectation, or the core can drift later. | Medium / medium | Pin from the measurement, never from the plan's guess. A disagreement with a plan row is reported in the gate log; one that implicates core behaviour goes to `kafka-critic` through the Manager, and the binding never adapts to it (D6, D9). |
| R4 | **Interface additions** break external implementers of `IProducer` / `IAsyncProducer`. | Low / low | Pre-publish package; the `Metrics()` / `Send(record, callback)` precedent (`bindings/dotnet/CLAUDE.md` §3); the four in-tree implementers are enumerated (D1); G8 builds the consumers outside the sln. |
| R5 | **`[Obsolete]` cascade** under `TreatWarningsAsErrors`. | Low / low | Today the only construction site is `ConsumerGroupMetadataMarshal.cs:50` (planning-time grep), which moves to the internal factory; tests and servicers suppress CS0618 locally; G2, G8 and G9 fail on any missed site. |
| R6 | **net8.0 is not runnable here** (§4.1): a net8-only failure surfaces only in CI. | Medium / medium | Q18: install .NET 8 into `$HOME/.dotnet` with the user's go-ahead, otherwise record every G5 as "CI-verified". Never `DOTNET_ROLL_FORWARD`. |
| R7 | **arm64 protoc crash** blocks the host route for grpc-server (`Makefile:267-271`). | Medium / low | CP0 measures the route; the image route (§4.5 step 3) is the fallback; an unavailable route is reported as "not run", never as passed. |
| R8 | **Broker and container flakes** in G10. | Medium / medium | G10 runs at CP0 (baseline) and CP7 (gate) against the **stored** CP0 log, not a derived set (M15/P12's method change); every rerun is recorded; CI's amd64 job is authoritative (§4.6). |
| R9 | **Suite duration** grows (bounded waits, the 5000-iteration loop, the >1 MiB grow case). | Low / low | No sleeps; two sanctioned windows only (§5.1 principle 3); attempt counting uses the fake-delegate helper, with one real end-to-end call; the net10.0 suite duration is recorded at CP0 and CP5. |
| R10 | **D10's sync twin** is a race by construction: the test cannot see the worker enter native. | Low / low | The 5 s `max.block.ms` window against a 50 ms retry; the async test is the deterministic contract. At CP5 the Actor runs the sync twin 50 times in a loop and records the pass count; any failure means a redesign before CP5 closes. |
| R11 | **The hook's setup-only rule is unguarded** (D8): a hook installed while a commit runs. | Low / low | The header calls it a logical race under the mock's own lock, **not** undefined behaviour (h:16562-16569); it is documented (X11), not guarded, as the ABI intends. |
| R12 | **Doc drift.** The residual, drain and cancellation claims are the kind of comparative text that went stale repeatedly in M14/P1. | Medium / medium | §7.1's canonical homes; on any repair, grep the clause's distinctive words across every document (ffi §A6 round-4 and round-5 rules); Critic 84 checks each canonical home against the code at CP5. |
| R13 | **`Clear()` stranding reaches a test.** A test that calls `Clear()` with manual sends pending hangs its producer's `Dispose` (file-forward item 10), leaking a blocked pump thread for the rest of the run. | Low / medium | No M17 test calls `Clear()` — none of the mapped Java or Python transaction tests does (`MockProducerTest` calls it only at `:81` and `:101`, outside the phase). The hazard is documented (X12) and filed forward (Q21), not exercised. |
| R14 | **Java version skew.** The checked-out `kafka/` tree reads `4.2.2-SNAPSHOT` (`kafka/gradle.properties:17`), while root `CLAUDE.md` names 4.3.1 and `producer-transactions.md` cites 4.2; a Java line cited here may move when the tree is updated. | Low / low | Every Java citation was re-read in the checked-out tree at planning time; KIP-939 (absent from it) is out of scope (O1, Q17). |
| R15 | **The recovery superset.** The KIP-1050 variant routes five more codes to "close" than Java's example (D6). | Low / medium | The remarks lead with the literal form and state the difference in one sentence (X2); `IsApplicationRecoverableError` itself is faithful to its Java class. |

---

## §9 Open questions

Every question has a default. Answers are collected at plan approval, and the Actor
proceeds on the default for any question left unanswered — with one exception: Q18's
default changes the user's environment outside the repository, so without an explicit
go-ahead it is treated as declined (net8.0 is then CI-verified). Approving the plan
counts as the go-ahead for Q26's local Docker gate unless the user says otherwise.
After approval the user is needed only for the push to CI (D16).

1. **Q1 — Which hierarchy predicates (D6)?** Default: the five
   (`IsTransactionAbortableError`, `IsApplicationRecoverableError`,
   `IsInvalidConfigurationError`, `IsAuthorizationError`, `IsOutOfOrderSequenceError`),
   with the remarks leading with the literal recovery form. Alternatives: the minimal
   three; adding `IsProducerFencedError` sugar (rejected by default: a leaf class needs
   no predicate, root `CLAUDE.md` §10.4).
2. **Q2 — Cancellation semantics (D5)?** Default: D5's table, in particular *success*
   when the token fires while awaiting the barrier (the commit happened). Alternative:
   fault with `OperationCanceledException` there — rejected by default because it
   invites aborting a committed transaction.
3. **Q3 — Also give `Flush` a completion barrier?** Default: no; filed forward
   (file-forward item 7). Java's flush post-condition has the same shape, so it is a
   candidate for its own small phase.
4. **Q4 — Mode-B rows B1-B7: schedule any?** Default: none in M17/P1 (the .NET
   personas may not author ABI). Recommendation for a follow-up: **B1**
   (`fenceProducer`, unlocks 9 `MockProducerTest` tests) and **B2** (the four
   transaction-state getters, turns 4 proxied tests into translated ones) as the next
   ABI shims — both thin wrappers over existing core functions (`mock_producer.rs:773`,
   `:787-815`) — as a Rust-core task (`actor-executor` / `kafka-critic`) followed by a
   small Mode-A .NET phase. B3, B5 and B6 would complete the projections; B4 affects
   only non-transactional tests; B7 has no Java test at the binding level. **Question:
   none, B1 + B2, or all of B1-B7?**
   **Answer (user, 2026-09-28):** none — the default.
5. **Q5 — Bind `begin_transaction_async`?** Default: no (B8) — Java's
   `beginTransaction` does not block, so there is no `Task` member to back.
6. **Q6 — Keep the async `BeginTransaction` a `void` member although it may now wait
   (the bounded drain)?** Default: yes, per `bindings/dotnet/CLAUDE.md` §4 (the wait is
   bounded and normally one batch window). Alternative: `Task BeginTransaction(ct)` —
   a surface change that also needs a §4 amendment.
7. **Q7 — The `[Obsolete]` text (D2)?** Default: "Deprecated since Kafka 4.2: use
   IConsumerCommon.GroupMetadata() instead. ConsumerGroupMetadata becomes an interface
   in Kafka 5.0.", warning level. Alternative: public constructors without
   `[Obsolete]` (rejected by default: Java deprecates them).
8. **Q8 — Shape of the commit-error hook (D8)?** Default: `SetCommitTransactionError(int
   code, string? message = null)` plus `ClearCommitTransactionError()`. Alternative: a
   property (a write-only property is an FDG anti-pattern and needs two inputs).
9. **Q9 — The metadata grow rule (D8)?** Default: 4101 bytes, x4 per ambiguous
   attempt, ceiling 1,048,581 (1 MiB + 5), then `InvalidOperationException`.
   Alternatives: Python's fixed 512-byte buffer (truncates silently), or a different
   initial size.
10. **Q10 — The stale Rust comment at `tests/integration/producer_transactions_test.rs:674-678`?**
    Default: report only (G1 keeps `tests/` untouched in a Mode-A phase); fix it in any
    later Rust-side change. Alternative: allow a one-comment edit as a recorded G1
    exception.
11. **Q11 — `ProducerConfig`-style string constants (`EnableIdempotence`,
    `TransactionalId`)?** Default: deferred — no constants type exists in the binding,
    and adding one is its own surface decision.
12. **Q12 — RULE-DRAFTs RD1-RD9?** Default: the Manager writes the two drafts at CP7
    (§7.2); the user applies, edits or declines them; the phase close does not wait.
    RD2 (the never-shipped `IsFatal` in the `CLAUDE.md` sketch) needs its own sign-off
    because it corrects pre-existing drift.
13. **Q13 — One phase or three?** Default: one phase, CP0-CP7 (D16).
14. **Q14 — R-b (a delivery callback that blocks on commit / abort)?** Default:
    document only. Java's analogue ends in a `max.block.ms` timeout; here the barrier
    wait has no bound except the token, so a callback that calls `.Wait()` on the
    commit hangs. Alternative: a pump-thread identity check that throws
    `InvalidOperationException` when commit / abort is awaited from the pump thread —
    binding scaffolding, not Kafka logic, but a behaviour Java does not have.
15. **Q15 — Mock helpers public?** Default: public inherent members on both mocks
    (Java's are public; the existing helpers are). Alternative: `internal`.
16. **Q16 — Internal test seams (D3)?** Default: the settings seam and the internal
    submit-delegate overloads. Alternative: environment-variable-driven tests in a
    serial collection (slower, and it couples tests through process state).
17. **Q17 — Java reference and KIP-939?** Default: the checked-out `kafka/` tree
    (`4.2.2-SNAPSHOT`) is the reference; KIP-939 2PC is out of scope (absent from both
    the tree and the ABI).
18. **Q18 — net8.0 locally?** Default: with the user's explicit go-ahead at approval,
    the Actor runs `.semaphore/install-dotnet.sh` into `$HOME/.dotnet` at CP0; without
    it, every G5 is recorded as "CI-verified". Never `DOTNET_ROLL_FORWARD`.
    **Question: may the Actor install .NET 8 into `$HOME/.dotnet`?**
    **Answer (user, 2026-09-28):** yes — an explicit go-ahead.
19. **Q19 — The file-forward list (D15)?** Default: reported in the STATUS entry, not
    acted on. Alternative: open follow-up tasks now.
20. **Q20 — Commit the per-checkpoint gate logs?** Default: yes, short summaries at
    `design/history/M17/P1-producer-transactions/gate/CP<n>.txt` (M15/P12's method-change
    note: store the baseline instead of reconstructing it); raw logs stay in scratch.
    RD9 would make the location a standing convention if the user wants it.
21. **Q21 — R-a and the `Clear()` amplification (file-forward item 10)?** Default:
    document (X5, X12) and file forward; no fix here. Established while planning: the
    same stranding already makes the producer's `Dispose` hang, independently of this
    phase. A fix needs either a core change (which would diverge from Java's `clear()`)
    or a pump that can be released without resolving its batch — its own phase.
    **Question: accept the default, or schedule that fix before or after M17/P1?**
    **Answer (user, 2026-09-28):** the default — document and file forward.
22. **Q22 — Branching?** Default: the stacked branch
    `prashah_dev_dotnet_producer_transactions` off `76629aea`, PR target
    `prashah_dev_dotnet_binding`. Alternative: commit on `prashah_dev_dotnet_binding`
    directly.
23. **Q23 — Critic cadence?** Default: `dotnet-critic` 84 after every checkpoint.
    Alternative: one review over CP1-CP7 (M15/P12's shape — cheaper, but a CP3 barrier
    defect would then be found after CP4-CP6 were built on it).
24. **Q24 — Accept the documented divergences?** Default: yes — the managed closed
    check precedes the core's checks (D2); `ArgumentNullException` where Java's mock
    throws NPE; MPT 170 / 176 / 638 / 645 translated with an empty map where Java passes
    `null`.
25. **Q25 — Who pushes?** Default: the user; agents never push, rebase or force-push
    without instruction.
26. **Q26 — Run the local Docker gate (G10)?** Default: yes, at CP0 and CP7 (it builds
    two amd64 images and runs the `__grpc_dotnet` roster against local brokers);
    approving the plan counts as the go-ahead. If declined, G10 is recorded as "not
    run" and CI is the only integration evidence.
    **Answer (user, 2026-09-28):** yes — an explicit go-ahead, at CP0 and CP7.
27. **Q27 — `ConsumerGroupMetadata` `Equals` / `GetHashCode` / `ToString` (file-forward
    item 11)?** Default: filed forward. If taken now: about 40 lines plus about 6 tests
    in CP2, with `ToString` rendering Java's
    `GroupMetadata(groupId = %s, generationId = %d, memberId = %s, groupInstanceId = %s)`.

---

## §10 Size, branch and execution

### 10.1 Size

Estimates of changed lines (added plus deleted), for scheduling only:

| CP | Production | Tests | Main contributors |
|---|---|---|---|
| CP0 | 0 | 0 | the gate log |
| CP1 | 180-220 | 200-300 | 18 `[DllImport]`s with ownership xmldoc; S1 |
| CP2 | 130-180 | 450-550 | five properties, the ctor chain, the two obsolete ctors and the factory; S2 (with the sweep), S3's constructor half |
| CP3 | 100-150 | 400-500 | `EnqueueBarrier`, the barrier form, `RunLoop` / stop handling; S4 |
| CP4 | 550-750 | 1,100-1,500 | nine control operations, the extracted drain helper, seams, D5 wiring, the grow helper, the transient handle; S5-S7 |
| CP5 | 550-750 | 1,700-2,300 | the public members and their xmldoc (§7.1), forwarders, remarks; S3's control half, S8-S12 |
| CP6 | 300-400 | 0 | ten servicer overrides, two `Translate` helpers |
| CP7 | about 24 (`Makefile`) | 0 | the comment block and the three `--skip` lines |

Plus 300-500 lines of history records (gate logs, the two RULE-DRAFTs, the STATUS
entry). **Total: about 6,000-8,000 changed lines, roughly two thirds tests**, before
the archived copy of this plan (about 2,000 lines) that the Manager adds on approval.
For scale, M15/P12 closed at +5116 / -32 (`design/current/STATUS.md:10`).

### 10.2 Branch and commits

- **Branch:** `prashah_dev_dotnet_producer_transactions`, created by the Actor at CP0
  from `76629aea` (the planning-time HEAD of `prashah_dev_dotnet_binding`; if `git
  rev-parse HEAD` differs at CP0, the Actor stops and reports rather than rebasing).
  PR target: `prashah_dev_dotnet_binding` (Q22).
- **Pre-existing worktree state** (planning time): `bindings/dotnet/.claude/agents/*`
  deleted, `.claude/agents/dotnet-*.md` untracked, the `kafka` submodule modified. It
  is the user's in-progress work: creating the branch carries it along untouched, and
  G7 keeps every one of those paths out of every commit.
- **Commits:** one or more per checkpoint, messages in the repository's
  `feat(dotnet): ...` / `test(dotnet): ...` / `docs(dotnet): ...` style with the
  attribution trailer; never squashed. Review fixes are `fixup!` commits referencing
  the commit that introduced the issue and the `COMMENTS.84.md` item (agent-roles
  §1). No rebase, autosquash or force-push without the user's instruction (Q25).
- **Review files:** `bindings/dotnet/COMMENTS.84.md` and `COMMENTS.DONE.84.md` are
  local working files, never committed (`bindings/dotnet/CLAUDE.md` §8.4).
- ⚠ **Execution note (added at the CP0 review, 2026-09-28): agents cannot commit.**
  `git commit` and `git push` are denied to agents in this environment, so the
  **user** makes every commit. The Commits bullet above is adapted as follows:
  - Each checkpoint is reviewed **uncommitted**. The Actor leaves its work unstaged
    and lists its new files; the Critic reviews `git diff` plus those files.
  - Review fixes are folded in before the commit, so there are no `fixup!` commits;
    `COMMENTS.DONE.84.md` is the record of what each fix changed.
  - When a checkpoint is clean, the Actor stages exactly its paths and writes the
    commit message to a file; the Manager hands the user the commands.
  - The next checkpoint may start while the previous one waits for the user's
    commit, but it stages nothing until that commit exists.
  - While work is uncommitted, G1 and G7 run in these forms, which keep §4.2's
    scopes and also cover untracked new files, which `git diff` never shows
    (corrected at the CP0 review, finding 84.2). Each must print nothing unless
    stated otherwise:
    - G1, tracked: `git diff 76629aea -- src/ src/ffi/ cbindgen.toml generator/ tests/`.
    - G1, new files: `git ls-files --others --exclude-standard -- src/ cbindgen.toml generator/ tests/`.
    - G1, header: the SHA-256 still equals the CP0 value.
    - G1, control positive: `git diff --stat 76629aea -- bindings/dotnet/src bindings/dotnet/tests bindings/dotnet/grpc-server`
      must be **non-empty** from CP1 on.
    - G7, tracked: `git diff 76629aea -- bindings/dotnet/src bindings/dotnet/tests bindings/dotnet/grpc-server Makefile | grep -nE '^\+.*\b(TODO|FIXME|XXX|HACK)\b'`.
    - G7, new files: `git ls-files -z --others --exclude-standard -- bindings/dotnet/src bindings/dotnet/tests bindings/dotnet/grpc-server | xargs -0 grep -nHE '\b(TODO|FIXME|XXX|HACK)\b' /dev/null`.
    The scopes matter: an unscoped `git diff 76629aea` includes this plan, which
    quotes the marker pattern, so the G7 grep could never pass.

### 10.3 Execution after approval (not started by this plan)

1. The Manager saves this plan as
   `bindings/dotnet/design/history/M17/P1-producer-transactions/PLAN.md` and records
   the user's answers to §9 in it.
2. For each checkpoint CP0-CP7: spawn `dotnet-actor` 84 with that checkpoint's rows
   from §4.3 and §5; then `dotnet-critic` 84 over the checkpoint's commits, writing
   only to `COMMENTS.84.md`; the Manager summarizes; fix cycles repeat until
   `COMMENTS.84.md` is empty and the checkpoint's gates are green; only then does the
   next checkpoint start (Q23). A finding that implicates the C ABI or the core is
   routed to `kafka-critic` / `actor-executor`, never fixed on the .NET side.
3. Step 7: the STATUS entry (§7.3), `COMMENTS.DONE.84.md` archived under
   `design/history/M17/P1-producer-transactions/`, `COMMENTS.84.md` reset, the
   RULE-DRAFTs handed over (§7.2), and the user pushes for CI (Q25).

This plan stops here: no agent has been spawned, and nothing has been built,
committed or branched.
