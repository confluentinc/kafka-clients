# M15/P14: split the .NET Admin client into a sync client and an async client

| | |
|---|---|
| Phase | M15/P14 |
| Agent number | **N = 95** (`dotnet/design/current/STATUS.md:55`) |
| Agents | `dotnet-actor` 95 and `dotnet-critic` 95 |
| Branch | `prashah_dev_dotnet_binding` (base HEAD `2d73a746`) |
| Status | **Approved** with user rulings (see §0, Approval record). |
| Design source | Confluence 6260590203, ".NET Design, Admin Client split, KafkaFuture options", §2 Option 2 (Decided) and §3(a) (Decided) |

**This plan supersedes two earlier decisions, on user direction:**

- **M15 D2**, "ONE `IAdmin`, no `IAsyncAdmin`". Recorded at `STATUS.md:659` and `:665`, at `design/history/M15/P1-admin-foundation/PLAN.md:96`, in the untracked roadmap `design/current/PLAN-M15-admin-client.md` (§D2 at lines 130-150, §5.3 at about 625-690), and in the `IAdmin.cs:31-41` remarks.
- **M15/P12 D1**, "the admin harness has a sync arm only". Recorded at `design/history/M15/P12-admin-gaps/PLAN.md:555`, `STATUS.md:435`, `grpc-server/Program.cs:122-125`, `rust/tests/common/backend_factory.rs:637-650`, `rust/tests/common/multilanguage_admin_test_macro.rs:23-24` and `rust/tests/common/backend_pool.rs:116-118`.

Both decisions were sound when made. Their premise was "a second interface would be a synonym", because both would return the same `Task`-based results. The team has now chosen two result families, so the premise no longer holds. The hazard analysis behind them (one native handle per client, span-the-op handle refs, the per-key countdown) is reused unchanged below. Historical PLANs stay as written. STATUS records the supersession in the close-out entry.

**The team decisions this plan carries out. They are not re-opened here:**

1. Split the admin client into a sync client and an async client, for **parity with Python** (`Admin` / `AsyncAdmin`).
2. The sync client makes **true blocking calls**, with no sync-over-async.
3. Sync Result types hold **`KafkaFuture`**. Async Result types hold **`AsyncKafkaFuture`**. Two Result families were weighed against the comparison chart and accepted.

**Out of scope.** The page's comments about the package name (`Confluent.Kafka` versus `Confluent.Kafka4`) and "Adhere namespace as Java". The namespace stays `Confluent.Kafka.Admin` and the package id is unchanged (see R12).

---

## 0. Approval record

The user approved the plan "with all PM recommendations including Q3 (add a follow-up on that though for later)."

| Item | Ruling |
|---|---|
| D1-D19 | Approved as written. |
| Q1 | Keep M11/P4.2 D7: `Get()` only, no `Get(TimeSpan)`. |
| Q2 | Add a public `IsDone` to the sync `KafkaFuture` and `KafkaFuture<T>` (S1). `AsyncKafkaFuture` gets none (P3.6 D6 stands). |
| Q3 | (b): the 10 public Result constructors become internal. See "Q3 in practice" and FU-5 below. |
| Q4 | No `ValueTask` on async RPCs. |
| Q5 | Yes: stop after S4 and Critic pass 1, and report for the user's checkpoint before S5a. |
| Q6 | Three Critic gates: after S4, after S5g, after S7. |
| Q7 | Append dated amendments under existing rule rows; do not rewrite them. |
| Q8 | Accept the 78 extra Docker-backed admin tests in CI. |

**Q3 in practice.**
- The 44 `Async*Result` types get internal constructors from the start (S3a).
- Each sync Result's constructor becomes internal in the slice that converts its family: CreateTopics, DeleteTopics, DescribeTopics and DeleteRecords in S4; ListOffsets in S5b; DescribeClassicGroups and DescribeConsumerGroups in S5c; AlterClientQuotas and DescribeClientQuotas in S5d; AlterUserScramCredentials in S5e.
- No code outside the unit tests calls these constructors. The tests keep compiling through the existing `InternalsVisibleTo`.
- **This is a recorded Java-parity deviation.** These are exactly the 10 constructors Java makes public (7) or protected (3). The xmldoc on each says so, and points to FU-5.

**FU-5 (follow-up, tracked).** Revisit making the 10 Result constructors public again, together with a public `KafkaFuture<T>.CompletedFuture(T)` (Java's `KafkaFuture.completedFuture`) and an async analog, so users can build Results, for example to mock `IAdmin`. FU-5 goes into this phase's STATUS entry at S7, alongside FU-1 to FU-4. It is not a TODO in code.

**A new rule, approved verbatim, for `dotnet/CLAUDE.md`.** Its substance:

> a. `Task` when the Java method itself blocks. The method call is the wait.
> b. `AsyncKafkaFuture` when Java hands you back a `Future` object to wait on later.

- Home: a dated amendment (Q7 style) directly under the table in §4 "Sync vs async — the governing rule" (`dotnet/CLAUDE.md` about lines 557-565). Cite M15/P14.
- Each line may carry one short example with its sync mirror. For a: `Task Flush(CancellationToken)` on `IAsyncProducer`, against `void Flush()` on `IProducer`. For b: `AsyncKafkaFuture<T>` inside `AsyncCreateTopicsResult`, against `KafkaFuture<T>` inside `CreateTopicsResult`.
- At most a one-line pointer from the §3 idiom-map `Future` row (line 487). The rule is not restated anywhere else.
- Applied in S7. It needs no further sign-off.

**The other rule-file edits in §10 are approved in principle only.** Their exact text goes to the user before it is applied, at the S4 checkpoint or at S7. Only the a/b rule above is approved verbatim.

**Q5 checkpoint rulings (after S4 and Critic pass 1).** The user's words: "go S5a, approve 1–8, phase tag fine, confirm all 3".
- S5a-S7 proceed with the S4 pattern unchanged.
- The §10 rule edits below are approved **verbatim**. Each is appended to its existing row and tagged "M15/P14" (no calendar date). They are applied at S7, together with the a/b rule, and no rule-file edit beyond them is approved.
  1. `dotnet/CLAUDE.md:59-61`, after "...`MockAdminClient`, M15)": "; since M15/P14 that is the sync flavour, beside the async `IAsyncAdmin` (`AsyncKafkaAdminClient` / `AsyncMockAdminClient`)."
  2. `dotnet/CLAUDE.md:431-433`, appended: "Since M15/P14 it ships in two flavours over one internal core: sync `IAdmin` (`KafkaAdminClient` / `MockAdminClient`; Results hold `KafkaFuture` / `KafkaFuture<T>`) and async `IAsyncAdmin` (`AsyncKafkaAdminClient` / `AsyncMockAdminClient`; `Async*Result` types hold `AsyncKafkaFuture` / `AsyncKafkaFuture<T>`)."
  3. `dotnet/CLAUDE.md:487`, the idiom-map `Future<RecordMetadata>` row.
     - C# cell, appended: "Since M15/P14 Java's Admin `KafkaFuture<T>` maps to `KafkaFuture<T>` in the sync admin Results and to `AsyncKafkaFuture<T>` in the async ones, with non-generic `KafkaFuture` / `AsyncKafkaFuture` for Java's `KafkaFuture<Void>`. This supersedes the 'mapped to `Task<T>`' note (M11/P4.2 D10). Which one a method returns: §4 Sync vs async, the M15/P14 a/b rule."
     - Rule cell, appended: "admin: one completion per key, ffi §B6 shape 4 (M15/P14)".
  4. `dotnet/CLAUDE.md:561`, the "Returns `Future<T>`" row, appended: "Admin (M15/P14): every RPC returns its Result at once, as in Java; the Result holds one `KafkaFuture<T>` per key on `IAdmin` and one `AsyncKafkaFuture<T>` per key on `IAsyncAdmin`. Only `Close` waits."
  5. `dotnet/CLAUDE.md:533`, the Disposal row, appended: "Admin (M15/P14): `IAsyncAdmin : IAsyncDisposable, IDisposable` follows this default; the sync `IAdmin : IDisposable` only (as `IProducer` / `IConsumer`), with a blocking `void Close(TimeSpan)` over the sync `kafka_admin_AdminClient_close`."
  6. `dotnet/.claude/rules/ffi-marshalling.md:2023`, §B6 sub-shapes 4a/4b, appended: "(M15/P14: read 'one `Task` per key' as 'one completion per key': a `TaskCompletionSource` with `RunContinuationsAsynchronously` on the async flavour, or a `SyncCompletion` latch on the sync flavour, chosen by the operation's `AdminFlavour`. `AdminCallbacks` and the countdown do not depend on the flavour.)"
  7. `dotnet/.claude/rules/ffi-marshalling.md`, a new bullet at the end of the §B7 Anti-patterns list: "A sync admin Result composed over Tasks (`Task.WhenAll`, an `async` projection, `ContinueWith`) and then blocked on (`GetAwaiter().GetResult()`, `.Wait()`, `.Result`). This is sync-over-async: with `RunContinuationsAsynchronously` the composite needs a thread-pool thread to complete, so a saturated pool starves it. Compose over `SyncFutureSource` instead (`ProjectedSyncFuture`, `AllOfSyncFuture`, `GatheringAllOfSyncFuture`), which runs on the thread that calls `Get()` (M15/P14)."
  8. `dotnet/.claude/rules/bindings.md:59-60`, appended to the host-idioms bullet: "A binding may offer both a blocking and an awaitable type for Java's `Future`; .NET does (`KafkaFuture` / `AsyncKafkaFuture`, M11/P4.2, M15/P14)."
- The a/b rule goes directly under the §4 table (after the "Non-blocking **action**" row), under the heading "M15/P14: `Task` or `AsyncKafkaFuture` on an async interface", with its two lines and the examples given above.
- Three Manager calls are confirmed:
  - a default sync future's `IsDone` throws the same `InvalidOperationException` as `Get()`;
  - the DescribeTopics aggregates are nullable futures, null on the side not requested;
  - the 44 `NativeAdminClient` delegators deleted in S3b stay deleted.
- Two corrections for later slices:
  - the harness admin share is **79**, not 78, so S6 expects `__grpc_dotnet_async` 45 + 79 = **124** and the native harness 169 + 79 = **248**. That supersedes the 78 / 123 / 247 in §6.3 and §8, and the STATUS entry uses 124 / 248;
  - the S7 xmldoc sweep also fixes the roughly 40 internal `NativeAdminClient` xmldocs that say "the async flavour's clients call this" (the sync clients call them too).

**Process.** No progress log or slice log in this PLAN at any point; the evidence goes once into the STATUS entry at S7. Commits stay on `prashah_dev_dotnet_binding` in the main tree. The user pushes.

---

## 1. Parity anchor

The anchor is fixed now. The Critic reviews against this section, not against what the code happens to do.

### 1.1 Java (Apache Kafka 4.3.1)

| Java | Shape |
|---|---|
| `Admin` | `interface Admin extends AutoCloseable`. Every RPC returns an `XxxResult` **at once**. It holds one `KafkaFuture<T>` per key, or one for the batch. |
| `AdminClient` / `KafkaAdminClient` | The real client. |
| `MockAdminClient` | In the clients test jar. Immediately resolved futures. |
| `close()` / `close(Duration)` | Blocks: it joins the I/O thread. |
| `KafkaFuture<T>` | `get()`, `get(timeout, unit)`, `getNow`, `isDone`, `isCancelled`, `isCompletedExceptionally`, `cancel`, `thenApply`, `whenComplete`, `toCompletionStage`, static `completedFuture`, static `allOf`. |
| `KafkaFuture.allOf` | `CompletableFuture.allOf`: it waits for **all** inputs, then fails with the first failure in input order. It is not fail-fast. |
| `ListTransactionsResult.allByBrokerId` | **Fail-fast**: a `whenComplete` per broker completes the aggregate on the first failure in time. |
| Results that use `whenComplete` | AlterConsumerGroupOffsets, DeleteConsumerGroupOffsets, DescribeUserScramCredentials, ElectLeaders, ListConfigResources, ListTransactions, RemoveMembersFromConsumerGroup. |
| Result constructors | Public: ListOffsets, DeleteRecords, AlterClientQuotas, AlterUserScramCredentials, DescribeClassicGroups, DescribeClientQuotas, DescribeConsumerGroups. Protected: CreateTopics, DeleteTopics, DescribeTopics. Package-private: ListTopics, DescribeCluster. |

Java has one Admin surface, and it is synchronous in shape: the RPC returns at once and the caller blocks at `get()`. The .NET sync flavour is that shape. The async flavour is the .NET-idiomatic twin, the same move Python made.

### 1.2 Python (`python/admin.py`)

| Python | .NET after P14 |
|---|---|
| `Admin` (sync base, `admin.py:3672`) | `IAdmin` |
| `AsyncAdmin` (async base, `admin.py:4439`) | `IAsyncAdmin` |
| `AdminClient` (`:5002`) | `KafkaAdminClient` |
| `AsyncAdminClient` (`:5025`) | `AsyncKafkaAdminClient` |
| `MockAdminClient` (`:5016`, via `_MockAdminClientMixin` at `:3605`) | `MockAdminClient` |
| `AsyncMockAdminClient` (`:5035`) | `AsyncMockAdminClient` |
| 47 public methods on each base, the same set | 48 .NET RPCs on each interface (four Java overload pairs are separate .NET methods; Python folds them into keyword arguments) |
| Sync per-key RPCs return `{key: concurrent.futures.Future}` at once | Sync per-key RPCs return the Result at once, holding `KafkaFuture` per key |
| 17 sync whole-batch RPCs block inside the call via `_run_sync` | **Not copied.** .NET follows Java: the RPC returns at once and the caller blocks at `.Get()`. See §1.4. |
| Sync `close` is `_run_sync` over `close_async` | Sync `Close` calls the existing sync close ABI (D4). Same observable behaviour. |
| `AsyncAdmin` methods are `async def` only because `loop.create_future()` needs a running loop; they never await inside | Async RPCs are plain methods returning the Result (D11). |

The 17 Python whole-batch RPCs that block inside the call: abort_transaction, create/describe/expire/renew delegation token, describe_acls, describe_client_quotas, describe_cluster, describe_features, describe_user_scram_credentials, elect_leaders, force_terminate_transaction, list_config_resources, list_groups, list_partition_reassignments, list_topics, list_transactions.

### 1.3 .NET precedents

| Precedent | Shape | What P14 copies |
|---|---|---|
| `IConsumer : IConsumerCommon, IDisposable` and `IAsyncConsumer : IConsumerCommon, IAsyncDisposable, IDisposable` | Sync and async twins over one native consumer | Sibling interfaces; disposal split |
| `IProducer : IDisposable` and `IAsyncProducer : IAsyncDisposable, IDisposable` | Same | Disposal split |
| All client implementations are `sealed class` | | Same for the four admin classes |
| `KafkaFuture<T>` (M11/P4.2) | `readonly struct` over a Monitor latch (`SyncCompletion<T>`) | The sync admin future |
| `AsyncKafkaFuture<T>` (M11/P3.6) | `readonly struct` over `Task<T>`, not awaitable (P3.6 D5) | The async admin future |
| `dotnet/CLAUDE.md` about lines 596-607 | "A sync member calling the sync ABI directly is not sync-over-async" | Sync `Close` / `Dispose` (D4) |
| `grpc-server/ProducerServiceImpl.cs` | Sync servicer blocks with `.Get()` on the handler thread, no `Task.Run`, returns `Task.FromResult` | The new sync `AdminServiceImpl` (S6) |
| `MockConsumer` / `AsyncMockConsumer`, `MockProducer` / `AsyncMockProducer` | Mock twins | `MockAdminClient` / `AsyncMockAdminClient` |

### 1.4 NOT adding (explicit)

The Critic flags any of these that appear.

| Not added | Why |
|---|---|
| A shared `IAdminCommon` base interface | Every admin member returns a flavour-specific type, so the two interfaces have no member in common. |
| Any conversion between `KafkaFuture` and `AsyncKafkaFuture`, any `ToTask()`, any `GetAwaiter` on `AsyncKafkaFuture` | P3.6 D5. A sync future that hands out a Task re-opens sync-over-async. |
| Public `ThenApply`, `WhenComplete`, `AllOf`, `CompletedFuture`, `GetNow`, `Cancel`, `IsCancelled`, `IsCompletedExceptionally` | Java has them, but every Result already exposes its composed futures. Adding them later is additive. Q2 and Q3 cover the two borderline members (`IsDone`, a public factory). |
| `Get(TimeSpan)` | M11/P4.2 D7 stands unless Q1 changes it. |
| `ValueTask` on async RPCs | D11. An admin RPC has no admission stage. Q4 confirms. |
| Python-style blocking whole-batch RPCs on the sync side | Java shape: the RPC returns at once. Python's `_run_sync` exists because Python had no sync future type for those Results. .NET has one. |
| Any sync-over-async in the sync flavour: `.Result`, `.Wait()`, `.GetAwaiter().GetResult()` on a Task, `Task.Run`, `Task.WhenAll` | Team decision 2. |
| New DllImports, ABI changes, header changes | §12 (Mode B: none). |
| Namespace or package changes | Out of scope. |
| Changes to Options, specs or value types (`TopicListing`, `TopicMetadataAndConfig`, `ConfigResource`, ...) | They carry no future, so both flavours share them (D2). |
| A static `Admin.Create(...)` factory | Not present today; not part of the split. |
| `CancellationToken` parameters on RPCs | Not present today; not part of the split. |

---

## 2. Public surface

Everything stays in `Confluent.Kafka.Admin`, except the two new future types, which sit beside the existing ones in `Confluent.Kafka` (page §3(a), Decided).

### 2.1 Interfaces and clients

| Type | Today | After P14 | Change |
|---|---|---|---|
| `IAdmin` | `IDisposable, IAsyncDisposable`; 48 RPCs returning Task-based Results; `Task Close(TimeSpan)` | `IDisposable` only; 48 RPCs returning **sync** Results; `void Close(TimeSpan)` | **Breaking.** Return types, `Close` and disposal all change. The `IAdmin.cs:31-41` "single interface" remarks are rewritten. |
| `IAsyncAdmin` | none | `IAsyncDisposable, IDisposable`; 48 RPCs returning `Async*Result`; `Task Close(TimeSpan)` | **New.** It is today's `IAdmin` with `AsyncKafkaFuture` in place of `Task`. |
| `KafkaAdminClient` | `sealed : IAdmin`, Task-based | `sealed : IAdmin`, sync | **Breaking.** Same constructor. |
| `AsyncKafkaAdminClient` | none | `sealed : IAsyncAdmin` | **New.** Same constructor as `KafkaAdminClient`. |
| `MockAdminClient` | `sealed : IAdmin`, Task-based; mock-only `TimeoutNextRequest(int)`, `UpdateBeginningOffsets`, `UpdateEndOffsets`, `UpdateConsumerGroupOffsets`, `SetFeatureLevels` | `sealed : IAdmin`, sync; the same mock-only methods | **Breaking** for RPC return types only. Mock-only methods are unchanged. |
| `AsyncMockAdminClient` | none | `sealed : IAsyncAdmin`; the same mock-only methods (they are sync on both, as today) | **New.** |

Every RPC on both interfaces keeps its current name, parameters and overloads. The 48 methods are listed by family in §6.

Example, sync:

```csharp
using var admin = new KafkaAdminClient(config);
CreateTopicsResult r = admin.CreateTopics(topics, new CreateTopicsOptions());
r.All().Get();                        // blocks on a latch; no thread-pool thread needed
int n = r.NumPartitions("t1").Get();
admin.Close(TimeSpan.FromSeconds(30));
```

Example, async:

```csharp
await using var admin = new AsyncKafkaAdminClient(config);
AsyncCreateTopicsResult r = admin.CreateTopics(topics, new CreateTopicsOptions());
await r.All().Get();
int n = await r.NumPartitions("t1").Get();
await admin.Close(TimeSpan.FromSeconds(30));
```

### 2.2 Result types

There are 44 `*Result.cs` files today (4,898 lines). After P14:

- the 44 existing names are the **sync** Results, holding `KafkaFuture` / `KafkaFuture<T>`;
- 44 new `Async*Result` types (for example `AsyncCreateTopicsResult`, `AsyncAlterConfigsResult`, `AsyncTerminateTransactionResult`) hold `AsyncKafkaFuture` / `AsyncKafkaFuture<T>`.

(The page says "42 result types". The tree has 44. The plan uses 44.)

The mapping inside every Result is mechanical:

| Today | Sync Result | Async Result |
|---|---|---|
| `Task<T>` | `KafkaFuture<T>` | `AsyncKafkaFuture<T>` |
| `Task` (a void key, erased from `Task<bool>`) | `KafkaFuture` | `AsyncKafkaFuture` |
| `IReadOnlyDictionary<K, Task<T>>` | `IReadOnlyDictionary<K, KafkaFuture<T>>` | `IReadOnlyDictionary<K, AsyncKafkaFuture<T>>` |
| `IReadOnlyDictionary<K, Task>` | `IReadOnlyDictionary<K, KafkaFuture>` | `IReadOnlyDictionary<K, AsyncKafkaFuture>` |
| `Task All()` | `KafkaFuture All()` | `AsyncKafkaFuture All()` |
| `Task<IReadOnlyDictionary<K, V>> All()` | `KafkaFuture<IReadOnlyDictionary<K, V>> All()` | `AsyncKafkaFuture<IReadOnlyDictionary<K, V>> All()` |
| `ListTransactionsResult.ByBrokerId()`: `Task<IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>>>` | `KafkaFuture<IReadOnlyDictionary<int, KafkaFuture<IReadOnlyCollection<TransactionListing>>>>` | `AsyncKafkaFuture<IReadOnlyDictionary<int, AsyncKafkaFuture<IReadOnlyCollection<TransactionListing>>>>` |

Behaviour that does not change, on either flavour:

- Synchronous argument guards stay synchronous. Example: `CreateTopicsResult.Config(null)` and `Config("unknown")` throw at the call, with today's messages ("Topic '{0}' was not part of this createTopics request.").
- Guards that throw inside the future today still throw at `Get()` / `await`. Example: `AlterConsumerGroupOffsetsResult.PartitionResult(tp)` throws `ArgumentException` "Alter offset for partition \"{0}\" was not attempted".
- `ListConsumerGroupOffsetsResult.PartitionsToOffsetAndMetadata()` still throws `InvalidOperationException` when the request named more than one group.
- `DeleteTopicsResult` keeps its "exactly one non-null" validation and messages.
- `ListGroupsResult.All()` still rethrows the first error with its original stack (`ExceptionDispatchInfo`).
- `ListTransactionsResult.AllByBrokerId()` stays **fail-fast** on both flavours.

The 10 Result types with a **public constructor** today: AlterClientQuotas, AlterUserScramCredentials, CreateTopics, DeleteRecords, DeleteTopics, DescribeClassicGroups, DescribeClientQuotas, DescribeConsumerGroups, DescribeTopics, ListOffsets. Their parameters change type with the split. Q3 was ruled (b): they become internal, as a recorded Java-parity deviation with follow-up FU-5 (§0). The other 34 constructors are internal and stay internal.

### 2.3 Futures

| Type | Today | After P14 | Change |
|---|---|---|---|
| `KafkaFuture<T>` | `readonly struct` over `SyncCompletion<T>`; `T Get()`; equality by completion identity | Same members, over an internal `SyncFutureSource<T>` base | Field type widens (internal). Xmldoc and the default-value message become neutral between producer and admin. A public `bool IsDone` (Q2, ruled yes). |
| `KafkaFuture` (non-generic) | none | `readonly struct`; `void Get()`; equality by source identity | **New.** Java's `KafkaFuture<Void>`. |
| `AsyncKafkaFuture<T>` | `readonly struct` over `Task<T>`; `Task<T> Get()` | Same | Xmldoc and the default-value message become neutral. Its naming remark ("the admin client maps Java's `KafkaFuture<T>` to `Task<T>`") is replaced. |
| `AsyncKafkaFuture` (non-generic) | none | `readonly struct` over `Task`; `Task Get()`; equality by Task identity | **New.** |

The default-value messages change from "...carries no send; only IProducer.Send returns a usable one." to a neutral text such as "This KafkaFuture is a default value and is bound to no operation; only IProducer.Send and the IAdmin result types return a usable one." The async one changes the same way. The tests asserting them (`PublicKafkaFutureTests.cs:38`, `PublicAsyncKafkaFutureTests.cs:36`) are updated in the same commit.

Erasure from `KafkaFuture<bool>` to `KafkaFuture` wraps the **same** source object. Erasure from `AsyncKafkaFuture<bool>` to `AsyncKafkaFuture` wraps the **same** Task. There is no public erasure or conversion API; erasure happens only inside Result construction.

---

## 3. Engine design

### 3.1 Where the flavour lives

The flavour lives **inside the per-key completion object**. Nothing else in the bridge knows about it.

Today:

- `AdminOperation` owns the GCHandle, the span-the-op `DangerousAddRef`, and a countdown armed at n+1 with a submit token.
- `KeyedAdminOperation<TKey,TValue>` keeps `Dictionary<TKey, TaskCompletionSource<TValue>> _perKey`, created with `RunContinuationsAsynchronously`.
- `VoidKeyedAdminOperation<TKey>` is `KeyedAdminOperation<TKey,bool>`.
- `SingleAdminOperation<TValue>` keeps one TCS.
- `ListTransactionsAdminOperation` has two stages: an outer completion of a broker dictionary, then one completion per broker.
- `AdminCallbacks` casts `GCHandle.Target` to these operation types and calls `SetResult` / `SetException`.

After P14:

```
internal interface IAdminCompletion<T>
{
    bool TrySetResult(T value);
    bool TrySetException(Exception error);
    bool IsCompleted { get; }
}

internal enum AdminFlavour { Sync, Async }

// async: allocation identical to today
internal sealed class TaskAdminCompletion<T>
    : TaskCompletionSource<T>, IAdminCompletion<T>   // ctor passes RunContinuationsAsynchronously

// sync: the producer's latch, unchanged in behaviour
internal sealed class SyncCompletion<T>
    : SyncFutureSource<T>, IAdminCompletion<T>
```

- Each operation constructor takes an `AdminFlavour` and creates the matching completion per key.
- `_perKey` becomes `Dictionary<TKey, IAdminCompletion<TValue>>`. `SetResult`, `SetException`, `FailAll` and `FailUncompleted` call the interface.
- The operation exposes two typed views: `SyncSources` (sync flavour) and `Tasks` (async flavour). Asking for the wrong view throws `InvalidOperationException` (a programming error inside the binding).
- `AdminCallbacks`, the countdown, `AdminOperation`, the GCHandle handling and every DllImport stay **unchanged**. The cast targets in `AdminCallbacks` are the same classes.

**Why not a third generic parameter on the operation?** `AdminCallbacks` casts `GCHandle.Target` to the operation classes at 11 sites (for example `AdminCallbacks.cs:2549`, `(KeyedAdminOperation<TKey, TValue>)handle.Target!`). Most are generic helpers shared by many RPCs, so a third type parameter would ripple into every helper signature and every callback that calls one. The interface call costs one virtual dispatch per key, and admin has no hot path (`admin-client.md` §10, DoD #10 N/A).

### 3.2 One core, two thin wrappings

- `NativeAdminClient` (7,028 lines) keeps one implementation per RPC: preconditions, de-duplicated keys, build the operation, `GCHandle.Alloc`, AddRef inside the try, marshal, `SetPendingCallbacks`, submit, `ReleaseSubmitToken`, `AbandonBeforeSubmit` on failure, destroy the inputs in `finally`.
- Each RPC gains an `AdminFlavour` parameter and **returns the operation** instead of `new XxxResult(operation.Tasks)`. There are 42 `return new ...Result` sites and 4 `Result.Of...` sites to change.
- Each Result type gets one internal factory per flavour, for example `CreateTopicsResult.From(KeyedAdminOperation<string, TopicMetadataAndConfig> op)` and `AsyncCreateTopicsResult.From(...)`. The wrapping is written **once per flavour**, not once per client.
- The four clients are one-line delegators, for example `=> CreateTopicsResult.From(_native.CreateTopics(newTopics, options, AdminFlavour.Sync))`.
- The submit-delegate seams used by the Interop tests (`NativeCreateTopicsSubmit` and the others) are unchanged.

### 3.3 Sync composites (internal)

Every sync composite derives from `SyncFutureSource<T>` and runs **on the thread that calls `Get()`**. None of them needs a thread-pool thread.

```
internal abstract class SyncFutureSource            { void Wait(); bool IsDone { get; } }
internal abstract class SyncFutureSource<T> : SyncFutureSource { T Get(); }
internal sealed class SyncCompletion<T>   : SyncFutureSource<T>   // existing latch
internal sealed class ProjectedSyncFuture<TIn,TOut> : SyncFutureSource<TOut>
internal sealed class AllOfSyncFuture      : SyncFutureSource       // void all-of
internal sealed class GatheringAllOfSyncFuture<K,V> : SyncFutureSource<IReadOnlyDictionary<K,V>>
internal sealed class FailFastGroupSyncFuture ...                    // ListTransactions only
```

| Composite | Semantics | Must match |
|---|---|---|
| Projection | On the first `Get()`, waits for the source, applies the function, and **memoizes** the value or the exception. Every later `Get()` returns the same value or rethrows the **same exception instance** (via `ExceptionDispatchInfo`). | Today's async projections (`Project` with `ConfigureAwait(false)`), and Java's `thenApply`. The function is pure, so lazy evaluation on the caller's thread is equivalent. |
| All-of (void) | Waits for **every** key in the operation's key order. Then, if any failed, rethrows the **first failure in key order**. | `await Task.WhenAll(...)`, which surfaces the first failure in input order, and Java's `allOf`. |
| Gathering all-of | All-of, then builds the dictionary once and memoizes it. | Today's gathering `All()` (for example `DescribeConsumerGroupsResult.All()`). |
| Fail-fast group | Used only by `ListTransactionsResult.AllByBrokerId()`. Waits until every broker completes **or the first broker fails in time**. | Today's `Task.WhenAny` loop, and Java's `whenComplete`. |

Key order: both flavours iterate the **same** ordered key list the operation built. The async side must not switch to a different order either.

**The fail-fast group latch.** `ListTransactionsAdminOperation` already publishes the broker dictionary with `Volatile`. On the sync flavour it also keeps a small group monitor: a remaining count and a first-error slot. `SetBrokerResult` / `SetBrokerException` update it under its lock and `PulseAll`. `AllByBrokerId().Get()` waits on the outer stage, then on the group monitor. `SyncCompletion` gains no callback hook.

### 3.4 No D9 guard for admin futures

The producer's `SyncCompletion` takes an optional `SendCompletionPump? owner`. `Get()` throws when it is called on the future's own pump thread (M11/P4.2 D9). Admin passes `owner: null`, so the guard is skipped.

The guard exists because producer delivery callbacks run user code on the pump thread. Admin runs **no user code** on any completion thread. The dispatcher thread, the inline caller path and the tokio-worker path only complete latches. A user may call an admin `Get()` from inside a **producer** delivery callback. That blocks that producer's pump until the admin call completes, but it cannot deadlock: each admin handle has its own runtime and its own dispatcher thread (`rust/src/ffi/admin.rs:227-277`, `spawn_dispatcher("kafka-admin-callback-dispatcher")`). The xmldoc says so.

### 3.5 Every completion-thread case

The ABI contract is at `rust/src/ffi/admin.rs:59-87`: exactly one callback per distinct key (or per batch), on one of three threads.

| Path | Thread | Sync flavour | Async flavour |
|---|---|---|---|
| Normal | The handle's dispatcher thread | `TrySet*` on the latch under its lock, then `PulseAll`. The waiter on the caller's thread wakes. | `TrySet*` on the TCS. Continuations go to the thread pool (`RunContinuationsAsynchronously`). |
| Pre-submit failure inside the ABI (bad input: unparseable base64 topic id, unknown `AlterConfigOp.OpType`) | **The caller's thread, before the `_async` entry point returns** | The latch exists before the submit (it is created with the operation), so it completes inline. No managed lock is held across the submit. The RPC returns a Result whose futures are already done. | Same. |
| Dispatcher died (a panic in an earlier callback) | A tokio worker thread | Same as normal. | Same as normal. |
| Managed failure before submit (marshalling throws in C#) | The caller's thread, in the `catch` block | `AbandonBeforeSubmit`, exactly as today. The code path is shared by both flavours. | Same. |

Rules carried over from the ABI text: publish everything the callback needs before the submit, and never hold a lock across `..._async(...)` that the callback re-acquires. The latch lock is taken only inside `TrySet*` and `Get()`.

### 3.6 Close and dispose

| Member | Sync flavour | Async flavour |
|---|---|---|
| `Close(TimeSpan)` | `void`. Validates a negative timeout (`ArgumentOutOfRangeException`, as today), runs the existing `TryBeginClose` latch, then calls the **existing sync** `NativeMethods.AdminClientClose(SafeAdminHandle, long)` (`NativeMethods.Admin.cs:131`), then `_handle.Dispose()`. | `Task`. Today's code: `close_async` via `OperationCompletionSource`. |
| `Dispose()` | Today's code: sync `AdminClientClose(_handle, UnsetTimeoutMs)`, then `_handle.Dispose()`. | Same as today. |
| `DisposeAsync()` | **Removed** (the sync types no longer implement `IAsyncDisposable`). | Same as today; swallows `KafkaException`. |

- The sync close passes the `SafeAdminHandle` as a P/Invoke parameter, so the marshaller holds a ref for the call (the "sync call takes a SafeHandle parameter" convention).
- No new DllImport. `AdminClientClose` is already declared and already used by `Dispose()` at `NativeAdminClient.cs:6519`.
- Close against outstanding work: the core waits up to the timeout for pending calls, then fails them (Java's `close(Duration)` contract). Their callbacks complete the latches, so a thread blocked in `Get()` wakes with the error. Outstanding operations keep their own span-the-op handle refs, so `_handle.Dispose()` defers the real release until they drop.
- An RPC called after close behaves exactly as today on both flavours.
- Calling sync `Close` or `Dispose` from async code blocks a pool thread for up to the timeout. That is the sync flavour's nature. The xmldoc points async callers to `IAsyncAdmin`.

---

## 4. Decisions

Each needs approval with the plan. "Recommended" is the Manager's lean.

| # | Decision | Recommended | Alternatives | Rationale |
|---|---|---|---|---|
| D1 | **Naming** | Sync takes the plain names (`IAdmin`, `KafkaAdminClient`, `MockAdminClient`, `CreateTopicsResult`). Async takes an `Async` prefix (`IAsyncAdmin`, `AsyncKafkaAdminClient`, `AsyncMockAdminClient`, `AsyncCreateTopicsResult`). | (b) `CreateTopicsResultAsync` suffix. (c) Nested types (`CreateTopicsResult.Async`). | Page Option 2, Python, and the consumer and producer twins all use the prefix. In .NET the `Async` suffix is for methods. |
| D2 | **What doubles** | Only the 44 Results, the two interfaces, the four clients and the two new non-generic futures. Options, specs and value types are shared. | (b) A generic `Result<TFuture>`. | C# cannot abstract over `KafkaFuture<T>` versus `AsyncKafkaFuture<T>` without higher-kinded types. A generic Result would leak a type parameter into every signature. |
| D3 | **Sync engine** | Sync RPCs call the same `_async` entry points. Each key gets a `SyncCompletion<T>` latch that the callback completes. The RPC returns at once. `Get()` blocks on the Monitor. | (b) The bare sync ABI entry points (they block until every key resolves). (c) A TCS plus `.GetAwaiter().GetResult()`. | (b) blocks inside the RPC, loses per-key timing, and breaks the Java shape. (c) is sync-over-async: `All()` and projections need a pool thread (§3.3), which is team decision 2's exact concern. |
| D4 | **Sync `Close`** | Call the existing sync `kafka_admin_AdminClient_close` directly. | (b) `close_async` plus a `SyncCompletion` latch. | `dotnet/CLAUDE.md` about 596-607: the sync ABI called directly is not sync-over-async. `Dispose()` already does this. (b) is also correct but adds code. Python's `_run_sync` over `close_async` is observably the same. |
| D5 | **No Task in the sync flavour** | No sync Result, `IAdmin` member or sync client member exposes `Task`, `ValueTask` or `AsyncKafkaFuture`. A reflection test enforces it (§7.3). | | Team decision 2. |
| D6 | **One core** | `NativeAdminClient` keeps one implementation per RPC, parameterised by `AdminFlavour`, returning the operation. Result wrapping is one internal factory per flavour per Result. | (b) Two cores. | Marshalling (7,028 lines) is the risky part. Writing it twice would double the review surface and invite drift. |
| D7 | **Per-key completion abstraction** | `IAdminCompletion<T>`, implemented by `SyncCompletion<T>` and `TaskAdminCompletion<T>`. `AdminCallbacks` unchanged. | (b) Separate operation classes per flavour. (c) TCS for both, and the sync side waits on the Task. | (b) duplicates 722 lines of countdown and rooting code. (c) allocates a Task per sync key and keeps the pool dependency for composites. |
| D8 | **Future types** | Add non-generic `KafkaFuture` and `AsyncKafkaFuture`. Add the internal `SyncFutureSource` hierarchy. Keep all composites internal. | (b) Use `KafkaFuture<bool>` for void keys. | Java's `KafkaFuture<Void>` has no value. `bool` would invent a meaningless value on the public surface. |
| D9 | **Composite semantics** | Inline memoized projection. All-of in key order, waiting for all. Memoized gathering all-of. A fail-fast group latch for ListTransactions only. The async flavour keeps today's Task composition. | (b) Eager projection on the completing thread. | Eager projection would run binding code on the dispatcher thread for every derived future, even ones nobody reads. Lazy and memoized is equivalent for pure functions and keeps the dispatcher thin. |
| D10 | **No D9 owner guard for admin** | Pass `owner: null`. | (b) A guard against `Get()` on the admin dispatcher thread. | No user code can run on that thread (§3.4), so (b) guards an unreachable case. |
| D11 | **No `ValueTask` on async RPCs** | Async RPCs return `Async*Result` directly, as today. | (b) `ValueTask<Async*Result>`, like the producer's `Send`. | The producer's `ValueTask` carries an admission stage that can block on a full buffer. An admin submit never blocks: it marshals and enqueues. Q4 confirms. |
| D12 | **Disposal** | `IAdmin : IDisposable`. `IAsyncAdmin : IAsyncDisposable, IDisposable`. | (b) Keep `IAsyncDisposable` on the sync side. | Mirrors `IProducer` / `IAsyncProducer`. An `await using` on a sync client would be sync-over-async in reverse. |
| D13 | **Mocks** | `MockAdminClient : IAdmin` and `AsyncMockAdminClient : IAsyncAdmin`, both over `NativeAdminClient.CreateMock(numBrokers)`. The five mock-only methods appear on both as one-line delegators, still synchronous. The Java-line remarks on unimplemented mock RPCs are copied to both. | (b) A shared internal base class. | Both classes are sealed delegators, so a base class saves only one-liners. Python's `_MockAdminClientMixin` is the same idea. |
| D14 | **Slice order: copy-then-convert** | Add the async flavour as new types first (S3). Then convert the existing types to sync in place, one family per commit (S4-S5). | (b) Rename everything to `Async*` first, then write sync from scratch. | Every commit stays green. The cref churn is spread across slices. The test migration can run in its own session. The temporary duplication ends at S5g. |
| D15 | **Test migration** | Existing admin tests move to the async flavour. They are `await`-based, and the compiler finds every site, because `AsyncKafkaFuture` is not awaitable. Sync coverage comes from new both-flavour theories and a per-RPC differential test (§7). | (b) Move existing tests to sync. | (b) would rewrite 856 awaits by hand. A mechanical move plus a differential test gives both flavours per-RPC coverage at lower cost. |
| D16 | **Harness** | The `dotnet` arm drives a new **sync** `AdminServiceImpl` that blocks with `.Get()` (the `ProducerServiceImpl` precedent). A new `dotnet-async` arm drives `AsyncAdminServiceImpl` (today's servicer, renamed). This supersedes M15/P12 D1. | (b) Keep the sync arm only. | The sync servicer blocks ASP.NET handler threads, which are pool threads. That is a real-world test of team decision 2. Python runs both arms. |
| D17 | **Callers** | The soak moves to `AsyncKafkaAdminClient`. PerfV3 `TopicProvisioning` moves to sync `KafkaAdminClient` with `.Get()`. | (b) The soak moves to sync, for Python soak parity. | The .NET soak is async end to end (`AsyncKafkaProducer`, `AsyncKafkaConsumer`). PerfV3 is sync code that today does `.GetAwaiter().GetResult()`, which is sync-over-async, so this removes it. |
| D18 | **Docs** | Future xmldocs and default-value messages become neutral between producer and admin. Java `KafkaFuture` mentions in sync admin xmldoc become `<see cref>` links to the new types. The stale "admin maps KafkaFuture to Task" remarks are replaced (supersedes M11/P4.2 D10's naming note). STATUS.md:80's stale-mention item is closed. | (b) Leave the 160 `<c>KafkaFuture&lt;Void&gt;</c>` mentions. | After P14 the Java name and the .NET name match, so the mentions become real references. |
| D19 | **Agents and Critic cadence** | `dotnet-actor` 95 and `dotnet-critic` 95. The Critic runs at three gates: after S4, after S5g, after S7. | (b) A Critic pass after every slice. | Each spawn costs about 230-310k tokens. The three gates sit where the pattern is set, where the conversion ends, and at close. Q6. |

---

## 5. Open questions

| # | Question | Options | Recommendation |
|---|---|---|---|
| **Q1** | **`Get(TimeSpan)` versus M11/P4.2 D7.** D7 ruled "no `Get(TimeSpan)` for now"; the user: "We will add one with cancellation token later if required" (follow-up FU-4). Java admin users often write `.get(30, SECONDS)`. | (a) Keep D7: `Get()` only, on both admin flavours and the producer. (b) Add `Get(TimeSpan)` to `KafkaFuture` / `KafkaFuture<T>` now; it throws `System.TimeoutException`, and it reaches the producer future too, because it is the same struct. (c) Add `Get(CancellationToken)` (FU-4 as written). | **(a).** Every admin operation is already bounded by its Options `TimeoutMs` or `default.api.timeout.ms`, so `Get()` cannot wait forever. Async users have `Task.WaitAsync` on net6+. (b) and (c) are additive later and would re-open a ruled producer decision. |
| Q2 | **Public `IsDone` on sync `KafkaFuture` / `KafkaFuture<T>`.** P4.2 deferred `IsDone` / `Cancel` / `IsCancelled` (P4.2 PLAN line 45). Without `Get(TimeSpan)`, `IsDone` is a sync user's only non-blocking check. | (a) Add `IsDone` only: a volatile read; P3.6 D6 still stands for `AsyncKafkaFuture`, whose Task already has `IsCompleted`. (b) Keep deferring. | **(a).** Java's `isDone()` is public and widely used with admin futures. It is one property and needs no new state. It is additive either way. |
| Q3 | **The 10 public Result constructors** (§2.2). | (a) Keep them public on both flavours, retyped, and add a public `KafkaFuture<T>.CompletedFuture(T)` (Java's `completedFuture`) plus an async analog, so users can build Results. (b) Make them internal now; re-adding later is additive. (c) Sync retyped plus the factory; the async side keeps `Task`-typed constructors. | **(b).** It adds no new public factory. `MockAdminClient` already covers user-side testing. Tests reach internals through `InternalsVisibleTo`. Choose (a) if mocking `IAdmin` with hand-built Results matters now; it is the more Java-faithful option. (c) gives the async Results a constructor shape that disagrees with their properties. |
| Q4 | **`ValueTask` on async RPCs.** The page's open footer comment is "Double check on `ValueTask`" (2026-10-09). | (a) No `ValueTask` (D11). (b) `ValueTask<Async*Result>`. | **(a)**, for the reason in D11. If the user picks (b), S3 changes and every async test gains one `await`. |
| Q5 | **User checkpoint after the S4 pilot?** | (a) Stop after S4 and its Critic pass, show the pattern, then continue. (b) Run S4 to S7 continuously. | **(a).** S4 fixes the pattern for 40 more RPCs. A stop there is cheap and a late pattern change is expensive. |
| Q6 | **Critic cadence** | (a) Three gates (D19). (b) After every slice (about 11 passes). | **(a).** |
| Q7 | **Rule-row wording** (§10). Some `dotnet/CLAUDE.md` rows are user-authored. | (a) Append dated amendments under the existing rows. (b) Rewrite the rows. | **(a)**, as M11/P4.2 did. The Actor applies only text the user has approved. |
| Q8 | **CI cost.** S6 adds 78 Docker-backed admin tests on the async image. | (a) Accept. (b) Run a subset on the async arm. | **(a).** Python's async arm runs all 78. A subset would hide flavour-specific failures. |

---

## 6. Slices

### 6.1 Rules for every slice

- One Actor session per slice, unless the slice says otherwise. The Actor commits at the end of each slice, with the gate evidence in the commit body: commands, pass counts per TFM, and the header SHA check.
- **Counts must add up.** After = before + the tests the slice added, named in the commit body. A drop is a fail. A zero is a fail. A "Test Run Aborted" is a fail, even with exit code 0 (trap 7). A libtest filter that matches zero tests is a fail (trap 6).
- Check the build output for `0 Error(s)` before any `--no-build` run (trap 9). Build and test with the same configuration (trap 8).
- Run the projects outside the solution whenever they touch admin types: grpc-server, the soak and PerfV3 (R3).
- The **Mode B gate** at every slice:
  - `/usr/bin/grep -c 'internal static extern' ${M}/dotnet/src/Confluent.Kafka/Internal/Interop/NativeMethods.cs ${M}/dotnet/src/Confluent.Kafka/Internal/Interop/NativeMethods.Admin.cs` prints 219 and 357 (576 in total);
  - `git -C ${M} diff --stat 2d73a746 -- rust/src rust/cbindgen.toml rust/build.rs` is empty;
  - `shasum ${M}/rust/target/include/confluent_kafka.h | cut -c1-12` prints `7c27280e1211`.

### 6.2 Commands

Every block starts with:

```sh
export PATH="/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin:$HOME/.cargo/bin:$HOME/.nix-profile/bin:$PATH"
M=/Users/pranavshah/WorkSpace/Confluent/example-confluent-kafka-rust
L=<your session scratchpad>
```

| Gate | Command | Pass signal |
|---|---|---|
| G1 build | `make -C ${M}/dotnet build-dotnet 2>&1 \| tee ${L}/build.log \| tail -5` | `0 Error(s)` and `0 Warning(s)` in `build.log` |
| G2 unit + soak | `make -C ${M}/dotnet test-dotnet 2>&1 \| tee ${L}/test.log \| /usr/bin/grep -E 'Passed!\|Failed!\|Test Run Aborted\|error' \| tail -8` | One `Passed!` line per TFM with the expected count for the unit tests (net8.0 and net10.0) and for the soak (165 per TFM); `Failed: 0`; no `Test Run Aborted` |
| G3 focused | `dotnet test -c Release -f net10.0 ${M}/dotnet/tests/Confluent.Kafka.UnitTests/Confluent.Kafka.UnitTests.csproj --filter 'FullyQualifiedName~<Class>' 2>&1 \| tail -5` | Non-zero `Passed`, `Failed: 0` |
| G4 grpc-server | `dotnet build -c Release ${M}/dotnet/grpc-server/Confluent.Kafka.GrpcServer.csproj 2>&1 \| tail -3` and `make -C ${M}/dotnet test-grpc-server-dotnet 2>&1 \| tail -5` | `0 Error(s)`; 101/101 (unchanged; S6 adds no grpc-server unit tests) |
| G5 PerfV3 | `dotnet build -c Release ${M}/dotnet/tests/Performance/PerfV3/PerfV3.csproj 2>&1 \| tail -3` and `make -C ${M}/dotnet perf-unit-test-dotnet 2>&1 \| tail -5` | `0 Error(s)`; perf unit tests pass |
| G6 native harness | `docker info >/dev/null 2>&1 && echo docker-up` first (do not assume Docker). Then `make -C ${M} test-integration-dotnet-native 2>&1 \| tee ${L}/native.log \| tail -15` | `test result: ok`, `0 failed`, passed count as expected (see S0 and S6) |
| G7 harness listing | `(cd ${M}/rust && cargo test --features integration-tests,multilanguage-tests --test integration -- --list 2>/dev/null) \| /usr/bin/grep -cE '__grpc_dotnet: test$'` and the same with `'__grpc_dotnet_async: test$'` | Matches the expected arm counts. Note that the filter `-- __grpc_dotnet` also matches `__grpc_dotnet_async`; count with the anchored patterns. |
| G8 Rust tidy (S6 only) | `(cd ${M}/rust && cargo xtask format-check 2>&1 \| tail -3)` and `(cd ${M}/rust && cargo xtask lint 2>&1 \| tail -3)` | Both clean |
| G9 Mode B | §6.1 | 576 externs; empty diff; SHA `7c27280e1211` |

### 6.3 The slices

| Slice | Content | Gates | Expected counts |
|---|---|---|---|
| **S0** Baseline (no commit) | Run G1, G2, G4, G5, G7, G9, and G6 if Docker is up. Record the counts in the S1 commit body and in agent memory, **not** in PLAN.md. | All | Unit 3010/3010 per TFM (net8.0, net10.0); soak 165; grpc-server 101; native harness 169 (`dotnet` 124, of which 78 are admin; `dotnet_async` 45). Re-derive each one; if any differs from this table, stop and report. |
| **S1** Futures | `SyncFutureSource` / `SyncFutureSource<T>`; `SyncCompletion<T>` derives from it with unchanged behaviour; `KafkaFuture<T>` field widens; new non-generic `KafkaFuture` and `AsyncKafkaFuture`; internal projection, all-of, gathering all-of; neutral xmldoc and default-value messages; a public `IsDone` on both sync futures (Q2). Unit tests for every composite. | G1, G2, G9; the producer allocation tests (A1, 272 B) and the D9 guard tests by name via G3 | Unit = 3010 + the new future tests (estimate 30-45), per TFM. |
| **S2** Core seam (internal only) | `IAdminCompletion<T>`, `TaskAdminCompletion<T>`, `AdminFlavour`; operations take a flavour; `NativeAdminClient` RPCs return operations; every existing Result gets an internal `From(op)` over `op.Tasks`; clients wrap. **Zero public change.** New Interop tests: a sync-flavour operation completes its latches on the inline path (via the submit seam), on the dispatcher path, and from a foreign thread standing in for the tokio worker. | G1, G2, G4, G9 | Unit = S1 + the new seam tests (estimate 10-20). |
| **S3a** Async flavour | `IAsyncAdmin`, `AsyncKafkaAdminClient`, `AsyncMockAdminClient`, 44 `Async*Result` (today's Results with `Task` replaced by `AsyncKafkaFuture` at the public boundary, built by `From(op)`). The grpc `AdminServiceImpl` and `TranslateAdmin` move to `IAsyncAdmin` (`await x.Get()`). The soak moves to `AsyncKafkaAdminClient` (code and the doc mentions at `SoakConfig.cs:32,40`, `SoakClient.csproj:7`, `soak/README.md:415`). The surface-parity reflection test (§7.3). `IAdmin` is untouched. | G1, G2, G4, G6, G9 | Unit = S2 + the surface tests (estimate 3-6). Native harness 169 (the `dotnet` admin arms now run on the async types). |
| **S3b** Test migration (one or two sessions) | Move the admin tests to the async types: the 57 `PublicAdmin*` files and 47 `Interop/Admin*` files (266 `new MockAdminClient` sites, 19 `new KafkaAdminClient` sites, 856 awaits). Mechanical; the compiler finds each site. Split by file group if the session runs long: `PublicAdmin*` first, then `Interop/Admin*`. | G1, G2, G9 | Unit count **equal** to S3a. Equality is the gate: a moved test is not a lost test. |
| **S4** Pilot: topics, sync in place | Convert the topic family on `IAdmin` / `KafkaAdminClient` / `MockAdminClient` to sync: `CreateTopics`, `DeleteTopics` x2, `DescribeTopics` x2, `ListTopics`, `CreatePartitions`, `DeleteRecords` (8 methods, 6 Results). CreateTopics first. Add the sync parity theories for these 6 Result pairs, the differential rows for these 8 methods, the starvation regression test (§7.4), and the sync inline-failure test. PerfV3 `TopicProvisioning.cs:59-86` moves to `.Get()`. **Critic pass 1.** **User checkpoint (Q5): stop here and report before S5a.** | G1, G2, G4, G5, G9; G3 on the new classes | Unit = S3b + the new rows (estimate 60-90). |
| **S5a** Cluster, configs, log dirs | `DescribeCluster`, `ListConfigResources`, `DescribeConfigs`, `IncrementalAlterConfigs` (`AlterConfigsResult`), `DescribeLogDirs`, `AlterReplicaLogDirs`, `DescribeReplicaLogDirs` (7 methods, 7 Results). Parity and differential rows. | G1, G2, G4, G9 | + rows |
| **S5b** Elections, reassignments, offsets | `ElectLeaders`, `AlterPartitionReassignments`, `ListPartitionReassignments` x2, `ListOffsets` (5 methods, 4 Results). | same | + rows |
| **S5c** Groups and group offsets | `ListGroups`, `DescribeConsumerGroups`, `DescribeClassicGroups`, `ListConsumerGroupOffsets` x2, `DeleteConsumerGroups`, `RemoveMembersFromConsumerGroup`, `AlterConsumerGroupOffsets`, `DeleteConsumerGroupOffsets` (9 methods, 8 Results). The largest family: gathering all-ofs and in-future guards. | same | + rows |
| **S5d** ACLs and quotas | `CreateAcls`, `DeleteAcls`, `DescribeAcls`, `DescribeClientQuotas`, `AlterClientQuotas` (5 methods, 5 Results). | same | + rows |
| **S5e** SCRAM, tokens, features | `DescribeUserScramCredentials`, `AlterUserScramCredentials`, `CreateDelegationToken`, `RenewDelegationToken`, `ExpireDelegationToken`, `DescribeDelegationToken`, `DescribeFeatures`, `UpdateFeatures` (8 methods, 8 Results). | same | + rows |
| **S5f** Producers and transactions | `FenceProducers`, `DescribeTransactions`, `DescribeProducers`, `ListTransactions`, `AbortTransaction`, `ForceTerminateTransaction` (`TerminateTransactionResult`) (6 methods, 6 Results). The fail-fast group latch (§3.3) and its timing rows. | same | + rows |
| **S5g** Close, dispose, cleanup | Sync `void Close(TimeSpan)` over the sync close ABI; drop `IAsyncDisposable` from `IAdmin`, `KafkaAdminClient`, `MockAdminClient`; delete the dead Task-based `From(op)` factories on the sync Results; rewrite the `IAdmin.cs:31-41` remarks. Close and dispose tests (§7.6). Duplication check (DoD #6). **Critic pass 2.** | G1, G2, G4, G5, G6, G9 | + close/dispose rows; native harness still 169 |
| **S6** Harness | `git mv` `AdminServiceImpl.cs` to `AsyncAdminServiceImpl.cs` (class renamed). A new sync `AdminServiceImpl.cs`. Sync helper overloads in `TranslateAdmin.cs`. `Program.cs` registers `AsyncAdminServiceImpl` in the async branch and `AdminServiceImpl` in the sync branch, with comments updated (lines 31, 41, 107, 122-125). Rust harness: `impl AdminBackendFactory for DotnetAsyncGrpcFactory` (label `"dotnet_async"`, mirroring `PythonAsyncGrpcFactory` at `backend_factory.rs:410-425`), the `__grpc_dotnet_async` macro arm, and the comments at `backend_factory.rs:637-650`, `multilanguage_admin_test_macro.rs:20-24` and `backend_pool.rs:116-118`. | G1, G4, G6, G7, G8, G9 | grpc-server 101; G7 `__grpc_dotnet` 124 and `__grpc_dotnet_async` 45 + 78 = 123; native harness 169 + 78 = **247** (re-derive from S0's listing) |
| **S7** Docs and close | Apply the a/b rule (§0, approved verbatim) and the other §10 rows whose exact text the user has approved. The xmldoc sweep (D18). The STATUS close-out entry: supersessions, evidence, the Q3 parity deviation, FU-5, next N = 96. Update `design/current`. Manager step 7: copy `COMMENTS.DONE.95.md` into this folder and reset `COMMENTS.95.md`. **Critic pass 3.** | G1, G2, G9 | No count change |

About 11 Actor sessions (S1, S2, S3a, S3b x1-2, S4, S5a-S5f grouped as 3-4 sessions, S5g, S6, S7) and 3 Critic passes, plus fix cycles. About 17-19 commits.

---

## 7. Test strategy

### 7.1 Principles

- **Production wrapping only (DoD #12).** Every parity test builds Results through the same internal `From(op)` factories the clients use, over operations whose completions the test completes by hand. No hand-built Result stands in for a production one.
- **Bounded waits.** Every sync `Get()` in a test runs on a dedicated `new Thread(...)` joined with a timeout, so a hang fails the test instead of hanging the run.
- **Exact messages.** Asserted, not just exception types.

### 7.2 Both-flavour composition theories

One `[Theory]` per Result pair. Each row drives both flavours with the same inputs and compares the outcomes: same value, or same exception type, message and timing (at the call versus at `Get` / `await`).

| Row | What it pins |
|---|---|
| All keys succeed | Values and the gathered dictionary are equal |
| One key fails | Only that key fails; `All()` fails with that error |
| Two keys fail | `All()` surfaces the **first in key order** on both flavours |
| All keys fail | Same |
| Unknown key | Same exception, same timing, same message |
| Null key | Same exception, same timing |
| Per-key independence | Completing key A leaves key B pending on both flavours (sync: `IsDone` is false; async: `IsCompleted` is false on the Task) |
| Memoization (sync) | Two `Get()` calls return the same value object or rethrow the same exception instance |
| Erasure identity (sync) | A void key's `KafkaFuture` completes exactly when its typed source does |
| ListTransactions fail-fast | Broker 2 fails while broker 1 is pending: `AllByBrokerId()` fails **before** broker 1 completes, on both flavours |

### 7.3 Surface tests (reflection)

- `IAdmin` and `IAsyncAdmin` have the same 48 method names, with the same parameter lists. Each return type name differs only by the `Async` prefix.
- No member of `IAdmin`, the two sync clients or the 44 sync Results exposes `Task`, `ValueTask`, `AsyncKafkaFuture` or `AsyncKafkaFuture<T>`, at any depth of generic arguments.
- `IAdmin` implements only `IDisposable`. `IAsyncAdmin` implements `IAsyncDisposable` and `IDisposable`.
- The four clients are sealed.
- The five mock-only methods exist on both mocks with the same signatures.

### 7.4 Starvation regression test (team decision 2)

- Lives in its own `[CollectionDefinition(Name, DisableParallelization = true)]` collection. xunit runs such a collection on its own, after the parallel collections (precedent: `PublicProducerFirstStageTests.cs:37`).
- Setup: `ThreadPool.SetMinThreads` and `SetMaxThreads` to `Environment.ProcessorCount` (min first); queue that many work items that block on a gate; confirm saturation with a probe item that stays pending.
- **Sync row:** on a dedicated `new Thread`, a `MockAdminClient` `CreateTopics(...).All().Get()` and a projection (`NumPartitions(t).Get()`) both return within the timeout.
- **Control row:** the async flavour's `All().Get().Wait(timeout)` does **not** complete while the pool is saturated. This proves the test can see starvation.
- **Close row (S5g):** sync `Close(TimeSpan)` completes under the same saturation.
- `finally`: open the gate, then restore the original min and max.

### 7.5 Per-RPC differential test

- One row per RPC (48 rows). The same `MockAdminClient` state drives the sync call and the async call.
- Assert equal values, or equal exception type and message. Unimplemented mock RPCs must fail the same way on both, with the "Not implemented yet" text.
- This catches wrong overload delegation and a missed conversion.

### 7.6 Sync-only tests

- **Inline failure:** drive the inline ABI path through the existing submit seam, with a delegate that fires the callback before it returns. The Result comes back with its futures already done, and `Get()` throws the error at once. The typed .NET API may not reach the ABI's bad-input cases (a `Uuid` always encodes cleanly). If a public input does reach one, for example an out-of-range `AlterConfigOp` op-type cast, add that row too.
- **Close and dispose:** sync `Close(TimeSpan)` with a negative timeout throws `ArgumentOutOfRangeException`; `Close` while another thread is blocked in `Get()` wakes it with the error; `Dispose` is idempotent; RPC after close behaves as today.
- **Default values:** `default(KafkaFuture).Get()` and `default(KafkaFuture<T>).Get()` throw `InvalidOperationException` with the new message; the same for the async types.

### 7.7 Mutation checks

The Actor applies each mutation with Edit, runs the named test, and reverts with Edit. Never `git stash`. Afterwards, `git -C ${M} diff --stat` must show no change to that file. The commit body lists each mutation and the test that caught it.

| # | Mutation | Must be caught by |
|---|---|---|
| M1 | Sync all-of returns after the first key | "Two keys fail" / "one key fails" rows |
| M2 | A sync projection runs via `Task.Run` | Starvation sync row; reflection test if a Task leaks |
| M3 | Erasure creates a new completion instead of wrapping the source | Erasure-identity row |
| M4 | Unknown-key guard removed | Unknown-key row |
| M5 | `TaskAdminCompletion` without `RunContinuationsAsynchronously` | A test that an async continuation does not run on the callback thread (add one in S2 if none exists) |
| M6 | A sync overload delegates to the wrong native overload (for example `DescribeTopics(names)` to the ids path) | Differential row |
| M7 | `default(KafkaFuture).Get()` does not throw | Default-value test |
| M8 | Projection not memoized | Memoization row |
| M9 | `AllByBrokerId` waits in key order instead of fail-fast | ListTransactions fail-fast row |
| M10 | The latch is created after the submit | Inline-failure test (bounded wait fails) |
| M11 | Sync `Close` built as `close_async` plus `.GetAwaiter().GetResult()` | Starvation close row |
| M12 | All-of rethrows the last failure, not the first in key order | "Two keys fail" row |

### 7.8 Existing tests

- 57 `PublicAdmin*` files (19,662 lines, 653 `[Fact]` / `[Theory]`, 580 awaits) and 47 `Interop/Admin*` files (24,904 lines, 524 `[Fact]` / `[Theory]`, 276 awaits).
- They move to the async types in S3b (D15). Their assertions do not change.
- Tests that assert the old single-interface shape (`IAdmin : IAsyncDisposable`, Task return types) are **rewritten** for the new shape in the slice that changes it, never deleted.
- The 45 public-constructor call sites (`PublicAdminTopicResultConstructionTests` 19, `PublicAdminDescribeClassicGroupsResultTests` 8, `PublicAdminDescribeConsumerGroupsResultTests` 8, `PublicAdminParityLowsTests` 6, `AdminP5ResultMarshalTests` 2, `AdminP6ResultMarshalTests` 1, `PublicAdminP2bShapeParityTests` 1) follow Q3 (b). They call the internal constructors through `InternalsVisibleTo`. A test that asserts a constructor is public is rewritten to assert that it is internal.

---

## 8. Harness

| Item | Change | Slice |
|---|---|---|
| `dotnet/grpc-server/AdminServiceImpl.cs` (2,178 lines, 65 awaits) | Moves to `IAsyncAdmin` in S3a. Renamed to `AsyncAdminServiceImpl` in S6. | S3a, S6 |
| New sync `AdminServiceImpl.cs` | Every handler blocks with `.Get()` on the handler thread and returns `Task.FromResult(response)`. No `Task.Run`. Error translation is unchanged. | S6 |
| `TranslateAdmin.cs` (1,634 lines) | The helpers at lines 129, 145 and 162 (`Resolve<TValue>`, `ResolveVoid`, `VoidResponse<TKey>`) take `AsyncKafkaFuture` from S3a. S6 adds sync overloads over `KafkaFuture`. | S3a, S6 |
| `Program.cs` (320 lines) | Async branch: `AddSingleton<AsyncAdminServiceImpl>()` and `MapGrpcService<AsyncAdminServiceImpl>()`. Sync branch: unchanged registration. Comments at lines 31, 41, 107 and 122-125. | S6 |
| `dotnet/Dockerfile.grpc.async` | No change. The image runs the same server with `CONSUMER_FLAVOR=async`. | |
| `rust/tests/common/backend_factory.rs` | `impl AdminBackendFactory for DotnetAsyncGrpcFactory`, label `"dotnet_async"`. Comment at 637-650. | S6 |
| `rust/tests/common/multilanguage_admin_test_macro.rs` | A `__grpc_dotnet_async` arm after `__grpc_dotnet`: `BackendKind::DotnetAsync` and `DotnetAsyncGrpcFactory`, mirroring the consumer macro. Module doc at 20-24. | S6 |
| `rust/tests/common/backend_pool.rs` | The `DotnetAsync` doc comment (116-118) now says it serves `AdminService`. | S6 |
| `rust/Makefile:228-239` | No change. The `-- __grpc_dotnet` filter already matches the new arm. `DOTNET_GRPC_SKIPS` (three producer transaction tests) is unchanged. | |
| Python harness | No change. | |

**Local gates:** G6 runs the native harness on macOS with Docker brokers; check Docker first, do not assume it. **CI-only gates:** the Linux container `make test-integration-dotnet` (it skips itself on macOS), the SSL and SASL-SSL variants, the `grpc-image-async` Docker image build, and net462 test runs (not run locally).

---

## 9. Migration of existing callers

| Caller | Today | After | Slice |
|---|---|---|---|
| `dotnet/soak/SoakClient/SoakClient.cs:1455-1477, 1520` | `new KafkaAdminClient`, `await result.Values[topic]` | `new AsyncKafkaAdminClient`, `await result.Values[topic].Get()` | S3a |
| `dotnet/soak/SoakClient/SoakConfig.cs:32, 40`, `SoakClient.csproj:7`, `soak/README.md:415` | Mention `KafkaAdminClient` | `AsyncKafkaAdminClient` | S3a |
| `dotnet/tests/Performance/PerfV3/TopicProvisioning.cs:59-86` | `KafkaAdminClient` with `.GetAwaiter().GetResult()` (sync-over-async) | Sync `KafkaAdminClient` with `.Get()` | S4 |
| `dotnet/grpc-server/*` | §8 | §8 | S3a, S6 |
| Unit tests | §7.8 | §7.8 | S3b onwards |
| PerfV2 | Uses confluent-kafka-dotnet | Unaffected | |
| Python, C, Rust core | | Unaffected | |

---

## 10. Rule-file changes (listed, not applied)

Q7 is ruled: each change is a dated amendment appended under the existing row. The a/b rule (§0) is approved verbatim. The rows below are approved in principle only, so the Actor applies one only after the user has seen and approved its exact text.

| File | Where | Change |
|---|---|---|
| `dotnet/CLAUDE.md` | §4 "Sync vs async — the governing rule", under the table (about 557-565) | **The a/b rule, approved verbatim (§0).** Its only home. At most a one-line pointer from line 487. |
| `dotnet/CLAUDE.md` | 59-61 and 431-433 | The admin client is exposed as `IAdmin` (sync: `KafkaAdminClient` / `MockAdminClient`) and `IAsyncAdmin` (async: `AsyncKafkaAdminClient` / `AsyncMockAdminClient`), M15/P14. |
| `dotnet/CLAUDE.md` | 487, the idiom row "Java's `org.apache.kafka.common.KafkaFuture` is the Admin type, mapped to `Task<T>`" | Amend: since M15/P14 it maps to `KafkaFuture<T>` in sync admin Results and `AsyncKafkaFuture<T>` in async admin Results. Supersedes the M11/P4.2 D10 naming note. |
| `dotnet/CLAUDE.md` | 561, the "Returns `Future<T>`" row | Add the admin futures. |
| `dotnet/CLAUDE.md` | 533, the Disposal row | Add `IAdmin : IDisposable` and `IAsyncAdmin : IAsyncDisposable, IDisposable`. |
| `dotnet/.claude/rules/ffi-marshalling.md` | §B6 about 1997-2040, shapes 4a/4b ("one `Task` per key") | "One completion per key: a `TaskCompletionSource` with `RunContinuationsAsynchronously` on the async flavour, or a `SyncCompletion` latch on the sync flavour." |
| `dotnet/.claude/rules/ffi-marshalling.md` | §B7 anti-patterns (from 2288) | Add: "A sync admin Result composed over Tasks (`WhenAll`, an `async` projection) and then blocked on. It needs a pool thread. Compose over `SyncFutureSource` instead." |
| `dotnet/.claude/rules/bindings.md` | The Java Future to host idiom line | Optional: a binding may offer both a blocking and an awaitable future type; .NET does (`KafkaFuture` / `AsyncKafkaFuture`). |
| Root `.claude/rules/admin-client.md` | §1, §11 | **Not touched.** §1 (RPCs return at once, only close blocks) already holds for both flavours. §11 is a stale Rust-side caveat outside this phase. |
| `dotnet/design/current/STATUS.md` | Close-out entry | Records the supersession of M15 D2 (lines 659, 665) and M15/P12 D1 (line 435), and closes the line 80 stale-mention item. |
| `dotnet/design/current/PLAN-M15-admin-client.md` (untracked) | §D2, §5.3 | Optional "superseded by M15/P14" note. It is untracked, so nothing is committed. |

---

## 11. Risks and sizing

### 11.1 Risks

| # | Risk | Mitigation |
|---|---|---|
| R1 | ListTransactions is the hardest RPC: two stages, a nested future, and fail-fast timing. | Its own slice (S5f), the group latch design in §3.3, and the timing row (M9). |
| R2 | Context budget in S3b (104 files) and S5c (9 methods with gathering all-ofs). | Split S3b by file group. One family per session. Give the Actor `wc -l` numbers and ranged reads (§13). |
| R3 | grpc-server, the soak and PerfV3 are outside `Confluent.Kafka.sln`, so `build-dotnet` does not build them. | G4 and G5 at every slice that touches admin types. |
| R4 | Sync `Close` / `Dispose` block the calling thread for up to the timeout, including a pool thread when called from async code. | Documented; async callers use `IAsyncAdmin`. This is pre-existing for `Dispose`. |
| R5 | The starvation test changes process-wide thread-pool limits and could make other tests flaky. | Its own non-parallel collection, restore in `finally`, bounded waits, and a control row. If it flakes in CI, mark it local-only with a recorded reason rather than weakening it. |
| R6 | Temporary duplication between S3a and S5g (DoD #6). | Justified by copy-then-convert (D14). S5g removes the dead code; the Critic checks it at pass 2. |
| R7 | netstandard2.0 lacks some APIs (non-generic `TaskCompletionSource`, `Task.WaitAsync`). | Use `TaskCompletionSource<bool>`. The sync side uses only `Monitor`. |
| R8 | Every existing .NET admin user breaks: `KafkaAdminClient` changes from Task-based to sync. | Allowed before 1.0. The break is loud: `await` on a `KafkaFuture` does not compile. Recorded in STATUS. |
| R9 | Thread-pool starvation in the sync gRPC servicer under the harness. | Sync `Get()` needs no pool thread, so blocked handler threads cannot starve the completions. The harness run is the evidence. |
| R10 | CI time grows by 78 Docker-backed tests. | Q8. |
| R11 | Xmldoc duplication between the sync and async types. | The async xmldoc points to the sync type for shared semantics, with `<inheritdoc>` where it fits. |
| R12 | The package-name (`Confluent.Kafka` versus `Confluent.Kafka4`) and Java-namespace questions on the design page are open elsewhere. | Out of scope here; nothing in this plan depends on them. |

### 11.2 Sizing

| Area | Added | Modified |
|---|---|---|
| Futures and composites | ~600 | ~150 |
| Core seam (operations, `NativeAdminClient` return sites, factories) | ~500 | ~1,500 |
| Async types (interface, two clients, 44 Results) | ~6,500 | |
| Sync conversion (44 Results, interface, two clients) | | ~5,500 |
| Tests: new theories, differential, surface, starvation, close | ~3,500 | |
| Tests: migration | | ~3,000 (mechanical) |
| gRPC server (sync servicer, helpers) | ~2,300 | ~200 |
| Rust harness | ~60 | ~20 |
| Docs and rules | ~150 | ~300 |
| **Total** | **~13,500** | **~10,500** |

About 7 slices (S0-S7, with S3 and S5 split into sub-slices), about 17-19 commits, about 11 Actor sessions and 3 Critic passes.

---

## 12. Mode B (Rust, ABI, core)

**No Rust core, ABI or header change is needed.**

- The sync flavour uses the existing `_async` entry points plus a managed latch.
- Sync `Close` uses the existing `kafka_admin_AdminClient_close`, already declared at `NativeMethods.Admin.cs:131`.
- The extern count stays 576 (219 + 357). The header SHA stays `7c27280e1211`. Gate G9 checks both at every slice.

**Flagged: S6 edits three files in the Rust tree.** `rust/tests/common/backend_factory.rs`, `multilanguage_admin_test_macro.rs` and `backend_pool.rs` are test-harness files, not core or ABI. They still need `cargo xtask format-check` and `cargo xtask lint` (G8). M15/P12 edited the same files, so this is precedented.

---

## 13. Environment notes for every brief

### 13.1 Shell traps

1. PATH may be broken. Export `PATH="/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin:$HOME/.cargo/bin:$HOME/.nix-profile/bin:$PATH"` first. `command -v` is unreliable.
2. `grep` may be aliased to ugrep. Use `/usr/bin/grep`.
3. Prefer `awk` to `sed`. If `sed` is GNU, `sed -i ''` fails. BSD sed has also been seen in the Manager shell. Use the Edit tool for edits.
4. `cat` may be shadowed by `bat`. Use `/bin/cat`, or the Write tool.
5. zsh: an unquoted `$var` is not word-split; an unquoted glob with no match aborts with "no matches found"; `path` is an array tied to `PATH`; `"$B:rust"` applies the `:r` modifier (write `"${B}:rust"`); a word starting with `=` expands to a command path, so quote it (`echo '====='`).
6. A libtest filter that matches zero tests exits 0 with "0 passed". Check the count.
7. An aborted `dotnet test` exits 0. "Test Run Aborted" is the only signal.
8. A `-c Release` build followed by a `--no-build` test run picks up the Debug binaries. Match the configurations.
9. A failed build followed by `--no-build` prints a bogus "Passed!". Check for `0 Error(s)` first.
10. The Rust FFI sits behind `--features ffi`.

### 13.2 Git and tree rules

- Main tree: `/Users/pranavshah/WorkSpace/Confluent/example-confluent-kafka-rust`, branch `prashah_dev_dotnet_binding`. Use absolute paths and `git -C`.
- **Never read from or edit** `.claude/worktrees/send-lanes-poc` or `.claude/worktrees/send-approach-2-poc`.
- No bare `cd`. Use `(cd x && ...)` subshells.
- Never `git stash`. No push, no branch switch, no checkout. Chain with `&&`; on a failed step, stop and report.
- Untracked notes and agent memory are normal in this tree. Do not add or delete them.

### 13.3 File sizes (read ranges, not whole files)

| File | Lines |
|---|---|
| `dotnet/src/Confluent.Kafka/Internal/NativeAdminClient.cs` | 7,028 |
| `dotnet/src/Confluent.Kafka/Internal/Interop/AdminCallbacks.cs` | 3,712 |
| `dotnet/src/Confluent.Kafka/Internal/Interop/NativeMethods.Admin.cs` | 3,568 |
| `dotnet/src/Confluent.Kafka/Admin/IAdmin.cs` | 1,488 |
| `dotnet/src/Confluent.Kafka/Internal/AdminOperation.cs` | 521 |
| `dotnet/src/Confluent.Kafka/Admin/MockAdminClient.cs` | 490 |
| `dotnet/src/Confluent.Kafka/Admin/KafkaAdminClient.cs` | 332 |
| `dotnet/src/Confluent.Kafka/Internal/ListTransactionsAdminOperation.cs` | 201 |
| `dotnet/src/Confluent.Kafka/Internal/SyncCompletion.cs` | 161 |
| `dotnet/src/Confluent.Kafka/AsyncKafkaFuture.cs` | 119 |
| `dotnet/src/Confluent.Kafka/KafkaFuture.cs` | 96 |
| `dotnet/src/Confluent.Kafka/Admin/*Result.cs` (44 files) | 4,898 |
| `dotnet/src/Confluent.Kafka/Admin/*Options.cs` (44 files) | 2,336 |
| `dotnet/grpc-server/AdminServiceImpl.cs` | 2,178 |
| `dotnet/grpc-server/TranslateAdmin.cs` | 1,634 |
| `dotnet/grpc-server/Program.cs` | 320 |
| `dotnet/CLAUDE.md` | 1,090 |
| `dotnet/.claude/rules/ffi-marshalling.md` | 2,413 |
| `python/admin.py` | 5,040 |

### 13.4 Do not read whole

- ⛔ `rust/target/include/confluent_kafka.h` (17,086 lines): grep only.
- ⛔ `rust/src/ffi/admin.rs` (40,670 lines): the module doc is lines 15-125; close is about 705-735; `close_async` is about 737-790.
- ⛔ `NativeAdminClient.cs`, `AdminCallbacks.cs` and `NativeMethods.Admin.cs` in one read: grep for the RPC, then read a range.
- ⛔ The POC worktrees: never.
