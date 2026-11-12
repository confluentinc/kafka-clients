# Plan: Java's shape for the admin factory, the records, five Results and the listeners (M23/P5)

Status: **approved 2026-11-12** (Pranav, main session: "I approve the plan with PM leans."; then "1. (b)" for D23,
"2. apply R1-R5" for D25, "3. Yes go ahead for the loop"). Every decision D1–D25 as the lean; D23 is option (b),
rebase onto `803c3b8a` now; D25 applies R1–R5 in S10, and R6 is dropped because D14 is `conf`. Ledger rows 502–526
Agreed. Manager N=126 (M23/P3 was 124, M23/P4 is 125). Branch `prashah_dev_dotnet_java_shape_gaps`, on M23/P4's squash
`803c3b8a6` (one commit on `master` `9152803b8`). Mode A: .NET only. No Rust, C ABI, header, Python or proto change.
Line numbers in `dotnet/` were re-read at `803c3b8a6`; Java and Python ones at the same commit.

Path prefixes: `J/` = `kafka/clients/src/main/java/org/apache/kafka/clients/`, `P/` = `python/confluent_kafka/`,
`N/` = `dotnet/src/Confluent.Kafka/`, `NT/` = `dotnet/tests/Confluent.Kafka.UnitTests/`, `D/` = `dotnet/`.
`B<n>` = line `n` of `D/analyzers/java-name-baseline.txt` at `803c3b8a6` (423 lines; once a step removes lines, match
the later ones by their text).

## 1. Summary

The .NET binding still differs from Java's public shape in five places. (1) Users build an admin client with
`new KafkaAdminClient(configs)`; Java and Python use the static factory `Admin.create`. (2) `ProducerRecord`,
`OffsetAndMetadata` and `ConsumerRecord` do not have Java's constructors. `ProducerRecord` even takes the value
before the key, so a Java-style call `new ProducerRecord<string, string>("t", "k", "v")` compiles today and swaps
key and value without a word. (3) Five admin Result classes hide the public constructor Java has. (4)
`ConsumerRecords` is a flat list: it has no `Records(partition)`, `Partitions()`, `IsEmpty()` or `EMPTY`, and it
throws away the core's next offsets, so `NextOffsets()` cannot exist. `ConsumerRecord` has no `DeliveryCount()`.
(5) The rebalance listeners keep Java's `onPartitionsLost` default in two base classes, not in the rule-10 shape
(`dotnet/CLAUDE.md` §3 rule 10). This phase fixes all five, in nine small commits plus a close-out. The C functions
are already in the bindings header; only one new P/Invoke import is needed (`next_offsets`).

| # | Item | What changes | Breaks users? |
|---|---|---|---|
| C2 | `Admin.Create` | `Admin.Create` / `AsyncAdmin.Create` (C# 14: `IAdmin.Create`); the constructors become `internal` | **Yes** |
| C4 | Record constructors | Java's six `ProducerRecord`, three `OffsetAndMetadata`, three public `ConsumerRecord` constructors | **Yes** (one case silently) |
| G4d | Five Result constructors | public, as Java's | No |
| G4e | `ConsumerRecords` and `DeliveryCount()` | Java's members; next offsets read from the core | No |
| R10 | Rebalance listeners | rule 10: `…WithPartitionsLost` interfaces and extension methods; the two base classes go | **Yes** |

## 2. C# ideas this plan uses (one line each)

- **`internal`**: visible only inside the `Confluent.Kafka` assembly (and to its test projects). Rust: `pub(crate)`.
- **Optional parameter** (`TKey? key = default`): the caller may leave it out and the compiler fills the default.
  Python: a keyword default. Rust has none.
- **Named argument** (`partition: 0`): binds to the parameter by name, in any order. Python: a keyword argument.
- **Overloads**: several constructors with one name and different parameter lists; the compiler picks one from the
  arguments. Rust has none; Python emulates them with `@overload` stubs plus `java_forms`.
- **Extension method**: a static method whose first parameter has `this`; you call it as if it were an instance
  method, `listener.OnPartitionsLost(p)`. Rust: like a blanket `impl<T: Listener> ListenerExt for T`.
- **`extension(IAdmin) { … }` block** (C# 14): adds members to another type. A `static` member in it lets a C# 14
  caller write `IAdmin.Create(…)`; the compiler also emits it as a plain static method of the enclosing class, so an
  older C# caller writes `Admin.Create(…)`. It compiles for netstandard2.0 (checked with SDK 10.0.400).
- **Static field of a generic class**: `ConsumerRecords<string, string>.EMPTY` and
  `ConsumerRecords<byte[], byte[]>.EMPTY` are two different objects, one per pair of type arguments. Java has one
  object for all. Rust has no generic statics; Python: a class attribute set on each concrete subclass.

## 3. Decisions for Pranav

Answer by number, e.g. "D1–D13: yes". Each row says what Java and Python do first.

### 3.1 Group A — follow Java, with Python as the tie-breaker (D1–D13)

| # | Topic | Java | Python | .NET today | Proposed |
|---|---|---|---|---|---|
| D1 | Admin client constructors | `private KafkaAdminClient(…)` (`J/admin/KafkaAdminClient.java:667`) | `KafkaAdminClient()` raises `TypeError("… use Admin.create(...)")` (`P/admin/kafka_admin_client.py:50`) | public `KafkaAdminClient(configs)` (`N/Admin/KafkaAdminClient.cs:52`), `AsyncKafkaAdminClient(configs)` (`AsyncKafkaAdminClient.cs:55`) | `internal`; both classes stay public and sealed |
| D2 | Async factory | none (one client) | `AsyncAdmin.create(*, configs)` (`P/admin/async_admin.py:298`) | none | static class `AsyncAdmin` with `extension(IAsyncAdmin) { static Create(…) }`. The analyzer already looks for this name (B138) |
| D3 | Factory return type | `Admin` | `Admin` / `AsyncAdmin` | — | `IAdmin` / `IAsyncAdmin` |
| D4 | `ProducerRecord` constructors | six, key before value (`J/producer/ProducerRecord.java:71,98,111,123,134,144`) | exactly Java's six forms (`P/producer/producer_record.py:63-70`) | one: `(topic, value, key = default, partition = null, timestamp = null, headers = null)` (`N/Producer/ProducerRecord.cs:75`) | Java's six, Java's order, no optional parameters. Messages and check order stay |
| D5 | `OffsetAndMetadata` constructors | `(offset, Optional leaderEpoch, metadata)`, `(offset, metadata)`, `(offset)` (`J/consumer/OffsetAndMetadata.java:50,67,76`) | the same three (`P/consumer/offset_and_metadata.py:62-65`) | one: `(offset, metadata = null, leaderEpoch = null)` (`N/Consumer/OffsetAndMetadata.cs:49`) | Java's three; `int?` for the `Optional`. "Invalid negative offset" and "negative epoch is absent" stay |
| D6 | `ConsumerRecord` constructors | three public (`J/consumer/ConsumerRecord.java:85,109,140`); `IllegalArgumentException("Topic cannot be null")`, `("Headers cannot be null")` | the same three forms and checks (`P/consumer/consumer_record.py:136-172`) | one `internal`, other order (`N/Consumer/ConsumerRecord.cs:61`) | Java's three, public; Java's `Headers` type, kept as given; the internal one goes |
| D7 | `DeliveryCount()` on a polled record | `Optional<Short>`, empty for this consumer | `None`: the poll uses the 11-argument form (`P/consumer/_poll.py:196-208`) | missing; `ToString()` prints a fixed `deliveryCount = null` | `short? DeliveryCount()`; a poll passes `null` (no C call per record); `ToString()` prints the stored value |
| D8 | The five Result constructors | public; keep the map by reference; only `AlterUserScramCredentialsResult` null-checks | public; `None` → `TypeError`; copy (`P/admin/list_offsets_result.py:82-88`) | `internal`, `ArgumentNullException`, by reference. The public `DescribeConsumerGroupsResult` copies (`N/Admin/DescribeConsumerGroupsResult.cs:37-51`) | public; `ArgumentNullException`; **copy** the map |
| D9 | `ConsumerRecords` constructor's maps | `records` by reference, `nextOffsets` copied (`Map.copyOf`) | both copied | — | both copied |
| D10 | `Records(null)` | `Records(TopicPartition null)` → empty list; `Records(String null)` → `IllegalArgumentException("Topic must be non-null.")` | empty list; `IllegalArgumentError("Topic must be non-None.")` | — | empty list; Java's text "Topic must be non-null." (.NET keeps Java's texts, as "Topic cannot be null.") |
| D11 | `IsEmpty()` | true when the partition map is empty | same | — | same (it differs from `Count() == 0` only for a partition given with an empty list) |
| D12 | Next offsets when a deserializer fails mid-batch | the fetch position stays at the failing record | a cut partition gets its last returned record's offset + 1, that record's leader epoch, metadata `""`; no entry if none was returned; other partitions keep the core's (`P/consumer/_poll.py:112-139`) | all next offsets are dropped | Python's rule |
| D13 | A poll that only moved past control records | returns records with next offsets and no records (`AsyncKafkaConsumer.java:957-971`, `Fetch.isEmpty`) | `empty()` only when both are empty | returns an empty set when `count <= 0` (`N/Internal/Interop/ConsumerRecordsMarshal.cs:58-61`) | `EMPTY` only when both are empty |

### 3.2 D14 — the parameter name of `Admin.Create`

| Java | Python | .NET today | Options |
|---|---|---|---|
| `create(Map<String,Object> conf)`; `create(Properties props)` | `create(*, configs)`, a stated permanent deviation: every Python call is keyword-only, so the name is in every call | constructors take `configs`. Rule 6 says parameters keep Java's names. The idiom map row "one `configs` per `Kafka*` constructor" is about constructors. The analyzer's own test writes `Create(… conf)` | **A** `conf`; **B** `configs`, plus one allowlist line and that idiom-map row widened |

**Lean A.** Rule 6 and the analyzer already say `conf`, and `Create` is not a constructor. In C# the name matters only
for a named argument (`IAdmin.Create(conf: …)`), which is rare. B is fine if you want one name on all three clients.

### 3.3 D15 — a class named `Admin` inside the namespace `Confluent.Kafka.Admin`

C# looks up the name `Admin` from the inside out. I checked this with a scratch build (SDK 10.0.400, netstandard2.0):

| Caller's namespace | `Admin.Create(…)` | `IAdmin.Create(…)` (C# 14) |
|---|---|---|
| a user's own, e.g. `MyApp`, with `using Confluent.Kafka.Admin;` | compiles | compiles |
| any `Confluent.Kafka.*` (our tests, gRPC server, soak, perf) | **CS0234**: C# finds the namespace `Confluent.Kafka.Admin` first | compiles |

Also, inside `Confluent.Kafka.Admin`, `Admin.UpgradeType.UNKNOWN` (`N/Admin/FeatureUpdate.cs:41,102-105`) would then mean
the class, and fail to compile; so would the four `Admin.OpType.*` lines M23/P4 added (`N/Admin/AlterConfigOp.cs:109-112`).
Python has no such clash (module `admin`, class `Admin`).

**Lean A:** keep the name `Admin` (rule 10, the analyzer, Java, Python). Our code calls `IAdmin.Create` /
`IAsyncAdmin.Create`. The in-namespace lines name the type in full, `Confluent.Kafka.Admin.UpgradeType.…` and
`Confluent.Kafka.Admin.OpType.…`, as `D/CLAUDE.md` §2 names `TimeoutException` in full (the idiom map's example
`Admin.UpgradeType.UPGRADE` stays true as written). `D/CLAUDE.md` §2 "Names that are also namespaces" already allows a
type named like its namespace (`Header.Header`) and keeps CA1724 off. `Create`'s doc
says: "From code in a `Confluent.Kafka.*` namespace, write `IAdmin.Create` or the full name." **B** (another name,
e.g. `AdminFactory`) breaks rule 10 and differs from Java and Python.

### 3.4 D16 — null arguments to the new `ConsumerRecords` constructor

| Java | Python | .NET precedent | Proposed |
|---|---|---|---|
| `nextOffsets` null, or a null key/value in it → NPE at construction (`Map.copyOf`). `records` null → NPE at first use; a null list → `records(tp)` returns empty, `iterator()`/`count()` throw | `next_offsets` `None` or a `None` in it → `TypeError` at construction; `records` `None` or a `None` list → `TypeError` at first use | public constructors check at construction (`DescribeConsumerGroupsResult`; `ProducerRecord`'s null header) | `ArgumentNullException` at construction for a null `records`, a null `nextOffsets`, a null list in `records`, a null `OffsetAndMetadata` in `nextOffsets`. A null record inside a list is kept, as Java keeps it |

**Lean: as proposed.** A failure at construction is easier to debug than one at first use, and it is how the other
.NET public constructors behave. The other choice is Python's timing (fail at first use).

### 3.5 D17 — `EMPTY` and `Empty()` on a generic class

Java: one `static final ConsumerRecords<Object,Object> EMPTY`, and `static <K,V> empty()` casts it. Python: one
`ConsumerRecords.EMPTY`. C# cannot share one object across type arguments (§2). **Lean:**
`public static readonly ConsumerRecords<TKey, TValue> EMPTY` (rule 5 keeps Java's spelling) and
`public static ConsumerRecords<TKey, TValue> Empty() => EMPTY;`. The caller writes
`ConsumerRecords<string, string>.Empty()`. A poll with nothing to return returns its type's `EMPTY`.

### 3.6 D18 — a public `ConsumerRecord` constructor now

Ledger 476 put "a public `ConsumerRecord` constructor" into the later typed-`MockConsumer` phase. Java and Python have
it public. We need it now: to build a `ConsumerRecords` (D9) and to translate Java's `ConsumerRecordsTest`.
**Lean: yes, now** (amends row 476). `MockConsumer.AddRecord(ConsumerRecord)` stays in that later phase (allowlist
lines 46–47 untouched).

### 3.7 D19 — names of the async listener pieces

`IAsyncConsumerRebalanceListenerWithPartitionsLost` and `AsyncConsumerRebalanceListenerExtensions`. These names are
already written in `D/CLAUDE.md` §4 (the listener paragraph, about line 1092), and the analyzer derives exactly these
names (`JavaNameOf` drops the `I`; `type.Name + "With" + suffix`). Python: `AsyncConsumerRebalanceListener`, whose
`on_partitions_lost` awaits `on_partitions_revoked`. **Lean: confirm.**

### 3.8 D20 — delete `ConsumerRebalanceListenerBase` and `AsyncConsumerRebalanceListenerBase`

Java has no base class; the default is on the interface. Python's listener *is* a class to subclass holding the
default, so Python's shape is the base-class shape. Rule 10, `D/CLAUDE.md` §4 ("rule 10 replaces them") and the pairs
file ("until the two bases go") all say delete. Ledger 48 created the async base.
**Lean: delete** (amends row 48). One way to write a listener, Java's. Keeping them would need two allowlist lines
(B422–B423) and a base that implements the `…WithPartitionsLost` interface.

### 3.9 D21 — where and how the consumer checks a listener (rule 10's guard)

Rule 10: since the analyzer does not ship, a client that calls a default checks the object once and throws
`IllegalArgumentException` for a public method of the default's name and parameters without the interface. Java has
no such check (it needs none). **Lean:** run it in `NativeConsumer.RequireListener` (`N/Internal/NativeConsumer.cs:760`),
right after Java's null check "RebalanceListener cannot be null". All six internal subscribe-with-listener paths call
it, so it covers `KafkaConsumer`, `MockConsumer`, `AsyncKafkaConsumer` and `AsyncMockConsumer`, sync and async
listeners. Message, in `SerdeGuard`'s form (`N/Internal/SerdeGuard.cs`):

```
MyListener declares OnPartitionsLost(IReadOnlyCollection<TopicPartition>) but does not implement
IConsumerRebalanceListenerWithPartitionsLost, so the consumer would never call it.
```

`SerdeGuard`'s metadata helpers (`Declares`, `TypeText`, the message) move into one shared internal helper that both
use; the serde messages do not change. Because the check runs before the closed check, a bad listener on a closed
consumer reports the listener, as Java's null check does.

### 3.10 D22 — how the `ProducerRecord` calls are migrated

**Lean:** two commits (S4, S5 in §4). S4 rewrites every call so that each argument after `topic` is named
(`value:`, `key:`, …), with the old constructor still in place. That commit changes no meaning, and its tests prove it.
S5 then swaps in Java's six constructors. Named arguments bind by name, so they keep their meaning, and a set of names
Java has no form for fails to compile. The migrated calls stay named; new tests and the README use Java's positional
order. (§8 R-1 explains why the compiler alone cannot find the risky calls.)

### 3.11 D23 — order with M23/P4

M23/P4 (N=125) is squashed (`803c3b8a` in `TempDir/example-confluent-kafka-rust`) but not on `master`. It touches files
this phase touches: the baseline, the pairs file, `N/Consumer/ConsumerRecord.cs` (`ToString`), `D/CLAUDE.md`, and it
adds `Admin.OpType.*` lines that S2 must also qualify. **Ruled (b):** this branch sits on `803c3b8a6` before S1, and
the line numbers here are read there. Regenerate the baseline, never merge it by hand. M23/P4 itself does not change.

### 3.12 D24 — logistics

M23/P5 "Java's shape: admin factory, records, Results, listeners"; N=126; this branch; `dotnet-actor` 126 and
`dotnet-critic` 126 (`D/CLAUDE.md` §8), a Critic pass after every Actor commit, comments in `dotnet/COMMENTS.126.md`;
steps S1–S10 (§4); squashing decided at the end; ledger rows 502–526 (§7).

### 3.13 D25 — `dotnet/CLAUDE.md` text

Six sentences in `D/CLAUDE.md` describe today's shape (the base classes, the `ProducerRecord` and `ConsumerRecords`
sketches). Drafts R1–R6 are in §9. **Ruled:** "apply R1-R5" in S10; R6 is dropped, as D14 is `conf`. Until S10, §9 is
the governing text for this phase. Any other `CLAUDE.md` change goes back to Pranav.

## 4. Steps

Each step is one commit. Gates for every step are in §6. Java tests are translated with exact message asserts
(`.claude/rules/definition-of-done.md` §3). Call-site counts were taken at `9152803b8`; the compiler is the final list.

### S1 — public constructors on the five Result classes (D8)

**Files:** `N/Admin/{AlterClientQuotasResult,AlterUserScramCredentialsResult,DeleteRecordsResult,DescribeClientQuotasResult,ListOffsetsResult}.cs`
and their five `Async*` peers.

Today (`N/Admin/ListOffsetsResult.cs:39-44`):
```csharp
// Internal, unlike Java's public constructor: a recorded deviation. The dictionary is held by
// reference, as Java holds its map.
internal ListOffsetsResult(IReadOnlyDictionary<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures)
{
    _futures = futures ?? throw new ArgumentNullException(nameof(futures));
}
```
After:
```csharp
/// <summary>Creates a result over the given futures, keyed by partition.</summary>
/// <param name="futures">The future of each partition's offsets. The map is copied.</param>
/// <exception cref="ArgumentNullException"><paramref name="futures"/> is <see langword="null"/>.</exception>
public ListOffsetsResult(IReadOnlyDictionary<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures)
{
    _futures = Copy(futures ?? throw new ArgumentNullException(nameof(futures)));  // as DescribeConsumerGroupsResult
}
```
A user now writes (Java: `new ListOffsetsResult(Map.of(tp, KafkaFuture.completedFuture(info)))`; Python:
`ListOffsetsResult(futures={tp: KafkaFuture.completed_future(value=info)})`):
```csharp
var result = new ListOffsetsResult(new Dictionary<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>>
{
    [tp] = KafkaFuture.CompletedFuture(new ListOffsetsResult.ListOffsetsResultInfo(5, -1, null)),
});
```
`DescribeClientQuotasResult(entities)` takes one future, so there is nothing to copy; `default` stays
`ArgumentNullException`. String keys copy with `StringComparer.Ordinal`, as `DescribeConsumerGroupsResult` does.

**Baseline lines removed:** B39–B44, B64, B96, B97, B183.
**Tests:** for each of the ten: null → `ArgumentNullException` with `ParamName` `futures` / `entities` (tests exist,
keep them); a user-built result's `All()` and per-key accessor return the given futures; changing the source
dictionary after construction does not change the result. **Also:** STATUS's follow-up FU-5 (M15/P14 Q3) closes.

### S2 — `Admin.Create` and `AsyncAdmin.Create` (D1–D3, D14, D15)

**Files:** `N/Admin/IAdmin.cs` (adds `Admin` at the end, as M23/P4 puts companions beside their type),
`N/Admin/IAsyncAdmin.cs` (adds `AsyncAdmin`), `KafkaAdminClient.cs` / `AsyncKafkaAdminClient.cs` (constructor
`internal`, its doc moves to `Create`), `N/Admin/FeatureUpdate.cs` (qualify `Admin.UpgradeType`), and the callers.

After (`IAdmin.cs`):
```csharp
/// <summary>Java's static methods of <c>Admin</c>.</summary>
public static class Admin
{
    extension(IAdmin)
    {
        /// <summary>Create a new Admin with the given configuration.</summary>
        /// <param name="conf">The configuration. …the text the constructor's configs had…</param>
        /// <returns>The new KafkaAdminClient.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="conf"/> is null.</exception>
        /// <exception cref="ConfigException">…as today…</exception>
        public static IAdmin Create(IReadOnlyDictionary<string, object?> conf) => new KafkaAdminClient(conf);
    }
}
```
A user writes:
```csharp
using IAdmin admin = IAdmin.Create(conf);      // C# 14
using IAdmin admin = Admin.Create(conf);       // any C# version, from the user's own namespace
```
Java: `Admin admin = Admin.create(props);`. Python: `admin = Admin.create(configs={...})`.
`FeatureUpdate.cs:41,102-105` today `Admin.UpgradeType.UNKNOWN => …` becomes `Confluent.Kafka.Admin.UpgradeType.UNKNOWN => …`;
`AlterConfigOp.cs:109-112` likewise for `Admin.OpType.*`.

**Call sites (69 in 26 files):** 22 unit-test files, `D/grpc-server/AdminServiceImpl.cs:74`,
`AsyncAdminServiceImpl.cs:74`, `D/soak/SoakClient/SoakClient.cs:1155`, `D/tests/Performance/PerfV3/TopicProvisioning.cs:45`.
All are in `Confluent.Kafka.*` namespaces, so all use `IAdmin.Create` / `IAsyncAdmin.Create` (D15).
**Baseline lines removed:** B105, B138, B402, B403.
**Tests:** `Create` returns a `KafkaAdminClient` / `AsyncKafkaAdminClient`; null `conf` → `ArgumentNullException`
(`ParamName` `conf` under D14 A); the existing config-refusal tests now go through `Create`; no public constructor on
the two clients (reflection); one test calls the full name `Confluent.Kafka.Admin.Admin.Create(…)`, the form older C#
uses. `NT/PublicNamespaceLayoutTests.cs` gains `("Admin", "=IAdmin")` and `("AsyncAdmin", "=IAsyncAdmin")`;
`NT/PublicAdminP14SurfaceTests.cs` checks `Create`'s types (sync returns sync, async returns async).

### S3 — `OffsetAndMetadata` in Java's three forms (D5)

Today: `public OffsetAndMetadata(long offset, string? metadata = null, int? leaderEpoch = null)`. After:
```csharp
public OffsetAndMetadata(long offset, int? leaderEpoch, string? metadata) { /* today's body */ }
public OffsetAndMetadata(long offset, string? metadata) : this(offset, null, metadata) { }
public OffsetAndMetadata(long offset) : this(offset, string.Empty) { }
```
Java: `new OffsetAndMetadata(5L, Optional.of(3), "m")`. Python: `OffsetAndMetadata(offset=5, leader_epoch=3, metadata="m")`.
.NET after: `new OffsetAndMetadata(5, 3, "m")`.

**Call sites:** 38 three-argument calls (30 unit tests, 2 gRPC tests, 5 gRPC server, 1 in `src`:
`OffsetMapMarshal.CopyOutValue`). Each with a non-null metadata or epoch fails to compile after the swap (a `string`
cannot go to `int?`), so the compiler finds them. The one `(o, null, null)` call means the same either way.
**Baseline lines removed:** B322, B414.
**Tests:** translate `OffsetAndMetadataTest.testInvalidNegativeOffset` (through each form),
`testEqualsWithNullAndNegativeLeaderEpoch`, `testEqualsWithNullAndEmptyMetadata`. Skip the three Java-serialization
tests, with the reason in the file (they test `java.io.Serializable`). A reflection test pins exactly three public
constructors with Java's parameter lists.

### S4 — name every `ProducerRecord` argument after the topic (D22; no API change)

Before / after, under the **old** constructor (same meaning):
```csharp
new ProducerRecord<string, string>(Topic, "v", "k")                  // today: value "v", key "k"
new ProducerRecord<string, string>(Topic, value: "v", key: "k")      // S4: the same, now said by name
```
**How every call is found:** (1) put `[Obsolete("find", error: true)]` on the constructor in the working tree, build
every project (§6), and count the CS0619 errors: that is every call, `new(…)` forms included (273 at `9152803b8`:
236 unit tests, 24 integration, 6 perf, 2 gRPC tests, 3 gRPC server, 1 soak, 1 package smoke). Remove the attribute; it
is never committed. (2) Rewrite the calls. (3) A scratch scan of `new ProducerRecord<…>(…)` finds the same count and
**zero** positional arguments after `topic`.
**Tests:** none new; all existing ones must pass unchanged.

### S5 — `ProducerRecord` in Java's six forms (D4)

After (doc comments from Java's Javadoc):
```csharp
public ProducerRecord(string topic, int? partition, long? timestamp, TKey? key, TValue? value, IEnumerable<Header>? headers)
{ /* today's checks: "Topic cannot be null.", then timestamp, then partition */ }
public ProducerRecord(string topic, int? partition, long? timestamp, TKey? key, TValue? value) : this(topic, partition, timestamp, key, value, null) { }
public ProducerRecord(string topic, int? partition, TKey? key, TValue? value, IEnumerable<Header>? headers) : this(topic, partition, null, key, value, headers) { }
public ProducerRecord(string topic, int? partition, TKey? key, TValue? value) : this(topic, partition, null, key, value, null) { }
public ProducerRecord(string topic, TKey? key, TValue? value) : this(topic, null, null, key, value, null) { }
public ProducerRecord(string topic, TValue? value) : this(topic, null, null, default, value, null) { }
```
Java: `new ProducerRecord<>("t", "k", "v")`. Python: `ProducerRecord(topic="t", key="k", value="v")`.
.NET after: `new ProducerRecord<string, string>("t", "k", "v")`.

**Call sites:** the old constructor is deleted in this commit. Named calls whose names match a Java form compile
unchanged. The rest fail to compile and get the missing name: the 93 `(topic, value:, partition:)` calls become
`(topic, partition:, key: null, value:)`; a value-type key gets `default`, which is what the old optional key was.
`D/README.md:51` becomes `new ProducerRecord<string, string>("orders", "key", "value")`.
**Baseline lines removed:** B397–B401, B415.
**Tests:** `ProducerRecordTest.testEqualsAndHashCode` and `testInvalidRecords` rewritten with Java's forms, positional,
exact messages. A same-type test for each form: `new ProducerRecord<string, string>("t", "k", "v")` has
`Key() == "k"` and `Value() == "v"`, and likewise the 4-, 5- and 6-argument forms. A reflection test pins exactly six
public constructors with Java's parameter lists, so an optional parameter cannot come back.

### S6 — `ConsumerRecord`: Java's three public constructors and `DeliveryCount()` (D6, D7, D18)

After:
```csharp
public ConsumerRecord(string topic, int partition, long offset, TKey? key, TValue? value)
    : this(topic, partition, offset, NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, NULL_SIZE, NULL_SIZE,
           key, value, new Headers(), null) { }
public ConsumerRecord(string topic, int partition, long offset, long timestamp, TimestampType timestampType,
    int serializedKeySize, int serializedValueSize, TKey? key, TValue? value, Headers headers, int? leaderEpoch)
    : this(…, headers, leaderEpoch, null) { }
public ConsumerRecord(…the same eleven…, short? deliveryCount)
{
    _topic = topic ?? throw new IllegalArgumentException("Topic cannot be null");
    _headers = headers ?? throw new IllegalArgumentException("Headers cannot be null");
    …
}
public short? DeliveryCount() => _deliveryCount;   // Java's Optional<Short> deliveryCount()
```
Java: `new ConsumerRecord<>("topic", 0, 0L, 0, "value")`. Python:
`ConsumerRecord(topic="topic", partition=0, offset=0, key=0, value="value")`.
The internal constructor goes; `ConsumerRecordsMarshal` (`:401`) calls the 11-argument form. `ToString()` prints
`deliveryCount = ` plus the stored value. The remark "There is no public constructor" goes.
**Baseline lines removed:** B287–B290.
**Tests:** translate `ConsumerRecordTest.testShortConstructor` and `testLongConstructor`; the two null messages;
`DeliveryCount()` and `ToString()` with a count; a polled record (`MockConsumer`) has `DeliveryCount() == null`; a
reflection test pins three constructors. `NT/PublicValueTypeJavaEqualityTests.cs` and `NT/PublicJavaMemberShapeTests.cs`
move to the public constructors.

### S7 — `ConsumerRecords`: per-partition storage and Java's read members (D10, D11, D17)

Today: a flat list with `Count()` and an enumerator (`N/Consumer/ConsumerRecords.cs:30-48`). After, in Java's member
order (the constructor and `NextOffsets()` come in S8):
```csharp
public static readonly ConsumerRecords<TKey, TValue> EMPTY = …;
public IReadOnlyList<ConsumerRecord<TKey, TValue>> Records(TopicPartition partition);   // unknown or null → empty
public IEnumerable<ConsumerRecord<TKey, TValue>> Records(string topic);               // null → IllegalArgumentException
public IReadOnlyCollection<TopicPartition> Partitions();
public IEnumerator<ConsumerRecord<TKey, TValue>> GetEnumerator();                     // Java's iterator()
public int Count();
public bool IsEmpty();
public static ConsumerRecords<TKey, TValue> Empty() => EMPTY;
```
Storage: an ordered list of (partition, read-only records) for Java's iteration order, plus a dictionary for lookup.
A `Dictionary` throws on a null key, so `Records(null partition)` returns the empty list before the lookup (D10). The
marshal groups records as it already reads them, partition by partition in the core's order; a partition with no
returned record has no entry (Java's `Fetch` adds none). Enumeration order is unchanged.
**Baseline lines removed:** B292–B294, B296–B298.
**Tests:** through `MockConsumer` polls of two partitions and two topics: `Partitions()`, both `Records` forms, the null
cases and message, `IsEmpty()`, `EMPTY` / `Empty()` on an empty poll, and that the returned lists are read-only.

### S8 — `ConsumerRecords`: `NextOffsets()` from the core and the public constructor (D9, D12, D13, D16)

After:
```csharp
public ConsumerRecords(
    IReadOnlyDictionary<TopicPartition, IReadOnlyList<ConsumerRecord<TKey, TValue>>> records,
    IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> nextOffsets);   // both copied; nulls per D16
public IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> NextOffsets();
```
A user writes (a `Dictionary<…, List<…>>` does not convert: `IReadOnlyDictionary` is invariant in its value type):
```csharp
var records = new ConsumerRecords<int?, string>(
    new Dictionary<TopicPartition, IReadOnlyList<ConsumerRecord<int?, string>>>
    {
        [tp] = new[] { new ConsumerRecord<int?, string>("topic", 0, 0L, 0, "value") },
    },
    new Dictionary<TopicPartition, OffsetAndMetadata> { [tp] = new OffsetAndMetadata(1L) });
```
Java: `new ConsumerRecords<>(Map.of(tp, List.of(record)), Map.of(tp, new OffsetAndMetadata(1L)))`. Python:
`ConsumerRecords(records={tp: [record]}, next_offsets={tp: OffsetAndMetadata(offset=1)})`.

The poll path (`ConsumerRecordsMarshal.CopyOut`, used by the sync and async consumers and by both mocks, whose core
fills next offsets too: `rust/src/consumer/mock_consumer.rs:868-887`):
```csharp
// Before the native batch is freed: kafka_consumer_ConsumerRecords_next_offsets_err, OffsetMapMarshal.CopyOut, kafka_Map_destroy.
IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> next = ReadNextOffsets(records);
if (error is not null) next = Cut(next, rewind, returned);   // D12: last returned offset + 1, its epoch, ""; no entry if none returned
if (byPartition.Count == 0 && next.Count == 0) return ConsumerRecords<TKey, TValue>.EMPTY;   // D13
return ConsumerRecords<TKey, TValue>.Owned(byPartition, next);                              // internal, no copy
```
The `count <= 0` early return goes (D13). **New P/Invoke:** `kafka_consumer_ConsumerRecords_next_offsets_err`
(already in the header; `NativeAbiConformanceTests` checks it; `NativeAbiAllowlist` lists only missing or differing
imports, so it needs no line). This is per poll, not per record.
**Baseline lines removed:** B291, B295. Closes ledger rows 148 (2) and (3), 149 and 426.
**Tests:** translate `ConsumerRecordsTest.testIterator`, `testRecordsByPartition`, `testRecordsByNullTopic`,
`testRecordsByTopic`, `testRecordsAreImmutable`, and the value asserts of
`testNextOffsetsDoesNotLogErrorWhenConstructedWithNextOffsets` and `testNextOffsetsDoesNotLogErrorForEmptyRecords`.
Skip `testNextOffsetsLogsErrorPeriodicallyWhenConstructedWithDeprecatedConstructor` (the deprecated constructor is not
translated). D16's four null cases with `ParamName`. Copy semantics. A `MockConsumer` poll's `NextOffsets()` equals
each partition's last offset + 1. A failing deserializer in partition 2 of 3: partition 1 keeps the core's next
offset, partition 2 gets the last returned offset + 1 with its epoch, partition 3 has none. A native batch with no
records and one next offset (built with a test-side import of `kafka_consumer_ConsumerRecords_with_next_offsets`)
gives `Count() == 0` and that next offset, not `EMPTY`. All `*AllocationBudget*` tests stay green.

### S9 — rebalance listeners in the rule-10 shape (D19, D20, D21)

Today (`N/Consumer/IConsumerRebalanceListener.cs`):
```csharp
public interface IConsumerRebalanceListener
{
    void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions);
    void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions);
    void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions);
}
public abstract class ConsumerRebalanceListenerBase : IConsumerRebalanceListener
{
    …
    public virtual void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) => OnPartitionsRevoked(partitions);
}
```
After (same file; the async file mirrors it with `Task`):
```csharp
public interface IConsumerRebalanceListener
{
    void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions);
    void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions);
}
public interface IConsumerRebalanceListenerWithPartitionsLost : IConsumerRebalanceListener
{
    void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions);
}
public static class ConsumerRebalanceListenerExtensions
{
    public static void OnPartitionsLost(this IConsumerRebalanceListener listener, IReadOnlyCollection<TopicPartition> partitions)
    {
        if (listener is null) throw new ArgumentNullException(nameof(listener));
        if (listener is IConsumerRebalanceListenerWithPartitionsLost withLost) withLost.OnPartitionsLost(partitions);
        else listener.OnPartitionsRevoked(partitions);   // Java's default
    }
}
```
What a user writes (Java: `implements ConsumerRebalanceListener`, `onPartitionsLost` optional; Python: subclass
`ConsumerRebalanceListener`, `on_partitions_lost` optional):
```csharp
class Mine : IConsumerRebalanceListener { /* Revoked, Assigned */ }                          // lost runs Revoked
class Mine : IConsumerRebalanceListenerWithPartitionsLost { /* Revoked, Assigned, Lost */ }  // its own Lost
class Mine : IConsumerRebalanceListener { /* …plus a public OnPartitionsLost */ }            // Subscribe throws (D21)
```
**Files:** the two interface files (add the `With` interface and the extension class); delete
`ConsumerRebalanceListenerBase.cs` and `AsyncConsumerRebalanceListenerBase.cs`. `N/Internal/ListenerRegistration.cs:197,210`
keeps its text `listener.OnPartitionsLost(partitions)`, which now binds to the extension method. The guard goes in
`RequireListener` (D21), with `SerdeGuard`'s helpers shared. `D/analyzers/java-name-pairs.txt:6-9` (the blank line, the two-line comment and the pair) goes. Doc comments
that point at the bases are rewritten.
**Implementers to migrate** (each adds the `…WithPartitionsLost` interface if its `OnPartitionsLost` does something of
its own, or drops a method that only repeats the default): 21 sync (19 unit tests, `D/grpc-server/CallbackLog.cs`,
`D/grpc-server/Chaos/ChaosRebalanceListener.cs`), 14 async (13 unit tests, `D/grpc-server/Chaos/AsyncChaosRebalanceListener.cs`),
8 base-class users (6 sync: 5 unit tests and the integration test `KafkaConsumerBrokerTests.cs`; 2 async, in unit
tests). The three gRPC listeners log a lost event of their own, so they take the `With` interface; the
multilanguage arms must still see it. 4 explicit `I(Async)ConsumerRebalanceListener.OnPartitionsLost` implementations
(`PublicConsumerListenerLifetimeTests.cs`, `PublicConsumerRebalanceListenerTests.cs`) stop compiling and move to the
`With` interface. `NT/PublicNamespaceLayoutTests.cs` swaps the two base
rows for the four new types.
**Baseline lines removed:** B3, B4, B422, B423; pairs lines 6–9.
**Tests:** the extension runs Revoked for a plain listener and the listener's own method for a `With` one (sync and
async, on `MockConsumer.LosePartitions` / `AsyncMockConsumer.LosePartitions`); the guard's exact message on all four
consumers, sync and async listener forms; no throw for an explicit `With` implementation or an inherited public method
on a `With` type; the null-listener message still comes first.

### S10 — close-out

STATUS.md entry; ledger rows 502–526 updated to your rulings; `COMMENTS.DONE.126.md` copied here;
`dotnet/COMMENTS.126.md` reset; R1–R5 applied to `D/CLAUDE.md` (D25, Pranav's own instruction); close gate (§6). Manager work, plus
any doc fix the Critic finds.

## 5. What breaks for users (pre-GA, but say it)

1. `new KafkaAdminClient(configs)` and `new AsyncKafkaAdminClient(configs)` no longer compile. Write
   `IAdmin.Create(conf)` / `IAsyncAdmin.Create(conf)` (C# 14) or `Admin.Create(conf)` / `AsyncAdmin.Create(conf)`. A
   named argument `configs:` breaks under D14 A.
2. `ProducerRecord`: **a call `new ProducerRecord<K, V>(topic, x, y)` still compiles but now means key `x`, value
   `y`.** When `K` and `V` are the same type, nothing warns. Every other old form (value first with 4–6 arguments,
   `partition:` without a key, …) is a compile error.
3. `OffsetAndMetadata(offset, metadata, leaderEpoch)` is now `(offset, leaderEpoch, metadata)`; a non-null call in the
   old order fails to compile. `(offset, leaderEpoch: e)` alone is gone.
4. `ConsumerRebalanceListenerBase` and `AsyncConsumerRebalanceListenerBase` are gone. `OnPartitionsLost` is no longer on
   the two interfaces: an explicit implementation of it fails to compile, and a public one on a class without the
   `…WithPartitionsLost` interface makes `Subscribe` throw `IllegalArgumentException`. Calling
   `listener.OnPartitionsLost(p)` still compiles (it is the extension now).
5. Additive only: the five Result constructors, the `ConsumerRecord` constructors and `DeliveryCount()`, and the new
   `ConsumerRecords` members. A user-built Result copies its map.

## 6. Gates

Per step, on this Mac, from the repository root unless said:
1. The native library exists: `cargo build --features ffi --release` in `rust/` once at the start, and after any
   rebase. No step changes Rust.
2. Build with 0 warnings, Release: `D/Confluent.Kafka.sln` (library, unit and integration tests, analyzer and its
   tests), plus the projects outside it that the step touches: `D/grpc-server/Confluent.Kafka.GrpcServer.csproj`,
   `D/tests/Confluent.Kafka.GrpcServer.UnitTests`, `D/soak/SoakClient` and `D/soak/SoakClient.Tests`,
   `D/tests/Performance/*`.
3. `dotnet format --verify-no-changes` on the solution and the touched outside projects.
4. `NT` on net8.0 and net10.0; `make -C dotnet test-grpc-server-dotnet` when the gRPC server changes (S2–S5, S9);
   `SoakClient.Tests` when the soak client changes (S2, S4, S5).
5. `cargo xtask dotnet-java-name-baseline` in `rust/`: the step's lines go, nothing is added (never `--add`), no
   CKRDA0014 is left.
6. Commit with `--no-verify`. The pre-commit hook runs only the Rust and C checks; a .NET-only commit runs the .NET
   gates above instead (ledger 58). Breaking steps use `feat(dotnet)!:`.
7. The Critic reviews the commit; fixes land as fixups.

Close gate (S10): `make verify-dotnet` on Linux. On this Mac: `make verify-dotnet-macos-docker`, then
`make test-integration-dotnet-native` and the IntegrationTests project, on a quiet machine with `RUST_TEST_THREADS=6`.
The plain `make test-integration-dotnet` prints SKIP on macOS and exits 0, which is not a pass. CI's nupkg job
(`.semaphore/test-nupkg.sh`) builds `D/tools/PackageSmoke`, which S5 edits. The agent briefs carry the sandbox
shell-trap list.

## 7. Ledger rows (Agreed 2026-11-12, as ruled in the status line)

Rows 502–526 of `DOTNET-PORT-DECISIONS.md`.

| # | Source | Decision | Pick | Status |
|---|---|---|---|---|
| 502 | PM 126, M23/P5 plan D1 | `KafkaAdminClient` / `AsyncKafkaAdminClient` constructors | `internal` | Agreed |
| 503 | D2 | The async factory | static `AsyncAdmin`, `extension(IAsyncAdmin)` `Create` | Agreed |
| 504 | D3 | Factory return types | `IAdmin` / `IAsyncAdmin` | Agreed |
| 505 | D4 | `ProducerRecord` constructors | Java's six, Java's order, no optional parameters | Agreed |
| 506 | D5 | `OffsetAndMetadata` constructors | Java's three | Agreed |
| 507 | D6 | `ConsumerRecord` constructors | Java's three, public; the internal one goes | Agreed |
| 508 | D7 | `DeliveryCount()` | `short?`; null from a poll, no C call per record | Agreed |
| 509 | D8 | The five Result constructors | public; `ArgumentNullException`; copy | Agreed |
| 510 | D9 | `ConsumerRecords` constructor's maps | both copied | Agreed |
| 511 | D10 | `Records(null)` | empty list; "Topic must be non-null." | Agreed |
| 512 | D11 | `IsEmpty()` | Java's: no partition has records | Agreed |
| 513 | D12 | Next offsets on a cut batch | Python's rule | Agreed |
| 514 | D13 | A poll past control records only | `EMPTY` only when both are empty | Agreed |
| 515 | D14 | `Create`'s parameter name | A `conf` | Agreed |
| 516 | D15 | The `Admin` class/namespace clash | A keep `Admin`; our code calls `IAdmin.Create`; in-namespace lines name the type in full | Agreed |
| 517 | D16 | Nulls to the `ConsumerRecords` constructor | `ArgumentNullException` at construction | Agreed |
| 518 | D17 | `EMPTY` / `Empty()` | per closed type, on the generic class | Agreed |
| 519 | D18 | Public `ConsumerRecord` constructor now | yes; amends 476 | Agreed |
| 520 | D19 | Async listener names | confirm the `D/CLAUDE.md` §4 names | Agreed |
| 521 | D20 | The two listener base classes | delete; amends 48 | Agreed |
| 522 | D21 | The listener guard | in `RequireListener`, after the null check; `SerdeGuard`'s message form | Agreed |
| 523 | D22 | `ProducerRecord` migration | named-argument commit, then the swap | Agreed |
| 524 | D23 | Order with M23/P4 | (b) rebase onto `803c3b8a` now | Agreed |
| 525 | D24 | Logistics | M23/P5, N=126, S1–S10, this branch | Agreed |
| 526 | D25 | `D/CLAUDE.md` drafts R1–R6 | R1–R5 applied in S10; R6 dropped (D14 is `conf`) | Agreed |

## 8. Risks and how each step guards against them

- **R-1 The silent key/value swap (S4, S5).** At `9152803b8`, 44 calls pass three positional arguments; in 32 of them
  key and value have the same type (or the key is `object`). After S5 those compile and swap. The compiler cannot find
  them, because the new `(topic, key, value)` constructor is a better match than the old one with its defaults, even
  if the old one is marked obsolete. Guards: S4 names every argument under the old constructor, which keeps the
  meaning; the obsolete-error count proves the list is complete; the scan proves no positional argument is left; the
  Critic checks S4's diff only adds names. S5's same-type tests and the reflection test pin Java's six forms.
- **R-2 The `Admin` name clash (S2).** Any in-namespace `Admin.X` fails to compile, so the compiler lists them. Repo
  code uses `IAdmin.Create`; one test uses the full name; the doc tells users. `AnalysisLevel latest` does not enable
  CA1724 (type name equals a namespace); S2's 0-warning build confirms it.
- **R-3 Conflicts with M23/P4 (all).** Rebase first (D23); regenerate the baseline after the rebase.
- **R-4 A listener whose own `OnPartitionsLost` is silently skipped (S9).** The guard catches it at `Subscribe`, but
  only for a listener that is subscribed. So S9 migrates every implementer by list (§4 S9), and the Critic checks the
  list is complete, unit tests and gRPC server included.
- **R-5 The gRPC harness's lost event (S9).** The three gRPC listeners take the `With` interface; the gRPC server tests
  and, at close, the multilanguage arms check the callback log.
- **R-6 Next offsets cost (S8).** One P/Invoke and one map per poll, not per record. The `*AllocationBudget*` tests
  stay green; the map is read before the native batch is freed.
- **R-7 Copying user maps (S1, S8).** Only the public constructors copy; the poll and admin paths hand over their own
  maps with no copy.

## 9. Draft text for `dotnet/CLAUDE.md` (D25; applied in S10 only on Pranav's own instruction)

- **R1** (§2 layout, about line 136): "(§3 Java names rule 10; today the base class `ConsumerRebalanceListenerBase`)"
  → "(§3 Java names rule 10: `ConsumerRebalanceListenerExtensions` and `IConsumerRebalanceListenerWithPartitionsLost`
  beside `IConsumerRebalanceListener`, `Admin` beside `IAdmin`)".
- **R2** (status, about lines 630 and 633): "(+ `ConsumerRebalanceListenerBase`)" → "(+
  `IConsumerRebalanceListenerWithPartitionsLost`, M23/P5)", and the async one likewise.
- **R3** (idiom map, about line 751): "`ConsumerRebalanceListenerBase` and `AsyncConsumerRebalanceListenerBase` carry
  Java's `onPartitionsLost` default." → "Java's `onPartitionsLost` default is an extension method in
  `ConsumerRebalanceListenerExtensions` / `AsyncConsumerRebalanceListenerExtensions`; a listener replaces it by
  implementing `I…WithPartitionsLost` (rule 10), and `Subscribe` refuses a public `OnPartitionsLost` without it."
- **R4** (§4 listener paragraph, about lines 1087–1095): replace "It lives on the public abstract … until then
  CKRDA0013 baselines `OnPartitionsLost` on the two interfaces." with "Rule 10 puts it in
  `ConsumerRebalanceListenerExtensions` and `AsyncConsumerRebalanceListenerExtensions`;
  `IConsumerRebalanceListenerWithPartitionsLost` and `IAsyncConsumerRebalanceListenerWithPartitionsLost` replace it
  (M23/P5)."
- **R5** (§3 sketches, about lines 240–242 and 438–440): `ProducerRecord`'s six constructors; `ConsumerRecords`' members
  as in S7–S8.
- **R6** (dropped: D14 is A; idiom map, about line 766): "one `IReadOnlyDictionary<string, object?> configs` per
  `Kafka*` constructor" → "… per `Kafka*` constructor and `Admin.Create` / `AsyncAdmin.Create`".

## 10. Notes on the evidence (where it differs from the brief)

- `AlterUserScramCredentialsResult`'s Java constructor does null-check (`Objects.requireNonNull`) and wraps the map in
  an unmodifiable view; the other four keep it with no check.
- No ledger row names the five Result classes, but the deviation was recorded: M15/P14 plan Q3 (b) made the Result
  constructors internal, with follow-up FU-5 in STATUS. FU-5 also asked for `KafkaFuture.CompletedFuture`, which now
  exists. S1 closes FU-5.
- Ledger 48 created `AsyncConsumerRebalanceListenerBase`; ledger 476 tied the public `ConsumerRecord` constructor to the
  typed-mock phase; ledger 148 (2)(3), 149 and 426 track `NextOffsets`, `DeliveryCount` and the `ConsumerRecords`
  members. This plan amends 48 and 476 and closes the others.
- Listener counts: 21 sync and 14 async implementers, 8 base-class users (the brief said about 41, 17 and 8).
- The `--no-verify` practice is ledger 58, not a line of `D/CLAUDE.md`.
- New: the `Admin` class/namespace clash (D15), and `FeatureUpdate.cs`'s `Admin.UpgradeType` lines.
- Out of scope, as asked: `MockProducer.History()` (M3), the `Get(CancellationToken)` fold (F1), rule-9 enum class
  renames, the rest of the baseline.
