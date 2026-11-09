# M22/P4 — Java getters become C# methods (tentative id; N = 120 tentative)

Status: **S1 in progress.** On 2026-11-06 the owner delegated the rulings: "take the best decision
possible … align with Java as much as possible, and also with Python and Rust". §0 records what was
decided under that delegation. The rows have not been individually marked *Agreed* by the owner.

## 0. Decisions (made under the owner's delegation, 2026-11-06)

Order of precedence: Java first, then Rust and Python where Java leaves the C# form open.

| Id | Decision | Java · Rust · Python |
|---|---|---|
| L-1 | Every Java method is a C# method with Java's exact name. `get` is kept: `GetMostSignificantBits()`, `GetName()` | Java `getX()`. Rust and Python drop `get` (`most_significant_bits`). The owner's earlier ruling keeps Java's name |
| L-2 | A getter and setter of one name become overloads, and the setter returns Java's return type | Java's overloads; Python's two stubs; Rust `x()` / `set_x()` (§2's `set_` rule) |
| L-3 | Java's own setter names (`InGroupStates`, `FilterStates`, …). No setter where Java has none; `ListOffsetsOptions(IsolationLevel)` is added | Java; Python identical |
| L-4 | `Serdes` becomes static methods; `public static final` becomes `static readonly` fields | Java; Python class attributes; Rust associated consts |
| L-5 | A property stays only where .NET requires it, with a guard test. The `FeatureUpdate.UpgradeType` enum moves beside its class | precedent: ledger 109 |
| D-a1 | Remove `TopicMetadataAndConfig.HasMetadata` | none in Java, Rust or Python |
| D-a2 | Keep `ClientQuotaFilterComponent.MatchType()` public and .NET-only, beside Java's `Match()` | Java's `Optional<String>`-or-`null` makes three public states, and Rust's `match()` returns the three-state `ClientQuotaMatch`. Python hides the third state, losing information |
| D-b | Java's setter chain only; no keyword constructor | Java; Rust uses `*OptionsBuilder` chains. Python's keyword constructor is its own deviation |
| D-c | No `[DebuggerDisplay]` in this phase | none in Java, Rust or Python |
| D-d1 | `Serdes.String()`, `ByteArray()`, `Integer()`, `Long()`, `Double()`, `UUID()`, `Void()` | Java's names; Rust and Python have no `Serdes` class |
| D-d2 | Add `KafkaPrincipal.TokenAuthenticated(bool)` returning `void`; equality unchanged | Java; Python has the setter |
| D-d3 | No rule for members named like a type; an expression that reaches the type's members qualifies the type, where C# binds the simple name to the method group; a type position needs none | — |

**Test gate on this machine:** the .NET 8 runtime is absent. The net8.0 legs therefore run with
`DOTNET_ROLL_FORWARD=Major`, on the .NET 10 runtime. That checks the net8.0 build's code paths, but
it is not the .NET 8 runtime; the real net8.0 leg runs in CI (`make verify-dotnet`).

Branch `dev_dotnet-getters-as-methods`, from `master` at `96b786709`.

## 1. Goal

Every public Java method becomes a C# method, getters included, as the Python binding has it.
This replaces the `dotnet/CLAUDE.md` rules "getters → properties" (§3 sketches and the idiom
map), the §4 table row "Non-blocking getter | sync property", and §6.3's "properties for getters".
The change is .NET only: no Rust, C, or Python changes.

Why. Today the surface mixes two forms for the same Java shape:

- Value-type getters are properties, but client state reads are methods.
- `CreateTopicsResult.Values` is a property that builds a new dictionary on every access, while
  `All()` is a method.
- `CloseOptions.Timeout()` is a method, while `TopicDescription.Name` is a property.
- `KafkaFuture.IsDone` and `IsCancelled` are properties, although some state reads settle a done
  handle (ffi-marshalling §C1).
- Java's fluent setters are lost: `CreateTopicsOptions.TimeoutMs { get; set; }` and similar.

## 2. Java and Python, the baseline

| Topic | Java | Python | .NET today |
|---|---|---|---|
| getter | method, `topic()`, `getName()`, `isDone()` | method; drops a leading `get` (`name()`) | property |
| getter + setter of one name | two overloads; the setter returns `this` (Options), `T` (`AbstractOptions.timeoutMs`) or `void` | one method, two stubs | `{ get; set; }` |
| setter with its own name | `ListGroupsOptions.inGroupStates(..)`, `ListTransactionsOptions.filterStates(..)` | Java's name | the getter's property gets a `set` |
| static factory | `Serdes.String()`, `Serdes.Integer()` | none (`string_serializer()` …) | static property `Serdes.String`, `Serdes.Int32` |
| `public static final` | field | class attribute | some fields (ledger 285), some static properties (`Uuid.ZERO_UUID`) |
| building an Options | `new X().timeoutMs(5000).validateOnly(true)` | keyword constructor too, a *(deviation)* | object initializer |

## 3. Inventory (machine-made)

A Roslyn tool built the inventory before the phase began. The per-member list and the tool are
not kept in the repository; the totals and counts below are what the plan used.

How the tool works:

- It compiles `src/Confluent.Kafka` semantically against the net10 reference pack.
- It lists every public or protected property of every effectively public type.
- For each property it finds the Java member through the class hierarchy, reading
  `kafka/clients/src/{main,testFixtures,test}`. A short alias table covers the eleven .NET names
  that do not follow Java's.
- It resolves call sites by symbol in each consumer project, after compiling that project against
  its own build output, signed with the repo key so `InternalsVisibleTo` holds. Every project
  compiled with 0 errors.

**Totals:** 458 public types and 428 public properties.

| Count | What |
|---|---|
| 1 | required by .NET: `ConsumerRecords.Count` implements `IReadOnlyCollection<T>.Count` (since the review fixes, a method plus an explicit implementation) |
| 416 | have a Java method counterpart. 84 of them also have a setter form; 15 change name |
| 10 | have a Java `public static final` field counterpart, and are static properties today |
| 2 | are .NET-only: `TopicMetadataAndConfig.HasMetadata` and `ClientQuotaFilterComponent.MatchType` |
| 1 | C#-forced collision: `FeatureUpdate.upgradeType()` against the nested enum `UpgradeType` |
| 0 | explicit interface implementations; 0 public mutable fields; 0 indexers |

No exception declares `Message` or `InnerException`; they are inherited. `KafkaException.Code` is
`internal`, and `IsRetriable` no longer exists.

Call sites of the properties that change (by symbol; a call through an interface counts on the
interface's row):

| Stage | Family | Props | src | tests | grpc-server | grpc tests | soak | perf | object initializers |
|---|---|---|---|---|---|---|---|---|---|
| S1 | value types | 230 | 1099 | 1807 | 265 | 22 | 31 | 30 | 0 |
| S2 | Options, builders, NewTopic | 91 | 353 | 805 | 166 | 0 | 5 | 2 | **582** |
| S3 | admin Results | 52 | 14 | 619 | 63 | 0 | 3 | 2 | 0 |
| S4 | futures | 12 | 0 | 221 | 1 | 0 | 0 | 0 | 0 (1 property pattern) |
| S5 | exception payload getters | 25 | 49 | 77 | 0 | 0 | 0 | 0 | 0 |
| S6 | serdes, constants, the rest | 7 + 10 | 0 | 1229 | 24 | 26 | 7 | 10 | 0 |

That is about 6,900 call sites in total. `Serdes.ByteArray` alone accounts for 1,197.

Not counted by symbol:

- `tools/PackageSmoke`, which builds only against a published package: 9 text matches.
- `README.md`: a handful of snippets.
- 36 unit-test files assert the shape by reflection (`GetProperty` / `GetProperties`). They move to
  `GetMethod` in the stage that changes their type.

No `System.Text.Json` (or other reflection-based) serialization of binding types exists in src,
tests, grpc-server, or soak, so no wire or JSON output changes silently.

## 4. Proposed ledger rows (numbers are placeholders; the owner assigns them)

**L-1. Every public Java method is a C# method.**
- The name is the PascalCase of Java's exact name.
- `get`, `is` and `has` are kept: `getMostSignificantBits()` becomes `GetMostSignificantBits()`, and
  `Get()`, `GetNow()`, `IsDone()`, `RecordMetadata.HasOffset()` stay as Java names them.
- *(.NET ruling, deviating from Python, which drops `get`.)* It touches four members:
  `Uuid.GetMostSignificantBits` and `GetLeastSignificantBits`, and `KafkaPrincipal.GetName` and
  `GetPrincipalType`.
- The same rule renames the members whose .NET name never followed Java:

  | Today | Becomes |
  |---|---|
  | `TopicPartitionInfo.InSyncReplicas` | `Isr()` |
  | `IMetric.Name` / `Value` and `KafkaMetric.Name` / `Value` | `MetricName()` / `MetricValue()` |
  | `ClientQuotaFilterComponent.MatchName` | `Match()` |
  | `FeatureUpdate.Type` | `UpgradeType()` (L-5) |

**L-2. A Java getter and setter of one name become two C# overloads.**
- `TimeoutMs()` reads.
- `TimeoutMs(int? timeoutMs)` sets and returns what Java's setter returns:
  - the Options type itself (Java's `this`, or `T` for `AbstractOptions.timeoutMs`; .NET has no
    `AbstractOptions`, so each class returns itself);
  - `void` for `RemoveMembersFromConsumerGroupOptions.reason(String)` and
    `KafkaPrincipal.tokenAuthenticated(boolean)`.
- `Setters.Ran` tracking (ledger 176) stays as it is. A setter of a Java primitive takes the C#
  primitive (amended after the PR 322 review, see "Review fixes"; ledger 181 amended).

**L-3. A setter Java names differently keeps Java's name, and a setter Java lacks goes.**
- `ListGroupsOptions`: `InGroupStates(..)`, `WithProtocolTypes(..)`, `WithTypes(..)`.
- `ListTransactionsOptions`: `FilterStates(..)`, `FilterProducerIds(..)`, `FilterOnDuration(..)`,
  `FilterOnTransactionalIdPattern(..)`.
- Each getter keeps Java's getter name (`GroupStates()`, `FilteredStates()`).
- `ListOffsetsOptions.IsolationLevel` loses its setter, and Java's constructor
  `ListOffsetsOptions(IsolationLevel)` is added. .NET has only the parameterless one today.

**L-4. A Java static method is a C# static method; a Java `public static final` field is a C# field.**
- `Serdes` becomes static methods (names: decision D1).
- These ten static properties become `static readonly` fields (ledger 285):
  - `Uuid.ZERO_UUID`, `ONE_UUID`, `METADATA_TOPIC_ID`, `RESERVED`
  - `KafkaPrincipal.ANONYMOUS`
  - `AccessControlEntryFilter.ANY`, `AclBindingFilter.ANY`, `ResourcePatternFilter.ANY`
  - `MockAdminClient.DEFAULT_LOG_DIRS` and its `AsyncMockAdminClient` twin

**L-5. A property remains only where .NET requires it.**
- None is left besides the inherited `Exception.Message`, `InnerException` and similar:
  `ConsumerRecords.Count()` is Java's method, and `IReadOnlyCollection<T>.Count` is implemented
  explicitly (amended after the PR 322 review, see "Review fixes").
- A reflection guard test fails on any other public property of a public type.
- A rename C# forces is recorded with its reason.
- The one such rename: `FeatureUpdate.upgradeType()` cannot sit beside the nested enum
  `FeatureUpdate.UpgradeType` (CS0102). Following the precedent of `GroupMembershipOperation`
  (ledger 109), `ConfigResourceType` and `AlterConfigOpType`, the **enum** moves beside the class as
  `Confluent.Kafka.Admin.UpgradeType` (next to the existing `UpgradeTypes`), and the method keeps
  Java's name `UpgradeType()`.

## 5. Open decisions (recommendation first)

**D-a. .NET-only properties.** There are only two left; everything else maps to Java.

- `CreateTopicsResult.TopicMetadataAndConfig.HasMetadata`.
  - Java has none; its accessors throw the stored `ApiException`. Python has none either.
  - It has 1 call site, in a test.
  - **Recommend: remove it.** It is shape Java does not have, and callers catch the exception as in
    Java.
- `ClientQuotaFilterComponent.MatchType`.
  - Java's `match()` is an `Optional<String>` that may itself be `null`, which gives three states:
    `ofEntity`, `ofDefaultEntity`, `ofEntityType`.
  - Python returns `str | None` from `match()` and keeps the third state in a private flag.
  - C# has no `Optional`. `MatchType` is how a reader tells "default entity" from "any".
  - **Recommend: keep it as a .NET-only method `MatchType()`,** recorded as a deviation with that
    reason. The alternative is to make it internal, as Python does, which loses information a C#
    reader cannot get any other way.
- `KafkaException.Code` / `IsRetriable`: nothing to do (internal / gone).

**D-b. Object initializers stop working.** 582 sites use them, almost all in tests and grpc-server.

- Java builds Options by chaining setters. Python adds a keyword constructor as a *(deviation)*.
- **Recommend: Java's chain only,** with no keyword constructor. With L-2, the chain is one C#
  expression: `new CreateTopicsOptions().TimeoutMs(5000).ValidateOnly(true)`.
- A constructor with optional parameters freezes its parameter list into callers' binaries. A field
  Java adds later would then need a new overload, which is the ABI problem the chain avoids.
- The 582 sites are rewritten mechanically in S2.

**D-c. `[DebuggerDisplay]` on value types.**

- Java and Python have none.
- Methods are not evaluated in the debugger's locals view, so values become one click away. Most
  value types already override `ToString()`, which the debugger shows.
- **Recommend: not in this phase.** Revisit only if users ask.

**D-d. What the rule does not settle:**

1. **`Serdes` names.** Java has `String()`, `ByteArray()`, `Integer()`, `Long()`, `Double()`,
   `UUID()` and `Void()`. .NET has `String`, `ByteArray`, `Int32`, `Int64`, `Double`, `Guid` and
   `Null`. Python has no `Serdes` class.
   - **Recommend Java's names as static methods:** `Serdes.String()`, `ByteArray()`, `Integer()`,
     `Long()`, `Double()`, `UUID()`, `Void()`. Rule 1 and bindings.md §2.2 say names follow Java,
     and the serde's type argument still says `int`, `long` and `Guid`.
   - Only five names change, with 43 sites. The 1,130 `ByteArray` sites keep their name and only
     gain `()`.
2. **`KafkaPrincipal.tokenAuthenticated(boolean)`.** Java's setter mutates the principal, and Java's
   `equals` ignores the field. Python has the setter.
   - **Recommend: add it** (L-2, returning `void`). Equality is unchanged, as in Java.
3. **Members named like their return type.** This compiles, but C#'s "Color Color" rule covers
   properties, not methods, so inside such a class an expression like `UpgradeType.UPGRADE` binds
   to the method group. A type position (`TopicPartition TopicPartition()`, a parameter or a local's
   type) still finds the type. Examples: `MetricName MetricName()`, `TopicPartition TopicPartition()`,
   `Headers Headers()`, `TimestampType TimestampType()`, `IsolationLevel IsolationLevel()`.
   - **Recommend: no rule.** The Actor qualifies the type in an expression that reaches its members
     (`Admin.UpgradeType.UPGRADE` in `FeatureUpdate`, `Resource.ResourceType.ANY` in
     `ResourcePattern` and `ResourcePatternFilter`); a type position needs no qualification.
4. **One interface changes:** `IMetric` is the only public interface with properties (`Name`,
   `Value`). A user implementation of it breaks; the `feat(dotnet)!` marker covers that.

## 6. Stages

Each stage changes one family. In the same commit it updates every caller: src, unit and
integration tests, grpc-server and its tests, soak, perf, PackageSmoke, `README.md`, and the
snippets in `dotnet/CLAUDE.md` that name the changed members.

| Stage | Family | Notable items |
|---|---|---|
| S1 | value types (230) | L-1 renames; `FeatureUpdate` enum hoist (L-5); `TopicPartition` stays a `readonly struct` |
| S2 | Options, builders, `NewTopic` (91) | L-2 overloads; L-3 Java setter names and `ListOffsetsOptions(IsolationLevel)`; 582 object initializers become chains |
| S3 | admin Results and nested types (52) | `Values()`, `Names()`, …; `HasMetadata` removed (D-a) |
| S4 | futures (12) | `IsDone()`, `IsCancelled()`, `IsCompletedExceptionally()` on all four future types; ffi-marshalling §C1 semantics unchanged (`IsDone()` settles nothing; the others settle a done handle) |
| S5 | exception payload getters (25) | `Partitions()`, `TopicPartition()`, …; all 25 are in hand-written files (14 classes), none in the generated `ErrorClasses.g.cs`, so no generator change |
| S6 | serdes, constants, the rest | `Serdes` methods (D-d1); the ten L-4 fields; `KafkaPrincipal` (D-d2) |
| Last | guard and docs | reflection guard test (L-5); `dotnet/CLAUDE.md` §3, the §4 table row, §6.3; the `dotnet-critic` persona's "getters→properties"; ffi-marshalling where it names these members; STATUS.md; one comment-only cleanup commit |

## 7. Gates (before every commit)

- The solution and the outside projects build with 0 warnings: grpc-server, its tests, both soak
  projects, and the perf projects.
- `dotnet format --verify-no-changes` passes on the solution, grpc-server, its tests, and both soak
  projects.
- Unit tests pass on net8.0 and net10.0. **The net8.0 runtime is not installed on this machine**, so
  only net10.0 can run here until one is installed.
- The gRPC server unit tests pass (net10.0).
- Before the phase closes: `make verify-dotnet` (needs Docker).
- Commits are `feat(dotnet)!:` and use `--no-verify`, since no Rust or C changes (ledger 58); the
  message says so. Only code is staged. Nothing is pushed.

## 8. Governance

- I act as Manager: I spawn dotnet-actor N=120 per stage, then dotnet-critic N=120 reviews it.
  `COMMENTS.120.md` and `COMMENTS.DONE.120.md` stay at the binding root and are never committed.
- The persona copies in the repo-root `.claude/agents/` are never committed.
- After each subagent run I check HEAD and `git status`.

## S1 notes

Commit `458f241d4`: 230 value-type getters became methods (reflection self-check: 230 ok, 0 still properties). Deviations and choices:

- `UpgradeType` is no longer nested in `FeatureUpdate`; it is `Confluent.Kafka.Admin.UpgradeType`, in the same file (CS0102 with the getter `UpgradeType()`). `PublicNamespaceLayoutTests` maps it to Java's nested class and lists it in `JavaNestedTypesKeptTopLevel`. `dotnet/CLAUDE.md` still names `UpgradeTypes` the old way (left for the last stage, with `ffi-marshalling.md`).
- `ClientQuotaFilterComponent.MatchType()` is .NET-only, documented as having no Java counterpart.
- `KafkaPrincipal` is now mutable through Java's `TokenAuthenticated(bool)`. Its "immutable" remark is gone, and equality still ignores the flag.
- In Equals, GetHashCode and ToString, the value types read their fields rather than their getters, because nullable flow analysis does not follow method calls. Outside the type, callers use `x.M() is { } v` (about 23 sites in grpc-server's Translate*.cs).
- Expressions qualify a type whose name a method shares (`Admin.UpgradeType.X`, `Resource.ResourceType.ANY`).
- New test helper `tests/Confluent.Kafka.UnitTests/Getters.cs`. The reflection shape tests moved from `GetProperty` to `GetMethod`. Some were renamed: `Getters_AreJavasShapes`, `P8ValueTypesDeclareNoProperties_AndTheOptionsNoMethods`, `TheValueTypes_ExposeJavasAccessorsAsMethods`. The bean-accessor allowlist now also has `KafkaPrincipal.GetName`/`GetPrincipalType` and `Uuid.GetMost`/`GetLeastSignificantBits`.
- The `<returns>` tags lost the "— Java's `x()`" suffix, because the summary already names the Java getter.
- In this checkout, 11 unit tests fail on both TFMs for environmental reasons. 8 are `PublicNamespaceLayoutTests.TheJavaClass_ExistsInTheKafkaSources` and 3 are `ErrorHierarchyTests`. They need Kafka 4.4 sources, and the `kafka` submodule here is at 4.3.1. Not related to S1.

## S2 notes

- 91 rows converted: 83 getter+setter pairs (one name, two overloads; the setter returns the type, `RemoveMembersFromConsumerGroupOptions.Reason(string)` returns `void`), 8 getter-only rows. `ListOffsetsOptions.IsolationLevel()` is a getter; Java's setter is replaced by the constructor `ListOffsetsOptions(IsolationLevel)`, which records the setter for the FFI replay.
- Setter parameter names are Java's, so `ArgumentNullException.ParamName` is Java's parameter name (`timeoutMs`, `brokerId`, `nodeId`, `protocolTypes`, ...) instead of `value`.
- New test helper `tests/Confluent.Kafka.UnitTests/SetterMethods.cs` (setters by reflection, `AssertGettersAndSetters`).
- Setter docs are a short "Sets X()" summary with the getter's text as the parameter description; the Last stage rewrites docs.

## S3 notes

- Converted 51 Result properties to Java's methods. The Java 4.4 comparison found exactly 52 deviating members before the change (the 51 plus `TopicMetadataAndConfig.HasMetadata`) and 0 after; 203 members match.
- D-a1: `HasMetadata` removed. Its only caller was a unit-test assert. The test still asserts that every accessor throws the stored exception, as Java's `ensureSuccess()` does.
- No hoisting needed: no nested type collides with a new method.
- Auto-properties became private readonly fields (`_binding`, `_exception`, `_values`, `_offset`, `_timestamp`, `_leaderEpoch`). `ToString` reads the fields.
- Nullable flow does not follow method calls. The grpc-server uses `x.M() is { } v`, `DeleteAclsResult.All()` uses `is { } exception`, and the P14 differential tests use `!` after `TopicIdValues()` / `TopicNameValues()`.
- Reflection shape tests moved from `GetProperty` to `Getters.Get(...).ReturnType`, with `Assert.Empty(GetProperties())` where the type now has none.
- `<returns>` tags drop the "— Java's <c>x()</c>" clause, following S1.
- PerfV2 and PerformanceCommon were not in the rewriter's projects: PerfV2 references the upstream NuGet package and PerformanceCommon does not reference the library. Neither uses an S3 member, and both build with 0 warnings.

## S4 notes

- Converted 12 properties: `IsDone()`, `IsCancelled()` and `IsCompletedExceptionally()` on `KafkaFuture<T>`, `KafkaFuture`, `AsyncKafkaFuture<T>` and `AsyncKafkaFuture`. Each method is the old property's body (`=> Impl.X`), so §C1 is unchanged: `IsDone()` asks the source whether it is done and settles nothing, and the other two settle a done handle through `ReadSource()`, which runs at most once. The internal `KafkaFutureImpl` state reads stay properties; they are not public, and keeping them leaves the settle logic untouched.
- The proof is `IsDone_OverADoneValueSource_RunsNothing_UntilASettlingReadRunsEachListenerOnce` (sync and async): `IsDone()` twice reads the source 0 times and runs no listener; then the first `IsCancelled()` / `IsCompletedExceptionally()` reads it once and runs each listener once on the reading thread; later reads leave the count at 1. `IsDone_OverAPendingValueSource_IsFalse_AndRunsNothing` passes too.
- Callers were found by the compiler (CS0428, CS1503, CS1662, CS8974 from 225 errors on 221 lines), not by a text pass, so no internal `Impl.IsDone` was touched. The one pattern use, grpc-server's `future is { IsDone: false }`, became `future is { } pending && !pending.IsDone()`. No soak, perf, PackageSmoke or README site used these members.
- The Java 4.4 comparison (`KafkaFuture` at `1156b2752`, with the `java.util.concurrent.Future` methods it declares: `cancel`, `get`, `get(long, TimeUnit)`, `isCancelled`, `isDone`): 42 matched, 0 properties left. The only gap is `toCompletionStage()` on the two sync structs, which `dotnet/CLAUDE.md` records as async-only. Before the change there were 26 findings (12 missing methods and 12 properties, plus those 2).
- `dotnet/CLAUDE.md`'s future section names these members without parentheses; it is left for the Last stage.

## S5 notes

Commit `fc16a04a6`. Converted 25 getters in 14 classes.
- The PLAN was wrong about where they live. 13 of the 14 classes (24 rows) are generated by `cargo xtask generate-error-codes` (`rust/xtask/src/error_hierarchy/csharp.rs`). Only `RecordDeserializationException` (8 rows) is hand-written.
- With the owner's approval, the C# emitter changed:
  - Every payload getter is now `private readonly T _field;` plus `public T Name() => _field;`.
  - `InvalidOffsetException.Partitions()` is abstract. It is overridden by `NoOffsetForPartitionException` and by `OffsetOutOfRangeException`, whose override is the key-set view of `_offsetOutOfRangePartitions`.
  - `RESERVED_MEMBERS` also refuses `GetBaseException`, `GetObjectData`, `GetType`, `GetHashCode` and `ToString`. A new emitter test covers it.
- The value-type stage had hand-edited `QuotaViolationException.cs` to `KafkaMetric.MetricName()` without changing the emitter, so the tree was already stale for `check-generated`. The emitter now writes `.MetricName()`.
- The generator reads `kafka/` (4.3.1 here), which lacks 4.4 classes, so it cannot run in place. I ran it in a scratch copy of HEAD with `kafka/` replaced by `git -C kafka archive 1156b2752`, then copied the 13 regenerated files back.
  - Python output did not change.
  - In that copy, `cargo test -p xtask` passes (345) and `check-generated` reports every generator up to date. The 11 environment failures pass there too.
- No getter collides with an inherited `Exception` member.
- New tests in `GeneratedErrorClassesTests`:
  - `PayloadGetters_AreJavasMethods_AndNotProperties`, 14 cases.
  - `NoPublicErrorClass_DeclaresAPublicProperty`.
- Java 4.4 comparison:
  - Before: 1 match and 50 findings (25 Java methods still properties, plus the same 25 counted again as public properties).
  - After: 26 matches (the 25 getters plus `ForGroupId`) and 0 findings.
  - Excluded as JVM-only: `QuotaViolationException.fillInStackTrace()` and its `toString()`; the latter is carried by the message.

## S6 notes

Commit `642d500c1`.
- `Serdes`: the seven properties became Java's static methods `String()`, `ByteArray()`, `Integer()`, `Long()`, `Double()`, `UUID()`, `Void()` (renames: `Int32` → `Integer`, `Int64` → `Long`, `Guid` → `UUID`, `Null` → `Void`). Each returns one shared stateless instance, where Java builds a new serde per call. Not added (.NET has no serde for them): `Short()`, `Float()`, `ByteBuffer()`, `Bytes()`, `Boolean()`, `ListSerde(..)`, `serdeFrom(..)` and the nested public serde classes. No other public serialization class has a static property.
- The ten constants are `public static readonly` fields. `Uuid`'s private reserved set now comes after `ZERO_UUID` and `ONE_UUID`, because static fields initialize in textual order. No caller mutates `KafkaPrincipal.ANONYMOUS`. D-d2 is confirmed: `TokenAuthenticated(bool)` returns `void`, as in Java.
- Callers: the compiler found 1229 error lines. A text pass over `Serdes.X` not followed by `(` made 1312 rewrites in .cs, .csproj and README.md. Crefs became `Serdes.X()`. `dotnet/CLAUDE.md` and `design/current/STATUS.md` still use the old names; they are left for the Last stage.
- Sweep, on net10.0, net8.0 and netstandard2.0: only `ConsumerRecords`2.Count` is left, kept because it implements `IReadOnlyCollection<T>.Count`. The guard test `PublicJavaMemberShapeTests` pins this.
- Java 4.4 comparison: the 10 constants plus `USER_TYPE` and `DEFAULT_CLUSTER_ID` (consts) match. 7 `Serdes` methods match, 7 are not added, and there are 0 extras and 0 properties.

## Review fixes (PR 322)

The review of "P1 Java methods that .NET made properties" found five shapes, all already on
`master`, where the method kept a .NET type or value Java does not have. The owner chose to follow
Java 4.4 for each. No Rust or C change.
- `NewTopic.NumPartitions()` and `ReplicationFactor()` return `int` and `short`, `-1` when unset,
  as Java's `orElse(CreateTopicsRequest.NO_NUM_PARTITIONS)` and `NO_REPLICATION_FACTOR`. The fields
  stay nullable, so `Equals` and `ToString` still tell unset from `-1`, as Java's `Optional`
  fields do.
- `UserScramCredentialUpsertion.Salt()` is never `null`. The three-argument constructors get a
  random salt from the core (`kafka_admin_UserScramCredentialUpsertion_with_bytes`, then `_salt`),
  as Java's call `generateRandomSalt()`; the binding generates none itself. The four-argument
  constructor throws `ArgumentNullException` for a `null` salt, as Java's `requireNonNull`.
- `DescribeFeaturesOptions.NodeId(int)` and `DescribeProducersOptions.BrokerId(int)` take Java's
  `int`. The getters stay `int?`, Java's `OptionalInt` (ledger 181 amended).
- `ConsumerRecords.Count()` is Java's `count()`, with an explicit `IReadOnlyCollection<T>.Count`.
  No public property is left (ledger 368 amended).
- `PartitionInfo.Replicas()`, `InSyncReplicas()` and `OfflineReplicas()` return `Node[]`, as
  Java's (proposed ledger 370). The `NewTopic` getters' sentinel is proposed ledger 371.

## Known gaps (not in this phase)

- .NET `Node` lacks four public members of Java 4.4's `Node`: `hasRack()`, `isEmpty()`, `idString()`
  and the static `noNode()`. It has `Id()`, `Host()`, `Port()`, `Rack()` and `IsFenced()`. The gap
  predates this phase, which changes shapes only and adds no members to `Node`.
