# COMMENTS.DONE.120 — resolved dotnet-critic comments, M22/P4

## S1 (`458f241d4`), resolved in fixup `12afe5b99`

1. **`KafkaPrincipal` — the token flag lost Java's `volatile`** (`dotnet/src/Confluent.Kafka/Common/Security/Auth/KafkaPrincipal.cs`, field `_tokenAuthenticated`, used by `TokenAuthenticated(bool)` and `TokenAuthenticated()`)
   - **What is wrong:** the new setter makes the principal mutable. The backing field is a plain `private bool`, so a write on one thread has no visibility guarantee for a reader on another.
     - `KafkaPrincipal.ANONYMOUS` is one shared static instance, so this is reachable without the user sharing anything themselves.
   - **Java evidence:** Java declares the field `private volatile boolean tokenAuthenticated;`, and `tokenAuthenticated(boolean)` / `tokenAuthenticated()` rely on it (`git -C kafka show 1156b2752:clients/src/main/java/org/apache/kafka/common/security/auth/KafkaPrincipal.java`).
   - **.NET precedent:** `Headers._isReadOnly` is `private volatile bool` for exactly this kind of cross-thread flag.
   - **Fix:** declare `private volatile bool _tokenAuthenticated;`. Equality and the hash stay unchanged.

2. **`KafkaPrincipal.TokenAuthenticated(bool)` and `TokenAuthenticated()` have no test** (`dotnet/tests/Confluent.Kafka.UnitTests/PublicAdminP7Tests.cs`, next to `KafkaPrincipal_EqualityIgnoresTokenAuthentication`)
   - **What is wrong:** S1 adds a new public mutator (PLAN.md D-d2 / L-2), but no .NET test calls it or reads the getter. `git grep TokenAuthenticated -- dotnet/tests` finds nothing.
     - The existing equality test builds two principals through the constructor. It never shows that a principal still equals itself, and keeps its hash, after the flag is flipped with the setter.
     - No shape test pins the setter's `void` return.
   - **Python evidence:** `python/test/unit/test_admin_common_types.py::test_kafka_principal_surface` asserts the default `False`, that the setter returns `None`, the read-back `True`, and equality with a `token_authenticated=False` principal after the flip.
   - **Fix:** add a fact mirroring that test. It should assert:
     - `new KafkaPrincipal("User", "alice").TokenAuthenticated()` is `false`;
     - after `p.TokenAuthenticated(true)`, the getter returns `true`;
     - `p` still equals `new KafkaPrincipal("User", "alice", false)`, with the same hash code;
     - `typeof(KafkaPrincipal).GetMethod("TokenAuthenticated", new[] { typeof(bool) })!.ReturnType == typeof(void)`.

3. **Doc comments — three broken `<returns>`, and the Java suffix was not stripped evenly** (low severity, doc only)
   - **Broken tags:** the generated `<returns>` wraps so that its closing period sits alone on a `///.</returns>` line. The rendered text reads "…did not report it ." with a stray space. Affected:
     - `dotnet/src/Confluent.Kafka/Admin/ConfigEntry.cs`, `Documentation()`
     - `dotnet/src/Confluent.Kafka/Admin/MemberDescription.cs`, `GroupInstanceId()`
     - `dotnet/src/Confluent.Kafka/Admin/ProducerState.cs`, `CurrentTransactionStartOffset()`
   - **Uneven stripping:** the S1 notes say the `<returns>` tags lost the "— Java's `x()`" suffix because the summary already names the Java getter. These `<returns>` still repeat it, often with the whole summary copied in:
     - `GroupListing.IsSimpleConsumerGroup()`, `MemberDescription.ConsumerId()`, `ProducerState.ProducerEpoch()`, `TransactionListing.State()`, `ConfigEntry.IsSensitive()`
     - `ClientQuotaAlteration.Op.Value()`, `DelegationToken.HmacAsBase64String()`, `ConsumerRecord.LeaderEpoch()`, `OffsetAndMetadata.LeaderEpoch()`, `OffsetAndTimestamp.LeaderEpoch()`
     - also, per the diff: `FeatureMetadata.FinalizedFeaturesEpoch()`, `LogDirDescription.TotalBytes()`, `Node.IsFenced()`
     - Find them with `git grep -nE "<returns>.*(— Java's|\(Java's)" -- dotnet/src`.
   - **Fix:**
     - Rewrap the three broken tags so the period ends the text line.
     - Cut the Java-name clause, and any copied remark, from the listed `<returns>`, as for the other ~150 getters. The summaries keep the Java reference.

**Resolution:**
1. `_tokenAuthenticated` is now `private volatile bool`.
2. Added `PublicAdminP7Tests.KafkaPrincipal_TokenAuthenticated_DefaultsToFalse_AndItsSetterLeavesEqualityAlone`. It asserts:
   - the default is false;
   - the value reads back as true after `TokenAuthenticated(true)`;
   - the principal still equals `new KafkaPrincipal("User", "alice", false)`, and its hash code is unchanged and equal to that principal's;
   - the setter returns `void`.
3. Rewrapped the three `///.</returns>` tags (`ConfigEntry.Documentation`, `MemberDescription.GroupInstanceId`, `ProducerState.CurrentTransactionStartOffset`). Cut the Java clause from all 13 remaining S1 `<returns>` tags:
   - `ConfigEntry.IsSensitive`, `FeatureMetadata.FinalizedFeaturesEpoch`, `GroupListing.IsSimpleConsumerGroup`, `LogDirDescription.TotalBytes`, `MemberDescription.ConsumerId`
   - `ProducerState.ProducerEpoch`, `TransactionListing.State`, `Node.IsFenced`, `ClientQuotaAlteration.Op.Value`, `DelegationToken.HmacAsBase64String`
   - `ConsumerRecord.LeaderEpoch`, `OffsetAndMetadata.LeaderEpoch`, `OffsetAndTimestamp.LeaderEpoch`

   A multi-line scan of every `<returns>` in the files S1 touched finds no other S1 getter with a Java clause. The remaining hits are `IAdmin`/`IAsyncAdmin` RPC methods, outside S1, plus `ConfigEntry.IsDefault`/`Type()`, which state how Java derives the value.

## S2 (`109a1120e`), resolved in fixup `4b387774c`

1. **Five Options classes give their boolean getters names Java does not have (L-3).**
   - Files and members:
     - `src/Confluent.Kafka/Admin/CreateTopicsOptions.cs`: `ValidateOnly()` and `RetryOnQuotaViolation()`.
     - `src/Confluent.Kafka/Admin/AlterConfigsOptions.cs`: `ValidateOnly()`.
     - `src/Confluent.Kafka/Admin/CreatePartitionsOptions.cs`: `RetryOnQuotaViolation()`. Its `ValidateOnly()` getter is correct, because Java has `validateOnly()` there.
     - `src/Confluent.Kafka/Admin/DeleteTopicsOptions.cs`: `RetryOnQuotaViolation()`.
     - `src/Confluent.Kafka/Admin/ListTopicsOptions.cs`: `ListInternal()`.
   - What is wrong:
     - L-3 says the getters keep Java's getter names, and the new rule says every Java method becomes a C# method with Java's exact name.
     - In these classes Java's getter is not named after the setter. The commit nonetheless made the getter an overload of the setter.
     - As a result, `shouldValidateOnly()`, `shouldRetryOnQuotaViolation()` and `shouldListInternal()` have no C# counterpart.
     - There is also a getter that Java does not have, for example `CreateTopicsOptions.validateOnly()`.
   - Evidence:
     - Java 4.4 (`git -C kafka show 1156b2752:clients/src/main/java/org/apache/kafka/clients/admin/<X>.java`):
       - `CreateTopicsOptions` declares `validateOnly(boolean)`, `shouldValidateOnly()`, `retryOnQuotaViolation(boolean)` and `shouldRetryOnQuotaViolation()`.
       - `AlterConfigsOptions` declares `shouldValidateOnly()` and `validateOnly(boolean)`.
       - `CreatePartitionsOptions` declares `validateOnly()`, `validateOnly(boolean)`, `retryOnQuotaViolation(boolean)` and `shouldRetryOnQuotaViolation()`.
       - `DeleteTopicsOptions` declares `retryOnQuotaViolation(boolean)` and `shouldRetryOnQuotaViolation()`.
       - `ListTopicsOptions` declares `listInternal(boolean)` and `shouldListInternal()`.
       - By contrast, `AlterClientQuotasOptions` and `UpdateFeaturesOptions` do have `validateOnly()`, so they are correct as they are.
     - Python stubs under `python/confluent_kafka/admin/` use Java's names: `should_validate_only` in `create_topics_options.pyi` and `alter_configs_options.pyi`, `should_retry_on_quota_violation` in `create_topics_options.pyi`, `create_partitions_options.pyi` and `delete_topics_options.pyi`, and `should_list_internal` in `list_topics_options.pyi`.
     - The C ABI that the tests already declare uses the same names: `CreateTopicsOptionsShouldValidateOnly`, `CreateTopicsOptionsShouldRetryOnQuotaViolation`, `AlterConfigsOptionsShouldValidateOnly`, `CreatePartitionsOptionsShouldRetryOnQuotaViolation`, `DeleteTopicsOptionsShouldRetryOnQuotaViolation` and `ListTopicsOptionsShouldListInternal`.
     - The docs of `ListTopicsOptions` and `AlterConfigsOptions` already mention `shouldListInternal()` and `shouldValidateOnly()`.
   - Fix:
     - Rename the getters to `ShouldValidateOnly()`, `ShouldRetryOnQuotaViolation()` and `ShouldListInternal()`.
     - Keep the setters `ValidateOnly(bool)`, `RetryOnQuotaViolation(bool)` and `ListInternal(bool)` unchanged. The `Setters.Ran(nameof(...))` names and the `case nameof(...)` labels in `NativeAdminClient.*` stay on the setter name, so the replay does not change.
     - Update the getter callers:
       - `Internal/NativeAdminClient.Topics.cs`, `.Configs.cs` and `.Partitions.cs`;
       - the grpc-server `AdminServiceImpl` / `AsyncAdminServiceImpl` / `TranslateAdmin`, where they read the getters;
       - the `cref`s in `IAdmin` / `IAsyncAdmin` / `TopicListing`;
       - the unit tests.
     - Update the shape tests' expected getter lists in `SetterMethods.AssertGettersAndSetters`, because for these classes the getter names no longer equal the setter names.
     - Update the java column of `inventory.csv` for these members.

2. **`ListGroupsOptions` setters declare non-nullable parameters, though Java and Python accept null (and treat it as empty).**
   - File and members: `src/Confluent.Kafka/Admin/ListGroupsOptions.cs`, `InGroupStates(IReadOnlyCollection<GroupState>)`, `WithProtocolTypes(IReadOnlyCollection<string>)` and `WithTypes(IReadOnlyCollection<GroupType>)`.
   - What is wrong:
     - The bodies treat `null` as the empty set, and so does the getter doc ("`null` and an empty collection both store the empty set").
     - However, the parameter is annotated non-nullable.
     - The tests therefore have to write `options.InGroupStates(null!)`, `WithTypes(null!)` and `WithProtocolTypes(null!)` (in `PublicAdminListGroupsOptionsTests` and `AdminP5SubmitArgumentTests`) to exercise a documented, supported input.
     - Before this commit, the single property type forced one annotation for both `get` (never null) and `set`. Now that getter and setter are separate methods, the setter can carry the precise annotation that `dotnet/CLAUDE.md` asks for ("annotate precisely").
   - Evidence:
     - Java 4.4 `ListGroupsOptions.inGroupStates`, `withProtocolTypes` and `withTypes` each begin `(x == null || x.isEmpty()) ? Set.of() : Set.copyOf(x)`. That is a non-throwing null branch, so null has a meaning.
     - Python declares `in_group_states(self, *, group_states: Iterable[GroupState] | None)`, and declares `with_protocol_types` and `with_types` the same way.
   - Fix:
     - Declare the three parameters `IReadOnlyCollection<T>?`, and make the private `CopyOf` / `CopyOfProtocolTypes` helpers accept a nullable argument.
     - Keep the getters non-nullable.
     - Drop the `null!` suppressions from the tests, which then pass a plain `null`.

**Resolution:**
1. The six getters take Java's names: `CreateTopicsOptions.ShouldValidateOnly()` and `ShouldRetryOnQuotaViolation()`, `AlterConfigsOptions.ShouldValidateOnly()`, `CreatePartitionsOptions.ShouldRetryOnQuotaViolation()` (its `ValidateOnly()` stays), `DeleteTopicsOptions.ShouldRetryOnQuotaViolation()` and `ListTopicsOptions.ShouldListInternal()`.
   - The setters, the `Setters.Ran(nameof(...))` names and the `case nameof(...)` labels are unchanged.
   - Callers updated: `NativeAdminClient.Topics.cs`, `.Configs.cs` and `.Partitions.cs`; the crefs in `IAdmin`, `IAsyncAdmin`, `TopicListing` and the classes' own `Equals` / `GetHashCode` docs; the unit tests. grpc-server, its tests, soak and perf call only the setters, so they needed no change (each builds with 0 warnings).
   - `SetterMethods.AssertGettersAndSetters(type, getters:, setters:)` is a new form for these classes; `AlterConfigsOptions`, `ListTopicsOptions` and `CreatePartitionsOptions` use it, and `CreateTopicsOptions` and `DeleteTopicsOptions` gained a shape assert with it.
   - A script compared every Java 4.4 public zero- and one-argument method with the C# methods for all 48 S2 types: these six are the only getters whose Java name differs from the setter's.
   - `inventory.csv`: the java, proposed, renamed and python columns of the six rows are corrected (uncommitted).
2. `InGroupStates`, `WithProtocolTypes` and `WithTypes` take `IReadOnlyCollection<T>?`, the `CopyOf` / `CopyOfProtocolTypes` helpers accept null, the `<param>` docs mention null, and the getters stay non-nullable. The tests pass a plain `null`; the two `null!` left are null *elements*, which Java's `Set.copyOf` rejects.
   - No other S2 setter needed the change: `ListGroupsOptions` is the only class whose Java setters have a non-throwing null branch. The plain-assignment setters are already nullable (`CreateDelegationTokenOptions.Owner`, `DescribeDelegationTokenOptions.Owners`, `ListConsumerGroupOffsetsSpec.TopicPartitions`, `ListTransactionsOptions.FilterOnTransactionalIdPattern`, `NewTopic.Configs`, `RemoveMembersFromConsumerGroupOptions.Reason`, `TimeoutMs`), except `CreateDelegationTokenOptions.Renewers`, whose null Java's admin client dereferences, and `ListTransactionsOptions.FilterStates` / `FilterProducerIds`, which Java's `new HashSet<>(null)` rejects.

# S5 — review of `fc16a04a6` (errors expose Java's payload getters as methods)

1. **`rust/xtask/src/error_hierarchy/csharp.rs`, `RESERVED_MEMBERS`: the list does not match what it says it guards** (low severity; the generated output for Kafka 4.4 is not affected).
   - **What is wrong.** The doc comment says the list holds the "Members of `System.Exception`, `object` and `KafkaException` a getter method must not hide", and `getters()` refuses a Java getter whose PascalCased name is in it. Now that a getter is a zero-argument method, the list is incomplete in one direction and too broad in the other:
     - **Missing: `TypeAndMessage`.** It is the one zero-argument method `KafkaException` declares itself (`internal virtual string TypeAndMessage()`, which also implements `IExceptionText.TypeAndMessage()`), and the emitter itself writes `internal override string TypeAndMessage()` into classes with a Java `toString()` override, such as `QuotaViolationException`. A Java getter `typeAndMessage()` would pass the guard. The emitter would then write `public T TypeAndMessage() => _x;`, which fails to compile with CS0114 (hides the inherited virtual, an error under `TreatWarningsAsErrors` in `dotnet/Directory.Build.props`), or with CS0111 (duplicate member) in a class that also gets the override. The refusal exists to report this at generation time instead of at the C# build.
     - **Missing: `MemberwiseClone`.** This is `object`'s protected zero-argument method. A public `MemberwiseClone()` getter would hide it (CS0108, also an error under `TreatWarningsAsErrors`).
     - **Too broad: `GetObjectData`.** `Exception.GetObjectData(SerializationInfo, StreamingContext)` takes two parameters, so a zero-argument `GetObjectData()` is an overload. It hides nothing and compiles, yet the guard refuses it. The commit message states the same wrong claim ("refuses a getter that would hide … GetObjectData").
     - **Stale: `IsRetriable`.** Nothing in `dotnet/src` declares an `IsRetriable` member any more (`git grep -n "IsRetriable\b" -- dotnet/src` finds nothing), so the entry guards nothing.
   - **Evidence.** `dotnet/src/Confluent.Kafka/Common/KafkaException.cs` (`TypeAndMessage`, `IExceptionText.TypeAndMessage`), `dotnet/src/Confluent.Kafka/Internal/IExceptionText.cs`, and the generated `QuotaViolationException.cs` (`internal override string TypeAndMessage()`). C# hides a method by name and signature, so only a zero-argument inherited method can collide with a getter. Inherited properties (`Message`, `Data`, …) collide by name alone.
   - **Fix.** Add `TypeAndMessage` and `MemberwiseClone` to `RESERVED_MEMBERS`. Drop `GetObjectData` (or keep it with a comment saying it is a deliberate name-level reservation rather than a hide) and drop `IsRetriable`. Correct the doc comment. Extend `a_getter_named_like_an_inherited_method_stops_generation`, or add a test, so it also covers `typeAndMessage`.

**Resolution** (fixup `1a552a295`):
1. `RESERVED_MEMBERS` changed:
   - It gains `TypeAndMessage` and `MemberwiseClone`.
   - It loses `GetObjectData` (a parameterless overload hides nothing) and `IsRetriable` (`git grep -n "IsRetriable\b" -- dotnet/src` finds nothing).
   - Its doc comment now says it holds the properties of `System.Exception` and `KafkaException`, which clash by name, and the parameterless methods of `object`, `System.Exception` and `KafkaException`, which a getter method would hide.
   - `a_getter_named_like_an_inherited_method_stops_generation` loops over `getType` and `typeAndMessage`.
   - The fixup's body corrects the original commit's `GetObjectData` claim.
   - Gates:
     - In this checkout, `cargo xtask format-check` and `cargo clippy -p xtask --all-targets -- -D warnings` are clean.
     - In a fresh 4.4 scratch copy built from the fixup:
       - `cargo test -p xtask` passes 345 tests.
       - After `cargo xtask generate-error-codes`, `git status` is clean, so no generated file changed.
       - `cargo xtask check-generated` reports every generator up to date.
     - No .NET file changed.

## S5 re-review of fixup `1a552a295`

2. **`rust/xtask/src/error_hierarchy/csharp.rs`, `RESERVED_MEMBERS`: the members of the .NET built-ins the emitter roots the JDK classes on are still unguarded** (low severity; no Java 4.4 class hits it).
   - **What is wrong.** The emitter writes the root JDK classes on top of .NET built-ins (`CSHARP_JDK_BASES`):
     - `IllegalArgumentException : System.ArgumentException`, which adds the virtual property `ParamName`;
     - `IndexOutOfBoundsException : System.ArgumentOutOfRangeException`, which adds the virtual property `ActualValue`.
     
     A Kafka class under either root with a Java getter `paramName()` or `actualValue()` would pass the guard. The emitter would then write `public T ParamName() => _x;`, which hides the inherited property (CS0108). That is an error under `TreatWarningsAsErrors`, so the failure would show at the C# build instead of at generation, which is the case this list exists to catch. `System.Exception`'s `SerializeObjectState` event is also a member that clashes by name, and the list leaves it out.
   - **Evidence.**
     - The `CSHARP_JDK_BASES` entries `java.lang.IllegalArgumentException` → `System.ArgumentException` and `java.lang.IndexOutOfBoundsException` → `System.ArgumentOutOfRangeException`.
     - The generated `dotnet/src/Confluent.Kafka/IllegalArgumentException.cs` and `IndexOutOfBoundsException.cs`.
     - The other built-in bases add no public instance members of their own: `System.InvalidOperationException`, `System.TimeoutException`, `System.NotSupportedException` and `System.Exception`.
     - Today the only Kafka classes under a JDK root are `BufferExhaustedException` (on `TimeoutException`) and `CorrelationIdMismatchException` (on `IllegalStateException`), so nothing is generated wrongly now.
   - **Fix.**
     - Add `ParamName`, `ActualValue` and `SerializeObjectState` to `RESERVED_MEMBERS`. Alternatively, reserve `ParamName` and `ActualValue` only below the two roots that inherit them.
     - Extend the doc comment to name the built-in bases, for example "… and of the .NET built-ins in `CSHARP_JDK_BASES`".
     - Add `paramName` to the refusal test's loop, or add a case under a class rooted on `IllegalArgumentException`.

**Resolution** (fixup `95f9dbfbb`):
2. I enumerated every base the emitter writes by reflection. Two `dotnet fsi` scripts with `MetadataLoadContext` are in the session scratchpad at `refl/bases.fsx` and `refl/kafka.fsx`, with their output in `bases.out` and `kafka.out`.
   - **What was covered:**
     - `object`, `System.Exception` and every built-in of `CSHARP_JDK_BASES`: `InvalidOperationException`, `ArgumentException`, `TimeoutException`, `NotSupportedException` and `ArgumentOutOfRangeException`.
     - Each of those in the net10.0, net8.0 and netstandard2.0 reference assemblies, listing public and protected members, instance and static, inherited included.
     - `KafkaException` and the generated JDK roots in the built `Confluent.Kafka.dll`, counting internal members too, since the generated classes share the assembly.
   - **What it found:**
     - The three frameworks agree.
     - The only properties, events and parameterless methods missing from the list were:
       - `SerializeObjectState`, `System.Exception`'s event;
       - `Finalize()`, `object`'s protected method;
       - `ParamName`, from `ArgumentException`, inherited by `ArgumentOutOfRangeException`;
       - `ActualValue`, from `ArgumentOutOfRangeException`.
     - The built-ins add no fields and no nested types.
     - `KafkaException` declares `Code`, `FfiId` and `TypeAndMessage()`, all already reserved, plus statics that take parameters.
     - The generated JDK roots declare nothing visible.
   - **Fix:**
     - All four names are added to `RESERVED_MEMBERS` and reserved under every root. `git -C kafka grep` at `1156b2752` finds no Java 4.4 getter named `paramName`, `actualValue`, `finalize` or `serializeObjectState`.
     - The doc comment names `object`, `System.Exception`, `KafkaException` (internal, but visible in the assembly) and the built-ins of `CSHARP_JDK_BASES`, and what `ArgumentException` and `ArgumentOutOfRangeException` add.
     - The refusal test gains `paramName` → `ParamName`.
   - **Gates:**
     - In this checkout, `cargo xtask format-check` and `cargo clippy -p xtask --all-targets -- -D warnings` are clean.
     - In a fresh 4.4 scratch copy from `95f9dbfbb`:
       - `cargo test -p xtask` passes 345 tests.
       - After `generate-error-codes`, `git status` is clean.
       - `check-generated` reports every generator up to date.
     - No .NET file changed.

## S6 (`642d500c1`), resolved in fixup `5367273cc`

1. **`DisconnectException.Instance` and `CoordinatorNotAvailableException.Instance` (and their emitter in `rust/xtask/src/error_hierarchy/csharp.rs`): two Java `public static final` constants on public types still have a .NET name** (low severity; outside the ten L-4 rows, but inside L-4's rule and the commit's "constants follow Java's shapes").
   - **What is wrong.** Java 4.4 (`1156b2752`) declares `public static final DisconnectException INSTANCE` and `public static final CoordinatorNotAvailableException INSTANCE`, both on `@InterfaceAudience.Public` classes. .NET emits them as `public static readonly X Instance`. The field shape is right; the name is not Java's.
   - **Evidence.**
     - `dotnet/CLAUDE.md`'s idiom-map row for ledger 285 says a `public static final` field takes "Java's own name and spelling" (`Uuid.ZERO_UUID`, `ResourceType.ANY`). Python's rule is the same (`DisconnectError.INSTANCE`).
     - `Instance` was a recorded .NET naming form in M22/P1 (`design/history/M22/P1-error-hierarchy-parity/PLAN.md`, §7: "properties for getters, `Instance` for `INSTANCE`"). This phase retires the first item of that list on the "align with Java, then Python" principle (§0). The second item is the same kind of choice and was not revisited. The deviation is recorded nowhere in `dotnet/CLAUDE.md`, whose ledger 285 row it contradicts.
     - The S6 Java comparison (`compare.py`) only scanned `Uuid`, `KafkaPrincipal`, the three filters, `MockAdminClient` and `Serdes`, so it could not see these two. Every other public `const` or `static readonly` field in `dotnet/src` keeps Java's name: `ClientQuotaEntity.USER`/`CLIENT_ID`/`IP`, `ResourcePattern.WILDCARD_RESOURCE`, `ConsumerRecord.NO_TIMESTAMP`, `RecordMetadata.UNKNOWN_PARTITION`, `KafkaPrincipal.USER_TYPE` and `MockAdminClient.DEFAULT_CLUSTER_ID` all keep Java's names.
   - **Fix.** Either:
     - rename both to `INSTANCE` in the emitter, update its test (`assert!(cs.contains("… Instance = new CoordinatorNotAvailableException();"))`), regenerate, and add them to `PublicJavaMemberShapeTests.JavasConstants_AreStaticReadonlyFields`; or
     - keep `Instance` and record the deviation, with its reason, in the Last stage's update of the ledger 285 row.
   
     The first fits L-4 and Python. The owner may prefer the second, since P1 chose `Instance` deliberately.
   - **Resolution (coordinator's decision under the owner's delegation: Java first):**
     - The emitter (`rust/xtask/src/error_hierarchy/csharp.rs`) writes a Java singleton under Java's own name, so both are `INSTANCE`. Its test is now `a_singleton_keeps_javas_name_and_a_private_constructor`.
     - The two files were regenerated in a fresh 4.4 scratch copy of HEAD (`kafka/` from `git -C kafka archive 1156b2752`). Only those two files changed, and the Python output is unchanged. No caller used `Instance`.
     - To close the gap, every public static field, const and enum member of every exported type was compared by reflection with the Java 4.4 class it translates. There are 0 name mismatches left. `PublicJavaMemberShapeTests.ThePublicStaticFields_AreExactlyJavasConstants_AllReadonly` now pins the 21 non-enum names and covers the two singletons.
     - `SerdesTests`: the `Int32_*`, `Int64_*`, `Guid_*` and `Null_*` methods are renamed `Integer_*`, `Long_*`, `UUID_*` and `Void_*`.

## Last stage (`1c53e75a6` and the uncommitted .md edits), resolved in the uncommitted .md edits

1. **`dotnet/CLAUDE.md` §3 idiom map, the ledger 365 row: the name-shadowing sentence describes a qualification the code never makes.**
   - The row says: "Where a method is named like its return type (`TopicPartition TopicPartition()`), code inside the class qualifies the type (`Common.TopicPartition`), since C# binds the name to the method group".
   - C# binds the simple name to the method group only in an expression. In a type position it still finds the type.
     - `AbortTransactionSpec` and `RecordDeserializationException` declare `TopicPartition TopicPartition()` and use `TopicPartition` unqualified as a type.
     - `Common.TopicPartition` appears nowhere in `dotnet/src`.
   - The qualifications the code does make are member accesses in expressions: `Admin.UpgradeType.UPGRADE` in `FeatureUpdate.cs`, and `Resource.ResourceType.ANY` in `ResourcePattern.cs` and `ResourcePatternFilter.cs`.
   - As written, the rule sends the next Actor to qualify type positions that need nothing, and it does not say that expressions need it.
   - **Fix:** "Where a method is named like a type (`UpgradeType UpgradeType()`), an expression inside the class that reaches the type's members qualifies it (`Admin.UpgradeType.UPGRADE`), since C# binds the simple name to the method group there; a type position needs no qualification."
   - **Resolution:** verified first: `AbortTransactionSpec` and `RecordDeserializationException` declare `TopicPartition TopicPartition()` and use the type unqualified, `Common.TopicPartition` is absent from `dotnet/src`, and the only qualifications are `Admin.UpgradeType.*` in `FeatureUpdate.cs` and `Resource.ResourceType.ANY` in `ResourcePattern.cs` / `ResourcePatternFilter.cs`. The ledger 365 row now says: where a method is named like a type (`UpgradeType UpgradeType()`), an expression inside the class that reaches the type's members qualifies the type (`Admin.UpgradeType.UPGRADE`, `Resource.ResourceType.ANY`), since C# binds the simple name to the method group there; a type position needs no qualification (`TopicPartition TopicPartition()`). The same wording is fixed in PLAN.md's D-d3 row and §5 D-d item 3. No other edited file carried it.

2. **`HasRack()` is cited as an existing .NET method, but the .NET `Node` has none.**
   - It appears in three places:
     - `dotnet/CLAUDE.md` idiom map, the ledger 365 row: "(`Topic()`, `IsDone()`, `GetName()`, `HasRack()`, …)";
     - `dotnet/design/current/STATUS.md`, the new top entry: "… `Uuid.GetMostSignificantBits()`, `HasRack()`";
     - `dotnet/design/history/M22/P4-getters-as-methods/PLAN.md:121`: "`HasRack()` stay[s] as Java names them".
   - `dotnet/src/Confluent.Kafka/Common/Node.cs` has only `Id()`, `Host()`, `Port()`, `Rack()` and `IsFenced()`. Java 4.4's `Node` also has `hasRack()`, `isEmpty()`, `idString()` and `noNode()`. That gap predates this change and is not filed here.
   - The STATUS text reads as a record of what the binding now has, so a reader takes `HasRack()` to exist.
   - **Fix:** use an existing `has` example, `RecordMetadata.HasOffset()` / `HasTimestamp()`. Alternatively, add `Node`'s four missing members, or record them as a known gap.
   - **Resolution:** verified that `Node.cs` has only `Id()`, `Host()`, `Port()`, `Rack()` and `IsFenced()`, that Java 4.4's `Node` (`1156b2752`) also has `hasRack()`, `isEmpty()`, `idString()` and `noNode()`, and that `RecordMetadata.HasOffset()` exists in .NET and `hasOffset()` in Java 4.4. `HasRack()` is replaced by `RecordMetadata.HasOffset()` (and `hasRack()` by `hasOffset()`) in the `dotnet/CLAUDE.md` row, the STATUS top entry and PLAN.md L-1. `Node` is unchanged; PLAN.md gains a "Known gaps (not in this phase)" section recording the four missing members.

3. **`dotnet/CLAUDE.md` idiom map, the ledger 369 row ("a .NET-only member | none, except … `MatchType()`"), is broader than the decision and contradicts the same file.**
   - The same `CLAUDE.md` keeps other .NET-only members:
     - `MockProducer.HistoryCount()`: "the .NET-only method `HistoryCount()`", because the ABI gives a count, not `history()`;
     - `KafkaFuture.Get(CancellationToken)`: "is added (no Java twin)".
   - PLAN D-a scoped the decision to .NET-only *getters/properties*, not to every member.
   - Read literally, the row makes both of those violations of ledger 369.
   - **Fix:** scope the row to "a .NET-only getter or property", or add "beyond those recorded elsewhere in this file (`HistoryCount()`, `Get(CancellationToken)`)".
   - **Resolution:** the ledger 369 row's first column is now "a .NET-only getter or property", matching PLAN D-a, so `HistoryCount()` and `Get(CancellationToken)` no longer fall under it.

4. **`dotnet/.claude/agents/dotnet-critic.md` line 37 contradicts the edited line 25.**
   - Line 25 now says "no `Async` suffix, the interface carries the async distinction". That matches the code: the only `…Async` public methods are Java's `commitAsync` and .NET's `DisposeAsync`.
   - Line 37 ("What NOT to report") still lists "the `Async` suffix" among the settled idioms the rulebook allows.
   - The two lines tell the Critic opposite things about the same idiom.
   - **Fix:** line 37 → "the `Async` prefix on the async types (`IAsyncProducer`, `AsyncKafkaConsumer`)".
   - Optional, same line 25: "never `KafkaException`" → "never the bare `KafkaException`". Java's own preconditions throw `KafkaException` subclasses (`InvalidGroupIdException`, `ConfigException`), which the binding raises as the generated classes.
   - **Resolution:** line 37 now lists "the `Async` prefix on the async types (`IAsyncProducer`, `AsyncKafkaConsumer`)"; line 25 says "never the bare `KafkaException`". The STATUS top entry's "Docs." list now names `dotnet-actor.md` beside `dotnet-critic.md` (the nit under "Checked and correct").
