# Plan: Java's overloads instead of `options = null` on the admin client (M23/P6)

Status: **done 2026-11-13** (S0–S6; evidence in `D/design/current/STATUS.md`, review record in
`COMMENTS.DONE.128.md` beside this plan; R1 handed to the main session as a patch, R2 a proposal). **Approved
2026-11-13** (Pranav, main session, relayed by the coordinator). D1, D2, D6 confirmed. D3: option C,
built the C-layer way ("option C is fine since it follows emanueles rule to most extent"). D4: "pass the null through
and let the C layer decide". D5: not answered; the narrow scope applies as the default and Pranav can still override
it. D7: "apply R1" (R2 stays a proposal; not applied). Manager N=128 (N=127 is in use: `dotnet/COMMENTS.127.md`,
dotnet-critic 127 on the security-review fixes, branch `prashah_dev_dotnet_security_review_fixes`). Branch
`prashah_dev_dotnet_admin_options_overloads`, from `master` `93d068b97` (#337). Mode A: .NET only. No Rust, C ABI,
header, Python or proto change. Decision page: "Admin's no-options default methods in .NET" (Confluence 6364168257,
v4): **option 1 now**; a default Java adds to `Admin` later follows option 2 (`dotnet/CLAUDE.md` §3, Java names rule 10).

Path prefixes: `J/` = `kafka/clients/src/main/java/org/apache/kafka/clients/admin/`, `JM/` =
`kafka/clients/src/testFixtures/java/org/apache/kafka/clients/admin/MockAdminClient.java`, `P/` =
`python/confluent_kafka/admin/`, `N/` = `dotnet/src/Confluent.Kafka/`, `NT/` = `dotnet/tests/Confluent.Kafka.UnitTests/`,
`D/` = `dotnet/`, `AL` = `D/analyzers/java-name-allowlist.txt`, `RF/` = `rust/src/ffi/`. Line numbers were read at
`93d068b97`.

## 1. Summary

`IAdmin` declares 47 RPCs as one method with a trailing `XxxOptions? options = null`; Java declares each as a pair, a
`default xxx(args)` that passes `new XxxOptions()` and an abstract `xxx(args, options)`. The same 47 signatures are on
`IAsyncAdmin`, `KafkaAdminClient`, `AsyncKafkaAdminClient`, `MockAdminClient` and `AsyncMockAdminClient`. This phase
declares Java's overload set on both interfaces with no optional parameter, writes each short form in the four
clients as a one-line forward, allowlists the 98 CKRDA0013 `default` findings this adds, fixes the callers, and makes
an explicit `null` options behave as in Java and Python by passing it to the C layer (D3, D4).

| | Today (each of the 6 types) | After |
|---|---|---|
| RPC signatures with an optional parameter | 47 (50 optional parameters: 47 `options`, plus `configResourceTypes`, `partitions`, `users`) | 0 |
| RPC members on each interface | 49 (47 + `ListPartitionReassignments(options)` + `RemoveMembersFromConsumerGroup`) | 98 (49 short forms added) |
| `IAdmin` / `IAsyncAdmin` public instance methods (`PublicAdminP14SurfaceTests`) | 51 | 100 |
| Forms Java lacks | `ListConfigResources(types)`, `ListConfigResources(options: o)`, `DescribeUserScramCredentials(options: o)` | gone |
| CKRDA0013 `default` keys in `AL` | 12 | 110 (+49 per interface) |
| An explicit `null` options | the core's defaults | `ArgumentNullException` "options must not be null" from the C layer, except where Java does not read it (§4.1) |

## 2. C# ideas this plan uses (one line each)

- **Optional parameter** (`options = null`): the compiler writes the default into the caller's call. Python: a keyword
  default. Rust has none.
- **Overloads**: two methods of one name; the compiler picks one from the arguments. `M(null)` with two one-argument
  overloads is CS0121 (ambiguous).
- **`cref` without a parameter list** (`<see cref="IAdmin.CreateTopics"/>`): fine for a single method, CS0419 (a
  warning, an error here through `TreatWarningsAsErrors`) once the name has two overloads.
- **Nullable annotation** (`CreateTopicsOptions` vs `CreateTopicsOptions?`): a compile-time hint only; passing a
  literal `null` to a non-annotated parameter is CS8625, an error here.
- **`<inheritdoc/>`**: copies the interface member's docs onto the class member (`dotnet/CLAUDE.md` Comments rule 1).

## 3. Inventory (read-only research)

### 3.1 The 47 signatures and Java's overloads

Same 47 on all six types (the `<X>Options? options = null` lists of the six files are identical). Java's overloads
are the `method org.apache.kafka.clients.admin.Admin` lines of `D/analyzers/java-api.txt` (lines 73–205).

| # | .NET today (`IAdmin` line) | Java forms (d = `default`, a = abstract) | .NET after |
|---|---|---|---|
| 1 | `CreateTopics(newTopics, options)` :84 | d `(Collection)`, a `(…, CreateTopicsOptions)` | short + long |
| 2 | `DeleteTopics(TopicCollection topics, options)` :116 | d `(TopicCollection)`, a `(…, options)` | short + long |
| 3 | `DeleteTopics(IReadOnlyCollection<string> topics, options)` :142 | d `(Collection)`, **d** `(Collection, options)` | short + long |
| 4 | `DescribeTopics(TopicCollection topics, options)` :175 | d, a | short + long |
| 5 | `DescribeTopics(topicNames, options)` :201 | d `(Collection)`, **d** `(Collection, options)` | short + long |
| 6 | `ListTopics(options)` :220 | d `()`, a | short + long |
| 7–9 | `CreatePartitions` :251, `DeleteRecords` :283, `DescribeCluster(options)` :307 | d, a | short + long |
| 10 | `ListConfigResources(configResourceTypes = null, options = null)` :335 | d `()`, a `(Set, options)` | `()` + `(configResourceTypes, options)`; `(types)` dropped |
| 11–15 | `DescribeConfigs` :369, `IncrementalAlterConfigs` :405, `DescribeLogDirs` :429, `AlterReplicaLogDirs` :454, `DescribeReplicaLogDirs` :478 | d, a | short + long |
| 16 | `ElectLeaders(electionType, partitions?, options)` :519 | d `(ElectionType, Set)`, a | short + long; `partitions` stays nullable (Java: null = all) |
| 17–19 | `AlterPartitionReassignments` :557, `AlterConsumerGroupOffsets` :592, `DeleteConsumerGroupOffsets` :626 | d, a | short + long |
| 20 | `ListPartitionReassignments(partitions = null, options = null)` :665, plus `(options)` :689 | d `()`, d `(options)`, d `(Set)`, d `(Set, options)`, a `(Optional<Set>, options)` | `()`, `(options)`, `(partitions)`, `(partitions, options)` |
| 21–24 | `ListOffsets` :735, `ListGroups(options)` :771, `DescribeConsumerGroups` :804, `DescribeClassicGroups` :838 | d, a | short + long |
| 25 | `ListConsumerGroupOffsets(groupId, options)` :868 | d `(String)`, **d** `(String, options)` | short + long |
| 26 | `ListConsumerGroupOffsets(groupSpecs, options)` :905 | d `(Map)`, a | short + long |
| 27–32 | `DeleteConsumerGroups` :931, `CreateAcls` :1002, `DeleteAcls` :1036, `DescribeAcls` :1066, `DescribeClientQuotas` :1096, `AlterClientQuotas` :1129 | d, a | short + long |
| 33 | `DescribeUserScramCredentials(users = null, options = null)` :1155 | d `()`, d `(List)`, a `(List, options)` | `()`, `(users)`, `(users, options)` |
| 34–47 | `AlterUserScramCredentials` :1184, `CreateDelegationToken(options)` :1204, `RenewDelegationToken` :1228, `ExpireDelegationToken` :1251, `DescribeDelegationToken(options)` :1276, `DescribeFeatures(options)` :1292, `UpdateFeatures` :1317, `UnregisterController` :1340, `FenceProducers` :1362, `DescribeTransactions` :1386, `DescribeProducers` :1413, `ListTransactions(options)` :1441, `AbortTransaction` :1462, `ForceTerminateTransaction` :1485 | d, a | short + long |

Unchanged: `RemoveMembersFromConsumerGroup(groupId, options)` (Java has no short form; it already throws
`ArgumentNullException` in C# for a null options), `Close()` / `Close(TimeSpan)` (allowlisted), `Admin.Create`.

### 3.2 Where .NET's set differs from Java's beyond the two the brief names

- **`DescribeUserScramCredentials`** also has two optional parameters. Today it accepts
  `DescribeUserScramCredentials(options: o)`, which Java lacks. After: Java's three forms.
- **`ListPartitionReassignments`**: Java's abstract `(Optional<Set>, options)` has no separate .NET form. CKRDA0013
  folds an `Optional` overload into its `Set` twin (`JavaNameAnalyzer.IsOptionalTwin`), so `(partitions, options)`
  with a nullable `partitions` stands for both, `null` meaning `Optional.empty()`, as Python reads `partitions=None`.
  After the split, `ListPartitionReassignments(null)` is CS0121 between `(partitions)` and `(options)` (today the
  optional-parameter tie-break picks `(options)`); the doc says so, as `DeleteTopics` already does.
- No other mismatch: the other 44 signatures each become exactly Java's pair.

### 3.3 Java short forms that are not a plain `new XxxOptions()` forward

Each is still a one-line forward in .NET, with the same outcome:

| Java default | Java body | .NET short form |
|---|---|---|
| `deleteTopics(Collection)` | `deleteTopics(TopicCollection.ofTopicNames(topics), new DeleteTopicsOptions())` | `DeleteTopics(topics, new DeleteTopicsOptions())` (the names form wraps them) |
| `listConfigResources()` | `listConfigResources(Set.of(), new ListConfigResourcesOptions())` | `ListConfigResources(Array.Empty<ConfigResourceType>(), new ListConfigResourcesOptions())` |
| `describeUserScramCredentials()` | `describeUserScramCredentials(null, new DescribeUserScramCredentialsOptions())` | `DescribeUserScramCredentials(null, new DescribeUserScramCredentialsOptions())` |
| `listPartitionReassignments()` | `listPartitionReassignments(new ListPartitionReassignmentsOptions())` (then `Optional.empty()`) | `ListPartitionReassignments(new ListPartitionReassignmentsOptions())` |
| `describeTopics(Collection)`, `listConsumerGroupOffsets(String)`, `listPartitionReassignments(Set)` | forward to another default with `new XxxOptions()` | forward to the long form with `new XxxOptions()` |

### 3.4 Analyzer impact (CKRDA0013 / CKRDA0014)

- Today an optional `options` lets one .NET method pair with Java's abstract form, and the shorter Java default is
  only "accepted" (`JavaNameAnalyzer.Match`), so no `default` finding. After the split each short form pairs with its
  Java `default`, and `CheckPlace` reports `default <DocId>` for each one declared on an interface. The four client
  classes get none (the check is for interfaces only).
- Expected new keys: **49 per interface, 98 in all** (44 single short forms + `ListConfigResources()` +
  `ListPartitionReassignments()` and `(partitions)` + `DescribeUserScramCredentials()` and `(users)`). They go into
  the existing block, comment at `AL` lines 177–180, keys at 181–192, in ordinal order; the comment is extended
  (§11.3). The Actor takes the exact keys from the build output.
- **CKRDA0014 stays clean**: the 12 existing keys keep matching (a DocId carries no optional-ness or nullability).
- `java-name-baseline.txt` has no admin `parameter` or `overload` lines; `cargo xtask dotnet-java-name-baseline`
  should report 0 removed / 0 added. No analyzer code change.

### 3.5 Callers and other sites the change breaks

Counted with a script over every `.cs` file outside `bin/` and `obj/`; the compiler and the test run are the final
list.

| Project | To change |
|---|---|
| `tests/Confluent.Kafka.UnitTests`, public types | 96 explicit-`null` options calls in 18 files (63 in `PublicAdminP14DifferentialTests.cs`); 9 one-argument `ListConfigResources(types)` calls in 3 files; about 4 `withOptions ? new …Options() : null` arguments (`PublicAdminUnregisterControllerTests.cs`) and a few helpers taking `XxxOptions?` (CS8604 once the parameter is non-nullable). |
| same, internal `NativeAdminClient` (`Interop/*Tests.cs`) | 117 explicit-`null` options calls. Today they get the defaults; once D3 makes `AdminOptionsMarshal.Build` return `IntPtr.Zero` for a null options (S4) they would throw, so S0 gives each `new XxxOptions()`. |
| same, shape tests (fail at run time, not compile time) | 13 `PublicAdmin*ShapeParityTests` / surface files: about 28 `IsOptional` / `HasDefaultValue` / nullable-annotation asserts on `options`, about 30 name-only `GetMethod(name)` lookups that throw `AmbiguousMatchException` with two overloads, and `PublicAdminP14SurfaceTests` `51` → `100` (2 asserts). |
| `grpc-server` (`AdminServiceImpl`, `AsyncAdminServiceImpl`, via `TranslateAdmin`), 92 admin calls | **0**: every RPC passes `new XxxOptions()…`; `TranslateAdmin.ConfigResourceTypes` never returns null; `OptionalPartitions` returns null only for `ListPartitionReassignments` / `ElectLeaders`, whose `partitions` stay nullable. |
| `soak` (8), `tests/Performance` (5), `tests/Confluent.Kafka.GrpcServer.UnitTests` (20), `tests/Confluent.Kafka.IntegrationTests` (2) | 0; `GrpcServer.UnitTests/Fixtures/ListGroupsOverride` is a `DispatchProxy` and forwards any new member |
| `src` docs | about 160 name-only `cref`s to a split method in about 115 files (74 `*Result.cs`, 31 `*Options.cs`, 41 `<seealso cref="IAsyncAdmin.X"/>` in `IAdmin.cs`, a few value types): CS0419. 33 `cref`s spelled with `XxxOptions?` still resolve; they are updated for accuracy. `CreateTopicsOptions`' remark para 1, inherited by 36 Options classes, says "`options: null` behaves as a new instance": false after S4. |
| `src` code that reads `options` in C# | all 47 RPC paths of `NativeAdminClient` reach the C entry through `AdminOptionsMarshal.Build`, except `ListOffsets`, whose `NewListOffsetsOptions` picks `ListOffsetsOptions_new` or `_with_isolation_level`; the C# reads of `options` in `ListGroups`, `ListOffsets`, `ListTransactions`, `CreateDelegationToken` and `DescribeDelegationToken` are all behind `options is not null` / `options?.`. `MockAdminClient.CreateDelegationToken` (and its async peer) throws `IndexOutOfBoundsException` for `options is null`; S4 narrows that to a non-null options with no renewer. |

## 4. Decisions (ruled 2026-11-13)

### 4.1 D3 — an explicit `null` options value: **option C, built the C-layer way**

Facts that led to the ruling:

| | What happens with `xxx(args, null)` |
|---|---|
| **Java `KafkaAdminClient`** | `NullPointerException` thrown from the call: every RPC reads `options` on the calling thread (`calcDeadlineMs(now, options.timeoutMs())`, `invokeDriver(handler, future, options.timeoutMs)`, or a handler's constructor), except where the body returns before reading it (the 11 RPCs below). |
| **Java `MockAdminClient`** | Reads `options` only in `createDelegationToken`, `renewDelegationToken`, `expireDelegationToken`, `describeDelegationToken` (always) and `updateFeatures` (only when no update fails and there is at least one; `JM` lines 646–719, 1344). Ignores it in the others. |
| **Python** (`P/admin.py`, `P/async_admin.py`, `P/_conversions.py`) | An explicit `options=None` crosses to the C API as `NULL`; the C API reports `LocalNullPointerError("options must not be null")`, raised as `TypeError`. Where Java returns before reading it, `options_or_null(…, read=False)` substitutes a new options object. The order against other arguments follows Java (`before=`, `after_options`). The mocks follow Java's mock. |
| **.NET before this phase** | `null` meant the core's Java defaults (`AdminOptionsMarshal.Build` always started from `<X>Options_new`). |

**The design (Emanuele's rule: validation lives in the core; the binding validates only where really needed):**

1. **No C# throw for `options`.** `AdminOptionsMarshal.Build` returns `IntPtr.Zero` for a null options instead of
   calling `<X>Options_new` (so does `NewListOffsetsOptions`). The C entry (`RF/admin/rpc.rs`, every
   `kafka_admin_Admin_*_with_*options`: 49 entries, each `<x>_options_ref(options, "options")`) calls
   `precondition::require_ref`, which panics with `Error::local_null_pointer("options must not be null")`; the
   `_err` twin returns it as -32, and `KafkaException.OfClass` makes it `ArgumentNullException` with the core's
   message and no `ParamName`. `AdminOptionsMarshal.Free(IntPtr.Zero)` calls `<X>Options_destroy(NULL)`, a no-op per
   the header. No C# path may fail earlier on a null options (§3.5, last row).
2. **The binding's only code is a swap where Java does not read `options`:** a null options becomes
   `new XxxOptions()` before the native call.
   - **Real clients** (`KafkaAdminClient`, `AsyncKafkaAdminClient`): only where Java's `KafkaAdminClient` returns
     before reading `options`, with Python's `read=` conditions (11 RPCs, 13 .NET long forms):

     | RPC (.NET long form) | Swap when (Java returns before reading `options`) | Python |
     |---|---|---|
     | `CreateTopics` | no topic has a non-null, non-empty name (an empty collection included): `if (!topics.isEmpty())` in `createTopics` | `any_topic_name` |
     | `DeleteTopics(names)`, `DeleteTopics(TopicCollection)` of names | no name is non-null and non-empty (`handleDeleteTopicsUsingNames`) | `any_topic_name` |
     | `DeleteTopics(TopicCollection)` of ids | every id is `Uuid.ZERO_UUID` (an empty collection included; `handleDeleteTopicsUsingIds`) | `any_topic_id` |
     | `DescribeTopics(names)`, `DescribeTopics(TopicCollection)` of names | no name is non-null and non-empty (`handleDescribeTopicsByNamesWithDescribeTopicPartitionsApi`); a collection of ids always reads | `any_topic_name` |
     | `DescribeAcls` | `filter.IsUnknown()` | `not filter.is_unknown()` |
     | `DescribeConfigs` | `resources` is empty | `bool(args[0])` |
     | `IncrementalAlterConfigs` | `configs` is empty | `bool(configs)` |
     | `DescribeLogDirs` | `brokers` is empty | `bool(broker_ids)` |
     | `AlterReplicaLogDirs` | `replicaAssignment` is empty | `bool(replica_assignment)` |
     | `DescribeReplicaLogDirs` | `replicas` is empty | `bool(args[0])` |
     | `CreatePartitions` | `newPartitions` is empty | `bool(args[0])` |
     | `ListPartitionReassignments(partitions, options)` | `partitions` is not null and some partition has a null or empty topic or a negative partition (Java fails the future and returns) | `all_partitions_valid` |

   - **Mocks** (`MockAdminClient`, `AsyncMockAdminClient`): .NET's mock runs on the C layer
     (`NativeAdminClient.StartMock`), so every long form except the 5 Java's mock reads gets an unconditional
     `options ?? new XxxOptions()`: 43 methods per mock (42 of the 47 long forms, plus
     `ListPartitionReassignments(options)`). The 5 pass the null through: `CreateDelegationToken`,
     `RenewDelegationToken`, `ExpireDelegationToken`, `DescribeDelegationToken`, `UpdateFeatures`.
3. **Python's `before=` ordering is not ported.** When `options` and another argument are both null, the caller gets
   the other argument's error: C# checks the other arguments first (`ArgumentNullException` with that `ParamName`),
   and the C entry checks its parameters in order, `options` last. Java and Python report `options` first in the 10
   RPCs Python marks with `before=` (`alterConsumerGroupOffsets`, `deleteConsumerGroupOffsets`,
   `describeClientQuotas`, `electLeaders`, `expireDelegationToken`, `forceTerminateTransaction`,
   `listConfigResources`, `listConsumerGroupOffsets`, `renewDelegationToken`, and `removeMembersFromConsumerGroup`,
   which is outside this phase).
4. **Known gaps, recorded as follow-ups, not fixed here:** (a) Java's mock reads `updateFeatures`' options only when
   no update fails; the .NET mock passes the null through, so it throws also where Java's mock reports the update's
   error instead (knowing that needs the mock's feature state, which only the core has). (b)
   `RemoveMembersFromConsumerGroup` keeps its C# `ArgumentNullException(nameof(options))`; Java's mock ignores the
   options there. (c) The core checks `options` at the C entry, not at Java's read point; moving the check into the
   core would remove the binding's swaps.

### 4.2 D4 — a `null` `configResourceTypes` in `ListConfigResources(types, options)`: **pass it through**

"Pass the null through and let the C layer decide." Facts: `kafka_admin_Admin_list_config_resources_with_options`
(`RF/admin/rpc.rs`) reads `config_resource_types` with `list_refs` → `require_list`, which panics with
`local_null_pointer("config_resource_types must not be null")` on `NULL`, before it reads `options`. So a null types
set throws `ArgumentNullException` "config_resource_types must not be null" (no `ParamName`), and when types and
options are both null that is the error the caller gets (Java and Python report `options` first). Java reads the set
later, on its admin thread, and fails the returned future; Python reports it at the call, as .NET now does. No C# check
and no swap: `NativeAdminClient.ListConfigResources` passes `IntPtr.Zero` for a null list instead of an empty one.
`ListConfigResources()` passes an empty set and lists every type. `users`, `ElectLeaders`' `partitions` and
`ListPartitionReassignments`' `partitions` keep their null meaning (Java and Python agree with it).

### 4.3 D5 — doc citations: **narrow (default; Pranav did not answer and can still override it)**

New text (the 49 short-form docs per interface) and rewritten text (each `options` parameter, the split summaries of
`DeleteTopics` / `DescribeTopics` by names and `ListConsumerGroupOffsets(groupId)`) cite neither Java nor Python. The
existing mentions stay (`dotnet/CLAUDE.md` Comments rule 4 allows naming a Java method).

### 4.4 D1, D2, D6 — confirmed

- **D1** Java's exact overload set on `IAdmin` and `IAsyncAdmin` (§3.1), no optional parameter on any public admin
  method; `ListConfigResources(types)` and the `(options)`-only forms Java lacks go. The long forms take
  `XxxOptions options` (not annotated nullable).
- **D2** Each of the four clients writes each short form as a one-line forward, with `new XxxOptions()` (§3.3),
  `<inheritdoc/>` docs.
- **D6** `AL`: +98 `default` keys in the existing block, comment extended (§11.3); CKRDA0014 clean.

### 4.5 D7 — rule text

R1 (`dotnet/CLAUDE.md` rule 10, line 916): **apply in S6** ("apply R1"). R2 (`ffi-marshalling.md`): a proposal only,
in the close-out report; `ffi-marshalling.md` is not edited.

### 4.6 D8 — logistics

M23/P6 "Java's overloads for the admin client's options"; N=128; this branch; `dotnet-actor` 128 and `dotnet-critic`
128 (`D/CLAUDE.md` §8), a Critic pass after every Actor commit, comments in `dotnet/COMMENTS.128.md`; steps S0–S6
(§5); squashing decided at the end; ledger rows 527–534 (§10). If the security-review work (N=127) is filed as
M23/P6, this becomes M23/P7.

## 5. Steps

Each step is one commit, `--no-verify` (dotnet/ only; ledger 58), gates in §9. The Critic reviews each commit; fixes
land as `fixup!` commits. Agent-memory files are never part of a commit.

### S0 — callers use Java's forms (no API change)

Compiles on today's API, and behaviour is unchanged (a `new XxxOptions()` is today's `null`). In `NT/`: the 96
explicit-`null` options calls on public admin types become the short form; the 117 explicit-`null` options calls on
`NativeAdminClient` (the `Interop/` tests) pass `new XxxOptions()`; the 9 `ListConfigResources(types)` calls become
`(types, new ListConfigResourcesOptions())`; the conditional `? new …Options() : null` arguments call the form they
mean. No assertion changes.

### S1 — `cref`s name their overload (doc-only, no API change)

Every name-only `cref` to one of the 44 split method names (about 160 in about 115 `N/` files, one in
`NT/PublicAdminCreateTopicsTests.cs`) names the long form with its parameter list, e.g.
`<see cref="IAdmin.CreateTopics(IEnumerable{NewTopic}, CreateTopicsOptions)"/>`; the 33 spelled with `XxxOptions?`
drop the `?`. Resolves on today's API. Build is the check (CS0419 / CS1574 = 0).

### S2 — the switch, sync family: `IAdmin`, `KafkaAdminClient`, `MockAdminClient`

- `IAdmin`: 47 signatures lose their optional parameters (long forms take `XxxOptions options`); 49 short forms added
  with docs (D5), each with a `<seealso>` to its `IAsyncAdmin` peer; `ListPartitionReassignments(null)` CS0121
  remark. The `options` docs say "the request options" (S4 adds the null behaviour).
- `KafkaAdminClient`, `MockAdminClient`: the 47 lose their defaults; 49 one-line forwards each (D2), `<inheritdoc/>`.
- Shape tests of the sync types: `IsOptional` / annotation asserts, name-only `GetMethod`, surface count `51` → `100`
  for `IAdmin`.
- `AL`: +49 `IAdmin` keys, comment extended (§11.3).
- New `NT/PublicAdminJavaOverloadsTests.cs` (sync part): (1) no public method of `IAdmin`, `KafkaAdminClient`,
  `MockAdminClient` has an optional parameter; (2) each long form `X(args, XxxOptions)` has its short form `X(args)`,
  and `ListConfigResources`, `ListPartitionReassignments`, `DescribeUserScramCredentials` have exactly Java's forms;
  (3) on `MockAdminClient`, both overloads of `CreateTopics` (the topic exists after each), `DeleteTopics` by names
  (the topic is gone after each), and `ListConfigResources` (`()` lists every type, `(types, options)` filters).

### S3 — the switch, async family: `IAsyncAdmin`, `AsyncKafkaAdminClient`, `AsyncMockAdminClient`

As S2 for the async types: +49 `IAsyncAdmin` keys, surface count `51` → `100` for `IAsyncAdmin`, shape tests, and the
async part of `PublicAdminJavaOverloadsTests` (the three RPCs on `AsyncMockAdminClient`; (1) and (2) cover the async
types). After S3: `grep -nE 'Options\? options = null' dotnet/src/Confluent.Kafka/Admin/*.cs` prints nothing.

### S4 — an explicit `null` options value and a `null` `configResourceTypes` (D3, D4)

- `AdminOptionsMarshal.Build` and `NewListOffsetsOptions` return `IntPtr.Zero` for a null options;
  `NativeAdminClient.ListConfigResources` passes `IntPtr.Zero` for a null types set. No new C# throw.
- Real clients: the swaps of §4.1 item 2 (13 long forms), with the conditions in one internal helper shared by the
  sync and async clients (a new internal class; its summary says why, `definition-of-done.md` §7).
- Mocks: `options ?? new XxxOptions()` in the 43 methods of each mock (§4.1 item 2); the 5 pass the null through.
  `CreateDelegationToken` on both mocks throws `IndexOutOfBoundsException` only for a non-null options with no
  renewer, so a null options reaches the C layer.
- Docs (interface only, D5): each long form's `options` parameter and an `<exception cref="ArgumentNullException">`
  for a null options, with the "not thrown when …" condition on the 13 real-client forms; `configResourceTypes`
  documents its `ArgumentNullException`; a remark on the two mock classes that they ignore a null options except in
  the 5 RPCs; `CreateTopicsOptions`' remark para 1 no longer says `options: null` behaves as a new instance.
- Tests: a null options throws `ArgumentNullException` with message "options must not be null" and a null
  `ParamName` on `CreateTopics`, `DeleteTopics` by names, `ListConfigResources`, `DescribeCluster` (real clients) and
  `CreateDelegationToken`, `RenewDelegationToken` (mocks), on the sync and async flavour; it does not throw for an
  empty `CreateTopics` on the real clients, nor for `CreateTopics` on the mocks; a null `configResourceTypes` throws
  "config_resource_types must not be null" with a null `ParamName` on all four clients.

### S5 — comment-only cleanup of the touched area (`dotnet/CLAUDE.md` Comments rule 8)

Scope per D5. No code change.

### S6 — close-out (Manager)

R1 applied to `dotnet/CLAUDE.md` (D7); STATUS entry in `D/design/current/STATUS.md`; ledger rows 527–534 written as
ruled; `COMMENTS.DONE.128.md` copied into this folder; `dotnet/COMMENTS.128.md` reset; this plan's status set to done;
close gate (§9). The plan folder is committed with the phase (`D/CLAUDE.md` §8.4). R2 goes in the close-out report.

## 6. Tests added (counts)

- S2: 3 RPC tests on `MockAdminClient` (each calls both overloads) + 3 shape tests = 6 test methods.
- S3: 3 RPC tests on `AsyncMockAdminClient`; the shape tests gain the async types as theory rows.
- S4: the null-options throw cases (4 real-client RPCs, 2 mock RPCs, sync and async), the 2 non-throwing cases per
  flavour, and the null `configResourceTypes` case on the four clients.
- Changed, not added: about 28 option-shape asserts and about 30 reflection lookups in 13 shape-test files, 2 surface
  counts, and the S0 call sites.

## 7. Risks and how the steps guard against them

1. **CS0419 / CS1574 on doc `cref`s** — S1 fixes them before the switch; the build is the check.
2. **Run-time-only failures** in shape tests (`AmbiguousMatchException`, `IsOptional`) — the unit tests run in each
   step, on net8.0 and net10.0.
3. **A short form that forwards wrongly** (a different `XxxOptions`, a wrong argument) — S2/S3's tests for three RPCs,
   the Critic reads all 196 forwards against §3.3, and the mocks' differential tests now run through the short forms.
4. **A test still passing `null` options to `NativeAdminClient` after S4** — S0 removes all 117; S4's test run is the
   check.
5. **A swap condition that differs from Java's early return** — the Critic checks each of the 13 against the Java
   method named in §4.1 and against Python's `read=` expression.
6. **Allowlist drift** — keys taken from the build output; CKRDA0014 fails on a stale line.
7. **Overlap with the security-review branch** (N=127, same base): it changes `NativeAdminClient.{GroupOffsets,
   Partitions,Scram,Topics,Transactions}.cs` and `dotnet/CLAUDE.md` line 951; S4 touches `NativeAdminClient.Partitions`
   / `.Configs`. Whichever merges second rebases. R1's line (916) does not overlap.
8. **Sandbox shell traps** (clobbered `PATH`, `ugrep`, `cat`→`bat`, zsh word splitting, a zero-match test filter,
   `set -e` ignored) — every brief carries the list; counts and exit codes are checked, not "no output".

## 8. What breaks for users (pre-GA, but say it)

1. A user's own `IAdmin` / `IAsyncAdmin` implementation (a fake or wrapper) must add the 49 short forms; one compiled
   against today's binding fails to load (`TypeLoadException`).
2. `ListConfigResources(types)`, `ListConfigResources(options: o)` and `DescribeUserScramCredentials(options: o)` no
   longer compile; write `ListConfigResources(types, new ListConfigResourcesOptions())`,
   `DescribeUserScramCredentials(null, o)`.
3. `ListPartitionReassignments(null)` no longer compiles (CS0121); write `(partitions: null)` or cast.
4. An explicit `null` options throws `ArgumentNullException` ("options must not be null") where Java's client reads
   it, and so does an already-compiled call that left `options` out (it was compiled as `null`) until it is
   recompiled. A null `configResourceTypes` throws instead of listing every type.
5. Source that omits `options` compiles unchanged and now calls the short form.

## 9. Gates

Per step, on this Mac, from the repository root unless said:
1. The native library exists (`cargo build --features ffi --release` in `rust/` once at the start); no step changes Rust.
2. Build, Release, 0 warnings / 0 errors, CKRDA0013 and CKRDA0014 included: `D/Confluent.Kafka.sln` on every target
   framework (library net462, netstandard2.0, net8.0, net10.0; unit tests net462 build only, net8.0, net10.0),
   plus `D/grpc-server/Confluent.Kafka.GrpcServer.csproj`, `D/soak/SoakClient`, `D/soak/SoakClient.Tests` and
   `D/tests/Performance/*`, which compile against the changed API.
3. `dotnet format --verify-no-changes` on the solution (and any outside project a step touches).
4. `NT` on net8.0 and net10.0 (net462 is built, not run, on macOS); the analyzer tests; `make -C dotnet
   test-grpc-server-dotnet` at S2, S3 and S4.
5. `cargo xtask dotnet-java-name-baseline` in `rust/`: 0 removed, 0 added (never `--add`).
6. After S3: the `grep -nE 'Options\? options = null'` check prints nothing.
7. Commit with `--no-verify`; breaking steps (S2, S3, S4) use `feat(dotnet)!:`.

Close gate (S6), as M23/P5: `RUST_TEST_THREADS=6 make verify-dotnet-macos-docker`, on a quiet machine. Plain
`make test-integration-dotnet` prints SKIP on macOS and exits 0, which is not a pass.

## 10. Ledger rows (Agreed 2026-11-13; written at S6)

Rows 527–534 of `DOTNET-PORT-DECISIONS.md` (they shift if the security review takes rows first).

| # | Source | Decision | Pick | Status |
|---|---|---|---|---|
| 527 | PM 128, M23/P6 plan D1 | Admin overload sets | Java's exact set, no optional parameters | Agreed |
| 528 | D2 | Short forms | one-line forwards in each client | Agreed |
| 529 | D3 | Explicit null options | C, the C-layer way: NULL to the core; swaps only where Java does not read it (11 real-client RPCs, 43 mock methods) | Agreed |
| 530 | D4 | Null `configResourceTypes` | passed through; the core throws | Agreed |
| 531 | D5 | Citation cleanup scope | narrow (default, not answered) | Agreed by default |
| 532 | D6 | Allowlist | +98 `default` keys, comment extended | Agreed |
| 533 | D7 | R1 / R2 | R1 applied in S6; R2 a proposal | Agreed |
| 534 | D8 | Logistics | M23/P6, N=128, S0–S6, this branch | Agreed |

## 11. Draft text

### 11.1 R1 — `dotnet/CLAUDE.md` §3 Java names rule 10 (replaces the sentence at line 916; applied in S6)

> Java's `Admin` defaults that only convert their arguments or fill in the options (`createTopics(newTopics)` passes
> `new CreateTopicsOptions()`) stay members of `IAdmin` and `IAsyncAdmin`, one member per Java overload with no
> optional parameter, each client writing the default's body; their clients are their only implementers
> (allowlisted). A default Java adds to `Admin` later follows the rest of this rule.

### 11.2 R2 — `ffi-marshalling.md`, Admin Options paragraph (proposal only; not applied)

> An explicit `null` options object crosses as `NULL`: `AdminOptionsMarshal.Build` returns `IntPtr.Zero`, and the C
> entry reports -32 "options must not be null", an `ArgumentNullException` with no `ParamName`, as Python's
> `TypeError`. The binding swaps a null for a new options object only where Java does not read it: on the real
> clients where Java's method returns first (Python's `read=` conditions), on the mocks except the five RPCs Java's
> `MockAdminClient` reads. Leaving the options out calls the short form, which passes a new one.

### 11.3 `AL` block comment (replaces lines 177–180)

```
# Java's Admin defaults that only convert their arguments (topic names to a TopicCollection, a group id to a one-group
# map, the partitions to an Optional, close() to close(Duration)) or only fill in the options (new XxxOptions(); Set.of()
# or null for the other argument of listConfigResources() and describeUserScramCredentials()) stay members of the admin
# interfaces: the clients are their only implementers, and nobody overrides them ("Java default methods in the .NET
# client", option C; "Admin's no-options default methods in .NET", option 1). A default Java adds later follows
# dotnet/CLAUDE.md §3 rule 10.
```

## 12. Notes on the evidence (where it differs from the brief)

- N=127 is taken (dotnet-critic 127, `dotnet/COMMENTS.127.md`, 2026-11-13), so this phase is N=128.
- The allowlist block's comment is lines 177–180; its 12 keys are lines 181–192.
- `DescribeUserScramCredentials` has two optional parameters too (§3.2).
- Java's early-return RPCs are 11, not "about 9": Python's `admin.py` and `async_admin.py` each have 11 `read=` calls.
- The switch also breaks about 160 doc `cref`s (S1), about 58 reflection asserts in shape tests that only fail at run
  time (S2, S3), and, with D3, the 117 `NativeAdminClient` test calls that pass `null` options (S0).
- grpc-server, soak, tests/Performance, GrpcServer.UnitTests and IntegrationTests need no change.
- `rust/target/include/confluent_kafka_bindings.h` already has Java's pairs (`kafka_admin_Admin_create_topics` and
  `_with_options`); the binding keeps calling the `_with_options` form.
