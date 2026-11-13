# COMMENTS.DONE.128

## Review of edac52b35 (S4)

### 128-S4-1 (Low, test coverage) — the "null still reaches the core" side of most swaps is unpinned: 11 of the 13 real-client forms, and 37 of the 43 forwards on each mock

- Where: `dotnet/tests/Confluent.Kafka.UnitTests/PublicAdminNullOptionsTests.cs:45-54` (`s_readingCalls`) and
  `:180` / `:201` (`Mock_NullOptions_AreTheDefaultOptions`, `AsyncMock_…`); production
  `dotnet/src/Confluent.Kafka/Internal/AdminEarlyReturns.cs:44-93` and the 43 `options ?? new …` forwards of
  `Admin/MockAdminClient.cs` / `Admin/AsyncMockAdminClient.cs` (e.g. `MockAdminClient.cs:200`).
- What: `s_unsentCalls` pins the swap side of all 13 forms (16 inputs), but the other side — a null options with
  arguments that DO send a request must still throw "options must not be null" — is pinned only for `CreateTopics`,
  `DeleteTopics(names)` and the ids branch of the two `TopicCollection` forms. Unpinned: `DescribeTopics(names)`, the
  names branch of `DeleteTopics(TopicCollection)` / `DescribeTopics(TopicCollection)`, `DescribeAcls` (known
  filter), `DescribeConfigs`, `IncrementalAlterConfigs`, `DescribeLogDirs`, `AlterReplicaLogDirs`,
  `DescribeReplicaLogDirs`, `CreatePartitions` (non-empty) and `ListPartitionReassignments(partitions, options)`
  (valid partitions). On the mocks only 6 of the 43 swapping methods are ever called with a null (`CreateTopics`,
  `ListTopics`, `DescribeCluster`, `ListConfigResources`, `ListPartitionReassignments(options)`,
  `DeleteTopics(names)`).
- Evidence (mutants in a `git archive edac52b35` tree, full net8.0 unit suite, 9158 tests, each build checked to
  have recompiled `Confluent.Kafka.dll`): `AdminEarlyReturns.DescribeConfigs` → `options ?? new
  DescribeConfigsOptions()` (the real client now swallows a null for a non-empty `resources`, where Java's
  `describeConfigs` dereferences it and Python's `read=bool(args[0])` raises): **0 failed**.
  `MockAdminClient.DescribeConfigs` without its `?? new DescribeConfigsOptions()` (the mock now throws, against its
  own class remark): **0 failed**. Controls, all killed: `AdminEarlyReturns.CreateTopics` ignoring empty names (2
  failed), the async mock's `CreateDelegationToken` back to `options is null || …` (1), `ListConfigResources` sending
  an empty list for a null types set (4).
- Why it matters: the 13 conditions and the 43 forwards are the step's whole contract (PLAN §4.1 item 2); they are
  correct at this commit (checked against Java's early returns and Python's `read=`), but a later edit that widens
  a condition or drops a `??` changes which calls throw and every gate stays green.
- Fix: add one reading row per swapped form with an input that sends a request (a non-empty name, a known filter,
  one resource / config / broker / replica assignment / replica / topic, a valid partition) to `s_readingCalls`, run
  on both flavours as today; and call each of the 43 swapping forwards of both mocks once with a null options,
  asserting the call does not throw `ArgumentNullException` "options must not be null" (its own outcome aside).

**Resolved:** f55af866f (`fixup!` of edac52b35) — 11 real-client reading rows (22 cases) whose arguments send a request now pin the core's `ArgumentNullException` "options must not be null" (null `ParamName`); the 43 swapping long forms of both mocks are each called with a null options and must return a result whose future completes (86 cases), with a reflection test pinning the 43 rows to each mock's long forms (2 cases). Mutants: `AdminEarlyReturns.DescribeConfigs` always swapping kills both real `DescribeConfigs` rows; `MockAdminClient.DescribeConfigs` without its `?? new` kills the sync mock row.

## Review of a2d2edb50 (S5)

### 128-S5-1 (Low, comment accuracy) — the rewritten summary of `PublicAdminJavaOverloadsTests` now claims a form without the options beside *every* form that takes them, which the class's own test asserts is false for `RemoveMembersFromConsumerGroup`

- Where: `dotnet/tests/Confluent.Kafka.UnitTests/PublicAdminJavaOverloadsTests.cs:31-32` (the class summary); the
  contradiction is in the same file at `:79-83` (`if (longForm.Name == nameof(IAdmin.RemoveMembersFromConsumerGroup))
  { Assert.Null(shortForm); continue; }`). Same kind, smaller: `PublicAdminP2bShapeParityTests.cs:66`.
- What: S2 wrote "declare Java's overloads of every RPC, a form without the options beside the form that takes them";
  the "Java's overloads" qualifier made it true, since Java's `removeMembersFromConsumerGroup` has only the
  options-taking form. S5 dropped the qualifier and made the claim universal: "declare a form without the options
  beside every form that takes them". `IAdmin` / `IAsyncAdmin` declare only
  `RemoveMembersFromConsumerGroup(string, RemoveMembersFromConsumerGroupOptions)` (`IAdmin.cs:1218`), and the test
  pins that absence. The P2b comment ("the one-argument overload is its own member") covers a loop over `ListTopics`,
  `CreatePartitions` and `DeleteRecords`; `ListTopics`' form without the options is `ListTopics()`, zero-argument
  (`IAdmin.cs:262`). That inaccuracy predates the phase ("collapsing Java's one-argument default overload"), but
  S2 and S5 each rewrote the line and kept it.
- Why: a comment-only commit has only its text to get right, and rule 2 asks for a summary of what the class does;
  this one now states the opposite of an assertion in its own body. Suggested wording, still free of Java/Python
  (D5): "…declare the forms without the options beside each form that takes them (none for
  `RemoveMembersFromConsumerGroup`), with no optional parameter, …"; and in P2b "the overload without them is its own
  member".

**Resolved:** effe212f2 (`fixup!` of a2d2edb50) — the `PublicAdminJavaOverloadsTests` summary now reads "declare the forms without the options beside each form that takes them (none for <c>RemoveMembersFromConsumerGroup</c>)", and the P2b comment "the overload without them is its own member", true for `ListTopics()`; comment-only (both files identical once full-line comments are removed).

## Review log (dotnet-critic 128, a pass after every Actor commit)

| Step | Commit | Review |
|---|---|---|
| S0 | `c523ab96a` | 0 issues |
| S1 | `21c87551c` | 0 issues |
| S2 | `e9c7ccc5f` | 0 issues (notes for S3: the short forms' seealsos to their async peers; restore the strict P14 check) |
| S3 | `637308ce0` | 0 issues (notes for S4: the null `configResourceTypes` test sites and doc lines) |
| S4 | `edac52b35` | 128-S4-1 (Low), fixed in `f55af866f`; re-check 0 new issues |
| S5 | `a2d2edb50` | 128-S5-1 (Low), fixed in `effe212f2`; re-check 0 new issues |

## Decisions and deviations made during execution

- **Plan counts corrected (S0).** The plan's 96 explicit-`null` options calls on public types were mostly a `null!` first
  argument. The compiler-bound count was 8 calls (4 `UnregisterController`, 2 `ListTopics`, 2 `ListTransactions`) plus
  `PublicAdminListGroupsTests`' `ListGroups(null)`, left for S3. The 117 `NativeAdminClient` calls and the 9
  one-argument `ListConfigResources(types)` calls were as planned.
- **Java's early returns are 11 RPCs, not "about 9"** (plan §12): Python's `admin.py` and `async_admin.py` each have 11
  `read=` calls; the real clients swap on 13 long forms through `Internal/AdminEarlyReturns.cs`.
- **Seealsos (S3).** `IAsyncAdmin`'s long forms have no member-level `<seealso>`, so its short forms have none either;
  each `IAdmin` short form links its `IAsyncAdmin` peer.
- **Short-form docs** reuse the long form's through `<inheritdoc cref=… path=…/>` (params other than `options`,
  `returns`, `remarks`, and the exceptions that do not concern the options). The compiler leaves these unexpanded in the
  XML file; the Critic expanded every path in S2, S3 and S4.
- **The mock class remark (S4)** names six RPCs that reject a null options: the five Java's `MockAdminClient` reads, plus
  `RemoveMembersFromConsumerGroup`, whose existing C# check stays (plan §4.1 item 4(b)).
- **`configResourceTypes` is non-nullable** on the six types after D4; the tests that used a null to mean "every type"
  now use an empty set, and a null now asserts the core's "config_resource_types must not be null".
- **The partial S4 work** of an attempt that ended on an API error was kept and finished by the next Actor; no file
  was reverted.
- **R1** (`dotnet/CLAUDE.md` rule 10) is prepared as a patch for the main session: a relayed instruction does not
  authorize an agent to edit a `CLAUDE.md`. R2 (`ffi-marshalling.md`) stays a proposal (plan §11.2).
