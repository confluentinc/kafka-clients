# Critic 133 — M23/P10 — comments done

Review of M23/P10 (the owner's decisions on the PR #351 review)

- Reviewed range: `git log 8be49a80e..HEAD` on `esabellico/dotnet_async_serdes_and_zero_copy`, HEAD `74c1ee7a0`:
  7 commits (`fcd35c814` S0 .. `74c1ee7a0` S5, with the S4 fixup `137a254ed`), 56 files, all under `dotnet/`. No
  file outside `dotnet/` changes.
- Plan: `dotnet/design/history/M23/P10-pr351-review/PLAN.md` (the owner's decisions 1-4 of 2026-11-20).
- Result: **0 blocker, 0 major, 2 minor.** Both are in the phase's design record and bookkeeping: the ledger and N
  numbering, and the reason recorded for four tokens left without a default. I found no bug in `dotnet/src`, no
  migrated caller that lost its headers handling, and no test that passes through the wrong serde form.
- Not re-reported: the findings of `COMMENTS.131.md` (outside P10 by its plan) and the owner-pending items
  (`make verify-dotnet`, the squash, the push).
- `dotnet/COMMENTS.FP.md` and `dotnet/COMMENTS.FN.md` do not exist.

## 1. [minor] P10's ledger rows 584-588 are numbers the parked P9 already holds, and P10's status entry gives no next N

- **Files / members**:
  - `dotnet/design/history/M23/P10-pr351-review/PLAN.md`, "Ledger rows", rows 584-588 (lines 51-55);
  - `dotnet/design/current/STATUS.md` line 10, the P10 entry: "(ledger 584–588, Agreed by the owner 2026-11-20)";
  - `dotnet/analyzers/java-name-allowlist.txt` lines 335 ("M23/P10, ledger 584"), 337 and 361 ("ledger 585"),
    339 and 358 ("ledger 586");
  - the message of `b77cf26dc` ("(ledger 584)"). The S4 fixup `137a254ed` cites 587 too, but a fixup's message
    goes at the autosquash.
- **What is wrong**: ledger rows are numbered across the binding's phases, not per plan. M23/P9 ("Consumed records
  wrap the native batch", N=132) is parked on `esabellico/dotnet_consumer_records_and_headers_wrapped` (`041bb6f2f`,
  `b63e47768`, `009e6773d`, on the merge `b558a23f1`; not an ancestor of this branch) and holds rows 584-590. P10
  started at `8be49a80e`, which does not contain P9, and numbered its rows from 584 again. When P9 comes back to
  this branch, every number from 584 to 588 names two decisions: 584 is P9's "Wrapped records" and P10's token
  default; 585 "Deserialization" and `IAsyncSerializerWithHeaders<T>`; 586 "Batch release" and
  `IAsyncDeserializerWithHeaders<T>`; 587 "Headers" and retention; 588 "Exception headers" and the rule text. The
  allowlist comments that cite them ship with the analyzer lists.

  The N numbering has the same gap. The P10 entry is the only phase entry of `STATUS.md` without its "Next unused
  dotnet N" line, so the newest such line on the branch is P8's "**Next unused dotnet N: 131.**" (line 20). But 131
  (Critic 131's `COMMENTS.131.md`), 132 (P9) and 133 (P10) are all taken.
- **Evidence**:
  - `git merge-base --is-ancestor esabellico/dotnet_consumer_records_and_headers_wrapped HEAD` exits 1; the merge
    base is `b558a23f1`.
  - `git show esabellico/dotnet_consumer_records_and_headers_wrapped:dotnet/design/history/M23/P9-wrapped-consumer-records/PLAN.md`,
    lines 151-157: 584 Wrapped records, 585 Deserialization, 586 Batch release, 587 Headers, 588 Exception headers,
    589 `NextOffsets` (all Proposed), 590 Rule text W1-W7 (Open). That branch's `STATUS.md`, line 10: "N=132 …
    (ledger 584–590, Proposed …)"; line 11: "Next unused dotnet N: 133."
  - The highest row on `master` is 564 (M24/P1), on this branch 588, and on the P9 branch 590, so the next free row
    is 591. No ref has a row in 591-599.
  - The project already checks a paused phase's rows. M24/P1's plan, line 46: "rows start at 551 (534 is the
    highest row; the paused M23/P7 reserved 535–550). Re-check both at approval."
- **Suggested fix**:
  - Renumber P10's rows 584-588 to 591-595 (584→591 … 588→595) in the plan's table, in `STATUS.md` line 10 and in
    the five allowlist comments.
  - Reword `b77cf26dc`'s "(ledger 584)" at the squash (the owner's).
  - Add "- **Next unused dotnet N: 134.** Not pushed." to the P10 entry, with a clause saying that N=132 and rows
    584-590 belong to P9, parked on the backup branch, so the next phase does not repeat this.
- **Pre-existing, not counted**: the same kind of overlap is already on the branch. M24/P1 (from master, #348) holds
  rows 551-564, Agreed (its plan, §9 "Rulings"). P7's close-out rows 551-552 and P8's 553-564 reuse those numbers.
  P8's entry notes the shared N=130, not the shared rows. The renumbering pass is a good time to settle that
  overlap too.

- **Done:** fixups `899c351b4` (of `115e90ea1`, the allowlist comments: line 335 584 → 591, lines 337 and 361
  585 → 592, lines 339 and 358 586 → 593), `7d481c6e7` (of `fcd35c814`, the plan's table 584–588 → 591–595, row
  595's "rows 584–587" → "591–594", and a line under "Ledger rows" saying the rows start at 591 because the parked P9
  holds 584–590) and `b3da96e18` (of `74c1ee7a0`, `STATUS.md`'s P10 entry: "ledger 591–595", and "**Next unused
  dotnet N: 134.**" naming N=131 as Critic 131's and N=132 with rows 584–590 as P9's). The five allowlist lines are
  in one fixup of S3 rather than one each of S1, S2 and S3: S2 and S3 rewrote that comment block, so the per-commit
  fixups conflicted at the autosquash (`git merge-tree` of the S1 and S2 fixups onto their targets), while the S3
  fixup applies cleanly to the block in its final form. `b77cf26dc`'s message keeps "(ledger 584)", to be reworded at the squash (the owner's). The pre-existing
  M24/P1 vs P7/P8 overlap of rows 551–564 is left as it is; it is not this phase's.

## 2. [minor] The plan's reason for keeping a required token on the four `Get(CancellationToken)` overloads is wrong on both counts

- **Files / members**: `dotnet/design/history/M23/P10-pr351-review/PLAN.md`, "**Tokens left without a default**
  (decision 1's report)" (lines 94-98); the P10 entry of `STATUS.md` (line 11) points to it. The members are:
  - `KafkaFuture<T>.Get(CancellationToken)` (`dotnet/src/Confluent.Kafka/Common/KafkaFuture.cs:102`);
  - `KafkaFuture.Get(CancellationToken)` (`:299`);
  - `AsyncKafkaFuture<T>.Get(CancellationToken)` (`AsyncKafkaFuture.cs:111`);
  - `AsyncKafkaFuture.Get(CancellationToken)` (`:292`).
- **What is wrong**: the plan says "Each sits beside a parameterless `Get()` (and `Get(TimeSpan)`), so a default
  would make `Get()` ambiguous (CS0121) and break §3 Java names rule 8 (an overload per member)." Neither part holds:
  - There is no CS0121. When both candidates apply, C# prefers the one whose parameters all receive arguments over
    one that needs a default filled in, so `f.Get()` binds `Get()`. A method-group conversion (`Func<T> g = f.Get;`,
    `Task.Run(af.Get)`) considers only methods that apply without omitting an optional parameter, so it binds
    `Get()` too.
  - There is no rule-8 break. Rule 8 (`dotnet/CLAUDE.md` §3 Java names, lines 952-954) reads: "Each overload is a
    member of its own: no public or protected method or constructor, on any public type, takes an optional
    parameter other than a last `CancellationToken cancellationToken` (rule 7)." Rule 7 (lines 944-945) reads: "A
    method may end with an optional `CancellationToken cancellationToken`." The default the plan rejects is the one
    exception the rule names.

  The paragraph is the phase's report to the owner on decision 1, so this reason is what the owner decides on. It
  also suggests that the current shape is the one the rules want, and the analyzer says the opposite. The four
  overloads are open CKRDA0013 findings in `dotnet/analyzers/java-name-baseline.txt` lines 362-365 (since #324):
  `JavaNameAnalyzer` treats a last token as rule 7's own only when it is optional (`IsCancellationToken` tests
  `parameter.IsOptional`, and `NetOverload` strips only such a token). A required token is therefore compared as a
  parameter that Java's `get` does not have.
- **Evidence**:
  - A probe outside the repository (net8.0, `TreatWarningsAsErrors`) declares three members on a class `F`
    returning `int` and on a class `AF` returning `Task<int>`: `Get()`, `Get(TimeSpan timeout)` and
    `Get(CancellationToken cancellationToken = default)`, returning 1, 2 and 3.
  - It builds with 0 warnings and 0 errors. The calls `f.Get()`, `f.Get(TimeSpan.Zero)`, `f.Get(CancellationToken.None)`,
    `f.Get(cancellationToken: CancellationToken.None)`, `Func<int> g = f.Get; g()`, `await af.Get()`,
    `await Task.Run(af.Get)` and `await Task.Run(() => af.Get())` print `1 2 3 3 1 1 1 1`.
  - The rest of the paragraph is right: a scan of `dotnet/src` finds no other public method whose token has no
    default. Every other such token belongs to an internal type under `src/Confluent.Kafka/Internal/`, or to the
    private `SendValidated` / `SendAwaited` of `AsyncKafkaProducer` and `AsyncMockProducer`, where it is not the
    last parameter.
- **Suggested fix**:
  - Replace the reason with the true one: decision 1 names the asynchronous serde methods only, and `Get()` already
    serves a call without a token. A default would add a second member for the same call; it would not resolve an
    ambiguity.
  - Whether the futures get rule 7's shape is a question for the owner. That shape is one
    `Get(CancellationToken cancellationToken = default)` in place of both `Get()` and `Get(CancellationToken)`. It
    removes a public member, which breaks compiled callers and method-group uses such as `Task.Run(future.Get)`.
  - By my reading of `JavaNameAnalyzer.Match`, a default added beside `Get()` leaves one of the two members
    unpaired, so it would not close the four baseline lines either.
  - No code change is asked for in this phase.

- **Done:** fixup `b3da96e18` (of `74c1ee7a0`). The plan's paragraph now gives the true reason (decision 1 names the
  asynchronous serde methods only, and `Get()` already serves a call without a token, so a default would add a second
  member for the same call), says a default would be neither ambiguous nor against rule 8, and records as *open for
  the owner* whether the futures take rule 7's shape (one `Get(CancellationToken cancellationToken = default)` in place
  of `Get()` and `Get(CancellationToken)`), with its cost (a removed public member breaks compiled callers and
  method-group uses such as `Task.Run(future.Get)`) and the four CKRDA0013 baseline lines. The STATUS outcome line
  points to the open item. No code change.

## Reviewed with no finding

- Decision 1, token defaults: every `Serialize`, `Deserialize` and `Configure` of the public asynchronous serde
  interfaces, the two new ones included, and every public method of the 22 built-in `Async*` serdes ends with
  `CancellationToken cancellationToken = default`. The required-token `Configure` entries left the allowlist.
- Decisions 2 and 3, the split, matches the sync pairs:
  - `IAsyncSerializerWithHeaders<T> : IAsyncSerializer<T>` and `IAsyncDeserializerWithHeaders<T> : IAsyncDeserializer<T>`;
  - a memberless `IAsyncDeserializerWithRetention<T> : IAsyncDeserializerWithHeaders<T>`;
  - `AsyncSerializerExtensions` / `AsyncDeserializerExtensions` run Java's defaults, the headers form falling back
    to the plain one. With an instance method preferred over an extension, no call becomes ambiguous.
  - `PublicNamespaceLayoutTests` pins the two new base types.
- Dispatch: `AsyncSerializerCall<T>` and `AsyncDeserializerCall<T>` choose the form once, when the client is built,
  on all four asynchronous clients, and a built-in is still called through its synchronous class.
- Headers identity:
  - The real producers give the headers form the record's own `Headers` and then set them read-only, as Java's
    `doSend`.
  - The mocks give a new `Headers`, as Java 4.4's `MockProducer.send` on its empty cluster
    (`keySerializer.serialize(record.topic(), new RecordHeaders(), record.key())`). The .NET mocks take no `Cluster`
    and no partitioner, so this is their only branch: Java passes `record.headers()` only from `partition()`, on a
    cluster that knows the topic.
  - A serializer without the headers form gets no headers, as Java's default `serialize(topic, headers, data)`
    forwards to `serialize(topic, data)`.
- Retention and zero-copy:
  - `ConsumerRecordsMarshal` hands an unrevoked memory (`HandOut`) only to a deserializer that retains. Every other
    deserializer gets a lent one, revoked after its call (`Lend` plus `DeserializeThenRevoke`).
  - Retention now requires, and applies to, the headers form.
  - `AsyncReadOnlyMemoryDeserializer` keeps its headers form without a copy, and its new plain form copies, as
    `ReadOnlyMemoryDeserializer`.
- The guard: a class that declares a headers-form method without implementing its interface is refused with "…
  declares … but does not implement …, so the … would never call it." `AsyncSerdeGuardTests` asserts the exact
  messages.
- Tests:
  - 6 `[Fact]` and 5 `[Theory]` (10 rows) are added and none is removed.
  - The headers fixtures implement `…WithHeaders` / `…WithRetention`, and their plain form throws, so a test cannot
    pass through the wrong form (`SerdeHeadersBrokerTests`, `ZeroCopyRetentionBrokerTests.YieldingRetainingDeserializer`).
  - The plain-form migrations keep every assertion, the allocation budgets included.
  - `AsyncSerializerOver<T>` / `AsyncDeserializerOver<T>` implement both forms.
  - `AsyncConsumerDeserializerFormsTests` and `AsyncProducerAwaitedSerializerTests` cover each form on the real
    and the mock clients.
- Callers:
  - The soak client's `SoakRecord.cs` moves to the plain form. Its old headers form ignored the headers, so nothing
    is lost.
  - No other implementer of the async serde interfaces exists in `dotnet/` outside `src` and the two test projects.
    `tools/AotSmoke` and `SerdeConfigCreateBrokerTests` hold built-ins only.
- Rule text:
  - P7's R1-R5 and P8's R1-R14 are in `dotnet/CLAUDE.md`, `dotnet/.claude/rules/ffi-marshalling.md` and `dotnet/README.md`.
  - No stale serde wording is left: no "single-method" or "Constructor arguments only" text. The one "one method"
    hit, `dotnet/CLAUDE.md:1111`, is about `EnforceRebalance`, and the "deferred" hits are about `IProducer`, the
    typed producer and deferred destroys, not the serde forms.
  - The P7 and P8 plans keep their drafts as written, with an "applied" note.
  - The P7 and P8 "Open" lines of `STATUS.md` keep their text under an "Applied" annotation, as past entries stay as
    written (M23/P2 plan, F6).

## Verification runs (mine)

- Only the probe of finding 2. I did not rebuild or rerun the repository's suites. The gate figures (build 0
  warnings, unit tests net8.0/net10.0 9635 passed / 51 skipped, gRPC server 254, format clean, AOT smoke) are the
  Actor's, from S5.

## Not verified

- `make verify-dotnet` (owner-pending); the integration tests and anything else that needs Docker; net462.
- The analyzer's output for a `Get` token given a default. Finding 2's last remark comes from reading
  `JavaNameAnalyzer`, not from a run.

## Coverage of the diff

- All 56 files of the range, diff by diff.
- In full: `IAsyncSerializer.cs`, `IAsyncDeserializer.cs`, `AsyncSerializerCall.cs`, `AsyncDeserializerCall.cs`,
  `SerdeGuard.cs`, `SerializedProducerRecord.cs`, the new and changed tests, the P10 plan, and the rule-text
  changes.
- Outside the diff: `KafkaFuture.cs` and `AsyncKafkaFuture.cs` (finding 2), the parts of `JavaNameAnalyzer.cs` that
  finding 2 cites, Java's `MockProducer.send`, the P9 backup branch's plan and status, and M24/P1's plan.

## Suggested rule note (for the owner; no rule file was changed)

- In the .NET phase workflow, wherever a plan allocates its ledger rows and N: take both above every number held by
  a parked or backup-branch phase, not only above the highest number on the phase's starting commit, as M24/P1 did
  for the paused M23/P7. Record the parked phase's range in the new status entry. Finding 1 is the second such
  overlap on this branch (the first is the pre-existing one it notes).
