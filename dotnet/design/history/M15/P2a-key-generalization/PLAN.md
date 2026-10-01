# M15/P2a — Key generalization, `TopicCollection`, and the by-name/by-id duality

**Status:** **PROPOSED — NOT APPROVED.** Design decisions **D7–D10 are ruled**
(§1); the plan itself awaits the maintainer's sign-off. No Actor or Critic has
been spawned. No source file has been modified.

**Agent number: N = 67.** (P2b is N = 68. Highest previously used is 66.)

**Parent roadmap:** the M15 admin roadmap under `bindings/dotnet/design/current/`
(approved 2026-09-08, decisions D1–D6). ⚠ **It is deliberately UNTRACKED** —
still the governing roadmap, still to be kept updated, but **never committed**,
so it is absent from a fresh clone. That is intentional, not an error. Named by
description rather than by path so this tracked file carries no citation that
cannot resolve.

**Base:** **`6aa5fc4a`** — P1 was squashed into one commit and force-pushed. The
five original P1 commits (`b35fb304`, `5f898c72`, `78548041`, `3b4f6c79`,
`d55f4297`) are **gone from branch history**; never cite them in a diff range.

**Mode:** **A**. No Rust, no new ABI function, no `cbindgen.toml` change.

**Branch:** `prashah_dev_dotnet_admin`. Do NOT create a branch, merge, or rebase.

⚠ **Authorization is per-phase.** P2b needs its own go-ahead after P2a closes.

---

## 0 · Why P2 was split, and why P2a is the risky half

The roadmap's §8 bet was that P1 lands the mechanism and P2…P8 repeat it.
**That bet is ~80% right, and the missing 20% is what forced the split.** Reading
the ABI, P2 needs P1's mechanism generalized **three independent ways**:

| # | Generalization | Why P1 could not have known | Edits P1 code? | Phase |
|---|---|---|---|---|
| **G1** | **Key type**: `KeyedAdminOperation<TValue>` is keyed by `string`. P2 needs `Uuid` keys (`topicIdValues()`) and `TopicPartition` keys (`lowWatermarks()`). | `CreateTopics` is topic-name-keyed only. | **YES** | **P2a** |
| G2 | **Optional `GetError`**: `ListTopicsResult` has **no `get_error` function at all**. | P1's two shapes both have per-key errors. | YES (one field) | P2b |
| G3 | **Non-keyed single-future op**: shape 3 is ONE `KafkaFuture<Map<K,V>>`. | P1 had no shape-3 RPC. | No — new sibling type | P2b |

**P2a is the half that rewrites P1's reviewed foundation**, which is exactly why
it is scoped small — the same argument that made P1 one RPC. A regression in the
`CreateTopics` path must be attributable to one diff.

---

## 1 · DESIGN DECISIONS — ruled 2026-09-08

All four confirm the defaults this plan proposed. **The reasoning is kept
deliberately.** D8 in particular will read as *wrong* to a C# reviewer's
instincts, so its justification must be findable both here and at the site — or
someone will "fix" it in a later phase.

| # | Decision | Ruling |
|---|---|---|
| D7 | Split P2 | **Yes — by mechanism.** P2a = G1 + duality (N=67); P2b = G2 + G3 (N=68) |
| D8 | Wrong-accessor read | **Return `null`**, typed nullable. Not a throw, not empty |
| D9 | `DescribeTopicsResult.all()` | **Mirror Java exactly** — no `All()`; `AllTopicNames()`/`AllTopicIds()` instead |
| D10 | `TopicCollection` namespace | **Root** — `Confluent.Kafka`, not `Confluent.Kafka.Admin` |

### D7 — Split P2 by mechanism ✅ ruled 2026-09-08

The obvious alternative — "two RPCs, then three" by count — puts **G1 and G3 in
the same diff**. Splitting by *mechanism* instead means P2a changes how a keyed
operation is **keyed**, and P2b adds the shapes that are **not keyed operations at
all**. A regression is then attributable to one sub-phase.

P2a is also the half that edits P1's already-reviewed foundation, so it is the
half that must stay small.

**Downstream numbering: P3…P9 keep their labels.** P2 becomes P2a/P2b — a
sub-phase suffix, matching the roadmap's own pre-sanctioned "P5a/P5b" wording and
the repo's existing `M11/P2.1`, `M11/P4.1` precedent. **Nothing renumbers**, so
every `STATUS.md` entry and archive path written so far stays valid. A silent
renumber would have invalidated all of them.

### D8 — A wrong-accessor read returns `null` ✅ ruled 2026-09-08

Java's javadoc is explicit, twice:

> *"…a map from topic IDs to futures which can be used to check the status of
> individual deletions **if the deleteTopics request used topic IDs. Otherwise
> return null.**"* — `DeleteTopicsResult.java:56-65`, and
> `DescribeTopicsResult.java:60-70` identically.

So calling `TopicIdValues` on a **name**-built result returns `null`.

⚠ **This will look wrong to a C# reviewer, and that is the point of writing it
down.** Every C# instinct says throw `InvalidOperationException` (the state is
invalid) or return an empty dictionary (null-hostile API design). **Both diverge
from Java**, and the divergence is silent: a caller ported from Java writes
`if (result.topicIdValues() != null)`, which an exception turns into a crash and
an empty map turns into a silently-skipped loop.

**Ruled: return `null`**, typed `IReadOnlyDictionary<Uuid, Task>?` under
`#nullable enable`. The nullable annotation makes Java's contract visible **in
the signature**, so the compiler warns the caller — strictly better than Java,
without changing the shape.

**Record this at the site.** A `// Java returns null here` comment citing
`DeleteTopicsResult.java:56-65` is required on both properties in both result
types, precisely so a later phase does not "fix" it.

### D9 — `DescribeTopicsResult` has no `All()` ✅ ruled 2026-09-08

Java's two result types are **deliberately asymmetric**:

  - `DeleteTopicsResult` → `all()` (`:72`), and **no** typed aggregate.
  - `DescribeTopicsResult` → `allTopicNames()` (`:80`) and `allTopicIds()`
    (`:90`), and **no** `all()`.

**Ruled: mirror exactly.** The asymmetry is Java's, not ours to smooth. Record it
at the site so a later reviewer does not add the "missing" member — the same
class of well-intentioned widening D8 guards against.

### D10 — `TopicCollection` at the root namespace ✅ ruled 2026-09-08

Java puts it in `org.apache.kafka.**common**`, not `.admin`
(`common/TopicCollection.java`). **Ruled: `Confluent.Kafka.TopicCollection`**,
beside `Uuid`, which P1 shipped at the root for the same reason. Roadmap §6
already said so.

---

## 2 · Deliverables

### 2.1 G1 — generalize `KeyedAdminOperation<TValue>` → `KeyedAdminOperation<TKey, TValue>`

P1 shipped `KeyedAdminOperation<TValue>` keyed by `string`, exposing
`IReadOnlyDictionary<string, Task<TValue>> Tasks`. P2a makes the key generic.

  - `KeyedAdminOperation<TKey, TValue> where TKey : notnull`.
  - `VoidKeyedAdminOperation` → `VoidKeyedAdminOperation<TKey>`.
  - The ABI **always** hands back a `const char*` key. So the operation takes a
    **key parser** applied once per index in the callback: identity for `string`,
    `Uuid.Parse` for `Uuid`.
  - `KeyedResultMarshal.Complete<TValue>` → `Complete<TKey, TValue>`, taking that
    parser.

⚠ **Design the parser as `Func<IntPtr, int, TKey>` (result handle + index), not
`Func<string, TKey>`.** P2b's `DeleteRecordsResult` has **no `get_key`** — its key
is composite, read from `get_topic(i)` **and** `get_partition(i)`. A
string-to-key parser cannot express that, and choosing the narrow signature here
means **P2b re-opens P2a's foundation** — the exact churn the split exists to
prevent. Pay the small generality cost now.

⚠ **The equality comparer must be passed in explicitly per `TKey`.** P1 used
`StringComparer.Ordinal` deliberately, so `Values` and the typed accessors could
not disagree on a key. The generic form must **not** silently fall back to
`EqualityComparer<TKey>.Default` for `string` — thread the comparer through and
keep `Ordinal` for string keys.

✅ **`Uuid` already ships `Equals`/`GetHashCode`** (verified in P1's `Uuid.cs`), so
`Dictionary<Uuid, …>` is safe and this is **not** a P2a deliverable.

⚠ **This edits P1's shipped, reviewed foundation.** The Critic must confirm the
`CreateTopics` path is behaviourally unchanged: its P1 tests must pass
**unmodified**, and one P1 defect re-injected **through the generalized code**
must still go red. A green suite alone does not prove the generalization
preserved sensitivity.

### 2.2 `TopicCollection` (D3 from the roadmap; D10 for its namespace)

Java is an **abstract class with a private constructor** and two **nested public
subclasses** — the private ctor is what makes *"subclassing beyond the classes
provided here is not supported"* enforceable. C# reproduces this exactly, since a
nested type can reach the outer private ctor and an external type cannot:

```csharp
namespace Confluent.Kafka;                                  // D10 — Java's `common`, not `admin`

public abstract class TopicCollection
{
    private TopicCollection() { }                           // blocks external subclassing

    public static TopicIdCollection   OfTopicIds(IEnumerable<Uuid> topics) => new(topics);
    public static TopicNameCollection OfTopicNames(IEnumerable<string> topics) => new(topics);

    public sealed class TopicIdCollection : TopicCollection
    {
        private readonly List<Uuid> _topicIds;
        internal TopicIdCollection(IEnumerable<Uuid> topicIds) => _topicIds = new List<Uuid>(topicIds);
        public IReadOnlyCollection<Uuid> TopicIds() => _topicIds;        // Java topicIds()
    }

    public sealed class TopicNameCollection : TopicCollection
    {
        private readonly List<string> _topicNames;
        internal TopicNameCollection(IEnumerable<string> topicNames) => _topicNames = new List<string>(topicNames);
        public IReadOnlyCollection<string> TopicNames() => _topicNames;  // Java topicNames()
    }
}
```

Each detail is Java-derived:

  - Java **copies** the incoming collection (`new ArrayList<>(topics)`) and returns
    an unmodifiable view. C# copies into a `List<T>` and returns
    `IReadOnlyCollection<T>`. **Do not store the caller's collection by
    reference** — Java does not, and a later mutation would corrupt the request.
  - `TopicIds()` / `TopicNames()` are **methods**, not properties — Java's are
    methods, matching the shipped `Assignment()`/`Subscription()`/`Paused()`
    precedent for a returned snapshot.
  - The nested ctors are `internal`, not `private`: a nested class can see the
    *outer* private ctor, but the subclass's **own** ctor must be reachable from
    the static factories. `internal` + `sealed` is the closest faithful form and
    still blocks external construction.

**Entry-point selection — the whole point of D3.** Each RPC is ONE C# method
dispatching on the runtime type:

```csharp
public DeleteTopicsResult DeleteTopics(TopicCollection topics, DeleteTopicsOptions? options = null) => topics switch
{
    TopicCollection.TopicNameCollection n => DeleteTopicsByNames(n.TopicNames(), options),  // ..._delete_topics
    TopicCollection.TopicIdCollection   i => DeleteTopicsByIds(i.TopicIds(), options),      // ..._delete_topics_by_ids
    _ => throw new ArgumentException(...)   // unreachable: private ctor + sealed subclasses
};
```

The `_` arm is unreachable by construction but required for exhaustiveness; give
it a message naming the invariant rather than a bare `default`.

### 2.3 The `Uuid` ↔ base64 topic-id round-trip

The by-id entry points take and return **base64 topic-id strings**, not binary
UUIDs. Header, verbatim (`:3142-3144`): *"`topic_ids` are base64 topic-id strings
(Java's `Uuid.toString()` form); an [invalid one is an] error, mirroring Java's
`Uuid.fromString`. **Result keys are the same base64**."*

So the round-trip is required in **both** directions:

  - **Out:** `Uuid` → `ToString()` → the `const char*const*` array.
  - **Back:** `get_key(i)` → base64 `string` → **`Uuid.Parse`** → the `Uuid` key of
    `TopicIdValues`. **This is G1's key parser, and it is why G1 exists.**

P1 shipped `Uuid.Parse`/`ToString` hardened across three review rounds (alphabet
screen, 24-char gate, padding screen), so P2a **consumes** that work. But P2a is
its **first consumer**, so it must add the round-trip test P1 could not:
`Uuid` → string → ABI → string → `Uuid` is identity.

⚠ **A malformed id is rejected by the ABI, and rejection arrives through the
INLINE callback path** (roadmap §4.5 case 2) — the callback fires **synchronously
on the calling thread, before the entry point returns**. P1 built for this but
could not exercise it (`CreateTopics` takes no parseable id). **P2a can and must:
pass an unparseable topic id and assert the operation faults cleanly, the
`GCHandle` is freed exactly once, and the `DangerousAddRef` is released.** This is
the first real test of P1's inline path.

⚠ **Finding 8 is discharged here.** `Uuid.cs:246` claims .NET is more permissive
than Java *"in **exactly** these two places"* while `:268-271` names a third
(embedded whitespace). **Delete the word `exactly`. Do NOT re-word the sentence.**
Re-wording a comparative clause is what generated the next round's false clause
four rounds running in M14/P1; `ffi-marshalling.md §A6`'s round-5 amendment makes
deletion the rule. **This must land on the first P2a commit that touches
`Uuid.cs`.**

### 2.4 The two result types — derived from Java's PUBLIC accessors

⚠ **P1's two Medium findings both came from reading a Java private field instead
of the public accessor. Every signature below is quoted from an accessor, with a
line citation, so it can be checked. Check it anyway (§6).**

```csharp
// DeleteTopicsResult.java:56, :65, :72
public sealed class DeleteTopicsResult
{
    public IReadOnlyDictionary<Uuid, Task>?   TopicIdValues   { get; }  // D8: null unless built from ids
    public IReadOnlyDictionary<string, Task>? TopicNameValues { get; }  // D8: null unless built from names
    public Task All();                                                  // D9: DeleteTopics HAS all()
}

// DescribeTopicsResult.java:60, :70, :80, :90 — D9: NO All()
public sealed class DescribeTopicsResult
{
    public IReadOnlyDictionary<Uuid, Task<TopicDescription>>?   TopicIdValues   { get; }
    public IReadOnlyDictionary<string, Task<TopicDescription>>? TopicNameValues { get; }
    public Task<IReadOnlyDictionary<string, TopicDescription>>? AllTopicNames();
    public Task<IReadOnlyDictionary<Uuid, TopicDescription>>?   AllTopicIds();
}
```

⚠⚠ **CORRECTED 2026-09-08 — this sketch was wrong, for the THIRD time in M15, and
the Actor caught it.** `AllTopicNames()` / `AllTopicIds()` were first written here
as **non-nullable**. They are not: both delegate to the private helper
`DescribeTopicsResult.java:98`, which opens `if (futures == null) return null;`,
and the javadoc on each says *"otherwise return null"*. So the **mismatched
aggregate is null**, exactly as D8's per-key maps are.

**The lesson, which is new and worth generalizing:** quoting a Java accessor's
**signature** is not enough — Java has no nullable-reference annotations, so an
accessor's *return-nullability* lives only in its **javadoc and its
implementation**. D8 was caught because the javadoc sentence was quoted; this was
missed because only the signature was. **For every remaining phase: read the
javadoc AND the body of each accessor you translate, not just its declaration.**

  - `Task`, not `Task<Void>` — Java's `KafkaFuture<Void>` (result shape 2). P1
    established this on `CreateTopicsResult.Values`.
  - The nullable maps are **D8**; exactly one is non-null, fixed at construction.
    Comment the Java citation at each site.
  - `All()` on `DeleteTopicsResult` aggregates whichever map is non-null
    (`DeleteTopicsResult.java:72-74`).
  - `AllTopicNames()` / `AllTopicIds()`: the one whose key type does not match the
    request must behave as Java's does — **read `:80-95` and mirror it** rather
    than guessing.

### 2.5 Value types

  - **`TopicDescription`** — ABI: `name`, `topic_id`, `is_internal`,
    `partition(i)`, `partition_count`, `authorized_operation(i)`,
    `authorized_operation_count`, **`has_authorized_operations`**. That last is an
    explicit absent-vs-empty discriminant: Java's `authorizedOperations()` is
    `null` when the broker did not return them, which is **not** an empty set.
    Model as `IReadOnlyCollection<AclOperation>?`.
  - **`TopicPartitionInfo`** — `partition`, `leader`, `replica(i)`/`replica_count`,
    `isr(i)`/`isr_count`, plus `elr` and `last_known_elr` each with a `has_*`
    discriminant (KIP-966 eligible-leader-replicas). Same absent-vs-empty care.
    ⚠ **Reuse the shipped `Confluent.Kafka.Node`** for `leader` — do not declare a
    second node type.
  - **`AclOperation`** — the enum only (roadmap §8 puts `AclBinding` in P6). Values
    must match Java's `AclOperation` **id codes** numerically, not be
    auto-assigned (roadmap §7 gate 4).

### 2.6 Options

`DeleteTopicsOptions` (`timeoutMs`, `retryOnQuotaViolation`) and
`DescribeTopicsOptions` (`timeoutMs`, `includeAuthorizedOperations`,
`partitionSizeLimitPerResponse` — confirmed at `DescribeTopicsOptions.java:36-68`)
as plain POCOs (roadmap D5), nullable ⇒ Java defaults, destructured at the
P/Invoke site.

⚠ Negative `timeout_ms` means **unset** (client default), not "zero timeout"
(roadmap §7 gate 3).

### 2.7 Close-out: reword `STATUS.md` line 10 (maintainer-ruled)

The committed `STATUS.md` line 10 references
`design/current/PLAN-M15-admin-client.md`, which is deliberately untracked — a
**dangling path in a fresh clone**. The maintainer ruled: fix it in the close-out,
at the earliest opportunity, which with the split is **P2a's**.

  - **Keep** the D1–D6 decision summary.
  - **Drop** the `design/current/PLAN-M15-admin-client.md` path so no broken
    reference survives. Point instead at the tracked per-phase plans under
    `design/history/M15/`.
  - **Not a standalone commit**, and **no force-push** before P2a starts — it
    rides P2a's close-out commit.

---

## 3 · Tests

Vehicle: **`MockAdminClient`**, no broker. P1 ended at **890 passing** on both
net10.0 and net8.0 — that is the floor; confirm the **count**, never the exit code.

### 3.1 Non-negotiable, inherited from P1's three review rounds

1. **Reflection assertions on every new public signature, sensitivity proven by
   re-injection.** ⚠ **C# upcasts and widens silently** — P1's three shape defects
   survived a green build, 883 green tests **and a 0-High first Critic pass**. A
   behavioural test cannot catch a widened signature. Every type in §2.4 and §2.5
   needs one, each shown red by injection.
2. **Borrowed-vs-owned `KafkaError`, by injection.** Per-key `get_error(i)` is
   `const`/**borrowed** → `FromBorrowedHandle`, **never destroyed**; the
   callback's `error` **parameter** is **owned** → `FromHandle`. Same C type;
   const-ness is the only signal. Verify by deliberately destroying the borrowed
   one, confirming the run goes red, and reverting.
3. **Span-the-op `DangerousAddRef` on every new submit.**
   `AdminClient_destroy` still has **no refcount and no drain**. Each new RPC needs
   the **differential** check: no op in flight → `Dispose` releases; op in flight →
   it does **not**; op completes → it then does. **All three cases** — a
   single-case assertion cannot distinguish a working refcount from a
   permanently-unbalanced one.
4. **`GCHandle` freed exactly once on every path**, including the inline-callback
   path — which §2.3 makes **reachable for the first time**.

### 3.2 P2a-specific

  - **`Uuid` round-trip**: `Uuid` → base64 → ABI → base64 → `Uuid` is identity.
  - **Malformed topic id** faults cleanly through the **inline** path, with handle
    and refcount accounting intact.
  - **D8 wrong-accessor null**: a name-built `DeleteTopicsResult` returns `null`
    from `TopicIdValues`, and vice versa. **Both** result types, **both**
    directions.
  - **D9 absence**: assert by reflection that `DescribeTopicsResult` has **no**
    `All()` member and `DeleteTopicsResult` has **no** typed aggregate. An absence
    is only pinned by a test that asserts it.
  - **Mixed outcome** per RPC — some keys succeed, some fail, each `Task` carrying
    its own outcome. The discriminator against "fault everything on any failure".
  - **`TopicCollection` invariant**: the two factories produce the two sealed
    types; assert by reflection that no ctor is publicly reachable.
  - **Entry-point dispatch**: a name collection drives `delete_topics`, an id
    collection drives `delete_topics_by_ids`. ⚠ **Prove the selection, not just the
    outcome** — the two share a result type, so a naive test passes either way.
  - **Absent-vs-empty**: `has_authorized_operations` false ⇒ `null`, not an empty
    collection; same for `elr` / `last_known_elr`.
  - **G1 regression**: `CreateTopics`'s P1 tests pass **unmodified**, and one P1
    defect re-injected through the generalized code still goes red.

### 3.3 Cross-cutting

  - **TFM-matrix smoke** on net462 (via netstandard2.0), net8.0, net10.0.
  - **DoD §10 (hot-path allocation audit): N/A** — Admin is batch/administrative
    with no per-record path (`admin-client.md §10`). **State it; never skip
    silently.**
  - **DoD §11: N/A** to `IAdmin`, spirit verified — every new RPC is a plain sync
    `fn` returning a `*Result`; only `Close` returns `Task`; no `async` in the
    marshallers.

---

## 4 · Definition of Done

```
cargo build --features ffi            # header MUST be byte-identical (Mode-A proof)
dotnet build -c Release --no-incremental   # 0W/0E across all 6 TFM outputs
dotnet test -f net10.0 && dotnet test -f net8.0
dotnet format --verify-no-changes
cargo xtask format-check && cargo xtask lint     # from the REPO ROOT (false-fails from bindings/dotnet)
```

Plus no `TODO`/`FIXME`, Apache-2.0 header on every new file, §3's tests green with
**counts confirmed**.

**Mode-A proof, every round, against the NEW base:**
`git diff 6aa5fc4a..HEAD -- src/ cbindgen.toml target/include/confluent_kafka.h generator/`
**empty**, header hash byte-identical. A genuine ABI gap **STOPS the phase and
escalates to the Manager** as a Rust-core dependency — `dotnet-actor` does not
author Rust and does not invent a managed workaround.

---

## 5 · Commit hygiene

  - Incremental commits; `fixup!` referencing the original when closing a comment.
  - **Never `git add -A`.**
  - **Do NOT commit:** `COMMENTS.67.md` / `COMMENTS.DONE.67.md` (local working
    files — only the Manager's archived copy under
    `design/history/M15/P2a-key-generalization/` is tracked); the repo-root
    `.claude/agents/dotnet-*.md` discovery copies; anything under
    `.claude/agent-memory/`; `bin`/`obj`; `.DS_Store`.
  - ⚠ **`design/current/PLAN-M15-admin-client.md` stays UNTRACKED.** Keep it
    updated, never `git add` it.

---

## 6 · The rule that produced P1's only real defects

⚠ **THE PLAN IS NOT REVIEW GROUND TRUTH.** P1's plan sketch was wrong **twice**,
and only the Java source caught it — after a green build and a 0-High first
review.

**A `*Result`'s public accessor signature is the contract, not the private field
it is derived from.** `CreateTopicsResult.java:33` is a private
`Map<String, KafkaFuture<TopicMetadataAndConfig>>`; `:43-48` publishes
`Map<String, KafkaFuture<Void>> values()`, erasing the metadata deliberately. The
plan published the private map. Likewise `replicationFactor` is `int` on the
result side and `short` only on the request side.

Every signature in §2.4 was quoted from a Java **public accessor** with a line
citation, precisely so it can be checked. **Where this plan and the Java source
disagree, the Java source wins and this plan is the defect** — report it rather
than implementing it.

The Critic's ground truth is the **C ABI header** + the **Java public API shape**
(`bindings/dotnet/CLAUDE.md §8.2`) — not Rust internals, not Java implementation
logic, and not this document.

---

## 7 · ⚠ Environment traps — these fabricate FALSE PASSES

A check can look green **because the tool never ran**.

1. **`PATH` is clobbered.** `git`, `cargo`, `sed` all appear absent. Start every
   Bash call with
   `export PATH="/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin:$HOME/.cargo/bin:$PATH"`.
   `command -v` is **not** reliable here.
2. **`grep` is aliased to `ugrep`** — rejects some patterns and emits *nothing*, so
   a pipeline reads as a pass. Use `/usr/bin/grep` for anything relied on as
   evidence.
3. **`sed` may be missing** — use `awk` or `/usr/bin/sed` explicitly.
4. **`cat` is shadowed by a missing `bat` alias** — `cat > f <<'EOF'` silently
   writes a **0-byte file and continues**. Use `/bin/cat`.
5. **This is zsh** — unquoted `$var` is not word-split; unquoted globs abort the
   command with "no matches found". Always quote.
6. **A test filter matching zero tests exits 0** printing `0 passed`. **Confirm the
   expected COUNT.**
7. ⚠ **An ABORTED test run ALSO exits 0** — P1's discovery. A double-free aborts
   the test host and `dotnet test` still returns 0; **`Test Run Aborted` in the
   output is the only signal.** Never certify a run by its exit code.

### 7.1 A FALSE-FAIL trap — do not burn a round on it

Two **pre-existing consumer** tests assert
`completingThreadId != continuationThreadId`, which is **unsound** because managed
thread ids are **recycled**:

  - `ConsumerPollBridgeTests.ResultBridge_RunsContinuationsAsynchronously_OffTheCompletingThread`
    (last touched by `26761aa4`, M6) — seen failing with both ids equal to 30.
  - `ConsumerRebalanceListenerBridgeTests.DisposeAsync_WithLiveRegistration_ReturnsWithoutHanging`
    (last touched by `b25bf7f0`, M9/P6).

**Neither file is in P2a's scope.** If one goes red it is **not** a P2a regression
— re-run, confirm, move on. The sound form is `Thread.CurrentThread` reference
identity, or the `[ThreadStatic]` flag P1 used on its own admin probe. Fixing them
is a separate consumer-side slice.
