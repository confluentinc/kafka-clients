# DRAFT for the maintainer to apply — RD1-RD6, RD9, RD10, RD12, RD13, RD14 → `bindings/dotnet/CLAUDE.md`

(RD14 was added at the CP7 review, finding 84.30; the file name predates it.)

**Status: NOT APPLIED. Handed to the maintainer 2026-09-29.**

`bindings/dotnet/CLAUDE.md` is a rule file; agents do not edit it (root `CLAUDE.md`). This
is the Manager's draft of M17/P1's rule changes to it (PLAN D15, §7.2, Q12). Apply, edit or
decline each item; the phase close does not wait. **No agent has modified the file.**
Line numbers were measured (`command grep -n`, `sed -n`) before any item is applied:
apply bottom-up (RD9 first, RD1 last) or re-locate each target by its quoted text.

⚠ **RD2 needs its own sign-off.** RD3's row-480 half removes the same `IsFatal`, and so
do the ffi draft's RD7 (§A5 half) and RD8; decide them together.

## RD1 — §1 Status: transactions and admin are exposed

**Reason.** The sentence predates M15 (admin) and M17/P1 (producer transactions); D15's
RD1 row. `begin_transaction_async` is unbound by decision (B8, D1, Q5).

**Insertion point.** `bindings/dotnet/CLAUDE.md:58-59`. Current text (replace the second
sentence only):
```text
`poll_async` is the only `_async` fn taking a timeout. Admin / transactions are
**not** exposed yet. Source of truth for the surface = `src/ffi/*.rs` +
```

**Proposed text.**
```markdown
`poll_async` is the only `_async` fn taking a timeout. Admin (M15) and producer
transactions (M17/P1) are exposed; `begin_transaction_async` is deliberately unbound
(§4 **Stays sync**). Source of truth for the surface = `src/ffi/*.rs` +
```

## RD2 — §3 sketch: the `KafkaException` shape ⚠ SEPARATE SIGN-OFF

⚠ **This corrects pre-existing drift, not something M17/P1 introduced** (Q12 asks for a
separate yes/no). The plan records `IsFatal` as never shipped; today
`command grep -rn 'IsFatal' --include='*.cs' bindings/dotnet/src` finds one line, the
remark at `KafkaException.cs:74` ("There is no `IsFatal` flag", `:74-81`; the plan's
`:47-54` is its planning-time position), and `command grep -c 'is_fatal' target/include/confluent_kafka.h`
prints `0`. D6's five properties are at `KafkaException.cs:204`, `:231`, `:269`, `:296`, `:319` (the CP6 worktree).

**Insertion point.** `bindings/dotnet/CLAUDE.md:154-158`, inside §3's producer ```` ```csharp ````
block. Current text (replace all five lines):
```text
public class KafkaException : Exception {        // flat, for now (§4, ffi §A5)
    public int Code { get; }
    public bool IsRetriable { get; }
    public bool IsFatal { get; }
}
```

**Proposed text.**
```csharp
public class KafkaException : Exception {        // flat, for now (§4, ffi §A5)
    public int Code { get; }
    public bool IsRetriable { get; }                     // Java RetriableException
    // Hierarchy predicates (M17/P1 D6; root CLAUDE.md §10.4) — NOT complements (29/53 answer
    // both Authorization and InvalidConfiguration; 45/59 both OutOfOrderSequence; 48 none):
    public bool IsTransactionAbortableError { get; }     // TransactionAbortableException (a leaf — bound, D6)
    public bool IsApplicationRecoverableError { get; }   // ApplicationRecoverableException
    public bool IsInvalidConfigurationError { get; }     // InvalidConfigurationException
    public bool IsAuthorizationError { get; }            // AuthorizationException
    public bool IsOutOfOrderSequenceError { get; }       // OutOfOrderSequenceException
    // No IsFatal — never shipped: the core keeps fatality off the ABI (contextual, §10.4).
    // A leaf such as ProducerFencedException is matched by Code (== 90).
}
```

## RD3 — §3 idiom map: rows 478 and 480

**Reason.** Row 478's producer examples omit the four blocking transaction methods, which
D1 maps to `Task` (`KafkaProducer.java:648`, `:732-745`, `:779`, `:813`). Row 480 lists
`IsFatal` and none of D6's five predicates. D15's RD3 row.

**Insertion point 1.** `bindings/dotnet/CLAUDE.md:478`. Current text of the first cell
(the rest of the row is unchanged):
```text
| **blocks** in Java, **or** returns `Future<T>`, **or** takes a completion callback — any one is enough (producer `send`/`flush`/`close`/`partitionsFor`; consumer `poll`/`commitSync`/`position`/`subscribe`/`assign`/`pause`/`resume`/`unsubscribe`) |
```

**Proposed text** (first cell only):
```markdown
| **blocks** in Java, **or** returns `Future<T>`, **or** takes a completion callback — any one is enough (producer `send`/`flush`/`close`/`partitionsFor`/`initTransactions`/`sendOffsetsToTransaction`/`commitTransaction`/`abortTransaction`; consumer `poll`/`commitSync`/`position`/`subscribe`/`assign`/`pause`/`resume`/`unsubscribe`) |
```

**Insertion point 2.** `bindings/dotnet/CLAUDE.md:480`. Current text (replace the row):
```text
| `KafkaException` hierarchy | one flat `KafkaException` (`Code`/`IsRetriable`/`IsFatal`) | ffi §A5 |
```

**Proposed text.**
```markdown
| `KafkaException` hierarchy | one flat `KafkaException`: `Code`, `IsRetriable`, and the five hierarchy predicates `IsTransactionAbortableError` / `IsApplicationRecoverableError` / `IsInvalidConfigurationError` / `IsAuthorizationError` / `IsOutOfOrderSequenceError` (M17/P1). Java `instanceof` an intermediate class → the matching property; a leaf class → compare `Code`. No `IsFatal` | ffi §A5; root `CLAUDE.md` §10.4 (naming, polarity, both-directions test); M17/P1 D6 |
```

## RD4 — §3 idiom map: new row for Java deprecation

**Reason.** D2 projects Java's `@Deprecated(since = "4.2", forRemoval = true)` constructors
as warning-level `[Obsolete]` over a non-obsolete factory (`ConsumerGroupMetadata.cs:147`).
⚠ **"First adopter" (the plan's word) is inaccurate as measured:** `command grep -rln 'Obsolete(' --include='*.cs' bindings/dotnet/src`
lists 12 files — `ConsumerGroupState.cs` (its `:43-47` already argues `forRemoval`), ten
under `Admin/`, and `ConsumerGroupMetadata.cs`, whose are the only `[Obsolete]`
**constructors** (`command grep -rn -A3 '^\s*\[Obsolete' --include='*.cs' bindings/dotnet/src`).
The factory rule is a preference: `command grep -rln 'CS0618' --include='*.cs' bindings/dotnet/src`
lists 5 files, three of them `Internal/` admin/group files with `#pragma warning disable`.

**Insertion point.** After `bindings/dotnet/CLAUDE.md:481`, whose current text is:
```text
| `IllegalArgumentException` / `IllegalStateException` | `ArgumentException` (family) / `InvalidOperationException` (`ObjectDisposedException` when used after close) | validate **before** the FFI call — ffi §A5 |
```

**Proposed text** (a new row, inserted as line 482):
```markdown
| `@Deprecated` / `@Deprecated(forRemoval = true)` | `[Obsolete("<Java's deprecation text, adapted>")]` at **warning** level — never `error: true` (`forRemoval` raises javac's removal *warning*; it does not reject the call). Where the binding itself calls an obsolete **constructor or member** of a type that is not itself obsolete, it goes through a non-obsolete internal factory so that call site needs no `CS0618` suppression; an obsolete **type** cannot be constructed that way, so `src/` builds it under a local `#pragma warning disable CS0618` / `restore` pair (`AdminCallbacks.cs:874-885` is an example); tests and `grpc-server` suppress `CS0618` locally | M17/P1 D2 (`ConsumerGroupMetadata`'s two constructors — the first on a constructor); the M15 admin types (`ConsumerGroupState`, `ListConsumerGroupsResult`, …) for types and members |
```

## RD5 — §4 **Stays sync**, producer: `BeginTransaction()` and the mock helpers

**Reason.** `BeginTransaction()` was listed ahead of shipping. It is now on both interfaces,
`void` on the async one too (D1, Q6), and its async form waits on a bounded drain first
(D3). The four mock-helper members are sync actions and state reads (D8).
(Checked 2026-09-29 against the CP6 worktree: `void BeginTransaction()` at
IAsyncProducer.cs:382 and IProducer.cs:277; the 30 s bound is `s_accumulatorDrainTimeout`,
NativeProducer.cs:144; the four helpers are at MockProducer.cs:278, :287, :298, :321 and
AsyncMockProducer.cs:345, :354, :365, :388.)

**Insertion point.** `bindings/dotnet/CLAUDE.md:575-576`. Current text:
```text
so its defensive null guard carries a producer-accurate message, decision D-5), `BeginTransaction()`,
and the two metric-subscription methods.
```

**Proposed text.**
```markdown
so its defensive null guard carries a producer-accurate message, decision D-5), `BeginTransaction()`
(**shipped M17/P1** on both interfaces — Java's `beginTransaction` is a local state transition; the
async form first drains the binding's send accumulator with a bounded 30 s wait, D3, and the sync
form has no accumulator), the mock transaction helpers on `MockProducer` / `AsyncMockProducer`
(`SetCommitTransactionError` / `ClearCommitTransactionError` / `SentOffsets()` /
`CommittedOffset(groupId, partition)`, M17/P1 D8), and the two metric-subscription methods.
```

## RD6 — §4: new divergence note for transaction control

**Reason.** D3-D5 add binding-side ordering Java does not need; D15 asks for the file's
`⚠ **§4 divergence — …**` marker (at `:579`, `:599`, `:629`, `:660`). Residuals are pointed
to, not restated (ffi §A6 round-5 rule; canonical home: `IAsyncProducer` remarks, X5).

**Insertion point.** Between `bindings/dotnet/CLAUDE.md:764` and `:766` — after the
delivery-callback divergence, before the `ConsumerHandle` row. Current text at those lines:
```text
  null-accepting form non-redundant.

⚠ **§4 reentrancy row — `ConsumerHandle` is host scaffolding that restores a Java
```

**Proposed text** (inserted after line 764, **preceded and** followed by a blank line, so Markdown does not fold it into the bullet that ends at `:764`):
```markdown
⚠ **§4 divergence — transaction control drains the send accumulator and orders
completions (M17/P1).** The five Java transaction methods ship on both producer interfaces
(async: four `Task` members + a sync `void BeginTransaction()`; sync: five blocking
members). Java's `send()` registers the record with the transaction synchronously; this
binding's async `Send` buffers it in a managed accumulator the core cannot see. Three
binding-side rules restore Java's contract, and none adds Kafka logic:

- **Drain before control (D3).** Every control entry point first drains the accumulator,
  so every `Send` that had **returned** has reached the core via `send_batch`: commit
  includes it, abort discards it, a later `BeginTransaction` never sweeps it in
  (`producer-transactions.md` §13, mirrored for the binding's own buffer). `Task`
  forms await the drain, bounded only by the token. The async `BeginTransaction()` waits at
  most 30 s, then throws `KafkaException` (`Code == 0`) *"The producer's send accumulator
  did not drain within 30 seconds, so records buffered in the binding have not reached the
  core and beginTransaction() was not attempted."* without calling native. The sync surface has no accumulator; its drain is a no-op.
- **Barrier for commit / abort (D4).** The async surface enqueues a barrier on the
  completion pump after the drain and before the native submit, and awaits it only after
  native **success**, so every send `Task` and delivery callback of the transaction's
  records has completed when the call does (`KafkaProducer.java:754-755`). Nothing is
  promised on failure or once the pump's gate has closed. The sync surface needs none.
- **Cancellation abandons the wait, not the operation (D5).** Cancelled at entry →
  `OperationCanceledException`, synchronously; during the drain → the `Task` is cancelled
  and native never called; after submit → the awaiter is cancelled, the operation runs on,
  and a retry meanwhile fails with Code -2 (not a reason to abort); during the barrier wait
  → the `Task` completes **successfully**, since the commit or abort happened.

The residuals R-a/R-b/R-c are enumerated once, in the `IAsyncProducer` remarks; the design
record is `design/history/M17/P1-producer-transactions/PLAN.md` D3-D5; the mechanics are
ffi §A7.
```

## RD9 — §8.4: per-checkpoint gate logs (OPTIONAL)

**Reason.** Q20 keeps short gate summaries at `gate/CP<n>.txt` under the phase's history
folder (at the CP7 close HEAD holds CP0-CP2, CP3.txt is staged, and CP4-CP7 are on disk, untracked, because the user commits each checkpoint from a snapshot). D15: **no change
unless the user wants it as a standing convention**; declining changes nothing. D15's
alternative home is the STATUS header.

**Insertion point.** After `bindings/dotnet/CLAUDE.md:1069` (the end of the "Plans &
design docs" bullet), whose current text is:
```text
  execution. Never commit `.DS_Store` here.
```

**Proposed text** (a new bullet, inserted as line 1070):
```markdown
- *(Optional convention, M17/P1 Q20.)* Gate logs: each checkpoint's commands, exit codes
  and counts go in `design/history/<Milestone>/<Phase>/gate/CP<n>.txt` — a short text
  summary committed with the checkpoint, not raw logs, which stay in the Actor's scratch
  directory. A later gate compares against the **stored** baseline, never a reconstructed
  one (M15/P12's method change).
```

## RD10, RD12, RD13 (record half) — §7.5 Definition of done: gate evidence

**Reason.** RD10: a libtest filter that selects nothing exits 0 (Critic 84, CP0, finding
84.1). RD12: table cells force `\|` escapes that change a copied command's meaning, and a
"= 0" check with no before-value cannot be shown able to fail (finding 84.6; PLAN §4.2
lists five such escapes). RD13's record half: counts went stale and an absence claim was
missed by a one-phrase grep (findings 84.7-84.9; file-forward item 13). All added after
approval.

**Insertion point.** After `bindings/dotnet/CLAUDE.md:1007`, the end of §7.5, whose
current text is:
```text
A port is not done until it builds on the TFM matrix, unit tests pass against
`MockProducer` / `MockConsumer`, lint/format are clean, and the `ffi-marshalling.md` anti-patterns
are satisfied (`definition-of-done.md`). Integration/multi-language suites are opt-in until
CI-stable.
```

**Proposed text** (a blank line, then):
```markdown
**Gate evidence (M17/P1).** What a gate step, a gate record and a phase plan may count:

- **A filtered test run passes on its asserted count, not on its exit code** (RD10). A
  gate step running a filtered selection — libtest `-- <filter>`, `--exact` or `--skip`, or
  `dotnet test --filter` — passes only when the reported selected count is asserted
  (libtest's `running N tests` / `N passed`, VSTest's `Total: N`). A libtest filter that
  selects nothing exits 0.
- **Gate commands live in list items or code blocks, never in table cells** (RD12). A
  cell's code span needs each `|` escaped as `\|`, the raw file keeps the backslash, and
  under basic `grep` the same `\|` is alternation — so a copied command can silently stop
  being a pipeline, or stop being able to fail. Where a cell cannot be avoided, the plan
  says for each escaped pipe whether it is markdown's escape or regex alternation. Every
  "= 0" count check records its **before-value** as the control positive.
- **A count or cause in a record is measured when it is written** (RD13). This covers gate
  records, commit messages and plan hand-offs: measure at the base and at the worktree and
  quote both values; count a claim about the ABI from the generated header, not from the
  binding's bound subset. A claim that something is **absent** is checked by a search that
  does not depend on one phrasing — read every matching block whole — or it is not made.
```

## RD13 (test half) — §7.4 Test conventions

**Reason.** A test doc comment claimed a property its assertions could not detect (Critic
84, CP1, finding 84.4). This is the test-side twin of ffi §A6's round-5 rule, that a claim
which is not made cannot go stale. Added after approval.

**Insertion point.** After `bindings/dotnet/CLAUDE.md:1000`, the last §7.4 bullet, whose
current text ends:
```text
  test — on the **send path** (producer) and, for the consumer, on the
  **receive path** (`consumer-threading.md §27`; the copy-out budget).
```

**Proposed text** (a new bullet, inserted as line 1001):
```markdown
- **A test's doc comment claims only what its assertions can detect.** Mutation-check the
  claim — break the property it names and watch the test fail — or drop it. This is the
  test-side twin of ffi §A6's round-5 rule: a claim that is not made cannot go stale.
```

## RD14 — §7.5 Definition of done: further record and review rules from Critic 84 (added at the CP7 review, finding 84.30)

**Reason.** These are Critic 84 rule suggestions that the Manager set aside for these
drafts; the first version of this file omitted them.
The exhaustiveness-count rule and the Java-remark rule are also recorded in
`COMMENTS.DONE.84.md` (the cycle-6 CP1 check and the CP2 decision on 84.10).

**Insertion point.** Directly after the RD13 record-half bullet that the RD10/RD12/RD13
item above inserts into §7.5. Apply that item first.

**Proposed text** (a separate list with its own lead-in, after that block):
```markdown
How a record and a review establish a claim:

- **A list of "sites that do X" comes from a sweep over every construction form** (public
  constructors included), not from a probe of one symbol's callers (M17/P1 finding 84.10:
  a one-symbol probe missed a result built through a public constructor). An absence probe
  **deletes** the symbol rather than retyping it.
- **For an exhaustiveness claim, give the matched count and the count left after reading**
  (both numbers), and make an "every block" sweep of the header include the
  function-pointer typedef doc blocks.
- **Check each "Java does X" remark against the Java source at the pinned commit**, never
  against the binding's own comment. When a checkpoint corrects one false "Java does X"
  remark, sweep the rest of that file's Java remarks, constructor and parameter types
  included (M17/P1: `TopicMetadataAndConfig`'s "Java wraps" and "(Throwable)").
- **Store the command that generated a dry-run diff next to the diff** (for example
  `git diff --no-index -U2 <a> <b>`), so a byte-for-byte check does not depend on guessing
  the context width.
```

## Noticed while measuring — not drafted

`CLAUDE.md:46` ("The C ABI exposes the **producer** and **consumer**."), `:422` ("The
**admin client** (`IAdminClient`) is still **Mode B**") and `:526` ("`IProducer` still
deferred") predate shipped work; no RD covers them.
