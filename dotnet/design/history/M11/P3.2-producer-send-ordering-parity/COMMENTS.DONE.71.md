# Critic 71 — M11/P3.2 · RESOLVED

Findings filed against `c28b491c` (slice S0) and resolved in the S0 fix round.
All three were in one file, `design/current/python-binding-send-batching.md`
(the F8 deliverable). All three judged real by the Manager; none contradicted an
approved decision (D1–D5), so none needed a stop.

**Fix commit:** `fixup! docs(M11/P3.2 S0): supersede the stale Option-C records and file the deviation list`

---

## 71.1 · MEDIUM · the doc's Summary still asserted the exact claim F8 was filed to remove — RESOLVED

**Was:** `:35-37` still read *"Stage 1 is **Python-specific**. The .NET binding has
no equivalent: it calls the singular `Producer_send` inline on the caller's
thread, so `linger.ms` is its only delay."* — all three clauses false of the async
surface as shipped, and each denied by text the same commit added 373 lines below.
The file was left self-contradictory, which is worse than the uniform staleness it
started from: the Summary is the normal reading path for a document whose
"Implications" section is 400 lines in.

**Accepted the finding in full.** The Critic's own justification is the decisive
one: the literal-scope defence ("the plan said §408-459") was not available to me,
because I had already extended past that range to fix the code-reference index on
the strength of F8's *purpose* rather than its line numbers. The same reasoning
applies to a claim more strongly than to a cite table.

**Fixed** at `:42-51` — the Summary now states the split (async adopted stage 1 in
M11/P3.1, so the 0–10 ms window applies there on top of `linger.ms`; sync still
inline, `linger.ms` its only delay, DV-6). The **original sentence is quoted
verbatim** inside the correction rather than deleted, because the rest of the
document was written against it (the supersede-by-quoting practice this record
already follows).

**One consequence the finding did not name, found by the sweep it asked for and
fixed here.** Saying ".NET adopted the same shape" silently falsifies three
*other* places that call stage 1 "hardcoded / untunable" — the Summary diagram
(`:29`), "The headline" (`:333`), and Observation 2 — all of which are true of the
anchor (a bare literal at `_confluentkafka.c:535`) and false of .NET (named
constant, env-overridable, P3.1 §3.2/§3.3). A new Summary paragraph (`:53-59`)
scopes "untunable" to Python for the whole document, and `:39-41`'s *"no
configuration can influence"* became *"no **Python-side** configuration can
influence"* so the claim is self-contained at the point a reader meets it instead
of relying on a note further down — which is the same reading-order failure as
71.1 itself.

---

## 71.2 · MEDIUM · the `Py_INCREF` rewrite claimed the fragmentation objection was *removed*; it is reduced and accepted — RESOLVED

**Was:** `:482-483` claimed interning *"removes the fragmentation the earlier
analysis charged against the port"*, dropping P3.1 §4.1's load-bearing hedge
*"a **whole class of** fragmentation"*; and `:508-512` said *"Neither was taken"*
of the objection's two alternatives, presenting argument 2 as **resolved** where
§F8(c) required an **accepted cost**.

**Verified the finding's mechanism in code before writing the fix** (not taken on
the Critic's word):

- `SendAccumulator.cs:269-270` — `Submit` pins key and value per record via
  `ProducerSendBatchMarshal.PinIfNeeded`, which is `buffer.Value.Pin()`
  (`ProducerSendBatchMarshal.cs:98`).
- `SendAccumulator.cs:907` — `ReleasePins(node, count)` in the `finally`, i.e.
  only *after* `send_batch` returns.
- `SendAccumulator.cs:716` — `ReleaseSpace(freed)` runs at **take** time, ahead of
  `SendChain()` at `:722`. So senders can refill to the full
  `MaxAccumulatedRecords` bound while the taken chain is still pinned, and the
  live pinned count reaches the same order as the "up to 2000" the objection
  named. **Finding confirmed: the pin alternative was taken in substance.**
- `PLAN.md:510` — P3.1 §4.1's wording is indeed hedged; `:517-518` records the
  1024-topic-cap fallback that restores a third pin.

**Fixed** three ways:

1. `:508-512` restores **"a whole class of"** and attributes it to the *topic's*
   class specifically, crediting P3.1 §4.1 as the source of the hedge.
2. The section heading became *"the **lifetime** problem is resolved"* (was
   "resolved, not worked around"), with a new `:517-519` splitting the two halves
   and forward-pointing to argument 2.
3. Argument 2 (`:540-567`) rewritten as an accepted cost: the pooled-buffer copy
   was **not** taken (root `CLAUDE.md` §12) but the per-in-flight-record pin
   **was**, with the four cites above, the note that `MemoryHandle` vs `GCHandle`
   changes the API and not the heap effect, and — matching argument 1's
   disposition — that the cost is **unmeasured**, pointing at P3.1 §8.2.

**Also folded in the item the Critic explicitly did not file** ("worth folding
into 71.2's rewrite if that is being touched anyway"): `:507`'s unqualified
*"drops the per-record pin count from 3 to ≤2"* now records P3.1 §4.1's D5
fallback, which restores the third pin beyond the 1024-topic cap. Added the
symmetric ceiling note too — `PinIfNeeded` returns `default` for absent/empty
buffers, so ≤2 is a ceiling a keyless, valueless record does not pay.

---

## 71.3 · LOW · two of three sibling-record cites lacked the `bindings/dotnet/` prefix — RESOLVED

**Was:** `:435-436` cited `design/history/M11/P3-producer-send/PLAN.md` and
`design/current/producer-send-completion-approaches.html` unprefixed, in the same
sentence as a correctly-prefixed third cite.

**Verified with a control positive:** both unprefixed forms are **MISSING**; both
`bindings/dotnet/`-prefixed forms **EXIST**. The Critic's "plausible wrong
destination" point also checks out — the repo-root `design/current/` really does
contain a `status.md`, so a reader normalising the third cite the other way has
somewhere wrong to land.

**Fixed:** both prefixed, plus an explicit parenthetical that **all three** are
under `bindings/dotnet/` and none under the repo-root `design/` this file lives
in — so the next reader cannot re-derive the same ambiguity from one prefixed and
two bare paths.

---

## Sweep performed as directed (the Critic's generalisation, applied to this file only)

Both MEDIUMs existed because the rewrite covered the section the plan named and
not the rest of the document. Swept the **whole file** for surviving pre-P3.1
claims: `grep -n '\.NET|dotnet|Option C|Option A|linger\.ms|Producer_send'` over
every line **outside** the rewritten range, then read the header, Summary, both
Observations and the whole Stage-2 / `linger.ms` discussion.

**Found and fixed:** `:35-37` (71.1); the three "untunable" sites and `:39-41`
(71.1's consequence, above); the header's `:3-17` framing, which still described
the .NET content as *what it implies for* the .NET binding — now marked as no
longer a prediction, with the .NET claims' own corpus branch named separately from
the Python corpus branch.

**Checked and deliberately NOT changed — every one is a Python-side claim that is
still true:** the two-stage diagram (`:26-33`); *"Stage 1 exists to amortise the
Python↔C boundary cost"*; all of Stage 1's thresholds/timer/queue analysis; the
whole Stage 2 section including the 500-record case-A example, the
`record_accumulator.rs:1091-1098` `sendable` quote and the `batch.size`/`linger.ms`
defaults; Observation 1 (the Python `flush()` accumulator gap); Observation 2 (the
bare literal at `_confluentkafka.c:535`, no `#define`, not surfaced through
`ProducerConfig`); and both anchor cite-index tables. Recorded so a later reader
can tell "checked and clean" from "not looked at".

**Not done, per the Manager's instruction:** the Critic's suggested rulebook
addition (*"a supersession slice sweeps the whole document, not just the named
section"*) is a **user decision** and was surfaced, not written. `bindings/dotnet/CLAUDE.md`,
root `CLAUDE.md`, `ffi-marshalling.md` (§A1 is S1's) and `STATUS.md:20` (S4's) are
all untouched.

---

## Verification (fix round)

`cargo build --features ffi` clean · `dotnet build` **0 Warning(s) / 0 Error(s)** ·
`dotnet test -f net10.0` **880 passed / 0 failed** (unchanged — the round is
documentation only; a changed count would have meant a behaviour change) ·
`Test Run Aborted` grepped on every run, **0 matches** ·
`dotnet format --verify-no-changes` clean ·
`git diff --stat HEAD -- src/ src/ffi/ confluent_kafka.h cbindgen.toml` **EMPTY**
(Mode A held).

Only `design/current/python-binding-send-batching.md` changed in this round — the
two `.cs` xmldoc edits and the P3.1 PLAN amendments from `c28b491c` drew no
findings and were not touched.

---

# Critic 71 · round 2 — RESOLVED

One finding, filed against the S0 fixup `e38e14d9` — i.e. against replacement text
the previous round introduced, not against `c28b491c`. Accepted in full.

**Fix commit:** `fixup! docs(M11/P3.2 S0): supersede the stale Option-C records and file the deviation list`

---

## 71.4 · LOW · the document-wide scoping note enumerated three "untunable" sites; there are four — RESOLVED

**Was:** `design/current/python-binding-send-batching.md:55-56` read *"Where stage
1 is called hardcoded or untunable below **(the diagram above, "The headline", and
Observation 2)**, that is true of the anchor"*. Four sites call stage 1 hardcoded
or untunable: `:29` (Summary diagram), `:338` (the diagram under *"Total, for the
500-record case-A example"*), `:349` (*"The headline"*), `:416` (Observation 2).
`:338` was not named. It and `:349` are eleven lines apart in consecutive `###`
subsections and are the *same construct* — an ASCII bar diagram labelling the
stage-1 half — so the omission was an oversight, not a scoping decision.

**Accepted in full.** The Critic's non-stylistic argument is the decisive one: an
enumeration that *positively excludes* `:338` licenses the inference that `:338`'s
"untunable" holds for **both** bindings — the same mid-document reading-order
failure I had myself cited as the reason for rescoping `:39-41` in place rather
than leaning on a note further down. Nothing false was asserted **about .NET**
(the note's blanket leading sentence covers `:338` in substance, and `:334-341` is
a continuation of the Python case-A example at `:179`), which is why the Critic
graded it LOW and why this needed no behaviour change.

**Fixed by DELETING the enumeration, not by naming the fourth site** —
`:55` now reads *"**Wherever** stage 1 is called hardcoded or untunable, that is
true of the anchor"*. This is the second of the two corrections the finding itself
offered (*"or drop the enumeration entirely and let the blanket first sentence
carry the whole document"*), and it is the one the recorded practice requires:
**incrementing a count is the same trap as re-scoping one** — "three sites" → a
fourth appears is precisely the shape that produced this finding, and naming four
would leave a live claim about the set that goes stale the next time a site is
added. Deletion is strictly *shrinking* in claim-space, so unlike any re-wording
it cannot introduce a new false clause; `Wherever` governs a strict superset of
the enumeration it replaced, including any future fifth site. The two clauses the
finding did **not** dispute — the anchor's bare literal and .NET's named
env-overridable constant — are carried through **unchanged**, so the fix adds no
new claim of any kind.

**A defect the finding did not name, introduced by this very fix and caught by the
re-sweep.** The first draft deleted only the parenthetical and left the word
*"below"*. That silently **excluded `:29`**, the one governed site that is *above*
the note — the old text was internally awkward (*"below (the diagram above …)"*)
but did at least name it, so deleting the parenthetical alone would have traded a
site-omission for a positional one, at a different site. `"below"` was therefore
dropped too; the sentence is now positionally unqualified, matching its own leading
*"this whole document"*. The single surviving `"below"` at `:59` is the directional
pointer to the "Implications for the .NET binding" section, which is genuinely
below. Recorded because it is the third instance in this phase of a *correction*
manufacturing the next round's defect.

**Sweep, run mechanically rather than by recall — this is what the finding asked
for and what the previous round did not do.** One `grep -niE` over the whole file
for every governed construct — `untunable|un-tunable|hardcoded|hard-coded|no
equivalent|Option C|blocker|only delay` — with a **control positive** in the same
pattern (`## Stage 1:`, known present) so a zero-match could not masquerade as
clean. 19 hits, each classified against the criterion:

  - **Governed, now covered without a count:** `:29`, `:338`, `:349`, `:416`.
  - **Not governed, verified scoped at the site:** `:44`/`:46` (inside the ⚠-marked
    verbatim quote of the superseded claim — 71.1's fix shape); `:51` (*"the only
    delay"*, correctly restated for the **sync** producer only, DV-6);
    `:434`/`:435`/`:438` (inside the `>` blockquote of the superseded record, framed
    as what it *said*); `:444`/`:448`/`:450` (*"Option C"*, scoped per row —
    *".NET is not 'Option C' as a whole"*); `:477`/`:495`/`:517` (*"blocker"*, framed
    as resolved / *was* "blocker"); `:526` (the one .NET-scoped use, which says so
    at the site: *"accepted, and no longer untunable"* — it *negates* the word, so
    the new `Wherever` sentence does not reach it).
  - **`:54`/`:55`** are the rule itself, not an instance.

**No fifth site.** The sweep's probe was proven *sensitive* rather than merely
green: the same count/enumeration pattern was run against `HEAD`'s version of the
paragraph in the same command and came back **RED** (matching the old *"diagram
above, "The headline", and Observation 2"*) while the working tree came back
**clean** — a green probe that was never red proves nothing.

**Python re-verified, and the `:39-41` rescoping was not over-corrected.** The
anchor's window genuinely is a bare literal: `bindings/python/_confluentkafka.c:535`
— `timeout = now + 10000000; // 10ms`, with no `#define` for it and no config key
(the `#define`s at `:19-27` are the *record* thresholds, `PRODUCER_RECORD_SLOT_*`,
not the window). .NET's genuinely is named and env-overridable, read **once** at
construction: `SendAccumulatorSettings.cs:67` (`DefaultBatchWindowMs = 10`), `:71`
(`CONFLUENT_KAFKA_PRODUCER_BATCH_WINDOW_MS`), `:147` (`ReadPositive(WindowVariable,
DefaultBatchWindowMs)` inside `FromEnvironment()`), whose sole call site in `src/`
is `NativeProducer.cs:967`, under a `??=` — confirmed as the *only* reference by
grep. `:39-41`'s *"no **Python-side** configuration can influence"* therefore
remains true of Python: the .NET override cannot reach the C extension's literal.

**Superseding 71.1's own enumeration, rather than rewriting it.** The 71.1 record
above (`:36-38`) says *"silently falsifies **three** other places … the Summary
diagram (`:29`), "The headline" (`:333`), and Observation 2"*. That is the same
undercount, and its `:333` is a pre-insertion line number for what is now `:349`.
It is left as written, because it records what was believed at the time; **this
entry is the correction** — the set is four, and the document no longer enumerates
it at all.

**Out of scope, confirmed untouched.** The pending `bindings/dotnet/CLAUDE.md` rule
(*a supersession slice sweeps the whole document*) remains an **open user
question** and was not written — applying the practice is in scope, changing the
rulebook is not. No `CLAUDE.md`, nothing under `.claude/rules/` (so
`ffi-marshalling.md` §A1 — S1's) and no `STATUS.md:20` (S4's) edit. The deviation
list still files DV-1/DV-2/DV-4/DV-6 only; no residual-axes paraphrase was added
(the axes stay solely at `IDeliveryCallback.cs:141-232`); no `.cs` file was
touched; none of S1–S5 was implemented.

---

## Verification (round-2 fix)

`cargo build --features ffi` clean ·
`dotnet build` **0 Warning(s) / 0 Error(s)** ·
`dotnet test -f net10.0` **880 passed / 0 failed / 880 total** — unchanged, as a
documentation-only round requires; a changed count would have meant a behaviour
change ·
`Test Run Aborted` grepped on the run, **0 matches** (a double-free aborts the host
while `dotnet test` still exits 0, so this string is the only signal) ·
`dotnet format --verify-no-changes` clean ·
`git diff --stat c28b491c~1..HEAD -- src/ src/ffi/ target/include/confluent_kafka.h cbindgen.toml`
**EMPTY** (Mode A held across all three S0 commits).

Only `design/current/python-binding-send-batching.md` changed — a single paragraph,
6 lines for 6.

---

# Round 3 — slice S1, commit `01cf014c` · RESOLVED

Three findings filed against `01cf014c` (slice S1) and resolved in the S1 fix
round: one MEDIUM memory-model defect in the submitter handshake, two LOW comment
corrections. All three judged real by the Manager. None contradicted an approved
decision (D1–D5), so none needed a stop; the 71.5 fix stayed inside the
one-token swap the finding prescribed, so the "report before restructuring the
handshake" stop condition was not reached either.

**Fix commit:** `fixup! feat(M11/P3.2 S1): submission order is call order — routing + a FIFO submission queue`

**Scope held.** Only 71.5/71.6/71.7 were addressed. S2 (close-completes), S3
(grouping), S4 (pre-stop drain) and S5 (`_spaceGate.Cancel()` in
`AbandonOnThreadFailure`) were **not** implemented — `_spaceGate.Cancel()` still
occurs exactly once in the file, at `Stop`, so F5/S5 stays separable; teardown
keeps this slice's **FAULT** semantics (`ClosedDuringBackpressure()`). The sync
`Send`/`Flush`/`Close` path, all public API, `STATUS.md`, every `CLAUDE.md` and
both allocation-budget test files are untouched (`PerSendBudgetBytes` is still
512). Mode A held. The pending `bindings/dotnet/CLAUDE.md` supersession-sweep rule
proposal remains **undecided with the user** and was not written.

---

## 71.5 · MEDIUM · the submitter exit handshake had no store→load fence — RESOLVED

**Was:** `SendAccumulator.cs:441` released the single-submitter token with
`Volatile.Write(ref _submitterRunning, 0)` and then read `_submissions.IsEmpty`.
That is one side of Dekker's pattern against `SubmitQueued`'s
enqueue-then-`CompareExchange`, and only the producer side was fenced. A
`Volatile.Write` is a **release** store: it orders earlier writes against the
store and says nothing about a **later load**, so "the submitter read the queue
empty" and "the producer read the token still taken" could both hold on any
target that buffers stores. The `||` short-circuits past the `CompareExchange` on
exactly the path where it matters, so the CAS could not supply the missing
barrier.

**Accepted in full, and the symmetry argument is the reason.** The Critic applied
the same standard this slice itself wrote into `ffi-marshalling.md` §A1 — *"Do not
rest this on `SemaphoreSlim` fairness — the .NET documentation guarantees no
ordering"*. A design that needs an unguaranteed ordering is a defect even where it
passes today; that standard cannot be applied to `SemaphoreSlim` in the rulebook
and declined for `_submitterRunning` in the code.

**Fixed** at `:465` — `Interlocked.Exchange(ref _submitterRunning, 0)`. One token,
a full fence, and it makes the two sides symmetric. No structural change: the
re-check, the `IsEmpty` short-circuit and the CAS are all unchanged. The comment
now states why `Volatile.Write` was insufficient and why the fence has to be on
the *store* rather than the CAS, and the field declaration records that **both**
sides of the handshake are `Interlocked` — that symmetry is what makes the exit
re-check sound. With the fence the re-check is sound by contract: `SubmitQueued`'s
`CompareExchange` is itself a full barrier placed after its `Enqueue`, so a stale
`1` read there orders the enqueue before the submitter's `IsEmpty` read.

**No honest regression test exists, and this is the measurement rather than an
assertion.** I built a targeted stress probe (bound of 1, one inline + one queued
send, an armed drain, then a third send racing the submitter's exit; a strand is
detectable because it keeps `_queued >= 1`, so the two-stage idle predicate never
holds and `DrainPending` fails at its bound). Measured on this host
(darwin-arm64, net10.0):

| Build under test | Windows sampled | Result |
|---|---|---|
| **post-fix** (`Interlocked.Exchange`) | 300 000 × 3 runs | **PASS 3/3** |
| **pre-fix** (`Volatile.Write` restored) | 300 000 × 5 runs | **PASS 5/5** — 0 failures in 1 500 000 windows |
| control: exit re-check disabled (the *software* window opened wide) | 300 000 | **PASS** |
| control: submitter never starts (a *certain* strand) | — | **FAIL on iteration 0**, `queued = 2` |

The last two rows are what make the verdict honest rather than a shrug. The
final control proves the probe's **detection** half works — an actual strand fails
it immediately, with its own message. The third row proves the probe does not
reliably **generate** the interleaving at all: even with the re-check removed
entirely — which widens the *software* window to "enqueue between the failed
dequeue and the store" — 300 000 attempts produced nothing. So the probe cannot
discriminate the fence, and a stress test that passes either way is not evidence
(the local standard, and the exact failure mode P3.1 caught twice). The probe was
therefore **deleted, not committed**.

The reason is structural, not effort: the software interleaving is closed by the
**re-check** (and would be reachable only by injecting a delay *inside* the
handshake, i.e. by adding a test seam to production code), while the hardware
interleaving is closed by the **fence** and its window is one store-buffer drain —
tens of cycles, not under program control, and not wideable by any managed
instrumentation. On this arm64 host it is narrower still: `Volatile.Write` lowers
to a store-release, whose ordering against the following acquire load is stronger
than x86-64's plain `mov`/`mov`. The fix therefore rests on the documented memory
model, which is precisely the standard the finding invoked, and the four existing
mutation rows below are what keep the surrounding mechanism graded.

---

## 71.6 · LOW · `RunSubmitterAsync`'s DoD §10 rationale described code that is not there — RESOLVED

**Was:** `:415-418` claimed the per-submission body was kept *"inline (rather than
in its own `async` helper)"* so a burst of N queued sends *"allocates one state
machine between them"*. It **is** its own helper — `AppendQueuedAsync`, awaited
once per submission — and it suspends in the saturated regime the commit's own
measurement used, so N submissions allocate ~2N boxes, not one between them.

**Accepted; verified against the pre-fix tree rather than taken on the finding's
word.** `baa9f0bb`'s `NativeProducer.SendWhenSpaceAvailable` was a per-send
`async Task<RecordMetadata>` whose `Task` was what the caller received, and it
ended in `return await completion.Task` — the second `Task` plus a continuation
chained onto the awaiter. Both are gone; the caller now gets `completion.Task`
itself on both routes (`NativeProducer.cs:568`).

**Fixed** at `:414-431` — the rationale now states the real mechanism and is
explicit about what did **not** get cheaper. Per *saturated* submission the cost
is still one `AppendQueuedAsync` state machine **and** the `WaitForSpaceAsync` one
inside it (both box, because that path suspends), plus one value-type queue entry
amortised over a `ConcurrentQueue<T>` segment; only the **loop's own** state
machine is amortised across the burst. The saving is on the caller-facing side —
the removed per-send carrier `Task` and its `await completion.Task` awaiter.

**The measured number is deliberately not restated.** The finding's instruction
was to correct the mechanism and not to repeat 952.0 → 314.6 B/send unless it
could be attributed correctly; `WaitForSpaceAsync` and its linked
`CancellationTokenSource` are **still per submission** (verified — unchanged
between `baa9f0bb` and `HEAD`), so a full attribution is not available from this
round's evidence and the comment claims none. The numbers live only in the S1
commit message; no code comment or public doc carries them.

---

## 71.7 · LOW · `SettleQueuedSubmissions`'s "still settles" reason held only on the `Stop` path — RESOLVED

**Was:** `:534-538` justified "anything enqueued after this still settles" with
*"the gate is already cancelled"*. True of `Stop` (which cancels `_spaceGate`
first, `:900`); **false** of the caller this slice newly added,
`AbandonOnThreadFailure`, which deliberately does not cancel the gate — that is
F5, correctly deferred to S5.

**Accepted.** No behavioural defect (the finding says so, and I did not find one
either: `RunLoopCore` releases the taken chain's permits before `SendChain`, and
`AbandonOnThreadFailure` releases the rest immediately after the sweep, so a late
submission does reach `Append` and does fault under `_closed`). It is worth fixing
because this comment is the record a reader consults when asking whether S5 is
still needed, and as written it asserted a precondition the path it was added for
does not have — a reader could conclude S5 is unnecessary, the opposite of true.

**Fixed** at `:557-573` — the clause is now split per caller: from `Stop`, the
cancelled gate faults the wait immediately (with `_closed` as the backstop); from
`AbandonOnThreadFailure`, the gate is **not** cancelled, so the `_closed` refusal
is the whole guarantee and settlement waits on a permit becoming free, which on
that path it does. The note ends by naming what would make it independent of
permit availability — F5/S5's unconditional `_spaceGate.Cancel()`, deliberately
its own slice. **`_spaceGate.Cancel()` was not added** (still exactly one
occurrence, in `Stop`).

---

## One rulebook addition, inside the one file the round permitted

`ffi-marshalling.md` §A1's anti-patterns already forbade *more than one* appender
draining the submission queue; they did not cover the **dual**, which is what
71.5 actually was — *zero* appenders. Added one bullet stating that "at most one
appender" is a start/stop handshake, i.e. Dekker's pattern, so **both** sides need
a store→load fence (an `Interlocked` op, not a `Volatile.Write`), and that a
one-sided fence strands a submission and hangs a queued-count drain out to its
bound. It closes with the same standard the §A1 `SemaphoreSlim` warning states, so
the two read as one rule rather than two.

Nothing else in that file changed, and no other rule file was touched.

---

## Verification (round-3 fix)

`cargo build --features ffi` clean ·
`dotnet build` **0 Error(s)** on every TFM ·
`dotnet test -f net10.0` **885 passed / 0 failed / 885 total** ·
`dotnet test -f net8.0` **885 passed / 0 failed / 885 total** ·
`Test Run Aborted` grepped with a control positive (`Passed!`) in the same
command: **0 matches on both**; the only occurrence anywhere is the **net462**
leg, which fails to launch for want of `mono` on this macOS host — reproduced at
**pristine `01cf014c`** with the fix reverted, so it is a host limitation and not
this change (net462 **compiles**; net8.0 and net10.0 each run the 28 TFM-smoke
tests green) ·
`dotnet format --verify-no-changes` clean ·
`git diff --stat baa9f0bb..HEAD -- src/ src/ffi/ target/include/confluent_kafka.h cbindgen.toml`
**EMPTY**, with the unfiltered range non-empty as the control positive — Mode A
held, in the commit range and in the working tree.

**S1's mutation battery re-run to confirm the fence did not weaken it** — each
mutation applied to the fixed source, built with `0 Error(s)` **asserted before
the test run** (the stale-binary trap bit once during this round: a
csproj-scoped build reported `0 Error(s)` and the first `--no-build` runs still
executed the previous binary, so every row below was re-taken with a full
solution build):

| Mutation | Test | Result |
|---|---|---|
| routing predicate → unconditional | `SendAccumulator_SubmissionOrder_IsCallOrder…` | **FAIL 3/3** |
| FIFO submitter → per-send `WaitForSpaceAsync` continuations | both ordering tests | **FAIL 3/3** (2 failed / 0 passed per run) |
| idle predicate → chain-only | `Flush_IncludesASendStillQueuedForSpace` | **FAIL 2/2** |
| `SignalIdleLocked` → single-stage, predicate left two-stage | same test | **FAIL 2/2** |

Baseline control positive: `SendAccumulatorTests` alone is **33 passed** on the
pristine fixed source.

---

# Round 4 — slice S2, commit `3d6b2677` · RESOLVED

Three findings — **71.8 (MEDIUM), 71.9 (MEDIUM), 71.10 (LOW)**. The Critic found
**no code defect**: it probed permit arithmetic across all six acquire/release
paths, the seal's monotonicity, the single-appender invariant, the pool-blocking
flush wait, and memory safety including the new path to `PinnedTopicCache.Rent`
after `Topics.Dispose()`, and confirmed every one correct. All three findings were
a **missing test** and **two wrong comments**. None contradicted an approved
decision D1–D5 — 71.9 is the opposite: the comment contradicted **D5**, which is
why it had to change.

**Fix commit:** `fixup! feat(M11/P3.2 S2): close completes a queued send instead of faulting it`

---

## 71.8 · MEDIUM · call order **through teardown** was claimed by S2's own rationale and asserted by no test — RESOLVED

**Was:** `SendAccumulator.Stop`'s step-2 doc item (and the S2 commit message)
stated that having the *submitter* do the bypass appends rather than the teardown
thread "keeps S1's single-appender invariant — and therefore its call-order
property — true through teardown". The claim is **true**; nothing asserted it. The
Critic mutated the submitter to dequeue **LIFO once `_queueSealed` is set** and the
suite stayed at **887 passed / 0 failed across 3 runs** — every record still
reached the core exactly once, every awaiter still succeeded, every delivery
callback still fired once, a stalled submitter still appended nothing; **only the
order was wrong**. F1 is the phase's HIGH, merge-blocking property, and it was
unguarded on the exact append path S2 introduces (the bypass under a sealed
queue). `STATUS.md:20`'s failure class: a property claimed in a comment, satisfied
by the code, invisible to the suite.

**Accepted in full, and the finding's own diagnosis of why the existing tests miss
it is the load-bearing part.** Part (i) of
`SendAccumulator_SubmissionOrder_IsCallOrder_AcrossTheBackpressureBound` asserts
the two flushed records *arrive* and derives their order from "nothing was
appended while queued" — which is the **mechanism**, not the order. Part (ii) (the
200-record stress half) never reaches teardown. And the three S2 tests assert
arrival, settlement and exactly-once counts, all of which a reorder preserves.

**Fixed** by adding `Close_FlushesQueuedSubmissionsToSendBatchInCallOrder`
(`SendAccumulatorTests.cs`), plus a `⚠` pointer from the claim at `Stop`'s step-2
item to the test that now carries it — so the claim and its guard cannot drift
apart.

Written independently of the Critic's sketch rather than copied, but the shape is
the same and deliberately so:

  - **Five** queued sends, not three, so a LIFO reversal is unambiguous whatever a
    mutation's buffering granularity turns out to be.
  - The **frozen bound** (`ConsumePermits(4)` with nothing accumulated), the same
    lever `Close_CompletesASendThatWasQueuedForSpace_RatherThanFaultingIt` uses, so
    the order under test is fixed by the enqueue and **not** raced against a permit
    release. This is what makes the new test deterministic where the S1 stress half
    is a race — see the note under the mutation table below, which turns out to
    matter.
  - The witness is the per-record **`OrderRecordingDeliveryCallback`** firing
    order, the phase's own cross-node witness, valid for the reason its stress-half
    comment already records (`send_batch` reads a node's slots in index order,
    `CompleteNode` enqueues them to the pump in that order, `Enqueue`/`DrainAll`
    are FIFO, `ProcessBatch` fires each batch in index order ⇒ callback order ==
    append order == the order records reached `send_batch`). It is also the **only**
    witness available on this path: the node the flush fills is taken, sent and
    recycled *inside* `Stop`, so no `PendingCompletions()` array survives it, and
    the mock core exposes a record **count**, not a history.
  - Arrival is asserted **before** the order loop (`HistoryCount`,
    `SendBatchRecordCount`, `QueuedSubmissionCount == 0`), because a short
    `observed` list would otherwise satisfy the loop vacuously — a zero-match
    assertion is not evidence.

**Observed mutation result — the mandate, met.** *Mutation Y*: a
`TryDequeueMutated` helper substituted at `RunSubmitterAsync`'s dequeue, draining
`_submissions` into a buffer and returning **LIFO once `_queueSealed` is set**.
Solution rebuilt, `0 Error(s) / 0 Warning(s)` asserted, DLL mtime cross-checked
against the clock (the stale-binary trap):

| Source | New test | Full net10.0 suite |
|---|---|---|
| shipped + fix | **Passed 5/5** consecutive runs | **888 passed / 0 failed / 888** |
| + Mutation Y | **FAILED** — `Assert.Equal() Failure: Expected: 1, Actual: 4` | **887 passed / 1 failed / 888** — the **only** failure is the new test |

The full-suite row is the finding reproduced exactly: the pre-existing 887 are all
green under LIFO-once-sealed, and the new test is the sole guard. The observed
order was `[0, 4, 3, 2, 1]` — the submitter had already dequeued submission 0 and
parked it on the gate before the seal, so it kept its FIFO position and the
remaining four came back reversed. (The Critic's 3-record probe saw the same shape,
`[0, 2, 1]` → `Expected: 1, Actual: 2`.) Mutation reverted, residue grepped to
**0** with control positives, and the file `diff`ed byte-identical against a
pre-mutation copy.

---

## 71.9 · MEDIUM · the comment at the `_spaceGate.Cancel()` site stated a hazard that cannot occur, and prohibited approved decision **D5** — RESOLVED

**Was:** `SendAccumulator.cs:1145-1147` read *"AbandonOnThreadFailure deliberately
does not cancel (M11/P3.2 §F5 is its own slice) — and **it must not start**, or a
submission it releases would take the bypass into an accumulator whose thread is
already gone."* Phrased as a correctness prohibition, with a causal justification,
against a decision **the user has already approved (D5)** and scheduled as **S5**.
Left in place it would have misdirected the S5 Actor into either abandoning D5 or
inventing machinery to guard a hazard that does not exist.

**Accepted, and independently re-derived rather than taken on the Critic's word**
(this was the round's one genuine stop-or-proceed question — a real hazard would
have been a conflict with D5 and the user's call):

  - `_queueSealed` is written in **exactly one place** — `Stop`'s step 1, confirmed
    by grep: of seven references, one is the field declaration, one is a
    `<see cref>`, three are reads (`SubmitQueued:424` under `_gate`,
    `AppendQueuedAsync:572`, the `:585` filter) and **one** is the assignment at
    `:1135`, inside `Stop`. `AbandonOnThreadFailure` never sets it.
  - So a submission released by a cancel from `AbandonOnThreadFailure` reaches
    `catch (ObjectDisposedException) when (Volatile.Read(ref _queueSealed))` with
    the filter **false** — `WaitForSpaceAsync` maps the gate's
    `OperationCanceledException` to `ClosedDuringBackpressure()`, an
    `ObjectDisposedException` (`:280-283`, `:294-299`) — the throw propagates to the
    outer `catch (Exception)` and the submission is **faulted**, not bypassed. That
    is exactly what §F5/S5 wants.
  - And where a concurrent `Stop` **has** sealed, the record still cannot be
    stranded: `AbandonOnThreadFailure` sets `_closed` and calls `TakeChainLocked` in
    **one** `_gate` acquisition (`:1261-1269`), so the bypass `Append` is either
    refused by `_closed` or lands in a node that same acquisition takes, and
    `SettleAbandonedChain` settles it (firing the delivery callback via
    `FaultNode(settled: 0)`).

So the stated consequence is false, not merely imprecise, and the finding stands.

**Fixed** by rewriting the comment to state the actual reason — **sequencing, not
safety** — naming D5 as *approving* the addition, giving both legs of the argument
above, and closing with an explicit note that the comment previously stated the
hazard as real (quoting the old clause, so a future reader can tell a correction
from a rewording). `_spaceGate.Cancel()` **remains a single call site** — verified
after the fix: three grep hits in the file, one doc-comment mention (`:685`), one
code comment (`:1157`), one call (`:1175`, inside `Stop`), and `0` `Cancel(` hits
inside `AbandonOnThreadFailure`'s body with the method's own declaration as the
control positive. **S5's one-liner was NOT applied** — this was a comment
correction only, per the round's explicit instruction, so S5 stays attributable.

---

## 71.10 · LOW · the Python-parity rationale had `cnd_timedwait` **holding** the mutex; it releases it — RESOLVED

**Was:** `AppendQueuedAsync`'s remarks (`:552-555`) said *"The common path is safe
because the thread spends its time in `cnd_timedwait` (`:548`) **holding** that
mutex."* `cnd_timedwait` atomically **releases** `record_batches_mutex` while the
thread is parked and reacquires it on wake — which is precisely how
`py_Producer_shutdown` takes the lock at `_confluentkafka.c:961` to set `closed` at
`:962`. As written the sentence was self-defeating: if the thread held the mutex
while parked, `shutdown` could never have set `closed` at all, and the conclusion
the paragraph draws from it would be unreachable.

**Accepted; the error is upstream of the code.** It came from **PLAN §1.3** and
from the S2 spawn brief, and was copied faithfully into the source. §1.3 was
already corrected in `b1c88449`; this round mirrors that corrected wording into the
source comment. The **conclusion is unchanged** — the thread holds the mutex
whenever it is *not* parked, so shutdown can only win the lock while it is parked,
which is the case the final take-and-send covers — and so is the narrow race the
paragraph exists to record (a record appended between `:577`/`:638` and the `:529`
re-test is never sent and never completed).

**Fixed** by replacing the mechanism sentence: the thread spends its time *inside*
the `record_batches_mutex` critical section, releasing the lock **only** while
parked in `cnd_timedwait` (`:548`), which atomically releases it for the duration
of the wait and reacquires it on wake — and that release is how
`py_Producer_shutdown` takes the lock at `:961` to set `closed` at `:962`; so the
park is the only window in which shutdown can win the lock, and the thread then
wakes holding it again, falls out of the inner wait (`:539`'s `!closed` guard) and
performs one final take-and-send. A parenthetical records that the paragraph first
had the mechanism inverted, and points at both the finding and the PLAN §1.3
correction. The S2 **commit message** carries the same wrong wording; it is
history, so it is left alone — the correction is stated in the fixup's own body.

---

## Verification (round-4 fix)

| Gate | Result |
|---|---|
| `cargo build --features ffi` | clean |
| `dotnet build Confluent.Kafka.sln` (**solution**, not csproj) | **0 Warning(s) / 0 Error(s)**, all TFMs |
| `dotnet test -f net10.0` | **888 passed / 0 failed / 888 total** (887 + the new test) |
| `dotnet test -f net8.0` | **888 passed / 0 failed / 888 total** |
| `Test Run Aborted` grep, both TFMs | **0 lines**, with `Passed!` = 1 as the control positive in the same output |
| `dotnet format --verify-no-changes` | clean (exit 0) |
| net462 | aborts — `Could not find 'mono' host`, in vstest host resolution before any assembly loads. **Pre-existing**, reproduced on commits predating S1; identified, not reported as new |
| `git diff --stat c0c19da0..HEAD -- src/ src/ffi/ target/include/confluent_kafka.h cbindgen.toml` | **EMPTY** — committed range *and* working tree, each with the unfiltered diff as the control positive. Mode A held |
| `_spaceGate.Cancel()` call sites | **1**, in `Stop`; `0` inside `AbandonOnThreadFailure` |
| both allocation-budget files | untouched; `PerSendBudgetBytes` still **512** |

**S1's ordering mutations re-run, to confirm the S2 changes and the new test did
not weaken them.** Each applied to the fixed source, **solution** rebuilt with
`0 Error(s)` asserted, then reverted and residue-grepped:

| Mutation | Test | Result |
|---|---|---|
| routing predicate → unconditional | `SendAccumulator_SubmissionOrder_IsCallOrder…` | **FAIL 10/10 isolated** (`Expected: 4, Actual: 85` at the stress half's order loop) |
| FIFO submitter → per-send `WaitForSpaceAsync` continuations | `SendAccumulator_SubmissionOrder_IsCallOrder…` | **FAIL 5/5** (`Expected: 4, Actual: 7`) |
| " | `SendAccumulator_TwoConsecutivelyParkedSends…` | **FAIL 5/5** (`Assert.Same()` — slots out of call order) |
| " | the **new** `Close_FlushesQueuedSubmissions…InCallOrder` | **FAIL 3/3** — it catches this one too, since per-send continuations bypass-append in arbitrary order under a sealed queue |

⚠ **One observation worth recording, because it is a property of the S1 test
rather than a regression.** The routing mutation fails **10/10 when the ordering
tests run isolated**, but only **2 of 4 times in the full suite** (the first full
run passed). That is the fragility the S1 commit message already flagged in its own
mutation row — *"It first passed — a 200-send loop finishes before the batch
thread's first window, so the release-then-inline step was never reached"* — i.e.
the stress half detects the mutation through a **race** (a permit released
mid-burst and taken by a later send), and full-suite thread-pool contention shifts
the drainer task's scheduling out of the window. It is **not** a weakening
introduced by S2 or by this round: the detector bites, reproducibly, when the test
is not competing with 880 others, and the deterministic half (part (i)) is
insensitive to this mutation by construction — with the bound frozen,
`TryAppendOne`'s refusal is over-determined (`TryAcquireSpace` fails anyway), so
only the stress half carries it. Two consequences for future rounds: **re-run this
mutation isolated**, and note that the new teardown test is **deterministic** where
this one is a race (frozen bound, order fixed by the enqueue) — 5/5 clean and 1/1
under Mutation Y, in the full suite.

**Not done, deliberately** (each named in the round's constraints): S3 (grouping),
S4 (pre-stop pump drain) and S5 (`_spaceGate.Cancel()` in
`AbandonOnThreadFailure`) are absent; `DrainAll`/`ProcessBatch`/`Enqueue`/
`CloseGate`/`_stopLock` semantics and the pump's queue type are untouched; the sync
`Send`/`Flush`/`Close` path, every public API, `STATUS.md`, every `CLAUDE.md`,
everything under `.claude/rules/` and `IDeliveryCallback.cs` are untouched (the
Critic confirmed S2 adds no residual, so the residual axes needed no edit); and the
pending `bindings/dotnet/CLAUDE.md` supersession-sweep rule was **not** written —
still an open question with the user, no decision.

---

# Round 5 — the `42cd412b` verification round · RESOLVED

Two items, deliberately **two separate fixup commits** because they belong to
different slices and attribution matters:

| item | slice | fixup commit | target |
|---|---|---|---|
| **71.11** (LOW) | S2 — a comment correction | `d32f805c` | `3d6b2677` |
| **FU-1** (follow-up) | S1 — a test-guard repair | `4b7cd84c` | `01cf014c` |

Both fixup subjects were built **programmatically** from
`git log -1 --format=%s <target>` and verified byte-identical to
`fixup! <target subject>`.

S3 (grouping), S4 (pre-stop drain) and S5 (`_spaceGate.Cancel()` in
`AbandonOnThreadFailure`) were **not** implemented — out of scope for this round.

---

## 71.11 · LOW · the 71.10 replacement text swapped one false mechanism claim for another — RESOLVED

**The finding is correct, and I re-derived it from `_confluentkafka.c` rather than
from the plan** (mirroring the plan is exactly how the error reached the comment
twice). Verified line by line:

| line | operation | mutex |
|---|---|---|
| `:533` | `mtx_lock` | `record_batches_mutex` |
| `:548` | `cnd_timedwait` (atomically releases + reacquires) | `record_batches_mutex` |
| `:556` | `mtx_unlock` — the `test_paused` early-continue | `record_batches_mutex` |
| `:563` | `mtx_unlock` — the "nothing accumulated" early-continue | `record_batches_mutex` |
| `:577` | `mtx_unlock` — after taking the chain | `record_batches_mutex` |
| `:629` / `:638` | `mtx_lock` / `mtx_unlock` | **`pending_batches_mutex`** — a different mutex |

So the thread runs the whole send loop `:581-638` — every
`kafka_producer_Producer_send_batch` call (`:593`) plus its GIL acquisition
(`:603`) — holding **no** `record_batches_mutex`, and `py_Producer_shutdown` can
take it at `:961` right there. The claim *"the park is the only window in which
shutdown can win the lock"* was false, and it contradicted the very next sentence.

I also confirmed the consequence the finding rests on, which the finding asserts
but does not show: there is **no post-join drain**. `next_batches_to_send` is
never read after `:569` (`grep -n next_batches_to_send` → `:377`, `:537`, `:538`,
`:562`, `:567`, `:569`, `:679`, `:763`, `:814` — the last three are
initialisation and the append). So records appended in that window really are
never taken, never sent and never completed.

**What changed** (`SendAccumulator.cs`, `AppendQueuedAsync`'s remarks; `grep` over
the whole binding for the false **exclusivity clause** found exactly one code hit
plus `PLAN.md:116`, which is the Manager's already-corrected record quoting it *as*
false). The text now mirrors the corrected PLAN §1.3 (`8c9aa077`):

⚠ **CORRECTED by 71.12 — this paragraph originally read "the only copy in code",
and that quantifier was FALSE.** The grep behind it was keyed on the exclusivity
clause, which genuinely appears once; but this same pass was **also** fixing a
second, narrower defect (the mispaired `:638` cite and the "narrow gap" / "one
narrow race" framing — see the two bullets below and the adjacent-inaccuracies
list), and *that* defect had **two** code copies. The unswept one was
`SendAccumulatorTests.cs:1184-1188`, added by `3d6b2677` (S2 itself). So the
exhaustiveness claim over-stated what was verified and is what closed the sweep
one file early. The scope-accurate statement is the one now above: *the
exclusivity clause* had one code hit. See 71.12 for the fix and the per-defect
re-sweep.

  - the justification is stated as **TIMING** — the thread spends almost all of
    its time parked, so that is where shutdown almost always wins the lock — and
    the exclusivity claim is **deleted, not re-scoped**;
  - the unlocked window is named as the **whole send loop** `:581-638`, and for a
    full `PRODUCER_RECORD_SLOT_CAPACITY` batch it lasts as long as a `send_batch`
    call, so Python's gap is materially **wider** than "one narrow race";
  - which **strengthens** S2: .NET completes such a record on every path, Python
    only when close lands while its thread is parked.

The *set* of records at risk is unchanged, so this was a wrong reason for a right
conclusion — a comment-accuracy defect, not a behaviour change. No code changed.

**Two adjacent inaccuracies the Critic identified but deliberately did not file,
fixed in the same pass** since this text was being edited anyway:

  - the unlock cites were **mispaired**: `:638` unlocks `pending_batches_mutex`,
    not `record_batches_mutex`. The `record_batches_mutex` unlocks are `:556`,
    `:563`, `:577`, and the comment now says so (and records the mispairing in its
    own correction note, so a reader who has seen the old text can tell what moved).
  - `Stop`'s step-2 comment said `_queueSealed` is written *"(below, under
    `_gate`)"* when the write is ~15 lines **above** it → now *"(in step 1 above,
    under `_gate`)"*.

`COMMENTS.DONE.71.md:645-646` / `:651-657` still contain the withdrawn clause;
per the finding's own instruction that file is a resolution **record** and was
left alone, as the S2 commit message was.

---

## FU-1 · S1's ordering guard was invisible in the DoD gate — RESOLVED (test-only, S1 file)

**The problem, confirmed and measured here.** With S1's routing predicate removed
from `TrySubmitInline`, the stress half of
`SendAccumulator_SubmissionOrder_IsCallOrder_AcrossTheBackpressureBound` failed
reliably in isolation but the **full net10.0 suite could pass** — i.e. the gate
was green with the phase's HIGH, merge-blocking fix reverted, the `STATUS.md:20`
failure class this phase exists to eliminate.

**Mechanism, as the Critic diagnosed it — agreed, not re-derived differently.**
The stress half detects the mutation through a **race** (a permit freed mid-burst
and taken by a later inline send while an earlier submission is parked), and
full-suite thread-pool contention shifts the drainer `Task` out of that window.
Part (i) is insensitive to the routing mutation **by construction** — with the
bound frozen, `TryAppendOne`'s refusal is over-determined because `TryAcquireSpace`
fails anyway — and slice S2's new frozen-bound close test does not compensate: a
frozen bound over-determines the predicate there too (the Critic measured that test
PASS 3/3 under this mutation). So the stress half is the predicate's **only**
detector, and it has to bite under contention.

**The fix — the Critic's verified recipe, implemented as specified and not
substituted.** The burst now runs **K=8 times with a fresh `Harness` per attempt**.
Fresh per attempt because a reused harness carries the previous attempt's node
chain, spare node and permit state, so attempts 2..K would no longer start from the
saturating-burst-from-cold shape the bug needs. The drainer's thread type was **not**
changed — the Critic measured `Task.Run` → dedicated `Thread` at only 2/4 in-suite,
so repetition, not the thread type, is the lever.

**Measured in the gate's own context, as directed** — the solution (not the csproj)
rebuilt with `0 Warning(s) / 0 Error(s)` and the DLL mtimes cross-checked before
each `--no-build` run:

| configuration | scope | result |
|---|---|---|
| routing predicate removed | isolated, **one** burst | **FAIL 4/4** (`Expected: 4/5/7`, `Actual: 78/82/134`) |
| routing predicate removed | **full net10.0 suite**, one burst | **FAIL 3/4** ← the gap |
| routing predicate removed | **full net10.0 suite**, K=8 | **FAIL 6/6**, and the failing test is the *only* failure (`887 passed / 1 failed / 888`) |
| HEAD (predicate restored) | **full net10.0 suite**, K=8 | **PASS 4/4** (`888 passed / 0 failed`) |
| HEAD | isolated, K=8 | **PASS 5/5** (`Total: 1` asserted each run) |

⚠ **One honest divergence from the finding's numbers, recorded rather than
smoothed over.** The Critic measured the single-burst in-suite detection at
**0/5** and the earlier Actor at **2/4**; here it is **3/4**. The gap is therefore
real but *weaker* on this machine, which makes the "one burst is enough" reading
even more dangerous (it would look fine most rounds and miss occasionally) and
does not change the remedy. The K=8 result — 6/6, with a per-attempt rate around
0.75 — is what the repetition is buying.

**Duration, before → after:** the test's own duration went **13 ms → 24 ms**
(measured isolated, `Attempts` temporarily pinned to 1 for the "before"). The full
**net10.0 suite is unchanged at 9–10 s** (baseline 9 s); net8.0 is `888/0` at 10 s.

**Nothing weakened.** Part (i) is kept intact — it guards a different,
deterministic aspect (a queued submission means nothing is appended and nothing is
pinned, the M11/P3.1 §4.4 pin-after-permit witness) — and the stress half's
assertions are byte-identical, only wrapped in the loop. No production change.

⚠ **Stated at the site and here, because it is the part most likely to be
re-litigated:** this is a **probability argument, not a proof of determinism**. A
deterministic guard for this predicate is not reachable without a **white-box
production seam**, because the state it governs — "a permit is free **and** a
submission is queued" — is transient by construction: the parked submitter consumes
the released permit promptly. Per the round's instruction, **no production seam was
added**, and none is proposed here; it is a design call for the Manager. The
site comment therefore also keeps the standing instruction that the **isolated**
re-run of the routing mutation stays the primary evidence in any round that touches
the routing predicate.

---

## Verification (round-5 fix)

| Gate / check | Result |
|---|---|
| `cargo build --features ffi` | clean (`Finished dev profile`) |
| `dotnet build Confluent.Kafka.sln` (**solution**, per the stale-binary trap) | **0 Warning(s) / 0 Error(s)**; net10.0 lib DLL 23:23:11 and test DLL 23:21:24 both newer than their sources (23:22:52 / 23:21:13) |
| `dotnet test -f net10.0` | **888 passed / 0 failed / 888**, ×4 consecutive runs, 9–10 s |
| `dotnet test -f net8.0` | **888 passed / 0 failed / 888**, ×2 runs, 10 s |
| `Test Run Aborted` grep, both TFMs | **0**, with `^Passed!` = **1** in the same output as the control positive |
| `dotnet format --verify-no-changes` | clean (exit 0) |
| net462 | aborts — `System.IO.FileNotFoundException: Could not find 'mono' host`, from `DotnetHostHelper.GetMonoPath()` in vstest host resolution **before any assembly loads**; the net462 test assembly still *builds* in the solution build. Pre-existing, **identified, not reported as new** |
| Mode A: `git diff --stat c0c19da0..HEAD -- src/ src/ffi/ target/include/confluent_kafka.h cbindgen.toml` | **EMPTY**, committed range **and** working tree, each with the unfiltered diff as the control positive (`PLAN.md` + the two binding files) |
| mutation residue | `MUTATION-ONLY` / `MEASUREMENT-ONLY` markers = **0** (control positive: `Volatile.Read(ref _queued) != 0` = **2**, the original count — `:353` routing + `:1248` drain predicate) |
| `_spaceGate.Cancel()` | **1** call site (`Stop`); control positive `Cancel(` = 3 in the file (two prose, one call). **S5 did not land** |
| `PerSendBudgetBytes` | **512**, both budget files — neither is in the diff |
| S3 / S4 absent | `PendingSendBatch` = **0** (control positive `PendingSend` = 112); no pre-stop drain; `SendCompletionPump.cs` untouched |
| boundary table (plan §8.3) | `DrainAll`/`ProcessBatch`/`Enqueue`/`CloseGate`/`_stopLock` semantics and the pump's queue type untouched |
| untouched axes | the sync `Send`/`Flush`/`Close` path, every public API, `STATUS.md`, every `CLAUDE.md`, everything under `.claude/rules/`, `IDeliveryCallback.cs` — the two commits touch exactly **one file each** (`SendAccumulator.cs`; `SendAccumulatorTests.cs`), which settles every axis at once |
| pending `bindings/dotnet/CLAUDE.md` supersession-sweep rule | **not written** — still an open question with the user, no decision |
| `.claude/agent-memory/project-manager/MEMORY.md` | left **unstaged** (`git status` shows ` M`, not `M `) |

---

## 71.12 · LOW · slice S2 · the `d32f805c` correction had a THIRD copy, in S2's own test — RESOLVED

**The finding is correct on both defects, and I did not conclude otherwise.** Both
errors `d32f805c` fixed in `SendAccumulator.cs` were alive at
`SendAccumulatorTests.cs:1184-1188` (added by `3d6b2677`, the S2 commit itself):

  - **(a) the mispaired `:638` cite.** The sentence's subject is
    `record_batches_mutex` and "**its** `mtx_unlock` (`:577`/`:638`)" attributes
    `:638` to it. `:638` unlocks **`pending_batches_mutex`** — a different mutex.
    The `record_batches_mutex` unlocks are `:556`, `:563`, `:577`.
  - **(b) the superseded "narrow gap" / "one narrow race" framing.** The phase has
    now reversed this twice — `SendAccumulator.cs:561-565` names the unlocked
    window as the whole send loop `:581-638`, lasting as long as a `send_batch`
    call, hence "materially wider than one narrow race"; `PLAN.md:99-105` says the
    same. The S2 regression test's own rationale header — the first place a reader
    of S2 looks — still carried the pre-correction version.

### What changed — one comment block, and it POINTS rather than restating

Per the Critic's own recommendation (and `ffi-marshalling.md` §A6's round-5
discipline: one canonical statement, every other site points at it), the five
lines were **not** repaired into a fourth statement of the argument. They now
state only the local, test-relevant fact and point at the single source:

  - Python's guarantee is almost-unconditional, **.NET's is unconditional**, which
    is what the three assertions below the header pin;
  - the mechanism — which mutex, which unlock sites, how wide the window is — is
    derived **once**, in `SendAccumulator.AppendQueuedAsync`'s remarks (M11/P3.2
    PLAN §1.3), and is deliberately not restated;
  - the block records *why* it is a pointer: it was a third copy that outlived two
    corrections of the canonical one (71.10 / 71.11, and 71.12 for itself).

No `:638`, no "narrow", no uniqueness quantifier about the *other* sites. The
`:529` cite and the `record_batches_mutex` name — the paragraph's structural
fingerprint — now appear in exactly **one** code file (`SendAccumulator.cs`),
which is the grep-verifiable form of "one statement is the only statement".

**Comment-only.** One test file, zero production lines, zero test-logic lines, no
new test. The suite count is unchanged at **888** — the check that a comment-only
change stayed comment-only.

### Why the original sweep's pattern missed it — the part to internalise

`COMMENTS.DONE.71.md`'s "What changed" paragraph claimed *"the only copy in code"*.
The grep behind it was keyed on the **exclusivity clause** ("the park is the only
window"), which genuinely appears once. But that same pass was fixing a **second,
narrower** defect — (a) and (b) above — and that one had **two** code copies. One
pattern was run for a pass that fixed two defects, so the quantifier was scoped to
the pattern rather than to the pass. That paragraph is now **corrected in place**
(a ⚠ note immediately under it), so the resolved-findings record no longer asserts
a sweep was complete when it was not.

**Standing rule taken from this**, since it is the third instance in this phase of
a claim propagating to a site a keyword sweep missed (F8's own existence, 71.1's
Summary, now this): when a fix addresses N defects, the sweep needs **N patterns,
one per defect**, each with a control positive — plus at least one **structural**
pattern (a cite or an identifier the paragraph cannot be written without), because
a keyword pattern only finds copies that kept the keyword.

### The re-sweep — one pattern per defect, control positive in every command

Every negative claim below was run in the same command as a positive control, so a
zero-match cannot masquerade as clean.

| # | pattern | scope | result | control positive |
|---|---|---|---|---|
| P1 | `one narrow race\|narrow gap\|narrow race` | repo, `*.cs *.md *.rs *.c *.py *.h` | `.cs`: **`SendAccumulator.cs:565`** (correct — *"materially wider than one narrow race"*) + `NativeMethods.cs:46` (**unrelated** — M9/P4's `ObjectDisposedException`/UAF race). `.md`: `PLAN.md:231` (Manager's, correct), `COMMENTS.DONE.71.md:647`/`:778` (historical record), `M9/P4/PLAN.md:433` (unrelated), critic agent-memory (local scratch). **Test file: gone** | bare `narrow` = **246** |
| P2 | `:577\s*/\s*:?638` / `:577/:638` | `*.cs` | **ZERO** | `:577` in `*.cs` = **2** (both `SendAccumulator.cs`, correctly paired) |
| P3 | `:638` (any spelling) | `*.cs` | **1** — `SendAccumulator.cs:575`, the *correction record* stating `:638` **is** a `pending_batches_mutex` unlock. Correct by construction | `:962` in `*.cs` = **3** |
| P4 | `only window\|only opportunity\|whenever it is not parked\|only chance` (the exclusivity clause) | `*.cs *.md` | live code: **only** `SendAccumulator.cs:573`, which quotes it *as false* in its correction note. Others are `COMMENTS.DONE.*` records + `PLAN.md:116` (Manager's, quoting as false) + `COMMENTS.DONE.57.md:1639` (unrelated) | `only window` in `SendAccumulator.cs` = **1** |
| P5 | `record_batches_mutex` — **structural**, wording-independent | `*.cs` | **5 hits, all in `SendAccumulator.cs`** (`:97`, `:550`, `:556`, `:561`, `:576`). One code file = single source | `pending_batches_mutex` in `*.cs` = **1** |
| P6 | `:529` (the `!closed` re-test anchor) — **structural** | `*.cs` | **1** — `SendAccumulator.cs:550`. One code file | `:533` in `*.cs` = **2** |

P5 and P6 are the ones that answer the STOP condition, because they cannot be
evaded by a reworded copy: the paragraph is unwritable without naming the mutex or
the `:529` re-test.

### Is there a FOURTH site? — NO

P5/P6 place the paragraph in exactly **two** code files before the fix
(`SendAccumulator.cs` canonical + `SendAccumulatorTests.cs` the survivor) and
**one** after. The propagation is exactly the three the Critic named:
`SendAccumulator.cs` (fixed, `d32f805c`), `PLAN.md` (Manager's, fixed,
`8c9aa077`/`d7a9d6dc`), `SendAccumulatorTests.cs` (fixed here). Everything else
carrying a matching keyword was checked individually and is **not** a copy:

  - `NativeMethods.cs:46` + `M9/P4/PLAN.md:433` — M9/P4's `ObjectDisposedException`
    vs use-after-free race. Same word, different subject.
  - `COMMENTS.DONE.57.md:1639` — an unrelated `_owner`-null slot window.
  - `STATUS.md` — **no** `record_batches_mutex` hit at all (verified as a
    deliberate zero, `grep -c` = 0 with P5's non-zero as the control); its
    `Producer_send_thread` mention at `:11` is a generic anchor reference.
  - `NativeProducer.cs:397`, `SendAccumulatorSettings.cs:24`,
    `SendAccumulator.cs:29`, `NativeMethods.cs:2255`,
    `ProducerSendBatchMarshal.cs:24` — generic *"the anchor is
    `Producer_send_thread`"* pointers, none carrying the close-race argument.
  - `.claude/agent-memory/dotnet-critic/feedback_stale_claim_and_citation_sweeps.md`
    — the Critic's own local memory *describing this finding*. Local-only scratch,
    not mine to edit, and not a copy of the claim.

### Gates

| gate | result |
|---|---|
| `cargo build --features ffi` | clean (`Finished dev profile`) |
| `dotnet build Confluent.Kafka.sln` (**solution**, per the stale-binary trap) | **0 Warning(s) / 0 Error(s)** — all six assemblies (ns2.0/net8.0/net10.0 lib; net462/net8.0/net10.0 tests) |
| `dotnet test Confluent.Kafka.sln -f net10.0` | **`Failed: 0, Passed: 888, Total: 888`**, 9 s — count **unchanged**, asserted non-zero |
| `Test Run Aborted` grep | **0**, with `Passed!` = **1** in the same output as the control positive |
| `dotnet format --verify-no-changes` | clean (exit **0**) |
| Mode A: `git diff --stat c0c19da0..HEAD -- src/ src/ffi/ target/include/confluent_kafka.h cbindgen.toml` | **EMPTY**; control positive `-- bindings/dotnet/src/` = 354/43 |
| working-tree scope | exactly **one** modified tracked file, `SendAccumulatorTests.cs`. `PLAN.md` **not touched** (Manager's) |
| net462 | not run in this leg (`-f net10.0`); its known benign abort — `Could not find 'mono' host` from `DotnetHostHelper.GetMonoPath()`, before any assembly loads — is **identified, not reported as new**. It still *builds* in the solution build above |
| S3 / S4 / S5 | **not implemented.** `_spaceGate.Cancel()` still a single call site in `Stop` |
| `PerSendBudgetBytes` | **512**, both budget files — neither in the diff |
| untouched axes | the sync path, every public API, `STATUS.md`, every `CLAUDE.md`, everything under `.claude/rules/`, `IDeliveryCallback.cs` |
| pending `bindings/dotnet/CLAUDE.md` supersession-sweep rule | **not written** — still an open question with the user, no decision. This finding is its third supporting data point; evidence is not authorisation |
| `.claude/agent-memory/project-manager/MEMORY.md` | left **unstaged** |
| D1–D5 | nothing here contradicts any approved decision — the change makes no claim of its own, it points at one |

---

## 71.13 · MEDIUM · S4's production call sites were unguarded; test 17's sensitivity was entirely the fixture's — RESOLVED

Filed against `76fdf03c` (slice S4). **Test-only fix**, as the finding scoped it:
S4's production code is unchanged, and nothing here contradicts D1–D5.

**Fix commit:** `fixup! feat(M11/P3.2 S4): a bounded pre-stop drain so teardown completes what the core accepted`

**Was:** the two `pump?.WaitForQueueDrain(s_pumpDrainTimeout)` call sites that
actually ship — `NativeProducer.cs:1220` (`StopPump`) and `:1285`
(`StopPumpAsync`) — had **no** test. The guard offered for them,
`SendCompletionPumpPreStopDrainTests.Teardown_CompletesSendsAlreadyQueuedToThePump_RatherThanFaultingThem`
(§6 test 17), drives `SendAccumulatorTests.Harness.Dispose` (`:2025`) and never
enters `NativeProducer` at all. The mutation offered as proof — M1, which deleted
the production lines **and** the fixture's together — could not separate them, and
separated they behave oppositely.

**Accepted in full, and the Critic's split reproduces exactly as reported.** I
re-ran both halves myself, in the repo tree, solution build, full suite, in-suite,
`-f net10.0`, with the mutated source's mtime checked against **both** the library
DLL and the test bin's copy of it before each battery. Baseline with the new test:
**895 / 0**.

**The finding's central claim, confirmed at HEAD:** under **MPROD** the *entire*
pre-existing suite is green. Test 17 passes, all four
`PublicProducerAccumulatorTeardownTests` §3.8 guards pass, and the only failure is
the test added here. So before this round nothing held production teardown to S4 —
DoD §12 from the inverse side: the fixture mirrored production faithfully, but
nothing pinned production to the mirror.

### The fix — a fifth test in `PublicProducerAccumulatorTeardownTests`

`Dispose_CompletesSendsAlreadyQueuedToThePump_RatherThanFaultingThem`: 48 rounds,
each a fresh `AsyncMockProducer` with 24 unawaited sends left in the accumulator,
then the **public** `Dispose`, asserting every send is `TaskStatus.RanToCompletion`.
Teardown's own `StopAccumulator` is what drains them, so the group reaches the
pump's queue in exactly the F4 window and the run goes through `NativeProducer.StopPump`.
Four decisions, each taken deliberately:

  - **`RanToCompletion` is the witness, not a counter.** `ProcessedBatchCount` —
    test 17's instrument for *"the pump resolved it"* — is **not** surfaced past
    `SendCompletionPump`; `NativeProducer` exposes only `DrainedSendCount`
    (`:1068`), which `DequeueGroup` increments for **both** consumers and so cannot
    separate them. Per the finding's point 4 I surfaced **nothing new** to make the
    test possible: `RanToCompletion` needs no new surface, and S4 is exactly what
    makes it deterministic here.
  - **Why the existing four cannot substitute.** `AssertDrainedIntoTheOpenGate`
    (`:275-294`) accepts *"success **or** a fault whose message contains `closed`"*
    — deliberately wide, so this is a fifth test rather than an edit to the shared
    helper, as the finding recommended.
    ⚠ **Reason CORRECTED by 71.14.** This bullet originally said the tolerance was
    *"load-bearing for the §3.8 ordering guard"*. It is not: the §3.8 guard is
    `Assert.Equal(sends.Length, producer.DrainedSendCount)` (`:344`), which sits
    **above** the tolerance and is wholly independent of it — tightening the
    tolerance would have cost that guard nothing. What the tolerance is actually
    load-bearing for is **DV-4's pathological residual**: `WaitForQueueDrain` is
    bounded at 30 s, and on expiry `Stop` legitimately faults the remainder, so a
    helper demanding `RanToCompletion` would be asserting the absence of a residual
    this phase deliberately keeps. The decision (a separate test, tolerance
    untouched) is unchanged and right; only its stated mechanism was wrong.
  - **A K-burst with a fresh producer per round**, for test 17's reason and FU-1's:
    the defect is a *scheduling* race, so a single round is a coin flip and no
    guard at all. Each round is an independent trial.
  - **Synchronous `Dispose`, not the `DisposeAsync` twin.** ⚠ **SUPERSEDED by
    71.14 — the twin was added.** As written this bullet said the sync path is *"the
    reliable instrument for the pair"* because MPROD deletes both lines. That
    argument only ever held for the **joint** mutation: under **M1285** (delete
    `:1285` alone) the sync test is green and nothing else moved, so `StopPumpAsync`
    was never guarded in isolation. The premise it rested on — *"the async probe
    detects only 2/3"* — was also refuted by measurement (71.14). See 71.14 below
    for the pair that replaced this decision.

### The SPLIT mutation evidence — the point of this round

Each half applied **alone**, in the repo tree (production restored with
`git checkout --` between batteries and the restore verified by count before the
next), solution build `0 Warning(s) / 0 Error(s)`, full suite, ≥3 runs.
`Test Run Aborted` grepped **0** on every run, with `Passed!`/`Failed!` = **1** in
the same output as the control positive.

| mutation | what it deletes | result |
|---|---|---|
| **MPROD** | **only** the two `pump?.WaitForQueueDrain(s_pumpDrainTimeout)` lines in `NativeProducer.cs` (`:1220`, `:1285`); the fixture keeps its call | **FAILS 3/3 — 894 / 1.** The sole failure is the new test, `Expected: RanToCompletion / Actual: Faulted`. Test 17 **passes**. |
| **MFIX** | **only** the fixture's `_pump.WaitForQueueDrain(s_deadline)` (`SendAccumulatorTests.cs:2025`); production keeps both calls | **FAILS 3/3 — 894 / 1.** The sole failure is **test 17**. The new test **passes**. |

The two are **disjoint**: each mutation fails exactly one test, and a different
one. So the call paths are not merely both covered — the failure signature
*identifies* which was broken, which the combined M1 could not do.
⚠ **Counts restated by 71.14.** The table and this sentence describe the suite as
it stood at `ad389871` (895 tests, one new test). With 71.14's pair added the suite
is 897 and MPROD fails **two** tests, not one; the three-way split (MPROD / MFIX /
M1285) and its measured signatures are in 71.14 below. The *conclusion* — split
mutations, disjoint signatures, never a joint deletion — is unchanged and is what
71.14 extends.

  - **`NativeProducer.StopPump` (`:1220`)** → guarded by
    `PublicProducerAccumulatorTeardownTests.Dispose_CompletesSendsAlreadyQueuedToThePump_RatherThanFaultingThem`.
    ⚠ **CORRECTED by 71.14.** This bullet originally read *"`NativeProducer.StopPump`
    / `StopPumpAsync` (`:1220` / `:1285`) → guarded by [that one test]"*, which was
    **false for `:1285`**: deleting `:1285` alone left the shipped suite at 895/0.
    `:1285` is guarded from 71.14 onward, by the **pair**
    `DisposeAsync_CompletesSendsAlreadyQueuedToThePump_RatherThanFaultingThem` +
    `Close_CompletesSendsAlreadyQueuedToThePump_RatherThanFaultingThem` — not by
    the sync test above, which stays green under that mutation by design (it is what
    distinguishes MPROD from M1285).
  - **`SendAccumulatorTests.Harness.Dispose` (`:2025`)**, the accumulator-side wait
    test 17 was written for → still guarded by
    `SendCompletionPumpPreStopDrainTests.Teardown_CompletesSendsAlreadyQueuedToThePump_RatherThanFaultingThem`,
    confirmed by re-running MFIX rather than assumed.

### `STATUS.md:20` / DV-4 — no change owed, and now evidenced

The finding permitted adjusting either **if** this round proved one wrong. It
proved them **right**: at HEAD the new test passes deterministically (48 rounds ×
24 sends, 3/3 batteries), which is the first *production-surface* evidence that
DV-4's *"NARROWED to the pathological case"* and `STATUS.md:20`'s *"narrowed, not
still open"* are true of `Dispose` / `DisposeAsync` / `Close` /
`Close(CancellationToken)` and not only of the fixture. Both left untouched, as
were the new dated `STATUS.md` phase entry (Manager's close-out), the P3.2
`PLAN.md`, every `CLAUDE.md`, everything under `.claude/rules/`, and the budget
files.

**S5 remains unimplemented, by instruction** — `_spaceGate.Cancel()` is still
exactly one call site, in `Stop`.

### Gates

| gate | result |
|---|---|
| `cargo build --features ffi` | clean (`Finished dev profile`) |
| `dotnet build Confluent.Kafka.sln` (**solution**, per the stale-binary trap) | **0 Warning(s) / 0 Error(s)** — all six assemblies; test-bin DLL mtime newer than the edited source, checked before every battery |
| `dotnet test Confluent.Kafka.sln -f net10.0` | **`Failed: 0, Passed: 895, Total: 895`**, 10 s — asserted non-zero; 894 → **895** is the one test added here |
| `dotnet test Confluent.Kafka.sln -f net8.0` | **`Failed: 0, Passed: 895, Total: 895`**, 10 s |
| `Test Run Aborted` grep | **0** on every run of every battery, with `Passed!`/`Failed!` = **1** in the same output as the control positive |
| `dotnet format --verify-no-changes` | clean (exit **0**) |
| Mode A: `git diff --stat 72137b6f..HEAD -- src/ src/ffi/ target/include/confluent_kafka.h cbindgen.toml` | **EMPTY**; control positive `-- bindings/dotnet/src/` = 131 insertions |
| net462 | benign abort — `Could not find 'mono' host` from `DotnetHostHelper.GetMonoPath()`, before any assembly loads. **Identified, not reported as new.** It still *builds* in the solution build above |
| working-tree scope | exactly **one** modified tracked file, `PublicProducerAccumulatorTeardownTests.cs`. Production untouched (the finding's own constraint) |
| `PerSendBudgetBytes` | **512 / 512 / 64** unchanged — no budget file in the diff |
| untouched axes | the sync path, every public API, `STATUS.md`, DV-4, the P3.2 `PLAN.md`, every `CLAUDE.md`, everything under `.claude/rules/` |
| pending `bindings/dotnet/CLAUDE.md` supersession-sweep rule | **not written** — still an open question with the user, no decision |
| `.claude/agent-memory/project-manager/MEMORY.md` | left **unstaged** |
| D1–D5 | nothing here contradicts any approved decision — the change adds a guard, it alters no behaviour |

---

## 71.14 · LOW · `StopPumpAsync`'s S4 call site was guarded only JOINTLY; three claims said otherwise — RESOLVED

Filed against `ad389871` (the 71.13 fixup). **Test-only fix**, as the finding and
the instruction scoped it: S4's production code is correct and **unchanged**, and
nothing here contradicts D1–D5.

**Fix commit:** `fixup! feat(M11/P3.2 S4): a bounded pre-stop drain so teardown completes what the core accepted`

**Was:** 71.13's fifth test made the **joint** deletion (MPROD) discriminating, but
deleting `NativeProducer.cs:1285` — `StopPumpAsync`'s
`pump?.WaitForQueueDrain(s_pumpDrainTimeout)` — **alone** left the shipped suite at
**895 / 0, zero `[FAIL]`**. So the async call site was covered only jointly, never
in isolation: the same non-discriminating shape 71.13 was filed against, one level
down. Its reach is not narrow — `StopPumpAsync` (`NativeProducer.cs:1250`) has
exactly two callers, `CloseWithCallback` (`await` at `:1441`) and `DisposeAsync`
(`:1571`), i.e. `DisposeAsync` + `Close()` + `Close(CancellationToken)`, **three of
the four** public teardown flavors. (The finding cited `:1440` / `:1570`; the
`await` lines at HEAD are `:1441` / `:1571` — verified, same two call sites.)

The reason 71.13 gave for omitting the async twin (*"the async probe detects only
2/3"*) was refuted by measurement, so the resolution is **(a) add the twin**, not
(b) record the gap.

### The fix — a PAIR, not a fifth-and-sixth independent test

Two new `[Fact]`s in `PublicProducerAccumulatorTeardownTests`, same shape as
71.13's sync test (48 rounds, fresh `AsyncMockProducer` per round, 24 unawaited
sends left in the accumulator, `TaskStatus.RanToCompletion` as the witness):

  - `DisposeAsync_CompletesSendsAlreadyQueuedToThePump_RatherThanFaultingThem`
  - `Close_CompletesSendsAlreadyQueuedToThePump_RatherThanFaultingThem`

**The guard is the pair**, and that is a measured decision rather than a hedge:

| regime | `DisposeAsync` alone | `Close` alone | at least one of the two |
|---|---|---|---|
| in-suite here, under **M1285** | **0/5** | **5/5** | **5/5** |
| the reviewer's worktree, each half measured alone | 4/5 | 4/5 | 5/5 (4/5 at 160 rounds) |

Which half loses the race is **not stable across regimes** — here `Close` carries
the whole detection and `DisposeAsync` never fires; on the reviewer's box the
sensitivity was split evenly. Dropping either half would therefore make the guard a
bet on which flavor happens to be sensitive today, and raising the round count is
not a substitute (a 160-round battery still gave 4/5 for the pair; run-to-run
variance dominates). Both halves are also *distinct public entry points* into
`StopPumpAsync` — `DisposeAsync` directly, `Close()` through `CloseWithCallback` —
so neither is redundant on coverage grounds either.

At HEAD (production intact) the pair is green **3/3** full-suite here (897/897) and
6/6 on the reviewer's box, so the imperfection is in **sensitivity, not in false
failures**.

### ⚠ Sensitivity is a property of the REGIME — and here it is *entirely* so

> ⚠ **SUPERSEDED IN PART BY 71.15 (round 10) — read the correction below, not the
> universal this section originally asserted.** As first written, this section said
> **all three** S4 public-surface tests detect nothing isolated — *"0/3 each"* — and
> gave CPU contention as the mechanism for all three. Re-measurement refutes both
> **for the sync half**. The originally-recorded numbers are kept below because they
> were genuinely measured; what was wrong was reading a **sample** as a **property**.

The finding's calibration note cut both ways and the re-measurement made it
sharper. Run under a `--filter` instead of in-suite, the three S4 public-surface
tests do **not** behave alike — **the two async halves detect nothing; the sync
half does**:

| mutation | regime | result |
|---|---|---|
| **MPROD** (both lines) | isolated, the **sync** `Dispose_Completes…` test, 11 runs | **4/11** — a real detection; ~24 ms per failing run |
| **MPROD** | isolated, the **sync** test **at HEAD** (control), 8 runs | **0/8** — so those 4 are detections, not flakiness |
| **MPROD** | isolated, each **async** half, 3 runs each | **0/3** each |
| **M1285** | isolated, the pair (filter matched exactly **2** tests) | **0/3** |
| **M1285** | isolated, each half separately | **0/3** each |
| **M1285** | **in-suite** | **5/5** |

The first three rows **replace** this section's original single row — *"MPROD |
isolated, each of the three tests, 3 runs each | 0/3 for all three"*. That 0/3 for
the sync half was a **sample**, not a property: at the ≈30 % per-run rate the
11-run battery measures, P(0 of 3) ≈ 0.34. n = 3 does not carry an "all three
detect nothing" quantifier — the same reasoning error 71.14 was filed to correct
one level up.

**The CPU-contention mechanism belongs to the async halves only.** An in-suite run
takes ~1 s against ~37 ms filtered, and for those two it is the suite's own
contention that keeps the pump thread off the CPU long enough for the missing drain
to be observable. It **cannot** be the mechanism for the sync half, which fails in
~24 ms with nothing else running.

⚠ **The parenthetical that let the universal through.** This section originally
discounted the one contradicting datum it already held, in a parenthetical reading
*"(The finding reported 1/3 isolated for the sync test on the reviewer's box; here
it is 0/3 — same conclusion, further along.)"* **That is wrong, and it is the
sentence to learn from:** 1/3 and 0/3 are not the same conclusion further along.
1/3 **refutes** the universal the next sentences then asserted; 0/3 is merely a
sample *compatible* with it. Two independent measurements on this machine (1/3 in
the main tree, 4/11 in a worktree) are non-zero. A single contradicting observation
outranks a compatible sample and should have stopped the universal being written at
all — discounting it as "further along" is what turned a calibration note into a
false claim.

So: always say which **regime** a ratio belongs to, and **never grade one of these
by an isolated PASS** — an isolated pass still proves nothing for any of the three,
and that directive survives intact. But an isolated **failure of the sync half** is
a real detection, and at ~0.7 s per filtered run against ~11 s in-suite it is the
cheapest re-verification available (five filtered runs ≈ 84 % detection in ~3.5 s).
Recorded at the class level in the test file, not only here.

### The THREE-WAY mutation split — the point of this round

Each mutation applied **alone** in the repo tree (restored with `git checkout --`
between batteries, restore verified by call-site count), **solution** build
`0 Warning(s) / 0 Error(s)` with the test-bin DLL mtime cross-checked against the
mutated source, full suite, `-f net10.0`, `Test Run Aborted` grepped **0** on every
run with `Passed!`/`Failed!` = **1** in the same output as the control positive.
Baseline at HEAD: **897 / 0**.

⚠ **All ratios in this table are the IN-SUITE regime** (see the corrected section
above for the isolated one). ⚠ **Read each row by its IDENTIFYING MEMBER, not by a
total** — the failing *set* varies run to run, so a total is one observed sample
while which member is present is the stable property, and it is the property this
split's conclusion actually rests on.

| mutation | deletes | runs | detected | **identified by** (stable) | totals observed (sample) |
|---|---|---|---|---|---|
| **MPROD** | **both** `pump?.WaitForQueueDrain(s_pumpDrainTimeout)` lines (`NativeProducer.cs:1220` **and** `:1285`); fixture intact | 3 + 7 | **10/10** | the sync `Dispose_Completes…` **is in the failing set** — present **7/7** across the seven-run battery, and absent **5/5** under M1285. §6 test 17 is never in it | **varies**: `{Dispose, Close, DisposeAsync}` 894/3 ×4 · `{Dispose, Close}` **895/2** ×1 · `{Dispose}` 896/1 ×2 |
| **M1285** | **only** `NativeProducer.cs:1285` (`StopPumpAsync`); `:1220` + fixture intact | 5 | **5/5** | the sync `Dispose_Completes…` stays **green** (5/5 — *by design*; that is what makes it the discriminant) while the async pair fails | 896/1, `Close_Completes…` only |
| **MFIX** | **only** the fixture's `_pump.WaitForQueueDrain(s_deadline)` (`SendAccumulatorTests.cs:2025`); production intact | 3 | **3/3** | **§6 test 17** is the only failure; all three public tests stay green — strictly disjoint from both others | 896/1 |

⚠ **The MPROD totals were originally recorded as THE signature, and they are a
sample.** This entry first stated MPROD as *"895 / 2, 3/3 — `Dispose_Completes…`
**and** `Close_Completes…`"*. Across seven further in-suite MPROD runs that exact
set occurred **once**; per-test the battery gave `Dispose_Completes…` **7/7**,
`Close_Completes…` 5/7, `DisposeAsync_Completes…` 4/7. So a reader checking "did I
apply MPROD correctly?" against 895/2 mismatches most of the time. Corrected per
71.15(e): state the row by its identifying member, and treat every total here as
one observed sample.

Failure signature in every case is exactly the S4 property:
`Assert.Equal() Failure: Values differ / Expected: RanToCompletion / Actual: Faulted`.

**The three signatures are distinct, and each is identified by a member the others
do not have:**

  - **MFIX** is strictly disjoint from both others (test 17 alone).
  - **MPROD vs M1285** differ by the **sync `Dispose_Completes…` test**: it fails
    under MPROD and is green under M1285. That test is therefore the discriminant
    for "was `:1220` removed too?", which is exactly the question the old joint-only
    recipe could not answer.

So the failing test name now says **which** drain was removed, for all three
mutations — the property 71.13 established for two and this round completes for
three.

### The claims that were false — corrected, and verified against what the tests now establish

(Line numbers in the "site" column are the **pre-fix** positions the finding cited;
the edits shifted them.)

| site | was | now |
|---|---|---|
| `PublicProducerAccumulatorTeardownTests.cs:124-125` | *"the two `pump?.WaitForQueueDrain(s_pumpDrainTimeout)` calls that actually ship are guarded"* — **false**, one was | the sync test is labelled **"PRODUCTION CALL SITE 1 OF 2"** and explicitly says `:1285` is on a path this flavor never enters, that deleting it alone leaves this test green (**measured**, not assumed), and that the async pair guards it |
| same file `:49-51` | *"S4's production call sites (`StopPump` / `StopPumpAsync`) therefore need their own guard here"* — read as both delivered | split into a two-item list mapping **each** call site to the test(s) that guard it, preceded by *"There are TWO production call sites, and one test does not cover both"* |
| `COMMENTS.DONE.71.md:1107` | *"`StopPump` / `StopPumpAsync` (`:1220` / `:1285`) → guarded by [the one test]"* — **false for `:1285`** | corrected in place with a ⚠ marker: `:1220` → the sync test; `:1285` → the pair, from 71.14 onward; and the sync test staying green under M1285 is stated as **by design** (it is the discriminant) |
| same file `:150-152` (the recipe) | the **joint** deletion (*"delete the two … lines"*) | the full **MPROD / M1285 / MFIX** split with each one's measured signature and totals, plus *"a joint deletion is not diagnostic — it was the recipe recorded here before, and it made the async call site look guarded when nothing tested it"* |

Each corrected claim was checked against what the suite **now** establishes rather
than assumed true once the twin landed: the "1 of 2" labelling, the "green under
M1285 by design" clause and every ratio in this entry come from the batteries
above, not from the fix's intent.

Two further stale statements inside 71.13's own record were marked rather than left
to be re-derived (both ⚠-flagged in place):

  - its **"Synchronous `Dispose`, not the `DisposeAsync` twin"** decision bullet —
    **superseded**; its premise (*"the async probe detects only 2/3"*) was refuted
    and its argument only ever held for the joint mutation.
  - its **"The two are disjoint: each mutation fails exactly one test"** sentence —
    counts restated: with the pair added the suite is 897 and MPROD fails **two**.
    The conclusion it drew (split mutations, disjoint signatures, never a joint
    deletion) is unchanged and is what this round extends.

### One wording fix the finding identified but did not file

71.13's *"Why the existing four cannot substitute"* bullet justified keeping
`AssertDrainedIntoTheOpenGate`'s tolerance by calling it *"load-bearing for the §3.8
ordering guard"*. That is imprecise: the §3.8 guard is
`Assert.Equal(sends.Length, producer.DrainedSendCount)` (`:344`), which sits **above**
the tolerance and is wholly **independent** of it — tightening the tolerance would
have cost that guard nothing. What the tolerance is actually load-bearing for is
**DV-4's pathological residual**: `WaitForQueueDrain` is bounded at 30 s and on
expiry `Stop` legitimately faults the remainder, so a helper demanding
`RanToCompletion` would be asserting the absence of a residual this phase
deliberately keeps. **The decision — a separate test, tolerance untouched — is right
and unchanged; only the stated mechanism was wrong.** Corrected in place.

### Gates

| gate | result |
|---|---|
| `cargo build --features ffi` | clean (`Finished dev profile`) |
| `dotnet build Confluent.Kafka.sln` (**solution**, per the stale-binary trap) | **0 Warning(s) / 0 Error(s)**; test-bin DLL mtime cross-checked against the mutated source before every battery |
| `dotnet test Confluent.Kafka.sln -f net10.0` | **`Failed: 0, Passed: 897, Total: 897`** — asserted non-zero; 895 → **897** is the pair added here |
| `dotnet test Confluent.Kafka.sln -f net8.0` | **`Failed: 0, Passed: 897, Total: 897`** |
| `Test Run Aborted` grep | **0** on every run of every battery, with `Passed!`/`Failed!` = **1** in the same output as the control positive |
| `dotnet format --verify-no-changes` | clean (exit **0**) |
| Mode A: `git diff --stat 72137b6f..HEAD -- src/ src/ffi/ target/include/confluent_kafka.h cbindgen.toml` | **EMPTY**; control positive over `bindings/dotnet/src/` = **131 insertions** (`NativeProducer.cs` + `SendCompletionPump.cs`). Working tree over the same pathspec: also empty |
| net462 | benign abort — `Could not find 'mono' host` from `DotnetHostHelper.GetMonoPath()`, before any assembly loads. **Identified, pre-existing, not reported as new.** It still *builds* in the solution build above |
| working-tree scope | exactly **one** modified tracked source file, `PublicProducerAccumulatorTeardownTests.cs` (+ this local comments record). **Production untouched** — the instruction's own constraint |
| **S5 absent** | `_spaceGate.Cancel();` still **exactly one** call site in `.cs` sources, `SendAccumulator.cs:1188` inside `Stop` — not implemented, by instruction |
| `PerSendBudgetBytes` / `PlainPerSendBudgetBytes` / `RegistrationDeltaBudgetBytes` | **512 / 512 / 64** unchanged — no budget file in the diff |
| untouched axes | the sync `Send`/`Flush`/`Close` path, every public API, `STATUS.md` (incl. no new dated phase entry — Manager's close-out), DV-4, the P3.2 `PLAN.md`, every `CLAUDE.md`, everything under `.claude/rules/` |
| pending `bindings/dotnet/CLAUDE.md` supersession-sweep rule | **not written** — still an open question with the user, no decision |
| `.claude/agent-memory/project-manager/MEMORY.md` | left **unstaged** |
| D1–D5 | nothing here contradicts any approved decision — the change adds a guard and corrects records; it alters no behaviour |

---

## 71.15 · LOW · the 71.14 fix's own "all three detect nothing isolated — 0/3 each" claim, and its stated mechanism, were refuted by measurement — RESOLVED

**Was:** the 71.14 fix round introduced a **new** measured-universal into the
permanent class-level `<remarks>` of `PublicProducerAccumulatorTeardownTests.cs`
(`:70-76`, restated at `:171-173` and mirrored in this record's own *"Sensitivity is
a property of the REGIME"* section): *"Run under a `--filter` instead and all three
detect **nothing** — 0/3 each … it is the suite's own CPU contention that keeps the
pump thread off the CPU long enough for the missing drain to show."* Both halves are
false for the **sync** test.

**Accepted in full.** This is the same defect class as 71.13 → 71.14, one level down
again — a **sample** read as a **property** — and it is worth the round for exactly
the reason the finding gives: a future reader reasons from a mechanism sentence far
more than from a ratio, and this one was stated as established.

**The measurement that refutes it** (Critic 71, throwaway worktree at `4f6c816b`,
solution build, `Total: 1` asserted on every filtered run, `Test Run Aborted` **0**
with a control positive):

| regime | runs | failures |
|---|---|---|
| **MPROD**, isolated, sync `Dispose_Completes…` | 11 | **4** |
| **HEAD (control)**, identical filter | 8 | **0** |

Signature `Expected: RanToCompletion / Actual: Faulted` at **[24 ms]** — which is
what kills the mechanism clause independently of the ratio: CPU contention from a
suite that is not running cannot be why a test fails in 24 ms. The two **async**
halves behave as written (`DisposeAsync_` **0/3**, `Close_` **0/3** isolated), so the
mechanism is right **for them** and stating it for them is what makes it true.

The fix round's own `0/3` was a sample, not a property — at the ≈30 % per-run rate
measured here, P(0 of 3) ≈ 0.34 — and this record already held a contradicting `1/3`
and **discounted it** as *"same conclusion, further along"*. That sentence is the one
that let the universal through, and replacing it is the substance of the fix.

**Fixed — comment and record text only, no production change, no new test, no
change to which tests exist** (the finding's own "not asked for and not wanted"):

| # | site | correction |
|---|---|---|
| 1 | `:70-76` class `<remarks>` | the universal is replaced by the **three-way split**: the two async halves detect nothing isolated (0/3 each, both machines), the sync half **does** (4/11 under MPROD, 0/8 at HEAD as control). The directive is kept — an isolated **pass** still proves nothing — and the cheapest-re-verification consequence is stated (≈84 % detection in ~3.5 s over five filtered runs) |
| 2 | same paragraph | the CPU-contention **mechanism** is scoped to the two async halves, with the sync half's ~24 ms isolated failure given as why it cannot be theirs |
| 3 | `:171-173` sync test inline | same correction, and the superseded `0/3` is named as a sample with P ≈ 0.34, not deleted silently |
| 4 | `:179-186` MPROD/M1285/MFIX recipe | each row now leads with its **identifying member** and marks the totals as one observed sample with the observed range — MPROD's failing set varies (`{D,C,DA}` ×4 · `{D,C}` ×1 · `{D}` ×2), so the recorded `895/2` occurred **once in seven** |
| 5 | **this record**, the REGIME section + the three-way-split table | mirrors 1–4: a supersession marker, the corrected six-row regime table, the mechanism scoped to the async halves, the table restated by identifying member with the totals marked as samples, and the *"same conclusion, further along"* parenthetical **replaced** by a statement of why a single contradicting observation outranks a compatible sample |
| 6 | `:225-232` (`DisposeAsync` block) | says **regimes** where the evidence is cross-regime, and names the reviewer's `4/5`-each figure as belonging to a *different regime* (each half alone in a 896-suite), not a different machine — the shipped 897-regime split is identical on both machines |

⚠ **Edit 6 was recorded as applied before this round and was not** — the target
sentence at `:225-232` was still verbatim from `4f6c816b`, *"not stable across
machines/regimes"*, with the reviewer's-box clause reading as cross-machine
evidence. Caught by grepping the target text rather than trusting the hand-off, and
applied here with edit 5. Same class as the finding itself: a claim about what had
been measured/applied, not checked against the artifact.

**What was NOT changed, deliberately:** S4's production code (correct), the round
counts, the tolerance rationale, and the directive *"never grade one of these by
running it alone"* — only its factual basis and its scope. 71.14's structural
substance (the pair, the discriminant, the three-way split) is untouched.
