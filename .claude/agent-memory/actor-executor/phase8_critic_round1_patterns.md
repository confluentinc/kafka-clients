---
name: phase8-critic-round1-patterns
description: Phase-8 Critic pass-1 lessons — inert translated surface, unreachable discriminating claims, defect-scope under-statement, and sweeps whose own denominator excludes the population just created
metadata:
  type: feedback
---

Five reusable failure shapes from Critic 48 pass 1 (eleven findings, all conceded).

**1. Translating a mock's method is not the same as translating its effect — check the
routing, not the signature.**

**Why:** Phase 8 translated `MockClient.disconnect(node, allowLateResponses)` and it was
**inert**: Java's second delivery reaches the Sender because `ClientRequest.callback()` is a
*getter*, so the retained request keeps its handler; this port routes produce responses by
correlation id through a map the first delivery `remove`s, so the late response was silently
discarded. The test named "…Twice" observed nothing, and its justifying comment asserted a
false Java fact ("the retained request carries no callback").

**How to apply:** before translating a test-harness method, trace how the *effect* reaches
production code in both languages. If the port routes differently (callbacks vs a map,
identity vs position), the method may be a no-op here. Prefer documenting the absence with
citations over shipping inert surface. The recovery that worked: find a different path to the
same observable state (here, the delivery-timeout expiry parks the batch *without* consuming
the routing entry), then pin it with assertions on the routing state that a deleted response
would fail.

**2. A test's stated discriminating property must be reachable by that test.**

**Why:** the epoch-bump integration test claimed "a client that failed to reset its sequences
would be rejected" — but step three used a *fresh* producer, whose sequences are 0 because
they were never anything else. No client reset code was on the path, so no mutation of it
could change the outcome. (Critic 46 issue 3 was the same shape.)

**How to apply:** for every "this test would catch X" sentence, name the function X lives in
and check it is on the path. If it is not, either restructure or describe only what the test
proves — and say where X *is* covered. Re-attributing is often the right call; say why you
chose it over restructuring.

**3. When you fix a defect, derive its trigger from the removed code, not from the story that
led you to it.**

**Why:** the consumer bail was found via an abort-then-commit test, so all four artifacts
described abort-then-commit as the trigger. The removed guard sat *after*
`consume_aborted_transactions_up_to`, and the ABORT marker batch carries the id that call had
just inserted — so **any** aborted transaction tripped it, and `read_committed` was unusable
on any partition that had ever had an abort. The record under-sold a defect I had fixed.

**How to apply:** read the guard's condition against the state at that program point and ask
what the *minimum* input is. Do the same for filed-not-fixed defects: §9.25 said "carries
stale sequence state into the retry" when the `?` fires before any retry exists, so the batch
is dropped un-completed — futures hang and the buffer leaks. Severity drives whether a fix
gets scheduled.

**4. A completeness sweep's denominator is itself a claim — ask what it excludes.**

**Why:** the re-run header sweep reported "52 headers, 0 mismatches" while silent about 50
headers *in the same file*, under the same convention, in a population this phase grew from 6
to 50. Nine deviated, six of them newly added, one spanning two Java methods.

**How to apply:** when re-running a shipped sweep, re-derive its population, not just its
result. If a phase created a new population matching the same convention, widen the pattern
before reporting the count. Phase 6 pass 4's "before asserting any N of M, ask what M would
miss" applies to the checker as much as the checked.

**5. Give one cause per correction, and prefer a mechanical sweep to a read when the mistake
is structural.**

**Why:** two off-by-one range fixes were attributed to a shared `try (Metrics ..)` wrapper;
it held for one and the other had no inner braces at all. And inserting a method into another
method's attribute list silently stole its `#[cfg(test)]` and doc — invisible to
`cargo xtask lint` because of a file-level `#![allow(dead_code)]`. A ten-line sweep
(doc-comment → attributes → doc-comment) proved it was the only instance crate-wide; reading
would not have.

**How to apply:** verify each instance's cause separately. When a mistake has a structural
signature, write the sweep — it is cheaper than the argument about whether you looked
everywhere.

**Also:** a number measured mid-phase will be invalidated by the same phase's later commits.
Round it, ship the command to re-derive it, and say the rounding is deliberate — rather than
re-writing an exact figure that resets the trap. The Critic generalised this and it is now the
standing rule: **ask for exactness only where the number cannot be re-derived**; where a
derivation can be shipped, ship it and round the display.

---

## Pass 2 added two more (loop 11 → 3), both about the *fix* rather than the code

**6. A sweep written for one shape will miss its sibling shape — so put it in `xtask`, not in
a commit message.**

**Why:** pass 1's fix for a migrated-attribute defect shipped a sweep for "doc → attributes →
doc" and reported 0 suspects. Pass 2 found that the *same fix* had created a **stacked doc
block** (two summaries, two `Translated from` lines, no separator), which that shape
structurally cannot see — and it was the only instance in the tree. Two consecutive rounds
where the fix for a doc-hygiene finding introduced one.

**How to apply:** when a defect's signature is structural, enumerate the *family* before
writing the check, and put it where the gate runs it — `cargo xtask doc-hygiene`, called from
`cargo xtask lint`, per CLAUDE.md §6. An ad-hoc script quoted in a commit message is a check
nobody is obliged to run, which means it finds the next instance one review round late.
Mutation-check each shape separately and record the exit codes.

**7. A taxonomy written as prose beside correct corrections drifts. Paste the derivation
instead.**

**Why:** the header-sweep bullet mis-classified the same entry in both rounds — pass 1 gave
one cause for two corrections, pass 2 called a both-ends-wrong entry "±1 at one end" and a
body-comment start an annotation. The nine *corrections* were right both times; the sentence
generalising them was not.

**How to apply:** if a record classifies a set, generate the table (deltas plus what the cited
line actually holds) and paste the output. Prose summarising a mechanical fact is the part that
rots.

**8. Check that a rule you cite actually reaches your case — read its scope note.**

**Why:** §9.28 justified a design conclusion with "CLAUDE.md §11 warns against per-message
callbacks on the hot path". §11 says nothing about callbacks, and its own "Hot path" definition
*excludes* per-RPC surfaces — which is exactly what a `RequestCompletionHandler` is (one per
produce request). The true obstacle was ownership: a Rust completion handler cannot capture
`&mut self`, documented three times in the same file (`sender.rs:182`, `:348`, `:374`). A
structural obstacle is a *stronger* reason than a performance budget, so the mis-citation
weakened the record it was meant to support.

**How to apply:** before appealing to a numbered rule, read the rule and its scope note. If the
real reason is structural, say so — a later phase weighing the trade-off needs the actual
constraint, not a rule that does not apply.
