---
name: review-loop51-generator-version-gate
description: Loop 51 (PLAN §9.1 generator guard) — 5 findings, 0 code defects; how to audit a generator emission set, and the "right fix, wrong justification" pattern
metadata:
  type: project
---

Loop 51 reviewed the generator change that added Java's
non-default-at-unsupported-version guard to all 197 message types. The **emitted code
was clean** — 5 findings, none a code defect: one false justification, one DoD #3 gap,
two test-coverage gaps, one precision error. Five habits that earned their keep:

**Re-derive a generator's emission set from the specs; do not sample the output.**
Re-implementing Java's rule in ~80 lines of Python over `generator/messages/*.json`
(intersect parent/struct versions, `VersionConditional`'s `ifNotMember` reachability,
`!field.ignorable()`) produced 227/127/100/12/53, matching the Actor exactly — then
diffing the derived *field names* against the emitted guard messages proved there was no
residual in either direction. A count match alone would not have; the name-level diff is
what makes it decisive, and it costs one extra step.
**Why:** this is the only way to review a change whose blast radius is "197 generated
files" without reading 197 files.

**"Demonstrated live" is a claim about a mechanism — trace the value to its setter.**
The commit's headline evidence was that a config flag was "silently dropped by the
version gate". The field was never *set* on the request (production sets four fields;
the two 2PC setters are `#[cfg(test)]`), so nothing was dropped and the fix changes
nothing there. The repo even documented this, in a doc comment on the same function.
**How to apply:** for any "field X was dropped" claim, grep `set_<x>` in `src/`, exclude
`#[cfg(test)]`, and confirm a production writer exists **before** accepting the severity
it supports. A right fix can carry a wrong reason, and the reason is what lands in
PLAN.md as settled fact.

**Skips recorded in comments outlive their blocker.** Two Java tests were skipped in a
comment block naming this exact generator gap; closing the gap did not surface them,
because no test runner lists a commented-out test. The Actor's coverage sweep of the
*Java* corpus found two tests and missed these two, which were recorded on the *Rust*
side.
**How to apply:** when a change closes a named blocker, grep the test tree for the
blocker's identifier **and** the prose used to describe it. Sibling of the `#[ignore]`
habit (Critic 50 S1) — that one covers tests parked in code.

**One guard, two emission sites.** Tagged and untagged fields took different code paths,
so a test asserting the guard on an untagged field pins only half of it. Ask of any new
generated construct: how many places emit it, and does a test reach each?

**`OUT_DIR` selection is a trap.** `ls -td target/*/build/*/out/generated` ranks by
*directory* mtime and put a stale 0-guard directory first, twice. `invoked.timestamp` in
the build dir is the reliable discriminator. Several sibling `OUT_DIR`s persist from
earlier feature/profile combinations and from stash-based baselines.

**Adjudication precedent set here (rules-file edits by an Actor).** A *factual status
correction* of a statement about the code that the Actor's own change falsified is
defensible in place — the repo already does this (`consumer-threading.md` §10,
`producer-transactions.md` §7/§12) and leaving a false statement invites a future false
positive. Adding or reshaping a **How to apply** / **Anti-patterns** bullet is normative
and is not. Note `COMMENTS.50.md` states the stricter convention ("for the human to
accept or reject"), so either way the edit needs ratification. Filed as suggestion S2 to
write the distinction into `agent-roles.md`.

**Pass 2 (5 findings fixed, 2 new, both low).** Three more habits:

  - **A subagent's finding is not yours until you verify it.** The panic report never
    reached me; the loop credited me with it anyway. Say so and re-derive it, or you are
    signing off on work you did not do.
  - **Check a *replacement* section for the defect it was written to fix.** PLAN §9.1's
    first correction was a mis-citation; its replacement §9.31 shipped a fresh one
    (`AlterPartitionReassignmentsRequest.java:60-65`, actually `:44-47`). Re-open every
    cited line range in a section whose whole point is citation accuracy.
  - **Audit a completeness claim by enumerating the complement.** "Two APIs / three
    checks" is only checkable by finding every *other* API with a Java `build(short)`
    gate and confirming each is ported. Eight were; the table was right. One candidate
    (`ConsumerGroupHeartbeat.SubscribedTopicRegex`) looked like a live consumer-path gap
    until I read the builder — it is gated. Read the builder before filing.

**Pass 3 (the reachability adjudication).** Two more:

  - **A reachability claim is settled by an exhaustive split over the code that picks
    the value, not by hunting for a counterexample.** For "can we send Metadata v0?" the
    three paths are: version info present (`min(broker.max, latest)`), version info
    absent (uses *latest*, never oldest), and a pinned version (test-only here). Close
    all three and the answer is derived. The strongest form needs no external fact at
    all: `OffsetFetch`'s null-topics gate threshold **equals** the field's
    `nullableVersions` lower bound, so the illegal window and the refused window
    coincide by construction.
  - **Check a scope sentence against its own title.** §9.32 was titled "outside
    `nullableVersions`" but scoped to "starts above" — one of two bounds. Deriving both
    gives 7 fields, not 6. Nothing behavioural turned on it, but the section instructs
    the next person to derive-then-diff, and the two formulations disagree.

Also: latent ≠ not worth fixing, and saying so is part of the ruling. A class gap in a
generator function goes live silently when a spec changes; that argument survives the
reachability answer, and is stronger than the live-wire claim it replaces.

See also [[review_loop50_split_panic]].
