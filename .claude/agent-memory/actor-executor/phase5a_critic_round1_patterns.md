---
name: phase5a-critic-round1-patterns
description: Critic 45 pass-1 reusable patterns — deleting a guard invalidates every deferral that cited it; a shipped check must be run from its own comment block; prose contradicted by the code it justifies; mutation that hangs instead of failing
metadata:
  type: feedback
---

Five reusable fixes from Critic 45 pass 1 on Milestone-11 Phase 5a (7 findings, 7
conceded, 0 disputed; none behavioural in production code).

**Deleting a guard invalidates every deferral that cited it — sweep the *reasons*, not
just the comments.** Removing `TransactionManager::new`'s transactional-id guard
invalidated two things beyond the five comments the sweep caught: `begin_abort`'s doc
(and its user-visible error string), and an 18-entry `SenderTest` deferral group whose
single stated rationale was that guard. Three of the 18 turned out to need nothing
further, and one was the only cover for a production branch no test reached.
**Why:** `grep` for the guard's wording finds comments; it does not find *groups
deferred because of it*. **How to apply:** when a guard is deleted, grep for its
wording AND re-derive every accounting group whose header cites it. Re-derive
per-entry — writing a fresh blanket rationale is what hides the entries that became
expressible.

**Run a shipped verification command by extracting it from its own comment block, not
by retyping it.** A shipped `awk` had `delete soft` (typing the name as an array) and a
later `soft = 1`, so it aborted and three of five checks printed `0` — including the
headline totals. It had worked in the scratchpad, where `soft` was an array; the defect
was introduced while compressing it for the comment. **Why:** retyping tests the
scratchpad version; only extraction tests what a reviewer would copy.
**How to apply:** `sed` the program out of the file, run it, `diff` its output against
the pasted transcript. Also make it deterministic first — `for (k in arr)` in awk and
a bare `sort -rn` both leave order unspecified, so a pasted transcript is not
reproducible without fixing them.

**A regex with an optional-modifier prefix silently matches constructors.**
`(?:(?:public|private|...)\s+)*<type>\s+<name>\(` backtracks to zero repetitions on
`public Foo(...)`, reads `public` as the type and captures `Foo` — while the prose
beside it claimed constructors were excluded. Then the phantom scored *present* against
a `#[cfg(test)]` helper with the colliding name. **How to apply:** put a negative
lookahead after the modifier run so it must consume all modifiers and a return type
*and* a name are both required; and cut each Rust file at `#[cfg(test)]` before scanning
for `fn`, so no test-only item can satisfy a production claim. Enforce exclusions in the
code, never in the sentence next to it.

**Prose contradicted by the code it justifies is worse than vague prose.** Three sites
claimed a field had one reader, justifying a placement decision — while the parameter
that decision produced existed *because* of a second reader. **How to apply:** when
writing "the only reader/writer is X", enumerate with `grep -n` and paste the line
numbers. If a method signature exists to serve a reader, that reader belongs in the
enumeration.

**A mutation check that hangs is not a passing mutation check.** Removing the code that
fails a pending `TransactionalRequestResult` made the test await a result nothing would
ever complete. **How to apply:** before `.await`-ing a result whose completion is the
thing under test, assert the non-blocking `is_completed()` first. Costs no fidelity —
the await still pins the error — and turns a CI hang into a named failure. Also: never
`git checkout --` a whole file to undo a mutation when uncommitted work is in it; copy
the file aside first (this cost a redo).
