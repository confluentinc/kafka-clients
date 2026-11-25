---
name: producer-pr-moves
description: PR #312, the base of the JS consumer branch, is force-pushed and moves often - fetch and check the base at the start of every run
metadata:
  type: project
---

PR #312 (`origin/dev/javascript-producer-bindings`) moves often: it was force-pushed twice by 2026-11-24, rebuilt
on the rules branch (`origin/kraina-js-claude-rules`, PR #327) as `675c9a99d`, and on 2026-11-25 got five more
commits (the producer's header rewrite) up to `208abeb64`, while a Manager run was checking it.

**Why:** the consumer branch has no commits of its own yet and sits on PR #312, and PLAN.md cites line numbers of
`javascript/CLAUDE.md`.

**How to apply:** at the start of every run or phase, `git fetch origin` and check that HEAD equals PR #312's tip and
that the rules branch is an ancestor. If not, stop and report; the user moves the branch. Also check whether the JS
conventions part changed: lines 1-224 of `javascript/CLAUDE.md` are the conventions (identical to the rules branch's
file), and "The binding's notes" start at line 228, so a diff limited to the notes leaves the cited line numbers
valid. Related: [[js-consumer-milestone]].
