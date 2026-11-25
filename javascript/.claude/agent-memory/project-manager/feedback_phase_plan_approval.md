---
name: phase-plan-approval
description: Before each phase of a milestone, write a short phase plan and wait for the user's approval before spawning any Actor or Critic
metadata:
  type: feedback
---

Before each phase, write a short phase plan next to the milestone plan (for the JS consumer,
`javascript/design/current/consumer/P<n>.md`, uncommitted) and wait for the user to approve it. Spawn no Actor or
Critic before that. Keep it short: what the phase delivers, file by file; the Java and core tests it translates, with
counts; its gate; and anything the user must choose.

When a brief says "if a check fails, stop and report", stop at that check and report. Do not do the later steps on a
guessed basis.

**Why:** decision 18 of the JS consumer plan (accepted 2026-11-25). The user wants to see each phase's scope before
agents spend effort on it.

**How to apply:** at the start of each phase of any milestone run here. Related: [[git-rules]],
[[js-consumer-milestone]].
