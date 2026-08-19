---
name: plan-review-workflow
description: User reviews Manager plans as an uncommitted in-repo diff, and expects gap analyses to be independently verified rather than accepted
metadata:
  type: feedback
---

When acting as Manager, write plan drafts to their final in-repo location
(`design/history/Milestone-N/PLAN.md`, `Milestone-N/Phase-K/PLAN.md`) but leave
them **uncommitted** so the user reviews them as a diff in place.

**Why:** `agent-roles.md` says to save the plan *after* approval, but the user
prefers reviewing the real file rather than a chat transcript. Uncommitted means
approval is still a separate, explicit step — no commit implies no consent.

**How to apply:** Write the files, do not `git commit`, do not spawn Actor or
Critic agents until the user approves. Milestone plan is approved first, then
each phase plan.

Related: when the user supplies a gap analysis and says "build on this, don't
repeat it", they still expect the *load-bearing* claims verified against the
source, and they want to be told when the briefing is wrong. On the Milestone-11
plan (2026-07-30) three briefing claims were wrong — all the txn error codes
already existed, `WriteTxnMarkers`/`EndTransactionMarker` were not producer-side
— and correcting them removed a phase's worth of work. Do not treat a supplied
gap analysis as settled fact; treat it as a starting hypothesis and report
deltas prominently.
