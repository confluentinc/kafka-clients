---
name: js-consumer-milestone
description: The JavaScript (Node.js) consumer binding milestone - where its plan is, its agent numbers, and what the user decided on 2026-11-25
metadata:
  type: project
---

- Branch `dev/javascript-consumer-bindings`, built on PR #312 (`origin/dev/javascript-producer-bindings`). The plan is
  `javascript/design/current/consumer/PLAN.md`, written by an earlier Manager.
- Agent numbers 150-157, one per phase (decision 17).
- On 2026-11-25 the user accepted every recommendation in PLAN.md section 8, with these specifics: decisions 2 and 3
  are settled by the rules' Q31 header rows (plain TypeScript `Headers`, consumed headers copied on first read or
  change, `[key, value]` pairs accepted); decision 7 builds the C close options as Java does
  (`CloseOptions_with_operation_err` with no timeout, marked a conventions conflict); decision 1 is option (a);
  P1-P5 start before decisions 1 and 4, refusing deserializers with `UnsupportedOperationError` until P6; the merge
  order in decision 14 is still the user's to pick.
- As of 2026-11-25 the plan was not yet updated or committed, because PR #312 moved during the run (see
  [[producer-pr-moves]]). Check PLAN.md's status line for the current state before relying on this.

**Why:** these decisions shape every phase brief; PLAN.md holds the full text once it is updated.

**How to apply:** read PLAN.md first in any run of this milestone, and follow [[phase-plan-approval]] and
[[git-rules]].
