---
name: stale-design-current-docs
description: design/current/{status,structure,design}.md are stale and understate the project by ~7 milestones; use design/history instead
metadata:
  type: project
---

`design/current/status.md`, `design/current/structure.md`, and
`design/current/design.md` are **stale**. They describe Milestone 3 and ~15k
lines of Rust. Reality as of 2026-07-30 is Milestone 10 shipped and ~143k lines.

**Why:** they stopped being updated somewhere around Milestone 3/4 while the
per-phase plans kept being written. Milestone 9 and 10 shipped with design notes
in `design/history/` and never refreshed `design/current/`.

**How to apply:** For actual project state read `design/history/MILESTONES.md`
(maintained, one prose section per milestone) and the per-phase
`design/history/Milestone-N/**/PLAN.md` files. Do not cite `design/current/*` as
the baseline, and do not append new-milestone detail to it without first
correcting the Milestone/LoC headline — otherwise the correction debt compounds.

Milestone-11 Phase-1 handoff is the point where this is scheduled to be fixed
(see [[milestone-11-producer-transactions]]).
