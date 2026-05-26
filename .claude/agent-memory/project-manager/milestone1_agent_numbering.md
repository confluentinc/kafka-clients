---
name: Milestone-1 agent numbering
description: For the Milestone-1 producer translation, agent numbers start at N=0 (Phase 1) and increment by phase, not by Actor/Critic cycle within a phase.
type: project
---

For Milestone-1 (`design/history/Milestone-1/PLAN.md`, end-to-end KafkaProducer fresh implementation), Phase 1 uses agent number N=0. The plan document originally specified N=1 for Phase 1, but the user explicitly overrode this to N=0 when starting execution on 2026-04-29.

**Why:** User's direct instruction at execution time — they want comment files for Phase 1 at `design/history/Milestone-1/Phase-1/COMMENTS.0.md`.

**How to apply:** Subsequent phases of this same milestone increment by 1 per phase (Phase 2 → N=1, Phase 3 → N=2, etc.), not per Actor/Critic cycle. Within a single phase, the same N is reused across Actor and Critic spawns and across fix cycles. Always check in with the user at phase boundaries before starting the next phase — do not silently chain phases.
