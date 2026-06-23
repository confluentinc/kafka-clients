---
name: phase-8a3-dropped
description: Phase 8a.3 perf-collapse investigation was dropped — the post-8a.2 collapse was a broker-side issue, not a client bug
metadata:
  type: project
---

The post-Phase-8a.2 perf collapse (5-min run holding ~118k msg/sec for ~50s then `errors=1` + `0 msg/sec` for ~4 min, captured in the now-deleted `perf-8a2-verify.jsonl`) is a **broker-end issue**, not a client-side bug. Do NOT track it as a sub-phase, do NOT insert a Phase 8a.3 row into `Phase-8/NOTES.md`, and do NOT add an "Open concerns" section about it. The decision was made on 2026-05-15 during the Manager session that opened Phase 8a Round 1 Critic review.

**Why:** User reviewed the perf data after the previous Manager run flagged it. Verdict: environmental, not actionable as a client-side translation task.

**How to apply:** When closing out Phase 8 housekeeping, just delete `perf-8a2-verify.jsonl` along with the other stray perf files. No NOTES.md addendum. The Manager workflow goes housekeeping → 8a Round 1 Critic → 8b → 8c → 8d → 8e → 8f → Phase 9, with no 8a.3 detour.
