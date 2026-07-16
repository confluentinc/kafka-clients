---
name: milestone11-agent-numbering
description: Actor/Critic agent-number assignments for the Kafka Rust translation project (N=1 = Milestone 11 AdminClient)
metadata:
  type: project
---

Actor/Critic agent-number (`N`) assignments, so numbers are not reused across
requirements (COMMENTS.<N>.md / COMMENTS.DONE.<N>.md are keyed by `N`):

- **N=0** — earlier message-layer work (pre-Milestone-11). `COMMENTS.0.md` is
  its (now-empty) file.
- **N=1** — **Milestone 11 (AdminClient)**, all phases across Tiers 1–3. The
  `actor-executor` and `kafka-critic` subagents both run as number 1;
  `COMMENTS.1.md` (approved issues) / `COMMENTS.DONE.1.md` (resolved) are the
  coordination files.

**How to apply:** Keep using N=1 for every Milestone 11 phase (Manager runs
one Actor/Critic pair per phase, iterating COMMENTS.1 → COMMENTS.DONE.1 until
clean, then commit and move to the next phase). Reset `COMMENTS.1.md` to a
clean placeholder at the start of each new phase; copy the phase's
`COMMENTS.DONE.1.md` into the phase's `design/history` directory before
resetting. If a genuinely separate future requirement starts, assign N=2+.
See [[milestone11-scope-rust-only]] for what is in/out of scope.
