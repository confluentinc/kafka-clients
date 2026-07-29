---
name: milestone11-cadence-and-stops
description: Milestone 11 (AdminClient) execution cadence and the one preserved hard stop (SCRAM crate)
metadata:
  type: project
---

Milestone 11 (AdminClient, N=1) runs on a **continuous cadence**: the Manager
drives Tier 2 → Tier 3 phase-by-phase (Actor → Critic → fix loop → handoff)
**without per-phase check-ins**. Do not pause for approval between phases; just
land each phase and report to the coordinator as it lands, then immediately
start the next phase's Actor. Phase breakdowns are in
`design/history/Milestone-11/PLAN.md` (Tier 2 = 3 phases, Tier 3 = 7 phases) —
do not re-plan; execute as written.

**Why:** the coordinator/user explicitly established this cadence to avoid
round-trip latency; they independently re-verify every landed phase.

**How to apply:**
- Order: Tier 2 P1 (done 2026-07-29) → T2 P2 "Group offsets" → T2 P3
  "Group/member deletion" → all of Tier 3 P1–P7.
- **Tier 4 is out of scope entirely** (Streams/Share/raft-voter). Do not start it.
- **ONE preserved hard stop: Tier 3 Phase 3 (SCRAM).** It needs a new PBKDF2
  crypto crate (`pbkdf2`+`hmac`+`sha2`, or `ring`) which changes `Cargo.toml`
  (CLAUDE.md §1.2 requires asking before adding a dependency). STOP before that
  phase touches `Cargo.toml` and let the coordinator relay the crate question to
  the user. Nothing else pauses. See PLAN finding #4.
- Scope is **Rust core + unit tests + real-broker integration tests only** — no
  C FFI, no Python bindings this milestone (see [[milestone11-scope-rust-only]]).
