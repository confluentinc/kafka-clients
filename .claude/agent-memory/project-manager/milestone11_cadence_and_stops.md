---
name: milestone11-cadence-and-stops
description: Milestone 11 (AdminClient) execution cadence; in-scope work COMPLETE as of 2026-07-30 (SCRAM stop resolved)
metadata:
  type: project
---

**STATUS (2026-07-30): Milestone 11 in-scope work is COMPLETE.** All 46/46
in-scope Admin RPCs are translated (Tier 1: 17, Tier 2: 9, Tier 3 P1–P7: 20),
each Critic-clean, 3029 lib tests passing. The SCRAM hard stop below was RESOLVED —
the user approved `aws-lc-rs` (see [[aws-lc-rs-crypto-decision]]) and Tier 3 Phase 3
landed Critic-clean on first pass. Remaining out-of-scope (not started): C FFI /
Python bindings (separate future task, reuse PR #116) and Tier 4. The cadence notes
below are retained for any resumed/follow-on Admin work.

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

**Guard: `tests/integration/admin_smoke_test_manual.rs` is developer-local — never commit it OR a `mod` line referencing it.**
It is the user's manual smoke test. As of 2026-07-29 the coordinator added it to
`.gitignore` (commit 0a9e4c0), so `git add` can no longer pick up the `.rs`
file. **This happened TWICE before that fix** (Tier 3 P1 commits 9fe737e/9114755
committed the file; Tier 3 P2 commit 85c6c44 re-introduced the
`mod admin_smoke_test_manual;` line into tracked `tests/integration/main.rs`
even after the file itself was untracked in d2b6e36 — a fresh clone then failed
to compile `tests/integration` because the referenced module wasn't tracked).
**Lesson: the failure mode is the `mod` line inside tracked `main.rs`, not just
the tracked `.rs` file.** Checking `git ls-tree | grep admin_smoke_test_manual`
(filenames only) is INSUFFICIENT — it misses the mod-line-in-main.rs case. After
every Actor phase also run `git show HEAD:tests/integration/main.rs | grep
admin_smoke_test_manual` (must be empty) and remind each Actor: the file is
gitignored/untracked and no commit may reference it (its `mod` line stays a
local uncommitted edit).
