---
name: milestone11-scope-rust-only
description: Milestone 11 (AdminClient) is Rust-core-only for the current task; C FFI + Python bindings deferred to a separate future task
metadata:
  type: project
---

Milestone 11 (AdminClient translation) is scoped to **Rust core + unit tests
+ real-broker integration tests ONLY** for the current task — across all of
Tiers 1–3, every phase. **No C FFI and no Python bindings** are built in this
task.

**Why:** The user decided (2026-07-15) that this task is purely "implement
the admin client in the Rust client by translating from Java and the tests
and checking they work." Bindings are explicitly a separate future task.
Original plan assumed a per-phase Rust→C→Python vertical slice; that vertical
is dropped for now. A contributing factor: the async C dispatcher the plan
assumed already existed does not exist on master/the working branch — it
lives unmerged in PR #116 "C and python consumer bindings" (branch
`dev/c_and_python_consumer_bindings`, `src/ffi/common.rs`). Only the
synchronous producer FFI is merged; there is no consumer FFI or `consumer.py`.

**Execution cadence (updated 2026-07-17):** the user authorized *continuous
unattended progression* — the Manager proceeds phase-to-phase across Tiers
1→2→3 (Tier 4 excluded) without waiting for a per-phase user go-ahead, and
does NOT pause at tier boundaries (overrides PLAN.md's "pause at tier
boundaries" language), using the phase breakdowns already in PLAN.md. Still
run the full Actor→Critic→fix→verify loop and hand off each phase; the
coordinator re-verifies each report. **One hard stop remains:** Tier 3 Phase 3
(SCRAM) needs a new PBKDF2 crate — CLAUDE.md §1.2 requires asking the user
before any `Cargo.toml` dependency add, so stop there and surface that specific
question before starting SCRAM Rust-core work.

**How to apply:** Do not spawn Actors for FFI/Python work on any Milestone 11
phase. Each phase's Definition of Done = Rust core + unit tests + integration
tests green (CMake/CTest and pytest runs are deferred, not part of this
task's DoD). When the future bindings task eventually starts, reuse PR #116's
`src/ffi/common.rs` async dispatcher — do not reinvent it. The plan doc
(`design/history/Milestone-11/PLAN.md`) has a scope banner and deferral notes
capturing this; the bindings-slice A/B/C grouping in it also applies only to
that future task. See [[milestone11-agent-numbering]] for the Actor/Critic
numbering.
