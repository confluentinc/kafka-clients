---
name: review-m13-phase6-closeout
description: M13 Phase 6 sweep/audit close-out review — how to audit a completeness table and skip-with-reason claims
metadata:
  type: project
---

Milestone 13 Phase 6 = final close-out (residual-file sweep.md + 81-commit audit
in PLAN §6 + docs). Reviewed as "audit the auditor". Verdict: substantively
sound, 2 LOW doc-accuracy findings. See [[review-m13-phase0]] for the corpus
verification recipe.

**Two findings worth remembering as recurring close-out traps:**

1. **"Phase N (already applied)" in a completeness table can be a mis-tally.**
   Audit attributed KAFKA-19012 (`1df2ac5b2b`, a real 4.3.1 fix) to "Phase 2" and
   counted it in the Phase-2 covered tally, but the deferral machinery
   (`buffer_deallocated`/`inflight` flags, `deallocate` guard, `complete_batch`/
   `complete_and_deallocate_batch`, `batches_awaiting_response`, `set_inflight`)
   was **already present at the milestone-base commit** and predates M13
   (introduced in M11 producer-txn work `782c4681`/`a2ca2f96`). **Verification
   recipe:** `git show <milestone-base>:path | grep -c <symbol>` — if the
   machinery exists at the base, it's pre-existing, not phase-covered. Also
   confirm the named phase commit actually touches the files
   (`git show <phasecommit> --stat | grep <file>`). The audit NOTES were honest
   ("verified already present"); only the table cell + tally contradicted them.

2. **Skip-with-reason coverage justifications overstate a single test file.**
   ProtocolRoundTripConsistencyTest skip premise was correct (Java cross-validates
   generated write/read/size vs runtime Struct/Schema serializer; Rust has no
   runtime Schema path — one serializer, nothing to diff). But the supporting
   claim that `message_serialization_test.rs` gives "per-message-type round-trips"
   overstated it: that file is ProduceRequest/FetchRequest + primitives (18
   tests). Aggregate coverage (`message_round_trip_test.rs`, the 4 generator
   test-message files, M11 byte-vector tests) is meaningful but NOT exhaustive
   across ~197 types. Skip still stands; only the coverage sentence is imprecise.

**What was verified clean (recipe reusable for future bump-milestone audits):**
- Re-derive changed files: `git -C kafka diff --name-status -M 4.2.0..4.3.1 --
  clients/.../clients clients/.../common` → 176 main files. Tags: verify
  `4.3.1^{commit}` (annotated tag → rev-parse gives tag object, not commit).
- (b) untranslated rows: grep `src/` by SYMBOL not just filename (SaslConfigs.rs
  EXISTS but the OAuth EXPECTED_AUDIENCE/ISSUER keys don't → correctly (b)).
- broker-only commit `cf9f8ad376` touched only Broker* specs, all synced by
  Phase 0 `55f6e95a` — broker specs ARE in `generator/messages/` (198 json).
- KAFKA-20309 `2f2d9b0172` AEP hunk wholly inside `process(SharePollEvent)` →
  Share-only reclass correct.
- Gate: `ff13c8c7..HEAD` = 29 commits all M13; tree clean; format/lint/`test
  --lib` (3719 pass) green.

**take_batch_data §12 note:** `memory_records_builder.rs:474`
`take_batch_data` copies (`buffer[..].to_vec()` → `Bytes`), documented as a
pre-existing "Phase 1 deviation" (bytes 1.x has no zero-copy Vec→BytesMut). This
IS a §12 "finalization must not copy" deviation but documented + pre-existing;
and it's what gives KAFKA-19012 "structural immunity" (network never writes the
pooled Vec). Do NOT flag as a new defect.
