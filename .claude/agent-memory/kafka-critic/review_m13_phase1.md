---
name: review-m13-phase1
description: M13 Phase 1 (common, AK 4.2→4.3.1) — record→record::internal move + ControlRecordType via generated schema; CLEAN, review heuristics for package-move phases
metadata:
  type: project
---

M13 Phase 1 (agent 61, branch milestone-12-ak-4.3.1) reviewed CLEAN — 0 defects.
Commits 3050032e/7c6d55f4/952b6320/0ee0bda5. See [[review-m13-phase0]] and
[[review-spec-corpus-two-sources]] for the corpus/version-bump context.

**Why:** these package-move + generated-schema phases have a narrow defect surface
but specific traps. **How to apply** the heuristics next time:

- **Package-move fidelity (KAFKA-20128 shape):** run
  `git -C kafka diff -M 4.2.0..4.3.1 -- '<old>/*' '<new>/*'` and read the per-file
  line counts. 2-line files = package+import churn (mechanical, trust the move).
  Only files with a bigger count carry real content — here ControlRecordType(53)
  and the MemoryRecordsBuilder `writeEndTransactionMarker`→`type.recordKey()` hunk
  (write-path, untranslated → N/A). Grep the Rust file to confirm the write-path
  method genuinely isn't translated before accepting an N/A.

- **`internal`-package visibility deviation:** CLAUDE.md §2 says internal→pub(crate),
  but Java keeps the classes `public` in the `internal` package. A `pub` re-export
  at `common::record` is therefore MORE Java-faithful, not less. Judge by (a) is
  the external consumer real and unmovable — here RecordsSerdeTest needs
  `SimpleRecordsMessageData`, generated ONLY into the test crate's OUT_DIR
  (build.rs test_generated), so it cannot be an in-crate #[cfg(test)] test; (b) is
  it minimal — only 2 of ~13 types re-exported; the builder/record types stay
  unnameable (their mods are pub(crate)), reachable only by method-chaining off the
  exposed type, same as Java. Don't flag a documented, minimal, Java-faithful one.

- **ControlRecordType byte-level check:** `to_version_prefixed_byte_buffer(0,schema)`
  = write_short(version) ++ write_short(type) = [0,0][type_be], byte-identical to a
  hand-built [version][type] vector. Verify against the generated write() body, not
  by trusting the round-trip test alone. testParseUnknownVersion must use version>0
  (here 5) to actually exercise the >HIGHEST clamp path.

- **`#[allow(dead_code)]` after a visibility reduction:** legitimate when the method
  is DoD#2 API-completeness AND its lost caller was crate-internal (write-path in
  Java). Confirm the ONE production reader isn't among the gated methods — here
  CompletedFetch::contains_abort_marker uses parse/from_type_id (ungated), while
  type_id/record_key/control_record_key_size (write/serialize chain) are gated. OK.

- **FP calibration reused (Critic 41):** `pub` fn with pub(crate)-typed params/return
  isn't externally reachable. Corollary here: a `pub` re-export of a `pub` struct IS
  reachable, and its methods returning pub(crate) types leak those types as callable-
  but-unnameable — that's fine and Java-faithful, not a bypass.
