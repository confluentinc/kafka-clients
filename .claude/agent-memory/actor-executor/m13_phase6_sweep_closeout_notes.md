---
name: m13-phase6-sweep-closeout
description: Milestone-13 Phase 6 sweep+close-out — zero residues, 81-commit audit method, two "already-applied" verifications
metadata:
  type: project
---

# M13 Phase 6 — sweep + close-out (agent 66)

Closed Milestone 13 (AK 4.2.0→4.3.1). Deliverables landed in 3 commits
(02405070 sweep+ProtocolRoundTrip, 4096fdd1 PLAN §6 audit, d2b11b91 docs).

**Key outcomes:**
- **Zero (d) uncovered in-scope residues.** Phases 0–5 were thorough; every one
  of the 176 changed main files + 117 test files maps to a phase / untranslated
  area / deleted-moved-out. The Phase-1 commit `952b6320` message is the single
  best cross-reference — it enumerates every common small-delta disposition.
- Classification table → `design/history/Milestone-13/sweep.md`; 81-commit audit
  → PLAN.md `## 6`. Tally: 48 phase-covered, 33 skipped.

**Method for the commit audit** (reusable for future version bumps):
`git -C kafka log --oneline --cherry-pick --right-only <old>...<new> -- <paths>`.
Classify by touched-file scope: `git show --name-only --format="" <sha> -- <paths>`
(NOT `--stat`, which wraps long paths and breaks grep). Most MINORs are
javadoc/import-move/cosmetic; the risky ones are behavioral bug fixes not named
in a phase coverage line — investigate those individually.

**Two "already-applied" verifications (not gaps):**
- `1df2ac5b2b` KAFKA-19012 (producer buffer-reuse-while-inflight): the
  `buffer_deallocated`/`inflight` flags + abort-path deferral are present in
  `src/producer/internals/{producer_batch,record_accumulator,sender}.rs` with
  explicit KAFKA-19012 comments. ALSO structurally immune: `take_batch_data()`
  in `memory_records_builder.rs` `.to_vec()`-copies finalized bytes into an owned
  `bytes::Bytes`, so the pooled `Vec<u8>` is never what's written to the network.
- `917d695322` KAFKA-17019 (TimeoutException root cause): the `await_result_timeout(timeout, reason)`
  overload + all four consts (INIT/SEND_OFFSETS/COMMIT/ABORT `_TIMEOUT_MSG`) are
  in `kafka_producer.rs`, applied at the four txn call sites (Phase 2 `dfa0090e`).

**ProtocolRoundTripConsistencyTest** (+180, new): skip-with-reason. It
cross-checks the generated serializer against the runtime `Schema`/`Struct`
serializer; Rust has only the generated path (`types.rs` `Schema` is
metadata-only), so the premise is unrepresentable. `AllTypeMessage.json` is a
Java test-only spec not in `generator/test-messages/`.

**make verify on macOS (item 5):** passed exit 0 — format-check, lint, 3781 lib
tests, 182 integration (native Rust), C Unity tests. Docker C/Python multilang
arms self-skip off Linux (expected, not failures). Env override used:
`PIP_INDEX_URL=https://pypi.org/simple/ PIP_EXTRA_INDEX_URL=https://pypi.org/simple/`.
The pre-commit hook (~6 min) runs the same native arms, so committing while a
background `make verify` runs causes Docker port contention — serialize them,
and `docker rm -f $(docker ps -aq --filter name=kafka-)` clears stale
"Created" containers that cause "address already in use" flakes.
