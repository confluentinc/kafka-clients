# Phase 4a Review — Critic N=0

## Round 2 verdict: APPROVED

Round 2 covered fixup commits `e109708`, `5d99bf5`, `2673983`, `97d5a12`,
plus bookkeeping commit `184cb19` resolving Issues 1–4 against base
`2739f7f`. Re-ran DoD checks:

- `cargo build --lib` — clean, no warnings.
- `cargo test --lib` — `test result: ok. 628 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.29s` (was 627, +1 from `with_partitions_shares_topic_arc`).
- `cargo xtask format-check` — clean.
- `cargo xtask lint` — clean.
- `cargo xtask check-generated` — clean (199 generated files).

Per-issue verification:

- **Issue 1** (`e109708`): `src/common/topic_id_partition.rs:66-78` — rustdoc block above `impl Display for TopicIdPartition` now explicitly documents the Java `null-N` vs Rust `-N` delta, with concrete worked example (`vDiRhkpVQgmtSLnsAZx7lA:null-1` vs `vDiRhkpVQgmtSLnsAZx7lA:-1`) and a flag for future readers that this is the only place to fix if Java parity is later required. Resolves the Round 1 concern that the test-side comment was invisible to a future Phase 4b/Phase 6 caller.
- **Issue 2** (`5d99bf5`): `grep -nE 'TODO|FIXME|todo!|unimplemented!' src/common/topic_partition.rs` returns empty (CLAUDE.md rule 5 satisfied). Deferral text lives in `design/history/Milestone-1/Phase-4/NOTES.md` under the new "Future perf opportunities" heading and includes the exact line refs (`TopicPartition.java:28,46-54`, `Node.java:35`), the candidate implementations (`OnceLock<u64>` vs `Cell<Option<u64>>`), and the trigger condition (post producer wire-up benchmark).
- **Issue 3** (`2673983`): The Round 1 suggestion to make `bootstrap` the hostname-preserving Java analogue was followed exactly. `Cluster::bootstrap(&[(String, u16)])` now takes hostnames (line 284) and is the direct analogue of Java's `Cluster.bootstrap(List<InetSocketAddress>)`. The IP-only constructor is now `Cluster::bootstrap_with_addresses(&[SocketAddr])` (line 319) with a strong rustdoc warning ("**Warning — this loses hostnames.**", lines 307-318) cross-referencing `bootstrap` as the Java analogue. The translated `test_bootstrap` (line 542) still uses `Cluster::bootstrap` and asserts `www.example.com` is preserved verbatim. `grep -rn 'bootstrap_with_hosts' src/` is empty — no stale callers of the old name.
- **Issue 4** (`97d5a12`): `Cluster::from_arc_inputs` (line 156) accepts `HashSet<Arc<str>>` and `HashMap<Arc<str>, Uuid>` and forwards them straight into `build` — no `String` round-trip. `Cluster::with_partitions` (line 346) calls `from_arc_inputs` with `self.unauthorized_topics.clone()`, `self.invalid_topics.clone()`, `self.internal_topics.clone()`, and `self.topic_ids.clone()` — all four are `Arc<str>`-keyed, so `.clone()` is a refcount bump only. The new regression test `with_partitions_shares_topic_arc` (line 894) exhaustively snapshots `as_ptr()` for every `Arc<str>`-keyed container (topic_ids key, unauthorized, invalid, internal) on the source side, calls `with_partitions`, and asserts pointer-identity on the result — proving refcount sharing rather than re-allocation. This is a real proof, not an Eq/Hash equality cheat.
- **Bookkeeping** (`184cb19`): `COMMENTS.0.md` now contains only the one-line pointer (verified). `COMMENTS.DONE.0.md` contains all 4 issues each with a `**Resolution:**` paragraph naming the fixup SHA and explaining the chosen approach.

No new regressions. No new findings. Phase 4a is ready for Phase 4b (Metadata stack).

---

All Phase 4a issues resolved; see `COMMENTS.DONE.0.md`.

---

# Phase 4b Review — Round 1

All Phase 4b issues resolved; see `COMMENTS.DONE.0.md`.

---

## Notes for `CLAUDE.md` / agent rules

The Phase-4b skip-list pattern — "covered by another test file" without a 1:1 grep that proves the integration path is also covered — recurred here. Recommend adding to `definition-of-done.md` rule #3 a sub-bullet: *"When a test is deferred on the grounds that another test covers the same behavior, the deferral note must include the exact Java test name and assertion line range that covers each assertion of the deferred test, not just a generic 'covered by FooTest' claim."* (Preserved from Round 1 review for Manager evaluation in a separate pass — do not modify CLAUDE.md or definition-of-done.md without that pass.)
