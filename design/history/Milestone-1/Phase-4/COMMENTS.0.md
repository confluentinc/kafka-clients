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

## Phase 4b Review — Round 2

## Round 2 verdict: APPROVED

Round 2 covered fixup commits `4ec14cd`, `e513cd9`, `0708bc6`, `4f56fbd`,
`8d8fd4a`, `4df52cc`, `8aeeebf`, plus bookkeeping commit `df09aee` and
actor memory note `e6b4858` resolving Issues 5–16 against base `96cf27c`.
Re-ran DoD checks:

- `cargo build --lib` — clean, no warnings.
- `cargo test --lib` — `test result: ok. 684 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.27s` (was 673 at end of Round 1; +11 net = +12 added test functions, −1 duplicate `await_update_returns_after_close_synchronously`).
- `cargo xtask format-check` — clean.
- `cargo xtask lint` — clean.
- `cargo xtask check-generated` — clean (199 generated files).

Per-issue verification:

- **Issue 5 — verified**: `topic_expiry` at `src/producer/internals/producer_metadata.rs:574` covers all three Java phases — idle-window expiry (lines 580-590), in-window re-add keeps topic alive (lines 592-603), late update on freshly-added topic still retains it (lines 605-613). Faithful translation of `ProducerMetadataTest.java:182-213`.
- **Issue 6 — verified**: `concurrent_update_and_fetch_for_snapshot_and_cluster` at `src/metadata.rs:1958` uses 6 `std::thread::spawn` workers + `std::sync::Barrier` (the direct synchronous-Mutex equivalent of Java's `ExecutorService`+`CountDownLatch`). Asserts post-test snapshot/cluster reflect strictly greater node count, partition counts, and leader epoch than the pre-test baseline. Same shape as `MetadataTest.java:1145-1232`.
- **Issue 7 — verified**: All three integration tests present and faithful — `epoch_update_on_changed_topic_ids` at line 1774 (6-phase, including topic-id-change-with-lower-epoch wins), `metadata_merge_on_id_downgrade` at line 1832 (uses `set_retain_topic_fn` to mirror Java's anonymous-subclass override), `topic_metadata_on_update_partition_leadership` at line 1888 (exercises the public `update_partition_leadership` method).
- **Issue 8 — verified**: `metadata_snapshot` is now `ArcSwap<MetadataSnapshot>` on `Metadata` (line 124), out of `MetadataInner`. `fetch()` (line 281) and `fetch_metadata_snapshot()` (line 289) do `metadata_snapshot.load_full()` with no lock acquisition. Writers (`bootstrap` at 442, `update` at 480, `update_partition_leadership` at 567) call `metadata_snapshot.store(...)` from inside the writer lock. `Cargo.toml:53` adds `arc-swap = "1"` (popular crate, ~50M crates.io downloads, satisfies CLAUDE.md rule 1.2). `topic_ids` (414), `topic_names` (419), and other read paths now use the lock-free load. No `MutexGuard` held across `.await`.
- **Issue 9 — verified**: `src/producer/internals/producer_metadata.rs:303-305` replaces the silent `unwrap_or_default()` with `.expect("ProducerMetadata.update received a topic-id-only MetadataResponse; use errorsByTopicId")`, matching Java's `IllegalArgumentException`-on-programming-error semantics at `ProducerMetadata.java:136`. The 16-line comment block above it documents why `expect()` is the chosen behavior (programmer error invariant: producer always operates on name-keyed responses).
- **Issue 10 — verified**: `cluster_listener_notified_on_update_not_on_bootstrap` at `src/metadata.rs:1405` captures the most-recent `on_update` argument via `Arc<Mutex<Option<ClusterResource>>>`, asserts `is_none()` after `bootstrap` (line 1424) and `cluster_id() == Some("dummy")` after `update` (line 1437). Faithful to `MetadataTest.java:299-322`.
- **Issue 11 — verified**: `epoch_update_after_topic_deletion` at line 1722 now has all three phases: empty→topic-id-A epoch 10 (1730-1736), `UNKNOWN_TOPIC_OR_PARTITION` error response keeps last-seen=10 (1738-1754), recreate with topic-id-B lower epoch 5 wins because topic id changed (1756-1766). Matches `MetadataTest.java:388-411`.
- **Issue 12 — verified**: Listener dispatch is now inside the inner-lock scope in both `update` (line 557) and `update_partition_leadership` (line 691). Critically, the snapshot store happens *before* the listener call (lines 523 and 688 respectively), so listeners observe the post-update snapshot. Type-level "Listener constraint" rustdoc at lines 97-113 documents the non-reentrancy adaptation explicitly: listeners must not call back into `Metadata` because `std::sync::Mutex` is not reentrant unlike Java's `synchronized`.
- **Issue 13 — verified**: All four named tests present — `stale_metadata_with_older_epoch_ignored` (line 2076), `request_version_in_flight_bump` (2142), `partial_metadata_update_full_vs_partial` (2169), `metadata_topic_errors_per_topic` (2227).
- **Issue 14 — verified**: ProducerMetadata equivalents present — `metadata_wait_aborted_on_fatal_error` (line 621) for `testMetadataWaitAbortedOnFatalException`, `time_to_next_update_overwrite_backoff` (633) for `testTimeToNextUpdateOverwriteBackoff`, `metadata_partial_update_lifecycle` (666) for `testMetadataPartialUpdate`. Duplicate `await_update_returns_after_close_synchronously` is removed (lines 548-554 carry an inline comment explaining the removal).
- **Issue 15 — verified**: `failed_update_resets_attempts_on_subsequent_success` at line 1544 now does the failed→successful sequence (lines 1565-1573), then a *subsequent* `failed_update` at t=1100 (line 1584), and asserts the resulting `time_to_next_update` is in the **base** `[80, 120]` band (lines 1586-1590). The assertion message explicitly notes that an unreset `attempts=4` would produce a value above 120 — the test would fail loudly on the original Issue 15 weakness.
- **Issue 16 — verified**: rustdoc on `is_invalid_metadata_kafka_error` at `src/metadata.rs:1092-1112` lists all 7 missing variants by name (`FencedLeaderEpoch`, `ReplicaNotAvailable`, `ListenerNotFound`, `ElectionNotNeeded`, `InconsistentTopicId`, `PreferredLeaderNotAvailable`, `EligibleLeadersNotAvailable`) with one-line Java semantics each, and notes the deferral target (Phase 4c or Phase 5 when `KafkaError` gains the variants).

Bookkeeping (`df09aee`): `COMMENTS.0.md` Round 1 section reads `All Phase 4b issues resolved; see COMMENTS.DONE.0.md.` (verified line 33). `COMMENTS.DONE.0.md` contains all 12 Phase 4b issues each with a `**Resolution:**` paragraph naming the fixup SHA and approach. Phase 4a Round 2 verdict (lines 1-23) is untouched.

No new regressions. No new findings. Phase 4b is ready for Phase 4c.

---

## Notes for `CLAUDE.md` / agent rules

The Phase-4b skip-list pattern — "covered by another test file" without a 1:1 grep that proves the integration path is also covered — recurred here. Recommend adding to `definition-of-done.md` rule #3 a sub-bullet: *"When a test is deferred on the grounds that another test covers the same behavior, the deferral note must include the exact Java test name and assertion line range that covers each assertion of the deferred test, not just a generic 'covered by FooTest' claim."* (Preserved from Round 1 review for Manager evaluation in a separate pass — do not modify CLAUDE.md or definition-of-done.md without that pass.)

---

# Phase 4c Review — Round 1

All Phase 4c issues resolved; see `COMMENTS.DONE.0.md`.
