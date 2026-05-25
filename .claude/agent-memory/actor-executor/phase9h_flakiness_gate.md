---
name: phase9h-flakiness-gate
description: Phase 9h closing patterns — the 3-consecutive-run integration matrix gate, cluster_pool behavior across cargo test invocations, and Milestone-1 close
metadata:
  type: project
---

Phase 9h is the folded-in Phase 8f flakiness gate. Per Phase-9
NOTES.md:53 + :70-:72 the bar is: full integration matrix
(PLAINTEXT 5-test suite from 8a-c + 9c..9g cases 1-5) passes
**3 consecutive times** under `cargo test --features
integration-tests`. Plus a lib-codec audit confirming Phase 3
codec tests still green (no regression from Phase 9 work).

**Why:** This is the explicit catch-flakes phase in the project.
Cold-start, slow leader-election, cert-load latency, and any
test-cross-talk issues are supposed to surface here, not in
Milestone-2. NOTES.md:114 makes it the literal Milestone-1
close criterion.

**The 10-test matrix actually run (`producer_smoke_test::*`):**

1. `producer_smoke_plaintext_1000_records` (8a)
2. `producer_smoke_plaintext_auto_partition` (8b)
3. `producer_smoke_plaintext_byte_fidelity` (8c)
4. `flush_drains_50_records_through_public_api` (8a/8b)
5. `close_flushes_pending_inflight` (8a/8b)
6. `producer_smoke_ssl_1000_records` (9c)
7. `producer_smoke_sasl_plaintext_1000_records` (9d)
8. `producer_smoke_sasl_ssl_1000_records` (9e)
9. `producer_smoke_sasl_plaintext_auth_failure` (9f)
10. `producer_smoke_sasl_ssl_auth_failure` (9f)

9g contributed **zero** integration tests by design (Option A
close). `performance_test::performance_test` is the optional 9i
benchmark — explicitly NOT in the 9h gate; filtered out via the
`producer_smoke_test` test-name prefix.

**Run command (the exact form that worked, save this):**

```
cargo test --features integration-tests --test integration \
  producer_smoke_test -- --test-threads=1
```

The `--test integration` flag is mandatory — there are stale
`integration_*_test` binaries in `target/debug/deps/` from
previous test layouts that `cargo test` will NOT invoke (only
`tests/integration/main.rs` is wired via `mod` declarations
there). Confirm with `cargo test --features integration-tests
--no-run 2>&1 | grep Executable` — should show one entry:
`target/debug/deps/integration-<hash>`.

**Cluster ID determinism.** Across all 30 test executions
(10 tests × 3 runs), the broker reported cluster ID
`5L6g3nShT-eMCtK--X86sw`. This is **NOT** evidence of
`cluster_pool` reuse across `cargo test` invocations — the
`atexit` hook in `cluster_pool.rs:74` tears the container down
at process exit, so each new `cargo test` invocation creates a
fresh container. The cluster ID is deterministic because the
test image (Apache Kafka image pinned in
`kafka_cluster.rs`) starts with the same KRaft metadata each
boot. Same value seen across Phase 9d/9e/9f memory files —
this is the test image's identity, not a warm-state artifact.

**Live timings (2026-05-25, M-series laptop, Docker Desktop):**

| Run | Test-internal | Wall-clock |
|---|---|---|
| 1 | 33.42s | 54s (1st-boot cost; Docker pull cache cold) |
| 2 | 30.03s | 50s |
| 3 | 29.94s | 51s |

Wall-clock includes container startup (~15-20s for KRaft init
+ all 4 listeners up) + Rust test compilation cache check
(~1s, post-first-build).

**Filtered tests:** `1 filtered out` per run — that's the
`performance_test`. Always confirm this number, not just the
pass count: if a test were silently filtered (e.g. someone
added `#[ignore]`), the filtered count would jump.

**Inter-run state.** No manual teardown between runs. The
`atexit` hook is the only cleanup. This is the conservative
stress shape — each run sees a fresh cold-start. If you needed
to test the "warm cluster pool" variant (one cluster reused
across many test files), you would need a single `cargo test`
invocation that mounts multiple test files — not three separate
invocations.

**Lib-codec audit summary (post-Phase-9):**

- `cargo test --lib`: 1343 passed (unchanged through 9d/9e/9f/9g/9h)
- `record::compress`: 12 passed (dispatch + ratio estimator)
- `record::default_record_batch`: 47 passed
- `record::memory_records*`: 53 passed (V0/V1/V2 builders,
  transactional record set, write-past-limit)

No codec regression detected.

**Closes:** Phase 9h. Phase 9 in its entirety (9.0, 9a..9h).
Milestone-1 entirely, per NOTES.md:114.

**Carries forward to Milestone-2 (or later):**

- Phase 9i CCloud env-var-gated performance test
- End-to-end compression matrix integration test (8d carry-forward)
- `kafka_channel.rs:291` Display-prefix cleanup (cosmetic)
- SCRAM / OAUTHBEARER / Kerberos / GSSAPI (Phase 9 explicit skip)
- Re-authentication
- `MockProducer`, `KafkaConsumer`

**Cross-references:**

- [[phase9d-sasl-plaintext-integration]] — original SASL_PLAINTEXT
  test + the 9d Round 2 selector filter fix this 3-run gate
  empirically validates as stable
- [[phase9e-sasl-ssl-integration]] — combined-channel parity
  argument; 9h proves that argument holds under repeat
- [[phase9f-auth-failure-integration]] — auth-failure semantics
  + the `KafkaError::Display` prefix wrinkle
- [[phase9g-option-a-zero-change]] — zero-code-change close
  pattern that 9h mirrors structurally
