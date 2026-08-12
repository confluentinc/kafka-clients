---
name: phase39-integration-commit-callback-notes
description: Phase 39 test-parity — PlaintextConsumerCommit/CallbackTest + ConsumerIntegrationTest failed-listener; Issue 8 reentrancy blocks the callback suite, broker-shutdown harness gap, raw-vs-wrapped listener error deviation
metadata:
  type: project
---

Phase 39 (Actor 39) translated three Java integration files into
`tests/integration/`: `plaintext_consumer_commit_test.rs` (11 tests, all
broker-gated), `plaintext_consumer_callback_test.rs` (9 tests; 1 runs, 8
#[ignore]), and two failed-listener tests appended to `consumer_test.rs`.

**Issue 8 is the dominant constraint for callback reentrancy.** A Rust
`ConsumerRebalanceListener` is `Arc<dyn ...>` with `&self` methods. The
consumer's `assign/position/beginning_offsets/seek/pause/resume` are all
`async fn(&mut self)`. The listener has no consumer handle, and a
channel-to-driver handshake deadlocks because §31 runs the listener on the
SAME task currently blocked inside `poll()`. So the ENTIRE
PlaintextConsumerCallbackTest in-callback-reentrancy surface is
structurally unsupported and #[ignore]d (same gap that already #[ignore]s
the poll-suite's `max_poll_interval_ms_delay_in_revocation`). Only
`testOnPartitionsAssignedCalledWithNewPartitionsOnly` runs — its listener
just reads its `partitions` arg.

**Harness facts that drive SKIP/ignore decisions:**
- Clusters are pooled+shared (`tests/common/cluster_pool.rs`, keyed by
  `ClusterConfig`). NO broker-shutdown API on `KafkaCluster`. Any Java test
  doing `cluster.shutdownBroker()` (e.g.
  testCommitAsyncFailsWhenCoordinatorUnavailableDuringClose) cannot run →
  #[ignore]d; the close-path CommitFailedException message
  ("Failed to commit offsets: Coordinator unknown and consumer is closing")
  is already unit-tested in `commit_request_manager.rs:4217+`.
- `ConsumerError::CommitFailed` flows through `KafkaError::IllegalState`
  (see `src/consumer/errors.rs:237` From impl) — there is NO
  `KafkaError::CommitFailed` variant. Assert via `IllegalState(msg)`.

**Documented Rust deviation (NOT fixed in a test-parity phase):**
`ConsumerRebalanceListenerInvoker` surfaces the listener's RAW error;
Java wraps it as `KafkaException("User rebalance callback throws an error")`
in `AsyncKafkaConsumer.invokeRebalanceCallbacks` (java line 2334). The
always-failed-listener test only asserts that message inside an OPTIONAL
catch arm (Java accepts 0-records-OR-that-error), so the durable invariant
"no record is ever delivered" is what the Rust test asserts. Flagged for a
dedicated review if the wrapper is ever wanted.

**Lint gotcha — STALE as of M11 G2 (2026-08-11).** `cargo xtask lint` now runs a
second `--workspace --all-targets --all-features` pass, and `--all-features`
turns on `integration-tests` / `multilanguage-tests`, so the integration and
performance test targets *are* linted. Verified while landing G2. The rest of
this paragraph still applies if you lint by hand: `clippy --fix` will edit OTHER (tracked)
integration files — `git checkout` them to keep the diff scoped. Common
hits in these files: `committed(&[tp.clone()])` →
`committed(std::slice::from_ref(&tp))` (single-element only; 2-element
slices are fine); `map.get(&x).is_none()` → `!map.contains_key(&x)`.

No Docker in the dev env → all broker tests are compile-only verified;
gating is identical to the existing plaintext_consumer_*_test files (same
CI path).
