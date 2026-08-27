# Critic 47 — Phase M7 (public metrics() API + config wiring) — RESOLVED

## Issue 1: `metrics_returns_full_registry_snapshot` does not verify the "full
registry" / "each manager family" its name and doc-comment claim — RESOLVED

- **File**: `src/consumer/async_kafka_consumer.rs`
- **Severity**: Behavior Mismatch (test strength + doc inaccuracy; NOT a
  production bug)
- **Original problem**: The test's name and doc claimed it verified "a
  representative metric from each manager family," but it asserted only the
  async-consumer family (`background-event-queue-size`) plus a tautological
  `metric_name() == key` loop. Its fixture builds
  `RequestManagers::new(None × 7)`, so the heartbeat / offset-commit /
  rebalance / rebalance-callback managers are never constructed and their
  families never register against the shared `Metrics` — the test could not
  prove their presence even if assertions were added.

- **Resolution** (chose the honest "tighten + state exactly what it proves"
  path, option (b) extended to all three eagerly-registered families):
  - Renamed `metrics_returns_full_registry_snapshot` →
    `metrics_snapshot_includes_eagerly_registered_families`.
  - Rewrote the doc comment to state exactly what it proves: the three
    families that register EAGERLY in this fixture — fetch
    (`create_fetch_metrics_manager`), kafka-consumer
    (`KafkaConsumerMetrics::new`), async-consumer (`AsyncConsumerMetrics::new`)
    — each surface a representative metric in the public `metrics()` snapshot,
    so the registry plumbing reaches the public accessor and the snapshot is
    the live registry, not an empty/partial map.
  - Added two real assertions on top of the existing async-consumer one, all
    on metrics that DO register in this fixture:
    - fetch family: `records-consumed-total`
      (group `consumer-fetch-manager-metrics`)
    - kafka-consumer family: `last-poll-seconds-ago`
      (group `consumer-metrics`)
    - async-consumer family: `background-event-queue-size`
      (group `consumer-metrics`)
  - Documented in the comment that the other four families
    (heartbeat / offset-commit / rebalance / rebalance-callback) register only
    through their request managers (which the fixture omits) and are covered by
    their own M4/M5 manager tests. Explicitly noted we do NOT force-register
    managers the fixture does not build, keeping the test honest.

- **Verification**: `cargo build`, `cargo test --lib` (2123 passed),
  `cargo xtask lint` (clean), `cargo xtask format-check` (clean) — all green.
