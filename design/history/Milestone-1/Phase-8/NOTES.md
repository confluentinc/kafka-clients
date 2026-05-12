# Phase 8 — Integration Test (Milestone-1 closer)

**Goal:** Real-broker round-trip. `KafkaProducer` (Phases 1–7) sends N
records to a Testcontainers Kafka 4.2 broker, asserts every record is
acked, and verifies delivery via a consumer. PLAINTEXT and SSL/TLS
variants. **No SASL** (Phase 9).

**Plan reference:** `design/history/Milestone-1/PLAN.md` lines 336–349.

## Sub-phase ladder

| # | Scope | Test entry point |
|---|---|---|
| 8.0 | **Prerequisite (planning gap surfaced by Actor 8 before 8a):** translate `NetworkClient.DefaultMetadataUpdater` (Java inner class, `NetworkClient.java:1174–1380`) as a top-level `DefaultMetadataUpdater` struct in `src/default_metadata_updater.rs` implementing the existing `MetadataUpdater` trait; promote `KafkaProducer::new` / `with_serializers` from `Err(UnsupportedOperation)` stubs to working public constructors that wire `NetworkClient<Selector, DefaultMetadataUpdater>`. Phase 7 deferred this to Phase 8 (see `src/producer/kafka_producer.rs:286–352` rustdoc + `Phase-7/COMMENTS.DONE.7.md:951–985`). | unit tests only |
| 8a.0 | **Wire-protocol blocker surfaced by Actor 8's first 8a attempt:** the Rust producer connects, sends an `ApiVersionsRequest`, never receives a usable `ApiVersionsResponse`, times out at 30s, loops until `max.block.ms=60s` fires. Topic creation, TCP connect, and broker health all verified. PLAN.md risk #1 ("wire-protocol byte-vector divergence from Java") materialized. Scope: diagnose, fix, and add a Java-captured hex-fixture regression so the same gap can't reopen. Also formally adopts Actor's `45b70a2` visibility fix (`DefaultMetadataUpdater` / `SupportsDefaultSerializer` `pub(crate) → pub + #[doc(hidden)]`) since `tests/integration/*` is a separate downstream crate, contradicting Phase-8.0 Round-2 archive premise. | hex-fixture round-trip + live broker re-run of 8a scaffold |
| 8a.1 | **Perf-test re-enable (user-requested, parallel to 8a Critic).** `tests/integration/performance_test.rs` was muted in Phase 1 cleanup; re-wires it into `tests/integration/main.rs` so PLAINTEXT perf-numbers can be captured against a local Testcontainer broker today. Production scope: promote `KafkaProducer::from_config` `pub(crate) → pub + #[doc(hidden)]` (same visibility-correction pattern Phase 8a.0 applied to `DefaultMetadataUpdater` / `SupportsDefaultSerializer`). Test scope: uncomment the `mod performance_test;` line + one short smoke-run to confirm it produces successfully. Does not gate 8a-8f; runs in parallel. | `cargo test --features integration-tests performance_test --release` smoke-run |
| 8a | `producer_smoke_test.rs` scaffold + PLAINTEXT 1k-record happy path; single broker, 3 partitions; assert acks + RecordMetadata partition/offset shape | `tests/integration/producer_smoke_test.rs` |
| 8b | Per-partition monotonic-offset assertion + partitioner consistency (record's selected partition == acked partition); multi-partition (≥3). Folds in the Phase-7f 50-record `flush()` fidelity carry-over. | extend 8a |
| 8c | End-to-end byte fidelity: consume produced batch via `kafka-console-consumer` (`docker exec`) and assert key/value bytes match per partition | extend 8a |
| 8d | Compression matrix: rerun 1k-record happy path with `compression.type ∈ {gzip, snappy, lz4, zstd}` | extend 8a (parameterized) |
| 8e | TLS variant: same flow over SSL listener using `tests/common/test_certs.rs`-generated certs; broker container's 9096 SSL listener | `tests/integration/producer_smoke_test.rs` (TLS case) |
| 8f | Flakiness gate: run 8a–e **3 consecutive times** under `cargo test --features integration-tests`. Address flakes (cold-start, slow leader-election). Re-enable `performance_test.rs` as compile-only gate (no perf assertions). | CI loop check |

Each sub-phase ends green on
`cargo build --features integration-tests && cargo test --features integration-tests producer_smoke && cargo xtask format-check && cargo xtask lint`.

## Approved phase-level decisions (Manager + user)

1. **Verification path:** `kafka-console-consumer` invoked via `docker
   exec` on the same Testcontainers broker. A pure-Rust Fetch-only
   consumer is out of Milestone-1 scope.
2. **Topic creation:** pre-create the test topic via a one-shot
   `docker exec kafka-topics --create` in the test harness. Do **not**
   rely on `auto.create.topics.enable` — it races against the
   producer's first `MetadataRequest`.
3. **Broker count:** single broker per test. Multi-broker scenarios
   exercise leader-election + reconnect, already unit-tested in
   Phases 5/6. Defer multi-broker integration to a future milestone.
4. **TLS scope:** one happy-path TLS test only. Hostname/CN-matching
   edge cases are unit-tested in Phase 5.
5. **Compression deps:** all four (`gzip`/`snappy`/`lz4`/`zstd`) are
   already in `Cargo.toml` from Phase 3. If a missing crate is
   discovered during 8d, Actor 8 may add it without further Manager
   approval (Phase 3 already vetted them in principle).

## Skip list (rejected or deferred)

- **SASL** (PLAIN, SCRAM, OAUTHBEARER, Kerberos) — Phase 9
- **Pure-Rust consumer** — out of Milestone-1
- **`KafkaConsumer` translation** — future milestone
- **`MockProducer`** — out of milestone
- **Performance assertions** — compile-only gate this phase; thresholds
  in a future perf-tuning milestone
- **Multi-broker cluster integration** — Phase 9 / later
- **TLS edge cases (hostname mismatch, expired cert, untrusted CA)** —
  unit-tested in Phase 5
- **`AdminClient` translation** — use `docker exec kafka-topics` from
  the test harness

## Phase-7 carry-overs retired here

These were tagged "Phase 8" in `COMMENTS.DONE.7.md`:

1. **`metadata.close()` lift point** — the graceful-close arm that
   closes `metadata` then `network` is currently untested. 8a's happy
   path hits it implicitly; add one explicit "close after pending
   in-flight" assertion.
2. **50-record `flush()` fidelity** — 8b's per-partition multi-record
   drain exercises `flush` through `producer.send()`, not the
   accumulator-direct shortcut Phase 7e was forced into. No separate
   task — fold into 8b assertions.

## DoD additions on top of `definition-of-done.md`

1. `cargo build --features integration-tests` clean.
2. `cargo test --features integration-tests producer_smoke` green **3
   consecutive runs**.
3. Each sub-phase test asserts:
   - **Ack count:** exactly N `RecordMetadata` returned, no `Err` on
     any send future.
   - **Partition consistency:** the partition the partitioner picked
     at `send()` time equals the partition in the returned
     `RecordMetadata`.
   - **Per-partition monotonic offsets:** offsets within a partition
     are strictly increasing.
   - **End-to-end byte fidelity:** the consumer sees the same key /
     value bytes the test produced (8c onward).
4. No new `String` clone, no `Box<dyn Future>` per send, no
   per-message `tokio::spawn` introduced in production code by this
   phase (`grep` audit before closing).
5. `cargo xtask format-check` + `cargo xtask lint` green on every
   commit.

## Comment files

- Open: `COMMENTS.8.md`
- Resolved: `COMMENTS.DONE.8.md`

## Workflow (Manager loop)

1. ✅ Plan approved (user, 2026-05-11).
2. ✅ This NOTES.md created.
3. ✅ Phase 8.0 prerequisite added after Actor 8's first attempt
   surfaced the `DefaultMetadataUpdater` planning gap (user approved
   2026-05-11).
4. Spawn **Actor 8** for sub-phase 8.0 (`DefaultMetadataUpdater` +
   public ctor wiring).
5. Spawn **Critic 8** to review 8.0 commits → comments to
   `COMMENTS.8.md`.
6. Loop fixup-and-review until `COMMENTS.8.md` is empty for 8.0,
   then proceed to 8a (PLAINTEXT scaffold + happy path).
7. Repeat steps 4–6 for 8b through 8f.
8. Phase 8 closes when 8f's 3-consecutive-run gate is green and all
   comment files are resolved.

Agent number for this phase: **N = 8**.
