# Chaos / Fault-Injection Harness — Producer & KIP-848 Consumer

Status: draft · Owner: Pranav Rathi · Date: 2026-09-03

This document specifies a **chaos / fault-injection test harness** for the Rust
client's **producer** and **KIP-848 (async) consumer**, modeled on librdkafka's
`tests/chaos` harness
(`confluentinc/librdkafka@dev_kip-932_chaos-test-harness`). It stands up a real
multi-broker Kafka cluster, runs produce/consume workloads, injects controlled
broker faults (clean/unclean stop, restart, leader migration, reassignment,
topic delete/recreate), and verifies that no data is lost and duplication stays
within expected bounds.

This is the **fault-injection harness** row that
`release-test-plan-share-consumer.md` §5 lists as a GA blocker, generalized to
the two client surfaces that are actually implemented today. The
share-consumer path (KIP-932) is **explicitly deferred** — see §8.

---

## 1. Motivation & mapping to the librdkafka harness

The librdkafka harness proves that a client survives broker instability without
losing acknowledged/committed records. It is built on **`trivup`** (native
local Kafka processes) and drives workloads via the **`rdkafka_performance`** C
binary, killing broker *processes* with SIGTERM/SIGKILL.

This project's stack is different in two load-bearing ways, so this is a
**re-implementation of the chaos concepts on our stack**, not a port of the
Python/bash:

- Brokers run as **one Docker container per broker** (`apache/kafka:4.2.0`,
  JVM), stood up by `tests/common/kafka_cluster.rs`.
- CLAUDE.md §6 requires **xtask Rust programs, not shell/Python scripts**, and
  workloads are driven **in-process through the Rust client** (we have a
  producer perf driver and a `consumer-perf` crate), not an external binary.

Every capability the librdkafka harness relies on has an equivalent here:

| librdkafka harness (trivup + scripts) | This project (Docker + AdminClient) | Status |
|---|---|---|
| `b.stop(force=True)` → SIGKILL | `docker kill <container-id>` | ✅ |
| `b.stop(force=False)` → SIGTERM | `docker stop <container-id>` (grace period) | ✅ |
| `b.start()` | `docker start <container-id>` (same node id, rejoins quorum) | ✅ |
| `b.wait_operational(timeout)` | re-poll metadata / AdminClient `describeCluster` until node present | ✅ |
| `b.pid()` liveness | `docker inspect -f {{.State.Running}}` | ✅ |
| broker-id → process | `KafkaCluster::container_ids()[node_id-1]` (order = node id) | ✅ |
| `kafka-topics.sh --create/--delete/--describe` | AdminClient `create_topics` / `delete_topics` / `describe_topics` | ✅ |
| `kafka-reassign-partitions.sh --execute/--verify` | AdminClient `alter_partition_reassignments` / `list_partition_reassignments` | ✅ |
| change-leader | AdminClient `elect_leaders(ElectionType, …)` | ✅ |
| `kafka-get-offsets.sh --time -1` | AdminClient `list_offsets(OffsetSpec::Latest)` | ✅ |
| `rdkafka_performance -P` producer workload | in-process `KafkaProducer` workload (§4) | ✅ |
| `rdkafka_performance -G` group-consumer workload | in-process `AsyncKafkaConsumer` workload (§4) | ✅ |
| workload SIGINT graceful stop | in-process `close()`/drop | ✅ |
| `verify.txt` loss/dup bookkeeping | in-process conservation + per-record bookkeeping (§5) | ✅ |
| `leader_changes.txt` | leader-snapshot log via `describe_topics` before/after each op (§5) | ✅ |
| `kafka-configs.sh share.auto.offset.reset` | share-consumer only | ⏸ deferred (§8) |
| `rdkafka_performance -S` share consumer | share-consumer only | ⏸ deferred (§8) |
| `metadata_triggers.txt` (librdkafka debug-log grep) | N/A — different internal logging; optional later (§5) | ⏸ optional |

**Conclusion:** everything the trivup harness did for producer + group-consumer
chaos is reproducible on Docker + our AdminClient. The only genuinely new
infrastructure is broker stop/kill/start control, a **dedicated (non-pooled)
cluster**, and the orchestration + bookkeeping.

---

## 2. Key constraint: a dedicated, non-pooled cluster

`tests/common/cluster_pool.rs` is a **shared, LRU-evicted** pool: tests
requesting the same `ClusterConfig` share one cluster, and idle clusters are
torn down with `docker rm -f` to stay within `TARGET_LIVE_CLUSTERS`.

A chaos harness **must not** use the pool:

- Stopping/killing a broker mutates cluster state other tests depend on.
- Eviction could `docker rm -f` a broker **mid-scenario**.

Therefore the harness constructs its **own** `KafkaCluster` via the existing
public `KafkaCluster::start_with_config(&ClusterConfig)` and owns it for the run.
No pool changes are needed; we simply bypass the pool. Because chaos runs are
slow and Docker-heavy, the harness is **opt-in**: gated behind the
`integration-tests` feature and `#[ignore]` (run explicitly), and/or exposed as
an `xtask chaos` command — never in the default `cargo test` sweep.

---

## 3. Module layout

Following CLAUDE.md §6 (xtask over scripts) and the existing test-infra layout:

```
tests/common/
  broker_control.rs      # NEW: docker stop/kill/start/inspect helpers over KafkaCluster
tests/chaos/             # NEW test binary (own [[test]] target, feature-gated)
  main.rs                # test entry; #[ignore] scenario fns
  harness.rs             # ChaosHarness: owns cluster + admin + workloads + reports
  workload_producer.rs   # in-process producer workload + per-record ledger
  workload_consumer.rs   # in-process KIP-848 consumer workload + per-record ledger
  actions.rs             # ChaosAction enum + executors (roll/change-leader/reassign/topic-chaos)
  report.rs              # conservation, bookkeeping, leader-change reports + verdict
xtask/src/main.rs        # NEW: `xtask chaos [--scenario …]` subcommand -> runs the test target
```

`broker_control.rs` lives in `tests/common/` (not `src/`) because it is
test-only infrastructure — it is **not** a translated Java class and must not
leak into the shipped client (DoD #7: no non-Java structs in `src/`). The chaos
harness likewise is test code, so its orchestration types are exempt from the
"must exist in Java" rule; this is stated here as the §7 justification.

---

## 4. Workloads (in-process, driven through the Rust client)

Two workloads, each a `tokio::spawn`ed task holding a shared **ledger** the
report layer reads after drain.

### 4.1 Producer workload

- One `KafkaProducer<Vec<u8>, Vec<u8>>` (`ByteArraySerializer`), configured with
  a **short-ish delivery timeout and bounded retries** so that a fault surfaces
  as a real send outcome rather than an indefinite hang.
- Sends records with a **monotonic key** = big-endian record index, so every
  record is individually identifiable and the consumer can detect gaps/dups.
- On each send completion (callback / awaited result): record `(index →
  Delivered{partition, offset} | Failed{error})` in the producer ledger. A
  `Failed` outcome is **not** loss — an unacknowledged record was never
  committed and the consumer is not expected to see it. Loss is a record the
  producer recorded `Delivered` that the consumer never observed.
- Rate control mirrors the perf driver's env knobs (reuse the
  `tests/performance/producer_perf_test.rs` conventions: `LIMIT_RPS`,
  `VALUE_SIZE`, …) so the workload is tunable without code changes.

### 4.2 Consumer workload (KIP-848)

- One or more `AsyncKafkaConsumer<Vec<u8>, Vec<u8>>` in the same group
  (`group.protocol=consumer`), subscribed to the chaos topic.
- Poll loop: for each record, record `index → observed{delivery_count?}` in the
  consumer ledger, then **commit** (sync or async per scenario). Duplicates
  (same index seen more than once) are counted, not errors — redelivery is
  expected under churn; the report bounds it, it does not fail on it.
- Adding/removing a consumer mid-run exercises **group rebalance** (the
  `--rebalance-mid-roll` analog).

---

## 5. Chaos actions & reports

### 5.1 `ChaosAction` (actions.rs)

Mirrors the librdkafka mechanisms (README "three leader-change mechanisms"):

| Action | Restarts broker? | Data move? | Implemented via |
|---|:--:|:--:|---|
| `BrokerRoll { clean }` | yes | no | `docker stop`(clean)/`docker kill`(unclean) → wait down → `docker start` → wait operational |
| `ChangeLeader` | no | no | AdminClient `elect_leaders(Preferred/Unclean, partitions)` after nudging preferred replica order |
| `ReassignPartitions` | no | yes | AdminClient `alter_partition_reassignments` → poll `list_partition_reassignments` until empty (the `--verify` analog) |
| `TopicChaos { mode }` | no | maybe | `delete_topics` then optionally `create_topics` (immediate / delayed), matching `--topic-chaos delete/recreate-immediate/recreate-delayed` |
| `LeaveBrokerDown { node, secs }` | yes (stays down) | no | stop without the matching start for `secs` |

Timing knobs from the librdkafka CLI carry over: `warmup`, `pre_roll`,
`stop_s`, `up_s`, `cooldown`, `drain`, `cycles`.

**Broker liveness detection** (the `wait_operational` analog): after `docker
start`, poll AdminClient `describe_cluster` until the node id reappears in the
broker set AND a `describe_topics` on the chaos topic shows the partition
leaders healthy (no `-1` leaders). Bounded by a timeout; a broker that never
returns fails the scenario.

### 5.2 Reports & verdict (report.rs)

Translations of the librdkafka report files, computed from the two ledgers:

- **Conservation** — every producer-`Delivered` index appears ≥1× in the
  consumer ledger. Any `Delivered` index never observed ⇒ **data loss ⇒ FAIL**
  (the `never-consumed` bucket).
- **Per-record bookkeeping** — histogram of consumer observation counts per
  index: `1` (clean), `>1` (redelivered — reported, bounded, not a fail unless
  it exceeds a configured ceiling), `0` for a delivered index (loss, FAIL).
- **Partition coverage** — every partition saw new records during the run
  (guards a silently-dead partition).
- **Leader-change log** — before/after leader snapshots (`describe_topics`)
  bracketing each action, the `leader_changes.txt` analog.

`ChaosVerdict::Pass | Fail(reasons)`; scenarios `assert!(verdict.is_pass())`.

`metadata_triggers.txt` (librdkafka greps its own debug log) has **no direct
analog** — our internal logging differs. Optional later work: a `log` capture
layer counting metadata-refresh reasons. Not in the first cut.

---

## 6. Phasing

1. **Phase 0 — `broker_control.rs`**: `docker stop/kill/start`, `is_running`,
   `wait_operational` (via AdminClient). Unit-smoke against a throwaway
   2-broker cluster (stop → assert down → start → assert back).
2. **Phase 1 — Simple flow (the "test a simple flow" deliverable)**: 3-broker
   cluster, RF=3, one producer + one consumer, a **single `BrokerRoll {clean}`**
   mid-run, conservation + bookkeeping reports, `assert!` no loss. `#[ignore]`,
   `integration-tests`-gated.
3. **Phase 2 — Action matrix**: add `unclean` roll, `ChangeLeader`,
   `ReassignPartitions`, `TopicChaos`, `LeaveBrokerDown`; parameterize timing.
4. **Phase 3 — Consumer-group churn**: add/remove consumer mid-roll (rebalance),
   commit-sync vs commit-async variants.
5. **Phase 4 — `xtask chaos`** command + `chaos_until_fail` loop analog + report
   archival under `target/chaos-runs/<id>/`.
6. **Phase 5 (later)** — share-consumer path once KIP-932 lands (§8).

Each phase: build + test + lint + self-review + commit, per agent-roles.md.

---

## 7. DoD notes specific to this harness

- **DoD #7 (no non-Java structs in `src/`)**: the harness is **test code**
  (`tests/`), so its orchestration types (`ChaosHarness`, `ChaosAction`, …)
  have no Java counterpart by design and do not touch `src/`. Stated here as the
  standing justification.
- **DoD #10 (hot-path alloc audit)**: N/A — chaos actions are administrative,
  not per-record. The per-record ledgers are test bookkeeping, deliberately
  outside the client hot path.
- **DoD #3 (translate Java tests)**: there is no Java test to translate — the
  source is a librdkafka Python harness, not an Apache Kafka JUnit class. These
  are new tests justified by `release-test-plan-share-consumer.md` §4–§5.
- Apache-2.0 header on every new file (CLAUDE.md §7).

---

## 8. Out of scope (this iteration)

- **Share consumer (KIP-932)** workloads and `share.auto.offset.reset` seeding —
  no `src/` implementation exists (`consumer-threading.md` §20). Added in
  Phase 5 when the share consumer lands.
- **trivup / native-process cluster** — we use Docker exclusively.
- **`metadata_triggers.txt`** forensic log grep — optional later.
- Any change to the shared `cluster_pool.rs` — the harness owns its own cluster.
