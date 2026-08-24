# Rust Client — Public Preview Performance Test Plan

| | |
|---|---|
| **Status** | Proposal — for review |
| **Last updated** | 2026-08-17 |
| **Scope** | Producer + consumer. Rust vs librdkafka vs Java, plus Python bindings |
| **Goal** | Enough coverage that a customer migrating from librdkafka does not report a performance regression we did not already know about and document |
| **Total** | 76 test cells, 220 runs per architecture, ~11.5 days of continuous running on two boxes |

**How to read this page.** Everything you need to review is above the fold: producer config → producer test cases (core, idempotence, transactions) → consumer config → consumer test cases → metrics, thresholds, and schedule. All supporting rationale — why each decision was made, architecture background, and the correctness risks behind the transaction cells — is collapsed into the appendix at the very end. Expand only what you need.

## Summary

The spike established that the Rust client is at or better than librdkafka and Java on most axes, with one known weak spot (consumer CPU at low partition counts and low rates) and one known cliff (producer gzip). Moving from spike mode to preview requires four things:

1. Absorbing PR #142 (producer idempotence and transactions), which changes the default producer path and therefore invalidates the existing producer baseline.
2. Running a defined, repeatable matrix rather than ad-hoc sweeps.
3. Encoding pass/fail thresholds so overnight runs can be triaged automatically instead of read by hand.
4. Closing the harness gaps that currently make unattended matrix runs impossible. Two of the required test arms do not exist in any form today.

Budget at 1 hour of measurement per run, two client boxes running in parallel (Intel and Graviton):

| Pass | Cells | Runs per arch | Wall clock |
|---|---|---|---|
| Gate A — baseline risk, can run today | 14 | 42 | ~3 days |
| Gate B — idempotence and transactions, after PR #142 merges | 14 | 42 | ~2 days |
| Full Tier 0 — the GA sign-off pass | 76 | 220 | ~11.5 days |

Recommendation: run Gate A now, Gate B the day #142 lands, and the full pass once as the release gate.

**Correctness note, not a performance number.** librdkafka defaults `isolation.level` to `read_committed`; Java and Rust default it to `read_uncommitted`. A customer migrating from librdkafka is, by default, already doing transactional reads — moving to the Rust client silently gives them `read_uncommitted`, and they will begin seeing uncommitted and aborted records they never saw before. This belongs in the migration guide ahead of any performance number; it is called out here because the perf work is what surfaced it. (Owner still needed — see Decisions Needed.)

## Anchor configuration

A full cross-product is unaffordable and mostly uninformative. Every test case below is one sweep off of this single anchor.

| Dimension | Value |
|---|---|
| Client boxes | `m7i.4xlarge` (Intel) and `m8g.4xlarge` (Graviton), same AZ as the cluster |
| Cluster | Reserved CCloud Dedicated, 12 CKU or larger, SASL_SSL |
| Topic | 200 partitions, RF=3 |
| Record | 1 KB value, 16 B key |
| Producer | Regime J, no transactions |
| Consumer | Regime J, `group.protocol=consumer`, auto-commit |
| Run shape | 5 min warmup, then 60 min measurement |
| Arms | Rust, librdkafka, Java — interleaved, not batched |

**The two config regimes**, referenced throughout every table below:

- **Regime J — Java defaults, the tracking baseline, runs on every case.** `enable.idempotence=true`, `acks=all`, `max.in.flight=5`, `linger.ms=5`, `batch.size=16KB`, `check.crcs=true`, `isolation.level=read_uncommitted`. Rust needs no producer overrides and one consumer override (`group.protocol=consumer`); librdkafka needs five.
- **Regime L — librdkafka defaults, the migration view, runs on a subset.** Producer: `enable.idempotence=false`, `max.in.flight=1000000`, `batch.size=1MB`. Consumer: `isolation.level=read_committed`, `check.crcs=false`. This is what a customer sees when they carry their librdkafka config across unchanged.

A case gets both regimes only when the librdkafka knobs could plausibly change the *ranking* between clients, not just the absolute numbers — full rationale in the appendix.

**Reading the cell tables below:** cell IDs are stable — use them when commenting on this page. Each cell runs on 3 clients × 2 architectures = 6 runs, except the Python cells, which name their own backend.

## Producer

### Producer defaults

| Property | Java 4.2 | librdkafka | Rust (post-#142) |
|---|---|---|---|
| `enable.idempotence` | true | **false** | true |
| `acks` | all | -1 (equivalent) | -1 |
| `max.in.flight` | 5 | **1,000,000** | 5 |
| `linger.ms` | 5 | 5 | 5 |
| `batch.size` | 16 KB | **1 MB** | 16 KB |
| `retries` | MAX_INT | MAX_INT | MAX_INT |
| `buffer.memory` | 32 MB | — | 32 MB |
| `compression.type` | none | none | none |

Rust matches Java exactly. Only 3 knobs differ from librdkafka: `enable.idempotence`, `max.in.flight`, `batch.size` — a three-line config snippet, not a vague "defaults differ." *(Verified against the vendored Apache Kafka 4.2 source, librdkafka's published `CONFIGURATION.md`, and this repo's config modules — not from memory.)*

### Producer core — 17 cells, Regime J

| ID | Sweep | Levels | Also Regime L |
|---|---|---|---|
| P1 | Record size at max throughput | 100 B, 1 KB, 10 KB, 100 KB | Yes (4) |
| P2 | Fixed-rate latency | 10k, 100k, 500k msg/s | Yes, at 100k (1) |
| P3 | Partitions | 6, 36 (200 is the anchor) | Yes (2) |
| P4 | Compression | lz4, snappy, zstd, gzip | Yes (4) |
| P5 | acks — note idempotence auto-disables | 1, 0 | No |
| P7 | Producer instances per box | 4 (1 is the anchor) | Yes (1) |
| P8 | Partition scaling at max throughput | 1000 partitions | No |

*P4/gzip is a known cliff — rationale in the appendix.*

### Idempotence isolation — 4 cells, Regime J

| ID | Cell | Pairs with |
|---|---|---|
| I1 | Idempotence off at 200 partitions | Anchor |
| I2 | Idempotence off at 36 partitions | P3 |
| I3 | Idempotence off at 6 partitions | P3 |
| I4 | Idempotence off at 1000 partitions | P8 |

Deliverable: a delta table of idempotence cost per client, not four standalone numbers. Deliberately not run in Regime L (full rationale in the appendix).

### Transactions — 9 cells, Regime J only

Regime L is inapplicable by definition, since transactions require idempotence.

| ID | Sweep | Levels |
|---|---|---|
| T1 | Records per transaction | 100, 1k, 10k, 100k |
| T2 | Partitions touched per transaction | 1, 36 (200 is the T1 anchor) |
| T3 | Concurrent transactional producers, distinct transactional IDs | 16 (1 is the anchor) |
| T4 | Abort rate | 50% (0% is the anchor) |
| T5 | EOS pipeline: consume, transform, produce, `send_offsets_to_transaction` | 200 partitions |

Two-phase commit (KIP-939) is deferred to Tier 1. *Per-cell rationale, including the concurrency risk areas these cells exist to catch, is in the appendix ("Transactions — rationale & correctness risk areas").*

## Consumer

### Consumer defaults

| Property | Java 4.2 | librdkafka | Rust |
|---|---|---|---|
| `fetch.min.bytes` | 1 | 1 | 1 |
| `fetch.max.wait.ms` | 500 | 500 | 500 |
| `fetch.max.bytes` | 50 MB | 50 MB | 50 MB |
| `max.partition.fetch.bytes` | 1 MB | 1 MB | 1 MB |
| `max.poll.records` | 500 | no equivalent | 500 |
| `enable.auto.commit` | true | true | true |
| `auto.commit.interval.ms` | 5000 | 5000 | 5000 |
| `auto.offset.reset` | latest | largest (equivalent) | latest |
| `session.timeout.ms` | 45000 | 45000 | 45000 |
| `check.crcs` | **true** | **false** | true |
| `isolation.level` | **read_uncommitted** | **read_committed** | read_uncommitted |
| Prefetch queue | none | **`queued.min.messages=100000`, `queued.max.messages.kbytes=64 MB`** | none |

Rust matches Java exactly. Notes: Rust requires `group.protocol=consumer` (classic is unsupported, so Rust's stock default config needs one override the other two clients don't); `check.crcs` and `isolation.level` are pinned explicitly on every arm rather than inherited. *Full notes, including why librdkafka's prefetch queue can't be equalized, are in the appendix.*

### Consumer core — 17 cells, Regime J

| ID | Sweep | Levels | Also Regime L |
|---|---|---|---|
| C1 | Partitions | 6, 36, 200 | Yes (3) |
| C2 | Record size | 100 B, 10 KB | No |
| C3 | Rate sweep | 50, 300 MB/s (150 is the anchor) | Yes, at 50 (1) |
| C4 | Compression on the decompress path | gzip, zstd | No |
| C5 | Consumers in the group | 4, 16 | No |
| C6 | Fetch tuning | `fetch.min.bytes=100KB`; `max.poll.records=5000` | No |
| C7 | Commit mode | manual sync per batch, manual async | No |
| C8 | Cold join and rebalance | 1 to 4 to 1 members at 200 partitions | No |
| C9 | Backlog catch-up | `auto.offset.reset=earliest`, drain 100 GB | No |

*C1 at 6 partitions is the known consumer CPU weak spot; C5/C8/C9 are unmeasured and are where a regression is most likely to surface (C8 in particular — suspected cold-join stall). Full rationale, including the spike's numbers, is in the appendix.*

### Consumer read_committed — 6 cells, Regime J

The consumer already implements `isolation.level=read_committed` and aborted-batch filtering, and none of it has been performance-tested. Given that `read_committed` is librdkafka's default, this group is must-run rather than optional.

| ID | Cell | What it measures |
|---|---|---|
| RC1 | `read_committed` versus `read_uncommitted` at the anchor | Baseline cost of LSO-bounded fetch. Run both directions on all three clients |
| RC2a | `read_committed`, producer commits every 100 records | End-to-end latency when the LSO advances often |
| RC2b | `read_committed`, commits every 10k records | Middle of the range |
| RC2c | `read_committed`, commits every 100k records | End-to-end latency when the LSO advances rarely |
| RC3 | `read_committed` at 50% abort rate | Per-record abort-filter cost |
| RC4 | `read_committed` at 6 partitions | LSO stall interacting with the known low-partition weakness |

*RC2 has an important gating caveat — see the appendix before reporting these numbers.*

### Cross-cutting — 3 cells, Regime J

| ID | Test | Detail |
|---|---|---|
| X1 | 8-hour soak at the anchor rate, idempotent producer | RSS slope, latency drift, error and retry counts, epoch bumps |
| X2 | Broker rolling restart mid-run, with a transactional producer | Recovery time, no stall, no loss or duplication |
| X3 | PLAINTEXT versus SASL_SSL | Isolates rustls cost from client cost |

*X2 is as much a robustness test as a performance test — rationale in the appendix.*

### Python bindings — 4 cells, Regime J

Producer and consumer, sync and async, Rust backend, at the anchor. Only if Python ships with preview.

### Regime L subset — 16 cells

| Source | Cells |
|---|---|
| P1 record size | 4 |
| P2 at 100k msg/s | 1 |
| P3 partitions 6 and 36 | 2 |
| P4 compression | 4 |
| P7 four producer instances | 1 |
| C1 partitions 6, 36, 200 | 3 |
| C3 at 50 MB/s | 1 |

*The P1/P3 Regime L numbers are the migration story — see the appendix.*

## Metrics

Identical across all arms.

| Metric | Notes |
|---|---|
| Throughput | msg/s and MiB/s, sustained across the measurement window |
| End-to-end latency | p50, p99, p999, max |
| Producer ack latency | send to ack, split into queue time and request latency |
| CPU | Percent of one core, from the `/proc` sampler. Not the `sysinfo` self-report, which reads 0% on aarch64 |
| RSS | Absolute, plus slope across the run for leak detection |
| Efficiency | msg/s per 1% CPU, and MiB/s per core. This is the metric that decides whether something is a regression |
| Errors | Error count, retry count, rebalance count |
| Java only | GC pause time and count |

Transaction and idempotence runs additionally report:

| Metric | Notes |
|---|---|
| Transactions per second | The headline for the T cells |
| Commit latency | `begin_transaction` to commit ack, p50 and p99 |
| Abort latency | p50 and p99, on T4 |
| Coordinator RPC counts per transaction | InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, TxnOffsetCommit, EndTxn. The AddPartitionsToTxn count is how TV1 versus TV2 behaviour is verified |
| Epoch bumps | Should be zero in steady state. Non-zero means retries are firing |
| Sequence-tracking memory | RSS delta against the idempotence-off comparator, at 200 and 1000 partitions |
| LSO lag | `read_committed` only: how far the last stable offset trails the log end |

Two rules that affect every number above:

- **Coordinated omission.** The harness must timestamp at the intended send time, not the actual one. Otherwise the fixed-rate arms under-report tail latency exactly when the client falls behind, which is the case we most want to catch.
- **Memory comparability.** RSS is only comparable at matched sustained throughput. Any cell where an arm fails to hold the target rate has its RSS marked non-comparable rather than tabulated.

## Pass and fail thresholds

These need to be agreed before the first pass, not after. Without them, unattended runs produce data nobody can triage. All gates are against the librdkafka arm within the same regime.

| Metric | Gate |
|---|---|
| Throughput | At least 0.95x librdkafka |
| End-to-end latency p99 | At most 1.20x librdkafka |
| CPU efficiency | At least 0.85x librdkafka, with a documented exception for the 36-partition-and-below low-rate cells |
| Cost of idempotence (I cells) | Rust's on/off delta at most 1.25x the librdkafka and Java deltas |
| Transactions per second | At least 0.90x librdkafka at matched records per transaction |
| Commit latency p99 | At most 1.25x librdkafka at matched records per transaction |
| `read_committed` p99 latency | At most 1.20x librdkafka at the same producer commit interval. Never against an absolute target |
| RSS slope across the 8-hour soak | Approximately zero |
| Epoch bumps in steady state | Zero |
| Unexpected errors | Zero |

A cell that breaches automatically re-runs three times before it is reported. Cloud noise otherwise generates a steady stream of false alarms.

## Schedule

**Interleave the arms; do not batch them.** Run cell by cell as Rust, then librdkafka, then Java — not all-Rust followed by all-librdkafka — and randomise cell order across nights. **Wall clock is cluster-bound, not box-bound**: a 12-CKU Dedicated cluster serves roughly 600 MB/s of ingress, so two concurrent 300 MB/s runs already saturate it; more boxes only helps the low-rate cells. Run hygiene: pin the client process with `taskset` or a cgroup, fix the CPU governor, and record instance ID, kernel version, client commit SHA, and negotiated transaction version in every result record. *Full rationale for all of the above is in the appendix.*

### Budget

| Group | Cells | Runs per arch |
|---|---|---|
| Producer core | 17 | 51 |
| Idempotence isolation | 4 | 12 |
| Transactions | 9 | 27 |
| Consumer core | 17 | 51 |
| Consumer read_committed | 6 | 18 |
| Cross-cutting | 3 | 9 |
| Python bindings | 4 | 4 |
| Regime L subset | 16 | 48 |
| **Total** | **76** | **220** |

At 1 hour of measurement plus roughly 10 minutes of warmup, setup and teardown, that is about 278 hours per architecture including the soak. Two boxes in parallel gives roughly 11.5 days of continuous running.

### Gate A — can run today, about 3 days

The highest-risk-per-hour selection from the work that does not depend on PR #142.

| Cells | Rationale |
|---|---|
| P1 (4) | Headline throughput versus record size. Needs re-running post-#142, but establishes the harness and the noise floor |
| P4 gzip and zstd (2) | The known producer cliff |
| C1 (3) | The known consumer CPU weak spot |
| C5 (2) | Unmeasured, high customer relevance |
| C8 (1) | Suspected cold-join regression |
| C9 (1) | Unmeasured, high customer relevance |
| X1 (1) | Soak and leak detection |

### Gate B — the day PR #142 lands, about 2 days

Chosen to cover every risk identified in the appendix's PR #142 impact section at least once.

| Cells | Risk covered |
|---|---|
| I1, I2, I4 (3) | Cost of idempotence at 200, 36 and 1000 partitions — the nested-lock delta |
| T1 (4) | Records-per-transaction sweep: the headline, and where guard churn shows |
| T5 (1) | EOS pipeline, the deployed workload |
| RC1, RC3 (2) | `read_committed` baseline and abort-filter cost |
| P1 in Regime L (4) | The migration number, available early |

## Harness gaps to close first

The existing tooling is close but cannot run a matrix unattended.

| # | Gap | Notes |
|---|---|---|
| 1 | `tools/deploy_and_run_perf/deploy_and_run_perf.py` runs one test per invocation with a hand-written `.env` | Needs a matrix driver on top: generate the `.env` per cell, run, collect, append to one results store, advance |
| 2 | Consumer librdkafka and Java arms exist only as ad-hoc shell scripts | Promote the `*_drive.sh` scripts under `consumer-perf/cloud-benchmarks/2026-06-18-use1-intel/` to first-class `--test` targets |
| 3 | **No transactional producer arm exists in any harness** | The C perf test already has `ENABLE_IDEMPOTENCE` and `MAX_IN_FLIGHT`, but nothing for `transactional.id`, records per transaction, or abort rate. Needed on all three arms. `examples/txn_eos_pipeline.rs` and `examples/txn_producer.rs` from #142 are the starting point for the Rust arm |
| 4 | **No `read_committed` consumer arm** | `consumer-perf` does not plumb `isolation.level` and cannot report LSO lag |
| 5 | `consumer-perf` does not plumb `fetch.min.bytes`, `max.poll.records`, commit mode, or consumer count | The README claims these need config builders that are "not yet exposed". That is stale — `ConsumerConfig::from_properties` already handles all of them. This is a CLI plumbing gap, not a client gap |
| 6 | CPU sampling | Always use the `/proc` sampler. The `sysinfo` self-report reads 0% on aarch64 |
| 7 | No comparator | Read the run's summary JSONL, diff against a committed baseline, exit non-zero on threshold breach, emit a one-page digest. This is what makes "trigger overnight, read results in the morning" work |
| 8 | Coordinated omission | Emit the intended send timestamp |

Items 3 and 4 are new work created by PR #142 and are on the critical path for Gate B. Neither exists in any form today.

## Decisions needed

1. **Confirm the PR #142 merge date.** Thirty of the 76 cells are blocked on it, and Gate B is scheduled against it.
2. **Who builds harness items 3 and 4** — the transactional producer arm and the `read_committed` consumer arm? Both are Gate B critical path and neither exists today.
3. **Approve the pass/fail thresholds.** They are the definition of "no regression". The three transaction and idempotence gates are the ones most worth arguing about, since there is no prior art to calibrate them against.
4. **Is the Python binding in preview scope?** Determines whether the PY cells run, and whether they need transactional coverage or whether the Rust-level result suffices.
5. **Can the reserved cluster's `transaction.version` be flipped** for the TV1 versus TV2 comparison, or do we record and move on?
6. **Is a second reserved cluster available** for the broker-count isolation test in Tier 1?
7. **Who owns the migration-guide entry** for the `read_committed` to `read_uncommitted` semantics change? This is not a perf deliverable but it was found by this work and is arguably higher-impact than any number in this plan.

---

## Appendix — rationale, architecture background, and open risk areas

Everything above is enough to review the plan. What follows is the *why* behind it: the reasoning for each decision, the architecture background, and — for the transaction and consumer cells especially — the specific correctness and concurrency risks each test case exists to catch.

<details>
<summary><b>PR #142 impact, in full — what changes and why it invalidates the old baseline</b></summary>

PR #142 (`milestone11-producer-transactions`, open and mergeable) lands the full producer transaction stack: idempotent sends with epoch bumping and sequence tracking, the transactional state machine with all six protocol handlers (InitProducerId, FindCoordinator, AddPartitionsToTxn, AddOffsetsToTxn, TxnOffsetCommit, EndTxn), the public API, KIP-890 TV2, the KIP-939 two-phase-commit surface, and the MockProducer equivalents.

The line that matters for performance: **`enable.idempotence` now defaults to `true`, matching Java.**

Three consequences:

1. **Every producer number from the spike is off-baseline.** The default send path gains producer-ID acquisition, per-partition sequence assignment, and in-flight batch tracking. Producer cells must be re-run after #142 merges. This is a re-baseline, not an addition — do not mix pre-#142 and post-#142 producer results in one table.
2. **The acks sweep is no longer a clean single-factor sweep.** Java-faithfully, `acks=1` or `acks=0` silently disables idempotence unless the user asked for it explicitly, and `max.in.flight > 5` is a hard error when idempotence is on. The acks cells therefore measure acks and idempotence together. Label them that way; there is no valid `acks=1` plus idempotent configuration to compare against.
3. **TV1 versus TV2 is not under client control.** KIP-890 transaction version is negotiated from the broker or cluster `transaction.version`, not a client config. If the reserved cluster can be flipped, run one cell each. If not, record which version was negotiated in every result record so the numbers stay interpretable.

**Where the performance risk actually is.** The design rules in `.claude/rules/producer-transactions.md` identify the risk precisely, which lets us target cells rather than guess:

| Risk | Mechanism | Cells that test it |
|---|---|---|
| Nested lock on the drain path | Sequence assignment runs inside `deque` then `TransactionManager`, in that order, per batch. Java gets this free from `synchronized` plus thread confinement; Rust takes two real locks | P3, P7, I1–I3 |
| Shared manager mutex | `Arc<Mutex<TransactionManager>>` shared between `KafkaProducer`, `Sender` and `RecordAccumulator` | P7, T3 |
| Guard churn around awaits | The guard cannot be held across `.await`, so `maybe_send_and_poll_transactional_request` acquires and drops it repeatedly where Java holds one region. Cost is per transaction request, not per record | T1 |
| Per-partition transaction state | `TxnPartitionMap` memory and lookup scaling with partition count | P8, I4 |
| Abort filtering on the receive path | `read_committed` does a per-record check against an `aborted_producer_ids` set plus a heap of aborted transactions, and fetching is bounded by the last stable offset | RC1, RC3 |

</details>

<details>
<summary><b>Producer and consumer config notes, in full</b></summary>

All defaults were verified against the vendored Apache Kafka 4.2 source in `kafka/`, librdkafka's published `CONFIGURATION.md`, and this repository's config modules. They are not from memory.

**Producer.** Rust matches Java exactly on every producer property. The librdkafka divergence reduces to exactly three knobs: `enable.idempotence`, `max.in.flight`, and `batch.size`. That makes the migration story explainable as a three-line config snippet rather than a vague "defaults differ".

**Consumer.** Rust matches Java exactly here too. Three notes:

- **`group.protocol`** defaults to `classic` in all three clients, but Rust only implements `consumer` (KIP-848) — `new_consumer` rejects classic. So Rust's stock default config is currently unusable and needs one override that the other two clients do not. Consumer arms for librdkafka and Java must also be run with `group.protocol=consumer` for the comparison to be meaningful, since otherwise we are comparing two different rebalance and fetch-session designs. One classic-protocol librdkafka arm at the anchor is worth running separately, labelled "what the customer is leaving".
- **`check.crcs`** was handled deliberately in the spike — the drive scripts set it explicitly and there is a full CRC matrix, with the finding that Rust's CRC costs +1–4% while librdkafka's costs +3–19%. Keep it explicit in both regimes rather than inherited.
- **`isolation.level`** appears nowhere in any spike drive script or result log, so the librdkafka consumer arm ran `read_committed` while Rust and Java ran `read_uncommitted`. In practice this was immaterial: the feed was non-transactional, so the last stable offset equalled the high watermark, the aborted-transactions list was empty, and `read_committed` cost essentially nothing. The spike numbers stand. It becomes material the moment the feed is transactional, which is exactly the RC cells — so every consumer cell must pin `isolation.level` explicitly on all three arms.

</details>

<details>
<summary><b>What Regime L cannot equalize — librdkafka's prefetch queue</b></summary>

librdkafka maintains a per-partition prefetch queue (`queued.min.messages=100000`, `queued.max.messages.kbytes=64 MB`) topped up by broker threads independently of the application's consume rate. **There is no Java or Rust knob that matches this.** Rust mirrors Java's fetch architecture exactly — `abstract_fetch.rs` tracks `nodes_with_pending_fetch_requests` and permits at most one in-flight fetch per broker. The available knobs (`fetch.min.bytes`, `fetch.max.bytes`, `max.partition.fetch.bytes`) change how much arrives per fetch, never how many fetches are in flight.

This is architectural and Java-faithful, so it is not something to fix. Two practical consequences:

- **Equalize batch shape instead**, which is what the spike did: raise `fetch.min.bytes` and co-vary `max.poll.records` so records-per-poll match across arms. That equalizes per-record CPU, batch traversal, and per-poll overhead — the axis that dominates the CPU and throughput comparison.
- **Report memory only at matched sustained throughput.** All three clients held 150k msg/s (293 MiB/s) in the spike; Rust did it on 29–57 MB RSS, librdkafka on 99–105 MB, Java on 645–767 MB. The Rust-versus-Java comparison is fully apples-to-apples — Java uses the same one-fetch-per-broker design with no deep prefetch queue, so nothing about prefetch explains that gap. The Rust-versus-librdkafka result is also a genuine win, stated as "same throughput, one third of the resident memory", since librdkafka needs the deeper queue to reach that rate and Rust does not. The only unsupportable claim would be that librdkafka wastes memory on identical buffering; it buffers more deliberately.

</details>

<details>
<summary><b>Test-design methodology — anchor config and the both-regimes rule</b></summary>

A full cross-product is unaffordable and mostly uninformative. Define one anchor configuration; every sweep rotates exactly one knob off it.

**Which cases get both regimes.** A case earns both regimes only if the librdkafka knobs plausibly change the ranking between clients, not merely the absolute numbers. If flipping the config moves all three arms in the same direction by roughly the same amount, one regime tells you everything and the second is wasted hours.

Because Regime J is idempotent and Regime L is not, running a cell in both regimes is itself the cost-of-idempotence measurement — bundled with the in-flight cap and batch size, which is the honest bundle, because those three move together in a real migration. Cells I1–I4 exist separately to isolate idempotence as a single factor.

</details>

<details>
<summary><b>Producer core cells — detailed rationale (the gzip cliff)</b></summary>

P4 and gzip is non-negotiable. The spike found the Rust producer collapsing to roughly 31k msg/s on gzip because `miniz_oxide` deflate is slow single-threaded. Preview must either fix it or ship a documented "do not use gzip on the producer" note with numbers behind it. Note that Regime J's 16 KB batches will make every compression arm look worse than the spike's 1 MB batches; the Regime L arm is how those numbers reconcile.

</details>

<details>
<summary><b>Idempotence isolation cells — detailed rationale</b></summary>

These are `enable.idempotence=false` with Java defaults otherwise, so they isolate idempotence as a single factor. They are deliberately not run in Regime L, which would confound the very thing they exist to separate.

The deliverable is a delta table — the cost of idempotence per client — not four standalone numbers. The question it answers is whether Rust pays more for idempotence than Java and librdkafka do. That is the migration-relevant figure, not the absolute.

</details>

<details>
<summary><b>Transactions cells — rationale and correctness risk areas</b></summary>

Regime L is inapplicable by definition, since transactions require idempotence. Transaction throughput is dominated by transaction shape, not record shape.

**T1** is the headline. Commit is two coordinator round trips plus a broker write, amortised over the transaction, so at 100 records per transaction the guard-churn cost shows, and at 100k it should converge on the idempotent-only number.

**T2** isolates AddPartitionsToTxn. Under TV1 that is an explicit RPC per new partition set per transaction, so 1-partition versus 200-partition transactions at the same record count differ in RPC count rather than data volume. Under TV2 the difference should largely vanish, which also makes this the cheapest available check that TV2 is actually in effect.

**T5** is the workload customers actually deploy. `examples/txn_eos_pipeline.rs` from PR #142 already has this shape; promote it to a measured arm rather than writing a new one.

Two-phase commit (KIP-939) is deferred to Tier 1 — a real surface, but not a preview-blocking workload.

These cells double as a targeted concurrency-risk sweep — see the PR #142 impact section above ("nested lock on the drain path", "shared manager mutex", "guard churn around awaits") for the specific mechanisms P3/P7/T1/T3/I1–I3 are designed to catch. If any of the pass/fail gates on the T cells breach, that risk table is the first place to look for *why*, not just *that*.

</details>

<details>
<summary><b>Consumer core cells — detailed rationale (known weak spot and suspected regressions)</b></summary>

**C1** is the strongest case in the plan for running both regimes. `check.crcs` is where the spike already showed the ranking moving: at `crcs=true` Rust looks better, and at `crcs=false` librdkafka closes much of the gap. Since librdkafka defaults it off and Java and Rust default it on, the two regimes give genuinely different answers to "who is more CPU-efficient".

C1 at 6 partitions is the known weak cell. The spike root-caused it: one Selector reads across all broker connections per cycle, so cost scales with cycle rate times connection count rather than with data volume, whereas librdkafka is thread-per-broker. Matched-CRC on Graviton the ratio was 1.19x at 300 MB/s widening to 1.59x at 50 MB/s. Do not re-litigate the design — thread-per-broker would violate the single-Selector mandate in `CLAUDE.md`. Quantify it precisely at preview config and write the documented carve-out.

**C4** is cut from five codecs to two. The spike covered all five thoroughly with a written-up result: Rust was the most CPU-efficient consumer for every codec, and librdkafka's gzip decompress unexpectedly costs about two cores. No need to re-derive that, only to guard it.

**C6** must co-vary `max.poll.records` with `fetch.min.bytes` so records-per-poll stay comparable across arms, as the spike did. Run as a single-knob sweep it produces a batch-shape mismatch rather than a fetch-tuning result.

**C5, C8 and C9** are entirely unmeasured today and are where I would most expect something to surface. On C8, `consumer-perf/README.md` notes that KIP-848 cold join "can take tens of seconds". If that holds at preview config it is a customer-visible regression against librdkafka, and `design/current/consumer-join-stall-rootcause.md` shows this path has bitten before.

</details>

<details>
<summary><b>read_committed cells — the RC2 gating caveat</b></summary>

RC2 needs a caveat carried into the report. `read_committed` end-to-end latency is gated by the producer's commit interval, not by consumer code — records are invisible until the transaction commits. RC2a through RC2c should show latency rising roughly with commit interval for all three clients. Gate it against librdkafka at the same commit interval, never against an absolute latency target, or someone will file physics as a Rust regression.

Run RC3 and T4 against the same stream where the harness allows it, so the abort-rate data is generated once.

</details>

<details>
<summary><b>Cross-cutting cells — detailed rationale</b></summary>

X2 with transactions enabled is worth calling out. Coordinator failover mid-transaction exercises FindCoordinator retry, epoch bumping, and the `TransactionalRequestResult` latch semantics under exactly the conditions where a cancellation or lock-ordering bug would surface. It is as much a robustness test as a performance test.

A Regime L soak is defensible but costs about 25 hours for one cell, so it is excluded. Add it only if the Regime J soak shows something.

</details>

<details>
<summary><b>Python bindings cells — rationale</b></summary>

Only if Python ships with preview. Producer and consumer, sync and async, Rust backend, at the anchor. Per-call binding overhead dominates at this layer, so the anchor establishes it and producer config is noise. The `confluent-kafka` backend comparison and the record-size points move to Tier 1.

</details>

<details>
<summary><b>Regime L subset — why these 16 cells and what they prove</b></summary>

The P1 and P3 Regime L results are the migration story. If Rust in Regime J is slower than librdkafka in Regime L — which is likely, since Regime L is the structurally faster configuration — that is the number a customer will report as "the Rust client regressed", and the Regime L Rust arm is the evidence that the gap is defaults rather than engine. Both numbers plus the three-knob opt-out need to be in the preview documentation before anyone hits it.

</details>

<details>
<summary><b>Run hygiene and scheduling — full detail</b></summary>

**Interleave the arms; do not batch them.** Run cell by cell as Rust, then librdkafka, then Java — not all-Rust followed by all-librdkafka. CCloud neighbour load drifts across a night, and batching by client aliases that drift directly onto the client comparison. Randomise cell order across nights as well.

**Wall clock is cluster-bound, not box-bound.** A 12-CKU Dedicated cluster serves roughly 600 MB/s of ingress, so about two concurrent 300 MB/s runs saturate it. Adding client boxes beyond two does not compress the schedule for the high-rate cells; that needs a larger cluster or a second one. Low-rate cells such as P2 at 10k msg/s and C3 at 50 MB/s can be packed more densely, which is worth exploiting in the scheduler. This is why the gates matter more than the full pass.

Other run hygiene: pin the client process with `taskset` or a cgroup, fix the CPU governor, and record instance ID, kernel version, client commit SHA, and negotiated transaction version in every result record.

</details>

<details>
<summary><b>Tier 1 — deferred scope, if Tier 0 lands clean</b></summary>

- Two-phase commit (KIP-939) transaction throughput
- TV1 versus TV2 explicitly, if the cluster's `transaction.version` can be flipped
- librdkafka with `queued.max.messages.kbytes` lowered to Rust's effective buffer bound. If it then cannot hold the rate, that is direct evidence the Rust and Java fetch design reaches the same throughput on less memory — a strong, cheap datapoint for the preview documentation
- Transaction timeout and long-open-transaction behaviour, and its effect on downstream `read_committed` consumers
- `send_offsets_to_transaction` at high consumer-group counts
- Cross-AZ and cross-region client placement
- 100 topics of 2 partitions versus 1 topic of 200 partitions, exercising the metadata path
- Header-heavy records
- `buffer.memory` exhaustion and send-path backpressure behaviour
- Static membership
- Small cluster versus large cluster at fixed partition count, isolating broker connection count from partition count. Needs a second reserved cluster
- Consumer group churn and repeated rebalance storms
- C binding overhead versus native Rust
- Mixed producer and consumer on one box, exercising tokio runtime contention
- Python `confluent-kafka` backend comparison and record-size points

</details>
