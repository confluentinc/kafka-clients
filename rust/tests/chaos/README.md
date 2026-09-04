# Chaos / Fault-Injection Test Harness

## Overview

A single-command harness that stresses the Rust client's **producer** and
**KIP-848 consumer** under broker instability. It stands up a real,
dedicated Kafka cluster, runs continuous produce/consume workloads, injects
controlled broker faults (clean/unclean stop, rolling restart, leader
migration, partition reassignment, topic delete/recreate), and verifies that
no acknowledged record is lost.

It is modeled on librdkafka's `tests/chaos/chaos.py`, re-implemented on this
project's stack: Docker containers instead of `trivup`, the AdminClient
instead of `kafka-*.sh`, the in-process Rust client instead of an external
`rdkafka_performance` binary, and a Rust `xtask` command instead of a Python
script (CLAUDE.md §6 — xtask over scripts). The full design and a running
parity checklist against `chaos.py` live in
[`design/current/chaos-fault-injection-harness.md`](../../design/current/chaos-fault-injection-harness.md)
and
[`design/current/chaos-parity-gap.md`](../../design/current/chaos-parity-gap.md).

The share consumer (KIP-932) is **out of scope** until it lands in `src/`
(`consumer-threading.md` §20); the harness is designed so it plugs in later
without changing the orchestrator — see [Extending the harness](#extending-the-harness).

## Architecture

- **Orchestrator** (`harness.rs`): owns a **dedicated, non-pooled**
  `KafkaCluster` and an AdminClient, injects faults, and drives the run. It
  does *only* chaos — it never references `KafkaProducer`/`Consumer` directly.
  A dedicated cluster is mandatory: chaos stops/kills brokers, which would
  corrupt the shared `cluster_pool` other integration tests use.
- **Broker control** (`../common/broker_control.rs`): `docker stop` (clean,
  SIGTERM), `docker kill` (unclean, SIGKILL), `docker start`, and a
  metadata-based "broker is operational again" wait — the Docker analog of
  trivup's `broker.stop(force=…)` / `broker.start()` / `wait_operational()`.
- **Workloads** (`workload.rs`): pluggable produce/consume units (see
  [Workloads](#workloads-pluggable)).
- **Verification** (`verifier.rs`): pluggable event recording + verdict (see
  [Event format & verification](#event-format--verification)).
- **Runner** (`run_test.rs`, `config.rs`): the flag-driven entry point.

## Operating mode

**Auto mode** (the only mode today) runs the full workflow: provision cluster
→ create topic → start workloads → warm up → *N* chaos cycles → cooldown →
drain → verdict → tear down. An interactive **manual REPL** (like `chaos.py
--manual`) is not yet implemented.

## Chaos scenarios & leader-change mechanisms

One fault type per run, chosen by `--action`:

| Action | Restarts brokers? | Data movement? | Mechanism |
|---|:---:|:---:|---|
| `broker-roll` (default) | yes | no | `docker stop`/`kill` each broker in turn, then `docker start` and wait until it rejoins the quorum |
| `change-leader` | no | no | AdminClient preferred-leader election (`elect_leaders`) |
| `reassign-partitions` | no | yes | AdminClient `alter_partition_reassignments` (rotate replicas), then poll until complete |
| `topic-recreate` | no | yes (destroys the topic) | AdminClient delete → wait-absent → optional dwell → recreate |

`broker-roll` accepts `--unclean` (SIGKILL instead of SIGTERM), rolls a
**seeded, reproducible order** each cycle (`--seed`), and can keep one broker
permanently down for the whole run (`--leave-broker-down N`).

## Workloads (pluggable)

The workload is a swappable plug-in, decoupled from the chaos logic — the
analog of `chaos.py` spawning any workload binary. Each `--workload` is
`role:backend`, repeatable to run several at once (including mixed backends,
e.g. a Rust producer feeding a C consumer):

- **roles**: `producer`, `consumer`
- **backends**: `rust` (in-process, async-native), `python` (sync binding),
  `python-async` (asyncio binding), `c` (C FFI). The non-Rust backends run
  the real binding in its own container over the gRPC bridge; requesting one
  makes `xtask` build with `--features multilanguage-tests` automatically
  (their gRPC-server Docker images must be built first — see
  [Dependencies](#dependencies)).

Consumers commit with `--commit sync|async`.

## How verification works (any producer/consumer mix)

The producer and consumer are **independent workloads that never talk to each
other**. Correctness does not depend on how many there are or which bindings
they use — it comes from a **stable record identity** and a **single shared
verifier**:

1. The **producer** stamps a monotonic logical `index` into each record's key
   and, on each broker ack, emits `Delivered { index, topic_id, partition,
   offset }` into the shared verifier.
2. The **consumer** recovers `index` from the key it reads back and emits
   `Consumed { index, topic_id, partition, offset }` into the *same* verifier.
3. The verifier joins them **by `index`**: conservation = every `Delivered`
   index must appear in `observed`. The join key is carried in the record's own
   payload, so a Rust producer + a C consumer (or several consumers, sync or
   async) verify identically — no cross-workload channel is needed.

```
producer:rust  ──Delivered{index=42,…}──┐
                                         ├─▶ Verifier (delivered/observed by index)
consumer:c     ──Consumed{index=42,…} ──┘        every delivered index observed?
```

The orchestrator's only coupling between producer and consumer is **lifecycle
ordering** at cooldown: it stops producers first, waits `--drain-s` for
consumers to catch up on the tail, then stops consumers — so a consumer never
reports false loss for records still in flight.

Verification is **dual-keyed** (see [Event format](#event-format--verification)):
`index` for conservation and logical dedup, and the physical
`(topic_id, partition, offset)` for offset-space anomalies `index` cannot see.

## CLI flags

Run via `cargo xtask chaos …`. Defaults mirror `chaos.py` where they overlap.

### Cluster
- `--brokers N` (3) — broker count
- `--partitions N` (6) — partitions on the chaos topic (RF = min(brokers, 3))
- `--topic NAME` (`chaos-run`)

### Workload
- `--workload role:backend` (`producer:rust`, `consumer:rust`) — repeatable
- `--rps N` (200) — producer target records/sec, `0` = max rate
- `--commit sync|async` (`sync`)

### Chaos
- `--action broker-roll|change-leader|reassign-partitions|topic-recreate` (`broker-roll`)
- `--cycles N` (3) — number of chaos cycles
- `--unclean` — SIGKILL instead of SIGTERM (broker-roll)
- `--stop-s N` (5) — seconds a broker stays down per roll
- `--up-wait-s N` (60) — max seconds to wait for a broker to rejoin
- `--leave-broker-down N` — keep broker N down for the whole run
- `--seed N` (0) — deterministic broker-roll order
- `--dwell-s N` (0) — delete→recreate dwell (topic-recreate)

### Timing
- `--warmup-s N` (5) — traffic before the first fault
- `--between-s N` (3) — pause between cycles
- `--drain-s N` (15) — time for consumers to catch up at cooldown

### Alternate entry point
- `--scenario NAME` — run a named `#[ignore]` smoke test instead of the flag
  runner (e.g. `--scenario simple_flow_clean_broker_roll`).

## Event format & verification

Workloads do not print records; they emit typed `WorkloadEvent`s into a
`Verifier` (the in-process analog of `chaos.py`'s line-delimited JSON events).
Every record carries **two identities**:

- **`index`** — a monotonic logical id the producer stamps into the message
  key and the consumer reads back. Stable across retries, reassignment, and
  topic recreate; the basis of the conservation check. (librdkafka has no
  such id — its perf tool embeds none.)
- **`(topic_id, partition, offset)`** — the broker's physical address, which
  catches anomalies `index` cannot: a duplicate written at a *new* offset, and
  topic-recreate generation collisions (offset resets to 0 after a recreate,
  so `topic_id` disambiguates old vs. new — `topic_id` is re-resolved after
  each recreate).

The default `ConservationVerifier` renders a verdict:

```
=== Chaos verdict: PASS ===
  delivered (acked) records : 1657
  failed sends (not loss)   : 0      # unacked sends — never committed, not loss
  duplicates (by index)     : 0      # redelivery of the same logical record
  duplicates (by offset)    : 0      # same physical (topic_id, partition, offset) seen twice
  partitions covered        : 6
  expected-lost (recreate)  : 0      # records legitimately destroyed by topic-recreate
  lost (delivered, unseen)  : 0      # acked-but-never-consumed  → FAIL if > 0
```

The run **fails** if any acknowledged record is never consumed
(`lost > 0`), or if partition coverage is below the expected minimum.
Duplicates are reported, not failures — redelivery is expected under churn.

## What a run prints

Everything goes to the test's **stderr** as human-readable progress (there are
no on-disk report files yet — see [Not yet implemented](#not-yet-implemented-vs-chaospy)).
A full run prints, in order:

```
chaos: brokers=3 partitions=6 cycles=1 action=ReassignPartitions unclean=false rps=120 workloads=[producer-rust-1, consumer-rust-1]
chaos: starting workload producer-rust-1
chaos: cycle 1/1
chaos:   p0 leader Some(2)->Some(3)  replicas [2, 3, 1]->[3, 1, 2]     # per-action effect proof
...
chaos: reassignment complete for topic chaos-run (6 partition(s) with changed replicas, 6 with changed leader)
=== Chaos verdict: PASS ===
  delivered (acked) records : 362
  failed sends (not loss)   : 0
  duplicates (by index)     : 0
  duplicates (by offset)    : 0
  partitions covered        : 6
  lost (delivered, unseen)  : 0
test result: ok. 1 passed; 0 failed; ...
```

Lines fall into three groups:

- **Run header** — the resolved config and the workload list.
- **Chaos progress + effect proof** — per cycle and per action: which broker
  rolled, the before→after `leader`/`replicas` per partition (reassign /
  change-leader), or the `topic_id` change (recreate), and an
  `N changed`-style summary that the action **asserts** on (a no-op fails the
  run).
- **Verdict** — the conservation + dual-key bookkeeping and the pass/fail.

Add `--nocapture` (the `xtask chaos` path already sets it) to see these live;
without it libtest buffers them until the test ends.

### vs. librdkafka's `chaos.py`

| librdkafka | Ours |
|---|---|
| Per-record JSON event lines on the workload's **stdout** (`{"e":"consumed",…}`), parsed by the orchestrator | No per-record printing — workloads emit typed events **in-process** to the verifier; only the aggregate verdict is printed |
| Client debug logs on the workload's **stderr**, greppable for signatures | Not captured yet (would come from the Rust `log` facade — a parity gap) |
| Report **files**: `verify.txt`, `summary.txt`, `leader_changes.txt`, `metadata-trigger.txt`, per-consumer stdout/stderr/stats, with rotation + budget | A single stderr verdict + progress; no files, no rotation yet |
| Leader-change log written per cycle to `leader_changes.txt` | The same before→after leader/replica diff, printed inline to stderr |
| Redelivery via a `delivery_count` (`dc`) field | Redelivery via `duplicates (by index)`; `dc` is share-consumer-specific (future) |

So the **information** overlaps (conservation, per-record identity, leader
diffs, redelivery), but librdkafka serializes it to files across processes
while we keep it in-process and print an aggregate. On-disk reports and Rust
client-log capture are the main remaining output gaps.

## Quick start

```bash
# Default: 3 brokers, 3 clean broker rolls, Rust producer + consumer
cargo xtask chaos

# Unclean (SIGKILL) rolls
cargo xtask chaos --unclean

# Leader migration with data movement
cargo xtask chaos --action reassign-partitions

# Delete/recreate the topic mid-run, with a 3s dwell
cargo xtask chaos --action topic-recreate --dwell-s 3

# Cross-binding: Rust producer feeding a C consumer (builds the gRPC image path)
cargo xtask chaos --workload producer:rust --workload consumer:c

# Async Python consumer, async commits
cargo xtask chaos --workload producer:rust --workload consumer:python-async --commit async

# Keep broker 2 permanently down while rolling the rest, reproducibly
cargo xtask chaos --leave-broker-down 2 --seed 7 --cycles 5

# Run the named smoke test
cargo xtask chaos --scenario simple_flow_clean_broker_roll
```

## Dependencies

- **Docker** running (the harness manages `kafka-*` containers on a private
  network; image `apache/kafka:4.2.0`).
- For `python` / `python-async` / `c` workloads, the gRPC-server images:
  `make build-grpc-images-c`, `make build-grpc-images-python` (built once;
  reused across runs).

The harness is excluded from `cargo test` (`test = false` in `Cargo.toml`
plus `#[ignore]` on every scenario) because it is slow and destructive to its
own cluster. The equivalent raw invocation is:

```bash
cargo test --features integration-tests --test chaos -- --ignored --nocapture --exact run_test::chaos_run
```

## Extending the harness

- **New verification** (e.g. share-consumer acks): add an `impl Verifier`
  (a `WorkloadEvent` variant already exists for `Acked` / `DeliveryCount`)
  and pass it to `ChaosHarness::start_with_verifier`. The orchestrator is
  unchanged.
- **New workload** (e.g. a transactional producer, or the share consumer):
  add a `Backend`/`Role`, implement the `Workload` trait, and wire it in
  `build_workload`. Chaos actions, timing, and the verdict are untouched.
- **Share consumer (future)**: a `ShareConsumerWorkload` emitting
  `Consumed`/`Acked`/`DeliveryCount` plus a `ShareAckVerifier`, selected by
  `--workload share-consumer:rust`. See the design doc §8.

## Reports (`--reports`)

With `--reports`, each run writes a directory under
`target/chaos-runs/<run-id>/` (the observability analog of librdkafka's report
files):

- `verdict.txt` — the persisted conservation / dual-key verdict.
- `leader-changes.txt` — timestamped before→after leader/replica diffs per
  action (the `leader_changes.txt` analog).
- `client-<clientId>.log` — the **captured Rust client `log` output, split per
  workload** (a process-global `log::Log` routes each record to its
  workload's file by the `clientId=` in the line; lines with no client id fall
  back to `client.log`). Each file rotates at `--log-budget-mb` (default 64,
  one backup kept) — the per-consumer-stderr + `--log-budget-bytes` analog.
- `summary.txt` — counts of known diagnostic signatures grepped from
  `client.log` (transport disconnects, metadata refreshes, leader-change /
  not-coordinator errors, timeouts, retries — the `summary.txt` /
  `metadata-trigger.txt` analog).

Files are written **before** the pass/fail assertion, so a failing run still
leaves full diagnostics on disk.

## Rebalance chaos & until-fail loop

- **Dynamic consumer add/remove** (`--rebalance-add-cycle N` /
  `--rebalance-remove-cycle N`): add or remove a consumer at the start of cycle
  N, forcing a group rebalance mid-run while other faults proceed.
- **Until-fail loop** (`--repeat N`): run up to N times, stop on the first
  failure, appending one TSV line per iteration to
  `target/chaos-runs/run-history.tsv` (the `chaos_until_fail.sh` analog).

## Not yet implemented (vs. `chaos.py`)

Tracked in
[`design/current/chaos-parity-gap.md`](../../design/current/chaos-parity-gap.md):
the interactive manual REPL, the idle-based early-drain exit, and the
share consumer (KIP-932, blocked on the client — §20).
