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

## Not yet implemented (vs. `chaos.py`)

Tracked in
[`design/current/chaos-parity-gap.md`](../../design/current/chaos-parity-gap.md):
manual REPL, on-disk report files (leader-change / metadata / summary logs
built from the Rust client's `log` output), a chaos-until-fail loop with run
archival, dynamic consumer add/remove mid-run, and the idle-based early-drain
exit.
