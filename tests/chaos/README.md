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

Broker rolling is the default fault; the other faults are layered on with named
flags (see [CLI flags](#cli-flags)), each with an optional per-N-cycle cadence,
so several can compose in one run:

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
- `--num-topics N` (1) — number of test topics. With `>1`, `--topic` is used as a
  **prefix** and topics are named `<topic>_0`, `<topic>_1`, …, `<topic>_{N-1}`
  (librdkafka's naming). One producer runs **per topic** (each at `--rps`, so the
  aggregate rate is `N × rps`); consumers subscribe to **all** topics. Leader
  migrations (change-leader/reassign) iterate every topic per librdkafka.

  > **Known limitation — multi-topic + topic-recreate is not yet supported.**
  > `--num-topics > 1` together with `--topic-recreate` is **rejected** up front
  > with a clear error (clean exit, not a mid-run panic); `--random` under
  > multi-topic silently **drops** topic-recreate from its candidate set (broker
  > roll / change-leader / reassign still randomize across topics) and prints a
  > notice. The reason is a genuine consumer offset-reset gap, not a harness
  > artifact: a new-generation (new topic-id) recreate leaves the KIP-848
  > consumer positioned past the new generation's tail **without** an offset
  > reset, so that topic's post-recreate tail is genuinely unconsumed. Diagnosed
  > and deferred as a separate consumer-side investigation. Single-topic recreate
  > (`--num-topics 1 --topic-recreate`) and multi-topic without recreate both
  > work and are covered.
- `--partitions N` (6) — partitions on each chaos topic
- `--replication-factor N` — replication factor per topic; default `min(brokers,
  3)`. FATAL (panics) if `N > brokers`, matching librdkafka.
- `--topic NAME` (`chaos-run`) — topic name (or prefix when `--num-topics > 1`)

### Workload
- `--workload role:backend` (`producer:rust`, `consumer:rust`) — repeatable
- `--rps N` (1000) — producer target records/sec (per topic), `0` = max rate
- `--msg-size N` (100) — producer value payload size in bytes. The 8-byte logical
  index is written into the first bytes of the value and padded to this size; the
  key stays the 8-byte index (logical identity is preserved even for
  `--msg-size < 8`).
- `--commit sync|async` (`sync`)

### Security protocol
- `--security-protocol plaintext|ssl|sasl_plaintext|sasl_ssl` (`plaintext`) —
  the broker listener every producer / consumer workload connects through.
  Every broker the harness starts exposes all four listeners at once (the same
  `KafkaCluster` the integration suite uses), so this only selects which one
  the clients under test use; the faults are unchanged. The harness's own admin
  client (create/delete topic, elect leaders, reassign) stays on PLAINTEXT on
  this branch, because its `KafkaAdminClient` does not yet take security config
  (see the comment in `harness.rs`).
  - `ssl` — one-way TLS. The client trusts the cluster's generated CA
    (`ssl.truststore.certificates` as PEM); hostname verification is off, as in
    the integration suite (`INTEGRATION_TEST_PROTOCOL=ssl`).
  - `sasl_plaintext` — SASL/PLAIN over plain TCP as `admin` / `admin-secret`.
  - `sasl_ssl` — SASL/PLAIN over TLS (both of the above).
  - **Rust workloads only.** The gRPC (python / c) backends run in a sibling
    container and reach the broker through its container-network listener,
    which on this branch exists for PLAINTEXT only; a secured run with a gRPC
    workload is rejected up front with a clear error.
  ```
  cargo xtask chaos --security-protocol sasl_ssl --cycles 3 --reports
  ```
  The run header prints `security=SASL_SSL` and the listener addresses the
  workloads use.

### Rebalance listener contract
Every Rust consumer workload registers a `ConsumerRebalanceListener`. Each
callback is recorded and, in `on_partitions_revoked`, the listener commits
through a `ConsumerHandle` the way a real application flushes offsets before
its partitions move. The verifier replays the callbacks into a per-consumer
ownership model and **fails the run** on any of:

- `on_partitions_assigned` for a partition the consumer already owns (a second
  assigned with no revoked/lost in between);
- `on_partitions_revoked` / `on_partitions_lost` for a partition the consumer
  does not own;
- `on_partitions_revoked` for a partition another consumer had already been
  assigned while this one still owned it. The broker moves a partition without
  waiting for its owner only when the owner is fenced, and a fenced member must
  be told `on_partitions_lost`, so a clean revocation here is a handoff that
  did not happen;
- `close()` returning while the consumer still owns partitions (Java's
  `runRebalanceCallbacksOnClose` releases the whole assignment first).

The verdict prints the callback counts:

```
  rebalance callbacks       : revoked=7 assigned=9 lost=0 (2 consumer(s) with listener)
```

All zeros means no consumer had a listener — the gRPC (python / c) backends
cannot carry one across the bridge — so the contract was not exercised on that
run. A commit failing inside `on_partitions_revoked` is reported under
`commit errors in revoked` and in the error breakdown, not scored, like the
other consumer errors. Broker rolls, consumer churn and the `--rebalance-*`
flags are what provoke the callbacks.

### Committed offsets
A commit is the consumer telling the broker "I have processed up to here"; the
next owner of the partition (after a crash, a rebalance or a restart) resumes
from it. A commit that runs *ahead* of what was really consumed loses records
on that handoff; one that falls *behind* re-delivers them. Conservation only
catches this indirectly, and only if a handoff happens to occur, so each Rust
consumer reads its committed offsets back from the broker (`committed()`) at
the points where they matter, and the verifier compares them with that
consumer's own consumption since it was assigned the partition:

- right after the commit inside `on_partitions_revoked` (what the next owner
  will resume from) — except for the revoke that `close()` itself fires: once
  closing, the client only drains commits, so an offset fetch from inside that
  callback would wait out `default.api.timeout.ms` (Java behaves the same);
- after the final commit before `close()`, which covers that last handoff;
- every 5 s after a successful post-poll commit, in sync-commit mode only (an
  async commit may not have reached the broker yet, so its read-back would
  prove nothing).

Each read-back happens right after a successful `commit_sync` with no poll in
between, so the committed offset must be exactly *last consumed + 1*. Any other
value **fails the run**:

```
FAIL: committed offsets: 1 violation(s) (sample: ["consumer-rust-1: committed offset 50 on chaos-run-0 is ahead of its own consumption (last consumed 41, expected 42)"])
```

Partitions the consumer has not consumed from since it was (re)assigned are
skipped: their committed offset is the previous owner's. The verdict shows how
many comparisons actually ran:

```
  committed offsets checked : 37 (0 violation(s))
```

Zero means the check never ran (gRPC consumers have no rebalance listener, so
their progress-since-assignment is unknown and they are not checked). A
failing `committed()` call itself is reported as
`consumer committed() read-back` in the error breakdown, not scored.

### Chaos
- **Broker rolling is the default fault** — no flag needed. Layer additional
  faults on with the flags below; each takes an **optional cadence `N`** (every
  N cycles; omitted = every cycle). Compose freely, e.g.
  `--topic-recreate 2 --reassign-partitions 3`.
- `--no-broker-roll` — disable the implicit broker roll (e.g. a pure
  leader-migration run with `--change-leader`)
- `--topic-recreate [N]` — also delete/recreate the topic
- `--reassign-partitions [N]` — also reassign partitions (data moves)
- `--change-leader [N]` — also do a preferred-leader change (no data move)
- `--cycles N` (3) — number of chaos cycles
- `--unclean` — SIGKILL instead of SIGTERM (broker-roll)
- `--stop-s N` (5) — seconds a broker stays down per roll
- `--up-wait-s N` (60) — max seconds to wait for a broker to rejoin
- `--leave-broker-down N` — keep broker N down for the whole run
- `--seed N` (0) — reproducibility seed; `0` = auto-pick and print it
- `--dwell-s N` (0) — delete→recreate dwell (topic-recreate)

#### Random (chaos-monkey) mode
- `--random` — instead of the fixed per-fault cadences, each cycle the seeded
  RNG decides **whether** a fault fires, **which** one (broker-roll,
  topic-recreate, reassign-partitions, change-leader — all candidates), and its
  **parameters** (broker index, clean/unclean, down duration, dwell), plus a
  random pre-action delay within the cycle. Any action, any time.
- `--action-prob P` (0.7) — per-cycle probability that a fault fires (`--random`).
- **Reproducible**: the entire random run is driven by one RNG seeded from
  `--seed`. Rerun with the printed seed to replay it byte-for-byte:
  ```
  cargo xtask chaos --random --cycles 20 --rps 1000 \
      --workload producer:rust --workload consumer:rust --reports
  # → "RANDOM mode — reproduce this exact run with --seed 8153…"
  # replay:
  cargo xtask chaos --random --seed 8153… --cycles 20 --rps 1000 \
      --workload producer:rust --workload consumer:rust
  ```

### Rebalance chaos
- `--rebalance-add-cycle N` — add a consumer at the start of cycle N (rebalance)
- `--rebalance-remove-cycle N` — remove that consumer at the start of cycle N
- `--rebalance-mid-roll` — fire the add/remove **inside the broker-roll
  down-window** instead of at the top of the cycle, so the group reassignment is
  in flight *while a broker is down and leaders are migrating* — a leader change
  and an assignment change around the same time (librdkafka's
  `--rebalance-mid-roll`). Needs a broker roll to fire on that cycle (the default
  unless `--no-broker-roll`); with no roll that cycle it silently falls back to
  the top-of-cycle placement. Give `--stop-s` enough room (≥ the rebalance
  settle time, e.g. `--stop-s 8`) so the reassignment actually overlaps the
  down-window rather than finishing after the broker is back.

### Timing
- `--warmup-s N` (5) — traffic before the first fault
- `--between-s N` (3) — pause between cycles
- `--drain-s N` (15) — **max** time for consumers to catch up at cooldown
- `--idle-threshold-s N` (3) — end the drain early once consumption has been
  quiet this long (0 = always wait the full `--drain-s`). Keeps runs that end on
  a heavy fault (reassign/recreate) from falsely reporting loss, without a large
  fixed drain. A topic recreate **arms a settle**: the early-drain will not
  declare quiescence until the consumer has demonstrably resumed after the most
  recent recreate (a post-recreate consume-progress advance), so the transient
  re-discovery / truncation-rewind stall right after a recreate is never mistaken
  for "caught up" and the just-recreated topic's tail is not dropped as false
  loss. Still capped by `--drain-s`.

### Reports & loop
- `--reports` — write `target/chaos-runs/<id>/` (verdict, leader changes,
  per-workload client logs, signature summary)
- `--log-budget-mb N` (64) — per-workload client-log rotation budget
- `--repeat N` (1) — run up to N times, stop on first failure, append
  `target/chaos-runs/run-history.tsv`

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
  so `topic_id` disambiguates old vs. new). The client does not expose the
  topic id on an ack or a consumed record, so the harness keeps a live
  name → id map: a recreate switches it to the new id **from the create-topics
  response**, before the producer can even learn the new leaders, and the
  producer stamps each record with the id current when its **ack arrives**.
  So a record written to the new generation always carries the new id, and
  the destroyed-generation excusal cannot hide loss on the new generation.

The default `ConservationVerifier` renders a verdict:

```
=== Chaos verdict: PASS ===
  delivered (acked) records : 1657
  failed sends              : 0      # sends rejected or unacked within the delivery timeout → FAIL if > 0
  duplicates (by index)     : 0      # redelivery of the same logical record
  duplicates (by offset)    : 0      # same physical (topic_id, partition, offset) seen twice
  duplicates (double write) : 0      # same logical record at 2+ offsets of one generation → FAIL if > 0
  partitions covered        : 6
  in-flight peak (producer) : 37     # most records one producer had in flight at once (reported)
  expected-lost (recreate)  : 0      # records legitimately destroyed by topic-recreate
  lost (delivered, unseen)  : 0      # acked-but-never-consumed  → FAIL if > 0
  lost by partition:                 # only when lost > 0: where the loss sits
    chaos-run p0: 143 lost at offsets 1..=143; last acked offset 143, last consumed offset none
  consumer poll errors      : 0      # poll() calls that returned an error (reported)
  consumer commit errors    : 7      # commit calls that returned an error (reported)
  errors by kind:                    # every client-reported error, grouped, most frequent first
    7x consumer commit: IllegalStateError: OffsetCommit failed with stale member epoch. ...
```

The last block is the **error summary**: every error a workload received from
the client (failed sends, consumer `poll` errors, consumer commit errors) is
recorded with its text and rendered grouped by operation and message, with a
count per distinct message. Failed sends fail the run (below); consumer errors
are reported only, because the consumer loop retries them and the conservation
check decides whether they had any effect. The block is omitted when no error
occurred. Each error is also printed on stderr as it happens, prefixed with the
workload label, so it can be placed against the fault in progress.

The run **fails** if any acknowledged record is never consumed
(`lost > 0`), or if partition coverage is below the expected minimum.

It also **fails** on any **failed send** (`failed sends > 0`). The producer
runs with `acks=all`, `enable.idempotence=true`, a 60 s `max.block.ms` and a
120 s `delivery.timeout.ms`; no fault in the matrix keeps a partition
unavailable for anywhere near that long, so a correct client retries through
every fault and fails nothing. A send that is rejected, or not acknowledged
within the delivery timeout, therefore indicates a client defect or an
environment problem. A failure after the timeout is also ambiguous (the record
may or may not have been written), which the verifier cannot resolve, so it
does not pass on it. The `FAIL:` reason lists each failed record with the
client's error text, and the producer prints the same line as it happens so it
can be correlated with the fault in progress. A scenario that deliberately
exceeds these timeouts (for example a topic-recreate `--dwell-s` longer than
60 s) is expected to fail this check.

The producer does not await each send's outcome: it hands the record to the
client with a completion callback and moves on, so the client batches records
and pipelines requests as it would in an application, and the only
backpressure is the client's own (`buffer.memory` / `max.block.ms`). The
producer emits a `Sent` event before each send and the callback emits
`Delivered` / `SendFailed`; the verifier derives the **in-flight peak** per
producer from that stream and reports it, so the verdict shows how deep the
client's pipeline got. A `Sent` that has not settled by verdict time
(`unsettled sends`) **fails** the run: every record handed to the client must
come back as an acknowledgement or a failure.

**Consumer re-reads** (`by index` / `by offset`) are reported, not failed;
redelivery is expected under churn. Like `chaos.py`, the verdict also enforces
a **conservation ratio bound**: it fails when total consume events exceed
**2× delivered** (guarded by ≥100 delivered records, so a small run cannot
trip it). This catches a broker or client that redelivers endlessly, which the
per-record duplicate counters alone would report but never fail on.

**Producer double writes** (`double write`) fail the run. The verifier keeps
every distinct `(topic_id, partition, offset)` at which a logical record was
consumed; a record observed at two or more addresses **within one topic
generation** was committed twice by the producer, which
`enable.idempotence=true` must prevent. The `FAIL:` reason lists the record
and each offset. A record observed in two *different* generations
(`cross-generation repeats`) is excused and only reported: a topic recreate
destroys the broker's idempotent-producer state together with the old
generation, so a retry whose acknowledgement was lost in the delete window is
legitimately committed again in the new one.

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
  failed sends              : 0
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
cargo xtask chaos --reassign-partitions        # roll brokers + reassign every cycle

# Delete/recreate the topic mid-run, with a 3s dwell
cargo xtask chaos --topic-recreate --dwell-s 3  # roll brokers + recreate topic (3s dwell)

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
- **Mid-roll rebalance** (`--rebalance-mid-roll`): move that add/remove into the
  broker-roll down-window so the group reassignment overlaps the leader
  migration in time (see [Rebalance chaos](#rebalance-chaos) above). The
  librdkafka `--rebalance-mid-roll` equivalent. Example:
  ```
  cargo xtask chaos --cycles 10 --rps 1000 \
      --topic-recreate --dwell-s 5 \
      --rebalance-mid-roll --rebalance-add-cycle 5 --rebalance-remove-cycle 8 \
      --stop-s 8 --unclean \
      --workload producer:rust --workload consumer:rust --reports
  ```
- **Until-fail loop** (`--repeat N`): run up to N times, stop on the first
  failure, appending one TSV line per iteration to
  `target/chaos-runs/run-history.tsv` (the `chaos_until_fail.sh` analog).

## Not yet implemented (vs. `chaos.py`)

Tracked in
[`design/current/chaos-parity-gap.md`](../../design/current/chaos-parity-gap.md):

- The interactive manual REPL.
- The idle-based early-drain exit.
- The share consumer (KIP-932, blocked on the client — §20).
