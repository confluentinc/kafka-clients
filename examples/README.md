# Manual transaction tests (`txn_*`)

Hand-runnable programs that exercise the transactional producer and consumer
against a **real broker** and print ✅/❌ verdicts. They complement
`tests/integration/producer_transactions_test.rs`: same client APIs, but
human-readable output, broader scenario coverage, and no test harness — run
them, read them, re-run them.

Every program exits `0` when all of its checks pass and non-zero otherwise,
so a shell loop over them works as a crude suite.

## Broker

A single-node Kafka 4.3 in Docker (the transaction-state topic's replication
factor must be lowered to 1 on a one-broker cluster):

```
docker run -d --name kafka-txn-manual-test -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_LOG_DIRS=/tmp/kraft-combined-logs \
  apache/kafka:4.3.0
```

Tear down with `docker rm -f kafka-txn-manual-test` (this also deletes all
topic data, giving the paired tests a fresh start).

Env vars, honored by every program: `KAFKA_BOOTSTRAP_SERVERS` (default
`localhost:9092`); the basic pair additionally honors `TXN_TEST_TOPIC`.

## The programs

Paired files share fixed topics: run the **producer first**, then its
consumer. Re-running a pair is fine — the consumers detect how many complete
producer runs a topic holds and scale their expectations. The two
self-contained programs create fresh run-unique topics every time and can be
run in any order, any number of times.

| Run | Program | Covers |
|---|---|---|
| 1 | `txn_producer` → `txn_consumer` | The basic use case: commit visibility, abort invisibility, reading past an abort marker, with a read_uncommitted control view. |
| 2 | `txn_atomicity_producer` → `txn_atomicity_consumer` | One transaction across three topics (commit + abort), a 10,000-record transaction, commit-implies-flush of unawaited sends. |
| 3 | `txn_lifecycle_producer` → `txn_lifecycle_consumer` | Zombie fencing, crash recovery via a successor's `init_transactions` (dangling transaction aborted), sticky fatal state (even `abort` refused), two transactional + one plain producer interleaved on one partition. |
| 4 | `txn_errors_producer` → `txn_errors_consumer` | Negative cases: config validation, API misuse, `transaction.timeout.ms` above the broker ceiling, server-side transaction timeout, an oversized record poisoning its transaction (commit refused / abort allowed), unreachable bootstrap failing within `max.block.ms` — and, on the consumer side, that none of those failed transactions left visible data. Takes ~1 min (deliberate waits). |
| 5 | `txn_eos_pipeline` (self-contained) | Exactly-once consume-transform-produce with `send_offsets_to_transaction`: an aborted attempt rolls back output *and* offsets (input replays), the committed retry lands every record exactly once. |
| 6 | `txn_lso_demo` (self-contained) | Last-Stable-Offset gating: an open transaction withholds *everything* behind it from `read_committed` — including later non-transactional records — until commit releases them in log order. |

`txn_common/` is shared plumbing (producer/consumer builders, drain loops,
verdict helpers), not an example.

Run one with:

```
cargo run --example txn_producer
```

## Reading a failure

A ❌ line states the violated guarantee and prints expected-vs-got (long
sequences show the first divergence). A **hang** is itself a finding — every
wait in these programs is bounded, so a stuck program means the client
deadlocked; `sample <pid>` (macOS) shows where.

All six programs are expected to exit `0`. There are no known failures.

**The one bug this suite has caught so far**, and why case 2 of
`txn_errors_producer` still looks different from its neighbours: a transactional
`send` outside a transaction used to deadlock instead of returning the
`IllegalState` error Java throws. `kafka_producer.rs`'s `do_send_bytes` held the
`TransactionManager` mutex guard from the `if let` scrutinee around
`maybe_add_partition` while the error path re-locked the same mutex in
`maybe_transition_to_error_state`. Fixed, with six bounded unit tests in
`src/producer/kafka_producer.rs` (`test_send_outside_transaction_returns_illegal_state`
and its siblings).

Case 2 keeps its spawned task + join timeout around that one send. It is the
tripwire, not a workaround: no `tokio::time::timeout` can bound a task blocked
in `std::sync::Mutex::lock`, because the future never yields and the runtime
never reaches its timers. Only a second task can see it. If that branch ever
fires again, a send has re-acquired a lock it was already holding — start at
`sample <pid>` and the `maybe_add_partition` error arm.

## Interrupted paired runs

Killing a paired *producer* mid-run breaks the run-counting arithmetic its
consumer relies on, and an interrupted open transaction blocks
`read_committed` reads of that topic until `transaction.timeout.ms` (60 s
here) expires. Recreate the broker container (or wait out the timeout and
accept a ❌ from the count mismatch) to reset.

## Not covered here

- `send_offsets_to_transaction` with stale/foreign group metadata — needs an
  active group whose generation can be made stale on cue; fiddly to stage
  reliably.
- Broker bounce mid-transaction (chaos): begin → send → `docker restart
  kafka-txn-manual-test` → commit. Either a clean retry-and-commit or a clean
  failure is acceptable; duplicates or a partially visible transaction are
  not. Left as a by-hand recipe since it manipulates the shared broker.
- `TransactionalIdAuthorizationFailed` — needs a SASL/ACL-enabled listener
  (the integration suite's `kafka_cluster.rs` has one; single-node quickstart
  does not).
