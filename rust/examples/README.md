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
| 7 | `txn_api_contracts` (self-contained) | Producer API contracts: double `init_transactions`, empty transactions, the javadoc recovery loop (abortable error → abort → retry → exactly one copy), commit surfacing an unawaited send failure, abort resolving pending sends (KIP-654), `close()` abandoning an open transaction, the `buffer.memory` record-size limit, KIP-939 2PC probes, error-classification flags. **Exits 1 — see "Known failures".** |
| 8 | `txn_concurrency` (self-contained) | 8 tasks sharing one producer inside one transaction, fencing while 300 sends are in flight, 90-transaction epoch churn, three overlapping transactions on one partition, 6-topic marker fan-out, `max.in.flight=1`. |
| 9 | `txn_consumer_contracts` (self-contained) | `end_offsets` LSO vs high-watermark, `position()` past markers, seeking into an aborted range, a delayed fetch spanning an abort, all four compression codecs under transactions. |
| 10 | `txn_offsets_contracts` (self-contained) | Offset-metadata round trip, `UNSTABLE_OFFSET_COMMIT` gating of pending offsets, and the stale-group-metadata probe (see "Broker differences"). |
| — | `txn_buffer_probe` (manual orchestration) | `buffer.memory` binding when batches cannot drain. Needs `docker pause` mid-run; the file's docs give the recipe. |

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

## Known failures

Every program is expected to exit `0` **except `txn_api_contracts`**, whose
case 8 fails on a real, still-open client bug: with
`transaction.two.phase.commit.enable=true`, `init_transactions` *succeeds*
against a broker that has 2PC disabled, because the `Enable2Pc` field is
silently dropped when `InitProducerId` goes out below v6. Java refuses to
serialize a non-default, non-ignorable field at a version that cannot carry
it; our generator emits no such check for any of the 197 message types
(`design/history/Milestone-11/PLAN.md` §9.1). A targeted stopgap is a builder
guard like the one `TxnOffsetCommitRequestBuilder` already has for group
metadata below v3.

## Broker differences

`txn_offsets_contracts` case 3 attaches offsets using a `ConsumerGroupMetadata`
captured *before* a second member joined the group. Kafka **4.2.0 rejects** it
with `ILLEGAL_GENERATION`, as its coordinator source mandates
(`OffsetMetadataManager.validateTransactionalOffsetCommit` →
`ConsumerGroup.validateOffsetCommit`). Kafka **4.3.0 accepts** it, and the
zombie's offsets land on commit — KIP-447 fencing is not enforced there. The
client sends the same bytes either way (verified at `RUST_LOG=debug`: request
v5 carrying the member id and the stale generation), so the case prints
`OBSERVATION` lines rather than failing, and the difference is a candidate
upstream report.

## Bugs this suite has caught

**A transactional `send` outside a transaction deadlocked** instead of
returning the `IllegalState` error Java throws — which is why case 2 of
`txn_errors_producer` still looks different from its neighbours.
`kafka_producer.rs`'s `do_send_bytes` held the `TransactionManager` mutex guard
from the `if let` scrutinee around `maybe_add_partition` while the error path
re-locked the same mutex in `maybe_transition_to_error_state`. Fixed, with six
bounded unit tests in `src/producer/kafka_producer.rs`
(`test_send_outside_transaction_returns_illegal_state` and its siblings).

**`commit_transaction` hung for the whole `max.block.ms` after a failed send**,
and the `abort_transaction` that Java's javadoc offers as the way out was then
refused — an application following the documented recipe was wedged
(`txn_api_contracts` case 4). `fail_batch_with_record_exceptions` passed an
*empty* batch pool to `handle_failed_batch`, so the sequence rewrite that
`TransactionManager.java:818` performs had nothing to rewrite; the pipelined
follow-up batch retried a stale sequence into `OUT_OF_ORDER_SEQUENCE_NUMBER`
forever, and the parked `EndTxn` was never dequeued. Fixed (commit resolves in
~120 ms with the batch's own error) plus
`test_failed_batch_adjusts_following_sequences_and_fails_pending_commit`.

**Telling "abort and retry" from "retry" or "give up"** after an abortable
produce failure (`txn_api_contracts` case 3). This was once surfaced through a
librdkafka-style `txn_requires_abort()` flag stamped onto the error, but Java has
no such flag: `TransactionManager` records the condition in its state
(`ABORTABLE_ERROR`, read via `hasAbortableError()`) and `commitTransaction()`
throws `KafkaException`, whose javadoc answer is to abort. The example now
demonstrates that contract — the surfaced error is a `KafkaException` and the
next call is `abort_transaction()` — rather than a flag the client does not
expose to applications.

**One false alarm, worth remembering.** Case 7 used to report `buffer.memory`
as unenforced. It was not: the case was staged against a healthy broker (where
batches drain as fast as they fill, so the pool never fills) and read only
`send()`'s immediate return (a rejected record reports on the *future*). The
pool was correct all along — `txn_buffer_probe` demonstrates it binding.

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

- Per-API `max.block.ms` timeouts (commit / abort / `send_offsets`) — a healthy
  coordinator answers in milliseconds, so these need a stalled broker like the
  chaos cases below.
- Producer-id and transactional-id expiration (KIP-360 / KIP-854) — needs a
  broker configured with short `producer.id.expiration.ms`, i.e. its own
  container.
- Broker bounce mid-transaction (chaos): begin → send → `docker restart
  kafka-txn-manual-test` → commit. Either a clean retry-and-commit or a clean
  failure is acceptable; duplicates or a partially visible transaction are
  not. Left as a by-hand recipe since it manipulates the shared broker.
- `TransactionalIdAuthorizationFailed` — needs a SASL/ACL-enabled listener
  (the integration suite's `kafka_cluster.rs` has one; single-node quickstart
  does not).
