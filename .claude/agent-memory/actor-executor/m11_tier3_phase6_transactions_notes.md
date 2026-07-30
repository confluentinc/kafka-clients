---
name: m11-tier3-phase6-transactions
description: M11 Tier3 P6 producers/transactions — AllBrokersStrategy fits driver; Unbatched serial; producer-txn-API gap blocks ongoing-txn integration
metadata:
  type: project
---

Tier 3 Phase 6 (producers & transactions): describeProducers, abortTransaction,
describeTransactions, fenceProducers, listTransactions, forceTerminateTransaction.
Sub-sliced into 3 commits (a/b/c) + 1 integration-test commit.

**AllBrokersStrategy design-gap verdict: FITS the Tier-1 driver cleanly, no amendment.**
The driver already supports keys *discovered during lookup*: `LookupResult` has a
`completed_keys` field (Java's 3-arg ctor), and `complete_lookup(mapped_keys)` →
`map(key, broker_id)` inserts brand-new fulfillment keys not in the original set.
AllBrokersStrategy.handle_response returns `completed=[any_broker]`,
`mapped={BrokerKey(Some(id))→id per broker}`. `AllBrokersFuture<V>` stores
`future: KafkaFutureImpl<HashMap<i32,KafkaFuture<V>>>` + `broker_futures:
Mutex<HashMap<i32,KafkaFutureImpl<V>>>` (interior mut because trait methods are
&self). The full AllBrokersStrategyIntegrationTest is translated (in-file
`mod integration_tests`) driving the real AdminApiDriver — all 5 pass.

**Unbatched handler translation (FenceProducersHandler):** the Rust driver's
`collect_fulfillment_requests` issues only `requests[0]` per broker per poll
cycle, so an Unbatched handler returning one-RequestAndKeys-per-key serializes
same-coordinator keys (one per completed request) instead of parallel like Java.
Still correct (all complete); not observed by in-scope tests (single id).
Documented on the handler.

**Result types key by String (idValue), not CoordinatorKey**, because
CoordinatorKey is pub(crate) — same as DescribeConsumerGroupsResult. Client uses
`coordinator_keyed_by_id(result_map)`.

**ListTransactionsResult nested futures:** `all_by_broker_id` is a custom
`KafkaFutureOps` (flatten top future → per-broker futures); `is_done` uses a
noop-waker `poll_once` on an already-done source. `all` = allByBrokerId +
then_apply flatten. `by_broker_id` = self.future.clone().

**Bare Java `KafkaException` (no code)** → `KafkaError::with_message(Errors::UnknownServerError, msg)`
(abort_transaction_handler malformed-response path).

**Handler validation panics** (mirror Java IllegalArgumentException) for
programming-error key sets the driver never produces (abort validate_topic_partitions,
list requireSingleton, describe_txn coordinator-type check, all_brokers validate_lookup_keys).
Test with `std::panic::catch_unwind`.

**Integration gap:** the plan's ongoing-transaction scenarios (in-flight producer
state, Ongoing txn, abort→CompleteAbort, ProducerFenced on old producer send)
CANNOT run: the Rust `Producer` has no transactional API (init/begin/commit/abort
are explicitly out of scope — src/producer/producer_trait.rs). What DOES run
without a producer: list_transactions (empty), describe_producers (no active),
describe_transactions (unknown→TransactionalIdNotFound), fence_producers +
force_terminate (fresh id → InitProducerId allocates PID at epoch 0). Ongoing-txn
test kept as compiling `#[ignore]` skeleton. Single-broker needs
`KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1` + `_MIN_ISR=1` for the txn
coordinator.

**ListTransactionsHandlerTest duration-filter v0 case NOT translated:** Java's
`build((short)0)` throws UnsupportedVersion for a set below-min-version field; the
Rust message generator silently omits such fields at serialize (generator-level
diff, out of scope). Documented on the test.

Wire wrappers (DescribeProducers/DescribeTransactions/ListTransactions/InitProducerId/
WriteTxnMarkers + ProducerIdAndEpoch) pre-landed by f03a133 — reused as-is, all
had byte-level tests already. The 7 untracked partial files (abort/describe-producers
POJOs/options/results + static_broker_strategy) were all faithful — reused verbatim.
