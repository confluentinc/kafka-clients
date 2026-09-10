// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Integration tests for the transaction-domain admin RPCs against a real Kafka
//! 4.2.0 broker: `describeProducers`, `describeTransactions`,
//! `abortTransaction`, `forceTerminateTransaction`, `listTransactions` and
//! `fenceProducers` — the last six of the 46 in-scope RPCs.
//!
//! Each scenario is a body generic over
//! [`AdminBackendFactory`](crate::common::backend_factory::AdminBackendFactory)
//! and registered with [`multilanguage_admin_test!`], so it runs against the
//! native Rust client, the Python sync binding, the Python asyncio binding and
//! the C FFI. With only `integration-tests` enabled the `__rust` arm is the whole
//! expansion.
//!
//! # What a client with no transactional producer can and cannot reach
//!
//! The Rust `Producer` trait does not implement the transactional API
//! (`init_transactions` / `begin_transaction` / ... — see
//! `src/producer/producer_trait.rs`), and `enable.idempotence` defaults to `true`
//! in `ProducerConfig` but nothing on the send path ever calls
//! `ProducerBatch::set_producer_state`, so every record this client produces
//! carries `RecordBatch::NO_PRODUCER_ID` (`src/producer/internals/sender.rs`).
//! The earlier revision of this file concluded from that that five of its six
//! scenarios could only be error paths and left `abortTransaction` as an
//! `#[ignore]`d skeleton. Measured against a real broker, most of that was wrong:
//!
//!   - **`fenceProducers` on a fresh transactional id creates a transaction.**
//!     `InitProducerId` allocates a producer id and the coordinator persists the
//!     transaction in state `Empty`. So `describeTransactions` and
//!     `listTransactions` both reach their **value** arms with real data, and
//!     `TransactionDescription` / `TransactionListing` cross all four backends.
//!   - **`abortTransaction` succeeds against any producer id.** The broker
//!     accepts a `WriteTxnMarkers` for a producer id it has never seen and
//!     appends an abort marker, so the RPC is a full round trip rather than an
//!     error path — and the appended control record makes the effect observable
//!     (see [`abort_transaction_appends_a_marker`]). No hanging transaction, and
//!     therefore no transactional producer, is needed.
//!   - **`describeProducers` reports a real `ProducerState`** once *some*
//!     idempotent producer has written to the partition. The broker image ships
//!     `kafka-console-producer.sh`, so one `docker exec` with
//!     `enable.idempotence=true` populates the partition's producer state in
//!     under a second (see [`produce_one_idempotent_record`]). Without it the six
//!     `ProducerState` fields are encoded by three servers and decoded by one
//!     client and never once carry a value.
//!
//! What genuinely stays out of reach, with the reason:
//!
//!   - `ProducerState.coordinatorEpoch` / `.currentTransactionStartOffset` and
//!     `TransactionDescription.transactionStartTimeMs` / `.topicPartitions` are
//!     populated only while a transaction is **in progress**
//!     (`ProducerStateEntry.currentTxnFirstOffset` is set by an
//!     `addPartitionsToTxn` + append sequence). Those need a transactional
//!     producer, so they are exercised on their `None` / empty side only. A
//!     dropped field is still caught, because `None` is what the assertions
//!     demand and a backend reporting `Some(0)` fails.
//!   - `TransactionState::{Ongoing, PrepareAbort, PrepareCommit, CompleteAbort,
//!     CompleteCommit, PrepareEpochFence}` — only `Empty` is reachable, for the
//!     same reason.
//!   - `describeProducers`' per-partition **error** arm. A partition that does
//!     not exist does not produce one: the `PartitionLeaderStrategy` lookup
//!     retries the metadata request until `default.api.timeout.ms` expires and
//!     the call fails as a whole-call `Timeout` (measured: ~145 000 metadata
//!     attempts in 30 s). That spin is **not a new observation** — it is the
//!     already-recorded DEFERRED 1 (`PLAN-multilanguage-admin.md` §5.6), whose
//!     four known triggers include `describeProducers` on an unknown topic; this
//!     is a fourth reproduction of it, not a discovery. The arm becomes reachable
//!     when DEFERRED 1 is fixed.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use confluent_kafka::admin::{
    AbortTransactionSpec, DescribeClusterOptions, DescribeProducersOptions, DescribeTransactionsOptions,
    FenceProducersOptions, ListOffsetsOptions, ListTransactionsOptions, OffsetSpec, TerminateTransactionOptions,
    TransactionListing, TransactionState,
};
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::protocol::Errors;

use crate::common::admin_backend::{AdminBackend, Outcomes, admin_for, all_of_exactly, create_topic};
use crate::common::backend_factory::AdminBackendFactory;
use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;
use crate::multilanguage_admin_test;

/// A single-broker cluster whose transaction-state log is replicated with a
/// factor of 1, so the transaction coordinator is usable on one node (the
/// defaults require three replicas).
fn txn_single_broker() -> ClusterConfig {
    let mut props = BTreeMap::new();
    props.insert("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR".to_string(), "1".to_string());
    props.insert("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR".to_string(), "1".to_string());
    ClusterConfig::with_properties(props)
}

/// A transaction-capable single-broker cluster that is *isolated* from the one
/// returned by [`txn_single_broker`].
///
/// [`crate::common::cluster_pool`] keys on `ClusterConfig`, so every scenario
/// sharing a config shares one container — and therefore shares global
/// transaction-coordinator state. Most scenarios in this file register a fresh
/// transactional id that then persists on the broker in state `Empty`, which
/// would break [`list_transactions_returns_empty_when_none_active`]'s assertion
/// of *global* emptiness. Giving that one scenario its own pool key is what keeps
/// the assertion sound, and it is sound for all four of its arms because none of
/// them creates a transaction.
///
/// The extra property restates the broker default for
/// `transaction.state.log.num.partitions` (50), so it changes the pool key
/// without changing any observable broker behavior.
fn txn_single_broker_isolated() -> ClusterConfig {
    let mut props = BTreeMap::new();
    props.insert("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR".to_string(), "1".to_string());
    props.insert("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR".to_string(), "1".to_string());
    props.insert("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS".to_string(), "50".to_string());
    ClusterConfig::with_properties(props)
}

/// A transactional id unique to this scenario *and* this backend.
///
/// All four arms of every scenario share one broker (the cluster pool keys on
/// `ClusterConfig`), and transaction state is global and persistent, so a shared
/// literal id would make the second arm see the first arm's transaction — and
/// [`fence_producers_allocates_producer_id_for_fresh_id`]'s `epoch == 0`
/// assertion would fail on it, since fencing an existing id *bumps* the epoch.
/// `TestContext::group_id` already derives its prefix from the running test's
/// thread name plus a random suffix, which is exactly the uniqueness needed; the
/// name says "group" but the helper is a unique-string generator.
fn transactional_id(ctx: &TestContext, base: &str) -> String {
    ctx.group_id(base)
}

/// The broker ids of the cluster the backend under test is talking to.
async fn broker_ids<B: AdminBackend>(admin: &B) -> Vec<i32> {
    let backend = admin.name();
    let description = admin
        .describe_cluster(DescribeClusterOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe cluster: {e}"));
    description.nodes.iter().map(|node| node.id()).collect()
}

/// Flattens a `listTransactions` result into Java's `all()`, failing the test on
/// any per-broker error.
///
/// The per-broker keying is asserted separately by
/// [`list_transactions_returns_empty_when_none_active`]; the filter scenarios
/// care only about *which* transactional ids came back.
fn all_listed(backend: &str, outcomes: &Outcomes<i32, Vec<TransactionListing>>) -> Vec<TransactionListing> {
    let mut listings = Vec::new();
    for (broker_id, outcome) in outcomes {
        match outcome {
            Ok(broker_listings) => listings.extend(broker_listings.iter().cloned()),
            Err(e) => panic!("{backend} backend: broker {broker_id} failed to list transactions: {e}"),
        }
    }
    listings
}

/// Whether `transactional_id` appears in the flattened listing.
fn lists_id(listings: &[TransactionListing], transactional_id: &str) -> bool {
    listings.iter().any(|listing| listing.transactional_id() == transactional_id)
}

/// Produces exactly one record to `topic` through an **idempotent** producer, so
/// the partition acquires a `ProducerState` the broker will report.
///
/// # Why this is a `docker exec` rather than a Rust producer
///
/// A producer state entry exists only for a producer that appends with a real
/// producer id, i.e. an idempotent or transactional one. This client does neither
/// (see the module note), and no other admin RPC creates one: the transaction
/// coordinator's own writes to `__transaction_state` and the group coordinator's
/// to `__consumer_offsets` carry no producer id. So the only way to make
/// `describeProducers`' value non-empty is a producer from outside this client,
/// and the broker image already ships one —
/// `kafka-console-producer.sh --producer-property enable.idempotence=true`.
///
/// The container is found by Docker network rather than by image tag so the
/// helper does not restate `KAFKA_TAG`, and the *container* listener is used for
/// bootstrap because the PLAINTEXT listener advertises a host-mapped port that is
/// unreachable from inside the container (measured: the run otherwise spends two
/// minutes retrying `127.0.0.1:<host port>`).
async fn produce_one_idempotent_record(ctx: &TestContext, topic: &str) {
    let network = ctx.broker_network_name().to_string();
    let listing = std::process::Command::new("docker")
        .args([
            "ps",
            "--filter",
            &format!("network={network}"),
            "--format",
            "{{.ID}} {{.Image}}",
        ])
        .output()
        .expect("docker ps");
    let listing = String::from_utf8_lossy(&listing.stdout).to_string();
    let container = listing
        .lines()
        .find(|line| line.contains("apache/kafka"))
        .and_then(|line| line.split_whitespace().next())
        .unwrap_or_else(|| panic!("no broker container on network {network}: {listing:?}"))
        .to_string();

    let bootstrap = ctx.container_bootstrap_servers().to_string();
    let script = format!(
        "echo idempotent-record | timeout 60 /opt/kafka/bin/kafka-console-producer.sh \
         --bootstrap-server {bootstrap} --topic {topic} \
         --producer-property enable.idempotence=true --producer-property acks=all"
    );
    let output = std::process::Command::new("docker")
        .args(["exec", &container, "bash", "-lc", &script])
        .output()
        .expect("docker exec kafka-console-producer.sh");
    assert!(
        output.status.success(),
        "idempotent produce to {topic} failed: {:?}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The partition's log end offset, used to observe that `abortTransaction`
/// appended a control record.
async fn latest_offset<B: AdminBackend>(admin: &B, tp: &TopicPartition) -> i64 {
    let backend = admin.name();
    let specs = HashMap::from([(tp.clone(), OffsetSpec::latest())]);
    let outcomes = admin
        .list_offsets(&specs, ListOffsetsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: list offsets for {tp}: {e}"));
    outcomes
        .get(tp)
        .unwrap_or_else(|| panic!("{backend} backend: no listOffsets entry for {tp}"))
        .as_ref()
        .unwrap_or_else(|e| panic!("{backend} backend: listOffsets for {tp}: {e}"))
        .offset()
}

// ---------------------------------------------------------------------------
// Test bodies — generic over AdminBackendFactory
// ---------------------------------------------------------------------------

/// (a) `list_transactions` fans out to every broker and reports an empty listing
/// on a quiet cluster. The first exercise of `AllBrokersStrategy` against a live
/// broker.
async fn list_transactions_returns_empty_when_none_active<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let outcomes = admin
        .list_transactions(ListTransactionsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: list transactions: {e}"));

    // The per-broker keying is the part only this scenario checks: the response
    // must carry an entry for exactly the cluster's brokers, and each of them
    // must have answered. `all_of_exactly` is what rules out a backend that
    // answered with no entries at all — an empty map would otherwise be
    // indistinguishable from "no transactions", which is what this scenario
    // asserts next.
    let ids = broker_ids(&admin).await;
    all_of_exactly(&admin, &outcomes, &ids, "listTransactions");

    let listings = all_listed(backend, &outcomes);
    assert!(
        listings.is_empty(),
        "{backend} backend: no transaction should be active on a quiet cluster, got {listings:?}"
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// (b) `list_transactions` reports a real [`TransactionListing`] for a
/// transactional id `fence_producers` has just registered.
///
/// This is the only route to `listTransactions`' value arm: the listing's three
/// fields (`transactionalId`, `producerId`, `state`) are otherwise encoded by
/// three servers and decoded by one client without ever carrying data.
async fn list_transactions_reports_a_fenced_transaction<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();
    let transactional_id = transactional_id(ctx, "list-txn");

    let fenced = admin
        .fence_producers(std::slice::from_ref(&transactional_id), FenceProducersOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: fence producers: {e}"));
    let producer = *fenced
        .get(&transactional_id)
        .unwrap_or_else(|| panic!("{backend} backend: no fenceProducers entry for {transactional_id}"))
        .as_ref()
        .unwrap_or_else(|e| panic!("{backend} backend: fence {transactional_id}: {e}"));

    let outcomes = admin
        .list_transactions(ListTransactionsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: list transactions: {e}"));
    let listings = all_listed(backend, &outcomes);
    let listing = listings
        .iter()
        .find(|listing| listing.transactional_id() == transactional_id)
        .unwrap_or_else(|| {
            panic!("{backend} backend: the fenced id {transactional_id} should be listed, got {listings:?}")
        });

    // Each field is asserted against a value obtained by a *different* route, so
    // a transposition fails: the producer id came back from `fenceProducers`, and
    // the state is the one the coordinator persists for a transaction with no
    // partitions yet.
    assert_eq!(
        listing.producer_id(),
        producer.producer_id,
        "{backend} backend: the listing should report the producer id fenceProducers allocated"
    );
    assert_eq!(
        listing.state(),
        TransactionState::Empty,
        "{backend} backend: a freshly initialized transactional id is Empty, got {:?}",
        listing.state()
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// (c) All four `ListTransactionsOptions` filters, each in both the including and
/// the excluding direction.
///
/// Both directions matter and one alone is not enough: a backend that dropped a
/// filter passes every *inclusive* check, and a backend that garbled it into
/// something matching nothing passes every *exclusive* one. The mechanism for
/// each is `TransactionStateManager.listTransactionStates`
/// (`core/src/main/scala/kafka/coordinator/transaction/TransactionStateManager.scala:339-357`).
async fn list_transactions_filters<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();
    let transactional_id = transactional_id(ctx, "filter-txn");

    let fenced = admin
        .fence_producers(std::slice::from_ref(&transactional_id), FenceProducersOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: fence producers: {e}"));
    let producer_id = fenced
        .get(&transactional_id)
        .and_then(|outcome| outcome.as_ref().ok())
        .unwrap_or_else(|| panic!("{backend} backend: fence {transactional_id} should succeed"))
        .producer_id;

    let listed = |options: ListTransactionsOptions| {
        let admin = &admin;
        let id = transactional_id.clone();
        async move {
            let outcomes = admin
                .list_transactions(options)
                .await
                .unwrap_or_else(|e| panic!("{backend} backend: list transactions: {e}"));
            lists_id(&all_listed(backend, &outcomes), &id)
        }
    };

    // (i) State filter. `Empty` is the state a fenced-but-unused id sits in;
    // `Ongoing` is a state nothing here can reach, so it must exclude.
    assert!(
        listed(ListTransactionsOptions::new().filter_states([TransactionState::Empty])).await,
        "{backend} backend: filtering on Empty must include an Empty transaction"
    );
    assert!(
        !listed(ListTransactionsOptions::new().filter_states([TransactionState::Ongoing])).await,
        "{backend} backend: filtering on Ongoing must exclude an Empty transaction"
    );

    // (ii) Producer id filter, with an id that exists and one that does not.
    assert!(
        listed(ListTransactionsOptions::new().filter_producer_ids([producer_id])).await,
        "{backend} backend: filtering on producer id {producer_id} must include its transaction"
    );
    assert!(
        !listed(ListTransactionsOptions::new().filter_producer_ids([producer_id + 424_242])).await,
        "{backend} backend: filtering on an unallocated producer id must exclude every transaction"
    );

    // (iii) Duration filter. The broker excludes a transaction when
    // `now - txnStartTimestamp <= filterDurationMs`, and an `Empty` transaction
    // has `txnStartTimestamp == -1`, so `now + 1` is compared: every ordinary
    // duration includes it and only `i64::MAX` excludes it. That is a real
    // exercise of the field in both directions even though the *reason* it
    // includes is the sentinel start time.
    assert!(
        listed(ListTransactionsOptions::new().filter_on_duration(0)).await,
        "{backend} backend: a zero duration filter must include every transaction"
    );
    assert!(
        !listed(ListTransactionsOptions::new().filter_on_duration(i64::MAX)).await,
        "{backend} backend: a duration filter of i64::MAX must exclude every transaction"
    );

    // (iv) Transactional id pattern (KIP-1152, ListTransactions v2). A regex
    // anchored on this id's own prefix includes it; one anchored elsewhere does
    // not.
    let prefix = transactional_id.split('_').next().expect("non-empty id").to_string();
    assert!(
        listed(ListTransactionsOptions::new().filter_on_transactional_id_pattern(Some(format!("^{prefix}.*")))).await,
        "{backend} backend: a pattern matching {transactional_id} must include it"
    );
    assert!(
        !listed(
            ListTransactionsOptions::new().filter_on_transactional_id_pattern(Some("^no-such-prefix-.*".to_string()))
        )
        .await,
        "{backend} backend: a pattern matching nothing must exclude every transaction"
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// (d) A malformed transactional-id pattern is rejected by the broker, in the
/// **per-broker** slot.
///
/// This is the only route to `listTransactions`' per-broker error arm on a
/// healthy cluster. `TransactionStateManager.listTransactionStates` compiles the
/// pattern and throws `InvalidRegularExpression` on a `PatternSyntaxException`
/// (`TransactionStateManager.scala:359-368`), which `KafkaApis`' catch-all turns
/// into `INVALID_REGULAR_EXPRESSION(128)`. The broker-discovery step succeeded,
/// so this is an entry error and not the response's top-level one — reading it out
/// of the entry is what distinguishes the two.
async fn list_transactions_rejects_a_malformed_id_pattern<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let outcomes = admin
        .list_transactions(
            ListTransactionsOptions::new().filter_on_transactional_id_pattern(Some("(ab(cd".to_string())),
        )
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: a malformed pattern must not fail the whole call: {e}"));

    let ids = broker_ids(&admin).await;
    assert_eq!(
        outcomes.keys().copied().collect::<HashSet<_>>(),
        ids.iter().copied().collect::<HashSet<_>>(),
        "{backend} backend: every broker should still have an entry, so the rejection is per-broker"
    );
    for broker_id in &ids {
        let error = outcomes[broker_id]
            .as_ref()
            .expect_err("a malformed regular expression should be rejected");
        assert_eq!(
            error.error(),
            Errors::InvalidRegularExpression,
            "{backend} backend: broker {broker_id} should reject the pattern with \
             INVALID_REGULAR_EXPRESSION, got {error:?}"
        );
    }

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// (e) `describe_producers` on a fresh partition reports no active producers,
/// through `PartitionLeaderStrategy` *and* through `StaticBrokerStrategy`.
///
/// The second half exercises `DescribeProducersOptions.brokerId`'s `OptionalInt`
/// — unset routes to the partition's leader, set routes straight to that broker —
/// but it discriminates in exactly **one** direction, and the claim has to say
/// which:
///
///   - **"unset → broker 0" is caught.** The first call sends no broker id;
///     a backend that substituted 0 would address a broker that does not exist
///     (ids in this fixture start at 1) and the call would fail.
///   - **"the field was dropped" is not caught.** The second call sets the
///     cluster's *only* broker, which is also the partition's leader, so a
///     backend that ignored the field takes the same route and the equality
///     below compares `[] == []`.
///
/// Making the dropped direction observable needs a multi-broker fixture — the
/// three-argument `multilanguage_admin_test!` form plus `kip848_3_broker(..)`, as
/// `admin_groups_test.rs` does — with `broker_id` naming a **non-leader** broker,
/// so the two answers may legally differ. Not attempted here.
async fn describe_producers_reports_no_active_producers<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let topic = ctx.topic("admin_describe_producers");
    create_topic(&admin, &topic, 1, 1).await;
    let tp = TopicPartition::new(topic.clone(), 0);
    let partitions = std::slice::from_ref(&tp);

    let by_leader = admin
        .describe_producers(partitions, DescribeProducersOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe producers: {e}"));
    all_of_exactly(&admin, &by_leader, partitions, "describeProducers");
    assert!(
        by_leader[&tp].as_ref().expect("checked above").active_producers().is_empty(),
        "{backend} backend: a fresh partition has no active producers"
    );

    let broker_id = broker_ids(&admin).await[0];
    let by_broker = admin
        .describe_producers(partitions, DescribeProducersOptions::new().set_broker_id(broker_id))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe producers on broker {broker_id}: {e}"));
    all_of_exactly(&admin, &by_broker, partitions, "describeProducers(brokerId)");
    assert_eq!(
        by_broker[&tp].as_ref().expect("checked above").active_producers(),
        by_leader[&tp].as_ref().expect("checked above").active_producers(),
        "{backend} backend: querying broker {broker_id} directly must agree with querying the leader"
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// (f) `describe_producers` reports a real [`ProducerState`] once an idempotent
/// producer has written to the partition.
///
/// Four of `ProducerState`'s six fields carry real values here; the two
/// `Optional`s stay absent, which needs an in-progress transaction (see the
/// module note) and is asserted on its `None` side so a backend defaulting them
/// to 0 fails.
async fn describe_producers_reports_an_idempotent_producer<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let topic = ctx.topic("admin_producer_state");
    create_topic(&admin, &topic, 1, 1).await;
    let tp = TopicPartition::new(topic.clone(), 0);
    produce_one_idempotent_record(ctx, &topic).await;

    let outcomes = admin
        .describe_producers(std::slice::from_ref(&tp), DescribeProducersOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe producers: {e}"));
    let state = outcomes[&tp]
        .as_ref()
        .unwrap_or_else(|e| panic!("{backend} backend: describe {tp}: {e}"));
    assert_eq!(
        state.active_producers().len(),
        1,
        "{backend} backend: exactly the one idempotent producer should be active, got {:?}",
        state.active_producers()
    );
    let producer = &state.active_producers()[0];

    // A real producer id is non-negative; `RecordBatch::NO_PRODUCER_ID` is -1, so
    // this separates "the broker reported a producer" from "the field defaulted".
    assert!(
        producer.producer_id() >= 0,
        "{backend} backend: an idempotent producer has a real producer id, got {}",
        producer.producer_id()
    );
    assert_eq!(
        producer.producer_epoch(),
        0,
        "{backend} backend: a freshly initialized idempotent producer is at epoch 0"
    );
    // One record was produced, so the last sequence number is 0 — not merely
    // non-negative, which a defaulted field would also satisfy.
    assert_eq!(
        producer.last_sequence(),
        0,
        "{backend} backend: after one record the last sequence is 0"
    );
    assert!(
        producer.last_timestamp() > 0,
        "{backend} backend: the append timestamp should be a real wall-clock value, got {}",
        producer.last_timestamp()
    );
    // Both Optionals are populated only for a producer inside a transaction.
    // Asserting `None` is what catches a backend that decoded an absent
    // `OptionalInt` / `OptionalLong` as 0 or -1.
    assert_eq!(
        producer.coordinator_epoch(),
        None,
        "{backend} backend: a non-transactional producer reports no coordinator epoch"
    );
    assert_eq!(
        producer.current_transaction_start_offset(),
        None,
        "{backend} backend: a non-transactional producer reports no transaction start offset"
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// (g) `describe_transactions` for an id that was never used exercises
/// `CoordinatorStrategy(TRANSACTION)` and fails in the **per-id** slot with
/// `TRANSACTIONAL_ID_NOT_FOUND`.
async fn describe_transactions_unknown_id_not_found<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let unknown = transactional_id(ctx, "unknown-txn");
    let outcomes = admin
        .describe_transactions(std::slice::from_ref(&unknown), DescribeTransactionsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe transactions: {e}"));
    let error = outcomes
        .get(&unknown)
        .unwrap_or_else(|| panic!("{backend} backend: no entry for {unknown}"))
        .as_ref()
        .expect_err("describing an unknown transactional id should fail");
    assert_eq!(
        error.error(),
        Errors::TransactionalIdNotFound,
        "{backend} backend: expected TRANSACTIONAL_ID_NOT_FOUND, got {error:?}"
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// (h) `describe_transactions` reports a full [`TransactionDescription`] for an
/// id `fence_producers` has registered.
///
/// The transaction timeout is what makes this scenario more than a shape check:
/// `FenceProducersHandler` writes the *option's* timeout into
/// `InitProducerIdRequest.transactionTimeoutMs`, so fencing with an unusual
/// timeout and reading it back through a second RPC is the one place in this
/// harness where an `*Options` value is observable in a response.
async fn describe_transactions_reports_a_fenced_transaction<F: AdminBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();
    let transactional_id = transactional_id(ctx, "describe-txn");

    // Deliberately not the client's 30 000 ms `request.timeout.ms` default, which
    // is what `FenceProducersHandler::new` falls back to — a backend that dropped
    // the option would report that default and fail below.
    const TXN_TIMEOUT_MS: i32 = 45_000;
    let fenced = admin
        .fence_producers(
            std::slice::from_ref(&transactional_id),
            FenceProducersOptions::new().set_timeout_ms(Some(TXN_TIMEOUT_MS)),
        )
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: fence producers: {e}"));
    let producer = *fenced
        .get(&transactional_id)
        .and_then(|outcome| outcome.as_ref().ok())
        .unwrap_or_else(|| panic!("{backend} backend: fence {transactional_id} should succeed"));

    let outcomes = admin
        .describe_transactions(std::slice::from_ref(&transactional_id), DescribeTransactionsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe transactions: {e}"));
    let description = outcomes
        .get(&transactional_id)
        .unwrap_or_else(|| panic!("{backend} backend: no entry for {transactional_id}"))
        .as_ref()
        .unwrap_or_else(|e| panic!("{backend} backend: describe {transactional_id}: {e}"));

    // The coordinator id must name a real broker of *this* backend's cluster,
    // which is the `assert_real_coordinator` idea applied to an id-only field.
    let ids = broker_ids(&admin).await;
    assert!(
        ids.contains(&description.coordinator_id()),
        "{backend} backend: the coordinator id {} should be one of the cluster's brokers {ids:?}",
        description.coordinator_id()
    );
    assert_eq!(
        description.state(),
        TransactionState::Empty,
        "{backend} backend: a freshly initialized transactional id is Empty"
    );
    assert_eq!(
        description.producer_id(),
        producer.producer_id,
        "{backend} backend: the description should report the producer id fenceProducers allocated"
    );
    assert_eq!(
        description.producer_epoch() as i16,
        producer.epoch,
        "{backend} backend: the description should report the epoch fenceProducers allocated"
    );
    assert_eq!(
        description.transaction_timeout_ms(),
        i64::from(TXN_TIMEOUT_MS),
        "{backend} backend: the coordinator stores the timeout fenceProducers sent"
    );
    // Both are populated only while a transaction is in progress; asserting the
    // empty side catches a backend that defaulted them.
    assert_eq!(
        description.transaction_start_time_ms(),
        None,
        "{backend} backend: a transaction that has not started reports no start time"
    );
    assert!(
        description.topic_partitions().is_empty(),
        "{backend} backend: no partition has been added to this transaction, got {:?}",
        description.topic_partitions()
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// (i) `fence_producers` for a fresh transactional id allocates a brand-new
/// producer id and epoch through `CoordinatorStrategy(TRANSACTION)` +
/// `InitProducerId`, even with no prior producer.
async fn fence_producers_allocates_producer_id_for_fresh_id<F: AdminBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();
    let transactional_id = transactional_id(ctx, "fence-fresh");

    let outcomes = admin
        .fence_producers(std::slice::from_ref(&transactional_id), FenceProducersOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: fence producers: {e}"));
    all_of_exactly(&admin, &outcomes, std::slice::from_ref(&transactional_id), "fenceProducers");
    let producer = outcomes[&transactional_id].as_ref().expect("checked above");

    assert!(
        producer.producer_id >= 0,
        "{backend} backend: the coordinator should allocate a valid producer id, got {}",
        producer.producer_id
    );
    // A never-before-seen transactional id starts at epoch 0; the id is unique per
    // scenario *and* backend precisely so this stays exact rather than `>= 0`
    // (fencing an existing id bumps the epoch).
    assert_eq!(
        producer.epoch, 0,
        "{backend} backend: a fresh producer id should be fenced at epoch 0"
    );
    // Deliberately *not* `producer.is_valid()`: that predicate is
    // `RecordBatch::NO_PRODUCER_ID < producer_id`
    // (`src/common/utils/producer_id_and_epoch.rs:50-52`), i.e. `producer_id > -1`,
    // which is entailed by the `>= 0` assertion above and so can never fire on its
    // own. The independent observable is that the coordinator *persisted* this
    // exact pair: the transactional id did not exist before this call (its sibling
    // scenario shows an unregistered id answers TRANSACTIONAL_ID_NOT_FOUND), so a
    // backend that resolved the future without sending `InitProducerId`, or that
    // fabricated the pair, cannot produce a matching description here.
    let descriptions = admin
        .describe_transactions(std::slice::from_ref(&transactional_id), DescribeTransactionsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe transactions: {e}"));
    let description = descriptions
        .get(&transactional_id)
        .unwrap_or_else(|| panic!("{backend} backend: no entry for {transactional_id}"))
        .as_ref()
        .unwrap_or_else(|e| panic!("{backend} backend: fenceProducers should have registered {transactional_id}: {e}"));
    assert_eq!(
        description.producer_id(),
        producer.producer_id,
        "{backend} backend: the coordinator should report the producer id fenceProducers allocated"
    );
    assert_eq!(
        description.producer_epoch() as i16,
        producer.epoch,
        "{backend} backend: the coordinator should report the epoch fenceProducers allocated"
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// (j) `force_terminate_transaction` for a fresh id succeeds via the same
/// `InitProducerId` path (Java implements it as a thin wrapper over
/// `fenceProducers` — `KafkaAdminClient.java:4848-4864`).
///
/// The follow-up `describe_transactions` is what makes this more than an "it
/// returned Ok" check: the id did not exist before the call
/// (`describe_transactions` on an unknown id is `TRANSACTIONAL_ID_NOT_FOUND`, as
/// its sibling scenario asserts), so its presence afterwards proves the RPC
/// reached the coordinator rather than being dropped.
async fn force_terminate_transaction_fresh_id<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();
    let transactional_id = transactional_id(ctx, "terminate-fresh");

    admin
        .force_terminate_transaction(&transactional_id, TerminateTransactionOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: force terminate {transactional_id}: {e}"));

    let outcomes = admin
        .describe_transactions(std::slice::from_ref(&transactional_id), DescribeTransactionsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe transactions: {e}"));
    let description = outcomes
        .get(&transactional_id)
        .unwrap_or_else(|| panic!("{backend} backend: no entry for {transactional_id}"))
        .as_ref()
        .unwrap_or_else(|e| {
            panic!("{backend} backend: forceTerminateTransaction should have registered {transactional_id}: {e}")
        });
    assert_eq!(
        description.state(),
        TransactionState::Empty,
        "{backend} backend: a terminated transaction with no partitions is Empty"
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// (k) `abort_transaction` appends an abort marker to the partition.
///
/// The [`AbortTransactionSpec`] is built from a **real** `describe_producers` row
/// rather than from constants, which is how Java documents the flow
/// (`Admin.abortTransaction`: the producer id and epoch come from
/// `describeProducers`) — so a backend that transposed the spec's four fields
/// would send a different marker.
///
/// The observable is the partition's log end offset: an abort marker is a control
/// record, so a successful call advances it by exactly one. A backend that
/// answered `Ok` without sending the request leaves it unchanged, which is what
/// makes this stronger than asserting `Ok` alone. (No error arm is reachable: the
/// broker accepts a marker for a producer id and epoch it has never seen —
/// measured for a fabricated id, a mismatched epoch and an unknown id, all three
/// `Ok`.)
async fn abort_transaction_appends_a_marker<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let topic = ctx.topic("admin_abort_txn");
    create_topic(&admin, &topic, 1, 1).await;
    let tp = TopicPartition::new(topic.clone(), 0);
    produce_one_idempotent_record(ctx, &topic).await;

    let described = admin
        .describe_producers(std::slice::from_ref(&tp), DescribeProducersOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe producers: {e}"));
    let state = described[&tp]
        .as_ref()
        .unwrap_or_else(|e| panic!("{backend} backend: describe {tp}: {e}"));
    let producer = state
        .active_producers()
        .first()
        .unwrap_or_else(|| panic!("{backend} backend: the idempotent produce should have left a producer state"));

    let before = latest_offset(&admin, &tp).await;
    let spec = AbortTransactionSpec::new(
        tp.clone(),
        producer.producer_id(),
        producer.producer_epoch() as i16,
        // The coordinator epoch is not part of a non-transactional producer's
        // state, and the broker does not validate it here; 0 is the value Java's
        // own `describeProducers`-driven flow would carry for a producer whose
        // `coordinatorEpoch` is absent.
        0,
    );
    admin
        .abort_transaction(spec, Default::default())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: abort transaction on {tp}: {e}"));

    let after = latest_offset(&admin, &tp).await;
    assert_eq!(
        after,
        before + 1,
        "{backend} backend: aborting should append exactly one control record to {tp}"
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

multilanguage_admin_test!(
    test_list_transactions_returns_empty_when_none_active,
    list_transactions_returns_empty_when_none_active,
    txn_single_broker_isolated()
);
multilanguage_admin_test!(
    test_list_transactions_reports_a_fenced_transaction,
    list_transactions_reports_a_fenced_transaction,
    txn_single_broker()
);
multilanguage_admin_test!(test_list_transactions_filters, list_transactions_filters, txn_single_broker());
multilanguage_admin_test!(
    test_list_transactions_rejects_a_malformed_id_pattern,
    list_transactions_rejects_a_malformed_id_pattern,
    txn_single_broker()
);
multilanguage_admin_test!(
    test_describe_producers_reports_no_active_producers,
    describe_producers_reports_no_active_producers,
    txn_single_broker()
);
multilanguage_admin_test!(
    test_describe_producers_reports_an_idempotent_producer,
    describe_producers_reports_an_idempotent_producer,
    txn_single_broker()
);
multilanguage_admin_test!(
    test_describe_transactions_unknown_id_not_found,
    describe_transactions_unknown_id_not_found,
    txn_single_broker()
);
multilanguage_admin_test!(
    test_describe_transactions_reports_a_fenced_transaction,
    describe_transactions_reports_a_fenced_transaction,
    txn_single_broker()
);
multilanguage_admin_test!(
    test_fence_producers_allocates_producer_id_for_fresh_id,
    fence_producers_allocates_producer_id_for_fresh_id,
    txn_single_broker()
);
multilanguage_admin_test!(
    test_force_terminate_transaction_fresh_id,
    force_terminate_transaction_fresh_id,
    txn_single_broker()
);
multilanguage_admin_test!(
    test_abort_transaction_appends_a_marker,
    abort_transaction_appends_a_marker,
    txn_single_broker()
);
