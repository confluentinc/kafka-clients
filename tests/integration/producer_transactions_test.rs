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

//! Broker integration tests for producer idempotence and transactions
//! (Milestone 11, PLAN §Phase-8).
//!
//! These are the four scenarios §Phase-8 specifies, against the pooled
//! testcontainers cluster:
//!
//! 1. idempotent produce surviving a forced epoch bump
//!    → [`test_idempotent_produce_survives_a_forced_epoch_bump`]
//! 2. a transactional commit visible only after the commit
//!    → [`transactional_records_are_visible_only_after_commit_inner`]
//! 3. an abort discards its records
//!    → [`aborted_transaction_records_are_discarded_inner`]
//! 4. consume-transform-produce with `send_offsets_to_transaction`
//!    → [`consume_transform_produce_with_offsets_inner`]
//!
//! It also holds translations from `TransactionsTest.scala` (AK 4.3.1, CONSUMER
//! arm only; native-only because fencing needs two producers sharing a
//! `transactional.id`):
//!
//! - `testFencingOnCommit` → [`test_fencing_on_commit`]
//! - `testFencingOnSendOffsets` → [`test_fencing_on_send_offsets`]
//! - `testFencingOnSend` → [`test_fencing_on_send`]
//! - `testConsecutivelyRunInitTransactions` → [`test_consecutively_run_init_transactions`]
//! - `testEmptyAbortAfterCommit` → [`test_empty_abort_after_commit`]
//! - `testOffsetMetadataInSendOffsetsToTransaction` →
//!   [`test_offset_metadata_in_send_offsets_to_transaction`]
//! - `testSendOffsetsWithGroupMetadata` → [`test_send_offsets_with_group_metadata`]
//! - `testReadCommittedConsumerShouldNotSeeUndecidedData` →
//!   [`test_read_committed_consumer_should_not_see_undecided_data`]
//! - `testDelayedFetchIncludesAbortedTransaction` →
//!   [`test_delayed_fetch_includes_aborted_transaction`]
//! - `testMultipleMarkersOneLeader` → [`test_multiple_markers_one_leader`]
//! - `testFencingOnTransactionExpiration` → [`test_fencing_on_transaction_expiration`]
//!   (on its own cluster config, for the abort-cleanup interval)
//!
//! and from the Java `clients-integration-tests` module (AK 4.3.1):
//!
//! - `ProducerIntegrationTest.testTransactionWithAndWithoutSend` →
//!   [`test_transaction_with_and_without_send`]
//! - `ProducerIntegrationTest.testTransactionWithInvalidSendAndEndTxnRequestSent` →
//!   [`test_transaction_with_invalid_send_and_end_txn_request_sent`]
//! - `ProducerIntegrationTest.testTransactionWithSendOffset` → its
//!   `listTransactions()` `COMPLETE_COMMIT` check is folded into
//!   [`consume_transform_produce_with_offsets_inner`], which already covers the
//!   rest of that scenario
//! - `TransactionsWithMaxInFlightOneTest.testTransactionalProducerSingleBrokerMaxInFlightOne` →
//!   [`test_transactional_producer_single_broker_max_in_flight_one`]
//! - `AdminFenceProducersTest.testFenceAfterProducerCommit` →
//!   [`test_fence_after_producer_commit`]
//! - `AdminFenceProducersTest.testFenceBeforeProducerCommit` →
//!   [`test_fence_before_producer_commit`]
//! - `ProducerIdExpirationTest.testProducerIdExpirationWithNoTransactions` →
//!   [`test_producer_id_expiration_with_no_transactions`]
//! - `ProducerIdExpirationTest.testTransactionAfterTransactionIdExpiresButProducerIdRemains` →
//!   [`test_transaction_after_transaction_id_expires_but_producer_id_remains`]
//! - `TransactionsExpirationTest.testFatalErrorAfterInvalidProducerIdMappingWithTV2` →
//!   [`test_fatal_error_after_invalid_producer_id_mapping_with_tv2`]
//! - `TransactionsExpirationTest.testTransactionAfterProducerIdExpiresWithTV2` →
//!   [`test_transaction_after_producer_id_expires_with_tv2`]
//!
//! (the TV1 rows of `TransactionsExpirationTest` and
//! `ProducerIdExpirationTest.testDynamicProducerIdExpirationMs` are not reachable
//! on the pooled harness — see the section note above
//! [`producer_id_expiration_cluster`]).
//!
//! # Why the four Phase-8 scenarios are not translations
//!
//! Unlike the translated tests above, these four have no Java counterpart to
//! cite. Apache Kafka's transactional broker-integration coverage lives in the
//! Scala suites (`core/src/test/scala/integration/kafka/api/TransactionsTest.scala`
//! and friends), which were out of scope for the Phase-8 client port — the client-side
//! Java tests are `TransactionManagerTest` / `SenderTest` / `KafkaProducerTest`,
//! all of which drive a `MockClient` and are translated as unit tests. So these
//! four are specified by PLAN §Phase-8 rather than derived from a Java method, and
//! their job is to prove the translated client actually interoperates with a real
//! broker end to end.
//!
//! # Cluster sharing
//!
//! All four use [`kip848_3_broker`], the config the `PlaintextConsumer*` suites
//! already use, so they share its pooled container rather than starting one of
//! their own (`tests/common/cluster_pool.rs`). Three brokers is not incidental:
//! `transaction.state.log.replication.factor` defaults to 3, so `__transaction_state`
//! cannot be created on a single-broker cluster and `init_transactions` would fail.
//!
//! # Determinism
//!
//! Every *verification* consumer uses `assign` rather than `subscribe`, so no
//! rebalance is involved and the read is a pure fetch against a known partition.
//! ([`assigned_consumer`] only builds the consumer; the caller chooses `assign` or
//! `subscribe`.)
//! Only [`consume_transform_produce_with_offsets_inner`]'s input consumer
//! subscribes, because `send_offsets_to_transaction` needs real group metadata
//! (a generation and member id) for the broker to accept the `TxnOffsetCommit`.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use confluent_kafka::admin::Admin;
use confluent_kafka::admin::AdminClientConfig;
use confluent_kafka::admin::KafkaAdminClient;
use confluent_kafka::admin::OffsetSpec;
use confluent_kafka::admin::ProducerState;
use confluent_kafka::admin::TransactionState;
use confluent_kafka::common::Error;
use confluent_kafka::common::Errors;
use confluent_kafka::common::KafkaFuture;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::header::Header;
use confluent_kafka::common::header::Headers;
use confluent_kafka::common::header::RecordHeaders;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::common::serialization::Deserializer;
use confluent_kafka::consumer::CloseOptions;
use confluent_kafka::consumer::Consumer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::ConsumerGroupMetadata;
use confluent_kafka::consumer::ConsumerRecord;
use confluent_kafka::consumer::KafkaConsumer;
use confluent_kafka::consumer::OffsetAndMetadata;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::RecordMetadata;
use confluent_kafka::producer::{ProducerRecord, ProducerRecordOptionsBuilder};

use crate::common::backend_factory::ProducerBackendFactory;
use crate::common::cluster_config::{ClusterConfig, kip848_3_broker, txn_single_broker};
use crate::common::test_context::TestContext;
use crate::common::test_utils;

/// How long a read_committed consumer is given to surface records that should be
/// there. Generous because a commit has to be replicated to `__transaction_state`
/// and the markers written to the data partition before the LSO advances.
const CONSUME_DEADLINE: Duration = Duration::from_secs(30);

/// How long a read_committed consumer is polled to establish that records are
/// **not** there. This one is a *negative* assertion, so it is a fixed budget
/// rather than a deadline: too short and the test passes vacuously.
const NEGATIVE_POLL_BUDGET: Duration = Duration::from_secs(8);

fn cluster_config() -> ClusterConfig {
    // Two partitions, matching the `PlaintextConsumer*` suites, so the pooled
    // container is shared with them.
    kip848_3_broker(2)
}

struct ByteArrayDeserializer;

impl Deserializer<Vec<u8>> for ByteArrayDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Vec<u8>, Error> {
        Ok(data.to_vec())
    }
}

/// A transactional producer. `enable.idempotence` is implied by `transactional.id`.
fn transactional_producer(bootstrap: &str, transactional_id: &str) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("transactional.id".to_string(), transactional_id.to_string()),
        ("client.id".to_string(), format!("txn-producer-{transactional_id}")),
        ("acks".to_string(), "all".to_string()),
        ("max.block.ms".to_string(), "30000".to_string()),
        ("linger.ms".to_string(), "0".to_string()),
        ("transaction.timeout.ms".to_string(), "60000".to_string()),
    ]);
    KafkaProducer::new(
        ProducerConfig::new(&props).expect("invalid transactional producer config"),
        Box::new(ByteArraySerializer),
        Box::new(ByteArraySerializer),
    )
    .expect("failed to build a transactional producer")
}

/// The transactional-producer config as a flat property map. The multilanguage
/// factories forward this verbatim to `CreateProducer`, and `RustNativeFactory`
/// parses it with `ProducerConfig::new` — same keys as
/// [`transactional_producer`], which stays for the native-only scenarios.
fn make_txn_config(bootstrap: &str, transactional_id: &str) -> HashMap<String, String> {
    HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("transactional.id".to_string(), transactional_id.to_string()),
        ("client.id".to_string(), format!("txn-producer-{transactional_id}")),
        ("acks".to_string(), "all".to_string()),
        ("max.block.ms".to_string(), "30000".to_string()),
        ("linger.ms".to_string(), "0".to_string()),
        ("transaction.timeout.ms".to_string(), "60000".to_string()),
    ])
}

/// Pick the bootstrap address the factory's backend can reach: the gRPC backends
/// run in containers and need the broker's container listener, while native rust
/// uses the host loopback. Mirrors `producer_test.rs`.
fn bootstrap_for<F: ProducerBackendFactory>(factory: &F, ctx: &TestContext) -> String {
    if factory.needs_container_bootstrap() {
        ctx.container_bootstrap_servers().to_string()
    } else {
        ctx.bootstrap_servers().to_string()
    }
}

/// A plain (non-idempotent, non-transactional) producer, for seeding input topics.
fn plain_producer(bootstrap: &str, client_id: &str) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), client_id.to_string()),
        ("acks".to_string(), "all".to_string()),
        ("max.block.ms".to_string(), "30000".to_string()),
        ("linger.ms".to_string(), "0".to_string()),
    ]);
    KafkaProducer::new(
        ProducerConfig::new(&props).expect("invalid plain producer config"),
        Box::new(ByteArraySerializer),
        Box::new(ByteArraySerializer),
    )
    .expect("failed to build a plain producer")
}

/// A consumer for `group_id` at the given isolation level.
///
/// Callers either `assign` a partition (every verification consumer, so no rebalance
/// is involved) or `subscribe` — see the module docstring for which and why.
fn assigned_consumer(bootstrap: &str, group_id: &str, isolation_level: &str) -> Box<dyn Consumer<Vec<u8>, Vec<u8>>> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("group.id".to_string(), group_id.to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
        ("isolation.level".to_string(), isolation_level.to_string()),
        ("client.id".to_string(), format!("txn-consumer-{group_id}")),
    ]);
    KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        ConsumerConfig::new(&props).expect("invalid consumer config"),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("KafkaConsumer::new should succeed")
}

/// Sends `values` inside the current transaction, awaiting each ack.
///
/// Generic over the producer so the same helper drives the native
/// `KafkaProducer` (scenarios 1 and 4) and any [`ProducerBackendFactory`]
/// backend (the multilanguage atomicity scenarios). Because `P` is a type
/// parameter, `send` resolves to the trait method with no inherent-method
/// shadowing, so no UFCS is needed.
async fn send_all<P: Producer<Vec<u8>, Vec<u8>>>(producer: &P, topic: &str, partition: i32, values: &[&str]) {
    for value in values {
        let options = ProducerRecordOptionsBuilder::new()
            .set_topic(topic.to_string())
            .set_value(Some(value.as_bytes().to_vec()))
            .set_partition(Some(partition))
            .set_key(Some(format!("k-{value}").into_bytes()))
            .build()
            .unwrap();
        let record = ProducerRecord::with_options(options).expect("ProducerRecord::new should not fail");
        let future = producer.send(record).await.expect("send should be accepted");
        future
            .get_with_timeout(Duration::from_secs(30))
            .await
            .expect("the broker acked the record");
    }
}

/// Polls until `expected` values have been collected, or the deadline expires.
///
/// Returns the values in arrival order so a caller can assert on ordering as well
/// as membership.
async fn consume_values(
    consumer: &mut Box<dyn Consumer<Vec<u8>, Vec<u8>>>,
    expected: usize,
    deadline: Duration,
) -> Vec<String> {
    let start = Instant::now();
    let mut collected: Vec<String> = Vec::new();
    while collected.len() < expected && start.elapsed() < deadline {
        let records = consumer.poll(Duration::from_millis(500)).await.expect("poll should not fail");
        for record in records {
            let value = record.value().expect("the test always sends a value");
            collected.push(String::from_utf8(value.to_vec()).expect("the test sends UTF-8"));
        }
    }
    collected
}

/// Polls for a fixed budget and returns everything seen — used for the negative
/// assertions, where finishing early would make the assertion vacuous.
async fn drain_for(consumer: &mut Box<dyn Consumer<Vec<u8>, Vec<u8>>>, budget: Duration) -> Vec<String> {
    let start = Instant::now();
    let mut collected: Vec<String> = Vec::new();
    while start.elapsed() < budget {
        let records = consumer.poll(Duration::from_millis(500)).await.expect("poll should not fail");
        for record in records {
            let value = record.value().expect("the test always sends a value");
            collected.push(String::from_utf8(value.to_vec()).expect("the test sends UTF-8"));
        }
    }
    collected
}

// ---------------------------------------------------------------------------
// 1. Idempotent produce surviving a forced epoch bump
// ---------------------------------------------------------------------------

/// Produce continues correctly across an epoch bump the broker forces.
///
/// The scenario is two producers sharing a `transactional.id`. A second
/// `initTransactions` on that id makes the coordinator fence the previous
/// incarnation and hand the new one a higher epoch, so the bump is broker-real:
///
///   - the first commits a transaction at its original epoch;
///   - the second calls `init_transactions`, which bumps the epoch and aborts any
///     transaction the first left open;
///   - the second produces and commits at the bumped epoch;
///   - the first, now fenced, cannot produce;
///   - and all five committed records read back exactly once each, in order.
///
/// # What this does and does not pin
///
/// It pins the broker-side contract end to end: the coordinator issues a higher
/// epoch, the old incarnation is fenced, the new one is accepted, and no record is
/// lost or duplicated across the two.
///
/// It does **not** exercise the client-side sequence reset. The second incarnation
/// is a fresh [`KafkaProducer`], so its `TransactionManager` and `TxnPartitionMap`
/// start empty and its sequences are 0 because they were never anything else —
/// `bump_idempotent_producer_epoch`, `start_sequences_at_beginning` and
/// `request_idempotent_epoch_bump_for_partition` are all off this path, and no
/// arrangement of them changes the outcome here. An earlier revision of this
/// comment claimed "a client that failed to reset its sequences after the bump
/// would be rejected with `OutOfOrderSequenceNumber`", which is a property this
/// test cannot distinguish.
///
/// That path is covered at unit level, where the same producer instance survives
/// the bump and the reset is therefore observable:
/// `sender.rs::test_out_of_order_sequence_is_retried_and_bumps_the_epoch`,
/// `sender.rs::test_bump_transactional_epoch_on_unknown_producer_id_error`, and
/// `transaction_manager.rs::test_producer_id_reset`.
///
/// Restructuring so one incarnation survives a *broker-issued* bump would need
/// either a deliberately-induced abortable error or a cluster with
/// `transaction.version` finalized at 2 (where every `EndTxn` returns a bumped
/// epoch). The latter is a different `ClusterConfig`, so it would fork this suite
/// off the pooled container the `PlaintextConsumer*` tests share; the former is
/// awkward to induce reliably against a real broker. Neither is worth it when the
/// reset already has three unit tests — hence: keep the scenario, and describe
/// only what it proves.
///
/// (A second `initTransactions` is also not the *only* broker-driven bump — an
/// abort after an abortable error bumps through the coordinator too. It is the one
/// that is easy to induce, which is a reason to choose it and not a reason to call
/// it unique.)
#[tokio::test]
async fn test_idempotent_produce_survives_a_forced_epoch_bump() {
    let mut ctx = TestContext::new(cluster_config()).await;
    let topic = ctx.topic("txn-epoch-bump");
    let bootstrap = ctx.bootstrap_servers().to_string();
    let txn_id = format!("{topic}-txn-id");

    // First incarnation: a complete transaction at the original epoch.
    let first = transactional_producer(&bootstrap, &txn_id);
    first.init_transactions().await.expect("initTransactions");
    first.begin_transaction().expect("beginTransaction");
    send_all(&first, &topic, 0, &["before-bump-1", "before-bump-2"]).await;
    first.commit_transaction().await.expect("commitTransaction");

    // Second incarnation with the same transactional id: initTransactions bumps
    // the epoch and fences the first.
    let second = transactional_producer(&bootstrap, &txn_id);
    second.init_transactions().await.expect("initTransactions must bump the epoch");

    // The bumped producer produces and commits. Its sequence numbers start again
    // at 0 under the new epoch; a broker that saw stale sequences would reject
    // this with OUT_OF_ORDER_SEQUENCE_NUMBER.
    second.begin_transaction().expect("beginTransaction at the bumped epoch");
    send_all(&second, &topic, 0, &["after-bump-1", "after-bump-2", "after-bump-3"]).await;
    second
        .commit_transaction()
        .await
        .expect("commitTransaction at the bumped epoch");

    // The fenced producer can no longer produce.
    first.begin_transaction().expect("beginTransaction is a local state change");
    let options = ProducerRecordOptionsBuilder::new()
        .set_topic(topic.clone())
        .set_value(Some(b"fenced".to_vec()))
        .set_partition(Some(0))
        .set_key(Some(b"k-fenced".to_vec()))
        .build()
        .unwrap();
    let record = ProducerRecord::with_options(options).expect("ProducerRecord::new should not fail");
    let sent = <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(&first, record).await;
    let fenced = match sent {
        // The fencing may be reported synchronously (the manager already knows it
        // is fenced) or asynchronously through the record's future, depending on
        // whether the EndTxn or the Produce is the first request to learn of it.
        Err(error) => error,
        Ok(future) => future
            .get_with_timeout(Duration::from_secs(30))
            .await
            .expect_err("a fenced producer must not be able to produce"),
    };
    // The wire code is `INVALID_PRODUCER_EPOCH` when the Produce is what learns of
    // the fencing, and `PRODUCER_FENCED` when a transactional request gets there
    // first — both are Java's "this producer has been superseded", and which one
    // arrives depends on request ordering rather than on anything under test.
    assert!(
        matches!(fenced.error(), Errors::InvalidProducerEpoch | Errors::ProducerFenced),
        "a fenced producer's send must fail with a fencing error, got {fenced} ({:?})",
        fenced.error()
    );

    // Exactly the five committed records are readable, in order, with no
    // duplicates — the two from before the bump and the three from after.
    let group = ctx.group_id("txn-epoch-bump-verify");
    let mut consumer = assigned_consumer(&bootstrap, &group, "read_committed");
    let tp = TopicPartition::new(topic.clone(), 0);
    consumer.assign(vec![tp]).await.expect("assign");
    let values = consume_values(&mut consumer, 5, CONSUME_DEADLINE).await;
    assert_eq!(
        values,
        vec![
            "before-bump-1".to_string(),
            "before-bump-2".to_string(),
            "after-bump-1".to_string(),
            "after-bump-2".to_string(),
            "after-bump-3".to_string(),
        ],
        "the committed records across the epoch bump must be exactly these, once each"
    );

    consumer.close().await.expect("close");
    ctx.cleanup().await;
}

// ---------------------------------------------------------------------------
// 2. A transactional commit is visible only after the commit
// ---------------------------------------------------------------------------

/// Records written inside an open transaction are invisible to a
/// `read_committed` consumer and visible to a `read_uncommitted` one; committing
/// makes them visible to both.
///
/// # Two independent ways this could pass vacuously, and the gate for each
///
/// *Duration*: too short a negative poll and the records simply had not arrived.
/// Handled by [`NEGATIVE_POLL_BUDGET`] being a fixed budget rather than a deadline.
///
/// *Liveness*: a `read_committed` reader that has not yet resolved metadata or
/// reset its position returns empty for reasons unrelated to the isolation level,
/// and the assertion passes anyway. The `read_uncommitted` pairing does **not**
/// close this — it proves the records are on the broker (which the awaited `send_all`
/// acks already proved), not that the `read_committed` reader was live during its
/// own budget. So one non-transactional record is seeded *before* the transaction
/// opens and the negative drain must return exactly that record: the reader is
/// proven to have fetched from the partition, and the transactional records are
/// proven absent, in a single assertion.
///
/// (`test_aborted_transaction_records_are_discarded` gets the same gate for free —
/// its negative drain runs on a consumer that has already delivered records.)
async fn transactional_records_are_visible_only_after_commit_inner<F: ProducerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let topic = ctx.topic("txn-visible-after-commit");
    let bootstrap = ctx.bootstrap_servers().to_string();
    let producer_bootstrap = bootstrap_for(factory, ctx);
    let tp = TopicPartition::new(topic.clone(), 0);

    // The liveness seed: one non-transactional record, before any transaction opens.
    // A read_committed consumer must always see this one, so its presence in the
    // negative drain below proves the reader actually fetched.
    let seeder = plain_producer(&bootstrap, "txn-visible-seed");
    send_all(&seeder, &topic, 0, &["seed"]).await;

    let producer = factory
        .create(make_txn_config(&producer_bootstrap, &format!("{topic}-txn-id")))
        .await
        .expect("create transactional producer");
    producer.init_transactions().await.expect("initTransactions");
    producer.begin_transaction().expect("beginTransaction");
    send_all(&producer, &topic, 0, &["v1", "v2", "v3"]).await;

    // Before the commit: read_committed sees the seed and nothing else.
    let committed_group = ctx.group_id("txn-visible-committed");
    let mut committed_reader = assigned_consumer(&bootstrap, &committed_group, "read_committed");
    committed_reader.assign(vec![tp.clone()]).await.expect("assign");
    let before = drain_for(&mut committed_reader, NEGATIVE_POLL_BUDGET).await;
    assert_eq!(
        before,
        vec!["seed".to_string()],
        "a read_committed consumer must see the seed (proving it fetched) and none of the \
         open transaction's records"
    );

    // Before the commit: read_uncommitted does see them, so they are genuinely
    // on the broker.
    let uncommitted_group = ctx.group_id("txn-visible-uncommitted");
    let mut uncommitted_reader = assigned_consumer(&bootstrap, &uncommitted_group, "read_uncommitted");
    uncommitted_reader.assign(vec![tp.clone()]).await.expect("assign");
    let uncommitted = consume_values(&mut uncommitted_reader, 4, CONSUME_DEADLINE).await;
    assert_eq!(
        uncommitted,
        vec!["seed".to_string(), "v1".to_string(), "v2".to_string(), "v3".to_string()],
        "a read_uncommitted consumer must see the open transaction's records"
    );

    // Commit, and the same read_committed consumer now sees them. It has already
    // consumed the seed, so only the three transactional records remain for it.
    producer.commit_transaction().await.expect("commitTransaction");
    let after = consume_values(&mut committed_reader, 3, CONSUME_DEADLINE).await;
    assert_eq!(
        after,
        vec!["v1".to_string(), "v2".to_string(), "v3".to_string()],
        "the committed records must become visible to read_committed"
    );

    producer.close().await.expect("close");
    committed_reader.close().await.expect("close");
    uncommitted_reader.close().await.expect("close");
    ctx.cleanup().await;
}

// ---------------------------------------------------------------------------
// 3. An abort discards its records
// ---------------------------------------------------------------------------

/// An aborted transaction's records never become visible to `read_committed`,
/// and a transaction committed afterwards on the same partition still is.
///
/// This is the first thing in the tree to put a **real abort marker** on a
/// partition that the translated consumer then reads, so it is also the
/// integration cover for `CompletedFetch`'s abort-marker handling. Committing a
/// second transaction after the abort is deliberate: it forces the consumer past
/// the abort marker rather than merely stopping at it, which is where a broken
/// marker skip would show up as either a hang or a leaked `aborted-1`.
async fn aborted_transaction_records_are_discarded_inner<F: ProducerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let topic = ctx.topic("txn-abort-discards");
    let bootstrap = ctx.bootstrap_servers().to_string();
    let producer_bootstrap = bootstrap_for(factory, ctx);
    let tp = TopicPartition::new(topic.clone(), 0);

    let producer = factory
        .create(make_txn_config(&producer_bootstrap, &format!("{topic}-txn-id")))
        .await
        .expect("create transactional producer");
    producer.init_transactions().await.expect("initTransactions");

    // Transaction 1: aborted.
    producer.begin_transaction().expect("beginTransaction");
    send_all(&producer, &topic, 0, &["aborted-1", "aborted-2"]).await;
    producer.abort_transaction().await.expect("abortTransaction");

    // Transaction 2: committed, so the consumer must read *past* the abort marker.
    producer.begin_transaction().expect("beginTransaction");
    send_all(&producer, &topic, 0, &["committed-1", "committed-2"]).await;
    producer.commit_transaction().await.expect("commitTransaction");

    let group = ctx.group_id("txn-abort-verify");
    let mut consumer = assigned_consumer(&bootstrap, &group, "read_committed");
    consumer.assign(vec![tp.clone()]).await.expect("assign");

    let values = consume_values(&mut consumer, 2, CONSUME_DEADLINE).await;
    assert_eq!(
        values,
        vec!["committed-1".to_string(), "committed-2".to_string()],
        "only the committed transaction's records may be delivered; the aborted ones must be dropped"
    );

    // Nothing further arrives — in particular no aborted record trailing behind
    // the marker.
    let extra = drain_for(&mut consumer, NEGATIVE_POLL_BUDGET).await;
    assert!(extra.is_empty(), "no further records were expected, got {extra:?}");

    // A read_uncommitted consumer sees all four, which is what proves the aborted
    // records were written and then filtered rather than never sent.
    let all_group = ctx.group_id("txn-abort-verify-all");
    let mut all_reader = assigned_consumer(&bootstrap, &all_group, "read_uncommitted");
    all_reader.assign(vec![tp]).await.expect("assign");
    let all = consume_values(&mut all_reader, 4, CONSUME_DEADLINE).await;
    assert_eq!(
        all,
        vec![
            "aborted-1".to_string(),
            "aborted-2".to_string(),
            "committed-1".to_string(),
            "committed-2".to_string(),
        ],
        "read_uncommitted must see the aborted records too"
    );

    producer.close().await.expect("close");
    consumer.close().await.expect("close");
    all_reader.close().await.expect("close");
    ctx.cleanup().await;
}

// ---------------------------------------------------------------------------
// 4. Consume-transform-produce with `send_offsets_to_transaction`
// ---------------------------------------------------------------------------

/// The read-process-write loop: consume from an input topic, produce the
/// transformed records to an output topic, and commit the consumed offsets
/// *inside the same transaction* via `send_offsets_to_transaction`.
///
/// Two things are asserted after the commit, and both are needed for
/// exactly-once: the transformed records are visible to a `read_committed`
/// consumer, and the input group's committed offset has advanced to the end of
/// what was consumed. The second is the half that only
/// `send_offsets_to_transaction` can deliver — a plain `commit_sync` would also
/// move it, but not atomically with the output records.
///
/// It also carries `ProducerIntegrationTest.testTransactionWithSendOffset`'s
/// final check (`ProducerIntegrationTest.java:194-199`): the admin client
/// eventually lists the transactional id in `COMPLETE_COMMIT`.
async fn consume_transform_produce_with_offsets_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let input_topic = ctx.topic("txn-ctp-input");
    let output_topic = ctx.topic("txn-ctp-output");
    let bootstrap = ctx.bootstrap_servers().to_string();
    let producer_bootstrap = bootstrap_for(factory, ctx);
    let input_tp = TopicPartition::new(input_topic.clone(), 0);

    // Seed the input topic with a plain (non-transactional) producer.
    let seeder = plain_producer(&bootstrap, "txn-ctp-seed");
    send_all(&seeder, &input_topic, 0, &["a", "b", "c"]).await;

    // The transform consumer subscribes rather than assigns: it must have real
    // group metadata (generation + member id) for the coordinator to accept the
    // TxnOffsetCommit the producer sends on its behalf.
    let input_group = ctx.group_id("txn-ctp-group");
    let mut input_consumer = assigned_consumer(&bootstrap, &input_group, "read_committed");
    input_consumer
        .subscribe_with_topics(vec![input_topic.clone()])
        .await
        .expect("subscribe");

    let consumed = consume_values(&mut input_consumer, 3, CONSUME_DEADLINE).await;
    assert_eq!(
        consumed,
        vec!["a".to_string(), "b".to_string(), "c".to_string()],
        "the input records must be consumed before they can be transformed"
    );
    let next_offset = input_consumer.position(&input_tp).await.expect("position");
    assert_eq!(next_offset, 3, "three records consumed, so the next offset is 3");

    // Transform and produce inside a transaction, committing the input offsets
    // with it. Only the transactional producer crosses the gRPC boundary; the
    // input transform-consumer (whose group_metadata() feeds
    // send_offsets_to_transaction) and every verification consumer stay native.
    let txn_id = format!("{output_topic}-txn-id");
    let producer = factory
        .create(make_txn_config(&producer_bootstrap, &txn_id))
        .await
        .expect("create transactional producer");
    producer.init_transactions().await.expect("initTransactions");
    producer.begin_transaction().expect("beginTransaction");
    let transformed: Vec<String> = consumed.iter().map(|value| value.to_uppercase()).collect();
    let transformed_refs: Vec<&str> = transformed.iter().map(String::as_str).collect();
    send_all(&producer, &output_topic, 0, &transformed_refs).await;

    let mut offsets = HashMap::new();
    offsets.insert(
        input_tp.clone(),
        OffsetAndMetadata::new(next_offset).expect("a non-negative offset"),
    );
    producer
        .send_offsets_to_transaction(offsets, input_consumer.group_metadata())
        .await
        .expect("sendOffsetsToTransaction");
    producer.commit_transaction().await.expect("commitTransaction");

    // Java `ProducerIntegrationTest.testTransactionWithSendOffset`
    // (`ProducerIntegrationTest.java:194-199`): the committed transaction is
    // eventually listed by the coordinator in `COMPLETE_COMMIT`. The admin client
    // is native harness infrastructure, so this holds for every backend arm.
    let admin = txn_test_admin(ctx);
    test_utils::wait_until_true_with_timeout(
        || {
            let listing = admin.list_transactions().all();
            let txn_id = txn_id.clone();
            async move {
                listing.get().await.is_ok_and(|listings| {
                    listings
                        .iter()
                        .filter(|txn| txn.transactional_id() == txn_id)
                        .any(|txn| txn.state() == TransactionState::CompleteCommit)
                })
            }
        },
        "transaction is not in COMPLETE_COMMIT state",
        test_utils::DEFAULT_MAX_WAIT_MS,
        test_utils::DEFAULT_PAUSE_MS,
    )
    .await;
    admin.close().await;

    // The transformed records are visible to read_committed.
    let output_group = ctx.group_id("txn-ctp-output-verify");
    let mut output_consumer = assigned_consumer(&bootstrap, &output_group, "read_committed");
    output_consumer
        .assign(vec![TopicPartition::new(output_topic.clone(), 0)])
        .await
        .expect("assign");
    let output = consume_values(&mut output_consumer, 3, CONSUME_DEADLINE).await;
    assert_eq!(
        output,
        vec!["A".to_string(), "B".to_string(), "C".to_string()],
        "the transformed records must be visible after the commit"
    );

    // And the input group's committed offset advanced as part of the same
    // transaction. Read it from a *different* consumer in the same group, so the
    // assertion is against what the coordinator stored rather than against local
    // state.
    let mut offset_reader = assigned_consumer(&bootstrap, &input_group, "read_committed");
    offset_reader.assign(vec![input_tp.clone()]).await.expect("assign");
    let committed = offset_reader
        .committed(std::slice::from_ref(&input_tp))
        .await
        .expect("committed");
    assert_eq!(
        committed.get(&input_tp).map(OffsetAndMetadata::offset),
        Some(next_offset),
        "sendOffsetsToTransaction must have committed the input offsets with the transaction"
    );

    input_consumer.close().await.expect("close");
    output_consumer.close().await.expect("close");
    offset_reader.close().await.expect("close");
    ctx.cleanup().await;
}

// ---------------------------------------------------------------------------
// TransactionsTest.scala translations (consumer arm): fencing & state errors
// ---------------------------------------------------------------------------
//
// These five are translations of `core/src/test/scala/integration/kafka/api/
// TransactionsTest.scala` (AK 4.3.1), CONSUMER (KIP-848) arm only. They are
// native-only: fencing needs two producers sharing a `transactional.id`, which
// the multilanguage harness does not model (one producer per id).
//
// Deviations shared by all five, each forced by the pooled harness:
//
//   - Java's fixed `topic1` / `topic2`, `"transactional-producer"`,
//     `"normalProducer"` and `"transactional-group"` become per-test names
//     (`ctx.topic` for topics, `ctx.group_id` for groups and transactional ids), because the cluster is pooled across tests
//     here and is not in Java.
//   - The cluster is the shared [`kip848_3_broker`] config rather than Java's
//     `overridingProps()` (`TransactionsTest.scala:60-74`). The differences do not
//     reach these tests: auto-create is irrelevant because the topics are created
//     explicitly through the admin client, and `__transaction_state` keeps the
//     broker defaults (50 partitions, RF 3, min ISR 2) instead of Java's
//     (3, 2, 2) — a coordinator placement detail, not an observable contract.
//   - Java's `setUp` pre-creates `transactionalProducerCount` producers and
//     `transactionalConsumerCount` consumers (`TransactionsTest.scala:93-104`);
//     each test here builds the ones it uses.

/// Java's `numPartitions` (`TransactionsTest.scala:55`).
const TXN_TEST_NUM_PARTITIONS: i32 = 4;
/// Java's `brokerCount` (`TransactionsTest.scala:47`), used as the replication
/// factor of `topic1` / `topic2` (`TransactionsTest.scala:95-96`).
const TXN_TEST_REPLICATION_FACTOR: i16 = 3;

/// `TestUtils.transactionStatusKey` / `committedValue` / `abortedValue`
/// (`TestUtils.scala:103-105`).
const TRANSACTION_STATUS_KEY: &str = "transactionStatus";
const COMMITTED_VALUE: &[u8] = b"committed";
const ABORTED_VALUE: &[u8] = b"aborted";

/// Admin client for topic provisioning. Always native: it is harness
/// infrastructure, not the client under test.
fn txn_test_admin(ctx: &TestContext) -> Box<dyn Admin> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), ctx.bootstrap_servers().to_string()),
        ("client.id".to_string(), "transactions-test-admin".to_string()),
        ("request.timeout.ms".to_string(), "30000".to_string()),
        ("default.api.timeout.ms".to_string(), "30000".to_string()),
    ]);
    Box::new(KafkaAdminClient::new(AdminClientConfig::new(&props).expect("valid admin config")).expect("admin client"))
}

/// `TransactionsTest.setUp`'s topic creation (`TransactionsTest.scala:95-96`):
/// `createTopic(topic, numPartitions, brokerCount, topicConfig())` with
/// `min.insync.replicas=2` (`topicConfig`, `TransactionsTest.scala:85-89`).
///
/// Java's `createTopic` returns only once every broker knows every partition's
/// leader; [`test_utils::wait_for_partition_leaders`] recovers that half, which
/// [`test_utils::create_topic_with_configs`] alone cannot prove.
async fn create_txn_test_topics(ctx: &mut TestContext) -> (String, String) {
    let topic1 = ctx.topic("topic1");
    let topic2 = ctx.topic("topic2");
    let admin = txn_test_admin(ctx);
    for topic in [&topic1, &topic2] {
        let topic_config = BTreeMap::from([("min.insync.replicas".to_string(), "2".to_string())]);
        test_utils::create_topic_with_configs(
            admin.as_ref(),
            topic,
            TXN_TEST_NUM_PARTITIONS,
            TXN_TEST_REPLICATION_FACTOR,
            topic_config,
        )
        .await;
        test_utils::wait_for_partition_leaders(admin.as_ref(), topic, 0..TXN_TEST_NUM_PARTITIONS).await;
    }
    admin.close().await;
    (topic1, topic2)
}

/// `TestUtils.createTransactionalProducer` with `TransactionsTest`'s defaults
/// (`TransactionsTest.scala:1089-1104`, `TestUtils.scala:1206-1227`).
fn create_transactional_producer(bootstrap: &str, transactional_id: &str) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    create_transactional_producer_with_transaction_timeout_ms(bootstrap, transactional_id, 60000)
}

/// [`create_transactional_producer`] with Java's `transactionTimeoutMs` parameter
/// (`TransactionsTest.scala:1090`) overridden.
fn create_transactional_producer_with_transaction_timeout_ms(
    bootstrap: &str,
    transactional_id: &str,
    transaction_timeout_ms: u64,
) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("acks".to_string(), "all".to_string()),
        ("batch.size".to_string(), "16384".to_string()),
        ("transactional.id".to_string(), transactional_id.to_string()),
        ("enable.idempotence".to_string(), "true".to_string()),
        ("transaction.timeout.ms".to_string(), transaction_timeout_ms.to_string()),
        ("max.block.ms".to_string(), "60000".to_string()),
        ("delivery.timeout.ms".to_string(), "120000".to_string()),
        ("request.timeout.ms".to_string(), "30000".to_string()),
        ("max.in.flight.requests.per.connection".to_string(), "5".to_string()),
    ]);
    KafkaProducer::new(
        ProducerConfig::new(&props).expect("invalid transactional producer config"),
        Box::new(ByteArraySerializer),
        Box::new(ByteArraySerializer),
    )
    .expect("failed to build a transactional producer")
}

/// `TransactionsTest.createReadCommittedConsumer` (`TransactionsTest.scala:1067-1078`)
/// → `TestUtils.createConsumer(..., enableAutoCommit = false, readCommitted = true)`
/// (`TestUtils.scala:551-573`): `auto.offset.reset=earliest`, the CONSUMER arm.
fn create_read_committed_consumer(bootstrap: &str, group_id: &str) -> Box<dyn Consumer<Vec<u8>, Vec<u8>>> {
    assigned_consumer(bootstrap, group_id, "read_committed")
}

/// `TestUtils.createConsumer(bootstrapServers, groupProtocol, groupId,
/// enableAutoCommit = false, readCommitted, maxPollRecords)` plus `extra`
/// properties (`TestUtils.scala:551-573`), CONSUMER arm. This is the full shape
/// `TransactionsTest.createReadCommittedConsumer(group, maxPollRecords, props)`
/// (`TransactionsTest.scala:1067-1078`) and `createReadUncommittedConsumer`
/// (`TransactionsTest.scala:1080-1087`) reduce to.
fn create_consumer(
    bootstrap: &str,
    group_id: &str,
    read_committed: bool,
    max_poll_records: usize,
    extra: &[(&str, &str)],
) -> Box<dyn Consumer<Vec<u8>, Vec<u8>>> {
    let mut props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("group.id".to_string(), group_id.to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
        ("max.poll.records".to_string(), max_poll_records.to_string()),
        (
            "isolation.level".to_string(),
            if read_committed {
                "read_committed"
            } else {
                "read_uncommitted"
            }
            .to_string(),
        ),
    ]);
    props.extend(extra.iter().map(|(key, value)| (key.to_string(), value.to_string())));
    KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        ConsumerConfig::new(&props).expect("invalid consumer config"),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("KafkaConsumer::new should succeed")
}

/// `TransactionsTest.createReadUncommittedConsumer(group)`
/// (`TransactionsTest.scala:1080-1087`).
fn create_read_uncommitted_consumer(bootstrap: &str, group_id: &str) -> Box<dyn Consumer<Vec<u8>, Vec<u8>>> {
    create_consumer(bootstrap, group_id, false, 500, &[])
}

/// `TestUtils.producerRecordWithExpectedTransactionStatus(topic, partition, key,
/// value, willBeCommitted)` (`TestUtils.scala:1270-1282`): a record carrying a
/// `transactionStatus` header naming the outcome the test expects.
fn producer_record_with_expected_transaction_status(
    topic: &str,
    partition: Option<i32>,
    key: &str,
    value: &str,
    will_be_committed: bool,
) -> ProducerRecord<Vec<u8>, Vec<u8>> {
    let mut headers = RecordHeaders::new();
    let status = if will_be_committed {
        COMMITTED_VALUE
    } else {
        ABORTED_VALUE
    };
    headers.add_key_value(TRANSACTION_STATUS_KEY, Some(status)).expect("add header");
    ProducerRecord::with_partition_key_headers(
        topic.to_string(),
        partition,
        Some(key.as_bytes().to_vec()),
        Some(value.as_bytes().to_vec()),
        headers,
    )
    .expect("ProducerRecord::with_partition_key_headers should succeed")
}

/// `KafkaProducer::send` through the `Producer` trait (the inherent method of the
/// same name would otherwise shadow it).
async fn send_record(
    producer: &KafkaProducer<Vec<u8>, Vec<u8>>,
    record: ProducerRecord<Vec<u8>, Vec<u8>>,
) -> Result<KafkaFuture<RecordMetadata>, Error> {
    <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(producer, record).await
}

/// `TestUtils.consumeRecords(consumer, numRecords)` (`TestUtils.scala:1198-1204`):
/// poll until at least `num_records` arrive within `DEFAULT_MAX_WAIT_MS`, then
/// assert exactly that many were consumed.
async fn consume_records(
    consumer: &mut Box<dyn Consumer<Vec<u8>, Vec<u8>>>,
    num_records: usize,
) -> Vec<ConsumerRecord<Vec<u8>, Vec<u8>>> {
    let records = poll_until_at_least_num_records(consumer, num_records).await;
    assert_eq!(num_records, records.len(), "Consumed more records than expected");
    records
}

/// `TestUtils.pollUntilAtLeastNumRecords(consumer, numRecords)`
/// (`TestUtils.scala:1184-1196`): poll (100 ms each, `pollRecordsUntilTrue`'s
/// default) until at least `num_records` arrive within `DEFAULT_MAX_WAIT_MS`.
async fn poll_until_at_least_num_records(
    consumer: &mut Box<dyn Consumer<Vec<u8>, Vec<u8>>>,
    num_records: usize,
) -> Vec<ConsumerRecord<Vec<u8>, Vec<u8>>> {
    let deadline = Duration::from_millis(test_utils::DEFAULT_MAX_WAIT_MS);
    let start = Instant::now();
    let mut records = Vec::new();
    while records.len() < num_records {
        assert!(
            start.elapsed() < deadline,
            "Consumed {} records before timeout instead of the expected {num_records} records",
            records.len()
        );
        records.extend(consumer.poll(Duration::from_millis(100)).await.expect("poll should not fail"));
    }
    records
}

/// `TestUtils.assertCommittedAndGetValue` (`TestUtils.scala:1255-1264`).
fn assert_committed_and_get_value(record: &ConsumerRecord<Vec<u8>, Vec<u8>>) -> String {
    match record.headers().headers(TRANSACTION_STATUS_KEY).first() {
        Some(header) => assert_eq!(
            String::from_utf8_lossy(COMMITTED_VALUE),
            String::from_utf8_lossy(header.value().unwrap_or_default()),
            "Got {} but expected the value to indicate committed status.",
            String::from_utf8_lossy(header.value().unwrap_or_default())
        ),
        None => panic!("expected the record header to include an expected transaction status, but received nothing."),
    }
    String::from_utf8(record.value().expect("the test always sends a value").clone()).expect("the test sends UTF-8")
}

/// Java's `assertThrows(classOf[ProducerFencedException], ...)`. Java asserts the
/// class only; the message is the error code's fixed default text (the broker sends
/// no custom one), so it is asserted too.
fn assert_producer_fenced(error: &Error) {
    assert!(
        matches!(error, Error::ProducerFenced(_)),
        "expected ProducerFenced, got {error:?}"
    );
    assert_eq!(error.message(), Errors::ProducerFenced.message());
}

/// The fencing prologue shared by `testFencingOnCommit` / `testFencingOnSendOffsets`
/// (`TransactionsTest.scala:385-404` / `417-436`): p1 opens a transaction and
/// flushes two records that must end up aborted, then p2 (same
/// `transactional.id`) initializes — fencing p1 and aborting its open
/// transaction — begins its own and sends the two records that will commit.
async fn fence_p1_with_p2(
    producer1: &KafkaProducer<Vec<u8>, Vec<u8>>,
    producer2: &KafkaProducer<Vec<u8>, Vec<u8>>,
    topic1: &str,
    topic2: &str,
) {
    producer1.init_transactions().await.expect("producer1.initTransactions");

    producer1.begin_transaction().expect("producer1.beginTransaction");
    send_record(
        producer1,
        producer_record_with_expected_transaction_status(topic1, None, "1", "1", false),
    )
    .await
    .expect("producer1 send");
    send_record(
        producer1,
        producer_record_with_expected_transaction_status(topic2, None, "3", "3", false),
    )
    .await
    .expect("producer1 send");
    producer1.flush().await.expect("producer1.flush");

    producer2
        .init_transactions()
        .await
        .expect("producer2.initTransactions: ok, will abort the open transaction.");
    producer2.begin_transaction().expect("producer2.beginTransaction");
    send_record(
        producer2,
        producer_record_with_expected_transaction_status(topic1, None, "2", "4", true),
    )
    .await
    .expect("producer2 send");
    send_record(
        producer2,
        producer_record_with_expected_transaction_status(topic2, None, "2", "4", true),
    )
    .await
    .expect("producer2 send");
}

/// A fenced producer's `commitTransaction` fails with `ProducerFencedException`;
/// the fencing producer's commit succeeds and only its records are visible.
///
/// Translates `TransactionsTest.testFencingOnCommit` (`TransactionsTest.scala:383-409`).
#[tokio::test]
async fn test_fencing_on_commit() {
    let mut ctx = TestContext::new(cluster_config()).await;
    let (topic1, topic2) = create_txn_test_topics(&mut ctx).await;
    let bootstrap = ctx.bootstrap_servers().to_string();
    let txn_id = ctx.group_id("transactional-producer");
    let producer1 = create_transactional_producer(&bootstrap, &txn_id);
    let producer2 = create_transactional_producer(&bootstrap, &txn_id);
    let mut consumer = create_read_committed_consumer(&bootstrap, &ctx.group_id("transactional-group"));

    consumer
        .subscribe_with_topics(vec![topic1.clone(), topic2.clone()])
        .await
        .expect("subscribe");

    fence_p1_with_p2(&producer1, &producer2, &topic1, &topic2).await;

    let fenced = producer1
        .commit_transaction()
        .await
        .expect_err("a fenced producer's commitTransaction must fail");
    assert_producer_fenced(&fenced);

    producer2.commit_transaction().await.expect("producer2.commitTransaction: ok");

    let records = consume_records(&mut consumer, 2).await;
    for record in &records {
        assert_committed_and_get_value(record);
    }

    consumer.close().await.expect("consumer close");
    producer1.close().await.expect("producer1 close");
    producer2.close().await.expect("producer2 close");
    ctx.cleanup().await;
}

/// A fenced producer's `sendOffsetsToTransaction` fails with
/// `ProducerFencedException`; the fencing producer's commit succeeds.
///
/// Translates `TransactionsTest.testFencingOnSendOffsets` (`TransactionsTest.scala:415-443`).
///
/// `ConsumerGroupMetadata::new` is deprecated exactly as Java's constructor is;
/// Java suppresses the warning with `@SuppressWarnings(Array("removal"))`.
#[tokio::test]
#[allow(deprecated)]
async fn test_fencing_on_send_offsets() {
    let mut ctx = TestContext::new(cluster_config()).await;
    let (topic1, topic2) = create_txn_test_topics(&mut ctx).await;
    let bootstrap = ctx.bootstrap_servers().to_string();
    let txn_id = ctx.group_id("transactional-producer");
    let producer1 = create_transactional_producer(&bootstrap, &txn_id);
    let producer2 = create_transactional_producer(&bootstrap, &txn_id);
    let mut consumer = create_read_committed_consumer(&bootstrap, &ctx.group_id("transactional-group"));

    consumer
        .subscribe_with_topics(vec![topic1.clone(), topic2.clone()])
        .await
        .expect("subscribe");

    fence_p1_with_p2(&producer1, &producer2, &topic1, &topic2).await;

    let fenced = producer1
        .send_offsets_to_transaction(
            HashMap::from([(
                TopicPartition::new(topic1.clone(), 0),
                OffsetAndMetadata::new(110).expect("valid offset"),
            )]),
            ConsumerGroupMetadata::new("foobarGroup"),
        )
        .await
        .expect_err("a fenced producer's sendOffsetsToTransaction must fail");
    assert_producer_fenced(&fenced);

    producer2.commit_transaction().await.expect("producer2.commitTransaction: ok");

    let records = consume_records(&mut consumer, 2).await;
    for record in &records {
        assert_committed_and_get_value(record);
    }

    consumer.close().await.expect("consumer close");
    producer1.close().await.expect("producer1 close");
    producer2.close().await.expect("producer2 close");
    ctx.cleanup().await;
}

/// A producer fenced while its transaction is open cannot send: the failure is
/// either a synchronous `ProducerFencedException` or an `InvalidProducerEpochException`
/// through the record's future. The fencing producer's commit succeeds.
///
/// Translates `TransactionsTest.testFencingOnSend` (`TransactionsTest.scala:515-555`).
#[tokio::test]
async fn test_fencing_on_send() {
    let mut ctx = TestContext::new(cluster_config()).await;
    let (topic1, topic2) = create_txn_test_topics(&mut ctx).await;
    let bootstrap = ctx.bootstrap_servers().to_string();
    let txn_id = ctx.group_id("transactional-producer");
    let producer1 = create_transactional_producer(&bootstrap, &txn_id);
    let producer2 = create_transactional_producer(&bootstrap, &txn_id);
    let mut consumer = create_read_committed_consumer(&bootstrap, &ctx.group_id("transactional-group"));

    consumer
        .subscribe_with_topics(vec![topic1.clone(), topic2.clone()])
        .await
        .expect("subscribe");

    producer1.init_transactions().await.expect("producer1.initTransactions");

    producer1.begin_transaction().expect("producer1.beginTransaction");
    send_record(
        &producer1,
        producer_record_with_expected_transaction_status(&topic1, None, "1", "1", false),
    )
    .await
    .expect("producer1 send");
    send_record(
        &producer1,
        producer_record_with_expected_transaction_status(&topic2, None, "3", "3", false),
    )
    .await
    .expect("producer1 send");

    producer2
        .init_transactions()
        .await
        .expect("producer2.initTransactions: ok, will abort the open transaction.");
    producer2.begin_transaction().expect("producer2.beginTransaction");
    for topic in [&topic1, &topic2] {
        send_record(
            &producer2,
            producer_record_with_expected_transaction_status(topic, None, "2", "4", true),
        )
        .await
        .expect("producer2 send")
        .get()
        .await
        .expect("producer2's record is acked");
    }

    // Java: `producer1.send(...)` then `result.get()`, catching
    // `ProducerFencedException` (thrown by `send` itself) or an
    // `ExecutionException` whose cause must be `InvalidProducerEpochException`;
    // a successful send or any other error fails the test.
    match send_record(
        &producer1,
        producer_record_with_expected_transaction_status(&topic1, None, "1", "5", false),
    )
    .await
    {
        Err(error @ Error::ProducerFenced(_)) => assert_producer_fenced(&error),
        Err(other) => panic!("Got an unexpected error from a fenced producer: {other:?}"),
        Ok(future) => match future.get().await {
            Ok(metadata) => panic!(
                "Should not be able to send messages from a fenced producer. Missed a producer fenced error \
                 when writing to {}-{}.",
                metadata.topic(),
                metadata.partition()
            ),
            Err(error) => {
                assert!(
                    matches!(error, Error::InvalidProducerEpoch(_)),
                    "expected the send future to fail with InvalidProducerEpoch, got {error:?}"
                );
                assert_eq!(error.message(), Errors::InvalidProducerEpoch.message());
            },
        },
    }

    producer2.commit_transaction().await.expect("producer2.commitTransaction: ok");

    let records = consume_records(&mut consumer, 2).await;
    for record in &records {
        assert_committed_and_get_value(record);
    }

    consumer.close().await.expect("consumer close");
    producer1.close().await.expect("producer1 close");
    producer2.close().await.expect("producer2 close");
    ctx.cleanup().await;
}

/// A second `initTransactions` on an already-initialized producer fails with
/// `IllegalStateException` (the READY → INITIALIZING transition is invalid).
///
/// Translates `TransactionsTest.testConsecutivelyRunInitTransactions`
/// (`TransactionsTest.scala:692-698`). Java asserts only the class; the message is
/// `TransactionManager.transitionTo`'s deterministic text, so it is asserted too.
#[tokio::test]
async fn test_consecutively_run_init_transactions() {
    let mut ctx = TestContext::new(cluster_config()).await;
    let bootstrap = ctx.bootstrap_servers().to_string();
    let txn_id = ctx.group_id("normalProducer");
    let producer = create_transactional_producer(&bootstrap, &txn_id);

    producer.init_transactions().await.expect("initTransactions");
    let error = producer
        .init_transactions()
        .await
        .expect_err("a second initTransactions must fail");
    match &error {
        Error::LocalIllegalState(e) => assert_eq!(
            e.message(),
            format!("TransactionalId {txn_id}: Invalid transition attempted from state READY to state INITIALIZING"),
        ),
        other => panic!("expected LocalIllegalState, got {other:?}"),
    }

    producer.close().await.expect("producer close");
    ctx.cleanup().await;
}

/// An empty transaction can be aborted right after a committed one.
///
/// Translates `TransactionsTest.testEmptyAbortAfterCommit`
/// (`TransactionsTest.scala:1047-1057`). Java runs only the `consumer, true`
/// (`isTV2Enabled`) row; the pooled `apache/kafka:4.2.0` KRaft brokers are formatted
/// at the latest metadata version, which finalizes `transaction.version=2`.
#[tokio::test]
async fn test_empty_abort_after_commit() {
    let mut ctx = TestContext::new(cluster_config()).await;
    let (topic1, _topic2) = create_txn_test_topics(&mut ctx).await;
    let bootstrap = ctx.bootstrap_servers().to_string();
    let txn_id = ctx.group_id("transactional-producer");
    let producer = create_transactional_producer(&bootstrap, &txn_id);

    producer.init_transactions().await.expect("initTransactions");
    producer.begin_transaction().expect("beginTransaction");
    send_record(
        &producer,
        producer_record_with_expected_transaction_status(&topic1, Some(1), "4", "4", false),
    )
    .await
    .expect("send");
    producer.commit_transaction().await.expect("commitTransaction");

    producer.begin_transaction().expect("beginTransaction");
    producer
        .abort_transaction()
        .await
        .expect("abortTransaction of an empty transaction");

    producer.close().await.expect("producer close");
    ctx.cleanup().await;
}

/// The consumed `committed` offset carries the leader epoch and metadata that
/// `sendOffsetsToTransaction` committed.
///
/// Translates `TransactionsTest.testOffsetMetadataInSendOffsetsToTransaction`
/// (`TransactionsTest.scala:445-470`), CONSUMER arm. Java's two
/// `transactionalProducers` share the `"transactional-producer"` id
/// (`TransactionsTest.scala:98-99`), so `producer2.initTransactions()` is what
/// guarantees the first transaction has completed before the read-back.
///
/// `ConsumerGroupMetadata::new` is deprecated exactly as Java's constructor is;
/// Java suppresses the warning with `@SuppressWarnings(Array("removal"))`.
#[tokio::test]
#[allow(deprecated)]
async fn test_offset_metadata_in_send_offsets_to_transaction() {
    let mut ctx = TestContext::new(cluster_config()).await;
    let (topic1, _topic2) = create_txn_test_topics(&mut ctx).await;
    let bootstrap = ctx.bootstrap_servers().to_string();
    let tp = TopicPartition::new(topic1.clone(), 0);
    let group_id = ctx.group_id("group");
    let txn_id = ctx.group_id("transactional-producer");

    let producer = create_transactional_producer(&bootstrap, &txn_id);
    let mut consumer = create_read_committed_consumer(&bootstrap, &group_id);

    consumer.subscribe_with_topics(vec![topic1.clone()]).await.expect("subscribe");

    producer.init_transactions().await.expect("initTransactions");

    producer.begin_transaction().expect("beginTransaction");
    let offset_and_metadata =
        OffsetAndMetadata::with_leader_epoch_metadata(110, Some(15), "some metadata").expect("valid offset");
    producer
        .send_offsets_to_transaction(
            HashMap::from([(tp.clone(), offset_and_metadata.clone())]),
            ConsumerGroupMetadata::new(group_id.clone()),
        )
        .await
        .expect("sendOffsetsToTransaction");
    producer.commit_transaction().await.expect("commitTransaction: ok");

    // The call to commit the transaction may return before all markers are visible, so we initialize a second
    // producer to ensure the transaction completes and the committed offsets are visible.
    let producer2 = create_transactional_producer(&bootstrap, &txn_id);
    producer2.init_transactions().await.expect("producer2.initTransactions");

    // `TestUtils.waitUntilTrue(condition, msg)` with its defaults (15 s, 100 ms
    // pause), inlined because the condition borrows the consumer mutably, which
    // `test_utils::wait_until_true_with_timeout`'s `FnMut` closure cannot hand out.
    let start = Instant::now();
    loop {
        let committed = consumer.committed(std::slice::from_ref(&tp)).await;
        if committed.is_ok_and(|committed| committed.get(&tp) == Some(&offset_and_metadata)) {
            break;
        }
        assert!(
            start.elapsed() <= Duration::from_millis(test_utils::DEFAULT_MAX_WAIT_MS),
            "cannot read committed offset"
        );
        tokio::time::sleep(Duration::from_millis(test_utils::DEFAULT_PAUSE_MS)).await;
    }

    consumer.close().await.expect("consumer close");
    producer.close().await.expect("producer close");
    producer2.close().await.expect("producer2 close");
    ctx.cleanup().await;
}

// ---------------------------------------------------------------------------
// TransactionsTest: exactly-once copy and read_committed visibility
// ---------------------------------------------------------------------------

/// `TestUtils.seedTopicWithNumberedRecords(topic, numRecords, brokers)`
/// (`TestUtils.scala:1229-1246`): an idempotent producer writes keys and values
/// `"0" .. numRecords` (no partition, so the default partitioner spreads them),
/// then flushes and closes.
async fn seed_topic_with_numbered_records(bootstrap: &str, topic: &str, num_records: usize) {
    let producer = cluster_producer(bootstrap, &[("enable.idempotence", "true")]);
    for i in 0..num_records {
        let value = i.to_string().into_bytes();
        send_record(
            &producer,
            ProducerRecord::with_key(topic.to_string(), Some(value.clone()), Some(value)),
        )
        .await
        .expect("seed send");
    }
    producer.flush().await.expect("seed flush");
    producer.close().await.expect("seed producer close");
}

/// `TestUtils.consumerPositions(consumer)` (`TestUtils.scala:1284-1291`): the
/// current position of every assigned partition, as offsets to commit.
async fn consumer_positions(
    consumer: &mut Box<dyn Consumer<Vec<u8>, Vec<u8>>>,
) -> HashMap<TopicPartition, OffsetAndMetadata> {
    let mut offsets_to_commit = HashMap::new();
    for topic_partition in consumer.assignment() {
        let position = consumer.position(&topic_partition).await.expect("position");
        offsets_to_commit.insert(topic_partition, OffsetAndMetadata::new(position).expect("valid offset"));
    }
    offsets_to_commit
}

/// `TestUtils.resetToCommittedPositions(consumer)` (`TestUtils.scala:1293-1303`):
/// seek every assigned partition to its committed offset, or to the beginning if
/// it has none. (Java filters `null` values out of `committed`; the Rust map omits
/// partitions without a committed offset, which is the same set.)
async fn reset_to_committed_positions(consumer: &mut Box<dyn Consumer<Vec<u8>, Vec<u8>>>) {
    let assignment: Vec<TopicPartition> = consumer.assignment().into_iter().collect();
    let committed = consumer.committed(&assignment).await.expect("committed");
    for topic_partition in assignment {
        match committed.get(&topic_partition) {
            Some(offset) => consumer.seek_with_offset(topic_partition, offset.offset()).await.expect("seek"),
            None => consumer
                .seek_to_beginning(std::slice::from_ref(&topic_partition))
                .await
                .expect("seekToBeginning"),
        }
    }
}

/// Wall-clock milliseconds, Java's `System.currentTimeMillis()`.
fn current_time_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock is after the epoch")
        .as_millis() as i64
}

/// A consume/process/produce loop copies 500 records from `topic1` to `topic2`,
/// committing the consumed offsets in each transaction and aborting every other
/// transaction (rewinding the consumer to its committed positions); `topic2` then
/// holds exactly the 500 values, each once and each committed.
///
/// Translates `TransactionsTest.testSendOffsetsWithGroupMetadata`
/// (`TransactionsTest.scala:303-379`, including the `sendOffset` body).
///
/// Java's "randomly abort" is in fact strict alternation (`shouldCommit =
/// !shouldCommit`), reproduced as is. `maybeWaitForAtLeastOneSegmentUpload` is a
/// no-op in Java outside the tiered-storage subclass and is omitted.
#[tokio::test]
async fn test_send_offsets_with_group_metadata() {
    let mut ctx = TestContext::new(cluster_config()).await;
    let (topic1, topic2) = create_txn_test_topics(&mut ctx).await;
    let bootstrap = ctx.bootstrap_servers().to_string();

    let consumer_group_id = ctx.group_id("foobar-consumer-group");
    let num_seed_messages = 500;

    seed_topic_with_numbered_records(&bootstrap, &topic1, num_seed_messages).await;

    let producer = create_transactional_producer(&bootstrap, &ctx.group_id("transactional-producer"));

    let mut consumer = create_consumer(&bootstrap, &consumer_group_id, true, num_seed_messages / 4, &[]);
    consumer.subscribe_with_topics(vec![topic1.clone()]).await.expect("subscribe");
    producer.init_transactions().await.expect("initTransactions");

    let mut should_commit = false;
    let mut records_processed = 0;
    while records_processed < num_seed_messages {
        let records =
            poll_until_at_least_num_records(&mut consumer, 10.min(num_seed_messages - records_processed)).await;

        producer.begin_transaction().expect("beginTransaction");
        should_commit = !should_commit;

        for record in &records {
            let key = String::from_utf8(record.key().expect("seeded with a key").clone()).expect("UTF-8 key");
            let value = String::from_utf8(record.value().expect("seeded with a value").clone()).expect("UTF-8 value");
            send_record(
                &producer,
                producer_record_with_expected_transaction_status(&topic2, None, &key, &value, should_commit),
            )
            .await
            .expect("send");
        }

        // The `commit` lambda of `testSendOffsetsWithGroupMetadata`.
        let offsets = consumer_positions(&mut consumer).await;
        producer
            .send_offsets_to_transaction(offsets, consumer.group_metadata())
            .await
            .expect("sendOffsetsToTransaction");
        if should_commit {
            producer.commit_transaction().await.expect("commitTransaction");
            records_processed += records.len();
        } else {
            producer.abort_transaction().await.expect("abortTransaction");
            reset_to_committed_positions(&mut consumer).await;
        }
    }
    consumer.close().await.expect("consumer close");

    // In spite of the aborts, we should still have exactly 500 messages in topic2.
    // I.e. we should not re-copy or miss any messages from topic1, since the
    // consumed offsets were committed transactionally.
    let mut verifying_consumer = create_read_committed_consumer(&bootstrap, &ctx.group_id("transactional-group"));
    verifying_consumer
        .subscribe_with_topics(vec![topic2.clone()])
        .await
        .expect("subscribe");
    let value_seq: Vec<i32> = poll_until_at_least_num_records(&mut verifying_consumer, num_seed_messages)
        .await
        .iter()
        .map(|record| assert_committed_and_get_value(record).parse().expect("a numbered value"))
        .collect();
    let value_set: std::collections::HashSet<i32> = value_seq.iter().copied().collect();
    assert_eq!(
        num_seed_messages,
        value_seq.len(),
        "Expected {num_seed_messages} values in {topic2}."
    );
    assert_eq!(
        value_seq.len(),
        value_set.len(),
        "Expected {} unique messages in {topic2}.",
        value_seq.len()
    );

    verifying_consumer.close().await.expect("verifying consumer close");
    producer.close().await.expect("producer close");
    ctx.cleanup().await;
}

/// A read_committed consumer stops at the last stable offset: it sees only the
/// records before an undecided transaction, `seekToEnd` lands on the LSO, and
/// `offsetsForTimes` finds nothing among the undecided records — while a
/// read_uncommitted consumer sees everything.
///
/// Translates `TransactionsTest.testReadCommittedConsumerShouldNotSeeUndecidedData`
/// (`TransactionsTest.scala:177-241`).
///
/// Deviation: Java asserts `offsetsForTimes(...).get(tp)` is `null` for the
/// undecided partitions. The Rust `offsets_for_times` omits an unresolved
/// partition instead of mapping it to `null` (see `Consumer::offsets_for_times`),
/// so the assertion is that the key is absent.
#[tokio::test]
async fn test_read_committed_consumer_should_not_see_undecided_data() {
    let mut ctx = TestContext::new(cluster_config()).await;
    let (topic1, topic2) = create_txn_test_topics(&mut ctx).await;
    let bootstrap = ctx.bootstrap_servers().to_string();
    let producer1 = create_transactional_producer(&bootstrap, &ctx.group_id("transactional-producer"));
    let producer2 = create_transactional_producer(&bootstrap, &ctx.group_id("other"));
    let mut read_committed_consumer = create_read_committed_consumer(&bootstrap, &ctx.group_id("transactional-group"));
    let mut read_uncommitted_consumer =
        create_read_uncommitted_consumer(&bootstrap, &ctx.group_id("non-transactional-group"));

    producer1.init_transactions().await.expect("producer1.initTransactions");
    producer2.init_transactions().await.expect("producer2.initTransactions");

    producer1.begin_transaction().expect("producer1.beginTransaction");
    producer2.begin_transaction().expect("producer2.beginTransaction");

    let record = |topic: &str, timestamp: i64, key: &str, value: &str| {
        ProducerRecord::with_partition_timestamp_key(
            topic.to_string(),
            Some(0),
            Some(timestamp),
            Some(key.as_bytes().to_vec()),
            Some(value.as_bytes().to_vec()),
        )
        .expect("valid record")
    };

    let latest_visible_timestamp = current_time_millis();
    for topic in [&topic1, &topic2] {
        send_record(&producer2, record(topic, latest_visible_timestamp, "x", "1"))
            .await
            .expect("producer2 send");
    }
    producer2.flush().await.expect("producer2.flush");

    let latest_written_timestamp = latest_visible_timestamp + 1;
    for (topic, key, value) in [
        (&topic1, "a", "1"),
        (&topic1, "b", "2"),
        (&topic2, "c", "3"),
        (&topic2, "d", "4"),
    ] {
        send_record(&producer1, record(topic, latest_written_timestamp, key, value))
            .await
            .expect("producer1 send");
    }
    producer1.flush().await.expect("producer1.flush");

    for topic in [&topic1, &topic2] {
        send_record(&producer2, record(topic, latest_written_timestamp, "x", "2"))
            .await
            .expect("producer2 send");
    }
    producer2.commit_transaction().await.expect("producer2.commitTransaction");

    // ensure the records are visible to the read uncommitted consumer
    let tp1 = TopicPartition::new(topic1.clone(), 0);
    let tp2 = TopicPartition::new(topic2.clone(), 0);
    read_uncommitted_consumer
        .assign(vec![tp1.clone(), tp2.clone()])
        .await
        .expect("assign");
    consume_records(&mut read_uncommitted_consumer, 8).await;
    let read_uncommitted_offsets_for_times = read_uncommitted_consumer
        .offsets_for_times(HashMap::from([
            (tp1.clone(), latest_written_timestamp),
            (tp2.clone(), latest_written_timestamp),
        ]))
        .await
        .expect("offsetsForTimes");
    assert_eq!(2, read_uncommitted_offsets_for_times.len());
    assert_eq!(latest_written_timestamp, read_uncommitted_offsets_for_times[&tp1].timestamp());
    assert_eq!(latest_written_timestamp, read_uncommitted_offsets_for_times[&tp2].timestamp());
    read_uncommitted_consumer.unsubscribe().await.expect("unsubscribe");

    // we should only see the first two records which come before the undecided second transaction
    read_committed_consumer
        .assign(vec![tp1.clone(), tp2.clone()])
        .await
        .expect("assign");
    let records = consume_records(&mut read_committed_consumer, 2).await;
    for record in &records {
        assert_eq!(b"x".as_slice(), record.key().expect("key").as_slice());
        assert_eq!(b"1".as_slice(), record.value().expect("value").as_slice());
    }

    // even if we seek to the end, we should not be able to see the undecided data
    let assignment: Vec<TopicPartition> = read_committed_consumer.assignment().into_iter().collect();
    assert_eq!(2, assignment.len());
    read_committed_consumer.seek_to_end(&assignment).await.expect("seekToEnd");
    for tp in &assignment {
        assert_eq!(1, read_committed_consumer.position(tp).await.expect("position"));
    }

    // undecided timestamps should not be searchable either
    let read_committed_offsets_for_times = read_committed_consumer
        .offsets_for_times(HashMap::from([
            (tp1.clone(), latest_written_timestamp),
            (tp2.clone(), latest_written_timestamp),
        ]))
        .await
        .expect("offsetsForTimes");
    assert!(
        !read_committed_offsets_for_times.contains_key(&tp1),
        "undecided data in {tp1:?} must not be searchable: {read_committed_offsets_for_times:?}"
    );
    assert!(
        !read_committed_offsets_for_times.contains_key(&tp2),
        "undecided data in {tp2:?} must not be searchable: {read_committed_offsets_for_times:?}"
    );

    read_committed_consumer.close().await.expect("consumer close");
    read_uncommitted_consumer.close().await.expect("consumer close");
    producer1.close().await.expect("producer1 close");
    producer2.close().await.expect("producer2 close");
    ctx.cleanup().await;
}

// ---------------------------------------------------------------------------
// TransactionsTest: markers, delayed fetch and transaction expiration
// ---------------------------------------------------------------------------

/// `TransactionsTest.verifyLogStartOffsets` (`TransactionsTest.scala:1109-1121`):
/// wait until each partition's log start offset equals the expected one.
///
/// Deviation: Java reads `replicaManager.localLog(partition).logStartOffset` on
/// every broker. A client cannot inspect replicas, so this waits on the log
/// start offset the leader reports through `ListOffsets(EARLIEST)` — the value a
/// client observes.
async fn verify_log_start_offsets(ctx: &TestContext, partition_start_offsets: &[(TopicPartition, i64)]) {
    let admin = txn_test_admin(ctx);
    let specs: HashMap<TopicPartition, OffsetSpec> = partition_start_offsets
        .iter()
        .map(|(partition, _)| (partition.clone(), OffsetSpec::earliest()))
        .collect();
    let current = std::sync::Mutex::new(HashMap::new());
    test_utils::wait_until_true_with_timeout(
        || async {
            let Ok(offsets) = admin.list_offsets(&specs).all().get().await else {
                return false;
            };
            let matches = partition_start_offsets
                .iter()
                .all(|(partition, offset)| offsets.get(partition).map(|info| info.offset()) == Some(*offset));
            *current.lock().unwrap() = offsets.into_iter().map(|(tp, info)| (tp, info.offset())).collect();
            matches
        },
        &format!("log start offset doesn't change to the expected position: {partition_start_offsets:?}"),
        test_utils::DEFAULT_MAX_WAIT_MS,
        test_utils::DEFAULT_PAUSE_MS,
    )
    .await;
    admin.close().await;
}

/// A read_committed fetch parked in purgatory (`fetch.min.bytes` far above the
/// data size) still carries the aborted-transaction index, so the consumer skips
/// the aborted records and returns only the committed ones, at their offsets.
///
/// Translates `TransactionsTest.testDelayedFetchIncludesAbortedTransaction`
/// (`TransactionsTest.scala:245-299`). `maybeVerifyLocalLogStartOffsets` and
/// `maybeWaitForAtLeastOneSegmentUpload` are no-ops in Java outside the
/// tiered-storage subclass and are omitted.
#[tokio::test]
async fn test_delayed_fetch_includes_aborted_transaction() {
    let mut ctx = TestContext::new(cluster_config()).await;
    let (topic1, _topic2) = create_txn_test_topics(&mut ctx).await;
    let bootstrap = ctx.bootstrap_servers().to_string();
    let producer1 = create_transactional_producer(&bootstrap, &ctx.group_id("transactional-producer"));
    let producer2 = create_transactional_producer(&bootstrap, &ctx.group_id("other"));
    let tp10 = TopicPartition::new(topic1.clone(), 0);

    producer1.init_transactions().await.expect("producer1.initTransactions");
    producer2.init_transactions().await.expect("producer2.initTransactions");

    let record = |key: &str, value: &str| {
        ProducerRecord::with_partition_key(
            topic1.clone(),
            Some(0),
            Some(key.as_bytes().to_vec()),
            Some(value.as_bytes().to_vec()),
        )
        .expect("valid record")
    };

    producer1.begin_transaction().expect("producer1.beginTransaction");
    producer2.begin_transaction().expect("producer2.beginTransaction");
    send_record(&producer2, record("x", "1")).await.expect("producer2 send");
    producer2.flush().await.expect("producer2.flush");

    send_record(&producer1, record("y", "1")).await.expect("producer1 send");
    send_record(&producer1, record("y", "2")).await.expect("producer1 send");
    producer1.flush().await.expect("producer1.flush");

    send_record(&producer2, record("x", "2")).await.expect("producer2 send");
    producer2.flush().await.expect("producer2.flush");

    // Since we haven't committed/aborted any records, the last stable offset is
    // still 0, no segments should be offloaded to remote storage
    verify_log_start_offsets(&ctx, &[(tp10.clone(), 0)]).await;

    producer1.abort_transaction().await.expect("producer1.abortTransaction");
    producer2.commit_transaction().await.expect("producer2.commitTransaction");

    // We've sent 4 records + 1 abort mark + 1 commit mark; the log start offset
    // is still 0.
    verify_log_start_offsets(&ctx, &[(tp10.clone(), 0)]).await;

    // ensure that the consumer's fetch will sit in purgatory
    let mut read_committed_consumer = create_consumer(
        &bootstrap,
        &ctx.group_id("group"),
        true,
        500,
        &[("fetch.min.bytes", "100000"), ("fetch.max.wait.ms", "100")],
    );

    read_committed_consumer.assign(vec![tp10.clone()]).await.expect("assign");
    let records = consume_records(&mut read_committed_consumer, 2).await;
    assert_eq!(2, records.len());

    let first = &records[0];
    assert_eq!(b"x".as_slice(), first.key().expect("key").as_slice());
    assert_eq!(b"1".as_slice(), first.value().expect("value").as_slice());
    assert_eq!(0, first.offset());

    let second = &records[1];
    assert_eq!(b"x".as_slice(), second.key().expect("key").as_slice());
    assert_eq!(b"2".as_slice(), second.value().expect("value").as_slice());
    assert_eq!(3, second.offset());

    read_committed_consumer.close().await.expect("consumer close");
    producer1.close().await.expect("producer1 close");
    producer2.close().await.expect("producer2 close");
    ctx.cleanup().await;
}

/// `TransactionsTest.sendTransactionalMessagesWithValueRange`
/// (`TransactionsTest.scala:1059-1065`): keys and values `start .. end` with the
/// expected-status header, then `flush`.
async fn send_transactional_messages_with_value_range(
    producer: &KafkaProducer<Vec<u8>, Vec<u8>>,
    topic: &str,
    start: i32,
    end: i32,
    will_be_committed: bool,
) {
    for i in start..end {
        let value = i.to_string();
        send_record(
            producer,
            producer_record_with_expected_transaction_status(topic, None, &value, &value, will_be_committed),
        )
        .await
        .expect("send");
    }
    producer.flush().await.expect("flush");
}

/// One transaction spanning twenty partitions — half of them on single-replica
/// partitions, so several markers go to the same leader in one
/// `WriteTxnMarkers` — is aborted, a second is committed; read_committed sees
/// exactly the 1000 committed records and read_uncommitted all 11000.
///
/// Translates `TransactionsTest.testMultipleMarkersOneLeader`
/// (`TransactionsTest.scala:654-688`).
#[tokio::test]
async fn test_multiple_markers_one_leader() {
    let mut ctx = TestContext::new(cluster_config()).await;
    let bootstrap = ctx.bootstrap_servers().to_string();
    let first_producer = create_transactional_producer(&bootstrap, &ctx.group_id("transactional-producer"));
    let mut consumer = create_read_committed_consumer(&bootstrap, &ctx.group_id("transactional-group"));
    let mut un_committed_consumer =
        create_read_uncommitted_consumer(&bootstrap, &ctx.group_id("non-transactional-group"));
    let topic_with_10_partitions = ctx.topic("largeTopic");
    let topic_with_10_partitions_and_one_replica = ctx.topic("largeTopicOneReplica");

    let admin = txn_test_admin(&ctx);
    test_utils::create_topic_with_configs(
        admin.as_ref(),
        &topic_with_10_partitions,
        10,
        TXN_TEST_REPLICATION_FACTOR,
        BTreeMap::from([("min.insync.replicas".to_string(), "2".to_string())]),
    )
    .await;
    test_utils::wait_for_partition_leaders(admin.as_ref(), &topic_with_10_partitions, 0..10).await;
    test_utils::create_topic_with_configs(
        admin.as_ref(),
        &topic_with_10_partitions_and_one_replica,
        10,
        1,
        BTreeMap::new(),
    )
    .await;
    test_utils::wait_for_partition_leaders(admin.as_ref(), &topic_with_10_partitions_and_one_replica, 0..10).await;
    admin.close().await;

    first_producer.init_transactions().await.expect("initTransactions");

    first_producer.begin_transaction().expect("beginTransaction");
    send_transactional_messages_with_value_range(&first_producer, &topic_with_10_partitions, 0, 5000, false).await;
    send_transactional_messages_with_value_range(
        &first_producer,
        &topic_with_10_partitions_and_one_replica,
        5000,
        10000,
        false,
    )
    .await;
    first_producer.abort_transaction().await.expect("abortTransaction");

    first_producer.begin_transaction().expect("beginTransaction");
    send_transactional_messages_with_value_range(&first_producer, &topic_with_10_partitions, 10000, 11000, true).await;
    first_producer.commit_transaction().await.expect("commitTransaction");

    let topics = vec![
        topic_with_10_partitions_and_one_replica.clone(),
        topic_with_10_partitions.clone(),
    ];
    consumer.subscribe_with_topics(topics.clone()).await.expect("subscribe");
    un_committed_consumer.subscribe_with_topics(topics).await.expect("subscribe");

    let records = consume_records(&mut consumer, 1000).await;
    for record in &records {
        assert_committed_and_get_value(record);
    }

    let all_records = consume_records(&mut un_committed_consumer, 11000).await;
    let expected_values: std::collections::HashSet<String> = (0..11000).map(|i| i.to_string()).collect();
    for record in &all_records {
        let value = String::from_utf8_lossy(record.value().expect("value")).into_owned();
        assert!(expected_values.contains(&value), "unexpected value {value}");
    }

    consumer.close().await.expect("consumer close");
    un_committed_consumer.close().await.expect("consumer close");
    first_producer.close().await.expect("producer close");
    ctx.cleanup().await;
}

/// `TransactionsTest`'s cluster with its abort-cleanup interval
/// (`TransactionsTest.scala:61-75`), for the one test that depends on it.
///
/// The other `TransactionsTest` translations share [`kip848_3_broker`]'s pooled
/// container (see the deviations above), but
/// `transaction.abort.timed.out.transaction.cleanup.interval.ms=200` is what lets
/// the coordinator expire a 300 ms transaction inside
/// [`test_fencing_on_transaction_expiration`]'s 600 ms sleep — at the broker
/// default (10 s) the transaction would still be open. So this config adds that
/// property, plus Java's `__transaction_state` shape (3 partitions, RF 2, min ISR
/// 2) and disabled auto-creation, on top of the KIP-848 settings, and gets its
/// own pooled container.
fn transactions_test_expiration_cluster() -> ClusterConfig {
    let mut config = kip848_3_broker(2);
    for (key, value) in [
        ("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"),
        ("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "3"),
        ("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "2"),
        ("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "2"),
        ("KAFKA_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS", "200"),
    ] {
        config.server_properties.insert(key.to_string(), value.to_string());
    }
    config
}

/// `TransactionsTest.consumeRecordsFor(consumer)` (`TransactionsTest.scala:1131-1141`):
/// everything the consumer returns while polling (50 ms each) for one second.
async fn consume_records_for(
    consumer: &mut Box<dyn Consumer<Vec<u8>, Vec<u8>>>,
) -> Vec<ConsumerRecord<Vec<u8>, Vec<u8>>> {
    let duration = Duration::from_millis(1000);
    let start = Instant::now();
    let mut records = Vec::new();
    loop {
        records.extend(consumer.poll(Duration::from_millis(50)).await.expect("poll should not fail"));
        if start.elapsed() > duration {
            return records;
        }
    }
}

/// A transaction left open past `transaction.timeout.ms` is aborted by the
/// coordinator, which bumps the epoch: the next send fails with
/// `InvalidProducerEpochException` (or `ConcurrentTransactionsException` while the
/// abort completes), the first record ends up aborted and the second is never
/// written.
///
/// Translates `TransactionsTest.testFencingOnTransactionExpiration`
/// (`TransactionsTest.scala:610-650`).
#[tokio::test]
async fn test_fencing_on_transaction_expiration() {
    let mut ctx = TestContext::new(transactions_test_expiration_cluster()).await;
    let (topic1, _topic2) = create_txn_test_topics(&mut ctx).await;
    let bootstrap = ctx.bootstrap_servers().to_string();
    let producer =
        create_transactional_producer_with_transaction_timeout_ms(&bootstrap, &ctx.group_id("expiringProducer"), 300);

    producer.init_transactions().await.expect("initTransactions");
    producer.begin_transaction().expect("beginTransaction");

    // The first message and hence the first AddPartitions request should be
    // successfully sent.
    let first_message_result = send_record(
        &producer,
        producer_record_with_expected_transaction_status(&topic1, None, "1", "1", false),
    )
    .await
    .expect("first send")
    .get()
    .await
    .expect("the first record is acked");
    assert!(first_message_result.has_offset());

    // Wait for the expiration cycle to kick in.
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Now that the transaction has expired, the second send should fail with an
    // InvalidProducerEpochException. We may see some ConcurrentTransactionsExceptions.
    match send_record(
        &producer,
        producer_record_with_expected_transaction_status(&topic1, None, "2", "2", false),
    )
    .await
    {
        // Java's bare `catch` arms: the exception escapes `send(...)` / `get()`
        // unwrapped.
        Err(Error::ConcurrentTransactions(_)) | Err(Error::InvalidProducerEpoch(_)) => {},
        Err(other) => panic!("Error was {other:?} and not InvalidProducerEpochException"),
        Ok(future) => match future.get().await {
            Ok(_) => panic!("should have raised an error due to concurrent transactions or invalid producer epoch"),
            Err(error) => {
                assert!(
                    matches!(error, Error::InvalidProducerEpoch(_)),
                    "Error was {error:?} and not InvalidProducerEpochException"
                );
                assert_eq!(error.message(), Errors::InvalidProducerEpoch.message());
            },
        },
    }

    // Verify that the first message was aborted and the second one was never
    // written at all.
    let mut non_transactional_consumer =
        create_read_uncommitted_consumer(&bootstrap, &ctx.group_id("non-transactional-group"));
    non_transactional_consumer
        .subscribe_with_topics(vec![topic1.clone()])
        .await
        .expect("subscribe");

    // Attempt to consume the one written record. We should not see the second.
    // The assertion does not strictly guarantee that the record wasn't written,
    // but the data is small enough that had it been written, it would have been
    // in the first fetch.
    let records = consume_records(&mut non_transactional_consumer, 1).await;
    assert_eq!(1, records.len());
    assert_eq!(b"1".as_slice(), records[0].value().expect("value").as_slice());

    let mut transactional_consumer = create_read_committed_consumer(&bootstrap, &ctx.group_id("transactional-group"));
    transactional_consumer
        .subscribe_with_topics(vec![topic1.clone()])
        .await
        .expect("subscribe");

    let transactional_records = consume_records_for(&mut transactional_consumer).await;
    assert!(
        transactional_records.is_empty(),
        "the expired transaction's record must not be visible to read_committed"
    );

    non_transactional_consumer.close().await.expect("consumer close");
    transactional_consumer.close().await.expect("consumer close");
    producer.close().await.expect("producer close");
    ctx.cleanup().await;
}

// ---------------------------------------------------------------------------
// ProducerIntegrationTest / TransactionsWithMaxInFlightOneTest translations
// ---------------------------------------------------------------------------
//
// Both Java classes run on a single KRaft broker with the transaction-state log
// at replication factor 1 (`ProducerIntegrationTest.java:62-68`,
// `TransactionsWithMaxInFlightOneTest.java:53-68`); [`txn_single_broker`] is
// that cluster. Deviations shared by these tests, each forced by the pooled
// harness:
//
//   - Java's fixed topic names and transactional ids become per-test names
//     (`ctx.topic` / `ctx.group_id`), because the cluster is pooled here.
//   - Topics are created explicitly through the admin client and awaited until
//     every partition has a leader. Java relies on auto-creation (`"test"`) or
//     fires `createTopics` without awaiting it; neither is observable in the
//     assertions.
//   - The remaining broker overrides Java sets (`__transaction_state` /
//     `__consumer_offsets` partition counts, auto-create disabled, controlled
//     shutdown, abort-cleanup interval, ...) are not reproduced: none reaches an
//     assertion here, and each would fork the pooled container.
//   - `ProducerIntegrationTest` runs each test at `transaction.version` 0, 1 and
//     2 (`@ClusterFeature`). The pooled `apache/kafka:4.2.0` brokers are
//     formatted at the latest metadata version, which finalizes
//     `transaction.version=2`, and the harness cannot re-format them at a lower
//     feature level — so only the TV2 row is translated; TV0/TV1 are not
//     reachable here.

/// `ClusterInstance.producer(configs)` (`ClusterInstance.java:150-156`):
/// `configs` plus byte-array serializers and the cluster's bootstrap servers.
fn cluster_producer(bootstrap: &str, configs: &[(&str, &str)]) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    let mut props: HashMap<String, String> = configs
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    props
        .entry("bootstrap.servers".to_string())
        .or_insert_with(|| bootstrap.to_string());
    KafkaProducer::new(
        ProducerConfig::new(&props).expect("invalid producer config"),
        Box::new(ByteArraySerializer),
        Box::new(ByteArraySerializer),
    )
    .expect("failed to build a producer")
}

/// Creates `topic` on the single-broker cluster and waits until every partition
/// has a leader (see the shared deviations above).
async fn create_single_broker_topic(
    ctx: &TestContext,
    topic: &str,
    num_partitions: i32,
    topic_config: BTreeMap<String, String>,
) {
    let admin = txn_test_admin(ctx);
    test_utils::create_topic_with_configs(admin.as_ref(), topic, num_partitions, 1, topic_config).await;
    test_utils::wait_for_partition_leaders(admin.as_ref(), topic, 0..num_partitions).await;
    admin.close().await;
}

/// A transaction with a send and an empty transaction both commit.
///
/// Translates `ProducerIntegrationTest.testTransactionWithAndWithoutSend`
/// (`ProducerIntegrationTest.java:92-114`), TV2 row only (see above).
#[tokio::test]
async fn test_transaction_with_and_without_send() {
    let mut ctx = TestContext::new(txn_single_broker()).await;
    let topic = ctx.topic("test");
    create_single_broker_topic(&ctx, &topic, 1, BTreeMap::new()).await;
    let bootstrap = ctx.bootstrap_servers().to_string();
    let txn_id = ctx.group_id("foobar");

    let producer = cluster_producer(
        &bootstrap,
        &[
            ("transactional.id", &txn_id),
            ("client.id", "test"),
            ("enable.idempotence", "true"),
        ],
    );
    producer.init_transactions().await.expect("initTransactions");
    producer.begin_transaction().expect("beginTransaction");
    // Java does not await the record's future; the commit flushes it.
    send_record(
        &producer,
        ProducerRecord::with_key(topic.clone(), Some(b"key".to_vec()), Some(b"value".to_vec())),
    )
    .await
    .expect("send");
    producer.commit_transaction().await.expect("commitTransaction with a send");

    producer.begin_transaction().expect("beginTransaction");
    producer.commit_transaction().await.expect("commitTransaction without a send");

    producer.close().await.expect("producer close");
    ctx.cleanup().await;
}

/// A record the broker rejects as too large fails with `RecordTooLargeException`,
/// and the transaction can still be aborted (the `EndTxn` request is sent).
///
/// Translates `ProducerIntegrationTest.testTransactionWithInvalidSendAndEndTxnRequestSent`
/// (`ProducerIntegrationTest.java:116-146`), TV2 row only (see above). Java asserts
/// only the cause's class; the message is broker-generated text that embeds the
/// batch size, so only the variant is asserted here too.
#[tokio::test]
async fn test_transaction_with_invalid_send_and_end_txn_request_sent() {
    let mut ctx = TestContext::new(txn_single_broker()).await;
    let topic = ctx.topic("foobar");
    create_single_broker_topic(
        &ctx,
        &topic,
        1,
        BTreeMap::from([("max.message.bytes".to_string(), "100".to_string())]),
    )
    .await;
    let bootstrap = ctx.bootstrap_servers().to_string();
    let txn_id = ctx.group_id("test-txn");

    let producer = cluster_producer(
        &bootstrap,
        &[
            ("transactional.id", &txn_id),
            ("client.id", "test"),
            ("enable.idempotence", "true"),
        ],
    );
    producer.init_transactions().await.expect("initTransactions");
    producer.begin_transaction().expect("beginTransaction");
    let error = send_record(
        &producer,
        ProducerRecord::with_key(topic.clone(), Some(vec![0u8; 100]), Some(vec![0u8; 100])),
    )
    .await
    .expect("send is accepted; the broker rejects the batch")
    .get()
    .await
    .expect_err("a record larger than max.message.bytes must fail");
    assert!(
        matches!(error, Error::RecordTooLarge(_)),
        "expected RecordTooLarge, got {error:?}"
    );

    producer.abort_transaction().await.expect("abortTransaction");

    producer.close().await.expect("producer close");
    ctx.cleanup().await;
}

/// With `max.in.flight.requests.per.connection=1` on a single broker, multiple
/// transactional requests queue on one connection; an aborted and a committed
/// transaction still yield exactly the committed records.
///
/// Translates `TransactionsWithMaxInFlightOneTest.testTransactionalProducerSingleBrokerMaxInFlightOne`
/// (`TransactionsWithMaxInFlightOneTest.java:76-125`). Java loops over
/// `supportedGroupProtocols()`; only the CONSUMER arm is translated
/// (`consumer-threading.md` §20).
#[tokio::test]
async fn test_transactional_producer_single_broker_max_in_flight_one() {
    let mut ctx = TestContext::new(txn_single_broker()).await;
    // We want to test with one broker to verify multiple requests queued on a connection
    assert_eq!(1, txn_single_broker().brokers);

    let topic1 = ctx.topic("topic1");
    let topic2 = ctx.topic("topic2");
    create_single_broker_topic(&ctx, &topic1, 4, BTreeMap::new()).await;
    create_single_broker_topic(&ctx, &topic2, 4, BTreeMap::new()).await;
    let bootstrap = ctx.bootstrap_servers().to_string();
    let txn_id = ctx.group_id("transactional-producer");

    let producer = cluster_producer(
        &bootstrap,
        &[
            ("transactional.id", &txn_id),
            ("max.in.flight.requests.per.connection", "1"),
        ],
    );
    producer.init_transactions().await.expect("initTransactions");

    producer.begin_transaction().expect("beginTransaction");
    for (topic, value) in [(&topic2, "2"), (&topic1, "4")] {
        send_record(
            &producer,
            producer_record_with_expected_transaction_status(topic, None, value, value, false),
        )
        .await
        .expect("send");
    }
    producer.flush().await.expect("flush");
    producer.abort_transaction().await.expect("abortTransaction");

    producer.begin_transaction().expect("beginTransaction");
    for (topic, value) in [(&topic1, "1"), (&topic2, "3")] {
        send_record(
            &producer,
            producer_record_with_expected_transaction_status(topic, None, value, value, true),
        )
        .await
        .expect("send");
    }
    producer.commit_transaction().await.expect("commitTransaction");

    let mut consumer_records: Vec<ConsumerRecord<Vec<u8>, Vec<u8>>> = Vec::new();
    {
        // `ClusterInstance.consumer` defaults plus Java's overrides:
        // `group.protocol=CONSUMER`, `enable.auto.commit=false`,
        // `isolation.level=read_committed`.
        let mut consumer = assigned_consumer(&bootstrap, &ctx.group_id("group"), "read_committed");
        consumer
            .subscribe_with_topics(vec![topic1.clone(), topic2.clone()])
            .await
            .expect("subscribe");
        let start = Instant::now();
        loop {
            let records = consumer.poll(Duration::from_millis(100)).await.expect("poll");
            consumer_records.extend(records);
            if consumer_records.len() == 2 {
                break;
            }
            assert!(
                start.elapsed() <= Duration::from_millis(15_000),
                "Consumer with protocol CONSUMER should consume 2 records, but get {}",
                consumer_records.len()
            );
            tokio::time::sleep(Duration::from_millis(test_utils::DEFAULT_PAUSE_MS)).await;
        }
        consumer.close().await.expect("consumer close");
    }
    for record in &consumer_records {
        let headers = record.headers().headers(TRANSACTION_STATUS_KEY);
        let header = headers.first().expect("the record carries a transactionStatus header");
        assert_eq!(
            Some(COMMITTED_VALUE),
            header.value(),
            "Record does not have the expected header value"
        );
    }

    producer.close().await.expect("producer close");
    ctx.cleanup().await;
}

// ---------------------------------------------------------------------------
// AdminFenceProducersTest translations
// ---------------------------------------------------------------------------
//
// `AdminFenceProducersTest` (clients-integration-tests, AK 4.3.1) drives a real
// transactional producer against `Admin.fenceProducers`, so it lives next to the
// other transactional-producer translations rather than in
// `admin_transactions_test.rs`. It is native-only: the multilanguage harness
// has no admin + transactional-producer pairing. `testFenceProducerTimeoutMs`
// uses no producer and is not part of this slice.
//
// Deviations forced by the pooled harness: the fixed `TOPIC_NAME` / `TXN_ID`
// become per-test names (`ctx.topic` / `ctx.group_id`), and the topic is created
// through the admin client and awaited until its partition has a leader.

/// `AdminFenceProducersTest`'s `@ClusterTestDefaults` broker
/// (`AdminFenceProducersTest.java:44-50`): one broker, auto-create disabled, a
/// one-partition RF-1 `__transaction_state`, and a 2 s abort-cleanup interval.
/// The interval differs from every other config here, so this is its own pooled
/// container.
fn admin_fence_producers_cluster() -> ClusterConfig {
    let props = BTreeMap::from([
        ("KAFKA_AUTO_CREATE_TOPICS_ENABLE".to_string(), "false".to_string()),
        ("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS".to_string(), "1".to_string()),
        ("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR".to_string(), "1".to_string()),
        ("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR".to_string(), "1".to_string()),
        (
            "KAFKA_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS".to_string(),
            "2000".to_string(),
        ),
    ]);
    ClusterConfig::with_properties(props)
}

/// `AdminFenceProducersTest.createProducer()` (`AdminFenceProducersTest.java:63-66`).
fn admin_fence_producers_producer(bootstrap: &str, txn_id: &str) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    cluster_producer(bootstrap, &[("transactional.id", txn_id), ("transaction.timeout.ms", "2000")])
}

/// `AdminFenceProducersTest.RECORD` (`AdminFenceProducersTest.java:59`): no key,
/// a one-byte value.
fn admin_fence_producers_record(topic: &str) -> ProducerRecord<Vec<u8>, Vec<u8>> {
    ProducerRecord::with_key(topic.to_string(), None, Some(vec![0u8]))
}

/// `adminClient.fenceProducers(List.of(txnId)).all().get()`.
async fn fence_producer(ctx: &TestContext, txn_id: &str) {
    let admin = txn_test_admin(ctx);
    admin
        .fence_producers(&[txn_id.to_string()])
        .all()
        .get()
        .await
        .expect("fenceProducers");
    admin.close().await;
}

/// Fencing a producer between transactions: its next send fails through the
/// future with `InvalidProducerEpochException` (Transaction V2 converts the
/// coordinator's `ProducerFencedException`), and `commitTransaction` returns that
/// same fatal error.
///
/// Translates `AdminFenceProducersTest.testFenceAfterProducerCommit`
/// (`AdminFenceProducersTest.java:68-96`). Java asserts the classes only; the
/// message is the error code's fixed default text, so it is asserted too.
#[tokio::test]
async fn test_fence_after_producer_commit() {
    let mut ctx = TestContext::new(admin_fence_producers_cluster()).await;
    let topic = ctx.topic("mytopic");
    create_single_broker_topic(&ctx, &topic, 1, BTreeMap::new()).await;
    let bootstrap = ctx.bootstrap_servers().to_string();
    let txn_id = ctx.group_id("mytxnid");
    let producer = admin_fence_producers_producer(&bootstrap, &txn_id);

    producer.init_transactions().await.expect("initTransactions");
    producer.begin_transaction().expect("beginTransaction");
    send_record(&producer, admin_fence_producers_record(&topic))
        .await
        .expect("send")
        .get()
        .await
        .expect("the first record is acked");
    producer.commit_transaction().await.expect("commitTransaction");

    fence_producer(&ctx, &txn_id).await;

    producer.begin_transaction().expect("beginTransaction");
    let exception_during_send = send_record(&producer, admin_fence_producers_record(&topic))
        .await
        .expect("send is accepted; the failure arrives through the future")
        .get()
        .await
        .expect_err("expected InvalidProducerEpochException");

    // In Transaction V2, the ProducerFencedException will be converted to
    // InvalidProducerEpochException when coordinator handles AddPartitionRequest.
    assert!(
        matches!(exception_during_send, Error::InvalidProducerEpoch(_)),
        "expected InvalidProducerEpoch, got {exception_during_send:?}"
    );
    assert_eq!(exception_during_send.message(), Errors::InvalidProducerEpoch.message());

    // InvalidProducerEpochException is treated as fatal error. The
    // commitTransaction will return this last fatal error.
    let commit_error = producer
        .commit_transaction()
        .await
        .expect_err("commitTransaction must return the fatal error");
    assert!(
        matches!(commit_error, Error::InvalidProducerEpoch(_)),
        "expected InvalidProducerEpoch, got {commit_error:?}"
    );
    // `TransactionManager.maybeFailWithError` (`TransactionManager.java:1181-1184`)
    // rethrows a fresh InvalidProducerEpochException naming the transactional id
    // and the producer id / epoch; the latter two are broker-assigned, so only
    // the fixed parts are asserted.
    let commit_message = commit_error.message();
    let expected_prefix = format!("Producer with transactionalId '{txn_id}' and (producerId=");
    assert!(
        commit_message.starts_with(&expected_prefix)
            && commit_message.ends_with(") attempted to produce with an old epoch"),
        "unexpected commitTransaction message: {commit_message}"
    );

    producer.close().await.expect("producer close");
    ctx.cleanup().await;
}

/// Fencing a producer inside an open transaction: its next send fails through
/// the future with `ProducerFencedException` or `InvalidProducerEpochException`,
/// and `commitTransaction` fails with an `ApiException` of one of those two.
///
/// Translates `AdminFenceProducersTest.testFenceBeforeProducerCommit`
/// (`AdminFenceProducersTest.java:111-137`).
#[tokio::test]
async fn test_fence_before_producer_commit() {
    let mut ctx = TestContext::new(admin_fence_producers_cluster()).await;
    let topic = ctx.topic("mytopic");
    create_single_broker_topic(&ctx, &topic, 1, BTreeMap::new()).await;
    let bootstrap = ctx.bootstrap_servers().to_string();
    let txn_id = ctx.group_id("mytxnid");
    let producer = admin_fence_producers_producer(&bootstrap, &txn_id);

    producer.init_transactions().await.expect("initTransactions");
    producer.begin_transaction().expect("beginTransaction");
    send_record(&producer, admin_fence_producers_record(&topic))
        .await
        .expect("send")
        .get()
        .await
        .expect("the first record is acked");

    fence_producer(&ctx, &txn_id).await;

    let exception_during_send = send_record(&producer, admin_fence_producers_record(&topic))
        .await
        .expect("send is accepted; the failure arrives through the future")
        .get()
        .await
        .expect_err("expected ProducerFencedException");
    assert!(
        matches!(exception_during_send, Error::ProducerFenced(_) | Error::InvalidProducerEpoch(_)),
        "expected ProducerFenced or InvalidProducerEpoch, got {exception_during_send:?}"
    );

    let exception_during_commit = producer.commit_transaction().await.expect_err("Expected Exception");
    assert!(
        exception_during_commit.is_api_error(),
        "expected an ApiException, got {exception_during_commit:?}"
    );
    assert!(
        matches!(
            exception_during_commit,
            Error::ProducerFenced(_) | Error::InvalidProducerEpoch(_)
        ),
        "expected ProducerFenced or InvalidProducerEpoch, got {exception_during_commit:?}"
    );

    producer.close().await.expect("producer close");
    ctx.cleanup().await;
}

// ---------------------------------------------------------------------------
// ProducerIdExpirationTest / TransactionsExpirationTest translations
// ---------------------------------------------------------------------------
//
// Both Java classes run on three brokers with short producer-id and
// transactional-id expiration intervals (`ProducerIdExpirationTest.java:76-100`,
// `TransactionsExpirationTest.java:65-86`). The two differ only in which of the
// two expires first, so each gets its own pooled cluster
// ([`producer_id_expiration_cluster`] / [`transactions_expiration_cluster`]).
// Deviations shared by these tests, each forced by the pooled harness:
//
//   - Java's fixed topic names (`topic1` / `topic2`) and transactional id
//     (`transactionalProducer`) become per-test names (`ctx.topic` /
//     `ctx.group_id`), because the cluster is pooled and both classes' tests
//     share it (concurrently, and across repeated runs of the same test).
//   - `TransactionsExpirationTest` runs each scenario at `transaction.version` 1
//     and 2 (`@ClusterFeature`). The pooled `apache/kafka:4.2.0` brokers are
//     formatted at the latest metadata version, which finalizes
//     `transaction.version=2`, and the harness cannot re-format them at a lower
//     feature level — so only the TV2 rows are translated;
//     `testFatalErrorAfterInvalidProducerIdMappingWithTV1` and
//     `testTransactionAfterProducerIdExpiresWithTV1` are not reachable here.
//   - `ProducerIdExpirationTest.testDynamicProducerIdExpirationMs` is not
//     translated: its second half restarts a broker (`kafkaBroker.shutdown()` /
//     `startup()`, `ProducerIdExpirationTest.java:196-200`), which the harness
//     cannot do, and its first half reads the broker's in-memory
//     `logManager().producerStateManagerConfig()` to prove the dynamic update
//     landed — neither is observable from a client.
//   - `assertConsumeRecords` iterates `supportedGroupProtocols()`
//     (`TransactionsExpirationTest.java:243`); only the CONSUMER (KIP-848) arm is
//     translated (`consumer-threading.md` §20).

/// The broker overrides both expiration suites share
/// (`ProducerIdExpirationTest.java:76-100` / `TransactionsExpirationTest.java:65-86`)
/// on top of [`kip848_3_broker`]'s KIP-848 settings (which already give
/// `__consumer_offsets` Java's single partition). Only the two expiration
/// intervals differ between the suites.
fn expiration_cluster(transactional_id_expiration_ms: &str, producer_id_expiration_ms: &str) -> ClusterConfig {
    let mut config = kip848_3_broker(1);
    for (key, value) in [
        ("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"),
        ("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "3"),
        ("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "2"),
        ("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "2"),
        ("KAFKA_CONTROLLED_SHUTDOWN_ENABLE", "true"),
        ("KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE", "false"),
        ("KAFKA_AUTO_LEADER_REBALANCE_ENABLE", "false"),
        ("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0"),
        ("KAFKA_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS", "200"),
        ("KAFKA_TRANSACTIONAL_ID_EXPIRATION_MS", transactional_id_expiration_ms),
        ("KAFKA_TRANSACTION_REMOVE_EXPIRED_TRANSACTION_CLEANUP_INTERVAL_MS", "500"),
        ("KAFKA_PRODUCER_ID_EXPIRATION_MS", producer_id_expiration_ms),
        ("KAFKA_PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS", "500"),
    ] {
        config.server_properties.insert(key.to_string(), value.to_string());
    }
    config
}

/// `ProducerIdExpirationTest`'s cluster (`ProducerIdExpirationTest.java:76-100`):
/// the transactional id (5 s) expires before the producer id (10 s).
fn producer_id_expiration_cluster() -> ClusterConfig {
    expiration_cluster("5000", "10000")
}

/// `TransactionsExpirationTest`'s cluster (`TransactionsExpirationTest.java:65-86`):
/// the producer id (5 s) expires before the transactional id (10 s).
/// (Java sets `log.unclean.leader.election.enable` here and
/// `unclean.leader.election.enable` in `ProducerIdExpirationTest`; both are the
/// broker default `false`, so the one key [`expiration_cluster`] sets covers both.)
fn transactions_expiration_cluster() -> ClusterConfig {
    expiration_cluster("10000", "5000")
}

/// `ClusterInstance.consumer(configs)` (`ClusterInstance.java:161-169`): `configs`
/// plus byte-array deserializers, `auto.offset.reset=earliest`, a random group id
/// and the cluster's bootstrap servers — CONSUMER (KIP-848) arm.
fn cluster_consumer(ctx: &TestContext, configs: &[(&str, &str)]) -> Box<dyn Consumer<Vec<u8>, Vec<u8>>> {
    let mut props: HashMap<String, String> = configs
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    for (key, value) in [
        ("group.protocol", "consumer".to_string()),
        ("auto.offset.reset", "earliest".to_string()),
        ("group.id", ctx.group_id("group")),
        ("bootstrap.servers", ctx.bootstrap_servers().to_string()),
    ] {
        props.entry(key.to_string()).or_insert(value);
    }
    KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        ConsumerConfig::new(&props).expect("invalid consumer config"),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("KafkaConsumer::new should succeed")
}

/// Creates `topic` with `num_partitions` partitions at replication factor 3 and
/// waits until every partition has a leader — `ClusterInstance.createTopic`
/// (which returns once the topic's metadata has propagated).
async fn create_expiration_topic(ctx: &TestContext, topic: &str, num_partitions: i32) {
    let admin = txn_test_admin(ctx);
    test_utils::create_topic(admin.as_ref(), topic, num_partitions, 3).await;
    test_utils::wait_for_partition_leaders(admin.as_ref(), topic, 0..num_partitions).await;
    admin.close().await;
}

/// `admin.describeProducers(List.of(tp)).partitionResult(tp).get().activeProducers()`
/// (`ProducerIdExpirationTest.java:261-266`, `TransactionsExpirationTest.java:232-236`).
async fn producer_states(admin: &dyn Admin, tp: &TopicPartition) -> Result<Vec<ProducerState>, Error> {
    let states = admin
        .describe_producers(std::slice::from_ref(tp))
        .partition_result(tp)?
        .get()
        .await?;
    Ok(states.active_producers().to_vec())
}

/// Whether `admin.describeTransactions(List.of(id)).description(id).get()` fails
/// with `TransactionalIdNotFoundException` — the condition of both suites'
/// `waitUntilTransactionalStateExpires` (`ProducerIdExpirationTest.java:237-250`,
/// `TransactionsExpirationTest.java:218-230`). Any other outcome is "not yet".
async fn transactional_state_expired(admin: &dyn Admin, transactional_id: &str) -> bool {
    let description = admin
        .describe_transactions(&[transactional_id.to_string()])
        .description(transactional_id);
    match description {
        Ok(future) => matches!(future.get().await, Err(Error::TransactionalIdNotFound(_))),
        Err(_) => false,
    }
}

/// `waitUntilTransactionalStateExpires(admin)` (`ProducerIdExpirationTest.java:237-250`,
/// `TransactionsExpirationTest.java:218-230`).
async fn wait_until_transactional_state_expires(admin: &dyn Admin, transactional_id: &str) {
    test_utils::wait_until_true_with_timeout(
        || transactional_state_expired(admin, transactional_id),
        "Transaction state never expired.",
        test_utils::DEFAULT_MAX_WAIT_MS,
        test_utils::DEFAULT_PAUSE_MS,
    )
    .await;
}

/// `TransactionsExpirationTest.waitUntilTransactionalStateExists`
/// (`TransactionsExpirationTest.java:205-216`).
async fn wait_until_transactional_state_exists(ctx: &TestContext, transactional_id: &str) {
    let admin = txn_test_admin(ctx);
    test_utils::wait_until_true_with_timeout(
        || async {
            match admin
                .describe_transactions(&[transactional_id.to_string()])
                .description(transactional_id)
            {
                Ok(future) => future.get().await.is_ok(),
                Err(_) => false,
            }
        },
        "Transactional state was never added.",
        test_utils::DEFAULT_MAX_WAIT_MS,
        test_utils::DEFAULT_PAUSE_MS,
    )
    .await;
    admin.close().await;
}

/// `ProducerIdExpirationTest.waitProducerIdExpire(admin)`
/// (`ProducerIdExpirationTest.java:219-231`). Java rethrows a `describeProducers`
/// failure out of the condition, failing the test; so does this.
async fn wait_producer_id_expire(admin: &dyn Admin, tp: &TopicPartition) {
    test_utils::wait_until_true_with_timeout(
        || async { producer_states(admin, tp).await.expect("describeProducers").is_empty() },
        "Producer ID expired.",
        test_utils::DEFAULT_MAX_WAIT_MS,
        test_utils::DEFAULT_PAUSE_MS,
    )
    .await;
}

/// The send that follows an expired mapping fails through its future with
/// `InvalidPidMappingException`, and the producer is then in a fatal state, so
/// `abortTransaction` fails with a `KafkaException`
/// (`ProducerIdExpirationTest.java:143-153`, `TransactionsExpirationTest.java:124-132`).
///
/// Java's `assertFutureThrows(InvalidPidMappingException.class, ...)` and
/// `assertThrows(KafkaException.class, ...)` check classes only; the variant, the
/// predicate and the exact messages are asserted here as well.
async fn assert_send_fails_with_invalid_pid_mapping_then_abort_is_fatal(
    producer: &KafkaProducer<Vec<u8>, Vec<u8>>,
    record: ProducerRecord<Vec<u8>, Vec<u8>>,
) {
    let failed_future = send_record(producer, record).await.expect("send registers the record");
    test_utils::wait_until_true_with_timeout(
        || async { failed_future.is_done() },
        "Producer future never completed.",
        test_utils::DEFAULT_MAX_WAIT_MS,
        test_utils::DEFAULT_PAUSE_MS,
    )
    .await;
    let error = failed_future.get().await.expect_err("the send must fail");
    assert!(
        matches!(error, Error::InvalidPidMapping(_)),
        "expected InvalidPidMapping, got {error:?}"
    );
    assert_eq!(error.message(), Errors::InvalidProducerIdMapping.message());

    let abort_error = producer
        .abort_transaction()
        .await
        .expect_err("abortTransaction must fail after the fatal error");
    // `TransactionManager.maybeFailWithError` throws a bare
    // `KafkaException("Cannot execute transactional method because we are in an
    // error state", lastError)` (`TransactionManager.java:1189`), so the variant is the
    // bare `KafkaError` and its source is the fatal `InvalidPidMapping`.
    assert!(abort_error.is_kafka_error(), "expected a KafkaException, got {abort_error:?}");
    assert!(
        matches!(abort_error, Error::KafkaError(_)),
        "expected a bare KafkaException, got {abort_error:?}"
    );
    assert_eq!(
        abort_error.message(),
        "Cannot execute transactional method because we are in an error state"
    );
    assert!(
        matches!(abort_error.source(), Some(Error::InvalidPidMapping(_))),
        "expected the InvalidPidMapping cause, got {:?}",
        abort_error.source()
    );
}

/// An idempotent producer's id expires from the partition's producer state once
/// it stops writing, and reappears when it writes again.
///
/// Translates `ProducerIdExpirationTest.testProducerIdExpirationWithNoTransactions`
/// (`ProducerIdExpirationTest.java:110-129`).
#[tokio::test]
async fn test_producer_id_expiration_with_no_transactions() {
    let mut ctx = TestContext::new(producer_id_expiration_cluster()).await;
    let topic1 = ctx.topic("topic1");
    let tp0 = TopicPartition::new(topic1.clone(), 0);
    create_expiration_topic(&ctx, &topic1, 1).await;
    let producer = cluster_producer(ctx.bootstrap_servers(), &[("enable.idempotence", "true")]);
    let record = || {
        ProducerRecord::with_partition_key(topic1.clone(), Some(0), Some(b"key".to_vec()), Some(b"value".to_vec()))
            .expect("record")
    };

    // Send records to populate producer state cache.
    send_record(&producer, record()).await.expect("send");
    producer.flush().await.expect("flush");
    let admin = txn_test_admin(&ctx);
    assert_eq!(1, producer_states(admin.as_ref(), &tp0).await.expect("describeProducers").len());

    wait_producer_id_expire(admin.as_ref(), &tp0).await;

    // Send more records to send producer ID back to brokers.
    send_record(&producer, record()).await.expect("send");
    producer.flush().await.expect("flush");

    // Producer IDs should repopulate.
    assert_eq!(1, producer_states(admin.as_ref(), &tp0).await.expect("describeProducers").len());

    admin.close().await;
    producer.close().await.expect("producer close");
    ctx.cleanup().await;
}

/// A transactional id that expires while its producer id is still in the
/// partition's producer state makes the next transactional send fail with
/// `InvalidPidMappingException` (fatal); a fresh producer recovers and commits.
///
/// Translates `ProducerIdExpirationTest.testTransactionAfterTransactionIdExpiresButProducerIdRemains`
/// (`ProducerIdExpirationTest.java:131-175`).
#[tokio::test]
async fn test_transaction_after_transaction_id_expires_but_producer_id_remains() {
    let mut ctx = TestContext::new(producer_id_expiration_cluster()).await;
    let topic1 = ctx.topic("topic1");
    let tp0 = TopicPartition::new(topic1.clone(), 0);
    let transactional_id = ctx.group_id("transactionalProducer");
    create_expiration_topic(&ctx, &topic1, 1).await;
    // `transactionalProducerConfig()` (`ProducerIdExpirationTest.java:252-257`).
    let transactional_producer_config = [
        ("transactional.id", transactional_id.as_str()),
        ("enable.idempotence", "true"),
        ("acks", "all"),
    ];
    let producer = cluster_producer(ctx.bootstrap_servers(), &transactional_producer_config);
    producer.init_transactions().await.expect("initTransactions");

    // Start and then abort a transaction to allow the producer ID to expire.
    producer.begin_transaction().expect("beginTransaction");
    send_record(
        &producer,
        producer_record_with_expected_transaction_status(&topic1, Some(0), "2", "2", false),
    )
    .await
    .expect("send");
    producer.flush().await.expect("flush");
    let mut consumer = cluster_consumer(&ctx, &[("isolation.level", "read_committed")]);

    let admin = txn_test_admin(&ctx);
    // Ensure producer IDs are added.
    test_utils::wait_until_true_with_timeout(
        || async { producer_states(admin.as_ref(), &tp0).await.expect("describeProducers").len() == 1 },
        "Producer IDs were not added.",
        test_utils::DEFAULT_MAX_WAIT_MS,
        100,
    )
    .await;

    producer.abort_transaction().await.expect("abortTransaction");

    // Wait for the transactional ID to expire.
    wait_until_transactional_state_expires(admin.as_ref(), &transactional_id).await;

    // Producer IDs should be retained.
    assert_eq!(1, producer_states(admin.as_ref(), &tp0).await.expect("describeProducers").len());

    // Start a new transaction and attempt to send, triggering an
    // AddPartitionsToTxnRequest that will fail due to the expired transactional
    // ID, resulting in a fatal error.
    producer.begin_transaction().expect("beginTransaction");
    assert_send_fails_with_invalid_pid_mapping_then_abort_is_fatal(
        &producer,
        producer_record_with_expected_transaction_status(&topic1, Some(0), "1", "1", false),
    )
    .await;

    // Close the producer and reinitialize to recover from the fatal error.
    producer.close().await.expect("producer close");
    let producer = cluster_producer(ctx.bootstrap_servers(), &transactional_producer_config);
    producer.init_transactions().await.expect("initTransactions");

    producer.begin_transaction().expect("beginTransaction");
    send_record(
        &producer,
        producer_record_with_expected_transaction_status(&topic1, Some(0), "4", "4", true),
    )
    .await
    .expect("send");
    send_record(
        &producer,
        producer_record_with_expected_transaction_status(&topic1, Some(0), "3", "3", true),
    )
    .await
    .expect("send");

    // Producer IDs should be retained.
    //
    // Deviation from Java's single `assertFalse(producerStates(admin).isEmpty())`
    // (`ProducerIdExpirationTest.java:165-166`): the old producer id's entry
    // expires ~10-10.5 s after the abort marker, and the two sends above are not
    // flushed, so a single check can land in the gap between that expiry and the
    // new records' append and see `Ok([])` — Java has the same race, it just
    // reaches this line faster. Waiting (like `waitUntilTrue`) until a
    // successful, non-empty result closes the gap without adding a `flush()`
    // Java does not make.
    test_utils::wait_until_true_with_timeout(
        || async {
            producer_states(admin.as_ref(), &tp0)
                .await
                .is_ok_and(|states| !states.is_empty())
        },
        "Producer IDs were not retained.",
        test_utils::DEFAULT_MAX_WAIT_MS,
        100,
    )
    .await;

    producer.commit_transaction().await.expect("commitTransaction");

    // Check we can still consume the transaction.
    consumer.subscribe_with_topics(vec![topic1.clone()]).await.expect("subscribe");
    for record in consume_records(&mut consumer, 2).await {
        assert_committed_and_get_value(&record);
    }

    admin.close().await;
    producer.close().await.expect("producer close");
    consumer
        .close_with_options(CloseOptions::new_timeout(Duration::from_secs(1)))
        .await
        .expect("consumer close");
    ctx.cleanup().await;
}

/// `new ProducerRecord<>(topic, partition, key, value, Set.of(new RecordHeader(HEADER_KEY, value)))`
/// as `TransactionsExpirationTest` builds its records inline
/// (`TransactionsExpirationTest.java:119-120`, ...).
fn expiration_record(
    topic: &str,
    partition: Option<i32>,
    key_and_value: &str,
    header_value: &[u8],
) -> ProducerRecord<Vec<u8>, Vec<u8>> {
    producer_record_with_expected_transaction_status(
        topic,
        partition,
        key_and_value,
        key_and_value,
        header_value == COMMITTED_VALUE,
    )
}

/// `TransactionsExpirationTest.assertConsumeRecords` (`TransactionsExpirationTest.java:238-263`),
/// CONSUMER arm: a read_committed consumer subscribed to `topics` sees exactly
/// `expected_count` records within 15 s, each carrying the committed header.
async fn assert_consume_records(ctx: &TestContext, topics: &[String], expected_count: usize) {
    let mut consumer = cluster_consumer(ctx, &[("enable.auto.commit", "false"), ("isolation.level", "read_committed")]);
    consumer.subscribe_with_topics(topics.to_vec()).await.expect("subscribe");
    let mut consumer_records = Vec::new();
    let start = Instant::now();
    loop {
        consumer_records.extend(consumer.poll(Duration::from_millis(100)).await.expect("poll"));
        if consumer_records.len() == expected_count {
            break;
        }
        assert!(
            start.elapsed() <= Duration::from_millis(15_000),
            "Consumer with protocol CONSUMER should consume {expected_count} records, but get {}",
            consumer_records.len()
        );
        tokio::time::sleep(Duration::from_millis(test_utils::DEFAULT_PAUSE_MS)).await;
    }
    consumer.close().await.expect("consumer close");
    for record in &consumer_records {
        let header = record
            .headers()
            .headers(TRANSACTION_STATUS_KEY)
            .first()
            .cloned()
            .expect("the record carries a transactionStatus header");
        assert_eq!(
            Some(COMMITTED_VALUE),
            header.value(),
            "Record does not have the expected header value."
        );
    }
}

/// Once the transactional id has expired, the next transactional send fails with
/// `InvalidPidMappingException` (fatal); a reinitialized producer with the same id
/// commits a new transaction across two topics.
///
/// Translates `TransactionsExpirationTest.testFatalErrorAfterInvalidProducerIdMappingWithTV2`
/// (`TransactionsExpirationTest.java:94-97`, body `:109-153`). The TV1 row is not
/// reachable (see the section note).
#[tokio::test]
async fn test_fatal_error_after_invalid_producer_id_mapping_with_tv2() {
    let mut ctx = TestContext::new(transactions_expiration_cluster()).await;
    let topic1 = ctx.topic("topic1");
    let topic2 = ctx.topic("topic2");
    let transaction_id = ctx.group_id("transactionalProducer");
    create_expiration_topic(&ctx, &topic1, 4).await;
    create_expiration_topic(&ctx, &topic2, 4).await;
    let bootstrap = ctx.bootstrap_servers().to_string();

    {
        let producer = cluster_producer(&bootstrap, &[("transactional.id", transaction_id.as_str())]);
        producer.init_transactions().await.expect("initTransactions");
        // Start and then abort a transaction to allow the transactional ID to expire.
        producer.begin_transaction().expect("beginTransaction");
        send_record(&producer, expiration_record(&topic1, Some(0), "2", ABORTED_VALUE))
            .await
            .expect("send");
        send_record(&producer, expiration_record(&topic2, Some(0), "4", ABORTED_VALUE))
            .await
            .expect("send");
        producer.abort_transaction().await.expect("abortTransaction");

        // Check the transactional state exists and then wait for it to expire.
        wait_until_transactional_state_exists(&ctx, &transaction_id).await;
        let admin = txn_test_admin(&ctx);
        wait_until_transactional_state_expires(admin.as_ref(), &transaction_id).await;
        admin.close().await;

        // Start a new transaction and attempt to send, triggering an
        // AddPartitionsToTxnRequest that will fail due to the expired
        // transactional ID, resulting in a fatal error.
        producer.begin_transaction().expect("beginTransaction");
        assert_send_fails_with_invalid_pid_mapping_then_abort_is_fatal(
            &producer,
            expiration_record(&topic1, Some(3), "1", ABORTED_VALUE),
        )
        .await;
        producer.close().await.expect("producer close");
    }

    // Reinitialize to recover from the fatal error.
    {
        let producer = cluster_producer(&bootstrap, &[("transactional.id", transaction_id.as_str())]);
        producer.init_transactions().await.expect("initTransactions");
        // Proceed with a new transaction after reinitializing.
        producer.begin_transaction().expect("beginTransaction");
        for record in [
            expiration_record(&topic2, None, "2", COMMITTED_VALUE),
            expiration_record(&topic1, Some(2), "4", COMMITTED_VALUE),
            expiration_record(&topic2, None, "1", COMMITTED_VALUE),
            expiration_record(&topic1, Some(3), "3", COMMITTED_VALUE),
        ] {
            send_record(&producer, record).await.expect("send");
        }
        producer.commit_transaction().await.expect("commitTransaction");

        wait_until_transactional_state_exists(&ctx, &transaction_id).await;
        producer.close().await.expect("producer close");
    }

    assert_consume_records(&ctx, &[topic1, topic2], 4).await;
    ctx.cleanup().await;
}

/// `TransactionsExpirationTest`'s "Ensure producer IDs are added" / "Producer IDs
/// should repopulate" wait (`TransactionsExpirationTest.java:167-177`, `:201-211`):
/// accumulate `describeProducers` results until non-empty, treating a failed call
/// as "not yet", then assert exactly one producer was seen.
async fn wait_for_single_producer_state(ctx: &TestContext, tp: &TopicPartition) -> ProducerState {
    let admin = txn_test_admin(ctx);
    // Java's `producerStates.addAll(...)` into a list the lambda captures; the
    // `RefCell` borrow is taken only after the await.
    let producer_states_seen = std::cell::RefCell::new(Vec::new());
    test_utils::wait_until_true_with_timeout(
        || async {
            if let Ok(states) = producer_states(admin.as_ref(), tp).await {
                producer_states_seen.borrow_mut().extend(states);
            }
            !producer_states_seen.borrow().is_empty()
        },
        &format!("Producer IDs for {tp} did not propagate quickly"),
        test_utils::DEFAULT_MAX_WAIT_MS,
        test_utils::DEFAULT_PAUSE_MS,
    )
    .await;
    admin.close().await;
    let mut producer_states_seen = producer_states_seen.into_inner();
    assert_eq!(1, producer_states_seen.len(), "Unexpected producer to {tp}");
    producer_states_seen.remove(0)
}

/// After the producer id expires from the partition's producer state, the
/// transactional id still maps to it: a new producer with the same id reuses the
/// producer id with a bumped epoch, and its transaction commits.
///
/// Translates `TransactionsExpirationTest.testTransactionAfterProducerIdExpiresWithTV2`
/// (`TransactionsExpirationTest.java:104-107`, body `:155-203`). The TV1 row is
/// not reachable (see the section note).
#[tokio::test]
async fn test_transaction_after_producer_id_expires_with_tv2() {
    let mut ctx = TestContext::new(transactions_expiration_cluster()).await;
    let topic1 = ctx.topic("topic1");
    let topic1_partition0 = TopicPartition::new(topic1.clone(), 0);
    let transaction_id = ctx.group_id("transactionalProducer");
    create_expiration_topic(&ctx, &topic1, 4).await;
    let bootstrap = ctx.bootstrap_servers().to_string();

    let (old_producer_id, old_producer_epoch) = {
        let producer = cluster_producer(&bootstrap, &[("transactional.id", transaction_id.as_str())]);
        producer.init_transactions().await.expect("initTransactions");

        // Start and then abort a transaction to allow the producer ID to expire.
        producer.begin_transaction().expect("beginTransaction");
        send_record(&producer, expiration_record(&topic1, Some(0), "2", ABORTED_VALUE))
            .await
            .expect("send");
        producer.flush().await.expect("flush");

        // Ensure producer IDs are added.
        let producer_state = wait_for_single_producer_state(&ctx, &topic1_partition0).await;

        producer.abort_transaction().await.expect("abortTransaction");

        // Wait for the producer ID to expire.
        let admin = txn_test_admin(&ctx);
        test_utils::wait_until_true_with_timeout(
            || async {
                producer_states(admin.as_ref(), &topic1_partition0)
                    .await
                    .is_ok_and(|states| states.is_empty())
            },
            &format!("Producer IDs for {topic1_partition0} did not expire."),
            test_utils::DEFAULT_MAX_WAIT_MS,
            test_utils::DEFAULT_PAUSE_MS,
        )
        .await;
        admin.close().await;
        producer.close().await.expect("producer close");
        (producer_state.producer_id(), producer_state.producer_epoch())
    };

    // Create a new producer to check that we retain the producer ID in
    // transactional state.
    let producer = cluster_producer(&bootstrap, &[("transactional.id", transaction_id.as_str())]);
    producer.init_transactions().await.expect("initTransactions");

    // Start a new transaction and attempt to send. This should work since only
    // the producer ID was removed from its mapping in ProducerStateManager.
    producer.begin_transaction().expect("beginTransaction");
    send_record(&producer, expiration_record(&topic1, Some(0), "4", COMMITTED_VALUE))
        .await
        .expect("send");
    send_record(&producer, expiration_record(&topic1, Some(3), "3", COMMITTED_VALUE))
        .await
        .expect("send");
    producer.commit_transaction().await.expect("commitTransaction");

    // Producer IDs should repopulate.
    let producer_state = wait_for_single_producer_state(&ctx, &topic1_partition0).await;
    let new_producer_id = producer_state.producer_id();
    let new_producer_epoch = producer_state.producer_epoch();

    // Because the transaction IDs outlive the producer IDs, creating a producer
    // with the same transactional id soon after the first will re-use the same
    // producerId, while bumping the epoch to indicate that they are distinct.
    assert_eq!(old_producer_id, new_producer_id);
    // TV2 bumps epoch on EndTxn, and the final commit may or may not have bumped
    // the epoch in the producer state. The epoch should be at least
    // oldProducerEpoch + 2 for the first commit and the restarted producer.
    assert!(
        old_producer_epoch + 2 <= new_producer_epoch,
        "expected epoch >= {} but was {new_producer_epoch}",
        old_producer_epoch + 2
    );

    assert_consume_records(&ctx, &[topic1], 2).await;
    producer.close().await.expect("producer close");
    ctx.cleanup().await;
}

// ---------------------------------------------------------------------------
// Multilanguage instantiations (Milestone 11 CFFI)
// ---------------------------------------------------------------------------
//
// The two atomicity scenarios (2, 3) use only the four transaction *control*
// RPCs (init / begin / commit / abort); the consume-transform-produce scenario
// (4) additionally uses `send_offsets_to_transaction`. The C gRPC server now
// exposes all of these, so under `multilanguage-tests` these three fan out to
// rust / python / c via the macro. Otherwise `rust_only_fallback` runs each
// against `RustNativeFactory` — the single native home for these scenarios,
// whose hand-written `#[tokio::test]` versions were folded into the `_inner`
// bodies above to avoid duplication. Either way only the *producer* crosses the
// gRPC boundary; the seed producer, scenario 4's input transform-consumer
// (whose `group_metadata()` feeds `send_offsets_to_transaction`), and every
// verification consumer stay native, reading the same broker at its host
// listener.
//
// Python is deferred: the Python gRPC servers have no transaction handlers yet
// (tracked to follow-up PR #175), so the `__grpc_python` / `__grpc_python_async`
// targets these macros generate fail at runtime with UNIMPLEMENTED. That is the
// expected, known-deferred state for every transactional multilanguage scenario
// here — do not read it as a regression.
//
// The epoch-bump (scenario 1) scenario stays native-only: fencing needs two
// producers sharing a transactional id, which the harness does not model
// (PLAN §9.6).

#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(
    test_transactional_records_are_visible_only_after_commit,
    transactional_records_are_visible_only_after_commit_inner,
    cluster_config()
);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(
    test_aborted_transaction_records_are_discarded,
    aborted_transaction_records_are_discarded_inner,
    cluster_config()
);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(
    test_consume_transform_produce_with_offsets,
    consume_transform_produce_with_offsets_inner,
    cluster_config()
);

#[cfg(all(feature = "integration-tests", not(feature = "multilanguage-tests")))]
mod rust_only_fallback {
    use super::*;
    use crate::common::backend_factory::RustNativeFactory;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_transactional_records_are_visible_only_after_commit() {
        let mut ctx = TestContext::new(cluster_config()).await;
        transactional_records_are_visible_only_after_commit_inner(&mut ctx, &RustNativeFactory).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_aborted_transaction_records_are_discarded() {
        let mut ctx = TestContext::new(cluster_config()).await;
        aborted_transaction_records_are_discarded_inner(&mut ctx, &RustNativeFactory).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_consume_transform_produce_with_offsets() {
        let mut ctx = TestContext::new(cluster_config()).await;
        consume_transform_produce_with_offsets_inner(&mut ctx, &RustNativeFactory).await;
    }
}
