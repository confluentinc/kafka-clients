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
use confluent_kafka::common::Error;
use confluent_kafka::common::Errors;
use confluent_kafka::common::KafkaFuture;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::header::Header;
use confluent_kafka::common::header::Headers;
use confluent_kafka::common::header::RecordHeaders;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::common::serialization::Deserializer;
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
use crate::common::cluster_config::{ClusterConfig, kip848_3_broker};
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
    let producer = factory
        .create(make_txn_config(&producer_bootstrap, &format!("{output_topic}-txn-id")))
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
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("acks".to_string(), "all".to_string()),
        ("batch.size".to_string(), "16384".to_string()),
        ("transactional.id".to_string(), transactional_id.to_string()),
        ("enable.idempotence".to_string(), "true".to_string()),
        ("transaction.timeout.ms".to_string(), "60000".to_string()),
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
    assert_eq!(num_records, records.len(), "Consumed more records than expected");
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
