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
//!    → [`test_transactional_records_are_visible_only_after_commit`]
//! 3. an abort discards its records
//!    → [`test_aborted_transaction_records_are_discarded`]
//! 4. consume-transform-produce with `send_offsets_to_transaction`
//!    → [`test_consume_transform_produce_with_offsets`]
//!
//! # Why these are not translations
//!
//! Unlike every other file in this directory, these have no Java counterpart to
//! cite. Apache Kafka's transactional broker-integration coverage lives in the
//! Scala suites (`core/src/test/scala/integration/kafka/api/TransactionsTest.scala`
//! and friends), which are out of scope for a Java-client port — the client-side
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
//! Only [`test_consume_transform_produce_with_offsets`]'s input consumer
//! subscribes, because `send_offsets_to_transaction` needs real group metadata
//! (a generation and member id) for the broker to accept the `TxnOffsetCommit`.

use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use confluent_kafka::common::KafkaError;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::protocol::Errors;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::common::serialization::Deserializer;
use confluent_kafka::consumer::Consumer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::OffsetAndMetadata;
use confluent_kafka::consumer::new_consumer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use crate::common::cluster_config::{ClusterConfig, kip848_3_broker};
use crate::common::test_context::TestContext;

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
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Vec<u8>, KafkaError> {
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
    KafkaProducer::from_config(
        ProducerConfig::from_properties(&props).expect("invalid transactional producer config"),
        Box::new(ByteArraySerializer),
        Box::new(ByteArraySerializer),
    )
    .expect("failed to build a transactional producer")
}

/// A consumer pinned to `tp` at its beginning, at the given isolation level.
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
    new_consumer::<Vec<u8>, Vec<u8>>(
        ConsumerConfig::from_properties(&props).expect("invalid consumer config"),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed")
}

/// Sends `values` inside the current transaction, awaiting each ack.
async fn send_all(producer: &KafkaProducer<Vec<u8>, Vec<u8>>, topic: &str, partition: i32, values: &[&str]) {
    for value in values {
        let record = ProducerRecord::new(
            topic.to_string(),
            Some(partition),
            None,
            Some(format!("k-{value}").into_bytes()),
            Some(value.as_bytes().to_vec()),
            None,
        )
        .expect("ProducerRecord::new should not fail");
        // UFCS, because `KafkaProducer` has both an inherent `send` and the trait
        // method and the inherent one would shadow it.
        let future = <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(producer, record)
            .await
            .expect("send should be accepted");
        future
            .get_timeout(Duration::from_secs(30))
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
/// The only way to make a *real* broker bump a producer's epoch from the client
/// API is a second `initTransactions` on the same `transactional.id`: the
/// coordinator fences the previous incarnation and hands the new one a higher
/// epoch. So the scenario is realised as two producers sharing a
/// `transactional.id`:
///
///   - the first commits a transaction at its original epoch;
///   - the second calls `init_transactions`, which bumps the epoch and aborts any
///     transaction the first left open;
///   - the second then produces and commits at the bumped epoch, and its records
///     land exactly once — which is the idempotence claim, since the sequence
///     numbers restart at 0 under the new epoch and the broker must accept them;
///   - the first, now fenced, cannot produce.
///
/// Asserting all four is what makes this a test of the *bump* rather than of
/// fencing alone: a client that failed to reset its sequences after the bump
/// would be rejected with `OutOfOrderSequenceNumber` at the third step.
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
    let record = ProducerRecord::new(
        topic.clone(),
        Some(0),
        None,
        Some(b"k-fenced".to_vec()),
        Some(b"fenced".to_vec()),
        None,
    )
    .expect("ProducerRecord::new should not fail");
    let sent = <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(&first, record).await;
    let fenced = match sent {
        // The fencing may be reported synchronously (the manager already knows it
        // is fenced) or asynchronously through the record's future, depending on
        // whether the EndTxn or the Produce is the first request to learn of it.
        Err(error) => error,
        Ok(future) => future
            .get_timeout(Duration::from_secs(30))
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
/// The `read_uncommitted` half is what makes the `read_committed` half
/// meaningful: it proves the records really are on the broker while the
/// transaction is open, so the empty `read_committed` read is the isolation level
/// doing its job rather than a produce that never happened.
#[tokio::test]
async fn test_transactional_records_are_visible_only_after_commit() {
    let mut ctx = TestContext::new(cluster_config()).await;
    let topic = ctx.topic("txn-visible-after-commit");
    let bootstrap = ctx.bootstrap_servers().to_string();
    let tp = TopicPartition::new(topic.clone(), 0);

    let producer = transactional_producer(&bootstrap, &format!("{topic}-txn-id"));
    producer.init_transactions().await.expect("initTransactions");
    producer.begin_transaction().expect("beginTransaction");
    send_all(&producer, &topic, 0, &["v1", "v2", "v3"]).await;

    // Before the commit: read_committed sees nothing.
    let committed_group = ctx.group_id("txn-visible-committed");
    let mut committed_reader = assigned_consumer(&bootstrap, &committed_group, "read_committed");
    committed_reader.assign(vec![tp.clone()]).await.expect("assign");
    let before = drain_for(&mut committed_reader, NEGATIVE_POLL_BUDGET).await;
    assert!(
        before.is_empty(),
        "a read_committed consumer must not see records of an open transaction, saw {before:?}"
    );

    // Before the commit: read_uncommitted does see them, so they are genuinely
    // on the broker.
    let uncommitted_group = ctx.group_id("txn-visible-uncommitted");
    let mut uncommitted_reader = assigned_consumer(&bootstrap, &uncommitted_group, "read_uncommitted");
    uncommitted_reader.assign(vec![tp.clone()]).await.expect("assign");
    let uncommitted = consume_values(&mut uncommitted_reader, 3, CONSUME_DEADLINE).await;
    assert_eq!(
        uncommitted,
        vec!["v1".to_string(), "v2".to_string(), "v3".to_string()],
        "a read_uncommitted consumer must see the open transaction's records"
    );

    // Commit, and the same read_committed consumer now sees them.
    producer.commit_transaction().await.expect("commitTransaction");
    let after = consume_values(&mut committed_reader, 3, CONSUME_DEADLINE).await;
    assert_eq!(
        after,
        vec!["v1".to_string(), "v2".to_string(), "v3".to_string()],
        "the committed records must become visible to read_committed"
    );

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
#[tokio::test]
async fn test_aborted_transaction_records_are_discarded() {
    let mut ctx = TestContext::new(cluster_config()).await;
    let topic = ctx.topic("txn-abort-discards");
    let bootstrap = ctx.bootstrap_servers().to_string();
    let tp = TopicPartition::new(topic.clone(), 0);

    let producer = transactional_producer(&bootstrap, &format!("{topic}-txn-id"));
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
#[tokio::test]
async fn test_consume_transform_produce_with_offsets() {
    let mut ctx = TestContext::new(cluster_config()).await;
    let input_topic = ctx.topic("txn-ctp-input");
    let output_topic = ctx.topic("txn-ctp-output");
    let bootstrap = ctx.bootstrap_servers().to_string();
    let input_tp = TopicPartition::new(input_topic.clone(), 0);

    // Seed the input topic with a plain (non-transactional) producer.
    let seed_props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.clone()),
        ("client.id".to_string(), "txn-ctp-seed".to_string()),
        ("acks".to_string(), "all".to_string()),
        ("max.block.ms".to_string(), "30000".to_string()),
        ("linger.ms".to_string(), "0".to_string()),
    ]);
    let seeder: KafkaProducer<Vec<u8>, Vec<u8>> = KafkaProducer::from_config(
        ProducerConfig::from_properties(&seed_props).expect("invalid seed producer config"),
        Box::new(ByteArraySerializer),
        Box::new(ByteArraySerializer),
    )
    .expect("failed to build the seed producer");
    send_all(&seeder, &input_topic, 0, &["a", "b", "c"]).await;

    // The transform consumer subscribes rather than assigns: it must have real
    // group metadata (generation + member id) for the coordinator to accept the
    // TxnOffsetCommit the producer sends on its behalf.
    let input_group = ctx.group_id("txn-ctp-group");
    let mut input_consumer = assigned_consumer(&bootstrap, &input_group, "read_committed");
    input_consumer.subscribe(vec![input_topic.clone()]).await.expect("subscribe");

    let consumed = consume_values(&mut input_consumer, 3, CONSUME_DEADLINE).await;
    assert_eq!(
        consumed,
        vec!["a".to_string(), "b".to_string(), "c".to_string()],
        "the input records must be consumed before they can be transformed"
    );
    let next_offset = input_consumer.position(&input_tp).await.expect("position");
    assert_eq!(next_offset, 3, "three records consumed, so the next offset is 3");

    // Transform and produce inside a transaction, committing the input offsets
    // with it.
    let producer = transactional_producer(&bootstrap, &format!("{output_topic}-txn-id"));
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
