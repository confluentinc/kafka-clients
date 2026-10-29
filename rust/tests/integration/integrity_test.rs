// Copyright 2026 Confluent Inc.
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

//! Data-integrity tests: every record is read back byte for byte (a SHA-256
//! digest in its identity header), exactly as often as its delivery outcome
//! allows, in order per partition and writer, and never when its transaction
//! was aborted (see `tests/common/integrity`).
//!
//! Each test runs one short workload against a pooled broker
//! (`detects_tampering` drives its clients by hand). The long-running
//! form of the same workloads is the `integrity` target
//! (`cargo xtask integrity-test`).
//!
//! These tests have no Java counterpart: Apache Kafka verifies record content
//! end to end only in small tests (`ProducerCompressionTest`,
//! `ClientsTestUtils.consumeAndVerifyRecords`) and its system tests check
//! sequence numbers and offsets, not bytes. The shapes borrow from them: the
//! expected transaction status carried in the record
//! (`TransactionsBounceTest`), the copy-with-aborts loop
//! (`TransactionalMessageCopier`), and offsets committed and read back around
//! consumer restarts (`OffsetValidationTest`).
//!
//! `self_tests` checks the checker itself, without a broker: failures are
//! produced on purpose and must be reported.
//!
//! A failing run prints its seed; `INTEGRITY_SEED=<seed>` replays the same
//! records.

use std::collections::BTreeMap;
use std::time::Duration;

use confluent_kafka::common::serialization::{ByteArraySerializer, Deserializer};
use confluent_kafka::common::{Error, TopicPartition};
use confluent_kafka::consumer::{Consumer, ConsumerConfig, KafkaConsumer};
use confluent_kafka::producer::{KafkaProducer, Producer, ProducerConfig};

use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;
use crate::integrity::checker::{Ledger, Requirements, StreamKind, Verdict};
use crate::integrity::record::{Identity, PayloadProfile, RecordId, TxnIntent, build_record};
use crate::integrity::workload::{
    GroupSpec, Props, Restart, RestartMode, RunSpec, TopicSpec, WriterSpec, create_topics, now_ms, run,
};

const CODECS: [&str; 5] = ["none", "gzip", "snappy", "lz4", "zstd"];

/// One pooled single-broker cluster for every integrity test: the transaction
/// log fits one broker, and members heartbeat every 500 ms so rebalances
/// complete quickly.
fn integrity_cluster() -> ClusterConfig {
    let props: BTreeMap<String, String> = [
        ("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1"),
        ("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1"),
        ("KAFKA_GROUP_CONSUMER_MIN_HEARTBEAT_INTERVAL_MS", "500"),
        ("KAFKA_GROUP_CONSUMER_HEARTBEAT_INTERVAL_MS", "500"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    ClusterConfig::with_properties(props)
}

/// `INTEGRITY_SEED` when set, so a failing run can be replayed; random
/// otherwise.
fn seed() -> u64 {
    match std::env::var("INTEGRITY_SEED") {
        Ok(seed) => seed.trim().parse().expect("INTEGRITY_SEED must be a u64"),
        Err(_) => rand::random(),
    }
}

async fn context() -> (TestContext, Props) {
    let ctx = TestContext::new(integrity_cluster()).await;
    let mut base = Props::new();
    ctx.configure(&mut base);
    (ctx, base)
}

fn spec() -> RunSpec {
    let mut spec = RunSpec::new(rand::random(), seed());
    spec.watchdog = Duration::from_secs(60);
    spec
}

fn assert_pass(verdict: &Verdict) {
    eprintln!("{verdict}");
    assert!(verdict.passed(), "{verdict}");
}

/// Every compression codec, the everyday record mix.
#[tokio::test(flavor = "multi_thread")]
async fn test_integrity_every_codec() {
    let (mut ctx, base) = context().await;
    let mut spec = spec();
    let mut topics = Vec::new();
    for (writer, codec) in CODECS.iter().enumerate() {
        let topic = ctx.topic(&format!("codec_{codec}"));
        spec.topics.push(TopicSpec::source(&topic, 3, 1));
        spec.writers.push(
            WriterSpec::new(writer as u32, &topic)
                .records(2000)
                .prop("compression.type", codec)
                .prop("linger.ms", "20"),
        );
        topics.push(topic);
    }
    spec.group = Some(GroupSpec::new(&ctx.group_id("codecs"), topics, 1));
    spec.requirements = Requirements { min_records_per_stream: 2000, every_partition_read: true, ..Default::default() };
    assert_pass(&run(spec, base).await);
}

/// Null and empty keys and values, tombstones, repeated header keys and null
/// header values, uncompressed and compressed.
#[tokio::test(flavor = "multi_thread")]
async fn test_integrity_record_shapes() {
    let (mut ctx, base) = context().await;
    let mut spec = spec();
    let mut topics = Vec::new();
    for (writer, codec) in ["none", "zstd"].iter().enumerate() {
        let topic = ctx.topic(&format!("shapes_{codec}"));
        spec.topics.push(TopicSpec::source(&topic, 2, 1));
        spec.writers.push(
            WriterSpec::new(writer as u32, &topic)
                .profile(PayloadProfile::edge_shapes())
                .records(3000)
                .prop("compression.type", codec),
        );
        topics.push(topic);
    }
    spec.group = Some(GroupSpec::new(&ctx.group_id("shapes"), topics, 1));
    spec.requirements = Requirements { min_records_per_stream: 3000, every_partition_read: true, ..Default::default() };
    assert_pass(&run(spec, base).await);
}

/// Sizes around the varint and batch boundaries, and records larger than the
/// consumer's fetch limits (KIP-74: the first batch is returned anyway).
#[tokio::test(flavor = "multi_thread")]
async fn test_integrity_record_sizes() {
    let (mut ctx, base) = context().await;
    let mut spec = spec();
    let boundaries = ctx.topic("sizes_boundaries");
    let large = ctx.topic("sizes_large");
    spec.topics.push(TopicSpec::source(&boundaries, 1, 1));
    spec.topics.push(TopicSpec::source(&large, 1, 1));
    let sizes = vec![
        0, 1, 2, 63, 64, 127, 128, 1023, 1024, 1025, 16383, 16384, 16385, 65536, 262_144,
    ];
    spec.writers.push(
        WriterSpec::new(0, &boundaries)
            .profile(PayloadProfile::sizes(sizes))
            .records(400),
    );
    spec.writers.push(
        WriterSpec::new(1, &large)
            .profile(PayloadProfile::sized(600_000, 900_000))
            .records(60),
    );
    spec.group = Some(
        GroupSpec::new(&ctx.group_id("sizes"), vec![boundaries, large], 1)
            .prop("max.partition.fetch.bytes", "65536")
            .prop("fetch.max.bytes", "65536"),
    );
    spec.requirements = Requirements { min_records_per_stream: 60, every_partition_read: true, ..Default::default() };
    assert_pass(&run(spec, base).await);
}

/// Transactions, one in three aborted, read with read_committed: every
/// committed record exactly once, no aborted one ever.
#[tokio::test(flavor = "multi_thread")]
async fn test_integrity_transactions() {
    let (mut ctx, base) = context().await;
    let mut spec = spec();
    let topic = ctx.topic("txn");
    spec.topics.push(TopicSpec::source(&topic, 3, 1));
    spec.writers.push(
        WriterSpec::new(0, &topic)
            .transactions(&ctx.group_id("txn-writer"), 20, 3)
            .records(1200),
    );
    spec.group = Some(GroupSpec::new(&ctx.group_id("txn"), vec![topic], 1));
    spec.requirements = Requirements {
        min_txns_committed: 5,
        min_txns_aborted: 2,
        min_records_per_stream: 100,
        ..Default::default()
    };
    assert_pass(&run(spec, base).await);
}

/// A consume-transform-produce loop with random aborts copies one topic into
/// another: every source record appears in the copy exactly once.
#[tokio::test(flavor = "multi_thread")]
async fn test_integrity_consume_transform_produce() {
    let (mut ctx, base) = context().await;
    let mut spec = spec();
    let input = ctx.topic("copy_in");
    let output = ctx.topic("copy_out");
    spec.topics.push(TopicSpec::source(&input, 3, 1));
    spec.topics.push(TopicSpec::mirror(&output, &input, 3, 1));
    spec.writers.push(WriterSpec::new(0, &input).records(2000));
    spec.copiers.push(crate::integrity::workload::CopierSpec {
        input: input.clone(),
        output: output.clone(),
        group_id: ctx.group_id("copier"),
        transactional_id: ctx.group_id("copier-txn"),
        abort_one_in: 3,
        props: Default::default(),
    });
    spec.group = Some(GroupSpec::new(&ctx.group_id("copy"), vec![input, output], 1));
    spec.requirements = Requirements { min_records_per_stream: 2000, min_copier_aborts: 1, ..Default::default() };
    assert_pass(&run(spec, base).await);
}

/// Several writers over many partitions, read by a group of three: every
/// delivery report names the partition and offset the record is read at.
#[tokio::test(flavor = "multi_thread")]
async fn test_integrity_many_partitions() {
    let (mut ctx, base) = context().await;
    let mut spec = spec();
    let topic = ctx.topic("partitions");
    spec.topics.push(TopicSpec::source(&topic, 24, 1));
    for writer in 0..3 {
        spec.writers.push(WriterSpec::new(writer, &topic).records(2000));
    }
    spec.group = Some(GroupSpec::new(&ctx.group_id("partitions"), vec![topic], 3));
    spec.requirements = Requirements { min_records_per_stream: 6000, every_partition_read: true, ..Default::default() };
    assert_pass(&run(spec, base).await);
}

/// Members close cleanly and rejoin while records keep coming. Each commits
/// everything it read before its partitions move, so nothing is read twice.
#[tokio::test(flavor = "multi_thread")]
async fn test_integrity_consumer_restart_clean() {
    let (mut ctx, base) = context().await;
    let mut spec = spec();
    let topic = ctx.topic("restart_clean");
    spec.topics.push(TopicSpec::source(&topic, 6, 1));
    spec.writers.push(WriterSpec::new(0, &topic).rate(500).records(5000));
    spec.group = Some(
        GroupSpec::new(&ctx.group_id("restart_clean"), vec![topic], 2)
            .restart(Duration::from_secs(2), &[RestartMode::Clean]),
    );
    spec.requirements = Requirements { min_records_per_stream: 5000, min_clean_restarts: 3, ..Default::default() };
    let verdict = run(spec, base).await;
    assert_pass(&verdict);
    assert_eq!(verdict.counts.redelivered, 0, "a clean handoff re-read records:\n{verdict}");
}

/// Members stop polling without `close()` (every other restart, the others
/// clean), leaving their last batch uncommitted. Once `max.poll.interval.ms`
/// expires, the next owner resumes at the committed offset: records may be
/// read again from there, never from below it, and none is lost.
#[tokio::test(flavor = "multi_thread")]
async fn test_integrity_consumer_restart_hard() {
    let (mut ctx, base) = context().await;
    let mut spec = spec();
    let topic = ctx.topic("restart_hard");
    spec.topics.push(TopicSpec::source(&topic, 6, 1));
    spec.writers.push(WriterSpec::new(0, &topic).rate(400).records(10_000));
    // Each member restarts about every 8 s, longer than the 6 s
    // `max.poll.interval.ms`, so a hard-stopped member's partitions are handed
    // over before the next restart; the group settles once the writer stops
    // (`run_member`).
    spec.group = Some(
        GroupSpec::new(&ctx.group_id("restart_hard"), vec![topic], 2)
            .restart(Duration::from_secs(8), &[RestartMode::Hard, RestartMode::Clean])
            .prop("max.poll.interval.ms", "6000"),
    );
    spec.drain_timeout = Duration::from_secs(60);
    spec.requirements = Requirements {
        min_records_per_stream: 10_000,
        min_hard_stops: 2,
        min_clean_restarts: 1,
        ..Default::default()
    };
    assert_pass(&run(spec, base).await);
}

/// Writers replace their producer every few hundred records or every second:
/// a new producer id each time, and for the transactional one a new epoch of
/// the same transactional id.
#[tokio::test(flavor = "multi_thread")]
async fn test_integrity_producer_restart() {
    let (mut ctx, base) = context().await;
    let mut spec = spec();
    let plain = ctx.topic("producer_restart");
    let txn = ctx.topic("producer_restart_txn");
    spec.topics.push(TopicSpec::source(&plain, 3, 1));
    spec.topics.push(TopicSpec::source(&txn, 3, 1));
    spec.writers.push(
        WriterSpec::new(0, &plain)
            .rate(1000)
            .records(4000)
            .restart(Restart::Every(Duration::from_secs(1))),
    );
    spec.writers.push(
        WriterSpec::new(1, &txn)
            .transactions(&ctx.group_id("restart-txn"), 20, 4)
            .records(2000)
            .restart(Restart::EveryRecords(400)),
    );
    spec.group = Some(GroupSpec::new(&ctx.group_id("producer_restart"), vec![plain, txn], 1));
    spec.requirements = Requirements { min_records_per_stream: 1000, min_producer_restarts: 6, ..Default::default() };
    assert_pass(&run(spec, base).await);
}

/// A deserializer that flips the last byte of every value, standing in for a
/// client that corrupts what it hands out.
struct FlipLastByte;

impl Deserializer<Vec<u8>> for FlipLastByte {
    async fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Vec<u8>, Error> {
        let mut value = data.to_vec();
        if let Some(last) = value.last_mut() {
            *last ^= 0x01;
        }
        Ok(value)
    }
}

/// The checks catch a corruption on the real read path, not only in
/// `self_tests`: every record read through a corrupting deserializer is
/// reported, and the run fails.
#[tokio::test(flavor = "multi_thread")]
async fn test_integrity_detects_tampering() {
    let (mut ctx, base) = context().await;
    let topic = ctx.topic("tamper");
    let topics = [TopicSpec::source(&topic, 1, 1)];
    create_topics(&base, &topics).await;
    let ledger = Ledger::new(rand::random(), seed());
    ledger.register_topic(&topic, 1, StreamKind::Source);
    let generator = ledger.register_writer(0, PayloadProfile::sized(16, 64));

    let producer = KafkaProducer::new(
        ProducerConfig::new(&base).expect("valid producer config"),
        ByteArraySerializer::default(),
        ByteArraySerializer::default(),
    )
    .expect("producer");
    let mut sent = Vec::new();
    for _ in 0..200 {
        let seq = ledger.next_seq(&topic, 0);
        let identity = Identity {
            run_id: ledger.run_id(),
            id: RecordId { writer: 0, seq },
            incarnation: 0,
            intent: TxnIntent::None,
            timestamp: now_ms(),
        };
        let record = build_record(&topic, None, &identity, generator.generate(0, seq));
        sent.push((seq, Producer::send(&producer, record).await.expect("send")));
    }
    for (seq, future) in sent {
        ledger.delivered(&topic, 0, seq, &future.get().await);
    }
    producer.close().await.expect("close");

    let mut props = base.clone();
    props.insert("group.protocol".to_string(), "consumer".to_string());
    let mut consumer = KafkaConsumer::<Vec<u8>, Vec<u8>>::new(
        ConsumerConfig::new(&props).expect("valid consumer config"),
        confluent_kafka::common::serialization::ByteArrayDeserializer::default(),
        FlipLastByte,
    )
    .expect("consumer");
    let tp = TopicPartition::new(topic.clone(), 0);
    consumer.assign(vec![tp.clone()]).await.expect("assign");
    consumer.seek_to_beginning(&[tp]).await.expect("seek");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while ledger.counts().final_consumed < 200 && std::time::Instant::now() < deadline {
        for record in consumer.poll(Duration::from_millis(200)).await.expect("poll") {
            ledger.final_record(&record);
        }
    }
    consumer.close().await.expect("close");

    let verdict = ledger.verdict(false, true, &Requirements::default());
    eprintln!("{verdict}");
    assert!(!verdict.passed(), "a corrupting deserializer passed:\n{verdict}");
    assert_eq!(verdict.counts.corrupt, 200, "{verdict}");
    let samples = &verdict.samples["corrupt records"];
    assert!(
        samples[0].contains("value byte"),
        "the sample does not locate the changed byte: {}",
        samples[0]
    );
}

/// The checker itself, without a broker: failures are produced on purpose and
/// must be reported, and an untouched run must pass.
mod self_tests {
    use std::collections::HashMap;

    use confluent_kafka::common::TopicPartition;
    use confluent_kafka::common::header::{Header, Headers, RecordHeader, RecordHeaders};
    use confluent_kafka::consumer::{ConsumerRecord, ConsumerRecordOptionsBuilder, OffsetAndMetadata};
    use confluent_kafka::producer::{ProducerRecord, RecordMetadata};

    use crate::integrity::checker::{Ledger, Requirements, StreamKind, TxnOutcome};
    use crate::integrity::record::{
        Defect, Identity, Payload, PayloadProfile, RecordGenerator, RecordId, TxnIntent, build_record, verify_record,
    };

    const RUN: u64 = 0x5EED;
    const TOPIC: &str = "t";

    fn identity(seq: u64, intent: TxnIntent) -> Identity {
        Identity {
            run_id: RUN,
            id: RecordId { writer: 0, seq },
            incarnation: 0,
            intent,
            timestamp: 1_700_000_000_000,
        }
    }

    fn generator() -> RecordGenerator {
        RecordGenerator::new(42, PayloadProfile::mixed())
    }

    fn produced(seq: u64, intent: TxnIntent) -> ProducerRecord<Vec<u8>, Vec<u8>> {
        build_record(TOPIC, None, &identity(seq, intent), generator().generate(0, seq))
    }

    /// `record` as a consumer would read it at `partition` / `offset`, with its
    /// fields replaced by `change`.
    fn consumed(
        record: &ProducerRecord<Vec<u8>, Vec<u8>>,
        partition: i32,
        offset: i64,
        change: impl FnOnce(&mut Option<Vec<u8>>, &mut Option<Vec<u8>>, &mut Vec<RecordHeader>, &mut i64),
    ) -> ConsumerRecord<Vec<u8>, Vec<u8>> {
        consumed_on(TOPIC, record, partition, offset, change)
    }

    /// [`consumed`], read from `topic`.
    fn consumed_on(
        topic: &str,
        record: &ProducerRecord<Vec<u8>, Vec<u8>>,
        partition: i32,
        offset: i64,
        change: impl FnOnce(&mut Option<Vec<u8>>, &mut Option<Vec<u8>>, &mut Vec<RecordHeader>, &mut i64),
    ) -> ConsumerRecord<Vec<u8>, Vec<u8>> {
        let mut key = record.key().cloned();
        let mut value = record.value().cloned();
        let mut headers = record.headers().to_array().to_vec();
        let mut timestamp = record.timestamp().expect("timestamp");
        change(&mut key, &mut value, &mut headers, &mut timestamp);
        let options = ConsumerRecordOptionsBuilder::new()
            .set_topic(topic)
            .set_partition(partition)
            .set_offset(offset)
            .set_timestamp(timestamp)
            .set_key(key)
            .set_value(value)
            .set_headers(RecordHeaders::with_header_iter(headers))
            .build()
            .expect("valid record");
        ConsumerRecord::with_options(options)
    }

    fn unchanged(
        record: &ProducerRecord<Vec<u8>, Vec<u8>>,
        partition: i32,
        offset: i64,
    ) -> ConsumerRecord<Vec<u8>, Vec<u8>> {
        consumed(record, partition, offset, |_, _, _, _| {})
    }

    fn generators() -> HashMap<u32, RecordGenerator> {
        HashMap::from([(0, generator())])
    }

    fn defect(record: &ConsumerRecord<Vec<u8>, Vec<u8>>) -> (Defect, String) {
        let failure = verify_record(record, RUN, &generators()).expect_err("the record must fail verification");
        (failure.defect, failure.detail)
    }

    /// A ledger with one source topic of one partition and writer 0.
    fn ledger() -> Ledger {
        let ledger = Ledger::new(RUN, 42);
        ledger.register_topic(TOPIC, 1, StreamKind::Source);
        ledger.register_writer(0, PayloadProfile::mixed());
        ledger
    }

    fn metadata(offset: i64) -> RecordMetadata {
        RecordMetadata::new(TopicPartition::new(TOPIC.to_string(), 0), offset, 0, 0, -1, -1)
    }

    /// Sends `count` records, all acknowledged at offsets `0..count`.
    fn deliver(ledger: &Ledger, count: u64) -> Vec<ProducerRecord<Vec<u8>, Vec<u8>>> {
        (0..count)
            .map(|_| {
                let seq = ledger.next_seq(TOPIC, 0);
                ledger.delivered(TOPIC, 0, seq, &Ok(metadata(seq as i64)));
                produced(seq, TxnIntent::None)
            })
            .collect()
    }

    fn failure_count(verdict: &crate::integrity::checker::Verdict, name: &str) -> u64 {
        verdict
            .counts
            .failures()
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, c)| *c)
            .expect("known failure")
    }

    #[test]
    fn test_generator_is_deterministic() {
        assert_eq!(generator().generate(3, 17), generator().generate(3, 17));
        assert_ne!(generator().generate(3, 17), generator().generate(3, 18));
        assert_ne!(
            generator().generate(3, 17),
            RecordGenerator::new(43, PayloadProfile::mixed()).generate(3, 17)
        );
    }

    #[test]
    fn test_verify_accepts_an_unchanged_record() {
        for seq in 0..200 {
            let record = produced(seq, TxnIntent::Commit);
            let got = verify_record(&unchanged(&record, 0, seq as i64), RUN, &generators()).expect("valid");
            assert_eq!(got, identity(seq, TxnIntent::Commit));
        }
    }

    #[test]
    fn test_verify_reports_a_changed_value_byte() {
        let record = (0..)
            .map(|seq| produced(seq, TxnIntent::None))
            .find(|r| r.value().is_some_and(|v| v.len() > 10))
            .unwrap();
        let (defect, detail) = defect(&consumed(&record, 0, 0, |_, value, _, _| {
            value.as_mut().unwrap()[10] ^= 0x80;
        }));
        assert_eq!(defect, Defect::DigestMismatch);
        assert!(detail.contains("value byte 10"), "{detail}");
    }

    #[test]
    fn test_verify_reports_null_instead_of_empty() {
        let empty = Payload { key: Some(Vec::new()), value: Some(Vec::new()), headers: Vec::new() };
        let record = build_record(TOPIC, None, &identity(0, TxnIntent::None), empty);
        let (d, detail) = defect(&consumed(&record, 0, 0, |_, value, _, _| *value = None));
        assert_eq!(d, Defect::DigestMismatch);
        assert!(detail.contains("value is null"), "{detail}");
        let (d, detail) = defect(&consumed(&record, 0, 0, |key, _, _, _| *key = None));
        assert_eq!(d, Defect::DigestMismatch);
        assert!(detail.contains("key is null"), "{detail}");
    }

    #[test]
    fn test_verify_reports_changed_headers() {
        let record = (0..)
            .map(|seq| produced(seq, TxnIntent::None))
            .find(|r| r.headers().to_array().len() >= 3 && r.headers().to_array()[0] != r.headers().to_array()[1])
            .unwrap();
        type HeaderChange = fn(&mut Vec<RecordHeader>);
        let cases: [(&str, HeaderChange); 3] = [
            ("dropped", |h| {
                h.remove(0);
            }),
            ("reordered", |h| h.swap(0, 1)),
            ("value changed", |h| {
                let key = h[0].key().to_string();
                let value = match h[0].value() {
                    Some(v) if !v.is_empty() => None,
                    _ => Some(b"x".to_vec()),
                };
                h[0] = RecordHeader::new(key, value);
            }),
        ];
        for (what, change) in cases {
            let (defect, _) = defect(&consumed(&record, 0, 0, |_, _, headers, _| change(headers)));
            assert_eq!(defect, Defect::DigestMismatch, "{what}");
        }
    }

    #[test]
    fn test_verify_reports_identity_problems() {
        let record = produced(0, TxnIntent::None);
        let (d, _) = defect(&consumed(&record, 0, 0, |_, _, _, timestamp| *timestamp += 1));
        assert_eq!(d, Defect::TimestampMismatch);
        let (d, _) = defect(&consumed(&record, 0, 0, |_, _, headers, _| {
            headers.pop();
        }));
        assert_eq!(d, Defect::MissingIdentity);
        let (d, _) = defect(&consumed(&record, 0, 0, |_, _, headers, _| {
            let last = headers.last().unwrap().clone();
            headers.insert(0, last);
        }));
        assert_eq!(d, Defect::MalformedIdentity);
        let foreign = build_record(
            TOPIC,
            None,
            &Identity { run_id: RUN + 1, ..identity(0, TxnIntent::None) },
            generator().generate(0, 0),
        );
        let (d, _) = defect(&unchanged(&foreign, 0, 0));
        assert_eq!(d, Defect::ForeignRecord);
        let (d, detail) = defect(&consumed(&record, 0, 0, |_, _, headers, _| {
            let last = headers.pop().unwrap();
            let mut bytes = last.value().unwrap().to_vec();
            bytes[20] ^= 0x01; // a byte of the sequence number
            headers.push(RecordHeader::new(last.key().to_string(), Some(bytes)));
        }));
        assert_eq!(d, Defect::DigestMismatch, "{detail}");
    }

    #[test]
    fn test_ledger_passes_an_untouched_run() {
        let ledger = ledger();
        for (offset, record) in deliver(&ledger, 50).iter().enumerate() {
            ledger.live_record(&unchanged(record, 0, offset as i64));
            ledger.final_record(&unchanged(record, 0, offset as i64));
        }
        let verdict = ledger.verdict(true, true, &Requirements { min_records_per_stream: 50, ..Default::default() });
        assert!(verdict.passed(), "{verdict}");
        let report = verdict.to_json();
        assert_eq!(report["verdict"], "PASS");
        assert_eq!(report["writers"]["acked"], 50);
        assert_eq!(report["final_read_back"]["consumed"], 50);
        assert!(report["failures"].as_object().unwrap().values().all(|n| n == 0), "{report}");
    }

    #[test]
    fn test_ledger_reports_lost_and_duplicated_records() {
        let ledger = ledger();
        let records = deliver(&ledger, 10);
        for (offset, record) in records.iter().enumerate().filter(|(o, _)| *o != 4) {
            ledger.live_record(&unchanged(record, 0, offset as i64));
            ledger.final_record(&unchanged(record, 0, offset as i64));
        }
        ledger.final_record(&unchanged(&records[7], 0, 7));
        let verdict = ledger.verdict(true, true, &Requirements::default());
        assert!(!verdict.passed());
        assert_eq!(failure_count(&verdict, "lost (live readers)"), 1);
        assert_eq!(failure_count(&verdict, "lost (final read-back)"), 1);
        assert_eq!(failure_count(&verdict, "duplicated (final read-back)"), 1);
        assert_eq!(failure_count(&verdict, "order violations"), 0, "{verdict}");
        assert!(verdict.samples["lost (final read-back)"][0].contains("seq 4..=4"), "{verdict}");
    }

    #[test]
    fn test_ledger_reports_order_violations_and_double_writes() {
        let ledger = ledger();
        let records = deliver(&ledger, 3);
        // seq 1 at offset 0 and seq 0 at offset 1: out of order.
        ledger.live_record(&unchanged(&records[1], 0, 0));
        ledger.live_record(&unchanged(&records[0], 0, 1));
        // seq 2, then seq 2 again at a later offset: written twice.
        ledger.live_record(&unchanged(&records[2], 0, 2));
        ledger.live_record(&unchanged(&records[2], 0, 3));
        // The final read-back sees seq 1 before seq 0 too.
        ledger.final_record(&unchanged(&records[1], 0, 0));
        ledger.final_record(&unchanged(&records[0], 0, 1));
        let verdict = ledger.verdict(true, true, &Requirements::default());
        assert_eq!(failure_count(&verdict, "order violations"), 2, "{verdict}");
        assert_eq!(failure_count(&verdict, "double writes"), 1, "{verdict}");
        // Offsets 0 and 1 were acknowledged for seq 0 and 1, read the other way round.
        assert_eq!(failure_count(&verdict, "placement mismatches"), 2, "{verdict}");
    }

    #[test]
    fn test_ledger_reports_aborted_and_rejected_records_that_are_read() {
        let ledger = ledger();
        let seq = ledger.next_seq(TOPIC, 0);
        ledger.resolve_txn(TOPIC, 0, &[(seq, Ok(metadata(0)))], TxnOutcome::Aborted);
        ledger.live_record(&unchanged(&produced(seq, TxnIntent::Abort), 0, 0));
        let rejected = ledger.next_seq(TOPIC, 0);
        let error = confluent_kafka::common::Error::local_unsupported_operation("rejected");
        ledger.send_rejected(TOPIC, 0, rejected, &error);
        ledger.final_record(&unchanged(&produced(rejected, TxnIntent::None), 0, 1));
        let verdict = ledger.verdict(true, true, &Requirements::default());
        assert_eq!(failure_count(&verdict, "aborted records visible"), 1, "{verdict}");
        assert_eq!(failure_count(&verdict, "rejected or aborted records visible"), 2, "{verdict}");
    }

    #[test]
    fn test_ledger_counts_failed_deliveries_but_not_aborted_ones() {
        use confluent_kafka::common::Error;
        let ledger = ledger();
        // Failed because its transaction was aborted first: expected.
        let aborted = ledger.next_seq(TOPIC, 0);
        ledger.resolve_txn(
            TOPIC,
            0,
            &[(aborted, Err(Error::local_unsupported_operation("aborted")))],
            TxnOutcome::Aborted,
        );
        let rejected = ledger.next_seq(TOPIC, 0);
        ledger.delivered(TOPIC, 0, rejected, &Err(Error::local_unsupported_operation("too large")));
        let ambiguous = ledger.next_seq(TOPIC, 0);
        ledger.delivered(TOPIC, 0, ambiguous, &Err(Error::timeout("expired")));
        let verdict = ledger.verdict(true, true, &Requirements::default());
        assert!(verdict.passed(), "{verdict}");
        assert_eq!(
            (verdict.counts.send_rejected, verdict.counts.send_ambiguous),
            (1, 1),
            "{verdict}"
        );
        let samples = &verdict.samples["failed deliveries (reported)"];
        assert_eq!(samples.len(), 2, "{verdict}");
        assert!(
            samples[0].contains("too large") && samples[1].contains("expired"),
            "{samples:?}"
        );
    }

    #[test]
    fn test_ledger_reports_a_failed_record_in_a_committed_transaction() {
        use confluent_kafka::common::Error;
        let ledger = ledger();
        let ok = ledger.next_seq(TOPIC, 0);
        let failed = ledger.next_seq(TOPIC, 0);
        ledger.resolve_txn(
            TOPIC,
            0,
            &[(ok, Ok(metadata(0))), (failed, Err(Error::timeout("expired")))],
            TxnOutcome::Committed,
        );
        let record = unchanged(&produced(ok, TxnIntent::Commit), 0, 0);
        ledger.live_record(&record);
        ledger.final_record(&record);
        let verdict = ledger.verdict(true, true, &Requirements::default());
        assert_eq!(
            failure_count(&verdict, "failed records in committed transactions"),
            1,
            "{verdict}"
        );
    }

    #[test]
    fn test_ledger_holds_an_ambiguous_record_found_in_the_log_to_once() {
        use confluent_kafka::common::Error;
        let ledger = ledger();
        // Its delivery timed out, but the final read-back finds it: it was
        // written. A group that stopped before reaching it did not skip it...
        let seq = ledger.next_seq(TOPIC, 0);
        ledger.delivered(TOPIC, 0, seq, &Err(Error::timeout("expired")));
        ledger.final_record(&unchanged(&produced(seq, TxnIntent::None), 0, 0));
        assert!(ledger.verdict(true, true, &Requirements::default()).passed());
        // ...one that committed past it did.
        let tp = TopicPartition::new(TOPIC.to_string(), 0);
        ledger.live_committed(&HashMap::from([(tp, OffsetAndMetadata::new(1).unwrap())]));
        let verdict = ledger.verdict(true, true, &Requirements::default());
        assert_eq!(failure_count(&verdict, "lost (live readers)"), 1, "{verdict}");
        // Read live as well, it passes.
        let ledger = self::ledger();
        let seq = ledger.next_seq(TOPIC, 0);
        ledger.delivered(TOPIC, 0, seq, &Err(Error::timeout("expired")));
        let record = unchanged(&produced(seq, TxnIntent::None), 0, 0);
        ledger.live_record(&record);
        ledger.final_record(&record);
        assert!(ledger.verdict(true, true, &Requirements::default()).passed());
        // Read live but gone from the final read-back: lost.
        let ledger = self::ledger();
        let seq = ledger.next_seq(TOPIC, 0);
        ledger.delivered(TOPIC, 0, seq, &Err(Error::timeout("expired")));
        ledger.live_record(&unchanged(&produced(seq, TxnIntent::None), 0, 0));
        let verdict = ledger.verdict(true, true, &Requirements::default());
        assert_eq!(failure_count(&verdict, "lost (final read-back)"), 1, "{verdict}");
    }

    #[test]
    fn test_ledger_reports_commit_problems() {
        let ledger = ledger();
        let records = deliver(&ledger, 4);
        for (offset, record) in records.iter().enumerate() {
            ledger.live_record(&unchanged(record, 0, offset as i64));
        }
        let tp = TopicPartition::new(TOPIC.to_string(), 0);
        let ours = HashMap::from([(tp.clone(), OffsetAndMetadata::new(4).unwrap())]);
        ledger.live_committed(&ours);
        // Read back lower than what was committed, then past what was read.
        ledger.readback(&ours, &HashMap::from([(tp.clone(), Some(OffsetAndMetadata::new(2).unwrap()))]));
        ledger.readback(&ours, &HashMap::from([(tp.clone(), Some(OffsetAndMetadata::new(9).unwrap()))]));
        // Within reach: neither.
        ledger.readback(&ours, &HashMap::from([(tp, Some(OffsetAndMetadata::new(4).unwrap()))]));
        // A new owner re-reads from offset 1, below the committed 4.
        ledger.live_record(&unchanged(&records[1], 0, 1));
        let verdict = ledger.verdict(true, true, &Requirements::default());
        assert_eq!(failure_count(&verdict, "committed offset read-back violations"), 2, "{verdict}");
        assert_eq!(failure_count(&verdict, "re-reads below the committed offset"), 1, "{verdict}");
    }

    #[test]
    fn test_ledger_reports_unresolved_sends_and_unmet_requirements() {
        let ledger = ledger();
        ledger.next_seq(TOPIC, 0);
        let verdict = ledger.verdict(
            false,
            false,
            &Requirements {
                min_records_per_stream: 1,
                min_hard_stops: 1,
                every_partition_read: true,
                ..Default::default()
            },
        );
        assert!(!verdict.passed());
        assert_eq!(failure_count(&verdict, "unresolved sends"), 1, "{verdict}");
        assert_eq!(verdict.unmet.len(), 4, "{verdict}");
    }

    #[test]
    fn test_ledger_reports_a_record_read_twice_in_one_poll() {
        let ledger = ledger();
        let records = deliver(&ledger, 2);
        ledger.live_batch(&[
            unchanged(&records[0], 0, 0),
            unchanged(&records[1], 0, 1),
            unchanged(&records[0], 0, 0),
        ]);
        let verdict = ledger.verdict(true, false, &Requirements::default());
        assert_eq!(failure_count(&verdict, "order violations"), 1, "{verdict}");
    }

    #[test]
    fn test_ledger_checks_a_mirror_against_its_source() {
        let ledger = ledger();
        ledger.register_topic("m", 1, StreamKind::Mirror(TOPIC.to_string()));
        let records = deliver(&ledger, 3);
        for (offset, record) in records.iter().enumerate() {
            ledger.live_record(&unchanged(record, 0, offset as i64));
            ledger.final_record(&unchanged(record, 0, offset as i64));
        }
        // Copies of seq 0 and 1, not of seq 2, and a copy of a record its
        // source never had.
        for (offset, record) in records[..2].iter().enumerate() {
            let copy = consumed_on("m", record, 0, offset as i64, |_, _, _, _| {});
            ledger.live_record(&copy);
            ledger.final_record(&copy);
        }
        let stray = consumed_on("m", &produced(7, TxnIntent::None), 0, 2, |_, _, _, _| {});
        ledger.final_record(&stray);
        let verdict = ledger.verdict(true, true, &Requirements::default());
        assert_eq!(failure_count(&verdict, "lost (live readers)"), 1, "{verdict}");
        assert_eq!(failure_count(&verdict, "lost (final read-back)"), 1, "{verdict}");
        assert_eq!(failure_count(&verdict, "rejected or aborted records visible"), 1, "{verdict}");
        assert!(
            verdict.samples["lost (final read-back)"][0].starts_with("m writer 0: seq 2..=2"),
            "{verdict}"
        );
    }

    #[test]
    fn test_ledger_takes_a_copy_as_proof_its_ambiguous_source_was_written() {
        use confluent_kafka::common::Error;
        let ledger = ledger();
        ledger.register_topic("m", 1, StreamKind::Mirror(TOPIC.to_string()));
        let seq = ledger.next_seq(TOPIC, 0);
        ledger.delivered(TOPIC, 0, seq, &Err(Error::timeout("expired")));
        // Copied, so it was written, but gone from its source.
        ledger.final_record(&consumed_on("m", &produced(seq, TxnIntent::None), 0, 0, |_, _, _, _| {}));
        let verdict = ledger.verdict(true, true, &Requirements::default());
        assert_eq!(failure_count(&verdict, "lost (final read-back)"), 1, "{verdict}");
        assert!(
            verdict.samples["lost (final read-back)"][0].starts_with("t writer 0"),
            "{verdict}"
        );
    }

    #[test]
    fn test_ledger_reports_a_partially_visible_transaction() {
        let ledger = ledger();
        let seqs: Vec<u64> = (0..2).map(|_| ledger.next_seq(TOPIC, 0)).collect();
        let results: Vec<_> = seqs.iter().map(|&seq| (seq, Ok(metadata(seq as i64)))).collect();
        ledger.resolve_txn(TOPIC, 0, &results, TxnOutcome::Unknown);
        ledger.final_record(&unchanged(&produced(seqs[0], TxnIntent::Commit), 0, 0));
        let verdict = ledger.verdict(true, true, &Requirements::default());
        assert_eq!(failure_count(&verdict, "partially visible transactions"), 1, "{verdict}");
        // All of it visible: committed after all.
        ledger.final_record(&unchanged(&produced(seqs[1], TxnIntent::Commit), 0, 1));
        assert!(ledger.verdict(true, true, &Requirements::default()).passed());
    }

    #[test]
    fn test_ledger_checks_the_copier_offsets() {
        use confluent_kafka::common::Error;
        let ledger = ledger();
        let tp = TopicPartition::new(TOPIC.to_string(), 0);
        let expected = HashMap::from([(tp.clone(), 4)]);
        ledger.copier_readback(
            &expected,
            &HashMap::from([(tp.clone(), Some(OffsetAndMetadata::new(4).unwrap()))]),
        );
        ledger.copier_readback(
            &expected,
            &HashMap::from([(tp.clone(), Some(OffsetAndMetadata::new(5).unwrap()))]),
        );
        // A partition it never committed holds its seeded 0.
        let other = TopicPartition::new(TOPIC.to_string(), 1);
        ledger.copier_readback(&expected, &HashMap::from([(other, Some(OffsetAndMetadata::new(0).unwrap()))]));
        ledger.copier_failed_in_committed_txn("m", "t-0@3", &Error::timeout("expired"));
        let verdict = ledger.verdict(false, true, &Requirements::default());
        assert_eq!(failure_count(&verdict, "committed offset read-back violations"), 1, "{verdict}");
        assert_eq!(
            failure_count(&verdict, "failed records in committed transactions"),
            1,
            "{verdict}"
        );
        assert!(verdict.samples["failed records in committed transactions"][0].contains("t-0@3"));
    }

    #[test]
    fn test_ledger_requires_planned_aborts_and_few_failures() {
        use confluent_kafka::common::Error;
        let ledger = ledger();
        // Aborted after a failure: never visible, but no planned abort.
        let seq = ledger.next_seq(TOPIC, 0);
        ledger.resolve_txn(TOPIC, 0, &[(seq, Ok(metadata(0)))], TxnOutcome::Failed);
        for _ in 0..3 {
            let seq = ledger.next_seq(TOPIC, 0);
            ledger.delivered(TOPIC, 0, seq, &Err(Error::timeout("expired")));
        }
        let requirements = Requirements { min_txns_aborted: 1, max_failed_percent: Some(50.0), ..Default::default() };
        let verdict = ledger.verdict(false, true, &requirements);
        assert_eq!(verdict.counts.txns_failed, 1, "{verdict}");
        assert!(verdict.unmet.iter().any(|u| u.contains("0 aborted transactions")), "{verdict}");
        assert!(
            verdict.unmet.iter().any(|u| u.starts_with("3 of 4 sends failed or ambiguous")),
            "{verdict}"
        );
        assert!(
            verdict
                .unmet
                .iter()
                .any(|u| u.starts_with("1 of 1 transactions failed or unknown")),
            "{verdict}"
        );
    }
}
