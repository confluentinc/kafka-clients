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

//! Shared plumbing for the `txn_*` manual-test examples.
//!
//! Not an example itself — every `txn_*` example declares `mod txn_common;`
//! and uses the subset it needs. See `examples/README.md` for the map of
//! which example covers which transactional use case and the run order.

#![allow(dead_code)] // each example compiles its own copy and uses a subset

use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use confluent_kafka::common::Error;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::serialization::ByteArrayDeserializer;
use confluent_kafka::common::serialization::StringSerializer;
use confluent_kafka::consumer::Consumer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::ConsumerRecords;
use confluent_kafka::consumer::new_consumer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::RecordMetadata;
use confluent_kafka::producer::{ProducerRecord, ProducerRecordOptionsBuilder};

/// Every manual-test producer sends `String` keys and values.
pub type StringProducer = KafkaProducer<String, String>;
/// Every manual-test consumer receives raw bytes and decodes them for printing.
pub type BytesConsumer = Box<dyn Consumer<Vec<u8>, Vec<u8>>>;

/// How long to wait for one record's broker ack.
pub const SEND_ACK_TIMEOUT: Duration = Duration::from_secs(30);
/// A drain stops once this long passes with no new records — end of partition.
pub const IDLE_WINDOW: Duration = Duration::from_secs(5);
/// Hard cap on any drain, so a dead broker fails a phase instead of hanging it.
pub const PHASE_DEADLINE: Duration = Duration::from_secs(60);
/// Deadline for reads that expect a specific record count.
pub const CONSUME_DEADLINE: Duration = Duration::from_secs(30);

pub fn bootstrap_servers() -> String {
    std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_string())
}

/// Nanosecond timestamp for run-unique topic / group / transactional ids.
pub fn unique_suffix() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before 1970")
        .as_nanos()
        .to_string()
}

/// A transactional producer (`transactional.id` implies idempotence).
pub fn transactional_producer(bootstrap: &str, txn_id: &str) -> Result<StringProducer, String> {
    transactional_producer_with(bootstrap, txn_id, &[])
}

/// A transactional producer with extra / overriding config entries, for the
/// cases that probe a specific setting (`transaction.timeout.ms`,
/// `max.request.size`, ...).
pub fn transactional_producer_with(
    bootstrap: &str,
    txn_id: &str,
    overrides: &[(&str, &str)],
) -> Result<StringProducer, String> {
    let mut props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("transactional.id".to_string(), txn_id.to_string()),
        ("client.id".to_string(), format!("{txn_id}-client")),
        ("acks".to_string(), "all".to_string()),
        ("linger.ms".to_string(), "0".to_string()),
        ("max.block.ms".to_string(), "30000".to_string()),
        ("transaction.timeout.ms".to_string(), "60000".to_string()),
    ]);
    for (key, value) in overrides {
        props.insert((*key).to_string(), (*value).to_string());
    }
    let config = ProducerConfig::new(&props).map_err(|e| format!("invalid producer config: {e}"))?;
    KafkaProducer::new_config(config, Box::new(StringSerializer), Box::new(StringSerializer))
        .map_err(|e| format!("building the producer: {e}"))
}

/// A plain (non-transactional) producer, for seeding and for the "mixed
/// writers" cases. Its records are visible to consumers immediately.
pub fn plain_producer(bootstrap: &str, client_id: &str) -> Result<StringProducer, String> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), client_id.to_string()),
        ("acks".to_string(), "all".to_string()),
        ("linger.ms".to_string(), "0".to_string()),
        ("max.block.ms".to_string(), "30000".to_string()),
    ]);
    let config = ProducerConfig::new(&props).map_err(|e| format!("invalid producer config: {e}"))?;
    KafkaProducer::new_config(config, Box::new(StringSerializer), Box::new(StringSerializer))
        .map_err(|e| format!("building the producer: {e}"))
}

/// A consumer for `group_id` at the given isolation level, reading from the
/// earliest offset when the group has no committed position.
pub fn build_consumer(bootstrap: &str, group_id: &str, isolation: &str) -> Result<BytesConsumer, String> {
    build_consumer_with(bootstrap, group_id, isolation, &[])
}

/// [`build_consumer`] with extra / overriding config entries, for cases that
/// probe a specific consumer setting (`fetch.min.bytes`,
/// `default.api.timeout.ms`, ...).
pub fn build_consumer_with(
    bootstrap: &str,
    group_id: &str,
    isolation: &str,
    overrides: &[(&str, &str)],
) -> Result<BytesConsumer, String> {
    let mut props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("group.id".to_string(), group_id.to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
        ("isolation.level".to_string(), isolation.to_string()),
        ("client.id".to_string(), format!("{group_id}-client")),
    ]);
    for (key, value) in overrides {
        props.insert((*key).to_string(), (*value).to_string());
    }
    let config = ConsumerConfig::new(&props).map_err(|e| format!("invalid consumer config: {e}"))?;
    new_consumer::<Vec<u8>, Vec<u8>>(config, Box::new(ByteArrayDeserializer), Box::new(ByteArrayDeserializer))
        .map_err(|e| format!("building the consumer: {e}"))
}

/// Builds a record for partition 0 with key `key-{value}`.
pub fn string_record(topic: &str, value: &str) -> Result<ProducerRecord<String, String>, String> {
    let options = ProducerRecordOptionsBuilder::new()
        .set_topic(topic.to_string())
        .set_value(Some(value.to_string()))
        .set_partition(Some(0))
        .set_key(Some(format!("key-{value}")))
        .build()
        .map_err(|e| format!("building the record {value}: {e}"))?;
    ProducerRecord::new_options(options).map_err(|e| format!("building the record {value}: {e}"))
}

/// Sends one record to partition 0 and awaits its broker ack.
pub async fn send_value(producer: &StringProducer, topic: &str, value: &str) -> Result<RecordMetadata, String> {
    let record = string_record(topic, value)?;
    let ack = producer.send(record).await.map_err(|e| format!("send {value}: {e}"))?;
    ack.get_with_timeout(SEND_ACK_TIMEOUT)
        .await
        .map_err(|e| format!("waiting for the ack of {value}: {e}"))
}

/// Sends one record and awaits its ack, printing the assigned offset.
pub async fn send_value_printed(producer: &StringProducer, topic: &str, value: &str) -> Result<(), String> {
    let metadata = send_value(producer, topic, value).await?;
    println!(
        "  sent {value:<12} → acked at {}-{}@{}",
        metadata.topic(),
        metadata.partition(),
        metadata.offset()
    );
    Ok(())
}

/// Which half of `doSend`'s contract delivered a send failure.
///
/// Java splits this by exception class, and the split is a guarantee in its own
/// right: `catch (ApiException e)` returns `new FutureFailure(e)`
/// (`KafkaProducer.java:1056-1068`), while `catch (KafkaException e)` and
/// `catch (Exception e)` rethrow out of `send()` (`:1073-1081`). A probe that
/// collapses the two can only report *that* a send failed, so it would stay
/// green if a client moved an error from one path to the other — which is half of
/// what a transactional `send` in the wrong state is supposed to prove.
pub enum SendFailure {
    /// `send()` itself returned `Err` — Java's rethrowing catch blocks. This is
    /// how a misuse of the transactional API surfaces.
    Synchronous(Error),
    /// `send()` returned a future that then resolved to an error — Java's
    /// `catch (ApiException e)`, or a broker-side rejection of a record the
    /// client accepted.
    ViaFuture(Error),
}

impl SendFailure {
    /// The error, whichever path carried it.
    pub fn error(&self) -> &Error {
        match self {
            Self::Synchronous(error) | Self::ViaFuture(error) => error,
        }
    }

    /// Unwraps a failure that must have come back from `send()` itself.
    ///
    /// `Err(..)` — reported as a ❌ by the caller — when the error arrived through
    /// the future instead, because that means the client routed a
    /// non-`ApiException` into Java's `ApiException` block.
    pub fn expect_synchronous(self, what: &str) -> Result<Error, String> {
        match self {
            Self::Synchronous(error) => Ok(error),
            Self::ViaFuture(error) => Err(format!(
                "{what} failed through the ack future, but Java rethrows this one out of send() \
                 (KafkaProducer.java:1073-1081): {error}"
            )),
        }
    }

    /// Unwraps a failure that must have come back through the ack future.
    ///
    /// `Err(..)` when `send()` returned it synchronously instead — for a record
    /// the client accepted, that would mean it never reached the broker.
    pub fn expect_via_future(self, what: &str) -> Result<Error, String> {
        match self {
            Self::ViaFuture(error) => Ok(error),
            Self::Synchronous(error) => Err(format!(
                "{what} failed synchronously out of send(), but this error is only reachable \
                 through the ack future (KafkaProducer.java:1056-1068): {error}"
            )),
        }
    }
}

/// Sends one record that is *expected to fail*, and reports which path failed it.
///
/// `Err(..)` means the send unexpectedly succeeded. Callers must then require the
/// path Java mandates via [`SendFailure::expect_synchronous`] /
/// [`SendFailure::expect_via_future`] — accepting either would make the verdict
/// weaker than the guarantee its label names.
pub async fn send_expect_failure(producer: &StringProducer, topic: &str, value: &str) -> Result<SendFailure, String> {
    let record = string_record(topic, value)?;
    match producer.send(record).await {
        Err(error) => Ok(SendFailure::Synchronous(error)),
        Ok(future) => match future.get_with_timeout(SEND_ACK_TIMEOUT).await {
            Err(error) => Ok(SendFailure::ViaFuture(error)),
            Ok(metadata) => Err(format!(
                "the send of {value} was unexpectedly acked at {}-{}@{}",
                metadata.topic(),
                metadata.partition(),
                metadata.offset()
            )),
        },
    }
}

/// Closes a producer, mapping the error for the manual-test `Result` chain.
pub async fn close_producer(producer: &StringProducer) -> Result<(), String> {
    producer.close().await.map_err(|e| format!("closing the producer: {e}"))
}

/// Decodes and prints one batch of records, appending values to `collected`.
fn collect(records: ConsumerRecords<Vec<u8>, Vec<u8>>, collected: &mut Vec<String>, verbose: bool) -> usize {
    let mut added = 0;
    for record in records {
        let value = String::from_utf8_lossy(record.value().map_or(&[][..], Vec::as_slice)).into_owned();
        if verbose {
            let key = record
                .key()
                .map_or_else(String::new, |k| String::from_utf8_lossy(k).into_owned());
            println!(
                "  {}-{}@{:<4} {key} = {value}",
                record.topic(),
                record.partition(),
                record.offset()
            );
        }
        collected.push(value);
        added += 1;
        if !verbose && collected.len().is_multiple_of(2000) {
            println!("  ... {} records so far", collected.len());
        }
    }
    added
}

/// Polls until [`IDLE_WINDOW`] passes with no records (or [`PHASE_DEADLINE`]).
pub async fn drain_until_idle(consumer: &mut BytesConsumer, verbose: bool) -> Result<Vec<String>, String> {
    let started = Instant::now();
    let mut last_record_at = Instant::now();
    let mut collected: Vec<String> = Vec::new();
    while started.elapsed() < PHASE_DEADLINE && last_record_at.elapsed() < IDLE_WINDOW {
        let records = consumer
            .poll(Duration::from_millis(500))
            .await
            .map_err(|e| format!("poll: {e}"))?;
        if collect(records, &mut collected, verbose) > 0 {
            last_record_at = Instant::now();
        }
    }
    Ok(collected)
}

/// Polls for a fixed budget and returns everything seen. Used for negative
/// assertions ("nothing must arrive"), where stopping early would make the
/// assertion vacuous.
pub async fn drain_for(consumer: &mut BytesConsumer, budget: Duration, verbose: bool) -> Result<Vec<String>, String> {
    let started = Instant::now();
    let mut collected: Vec<String> = Vec::new();
    while started.elapsed() < budget {
        let records = consumer
            .poll(Duration::from_millis(500))
            .await
            .map_err(|e| format!("poll: {e}"))?;
        collect(records, &mut collected, verbose);
    }
    Ok(collected)
}

/// Polls until `expected` records arrive, or [`CONSUME_DEADLINE`] passes.
pub async fn consume_exactly(
    consumer: &mut BytesConsumer,
    expected: usize,
    verbose: bool,
) -> Result<Vec<String>, String> {
    let started = Instant::now();
    let mut collected: Vec<String> = Vec::new();
    while collected.len() < expected && started.elapsed() < CONSUME_DEADLINE {
        let records = consumer
            .poll(Duration::from_millis(500))
            .await
            .map_err(|e| format!("poll: {e}"))?;
        collect(records, &mut collected, verbose);
    }
    Ok(collected)
}

/// Reads partition 0 of `topic` from the beginning under `isolation` with a
/// fresh unique group, draining until the partition goes idle.
pub async fn read_partition(
    bootstrap: &str,
    topic: &str,
    isolation: &str,
    verbose: bool,
) -> Result<Vec<String>, String> {
    read_partition_idle(bootstrap, topic, isolation, verbose, IDLE_WINDOW).await
}

/// [`read_partition`] with a caller-chosen idle window — fan-out style cases
/// read many topics, where a full 5 s idle wait per topic adds up.
pub async fn read_partition_idle(
    bootstrap: &str,
    topic: &str,
    isolation: &str,
    verbose: bool,
    idle_window: Duration,
) -> Result<Vec<String>, String> {
    let group_id = format!("txn-manual-verify-{isolation}-{}", unique_suffix());
    println!();
    println!("--- isolation.level={isolation}: reading {topic}-0 from the beginning ---");
    let mut consumer = build_consumer(bootstrap, &group_id, isolation)?;
    consumer
        .assign(vec![TopicPartition::new(topic.to_string(), 0)])
        .await
        .map_err(|e| format!("assign {topic}: {e}"))?;
    let started = Instant::now();
    let mut last_record_at = Instant::now();
    let mut collected: Vec<String> = Vec::new();
    while started.elapsed() < PHASE_DEADLINE && last_record_at.elapsed() < idle_window {
        let records = consumer
            .poll(Duration::from_millis(500))
            .await
            .map_err(|e| format!("poll {topic}: {e}"))?;
        if collect(records, &mut collected, verbose) > 0 {
            last_record_at = Instant::now();
        }
    }
    if collected.is_empty() {
        println!("  (no records within {idle_window:?})");
    } else if !verbose {
        println!("  received {} records", collected.len());
    }
    consumer.close().await.map_err(|e| format!("close {topic}: {e}"))?;
    Ok(collected)
}

/// `base` repeated `times` times, flattened — what N producer runs append.
pub fn repeated(base: &[&str], times: usize) -> Vec<String> {
    (0..times).flat_map(|_| base.iter().map(|s| (*s).to_string())).collect()
}

/// Prints one ✅/❌ verdict line and passes `ok` through for folding.
pub fn report(ok: bool, label: &str, detail: String) -> bool {
    let mark = if ok { "✅" } else { "❌" };
    println!("{mark} {label} — {detail}");
    ok
}

/// Verdict helper: `got` must equal `base` repeated `runs` times. Prints a
/// full diff for short sequences and a first-divergence summary for long ones.
pub fn sequence_check(label: &str, got: &[String], base: &[&str], runs: usize) -> bool {
    let expected = repeated(base, runs);
    let ok = got == expected;
    let detail = if ok {
        format!("{} records, exactly as expected", got.len())
    } else if expected.len() <= 12 && got.len() <= 12 {
        format!("expected {expected:?}, got {got:?}")
    } else {
        divergence_detail(&expected, got)
    };
    report(ok, label, detail)
}

fn divergence_detail(expected: &[String], got: &[String]) -> String {
    match expected.iter().zip(got.iter()).position(|(e, g)| e != g) {
        Some(index) => format!(
            "expected {} records, got {}; first divergence at index {index}: expected {:?}, got {:?}",
            expected.len(),
            got.len(),
            expected[index],
            got[index]
        ),
        None => format!(
            "expected {} records, got {} — the shorter is a prefix of the longer",
            expected.len(),
            got.len()
        ),
    }
}
