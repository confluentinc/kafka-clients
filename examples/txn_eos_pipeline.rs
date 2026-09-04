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

//! Manual test: **exactly-once consume-transform-produce**, both halves.
//!
//! Self-contained (it is both producer and consumer, which is the point of
//! the pattern) and uses fresh run-unique topics/groups, so it can be run any
//! number of times. The classic read-process-write loop:
//!
//! ```text
//! input topic → consume → transform → produce to output topic
//!                                   → send_offsets_to_transaction
//!                                   → commit (or abort) — ONE transaction
//! ```
//!
//! **Abort half (the failure story):** the first processing attempt produces
//! the transformed records and attaches the consumed offsets to the
//! transaction — then aborts. Three things must follow: the output topic
//! shows nothing under `read_committed`; the input group's committed offset
//! did NOT move; and a fresh consumer in the group therefore *replays* the
//! same input records. Nothing was lost, nothing half-processed.
//!
//! **Commit half (the success story):** the second attempt does the same work
//! and commits. The output topic then shows exactly ONE copy of each
//! transformed record — the aborted attempt's copies stay invisible, which is
//! literally "exactly once despite a retry" — and the input offset is now
//! committed, so the work is never replayed again.
//!
//! Broker setup and env vars: see `examples/README.md`.

mod txn_common;

use std::collections::HashMap;

use confluent_kafka::common::TopicPartition;
use confluent_kafka::consumer::OffsetAndMetadata;

use txn_common::BytesConsumer;
use txn_common::build_consumer;
use txn_common::close_producer;
use txn_common::consume_exactly;
use txn_common::plain_producer;
use txn_common::read_partition;
use txn_common::report;
use txn_common::send_value_printed;
use txn_common::transactional_producer;

const INPUT_VALUES: [&str; 3] = ["in-1", "in-2", "in-3"];
const OUTPUT_VALUES: [&str; 3] = ["IN-1", "IN-2", "IN-3"];

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(message) => {
            println!();
            println!("❌ EXACTLY-ONCE PIPELINE TEST FAILED: {message}");
            println!("   Is the broker up? See examples/README.md for the docker command.");
            std::process::ExitCode::FAILURE
        },
    }
}

async fn run() -> Result<(), String> {
    let bootstrap = txn_common::bootstrap_servers();
    let suffix = txn_common::unique_suffix();
    let input_topic = format!("txn-eos-input-{suffix}");
    let output_topic = format!("txn-eos-output-{suffix}");
    let group = format!("txn-eos-group-{suffix}");
    let input_tp = TopicPartition::new(input_topic.clone(), 0);

    println!("=== exactly-once consume-transform-produce — manual test ===");
    println!("bootstrap.servers : {bootstrap}");
    println!("input topic       : {input_topic}");
    println!("output topic      : {output_topic}");
    println!("consumer group    : {group}");

    // Seed the input topic.
    println!();
    println!("--- seeding the input topic ---");
    let seeder = plain_producer(&bootstrap, "txn-eos-seeder")?;
    for value in INPUT_VALUES {
        send_value_printed(&seeder, &input_topic, value).await?;
    }
    close_producer(&seeder).await?;

    // First consumer incarnation: consume the input.
    println!();
    println!("--- attempt 1: consume ---");
    let mut consumer_1 = build_consumer(&bootstrap, &group, "read_committed")?;
    consumer_1
        .subscribe_topics(vec![input_topic.clone()])
        .await
        .map_err(|e| format!("subscribe: {e}"))?;
    let consumed_1 = consume_exactly(&mut consumer_1, INPUT_VALUES.len(), true).await?;
    if consumed_1 != INPUT_VALUES {
        return Err(format!("expected to consume {INPUT_VALUES:?}, got {consumed_1:?}"));
    }
    let next_offset = consumer_1.position(&input_tp).await.map_err(|e| format!("position: {e}"))?;
    println!("  consumed {} records; next input offset = {next_offset}", consumed_1.len());

    let producer = transactional_producer(&bootstrap, &format!("txn-manual-eos-{suffix}"))?;
    producer
        .init_transactions()
        .await
        .map_err(|e| format!("init_transactions: {e}"))?;

    // Attempt 1: transform + produce + attach offsets, then ABORT.
    println!();
    println!("--- attempt 1: transform, produce, attach offsets … then ABORT ---");
    producer.begin_transaction().map_err(|e| format!("begin #1: {e}"))?;
    produce_transformed(&producer, &output_topic, &consumed_1).await?;
    send_offsets(&producer, &input_tp, next_offset, consumer_1.group_metadata()).await?;
    producer.abort_transaction().await.map_err(|e| format!("abort: {e}"))?;
    println!("  abort_transaction ....... ok — the output AND the offset commit must both roll back");

    // A side consumer in the same group, used only to ask the coordinator
    // what offset is committed. `assign` keeps it out of the group's
    // membership, so it does not disturb the subscribed consumers.
    let mut offset_reader = build_consumer(&bootstrap, &group, "read_committed")?;
    offset_reader
        .assign(vec![input_tp.clone()])
        .await
        .map_err(|e| format!("assign offset reader: {e}"))?;

    let mut all_ok = true;
    let after_abort = committed_offset(&mut offset_reader, &input_tp).await?;
    all_ok &= report(
        after_abort.is_none(),
        "abort: the input offset was NOT committed",
        match after_abort {
            None => "the group still has no committed offset — the work will be replayed".to_string(),
            Some(offset) => format!("committed offset unexpectedly moved to {offset}"),
        },
    );
    let output_after_abort = read_partition(&bootstrap, &output_topic, "read_committed", true).await?;
    all_ok &= report(
        output_after_abort.is_empty(),
        "abort: the output topic shows nothing under read_committed",
        if output_after_abort.is_empty() {
            "the transformed records were discarded with the transaction".to_string()
        } else {
            format!("leaked: {output_after_abort:?}")
        },
    );

    // The replay: a fresh consumer in the same group starts from scratch.
    println!();
    println!("--- attempt 2: a fresh consumer replays the input ---");
    consumer_1.close().await.map_err(|e| format!("close consumer 1: {e}"))?;
    let mut consumer_2 = build_consumer(&bootstrap, &group, "read_committed")?;
    consumer_2
        .subscribe_topics(vec![input_topic.clone()])
        .await
        .map_err(|e| format!("subscribe #2: {e}"))?;
    let consumed_2 = consume_exactly(&mut consumer_2, INPUT_VALUES.len(), true).await?;
    all_ok &= report(
        consumed_2 == INPUT_VALUES,
        "abort: the same input records were consumed again",
        if consumed_2 == INPUT_VALUES {
            "no committed offset → replay from the beginning; the aborted attempt lost nothing".to_string()
        } else {
            format!("expected {INPUT_VALUES:?}, got {consumed_2:?}")
        },
    );

    // Attempt 2: same work, this time COMMIT.
    println!();
    println!("--- attempt 2: transform, produce, attach offsets … then COMMIT ---");
    producer.begin_transaction().map_err(|e| format!("begin #2: {e}"))?;
    produce_transformed(&producer, &output_topic, &consumed_2).await?;
    let next_offset_2 = consumer_2.position(&input_tp).await.map_err(|e| format!("position #2: {e}"))?;
    send_offsets(&producer, &input_tp, next_offset_2, consumer_2.group_metadata()).await?;
    producer.commit_transaction().await.map_err(|e| format!("commit: {e}"))?;
    println!("  commit_transaction ...... ok");

    let output_committed = read_partition(&bootstrap, &output_topic, "read_committed", true).await?;
    let expected: Vec<String> = OUTPUT_VALUES.iter().map(|s| (*s).to_string()).collect();
    all_ok &= report(
        output_committed == expected,
        "commit: the output shows each transformed record exactly ONCE",
        if output_committed == expected {
            "one copy each, despite two produce attempts — the aborted copies stay invisible".to_string()
        } else {
            format!("expected {expected:?}, got {output_committed:?}")
        },
    );
    let output_all = read_partition(&bootstrap, &output_topic, "read_uncommitted", true).await?;
    all_ok &= report(
        output_all.len() == OUTPUT_VALUES.len() * 2,
        "commit: read_uncommitted shows both attempts' copies (control)",
        format!(
            "{} records in the log — the aborted attempt was really written, then filtered",
            output_all.len()
        ),
    );
    let after_commit = committed_offset(&mut offset_reader, &input_tp).await?;
    all_ok &= report(
        after_commit == Some(next_offset_2),
        "commit: the input offset was committed atomically with the output",
        format!("committed offset = {after_commit:?}, expected Some({next_offset_2})"),
    );

    consumer_2.close().await.map_err(|e| format!("close consumer 2: {e}"))?;
    offset_reader.close().await.map_err(|e| format!("close offset reader: {e}"))?;
    close_producer(&producer).await?;

    if !all_ok {
        return Err("the exactly-once guarantee did not hold — see the ❌ lines above".to_string());
    }
    println!();
    println!("✅ EXACTLY-ONCE HOLDS: an aborted attempt rolls back output and offsets together");
    println!("   (safe replay), and the committed retry lands each record exactly once.");
    Ok(())
}

/// Uppercases each consumed value and produces it to the output topic — the
/// "transform" step of the pipeline.
async fn produce_transformed(
    producer: &txn_common::StringProducer,
    output_topic: &str,
    consumed: &[String],
) -> Result<(), String> {
    for value in consumed {
        send_value_printed(producer, output_topic, &value.to_uppercase()).await?;
    }
    Ok(())
}

/// Attaches the consumed input offset to the producer's open transaction.
async fn send_offsets(
    producer: &txn_common::StringProducer,
    input_tp: &TopicPartition,
    next_offset: i64,
    group_metadata: confluent_kafka::consumer::ConsumerGroupMetadata,
) -> Result<(), String> {
    let offset = OffsetAndMetadata::new(next_offset).map_err(|e| format!("OffsetAndMetadata: {e}"))?;
    let offsets = HashMap::from([(input_tp.clone(), offset)]);
    producer
        .send_offsets_to_transaction(offsets, group_metadata)
        .await
        .map_err(|e| format!("send_offsets_to_transaction: {e}"))?;
    println!("  send_offsets_to_transaction({next_offset}) attached to the open transaction");
    Ok(())
}

/// The group's committed offset for `tp`, or `None` when nothing is committed.
async fn committed_offset(consumer: &mut BytesConsumer, tp: &TopicPartition) -> Result<Option<i64>, String> {
    let committed = consumer
        .committed(std::slice::from_ref(tp))
        .await
        .map_err(|e| format!("committed: {e}"))?;
    Ok(committed.get(tp).map(OffsetAndMetadata::offset).filter(|offset| *offset >= 0))
}
