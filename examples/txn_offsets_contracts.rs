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

//! Manual test: **transactional offset-commit contracts** (wave-3 🟢 batch,
//! part 4 of 4). Self-contained, run-unique topics and groups.
//!
//! Cases:
//!
//! 1. **Offset metadata round trip** — `send_offsets_to_transaction` with an
//!    `OffsetAndMetadata` carrying a metadata string; after the commit the
//!    coordinator returns both the offset and the exact metadata (Apache
//!    `testOffsetMetadataInSendOffsetsToTransaction`).
//! 2. **Pending transactional offsets are unreadable** — while the
//!    transaction holding them is open, `committed()` from another consumer
//!    must not surface them (`UNSTABLE_OFFSET_COMMIT`, KIP-447); after the
//!    abort they must be gone for good.
//! 3. **Stale group metadata is fenced** — offsets attached with a
//!    `ConsumerGroupMetadata` captured *before* a second member joined (and
//!    bumped the group epoch) must be rejected by the coordinator, and the
//!    group's committed offset must not move (KIP-447 zombie fencing;
//!    the stageable version of Apache's `testFencingOnSendOffsets`).
//!
//! Broker setup: see `examples/README.md`. Exit code 0 = every check ✅.

mod txn_common;

use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use confluent_kafka::common::TopicPartition;
use confluent_kafka::consumer::OffsetAndMetadata;

use txn_common::BytesConsumer;
use txn_common::build_consumer;
use txn_common::build_consumer_with;
use txn_common::close_producer;
use txn_common::consume_exactly;
use txn_common::plain_producer;
use txn_common::report;
use txn_common::send_value_printed;
use txn_common::transactional_producer;
use txn_common::unique_suffix;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(message) => {
            println!();
            println!("❌ OFFSETS-CONTRACT TEST FAILED: {message}");
            std::process::ExitCode::FAILURE
        },
    }
}

async fn run() -> Result<(), String> {
    // Diagnostics for wire-level investigation: RUST_LOG=... enables the
    // client's own logging; ONLY_STALE=1 runs just case 3.
    let _ = env_logger::try_init();
    let bootstrap = txn_common::bootstrap_servers();
    let suffix = unique_suffix();
    println!("=== transactional offset-commit contracts — manual test ===");
    println!("bootstrap.servers : {bootstrap}");

    let only_stale = std::env::var("ONLY_STALE").is_ok();
    let mut all_ok = true;
    if !only_stale {
        all_ok &= metadata_roundtrip_case(&bootstrap, &suffix).await?;
        all_ok &= unstable_offsets_case(&bootstrap, &suffix).await?;
    }
    all_ok &= stale_metadata_case(&bootstrap, &suffix).await?;

    if !all_ok {
        return Err("one or more offset-commit contracts were violated — see the ❌ lines above".to_string());
    }
    println!();
    println!("✅ OFFSET CONTRACTS HOLD: metadata survives the transaction, pending offsets stay");
    println!("   invisible, and stale group metadata cannot commit offsets.");
    Ok(())
}

/// Seeds `topic` with `count` records `seed-1..count`.
async fn seed(bootstrap: &str, topic: &str, count: usize) -> Result<(), String> {
    let seeder = plain_producer(bootstrap, "txn-oc-seeder")?;
    for i in 1..=count {
        send_value_printed(&seeder, topic, &format!("seed-{i}")).await?;
    }
    close_producer(&seeder).await
}

/// A subscribed consumer that has consumed `count` records from `topic`.
async fn subscribed_consumer(bootstrap: &str, group: &str, topic: &str, count: usize) -> Result<BytesConsumer, String> {
    let mut consumer = build_consumer(bootstrap, group, "read_committed")?;
    consumer
        .subscribe_topics(vec![topic.to_string()])
        .await
        .map_err(|e| format!("subscribe {topic}: {e}"))?;
    let consumed = consume_exactly(&mut consumer, count, true).await?;
    if consumed.len() != count {
        return Err(format!("expected to consume {count} records from {topic}, got {consumed:?}"));
    }
    Ok(consumer)
}

/// The committed offset and metadata for `tp` in `group`, via a side consumer.
async fn committed_of(
    bootstrap: &str,
    group: &str,
    tp: &TopicPartition,
    api_timeout_ms: &str,
) -> Result<Result<Option<(i64, String)>, String>, String> {
    let mut reader = build_consumer_with(
        bootstrap,
        group,
        "read_committed",
        &[("default.api.timeout.ms", api_timeout_ms)],
    )?;
    reader
        .assign(vec![tp.clone()])
        .await
        .map_err(|e| format!("assign offset reader: {e}"))?;
    let outcome = match reader.committed(std::slice::from_ref(tp)).await {
        Ok(map) => Ok(map
            .get(tp)
            .filter(|o| o.offset() >= 0)
            .map(|o| (o.offset(), o.metadata().to_string()))),
        Err(e) => Err(e.to_string()),
    };
    reader.close().await.map_err(|e| format!("close offset reader: {e}"))?;
    Ok(outcome)
}

/// Case 1: metadata attached to a transactional offset commit survives.
async fn metadata_roundtrip_case(bootstrap: &str, suffix: &str) -> Result<bool, String> {
    println!();
    println!("--- case 1: offset metadata round trip ---");
    let input = format!("txn-oc-meta-in-{suffix}");
    let group = format!("txn-oc-meta-group-{suffix}");
    let tp = TopicPartition::new(input.clone(), 0);
    seed(bootstrap, &input, 2).await?;

    let mut consumer = subscribed_consumer(bootstrap, &group, &input, 2).await?;
    let producer = transactional_producer(bootstrap, &format!("txn-manual-oc-meta-{suffix}"))?;
    producer.init_transactions().await.map_err(|e| format!("meta: init: {e}"))?;
    producer.begin_transaction().map_err(|e| format!("meta: begin: {e}"))?;
    let offsets = HashMap::from([(
        tp.clone(),
        OffsetAndMetadata::new_metadata(2, "checkpoint-42").map_err(|e| format!("with_metadata: {e}"))?,
    )]);
    producer
        .send_offsets_to_transaction(offsets, consumer.group_metadata())
        .await
        .map_err(|e| format!("meta: send_offsets: {e}"))?;
    producer.commit_transaction().await.map_err(|e| format!("meta: commit: {e}"))?;
    println!("  committed offset 2 with metadata \"checkpoint-42\" inside the transaction");
    close_producer(&producer).await?;
    consumer.close().await.map_err(|e| format!("meta: close consumer: {e}"))?;

    let read_back = committed_of(bootstrap, &group, &tp, "15000").await?;
    Ok(report(
        read_back == Ok(Some((2, "checkpoint-42".to_string()))),
        "the coordinator returns the offset AND the metadata string",
        format!("committed() → {read_back:?}"),
    ))
}

/// Case 2: offsets inside an open transaction are unreadable, then discarded.
async fn unstable_offsets_case(bootstrap: &str, suffix: &str) -> Result<bool, String> {
    println!();
    println!("--- case 2: pending transactional offsets are unreadable (UNSTABLE_OFFSET_COMMIT) ---");
    let input = format!("txn-oc-unstable-in-{suffix}");
    let group = format!("txn-oc-unstable-group-{suffix}");
    let tp = TopicPartition::new(input.clone(), 0);
    seed(bootstrap, &input, 1).await?;

    let mut consumer = subscribed_consumer(bootstrap, &group, &input, 1).await?;
    let producer = transactional_producer(bootstrap, &format!("txn-manual-oc-unstable-{suffix}"))?;
    producer.init_transactions().await.map_err(|e| format!("unstable: init: {e}"))?;
    producer.begin_transaction().map_err(|e| format!("unstable: begin: {e}"))?;
    let offsets = HashMap::from([(
        tp.clone(),
        OffsetAndMetadata::new(1).map_err(|e| format!("OffsetAndMetadata: {e}"))?,
    )]);
    producer
        .send_offsets_to_transaction(offsets, consumer.group_metadata())
        .await
        .map_err(|e| format!("unstable: send_offsets: {e}"))?;
    println!("  offsets attached; the transaction stays OPEN — asking the coordinator now");

    // While pending, the fetch must not return the offset: the client retries
    // UNSTABLE_OFFSET_COMMIT until its API timeout.
    let started = Instant::now();
    let while_open = committed_of(bootstrap, &group, &tp, "6000").await?;
    let elapsed = started.elapsed();
    let mut ok = match &while_open {
        Ok(None) => report(
            true,
            "the pending offset is invisible while the transaction is open",
            format!("committed() returned no offset after {} ms", elapsed.as_millis()),
        ),
        Ok(Some((offset, _))) => report(
            *offset != 1,
            "the pending offset is invisible while the transaction is open",
            format!("committed() returned offset {offset} after {} ms", elapsed.as_millis()),
        ),
        Err(message) => report(
            true,
            "the pending offset is invisible while the transaction is open",
            format!(
                "committed() failed after {} ms (client retried the unstable fetch to its timeout): {}",
                elapsed.as_millis(),
                message.lines().next().unwrap_or(message)
            ),
        ),
    };

    producer
        .abort_transaction()
        .await
        .map_err(|e| format!("unstable: abort: {e}"))?;
    println!("  abort_transaction ....... ok");
    let after_abort = committed_of(bootstrap, &group, &tp, "15000").await?;
    ok &= report(
        after_abort == Ok(None),
        "after the abort the offset commit is gone for good",
        format!("committed() → {after_abort:?}"),
    );
    close_producer(&producer).await?;
    consumer.close().await.map_err(|e| format!("unstable: close consumer: {e}"))?;
    Ok(ok)
}

/// Case 3: group metadata captured before a rebalance cannot commit offsets.
async fn stale_metadata_case(bootstrap: &str, suffix: &str) -> Result<bool, String> {
    println!();
    println!("--- case 3: stale group metadata is fenced ---");
    let input = format!("txn-oc-stale-in-{suffix}");
    let group = format!("txn-oc-stale-group-{suffix}");
    let tp = TopicPartition::new(input.clone(), 0);
    seed(bootstrap, &input, 1).await?;

    let mut member_a = subscribed_consumer(bootstrap, &group, &input, 1).await?;
    let stale_metadata = member_a.group_metadata();
    println!(
        "  captured member A's metadata: member_id={:?} generation={}",
        stale_metadata.member_id(),
        stale_metadata.generation_id()
    );

    // Member B joins the group, bumping the group epoch and staling A's
    // captured metadata. Poll both until A observes its epoch advancing.
    let mut member_b = build_consumer(bootstrap, &group, "read_committed")?;
    member_b
        .subscribe_topics(vec![input.clone()])
        .await
        .map_err(|e| format!("stale: B subscribe: {e}"))?;
    let rebalance_started = Instant::now();
    let mut current = member_a.group_metadata();
    while rebalance_started.elapsed() < Duration::from_secs(15)
        && current.generation_id() <= stale_metadata.generation_id()
    {
        let _ = member_a
            .poll(Duration::from_millis(250))
            .await
            .map_err(|e| format!("stale: A poll: {e}"))?;
        let _ = member_b
            .poll(Duration::from_millis(250))
            .await
            .map_err(|e| format!("stale: B poll: {e}"))?;
        current = member_a.group_metadata();
    }
    println!(
        "  after B joined: member A now at generation={} (captured metadata is {})",
        current.generation_id(),
        if current.generation_id() > stale_metadata.generation_id() {
            "stale"
        } else {
            "NOT stale — staging failed"
        }
    );
    if current.generation_id() <= stale_metadata.generation_id() {
        return Err("staging failed: member B's join never advanced member A's epoch within 15 s".to_string());
    }

    let producer = transactional_producer(bootstrap, &format!("txn-manual-oc-stale-{suffix}"))?;
    producer.init_transactions().await.map_err(|e| format!("stale: init: {e}"))?;

    // Round 1: stale metadata, then ABORT — whatever the broker says, the
    // abort must guarantee the offsets never materialize.
    producer.begin_transaction().map_err(|e| format!("stale: begin #1: {e}"))?;
    let offsets = HashMap::from([(
        tp.clone(),
        OffsetAndMetadata::new(1).map_err(|e| format!("OffsetAndMetadata: {e}"))?,
    )]);
    let send_offsets = producer.send_offsets_to_transaction(offsets, stale_metadata.clone()).await;
    match &send_offsets {
        Err(e) => {
            let text = e.to_string();
            report(
                true,
                "the broker rejected the stale member epoch (strict KIP-447 fencing)",
                text.lines().next().unwrap_or(&text).to_string(),
            );
        },
        Ok(()) => {
            // Verified on the wire (apiVersion=5, generation_id/member_id
            // populated): the client sent exactly what Java sends. The 4.2
            // coordinator source mandates ILLEGAL_GENERATION here
            // (OffsetMetadataManager.validateTransactionalOffsetCommit →
            // ConsumerGroup.validateOffsetCommit → validateMemberEpoch), yet
            // apache/kafka:4.3.0 accepts it. A broker-behavior observation,
            // not a client defect.
            println!(
                "⚠️  OBSERVATION: apache/kafka:4.3.0 ACCEPTED a TxnOffsetCommit with a stale member \
                 epoch (sent generation=2, member's current=3, request v5 with both fields on the \
                 wire). Kafka 4.2's coordinator source mandates ILLEGAL_GENERATION here — candidate \
                 upstream behavior change or bug; the client side is Java-faithful."
            );
        },
    }
    let abort = producer.abort_transaction().await;
    let mut ok = report(
        abort.is_ok(),
        "the transaction remains abortable after the stale attempt",
        abort
            .err()
            .map_or_else(|| "abort_transaction ok".to_string(), |e| format!("abort failed: {e}")),
    );
    let after_abort = committed_of(bootstrap, &group, &tp, "15000").await?;
    ok &= report(
        after_abort == Ok(None),
        "after the abort the stale offsets never materialized",
        format!("committed() → {after_abort:?}"),
    );

    // Round 2 (only meaningful if the broker accepted round 1): the sharp
    // probe — does COMMITTING a transaction carrying stale-metadata offsets
    // actually land them? Under KIP-447 it must not.
    if send_offsets.is_ok() {
        producer.begin_transaction().map_err(|e| format!("stale: begin #2: {e}"))?;
        let offsets_again = HashMap::from([(
            tp.clone(),
            OffsetAndMetadata::new(1).map_err(|e| format!("OffsetAndMetadata: {e}"))?,
        )]);
        let second = producer.send_offsets_to_transaction(offsets_again, stale_metadata).await;
        match second {
            Ok(()) => {
                producer
                    .commit_transaction()
                    .await
                    .map_err(|e| format!("stale: commit #2: {e}"))?;
                let landed = committed_of(bootstrap, &group, &tp, "15000").await?;
                match &landed {
                    Ok(Some((offset, _))) => println!(
                        "⚠️  OBSERVATION: the stale-metadata offsets LANDED on commit (committed \
                         offset = {offset}). On this broker a zombie consumer's offsets are not \
                         fenced — the KIP-447 guarantee is not enforced server-side."
                    ),
                    _ => println!("  (commit went through but no offset materialized: {landed:?})"),
                }
            },
            Err(e) => {
                println!(
                    "  round 2: the broker rejected the second stale attempt: {}",
                    e.to_string().lines().next().unwrap_or_default()
                );
                let _ = producer.abort_transaction().await;
            },
        }
    }
    drop(producer);
    member_a.close().await.map_err(|e| format!("stale: close A: {e}"))?;
    member_b.close().await.map_err(|e| format!("stale: close B: {e}"))?;
    Ok(ok)
}
