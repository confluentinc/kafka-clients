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

//! Manual test, write side: **transaction atomicity beyond one partition**.
//! Verify with `cargo run --example txn_atomicity_consumer` afterwards.
//!
//! Three cases, each with its own producer and topic(s):
//!
//! 1. **Multi-topic transaction** — one transaction sends to three topics,
//!    commits; a second transaction sends to the same three topics, aborts.
//!    All-or-nothing must hold *across topics*: after the commit every topic
//!    shows the committed values, after the abort no topic shows the aborted
//!    ones. Exercises `AddPartitionsToTxn` with several partitions in one
//!    transaction.
//! 2. **Large transaction** — one transaction with 10,000 records (many
//!    internal batches, pipelined produce requests), committed. The whole set
//!    must become visible at once, complete and in order.
//! 3. **Commit implies flush** — 1,000 records are sent but their acks are
//!    deliberately *not* awaited before `commit_transaction`. The commit must
//!    flush everything still buffered in the client; a lost record here means
//!    commit did not flush.
//!
//! Broker setup and env vars: see `examples/README.md`.

mod txn_common;

use std::time::Instant;

use confluent_kafka::producer::Producer;

use txn_common::close_producer;
use txn_common::send_value_printed;
use txn_common::string_record;
use txn_common::transactional_producer;

/// The three topics of the multi-topic case.
const MULTI_TOPICS: [&str; 3] = ["txn-multi-a", "txn-multi-b", "txn-multi-c"];
/// Committed on every multi-topic; the consumer expects these everywhere.
const MULTI_COMMITTED: [&str; 2] = ["multi-committed-1", "multi-committed-2"];
/// Aborted on every multi-topic; the consumer expects these nowhere.
const MULTI_ABORTED: [&str; 2] = ["multi-aborted-1", "multi-aborted-2"];

/// Topic and record count of the large-transaction case.
const LARGE_TOPIC: &str = "txn-large";
const LARGE_COUNT: usize = 10_000;

/// Topic and record count of the commit-implies-flush case.
const FLUSH_TOPIC: &str = "txn-flush";
const FLUSH_COUNT: usize = 1_000;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(message) => {
            println!();
            println!("❌ PRODUCER FAILED: {message}");
            println!("   Is the broker up? See examples/README.md for the docker command.");
            std::process::ExitCode::FAILURE
        },
    }
}

async fn run() -> Result<(), String> {
    let bootstrap = txn_common::bootstrap_servers();
    println!("=== transaction atomicity — write side ===");
    println!("bootstrap.servers : {bootstrap}");

    multi_topic_case(&bootstrap).await?;
    large_case(&bootstrap).await?;
    flush_case(&bootstrap).await?;

    println!();
    println!("✅ PRODUCER OK: multi-topic commit+abort, a 10k-record transaction, and an");
    println!("   unflushed commit all completed.");
    println!("   Verify the reader-side guarantees with: cargo run --example txn_atomicity_consumer");
    Ok(())
}

/// Case 1: one transaction across three topics, committed; a second one
/// across the same topics, aborted.
async fn multi_topic_case(bootstrap: &str) -> Result<(), String> {
    println!();
    println!("--- case 1: one transaction across {} topics ---", MULTI_TOPICS.len());
    let producer = transactional_producer(bootstrap, "txn-manual-atomicity-multi")?;
    producer
        .init_transactions()
        .await
        .map_err(|e| format!("multi: init_transactions: {e}"))?;

    producer.begin_transaction().map_err(|e| format!("multi: begin #1: {e}"))?;
    for topic in MULTI_TOPICS {
        for value in MULTI_COMMITTED {
            send_value_printed(&producer, topic, value).await?;
        }
    }
    producer.commit_transaction().await.map_err(|e| format!("multi: commit: {e}"))?;
    println!("  commit_transaction ...... ok — one commit covers all three topics");

    producer.begin_transaction().map_err(|e| format!("multi: begin #2: {e}"))?;
    for topic in MULTI_TOPICS {
        for value in MULTI_ABORTED {
            send_value_printed(&producer, topic, value).await?;
        }
    }
    producer.abort_transaction().await.map_err(|e| format!("multi: abort: {e}"))?;
    println!("  abort_transaction ....... ok — one abort discards them on all three topics");

    close_producer(&producer).await
}

/// Case 2: 10,000 records in a single committed transaction.
async fn large_case(bootstrap: &str) -> Result<(), String> {
    println!();
    println!("--- case 2: one transaction with {LARGE_COUNT} records ---");
    let producer = transactional_producer(bootstrap, "txn-manual-atomicity-large")?;
    producer
        .init_transactions()
        .await
        .map_err(|e| format!("large: init_transactions: {e}"))?;
    producer.begin_transaction().map_err(|e| format!("large: begin: {e}"))?;

    let started = Instant::now();
    let mut acks = Vec::with_capacity(LARGE_COUNT);
    for i in 1..=LARGE_COUNT {
        let value = format!("large-{i:05}");
        let record = string_record(LARGE_TOPIC, &value)?;
        let ack = producer.send(record).await.map_err(|e| format!("large: send {value}: {e}"))?;
        acks.push(ack);
        if i % 2000 == 0 {
            println!("  enqueued {i}/{LARGE_COUNT}");
        }
    }
    for (i, ack) in acks.iter().enumerate() {
        ack.get_timeout(txn_common::SEND_ACK_TIMEOUT)
            .await
            .map_err(|e| format!("large: ack of record {}: {e}", i + 1))?;
    }
    println!("  all {LARGE_COUNT} records acked in {} ms", started.elapsed().as_millis());
    producer.commit_transaction().await.map_err(|e| format!("large: commit: {e}"))?;
    println!("  commit_transaction ...... ok — the whole set becomes visible atomically");

    close_producer(&producer).await
}

/// Case 3: send without awaiting a single ack, then commit — the commit has
/// to flush the client's buffers.
async fn flush_case(bootstrap: &str) -> Result<(), String> {
    println!();
    println!("--- case 3: commit with {FLUSH_COUNT} unawaited sends in the buffer ---");
    let producer = transactional_producer(bootstrap, "txn-manual-atomicity-flush")?;
    producer
        .init_transactions()
        .await
        .map_err(|e| format!("flush: init_transactions: {e}"))?;
    producer.begin_transaction().map_err(|e| format!("flush: begin: {e}"))?;

    for i in 1..=FLUSH_COUNT {
        let value = format!("flush-{i:04}");
        let record = string_record(FLUSH_TOPIC, &value)?;
        // Enqueue only — the ack future is deliberately dropped.
        let _unawaited_ack = producer.send(record).await.map_err(|e| format!("flush: send {value}: {e}"))?;
    }
    println!("  {FLUSH_COUNT} sends enqueued, zero acks awaited — committing immediately");
    let started = Instant::now();
    producer.commit_transaction().await.map_err(|e| format!("flush: commit: {e}"))?;
    println!(
        "  commit_transaction ...... ok ({} ms) — commit may only succeed after every buffered \
         record is acked",
        started.elapsed().as_millis()
    );

    close_producer(&producer).await
}
