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

//! Manual smoke test for the **transactional producer** against a real broker
//! — the basic use case. Start here; `examples/README.md` maps the whole
//! `txn_*` suite.
//!
//! # The basic transactional use case
//!
//! A transaction makes a group of records atomic: after `commit_transaction`
//! all of them become visible to `read_committed` consumers, and after
//! `abort_transaction` none of them ever do. This program exercises exactly
//! that:
//!
//! 1. `init_transactions` — register `transactional.id` with the coordinator.
//! 2. Transaction #1: begin → send `committed-1..3` → **commit**.
//! 3. Transaction #2: begin → send `aborted-1..3` → **abort**.
//! 4. Transaction #3: begin → send `committed-4..6` → **commit**.
//!
//! Every send is awaited for its broker ack and printed with the offset the
//! broker assigned — an acked offset for an `aborted-*` record is the
//! interesting part: the data *is* written to the log, and it is the abort
//! marker that makes consumers skip it. Transaction #3 exists so the
//! companion consumer has committed data *after* the aborted batch and must
//! read past the abort marker rather than merely stop in front of it.
//!
//! If all three transactions complete, the program prints `PRODUCER OK`. The
//! reader-side half of the guarantee is then verified by
//! `cargo run --example txn_consumer`.
//!
//! # Broker
//!
//! Expects a broker on `localhost:9092` (override with
//! `KAFKA_BOOTSTRAP_SERVERS`). Single-node Kafka 4.3 in Docker works, as long
//! as the transaction-state topic's replication factor is lowered to 1:
//!
//! ```text
//! docker run -d --name kafka-txn-manual-test -p 9092:9092 \
//!   -e KAFKA_NODE_ID=1 \
//!   -e KAFKA_PROCESS_ROLES=broker,controller \
//!   -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
//!   -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
//!   -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
//!   -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
//!   -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
//!   -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
//!   -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
//!   -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
//!   -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
//!   -e KAFKA_LOG_DIRS=/tmp/kraft-combined-logs \
//!   apache/kafka:4.3.0
//! ```
//!
//! # Run
//!
//! ```text
//! cargo run --example txn_producer
//! cargo run --example txn_consumer
//! ```
//!
//! Re-running is fine: the consumer detects how many complete producer runs
//! the topic holds. Interrupted runs leave partial data behind — start over
//! with a fresh topic via `TXN_TEST_TOPIC` if that happens.

mod txn_common;

use std::time::Instant;

use txn_common::StringProducer;
use txn_common::close_producer;
use txn_common::send_value_printed;
use txn_common::transactional_producer;

/// Values of the first committed transaction.
const COMMITTED_BATCH_1: [&str; 3] = ["committed-1", "committed-2", "committed-3"];
/// Values of the aborted transaction — written to the log, then discarded by
/// the abort marker; a `read_committed` consumer must never deliver them.
const ABORTED_BATCH: [&str; 3] = ["aborted-1", "aborted-2", "aborted-3"];
/// Values of the second committed transaction, sent *after* the abort so the
/// consumer has to read past the abort marker to reach them.
const COMMITTED_BATCH_2: [&str; 3] = ["committed-4", "committed-5", "committed-6"];

/// The fixed transactional id. Re-running the program re-registers it, which
/// bumps the producer epoch and fences the (already exited) previous run —
/// that is the normal transactional-producer restart story.
const TRANSACTIONAL_ID: &str = "txn-manual-test-producer";

fn topic() -> String {
    std::env::var("TXN_TEST_TOPIC").unwrap_or_else(|_| "txn-manual-test".to_string())
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(message) => {
            println!();
            println!("❌ PRODUCER FAILED: {message}");
            println!("   Is the broker up? See this file's docs for the docker command.");
            std::process::ExitCode::FAILURE
        },
    }
}

async fn run() -> Result<(), String> {
    let bootstrap = txn_common::bootstrap_servers();
    let topic = topic();

    println!("=== transactional producer — manual test ===");
    println!("bootstrap.servers : {bootstrap}");
    println!("topic             : {topic} (auto-created on first send)");
    println!("transactional.id  : {TRANSACTIONAL_ID}");

    let producer = transactional_producer(&bootstrap, TRANSACTIONAL_ID)?;

    println!();
    let started = Instant::now();
    producer
        .init_transactions()
        .await
        .map_err(|e| format!("init_transactions: {e}"))?;
    println!(
        "init_transactions ......... ok ({} ms) — coordinator found, producer id + epoch assigned",
        started.elapsed().as_millis()
    );

    run_transaction(&producer, &topic, "#1", &COMMITTED_BATCH_1, Outcome::Commit).await?;
    run_transaction(&producer, &topic, "#2", &ABORTED_BATCH, Outcome::Abort).await?;
    run_transaction(&producer, &topic, "#3", &COMMITTED_BATCH_2, Outcome::Commit).await?;

    close_producer(&producer).await?;

    println!();
    println!("✅ PRODUCER OK: 2 transactions committed, 1 aborted, every send acked by the broker.");
    println!("   A read_committed consumer must now see exactly committed-1..6 and no aborted-*.");
    println!("   Verify with: cargo run --example txn_consumer");
    Ok(())
}

/// Which end-of-transaction call to make.
enum Outcome {
    Commit,
    Abort,
}

/// Runs one whole transaction: begin, send `values` to partition 0 awaiting
/// each ack, then commit or abort per `outcome`.
async fn run_transaction(
    producer: &StringProducer,
    topic: &str,
    label: &str,
    values: &[&str],
    outcome: Outcome,
) -> Result<(), String> {
    let verb = match outcome {
        Outcome::Commit => "commit",
        Outcome::Abort => "abort",
    };
    println!();
    println!("--- transaction {label} (will {verb}) ---");

    producer
        .begin_transaction()
        .map_err(|e| format!("begin_transaction {label}: {e}"))?;
    println!("  begin_transaction ....... ok");

    for value in values {
        send_value_printed(producer, topic, value).await?;
    }

    let started = Instant::now();
    match outcome {
        Outcome::Commit => {
            producer
                .commit_transaction()
                .await
                .map_err(|e| format!("commit_transaction {label}: {e}"))?;
            println!(
                "  commit_transaction ...... ok ({} ms) — records now visible to read_committed",
                started.elapsed().as_millis()
            );
        },
        Outcome::Abort => {
            producer
                .abort_transaction()
                .await
                .map_err(|e| format!("abort_transaction {label}: {e}"))?;
            println!(
                "  abort_transaction ....... ok ({} ms) — records stay in the log but must never be delivered",
                started.elapsed().as_millis()
            );
        },
    }
    Ok(())
}
