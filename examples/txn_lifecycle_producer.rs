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

//! Manual test, write side: **producer lifecycle** — fencing, crash recovery,
//! and multiple writers sharing a partition.
//! Verify with `cargo run --example txn_lifecycle_consumer` afterwards.
//!
//! Two cases:
//!
//! 1. **Fencing and crash recovery.** Producer A begins a transaction, sends
//!    two records, and then "crashes" — the transaction is simply left open.
//!    Producer B starts with the *same* `transactional.id`: its
//!    `init_transactions` makes the coordinator abort A's dangling
//!    transaction and bump the producer epoch, which is exactly how a
//!    restarted application recovers. B then commits a batch. A is now a
//!    zombie: its next send must fail with a fencing error, and after that
//!    *every* call on A (begin/commit/abort) must fail fast — a fenced
//!    producer is permanently dead, which is what prevents a stale instance
//!    from corrupting the new one's transactions.
//! 2. **Interleaved writers on one partition.** Two transactional producers
//!    (different ids) and one plain producer write to the same partition in a
//!    fixed interleaving; one of the transactions aborts. `read_committed`
//!    must untangle this by producer: deliver both producers' committed
//!    records and the plain ones, drop only the aborted batch.
//!
//! Broker setup and env vars: see `examples/README.md`.

mod txn_common;

use confluent_kafka::common::protocol::Errors;

use txn_common::close_producer;
use txn_common::plain_producer;
use txn_common::report;
use txn_common::send_expect_failure;
use txn_common::send_value_printed;
use txn_common::transactional_producer;

const FENCING_TOPIC: &str = "txn-fencing";
const INTERLEAVED_TOPIC: &str = "txn-interleaved";

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
    println!("=== producer lifecycle — write side ===");
    println!("bootstrap.servers : {bootstrap}");

    let fencing_ok = fencing_case(&bootstrap).await?;
    interleaved_case(&bootstrap).await?;

    if !fencing_ok {
        return Err("the zombie producer was not fenced correctly — see the ❌ lines above".to_string());
    }
    println!();
    println!("✅ PRODUCER OK: the successor took over the transactional.id (aborting the");
    println!("   dangling transaction), the zombie was fenced on every call, and the");
    println!("   interleaved writers all completed.");
    println!("   Verify the reader-side view with: cargo run --example txn_lifecycle_consumer");
    Ok(())
}

/// Case 1. Returns whether the zombie-side expectations held; infrastructure
/// failures (B's path — it must simply work) surface as `Err`.
async fn fencing_case(bootstrap: &str) -> Result<bool, String> {
    println!();
    println!("--- case 1: fencing a zombie, recovering its dangling transaction ---");
    let txn_id = "txn-manual-lifecycle-fence";

    let producer_a = transactional_producer(bootstrap, txn_id)?;
    producer_a
        .init_transactions()
        .await
        .map_err(|e| format!("A: init_transactions: {e}"))?;
    producer_a.begin_transaction().map_err(|e| format!("A: begin: {e}"))?;
    send_value_printed(&producer_a, FENCING_TOPIC, "abandoned-1").await?;
    send_value_printed(&producer_a, FENCING_TOPIC, "abandoned-2").await?;
    println!("  A \"crashes\" here: its transaction is left open, never committed or aborted");

    let producer_b = transactional_producer(bootstrap, txn_id)?;
    producer_b
        .init_transactions()
        .await
        .map_err(|e| format!("B: init_transactions must fence A and abort its transaction: {e}"))?;
    println!("  B.init_transactions ..... ok — same transactional.id: epoch bumped, A's dangling transaction aborted");
    producer_b.begin_transaction().map_err(|e| format!("B: begin: {e}"))?;
    for value in ["fenced-committed-1", "fenced-committed-2", "fenced-committed-3"] {
        send_value_printed(&producer_b, FENCING_TOPIC, value).await?;
    }
    producer_b.commit_transaction().await.map_err(|e| format!("B: commit: {e}"))?;
    println!("  B.commit_transaction .... ok — the successor works normally at the new epoch");
    close_producer(&producer_b).await?;

    // A is now a zombie. Its send must fail with a fencing error, and every
    // later call must fail fast — the fatal state is sticky.
    println!("  now poking the zombie A — every call must fail:");
    let mut ok = true;
    match send_expect_failure(&producer_a, FENCING_TOPIC, "zombie-never-1").await {
        Ok(error) => {
            let fenced = matches!(error.error(), Errors::InvalidProducerEpoch | Errors::ProducerFenced);
            ok &= report(
                fenced,
                "A.send failed with a fencing error",
                format!("{:?}: {error}", error.error()),
            );
        },
        Err(unexpected) => ok &= report(false, "A.send failed", unexpected),
    }
    ok &= expect_error(
        "A.begin_transaction",
        producer_a.begin_transaction().err().map(|e| e.to_string()),
    );
    ok &= expect_error(
        "A.commit_transaction",
        producer_a.commit_transaction().await.err().map(|e| e.to_string()),
    );
    ok &= expect_error(
        "A.abort_transaction (even abort is refused once fatally fenced)",
        producer_a.abort_transaction().await.err().map(|e| e.to_string()),
    );
    drop(producer_a);
    Ok(ok)
}

/// Case 2: a deterministic interleaving of three writers on one partition.
/// Every send awaits its ack before the next, so the log order is exactly the
/// order below — which is what the consumer file asserts against.
async fn interleaved_case(bootstrap: &str) -> Result<(), String> {
    println!();
    println!("--- case 2: two transactional producers + one plain producer, one partition ---");
    let producer_x = transactional_producer(bootstrap, "txn-manual-lifecycle-x")?;
    let producer_y = transactional_producer(bootstrap, "txn-manual-lifecycle-y")?;
    let producer_z = plain_producer(bootstrap, "txn-manual-lifecycle-plain")?;
    producer_x
        .init_transactions()
        .await
        .map_err(|e| format!("X: init_transactions: {e}"))?;
    producer_y
        .init_transactions()
        .await
        .map_err(|e| format!("Y: init_transactions: {e}"))?;

    producer_x.begin_transaction().map_err(|e| format!("X: begin: {e}"))?;
    send_value_printed(&producer_x, INTERLEAVED_TOPIC, "x-1").await?;
    producer_y.begin_transaction().map_err(|e| format!("Y: begin #1: {e}"))?;
    send_value_printed(&producer_y, INTERLEAVED_TOPIC, "y-abort-1").await?;
    send_value_printed(&producer_z, INTERLEAVED_TOPIC, "plain-1").await?;
    send_value_printed(&producer_x, INTERLEAVED_TOPIC, "x-2").await?;
    producer_x.commit_transaction().await.map_err(|e| format!("X: commit: {e}"))?;
    println!("  X committed (x-1, x-2) while Y's transaction was still open");
    send_value_printed(&producer_y, INTERLEAVED_TOPIC, "y-abort-2").await?;
    producer_y.abort_transaction().await.map_err(|e| format!("Y: abort: {e}"))?;
    println!("  Y aborted (y-abort-1, y-abort-2)");
    producer_y.begin_transaction().map_err(|e| format!("Y: begin #2: {e}"))?;
    send_value_printed(&producer_y, INTERLEAVED_TOPIC, "y-3").await?;
    producer_y.commit_transaction().await.map_err(|e| format!("Y: commit: {e}"))?;
    println!("  Y committed its second transaction (y-3)");
    send_value_printed(&producer_z, INTERLEAVED_TOPIC, "plain-2").await?;

    close_producer(&producer_x).await?;
    close_producer(&producer_y).await?;
    close_producer(&producer_z).await
}

fn expect_error(label: &str, error: Option<String>) -> bool {
    match error {
        Some(message) => report(true, &format!("{label} failed as required"), message),
        None => report(
            false,
            &format!("{label} failed as required"),
            "it unexpectedly succeeded".to_string(),
        ),
    }
}
