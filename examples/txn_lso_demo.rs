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

//! Manual test: **an open transaction gates read_committed delivery** (the
//! broker's Last Stable Offset).
//!
//! Self-contained — it interleaves a producer and a live consumer, so the two
//! roles cannot be split into separate programs. Uses a fresh run-unique
//! topic every time.
//!
//! The subtle part of `read_committed` is not that it hides uncommitted
//! records — it is that it hides *everything after them* too, until the
//! transaction resolves. The broker only serves a `read_committed` consumer
//! up to the Last Stable Offset (LSO), the offset of the first still-open
//! transaction. The script:
//!
//! 1. a plain record `before` — delivered immediately;
//! 2. a transaction opens and sends `open-1` — not delivered;
//! 3. a plain record `plain-behind` — **also not delivered**, although it is
//!    non-transactional and fully acked, because it sits behind the open
//!    transaction (this is the LSO doing its job: without it, consumers could
//!    observe effects out of order);
//! 4. a `read_uncommitted` consumer sees all three — the control proving the
//!    records are on the broker and fetchable;
//! 5. the transaction commits — the same `read_committed` consumer now
//!    receives `open-1` and `plain-behind`, in log order.
//!
//! Broker setup and env vars: see `examples/README.md`.

mod txn_common;

use std::time::Duration;

use confluent_kafka::common::TopicPartition;

use txn_common::build_consumer;
use txn_common::close_producer;
use txn_common::consume_exactly;
use txn_common::drain_for;
use txn_common::drain_until_idle;
use txn_common::plain_producer;
use txn_common::report;
use txn_common::send_value_printed;
use txn_common::transactional_producer;

/// The negative window: how long the gated records get to (not) arrive.
const GATE_BUDGET: Duration = Duration::from_secs(6);

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(message) => {
            println!();
            println!("❌ LSO TEST FAILED: {message}");
            println!("   Is the broker up? See examples/README.md for the docker command.");
            std::process::ExitCode::FAILURE
        },
    }
}

async fn run() -> Result<(), String> {
    let bootstrap = txn_common::bootstrap_servers();
    let suffix = txn_common::unique_suffix();
    let topic = format!("txn-lso-{suffix}");
    let tp = TopicPartition::new(topic.clone(), 0);

    println!("=== open-transaction gating (LSO) — manual test ===");
    println!("bootstrap.servers : {bootstrap}");
    println!("topic             : {topic}");

    println!();
    println!("--- writing: plain, then an OPEN transaction, then plain again ---");
    let plain = plain_producer(&bootstrap, "txn-lso-plain")?;
    send_value_printed(&plain, &topic, "before").await?;
    let transactional = transactional_producer(&bootstrap, &format!("txn-manual-lso-{suffix}"))?;
    transactional
        .init_transactions()
        .await
        .map_err(|e| format!("init_transactions: {e}"))?;
    transactional.begin_transaction().map_err(|e| format!("begin: {e}"))?;
    send_value_printed(&transactional, &topic, "open-1").await?;
    println!("  (the transaction stays open)");
    send_value_printed(&plain, &topic, "plain-behind").await?;

    println!();
    println!("--- read_committed, while the transaction is open ({GATE_BUDGET:?} budget) ---");
    let mut gated = build_consumer(&bootstrap, &format!("txn-lso-gated-{suffix}"), "read_committed")?;
    gated.assign(vec![tp.clone()]).await.map_err(|e| format!("assign: {e}"))?;
    let while_open = drain_for(&mut gated, GATE_BUDGET, true).await?;
    let mut all_ok = report(
        while_open == ["before"],
        "the open transaction gates everything behind it",
        if while_open == ["before"] {
            "only `before` was delivered — `plain-behind` is withheld too, although it is \
             non-transactional and fully acked"
                .to_string()
        } else {
            format!("expected only [\"before\"], got {while_open:?}")
        },
    );

    println!();
    println!("--- read_uncommitted control, same moment ---");
    let mut control = build_consumer(&bootstrap, &format!("txn-lso-control-{suffix}"), "read_uncommitted")?;
    control
        .assign(vec![tp.clone()])
        .await
        .map_err(|e| format!("assign control: {e}"))?;
    let uncommitted = consume_exactly(&mut control, 3, true).await?;
    all_ok &= report(
        uncommitted == ["before", "open-1", "plain-behind"],
        "read_uncommitted sees all three records right now (control)",
        if uncommitted == ["before", "open-1", "plain-behind"] {
            "the records are on the broker and fetchable — read_committed is withholding, not missing them".to_string()
        } else {
            format!("expected all three records, got {uncommitted:?}")
        },
    );
    control.close().await.map_err(|e| format!("close control: {e}"))?;

    println!();
    println!("--- committing the transaction ---");
    transactional.commit_transaction().await.map_err(|e| format!("commit: {e}"))?;
    println!("  commit_transaction ...... ok — the LSO advances past the transaction");

    let released = drain_until_idle(&mut gated, true).await?;
    all_ok &= report(
        released == ["open-1", "plain-behind"],
        "the commit releases the gated records, in log order",
        if released == ["open-1", "plain-behind"] {
            "the same consumer now received the transactional record and the plain one behind it".to_string()
        } else {
            format!("expected [\"open-1\", \"plain-behind\"], got {released:?}")
        },
    );

    gated.close().await.map_err(|e| format!("close: {e}"))?;
    close_producer(&transactional).await?;
    close_producer(&plain).await?;

    if !all_ok {
        return Err("the LSO gating behavior did not hold — see the ❌ lines above".to_string());
    }
    println!();
    println!("✅ LSO GATING WORKS: read_committed never runs ahead of an open transaction,");
    println!("   and a commit releases exactly what was gated, in order.");
    Ok(())
}
