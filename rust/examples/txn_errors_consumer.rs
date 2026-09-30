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

//! Manual test, verify side: **failed transactions leave nothing visible**.
//! Run `cargo run --example txn_errors_producer` first.
//!
//! The producer file deliberately drove three transactions into the ground;
//! this file checks the log agrees:
//!
//! - **timed-out topic** — the coordinator aborted the idle transaction, so
//!   `read_committed` must deliver nothing, while `read_uncommitted` shows
//!   the record that was written before the timeout (written, then aborted).
//! - **poison topic** — same shape: the small record that was acked before
//!   the oversized one poisoned the transaction must be invisible to
//!   `read_committed` (the whole transaction aborted, not just the bad
//!   record) and visible to `read_uncommitted`.
//! - **misuse topic** — every write attempt was rejected before reaching the
//!   log, so even `read_uncommitted` must be empty.
//!
//! Re-running is fine: expectations scale with the number of producer runs,
//! inferred from the `read_uncommitted` data.

mod txn_common;

use txn_common::read_partition;
use txn_common::report;
use txn_common::sequence_check;

const MISUSE_TOPIC: &str = "txn-errors-misuse";
const TIMEDOUT_TOPIC: &str = "txn-errors-timedout";
const POISON_TOPIC: &str = "txn-errors-poison";

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(message) => {
            println!();
            println!("❌ CONSUMER TEST FAILED: {message}");
            std::process::ExitCode::FAILURE
        },
    }
}

async fn run() -> Result<(), String> {
    let bootstrap = txn_common::bootstrap_servers();
    println!("=== transaction error handling — verify side ===");
    println!("bootstrap.servers : {bootstrap}");

    let timedout_committed = read_partition(&bootstrap, TIMEDOUT_TOPIC, "read_committed", true).await?;
    let timedout_full = read_partition(&bootstrap, TIMEDOUT_TOPIC, "read_uncommitted", true).await?;
    let poison_committed = read_partition(&bootstrap, POISON_TOPIC, "read_committed", true).await?;
    let poison_full = read_partition(&bootstrap, POISON_TOPIC, "read_uncommitted", true).await?;
    let misuse_full = read_partition(&bootstrap, MISUSE_TOPIC, "read_uncommitted", true).await?;

    println!();
    println!("=== verdict ===");

    // The failed transactions' records only ever exist below the commit
    // marker level, so the run count comes from the read_uncommitted view.
    let timedout_runs = count_of(&timedout_full, "timedout-1");
    let poison_runs = count_of(&poison_full, "poison-small-1");
    let mut all_ok = report(
        timedout_runs >= 1 && poison_runs >= 1,
        "the failed transactions' records are in the log",
        if timedout_runs >= 1 && poison_runs >= 1 {
            format!("timed-out topic: {timedout_runs} run(s), poison topic: {poison_runs} run(s)")
        } else {
            "no attempted records found — did `cargo run --example txn_errors_producer` succeed first?".to_string()
        },
    );
    if timedout_runs == 0 || poison_runs == 0 {
        println!("   (remaining checks skipped — no data to verify)");
        return Err("nothing to verify — run the producer first".to_string());
    }

    all_ok &= report(
        timedout_committed.is_empty(),
        "timed-out transaction: read_committed delivers nothing",
        if timedout_committed.is_empty() {
            "the coordinator's abort made the records permanently invisible".to_string()
        } else {
            format!("leaked: {timedout_committed:?}")
        },
    );
    all_ok &= sequence_check(
        "timed-out transaction: read_uncommitted shows the attempted record (control)",
        &timedout_full,
        &["timedout-1"],
        timedout_runs,
    );

    all_ok &= report(
        poison_committed.is_empty(),
        "poisoned transaction: read_committed delivers nothing — not even the record that was acked fine",
        if poison_committed.is_empty() {
            "the whole transaction aborted, not just the oversized record".to_string()
        } else {
            format!("leaked: {poison_committed:?}")
        },
    );
    all_ok &= sequence_check(
        "poisoned transaction: read_uncommitted shows the acked small record (control)",
        &poison_full,
        &["poison-small-1"],
        poison_runs,
    );

    all_ok &= report(
        misuse_full.is_empty(),
        "API misuse: nothing ever reached the log, even under read_uncommitted",
        if misuse_full.is_empty() {
            "every misused send was rejected before it was written".to_string()
        } else {
            format!("leaked: {misuse_full:?}")
        },
    );

    if !all_ok {
        return Err("a failed transaction left visible data — see the ❌ lines above".to_string());
    }

    println!();
    println!("✅ FAILED TRANSACTIONS ARE CLEAN: timed-out and poisoned transactions were fully");
    println!("   aborted, and misused calls never wrote anything at all.");
    Ok(())
}

fn count_of(values: &[String], needle: &str) -> usize {
    values.iter().filter(|v| v.as_str() == needle).count()
}
