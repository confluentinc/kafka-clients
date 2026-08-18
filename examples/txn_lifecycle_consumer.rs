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

//! Manual test, verify side: **producer lifecycle**.
//! Run `cargo run --example txn_lifecycle_producer` first.
//!
//! Checks (see the producer file for the script it ran):
//!
//! 1. **Fencing / crash recovery topic.** `read_committed` delivers only the
//!    successor's committed batch. The crashed producer's `abandoned-*`
//!    records must be invisible — the successor's `init_transactions` aborted
//!    them — and the zombie's `zombie-*` record must appear nowhere at all,
//!    not even under `read_uncommitted`, because the fenced send never
//!    reached the log. `read_uncommitted` *does* show the abandoned records,
//!    proving they were written and then aborted rather than never sent.
//! 2. **Interleaved topic.** `read_committed` yields exactly the interleaved
//!    committed/plain values in log order with the aborted transaction
//!    removed from the middle; `read_uncommitted` shows the full interleaving
//!    — per-producer marker attribution works.
//!
//! Re-running is fine: expectations scale with the number of complete
//! producer runs, inferred from the data.

mod txn_common;

use txn_common::read_partition;
use txn_common::report;
use txn_common::sequence_check;

const FENCING_TOPIC: &str = "txn-fencing";
/// Only the successor's batch survives for read_committed.
const FENCING_COMMITTED: [&str; 3] = ["fenced-committed-1", "fenced-committed-2", "fenced-committed-3"];
/// read_uncommitted also sees the crashed producer's abandoned batch — but
/// never the zombie's record, whose send was rejected outright.
const FENCING_FULL: [&str; 5] = [
    "abandoned-1",
    "abandoned-2",
    "fenced-committed-1",
    "fenced-committed-2",
    "fenced-committed-3",
];

const INTERLEAVED_TOPIC: &str = "txn-interleaved";
/// Log order minus Y's aborted transaction.
const INTERLEAVED_COMMITTED: [&str; 5] = ["x-1", "plain-1", "x-2", "y-3", "plain-2"];
/// Full log order as the three writers interleaved their sends.
const INTERLEAVED_FULL: [&str; 7] = ["x-1", "y-abort-1", "plain-1", "x-2", "y-abort-2", "y-3", "plain-2"];

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
    println!("=== producer lifecycle — verify side ===");
    println!("bootstrap.servers : {bootstrap}");

    let fencing_committed = read_partition(&bootstrap, FENCING_TOPIC, "read_committed", true).await?;
    let fencing_full = read_partition(&bootstrap, FENCING_TOPIC, "read_uncommitted", true).await?;
    let interleaved_committed = read_partition(&bootstrap, INTERLEAVED_TOPIC, "read_committed", true).await?;
    let interleaved_full = read_partition(&bootstrap, INTERLEAVED_TOPIC, "read_uncommitted", true).await?;

    println!();
    println!("=== verdict ===");

    let fencing_runs = count_of(&fencing_committed, FENCING_COMMITTED[0]);
    let interleaved_runs = count_of(&interleaved_committed, INTERLEAVED_COMMITTED[0]);
    let mut all_ok = report(
        fencing_runs >= 1 && interleaved_runs >= 1,
        "committed data was delivered",
        if fencing_runs >= 1 && interleaved_runs >= 1 {
            format!("fencing topic: {fencing_runs} run(s), interleaved topic: {interleaved_runs} run(s)")
        } else {
            "no committed values seen — did `cargo run --example txn_lifecycle_producer` succeed first?".to_string()
        },
    );
    if fencing_runs == 0 || interleaved_runs == 0 {
        println!("   (remaining checks skipped — no data to verify)");
        return Err("nothing to verify — run the producer first".to_string());
    }

    // Case 1: fencing / crash recovery.
    all_ok &= sequence_check(
        "fencing: read_committed sees only the successor's batch",
        &fencing_committed,
        &FENCING_COMMITTED,
        fencing_runs,
    );
    all_ok &= sequence_check(
        "fencing: read_uncommitted additionally sees the abandoned batch (aborted by the successor's init)",
        &fencing_full,
        &FENCING_FULL,
        fencing_runs,
    );
    let zombie_leaked: Vec<&String> = fencing_full.iter().filter(|v| v.starts_with("zombie")).collect();
    all_ok &= report(
        zombie_leaked.is_empty(),
        "the fenced producer's record never reached the log",
        if zombie_leaked.is_empty() {
            "no zombie-* value even under read_uncommitted — the broker rejected the fenced send".to_string()
        } else {
            format!("leaked: {zombie_leaked:?}")
        },
    );

    // Case 2: interleaved writers.
    all_ok &= sequence_check(
        "interleaved: read_committed untangles the three writers (aborted batch dropped)",
        &interleaved_committed,
        &INTERLEAVED_COMMITTED,
        interleaved_runs,
    );
    all_ok &= sequence_check(
        "interleaved: read_uncommitted shows the full interleaving (control)",
        &interleaved_full,
        &INTERLEAVED_FULL,
        interleaved_runs,
    );

    if !all_ok {
        return Err("one or more lifecycle guarantees were violated — see the ❌ lines above".to_string());
    }

    println!();
    println!("✅ LIFECYCLE HOLDS: the successor recovered the dangling transaction, the zombie");
    println!("   never contaminated the log, and markers are attributed per producer.");
    Ok(())
}

fn count_of(values: &[String], needle: &str) -> usize {
    values.iter().filter(|v| v.as_str() == needle).count()
}
