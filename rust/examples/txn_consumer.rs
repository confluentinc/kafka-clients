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

//! Manual smoke test for the **consumer side of transactions** against a real
//! broker — the basic use case. Run `cargo run --example txn_producer` first —
//! it writes the data this program verifies.
//!
//! # What "transactions work" looks like from the consumer
//!
//! The producer committed `committed-1..3`, aborted `aborted-1..3`, then
//! committed `committed-4..6`, all on partition 0. The consumer-side
//! guarantee is checked from two angles:
//!
//! - **Phase 1, `read_committed`** (the guarantee): reading the partition
//!   from the beginning must deliver exactly the committed values, in order,
//!   as whole batches — and none of the `aborted-*` values. Because
//!   `committed-4..6` sit *behind* the aborted batch in the log, seeing them
//!   proves the consumer read past the abort marker and skipped the aborted
//!   records, rather than merely stopping in front of them.
//! - **Phase 2, `read_uncommitted`** (the control): the same read must
//!   additionally deliver the `aborted-*` values. That proves the aborted
//!   records physically exist on the partition and phase 1 *filtered* them —
//!   without this, phase 1 would also pass if the producer had never written
//!   them at all.
//!
//! Every delivered record is printed with its offset; the verdict at the end
//! prints one ✅/❌ line per check and the process exits non-zero if any
//! check fails.
//!
//! Re-running either program is fine: each complete producer run appends the
//! same nine values, so the consumer infers the number of runs and expects
//! every batch that many times — atomicity means committed batches are
//! all-or-nothing, so the counts must line up exactly. (A producer run that
//! was killed part-way breaks that arithmetic and can leave an open
//! transaction that blocks `read_committed` readers until
//! `transaction.timeout.ms` expires — point `TXN_TEST_TOPIC` at a fresh topic
//! if that happens.)
//!
//! Broker and env vars: same as `txn_producer` — `KAFKA_BOOTSTRAP_SERVERS`
//! (default `localhost:9092`) and `TXN_TEST_TOPIC` (default
//! `txn-manual-test`); see that file's docs for the docker command.

mod txn_common;

use txn_common::read_partition;
use txn_common::report;
use txn_common::sequence_check;

/// What one producer run leaves visible to `read_committed`, in log order.
const COMMITTED_SEQUENCE: [&str; 6] = [
    "committed-1",
    "committed-2",
    "committed-3",
    "committed-4",
    "committed-5",
    "committed-6",
];

/// What one producer run leaves visible to `read_uncommitted`, in log order:
/// the aborted batch sits between the two committed ones.
const FULL_SEQUENCE: [&str; 9] = [
    "committed-1",
    "committed-2",
    "committed-3",
    "aborted-1",
    "aborted-2",
    "aborted-3",
    "committed-4",
    "committed-5",
    "committed-6",
];

fn topic() -> String {
    std::env::var("TXN_TEST_TOPIC").unwrap_or_else(|_| "txn-manual-test".to_string())
}

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
    let topic = topic();

    println!("=== transactional consumer — manual test ===");
    println!("bootstrap.servers : {bootstrap}");
    println!("topic             : {topic}");

    let committed = read_partition(&bootstrap, &topic, "read_committed", true).await?;
    let uncommitted = read_partition(&bootstrap, &topic, "read_uncommitted", true).await?;

    println!();
    println!("=== verdict ===");

    // Each complete producer run appends "committed-1" exactly once, so its
    // count says how many runs the topic holds.
    let runs = committed.iter().filter(|v| v.as_str() == "committed-1").count();

    let mut all_ok = report(
        runs >= 1,
        "committed data was delivered",
        if runs >= 1 {
            format!("the topic holds {runs} complete producer run(s)")
        } else {
            "no committed values seen — did `cargo run --example txn_producer` succeed first?".to_string()
        },
    );
    if runs == 0 {
        println!("   (remaining checks skipped — no data to verify)");
        return Err("nothing to verify — run the producer first".to_string());
    }

    all_ok &= sequence_check(
        "read_committed delivered exactly the committed values, in order",
        &committed,
        &COMMITTED_SEQUENCE,
        runs,
    );

    let leaked: Vec<&String> = committed.iter().filter(|v| v.starts_with("aborted")).collect();
    all_ok &= report(
        leaked.is_empty(),
        "no aborted value leaked into read_committed",
        if leaked.is_empty() {
            "aborted records were skipped".to_string()
        } else {
            format!("leaked: {leaked:?}")
        },
    );

    all_ok &= sequence_check(
        "read_uncommitted also sees the aborted values (control)",
        &uncommitted,
        &FULL_SEQUENCE,
        runs,
    );

    if !all_ok {
        return Err("one or more transactional guarantees were violated — see the ❌ lines above".to_string());
    }

    println!();
    println!("✅ TRANSACTIONS WORK: committed data (and only committed data) is delivered under");
    println!("   read_committed, and the aborted records exist in the log but were filtered out.");
    Ok(())
}
