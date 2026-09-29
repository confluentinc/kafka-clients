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

//! Manual test, verify side: **transaction atomicity beyond one partition**.
//! Run `cargo run --example txn_atomicity_producer` first.
//!
//! Checks, per case (see the producer file for what was written):
//!
//! 1. **Multi-topic transaction** — every one of the three topics shows
//!    exactly the committed values under `read_committed`, the same number of
//!    producer runs on each (a differing count would be a commit torn across
//!    topics), and none of the aborted values anywhere. A `read_uncommitted`
//!    spot check on the first topic proves the aborted records were written
//!    and filtered, not never sent.
//! 2. **Large transaction** — exactly 10,000 records per producer run, in
//!    order, nothing missing or duplicated.
//! 3. **Commit implies flush** — exactly 1,000 records per producer run: the
//!    commit really did flush every unawaited send.
//!
//! Re-running is fine: the number of complete producer runs is inferred from
//! the data and every expectation scales with it.

mod txn_common;

use txn_common::read_partition;
use txn_common::report;
use txn_common::sequence_check;

const MULTI_TOPICS: [&str; 3] = ["txn-multi-a", "txn-multi-b", "txn-multi-c"];
const MULTI_COMMITTED: [&str; 2] = ["multi-committed-1", "multi-committed-2"];
/// Per-topic log order seen by `read_uncommitted`: the committed batch, then
/// the aborted one.
const MULTI_FULL: [&str; 4] = [
    "multi-committed-1",
    "multi-committed-2",
    "multi-aborted-1",
    "multi-aborted-2",
];

const LARGE_TOPIC: &str = "txn-large";
const LARGE_COUNT: usize = 10_000;

const FLUSH_TOPIC: &str = "txn-flush";
const FLUSH_COUNT: usize = 1_000;

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
    println!("=== transaction atomicity — verify side ===");
    println!("bootstrap.servers : {bootstrap}");

    // Collect everything first, then judge, so the verdict lines sit together.
    let mut multi_committed = Vec::new();
    for topic in MULTI_TOPICS {
        multi_committed.push(read_partition(&bootstrap, topic, "read_committed", true).await?);
    }
    let multi_uncommitted_a = read_partition(&bootstrap, MULTI_TOPICS[0], "read_uncommitted", true).await?;
    let large = read_partition(&bootstrap, LARGE_TOPIC, "read_committed", false).await?;
    let flush = read_partition(&bootstrap, FLUSH_TOPIC, "read_committed", false).await?;

    println!();
    println!("=== verdict ===");

    let runs = multi_committed[0].iter().filter(|v| v.as_str() == MULTI_COMMITTED[0]).count();
    let mut all_ok = report(
        runs >= 1,
        "committed data was delivered",
        if runs >= 1 {
            format!("the topics hold {runs} complete producer run(s)")
        } else {
            "no committed values seen — did `cargo run --example txn_atomicity_producer` succeed first?".to_string()
        },
    );
    if runs == 0 {
        println!("   (remaining checks skipped — no data to verify)");
        return Err("nothing to verify — run the producer first".to_string());
    }

    // Case 1: the same committed content on every topic, the aborted batch on
    // none — atomicity across topics.
    for (topic, values) in MULTI_TOPICS.iter().zip(&multi_committed) {
        all_ok &= sequence_check(
            &format!("{topic}: read_committed sees exactly the committed values"),
            values,
            &MULTI_COMMITTED,
            runs,
        );
    }
    let leaked: Vec<&String> = multi_committed
        .iter()
        .flatten()
        .filter(|v| v.starts_with("multi-aborted"))
        .collect();
    all_ok &= report(
        leaked.is_empty(),
        "the aborted transaction is invisible on all three topics",
        if leaked.is_empty() {
            "no aborted value on any topic — the abort covered every partition of the transaction".to_string()
        } else {
            format!("leaked: {leaked:?}")
        },
    );
    all_ok &= sequence_check(
        &format!("{}: read_uncommitted also sees the aborted values (control)", MULTI_TOPICS[0]),
        &multi_uncommitted_a,
        &MULTI_FULL,
        runs,
    );

    // Case 2: the large transaction arrived complete and ordered.
    let large_expected: Vec<String> = (1..=LARGE_COUNT).map(|i| format!("large-{i:05}")).collect();
    let large_expected_refs: Vec<&str> = large_expected.iter().map(String::as_str).collect();
    all_ok &= sequence_check(
        &format!("{LARGE_TOPIC}: all {LARGE_COUNT} records of the large transaction, in order"),
        &large,
        &large_expected_refs,
        runs_for(&large, large_expected_refs[0]),
    );

    // Case 3: the unawaited sends were all flushed by the commit.
    let flush_expected: Vec<String> = (1..=FLUSH_COUNT).map(|i| format!("flush-{i:04}")).collect();
    let flush_expected_refs: Vec<&str> = flush_expected.iter().map(String::as_str).collect();
    all_ok &= sequence_check(
        &format!("{FLUSH_TOPIC}: all {FLUSH_COUNT} unawaited sends were flushed by the commit"),
        &flush,
        &flush_expected_refs,
        runs_for(&flush, flush_expected_refs[0]),
    );

    if !all_ok {
        return Err("one or more atomicity guarantees were violated — see the ❌ lines above".to_string());
    }

    println!();
    println!("✅ ATOMICITY HOLDS: commits and aborts are all-or-nothing across topics, at");
    println!("   10k-record scale, and commit flushes everything still buffered.");
    Ok(())
}

/// Number of complete producer runs on a topic, inferred from how often its
/// first expected value appears — **floored at one**.
///
/// The floor is load-bearing, not defensive. `sequence_check` compares against
/// `base` repeated `runs` times, so a `runs` of 0 makes the expectation *empty*,
/// an empty topic compares equal to it, and the verdict prints
/// `✅ … — 0 records, exactly as expected`. That is the precise symptom cases 2
/// and 3 exist to detect: a `commit_transaction` that returns `Ok` while dropping
/// the records still buffered behind it leaves exactly an empty topic. Flooring
/// at one makes that case fail against a full expected run instead.
///
/// An earlier version of this comment claimed check 1 already gated the empty
/// case. It does not: check 1 counts `multi-committed-1` on `txn-multi-a` — a
/// different topic, written by a different producer in a different case — so its
/// early return says nothing about `txn-large` or `txn-flush`.
fn runs_for(values: &[String], first_expected: &str) -> usize {
    values.iter().filter(|v| v.as_str() == first_expected).count().max(1)
}
