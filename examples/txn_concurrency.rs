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

//! Manual test: **concurrency, scale, and interleaving** (wave-3 🟢 batch,
//! part 2 of 4). Self-contained, run-unique topics.
//!
//! Cases:
//!
//! 1. **Concurrent sends into one transaction** — 8 tasks × 250 records
//!    through one `Arc`-shared producer, one commit. Exactly 2,000 records,
//!    no duplicates, and each task's own subsequence in order (the producer
//!    is documented thread-safe; this also stresses the lock topology).
//! 2. **Fencing while sends are in flight** — a successor `init_transactions`
//!    lands while 300 unawaited sends race the sender. Every future must
//!    resolve (acked or fenced) — none may hang — and the zombie's commit
//!    must fail (KIP-890; CLAUDE.md §9.5 callback obligation).
//! 3. **Transaction-per-message churn** — 90 back-to-back transactions,
//!    every third aborted. Under `transaction.version=2` each EndTxn bumps
//!    the producer epoch, so this exercises epoch tracking 90 times
//!    (Apache `testBumpTransactionalEpochWithTV2Enabled`, scaled).
//! 4. **Three producers with overlapping open transactions** on one
//!    partition, deterministic interleave, mixed commit/abort — the hardest
//!    `read_committed` filtering shape (multiple overlapping aborted ranges,
//!    KIP-98's aborted-transaction list).
//! 5. **Marker fan-out** — one commit and one abort each spanning six topics
//!    at once; on a single broker that is one `WriteTxnMarkers` fan-out
//!    (Apache `testMultipleMarkersOneLeader`).
//! 6. **`max.in.flight.requests.per.connection=1`** — the full
//!    commit/abort/commit flow with pipelining disabled (Apache
//!    `testTransactionalProducerSingleBrokerMaxInFlightOne`).
//!
//! Broker setup: see `examples/README.md`. Exit code 0 = every check ✅.

mod txn_common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use confluent_kafka::producer::Producer;

use txn_common::close_producer;
use txn_common::read_partition_idle;
use txn_common::report;
use txn_common::send_value;
use txn_common::send_value_printed;
use txn_common::string_record;
use txn_common::transactional_producer;
use txn_common::transactional_producer_with;

const IDLE: Duration = Duration::from_secs(3);

#[tokio::main]
async fn main() {
    let code = match run().await {
        Ok(()) => 0,
        Err(message) => {
            println!();
            println!("❌ CONCURRENCY TEST FAILED: {message}");
            1
        },
    };
    // Hard exit: case 2 deliberately leaves a fenced producer behind.
    std::process::exit(code);
}

async fn run() -> Result<(), String> {
    let bootstrap = txn_common::bootstrap_servers();
    let suffix = txn_common::unique_suffix();
    println!("=== transactions under concurrency and scale — manual test ===");
    println!("bootstrap.servers : {bootstrap}");

    let mut all_ok = true;
    all_ok &= concurrent_sends_case(&bootstrap, &suffix).await?;
    all_ok &= fencing_in_flight_case(&bootstrap, &suffix).await?;
    all_ok &= churn_case(&bootstrap, &suffix).await?;
    all_ok &= overlapping_case(&bootstrap, &suffix).await?;
    all_ok &= fanout_case(&bootstrap, &suffix).await?;
    all_ok &= max_in_flight_one_case(&bootstrap, &suffix).await?;

    if !all_ok {
        return Err("one or more concurrency guarantees were violated — see the ❌ lines above".to_string());
    }
    println!();
    println!("✅ CONCURRENCY HOLDS: shared-producer sends, in-flight fencing, epoch churn,");
    println!("   overlapping transactions, marker fan-out and max.in.flight=1 all behave.");
    Ok(())
}

/// Case 1: 8 tasks send through one producer inside one transaction.
async fn concurrent_sends_case(bootstrap: &str, suffix: &str) -> Result<bool, String> {
    const TASKS: usize = 8;
    const PER_TASK: usize = 250;
    println!();
    println!("--- case 1: {TASKS} tasks × {PER_TASK} sends, one transaction ---");
    let topic = Arc::new(format!("txn-conc-shared-{suffix}"));
    let producer = Arc::new(transactional_producer(bootstrap, &format!("txn-manual-conc-shared-{suffix}"))?);
    producer
        .init_transactions()
        .await
        .map_err(|e| format!("concurrent: init: {e}"))?;
    producer.begin_transaction().map_err(|e| format!("concurrent: begin: {e}"))?;

    let mut handles = Vec::with_capacity(TASKS);
    for task in 0..TASKS {
        let producer = Arc::clone(&producer);
        let topic = Arc::clone(&topic);
        handles.push(tokio::spawn(async move {
            for i in 0..PER_TASK {
                let value = format!("t{task}-{i:03}");
                send_value(&producer, &topic, &value).await?;
            }
            Ok::<(), String>(())
        }));
    }
    for (task, handle) in handles.into_iter().enumerate() {
        handle
            .await
            .map_err(|e| format!("task {task} panicked: {e}"))?
            .map_err(|e| format!("task {task}: {e}"))?;
    }
    println!("  all {} sends acked from {TASKS} concurrent tasks", TASKS * PER_TASK);
    producer
        .commit_transaction()
        .await
        .map_err(|e| format!("concurrent: commit: {e}"))?;
    close_producer(&producer).await?;

    let committed = read_partition_idle(bootstrap, &topic, "read_committed", false, IDLE).await?;
    let unique: HashSet<&String> = committed.iter().collect();
    let mut ok = report(
        committed.len() == TASKS * PER_TASK && unique.len() == committed.len(),
        "exactly one copy of every concurrent send is visible",
        format!("{} records, {} unique", committed.len(), unique.len()),
    );
    // Per-producer ordering is a Kafka guarantee; concurrent tasks interleave,
    // but each task's own values must stay in its send order.
    let mut per_task_ordered = true;
    for task in 0..TASKS {
        let prefix = format!("t{task}-");
        let seen: Vec<&String> = committed.iter().filter(|v| v.starts_with(&prefix)).collect();
        let mut sorted = seen.clone();
        sorted.sort();
        if seen != sorted {
            per_task_ordered = false;
        }
    }
    ok &= report(
        per_task_ordered,
        "each task's subsequence arrived in its send order",
        "zero-padded indexes, so lexicographic order = send order".to_string(),
    );
    Ok(ok)
}

/// Case 2: a fencing init lands mid-storm; every in-flight future resolves.
async fn fencing_in_flight_case(bootstrap: &str, suffix: &str) -> Result<bool, String> {
    const IN_FLIGHT: usize = 300;
    println!();
    println!("--- case 2: fencing while {IN_FLIGHT} sends are in flight ---");
    let topic = format!("txn-conc-fence-{suffix}");
    let txn_id = format!("txn-manual-conc-fence-{suffix}");

    let zombie = transactional_producer(bootstrap, &txn_id)?;
    zombie.init_transactions().await.map_err(|e| format!("fence: init A: {e}"))?;
    zombie.begin_transaction().map_err(|e| format!("fence: begin A: {e}"))?;
    send_value(&zombie, &topic, "storm-000")
        .await
        .map_err(|e| format!("fence: first send: {e}"))?;

    let mut futures = Vec::with_capacity(IN_FLIGHT);
    for i in 1..=IN_FLIGHT {
        let record = string_record(&topic, &format!("storm-{i:03}"))?;
        futures.push(zombie.send(record).await.map_err(|e| format!("fence: enqueue {i}: {e}"))?);
    }
    // The successor initializes while the storm is still in flight.
    let successor = transactional_producer(bootstrap, &txn_id)?;
    successor
        .init_transactions()
        .await
        .map_err(|e| format!("fence: successor init: {e}"))?;
    println!("  successor init_transactions completed while the storm was in flight");

    let mut resolved_ok = 0usize;
    let mut resolved_err = 0usize;
    let mut unresolved = 0usize;
    let mut sample_error = String::new();
    for future in &futures {
        match future.get_timeout(Duration::from_secs(15)).await {
            Ok(_) => resolved_ok += 1,
            Err(e) if e.to_string().contains("Timeout expired") => unresolved += 1,
            Err(e) => {
                resolved_err += 1;
                if sample_error.is_empty() {
                    sample_error = first_line(&e.to_string());
                }
            },
        }
    }
    let mut ok = report(
        unresolved == 0,
        "every in-flight future resolved through the fencing",
        format!("{resolved_ok} acked, {resolved_err} failed ({sample_error})"),
    );

    let commit = zombie.commit_transaction().await;
    match commit {
        Err(error) => {
            ok &= report(
                true,
                "the zombie's commit is refused",
                format!("{} (fatal={})", first_line(&error.to_string()), error.is_fatal()),
            );
        },
        Ok(()) => ok &= report(false, "the zombie's commit is refused", "it unexpectedly succeeded".to_string()),
    }
    drop(zombie);

    successor
        .begin_transaction()
        .map_err(|e| format!("fence: successor begin: {e}"))?;
    send_value_printed(&successor, &topic, "successor-1").await?;
    successor
        .commit_transaction()
        .await
        .map_err(|e| format!("fence: successor commit: {e}"))?;
    close_producer(&successor).await?;

    let committed = read_partition_idle(bootstrap, &topic, "read_committed", false, IDLE).await?;
    ok &= report(
        committed.last().map(String::as_str) == Some("successor-1") && committed.len() == 1,
        "only the successor's transaction is visible",
        format!(
            "read_committed: {} record(s), last = {:?} — the zombie's storm was aborted by the successor's init",
            committed.len(),
            committed.last()
        ),
    );
    Ok(ok)
}

/// Case 3: 90 transactions back to back, every third aborted.
async fn churn_case(bootstrap: &str, suffix: &str) -> Result<bool, String> {
    const TXNS: usize = 90;
    println!();
    println!("--- case 3: {TXNS} transactions back to back (every third aborted) ---");
    let topic = format!("txn-conc-churn-{suffix}");
    let producer = transactional_producer(bootstrap, &format!("txn-manual-conc-churn-{suffix}"))?;
    producer.init_transactions().await.map_err(|e| format!("churn: init: {e}"))?;

    let mut expected: Vec<String> = Vec::new();
    for i in 0..TXNS {
        let abort = i % 3 == 0;
        producer.begin_transaction().map_err(|e| format!("churn: begin {i}: {e}"))?;
        for part in ["a", "b"] {
            let value = format!("churn-{i:02}-{part}");
            send_value(&producer, &topic, &value)
                .await
                .map_err(|e| format!("churn: send {value}: {e}"))?;
            if !abort {
                expected.push(value);
            }
        }
        if abort {
            producer
                .abort_transaction()
                .await
                .map_err(|e| format!("churn: abort {i}: {e}"))?;
        } else {
            producer
                .commit_transaction()
                .await
                .map_err(|e| format!("churn: commit {i}: {e}"))?;
        }
    }
    println!("  {TXNS} transactions completed (30 aborted, 60 committed)");
    close_producer(&producer).await?;

    let committed = read_partition_idle(bootstrap, &topic, "read_committed", false, IDLE).await?;
    Ok(report(
        committed == expected,
        "exactly the 60 committed transactions' records, in order",
        format!("{} records (expected {})", committed.len(), expected.len()),
    ))
}

/// Case 4: three producers, overlapping open transactions, one partition.
async fn overlapping_case(bootstrap: &str, suffix: &str) -> Result<bool, String> {
    println!();
    println!("--- case 4: three producers with overlapping transactions ---");
    let topic = format!("txn-conc-overlap-{suffix}");
    let x = transactional_producer(bootstrap, &format!("txn-manual-conc-x-{suffix}"))?;
    let y = transactional_producer(bootstrap, &format!("txn-manual-conc-y-{suffix}"))?;
    let z = transactional_producer(bootstrap, &format!("txn-manual-conc-z-{suffix}"))?;
    x.init_transactions().await.map_err(|e| format!("overlap: init x: {e}"))?;
    y.init_transactions().await.map_err(|e| format!("overlap: init y: {e}"))?;
    z.init_transactions().await.map_err(|e| format!("overlap: init z: {e}"))?;

    // Every send is awaited, so the log order is exactly this script. Three
    // transactions are open at once; X commits, Y aborts, Z commits.
    x.begin_transaction().map_err(|e| format!("overlap: begin x: {e}"))?;
    y.begin_transaction().map_err(|e| format!("overlap: begin y: {e}"))?;
    z.begin_transaction().map_err(|e| format!("overlap: begin z: {e}"))?;
    send_value_printed(&x, &topic, "x1").await?;
    send_value_printed(&y, &topic, "y1").await?;
    send_value_printed(&z, &topic, "z1").await?;
    send_value_printed(&x, &topic, "x2").await?;
    x.commit_transaction().await.map_err(|e| format!("overlap: commit x: {e}"))?;
    send_value_printed(&y, &topic, "y2").await?;
    send_value_printed(&z, &topic, "z2").await?;
    y.abort_transaction().await.map_err(|e| format!("overlap: abort y: {e}"))?;
    send_value_printed(&z, &topic, "z3").await?;
    z.commit_transaction().await.map_err(|e| format!("overlap: commit z: {e}"))?;

    // Round 2: X aborts, Y commits.
    x.begin_transaction().map_err(|e| format!("overlap: begin x2: {e}"))?;
    y.begin_transaction().map_err(|e| format!("overlap: begin y2: {e}"))?;
    send_value_printed(&x, &topic, "x3").await?;
    send_value_printed(&y, &topic, "y3").await?;
    x.abort_transaction().await.map_err(|e| format!("overlap: abort x: {e}"))?;
    y.commit_transaction().await.map_err(|e| format!("overlap: commit y2: {e}"))?;
    close_producer(&x).await?;
    close_producer(&y).await?;
    close_producer(&z).await?;

    let committed = read_partition_idle(bootstrap, &topic, "read_committed", true, IDLE).await?;
    let full = read_partition_idle(bootstrap, &topic, "read_uncommitted", false, IDLE).await?;
    let mut ok = report(
        committed == ["x1", "z1", "x2", "z2", "z3", "y3"],
        "read_committed untangles three overlapping transactions",
        format!("got {committed:?}"),
    );
    ok &= report(
        full == ["x1", "y1", "z1", "x2", "y2", "z2", "z3", "x3", "y3"],
        "read_uncommitted shows the full interleaving (control)",
        format!("{} records in log order", full.len()),
    );
    Ok(ok)
}

/// Case 5: one commit and one abort spanning six topics each.
async fn fanout_case(bootstrap: &str, suffix: &str) -> Result<bool, String> {
    const TOPICS: usize = 6;
    println!();
    println!("--- case 5: marker fan-out across {TOPICS} topics ---");
    let topics: Vec<String> = (0..TOPICS).map(|i| format!("txn-conc-fan{i}-{suffix}")).collect();
    let producer = transactional_producer(bootstrap, &format!("txn-manual-conc-fan-{suffix}"))?;
    producer.init_transactions().await.map_err(|e| format!("fanout: init: {e}"))?;

    producer.begin_transaction().map_err(|e| format!("fanout: begin #1: {e}"))?;
    for topic in &topics {
        send_value(&producer, topic, "fan-committed")
            .await
            .map_err(|e| format!("fanout: {topic}: {e}"))?;
    }
    producer
        .commit_transaction()
        .await
        .map_err(|e| format!("fanout: commit: {e}"))?;
    println!("  one commit covered all {TOPICS} topics");

    producer.begin_transaction().map_err(|e| format!("fanout: begin #2: {e}"))?;
    for topic in &topics {
        send_value(&producer, topic, "fan-aborted")
            .await
            .map_err(|e| format!("fanout: {topic}: {e}"))?;
    }
    producer.abort_transaction().await.map_err(|e| format!("fanout: abort: {e}"))?;
    println!("  one abort covered all {TOPICS} topics");
    close_producer(&producer).await?;

    let mut ok = true;
    for topic in &topics {
        let committed = read_partition_idle(bootstrap, topic, "read_committed", false, IDLE).await?;
        ok &= report(
            committed == ["fan-committed"],
            &format!("{topic}: exactly the committed record"),
            format!("got {committed:?}"),
        );
    }
    let control = read_partition_idle(bootstrap, &topics[0], "read_uncommitted", false, IDLE).await?;
    ok &= report(
        control == ["fan-committed", "fan-aborted"],
        "the aborted fan-out was written then filtered (control on topic 0)",
        format!("read_uncommitted: {control:?}"),
    );
    Ok(ok)
}

/// Case 6: the whole flow with pipelining disabled.
async fn max_in_flight_one_case(bootstrap: &str, suffix: &str) -> Result<bool, String> {
    println!();
    println!("--- case 6: max.in.flight.requests.per.connection=1 ---");
    let topic = format!("txn-conc-mif1-{suffix}");
    let producer = transactional_producer_with(
        bootstrap,
        &format!("txn-manual-conc-mif1-{suffix}"),
        &[("max.in.flight.requests.per.connection", "1")],
    )?;
    producer.init_transactions().await.map_err(|e| format!("mif1: init: {e}"))?;

    producer.begin_transaction().map_err(|e| format!("mif1: begin #1: {e}"))?;
    for value in ["mif-1", "mif-2", "mif-3"] {
        send_value(&producer, &topic, value)
            .await
            .map_err(|e| format!("mif1: send {value}: {e}"))?;
    }
    producer
        .commit_transaction()
        .await
        .map_err(|e| format!("mif1: commit #1: {e}"))?;
    producer.begin_transaction().map_err(|e| format!("mif1: begin #2: {e}"))?;
    send_value(&producer, &topic, "mif-aborted")
        .await
        .map_err(|e| format!("mif1: send aborted: {e}"))?;
    producer.abort_transaction().await.map_err(|e| format!("mif1: abort: {e}"))?;
    producer.begin_transaction().map_err(|e| format!("mif1: begin #3: {e}"))?;
    send_value(&producer, &topic, "mif-4")
        .await
        .map_err(|e| format!("mif1: send mif-4: {e}"))?;
    producer
        .commit_transaction()
        .await
        .map_err(|e| format!("mif1: commit #2: {e}"))?;
    close_producer(&producer).await?;

    let committed = read_partition_idle(bootstrap, &topic, "read_committed", true, IDLE).await?;
    Ok(report(
        committed == ["mif-1", "mif-2", "mif-3", "mif-4"],
        "the full flow works with pipelining disabled",
        format!("got {committed:?}"),
    ))
}

fn first_line(text: &str) -> String {
    text.lines().next().unwrap_or(text).to_string()
}
