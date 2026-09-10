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

//! Manual test: **consumer-side transaction contracts** (wave-3 🟢 batch,
//! part 3 of 4). Self-contained, run-unique topics.
//!
//! Cases:
//!
//! 1. **`end_offsets()` = LSO vs high-watermark** — with a transaction open,
//!    a `read_committed` consumer's `end_offsets` stops at the Last Stable
//!    Offset while `read_uncommitted` reports the high-watermark; commit
//!    converges them (KIP-98).
//! 2. **`position()` skips the marker** — after draining a topic that ends in
//!    a commit marker, the position is `marker offset + 1`, not
//!    `last record + 1` (the offset-gap contract).
//! 3. **Seek into an aborted range** — a consumer that seeks to an offset
//!    *inside* an aborted transaction must deliver none of it and resume at
//!    the committed data behind it (KIP-98 aborted-transaction list).
//! 4. **Delayed fetch across an abort** — with `fetch.min.bytes` large enough
//!    that the broker parks the fetch, `fetch.max.wait.ms` must still bound
//!    it, and an abort completing while a fetch is parked must never leak the
//!    aborted records (Apache `testDelayedFetchIncludesAbortedTransaction`).
//! 5. **Compression × transactions** — committed/aborted batches under gzip,
//!    snappy, lz4 and zstd; marker filtering must survive decompression.
//!
//! Broker setup: see `examples/README.md`. Exit code 0 = every check ✅.

mod txn_common;

use std::time::Duration;
use std::time::Instant;

use confluent_kafka::common::TopicPartition;

use txn_common::build_consumer;
use txn_common::build_consumer_with;
use txn_common::close_producer;
use txn_common::drain_until_idle;
use txn_common::plain_producer;
use txn_common::read_partition_idle;
use txn_common::report;
use txn_common::send_value;
use txn_common::send_value_printed;
use txn_common::transactional_producer;
use txn_common::transactional_producer_with;
use txn_common::unique_suffix;

const IDLE: Duration = Duration::from_secs(3);

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(message) => {
            println!();
            println!("❌ CONSUMER-CONTRACT TEST FAILED: {message}");
            std::process::ExitCode::FAILURE
        },
    }
}

async fn run() -> Result<(), String> {
    let bootstrap = txn_common::bootstrap_servers();
    let suffix = unique_suffix();
    println!("=== consumer-side transaction contracts — manual test ===");
    println!("bootstrap.servers : {bootstrap}");

    let mut all_ok = true;
    all_ok &= lso_and_position_case(&bootstrap, &suffix).await?;
    all_ok &= seek_into_aborted_case(&bootstrap, &suffix).await?;
    all_ok &= delayed_fetch_case(&bootstrap, &suffix).await?;
    all_ok &= compression_case(&bootstrap, &suffix).await?;

    if !all_ok {
        return Err("one or more consumer contracts were violated — see the ❌ lines above".to_string());
    }
    println!();
    println!("✅ CONSUMER CONTRACTS HOLD: LSO/end_offsets, positions over markers, seeks into");
    println!("   aborted ranges, parked fetches and compressed batches all behave.");
    Ok(())
}

/// Cases 1 + 2 share one topic: `before`@0, open txn `open-1`@1, then commit
/// (marker @2).
async fn lso_and_position_case(bootstrap: &str, suffix: &str) -> Result<bool, String> {
    println!();
    println!("--- cases 1+2: end_offsets (LSO vs HW) and position over a marker ---");
    let topic = format!("txn-cc-lso-{suffix}");
    let tp = TopicPartition::new(topic.clone(), 0);

    let plain = plain_producer(bootstrap, "txn-cc-lso-plain")?;
    send_value_printed(&plain, &topic, "before").await?;
    let transactional = transactional_producer(bootstrap, &format!("txn-manual-cc-lso-{suffix}"))?;
    transactional.init_transactions().await.map_err(|e| format!("lso: init: {e}"))?;
    transactional.begin_transaction().map_err(|e| format!("lso: begin: {e}"))?;
    send_value_printed(&transactional, &topic, "open-1").await?;

    let mut committed_reader = build_consumer(bootstrap, &format!("txn-cc-lso-rc-{suffix}"), "read_committed")?;
    committed_reader
        .assign(vec![tp.clone()])
        .await
        .map_err(|e| format!("lso: assign rc: {e}"))?;
    let mut uncommitted_reader = build_consumer(bootstrap, &format!("txn-cc-lso-ru-{suffix}"), "read_uncommitted")?;
    uncommitted_reader
        .assign(vec![tp.clone()])
        .await
        .map_err(|e| format!("lso: assign ru: {e}"))?;

    let lso = committed_reader
        .end_offsets(std::slice::from_ref(&tp))
        .await
        .map_err(|e| format!("end_offsets rc: {e}"))?;
    let hw = uncommitted_reader
        .end_offsets(std::slice::from_ref(&tp))
        .await
        .map_err(|e| format!("end_offsets ru: {e}"))?;
    let lso_open = lso.get(&tp).copied();
    let hw_open = hw.get(&tp).copied();
    let mut ok = report(
        lso_open == Some(1) && hw_open == Some(2),
        "with the transaction open: read_committed end_offsets = LSO, read_uncommitted = HW",
        format!("LSO view = {lso_open:?} (first open-transaction offset), HW view = {hw_open:?}"),
    );

    transactional
        .commit_transaction()
        .await
        .map_err(|e| format!("lso: commit: {e}"))?;
    println!("  commit_transaction ...... ok — marker at offset 2");
    // commit_transaction returns after EndTxn; the marker write that advances
    // the LSO is asynchronous on the broker, so poll briefly for convergence.
    let deadline = Instant::now() + Duration::from_secs(5);
    let (mut lso_after, mut hw_after) = (None, None);
    while Instant::now() < deadline {
        let lso = committed_reader
            .end_offsets(std::slice::from_ref(&tp))
            .await
            .map_err(|e| format!("end_offsets rc #2: {e}"))?;
        let hw = uncommitted_reader
            .end_offsets(std::slice::from_ref(&tp))
            .await
            .map_err(|e| format!("end_offsets ru #2: {e}"))?;
        lso_after = lso.get(&tp).copied();
        hw_after = hw.get(&tp).copied();
        if lso_after == Some(3) && hw_after == Some(3) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    ok &= report(
        lso_after == Some(3) && hw_after == Some(3),
        "after the commit both views converge past the marker",
        format!("LSO view = {lso_after:?}, HW view = {hw_after:?} (marker occupies offset 2)"),
    );

    // Case 2: drain and check the position jumped the marker.
    let drained = drain_until_idle(&mut committed_reader, true).await?;
    let position = committed_reader.position(&tp).await.map_err(|e| format!("position: {e}"))?;
    ok &= report(
        drained == ["before", "open-1"] && position == 3,
        "after draining, position() is marker offset + 1",
        format!("delivered {drained:?}, position = {position} (last record was offset 1)"),
    );

    committed_reader.close().await.map_err(|e| format!("lso: close rc: {e}"))?;
    uncommitted_reader.close().await.map_err(|e| format!("lso: close ru: {e}"))?;
    close_producer(&transactional).await?;
    close_producer(&plain).await?;
    Ok(ok)
}

/// Case 3: seek to an offset inside an aborted transaction.
async fn seek_into_aborted_case(bootstrap: &str, suffix: &str) -> Result<bool, String> {
    println!();
    println!("--- case 3: seek into the middle of an aborted range ---");
    let topic = format!("txn-cc-seek-{suffix}");
    let tp = TopicPartition::new(topic.clone(), 0);

    let producer = transactional_producer(bootstrap, &format!("txn-manual-cc-seek-{suffix}"))?;
    producer.init_transactions().await.map_err(|e| format!("seek: init: {e}"))?;
    producer.begin_transaction().map_err(|e| format!("seek: begin #1: {e}"))?;
    for value in ["seek-aborted-0", "seek-aborted-1", "seek-aborted-2"] {
        send_value_printed(&producer, &topic, value).await?;
    }
    producer.abort_transaction().await.map_err(|e| format!("seek: abort: {e}"))?;
    producer.begin_transaction().map_err(|e| format!("seek: begin #2: {e}"))?;
    for value in ["seek-committed-4", "seek-committed-5"] {
        send_value_printed(&producer, &topic, value).await?;
    }
    producer.commit_transaction().await.map_err(|e| format!("seek: commit: {e}"))?;
    close_producer(&producer).await?;

    let mut consumer = build_consumer(bootstrap, &format!("txn-cc-seek-{suffix}"), "read_committed")?;
    consumer
        .assign(vec![tp.clone()])
        .await
        .map_err(|e| format!("seek: assign: {e}"))?;
    consumer.seek_offset(tp.clone(), 1).await.map_err(|e| format!("seek(1): {e}"))?;
    println!("  seek({topic}-0, 1) — offset 1 is the middle of the aborted batch");
    let delivered = drain_until_idle(&mut consumer, true).await?;
    let ok = report(
        delivered == ["seek-committed-4", "seek-committed-5"],
        "the aborted remainder is skipped; delivery resumes at the committed batch",
        format!("got {delivered:?}"),
    );
    consumer.close().await.map_err(|e| format!("seek: close: {e}"))?;
    Ok(ok)
}

/// Case 4: a fetch parked by fetch.min.bytes spans an abort.
async fn delayed_fetch_case(bootstrap: &str, suffix: &str) -> Result<bool, String> {
    println!();
    println!("--- case 4: delayed fetch (fetch.min.bytes=1 MiB, fetch.max.wait.ms=2000) ---");
    let topic = format!("txn-cc-delayed-{suffix}");
    let tp = TopicPartition::new(topic.clone(), 0);

    let plain = plain_producer(bootstrap, "txn-cc-delayed-plain")?;
    send_value_printed(&plain, &topic, "delayed-seed").await?;
    let transactional = transactional_producer(bootstrap, &format!("txn-manual-cc-delayed-{suffix}"))?;
    transactional
        .init_transactions()
        .await
        .map_err(|e| format!("delayed: init: {e}"))?;
    transactional.begin_transaction().map_err(|e| format!("delayed: begin: {e}"))?;
    send_value_printed(&transactional, &topic, "delayed-open").await?;

    // A tiny record set never reaches 1 MiB, so every fetch parks server-side
    // until fetch.max.wait.ms — the poll loop must still make progress.
    let mut consumer = build_consumer_with(
        bootstrap,
        &format!("txn-cc-delayed-{suffix}"),
        "read_committed",
        &[("fetch.min.bytes", "1048576"), ("fetch.max.wait.ms", "2000")],
    )?;
    consumer
        .assign(vec![tp.clone()])
        .await
        .map_err(|e| format!("delayed: assign: {e}"))?;

    let started = Instant::now();
    let mut while_open: Vec<String> = Vec::new();
    while started.elapsed() < Duration::from_secs(6) {
        let records = consumer
            .poll(Duration::from_millis(500))
            .await
            .map_err(|e| format!("delayed poll: {e}"))?;
        for record in records {
            while_open.push(String::from_utf8_lossy(record.value().map_or(&[][..], Vec::as_slice)).into_owned());
        }
    }
    let mut ok = report(
        while_open == ["delayed-seed"],
        "the parked fetch is bounded by fetch.max.wait.ms and delivers only pre-transaction data",
        format!("while the transaction was open: {while_open:?}"),
    );

    // Abort while the next fetch is (very likely) parked in purgatory.
    transactional
        .abort_transaction()
        .await
        .map_err(|e| format!("delayed: abort: {e}"))?;
    println!("  abort_transaction ....... ok — the marker completes any parked fetch");
    send_value_printed(&plain, &topic, "delayed-after").await?;

    let after = drain_until_idle(&mut consumer, true).await?;
    ok &= report(
        after == ["delayed-after"],
        "the aborted record never surfaces; the next plain record does",
        format!("after the abort: {after:?}"),
    );

    consumer.close().await.map_err(|e| format!("delayed: close: {e}"))?;
    close_producer(&transactional).await?;
    close_producer(&plain).await?;
    Ok(ok)
}

/// Case 5: committed and aborted batches under every codec.
async fn compression_case(bootstrap: &str, suffix: &str) -> Result<bool, String> {
    println!();
    println!("--- case 5: compression × transactions ---");
    let mut ok = true;
    for codec in ["gzip", "snappy", "lz4", "zstd"] {
        let topic = format!("txn-cc-comp-{codec}-{suffix}");
        let producer = transactional_producer_with(
            bootstrap,
            &format!("txn-manual-cc-comp-{codec}-{suffix}"),
            &[("compression.type", codec)],
        )?;
        producer.init_transactions().await.map_err(|e| format!("{codec}: init: {e}"))?;
        producer.begin_transaction().map_err(|e| format!("{codec}: begin #1: {e}"))?;
        for value in [format!("{codec}-c1"), format!("{codec}-c2")] {
            send_value(&producer, &topic, &value)
                .await
                .map_err(|e| format!("{codec}: send: {e}"))?;
        }
        producer
            .commit_transaction()
            .await
            .map_err(|e| format!("{codec}: commit: {e}"))?;
        producer.begin_transaction().map_err(|e| format!("{codec}: begin #2: {e}"))?;
        send_value(&producer, &topic, &format!("{codec}-a1"))
            .await
            .map_err(|e| format!("{codec}: send: {e}"))?;
        producer.abort_transaction().await.map_err(|e| format!("{codec}: abort: {e}"))?;
        close_producer(&producer).await?;

        let committed = read_partition_idle(bootstrap, &topic, "read_committed", false, IDLE).await?;
        ok &= report(
            committed == [format!("{codec}-c1"), format!("{codec}-c2")],
            &format!("{codec}: committed batch decodes, aborted batch is filtered"),
            format!("read_committed: {committed:?}"),
        );
    }
    Ok(ok)
}
