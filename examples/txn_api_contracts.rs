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

//! Manual test: **producer API contracts** (wave-3 🟢 batch, part 1 of 4).
//! Self-contained — writes to run-unique topics and verifies them itself.
//!
//! Cases, each mapped to its source:
//!
//! 1. Double `init_transactions` errors (Apache `testConsecutivelyRunInitTransactions`).
//! 2. Empty transactions leave no trace: empty commit, empty abort, and an
//!    empty abort *after* a data commit (KIP-890 pt 2 / `testEmptyAbortAfterCommit`).
//! 3. The javadoc's canonical recovery loop: an abortable error → abort →
//!    retry the same batch → commit; exactly one copy lands (javadoc example
//!    plus `testBumpTransactionalEpochWithTV2Enabled`). Also asserts the
//!    error is a `KafkaException`, which the javadoc says to answer by aborting.
//! 4. Commit surfaces an *unawaited* send failure — no `.get()`/callback
//!    needed, per the javadoc's "exceptions to communicate error states".
//! 5. Abort resolves pending sends: with `linger.ms=5000` nothing drains, so
//!    an immediate abort must fail every in-flight future with
//!    `TransactionAborted` — none may hang (KIP-654).
//! 6. `close()` abandons an open transaction (commits nothing); a successor's
//!    `init_transactions` recovers the id (producer javadoc).
//! 7. A record larger than the whole `buffer.memory` budget is refused with
//!    an error naming that config (javadoc `buffer.memory` contract). The
//!    pool-*exhaustion* half needs a stalled broker and lives in
//!    `txn_buffer_probe`.
//! 8. KIP-939 2PC probes: `transaction.two.phase.commit.enable=true` with an
//!    explicit `transaction.timeout.ms` is rejected at build; 2PC alone
//!    against a broker with 2PC disabled is *observed* — Java refuses to
//!    serialize the non-ignorable `Enable2Pc` field below InitProducerId v6,
//!    so anything but a clean error is a divergence worth reporting.
//! 9. A connect timeout reports `is_retriable_error()` (Java `RetriableException`).
//!
//! Broker setup: see `examples/README.md`. Exit code 0 = every check ✅.

mod txn_common;

use std::collections::HashMap;
use std::time::Duration;

use confluent_kafka::common::Error;
use confluent_kafka::common::serialization::StringSerializer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;

use txn_common::StringProducer;
use txn_common::read_partition_idle;
use txn_common::report;
use txn_common::send_expect_failure;
use txn_common::send_value_printed;
use txn_common::string_record;
use txn_common::transactional_producer;
use txn_common::transactional_producer_with;

/// Shorter drain for the single-topic verifications here.
const IDLE: Duration = Duration::from_secs(3);

#[tokio::main]
async fn main() {
    let code = match run().await {
        Ok(()) => 0,
        Err(message) => {
            println!();
            println!("❌ API-CONTRACT TEST FAILED: {message}");
            1
        },
    };
    // Hard exit: case 7 deliberately leaves a producer with undrained,
    // never-to-be-sent records behind; runtime teardown must not hang on it.
    std::process::exit(code);
}

async fn run() -> Result<(), String> {
    // Diagnostics: RUST_LOG=... enables the client's own logging.
    let _ = env_logger::try_init();
    let bootstrap = txn_common::bootstrap_servers();
    let suffix = txn_common::unique_suffix();
    println!("=== producer API contracts — manual test ===");
    println!("bootstrap.servers : {bootstrap}");

    let mut all_ok = true;
    all_ok &= double_init_case(&bootstrap, &suffix).await?;
    all_ok &= empty_transactions_case(&bootstrap, &suffix).await?;
    all_ok &= recovery_loop_case(&bootstrap, &suffix).await?;
    all_ok &= unawaited_failure_case(&bootstrap, &suffix).await?;
    all_ok &= abort_pending_case(&bootstrap, &suffix).await?;
    all_ok &= close_abandons_case(&bootstrap, &suffix).await?;
    all_ok &= buffer_limit_case(&bootstrap).await?;
    all_ok &= two_phase_commit_case(&bootstrap, &suffix).await?;
    all_ok &= retriable_flag_case().await;

    if !all_ok {
        return Err("one or more API contracts were violated — see the ❌ lines above".to_string());
    }
    println!();
    println!("✅ API CONTRACTS HOLD: misuse and failures surface per the Java contract, and");
    println!("   every pending future is resolved rather than abandoned.");
    Ok(())
}

/// Case 1: a second `init_transactions` on the same producer must fail.
async fn double_init_case(bootstrap: &str, suffix: &str) -> Result<bool, String> {
    println!();
    println!("--- case 1: init_transactions twice ---");
    let producer = transactional_producer(bootstrap, &format!("txn-manual-api-init2-{suffix}"))?;
    producer
        .init_transactions()
        .await
        .map_err(|e| format!("first init_transactions: {e}"))?;
    let second = producer.init_transactions().await;
    let ok = report(
        second.is_err(),
        "the second init_transactions is refused",
        second
            .err()
            .map_or_else(|| "it unexpectedly succeeded".to_string(), |e| first_line(&e.to_string())),
    );
    txn_common::close_producer(&producer).await?;
    Ok(ok)
}

/// Case 2: empty commit, empty abort, and an empty abort after a data commit
/// all succeed and leave nothing on the topic but the data transaction.
async fn empty_transactions_case(bootstrap: &str, suffix: &str) -> Result<bool, String> {
    println!();
    println!("--- case 2: empty transactions ---");
    let topic = format!("txn-api-empty-{suffix}");
    let producer = transactional_producer(bootstrap, &format!("txn-manual-api-empty-{suffix}"))?;
    producer.init_transactions().await.map_err(|e| format!("empty: init: {e}"))?;

    producer.begin_transaction().map_err(|e| format!("empty: begin #1: {e}"))?;
    producer
        .commit_transaction()
        .await
        .map_err(|e| format!("empty commit (zero records): {e}"))?;
    println!("  empty commit ............ ok");
    producer.begin_transaction().map_err(|e| format!("empty: begin #2: {e}"))?;
    producer
        .abort_transaction()
        .await
        .map_err(|e| format!("empty abort (zero records): {e}"))?;
    println!("  empty abort ............. ok");

    producer.begin_transaction().map_err(|e| format!("empty: begin #3: {e}"))?;
    send_value_printed(&producer, &topic, "empty-case-data-1").await?;
    producer.commit_transaction().await.map_err(|e| format!("data commit: {e}"))?;

    // Apache's testEmptyAbortAfterCommit: an empty abort directly after a
    // committed data transaction.
    producer.begin_transaction().map_err(|e| format!("empty: begin #4: {e}"))?;
    producer
        .abort_transaction()
        .await
        .map_err(|e| format!("empty abort after a data commit: {e}"))?;
    println!("  empty abort after a data commit ... ok");
    txn_common::close_producer(&producer).await?;

    let committed = read_partition_idle(bootstrap, &topic, "read_committed", true, IDLE).await?;
    Ok(report(
        committed == ["empty-case-data-1"],
        "empty transactions left no trace",
        format!("read_committed sees exactly the data transaction: {committed:?}"),
    ))
}

/// Case 3: the canonical javadoc loop — abortable error, abort, retry, commit.
async fn recovery_loop_case(bootstrap: &str, suffix: &str) -> Result<bool, String> {
    println!();
    println!("--- case 3: the documented recovery loop (abort → retry) ---");
    let topic = format!("txn-api-recovery-{suffix}");
    let producer = transactional_producer_with(
        bootstrap,
        &format!("txn-manual-api-recovery-{suffix}"),
        &[("max.request.size", "5242880")],
    )?;
    producer.init_transactions().await.map_err(|e| format!("recovery: init: {e}"))?;

    // Attempt 1: two good records, then a broker-rejected oversized one.
    producer.begin_transaction().map_err(|e| format!("recovery: begin #1: {e}"))?;
    send_value_printed(&producer, &topic, "loop-1").await?;
    send_value_printed(&producer, &topic, "loop-2").await?;
    let giant = "x".repeat(1_500_000);
    let mut ok = true;
    let mut send_flag = false;
    let mut send_detail = String::new();
    match send_expect_failure(&producer, &topic, &giant).await {
        Ok(failure) => {
            let error = failure.error();
            send_flag = error.is_kafka_error();
            send_detail = format!("send error {:?}: is_kafka_error={send_flag}", error.error());
        },
        Err(unexpected) => ok &= report(false, "the oversized record was rejected", unexpected),
    }
    let commit = producer.commit_transaction().await;
    let (commit_failed, commit_flag, commit_detail) = match commit {
        Err(error) => (
            true,
            error.is_kafka_error(),
            format!(
                "commit error: {} (is_kafka_error={})",
                first_line(error.message()),
                error.is_kafka_error()
            ),
        ),
        Ok(()) => (false, false, "commit unexpectedly succeeded".to_string()),
    };
    ok &= report(commit_failed, "commit of the failed attempt is refused", commit_detail.clone());
    // Java gives the application no per-error "requires abort" flag: the producer
    // records the ABORTABLE_ERROR state internally and `commitTransaction()` throws
    // `KafkaException`, whose javadoc says to answer by aborting (done just below).
    ok &= report(
        send_flag || commit_flag,
        // Java surfaces a bare `KafkaException` here.
        "the surfaced error is a bare Kafka error, so the javadoc's answer is abort",
        format!("{send_detail}; {commit_detail}"),
    );
    // The javadoc's `catch (KafkaException e)` branch: abort and try again.
    producer
        .abort_transaction()
        .await
        .map_err(|e| format!("recovery: abort: {e}"))?;
    println!("  abort_transaction ....... ok — taking the documented retry branch");

    // Attempt 2: the same batch, minus the poison pill.
    producer.begin_transaction().map_err(|e| format!("recovery: begin #2: {e}"))?;
    send_value_printed(&producer, &topic, "loop-1").await?;
    send_value_printed(&producer, &topic, "loop-2").await?;
    producer
        .commit_transaction()
        .await
        .map_err(|e| format!("recovery: retry commit: {e}"))?;
    println!("  retry commit ............ ok");
    txn_common::close_producer(&producer).await?;

    let committed = read_partition_idle(bootstrap, &topic, "read_committed", true, IDLE).await?;
    ok &= report(
        committed == ["loop-1", "loop-2"],
        "exactly one copy of the batch is visible after the retry",
        format!("read_committed: {committed:?} (the aborted attempt's copies stay invisible)"),
    );
    Ok(ok)
}

/// Case 4: a failed send that was never awaited must still fail the commit.
async fn unawaited_failure_case(bootstrap: &str, suffix: &str) -> Result<bool, String> {
    println!();
    println!("--- case 4: commit surfaces an unawaited send failure ---");
    let topic = format!("txn-api-unawaited-{suffix}");
    let producer = transactional_producer_with(
        bootstrap,
        &format!("txn-manual-api-unawaited-{suffix}"),
        &[("max.request.size", "5242880")],
    )?;
    producer
        .init_transactions()
        .await
        .map_err(|e| format!("unawaited: init: {e}"))?;
    producer.begin_transaction().map_err(|e| format!("unawaited: begin: {e}"))?;

    // Three sends, zero awaits — the middle one is doomed at the broker.
    let giant = "x".repeat(1_500_000);
    for value in ["unawaited-1", giant.as_str(), "unawaited-2"] {
        let record = string_record(&topic, value)?;
        let _unawaited = producer.send(record).await.map_err(|e| format!("unawaited enqueue: {e}"))?;
    }
    println!("  three sends enqueued (one oversized), no ack awaited — committing");
    let commit_started = std::time::Instant::now();
    let commit = producer.commit_transaction().await;
    let commit_elapsed = commit_started.elapsed();
    let mut ok = report(
        commit.is_err(),
        "commit itself surfaces the send failure (no .get() required)",
        match &commit {
            Err(e) => first_line(&e.to_string()),
            Ok(()) => "it unexpectedly succeeded".to_string(),
        },
    );
    // The javadoc: "if any of the send calls which were part of the
    // transaction hit irrecoverable errors, this method will throw the last
    // received exception immediately" — immediately, not after max.block.ms.
    ok &= report(
        commit.is_err() && commit_elapsed < Duration::from_secs(10),
        "the commit failed fast with the send's error (javadoc: \"immediately\")",
        format!("commit resolved after {} ms", commit_elapsed.as_millis()),
    );
    // The javadoc's recovery branch: abort must now be possible.
    match producer.abort_transaction().await {
        Ok(()) => {
            ok &= report(
                true,
                "abort_transaction is accepted afterwards",
                "the documented recovery branch works".to_string(),
            )
        },
        Err(error) => {
            ok &= report(
                false,
                "abort_transaction is accepted afterwards",
                format!(
                    "refused: {} — the application is wedged between a commit it may not retry usefully and an abort it is denied",
                    first_line(&error.to_string())
                ),
            );
        },
    }
    drop(producer);

    let committed = read_partition_idle(bootstrap, &topic, "read_committed", true, IDLE).await?;
    ok &= report(
        committed.is_empty(),
        "nothing from the failed transaction is visible",
        format!("read_committed: {committed:?}"),
    );
    Ok(ok)
}

/// Case 5: abort with a buffer full of undrained sends resolves every future.
async fn abort_pending_case(bootstrap: &str, suffix: &str) -> Result<bool, String> {
    println!();
    println!("--- case 5: abort resolves pending sends (KIP-654) ---");
    let topic = format!("txn-api-abortpending-{suffix}");
    // linger.ms=5000 keeps every batch undrained until the abort.
    let producer = transactional_producer_with(
        bootstrap,
        &format!("txn-manual-api-abortpending-{suffix}"),
        &[("linger.ms", "5000")],
    )?;
    producer
        .init_transactions()
        .await
        .map_err(|e| format!("abort-pending: init: {e}"))?;
    producer.begin_transaction().map_err(|e| format!("abort-pending: begin: {e}"))?;

    const PENDING: usize = 400;
    let mut futures = Vec::with_capacity(PENDING);
    for i in 0..PENDING {
        let record = string_record(&topic, &format!("pending-{i:03}"))?;
        futures.push(
            producer
                .send(record)
                .await
                .map_err(|e| format!("abort-pending enqueue {i}: {e}"))?,
        );
    }
    println!("  {PENDING} sends buffered behind linger.ms=5000 — aborting immediately");
    producer
        .abort_transaction()
        .await
        .map_err(|e| format!("abort with pending sends: {e}"))?;

    let mut aborted = 0usize;
    let mut acked = 0usize;
    let mut other_count = 0usize;
    let mut first_other: Option<Error> = None;
    let mut unresolved = 0usize;
    for future in &futures {
        match future.get_timeout(Duration::from_secs(10)).await {
            Ok(_) => acked += 1,
            Err(Error::TransactionAborted(_)) => aborted += 1,
            // get_timeout wraps an unresolved future in its own timeout error;
            // a resolved-but-failed future keeps its original error above.
            Err(e) if e.to_string().contains("Timeout expired") => unresolved += 1,
            Err(e) => {
                other_count += 1;
                if first_other.is_none() {
                    first_other = Some(e);
                }
            },
        }
    }
    let mut ok = report(
        unresolved == 0,
        "every pending future resolved — none left hanging",
        format!(
            "{aborted} failed with TransactionAborted, {acked} were already acked, {other_count} other error(s){}",
            first_other.map_or_else(String::new, |e| format!(" (first: {})", first_line(&e.to_string())))
        ),
    );
    ok &= report(
        aborted > 0,
        "undrained batches fail with TransactionAborted specifically",
        format!("{aborted} of {PENDING}"),
    );

    // The producer stays usable: commit forces the flush past the linger.
    producer
        .begin_transaction()
        .map_err(|e| format!("abort-pending: begin #2: {e}"))?;
    let record = string_record(&topic, "post-abort-1")?;
    let _unawaited = producer.send(record).await.map_err(|e| format!("post-abort send: {e}"))?;
    producer
        .commit_transaction()
        .await
        .map_err(|e| format!("post-abort commit: {e}"))?;
    txn_common::close_producer(&producer).await?;

    let committed = read_partition_idle(bootstrap, &topic, "read_committed", true, IDLE).await?;
    ok &= report(
        committed == ["post-abort-1"],
        "only the post-abort transaction is visible",
        format!("read_committed: {committed:?}"),
    );
    Ok(ok)
}

/// Case 6: `close()` with an open transaction abandons it — never commits it.
async fn close_abandons_case(bootstrap: &str, suffix: &str) -> Result<bool, String> {
    println!();
    println!("--- case 6: close() abandons an open transaction ---");
    let topic = format!("txn-api-close-{suffix}");
    let txn_id = format!("txn-manual-api-close-{suffix}");

    let first = transactional_producer(bootstrap, &txn_id)?;
    first.init_transactions().await.map_err(|e| format!("close: init #1: {e}"))?;
    first.begin_transaction().map_err(|e| format!("close: begin: {e}"))?;
    send_value_printed(&first, &topic, "abandoned-by-close-1").await?;
    first
        .close()
        .await
        .map_err(|e| format!("close with an open transaction: {e}"))?;
    println!("  close() returned Ok with the transaction still open");

    let successor = transactional_producer(bootstrap, &txn_id)?;
    successor
        .init_transactions()
        .await
        .map_err(|e| format!("close: successor init: {e}"))?;
    successor
        .begin_transaction()
        .map_err(|e| format!("close: successor begin: {e}"))?;
    send_value_printed(&successor, &topic, "after-close-1").await?;
    successor
        .commit_transaction()
        .await
        .map_err(|e| format!("close: successor commit: {e}"))?;
    txn_common::close_producer(&successor).await?;

    let committed = read_partition_idle(bootstrap, &topic, "read_committed", true, IDLE).await?;
    let full = read_partition_idle(bootstrap, &topic, "read_uncommitted", true, IDLE).await?;
    let mut ok = report(
        committed == ["after-close-1"],
        "the closed producer's records were never committed",
        format!("read_committed: {committed:?}"),
    );
    ok &= report(
        full == ["abandoned-by-close-1", "after-close-1"],
        "they were written and then aborted by the successor's init (control)",
        format!("read_uncommitted: {full:?}"),
    );
    Ok(ok)
}

/// Case 7: a full buffer fails the send within `max.block.ms`, cleanly.
async fn buffer_limit_case(bootstrap: &str) -> Result<bool, String> {
    println!();
    println!("--- case 7: a record larger than buffer.memory is rejected ---");
    // Plain producer: buffer accounting is orthogonal to transactions.
    // max.request.size is raised above buffer.memory so the *buffer* limit is
    // the one that fires, not the request-size limit checked before it
    // (`KafkaProducer::ensure_valid_record_size`).
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), "txn-manual-api-buffer".to_string()),
        ("acks".to_string(), "all".to_string()),
        ("buffer.memory".to_string(), "65536".to_string()),
        ("max.request.size".to_string(), "5242880".to_string()),
        ("max.block.ms".to_string(), "10000".to_string()),
    ]);
    let config = ProducerConfig::from_properties(&props).map_err(|e| format!("buffer config: {e}"))?;
    let producer: StringProducer =
        KafkaProducer::from_config(config, Box::new(StringSerializer), Box::new(StringSerializer))
            .map_err(|e| format!("building the buffer producer: {e}"))?;

    // 100 KB into a 64 KiB budget: unsatisfiable no matter how long we wait, so
    // Java rejects it outright rather than blocking (`BufferPool.allocate`'s
    // "hard limit" guard has the same shape).
    let oversized = "y".repeat(100_000);
    let outcome = send_expect_failure(&producer, "txn-api-buffer-sink", &oversized).await;
    let ok = match outcome {
        Ok(failure) => {
            let message = first_line(&failure.error().to_string());
            report(
                message.contains("buffer.memory"),
                "a record larger than buffer.memory is refused, naming the config",
                message,
            )
        },
        Err(unexpected) => report(false, "a record larger than buffer.memory is refused", unexpected),
    };
    // NOT tested here: the *exhaustion* path — pool full, allocate blocks for
    // max.block.ms, then BufferExhausted. It cannot be staged against a healthy
    // broker at all: full batches drain as fast as they fill (only the open
    // batch honours linger.ms), so the pool recycles and never fills. An earlier
    // revision tried exactly that and reported a phantom "buffer limit not
    // enforced" ❌ for days. `txn_buffer_probe` stages it correctly by pausing
    // the broker, and shows the cap binding on the 5th record.
    println!("  (the pool-exhaustion path needs a stalled broker — see `cargo run --example txn_buffer_probe`)");
    Ok(ok)
}

/// Case 8: KIP-939 probes.
async fn two_phase_commit_case(bootstrap: &str, suffix: &str) -> Result<bool, String> {
    println!();
    println!("--- case 8: KIP-939 two-phase-commit probes ---");
    // (a) 2PC + explicit transaction.timeout.ms is rejected at build time.
    // transactional_producer_with always sets transaction.timeout.ms, so the
    // clash is intrinsic here.
    let clash = transactional_producer_with(
        bootstrap,
        &format!("txn-manual-api-2pc-clash-{suffix}"),
        &[("transaction.two.phase.commit.enable", "true")],
    );
    let mut ok = report(
        clash.is_err(),
        "2PC with an explicit transaction.timeout.ms is rejected at build",
        clash.err().unwrap_or_else(|| "it unexpectedly succeeded".to_string()),
    );

    // (b) 2PC alone against a broker with 2PC disabled: observe. Java refuses
    // to serialize the non-ignorable Enable2Pc field below InitProducerId v6,
    // so a silent success here means the flag was dropped on the wire.
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("transactional.id".to_string(), format!("txn-manual-api-2pc-{suffix}")),
        ("client.id".to_string(), "txn-manual-api-2pc".to_string()),
        ("acks".to_string(), "all".to_string()),
        ("max.block.ms".to_string(), "10000".to_string()),
        ("transaction.two.phase.commit.enable".to_string(), "true".to_string()),
    ]);
    let config = ProducerConfig::from_properties(&props).map_err(|e| format!("2pc config: {e}"))?;
    let producer: StringProducer =
        KafkaProducer::from_config(config, Box::new(StringSerializer), Box::new(StringSerializer))
            .map_err(|e| format!("building the 2pc producer: {e}"))?;
    match tokio::time::timeout(Duration::from_secs(15), producer.init_transactions()).await {
        Ok(Err(error)) => {
            ok &= report(
                true,
                "2PC init_transactions against a non-2PC broker fails cleanly",
                format!("{:?}: {}", error.error(), first_line(&error.to_string())),
            );
        },
        Ok(Ok(())) => {
            ok &= report(
                false,
                "2PC init_transactions against a non-2PC broker fails cleanly",
                "it SUCCEEDED — the Enable2Pc flag was silently dropped on the wire (Java would \
                 refuse to serialize it below InitProducerId v6); likely the §9.1 generator gap"
                    .to_string(),
            );
        },
        Err(_) => {
            ok &= report(
                false,
                "2PC init_transactions against a non-2PC broker fails cleanly",
                "still blocked after 15 s".to_string(),
            );
        },
    }
    drop(producer);
    Ok(ok)
}

/// Case 9: a connect timeout is a retriable error.
async fn retriable_flag_case() -> bool {
    println!();
    println!("--- case 9: is_retriable_error() on a connect timeout ---");
    let producer =
        match transactional_producer_with("localhost:1", "txn-manual-api-retriable", &[("max.block.ms", "2500")]) {
            Ok(producer) => producer,
            Err(e) => return report(false, "building the unreachable producer", e),
        };
    match tokio::time::timeout(Duration::from_secs(10), producer.init_transactions()).await {
        Ok(Err(error)) => report(
            error.is_retriable_error(),
            "the timeout error reports is_retriable_error()",
            format!(
                "{} (retriable={} fatal={})",
                first_line(error.message()),
                error.is_retriable_error(),
                // Java's `RequestUtils.isFatalException` static — fatality is a
                // classification over an error, not a flag carried by it.
                confluent_kafka::common::requests::request_utils::is_fatal_error(&error)
            ),
        ),
        Ok(Ok(())) => report(
            false,
            "init_transactions against localhost:1",
            "unexpectedly succeeded".to_string(),
        ),
        Err(_) => report(false, "init_transactions against localhost:1", "hung past 10 s".to_string()),
    }
}

fn first_line(text: &str) -> String {
    text.lines().next().unwrap_or(text).to_string()
}
