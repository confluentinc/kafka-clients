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

//! Manual test, write side: **things that must FAIL, failing correctly**.
//! Verify the log side with `cargo run --example txn_errors_consumer`.
//!
//! Every case here expects an error; a case turns ❌ when the operation
//! *succeeds* (or hangs). Six cases, each with its own producer:
//!
//! 1. **Config validation** (no broker involved) — `transactional.id` with
//!    `acks=1`, and `transactional.id` with `enable.idempotence=false`, must
//!    both be rejected when the config is built.
//! 2. **API misuse** — transactional calls in the wrong order (begin before
//!    init, send outside a transaction, commit without begin, double begin)
//!    must fail with clear errors and leave the topic untouched.
//! 3. **Timeout above the broker ceiling** — `transaction.timeout.ms` of
//!    20 min exceeds the broker's `transaction.max.timeout.ms` (default
//!    15 min); `init_transactions` must fail fatally, reporting the broker's
//!    `INVALID_TRANSACTION_TIMEOUT` text. Java does *not* surface
//!    `InvalidTxnTimeoutException` for this: the code matches none of
//!    `InitProducerIdHandler.handleResponse`'s arms and falls through to
//!    `fatalError(new KafkaException("Unexpected error in InitProducerIdResponse; " +
//!    error.message()))` (`TransactionManager.java:1535-1536`) — a *bare*
//!    `KafkaException`, which carries no wire code.
//! 4. **Server-side transaction timeout** — a transaction opened with a 5 s
//!    timeout and then left idle is aborted by the coordinator; the late
//!    commit must fail, and the records stay invisible forever (consumer
//!    file). This is what stops a hung application from blocking a topic.
//! 5. **Poison pill** — one oversized record (rejected by the broker with
//!    `MESSAGE_TOO_LARGE`) inside an otherwise fine transaction must make the
//!    transaction abort-only: `commit_transaction` refuses,
//!    `abort_transaction` works.
//! 6. **Unreachable broker** — with nothing listening on the bootstrap
//!    address, `init_transactions` must fail within `max.block.ms`, not hang.
//!
//! Case 4 sleeps ~25 s (5 s timeout + the coordinator's 10 s abort sweep +
//! slack), so the whole file takes about half a minute.
//!
//! Broker setup and env vars: see `examples/README.md`.

mod txn_common;

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use confluent_kafka::common::protocol::Errors;
use txn_common::report;
use txn_common::send_expect_failure;
use txn_common::send_value_printed;
use txn_common::transactional_producer;
use txn_common::transactional_producer_with;

/// Topic the API-misuse case aims at; nothing may ever land on it.
const MISUSE_TOPIC: &str = "txn-errors-misuse";
/// Topic of the timed-out transaction; visible only to read_uncommitted.
const TIMEDOUT_TOPIC: &str = "txn-errors-timedout";
/// Topic of the poisoned transaction; visible only to read_uncommitted.
const POISON_TOPIC: &str = "txn-errors-poison";

#[tokio::main]
async fn main() {
    let code = match run().await {
        Ok(()) => 0,
        Err(message) => {
            println!();
            println!("❌ ERROR-HANDLING TEST FAILED: {message}");
            1
        },
    };
    // Hard exit, skipping destructors: if case 2's deadlock probe wedged a
    // task inside `poll`, dropping the tokio runtime would block forever
    // waiting for that worker thread. Everything is printed and flushed.
    std::process::exit(code);
}

async fn run() -> Result<(), String> {
    let bootstrap = txn_common::bootstrap_servers();
    println!("=== transaction error handling — write side ===");
    println!("bootstrap.servers : {bootstrap}");

    let mut all_ok = true;
    all_ok &= config_validation_case(&bootstrap);
    all_ok &= misuse_case(&bootstrap).await?;
    all_ok &= ceiling_case(&bootstrap).await?;
    all_ok &= timeout_case(&bootstrap).await?;
    all_ok &= poison_case(&bootstrap).await?;
    all_ok &= unreachable_case().await;

    if !all_ok {
        return Err("an operation that must fail succeeded (or failed wrongly) — see the ❌ lines above".to_string());
    }
    println!();
    println!("✅ ERRORS SURFACE CORRECTLY: bad configs are rejected, misuse fails fast, timed-out");
    println!("   and poisoned transactions are refused at commit and abortable.");
    println!("   Verify their records stay invisible with: cargo run --example txn_errors_consumer");
    Ok(())
}

/// Case 1: invalid config combinations must be rejected at build time.
fn config_validation_case(bootstrap: &str) -> bool {
    println!();
    println!("--- case 1: config validation ---");
    let acks_one = transactional_producer_with(bootstrap, "txn-manual-errors-acks", &[("acks", "1")]);
    let idempotence_off =
        transactional_producer_with(bootstrap, "txn-manual-errors-idem", &[("enable.idempotence", "false")]);
    let mut ok = true;
    ok &= expect_error("transactional.id with acks=1 is rejected", acks_one.err());
    ok &= expect_error(
        "transactional.id with enable.idempotence=false is rejected",
        idempotence_off.err(),
    );
    ok
}

/// Case 2: transactional calls in the wrong order fail fast and recoverably.
async fn misuse_case(bootstrap: &str) -> Result<bool, String> {
    println!();
    println!("--- case 2: API misuse ---");
    let producer = Arc::new(transactional_producer(bootstrap, "txn-manual-errors-misuse")?);
    let mut ok = true;

    ok &= expect_error(
        "begin_transaction before init_transactions",
        producer.begin_transaction().err().map(|e| e.to_string()),
    );
    // The misused send runs on its own task with a join timeout. Java fails it
    // synchronously with IllegalStateException and this client does the same, in
    // microseconds — the guard is not for slowness. It is for the failure mode
    // this case found once already: a send that re-acquires a lock it is holding
    // wedges its task inside a single `poll`, and a task blocked in
    // `std::sync::Mutex::lock` never yields, so no `tokio::time::timeout` around
    // the send itself could ever fire. Only a second task can observe it.
    let sender = Arc::clone(&producer);
    let guarded_send = tokio::spawn(async move { send_expect_failure(&sender, MISUSE_TOPIC, "misuse-never-1").await });
    match tokio::time::timeout(Duration::from_secs(20), guarded_send).await {
        // `expect_synchronous`: Java throws `IllegalStateException` here, which is
        // not even a `KafkaException`, so it reaches `catch (Exception e)` and is
        // rethrown out of `send()` rather than reported through the future
        // (`KafkaProducer.java:1077-1081`). Accepting either path would let a client
        // misfile this into the `ApiException` block and still print ✅ — and that
        // block additionally runs `maybeTransitionToErrorState`, which would poison
        // a producer Java leaves usable. The three later calls in this case prove
        // it stays usable; this line is what pins *how* the error arrived.
        Ok(Ok(Ok(failure))) => match failure.expect_synchronous("the send outside a transaction") {
            Ok(error) => ok &= report(true, "send outside a transaction", format!("{error}")),
            Err(wrong_path) => ok &= report(false, "send outside a transaction", wrong_path),
        },
        Ok(Ok(Err(unexpected))) => ok &= report(false, "send outside a transaction", unexpected),
        Ok(Err(join_error)) => ok &= report(false, "send outside a transaction", format!("panicked: {join_error}")),
        Err(_) => {
            report(
                false,
                "send outside a transaction",
                "DEADLOCK — the send never returned, so the producer has re-acquired a lock \
                 it was already holding. This is a regression: the send path must release the \
                 TransactionManager guard before handling an error, because \
                 maybe_transition_to_error_state re-locks the same non-reentrant mutex. \
                 `sample <pid>` names the two frames; start at do_send_bytes's \
                 maybe_add_partition arm."
                    .to_string(),
            );
            println!("   (skipping the rest of the misuse case — this producer's lock is permanently wedged)");
            // The wedged task keeps an Arc clone forever, so the producer is
            // deliberately leaked rather than dropped/closed here.
            return Ok(false);
        },
    }
    ok &= expect_error(
        "commit_transaction before init_transactions",
        producer.commit_transaction().await.err().map(|e| e.to_string()),
    );

    // These misuse errors are recoverable (they poison nothing): the same
    // producer must now be able to initialize and run normally.
    producer
        .init_transactions()
        .await
        .map_err(|e| format!("misuse: init_transactions after recoverable errors: {e}"))?;
    ok &= expect_error(
        "commit_transaction with no transaction begun",
        producer.commit_transaction().await.err().map(|e| e.to_string()),
    );
    producer.begin_transaction().map_err(|e| format!("misuse: first begin: {e}"))?;
    ok &= expect_error(
        "begin_transaction while one is already in progress",
        producer.begin_transaction().err().map(|e| e.to_string()),
    );
    producer
        .abort_transaction()
        .await
        .map_err(|e| format!("misuse: aborting the (empty) probe transaction: {e}"))?;
    println!("  (the same producer then initialized, begun and aborted cleanly — misuse errors are recoverable)");
    txn_common::close_producer(&producer).await?;
    Ok(ok)
}

/// Case 3: a transaction timeout above the broker's ceiling is refused.
async fn ceiling_case(bootstrap: &str) -> Result<bool, String> {
    println!();
    println!("--- case 3: transaction.timeout.ms above the broker's 15 min ceiling ---");
    let producer = transactional_producer_with(
        bootstrap,
        "txn-manual-errors-ceiling",
        &[("transaction.timeout.ms", "1200000")], // 20 min
    )?;
    // Assert the shape Java produces, not merely that something failed. Three
    // different outcomes used to print the same ✅ here: today's correct one, an
    // `InvalidTransactionTimeout` (which would be the real divergence), and a plain
    // network timeout that never reached the coordinator. The message prefix is what
    // separates the third from the first two, and the error code separates the second.
    const PREFIX: &str = "Unexpected error in InitProducerIdResponse;";
    let label = "init_transactions is refused with the broker's INVALID_TRANSACTION_TIMEOUT text, \
                 wrapped as a bare Kafka error the way Java does";
    Ok(match producer.init_transactions().await {
        Ok(()) => report(false, label, "it unexpectedly succeeded".to_string()),
        Err(error) => {
            let text = error.to_string();
            // `UnknownServerError` is how this crate spells Java's bare
            // `KafkaException`, which has no wire code of its own.
            let shape_ok = error.error() == Errors::UnknownServerError && text.starts_with(PREFIX);
            report(
                shape_ok,
                label,
                if shape_ok {
                    format!("{:?}: {}", error.error(), first_line(&text))
                } else {
                    format!(
                        "expected UnknownServerError whose message starts {PREFIX:?}, got {:?}: {}",
                        error.error(),
                        first_line(&text)
                    )
                },
            )
        },
    })
}

/// Case 4: the coordinator aborts an idle transaction after its timeout; the
/// late commit must fail.
async fn timeout_case(bootstrap: &str) -> Result<bool, String> {
    println!();
    println!("--- case 4: server-side transaction timeout ---");
    let producer =
        transactional_producer_with(bootstrap, "txn-manual-errors-timeout", &[("transaction.timeout.ms", "5000")])?;
    producer
        .init_transactions()
        .await
        .map_err(|e| format!("timeout: init_transactions: {e}"))?;
    producer.begin_transaction().map_err(|e| format!("timeout: begin: {e}"))?;
    send_value_printed(&producer, TIMEDOUT_TOPIC, "timedout-1").await?;
    println!("  sleeping 25 s — transaction.timeout.ms is 5 s, plus the coordinator's 10 s abort sweep …");
    tokio::time::sleep(Duration::from_secs(25)).await;
    let commit = producer.commit_transaction().await;
    let ok = expect_error(
        "commit_transaction after the coordinator timed the transaction out",
        commit.err().map(|e| format!("{:?}: {e}", e.error())),
    );
    drop(producer);
    Ok(ok)
}

/// Case 5: an oversized record fails its send and must make the whole
/// transaction abort-only.
async fn poison_case(bootstrap: &str) -> Result<bool, String> {
    println!();
    println!("--- case 5: a rejected record poisons its transaction (abort-only) ---");
    // Client cap raised to 5 MB so the oversized record reaches the broker,
    // whose message.max.bytes (default ~1 MB) rejects it — the end-to-end path.
    let producer =
        transactional_producer_with(bootstrap, "txn-manual-errors-poison", &[("max.request.size", "5242880")])?;
    producer
        .init_transactions()
        .await
        .map_err(|e| format!("poison: init_transactions: {e}"))?;
    producer.begin_transaction().map_err(|e| format!("poison: begin: {e}"))?;
    send_value_printed(&producer, POISON_TOPIC, "poison-small-1").await?;

    let mut ok = true;
    let giant = "x".repeat(1_500_000);
    // The error **code** is what pins the end-to-end path, not the surface.
    // `Errors::MessageTooLarge` can only come from a broker response: the client cap
    // is raised to 5 MB above so this record is accepted locally, and a local
    // rejection would be `Error::RecordTooLarge`, which carries no wire code and
    // whose `error()` therefore degrades to `UnknownServerError`. A broker that never
    // answered gives a `Timeout`. So without this assertion, dropping the
    // `max.request.size` override would leave the case green while the record never
    // left the client.
    //
    // `expect_via_future` is kept because it states the surface Java specifies —
    // `RecordTooLargeException` is an `ApiException`, so `catch (ApiException e)`
    // returns a `FutureFailure` rather than throwing (`KafkaProducer.java:1056-1068`)
    // — but it does NOT separate local from remote. A local `ensure_valid_record_size`
    // rejection takes that same route (`kafka_producer.rs`'s `handle_api_error`
    // returns `Ok(failed future)`), in Java as much as here. An earlier version of
    // this comment claimed otherwise; it was wrong.
    let poison_label = "the 1.5 MB record was rejected by the broker";
    match send_expect_failure(&producer, POISON_TOPIC, &giant)
        .await
        .and_then(|failure| failure.expect_via_future("the 1.5 MB record"))
    {
        Ok(error) if error.error() == Errors::MessageTooLarge => {
            ok &= report(
                true,
                poison_label,
                format!("{:?}: {}", error.error(), first_line(&error.to_string())),
            );
        },
        Ok(error) => {
            ok &= report(
                false,
                poison_label,
                format!(
                    "expected MessageTooLarge from a broker response, got {:?}: {} \
                     (UnknownServerError here means max.request.size rejected it locally)",
                    error.error(),
                    first_line(&error.to_string())
                ),
            );
        },
        Err(unexpected) => ok &= report(false, poison_label, unexpected),
    }
    ok &= expect_error(
        "commit_transaction refuses to commit the poisoned transaction",
        producer.commit_transaction().await.err().map(|e| e.to_string()),
    );
    match producer.abort_transaction().await {
        Ok(()) => {
            ok &= report(
                true,
                "abort_transaction still works",
                "the abortable error was cleared".to_string(),
            )
        },
        Err(e) => ok &= report(false, "abort_transaction still works", format!("abort failed: {e}")),
    }
    txn_common::close_producer(&producer).await?;
    Ok(ok)
}

/// Case 6: nothing listens on the bootstrap address — fail within
/// max.block.ms rather than hanging.
async fn unreachable_case() -> bool {
    println!();
    println!("--- case 6: unreachable bootstrap server (max.block.ms=3000) ---");
    let producer = match transactional_producer_with(
        "localhost:1",
        "txn-manual-errors-unreachable",
        &[("max.block.ms", "3000")],
    ) {
        Ok(producer) => producer,
        Err(e) => return report(false, "building the producer for an unreachable broker", e),
    };
    let started = Instant::now();
    match tokio::time::timeout(Duration::from_secs(15), producer.init_transactions()).await {
        Ok(Err(error)) => report(
            started.elapsed() <= Duration::from_secs(10),
            "init_transactions failed promptly",
            format!("after {} ms: {error}", started.elapsed().as_millis()),
        ),
        Ok(Ok(())) => report(
            false,
            "init_transactions failed promptly",
            "it unexpectedly succeeded".to_string(),
        ),
        Err(_) => report(
            false,
            "init_transactions failed promptly",
            "still blocked after 15 s — max.block.ms was not respected".to_string(),
        ),
    }
}

fn expect_error<E: std::fmt::Display>(label: &str, error: Option<E>) -> bool {
    match error {
        Some(e) => report(true, label, first_line(&e.to_string()).to_string()),
        None => report(false, label, "it unexpectedly succeeded".to_string()),
    }
}

/// Errors can be multi-line; the verdict lines stay single-line.
fn first_line(text: &str) -> &str {
    text.lines().next().unwrap_or(text)
}
