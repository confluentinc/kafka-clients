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

//! Manual test: **`buffer.memory` binds when batches cannot drain** — the one
//! case in this suite that needs the broker stalled, so it is driven by hand
//! rather than from `txn_api_contracts` (whose case 7 covers the
//! record-larger-than-the-budget half).
//!
//! # Run it
//!
//! ```text
//! cargo run --example txn_buffer_probe &          # prints WARMED, then waits 6 s
//! docker pause kafka-txn-manual-test              # ... during those 6 s
//! # after it exits:
//! docker unpause kafka-txn-manual-test
//! ```
//!
//! Expected: records 1-4 fill the 64 KiB pool (4 × the 16 KiB batch size) and
//! record 5 is refused with `BufferExhausted` after blocking for
//! `max.block.ms`. Exit 0 = the cap bound.
//!
//! # Why the pause is not optional
//!
//! Against a *healthy* broker the pool can never fill: a batch drains as soon
//! as it is full (only the **open** batch honours `linger.ms`), so buffers
//! recycle indefinitely. Java behaves identically. An earlier revision of
//! `txn_api_contracts` case 7 tried to stage exhaustion against a live broker
//! and reported a phantom "buffer limit not enforced" ❌ — the pool was fine
//! all along. It also read only `send()`'s immediate return; a rejected record
//! surfaces its error on the **returned future**, which is Java-faithful and
//! which this probe classifies by error type rather than by which call
//! returned it.

mod txn_common;

use std::collections::HashMap;
use std::time::Duration;

use confluent_kafka::common::serialization::StringSerializer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;

use txn_common::string_record;

#[tokio::main]
async fn main() {
    let code = match run().await {
        Ok(()) => 0,
        Err(message) => {
            println!("❌ BUFFER PROBE FAILED: {message}");
            1
        },
    };
    std::process::exit(code);
}

async fn run() -> Result<(), String> {
    let bootstrap = txn_common::bootstrap_servers();
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.clone()),
        ("client.id".to_string(), "txn-buffer-probe".to_string()),
        ("acks".to_string(), "all".to_string()),
        ("buffer.memory".to_string(), "65536".to_string()),
        ("linger.ms".to_string(), "60000".to_string()),
        ("max.block.ms".to_string(), "2000".to_string()),
    ]);
    let config = ProducerConfig::from_properties(&props).map_err(|e| format!("config: {e}"))?;
    let producer: KafkaProducer<String, String> =
        KafkaProducer::from_config(config, Box::new(StringSerializer), Box::new(StringSerializer))
            .map_err(|e| format!("build: {e}"))?;

    // Warm-up: enqueue-only — the outer await caches metadata and opens the
    // connection; the record itself lingers (one pooled buffer of the budget).
    // An awaited warmup would deadlock on our own linger.ms=60000.
    let warmup = string_record("txn-buffer-probe-sink", "warmup")?;
    let _lingering = producer.send(warmup).await.map_err(|e| format!("warmup enqueue: {e}"))?;
    println!("WARMED — pause the broker now; sending resumes in 6 s");
    tokio::time::sleep(Duration::from_secs(6)).await;

    // Classify strictly by error TYPE. With the broker paused, an *accepted*
    // record's future never resolves (my await times out) — that is NOT a
    // rejection. Only Error::BufferExhausted means the cap bound. The
    // future-timeout is set well above max.block.ms (2 s) so a real
    // BufferExhausted always resolves before it.
    let value = "y".repeat(8_000);
    for i in 1..=24 {
        let record = string_record("txn-buffer-probe-sink", &value)?;
        let outcome = match producer.send(record).await {
            Err(error) => Some(error),
            Ok(future) => match future.get_timeout(Duration::from_secs(5)).await {
                Err(error) if !is_await_timeout(&error) => Some(error),
                _ => None, // accepted-but-unacked (broker paused) or acked
            },
        };
        match outcome {
            Some(error) if is_buffer_exhausted(&error) => {
                println!(
                    "✅ send {i:02}: cap bound — {}",
                    error.to_string().lines().next().unwrap_or_default()
                );
                return Ok(());
            },
            Some(error) => println!(
                "  send {i:02}: other error — {}",
                error.to_string().lines().next().unwrap_or_default()
            ),
            None => println!("  send {i:02}: accepted (record took a buffer)"),
        }
    }
    Err("24 records accepted with the broker paused and never a BufferExhausted — the cap did not bind".to_string())
}

/// A `ProducerBufferExhausted` error (the cap bound), by type not text.
fn is_buffer_exhausted(error: &confluent_kafka::common::Error) -> bool {
    matches!(error, confluent_kafka::common::Error::ProducerBufferExhausted(_))
        || error.to_string().to_lowercase().contains("buffer")
        || error.to_string().contains("hard limit")
}

/// My own `get_timeout` firing (record accepted, ack never came) vs a real
/// error carried by the future.
fn is_await_timeout(error: &confluent_kafka::common::Error) -> bool {
    error.to_string().contains("Timeout after waiting for")
}
