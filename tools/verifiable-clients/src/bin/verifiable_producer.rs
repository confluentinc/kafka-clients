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

//! Command-line entry point for the `VerifiableProducer` system-test tool.
//!
//! Translated from `VerifiableProducer.main`. A thin `#[tokio::main] async fn`
//! delegating to the library (precedent: `src/bin/consumer_test.rs`).
//!
//! Run, e.g.:
//!
//! ```sh
//! cargo run -p verifiable-clients --bin verifiable_producer -- \
//!     --topic t --bootstrap-server localhost:9092
//! ```

#![deny(warnings)]

use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use confluent_kafka::common::Error;
use verifiable_clients::verifiable_producer::create_from_args;
use verifiable_clients::{ThroughputThrottler, wait_for_shutdown_signal};

/// Wall-clock milliseconds since the Unix epoch (Java `System.currentTimeMillis`).
fn now_millis() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

fn print_help() {
    // Mirrors the arguments Java's `argParser()` declares (defaults shown).
    println!(
        "usage: verifiable-producer [-h] --topic TOPIC --bootstrap-server \
HOST1:PORT1[,HOST2:PORT2[...]]\n\
                           [--max-messages MAX-MESSAGES] [--throughput THROUGHPUT]\n\
                           [--acks ACKS] [--producer.config CONFIG-FILE]\n\
                           [--message-create-time CREATE-TIME] [--value-prefix VALUE-PREFIX]\n\
                           [--repeating-keys REPEATING-KEYS] [--command-config CONFIG-FILE]\n\n\
This tool produces increasing integers to the specified topic and prints JSON\n\
metadata to stdout on each \"send\" request, making externally visible which\n\
messages have been acked and which have not.\n\n\
required arguments:\n\
  --topic TOPIC                 Produce messages to this topic.\n\
  --bootstrap-server HOST1:PORT1[,...]\n\
                                REQUIRED: The server(s) to connect to.\n\n\
optional arguments:\n\
  --max-messages MAX-MESSAGES   Produce this many messages. If -1, produce until killed. (default: -1)\n\
  --throughput THROUGHPUT       If >= 0, throttle to approximately THROUGHPUT messages/sec. (default: -1)\n\
  --acks ACKS                   Acks required on each produced message: 0, 1, or -1. (default: -1)\n\
  --producer.config CONFIG-FILE (DEPRECATED) Producer config properties file. Use --command-config instead.\n\
  --message-create-time CREATE-TIME  Message creation time, in ms since epoch. (default: -1 => none)\n\
  --value-prefix VALUE-PREFIX   If set, each value has this prefix with a dot separator.\n\
  --repeating-keys REPEATING-KEYS    If set, keys cycle 0..REPEATING-KEYS (exclusive).\n\
  --command-config CONFIG-FILE  Config properties file (mutually exclusive with --producer.config)."
    );
}

async fn run(args: &[String]) -> Result<(), Error> {
    let mut producer = create_from_args(args).await?;

    let start_ms = now_millis();
    let throttler = ThroughputThrottler::new(producer.throughput() as f64, start_ms);

    // Java can't use `Runtime.addShutdownHook`'s exact semantics here; CLAUDE.md
    // §9 maps the JVM shutdown hook to a signal task that flips the stop flag.
    // Java's shutdown hook fires on both SIGINT and SIGTERM, and ducktape's
    // clean shutdown of a verifiable client sends **SIGTERM** by default and
    // then waits for the flush/close output, so we wait on either signal (see
    // [`wait_for_shutdown_signal`]) rather than SIGINT alone. The producing loop
    // observes the flag between iterations and stops.
    let stop = producer.stop_producing_handle();
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        stop.store(true, Ordering::Release);
    });

    producer.run(&throttler).await;

    // Shutdown sequence (Java shutdown hook, VerifiableProducer.java 553-565):
    // flush + close (prints shutdown_complete), then print the tool_data summary.
    producer.close().await?;
    let stop_ms = now_millis();
    let elapsed = stop_ms - start_ms;
    let avg_throughput = if elapsed != 0 {
        1000.0 * (producer.num_acked() as f64 / elapsed as f64)
    } else {
        // Java divides by zero here, yielding Infinity; serde_json cannot encode
        // a non-finite float, so we emit 0.0 instead of failing the whole event.
        // Reachable only if start and stop fall in the same millisecond.
        0.0
    };
    producer.print_tool_data(avg_throughput);
    Ok(())
}

#[tokio::main]
async fn main() {
    let _ = env_logger::Builder::from_default_env().format_timestamp_millis().try_init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() || args.iter().any(|a| a == "-h" || a == "--help") {
        print_help();
        return;
    }

    if let Err(e) = run(&args).await {
        // Java: `parser.handleError(e); System.exit(1);`
        eprintln!("{e}");
        std::process::exit(1);
    }
}
