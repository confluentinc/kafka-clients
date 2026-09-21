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

//! Command-line entry point for the `VerifiableConsumer` system-test tool.
//!
//! Translated from `VerifiableConsumer.main`. A thin `#[tokio::main] async fn`
//! delegating to the library (precedent: `src/bin/verifiable_producer.rs`).
//!
//! This client supports only the KIP-848 `consumer` group protocol
//! (`consumer-threading.md` §20); running with `--group-protocol classic` fails
//! at startup with an unsupported-version error.
//!
//! Run, e.g.:
//!
//! ```sh
//! cargo run -p verifiable-clients --bin verifiable_consumer -- \
//!     --bootstrap-server localhost:9092 --topic t --group-id g \
//!     --group-protocol consumer
//! ```

#![deny(warnings)]

use confluent_kafka::common::Error;
use verifiable_clients::verifiable_consumer::{create_from_args, help_text};
use verifiable_clients::wait_for_shutdown_signal;

async fn run(args: &[String]) -> Result<(), Error> {
    let mut consumer = create_from_args(args)?;

    // Java maps this to `Runtime.addShutdownHook(new Thread(consumer::close))`.
    // CLAUDE.md §9 maps the JVM shutdown hook to a signal task. Java's shutdown
    // hook fires on both SIGINT and SIGTERM; ducktape's clean shutdown of a
    // verifiable client sends **SIGTERM** by default and then waits for
    // `shutdown_complete`, so we wait on either signal (see
    // [`wait_for_shutdown_signal`]) rather than SIGINT alone. Java's `close()`
    // prints `shutdown_requested` and calls `consumer.wakeup()`, which
    // interrupts the blocking `poll` so `run` unwinds, closes the consumer, and
    // prints `shutdown_complete`.
    let handle = consumer.handle();
    let reporter = consumer.reporter();
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        reporter.print_shutdown_requested();
        handle.wakeup();
    });

    consumer.run().await;
    Ok(())
}

#[tokio::main]
async fn main() {
    let _ = env_logger::Builder::from_default_env().format_timestamp_millis().try_init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() || args.iter().any(|a| a == "-h" || a == "--help") {
        println!("{}", help_text());
        return;
    }

    if let Err(e) = run(&args).await {
        // Java: `parser.handleError(e); System.exit(1);`
        eprintln!("{e}");
        std::process::exit(1);
    }
}
