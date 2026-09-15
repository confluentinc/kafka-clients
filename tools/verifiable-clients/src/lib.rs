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

//! Rust translations of Apache Kafka's client-facing system-test tools.
//!
//! Milestone 14 delivers `org.apache.kafka.tools.VerifiableProducer` and
//! `VerifiableConsumer` (plus their dependencies) as runnable system-test
//! clients over the already-translated producer/consumer. Each tool mirrors
//! Java's **stdout JSON contract** exactly (event `name`s, field names, field
//! order), because that contract is what a downstream ducktape-style harness
//! parses — the tool equivalent of the wire-level fidelity the client crate's
//! generated types hold.

#![deny(warnings)]

pub mod throughput_throttler;
pub mod verifiable_consumer;
pub mod verifiable_producer;

pub use throughput_throttler::ThroughputThrottler;
pub use verifiable_consumer::VerifiableConsumer;
pub use verifiable_producer::VerifiableProducer;

/// Wait for a process-shutdown signal, resolving as soon as the first one
/// arrives: SIGINT (Ctrl-C) or, on Unix, SIGTERM.
///
/// Java maps a verifiable client's shutdown to a JVM shutdown hook
/// (`Runtime.getRuntime().addShutdownHook(...)`), which the JVM runs on **both**
/// SIGINT and SIGTERM. Crucially, the ducktape system-test harness performs a
/// *clean* shutdown of a verifiable client by sending **SIGTERM** by default
/// (`kafka/tests/kafkatest/services/verifiable_client.py`: `kill_signal`
/// defaults to `signal.SIGTERM`) and then waits for the tool to print
/// `shutdown_complete`. Awaiting only [`tokio::signal::ctrl_c`] (SIGINT) would
/// let SIGTERM kill the process abruptly before the clean-shutdown path runs, so
/// the harness's wait for `shutdown_complete` would hang. We therefore wait on
/// either signal and let the caller run the same clean-shutdown routine for both.
///
/// `tokio::signal::unix` is Unix-only; on other platforms this falls back to
/// Ctrl-C alone. The function only *detects* the signal — it performs no side
/// effects — so racing the two arms in a `select!` loses nothing
/// (`consumer-threading.md` §10 spirit: no irreversible work inside a racing
/// select arm).
pub async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        // If SIGTERM can't be registered, fall back to Ctrl-C alone rather than
        // failing to install any handler.
        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {},
                    _ = sigterm.recv() => {},
                }
            },
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            },
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
