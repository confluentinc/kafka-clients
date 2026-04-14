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

//! Simple producer performance test.
//!
//! Sends messages to a Kafka broker and measures throughput and latency.
//! Uses a bounded channel between the send loop and a drain task, matching
//! the C performance test's producer-consumer architecture.
//!
//! Usage:
//!     cargo run -p performance-tests --release --bin producer_perf
//!
//! Environment variables:
//!     BOOTSTRAP_SERVERS  -- broker address (default: localhost:9092)
//!     TOPIC              -- topic name (default: perf-test)
//!     NUM_MESSAGES       -- number of messages to send (default: 100000)
//!     VALUE_SIZE         -- value size in bytes (default: 2048)
//!     BATCH_SIZE         -- batch.size in bytes (default: 16384)
//!     LINGER_MS          -- linger.ms (default: 5)
//!     BUFFER_MEMORY      -- buffer.memory in bytes (default: 33554432)

use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use confluent_kafka_rust::clients::api_versions::ApiVersions;
use confluent_kafka_rust::clients::network_client::NetworkClient;
use confluent_kafka_rust::clients::producer::batch::SendFuture;
use confluent_kafka_rust::clients::producer::{KafkaProducer, ProducerConfig, ProducerMetadata, ProducerRecord};
use confluent_kafka_rust::common::internals::ClusterResourceListeners;
use confluent_kafka_rust::common::network::plaintext_channel_builder::PlaintextChannelBuilder;
use confluent_kafka_rust::common::network::selectable::USE_DEFAULT_BUFFER_SIZE;
use confluent_kafka_rust::common::network::selector::{NO_IDLE_TIMEOUT_MS, Selector};
use tokio::sync::mpsc;

use rand::Rng;

/// Tracks results from the drain task, shared with the main task for reporting.
struct Stats {
    ack_count: AtomicU64,
    ack_errors: AtomicU64,
    total_latency_us: AtomicU64,
    max_latency_us: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        Stats {
            ack_count: AtomicU64::new(0),
            ack_errors: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
            max_latency_us: AtomicU64::new(0),
        }
    }
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn env_or_str(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

/// Pre-generate a pool of random message payloads to avoid allocation in the
/// hot loop.
fn generate_messages(count: usize, value_size: usize) -> Vec<Vec<u8>> {
    let mut rng = rand::rng();
    let static_size = value_size / 2;
    let random_size = value_size - static_size;

    let mut static_bytes = vec![0u8; static_size];
    rng.fill(static_bytes.as_mut_slice());

    (0..count)
        .map(|_| {
            let mut buf = Vec::with_capacity(value_size);
            buf.extend_from_slice(&static_bytes);
            let mut random_bytes = vec![0u8; random_size];
            rng.fill(random_bytes.as_mut_slice());
            buf.extend_from_slice(&random_bytes);
            buf
        })
        .collect()
}

/// Drain task: receives (send_time, future) pairs from the channel, awaits
/// each future, and records latency at the moment the ack arrives.
async fn drain_task(mut rx: mpsc::Receiver<(Instant, SendFuture)>, stats: Arc<Stats>) {
    while let Some((send_time, future)) = rx.recv().await {
        match future.await {
            Ok(_metadata) => {
                let latency_us = send_time.elapsed().as_micros() as u64;
                stats.ack_count.fetch_add(1, Ordering::Relaxed);
                stats.total_latency_us.fetch_add(latency_us, Ordering::Relaxed);
                stats.max_latency_us.fetch_max(latency_us, Ordering::Relaxed);
            },
            Err(e) => {
                let n = stats.ack_errors.fetch_add(1, Ordering::Relaxed);
                if n < 5 {
                    eprintln!("Ack error #{}: {e}", n + 1);
                }
            },
        }
    }
}

#[tokio::main]
async fn main() {
    let bootstrap_servers = env_or_str("BOOTSTRAP_SERVERS", "localhost:9092");
    let topic = env_or_str("TOPIC", "perf-test");
    let num_messages: u64 = env_or("NUM_MESSAGES", 100_000);
    let value_size: usize = env_or("VALUE_SIZE", 2048);
    let batch_size: usize = env_or("BATCH_SIZE", 16384);
    let linger_ms: u64 = env_or("LINGER_MS", 5);
    let buffer_memory: usize = env_or("BUFFER_MEMORY", 33_554_432);

    println!("=== Producer Performance Test ===");
    println!("Bootstrap servers : {bootstrap_servers}");
    println!("Topic             : {topic}");
    println!("Messages          : {num_messages}");
    println!("Value size        : {value_size} bytes");
    println!("Batch size        : {batch_size} bytes");
    println!("Linger            : {linger_ms} ms");
    println!("Buffer memory     : {buffer_memory} bytes");
    println!();

    // --- Pre-generate message pool ---
    const POOL_SIZE: usize = 10_000;
    println!("Generating {POOL_SIZE} message payloads...");
    let messages = generate_messages(POOL_SIZE, value_size);

    // --- Create producer using NetworkClient ---
    let addr: SocketAddr = match bootstrap_servers.parse() {
        Ok(a) => a,
        Err(_) => {
            let resolved: Vec<SocketAddr> = tokio::net::lookup_host(&bootstrap_servers)
                .await
                .unwrap_or_else(|e| panic!("Cannot resolve '{bootstrap_servers}': {e}"))
                .collect();
            *resolved
                .first()
                .unwrap_or_else(|| panic!("No addresses found for '{bootstrap_servers}'"))
        },
    };

    let metadata = Arc::new(ProducerMetadata::new(
        50,      // refresh_backoff_ms
        5000,    // refresh_backoff_max_ms
        300_000, // metadata_expire_ms
        60_000,  // metadata_idle_ms
        ClusterResourceListeners::new(),
    ));
    metadata.metadata().bootstrap(vec![addr]);

    let channel_builder = Box::new(PlaintextChannelBuilder::new(None));
    let selector = Selector::new(USE_DEFAULT_BUFFER_SIZE, NO_IDLE_TIMEOUT_MS, channel_builder);
    let api_versions = Arc::new(ApiVersions::new());
    let host_resolver = confluent_kafka_rust::clients::DefaultHostResolver;

    let client = NetworkClient::with_metadata(
        selector,
        Arc::clone(metadata.metadata()),
        "producer-perf-test",
        5,    // max_in_flight_requests_per_connection
        50,   // reconnect_backoff_ms
        5000, // reconnect_backoff_max_ms
        USE_DEFAULT_BUFFER_SIZE,
        USE_DEFAULT_BUFFER_SIZE,
        30_000,  // default_request_timeout_ms
        10_000,  // connection_setup_timeout_ms
        127_000, // connection_setup_timeout_max_ms
        true,    // discover_broker_versions
        api_versions,
        host_resolver,
        300_000, // rebootstrap_trigger_ms
        confluent_kafka_rust::clients::metadata_recovery_strategy::MetadataRecoveryStrategy::None,
    );

    let config = ProducerConfig::builder()
        .bootstrap_servers(vec![bootstrap_servers.clone()])
        .batch_size(batch_size)
        .linger_ms(linger_ms)
        .buffer_memory(buffer_memory)
        .build()
        .expect("Failed to build ProducerConfig");

    let producer = KafkaProducer::new(config, client, metadata);

    // --- Bounded channel + drain task ---
    let (tx, rx) = mpsc::channel::<(Instant, SendFuture)>(10_000);
    let stats = Arc::new(Stats::new());
    let drain_handle = tokio::spawn(drain_task(rx, Arc::clone(&stats)));

    // --- Send loop ---
    println!("Sending {num_messages} messages...");
    let mut send_errors: u64 = 0;

    let start = Instant::now();

    for i in 0..num_messages {
        let payload = &messages[i as usize % POOL_SIZE];
        let record = ProducerRecord::new(&topic).value(payload);

        let send_time = Instant::now();
        match producer.send(&record).await {
            Ok(future) => {
                if tx.send((send_time, future)).await.is_err() {
                    eprintln!("Drain task gone, stopping.");
                    break;
                }
            },
            Err(e) => {
                send_errors += 1;
                if send_errors <= 5 {
                    eprintln!("Send error #{send_errors}: {e}");
                }
            },
        }

        if (i + 1) % 10_000 == 0 {
            let elapsed = start.elapsed().as_secs_f64();
            let rate = (i + 1) as f64 / elapsed;
            println!("  sent {}: {rate:.0} msg/s", i + 1);
        }
    }

    let send_elapsed = start.elapsed();
    println!(
        "All sends dispatched in {:.2}s ({:.0} msg/s)",
        send_elapsed.as_secs_f64(),
        num_messages as f64 / send_elapsed.as_secs_f64()
    );

    // --- Flush and wait for drain ---
    println!("Flushing...");
    if let Err(e) = producer.flush().await {
        eprintln!("Flush error: {e}");
    }

    drop(tx);
    if let Err(e) = drain_handle.await {
        eprintln!("Drain task panicked: {e}");
    }

    let total_elapsed = start.elapsed();

    // --- Results ---
    let ack_count = stats.ack_count.load(Ordering::Relaxed);
    let ack_errors = stats.ack_errors.load(Ordering::Relaxed);
    let total_latency_us = stats.total_latency_us.load(Ordering::Relaxed);
    let max_latency_us = stats.max_latency_us.load(Ordering::Relaxed);

    println!();
    println!("=== Results ===");
    println!("Total time        : {:.2}s", total_elapsed.as_secs_f64());
    println!("Messages sent     : {num_messages}");
    println!("Messages acked    : {ack_count}");
    if send_errors > 0 {
        println!("Send errors       : {send_errors}");
    }
    if ack_errors > 0 {
        println!("Ack errors        : {ack_errors}");
    }
    println!(
        "Throughput        : {:.0} msg/s",
        ack_count as f64 / total_elapsed.as_secs_f64()
    );
    let total_bytes = ack_count as f64 * value_size as f64;
    println!(
        "Throughput        : {:.2} MiB/s",
        total_bytes / (1024.0 * 1024.0) / total_elapsed.as_secs_f64()
    );
    if ack_count > 0 {
        println!(
            "Avg latency       : {:.2} ms",
            total_latency_us as f64 / ack_count as f64 / 1000.0
        );
        println!("Max latency       : {:.2} ms", max_latency_us as f64 / 1000.0);
    }

    // --- Cleanup ---
    if let Err(e) = producer.close().await {
        eprintln!("Close error: {e}");
    }
}
