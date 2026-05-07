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

//! Producer perf-shape integration tests.
//!
//! Mirror the burst pattern of `bindings/python/test/performance/producer_performance_test.py`
//! (100 records every ~1 s, 10 s total) but call `KafkaProducer` directly —
//! no FFI, no per-record `block_on`, no cross-thread `.result()` worker.
//! This isolates whether the >2 s avg-latency regression observed via the
//! Python binding lives in the core Rust client or in the binding.
//!
//! Run with:
//!     cargo test --features integration-tests --test integration \
//!         producer_perf_test -- --ignored --nocapture --test-threads=1

use std::collections::HashMap;
use std::time::{Duration, Instant};

use confluent_kafka::common::KafkaFuture;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;
use confluent_kafka::producer::RecordMetadata;
use tokio::sync::mpsc;

use crate::common::cluster_config::ClusterConfig;
use crate::common::kafka_cluster::{SASL_PASSWORD, SASL_USERNAME};
use crate::common::test_context::TestContext;

const VALUE_SIZE: usize = 2048;
const RECORDS_PER_BURST: usize = 100;
const BURST_INTERVAL: Duration = Duration::from_secs(1);
const TEST_DURATION: Duration = Duration::from_secs(10);
/// Per-record p99 latency budget for the burst-pattern perf tests.
/// Currently observed p99 is ~25-40 ms across PLAINTEXT and SASL_SSL;
/// 70 ms gives ~2x headroom over the worst observed run.
const P99_LIMIT_MS: u128 = 70;

fn make_value(seed: u8) -> Vec<u8> {
    vec![seed; VALUE_SIZE]
}

fn percentile(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

async fn run_perf_loop(producer: KafkaProducer<Vec<u8>, Vec<u8>>, topic: String) {
    let (tx, mut rx) = mpsc::unbounded_channel::<(KafkaFuture<RecordMetadata>, Instant)>();

    // Worker task: drain futures sequentially and record latency from
    // pre-send to post-future-resolution. This mirrors the Python perf
    // test's `record_completed_calls_worker`.
    let worker = tokio::spawn(async move {
        let mut latencies_us: Vec<u128> = Vec::new();
        while let Some((future, sent_at)) = rx.recv().await {
            let _ = future.get_timeout(Duration::from_secs(30)).await;
            latencies_us.push(sent_at.elapsed().as_micros());
        }
        latencies_us
    });

    let test_start = Instant::now();
    let mut produced: usize = 0;
    let mut burst_idx: usize = 0;
    while test_start.elapsed() < TEST_DURATION {
        let burst_started = Instant::now();
        for i in 0..RECORDS_PER_BURST {
            let value = make_value((produced + i) as u8);
            let record: ProducerRecord<&[u8], &[u8]> =
                ProducerRecord::with_key(topic.clone(), None, Some(value.as_slice()));
            let sent_at = Instant::now();
            match producer.send(record, None).await {
                Ok(future) => {
                    if tx.send((future, sent_at)).is_err() {
                        return;
                    }
                },
                Err(e) => {
                    eprintln!("[perf] producer.send error at record {}: {}", produced + i, e);
                },
            }
        }
        produced += RECORDS_PER_BURST;
        burst_idx += 1;

        let elapsed_in_burst = burst_started.elapsed();
        if elapsed_in_burst < BURST_INTERVAL {
            tokio::time::sleep(BURST_INTERVAL - elapsed_in_burst).await;
        }
    }

    drop(tx);
    let latencies_us = worker.await.expect("worker panicked");

    let completed = latencies_us.len();
    let mut sorted = latencies_us.clone();
    sorted.sort_unstable();
    let sum_us: u128 = sorted.iter().sum();
    let avg_us = if completed > 0 { sum_us / completed as u128 } else { 0 };
    let max_us = sorted.last().copied().unwrap_or(0);
    let p50_us = percentile(&sorted, 0.50);
    let p95_us = percentile(&sorted, 0.95);
    let p99_us = percentile(&sorted, 0.99);

    println!("[perf] bursts={}", burst_idx);
    println!("[perf] produced={}, completed={}", produced, completed);
    println!("[perf] avg={:.2} ms", avg_us as f64 / 1000.0);
    println!("[perf] p50={:.2} ms", p50_us as f64 / 1000.0);
    println!("[perf] p95={:.2} ms", p95_us as f64 / 1000.0);
    println!("[perf] p99={:.2} ms", p99_us as f64 / 1000.0);
    println!("[perf] max={:.2} ms", max_us as f64 / 1000.0);

    assert_eq!(produced, completed, "every produced record must complete");
    assert!(
        p99_us <= P99_LIMIT_MS * 1000,
        "p99 latency {:.2} ms exceeds {} ms budget",
        p99_us as f64 / 1000.0,
        P99_LIMIT_MS,
    );
}

fn make_plaintext_config(bootstrap: &str) -> ProducerConfig {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), "perf-test-plaintext".to_string()),
        ("acks".to_string(), "all".to_string()),
        ("max.block.ms".to_string(), "30000".to_string()),
        ("linger.ms".to_string(), "0".to_string()),
    ]);
    ProducerConfig::from_properties(&props).expect("invalid PLAINTEXT perf config")
}

fn make_sasl_ssl_config(bootstrap: &str, ca_cert_pem: &str) -> ProducerConfig {
    let jaas = format!(
        "org.apache.kafka.common.security.plain.PlainLoginModule required \
         username=\"{SASL_USERNAME}\" password=\"{SASL_PASSWORD}\";"
    );
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), "perf-test-sasl-ssl".to_string()),
        ("acks".to_string(), "all".to_string()),
        ("max.block.ms".to_string(), "30000".to_string()),
        ("linger.ms".to_string(), "0".to_string()),
        ("security.protocol".to_string(), "SASL_SSL".to_string()),
        ("sasl.mechanism".to_string(), "PLAIN".to_string()),
        ("sasl.jaas.config".to_string(), jaas),
        ("ssl.truststore.certificates".to_string(), ca_cert_pem.to_string()),
        ("ssl.endpoint.identification.algorithm".to_string(), String::new()),
    ]);
    ProducerConfig::from_properties(&props).expect("invalid SASL_SSL perf config")
}

/// Produce 100 records every 1 s for 10 s over a PLAINTEXT connection.
/// Asserts every record completes; prints latency distribution.
#[ignore = "perf test; opt in with --ignored"]
#[tokio::test(flavor = "multi_thread")]
async fn perf_plaintext_burst_pattern() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("perf_plaintext");
    let config = make_plaintext_config(ctx.bootstrap_servers());
    let producer: KafkaProducer<Vec<u8>, Vec<u8>> =
        KafkaProducer::from_config(config, Box::new(ByteArraySerializer), Box::new(ByteArraySerializer))
            .expect("failed to create PLAINTEXT producer");

    println!("[perf] PLAINTEXT bootstrap={}", ctx.bootstrap_servers());
    run_perf_loop(producer, topic).await;
}

/// Produce 100 records every 1 s for 10 s over a SASL_SSL connection.
/// This is the path the Python binding's perf test exercises.
#[ignore = "perf test; opt in with --ignored"]
#[tokio::test(flavor = "multi_thread")]
async fn perf_sasl_ssl_burst_pattern() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("perf_sasl_ssl");
    let bootstrap = ctx.sasl_ssl_bootstrap_servers().to_string();
    let ca_cert_pem = ctx.ca_cert_pem().to_string();
    let config = make_sasl_ssl_config(&bootstrap, &ca_cert_pem);
    let producer: KafkaProducer<Vec<u8>, Vec<u8>> =
        KafkaProducer::from_config(config, Box::new(ByteArraySerializer), Box::new(ByteArraySerializer))
            .expect("failed to create SASL_SSL producer");

    println!("[perf] SASL_SSL bootstrap={}", bootstrap);
    run_perf_loop(producer, topic).await;
}
