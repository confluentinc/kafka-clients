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

//! Phase 9i — Confluent Cloud (CCloud) smoke test.
//!
//! Exercises the producer end-to-end against an externally-managed
//! Kafka cluster (typically Confluent Cloud or an EC2-hosted broker)
//! over `SASL_SSL` + `PLAIN` credentials, validating the Phase-9i
//! system-trust-store fallback path
//! (`src/common/security/ssl/mod.rs:load_native_certs_into_root_store`).
//!
//! Unlike the in-tree integration tests (which spin up a Testcontainers
//! broker via `cluster_pool`), this test does NOT start a broker — it
//! connects to whichever cluster the operator's env vars point at:
//!
//! | Variable               | Default            | Description                            |
//! |------------------------|--------------------|----------------------------------------|
//! | `BOOTSTRAP_SERVERS`    | (required)         | CCloud bootstrap URL, e.g. `pkc-…:9092`|
//! | `TOPIC_NAME`           | `ccloud-smoke-test`| Topic (must exist on the cluster)      |
//! | `SECURITY_PROTOCOL`    | `SASL_SSL`         | `SASL_SSL` for CCloud                  |
//! | `SASL_MECHANISM`       | `PLAIN`            | CCloud uses PLAIN                      |
//! | `SASL_USERNAME`        | (required)         | CCloud API key                         |
//! | `SASL_PASSWORD`        | (required)         | CCloud API secret                      |
//! | `SSL_CA_LOCATION`      | (none)             | Optional explicit truststore PEM       |
//!
//! **Skip behavior.** If `SASL_USERNAME` is unset, the test prints a
//! single-line skip notice and exits success — so the test is safe to
//! include in `cargo test --features integration-tests` without
//! gating it behind a separate feature. This mirrors what the broader
//! ecosystem (e.g. confluent-kafka-go's `tests/integration_test.go`)
//! does to keep cloud-dependent tests opt-in via env vars while
//! staying in the default integration test set.
//!
//! **Verification of the un-skip path.** When the env vars ARE set,
//! the test produces 10 records to the configured topic and asserts:
//!   1. Every send future resolves Ok (broker ack'd).
//!   2. The acks carry the expected topic + non-negative offset shape.
//!   3. No `ssl.truststore.location` is configured — exercising the
//!      Phase-9i system-trust-store fallback path. (If
//!      `SSL_CA_LOCATION` is also set, it overrides the fallback —
//!      use the same env var that's documented for `performance_test`
//!      so operators can switch between system-trust-store mode and
//!      explicit-PEM mode without changing test invocation.)
//!
//! **Why we re-use `PerfTestConfig::sasl_props()`.** The plumbing of
//! env-var → producer props for CCloud auth is identical to the
//! performance test's. Pull it via `super::performance_test`'s public
//! surface? No — the perf test's symbols are private to that module.
//! Duplicating the small `sasl_props` helper here keeps each test
//! file self-contained.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use confluent_kafka::common::serialization::serdes::ByteArrayOwnedSerializer;
use confluent_kafka::producer::{KafkaProducer, Producer, ProducerRecord, RecordMetadata};

/// Number of records the un-skip path produces. Kept small —
/// the assertion (every ack OK) is what matters, not throughput.
/// (The full throughput rig is `performance_test`.)
const CCLOUD_SMOKE_RECORDS: usize = 10;

/// Default topic name when `TOPIC_NAME` is unset.
const DEFAULT_TOPIC: &str = "ccloud-smoke-test";

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Phase 9i: producer-side smoke test against a CCloud-style external
/// broker. Skips when `SASL_USERNAME` is unset — runs only on EC2/CCloud
/// CI where the operator has populated the auth env vars.
///
/// Java parity reference: the equivalent JVM-client behavior is what
/// `KafkaProducer` does when given `security.protocol=SASL_SSL` +
/// `sasl.mechanism=PLAIN` + no `ssl.truststore.location` — JSSE
/// resolves `TrustManagerFactory.init(null)` to the JVM-default trust
/// store, which validates the broker's publicly-trusted CA. See
/// `DefaultSslEngineFactory.java:270-275,307-327`.
#[tokio::test(flavor = "multi_thread")]
async fn ccloud_smoke_test() {
    let sasl_username = env_or("SASL_USERNAME", "");
    if sasl_username.is_empty() {
        println!(
            "ccloud_smoke_test: skipping (SASL_USERNAME unset). Set SASL_USERNAME, SASL_PASSWORD, and BOOTSTRAP_SERVERS to run against a real CCloud cluster."
        );
        return;
    }

    let bootstrap_servers = env_or("BOOTSTRAP_SERVERS", "");
    assert!(
        !bootstrap_servers.is_empty(),
        "BOOTSTRAP_SERVERS must be set when SASL_USERNAME is set (no Testcontainers fallback for CCloud test)"
    );

    let sasl_password = env_or("SASL_PASSWORD", "");
    assert!(!sasl_password.is_empty(), "SASL_PASSWORD must be set when SASL_USERNAME is set");

    let topic = env_or("TOPIC_NAME", DEFAULT_TOPIC);
    let security_protocol = env_or("SECURITY_PROTOCOL", "SASL_SSL");
    let sasl_mechanism = env_or("SASL_MECHANISM", "PLAIN");
    let ssl_ca_location = env_or("SSL_CA_LOCATION", "");

    println!(
        "ccloud_smoke_test: connecting to {bootstrap_servers} (topic={topic}, protocol={security_protocol}, mechanism={sasl_mechanism})"
    );
    if ssl_ca_location.is_empty() {
        println!("ccloud_smoke_test: ssl.truststore.location unset — exercising Phase 9i system trust store fallback");
    } else {
        println!("ccloud_smoke_test: ssl.truststore.location={ssl_ca_location} — explicit PEM truststore in use");
    }

    // Compose the canonical Java `sasl.jaas.config` string for PLAIN.
    // Shape matches the SASL_SSL happy-path producer-smoke test —
    // pulled from `confluent_kafka::common::security::jaas_config::PLAIN_LOGIN_MODULE`
    // so a refactor of the FQCN remains in sync.
    let jaas_config = format!(
        r#"{module} required username="{user}" password="{pass}";"#,
        module = confluent_kafka::common::security::jaas_config::PLAIN_LOGIN_MODULE,
        user = sasl_username,
        pass = sasl_password,
    );

    let mut props: HashMap<String, String> = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap_servers),
        ("acks".to_string(), "all".to_string()),
        ("linger.ms".to_string(), "10".to_string()),
        ("compression.type".to_string(), "none".to_string()),
        ("client.id".to_string(), "ccloud-smoke-test-rust".to_string()),
        ("max.block.ms".to_string(), "60000".to_string()),
        (
            "key.serializer".to_string(),
            "org.apache.kafka.common.serialization.ByteArraySerializer".to_string(),
        ),
        (
            "value.serializer".to_string(),
            "org.apache.kafka.common.serialization.ByteArraySerializer".to_string(),
        ),
        ("security.protocol".to_string(), security_protocol),
        ("sasl.mechanism".to_string(), sasl_mechanism),
        ("sasl.jaas.config".to_string(), jaas_config),
    ]);
    if !ssl_ca_location.is_empty() {
        // Explicit PEM truststore. The Phase 9i fallback is bypassed
        // for this codepath; useful when CCloud rotates roots faster
        // than the OS keychain bundles can keep up.
        props.insert("ssl.truststore.location".to_string(), ssl_ca_location);
        props.insert("ssl.truststore.type".to_string(), "PEM".to_string());
    }

    let key_ser: Box<dyn confluent_kafka::common::serialization::Serializer<Vec<u8>>> =
        Box::new(ByteArrayOwnedSerializer);
    let value_ser: Box<dyn confluent_kafka::common::serialization::Serializer<Vec<u8>>> =
        Box::new(ByteArrayOwnedSerializer);
    let producer = Arc::new(
        KafkaProducer::with_serializers(props, key_ser, value_ser).expect("KafkaProducer::with_serializers failed"),
    );

    let topic_arc: Arc<str> = Arc::from(topic.as_str());
    let mut futures = Vec::with_capacity(CCLOUD_SMOKE_RECORDS);
    for i in 0..CCLOUD_SMOKE_RECORDS {
        let topic = topic_arc.clone();
        let key = format!("k{i:04}").into_bytes();
        let value = format!("v{i:04}").into_bytes();
        let record = ProducerRecord::with_key(topic, Some(key), Some(value)).expect("ProducerRecord::with_key");
        let fut = producer
            .send(record)
            .await
            .unwrap_or_else(|e| panic!("send #{i} enqueue failed: {e:?}"));
        futures.push(fut);
    }

    let results = futures_util::future::join_all(futures.into_iter().map(|f| async move { f.get().await })).await;
    let mut metadatas: Vec<RecordMetadata> = Vec::with_capacity(CCLOUD_SMOKE_RECORDS);
    for (i, r) in results.into_iter().enumerate() {
        let meta = r.unwrap_or_else(|e| panic!("send #{i} broker ack failed: {e:?}"));
        metadatas.push(meta);
    }

    assert_eq!(
        metadatas.len(),
        CCLOUD_SMOKE_RECORDS,
        "expected {CCLOUD_SMOKE_RECORDS} acked records, got {}",
        metadatas.len(),
    );

    for (i, m) in metadatas.iter().enumerate() {
        assert_eq!(m.topic(), topic.as_str(), "record #{i}: topic mismatch");
        assert!(m.offset() >= 0, "record #{i}: negative offset {}", m.offset());
    }

    let producer = Arc::into_inner(producer).expect("producer Arc had outstanding refs at close");
    producer
        .close_with_timeout(Duration::from_secs(30))
        .await
        .expect("graceful close failed");

    println!("ccloud_smoke_test: PASS ({CCLOUD_SMOKE_RECORDS} records acked)");
}
