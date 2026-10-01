// Copyright 2026 Confluent Inc.
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

//! Translated from
//! `org.apache.kafka.clients.consumer.SaslPlainPlaintextConsumerTest`
//! (`clients-integration-tests`, AK 4.3.1): the consumer over a
//! PLAIN-authenticated SASL_PLAINTEXT listener.
//!
//! - `testAsyncConsumerSimpleConsumption` → [`test_async_consumer_simple_consumption`]
//! - SKIP: `testClassicConsumerSimpleConsumption`,
//!   `testClassicConsumerClusterResourceListener`,
//!   `testClassicConsumerCoordinatorFailover` — classic-protocol-only
//!   (`consumer-threading.md` §20).
//! - Not in this phase: `testAsyncConsumerClusterResourceListener` (needs
//!   `ClusterResourceListener` interceptor/serializer injection) and
//!   `testAsyncConsumeCoordinatorFailover` (needs broker shutdown, which the
//!   pooled harness does not provide).

use std::collections::HashMap;

use crate::common::kafka_cluster::{SASL_PASSWORD, SASL_USERNAME};
use crate::common::test_context::TestContext;

use super::base_consumer_test::{self, SimpleConsumptionConfig};

/// Java's `MECHANISMS` (`SaslPlainPlaintextConsumerTest.java`).
const MECHANISMS: &str = "PLAIN";

/// Java's `SASL_JAAS`: `PlainLoginModule` with the cluster's PLAIN admin
/// credentials (`KAFKA_PLAIN_ADMIN` / `KAFKA_PLAIN_ADMIN_PASSWORD` there,
/// [`SASL_USERNAME`] / [`SASL_PASSWORD`] in this harness).
fn sasl_jaas() -> String {
    format!(
        "org.apache.kafka.common.security.plain.PlainLoginModule required \
         username=\"{SASL_USERNAME}\" password=\"{SASL_PASSWORD}\";"
    )
}

/// Translated from `SaslPlainPlaintextConsumerTest.testAsyncConsumerSimpleConsumption`
/// (`SaslPlainPlaintextConsumerTest.java:81-91`), which runs
/// `BaseConsumerTestcase.testSimpleConsumption` with
/// `security.protocol=SASL_PLAINTEXT`, `sasl.mechanism=PLAIN`, the PLAIN JAAS
/// config and `group.protocol=consumer`. Java's `ClusterInstance` applies the
/// same SASL client config to its producer and admin (`setClientSaslConfig`)
/// and defaults the consumer to `auto.offset.reset=earliest` and a random
/// `group_*` group id (`ClusterInstance.java:164-169`).
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_simple_consumption() {
    let mut ctx = TestContext::new(base_consumer_test::cluster_config()).await;
    let client_props = HashMap::from([
        (
            "bootstrap.servers".to_string(),
            ctx.sasl_plaintext_bootstrap_servers().to_string(),
        ),
        ("security.protocol".to_string(), "SASL_PLAINTEXT".to_string()),
        ("sasl.mechanism".to_string(), MECHANISMS.to_string()),
        ("sasl.jaas.config".to_string(), sasl_jaas()),
    ]);
    let consumer_props = HashMap::from([
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("group.id".to_string(), ctx.group_id("group")),
    ]);
    base_consumer_test::test_simple_consumption(
        &mut ctx,
        SimpleConsumptionConfig { client_props, producer_props: HashMap::new(), consumer_props },
    )
    .await;
}
