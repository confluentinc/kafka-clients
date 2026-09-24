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

//! Translated from `kafka.api.SslConsumerTest`
//! (`core/src/test/scala/integration/kafka/api/SslConsumerTest.scala`, AK
//! 4.3.1), which runs `BaseConsumerTest` with `securityProtocol = SSL`.
//!
//! - `testSimpleConsumption` (CONSUMER group-protocol arm) →
//!   [`test_simple_consumption`]
//! - The CLASSIC arm of the `getTestGroupProtocolParametersAll` parameter set is
//!   out of scope (`consumer-threading.md` §20).
//! - Not in this phase: `testClusterResourceListener` (needs
//!   `ClusterResourceListener` interceptor/serializer injection) and
//!   `testCoordinatorFailover` (needs broker shutdown, which the pooled harness
//!   does not provide).

use std::collections::HashMap;

use crate::common::test_context::TestContext;

use super::base_consumer_test::{self, SimpleConsumptionConfig};

/// Translated from `BaseConsumerTest.testSimpleConsumption`
/// (`BaseConsumerTest.scala:60-76`) as run by `SslConsumerTest`.
///
/// Every client of the Scala harness uses the SSL listener with the generated
/// truststore; here that is the cluster CA PEM. Hostname verification is off
/// because the tests reach the broker via `127.0.0.1`, matching the other SSL
/// suites (`ssl_sasl_test`, `sasl_ssl_consumer_test`).
///
/// Client overrides are `AbstractConsumerTest`'s
/// (`AbstractConsumerTest.scala:60-67`): producer `acks=all` +
/// `client.id=ConsumerTestProducer`; consumer
/// `client.id=ConsumerTestConsumer`, `auto.offset.reset=earliest`,
/// `enable.auto.commit=false`, `metadata.max.age.ms=100`,
/// `max.poll.interval.ms=6000`. Deviation: Java's fixed group id `"my-test"`
/// becomes a per-test id because clusters are pooled across tests here.
#[tokio::test(flavor = "multi_thread")]
async fn test_simple_consumption() {
    let mut ctx = TestContext::new(base_consumer_test::cluster_config()).await;
    let client_props = HashMap::from([
        ("bootstrap.servers".to_string(), ctx.ssl_bootstrap_servers().to_string()),
        ("security.protocol".to_string(), "SSL".to_string()),
        ("ssl.truststore.certificates".to_string(), ctx.ca_cert_pem().to_string()),
        ("ssl.endpoint.identification.algorithm".to_string(), String::new()),
    ]);
    let producer_props = HashMap::from([
        ("acks".to_string(), "all".to_string()),
        ("client.id".to_string(), "ConsumerTestProducer".to_string()),
    ]);
    let consumer_props = HashMap::from([
        ("group.protocol".to_string(), "consumer".to_string()),
        ("client.id".to_string(), "ConsumerTestConsumer".to_string()),
        ("group.id".to_string(), ctx.group_id("my-test")),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
        ("metadata.max.age.ms".to_string(), "100".to_string()),
        ("max.poll.interval.ms".to_string(), "6000".to_string()),
    ]);
    base_consumer_test::test_simple_consumption(
        &mut ctx,
        SimpleConsumptionConfig { client_props, producer_props, consumer_props },
    )
    .await;
}
