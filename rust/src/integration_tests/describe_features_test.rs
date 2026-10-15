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

//! The node API versions `describeFeatures` exposes through the crate-private
//! `InternalDescribeFeaturesResult` (KAFKA-19663), against a real broker.
//!
//! It lives here rather than in `tests/integration` because
//! `KafkaAdminClient::describe_features_internal` and `InternalDescribeFeaturesResult`
//! are crate-private (`admin.internals`).

use std::collections::HashMap;
use std::time::Duration;

use crate::admin::{Admin, AdminClientConfig, DescribeFeaturesOptions, KafkaAdminClient};
use crate::api_message_type::ListenerType;

use crate::integration_tests::common::cluster_config::ClusterConfig;
use crate::integration_tests::common::test_context::TestContext;

/// Translated from the broker half of `DescribeFeaturesTest.testApiVersions`
/// (`clients-integration-tests`, which `check-java-name` does not index, hence
/// no `doc(alias)`): every API a broker-connected admin's
/// `nodeApiVersions()` reports is in scope for the BROKER listener. This also
/// cross-checks the crate's `ApiKeys` listener table against a real broker.
///
/// The second half of the Java test builds a `bootstrap.controllers` admin and
/// expects only CONTROLLER-listener APIs; it is not translated, because this
/// client still rejects `bootstrap.controllers` in `AdminClientConfig`.
#[tokio::test]
async fn test_api_versions() {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let mut props = HashMap::new();
    ctx.configure(&mut props);
    let admin = KafkaAdminClient::new(AdminClientConfig::new(&props).expect("valid admin config"))
        .expect("create admin client");

    let versions = admin
        .describe_features_internal(DescribeFeaturesOptions::new())
        .node_api_versions()
        .get_with_timeout(Duration::from_secs(30))
        .await
        .expect("node API versions");
    let supported = versions.all_supported_api_versions();
    assert!(!supported.is_empty(), "the broker should report at least one API");
    for key in supported.keys() {
        assert!(
            key.in_scope(ListenerType::Broker),
            "the broker reported {key}, which the client does not scope to the BROKER listener"
        );
    }

    admin.close_with_timeout(Duration::from_secs(5)).await;
}
