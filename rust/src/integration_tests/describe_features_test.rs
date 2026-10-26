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
//! `InternalDescribeFeaturesResult` (KAFKA-19663), against a real broker and,
//! through `bootstrap.controllers`, a real controller.
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

/// Translated from `DescribeFeaturesTest.testApiVersions`
/// (`clients-integration-tests`, which `check-java-name` does not index, hence
/// no `doc(alias)`): every API a broker-connected admin's `nodeApiVersions()`
/// reports is in scope for the BROKER listener, and every API a
/// `bootstrap.controllers` admin's reports is in scope for the CONTROLLER
/// listener. This also cross-checks the crate's `ApiKeys` listener table
/// against a real broker and controller.
///
/// The controller admin connects to the PLAINTEXT controller listener the
/// harness publishes ([`KafkaCluster::bootstrap_controllers`]), whatever
/// protocol the run selects; Java's `clusterInstance.admin(Map.of(), true)`
/// applies the run's security to it.
///
/// [`KafkaCluster::bootstrap_controllers`]: crate::integration_tests::common::kafka_cluster::KafkaCluster::bootstrap_controllers
#[tokio::test]
async fn test_api_versions() {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let mut props = HashMap::new();
    ctx.configure(&mut props);
    assert_node_api_versions_in_scope(props, ListenerType::Broker).await;

    let bootstrap_controllers = ctx
        .cluster()
        .bootstrap_controllers()
        .expect("a combined node publishes its controller listener");
    let props = HashMap::from([(
        AdminClientConfig::BOOTSTRAP_CONTROLLERS_CONFIG.to_string(),
        bootstrap_controllers.to_string(),
    )]);
    assert_node_api_versions_in_scope(props, ListenerType::Controller).await;
}

/// Creates an admin from `props` and asserts that every API its
/// `describeFeatures` node reports is in scope for `listener`.
async fn assert_node_api_versions_in_scope(props: HashMap<String, String>, listener: ListenerType) {
    let admin = KafkaAdminClient::new(AdminClientConfig::new(&props).expect("valid admin config"))
        .expect("create admin client");

    let versions = admin
        .describe_features_internal(DescribeFeaturesOptions::new())
        .node_api_versions()
        .get_with_timeout(Duration::from_secs(30))
        .await
        .expect("node API versions");
    let supported = versions.all_supported_api_versions();
    assert!(!supported.is_empty(), "the node should report at least one API");
    for key in supported.keys() {
        assert!(
            key.in_scope(listener),
            "the node reported {key}, which the client does not scope to the {listener:?} listener"
        );
    }

    admin.close_with_timeout(Duration::from_secs(5)).await;
}
