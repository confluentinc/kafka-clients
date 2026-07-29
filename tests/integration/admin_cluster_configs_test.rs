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

//! Integration tests for the `KafkaAdminClient` cluster & config RPCs against a
//! real Kafka 4.2.0 broker.
//!
//! Mirrors the describeCluster / describeConfigs / incrementalAlterConfigs /
//! listConfigResources scenarios in Java's `KafkaAdminClientIntegrationTest`,
//! exercising the real network engine end to end rather than the `MockClient`
//! unit-test harness.

use std::collections::HashMap;
use std::time::Duration;

use confluent_kafka::admin::{
    Admin, AdminClientConfig, AlterConfigOp, AlterConfigsOptions, ConfigEntry, CreateTopicsOptions,
    DeleteTopicsOptions, DescribeClusterOptions, DescribeConfigsOptions, ListConfigResourcesOptions, NewTopic, OpType,
    new_admin_client,
};
use confluent_kafka::common::TopicCollection;
use confluent_kafka::common::config::{ConfigResource, ConfigResourceType};

use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;

/// Build an admin client pointed at the cluster's PLAINTEXT listener.
fn admin_for(bootstrap_servers: &str) -> Box<dyn Admin> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap_servers.to_string()),
        ("client.id".to_string(), "integration-test-admin".to_string()),
        ("request.timeout.ms".to_string(), "30000".to_string()),
        ("default.api.timeout.ms".to_string(), "30000".to_string()),
    ]);
    let config = AdminClientConfig::from_properties(&props).expect("valid admin config");
    new_admin_client(config).expect("admin client")
}

#[tokio::test]
async fn test_describe_cluster_returns_nodes_controller_and_id() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let result = admin.describe_cluster(DescribeClusterOptions::new());
    let nodes = result.nodes().get().await.expect("describe cluster nodes");
    assert!(!nodes.is_empty(), "cluster should report at least one node");

    let controller = result.controller().get().await.expect("controller");
    assert!(controller.is_some(), "cluster should have a controller");
    let controller = controller.unwrap();
    assert!(
        nodes.iter().any(|n| n.id() == controller.id()),
        "controller {} should be one of the reported nodes",
        controller.id()
    );

    let cluster_id = result.cluster_id().get().await.expect("cluster id");
    assert!(!cluster_id.is_empty(), "cluster id should be non-empty");

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

#[tokio::test]
async fn test_describe_configs_topic_returns_defaults() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let topic = ctx.topic("admin_describe_topic_config");
    admin
        .create_topics(&[NewTopic::new(topic.clone(), 1, 1)], CreateTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("create topic");

    let resource = ConfigResource::new(ConfigResourceType::Topic, topic.clone());
    let result = admin.describe_configs(std::slice::from_ref(&resource), DescribeConfigsOptions::new());
    let config = result.values()[&resource].get().await.expect("describe topic configs");

    // A topic should report standard default configs.
    assert!(config.get("retention.ms").is_some(), "retention.ms should be present");
    assert!(config.get("cleanup.policy").is_some(), "cleanup.policy should be present");

    admin
        .delete_topics(TopicCollection::of_topic_names(vec![topic]), DeleteTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("delete topic");
    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

#[tokio::test]
async fn test_incremental_alter_configs_set_and_delete_topic_config() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let topic = ctx.topic("admin_alter_topic_config");
    admin
        .create_topics(&[NewTopic::new(topic.clone(), 1, 1)], CreateTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("create topic");

    let resource = ConfigResource::new(ConfigResourceType::Topic, topic.clone());

    // SET retention.ms to a custom value.
    let set_op = AlterConfigOp::new(
        ConfigEntry::new("retention.ms".to_string(), Some("123456789".to_string())),
        OpType::Set,
    );
    let mut configs = HashMap::new();
    configs.insert(resource.clone(), vec![set_op]);
    admin
        .incremental_alter_configs(&configs, AlterConfigsOptions::new())
        .all()
        .get()
        .await
        .expect("set retention.ms");

    // describe_configs confirms the new value.
    let described = admin
        .describe_configs(std::slice::from_ref(&resource), DescribeConfigsOptions::new())
        .values()[&resource]
        .get()
        .await
        .expect("describe after set");
    assert_eq!(
        described.get("retention.ms").and_then(|e| e.value()),
        Some("123456789"),
        "retention.ms should reflect the set value"
    );

    // DELETE retention.ms reverts it to the default.
    let delete_op = AlterConfigOp::new(ConfigEntry::new("retention.ms".to_string(), None), OpType::Delete);
    let mut configs = HashMap::new();
    configs.insert(resource.clone(), vec![delete_op]);
    admin
        .incremental_alter_configs(&configs, AlterConfigsOptions::new())
        .all()
        .get()
        .await
        .expect("delete retention.ms");

    let described = admin
        .describe_configs(std::slice::from_ref(&resource), DescribeConfigsOptions::new())
        .values()[&resource]
        .get()
        .await
        .expect("describe after delete");
    let entry = described.get("retention.ms").expect("retention.ms still present as a default");
    assert_ne!(
        entry.value(),
        Some("123456789"),
        "retention.ms should no longer be the custom value after delete"
    );

    admin
        .delete_topics(TopicCollection::of_topic_names(vec![topic]), DeleteTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("delete topic");
    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

#[tokio::test]
async fn test_describe_configs_broker_returns_broker_configs() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    // Discover a broker id from describeCluster.
    let broker_id = admin
        .describe_cluster(DescribeClusterOptions::new())
        .nodes()
        .get()
        .await
        .expect("cluster nodes")
        .first()
        .expect("at least one broker")
        .id();

    let resource = ConfigResource::new(ConfigResourceType::Broker, broker_id.to_string());
    let result = admin.describe_configs(std::slice::from_ref(&resource), DescribeConfigsOptions::new());
    let config = result.values()[&resource].get().await.expect("describe broker configs");

    // A broker reports many configs; a couple of universal ones must exist.
    assert!(
        config.get("broker.id").is_some() || config.get("node.id").is_some(),
        "broker.id/node.id should be present"
    );
    assert!(config.get("log.retention.ms").is_some(), "log.retention.ms should be present");

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

#[tokio::test]
async fn test_list_config_resources_lists_resources() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    // Create a topic so at least one TOPIC config resource is listable.
    let topic = ctx.topic("admin_list_config_resources");
    admin
        .create_topics(&[NewTopic::new(topic.clone(), 1, 1)], CreateTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("create topic");

    // An empty type set requests all supported config-resource types.
    let resources = admin
        .list_config_resources(&std::collections::HashSet::new(), ListConfigResourcesOptions::new())
        .all()
        .get()
        .await
        .expect("list config resources");

    assert!(
        resources.iter().any(|r| r.resource_type() == ConfigResourceType::Broker),
        "expected at least one BROKER config resource"
    );
    assert!(
        resources
            .iter()
            .any(|r| r.resource_type() == ConfigResourceType::Topic && r.name() == topic),
        "expected the created topic among the TOPIC config resources"
    );

    admin
        .delete_topics(TopicCollection::of_topic_names(vec![topic]), DeleteTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("delete topic");
    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

/// End-to-end check of the deprecated `listClientMetricsResources` RPC against
/// a real Kafka 4.2.0 broker. Seeds a KIP-714 client-metrics subscription with
/// `incrementalAlterConfigs` on a `CLIENT_METRICS` config resource, then asserts
/// it shows up in the listing. Mirrors the intent of Java's
/// `KafkaAdminClientIntegrationTest` client-metrics coverage.
#[tokio::test]
#[allow(deprecated)]
async fn test_list_client_metrics_resources_lists_subscription() {
    use confluent_kafka::admin::ListClientMetricsResourcesOptions;

    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    // A client-metrics subscription is a CLIENT_METRICS config resource; create
    // one by setting its subscription configs (KIP-714). `interval.ms` is the
    // push interval; `metrics` scopes which client metrics are collected.
    let subscription = ctx.topic("admin_client_metrics_sub");
    let resource = ConfigResource::new(ConfigResourceType::ClientMetrics, subscription.clone());
    let ops = vec![
        AlterConfigOp::new(
            ConfigEntry::new("interval.ms".to_string(), Some("60000".to_string())),
            OpType::Set,
        ),
        AlterConfigOp::new(ConfigEntry::new("metrics".to_string(), Some(String::new())), OpType::Set),
    ];
    let mut configs = HashMap::new();
    configs.insert(resource.clone(), ops);
    admin
        .incremental_alter_configs(&configs, AlterConfigsOptions::new())
        .all()
        .get()
        .await
        .expect("create client-metrics subscription");

    let listings = admin
        .list_client_metrics_resources(ListClientMetricsResourcesOptions::new())
        .all()
        .get()
        .await
        .expect("list client metrics resources");
    assert!(
        listings.iter().any(|l| l.name() == subscription),
        "expected the created subscription {subscription:?} in {listings:?}"
    );

    // Delete the subscription so the broker is left clean.
    let delete_ops = vec![AlterConfigOp::new(
        ConfigEntry::new("interval.ms".to_string(), None),
        OpType::Delete,
    )];
    let mut delete_configs = HashMap::new();
    delete_configs.insert(resource, delete_ops);
    let _ = admin
        .incremental_alter_configs(&delete_configs, AlterConfigsOptions::new())
        .all()
        .get()
        .await;

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}
