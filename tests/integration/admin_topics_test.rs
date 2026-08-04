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

//! Integration tests for the `KafkaAdminClient` topic CRUD RPCs against a real
//! Kafka 4.2.0 broker.
//!
//! Mirrors the topic-management scenarios in Java's `KafkaAdminClientIntegrationTest`
//! (create / list / describe / delete), exercising the real network engine end to
//! end rather than the `MockClient` unit-test harness.

use std::collections::HashMap;
use std::time::Duration;

use confluent_kafka::admin::{
    Admin, AdminClientConfig, CreateTopicsOptions, DeleteTopicsOptions, DescribeTopicsOptions, ListTopicsOptions,
    NewTopic, new_admin_client,
};
use confluent_kafka::common::TopicCollection;
use confluent_kafka::common::protocol::Errors;

use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;
use crate::common::test_utils::{create_topic, wait_for_all_partitions_metadata};

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

/// Poll `list_topics` until `topic` appears (or times out), tolerating the brief
/// metadata-propagation window after a create.
async fn wait_until_listed(admin: &dyn Admin, topic: &str, present: bool) -> bool {
    for _ in 0..50 {
        let names = admin
            .list_topics(ListTopicsOptions::new())
            .names()
            .get()
            .await
            .expect("list topics");
        if names.contains(topic) == present {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

#[tokio::test]
async fn test_create_then_list_and_describe_topics() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let topic = ctx.topic("admin_create_list");
    let result = admin.create_topics(&[NewTopic::new(topic.clone(), 2, 1)], CreateTopicsOptions::new());
    result.all().get().await.expect("create topics should succeed");

    // The topic shows up in list_topics.
    assert!(wait_until_listed(admin.as_ref(), &topic, true).await, "topic should be listed");
    // As above: being listed does not imply the describe target's metadata cache
    // is populated, and the assertions below depend on the partition count.
    wait_for_all_partitions_metadata(admin.as_ref(), &topic, 2).await;

    // describe_topics reports the partition count and replication factor.
    let described = admin
        .describe_topics(
            TopicCollection::of_topic_names(vec![topic.clone()]),
            DescribeTopicsOptions::new(),
        )
        .all_topic_names()
        .expect("described by name")
        .get()
        .await
        .expect("describe topics should succeed");
    let desc = &described[&topic];
    assert_eq!(desc.name(), topic);
    assert_eq!(desc.partitions().len(), 2, "should have 2 partitions");
    for partition in desc.partitions() {
        assert_eq!(partition.replicas().len(), 1, "replication factor should be 1");
    }

    // Clean up.
    admin
        .delete_topics(TopicCollection::of_topic_names(vec![topic.clone()]), DeleteTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("delete topics should succeed");
    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

#[tokio::test]
async fn test_describe_nonexistent_topic_is_unknown() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let topic = ctx.topic("admin_nonexistent");
    let result = admin.describe_topics(
        TopicCollection::of_topic_names(vec![topic.clone()]),
        DescribeTopicsOptions::new(),
    );
    let err = result.topic_name_values().unwrap()[&topic]
        .get()
        .await
        .expect_err("describing a nonexistent topic should fail");
    assert_eq!(
        err.error(),
        Errors::UnknownTopicOrPartition,
        "expected UNKNOWN_TOPIC_OR_PARTITION, got {err:?}"
    );

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

#[tokio::test]
async fn test_delete_topics_removes_them() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let topic = ctx.topic("admin_delete");
    create_topic(admin.as_ref(), &topic, 1, 1).await;
    assert!(
        wait_until_listed(admin.as_ref(), &topic, true).await,
        "topic should be listed after create"
    );

    // Delete and verify it is gone.
    admin
        .delete_topics(TopicCollection::of_topic_names(vec![topic.clone()]), DeleteTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("delete topics should succeed");
    assert!(
        wait_until_listed(admin.as_ref(), &topic, false).await,
        "topic should be gone after delete"
    );

    // Describing the deleted topic now fails with UNKNOWN_TOPIC_OR_PARTITION.
    let describe = admin.describe_topics(
        TopicCollection::of_topic_names(vec![topic.clone()]),
        DescribeTopicsOptions::new(),
    );
    let err = describe.topic_name_values().unwrap()[&topic]
        .get()
        .await
        .expect_err("describing a deleted topic should fail");
    assert_eq!(err.error(), Errors::UnknownTopicOrPartition);

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

#[tokio::test]
async fn test_create_multiple_topics_partition_round_trip() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let topic_a = ctx.topic("admin_multi_a");
    let topic_b = ctx.topic("admin_multi_b");
    admin
        .create_topics(
            &[
                NewTopic::new(topic_a.clone(), 3, 1),
                NewTopic::new(topic_b.clone(), 1, 1),
            ],
            CreateTopicsOptions::new(),
        )
        .all()
        .get()
        .await
        .expect("create topics should succeed");
    assert!(wait_until_listed(admin.as_ref(), &topic_a, true).await);
    assert!(wait_until_listed(admin.as_ref(), &topic_b, true).await);
    // Appearing in `list_topics` does not guarantee that the broker answering
    // the `describe_topics` below has the topic in its metadata cache yet, so
    // wait on the partition counts the assertions rely on.
    wait_for_all_partitions_metadata(admin.as_ref(), &topic_a, 3).await;
    wait_for_all_partitions_metadata(admin.as_ref(), &topic_b, 1).await;

    let described = admin
        .describe_topics(
            TopicCollection::of_topic_names(vec![topic_a.clone(), topic_b.clone()]),
            DescribeTopicsOptions::new(),
        )
        .all_topic_names()
        .expect("described by name")
        .get()
        .await
        .expect("describe topics should succeed");
    assert_eq!(described[&topic_a].partitions().len(), 3);
    assert_eq!(described[&topic_b].partitions().len(), 1);

    // Clean up.
    admin
        .delete_topics(
            TopicCollection::of_topic_names(vec![topic_a.clone(), topic_b.clone()]),
            DeleteTopicsOptions::new(),
        )
        .all()
        .get()
        .await
        .expect("delete topics should succeed");
    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}
