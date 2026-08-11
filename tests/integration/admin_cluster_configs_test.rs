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

//! Integration tests for the admin cluster & config RPCs against a real Kafka
//! 4.2.0 broker.
//!
//! Mirrors the describeCluster / describeConfigs / incrementalAlterConfigs /
//! listConfigResources / listClientMetricsResources scenarios in Java's
//! `KafkaAdminClientIntegrationTest`, exercising the real network engine end to
//! end rather than the `MockClient` unit-test harness.
//!
//! Each scenario is a body generic over
//! [`AdminBackendFactory`](crate::common::backend_factory::AdminBackendFactory)
//! and registered with [`multilanguage_admin_test!`], so it runs against the
//! native Rust client, the Python sync binding, the Python asyncio binding and
//! the C FFI — a disagreement between them shows up as three backends agreeing
//! and one not (see `design/history/Milestone-11/PLAN-multilanguage-admin.md`).
//! With only `integration-tests` enabled the `__rust` arm is the whole
//! expansion, and it drives the same production `Admin` trait against the same
//! broker as the single-backend tests these scenarios were converted from.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use confluent_kafka::admin::{
    AlterConfigOp, AlterConfigsOptions, ConfigEntry, DeleteTopicsOptions, DescribeClusterOptions,
    DescribeConfigsOptions, ListConfigResourcesOptions, OpType,
};
use confluent_kafka::common::acl::AclOperation;
use confluent_kafka::common::config::{ConfigResource, ConfigResourceType};

use crate::common::admin_backend::{AdminBackend, ConfigEntryView, ConfigView, admin_for, all_of, create_topic};
use crate::common::backend_factory::AdminBackendFactory;
use crate::common::test_context::TestContext;
use crate::common::test_utils::retry_on_exception_with_timeout;
use crate::multilanguage_admin_test;

/// How long to retry a config read-back before failing. Mirrors the `5000L`
/// that `ClientQuotasRequestTest` passes to
/// `TestUtils.retryOnExceptionWithTimeout`.
const CONFIG_PROPAGATION_TIMEOUT: Duration = Duration::from_secs(5);

/// Java's `ConfigEntry.ConfigSource` enum constants. The wire carries the
/// constant *name* because that enum has no numeric id in Java, so an unknown
/// string here means a backend invented or dropped one.
const CONFIG_SOURCES: [&str; 9] = [
    "DYNAMIC_TOPIC_CONFIG",
    "DYNAMIC_BROKER_LOGGER_CONFIG",
    "DYNAMIC_BROKER_CONFIG",
    "DYNAMIC_DEFAULT_BROKER_CONFIG",
    "DYNAMIC_CLIENT_METRICS_CONFIG",
    "DYNAMIC_GROUP_CONFIG",
    "STATIC_BROKER_CONFIG",
    "DEFAULT_CONFIG",
    "UNKNOWN",
];

/// Java's `ConfigEntry.ConfigType` enum constants, for the same reason.
const CONFIG_TYPES: [&str; 10] = [
    "UNKNOWN", "BOOLEAN", "STRING", "INT", "SHORT", "LONG", "DOUBLE", "LIST", "CLASS", "PASSWORD",
];

/// Asserts the invariants that hold for *every* `describeConfigs` entry on every
/// backend, so a backend that drops or transposes one of the four fields slice G2
/// added to the wire fails here rather than passing unnoticed.
///
/// The two enums are checked against their full constant sets rather than a
/// specific expected value: which source a given config has is broker
/// configuration, but that it is one of Java's nine names is the contract.
/// `is_default` is checked against `source` because Java *derives* it
/// (`ConfigEntry.isDefault()` is `source == DEFAULT_CONFIG`), so the pair
/// disagreeing means a backend built the entry from two sources of truth.
fn assert_entry_well_formed(backend: &str, entry: &ConfigEntryView) {
    let source = entry
        .source
        .as_deref()
        .unwrap_or_else(|| panic!("{backend} backend: describeConfigs must report a source for {}", entry.name));
    assert!(
        CONFIG_SOURCES.contains(&source),
        "{backend} backend: {} has source {source:?}, not a ConfigSource constant",
        entry.name
    );
    let config_type = entry
        .config_type
        .as_deref()
        .unwrap_or_else(|| panic!("{backend} backend: describeConfigs must report a type for {}", entry.name));
    assert!(
        CONFIG_TYPES.contains(&config_type),
        "{backend} backend: {} has type {config_type:?}, not a ConfigType constant",
        entry.name
    );
    assert_eq!(
        entry.is_default,
        source == "DEFAULT_CONFIG",
        "{backend} backend: {}'s is_default must be `source == DEFAULT_CONFIG` (source {source:?})",
        entry.name
    );
    // Java suppresses the value of a sensitive config, so the broker never sends
    // one; a backend reporting both is reporting something it cannot have.
    if entry.is_sensitive {
        assert_eq!(
            entry.value, None,
            "{backend} backend: sensitive config {} must not carry a value",
            entry.name
        );
    }
    for synonym in &entry.synonyms {
        assert!(
            CONFIG_SOURCES.contains(&synonym.source.as_str()),
            "{backend} backend: synonym {} of {} has source {:?}, not a ConfigSource constant",
            synonym.name,
            entry.name,
            synonym.source
        );
    }
}

/// Describes `resource` and returns its config, panicking with the backend's
/// name on either failure level.
async fn config_of<B: AdminBackend>(admin: &B, resource: &ConfigResource) -> ConfigView {
    let described = admin
        .describe_configs(std::slice::from_ref(resource), DescribeConfigsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{} backend: describe configs: {e}", admin.name()));
    described
        .get(resource)
        .unwrap_or_else(|| panic!("{} backend: {resource:?} missing from the result", admin.name()))
        .as_ref()
        .unwrap_or_else(|e| panic!("{} backend: describe configs {resource:?}: {e}", admin.name()))
        .clone()
}

/// Reads `retention.ms` for `resource`. `Ok(None)` means the entry is absent;
/// `Err` means the describe itself failed (retryable by the caller).
async fn retention_ms<B: AdminBackend>(admin: &B, resource: &ConfigResource) -> Result<Option<String>, String> {
    let described = admin
        .describe_configs(std::slice::from_ref(resource), DescribeConfigsOptions::new())
        .await
        .map_err(|e| format!("describe configs: {e}"))?
        .remove(resource)
        .ok_or("resource missing from describe_configs result")?
        .map_err(|e| format!("describe configs: {e}"))?;
    Ok(described.get("retention.ms").and_then(|entry| entry.value.clone()))
}

/// Applies one incremental config change and asserts every resource succeeded.
async fn alter<B: AdminBackend>(admin: &B, resource: &ConfigResource, ops: Vec<AlterConfigOp>, what: &str) {
    let mut configs = HashMap::new();
    configs.insert(resource.clone(), ops);
    let altered = admin
        .incremental_alter_configs(&configs, AlterConfigsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{} backend: {what}: {e}", admin.name()));
    all_of(&altered).unwrap_or_else(|e| panic!("{} backend: {what}: {e}", admin.name()));
}

/// Deletes `topics` and closes the client, asserting both.
async fn delete_and_close<B: AdminBackend>(admin: &B, topics: &[String]) {
    let deleted = admin
        .delete_topics(topics, DeleteTopicsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{} backend: delete topics: {e}", admin.name()));
    all_of(&deleted).unwrap_or_else(|e| panic!("{} backend: delete topics should succeed: {e}", admin.name()));
    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{} backend: close: {e}", admin.name()));
}

/// The id of the first node `describe_cluster` reports.
async fn first_broker_id<B: AdminBackend>(admin: &B) -> i32 {
    admin
        .describe_cluster(DescribeClusterOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{} backend: describe cluster: {e}", admin.name()))
        .nodes
        .first()
        .unwrap_or_else(|| panic!("{} backend: at least one broker", admin.name()))
        .id()
}

// ---------------------------------------------------------------------------
// Test bodies — generic over AdminBackendFactory
// ---------------------------------------------------------------------------

async fn describe_cluster_returns_nodes_controller_and_id<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let description = admin
        .describe_cluster(DescribeClusterOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe cluster: {e}"));
    assert!(
        !description.nodes.is_empty(),
        "{backend} backend: cluster should report at least one node"
    );

    let controller = description
        .controller
        .as_ref()
        .unwrap_or_else(|| panic!("{backend} backend: cluster should have a controller"));
    assert!(
        description.nodes.iter().any(|n| n.id() == controller.id()),
        "{backend} backend: controller {} should be one of the reported nodes {:?}",
        controller.id(),
        description.nodes.iter().map(|n| n.id()).collect::<Vec<_>>()
    );
    // Not in the original, and the point of the harness: the controller must be
    // a *real* node, not a placeholder. A fabricated
    // `ConsumerGroupDescription.coordinator()` (`host=""`, `port=-1`) is the
    // defect PLAN-multilanguage-admin.md cites as this milestone's motivation,
    // and `describeCluster` has the same shape.
    assert!(
        !controller.host().is_empty() && controller.port() > 0,
        "{backend} backend: controller must carry a real host:port, got {}:{}",
        controller.host(),
        controller.port()
    );

    assert!(
        !description.cluster_id.is_empty(),
        "{backend} backend: cluster id should be non-empty"
    );

    // `includeAuthorizedOperations` is honoured even with no authorizer
    // configured: a KRaft broker computes the operations `User:ANONYMOUS` may
    // perform, and as a super user that is most of them. So the reachable state
    // here is `Some(..)` rather than Java's null; the *null* branch is the
    // unreachable one on this fixture, since it needs a broker that omits the
    // field (`Integer.MIN_VALUE`, which
    // `src/admin/internals/admin_utils.rs::valid_acl_operations` maps to `None`).
    //
    // Every reported code must decode to a real operation. `AclOperation::Unknown`
    // is what `from_code` yields for a code it does not recognise, so a backend
    // that mangled the int32 list shows up here rather than as a silently smaller
    // set.
    let with_operations = admin
        .describe_cluster(DescribeClusterOptions::new().include_authorized_operations(true))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe cluster with operations: {e}"));
    let operations = with_operations
        .authorized_operations
        .as_ref()
        .unwrap_or_else(|| panic!("{backend} backend: the broker reports the operations when they are requested"));
    assert!(
        operations.contains(&AclOperation::Describe) && operations.contains(&AclOperation::DescribeConfigs),
        "{backend} backend: a super user may DESCRIBE and DESCRIBE_CONFIGS the cluster, got {operations:?}"
    );
    assert!(
        !operations.contains(&AclOperation::Unknown) && !operations.contains(&AclOperation::Any),
        "{backend} backend: every reported operation code must decode, got {operations:?}"
    );
    // Requesting nothing must not report the set — the difference between "the
    // broker did not report them" and "reported none", which the wire carries as
    // an absent wrapper rather than an empty list.
    assert_eq!(
        description.authorized_operations, None,
        "{backend} backend: operations must be absent when they were not requested"
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

async fn describe_configs_topic_returns_defaults<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let topic = ctx.topic("admin_describe_topic_config");
    // Creates the topic and waits for its metadata to reach the brokers, so the
    // describes below cannot race creation. Mirrors Java's
    // `TestUtils.createTopicWithAdmin`.
    create_topic(&admin, &topic, 1, 1).await;

    let resource = ConfigResource::new(ConfigResourceType::Topic, topic.clone());
    let config = config_of(&admin, &resource).await;

    // A topic should report standard default configs.
    assert!(
        config.get("retention.ms").is_some(),
        "{backend} backend: retention.ms should be present"
    );
    assert!(
        config.get("cleanup.policy").is_some(),
        "{backend} backend: cleanup.policy should be present"
    );
    // Every entry's metadata must be well formed, which is what proves the four
    // fields slice G2 put on the wire actually cross.
    for entry in &config.entries {
        assert_entry_well_formed(backend, entry);
    }

    delete_and_close(&admin, &[topic]).await;
    ctx.cleanup().await;
}

async fn incremental_alter_configs_set_and_delete_topic_config<F: AdminBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let topic = ctx.topic("admin_alter_topic_config");
    // Creates the topic and waits for its metadata to reach the brokers, so the
    // describes below cannot race creation. Mirrors Java's
    // `TestUtils.createTopicWithAdmin`.
    create_topic(&admin, &topic, 1, 1).await;

    let resource = ConfigResource::new(ConfigResourceType::Topic, topic.clone());

    // SET retention.ms to a custom value.
    alter(
        &admin,
        &resource,
        vec![AlterConfigOp::new(
            ConfigEntry::new("retention.ms".to_string(), Some("123456789".to_string())),
            OpType::Set,
        )],
        "set retention.ms",
    )
    .await;

    // Config changes reach the brokers asynchronously, so retry the read-back
    // until it holds (Java: `TestUtils.retryOnExceptionWithTimeout` around the
    // describe-and-assert).
    retry_on_exception_with_timeout(CONFIG_PROPAGATION_TIMEOUT, || async {
        let value = retention_ms(&admin, &resource).await?;
        if value.as_deref() == Some("123456789") {
            Ok(())
        } else {
            Err(format!(
                "{backend} backend: retention.ms should reflect the set value, got {value:?}"
            ))
        }
    })
    .await;

    // A set config is no longer a default, and its source says which kind of
    // dynamic override it now is. Not in the original — the original had no
    // `source` to read — and it is the strongest available check that the four
    // backends carry the field rather than defaulting it.
    let entry = config_of(&admin, &resource)
        .await
        .get("retention.ms")
        .unwrap_or_else(|| panic!("{backend} backend: retention.ms should be present after the set"))
        .clone();
    assert_eq!(
        entry.source.as_deref(),
        Some("DYNAMIC_TOPIC_CONFIG"),
        "{backend} backend: a per-topic override is a DYNAMIC_TOPIC_CONFIG"
    );
    assert!(
        !entry.is_default,
        "{backend} backend: an explicitly set config is not a default"
    );

    // DELETE retention.ms reverts it to the default.
    alter(
        &admin,
        &resource,
        vec![AlterConfigOp::new(
            ConfigEntry::new("retention.ms".to_string(), None),
            OpType::Delete,
        )],
        "delete retention.ms",
    )
    .await;

    // Likewise retry the post-delete read-back. `retention.ms` must still be
    // present (as the broker default) but no longer hold the custom value.
    retry_on_exception_with_timeout(CONFIG_PROPAGATION_TIMEOUT, || async {
        let described = admin
            .describe_configs(std::slice::from_ref(&resource), DescribeConfigsOptions::new())
            .await
            .map_err(|e| format!("describe after delete: {e}"))?
            .remove(&resource)
            .ok_or("resource missing from describe_configs result")?
            .map_err(|e| format!("describe after delete: {e}"))?;
        let entry = described
            .get("retention.ms")
            .ok_or("retention.ms still present as a default")?
            .clone();
        if entry.value.as_deref() == Some("123456789") {
            return Err("retention.ms should no longer be the custom value after delete".to_string());
        }
        Ok(())
    })
    .await;

    delete_and_close(&admin, &[topic]).await;
    ctx.cleanup().await;
}

async fn describe_configs_broker_returns_broker_configs<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    // Discover a broker id from describeCluster.
    let broker_id = first_broker_id(&admin).await;

    let resource = ConfigResource::new(ConfigResourceType::Broker, broker_id.to_string());
    let config = config_of(&admin, &resource).await;

    // A broker reports many configs; a couple of universal ones must exist.
    assert!(
        config.get("broker.id").is_some() || config.get("node.id").is_some(),
        "{backend} backend: broker.id/node.id should be present"
    );
    assert!(
        config.get("log.retention.ms").is_some(),
        "{backend} backend: log.retention.ms should be present"
    );
    for entry in &config.entries {
        assert_entry_well_formed(backend, entry);
    }

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// `describeConfigs(includeSynonyms, includeDocumentation)` on a broker
/// resource.
///
/// Not a conversion: no committed test sets either flag, so the four
/// `ConfigEntry` fields slice G2 added to the wire — `source`, `configType`,
/// `documentation` and the precedence-ordered synonym list — would otherwise be
/// wired through three servers and never populated. Broker configs are the only
/// resource that reports synonyms on a single-node cluster, since a topic config
/// has no static-broker or default-broker layer beneath it. Java asserts the
/// same shape in `PlaintextAdminIntegrationTest.testDescribeConfigsForLog4jLogLevels`
/// and `testDescribeConfigsWithDocumentation`.
async fn describe_configs_reports_synonyms_and_documentation<F: AdminBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let broker_id = first_broker_id(&admin).await;
    let resource = ConfigResource::new(ConfigResourceType::Broker, broker_id.to_string());
    let described = admin
        .describe_configs(
            std::slice::from_ref(&resource),
            DescribeConfigsOptions::new().include_synonyms(true).include_documentation(true),
        )
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe configs: {e}"));
    let config = described[&resource]
        .as_ref()
        .unwrap_or_else(|e| panic!("{backend} backend: describe broker configs: {e}"))
        .clone();

    for entry in &config.entries {
        assert_entry_well_formed(backend, entry);
    }

    // With synonyms requested, the entry's own value heads a non-empty
    // precedence list (Java: "The list starts with the value returned in this
    // ConfigEntry"), so at least one broker config must carry one — a backend
    // that dropped the repeated field entirely fails here.
    let with_synonyms = config
        .entries
        .iter()
        .find(|entry| !entry.synonyms.is_empty())
        .unwrap_or_else(|| {
            panic!("{backend} backend: include_synonyms must report synonyms for at least one broker config")
        });
    assert_eq!(
        with_synonyms.synonyms[0].source.as_str(),
        with_synonyms.source.as_deref().unwrap_or_default(),
        "{backend} backend: the first synonym of {} is the entry's own value, so it shares its source",
        with_synonyms.name
    );

    // With documentation requested, at least one entry must carry a non-empty
    // string. Not every config is documented, so the assertion is over the set.
    let documented = config
        .entries
        .iter()
        .filter(|entry| entry.documentation.as_deref().is_some_and(|doc| !doc.is_empty()))
        .count();
    assert!(
        documented > 0,
        "{backend} backend: include_documentation must document at least one of the {} broker configs",
        config.entries.len()
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

async fn list_config_resources_lists_resources<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    // Create a topic so at least one TOPIC config resource is listable.
    let topic = ctx.topic("admin_list_config_resources");
    // Creates the topic and waits for its metadata to reach the brokers, so the
    // describes below cannot race creation. Mirrors Java's
    // `TestUtils.createTopicWithAdmin`.
    create_topic(&admin, &topic, 1, 1).await;

    // An empty type set requests all supported config-resource types.
    let resources = admin
        .list_config_resources(&HashSet::new(), ListConfigResourcesOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: list config resources: {e}"));

    assert!(
        resources.iter().any(|r| r.resource_type() == ConfigResourceType::Broker),
        "{backend} backend: expected at least one BROKER config resource"
    );
    assert!(
        resources
            .iter()
            .any(|r| r.resource_type() == ConfigResourceType::Topic && r.name() == topic),
        "{backend} backend: expected the created topic among the TOPIC config resources"
    );

    // A filtered listing must not leak the other types — the only observable
    // effect of the `resource_types` argument, and a wire field that is
    // otherwise never exercised with a non-empty value.
    let topics_only = admin
        .list_config_resources(&HashSet::from([ConfigResourceType::Topic]), ListConfigResourcesOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: list TOPIC config resources: {e}"));
    assert!(
        topics_only.iter().any(|r| r.name() == topic),
        "{backend} backend: the created topic should be in the TOPIC-only listing"
    );
    assert!(
        topics_only.iter().all(|r| r.resource_type() == ConfigResourceType::Topic),
        "{backend} backend: a TOPIC-only listing must contain only TOPIC resources, got {:?}",
        topics_only.iter().map(|r| r.resource_type()).collect::<HashSet<_>>()
    );

    delete_and_close(&admin, &[topic]).await;
    ctx.cleanup().await;
}

/// End-to-end check of the deprecated `listClientMetricsResources` RPC. Seeds a
/// KIP-714 client-metrics subscription with `incrementalAlterConfigs` on a
/// `CLIENT_METRICS` config resource, then asserts it shows up in the listing.
/// Mirrors the intent of Java's `KafkaAdminClientIntegrationTest` client-metrics
/// coverage.
async fn list_client_metrics_resources_lists_subscription<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    #[allow(deprecated)]
    use confluent_kafka::admin::ListClientMetricsResourcesOptions;

    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    // A client-metrics subscription is a CLIENT_METRICS config resource; create
    // one by setting its subscription configs (KIP-714). `interval.ms` is the
    // push interval; `metrics` scopes which client metrics are collected.
    let subscription = ctx.topic("admin_client_metrics_sub");
    let resource = ConfigResource::new(ConfigResourceType::ClientMetrics, subscription.clone());
    alter(
        &admin,
        &resource,
        vec![
            AlterConfigOp::new(
                ConfigEntry::new("interval.ms".to_string(), Some("60000".to_string())),
                OpType::Set,
            ),
            AlterConfigOp::new(ConfigEntry::new("metrics".to_string(), Some(String::new())), OpType::Set),
        ],
        "create client-metrics subscription",
    )
    .await;

    // The new subscription becomes visible to the listing asynchronously.
    retry_on_exception_with_timeout(CONFIG_PROPAGATION_TIMEOUT, || async {
        #[allow(deprecated)]
        let listings = admin
            .list_client_metrics_resources(ListClientMetricsResourcesOptions::new())
            .await
            .map_err(|e| format!("list client metrics resources: {e}"))?;
        if listings.iter().any(|l| l.name() == subscription) {
            Ok(())
        } else {
            Err(format!(
                "{backend} backend: expected the created subscription {subscription:?} in {listings:?}"
            ))
        }
    })
    .await;

    // The same subscription is listable through the non-deprecated route, which
    // is what Java says `listClientMetricsResources` was replaced by. Both must
    // agree, on every backend.
    let via_config_resources = admin
        .list_config_resources(
            &HashSet::from([ConfigResourceType::ClientMetrics]),
            ListConfigResourcesOptions::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: list CLIENT_METRICS config resources: {e}"));
    assert!(
        via_config_resources.iter().any(|r| r.name() == subscription),
        "{backend} backend: listConfigResources(CLIENT_METRICS) must report the same subscription as the \
         deprecated listClientMetricsResources, got {via_config_resources:?}"
    );

    // Delete the subscription so the broker is left clean. Unlike the alters
    // above this is not asserted: the original tolerated a failure here too,
    // since a leftover subscription cannot fail a later scenario.
    let mut delete_configs = HashMap::new();
    delete_configs.insert(
        resource,
        vec![AlterConfigOp::new(
            ConfigEntry::new("interval.ms".to_string(), None),
            OpType::Delete,
        )],
    );
    let _ = admin
        .incremental_alter_configs(&delete_configs, AlterConfigsOptions::new())
        .await;

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

multilanguage_admin_test!(
    test_describe_cluster_returns_nodes_controller_and_id,
    describe_cluster_returns_nodes_controller_and_id
);
multilanguage_admin_test!(
    test_describe_configs_topic_returns_defaults,
    describe_configs_topic_returns_defaults
);
multilanguage_admin_test!(
    test_incremental_alter_configs_set_and_delete_topic_config,
    incremental_alter_configs_set_and_delete_topic_config
);
multilanguage_admin_test!(
    test_describe_configs_broker_returns_broker_configs,
    describe_configs_broker_returns_broker_configs
);
multilanguage_admin_test!(
    test_describe_configs_reports_synonyms_and_documentation,
    describe_configs_reports_synonyms_and_documentation
);
multilanguage_admin_test!(
    test_list_config_resources_lists_resources,
    list_config_resources_lists_resources
);
multilanguage_admin_test!(
    test_list_client_metrics_resources_lists_subscription,
    list_client_metrics_resources_lists_subscription
);
