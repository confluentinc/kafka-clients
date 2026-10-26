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

//! An admin client configured with `bootstrap.controllers` (KIP-919) against a
//! real cluster.
//!
//! Translated from Java's
//! `server/src/test/java/org/apache/kafka/server/BootstrapControllersIntegrationTest.java`
//! (KAFKA-15466; the `server` module, which `check-java-name` does not index,
//! hence no `doc(alias)`). Each Java test that pairs a controller arm with a
//! broker arm (`testXByControllers` / `testX`) is two tests here too. The other
//! KIP-919 integration tests are in `admin_controllers_test.rs` (the controller
//! arm of `KRaftClusterTest.testUnregisterControllerError`) and
//! `src/integration_tests/describe_features_test.rs`
//! (`DescribeFeaturesTest.testApiVersions`).
//!
//! # How the harness differs from Java's
//!
//! - **Topology.** Java's `@ClusterTestDefaults(types = {Type.KRAFT})` runs
//!   separate controller nodes (ids from 3000). Here every node is a combined
//!   (`broker,controller`) node of a pooled [`Type::CoKraft`] cluster, so a
//!   node's controller id is its broker id: a controller-only container cannot
//!   publish a host listener (the image rejects `KAFKA_ADVERTISED_LISTENERS` on
//!   a controller). The controllers are reached through the PLAINTEXT
//!   `CONTROLLERHOST` listener ([`KafkaCluster::bootstrap_controllers`]), and the
//!   broker arms use the PLAINTEXT broker listener, as Java's `Admin.create`
//!   with no security keys does, whatever protocol the run selects.
//! - **Shared clusters.** Java gives every test a fresh cluster. Here the
//!   clusters are pooled, so:
//!   - the ACL tests use a resource of their own and filter on it, where Java
//!     filters with `AclBindingFilter.ANY`: the authorizer cluster is shared
//!     with `admin_acls_test.rs`;
//!   - the tests that change dynamic broker configs run on the shared default
//!     cluster, where each Java test gets a cluster of its default shape to
//!     itself. They run one at a time ([`DYNAMIC_CONFIG_PERMIT`]) and delete
//!     every config they set when they finish, pass or fail
//!     ([`restoring_configs`]). No other suite touches those configs on this
//!     cluster (the elections suite sets the throttle rates on the 3-broker
//!     one). A cluster of their own would add a resident cluster to a suite
//!     that already fills the Docker VM's memory;
//!   - `testDescribeConfigs` runs on the default cluster's single node, where
//!     Java uses two brokers with `min.insync.replicas=2`: a server property
//!     there would need a cluster of its own, and setting it dynamically
//!     would fail every `acks=all` produce to a one-replica topic on the
//!     shared cluster. Either way, the controller writes the static
//!     `min.insync.replicas` as a cluster-level config when it activates a
//!     cluster whose bootstrap metadata enables ELR (KIP-966,
//!     `ActivationRecordsGenerator.recordsForEmptyLog`, given
//!     `getStaticallyConfiguredMinInsyncReplicas()` by `QuorumController`), so
//!     the test expects the default, `1`, with Java's source.
//! - **The native client only.** The gRPC backends of the multilanguage suite
//!   run in containers, which cannot reach the host-advertised controller
//!   listener.
//!
//! # Not translated
//!
//! - `testDescribeMetadataQuorumByControllers` / `testDescribeMetadataQuorum`:
//!   this client has no `describeMetadataQuorum`.
//! - The `verifySocketServerMaxConnectionsUpdated` /
//!   `verifySocketServerMaxConnectionCreationRateUpdated` checks inside
//!   `testIncrementalAlterConfigs`: they read the node's `SocketServer` and
//!   metrics by reflection, inside the JVM. The configs they check are read back
//!   with `describeConfigs`, as Java also does.
//! - `testIncrementalAlterConfigsBySingleControllerWithDynamicQuorum` /
//!   `testIncrementalAlterConfigsByAllControllersWithDynamicQuorum`
//!   (`standalone = true`): the harness formats a static quorum
//!   (`controller.quorum.voters`), not a dynamic one.
//! - `clients-integration-tests`' `StaticBrokerConfigTest`: three of its four
//!   tests use `admin(Map.of(), true)`, to describe the configs of a
//!   controller-only node (id 3000) and compare them with a broker's. Two of
//!   them give only one of the two nodes a static config. That needs separate
//!   controller nodes reachable from the host, which this harness cannot
//!   provide (see above).
//!
//! [`KafkaCluster::bootstrap_controllers`]: crate::common::kafka_cluster::KafkaCluster::bootstrap_controllers
//! [`Type::CoKraft`]: crate::common::cluster_config::Type::CoKraft

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::time::Duration;

use confluent_kafka::admin::config_entry::ConfigSource;
use confluent_kafka::admin::{
    Admin, AdminClient, AdminClientConfig, AlterConfigOp, ConfigEntry, FeatureUpdate, FinalizedVersionRange,
    NewPartitionReassignment, NewTopic, OffsetSpec, OpType, UpgradeType,
};
use confluent_kafka::common::acl::{
    AccessControlEntry, AccessControlEntryFilter, AclBinding, AclBindingFilter, AclOperation, AclPermissionType,
};
use confluent_kafka::common::config::{ConfigResource, config_resource};
use confluent_kafka::common::resource::{PatternType, ResourcePattern, ResourceType};
use confluent_kafka::common::{Error, TopicPartition};

use crate::common::cluster_config::{ClusterConfig, authorizer_single_broker};
use crate::common::kafka_cluster;
use crate::common::test_context::TestContext;
use crate::common::test_utils::{retry_on_error_with_timeout, wait_until_true_with_timeout};

/// How long a test waits for a call to complete (Java's `get(1, TimeUnit.MINUTES)`).
const CALL_TIMEOUT: Duration = Duration::from_secs(60);

/// Java's `TestUtils.retryOnExceptionWithTimeout(30_000, ..)` in `verifyConfigValue`.
const CONFIG_PROPAGATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Java's `TestUtils.waitForCondition` default, 15 s.
const WAIT_FOR_CONDITION_MS: u64 = 15_000;

/// Java's `UnsupportedEndpointTypeException` text (`KafkaAdminClient.java:1711-1712`).
const UNSUPPORTED_ENDPOINT_MESSAGE: &str =
    "This Admin API is not yet supported when communicating directly with the controller quorum.";

/// Java's `adminConfig(clusterInstance, usingBootstrapControllers)`:
/// `bootstrap.controllers` set to the controllers, or `bootstrap.servers` to
/// the brokers, and nothing else.
fn admin_config(ctx: &TestContext, using_bootstrap_controllers: bool) -> HashMap<String, String> {
    if using_bootstrap_controllers {
        HashMap::from([(
            AdminClientConfig::BOOTSTRAP_CONTROLLERS_CONFIG.to_string(),
            bootstrap_controllers(ctx).to_string(),
        )])
    } else {
        HashMap::from([(
            AdminClientConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
            ctx.bootstrap_servers().to_string(),
        )])
    }
}

/// Java's `clusterInstance.bootstrapControllers()`.
fn bootstrap_controllers(ctx: &TestContext) -> &str {
    ctx.cluster()
        .bootstrap_controllers()
        .expect("a combined node publishes its controller listener")
}

/// Java's `Admin.create(config)`.
fn create_admin(config: &HashMap<String, String>) -> Box<dyn Admin> {
    AdminClient::create(AdminClientConfig::new(config).expect("valid admin config")).expect("create admin client")
}

/// Lets one test at a time change dynamic broker configs on the shared
/// default cluster: the two `incrementalAlterConfigs` arms set the same
/// configs, and one arm's clean-up would otherwise race the other's read-back.
static DYNAMIC_CONFIG_PERMIT: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

/// Runs `body`, which sets dynamic broker configs on `ctx`'s cluster, then
/// deletes the configs `restore` names (through the brokers), whether `body`
/// passed or panicked, and then re-raises its panic. Holds
/// [`DYNAMIC_CONFIG_PERMIT`] throughout.
async fn restoring_configs<F>(ctx: &TestContext, restore: HashMap<ConfigResource, Vec<String>>, body: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let _permit = DYNAMIC_CONFIG_PERMIT.acquire().await.expect("the permit is never closed");
    let outcome = tokio::spawn(body).await;
    let deletions: HashMap<ConfigResource, Vec<AlterConfigOp>> = restore
        .into_iter()
        .map(|(resource, names)| {
            let ops = names
                .into_iter()
                .map(|name| AlterConfigOp::new(ConfigEntry::new(name, None), OpType::Delete))
                .collect();
            (resource, ops)
        })
        .collect();
    let admin = create_admin(&admin_config(ctx, false));
    let restored = admin
        .incremental_alter_configs(&deletions)
        .all()
        .get_with_timeout(CALL_TIMEOUT)
        .await;
    admin.close_with_timeout(Duration::from_secs(5)).await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic.into_panic());
    }
    restored.expect("the configs the test set are deleted");
}

/// A BROKER config resource: the default one for `""`, a node's otherwise.
fn broker_resource(name: &str) -> ConfigResource {
    ConfigResource::new(config_resource::Type::Broker, name.to_string())
}

fn set(name: &str, value: &str) -> AlterConfigOp {
    AlterConfigOp::new(ConfigEntry::new(name.to_string(), Some(value.to_string())), OpType::Set)
}

/// Translates `testPutBrokersInBootstrapControllersConfig`: the brokers given
/// as controllers answer the CONTROLLER `DescribeCluster` with
/// `MISMATCHED_ENDPOINT_TYPE`, which fails the metadata refresh and so the call.
#[tokio::test(flavor = "multi_thread")]
async fn test_put_brokers_in_bootstrap_controllers_config() {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let config = HashMap::from([(
        AdminClientConfig::BOOTSTRAP_CONTROLLERS_CONFIG.to_string(),
        ctx.bootstrap_servers().to_string(),
    )]);
    let admin = create_admin(&config);
    let error = admin
        .describe_cluster()
        .cluster_id()
        .get_with_timeout(CALL_TIMEOUT)
        .await
        .expect_err("a broker is not a controller");
    assert!(matches!(error, Error::MismatchedEndpointType(_)), "got {error:?}");
    assert_eq!(
        error.message(),
        "The request was sent to an endpoint of type BROKER, but we wanted an endpoint of type CONTROLLER"
    );
    admin.close_with_timeout(Duration::from_secs(5)).await;
}

/// Translates `testPutControllersInBootstrapBrokersConfig`: the controllers
/// given as brokers do not support the `Metadata` API, so the broker metadata
/// refresh fails with the client's `UnsupportedVersionException`, which
/// `updateFailed` keeps, and the call fails with it.
#[tokio::test(flavor = "multi_thread")]
async fn test_put_controllers_in_bootstrap_brokers_config() {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let config = HashMap::from([(
        AdminClientConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
        bootstrap_controllers(&ctx).to_string(),
    )]);
    let admin = create_admin(&config);
    let error = admin
        .describe_cluster()
        .cluster_id()
        .get_with_timeout(CALL_TIMEOUT)
        .await
        .expect_err("a controller does not serve Metadata");
    assert!(matches!(error, Error::UnsupportedVersion(_)), "got {error:?}");
    assert_eq!(error.message(), "The node does not support METADATA");
    admin.close_with_timeout(Duration::from_secs(5)).await;
}

/// Java's `testDescribeCluster(clusterInstance, usingBootstrapControllers)`.
async fn describe_cluster(using_bootstrap_controllers: bool) {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = create_admin(&admin_config(&ctx, using_bootstrap_controllers));
    let result = admin.describe_cluster();
    assert_eq!(
        result
            .cluster_id()
            .get_with_timeout(CALL_TIMEOUT)
            .await
            .expect("the cluster id"),
        ctx.cluster().cluster_id()
    );
    if using_bootstrap_controllers {
        let controller = result
            .controller()
            .get_with_timeout(CALL_TIMEOUT)
            .await
            .expect("the controller")
            .expect("a quorum has an active controller");
        assert!(
            ctx.cluster().controller_ids().contains(&controller.id()),
            "{controller:?} is one of the controllers"
        );

        // Beyond Java's test: the nodes are the controllers, described at the
        // addresses of the listener the client connected to.
        let nodes = result.nodes().get_with_timeout(CALL_TIMEOUT).await.expect("the controllers");
        let ids: BTreeSet<i32> = nodes.iter().map(|node| node.id()).collect();
        assert_eq!(ids, ctx.cluster().controller_ids());
        let addresses: BTreeSet<String> = nodes.iter().map(|node| format!("{}:{}", node.host(), node.port())).collect();
        assert_eq!(
            addresses,
            bootstrap_controllers(&ctx)
                .split(',')
                .map(str::to_string)
                .collect::<BTreeSet<String>>()
        );
    }
    admin.close_with_timeout(Duration::from_secs(5)).await;
}

/// Translates `testDescribeClusterByControllers`.
#[tokio::test(flavor = "multi_thread")]
async fn test_describe_cluster_by_controllers() {
    describe_cluster(true).await;
}

/// Translates `testDescribeCluster`.
#[tokio::test(flavor = "multi_thread")]
async fn test_describe_cluster() {
    describe_cluster(false).await;
}

/// Java's `testDescribeFeatures(clusterInstance, usingBootstrapControllers)`:
/// the finalized `metadata.version` is the cluster's.
///
/// Java reads the cluster's version from `clusterInstance.config()`. The
/// harness does not choose one: the image formats every node with its
/// release's latest production `metadata.version`, which is also the top of the
/// range the node supports, so that is the expected level here.
async fn describe_features(using_bootstrap_controllers: bool) {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = create_admin(&admin_config(&ctx, using_bootstrap_controllers));
    let features = admin
        .describe_features()
        .feature_metadata()
        .get_with_timeout(CALL_TIMEOUT)
        .await
        .expect("the feature metadata");
    let metadata_version = features
        .supported_features()
        .get("metadata.version")
        .expect("metadata.version is a supported feature")
        .max_version();
    assert_eq!(
        features.finalized_features().get("metadata.version"),
        Some(&FinalizedVersionRange::new(metadata_version, metadata_version).expect("a valid range"))
    );
    admin.close_with_timeout(Duration::from_secs(5)).await;
}

/// Translates `testDescribeFeaturesByControllers`.
#[tokio::test(flavor = "multi_thread")]
async fn test_describe_features_by_controllers() {
    describe_features(true).await;
}

/// Translates `testDescribeFeatures`.
#[tokio::test(flavor = "multi_thread")]
async fn test_describe_features() {
    describe_features(false).await;
}

/// Java's `testUpdateFeatures(clusterInstance, usingBootstrapControllers)`: an
/// unknown feature is rejected by the active controller.
async fn update_features(using_bootstrap_controllers: bool) {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = create_admin(&admin_config(&ctx, using_bootstrap_controllers));
    let updates = HashMap::from([(
        "foo.bar.feature".to_string(),
        FeatureUpdate::new(1, UpgradeType::Upgrade).expect("a valid update"),
    )]);
    let result = admin.update_features(&updates).expect("the update passes the client checks");
    let error = result
        .all()
        .get_with_timeout(CALL_TIMEOUT)
        .await
        .expect_err("the cluster does not know the feature");
    assert!(matches!(error, Error::InvalidUpdateVersion(_)), "got {error:?}");
    assert!(
        error.message().ends_with("does not support this feature."),
        "expected message to end with 'does not support this feature', but it was: {}",
        error.message()
    );
    admin.close_with_timeout(Duration::from_secs(5)).await;
}

/// Translates `testUpdateFeaturesByControllers`.
#[tokio::test(flavor = "multi_thread")]
async fn test_update_features_by_controllers() {
    update_features(true).await;
}

/// Translates `testUpdateFeatures`.
#[tokio::test(flavor = "multi_thread")]
async fn test_update_features() {
    update_features(false).await;
}

/// Translates `testUsingBootstrapControllersOnUnsupportedAdminApi`: `listOffsets`
/// cannot be sent to the controllers, and fails at once with Java's
/// `UnsupportedEndpointTypeException` message.
#[tokio::test(flavor = "multi_thread")]
async fn test_using_bootstrap_controllers_on_unsupported_admin_api() {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = create_admin(&admin_config(&ctx, true));
    let result = admin.list_offsets(&HashMap::from([(TopicPartition::new("foo", 0), OffsetSpec::Earliest)]));
    let error = result
        .all()
        .get_with_timeout(CALL_TIMEOUT)
        .await
        .expect_err("listOffsets is not a controller API");
    assert!(matches!(error, Error::UnsupportedEndpointType(_)), "got {error:?}");
    assert_eq!(error.message(), UNSUPPORTED_ENDPOINT_MESSAGE);

    // Beyond Java's test: so does `createTopics`, which goes to the controller
    // of a broker client (`ControllerNodeProvider()`).
    let topic = NewTopic::with_num_partitions_replication_factor("bootstrap-controllers-topic", Some(1), Some(1));
    let error = admin
        .create_topics(&[topic])
        .all()
        .get_with_timeout(CALL_TIMEOUT)
        .await
        .expect_err("createTopics is not a controller API");
    assert!(matches!(error, Error::UnsupportedEndpointType(_)), "got {error:?}");
    assert_eq!(error.message(), UNSUPPORTED_ENDPOINT_MESSAGE);
    admin.close_with_timeout(Duration::from_secs(5)).await;
}

/// Java's `verifyConfigValue`: retries until `config_name` of `resource` has
/// `expected_value` from `expected_source`.
async fn verify_config_value(
    admin: &dyn Admin,
    resource: &ConfigResource,
    config_name: &str,
    expected_source: ConfigSource,
    expected_value: &str,
) {
    retry_on_error_with_timeout(CONFIG_PROPAGATION_TIMEOUT, || async {
        let configs = admin
            .describe_configs(std::slice::from_ref(resource))
            .all()
            .get_with_timeout(CALL_TIMEOUT)
            .await
            .map_err(|e| format!("describeConfigs {resource:?}: {e}"))?;
        let entry = configs
            .get(resource)
            .and_then(|config| config.get(config_name))
            .ok_or_else(|| format!("no {config_name} in {resource:?}"))?;
        if entry.source() != expected_source {
            return Err(format!(
                "Expected entry for {config_name} to come from {expected_source:?}. Instead, the entry was: {entry:?}"
            ));
        }
        if entry.value() != Some(expected_value) {
            return Err(format!(
                "Expected value for {config_name} to be {expected_value}. Instead, the value was: {:?}",
                entry.value()
            ));
        }
        Ok(())
    })
    .await;
}

/// Java's `testIncrementalAlterConfigs(clusterInstance, usingBootstrapControllers)`:
/// sets `max.connections` for each node and both connection configs for the
/// default resource, then reads them back. Through the controllers, both
/// resources go to the active controller (`KafkaAdminClient.java:2980-2984`).
async fn incremental_alter_configs(using_bootstrap_controllers: bool) {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let node_ids = if using_bootstrap_controllers {
        ctx.cluster().controller_ids()
    } else {
        ctx.cluster().broker_ids()
    };
    let mut restore = HashMap::from([(
        broker_resource(""),
        vec![
            "max.connections".to_string(),
            "max.connection.creation.rate".to_string(),
        ],
    )]);
    for node_id in &node_ids {
        restore.insert(broker_resource(&node_id.to_string()), vec!["max.connections".to_string()]);
    }
    let config = admin_config(&ctx, using_bootstrap_controllers);
    restoring_configs(&ctx, restore, async move {
        let admin = create_admin(&config);
        incremental_alter_configs_on(admin.as_ref(), node_ids).await;
        admin.close_with_timeout(Duration::from_secs(5)).await;
    })
    .await;
}

/// The body of [`incremental_alter_configs`], for each of `node_ids`.
async fn incremental_alter_configs_on(admin: &dyn Admin, node_ids: BTreeSet<i32>) {
    for node_id in node_ids {
        let node_resource = broker_resource(&node_id.to_string());
        let default_resource = broker_resource("");
        let node_max_connections_value = (1000 + node_id).to_string();
        let default_max_connections_value = (2000 + node_id).to_string();
        let default_connection_rate_value = (2000 + node_id).to_string();

        // Set configs: MAX_CONNECTIONS_CONFIG for per-broker, both configs for default
        let alterations = HashMap::from([
            (node_resource.clone(), vec![set("max.connections", &node_max_connections_value)]),
            (
                default_resource.clone(),
                vec![
                    set("max.connections", &default_max_connections_value),
                    set("max.connection.creation.rate", &default_connection_rate_value),
                ],
            ),
        ]);
        admin
            .incremental_alter_configs(&alterations)
            .all()
            .get_with_timeout(CALL_TIMEOUT)
            .await
            .expect("the configs are altered");

        // Verify per-broker configs: MAX_CONNECTIONS_CONFIG and MAX_CONNECTION_CREATION_RATE_CONFIG
        verify_config_value(
            admin,
            &node_resource,
            "max.connections",
            ConfigSource::DynamicBrokerConfig,
            &node_max_connections_value,
        )
        .await;
        verify_config_value(
            admin,
            &node_resource,
            "max.connection.creation.rate",
            ConfigSource::DynamicDefaultBrokerConfig,
            &default_connection_rate_value,
        )
        .await;

        // Verify default broker configs: MAX_CONNECTIONS_CONFIG and MAX_CONNECTION_CREATION_RATE_CONFIG
        verify_config_value(
            admin,
            &default_resource,
            "max.connections",
            ConfigSource::DynamicDefaultBrokerConfig,
            &default_max_connections_value,
        )
        .await;
        verify_config_value(
            admin,
            &default_resource,
            "max.connection.creation.rate",
            ConfigSource::DynamicDefaultBrokerConfig,
            &default_connection_rate_value,
        )
        .await;
    }
}

/// Translates `testIncrementalAlterConfigsByControllers`.
#[tokio::test(flavor = "multi_thread")]
async fn test_incremental_alter_configs_by_controllers() {
    incremental_alter_configs(true).await;
}

/// Translates `testIncrementalAlterConfigs`.
#[tokio::test(flavor = "multi_thread")]
async fn test_incremental_alter_configs() {
    incremental_alter_configs(false).await;
}

/// Java's `translatePartitionInfoToNodeIdList`.
fn replica_ids(admin_partitions: &[confluent_kafka::common::TopicPartitionInfo]) -> Vec<Vec<i32>> {
    admin_partitions
        .iter()
        .map(|partition| partition.replicas().iter().map(|node| node.id()).collect())
        .collect()
}

/// Translates `testAlterReassignmentsWithBootstrapControllers` (`brokers = 3`):
/// the reassignment is submitted and listed through the controllers
/// (`ControllerNodeProvider(true)`), and the brokers then report the new
/// replicas. The node ids are 1-based here, so each of Java's ids is one
/// higher.
#[tokio::test(flavor = "multi_thread")]
async fn test_alter_reassignments_with_bootstrap_controllers() {
    let mut ctx = TestContext::new(ClusterConfig::with_brokers(3)).await;
    let topic_name = ctx.topic("foo");
    let admin = create_admin(&admin_config(&ctx, false));
    let assignments = BTreeMap::from([(0, vec![1, 2, 3]), (1, vec![2, 3, 1]), (2, vec![3, 2, 1])]);
    admin
        .create_topics(&[NewTopic::with_replicas_assignments(topic_name.clone(), assignments)])
        .all()
        .get_with_timeout(CALL_TIMEOUT)
        .await
        .expect("the topic is created");
    // Java's `waitForTopics`.
    wait_until_true_with_timeout(
        || async {
            admin
                .list_topics()
                .names()
                .get_with_timeout(CALL_TIMEOUT)
                .await
                .is_ok_and(|names| names.contains(&topic_name))
        },
        "timed out waiting for topics",
        WAIT_FOR_CONDITION_MS,
        100,
    )
    .await;

    let part0_reassignment = vec![3, 2, 1];
    let part1_reassignment = vec![1, 2, 3];
    let part2_reassignment = vec![2, 3];
    let reassignments = HashMap::from([
        (
            TopicPartition::new(topic_name.as_str(), 0),
            Some(NewPartitionReassignment::new(part0_reassignment.clone()).unwrap()),
        ),
        (
            TopicPartition::new(topic_name.as_str(), 1),
            Some(NewPartitionReassignment::new(part1_reassignment.clone()).unwrap()),
        ),
        (
            TopicPartition::new(topic_name.as_str(), 2),
            Some(NewPartitionReassignment::new(part2_reassignment.clone()).unwrap()),
        ),
    ]);

    let admin_with_bootstrap_controllers = create_admin(&admin_config(&ctx, true));
    admin_with_bootstrap_controllers
        .alter_partition_reassignments(&reassignments)
        .all()
        .get_with_timeout(CALL_TIMEOUT)
        .await
        .expect("the reassignment is accepted");
    wait_until_true_with_timeout(
        || async {
            admin_with_bootstrap_controllers
                .list_partition_reassignments()
                .reassignments()
                .get_with_timeout(CALL_TIMEOUT)
                .await
                .is_ok_and(|reassignments| reassignments.is_empty())
        },
        "The reassignment never completed.",
        WAIT_FOR_CONDITION_MS,
        100,
    )
    .await;
    admin_with_bootstrap_controllers
        .close_with_timeout(Duration::from_secs(5))
        .await;

    let expected_mapping = vec![part0_reassignment, part1_reassignment, part2_reassignment];
    wait_until_true_with_timeout(
        || async {
            let Some(all) = admin
                .describe_topics_with_topic_names(std::slice::from_ref(&topic_name))
                .all_topic_names()
            else {
                return false;
            };
            match all.get_with_timeout(CALL_TIMEOUT).await {
                Ok(descriptions) => descriptions
                    .get(&topic_name)
                    .is_some_and(|description| replica_ids(description.partitions()) == expected_mapping),
                Err(_) => false,
            }
        },
        &format!("Timed out waiting for replica assignments for topic {topic_name}"),
        WAIT_FOR_CONDITION_MS,
        100,
    )
    .await;
    admin.close_with_timeout(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

/// Java's `testAcls(clusterInstance, usingBootstrapControllers)`: create,
/// describe and delete an ACL, on the authorizer cluster.
async fn acls(using_bootstrap_controllers: bool) {
    let mut ctx = TestContext::new(authorizer_single_broker()).await;
    let admin = create_admin(&admin_config(&ctx, using_bootstrap_controllers));
    // A topic name of this test's own: Java's "mytopic3", made unique because
    // the authorizer cluster is shared.
    let resource_pattern = ResourcePattern::new(ResourceType::Topic, ctx.topic("mytopic3"), PatternType::Literal)
        .expect("a valid resource pattern");
    let access_control_entry =
        AccessControlEntry::new("User:ANONYMOUS", "*", AclOperation::Describe, AclPermissionType::Allow)
            .expect("a valid entry");
    let acl_binding = AclBinding::new(resource_pattern.clone(), access_control_entry.clone());
    admin
        .create_acls(std::slice::from_ref(&acl_binding))
        .all()
        .get_with_timeout(CALL_TIMEOUT)
        .await
        .expect("the ACL is created");

    // Java's `clusterInstance.waitAcls`, and `AclBindingFilter.ANY` narrowed to
    // this test's resource.
    let filter = AclBindingFilter::new(resource_pattern.to_filter(), AccessControlEntryFilter::any());
    wait_until_true_with_timeout(
        || async {
            admin
                .describe_acls(&filter)
                .values()
                .get_with_timeout(CALL_TIMEOUT)
                .await
                .is_ok_and(|bindings| !bindings.is_empty())
        },
        "the ACL never appeared",
        WAIT_FOR_CONDITION_MS,
        100,
    )
    .await;

    let acl_bindings = admin
        .describe_acls(&filter)
        .values()
        .get_with_timeout(CALL_TIMEOUT)
        .await
        .expect("the ACLs");
    assert_eq!(acl_bindings, vec![acl_binding.clone()]);

    let deleted_acl_bindings = admin
        .delete_acls(std::slice::from_ref(&filter))
        .all()
        .get_with_timeout(CALL_TIMEOUT)
        .await
        .expect("the ACL is deleted");
    assert_eq!(deleted_acl_bindings, vec![acl_binding]);
    admin.close_with_timeout(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

/// Translates `testAclsByControllers`.
#[tokio::test(flavor = "multi_thread")]
async fn test_acls_by_controllers() {
    acls(true).await;
}

/// Translates `testAcls`.
#[tokio::test(flavor = "multi_thread")]
async fn test_acls() {
    acls(false).await;
}

/// Translates `testDescribeConfigs`: the default BROKER resource, described
/// through the controllers, reports the cluster's `min.insync.replicas` as a
/// dynamic default. Java's cluster sets it to `2`; this one keeps the default,
/// `1` (see the module docs).
#[tokio::test(flavor = "multi_thread")]
async fn test_describe_configs() {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = create_admin(&admin_config(&ctx, true));
    let resource = broker_resource("");
    let resource_to_config = admin
        .describe_configs(std::slice::from_ref(&resource))
        .all()
        .get_with_timeout(CALL_TIMEOUT)
        .await
        .expect("the configs");
    let config = resource_to_config.get(&resource).expect("the default resource's config");
    let config_entry = config.get("min.insync.replicas").expect("min.insync.replicas");
    assert_eq!(config_entry.source(), ConfigSource::DynamicDefaultBrokerConfig);
    assert_eq!(config_entry.value(), Some("1"));
    admin.close_with_timeout(Duration::from_secs(5)).await;
}

/// The first broker release whose throttle configs are not read-only
/// (KAFKA-19984).
const QUOTA_CONFIGS_WRITABLE_RELEASE: (u32, u32) = (4, 3);

/// Translates `testQuotaConfigsIsReadOnlyShouldBeFalse`: the three throttle
/// configs a node's resource takes through the controllers are reported as not
/// read-only. Skips itself on a broker older than 4.3, which reports them as
/// read-only: the fix is on the broker side.
#[tokio::test(flavor = "multi_thread")]
async fn test_quota_configs_is_read_only_should_be_false() {
    if kafka_cluster::broker_release() < QUOTA_CONFIGS_WRITABLE_RELEASE {
        eprintln!(
            "skipped: the throttle configs are read-only on a broker older than 4.3; the broker is {:?}",
            kafka_cluster::broker_release()
        );
        return;
    }
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let node_id = *ctx.cluster().controller_ids().iter().next().expect("a controller");
    let node_resource = broker_resource(&node_id.to_string());
    const THROTTLES: [&str; 3] = [
        "leader.replication.throttled.rate",
        "follower.replication.throttled.rate",
        "replica.alter.log.dirs.io.max.bytes.per.second",
    ];
    let restore = HashMap::from([(node_resource.clone(), THROTTLES.map(str::to_string).to_vec())]);
    let config = admin_config(&ctx, true);
    restoring_configs(&ctx, restore, async move {
        let admin = create_admin(&config);
        let alterations = HashMap::from([(
            node_resource.clone(),
            THROTTLES.iter().map(|name| set(name, "16800")).collect::<Vec<_>>(),
        )]);
        admin
            .incremental_alter_configs(&alterations)
            .all()
            .get_with_timeout(CALL_TIMEOUT)
            .await
            .expect("the throttles are set");
        retry_on_error_with_timeout(CONFIG_PROPAGATION_TIMEOUT, || async {
            let configs = admin
                .describe_configs(std::slice::from_ref(&node_resource))
                .all()
                .get_with_timeout(CALL_TIMEOUT)
                .await
                .map_err(|e| format!("describeConfigs: {e}"))?;
            let config = configs.get(&node_resource).ok_or("no config for the node")?;
            let read_only: HashSet<&str> = THROTTLES
                .iter()
                .copied()
                .filter(|name| config.get(name).is_none_or(ConfigEntry::is_read_only))
                .collect();
            if read_only.is_empty() {
                Ok(())
            } else {
                Err(format!("missing or read-only: {read_only:?}"))
            }
        })
        .await;
        admin.close_with_timeout(Duration::from_secs(5)).await;
    })
    .await;
}
