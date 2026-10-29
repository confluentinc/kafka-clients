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
//! Each scenario is a body generic over
//! [`AdminBackendFactory`](crate::common::backend_factory::AdminBackendFactory)
//! and registered with [`multilanguage_admin_test!`], so it runs against the
//! native Rust client, the Python sync binding, the Python asyncio binding and
//! the C FFI. With only `integration-tests` enabled the `__rust` arm is the whole
//! expansion. The admin client under test comes from the factory; the fixtures
//! (the topic of the reassignment test, the clean-up of dynamic configs) use the
//! native client.
//!
//! # How the harness differs from Java's
//!
//! - **Topology.** Java's `@ClusterTestDefaults(types = {Type.KRAFT})` runs
//!   separate controller nodes (ids from 3000). Here every node is a combined
//!   (`broker,controller`) node of a pooled [`Type::CoKraft`] cluster, so a
//!   node's controller id is its broker id: a controller-only container cannot
//!   publish a host listener (the image rejects `KAFKA_ADVERTISED_LISTENERS` on
//!   a controller). The controllers are reached through a PLAINTEXT controller
//!   listener, and the broker arms use the PLAINTEXT broker listener, as Java's
//!   `Admin.create` with no security keys does, whatever protocol the run
//!   selects.
//! - **Which controller listener.** A backend on the host (the native client, or
//!   a gRPC server run natively) uses the `CONTROLLERHOST` listener, advertised
//!   on the host loopback ([`KafkaCluster::bootstrap_controllers`]); a gRPC
//!   backend in a container uses the `CONTROLLERCONTAINER` listener, advertised
//!   by container name ([`KafkaCluster::container_bootstrap_controllers`]),
//!   both picked by [`TestContext::bootstrap_controllers_for`]. A controller
//!   answers `DescribeCluster` with the endpoints of the listener the request
//!   came in on, so each backend is handed controller addresses it can reach.
//! - **Shared clusters.** Java gives every test a fresh cluster. Here the
//!   clusters are pooled, so:
//!   - the ACL tests use a resource of their own and filter on it, where Java
//!     filters with `AclBindingFilter.ANY`: the authorizer cluster is shared
//!     with `admin_acls_test.rs`;
//!   - the tests that change dynamic broker configs run on the shared default
//!     cluster, where each Java test gets a cluster of its default shape to
//!     itself. They run one at a time, every backend arm included
//!     ([`DYNAMIC_CONFIG_PERMIT`]), and delete every config they set when they
//!     finish, pass or fail ([`restoring_configs`]). No other suite touches
//!     those configs on this cluster (the elections suite sets the throttle
//!     rates on the 3-broker one). A cluster of their own would add a resident
//!     cluster to a suite that already fills the Docker VM's memory;
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
//!   controller nodes reachable from the clients, which this harness cannot
//!   provide (see above).
//! - Other controller-side tests that need what the combined-node harness does
//!   not have, so they are not translated on purpose:
//!   - `DescribeFeaturesTest.testSendRequestToWrongNodeType` sends to a
//!     controller-only node through `bootstrap.servers` and to a broker-only
//!     node through `bootstrap.controllers`. A combined node is both, so
//!     neither request reaches the wrong node type.
//!   - `DescribeFeaturesTest.testUnstableApiVersions` gives two controller-only
//!     nodes (3000, 3001) different `unstable.api.versions.enable` values,
//!     which needs controller nodes of their own.
//!   - `KRaftClusterTest.testUnregisterBroker(true)`: this client has no
//!     `unregisterBroker`, in either mode.
//!   - `SslAdminIntegrationTest.testListNodesFromControllers` /
//!     `testListNodesFromControllersIncludingFencedBrokers` reach the
//!     controllers over SSL; the harness's controller listeners are PLAINTEXT
//!     only. The client-side check of the second ("Cannot request fenced
//!     brokers from controller endpoint") is covered by a unit test in
//!     `kafka_admin_client.rs`.
//!
//! [`KafkaCluster::bootstrap_controllers`]: crate::common::kafka_cluster::KafkaCluster::bootstrap_controllers
//! [`KafkaCluster::container_bootstrap_controllers`]: crate::common::kafka_cluster::KafkaCluster::container_bootstrap_controllers
//! [`Type::CoKraft`]: crate::common::cluster_config::Type::CoKraft

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::Hash;
use std::time::Duration;

use confluent_kafka::admin::{
    Admin, AdminClient, AdminClientConfig, AlterConfigOp, AlterConfigsOptions, AlterPartitionReassignmentsOptions,
    ConfigEntry, CreateAclsOptions, CreateTopicsOptions, DeleteAclsOptions, DescribeAclsOptions,
    DescribeClusterOptions, DescribeConfigsOptions, DescribeFeaturesOptions, FeatureUpdate, FinalizedVersionRange,
    ListOffsetsOptions, ListPartitionReassignmentsOptions, NewPartitionReassignment, NewTopic, OpType,
    UpdateFeaturesOptions, UpgradeType,
};
use confluent_kafka::common::acl::{
    AccessControlEntry, AccessControlEntryFilter, AclBinding, AclBindingFilter, AclOperation, AclPermissionType,
};
use confluent_kafka::common::config::{ConfigResource, config_resource};
use confluent_kafka::common::resource::{PatternType, ResourcePattern, ResourceType};
use confluent_kafka::common::{Error, TopicPartition};
use futures_util::FutureExt;

use crate::common::admin_backend::{AdminBackend, OffsetSpecArg, Outcomes, all_of_exactly, plaintext_bootstrap_for};
use crate::common::backend_factory::AdminBackendFactory;
use crate::common::cluster_config::{ClusterConfig, authorizer_single_broker};
use crate::common::kafka_cluster;
use crate::common::test_context::TestContext;
use crate::common::test_utils::{retry_on_error_with_timeout, wait_until_true_with_timeout};
use crate::multilanguage_admin_test;

/// How long a test waits for a call to complete (Java's `get(1, TimeUnit.MINUTES)`).
const CALL_TIMEOUT: Duration = Duration::from_secs(60);

/// Java's `TestUtils.retryOnExceptionWithTimeout(30_000, ..)` in `verifyConfigValue`.
const CONFIG_PROPAGATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Java's `TestUtils.waitForCondition` default, 15 s.
const WAIT_FOR_CONDITION_MS: u64 = 15_000;

/// How long `close` may take, on every backend.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Java's `UnsupportedEndpointTypeException` text (`KafkaAdminClient.AdminClientRunnable#call`).
const UNSUPPORTED_ENDPOINT_MESSAGE: &str =
    "This Admin API is not yet supported when communicating directly with the controller quorum.";

/// Java's `clusterInstance.bootstrapControllers()`, at the controller listener
/// the backend can reach.
fn bootstrap_controllers<F: AdminBackendFactory>(ctx: &TestContext, factory: &F) -> String {
    ctx.bootstrap_controllers_for(factory.needs_container_bootstrap())
        .expect("a combined node publishes its controller listeners")
        .to_string()
}

/// Java's `adminConfig(clusterInstance, usingBootstrapControllers)`:
/// `bootstrap.controllers` set to the controllers, or `bootstrap.servers` to
/// the brokers' PLAINTEXT listener, and nothing else, at addresses the backend
/// can reach.
fn admin_config<F: AdminBackendFactory>(
    ctx: &TestContext,
    factory: &F,
    using_bootstrap_controllers: bool,
) -> HashMap<String, String> {
    if using_bootstrap_controllers {
        HashMap::from([(
            AdminClientConfig::BOOTSTRAP_CONTROLLERS_CONFIG.to_string(),
            bootstrap_controllers(ctx, factory),
        )])
    } else {
        HashMap::from([(
            AdminClientConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
            plaintext_bootstrap_for(factory, ctx),
        )])
    }
}

/// Java's `Admin.create(config)`, on the backend under test.
async fn create_admin<F: AdminBackendFactory>(factory: &F, config: HashMap<String, String>) -> F::Admin {
    factory
        .create(config)
        .await
        .unwrap_or_else(|e| panic!("{} backend: create admin client: {e}", factory.name()))
}

/// Closes the admin client under test.
async fn close<A: AdminBackend>(admin: A, backend: &str) {
    admin
        .close(Some(CLOSE_TIMEOUT))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
}

/// A native admin client over the brokers' host PLAINTEXT listener, for the
/// fixtures.
fn native_admin(ctx: &TestContext) -> Box<dyn Admin> {
    let config = HashMap::from([(
        AdminClientConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
        ctx.bootstrap_servers().to_string(),
    )]);
    AdminClient::create(AdminClientConfig::new(&config).expect("valid admin config")).expect("create admin client")
}

/// The error Java's `all().get()` sees for a call on one key. Java makes the
/// call outside `assertThrows`, so the call must return its result; with one
/// key, `all()` is `KafkaFuture.allOf` over that key's future.
fn key_error<K, V>(backend: &str, what: &str, outcome: Result<Outcomes<K, V>, Error>, key: &K) -> Error
where
    K: Hash + Eq + std::fmt::Debug,
{
    let mut outcomes = outcome.unwrap_or_else(|e| panic!("{backend} backend: {what} must return its result: {e}"));
    match outcomes.remove(key) {
        Some(Err(error)) => error,
        Some(Ok(_)) => panic!("{backend} backend: {what} must fail for {key:?}"),
        None => panic!("{backend} backend: {what} reported no outcome for {key:?}"),
    }
}

/// Lets one test at a time change dynamic broker configs on the shared
/// default cluster: the `incrementalAlterConfigs` arms of every backend set the
/// same configs, and one arm's clean-up would otherwise race another's
/// read-back.
static DYNAMIC_CONFIG_PERMIT: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

/// Runs `body`, which sets dynamic broker configs on `ctx`'s cluster, then
/// deletes the configs `restore` names (through the brokers, with the native
/// client), whether `body` passed or panicked, and then re-raises its panic.
/// Holds [`DYNAMIC_CONFIG_PERMIT`] throughout.
async fn restoring_configs<Fut>(ctx: &TestContext, restore: HashMap<ConfigResource, Vec<String>>, body: Fut)
where
    Fut: std::future::Future<Output = ()>,
{
    let _permit = DYNAMIC_CONFIG_PERMIT.acquire().await.expect("the permit is never closed");
    let outcome = std::panic::AssertUnwindSafe(body).catch_unwind().await;
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
    let admin = native_admin(ctx);
    let restored = admin
        .incremental_alter_configs(&deletions)
        .all()
        .get_with_timeout(CALL_TIMEOUT)
        .await;
    admin.close_with_timeout(CLOSE_TIMEOUT).await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
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

// ---------------------------------------------------------------------------
// Test bodies — generic over AdminBackendFactory
// ---------------------------------------------------------------------------

/// Translates `testPutBrokersInBootstrapControllersConfig`: the brokers given
/// as controllers answer the CONTROLLER `DescribeCluster` with
/// `MISMATCHED_ENDPOINT_TYPE`, which fails the metadata refresh and so the call.
async fn put_brokers_in_bootstrap_controllers_config<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let config = HashMap::from([(
        AdminClientConfig::BOOTSTRAP_CONTROLLERS_CONFIG.to_string(),
        plaintext_bootstrap_for(factory, ctx),
    )]);
    let admin = create_admin(factory, config).await;
    let error = admin
        .describe_cluster(DescribeClusterOptions::new())
        .await
        .expect_err("a broker is not a controller");
    assert!(
        matches!(error, Error::MismatchedEndpointType(_)),
        "{backend} backend: got {error:?}"
    );
    assert_eq!(
        error.message(),
        "The request was sent to an endpoint of type BROKER, but we wanted an endpoint of type CONTROLLER",
        "{backend} backend"
    );
    close(admin, backend).await;
}

/// Translates `testPutControllersInBootstrapBrokersConfig`: the controllers
/// given as brokers do not support the `Metadata` API, so the broker metadata
/// refresh fails with the client's `UnsupportedVersionException`, which
/// `updateFailed` keeps, and the call fails with it.
async fn put_controllers_in_bootstrap_brokers_config<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let config = HashMap::from([(
        AdminClientConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
        bootstrap_controllers(ctx, factory),
    )]);
    let admin = create_admin(factory, config).await;
    let error = admin
        .describe_cluster(DescribeClusterOptions::new())
        .await
        .expect_err("a controller does not serve Metadata");
    assert!(
        matches!(error, Error::UnsupportedVersion(_)),
        "{backend} backend: got {error:?}"
    );
    assert_eq!(error.message(), "The node does not support METADATA", "{backend} backend");
    close(admin, backend).await;
}

/// Java's `testDescribeCluster(clusterInstance, usingBootstrapControllers)`.
async fn describe_cluster<F: AdminBackendFactory>(ctx: &TestContext, factory: &F, using_bootstrap_controllers: bool) {
    let backend = factory.name();
    let admin = create_admin(factory, admin_config(ctx, factory, using_bootstrap_controllers)).await;
    let result = admin
        .describe_cluster(DescribeClusterOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe cluster: {e}"));
    assert_eq!(result.cluster_id, ctx.cluster().cluster_id(), "{backend} backend");
    if using_bootstrap_controllers {
        let controller = result
            .controller
            .as_ref()
            .unwrap_or_else(|| panic!("{backend} backend: a quorum has an active controller"));
        assert!(
            ctx.cluster().controller_ids().contains(&controller.id()),
            "{backend} backend: {controller:?} is one of the controllers"
        );

        // Beyond Java's test: the nodes are the controllers, described at the
        // addresses of the listener this backend connected to.
        let ids: BTreeSet<i32> = result.nodes.iter().map(|node| node.id()).collect();
        assert_eq!(ids, ctx.cluster().controller_ids(), "{backend} backend");
        let addresses: BTreeSet<String> = result
            .nodes
            .iter()
            .map(|node| format!("{}:{}", node.host(), node.port()))
            .collect();
        assert_eq!(
            addresses,
            bootstrap_controllers(ctx, factory)
                .split(',')
                .map(str::to_string)
                .collect::<BTreeSet<String>>(),
            "{backend} backend"
        );
    }
    close(admin, backend).await;
}

/// Translates `testDescribeClusterByControllers`.
async fn describe_cluster_by_controllers<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    describe_cluster(ctx, factory, true).await;
}

/// Translates `testDescribeCluster`.
async fn describe_cluster_by_brokers<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    describe_cluster(ctx, factory, false).await;
}

/// Java's `testDescribeFeatures(clusterInstance, usingBootstrapControllers)`:
/// the finalized `metadata.version` is the cluster's.
///
/// Java reads the cluster's version from `clusterInstance.config()`. The
/// harness does not choose one: the image formats every node with its
/// release's latest production `metadata.version`, which is also the top of the
/// range the node supports, so that is the expected level here.
async fn describe_features<F: AdminBackendFactory>(ctx: &TestContext, factory: &F, using_bootstrap_controllers: bool) {
    let backend = factory.name();
    let admin = create_admin(factory, admin_config(ctx, factory, using_bootstrap_controllers)).await;
    let features = admin
        .describe_features(DescribeFeaturesOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe features: {e}"));
    let metadata_version = features
        .supported_features
        .get("metadata.version")
        .unwrap_or_else(|| panic!("{backend} backend: metadata.version is a supported feature"))
        .max_version();
    assert_eq!(
        features.finalized_features.get("metadata.version"),
        Some(&FinalizedVersionRange::new(metadata_version, metadata_version).expect("a valid range")),
        "{backend} backend"
    );
    close(admin, backend).await;
}

/// Translates `testDescribeFeaturesByControllers`.
async fn describe_features_by_controllers<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    describe_features(ctx, factory, true).await;
}

/// Translates `testDescribeFeatures`.
async fn describe_features_by_brokers<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    describe_features(ctx, factory, false).await;
}

/// Java's `testUpdateFeatures(clusterInstance, usingBootstrapControllers)`: an
/// unknown feature is rejected by the active controller.
async fn update_features<F: AdminBackendFactory>(ctx: &TestContext, factory: &F, using_bootstrap_controllers: bool) {
    let backend = factory.name();
    let admin = create_admin(factory, admin_config(ctx, factory, using_bootstrap_controllers)).await;
    let feature = "foo.bar.feature".to_string();
    let updates = HashMap::from([(
        feature.clone(),
        FeatureUpdate::new(1, UpgradeType::Upgrade).expect("a valid update"),
    )]);
    let error = key_error(
        backend,
        "updateFeatures",
        admin.update_features(&updates, UpdateFeaturesOptions::new()).await,
        &feature,
    );
    assert!(
        matches!(error, Error::InvalidUpdateVersion(_)),
        "{backend} backend: got {error:?}"
    );
    assert!(
        error.message().ends_with("does not support this feature."),
        "{backend} backend: expected message to end with 'does not support this feature', but it was: {}",
        error.message()
    );
    close(admin, backend).await;
}

/// Translates `testUpdateFeaturesByControllers`.
async fn update_features_by_controllers<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    update_features(ctx, factory, true).await;
}

/// Translates `testUpdateFeatures`.
async fn update_features_by_brokers<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    update_features(ctx, factory, false).await;
}

/// Translates `testUsingBootstrapControllersOnUnsupportedAdminApi`: `listOffsets`
/// cannot be sent to the controllers, and fails at once with Java's
/// `UnsupportedEndpointTypeException` message.
async fn using_bootstrap_controllers_on_unsupported_admin_api<F: AdminBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let admin = create_admin(factory, admin_config(ctx, factory, true)).await;
    let partition = TopicPartition::new("foo", 0);
    let offsets = HashMap::from([(partition.clone(), OffsetSpecArg::Earliest)]);
    let error = key_error(
        backend,
        "listOffsets",
        admin.list_offsets(&offsets, ListOffsetsOptions::new()).await,
        &partition,
    );
    assert!(
        matches!(error, Error::UnsupportedEndpointType(_)),
        "{backend} backend: got {error:?}"
    );
    assert_eq!(error.message(), UNSUPPORTED_ENDPOINT_MESSAGE, "{backend} backend");

    // Beyond Java's test: so does `createTopics`, which goes to the controller
    // of a broker client (`ControllerNodeProvider()`).
    let name = "bootstrap-controllers-topic".to_string();
    let topic = NewTopic::with_num_partitions_replication_factor(name.clone(), Some(1), Some(1));
    let error = key_error(
        backend,
        "createTopics",
        admin.create_topics(&[topic], CreateTopicsOptions::new()).await,
        &name,
    );
    assert!(
        matches!(error, Error::UnsupportedEndpointType(_)),
        "{backend} backend: got {error:?}"
    );
    assert_eq!(error.message(), UNSUPPORTED_ENDPOINT_MESSAGE, "{backend} backend");
    close(admin, backend).await;
}

/// Java's `verifyConfigValue`: retries until `config_name` of `resource` has
/// `expected_value` from `expected_source` (a `ConfigSource` constant name).
async fn verify_config_value<A: AdminBackend>(
    admin: &A,
    resource: &ConfigResource,
    config_name: &str,
    expected_source: &str,
    expected_value: &str,
) {
    let backend = admin.name();
    retry_on_error_with_timeout(CONFIG_PROPAGATION_TIMEOUT, || async {
        let configs = admin
            .describe_configs(std::slice::from_ref(resource), DescribeConfigsOptions::new())
            .await
            .map_err(|e| format!("{backend} backend: describeConfigs {resource:?}: {e}"))?;
        let config = configs
            .get(resource)
            .ok_or_else(|| format!("{backend} backend: no outcome for {resource:?}"))?
            .as_ref()
            .map_err(|e| format!("{backend} backend: describeConfigs {resource:?}: {e}"))?;
        let entry = config
            .get(config_name)
            .ok_or_else(|| format!("{backend} backend: no {config_name} in {resource:?}"))?;
        if entry.source.as_deref() != Some(expected_source) {
            return Err(format!(
                "{backend} backend: Expected entry for {config_name} to come from {expected_source}. \
                 Instead, the entry was: {entry:?}"
            ));
        }
        if entry.value.as_deref() != Some(expected_value) {
            return Err(format!(
                "{backend} backend: Expected value for {config_name} to be {expected_value}. \
                 Instead, the value was: {:?}",
                entry.value
            ));
        }
        Ok(())
    })
    .await;
}

/// Java's `testIncrementalAlterConfigs(clusterInstance, usingBootstrapControllers)`:
/// sets `max.connections` for each node and both connection configs for the
/// default resource, then reads them back. Through the controllers, both
/// resources go to the active controller (`KafkaAdminClient#incrementalAlterConfigs`).
async fn incremental_alter_configs<F: AdminBackendFactory>(
    ctx: &TestContext,
    factory: &F,
    using_bootstrap_controllers: bool,
) {
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
    let config = admin_config(ctx, factory, using_bootstrap_controllers);
    restoring_configs(ctx, restore, async move {
        let admin = create_admin(factory, config).await;
        incremental_alter_configs_on(&admin, node_ids).await;
        close(admin, factory.name()).await;
    })
    .await;
}

/// The body of [`incremental_alter_configs`], for each of `node_ids`.
async fn incremental_alter_configs_on<A: AdminBackend>(admin: &A, node_ids: BTreeSet<i32>) {
    let backend = admin.name();
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
        let altered = admin
            .incremental_alter_configs(&alterations, AlterConfigsOptions::new())
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: incremental alter configs: {e}"));
        all_of_exactly(
            admin,
            &altered,
            &[node_resource.clone(), default_resource.clone()],
            "incrementalAlterConfigs",
        );

        // Verify per-broker configs: MAX_CONNECTIONS_CONFIG and MAX_CONNECTION_CREATION_RATE_CONFIG
        verify_config_value(
            admin,
            &node_resource,
            "max.connections",
            "DYNAMIC_BROKER_CONFIG",
            &node_max_connections_value,
        )
        .await;
        verify_config_value(
            admin,
            &node_resource,
            "max.connection.creation.rate",
            "DYNAMIC_DEFAULT_BROKER_CONFIG",
            &default_connection_rate_value,
        )
        .await;

        // Verify default broker configs: MAX_CONNECTIONS_CONFIG and MAX_CONNECTION_CREATION_RATE_CONFIG
        verify_config_value(
            admin,
            &default_resource,
            "max.connections",
            "DYNAMIC_DEFAULT_BROKER_CONFIG",
            &default_max_connections_value,
        )
        .await;
        verify_config_value(
            admin,
            &default_resource,
            "max.connection.creation.rate",
            "DYNAMIC_DEFAULT_BROKER_CONFIG",
            &default_connection_rate_value,
        )
        .await;
    }
}

/// Translates `testIncrementalAlterConfigsByControllers`.
async fn incremental_alter_configs_by_controllers<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    incremental_alter_configs(ctx, factory, true).await;
}

/// Translates `testIncrementalAlterConfigs`.
async fn incremental_alter_configs_by_brokers<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    incremental_alter_configs(ctx, factory, false).await;
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
/// higher. The topic is created and described through the brokers with the
/// native client; the backend under test is the `bootstrap.controllers` one.
async fn alter_reassignments_with_bootstrap_controllers<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic_name = ctx.topic("foo");
    let admin = native_admin(ctx);
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
    let partitions: Vec<TopicPartition> = (0..3).map(|p| TopicPartition::new(topic_name.as_str(), p)).collect();
    let reassignments = HashMap::from([
        (
            partitions[0].clone(),
            Some(NewPartitionReassignment::new(part0_reassignment.clone()).unwrap()),
        ),
        (
            partitions[1].clone(),
            Some(NewPartitionReassignment::new(part1_reassignment.clone()).unwrap()),
        ),
        (
            partitions[2].clone(),
            Some(NewPartitionReassignment::new(part2_reassignment.clone()).unwrap()),
        ),
    ]);

    let admin_with_bootstrap_controllers = create_admin(factory, admin_config(ctx, factory, true)).await;
    let altered = admin_with_bootstrap_controllers
        .alter_partition_reassignments(&reassignments, AlterPartitionReassignmentsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: alter partition reassignments: {e}"));
    all_of_exactly(
        &admin_with_bootstrap_controllers,
        &altered,
        &partitions,
        "alterPartitionReassignments",
    );
    wait_until_true_with_timeout(
        || async {
            admin_with_bootstrap_controllers
                .list_partition_reassignments(None, ListPartitionReassignmentsOptions::new())
                .await
                .is_ok_and(|reassignments| reassignments.is_empty())
        },
        &format!("{backend} backend: The reassignment never completed."),
        WAIT_FOR_CONDITION_MS,
        100,
    )
    .await;
    close(admin_with_bootstrap_controllers, backend).await;

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
        &format!("{backend} backend: Timed out waiting for replica assignments for topic {topic_name}"),
        WAIT_FOR_CONDITION_MS,
        100,
    )
    .await;
    admin.close_with_timeout(CLOSE_TIMEOUT).await;
    ctx.cleanup().await;
}

/// Java's `testAcls(clusterInstance, usingBootstrapControllers)`: create,
/// describe and delete an ACL, on the authorizer cluster.
async fn acls<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F, using_bootstrap_controllers: bool) {
    let backend = factory.name();
    let admin = create_admin(factory, admin_config(ctx, factory, using_bootstrap_controllers)).await;
    // A topic name of this test's own: Java's "mytopic3", made unique because
    // the authorizer cluster is shared.
    let resource_pattern = ResourcePattern::new(ResourceType::Topic, ctx.topic("mytopic3"), PatternType::Literal)
        .expect("a valid resource pattern");
    let access_control_entry =
        AccessControlEntry::new("User:ANONYMOUS", "*", AclOperation::Describe, AclPermissionType::Allow)
            .expect("a valid entry");
    let acl_binding = AclBinding::new(resource_pattern.clone(), access_control_entry.clone());
    let created = admin
        .create_acls(std::slice::from_ref(&acl_binding), CreateAclsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: create acls: {e}"));
    all_of_exactly(&admin, &created, std::slice::from_ref(&acl_binding), "createAcls");

    // Java's `clusterInstance.waitAcls`, and `AclBindingFilter.ANY` narrowed to
    // this test's resource.
    let filter = AclBindingFilter::new(resource_pattern.to_filter(), AccessControlEntryFilter::any());
    wait_until_true_with_timeout(
        || async {
            admin
                .describe_acls(&filter, DescribeAclsOptions::new())
                .await
                .is_ok_and(|bindings| !bindings.is_empty())
        },
        &format!("{backend} backend: the ACL never appeared"),
        WAIT_FOR_CONDITION_MS,
        100,
    )
    .await;

    let acl_bindings = admin
        .describe_acls(&filter, DescribeAclsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe acls: {e}"));
    assert_eq!(acl_bindings, vec![acl_binding.clone()], "{backend} backend");

    let deleted = admin
        .delete_acls(std::slice::from_ref(&filter), DeleteAclsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: delete acls: {e}"));
    all_of_exactly(&admin, &deleted, std::slice::from_ref(&filter), "deleteAcls");
    let results = deleted[&filter].as_ref().expect("checked by all_of_exactly");
    let deleted_acl_bindings: Vec<AclBinding> = results
        .values
        .iter()
        .map(|result| {
            assert!(
                result.error.is_none(),
                "{backend} backend: deleting the ACL failed inside the FilterResult: {:?}",
                result.error
            );
            result
                .binding
                .clone()
                .unwrap_or_else(|| panic!("{backend} backend: a FilterResult with neither binding nor error"))
        })
        .collect();
    assert_eq!(deleted_acl_bindings, vec![acl_binding], "{backend} backend");
    close(admin, backend).await;
    ctx.cleanup().await;
}

/// Translates `testAclsByControllers`.
async fn acls_by_controllers<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    acls(ctx, factory, true).await;
}

/// Translates `testAcls`.
async fn acls_by_brokers<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    acls(ctx, factory, false).await;
}

/// Logs that `test` skipped itself, and why, where CI shows it: libtest keeps
/// a passing test's `eprintln!` output to itself, so this writes to the
/// process's stderr directly.
fn log_skip(test: &str, backend: &str, reason: &str) {
    use std::io::Write;
    let _ = writeln!(
        std::io::stderr(),
        "{test} ({backend} backend): skipped: {reason}; the broker is {:?}",
        kafka_cluster::broker_release()
    );
}

/// The first broker release whose controller writes `min.insync.replicas` as a
/// cluster-level config when it activates a cluster, which ELR (KIP-966) does
/// by default from 4.1.
const ELR_DEFAULT_RELEASE: (u32, u32) = (4, 1);

/// Translates `testDescribeConfigs`: the default BROKER resource, described
/// through the controllers, reports the cluster's `min.insync.replicas` as a
/// dynamic default. Java's cluster sets it to `2`; this one keeps the default,
/// `1` (see the module docs), so the source is what shows the value came from
/// the cluster-level config: `1` is also the static default. Skips itself on a
/// broker older than 4.1, which writes no such config.
async fn describe_configs<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    if kafka_cluster::broker_release() < ELR_DEFAULT_RELEASE {
        log_skip(
            "test_describe_configs",
            backend,
            "no cluster-level min.insync.replicas before 4.1",
        );
        return;
    }
    let admin = create_admin(factory, admin_config(ctx, factory, true)).await;
    let resource = broker_resource("");
    let resource_to_config = admin
        .describe_configs(std::slice::from_ref(&resource), DescribeConfigsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe configs: {e}"));
    let config = resource_to_config
        .get(&resource)
        .unwrap_or_else(|| panic!("{backend} backend: the default resource's config"))
        .as_ref()
        .unwrap_or_else(|e| panic!("{backend} backend: describe configs of the default resource: {e}"));
    let config_entry = config
        .get("min.insync.replicas")
        .unwrap_or_else(|| panic!("{backend} backend: min.insync.replicas"));
    assert_eq!(
        config_entry.source.as_deref(),
        Some("DYNAMIC_DEFAULT_BROKER_CONFIG"),
        "{backend} backend"
    );
    assert_eq!(config_entry.value.as_deref(), Some("1"), "{backend} backend");
    close(admin, backend).await;
}

/// The first broker release whose throttle configs are not read-only
/// (KAFKA-19984).
const QUOTA_CONFIGS_WRITABLE_RELEASE: (u32, u32) = (4, 3);

/// Translates `testQuotaConfigsIsReadOnlyShouldBeFalse`: the three throttle
/// configs a node's resource takes through the controllers are reported as not
/// read-only. Skips itself on a broker older than 4.3, which reports them as
/// read-only: the fix is on the broker side.
async fn quota_configs_is_read_only_should_be_false<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    if kafka_cluster::broker_release() < QUOTA_CONFIGS_WRITABLE_RELEASE {
        log_skip(
            "test_quota_configs_is_read_only_should_be_false",
            backend,
            "the throttle configs are read-only on a broker older than 4.3",
        );
        return;
    }
    let node_id = *ctx.cluster().controller_ids().iter().next().expect("a controller");
    let node_resource = broker_resource(&node_id.to_string());
    const THROTTLES: [&str; 3] = [
        "leader.replication.throttled.rate",
        "follower.replication.throttled.rate",
        "replica.alter.log.dirs.io.max.bytes.per.second",
    ];
    let restore = HashMap::from([(node_resource.clone(), THROTTLES.map(str::to_string).to_vec())]);
    let config = admin_config(ctx, factory, true);
    restoring_configs(ctx, restore, async move {
        let admin = create_admin(factory, config).await;
        let alterations = HashMap::from([(
            node_resource.clone(),
            THROTTLES.iter().map(|name| set(name, "16800")).collect::<Vec<_>>(),
        )]);
        let altered = admin
            .incremental_alter_configs(&alterations, AlterConfigsOptions::new())
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: incremental alter configs: {e}"));
        all_of_exactly(
            &admin,
            &altered,
            std::slice::from_ref(&node_resource),
            "incrementalAlterConfigs",
        );
        retry_on_error_with_timeout(CONFIG_PROPAGATION_TIMEOUT, || async {
            let configs = admin
                .describe_configs(std::slice::from_ref(&node_resource), DescribeConfigsOptions::new())
                .await
                .map_err(|e| format!("{backend} backend: describeConfigs: {e}"))?;
            let config = configs
                .get(&node_resource)
                .ok_or_else(|| format!("{backend} backend: no config for the node"))?
                .as_ref()
                .map_err(|e| format!("{backend} backend: describeConfigs of the node: {e}"))?;
            let read_only: HashSet<&str> = THROTTLES
                .iter()
                .copied()
                .filter(|name| config.get(name).is_none_or(|entry| entry.is_read_only))
                .collect();
            if read_only.is_empty() {
                Ok(())
            } else {
                Err(format!("{backend} backend: missing or read-only: {read_only:?}"))
            }
        })
        .await;
        close(admin, backend).await;
    })
    .await;
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

multilanguage_admin_test!(
    test_put_brokers_in_bootstrap_controllers_config,
    put_brokers_in_bootstrap_controllers_config
);
multilanguage_admin_test!(
    test_put_controllers_in_bootstrap_brokers_config,
    put_controllers_in_bootstrap_brokers_config
);
multilanguage_admin_test!(test_describe_cluster_by_controllers, describe_cluster_by_controllers);
multilanguage_admin_test!(test_describe_cluster, describe_cluster_by_brokers);
multilanguage_admin_test!(test_describe_features_by_controllers, describe_features_by_controllers);
multilanguage_admin_test!(test_describe_features, describe_features_by_brokers);
multilanguage_admin_test!(test_update_features_by_controllers, update_features_by_controllers);
multilanguage_admin_test!(test_update_features, update_features_by_brokers);
multilanguage_admin_test!(
    test_using_bootstrap_controllers_on_unsupported_admin_api,
    using_bootstrap_controllers_on_unsupported_admin_api
);
multilanguage_admin_test!(
    test_incremental_alter_configs_by_controllers,
    incremental_alter_configs_by_controllers
);
multilanguage_admin_test!(test_incremental_alter_configs, incremental_alter_configs_by_brokers);
multilanguage_admin_test!(
    test_alter_reassignments_with_bootstrap_controllers,
    alter_reassignments_with_bootstrap_controllers,
    ClusterConfig::with_brokers(3)
);
multilanguage_admin_test!(test_acls_by_controllers, acls_by_controllers, authorizer_single_broker());
multilanguage_admin_test!(test_acls, acls_by_brokers, authorizer_single_broker());
multilanguage_admin_test!(test_describe_configs, describe_configs);
multilanguage_admin_test!(
    test_quota_configs_is_read_only_should_be_false,
    quota_configs_is_read_only_should_be_false
);
