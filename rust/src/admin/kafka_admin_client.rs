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

//! The network-backed administrative client.
//!
//! Translated from `org.apache.kafka.clients.admin.KafkaAdminClient`. Per
//! `.claude/rules/admin-client.md` §1 the RPC methods are plain sync `fn`s that
//! enqueue a [`Call`] onto the single background task
//! ([`AdminClientRunnable`]) and return a `*Result` holding per-key
//! [`KafkaFuture`]s; only [`close`](KafkaAdminClient::close) is `async`.
//!
//! # `describeTopics`
//!
//! By **name**, topics are described as Java 4.3.1 describes them: a
//! `describeCluster` call for the node map, then the paginated KIP-966
//! `describeTopicPartitions` call, which follows each response's `NextCursor`,
//! honours `DescribeTopicsOptions::partition_size_limit_per_response`, and
//! falls back to the Metadata API (`describeTopics`,
//! `generateDescribeTopicsCallWithMetadataApi`) on `UnsupportedVersionException`.
//! By **id** Java uses the Metadata API only (`handleDescribeTopicsByIds`), and
//! so does this port.
//!
//! `bootstrap.controllers` (KIP-919) is unsupported in Phase 1, so the metadata
//! refresh always uses the broker `Metadata` API (never `DescribeCluster`), and
//! controller/least-loaded node selection never needs the
//! `LeastLoadedBrokerOrActiveKController` provider.

use crate::admin::CreateTopicsResult;
use crate::common::requests::DescribeClusterRequest;
use crate::common::requests::MetadataResponse;
use crate::{kafka_debug, kafka_error};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::AlterReplicaLogDirsRequestData;
use crate::AlterUserScramCredentialsRequestData;
use crate::ApiVersions;
use crate::ClientUtils;
use crate::CreateAclsRequestData;
use crate::CreatePartitionsRequestData;
use crate::CreateTopicsRequestData;
use crate::DefaultHostResolver;
use crate::DeleteAclsRequestData;
use crate::DeleteTopicsRequestData;
use crate::DescribeClusterRequestData;
use crate::DescribeConfigsRequestData;
use crate::DescribeLogDirsRequestData;
use crate::DescribeTopicPartitionsRequestData;
use crate::DescribeUserScramCredentialsRequestData;
use crate::DescribeUserScramCredentialsResponseData;
use crate::HostResolver;
use crate::IncrementalAlterConfigsRequestData;
use crate::KafkaClient;
use crate::ListConfigResourcesRequestData;
use crate::ListGroupsRequestData;
use crate::MetadataRecoveryStrategy;
use crate::MetadataUpdater;
use crate::NetworkClient;
use crate::admin::ConfigEntryOptionsBuilder;
use crate::admin::config_entry::{ConfigSource, ConfigSynonym, ConfigType};
use crate::alter_replica_log_dirs_request_data::{AlterReplicaLogDir, AlterReplicaLogDirTopic};
use crate::alter_user_scram_credentials_request_data::{ScramCredentialDeletion, ScramCredentialUpsertion};
use crate::common::acl::{AclBinding, AclBindingFilter, AclOperation};
use crate::common::config::{ConfigResource, config_resource};
use crate::common::errors::ApiError;
use crate::common::internals::KafkaFutureImpl;
use crate::common::network::ChannelBuilders;
use crate::common::network::Selectable;
use crate::common::network::Selector;
use crate::common::network::selectable::USE_DEFAULT_BUFFER_SIZE;
use crate::common::protocol::Errors;
use crate::common::quota::{ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter};
use crate::common::requests::{
    ConcreteResponse, CreateAclsRequest, DeleteAclsRequest, DeleteAclsResponse, DescribeAclsResponse,
    DescribeLogDirsResponse, DescribeTopicPartitionsResponse, RequestBuilder, alter_client_quotas_request,
    alter_replica_log_dirs_request, alter_user_scram_credentials_request, create_acls_request,
    create_delegation_token_request, create_partitions_request, create_topics_request, delete_acls_request,
    delete_topics_request, describe_acls_request, describe_client_quotas_request, describe_cluster_request,
    describe_configs_request, describe_delegation_token_request, describe_log_dirs_request,
    describe_topic_partitions_request, describe_user_scram_credentials_request, expire_delegation_token_request,
    incremental_alter_configs_request, list_config_resources_request, list_groups_request, metadata_request,
    renew_delegation_token_request,
};
use crate::common::security::auth::KafkaPrincipal;
use crate::common::security::scram::internals::{ScramFormatter, ScramMechanism as InternalScramMechanism};
use crate::common::security::token::delegation::{DelegationToken, TokenInformation};
use crate::common::utils::{ExponentialBackoff, LogContext, SystemTime, Time};
use crate::common::{
    Cluster, Error, GroupState, GroupType, KafkaFuture, TopicCollection, TopicPartition, TopicPartitionInfo, Uuid,
};
use crate::consumer::OffsetAndMetadata;
use crate::create_acls_request_data::AclCreation;
use crate::create_partitions_request_data::{CreatePartitionsAssignment, CreatePartitionsTopic};
use crate::create_topics_request_data::CreatableTopic;
use crate::delete_acls_request_data::DeleteAclsFilter;
use crate::delete_topics_request_data::DeleteTopicState;
use crate::describe_configs_request_data::DescribeConfigsResource;
use crate::describe_log_dirs_request_data::DescribableLogDirTopic;
use crate::describe_topic_partitions_request_data::{Cursor as DescribeTopicPartitionsCursor, TopicRequest};
use crate::describe_topic_partitions_response_data::DescribeTopicPartitionsResponseTopic;
use crate::describe_user_scram_credentials_request_data::UserName;
use crate::incremental_alter_configs_request_data::{AlterConfigsResource, AlterableConfig};

use super::RecordsToDelete;
use super::internals::AbortTransactionHandler;
use super::internals::AdminApiFuture;
use super::internals::AdminMetadataManager;
use super::internals::AdminUtils;
use super::internals::AlterConsumerGroupOffsetsHandler;
use super::internals::CoordinatorKey;
use super::internals::DeleteConsumerGroupOffsetsHandler;
use super::internals::DeleteConsumerGroupsHandler;
use super::internals::DeleteGroupsHandler;
use super::internals::DeleteRecordsHandler;
use super::internals::DescribeClassicGroupsHandler;
use super::internals::DescribeConsumerGroupsHandler;
use super::internals::DescribeProducersHandler;
use super::internals::DescribeTransactionsHandler;
use super::internals::FenceProducersHandler;
use super::internals::ListConsumerGroupOffsetsHandler;
use super::internals::ListOffsetsHandler;
use super::internals::ListTransactionsHandler;
use super::internals::PartitionLeaderCache;
use super::internals::RemoveMembersFromConsumerGroupHandler;
use super::internals::admin_client_runnable::enqueue_within_max_retries;
use super::internals::{AdminApiDriver, RequestSpec};
use super::internals::{AdminClientRunnable, ShutdownSignal};
use super::internals::{Call, HandleResult, MaybeRetryOutcome, NodeProvider};
use super::{
    AbortTransactionOptions, AbortTransactionResult, AbortTransactionSpec, DescribeProducersOptions,
    DescribeProducersResult, DescribeTransactionsOptions, DescribeTransactionsResult, FenceProducersOptions,
    FenceProducersResult, ListTransactionsOptions, ListTransactionsResult, TerminateTransactionOptions,
    TerminateTransactionResult,
};
use super::{
    Admin, AdminClientConfig, AlterClientQuotasOptions, AlterClientQuotasResult, AlterConfigOp, AlterConfigsOptions,
    AlterConfigsResult, AlterConsumerGroupOffsetsOptions, AlterConsumerGroupOffsetsResult,
    AlterPartitionReassignmentsOptions, AlterPartitionReassignmentsResult, AlterReplicaLogDirsOptions,
    AlterReplicaLogDirsResult, Config, ConfigEntry, CreateAclsOptions, CreateAclsResult, CreateDelegationTokenOptions,
    CreateDelegationTokenResult, CreatePartitionsOptions, CreatePartitionsResult, CreateTopicsOptions,
    DeleteAclsOptions, DeleteAclsResult, DeleteConsumerGroupOffsetsOptions, DeleteConsumerGroupOffsetsResult,
    DeleteConsumerGroupsOptions, DeleteConsumerGroupsResult, DeleteRecordsOptions, DeleteRecordsResult,
    DeleteTopicsOptions, DeleteTopicsResult, DescribeAclsOptions, DescribeAclsResult, DescribeClassicGroupsOptions,
    DescribeClassicGroupsResult, DescribeClientQuotasOptions, DescribeClientQuotasResult, DescribeClusterOptions,
    DescribeClusterResult, DescribeConfigsOptions, DescribeConfigsResult, DescribeConsumerGroupsOptions,
    DescribeConsumerGroupsResult, DescribeDelegationTokenOptions, DescribeDelegationTokenResult,
    DescribeLogDirsOptions, DescribeLogDirsResult, DescribeReplicaLogDirsOptions, DescribeReplicaLogDirsResult,
    DescribeTopicsOptions, DescribeTopicsResult, ElectLeadersOptions, ElectLeadersResult, ExpireDelegationTokenOptions,
    ExpireDelegationTokenResult, FilterResult, FilterResults, GroupListing, ListConfigResourcesOptions,
    ListConfigResourcesResult, ListConsumerGroupOffsetsOptions, ListConsumerGroupOffsetsResult,
    ListConsumerGroupOffsetsSpec, ListGroupsOptions, ListGroupsResult, ListOffsetsOptions, ListOffsetsResult,
    ListPartitionReassignmentsOptions, ListPartitionReassignmentsResult, ListTopicsOptions, ListTopicsResult,
    LogDirDescription, NewPartitionReassignment, NewPartitions, NewTopic, OffsetSpec, PartitionReassignment,
    RemoveMembersFromConsumerGroupOptions, RemoveMembersFromConsumerGroupResult, RenewDelegationTokenOptions,
    RenewDelegationTokenResult, ReplicaInfo, ReplicaLogDirInfo, TopicDescription, TopicListing, TopicMetadataAndConfig,
};
use super::{
    AlterUserScramCredentialsOptions, AlterUserScramCredentialsResult, DescribeUserScramCredentialsOptions,
    DescribeUserScramCredentialsResult, ScramMechanism, UserScramCredentialAlteration, UserScramCredentialDeletion,
    UserScramCredentialUpsertion,
};
use super::{
    DescribeFeaturesOptions, DescribeFeaturesResult, FeatureMetadata, FeatureUpdate, FinalizedVersionRange,
    SupportedVersionRange, UpdateFeaturesOptions, UpdateFeaturesResult,
};
use crate::AlterPartitionReassignmentsRequestData;
use crate::ApiVersionsResponseData;
use crate::CreateDelegationTokenRequestData;
use crate::ExpireDelegationTokenRequestData;
use crate::ListPartitionReassignmentsRequestData;
use crate::RenewDelegationTokenRequestData;
use crate::UpdateFeaturesRequestData;
use crate::alter_partition_reassignments_request_data::{ReassignablePartition, ReassignableTopic};
use crate::common::TopicPartitionReplica;
use crate::common::requests::{
    ElectLeadersResponse, JoinGroupRequest, alter_partition_reassignments_request, api_versions_request,
    elect_leaders_request, list_partition_reassignments_request, update_features_request,
};
use crate::common::{ElectionType, Node};
use crate::create_delegation_token_request_data::CreatableRenewers;
use crate::leave_group_request_data::MemberIdentity;
use crate::list_partition_reassignments_request_data::ListPartitionReassignmentsTopics;
use crate::update_features_request_data::FeatureUpdateKey;
use std::collections::{BTreeSet, HashSet};
use std::sync::atomic::{self, AtomicI32};

/// The default reason sent in a `LeaveGroup` request when an admin removes a
/// member without providing one. Mirrors
/// `KafkaAdminClient.DEFAULT_LEAVE_GROUP_REASON`.
const DEFAULT_LEAVE_GROUP_REASON: &str = "member was removed by an admin";

/// Process-wide counter for deriving a default `client.id`.
///
/// Corresponds to Java's `static AtomicInteger ADMIN_CLIENT_ID_SEQUENCE`
/// (`KafkaAdminClient.java:325`), which starts at 1. Crate-level (not
/// per-instance) to match Java's static scope, so successive admin clients in
/// one process get distinct ids.
static ADMIN_CLIENT_ID_SEQUENCE: AtomicI32 = AtomicI32::new(1);

/// The `RETRY_BACKOFF_EXP_BASE` used by the admin retry backoff (Java constant).
const RETRY_BACKOFF_EXP_BASE: i32 = 2;
/// The `RETRY_BACKOFF_JITTER` used by the admin retry backoff (Java constant).
const RETRY_BACKOFF_JITTER: f64 = 0.2;

/// Upper bound on `close`'s wait, mirroring
/// `Math.min(TimeUnit.DAYS.toMillis(365), waitTimeMs)` in
/// `KafkaAdminClient.close(Duration)` ("Limit the timeout to a year").
const MAX_CLOSE_WAIT_TIME_MS: i64 = 365 * 24 * 60 * 60 * 1000;

/// State shared between the `KafkaAdminClient` handle and (indirectly) the
/// background task.
struct Shared {
    client_id: String,
    default_api_timeout_ms: i32,
    /// The `request.timeout.ms` config, used as the default transaction timeout
    /// for `fenceProducers` (mirrors `KafkaAdminClient.requestTimeoutMs`).
    request_timeout_ms: i32,
    admin_tx: mpsc::UnboundedSender<Call>,
    wakeup: Arc<Notify>,
    shutdown: Arc<ShutdownSignal>,
    metadata_manager: AdminMetadataManager,
    time: Arc<dyn Time>,
    bg_handle: Mutex<Option<JoinHandle<()>>>,
    /// Retry-backoff parameters for the `AdminApiDriver` (mirrors the fields
    /// passed to the driver in `invokeDriver`).
    retry_backoff_ms: i64,
    retry_backoff_max_ms: i64,
    /// The `retries` config (Java's `KafkaAdminClient.maxRetries`), checked by
    /// [`runnable_call`] as `AdminClientRunnable.enqueue` checks it.
    max_retries: i32,
    /// Cache of partition-to-leader mappings shared across driver-backed calls
    /// (`deleteRecords`), mirroring `KafkaAdminClient.partitionLeaderCache`.
    partition_leader_cache: Arc<PartitionLeaderCache>,
    /// Prefix for log lines emitted on the application side (`close`), mirroring
    /// `KafkaAdminClient.logContext`.
    log_context: LogContext,
}

/// The administrative client for Kafka.
///
/// Corresponds to `org.apache.kafka.clients.admin.KafkaAdminClient`.
#[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient")]
pub struct KafkaAdminClient {
    shared: Arc<Shared>,
}

impl KafkaAdminClient {
    /// Returns the response error message with a fallback to the error code's
    /// default message. Mirrors Java's `ApiError.messageWithFallback`.
    pub(crate) fn message_with_fallback(code: i16, message: &Option<String>) -> String {
        // Java `ApiError.messageWithFallback()` falls back to the code's default text
        // ONLY when the broker sent no message (null); a non-null empty message is
        // returned verbatim.
        match message {
            Some(m) => m.clone(),
            None => Errors::for_code(code).message().to_string(),
        }
    }

    /// Sentinel for "no hard-shutdown deadline set".
    ///
    /// Translates Java's `KafkaAdminClient.INVALID_SHUTDOWN_TIME`
    /// (`KafkaAdminClient.java:335`): it is lower than every reachable deadline,
    /// so the "is an earlier deadline already installed?" comparison in
    /// [`close`](Self::close) orders the same way.
    pub(crate) const NO_HARD_SHUTDOWN: i64 = i64::MIN;

    /// Creates a network-backed admin client from configuration, spawning the
    /// background I/O task.
    ///
    /// Mirrors `KafkaAdminClient.createInternal`. Selects the channel builder
    /// from `security.protocol` + `ssl.*` / `sasl.*` (PLAINTEXT / SSL /
    /// SASL_PLAINTEXT / SASL_SSL); SASL mechanism PLAIN only.
    ///
    /// # Errors
    ///
    /// Returns an error if the bootstrap addresses cannot be resolved or the
    /// channel builder cannot be created.
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient#KafkaAdminClient")]
    pub(crate) fn new(config: AdminClientConfig) -> Result<Self, Error> {
        // Java wraps the whole constructor in `catch (Throwable exc)` and relabels
        // every failure (`KafkaAdminClient.java:569-573` / `:592-595`):
        //
        //     throw new KafkaException("Failed to create new KafkaAdminClient", exc);
        //
        // so a caller has one class and one message to guard construction with.
        // Without it the three failure points surfaced in three different shapes,
        // none of them Java's.
        //
        // The `closeQuietly(metrics, ..)` / `closeQuietly(networkClient, ..)` half of
        // Java's catch is not needed here: the `Selector` / `NetworkClient` are
        // RVO'd locals that `Drop` cleans up, and every fallible point precedes
        // their construction.
        Self::new_inner(config).map_err(|e| Error::kafka_message_source("Failed to create new KafkaAdminClient", e))
    }

    /// Returns the configured `client.id`, or generates `adminclient-<n>` when
    /// it is empty.
    ///
    /// Translated from `KafkaAdminClient.generateClientId`
    /// (`KafkaAdminClient.java:478-483`); `<n>` comes from the process-wide
    /// [`ADMIN_CLIENT_ID_SEQUENCE`], starting at 1.
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient#generateClientId")]
    pub(crate) fn generate_client_id(config: &AdminClientConfig) -> String {
        let client_id = config.client_id();
        if !client_id.is_empty() {
            return client_id.to_string();
        }
        format!(
            "adminclient-{}",
            ADMIN_CLIENT_ID_SEQUENCE.fetch_add(1, atomic::Ordering::Relaxed)
        )
    }

    fn new_inner(config: AdminClientConfig) -> Result<Self, Error> {
        let client_id = Self::generate_client_id(&config);
        let log_context = LogContext::new(format!("[AdminClient clientId={client_id}] "));

        let bootstrap: Vec<String> = config.bootstrap_servers().to_vec();
        let addresses = ClientUtils::parse_and_validate_addresses(&bootstrap, config.client_dns_lookup())?;

        // Java's `Time.SYSTEM`.
        let time: Arc<dyn Time> = Arc::new(SystemTime);

        let metadata_manager = AdminMetadataManager::new(
            config.retry_backoff_ms(),
            config.metadata_max_age_ms(),
            false, // bootstrap.controllers unsupported in Phase 1
            log_context.clone(),
        );
        // Seed with the bootstrap cluster so the first metadata refresh has
        // nodes to talk to (mirrors Java's constructor `metadataManager.update`).
        let now = time.milliseconds();
        metadata_manager.update(Cluster::bootstrap(&addresses), now);

        // Selects the channel builder from `security.protocol` + `ssl.*` /
        // `sasl.*` (PLAINTEXT / SSL / SASL_PLAINTEXT / SASL_SSL); SASL mechanism
        // PLAIN only. Mirrors `ClientUtils.createChannelBuilder(config, time,
        // logContext)` as used by `KafkaAdminClient.createInternal`.
        let channel_builder = ChannelBuilders::client_channel_builder(
            config.security_protocol(),
            Some(config.ssl_config()),
            Some(config.sasl_config()),
            None,
            &client_id,
            log_context.clone(),
        )
        // `ConfigException` in Java (`SslFactory.java:104-107`), i.e. inside the
        // `KafkaException` hierarchy; `illegal_argument` put it outside, where
        // `is_kafka_error()` answers `false`. Same fix as `KafkaProducer::new`.
        .map_err(|e| Error::config_message(format!("Failed to create channel builder: {e}")))?;
        let mut selector = Selector::with_defaults_and_log_context(
            config.connections_max_idle_ms(),
            channel_builder,
            log_context.clone(),
        );
        selector.set_time(Arc::clone(&time));
        let api_versions = Arc::new(ApiVersions::new());

        let mut client = Self::create_network_client(
            &config,
            selector,
            metadata_manager.updater(),
            &client_id,
            api_versions,
            DefaultHostResolver::new(),
            log_context.clone(),
        );
        client.set_time(Arc::clone(&time));

        crate::preview_warning::log_preview_warning(&log_context);
        let (admin, runnable) = Self::build(client, metadata_manager, &config, client_id, time, log_context)?;
        admin.spawn(runnable);
        Ok(admin)
    }

    /// Builds the admin `NetworkClient` with the arguments
    /// `KafkaAdminClient.createInternal` passes to `ClientUtils.createNetworkClient`
    /// (`KafkaAdminClient.java:553-566`).
    ///
    /// `maxInFlightRequestsPerConnection` is `1` (`:561`). The admin loop sends at
    /// most one call per node and checks `client.ready(..)` first, so the limit
    /// does not change what is sent; it matters to `least_loaded_node` and
    /// `ready`, which treat a node with an in-flight request as busy only when the
    /// connection cannot take another one.
    pub(crate) fn create_network_client<S: Selectable, H: HostResolver>(
        config: &AdminClientConfig,
        selector: S,
        metadata_updater: Box<dyn MetadataUpdater>,
        client_id: &str,
        api_versions: Arc<ApiVersions>,
        host_resolver: H,
        log_context: LogContext,
    ) -> NetworkClient<S, H> {
        NetworkClient::with_metadata_updater(
            selector,
            metadata_updater,
            client_id,
            1, // maxInFlightRequestsPerConnection (`KafkaAdminClient.java:561`)
            config.reconnect_backoff_ms(),
            config.reconnect_backoff_max_ms(),
            USE_DEFAULT_BUFFER_SIZE,
            USE_DEFAULT_BUFFER_SIZE,
            config.request_timeout_ms(),
            config.socket_connection_setup_timeout_ms(),
            config.socket_connection_setup_timeout_ms(),
            true, // discover_broker_versions
            api_versions,
            host_resolver,
            MetadataRecoveryStrategy::None,
            log_context,
        )
    }

    /// Wires up the shared state and the (not-yet-running) background runnable.
    ///
    /// `client_id` is the resolved id from [`generate_client_id`](Self::generate_client_id),
    /// not `config.client_id()`, which may be empty.
    fn build<C: KafkaClient + Send + 'static>(
        client: C,
        metadata_manager: AdminMetadataManager,
        config: &AdminClientConfig,
        client_id: String,
        time: Arc<dyn Time>,
        log_context: LogContext,
    ) -> Result<(Self, AdminClientRunnable<C>), Error> {
        let (admin_tx, admin_rx) = mpsc::unbounded_channel();
        let wakeup = client.wakeup_notify();
        let shutdown = Arc::new(ShutdownSignal::new());
        // Propagated rather than `expect`ed: Java's constructor-wide
        // `catch (Throwable exc)` (`KafkaAdminClient.java:569-573`) converts every
        // construction failure into a `KafkaException`, and `new` is where
        // that wrap happens. `ExponentialBackoff::new` only rejects an
        // out-of-range jitter, so this is unreachable with the constant above — but
        // panicking on the admin construction path is precisely what Java does not.
        let retry_backoff = ExponentialBackoff::new(
            config.retry_backoff_ms(),
            RETRY_BACKOFF_EXP_BASE,
            config.retry_backoff_max_ms(),
            RETRY_BACKOFF_JITTER,
        )
        .map_err(Error::config_message)?;

        let runnable = AdminClientRunnable::new(
            client,
            metadata_manager.clone(),
            admin_rx,
            retry_backoff,
            config.retry_backoff_ms(),
            config.retries(),
            config.request_timeout_ms(),
            Arc::clone(&time),
            Arc::clone(&shutdown),
            log_context.clone(),
        );

        let shared = Shared {
            client_id,
            default_api_timeout_ms: config.default_api_timeout_ms(),
            request_timeout_ms: config.request_timeout_ms(),
            admin_tx,
            wakeup,
            shutdown,
            metadata_manager,
            time,
            bg_handle: Mutex::new(None),
            retry_backoff_ms: config.retry_backoff_ms(),
            retry_backoff_max_ms: config.retry_backoff_max_ms(),
            max_retries: config.retries(),
            partition_leader_cache: Arc::new(PartitionLeaderCache::new()),
            log_context,
        };
        Ok((Self { shared: Arc::new(shared) }, runnable))
    }

    /// Spawns the background task.
    fn spawn<C: KafkaClient + Send + 'static>(&self, mut runnable: AdminClientRunnable<C>) {
        let handle = tokio::task::spawn(async move {
            runnable.run().await;
        });
        *self.shared.bg_handle.lock().unwrap() = Some(handle);
    }

    /// Submits a call to the background task, failing it immediately if the
    /// client is closing. Mirrors `AdminClientRunnable.call` / `enqueue`; see
    /// [`runnable_call`], which every submission path shares.
    fn submit(&self, call: Call) {
        runnable_call(
            &self.shared.admin_tx,
            &self.shared.wakeup,
            &self.shared.shutdown,
            self.shared.metadata_manager.using_bootstrap_controllers(),
            self.shared.max_retries,
            &self.shared.log_context,
            call,
        );
    }

    fn now(&self) -> i64 {
        self.shared.time.milliseconds()
    }

    /// `describeTopics` by name: describes the cluster for the node map, then
    /// issues the paginated `describeTopicPartitions` call (KIP-966), which falls
    /// back to the Metadata API on an older broker. Names that cannot be
    /// represented in a request fail at once, and no request is sent if none is
    /// left.
    ///
    /// Translated from
    /// `KafkaAdminClient.handleDescribeTopicsByNamesWithDescribeTopicPartitionsApi`
    /// (`KafkaAdminClient.java:2327-2367`). The follow-up call is chained on the
    /// `describeCluster` nodes future as Java chains it with `whenComplete`, so it
    /// is issued from the I/O task through `runnable.call` ([`DriverContext::call`]).
    fn handle_describe_topics_by_names_with_describe_topic_partitions_api(
        &self,
        topic_names: &[String],
        options: &DescribeTopicsOptions,
    ) -> HashMap<String, KafkaFuture<TopicDescription>> {
        let mut topic_futures: HashMap<String, KafkaFutureImpl<TopicDescription>> = HashMap::new();
        let mut topic_names_list: Vec<String> = Vec::new();
        for topic_name in topic_names {
            if topic_name_is_unrepresentable(topic_name) {
                let future: KafkaFutureImpl<TopicDescription> = KafkaFutureImpl::new();
                future.complete_with_error(Error::with_message(
                    Errors::InvalidTopicError,
                    format!("The given topic name '{topic_name}' cannot be represented in a request."),
                ));
                topic_futures.insert(topic_name.clone(), future);
            } else if let std::collections::hash_map::Entry::Vacant(entry) = topic_futures.entry(topic_name.clone()) {
                entry.insert(KafkaFutureImpl::new());
                topic_names_list.push(topic_name.clone());
            }
        }
        let public: HashMap<String, KafkaFuture<TopicDescription>> = topic_futures
            .iter()
            .map(|(name, future)| (name.clone(), future.future()))
            .collect();
        if topic_names_list.is_empty() {
            return public;
        }

        // First, we need to retrieve the node info.
        let (_cluster_result, nodes) =
            self.describe_cluster_with_nodes_handle(DescribeClusterOptions::new().set_timeout_ms(options.timeout_ms()));
        let topic_futures = Arc::new(topic_futures);
        let ctx = self.driver_context();
        let default_api_timeout_ms = self.shared.default_api_timeout_ms;
        let options = options.clone();
        nodes.when_complete(move |result| match result {
            Err(error) => {
                for future in topic_futures.values() {
                    future.complete_with_error(error.clone());
                }
            },
            Ok(nodes) => {
                let now = ctx.time.milliseconds();
                let node_id_map: HashMap<i32, Node> = nodes.iter().map(|node| (node.id(), node.clone())).collect();
                let call = generate_describe_topics_call_with_describe_topic_partitions_api(
                    topic_names_list,
                    topic_futures,
                    node_id_map,
                    options,
                    now,
                    ctx.clone(),
                    default_api_timeout_ms,
                );
                ctx.call(call);
            },
        });
        public
    }

    /// `describeCluster`, also returning the completable handle behind
    /// `DescribeClusterResult.nodes()`.
    ///
    /// `describeTopics` by name chains its `DescribeTopicPartitions` call on
    /// `describeCluster(..).nodes().whenComplete(..)`
    /// (`KafkaAdminClient.java:2350-2364`). Java's public `KafkaFuture` has
    /// `whenComplete`; the Rust one does not (`admin-client.md` §4 keeps it on the
    /// crate-internal `KafkaFutureImpl`), so the handle is returned alongside the
    /// public result.
    fn describe_cluster_with_nodes_handle(
        &self,
        options: DescribeClusterOptions,
    ) -> (DescribeClusterResult, KafkaFutureImpl<Vec<Node>>) {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);

        let nodes_handle: KafkaFutureImpl<Vec<Node>> = KafkaFutureImpl::new();
        let controller_handle: KafkaFutureImpl<Option<Node>> = KafkaFutureImpl::new();
        let cluster_id_handle: KafkaFutureImpl<String> = KafkaFutureImpl::new();
        let authorized_ops_handle: KafkaFutureImpl<Option<BTreeSet<AclOperation>>> = KafkaFutureImpl::new();

        let public = DescribeClusterResult::new(
            nodes_handle.future(),
            controller_handle.future(),
            cluster_id_handle.future(),
            authorized_ops_handle.future(),
        );

        // `useMetadataRequest` is toggled to true by the UnsupportedVersion
        // handler so the retry falls back to a Metadata request (mirrors Java).
        let use_metadata_request = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mm = self.shared.metadata_manager.clone();
        let include_authorized_operations = options.include_authorized_operations();
        let include_fenced_brokers = options.include_fenced_brokers();

        let req_use_metadata = Arc::clone(&use_metadata_request);
        let req_mm = mm.clone();
        let create_request = Box::new(move |_timeout_ms: i32| {
            if req_use_metadata.load(std::sync::atomic::Ordering::Acquire) {
                // Only requests node information; allow_auto_topic_creation=true
                // simplifies communication with older brokers.
                let mut data = crate::MetadataRequestData::new();
                data.set_topics(Some(Vec::new()));
                data.set_allow_auto_topic_creation(true);
                data.set_include_cluster_authorized_operations(include_authorized_operations);
                Ok(Box::new(metadata_request::Builder::with_data(data)) as Box<dyn RequestBuilder>)
            } else {
                if req_mm.using_bootstrap_controllers() && include_fenced_brokers {
                    return Err(Error::local_illegal_argument(
                        "Cannot request fenced brokers from controller endpoint",
                    ));
                }
                let endpoint_type = if req_mm.using_bootstrap_controllers() {
                    DescribeClusterRequest::ENDPOINT_TYPE_CONTROLLER
                } else {
                    DescribeClusterRequest::ENDPOINT_TYPE_BROKER
                };
                let mut data = DescribeClusterRequestData::new();
                data.set_include_cluster_authorized_operations(include_authorized_operations);
                data.set_endpoint_type(endpoint_type);
                data.set_include_fenced_brokers(include_fenced_brokers);
                Ok(Box::new(describe_cluster_request::Builder::new(data)) as Box<dyn RequestBuilder>)
            }
        });

        let resp_use_metadata = Arc::clone(&use_metadata_request);
        let resp_nodes = nodes_handle.clone();
        let resp_controller = controller_handle.clone();
        let resp_cluster_id = cluster_id_handle.clone();
        let resp_authorized = authorized_ops_handle.clone();
        let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
            if resp_use_metadata.load(std::sync::atomic::Ordering::Acquire) {
                let ConcreteResponse::Metadata(metadata_response) = response else {
                    return HandleResult::Retry(Error::local_illegal_state("Expected a Metadata response"));
                };
                resp_nodes.complete(metadata_response.brokers().to_vec());
                let controller = metadata_response
                    .controller()
                    .filter(|c| c.id() != MetadataResponse::NO_CONTROLLER_ID)
                    .cloned();
                resp_controller.complete(controller);
                resp_cluster_id.complete(metadata_response.cluster_id().unwrap_or_default().to_string());
                resp_authorized.complete(AdminUtils::valid_acl_operations(
                    metadata_response.cluster_authorized_operations(),
                ));
            } else {
                let ConcreteResponse::DescribeCluster(describe_response) = response else {
                    return HandleResult::Retry(Error::local_illegal_state("Expected a DescribeCluster response"));
                };
                let error = Errors::for_code(describe_response.data().error_code);
                if error != Errors::None {
                    // Mirrors Java's `handleFailure(error.exception(errorMessage))`:
                    // fail all four futures directly rather than retrying.
                    let err = api_error(describe_response.data().error_code, &describe_response.data().error_message);
                    resp_nodes.complete_with_error(err.clone());
                    resp_controller.complete_with_error(err.clone());
                    resp_cluster_id.complete_with_error(err.clone());
                    resp_authorized.complete_with_error(err);
                    return HandleResult::Done;
                }
                let nodes = describe_response.nodes();
                let controller_id = describe_response.data().controller_id;
                resp_nodes.complete(nodes.values().cloned().collect());
                // Controller is None if the controller id is NO_CONTROLLER_ID.
                resp_controller.complete(nodes.get(&controller_id).cloned());
                resp_cluster_id.complete(describe_response.data().cluster_id.clone());
                resp_authorized.complete(AdminUtils::valid_acl_operations(
                    describe_response.data().cluster_authorized_operations,
                ));
            }
            HandleResult::Done
        });

        let fail_nodes = nodes_handle.clone();
        let fail_controller = controller_handle.clone();
        let fail_cluster_id = cluster_id_handle.clone();
        let fail_authorized = authorized_ops_handle.clone();
        let handle_failure = Box::new(move |error: &Error| {
            fail_nodes.complete_with_error(error.clone());
            fail_controller.complete_with_error(error.clone());
            fail_cluster_id.complete_with_error(error.clone());
            fail_authorized.complete_with_error(error.clone());
        });

        let uv_mm = mm.clone();
        let uv_use_metadata = Arc::clone(&use_metadata_request);
        let handle_uv = Box::new(move || {
            if uv_mm.using_bootstrap_controllers() {
                return false;
            }
            if uv_use_metadata.load(std::sync::atomic::Ordering::Acquire) {
                return false;
            }
            // If the UnsupportedVersion was caused by requesting fenced brokers
            // (only supported at v2+), do not fall back to the metadata request.
            if include_fenced_brokers {
                return false;
            }
            uv_use_metadata.store(true, std::sync::atomic::Ordering::Release);
            true
        });

        let call = Call::new(
            "listNodes",
            deadline,
            NodeProvider::LeastLoadedBrokerOrActiveKController,
            create_request,
            handle_response,
            handle_failure,
            handle_uv,
        );
        self.submit(call);
        (public, nodes_handle)
    }

    /// Builds the context used to submit `AdminApiDriver`-generated calls
    /// (mirrors the closure over `runnable` in `KafkaAdminClient.maybeSendRequests`).
    fn driver_context(&self) -> DriverContext {
        DriverContext {
            tx: self.shared.admin_tx.clone(),
            wakeup: Arc::clone(&self.shared.wakeup),
            shutdown: Arc::clone(&self.shared.shutdown),
            using_bootstrap_controllers: self.shared.metadata_manager.using_bootstrap_controllers(),
            max_retries: self.shared.max_retries,
            time: Arc::clone(&self.shared.time),
            log_context: LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id)),
        }
    }

    /// Builds the exponential retry backoff used by `AdminApiDriver`-backed RPCs.
    fn retry_backoff(&self) -> ExponentialBackoff {
        ExponentialBackoff::new(
            self.shared.retry_backoff_ms,
            RETRY_BACKOFF_EXP_BASE,
            self.shared.retry_backoff_max_ms,
            RETRY_BACKOFF_JITTER,
        )
        .expect("ExponentialBackoff::new only fails on invalid jitter")
    }

    /// Drives the `listGroups` broker-enumeration RPC: a `findAllBrokers`
    /// metadata call whose response fans out one per-broker `ListGroups` call,
    /// all feeding a shared [`ListGroupsResults`] accumulator.
    ///
    /// `maybe_add` maps a wire `ListedGroup` to an optional keyed listing
    /// (returning `None` filters the group out). Mirrors the structure of
    /// `KafkaAdminClient.listGroups`.
    fn submit_list_groups<F>(
        &self,
        deadline: i64,
        states_filter: Vec<String>,
        types_filter: Vec<String>,
        maybe_add: F,
    ) -> KafkaFuture<Vec<Result<GroupListing, Error>>>
    where
        F: Fn(&crate::list_groups_response_data::ListedGroup) -> Option<(String, GroupListing)>
            + Clone
            + Send
            + Sync
            + 'static,
    {
        let all: KafkaFutureImpl<Vec<Result<GroupListing, Error>>> = KafkaFutureImpl::new();
        let public = all.future();
        let ctx = self.driver_context();

        let fail_all = all.clone();
        let handle_failure = Box::new(move |error: &Error| {
            // `new KafkaException("Failed to find brokers to send ListGroups", throwable)`
            // (`KafkaAdminClient.java:3565`). A *bare*
            // `KafkaException`: `is_kafka_error()` true, `is_api_error()` /
            // `is_retriable_error()` / `is_authorization_error()` all false, the cause
            // reachable through `source()`, and the message fixed — it does not carry
            // the cause's text. Reusing `error.error()` made the wrapper inherit the
            // inner class, so a metadata `TimeoutError` came back retriable and
            // causeless (finding 243).
            let wrapped = Error::kafka_message_source("Failed to find brokers to send ListGroups", error.clone());
            fail_all.complete(vec![Err(wrapped)]);
        });

        let create_request = Box::new(move |_timeout_ms: i32| {
            // Empty topic list (just the broker list), matching Java's
            // MetadataRequest with setTopics(emptyList).setAllowAutoTopicCreation(true).
            Ok(Box::new(metadata_request::Builder::with_topics_allow_auto_topic_creation(
                Some(&[]),
                true,
            )) as Box<dyn RequestBuilder>)
        });

        let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
            let ConcreteResponse::Metadata(metadata_response) = response else {
                return HandleResult::Retry(Error::local_illegal_state("Expected a Metadata response"));
            };
            let nodes: Vec<Node> = metadata_response.brokers().to_vec();
            if nodes.is_empty() {
                // Java throws StaleMetadataException (retriable) so the metadata
                // fetch is retried; there is no dedicated StaleMetadata error code
                // in Rust, so we surface a retriable metadata error to trigger the
                // same retry.
                return HandleResult::Retry(Error::with_message(
                    Errors::LeaderNotAvailable,
                    "Metadata fetch failed due to missing broker list",
                ));
            }

            let node_ids: HashSet<i32> = nodes.iter().map(Node::id).collect();
            let results = ListGroupsResults::new(node_ids, all.clone());

            for node in nodes {
                let node_id = node.id();
                let states = states_filter.clone();
                let types = types_filter.clone();
                let node_create = node.clone();
                let create_list_request = Box::new(move |_timeout_ms: i32| {
                    let mut data = ListGroupsRequestData::new();
                    data.set_states_filter(states.clone());
                    data.set_types_filter(types.clone());
                    let _ = &node_create;
                    Ok(Box::new(list_groups_request::Builder::new(data)) as Box<dyn RequestBuilder>)
                });

                let resp_results = Arc::clone(&results);
                let resp_node = node.clone();
                let resp_add = maybe_add.clone();
                let handle_list_response =
                    Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
                        let ConcreteResponse::ListGroups(list_response) = response else {
                            return HandleResult::Retry(Error::local_illegal_state("Expected a ListGroups response"));
                        };
                        let error = Errors::for_code(list_response.data().error_code);
                        if error == Errors::CoordinatorLoadInProgress || error == Errors::CoordinatorNotAvailable {
                            // Retriable at the broker level: retry this per-broker call.
                            return HandleResult::Retry(Error::new(error));
                        }
                        let mut results = resp_results.lock().unwrap();
                        if error != Errors::None {
                            results.add_error(&Error::new(error), &resp_node);
                        } else {
                            for group in &list_response.data().groups {
                                if let Some((group_id, listing)) = resp_add(group) {
                                    results.add_listing(group_id, listing);
                                }
                            }
                        }
                        results.complete_node(node_id);
                        HandleResult::Done
                    });

                let fail_results = Arc::clone(&results);
                let fail_node = node.clone();
                let handle_list_failure = Box::new(move |error: &Error| {
                    let mut results = fail_results.lock().unwrap();
                    results.add_error(error, &fail_node);
                    results.complete_node(node_id);
                });

                let list_call = Call::new(
                    "listGroups",
                    deadline,
                    NodeProvider::ConstantNodeId(node_id),
                    create_list_request,
                    handle_list_response,
                    handle_list_failure,
                    Box::new(|| false),
                );
                // `runnable.call(new Call(..) {..}, nowList)` in Java: the same
                // closing gate as a user-submitted call.
                ctx.call(list_call);
            }
            HandleResult::Done
        });

        let call = Call::new(
            "findAllBrokers",
            deadline,
            NodeProvider::LeastLoaded,
            create_request,
            handle_response,
            handle_failure,
            Box::new(|| false),
        );
        self.submit(call);
        public
    }

    /// Submits one `incrementalAlterConfigs` [`Call`] for the given `resources`
    /// (all routed to `node_provider`) and returns the per-resource futures.
    ///
    /// Translated from the private
    /// `KafkaAdminClient.incrementalAlterConfigs(configs, options, resources, nodeProvider)`.
    fn submit_incremental_alter_configs(
        &self,
        configs: &HashMap<ConfigResource, Vec<AlterConfigOp>>,
        options: &AlterConfigsOptions,
        resources: &[ConfigResource],
        node_provider: NodeProvider,
    ) -> HashMap<ConfigResource, KafkaFuture<()>> {
        let mut handles: HashMap<ConfigResource, KafkaFutureImpl<()>> = HashMap::new();
        for resource in resources {
            handles.insert(resource.clone(), KafkaFutureImpl::new());
        }
        let public: HashMap<ConfigResource, KafkaFuture<()>> =
            handles.iter().map(|(k, v)| (k.clone(), v.future())).collect();

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let validate_only = options.should_validate_only();

        // Build the request data from the admin `ConfigResource`/`AlterConfigOp`
        // types (Java's `IncrementalAlterConfigsRequest.Builder` does this).
        let mut request_data = IncrementalAlterConfigsRequestData::new();
        request_data.set_validate_only(validate_only);
        let mut wire_resources = Vec::with_capacity(resources.len());
        for resource in resources {
            let mut alterable_configs = Vec::new();
            if let Some(ops) = configs.get(resource) {
                for op in ops {
                    let mut c = AlterableConfig::new();
                    c.set_name(op.config_entry().name().to_string());
                    c.set_value(op.config_entry().value().map(str::to_string));
                    c.set_config_operation(op.op_type().id());
                    alterable_configs.push(c);
                }
            }
            let mut wire_resource = AlterConfigsResource::new();
            wire_resource.set_resource_type(resource.resource_type().id());
            wire_resource.set_resource_name(resource.name().to_string());
            wire_resource.set_configs(alterable_configs);
            wire_resources.push(wire_resource);
        }
        request_data.set_resources(wire_resources);

        let create_request = Box::new(move |_timeout_ms: i32| {
            Ok(
                Box::new(incremental_alter_configs_request::Builder::with_data(request_data.clone()))
                    as Box<dyn RequestBuilder>,
            )
        });

        let handles = Arc::new(handles);
        let resp_mm = self.shared.metadata_manager.clone();
        let resp_handles = Arc::clone(&handles);
        let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
            let ConcreteResponse::IncrementalAlterConfigs(alter_response) = response else {
                return HandleResult::Retry(Error::local_illegal_state("Expected an IncrementalAlterConfigs response"));
            };
            if let Some(err) = handle_not_controller_error(&resp_mm, &alter_response.error_counts()) {
                return HandleResult::Retry(err);
            }
            let errors = alter_response.errors_by_resource();
            for (resource, future) in resp_handles.iter() {
                match errors.get(resource) {
                    Some((code, message)) if *code != Errors::None.code() => {
                        future.complete_with_error(api_error(*code, message));
                    },
                    _ => {
                        future.complete(());
                    },
                }
            }
            HandleResult::Done
        });

        let fail_handles = Arc::clone(&handles);
        let handle_failure = Box::new(move |error: &Error| {
            for future in fail_handles.values() {
                future.complete_with_error(error.clone());
            }
        });

        let call = Call::new(
            "incrementalAlterConfigs",
            deadline,
            node_provider,
            create_request,
            handle_response,
            handle_failure,
            Box::new(|| false),
        );
        self.submit(call);
        public
    }
}

/// Shared handle used to submit `AdminApiDriver`-generated [`Call`]s onto the
/// background task, mirroring the `runnable.call(...)` closure in
/// `KafkaAdminClient.newCall` / `maybeSendRequests`.
#[derive(Clone)]
struct DriverContext {
    tx: mpsc::UnboundedSender<Call>,
    wakeup: Arc<Notify>,
    /// The hard-shutdown deadline [`runnable_call`] gates on, shared with
    /// `KafkaAdminClient::submit` so a driver follow-up is rejected exactly when
    /// a user call would be.
    shutdown: Arc<ShutdownSignal>,
    using_bootstrap_controllers: bool,
    /// The `retries` config: a driver follow-up carries its spec's `tries`, so
    /// [`runnable_call`] fails it once they exceed this, as Java's `enqueue` does.
    max_retries: i32,
    time: Arc<dyn Time>,
    log_context: LogContext,
}

impl DriverContext {
    /// `runnable.call(call, now)`: queues a call issued by a driver or by a
    /// response hook through the same path as `KafkaAdminClient::submit`.
    fn call(&self, call: Call) {
        runnable_call(
            &self.tx,
            &self.wakeup,
            &self.shutdown,
            self.using_bootstrap_controllers,
            self.max_retries,
            &self.log_context,
            call,
        );
    }
}

/// Initiates a new call on the admin I/O task, or fails it at once when the
/// task cannot accept it. The submission path for user calls
/// (`KafkaAdminClient::submit`), `AdminApiDriver` follow-ups
/// ([`maybe_send_requests`]) and response-hook follow-ups (`listGroups`'
/// per-broker calls), as `AdminClientRunnable.call` is in Java. Steps 1 and 2
/// are [`ShutdownSignal::admit_new_call`], which the I/O task also applies to the follow-ups a
/// response hook returns as `HandleResult::NewCall` (quota retries), so every
/// new call passes the same gate.
///
/// Translated from `AdminClientRunnable.call` (`KafkaAdminClient.java:1598-1609`)
/// and the hand-off half of `enqueue` (`:1563-1588`):
///
/// 1. Once `close()` has published the hard-shutdown deadline, reject the call
///    with `IllegalStateException("Cannot accept new calls when AdminClient is
///    closing.")` (`:1599-1601`). This applies to driver follow-ups
///    too: Java's `maybeSendRequests` goes through `runnable.call`
///    (`:5110`), so a failure hook that runs during the I/O task's final
///    `fail_all_remaining` and asks for the next fulfillment request gets this
///    error instead of queueing a call nobody will drain.
/// 2. Reject a call whose endpoint a `bootstrap.controllers` client cannot
///    serve (`:1602-1605`).
/// 3. Fail a call whose `tries` exceed `retries` with
///    `TimeoutException("Exceeded maxRetries after <tries> tries.")`
///    (`enqueue`, `:1563-1568`; [`enqueue_within_max_retries`]). Only driver
///    follow-ups reach this with `tries > 0`: each carries its spec's `tries`.
/// 4. Otherwise hand it to the I/O task and wake the task's poll
///    (`client.wakeup()`, `:1582`). If the task has stopped accepting calls —
///    its receiver is closed (the `finally`'s `closing = true`) or gone — fail it
///    with
///    `TimeoutException("The AdminClient thread has exited.")` (`:1585-1586`).
fn runnable_call(
    tx: &mpsc::UnboundedSender<Call>,
    wakeup: &Notify,
    shutdown: &ShutdownSignal,
    using_bootstrap_controllers: bool,
    max_retries: i32,
    log_context: &LogContext,
    call: Call,
) {
    // Steps 1 to 3, shared with the runnable's own follow-ups.
    let Some(call) = shutdown
        .admit_new_call(using_bootstrap_controllers, call)
        .and_then(|call| enqueue_within_max_retries(max_retries, log_context, call))
    else {
        return;
    };
    match tx.send(call) {
        Ok(()) => wakeup.notify_one(),
        Err(mpsc::error::SendError(mut call)) => {
            // `handleTimeoutFailure` short-circuits on `cause instanceof
            // TimeoutException` (`:969-970`), so the user sees exactly a
            // `TimeoutException` — a `RetriableException`. `illegal_state` sits
            // outside the `KafkaException` hierarchy entirely, so it answered
            // `false` to both `is_retriable_error()` and `is_kafka_error()`.
            call.handle_failure(&Error::timeout("The AdminClient thread has exited."));
        },
    }
}

/// Kicks off a driver-backed RPC: polls the driver for its initial requests and
/// submits them. Mirrors `KafkaAdminClient.invokeDriver` + the initial
/// `maybeSendRequests`.
#[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient#invokeDriver")]
fn invoke_driver<K, V>(driver: AdminApiDriver<K, V>, ctx: DriverContext, now: i64)
where
    K: Clone + Eq + std::hash::Hash + std::fmt::Display + Send + 'static,
    V: Send + 'static,
{
    let driver = Arc::new(Mutex::new(driver));
    maybe_send_requests(&driver, &ctx, now);
}

/// Polls the driver and submits one [`Call`] per produced request spec.
/// Mirrors `KafkaAdminClient.maybeSendRequests`.
#[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient#maybeSendRequests")]
fn maybe_send_requests<K, V>(driver: &Arc<Mutex<AdminApiDriver<K, V>>>, ctx: &DriverContext, _now: i64)
where
    K: Clone + Eq + std::hash::Hash + std::fmt::Display + Send + 'static,
    V: Send + 'static,
{
    // Also poison-tolerant: `handle_failure` calls this on the post-panic recovery
    // path, where the driver mutex may already be poisoned (see there).
    let specs = driver.lock().unwrap_or_else(std::sync::PoisonError::into_inner).poll();
    for spec in specs {
        // `runnable.call(newCall(driver, spec), currentTimeMs)`
        // (`KafkaAdminClient.java:5110`): the closing gate applies here too.
        ctx.call(new_driver_call(Arc::clone(driver), spec, ctx.clone()));
    }
}

/// Wraps a driver [`RequestSpec`] in a [`Call`] whose hooks feed responses and
/// failures back into the driver. Mirrors `KafkaAdminClient.newCall`.
fn new_driver_call<K, V>(driver: Arc<Mutex<AdminApiDriver<K, V>>>, spec: RequestSpec<K>, ctx: DriverContext) -> Call
where
    K: Clone + Eq + std::hash::Hash + std::fmt::Display + Send + 'static,
    V: Send + 'static,
{
    let RequestSpec { name, scope, keys, request, next_allowed_try_ms, deadline_ms, tries } = spec;
    let node_provider = match scope.destination_broker_id() {
        Some(node_id) => NodeProvider::ConstantNodeId(node_id),
        None => NodeProvider::LeastLoaded,
    };
    // create_request: hand over the pre-built builder on first send; rebuild
    // from the driver on the rare non-disconnect retriable re-send.
    let mut prebuilt: Option<Box<dyn RequestBuilder>> = Some(request);
    let cr_driver = Arc::clone(&driver);
    let cr_scope = scope.clone();
    let cr_keys = keys.clone();
    let create_request = Box::new(move |_timeout_ms: i32| match prebuilt.take() {
        Some(rb) => Ok(rb),
        None => cr_driver
            .lock()
            .unwrap()
            .build_request_for_spec(&cr_scope, &cr_keys)
            .ok_or_else(|| Error::local_illegal_state("AdminApiDriver produced no request on retry")),
    });

    let hr_driver = Arc::clone(&driver);
    let hr_ctx = ctx.clone();
    let hr_scope = scope.clone();
    let hr_keys = keys.clone();
    let handle_response = Box::new(move |response: &ConcreteResponse, now: i64, cur_node: Option<&Node>| {
        // Java passes `this.curNode()` — the fully resolved broker the request
        // was sent to. Handlers put it in public API (e.g.
        // `ConsumerGroupDescription.coordinator()` /
        // `ClassicGroupDescription.coordinator()`), so it must carry the real
        // host and port, not just the broker id.
        let Some(node) = cur_node else {
            // Unreachable: `maybe_drain_pending_call` assigns `cur_node` before
            // the request is sent and only clears it on unassign / failure, so a
            // response always arrives with its node still attached.
            return HandleResult::Retry(Error::local_illegal_state(
                "AdminApiDriver response arrived with no node assigned to the call",
            ));
        };
        hr_driver.lock().unwrap().on_response(now, &hr_scope, &hr_keys, response, node);
        maybe_send_requests(&hr_driver, &hr_ctx, now);
        HandleResult::Done
    });

    let hf_driver = Arc::clone(&driver);
    let hf_ctx = ctx.clone();
    let hf_scope = scope.clone();
    let hf_keys = keys.clone();
    let hf_time = Arc::clone(&ctx.time);
    let handle_failure = Box::new(move |error: &Error| {
        let now = hf_time.milliseconds();
        // Poison-tolerant on purpose. This closure is the recovery path: it runs
        // from `AdminClientRunnable::fail_all_remaining` after `run()` catches a
        // panic, and that panic may have poisoned this very mutex inside
        // `handle_response` above. A `.unwrap()` here would panic a second time,
        // so the cleanup would not finish and every outstanding `KafkaFuture`
        // would hang — the opposite of what Java's `finally` guarantees.
        //
        // The trade-off being accepted: the driver's state is NOT per-call.
        // `AdminApiDriver` owns the persistent `lookup_map` / `fulfillment_map` /
        // `request_states` machinery for the whole RPC, and the panic poisoned
        // this mutex precisely because it interrupted a mutation of those maps,
        // so the guard taken here may expose half-updated ones. Running
        // `on_failure` against possibly-inconsistent state is accepted because
        // the alternative is a second panic that strands every future.
        //
        // This reasoning does NOT extend to the normal path: `create_request` and
        // `handle_response` above, and `maybe_retry`, keep `.unwrap()` and must.
        // They are not recovery code, so continuing there would drive the
        // *normal* protocol against half-mutated maps instead of surfacing the
        // fault.
        hf_driver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .on_failure(now, &hf_scope, &hf_keys, error);
        maybe_send_requests(&hf_driver, &hf_ctx, now);
    });

    let hnu_name = name.clone();
    let mut call = Call::new(
        name,
        deadline_ms,
        node_provider,
        create_request,
        handle_response,
        handle_failure,
        Box::new(|| false),
    );
    call.next_allowed_try_ms = next_allowed_try_ms;
    call.tries = tries;

    // handleNodeUnavailable override (KAFKA-20673): when no node can be assigned
    // to this fulfillment call because its target broker has left the cluster
    // metadata (a stale partition-leader-cache entry), send the keys back to the
    // lookup stage so the leader is re-resolved, rather than sitting in
    // pendingCalls until the deadline expires. The liveness check runs on the
    // admin client (background) task where `AdminMetadataManager` is safe to
    // access. Mirrors the `handleNodeUnavailable` override in `newCall`.
    let hnu_driver = Arc::clone(&driver);
    let hnu_ctx = ctx.clone();
    let hnu_scope = scope.clone();
    let hnu_keys = keys.clone();
    let hnu_log = ctx.log_context.clone();
    call.set_handle_node_unavailable_fn(Box::new(move |mm: &AdminMetadataManager, now: i64| {
        if let Some(broker_id) = hnu_scope.destination_broker_id()
            && mm.is_ready().unwrap_or(false)
            && mm.node_by_id(broker_id).is_none()
            && hnu_driver.lock().unwrap().maybe_retry_lookup(now, &hnu_scope, &hnu_keys)
        {
            kafka_debug!(
                hnu_log,
                "Broker {} for {} is no longer in the cluster metadata; retrying lookup.",
                broker_id,
                hnu_name
            );
            maybe_send_requests(&hnu_driver, &hnu_ctx, now);
            return true;
        }
        false
    }));

    // maybeRetry override: a disconnect retries lookup via the driver rather
    // than re-sending to the (possibly dead) node. Mirrors `newCall.maybeRetry`.
    let mr_driver = Arc::clone(&driver);
    let mr_ctx = ctx.clone();
    let mr_scope = scope;
    let mr_keys = keys;
    call.set_maybe_retry_fn(Box::new(move |error: &Error, now: i64| {
        // `throwable instanceof DisconnectException`
        // (`KafkaAdminClient.java:5127`), raised as `Error::Disconnect` by the
        // runnable — not `Errors::NetworkError`, which is a broker-reported code.
        if matches!(error, Error::Disconnect(_)) {
            mr_driver.lock().unwrap().on_failure(now, &mr_scope, &mr_keys, error);
            maybe_send_requests(&mr_driver, &mr_ctx, now);
            MaybeRetryOutcome::Handled
        } else {
            MaybeRetryOutcome::Requeue
        }
    }));

    call
}

/// Computes the absolute deadline for a call. Mirrors
/// `KafkaAdminClient.calcDeadlineMs` (`KafkaAdminClient.java:496-500`): a
/// negative option timeout is clamped to zero (`now + Math.max(0, optionTimeoutMs)`),
/// so the call is still sent once instead of expiring before it is assigned a node.
/// The default API timeout is not clamped, as in Java.
#[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient#calcDeadlineMs")]
fn calc_deadline_ms(now: i64, option_timeout: Option<i32>, default_api_timeout_ms: i32) -> i64 {
    match option_timeout {
        Some(option_timeout_ms) => now + i64::from(option_timeout_ms.max(0)),
        None => now + i64::from(default_api_timeout_ms),
    }
}

/// Re-keys a `CoordinatorKey`-keyed future map by the coordinator key's id
/// value (the group id), mirroring Java's
/// `future.all().entrySet().stream().collect(toMap(e -> e.getKey().idValue, ...))`.
fn coordinator_keyed_by_id<V: Send + 'static>(
    map: HashMap<CoordinatorKey, KafkaFuture<V>>,
) -> HashMap<String, KafkaFuture<V>> {
    map.into_iter().map(|(key, future)| (key.id_value, future)).collect()
}

/// Accumulates the per-broker results of a `listGroups` broker-enumeration RPC,
/// completing the combined future once every broker has reported. Mirrors
/// `KafkaAdminClient.ListGroupsResults`.
#[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient$ListGroupsResults")]
struct ListGroupsResults {
    errors: Vec<Error>,
    listings: HashMap<String, GroupListing>,
    remaining: HashSet<i32>,
    future: KafkaFutureImpl<Vec<Result<GroupListing, Error>>>,
}

impl ListGroupsResults {
    /// Creates the accumulator for the given broker node ids, completing the
    /// future immediately if there are no brokers.
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient$ListGroupsResults#ListGroupsResults")]
    fn new(node_ids: HashSet<i32>, future: KafkaFutureImpl<Vec<Result<GroupListing, Error>>>) -> Arc<Mutex<Self>> {
        let results = Arc::new(Mutex::new(Self {
            errors: Vec::new(),
            listings: HashMap::new(),
            remaining: node_ids,
            future,
        }));
        results.lock().unwrap().try_complete();
        results
    }

    /// Records an error for a broker, wrapping it with the broker context
    /// (mirrors Java's `ApiError.fromThrowable` + "Error listing groups on N").
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient$ListGroupsResults#addError")]
    fn add_error(&mut self, error: &Error, node: &Node) {
        let message = error.message();
        let wrapped = if message.is_empty() {
            Error::with_message(error.error(), format!("Error listing groups on {node}"))
        } else {
            Error::with_message(error.error(), format!("Error listing groups on {node}: {message}"))
        };
        self.errors.push(wrapped);
    }

    /// Records a listing keyed by group id.
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient$ListGroupsResults#addListing")]
    fn add_listing(&mut self, group_id: String, listing: GroupListing) {
        self.listings.insert(group_id, listing);
    }

    /// Marks a broker done and completes the future if it was the last one.
    fn complete_node(&mut self, node_id: i32) {
        self.remaining.remove(&node_id);
        self.try_complete();
    }

    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient$ListGroupsResults#tryComplete")]
    fn try_complete(&mut self) {
        if self.remaining.is_empty() {
            let mut results: Vec<Result<GroupListing, Error>> = self.listings.values().cloned().map(Ok).collect();
            results.extend(self.errors.iter().cloned().map(Err));
            self.future.complete(results);
        }
    }
}

/// Builds an [`Error`] from a wire error code and optional message, mirroring
/// Java's `ApiError.exception()`.
fn api_error(code: i16, message: &Option<String>) -> Error {
    let error = Errors::for_code(code);
    // `Errors.exception(String)` (`Errors.java:462-469`) falls back to the code's
    // default text ONLY when the broker sent `null`; a non-null EMPTY message is
    // passed through verbatim. The generated decoders keep the two apart
    // (`len == 0` -> `None`, `len == 1` -> `Some("")`), so treating `Some("")` as
    // absent substituted the code's default text where Java reports `""`
    // (finding 245). Same shape as `message_with_fallback` below.
    match message {
        Some(m) => Error::with_message(error, m.clone()),
        None => Error::new(error),
    }
}

/// Builds a [`FeatureMetadata`] from an `ApiVersionsResponse`'s data, mirroring
/// the `createFeatureMetadata` closure inside `KafkaAdminClient.describeFeatures`.
///
/// # Errors
///
/// Returns an error if a finalized/supported version range from the response is
/// invalid (mirrors Java's constructor throwing `IllegalArgumentException`).
fn create_feature_metadata(data: &ApiVersionsResponseData) -> Result<FeatureMetadata, Error> {
    let mut finalized_features = HashMap::new();
    for key in &data.finalized_features {
        finalized_features.insert(
            key.name.clone(),
            FinalizedVersionRange::new(key.min_version_level, key.max_version_level)?,
        );
    }

    // A finalized-features epoch of >= 0 is present; otherwise it is absent.
    let finalized_features_epoch = if data.finalized_features_epoch >= 0 {
        Some(data.finalized_features_epoch)
    } else {
        None
    };

    let mut supported_features = HashMap::new();
    for key in &data.supported_features {
        supported_features.insert(key.name.clone(), SupportedVersionRange::new(key.min_version, key.max_version)?);
    }

    Ok(FeatureMetadata::new(
        finalized_features,
        finalized_features_epoch,
        supported_features,
    ))
}

/// Returns `true` if a topic name cannot be represented in an RPC (empty).
/// Mirrors `KafkaAdminClient.topicNameIsUnrepresentable`.
#[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient#topicNameIsUnrepresentable")]
fn topic_name_is_unrepresentable(topic_name: &str) -> bool {
    topic_name.is_empty()
}

/// Returns `true` if a topic id cannot be represented in an RPC (the zero id).
/// Mirrors `KafkaAdminClient.topicIdIsUnrepresentable`.
#[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient#topicIdIsUnrepresentable")]
fn topic_id_is_unrepresentable(topic_id: Uuid) -> bool {
    topic_id == Uuid::ZERO_UUID
}

/// Completes any future that was retried due to a quota-exceeded error with the
/// carried [`ThrottlingQuotaExceeded`](Error::ThrottlingQuotaExceeded)
/// error (reduced by the elapsed throttle time) when the request ultimately
/// timed out. Mirrors `KafkaAdminClient.maybeCompleteQuotaExceededException`.
fn maybe_complete_quota_exceeded<K, T>(
    should_retry_on_quota_violation: bool,
    error: &Error,
    futures: &HashMap<K, KafkaFutureImpl<T>>,
    quota_exceeded_errors: &HashMap<K, Error>,
    throttle_time_delta: i32,
) where
    K: std::hash::Hash + Eq,
    T: Clone + Send + Sync + 'static,
{
    if should_retry_on_quota_violation && matches!(error, Error::Timeout(_)) {
        for (key, quota_error) in quota_exceeded_errors {
            if let Some(future) = futures.get(key) {
                let throttle = quota_error.throttle_time_ms().unwrap_or(0);
                future.complete_with_error(Error::throttling_quota_exceeded(
                    (throttle - throttle_time_delta).max(0),
                    quota_error.message().to_string(),
                ));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ACLs (createAcls / describeAcls / deleteAcls)
// ---------------------------------------------------------------------------

/// Builds a `createAcls` [`Call`]. Translated from the anonymous `Call` in
/// `KafkaAdminClient.createAcls`.
fn get_create_acls_call(
    mm: AdminMetadataManager,
    futures: Arc<HashMap<AclBinding, KafkaFutureImpl<()>>>,
    acl_creations: Vec<AclCreation>,
    acl_bindings_sent: Vec<AclBinding>,
    deadline: i64,
) -> Call {
    let create_request = Box::new(move |_timeout_ms: i32| {
        let mut data = CreateAclsRequestData::new();
        data.set_creations(acl_creations.clone());
        Ok(Box::new(create_acls_request::Builder::new(data)) as Box<dyn RequestBuilder>)
    });

    let resp_mm = mm.clone();
    let resp_futures = Arc::clone(&futures);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::CreateAcls(create_response) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected a CreateAcls response"));
        };
        if let Some(err) = handle_not_controller_error(&resp_mm, &create_response.error_counts()) {
            return HandleResult::Retry(err);
        }
        let mut iter = create_response.results().iter();
        for binding in &acl_bindings_sent {
            let Some(future) = resp_futures.get(binding) else {
                continue;
            };
            match iter.next() {
                None => {
                    future.complete_with_error(Error::with_message(
                        Errors::UnknownServerError,
                        format!("The broker reported no creation result for the given ACL: {binding}"),
                    ));
                },
                Some(creation) => {
                    if Errors::for_code(creation.error_code) != Errors::None {
                        future.complete_with_error(api_error(creation.error_code, &creation.error_message));
                    } else {
                        future.complete(());
                    }
                },
            }
        }
        HandleResult::Done
    });

    let fail_futures = Arc::clone(&futures);
    let handle_failure = Box::new(move |error: &Error| {
        for future in fail_futures.values() {
            future.complete_with_error(error.clone());
        }
    });

    Call::new(
        "createAcls",
        deadline,
        NodeProvider::LeastLoadedBrokerOrActiveKController,
        create_request,
        handle_response,
        handle_failure,
        Box::new(|| false),
    )
}

/// Builds a `describeAcls` [`Call`]. Translated from the anonymous `Call` in
/// `KafkaAdminClient.describeAcls`.
fn get_describe_acls_call(filter: AclBindingFilter, handle: KafkaFutureImpl<Vec<AclBinding>>, deadline: i64) -> Call {
    let create_request = Box::new(move |_timeout_ms: i32| {
        Ok(Box::new(describe_acls_request::Builder::new(&filter)) as Box<dyn RequestBuilder>)
    });

    let resp_handle = handle.clone();
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::DescribeAcls(describe_response) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected a DescribeAcls response"));
        };
        if Errors::for_code(describe_response.error_code()) != Errors::None {
            resp_handle.complete_with_error(api_error(
                describe_response.error_code(),
                &describe_response.error_message().map(str::to_string),
            ));
        } else {
            match DescribeAclsResponse::acl_bindings(describe_response.acls()) {
                Ok(bindings) => {
                    resp_handle.complete(bindings);
                },
                Err(e) => {
                    resp_handle.complete_with_error(e);
                },
            }
        }
        HandleResult::Done
    });

    let fail_handle = handle.clone();
    let handle_failure = Box::new(move |error: &Error| {
        fail_handle.complete_with_error(error.clone());
    });

    Call::new(
        "describeAcls",
        deadline,
        NodeProvider::LeastLoadedBrokerOrActiveKController,
        create_request,
        handle_response,
        handle_failure,
        Box::new(|| false),
    )
}

/// Builds a `describeClientQuotas` [`Call`]. Translated from the anonymous
/// `Call` in `KafkaAdminClient.describeClientQuotas`.
fn get_describe_client_quotas_call(
    filter: ClientQuotaFilter,
    handle: KafkaFutureImpl<HashMap<ClientQuotaEntity, HashMap<String, f64>>>,
    deadline: i64,
) -> Call {
    let create_request = Box::new(move |_timeout_ms: i32| {
        Ok(Box::new(describe_client_quotas_request::Builder::new(&filter)) as Box<dyn RequestBuilder>)
    });

    let resp_handle = handle.clone();
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::DescribeClientQuotas(describe_response) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected a DescribeClientQuotas response"));
        };
        // Mirrors DescribeClientQuotasResponse.complete: error first, else the
        // decoded entity map.
        if Errors::for_code(describe_response.error_code()) != Errors::None {
            resp_handle.complete_with_error(api_error(
                describe_response.error_code(),
                &describe_response.error_message().map(str::to_string),
            ));
        } else {
            resp_handle.complete(describe_response.entities());
        }
        HandleResult::Done
    });

    let fail_handle = handle.clone();
    let handle_failure = Box::new(move |error: &Error| {
        fail_handle.complete_with_error(error.clone());
    });

    Call::new(
        "describeClientQuotas",
        deadline,
        NodeProvider::LeastLoaded,
        create_request,
        handle_response,
        handle_failure,
        Box::new(|| false),
    )
}

/// Builds an `alterClientQuotas` [`Call`]. Translated from the anonymous `Call`
/// in `KafkaAdminClient.alterClientQuotas`.
fn get_alter_client_quotas_call(
    entries: Vec<ClientQuotaAlteration>,
    validate_only: bool,
    futures: Arc<HashMap<ClientQuotaEntity, KafkaFutureImpl<()>>>,
    deadline: i64,
) -> Call {
    let request_entries = entries;
    let create_request = Box::new(move |_timeout_ms: i32| {
        Ok(
            Box::new(alter_client_quotas_request::Builder::new(&request_entries, validate_only))
                as Box<dyn RequestBuilder>,
        )
    });

    let resp_futures = Arc::clone(&futures);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::AlterClientQuotas(alter_response) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected an AlterClientQuotas response"));
        };
        // Mirrors AlterClientQuotasResponse.complete: complete each entity's
        // future by its result.
        for (entity, outcome) in alter_response.results() {
            let Some(future) = resp_futures.get(&entity) else {
                // Java throws IllegalArgumentException if the future map lacks
                // the entity; the broker only echoes requested entities, so an
                // unknown entity is skipped rather than aborting the bg task.
                continue;
            };
            match outcome {
                Ok(()) => {
                    future.complete(());
                },
                Err(e) => {
                    future.complete_with_error(e);
                },
            }
        }
        HandleResult::Done
    });

    let fail_futures = Arc::clone(&futures);
    let handle_failure = Box::new(move |error: &Error| {
        for future in fail_futures.values() {
            future.complete_with_error(error.clone());
        }
    });

    Call::new(
        "alterClientQuotas",
        deadline,
        NodeProvider::LeastLoaded,
        create_request,
        handle_response,
        handle_failure,
        Box::new(|| false),
    )
}

/// Builds a `describeUserScramCredentials` [`Call`]. Translated from the
/// anonymous `Call` in `KafkaAdminClient.describeUserScramCredentials`.
fn get_describe_user_scram_credentials_call(
    users: Vec<String>,
    handle: KafkaFutureImpl<DescribeUserScramCredentialsResponseData>,
    deadline: i64,
) -> Call {
    let create_request = Box::new(move |_timeout_ms: i32| {
        let mut data = DescribeUserScramCredentialsRequestData::new();
        // Mirrors Java: only set users when the list is non-empty, skipping any
        // null entries; an empty/absent list describes all users.
        if !users.is_empty() {
            let user_names: Vec<UserName> = users
                .iter()
                .map(|user| {
                    let mut name = UserName::new();
                    name.set_name(user.clone());
                    name
                })
                .collect();
            if !user_names.is_empty() {
                data.set_users(Some(user_names));
            }
        }
        Ok(Box::new(describe_user_scram_credentials_request::Builder::new(data)) as Box<dyn RequestBuilder>)
    });

    let resp_handle = handle.clone();
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::DescribeUserScramCredentials(describe_response) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected a DescribeUserScramCredentials response"));
        };
        // Mirrors handleResponse: a message-level error fails the whole future,
        // otherwise the raw data is handed to the *Result view helpers.
        let data = describe_response.data();
        let message_level_error_code = data.error_code;
        if message_level_error_code != Errors::None.code() {
            resp_handle.complete_with_error(api_error(message_level_error_code, &data.error_message));
        } else {
            resp_handle.complete(data.clone());
        }
        HandleResult::Done
    });

    let fail_handle = handle.clone();
    let handle_failure = Box::new(move |error: &Error| {
        fail_handle.complete_with_error(error.clone());
    });

    Call::new(
        "describeUserScramCredentials",
        deadline,
        NodeProvider::LeastLoaded,
        create_request,
        handle_response,
        handle_failure,
        Box::new(|| false),
    )
}

/// Builds an `alterUserScramCredentials` [`Call`]. Translated from the anonymous
/// `Call` in `KafkaAdminClient.alterUserScramCredentials`.
fn get_alter_user_scram_credentials_call(
    deletions: Vec<ScramCredentialDeletion>,
    upsertions: Vec<ScramCredentialUpsertion>,
    illegal: Arc<HashMap<String, Error>>,
    futures: Arc<HashMap<String, KafkaFutureImpl<()>>>,
    metadata_manager: AdminMetadataManager,
    deadline: i64,
) -> Call {
    let create_request = Box::new(move |_timeout_ms: i32| {
        let mut data = AlterUserScramCredentialsRequestData::new();
        data.set_upsertions(upsertions.clone()).set_deletions(deletions.clone());
        Ok(Box::new(alter_user_scram_credentials_request::Builder::new(data)) as Box<dyn RequestBuilder>)
    });

    let resp_mm = metadata_manager;
    let resp_illegal = Arc::clone(&illegal);
    let resp_futures = Arc::clone(&futures);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::AlterUserScramCredentials(alter_response) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected an AlterUserScramCredentials response"));
        };
        // Check for controller change first, so that all errors are consistent
        // in that case (mirrors the NOT_CONTROLLER handling before completion).
        if let Some(err) = handle_not_controller_error(&resp_mm, &alter_response.error_counts()) {
            return HandleResult::Retry(err);
        }
        // Now that we have the results for the ones we sent, fail any users that
        // have an illegal alteration as identified above.
        for (user, error) in resp_illegal.iter() {
            if let Some(future) = resp_futures.get(user) {
                future.complete_with_error(error.clone());
            }
        }
        for result in &alter_response.data().results {
            match resp_futures.get(&result.user) {
                None => {
                    log::warn!("Server response mentioned unknown user {}", result.user);
                },
                Some(future) => {
                    let error = Errors::for_code(result.error_code);
                    if error != Errors::None {
                        future.complete_with_error(api_error(result.error_code, &result.error_message));
                    } else {
                        future.complete(());
                    }
                },
            }
        }
        // Sanity check: the broker should send back a result for every user
        // (mirrors completeUnrealizedFutures).
        for (user, future) in resp_futures.iter() {
            if !future.is_done() {
                // // Java's `completeUnrealizedFutures` throws `new ApiException(..)`
                // (`KafkaAdminClient.java:1748`) — the concrete base, not the
                // `UnknownServerException` subclass it uses elsewhere for
                // response sanity checks (`:2631`, `:2684`, `:4020`). Finding 246.
                future.complete_with_error(Error::Api(ApiError::new(format!(
                    "The broker response did not contain a result for user {user}"
                ))));
            }
        }
        HandleResult::Done
    });

    let fail_futures = Arc::clone(&futures);
    let handle_failure = Box::new(move |error: &Error| {
        for future in fail_futures.values() {
            future.complete_with_error(error.clone());
        }
    });

    Call::new(
        "alterUserScramCredentials",
        deadline,
        NodeProvider::Controller,
        create_request,
        handle_response,
        handle_failure,
        Box::new(|| false),
    )
}

/// Builds the wire upsertion for a user, computing the salted password via
/// PBKDF2. Mirrors `KafkaAdminClient.getScramCredentialUpsertion` /
/// `getSaltedPassword`.
///
/// # Errors
///
/// Returns an [`Errors::UnsupportedSaslMechanism`] error if the public mechanism
/// has no internal SCRAM mapping (the Rust analog of Java's
/// `NoSuchAlgorithmException`).
#[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient#getScramCredentialUpsertion")]
fn get_scram_credential_upsertion(upsertion: &UserScramCredentialUpsertion) -> Result<ScramCredentialUpsertion, Error> {
    let public_mechanism = upsertion.credential_info().mechanism();
    let internal = InternalScramMechanism::for_mechanism_name(public_mechanism.mechanism_name())
        .ok_or_else(|| unsupported_sasl_mechanism("Unknown SCRAM mechanism"))?;
    let salted_password = ScramFormatter::new(internal).hi(
        upsertion.password(),
        upsertion.salt(),
        upsertion.credential_info().iterations(),
    );
    let mut wire = ScramCredentialUpsertion::new();
    wire.set_name(upsertion.user().to_string())
        .set_mechanism(public_mechanism.r#type())
        .set_iterations(upsertion.credential_info().iterations())
        .set_salt(upsertion.salt().to_vec())
        .set_salted_password(salted_password);
    Ok(wire)
}

/// Builds the wire deletion for a user. Mirrors
/// `KafkaAdminClient.getScramCredentialDeletion`.
#[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient#getScramCredentialDeletion")]
fn get_scram_credential_deletion(deletion: &UserScramCredentialDeletion) -> ScramCredentialDeletion {
    let mut wire = ScramCredentialDeletion::new();
    wire.set_name(deletion.user().to_string())
        .set_mechanism(deletion.mechanism().r#type());
    wire
}

/// Mirrors `new UnacceptableCredentialException(message)`.
fn unacceptable_credential(message: &str) -> Error {
    Error::with_message(Errors::UnacceptableCredential, message)
}

/// Mirrors `new UnsupportedSaslMechanismException(message)`.
fn unsupported_sasl_mechanism(message: &str) -> Error {
    Error::with_message(Errors::UnsupportedSaslMechanism, message)
}

/// Builds a `createDelegationToken` [`Call`]. Translated from the anonymous
/// `Call` in `KafkaAdminClient.createDelegationToken`.
fn get_create_delegation_token_call(
    options: CreateDelegationTokenOptions,
    handle: KafkaFutureImpl<DelegationToken>,
    deadline: i64,
) -> Call {
    // The renewer principals are needed both to build the request and to
    // reconstruct the returned TokenInformation (Java uses `options.renewers()`
    // in handleResponse), so capture them once.
    let renewer_principals = options.renewers().to_vec();
    let owner = options.owner().cloned();
    let max_lifetime_ms = options.max_lifetime_ms();

    let create_request = Box::new(move |_timeout_ms: i32| {
        let mut data = CreateDelegationTokenRequestData::new();
        data.max_lifetime_ms = max_lifetime_ms;
        data.renewers = renewer_principals
            .iter()
            .map(|principal| {
                let mut renewer = CreatableRenewers::new();
                renewer.principal_name = principal.name().to_string();
                renewer.principal_type = principal.principal_type().to_string();
                renewer
            })
            .collect();
        if let Some(owner) = &owner {
            data.owner_principal_name = Some(owner.name().to_string());
            data.owner_principal_type = Some(owner.principal_type().to_string());
        }
        Ok(Box::new(create_delegation_token_request::Builder::new(data)) as Box<dyn RequestBuilder>)
    });

    let resp_handle = handle.clone();
    let resp_renewers = options.renewers().to_vec();
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::CreateDelegationToken(create_response) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected a CreateDelegationToken response"));
        };
        // Mirrors CreateDelegationToken handleResponse: error first, else build
        // the TokenInformation / DelegationToken from the response data using
        // the requested renewers.
        if create_response.has_error() {
            resp_handle.complete_with_error(Error::new(create_response.error()));
        } else {
            let data = create_response.data();
            let token_info = TokenInformation::with_token_requester(
                data.token_id.clone(),
                KafkaPrincipal::new(data.principal_type.clone(), data.principal_name.clone()),
                KafkaPrincipal::new(
                    data.token_requester_principal_type.clone(),
                    data.token_requester_principal_name.clone(),
                ),
                resp_renewers.clone(),
                data.issue_timestamp_ms,
                data.max_timestamp_ms,
                data.expiry_timestamp_ms,
            );
            let token = DelegationToken::new(token_info, data.hmac.clone());
            resp_handle.complete(token);
        }
        HandleResult::Done
    });

    let fail_handle = handle.clone();
    let handle_failure = Box::new(move |error: &Error| {
        fail_handle.complete_with_error(error.clone());
    });

    Call::new(
        "createDelegationToken",
        deadline,
        NodeProvider::LeastLoaded,
        create_request,
        handle_response,
        handle_failure,
        Box::new(|| false),
    )
}

/// Builds a `renewDelegationToken` [`Call`]. Translated from the anonymous
/// `Call` in `KafkaAdminClient.renewDelegationToken`.
fn get_renew_delegation_token_call(
    hmac: Vec<u8>,
    renew_time_period_ms: i64,
    handle: KafkaFutureImpl<i64>,
    deadline: i64,
) -> Call {
    let create_request = Box::new(move |_timeout_ms: i32| {
        let mut data = RenewDelegationTokenRequestData::new();
        data.hmac = hmac.clone();
        data.renew_period_ms = renew_time_period_ms;
        Ok(Box::new(renew_delegation_token_request::Builder::new(data)) as Box<dyn RequestBuilder>)
    });

    let resp_handle = handle.clone();
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::RenewDelegationToken(renew_response) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected a RenewDelegationToken response"));
        };
        if renew_response.has_error() {
            resp_handle.complete_with_error(Error::new(renew_response.error()));
        } else {
            resp_handle.complete(renew_response.expiry_timestamp());
        }
        HandleResult::Done
    });

    let fail_handle = handle.clone();
    let handle_failure = Box::new(move |error: &Error| {
        fail_handle.complete_with_error(error.clone());
    });

    Call::new(
        "renewDelegationToken",
        deadline,
        NodeProvider::LeastLoaded,
        create_request,
        handle_response,
        handle_failure,
        Box::new(|| false),
    )
}

/// Builds an `expireDelegationToken` [`Call`]. Translated from the anonymous
/// `Call` in `KafkaAdminClient.expireDelegationToken`.
fn get_expire_delegation_token_call(
    hmac: Vec<u8>,
    expiry_time_period_ms: i64,
    handle: KafkaFutureImpl<i64>,
    deadline: i64,
) -> Call {
    let create_request = Box::new(move |_timeout_ms: i32| {
        let mut data = ExpireDelegationTokenRequestData::new();
        data.hmac = hmac.clone();
        data.expiry_time_period_ms = expiry_time_period_ms;
        Ok(Box::new(expire_delegation_token_request::Builder::new(data)) as Box<dyn RequestBuilder>)
    });

    let resp_handle = handle.clone();
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::ExpireDelegationToken(expire_response) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected an ExpireDelegationToken response"));
        };
        if expire_response.has_error() {
            resp_handle.complete_with_error(Error::new(expire_response.error()));
        } else {
            resp_handle.complete(expire_response.expiry_timestamp());
        }
        HandleResult::Done
    });

    let fail_handle = handle.clone();
    let handle_failure = Box::new(move |error: &Error| {
        fail_handle.complete_with_error(error.clone());
    });

    Call::new(
        "expireDelegationToken",
        deadline,
        NodeProvider::LeastLoaded,
        create_request,
        handle_response,
        handle_failure,
        Box::new(|| false),
    )
}

/// Builds a `describeDelegationToken` [`Call`]. Translated from the anonymous
/// `Call` in `KafkaAdminClient.describeDelegationToken`.
fn get_describe_delegation_token_call(
    owners: Option<Vec<KafkaPrincipal>>,
    handle: KafkaFutureImpl<Vec<DelegationToken>>,
    deadline: i64,
) -> Call {
    let create_request = Box::new(move |_timeout_ms: i32| {
        Ok(Box::new(describe_delegation_token_request::Builder::new(owners.as_deref())) as Box<dyn RequestBuilder>)
    });

    let resp_handle = handle.clone();
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::DescribeDelegationToken(describe_response) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected a DescribeDelegationToken response"));
        };
        if describe_response.has_error() {
            resp_handle.complete_with_error(Error::new(describe_response.error()));
        } else {
            resp_handle.complete(describe_response.tokens());
        }
        HandleResult::Done
    });

    let fail_handle = handle.clone();
    let handle_failure = Box::new(move |error: &Error| {
        fail_handle.complete_with_error(error.clone());
    });

    Call::new(
        "describeDelegationToken",
        deadline,
        NodeProvider::LeastLoaded,
        create_request,
        handle_response,
        handle_failure,
        Box::new(|| false),
    )
}

/// Builds a `deleteAcls` [`Call`]. Translated from the anonymous `Call` in
/// `KafkaAdminClient.deleteAcls`.
fn get_delete_acls_call(
    mm: AdminMetadataManager,
    futures: Arc<HashMap<AclBindingFilter, KafkaFutureImpl<FilterResults>>>,
    acl_binding_filters_sent: Vec<AclBindingFilter>,
    delete_acls_filters: Vec<DeleteAclsFilter>,
    deadline: i64,
) -> Call {
    let create_request = Box::new(move |_timeout_ms: i32| {
        let mut data = DeleteAclsRequestData::new();
        data.set_filters(delete_acls_filters.clone());
        Ok(Box::new(delete_acls_request::Builder::new(data)) as Box<dyn RequestBuilder>)
    });

    let resp_mm = mm.clone();
    let resp_futures = Arc::clone(&futures);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::DeleteAcls(delete_response) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected a DeleteAcls response"));
        };
        if let Some(err) = handle_not_controller_error(&resp_mm, &delete_response.error_counts()) {
            return HandleResult::Retry(err);
        }
        let mut iter = delete_response.filter_results().iter();
        for binding_filter in &acl_binding_filters_sent {
            let Some(future) = resp_futures.get(binding_filter) else {
                continue;
            };
            match iter.next() {
                None => {
                    future.complete_with_error(Error::with_message(
                        Errors::UnknownServerError,
                        "The broker reported no deletion result for the given filter.",
                    ));
                },
                Some(filter_result) => {
                    if Errors::for_code(filter_result.error_code) != Errors::None {
                        future.complete_with_error(api_error(filter_result.error_code, &filter_result.error_message));
                    } else {
                        let mut results = Vec::new();
                        for matching_acl in &filter_result.matching_acls {
                            let binding = DeleteAclsResponse::acl_binding(matching_acl).ok();
                            let error = if Errors::for_code(matching_acl.error_code) != Errors::None {
                                Some(api_error(matching_acl.error_code, &matching_acl.error_message))
                            } else {
                                None
                            };
                            results.push(FilterResult::new(binding, error));
                        }
                        future.complete(FilterResults::new(results));
                    }
                },
            }
        }
        HandleResult::Done
    });

    let fail_futures = Arc::clone(&futures);
    let handle_failure = Box::new(move |error: &Error| {
        for future in fail_futures.values() {
            future.complete_with_error(error.clone());
        }
    });

    Call::new(
        "deleteAcls",
        deadline,
        NodeProvider::LeastLoadedBrokerOrActiveKController,
        create_request,
        handle_response,
        handle_failure,
        Box::new(|| false),
    )
}

/// Checks a create/delete response for a controller-change error, mirroring
/// `KafkaAdminClient.handleNotControllerError`. Returns the error to retry with
/// if the controller changed.
#[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient#handleNotControllerError")]
fn handle_not_controller_error(mm: &AdminMetadataManager, error_counts: &HashMap<Errors, i32>) -> Option<Error> {
    // Java dispatches on which code was present and rethrows `error.exception()`
    // for *that* code (`KafkaAdminClient.java:4145-4153`), so a
    // `NOT_LEADER_OR_FOLLOWER` seen by a `bootstrap.controllers` client surfaces
    // as `NotLeaderOrFollower`, not `NotController` (finding 247).
    let matched = if error_counts.contains_key(&Errors::NotController) {
        Some(Errors::NotController)
    } else if mm.using_bootstrap_controllers() && error_counts.contains_key(&Errors::NotLeaderOrFollower) {
        Some(Errors::NotLeaderOrFollower)
    } else {
        None
    };
    if let Some(error) = matched {
        mm.clear_controller();
        mm.request_update();
        Some(Error::new(error))
    } else {
        None
    }
}

/// Maps an [`OffsetSpec`] to the wire-protocol timestamp sentinel used by
/// `ListOffsets`.
///
/// Mirrors `KafkaAdminClient.getOffsetFromSpec`.
#[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient#getOffsetFromSpec")]
fn get_offset_from_spec(offset_spec: OffsetSpec) -> i64 {
    use crate::common::requests::ListOffsetsRequest;
    match offset_spec {
        OffsetSpec::Timestamp(timestamp) => timestamp,
        OffsetSpec::Earliest => ListOffsetsRequest::EARLIEST_TIMESTAMP,
        OffsetSpec::MaxTimestamp => ListOffsetsRequest::MAX_TIMESTAMP,
        OffsetSpec::EarliestLocal => ListOffsetsRequest::EARLIEST_LOCAL_TIMESTAMP,
        OffsetSpec::LatestTiered => ListOffsetsRequest::LATEST_TIERED_TIMESTAMP,
        OffsetSpec::EarliestPendingUpload => ListOffsetsRequest::EARLIEST_PENDING_UPLOAD_TIMESTAMP,
        OffsetSpec::Latest => ListOffsetsRequest::LATEST_TIMESTAMP,
    }
}

/// Builds the `alterPartitionReassignments` controller call.
///
/// Mirrors the anonymous `Call` in `KafkaAdminClient.alterPartitionReassignments`.
fn get_alter_partition_reassignments_call(
    mm: AdminMetadataManager,
    futures: Arc<HashMap<TopicPartition, KafkaFutureImpl<()>>>,
    topics_to_reassignments: Arc<
        std::collections::BTreeMap<String, std::collections::BTreeMap<i32, Option<NewPartitionReassignment>>>,
    >,
    allow_replication_factor_change: bool,
    expected_responses_count: usize,
    deadline: i64,
) -> Call {
    let req_topics = Arc::clone(&topics_to_reassignments);
    let create_request = Box::new(move |timeout_ms: i32| {
        let mut data = AlterPartitionReassignmentsRequestData::new();
        let mut topics = Vec::new();
        for (topic_name, partitions_to_reassignments) in req_topics.iter() {
            let mut reassignable_partitions = Vec::new();
            for (partition_index, reassignment) in partitions_to_reassignments {
                let mut reassignable_partition = ReassignablePartition::new();
                reassignable_partition.set_partition_index(*partition_index);
                reassignable_partition.set_replicas(reassignment.as_ref().map(|r| r.target_replicas().to_vec()));
                reassignable_partitions.push(reassignable_partition);
            }
            let mut reassignable_topic = ReassignableTopic::new();
            reassignable_topic.set_name(topic_name.clone());
            reassignable_topic.set_partitions(reassignable_partitions);
            topics.push(reassignable_topic);
        }
        data.set_topics(topics);
        data.set_timeout_ms(timeout_ms);
        data.set_allow_replication_factor_change(allow_replication_factor_change);
        Ok(Box::new(alter_partition_reassignments_request::Builder::new(data)) as Box<dyn RequestBuilder>)
    });

    let resp_mm = mm.clone();
    let resp_futures = Arc::clone(&futures);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::AlterPartitionReassignments(alter_response) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected an AlterPartitionReassignments response"));
        };
        let data = alter_response.data();
        let mut errors: HashMap<TopicPartition, Option<Error>> = HashMap::new();
        let mut received_responses_count: usize = 0;
        let top_level_error = Errors::for_code(data.error_code);
        match top_level_error {
            Errors::None => {
                for topic_response in &data.responses {
                    for part_response in &topic_response.partitions {
                        let tp = TopicPartition::new(topic_response.name.as_str(), part_response.partition_index);
                        let partition_error = Errors::for_code(part_response.error_code);
                        if partition_error == Errors::None {
                            errors.insert(tp, None);
                        } else {
                            let message = part_response.error_message.as_deref();
                            errors.insert(tp, Some(partition_error.error_with_optional_message(message)));
                        }
                        received_responses_count += 1;
                    }
                }
            },
            Errors::NotController => {
                if let Some(err) = handle_not_controller_error(&resp_mm, &alter_response.error_counts()) {
                    return HandleResult::Retry(err);
                }
            },
            _ => {
                for topic_response in &data.responses {
                    for part_response in &topic_response.partitions {
                        let tp = TopicPartition::new(topic_response.name.as_str(), part_response.partition_index);
                        let message = data.error_message.as_deref();
                        errors.insert(tp, Some(top_level_error.error_with_optional_message(message)));
                        received_responses_count += 1;
                    }
                }
            },
        }

        // assertResponseCountMatch: if the server returned an inconsistent
        // number of results, fail every future with an UnknownServerException.
        if errors.values().all(Option::is_none) && received_responses_count != expected_responses_count {
            let quantifier = if received_responses_count > expected_responses_count {
                "many"
            } else {
                "less"
            };
            let error = Error::with_message(
                Errors::UnknownServerError,
                format!(
                    "The server returned too {quantifier} results.Expected {expected_responses_count} but received {received_responses_count}"
                ),
            );
            for future in resp_futures.values() {
                future.complete_with_error(error.clone());
            }
            return HandleResult::Done;
        }

        for (tp, error) in errors {
            let Some(future) = resp_futures.get(&tp) else {
                continue;
            };
            match error {
                None => {
                    future.complete(());
                },
                Some(error) => {
                    future.complete_with_error(error);
                },
            }
        }
        HandleResult::Done
    });

    let fail_futures = Arc::clone(&futures);
    let handle_failure = Box::new(move |error: &Error| {
        for future in fail_futures.values() {
            future.complete_with_error(error.clone());
        }
    });

    Call::new(
        "alterPartitionReassignments",
        deadline,
        NodeProvider::Controller,
        create_request,
        handle_response,
        handle_failure,
        Box::new(|| false),
    )
}

/// Builds the `listPartitionReassignments` controller call.
///
/// Mirrors the anonymous `Call` in `KafkaAdminClient.listPartitionReassignments`.
fn get_list_partition_reassignments_call(
    mm: AdminMetadataManager,
    handle: KafkaFutureImpl<HashMap<TopicPartition, PartitionReassignment>>,
    request_partitions: Option<Vec<TopicPartition>>,
    deadline: i64,
) -> Call {
    let req_partitions = request_partitions.clone();
    let create_request = Box::new(move |timeout_ms: i32| {
        let mut list_data = ListPartitionReassignmentsRequestData::new();
        list_data.set_timeout_ms(timeout_ms);
        if let Some(partitions) = &req_partitions {
            let mut topics_by_name: HashMap<String, ListPartitionReassignmentsTopics> = HashMap::new();
            for tp in partitions {
                let topic = topics_by_name.entry(tp.topic().to_string()).or_insert_with(|| {
                    let mut t = ListPartitionReassignmentsTopics::new();
                    t.set_name(tp.topic().to_string());
                    t
                });
                topic.partition_indexes.push(tp.partition());
            }
            list_data.set_topics(Some(topics_by_name.into_values().collect()));
        }
        Ok(Box::new(list_partition_reassignments_request::Builder::new(list_data)) as Box<dyn RequestBuilder>)
    });

    let resp_mm = mm.clone();
    let resp_handle = handle.clone();
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::ListPartitionReassignments(list_response) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected a ListPartitionReassignments response"));
        };
        let data = list_response.data();
        let error = Errors::for_code(data.error_code);
        match error {
            Errors::None => {},
            Errors::NotController => {
                if let Some(err) = handle_not_controller_error(&resp_mm, &list_response.error_counts()) {
                    return HandleResult::Retry(err);
                }
            },
            _ => {
                resp_handle.complete_with_error(error.error_with_optional_message(data.error_message.as_deref()));
            },
        }
        let mut reassignment_map: HashMap<TopicPartition, PartitionReassignment> = HashMap::new();
        for topic_reassignment in &data.topics {
            for partition_reassignment in &topic_reassignment.partitions {
                reassignment_map.insert(
                    TopicPartition::new(topic_reassignment.name.as_str(), partition_reassignment.partition_index),
                    PartitionReassignment::new(
                        partition_reassignment.replicas.clone(),
                        partition_reassignment.adding_replicas.clone(),
                        partition_reassignment.removing_replicas.clone(),
                    ),
                );
            }
        }
        // First-writer-wins: a no-op if the future was already failed above.
        resp_handle.complete(reassignment_map);
        HandleResult::Done
    });

    let fail_handle = handle.clone();
    let handle_failure = Box::new(move |error: &Error| {
        fail_handle.complete_with_error(error.clone());
    });

    Call::new(
        "listPartitionReassignments",
        deadline,
        NodeProvider::Controller,
        create_request,
        handle_response,
        handle_failure,
        Box::new(|| false),
    )
}

/// Returns the broker id pertaining to the given resource, or `None` if the
/// resource is not associated with a particular broker.
///
/// Mirrors `KafkaAdminClient.nodeFor`.
#[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient#nodeFor")]
fn node_for(resource: &ConfigResource) -> Option<i32> {
    if (resource.resource_type() == config_resource::Type::Broker && !resource.is_default())
        || resource.resource_type() == config_resource::Type::BrokerLogger
    {
        // Java parses `Integer.valueOf(resource.name())`; a non-numeric name
        // would throw. Here a parse failure degrades to "any broker" rather
        // than panicking on a recoverable path (CLAUDE.md §12).
        resource.name().parse::<i32>().ok()
    } else {
        None
    }
}

/// Converts a `DescribeConfigsResult` wire result into a [`Config`], mirroring
/// `KafkaAdminClient.describeConfigResult`.
#[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient#describeConfigResult")]
fn describe_config_result(result: &crate::describe_configs_response_data::DescribeConfigsResult) -> Config {
    Config::new(result.configs.iter().map(|config| {
        let synonyms = config
            .synonyms
            .iter()
            .map(|synonym| {
                ConfigSynonym::new(
                    synonym.name.clone(),
                    synonym.value.clone(),
                    ConfigSource::for_id(synonym.source),
                )
            })
            .collect();
        ConfigEntry::with_options(
            ConfigEntryOptionsBuilder::new()
                .set_name(config.name.clone())
                .set_value(config.value.clone())
                .set_source(ConfigSource::for_id(config.config_source))
                .set_is_sensitive(config.is_sensitive)
                .set_is_read_only(config.read_only)
                .set_synonyms(synonyms)
                .set_config_type(ConfigType::for_id(config.config_type))
                .set_documentation(config.documentation.clone())
                .build()
                .expect("ConfigEntryOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }))
}

/// Builds a `describeConfigs` [`Call`] for a set of resources routed to a single
/// node (`Some(broker)`) or the least-loaded broker (`None`). Translated from
/// the `Call` created inside `KafkaAdminClient.describeConfigs`.
fn get_describe_configs_call(
    node: Option<i32>,
    unified: Arc<HashMap<ConfigResource, KafkaFutureImpl<Config>>>,
    include_synonyms: bool,
    include_documentation: bool,
    deadline: i64,
) -> Call {
    let req_unified = Arc::clone(&unified);
    let create_request = Box::new(move |_timeout_ms: i32| {
        let resources = req_unified
            .keys()
            .map(|resource| {
                let mut r = DescribeConfigsResource::new();
                r.set_resource_name(resource.name().to_string());
                r.set_resource_type(resource.resource_type().id());
                r.set_configuration_keys(None);
                r
            })
            .collect();
        let mut data = DescribeConfigsRequestData::new();
        data.set_resources(resources);
        data.set_include_synonyms(include_synonyms);
        data.set_include_documentation(include_documentation);
        Ok(Box::new(describe_configs_request::Builder::new(data)) as Box<dyn RequestBuilder>)
    });

    let resp_unified = Arc::clone(&unified);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::DescribeConfigs(describe_response) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected a DescribeConfigs response"));
        };
        for (config_resource, result) in describe_response.result_map() {
            let Some(future) = resp_unified.get(&config_resource) else {
                // A config in the response that was not in the request; Java
                // logs a warning and ignores it.
                continue;
            };
            if result.error_code != Errors::None.code() {
                future.complete_with_error(api_error(result.error_code, &result.error_message));
            } else {
                future.complete(describe_config_result(result));
            }
        }
        // Complete any future for which the node did not return a result.
        for (resource, future) in resp_unified.iter() {
            if !future.is_done() {
                // // Java's `completeUnrealizedFutures` throws `new ApiException(..)`
                // (`KafkaAdminClient.java:1748`) — the concrete base, not the
                // `UnknownServerException` subclass it uses elsewhere for
                // response sanity checks (`:2631`, `:2684`, `:4020`). Finding 246.
                future.complete_with_error(Error::Api(ApiError::new(format!(
                    "The node response did not contain a result for config resource {resource}"
                ))));
            }
        }
        HandleResult::Done
    });

    let fail_unified = Arc::clone(&unified);
    let handle_failure = Box::new(move |error: &Error| {
        for future in fail_unified.values() {
            future.complete_with_error(error.clone());
        }
    });

    let node_provider = match node {
        Some(node_id) => NodeProvider::ConstantNodeId(node_id),
        None => NodeProvider::LeastLoadedBrokerOrActiveKController,
    };

    Call::new(
        "describeConfigs",
        deadline,
        node_provider,
        create_request,
        handle_response,
        handle_failure,
        Box::new(|| false),
    )
}

/// Completes any future not yet realized, mirroring
/// `KafkaAdminClient.completeUnrealizedFutures`.
fn complete_unrealized<T: Clone + Send + Sync + 'static>(
    futures: &HashMap<String, KafkaFutureImpl<T>>,
    message: impl Fn(&str) -> String,
) {
    for (name, future) in futures {
        if !future.is_done() {
            // Java throws `new ApiException(messageFormatter.apply(key))`
            // (`KafkaAdminClient.java:1748`) — the concrete base class, NOT the
            // `UnknownServerException` subclass Java uses for the *other* response
            // sanity checks (`:2631`, `:2684`, `:4020`). Finding 246.
            future.complete_with_error(Error::Api(ApiError::new(message(name))));
        }
    }
}

// ---------------------------------------------------------------------------
// describeLogDirs / alterReplicaLogDirs / describeReplicaLogDirs
// ---------------------------------------------------------------------------

/// Maps a protocol error code to an optional error, mirroring Java's
/// `Errors.forCode(code).exception()` which returns `null` for `NONE`.
fn api_error_for_code(error_code: i16) -> Option<Error> {
    let error = Errors::for_code(error_code);
    (error != Errors::None).then(|| Error::new(error))
}

/// Builds a map from log-directory path to [`LogDirDescription`] from a
/// `DescribeLogDirs` response. Mirrors `KafkaAdminClient.logDirDescriptions`.
#[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient#logDirDescriptions")]
fn log_dir_descriptions(response: &DescribeLogDirsResponse) -> HashMap<String, LogDirDescription> {
    let mut result = HashMap::with_capacity(response.data().results.len());
    for log_dir_result in &response.data().results {
        let mut replica_info_map = HashMap::new();
        for t in &log_dir_result.topics {
            for p in &t.partitions {
                replica_info_map.insert(
                    TopicPartition::new(t.name.clone(), p.partition_index),
                    ReplicaInfo::new(p.partition_size, p.offset_lag, p.is_future_key),
                );
            }
        }
        result.insert(
            log_dir_result.log_dir.clone(),
            LogDirDescription::with_total_bytes_usable_bytes_is_cordoned(
                api_error_for_code(log_dir_result.error_code),
                replica_info_map,
                log_dir_result.total_bytes,
                log_dir_result.usable_bytes,
                log_dir_result.is_cordoned,
            ),
        );
    }
    result
}

/// Builds a per-broker `describeLogDirs` [`Call`]. Mirrors the anonymous `Call`
/// in `KafkaAdminClient.describeLogDirs`.
fn get_describe_log_dirs_call(
    broker_id: i32,
    handle: KafkaFutureImpl<HashMap<String, LogDirDescription>>,
    deadline: i64,
) -> Call {
    let create_request = Box::new(move |_timeout_ms: i32| {
        // Query selected partitions in all log directories (topics == null).
        let mut data = DescribeLogDirsRequestData::new();
        data.set_topics(None);
        Ok(Box::new(describe_log_dirs_request::Builder::new(data)) as Box<dyn RequestBuilder>)
    });

    let resp_handle = handle.clone();
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::DescribeLogDirs(resp) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected a DescribeLogDirs response"));
        };
        let descriptions = log_dir_descriptions(resp);
        if !descriptions.is_empty() {
            resp_handle.complete(descriptions);
        } else {
            // Up to v3 DescribeLogDirsResponse did not have an error code field,
            // hence it defaults to NONE.
            let error = if resp.data().error_code == Errors::None.code() {
                Errors::ClusterAuthorizationFailed
            } else {
                Errors::for_code(resp.data().error_code)
            };
            resp_handle.complete_with_error(Error::new(error));
        }
        HandleResult::Done
    });

    let fail_handle = handle;
    let handle_failure = Box::new(move |error: &Error| {
        fail_handle.complete_with_error(error.clone());
    });

    Call::new(
        "describeLogDirs",
        deadline,
        NodeProvider::ConstantNodeId(broker_id),
        create_request,
        handle_response,
        handle_failure,
        Box::new(|| false),
    )
}

/// Builds a per-broker `alterReplicaLogDirs` [`Call`]. Mirrors the anonymous
/// `Call` in `KafkaAdminClient.alterReplicaLogDirs`. `futures` is shared across
/// all per-broker calls; each call only completes the replicas targeting its
/// own broker.
fn get_alter_replica_log_dirs_call(
    broker_id: i32,
    assignment: AlterReplicaLogDirsRequestData,
    futures: Arc<HashMap<TopicPartitionReplica, KafkaFutureImpl<()>>>,
    deadline: i64,
) -> Call {
    let create_request = Box::new(move |_timeout_ms: i32| {
        Ok(Box::new(alter_replica_log_dirs_request::Builder::new(assignment.clone())) as Box<dyn RequestBuilder>)
    });

    let resp_futures = Arc::clone(&futures);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::AlterReplicaLogDirs(resp) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected an AlterReplicaLogDirs response"));
        };
        for topic_result in &resp.data().results {
            for partition_result in &topic_result.partitions {
                let replica = TopicPartitionReplica::new(
                    topic_result.topic_name.clone(),
                    partition_result.partition_index,
                    broker_id,
                );
                match resp_futures.get(&replica) {
                    // The partition in the response was not in the request;
                    // Java logs a warning and ignores it.
                    None => {},
                    Some(future) => {
                        if partition_result.error_code == Errors::None.code() {
                            future.complete(());
                        } else {
                            future.complete_with_error(Error::new(Errors::for_code(partition_result.error_code)));
                        }
                    },
                }
            }
        }
        // The server should send back a result for every replica. Do a sanity
        // check anyway (mirrors `completeUnrealizedFutures`).
        for (replica, future) in resp_futures.iter() {
            if replica.broker_id() == broker_id && !future.is_done() {
                // // Java's `completeUnrealizedFutures` throws `new ApiException(..)`
                // (`KafkaAdminClient.java:1748`) — the concrete base, not the
                // `UnknownServerException` subclass it uses elsewhere for
                // response sanity checks (`:2631`, `:2684`, `:4020`). Finding 246.
                future.complete_with_error(Error::Api(ApiError::new(format!(
                    "The response from broker {broker_id} did not contain a result for replica {replica}"
                ))));
            }
        }
        HandleResult::Done
    });

    let fail_futures = Arc::clone(&futures);
    let handle_failure = Box::new(move |error: &Error| {
        // Only completes the futures of brokerId.
        for (replica, future) in fail_futures.iter() {
            if replica.broker_id() == broker_id {
                future.complete_with_error(error.clone());
            }
        }
    });

    Call::new(
        "alterReplicaLogDirs",
        deadline,
        NodeProvider::ConstantNodeId(broker_id),
        create_request,
        handle_response,
        handle_failure,
        Box::new(|| false),
    )
}

/// Builds a per-broker `describeReplicaLogDirs` [`Call`]. Mirrors the anonymous
/// `Call` in `KafkaAdminClient.describeReplicaLogDirs`, which reshapes a
/// `DescribeLogDirs` response into per-replica `ReplicaLogDirInfo`s.
fn get_describe_replica_log_dirs_call(
    broker_id: i32,
    request_data: DescribeLogDirsRequestData,
    mut replica_dir_info_by_partition: HashMap<TopicPartition, ReplicaLogDirInfo>,
    futures: Arc<HashMap<TopicPartitionReplica, KafkaFutureImpl<ReplicaLogDirInfo>>>,
    deadline: i64,
) -> Call {
    let create_request = Box::new(move |_timeout_ms: i32| {
        // Query selected partitions in all log directories.
        Ok(Box::new(describe_log_dirs_request::Builder::new(request_data.clone())) as Box<dyn RequestBuilder>)
    });

    let resp_futures = Arc::clone(&futures);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::DescribeLogDirs(resp) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected a DescribeLogDirs response"));
        };
        for (log_dir, log_dir_info) in log_dir_descriptions(resp) {
            if let Some(error) = log_dir_info.error() {
                // No replica info is provided if the log directory is offline.
                if error.error() == Errors::KafkaStorageError {
                    continue;
                }
                // Any other error for a log directory is illegal (mirrors Java's
                // `handleFailure(new IllegalStateException(...))`, which fails
                // every replica future).
                let illegal = Error::local_illegal_state(format!(
                    "The error {:?} for log directory {log_dir} in the response from broker {broker_id} is illegal",
                    error.error()
                ));
                for future in resp_futures.values() {
                    future.complete_with_error(illegal.clone());
                }
            }

            for (tp, replica_info) in log_dir_info.replica_infos() {
                let Some(existing) = replica_dir_info_by_partition.get(tp) else {
                    // Server response mentioned an unknown partition; Java logs
                    // a warning.
                    continue;
                };
                let updated = if replica_info.is_future() {
                    ReplicaLogDirInfo::new(
                        existing.current_replica_log_dir().map(String::from),
                        existing.current_replica_offset_lag(),
                        Some(log_dir.clone()),
                        replica_info.offset_lag(),
                    )
                } else {
                    ReplicaLogDirInfo::new(
                        Some(log_dir.clone()),
                        replica_info.offset_lag(),
                        existing.future_replica_log_dir().map(String::from),
                        existing.future_replica_offset_lag(),
                    )
                };
                replica_dir_info_by_partition.insert(tp.clone(), updated);
            }
        }

        for (tp, info) in &replica_dir_info_by_partition {
            let replica = TopicPartitionReplica::new(tp.topic().to_string(), tp.partition(), broker_id);
            if let Some(future) = resp_futures.get(&replica) {
                future.complete(info.clone());
            }
        }
        HandleResult::Done
    });

    let fail_futures = Arc::clone(&futures);
    let handle_failure = Box::new(move |error: &Error| {
        for future in fail_futures.values() {
            future.complete_with_error(error.clone());
        }
    });

    Call::new(
        "describeReplicaLogDirs",
        deadline,
        NodeProvider::ConstantNodeId(broker_id),
        create_request,
        handle_response,
        handle_failure,
        Box::new(|| false),
    )
}

/// Builds a [`TopicDescription`] from cluster metadata, mirroring
/// `KafkaAdminClient.getTopicDescriptionFromCluster`.
#[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient#getTopicDescriptionFromCluster")]
fn topic_description_from_cluster(
    cluster: &Cluster,
    topic_name: &str,
    topic_id: Uuid,
    authorized_operations: i32,
) -> TopicDescription {
    let is_internal = cluster.internal_topics().contains(topic_name);
    let mut partitions: Vec<TopicPartitionInfo> = cluster
        .partitions_for_topic(topic_name)
        .iter()
        .map(|p| {
            let leader = match p.leader() {
                Some(node) if !node.is_empty() => Some(node.clone()),
                _ => None,
            };
            TopicPartitionInfo::new(p.partition(), leader, p.replicas().to_vec(), p.in_sync_replicas().to_vec())
        })
        .collect();
    partitions.sort_by_key(|p| p.partition());
    TopicDescription::with_authorized_operations_topic_id(
        topic_name,
        is_internal,
        partitions,
        AdminUtils::valid_acl_operations(authorized_operations),
        topic_id,
    )
}

// ---------------------------------------------------------------------------
// createTopics
// ---------------------------------------------------------------------------

/// Builds a `createTopics` [`Call`]. Free function so the quota-retry path can
/// rebuild a fresh call with the same futures. Translated from
/// `KafkaAdminClient.getCreateTopicsCall`.
#[expect(clippy::too_many_arguments)]
#[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient#getCreateTopicsCall")]
fn get_create_topics_call(
    mm: AdminMetadataManager,
    futures: Arc<HashMap<String, KafkaFutureImpl<TopicMetadataAndConfig>>>,
    topics_by_name: Arc<HashMap<String, CreatableTopic>>,
    names: Vec<String>,
    quota_exceeded_errors: HashMap<String, Error>,
    validate_only: bool,
    retry_on_quota: bool,
    now: i64,
    deadline: i64,
    time: Arc<dyn Time>,
) -> Call {
    let req_names = names.clone();
    let req_topics = Arc::clone(&topics_by_name);
    let create_request = Box::new(move |timeout_ms: i32| {
        let mut data = CreateTopicsRequestData::new();
        let topics: Vec<CreatableTopic> = req_names.iter().filter_map(|n| req_topics.get(n).cloned()).collect();
        data.set_topics(topics);
        data.set_timeout_ms(timeout_ms);
        data.set_validate_only(validate_only);
        Ok(Box::new(create_topics_request::Builder::new(data)) as Box<dyn RequestBuilder>)
    });

    let resp_mm = mm.clone();
    let resp_futures = Arc::clone(&futures);
    let resp_topics = Arc::clone(&topics_by_name);
    let resp_time = Arc::clone(&time);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::CreateTopics(create_response) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected a CreateTopics response"));
        };
        if let Some(err) = handle_not_controller_error(&resp_mm, &create_response.error_counts()) {
            return HandleResult::Retry(err);
        }
        let throttle_time_ms = create_response.throttle_time_ms();
        let mut retry_names: Vec<String> = Vec::new();
        let mut retry_quota_exceeded: HashMap<String, Error> = HashMap::new();
        for result in &create_response.data().topics {
            let Some(future) = resp_futures.get(&result.name) else {
                continue;
            };
            let error = Errors::for_code(result.error_code);
            if error != Errors::None {
                if error == Errors::ThrottlingQuotaExceeded {
                    let quota_error = Error::throttling_quota_exceeded(
                        throttle_time_ms,
                        KafkaAdminClient::message_with_fallback(result.error_code, &result.error_message),
                    );
                    if retry_on_quota {
                        retry_names.push(result.name.clone());
                        retry_quota_exceeded.insert(result.name.clone(), quota_error);
                    } else {
                        future.complete_with_error(quota_error);
                    }
                } else {
                    future.complete_with_error(api_error(result.error_code, &result.error_message));
                }
            } else if result.topic_config_error_code != Errors::None.code() {
                future.complete(TopicMetadataAndConfig::with_error(Error::new(Errors::for_code(
                    result.topic_config_error_code,
                ))));
            } else if result.num_partitions == CreateTopicsResult::UNKNOWN {
                future.complete(TopicMetadataAndConfig::with_error(Error::unsupported_version(
                    "Topic metadata and configs in CreateTopics response not supported",
                )));
            } else {
                let config = result
                    .configs
                    .as_ref()
                    .map(|configs| {
                        Config::new(configs.iter().map(|c| {
                            ConfigEntry::with_options(
                                ConfigEntryOptionsBuilder::new()
                                    .set_name(c.name.clone())
                                    .set_value(c.value.clone())
                                    .set_source(ConfigSource::for_id(c.config_source))
                                    .set_is_sensitive(c.is_sensitive)
                                    .set_is_read_only(c.read_only)
                                    .build()
                                    .expect("ConfigEntryOptionsBuilder::build: every mandatory parameter is set above"),
                            )
                        }))
                    })
                    .unwrap_or_else(|| Config::new(std::iter::empty()));
                future.complete(TopicMetadataAndConfig::new(
                    result.topic_id,
                    result.num_partitions,
                    result.replication_factor as i32,
                    config,
                ));
            }
        }
        if retry_names.is_empty() {
            complete_unrealized(&resp_futures, |topic| {
                format!("The controller response did not contain a result for topic {topic}")
            });
            HandleResult::Done
        } else {
            let retry_now = resp_time.milliseconds();
            let call = get_create_topics_call(
                resp_mm.clone(),
                Arc::clone(&resp_futures),
                Arc::clone(&resp_topics),
                retry_names,
                retry_quota_exceeded,
                validate_only,
                retry_on_quota,
                retry_now,
                deadline,
                Arc::clone(&resp_time),
            );
            HandleResult::NewCall(Box::new(call))
        }
    });

    let fail_futures = Arc::clone(&futures);
    let fail_time = Arc::clone(&time);
    let handle_failure = Box::new(move |error: &Error| {
        // If there were any topics retried due to a quota exceeded exception,
        // propagate the initial error back to the caller if the request timed
        // out (mirrors maybeCompleteQuotaExceededException).
        let throttle_time_delta = (fail_time.milliseconds() - now).clamp(0, i32::MAX as i64) as i32;
        maybe_complete_quota_exceeded(
            retry_on_quota,
            error,
            &fail_futures,
            &quota_exceeded_errors,
            throttle_time_delta,
        );
        for future in fail_futures.values() {
            future.complete_with_error(error.clone());
        }
    });

    Call::new(
        "createTopics",
        deadline,
        NodeProvider::Controller,
        create_request,
        handle_response,
        handle_failure,
        Box::new(|| false),
    )
}

// ---------------------------------------------------------------------------
// createPartitions
// ---------------------------------------------------------------------------

/// Builds a `createPartitions` [`Call`]. Free function so the quota-retry path
/// can rebuild a fresh call with the same futures. Translated from
/// `KafkaAdminClient.getCreatePartitionsCall`.
#[expect(clippy::too_many_arguments)]
#[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient#getCreatePartitionsCall")]
fn get_create_partitions_call(
    mm: AdminMetadataManager,
    futures: Arc<HashMap<String, KafkaFutureImpl<()>>>,
    topics_by_name: Arc<HashMap<String, CreatePartitionsTopic>>,
    names: Vec<String>,
    quota_exceeded_errors: HashMap<String, Error>,
    validate_only: bool,
    retry_on_quota: bool,
    now: i64,
    deadline: i64,
    time: Arc<dyn Time>,
) -> Call {
    let req_names = names.clone();
    let req_topics = Arc::clone(&topics_by_name);
    let create_request = Box::new(move |timeout_ms: i32| {
        let mut data = CreatePartitionsRequestData::new();
        let topics: Vec<CreatePartitionsTopic> = req_names.iter().filter_map(|n| req_topics.get(n).cloned()).collect();
        data.set_topics(topics);
        data.set_timeout_ms(timeout_ms);
        data.set_validate_only(validate_only);
        Ok(Box::new(create_partitions_request::Builder::new(data)) as Box<dyn RequestBuilder>)
    });

    let resp_mm = mm.clone();
    let resp_futures = Arc::clone(&futures);
    let resp_topics = Arc::clone(&topics_by_name);
    let resp_time = Arc::clone(&time);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::CreatePartitions(create_response) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected a CreatePartitions response"));
        };
        if let Some(err) = handle_not_controller_error(&resp_mm, &create_response.error_counts()) {
            return HandleResult::Retry(err);
        }
        let throttle_time_ms = create_response.throttle_time_ms();
        let mut retry_names: Vec<String> = Vec::new();
        let mut retry_quota_exceeded: HashMap<String, Error> = HashMap::new();
        for result in &create_response.data().results {
            let Some(future) = resp_futures.get(&result.name) else {
                continue;
            };
            let error = Errors::for_code(result.error_code);
            if error != Errors::None {
                if error == Errors::ThrottlingQuotaExceeded {
                    let quota_error = Error::throttling_quota_exceeded(
                        throttle_time_ms,
                        KafkaAdminClient::message_with_fallback(result.error_code, &result.error_message),
                    );
                    if retry_on_quota {
                        retry_names.push(result.name.clone());
                        retry_quota_exceeded.insert(result.name.clone(), quota_error);
                    } else {
                        future.complete_with_error(quota_error);
                    }
                } else {
                    future.complete_with_error(api_error(result.error_code, &result.error_message));
                }
            } else {
                future.complete(());
            }
        }
        if retry_names.is_empty() {
            complete_unrealized(&resp_futures, |topic| {
                format!("The controller response did not contain a result for topic {topic}")
            });
            HandleResult::Done
        } else {
            let retry_now = resp_time.milliseconds();
            let call = get_create_partitions_call(
                resp_mm.clone(),
                Arc::clone(&resp_futures),
                Arc::clone(&resp_topics),
                retry_names,
                retry_quota_exceeded,
                validate_only,
                retry_on_quota,
                retry_now,
                deadline,
                Arc::clone(&resp_time),
            );
            HandleResult::NewCall(Box::new(call))
        }
    });

    let fail_futures = Arc::clone(&futures);
    let fail_time = Arc::clone(&time);
    let handle_failure = Box::new(move |error: &Error| {
        let throttle_time_delta = (fail_time.milliseconds() - now).clamp(0, i32::MAX as i64) as i32;
        maybe_complete_quota_exceeded(
            retry_on_quota,
            error,
            &fail_futures,
            &quota_exceeded_errors,
            throttle_time_delta,
        );
        for future in fail_futures.values() {
            future.complete_with_error(error.clone());
        }
    });

    Call::new(
        "createPartitions",
        deadline,
        NodeProvider::Controller,
        create_request,
        handle_response,
        handle_failure,
        Box::new(|| false),
    )
}

// ---------------------------------------------------------------------------
// deleteTopics
// ---------------------------------------------------------------------------

/// Builds a `deleteTopics` (by name) [`Call`]. Translated from
/// `KafkaAdminClient.getDeleteTopicsCall`.
#[expect(clippy::too_many_arguments)]
#[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient#getDeleteTopicsCall")]
fn get_delete_topics_call(
    mm: AdminMetadataManager,
    futures: Arc<HashMap<String, KafkaFutureImpl<()>>>,
    names: Vec<String>,
    quota_exceeded_errors: HashMap<String, Error>,
    retry_on_quota: bool,
    now: i64,
    deadline: i64,
    time: Arc<dyn Time>,
) -> Call {
    let req_names = names.clone();
    let create_request = Box::new(move |timeout_ms: i32| {
        let mut data = DeleteTopicsRequestData::new();
        data.set_topic_names(req_names.clone());
        data.set_timeout_ms(timeout_ms);
        Ok(Box::new(delete_topics_request::Builder::new(data)) as Box<dyn RequestBuilder>)
    });

    let resp_mm = mm.clone();
    let resp_futures = Arc::clone(&futures);
    let resp_time = Arc::clone(&time);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::DeleteTopics(delete_response) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected a DeleteTopics response"));
        };
        if let Some(err) = handle_not_controller_error(&resp_mm, &delete_response.error_counts()) {
            return HandleResult::Retry(err);
        }
        let throttle_time_ms = delete_response.throttle_time_ms();
        let mut retry_names: Vec<String> = Vec::new();
        let mut retry_quota_exceeded: HashMap<String, Error> = HashMap::new();
        for result in &delete_response.data().responses {
            let Some(name) = result.name.as_ref() else {
                continue;
            };
            let Some(future) = resp_futures.get(name) else {
                continue;
            };
            let error = Errors::for_code(result.error_code);
            if error != Errors::None {
                if error == Errors::ThrottlingQuotaExceeded {
                    let quota_error = Error::throttling_quota_exceeded(
                        throttle_time_ms,
                        KafkaAdminClient::message_with_fallback(result.error_code, &result.error_message),
                    );
                    if retry_on_quota {
                        retry_names.push(name.clone());
                        retry_quota_exceeded.insert(name.clone(), quota_error);
                    } else {
                        future.complete_with_error(quota_error);
                    }
                } else {
                    future.complete_with_error(api_error(result.error_code, &result.error_message));
                }
            } else {
                future.complete(());
            }
        }
        if retry_names.is_empty() {
            complete_unrealized(&resp_futures, |topic| {
                format!("The controller response did not contain a result for topic {topic}")
            });
            HandleResult::Done
        } else {
            let retry_now = resp_time.milliseconds();
            let call = get_delete_topics_call(
                resp_mm.clone(),
                Arc::clone(&resp_futures),
                retry_names,
                retry_quota_exceeded,
                retry_on_quota,
                retry_now,
                deadline,
                Arc::clone(&resp_time),
            );
            HandleResult::NewCall(Box::new(call))
        }
    });

    let fail_futures = Arc::clone(&futures);
    let fail_time = Arc::clone(&time);
    let handle_failure = Box::new(move |error: &Error| {
        let throttle_time_delta = (fail_time.milliseconds() - now).clamp(0, i32::MAX as i64) as i32;
        maybe_complete_quota_exceeded(
            retry_on_quota,
            error,
            &fail_futures,
            &quota_exceeded_errors,
            throttle_time_delta,
        );
        for future in fail_futures.values() {
            future.complete_with_error(error.clone());
        }
    });

    Call::new(
        "deleteTopics",
        deadline,
        NodeProvider::Controller,
        create_request,
        handle_response,
        handle_failure,
        Box::new(|| false),
    )
}

/// Builds a `deleteTopics` (by id) [`Call`]. Translated from
/// `KafkaAdminClient.getDeleteTopicsWithIdsCall`.
#[expect(clippy::too_many_arguments)]
#[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClient#getDeleteTopicsWithIdsCall")]
fn get_delete_topics_with_ids_call(
    mm: AdminMetadataManager,
    futures: Arc<HashMap<Uuid, KafkaFutureImpl<()>>>,
    ids: Vec<Uuid>,
    quota_exceeded_errors: HashMap<Uuid, Error>,
    retry_on_quota: bool,
    now: i64,
    deadline: i64,
    time: Arc<dyn Time>,
) -> Call {
    let req_ids = ids.clone();
    let create_request = Box::new(move |timeout_ms: i32| {
        let mut data = DeleteTopicsRequestData::new();
        let states: Vec<DeleteTopicState> = req_ids
            .iter()
            .map(|id| {
                let mut s = DeleteTopicState::new();
                s.set_topic_id(*id);
                s
            })
            .collect();
        data.set_topics(states);
        data.set_timeout_ms(timeout_ms);
        Ok(Box::new(delete_topics_request::Builder::new(data)) as Box<dyn RequestBuilder>)
    });

    let resp_mm = mm.clone();
    let resp_futures = Arc::clone(&futures);
    let resp_time = Arc::clone(&time);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::DeleteTopics(delete_response) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected a DeleteTopics response"));
        };
        if let Some(err) = handle_not_controller_error(&resp_mm, &delete_response.error_counts()) {
            return HandleResult::Retry(err);
        }
        let throttle_time_ms = delete_response.throttle_time_ms();
        let mut retry_ids: Vec<Uuid> = Vec::new();
        let mut retry_quota_exceeded: HashMap<Uuid, Error> = HashMap::new();
        for result in &delete_response.data().responses {
            let Some(future) = resp_futures.get(&result.topic_id) else {
                continue;
            };
            let error = Errors::for_code(result.error_code);
            if error != Errors::None {
                if error == Errors::ThrottlingQuotaExceeded {
                    let quota_error = Error::throttling_quota_exceeded(
                        throttle_time_ms,
                        KafkaAdminClient::message_with_fallback(result.error_code, &result.error_message),
                    );
                    if retry_on_quota {
                        retry_ids.push(result.topic_id);
                        retry_quota_exceeded.insert(result.topic_id, quota_error);
                    } else {
                        future.complete_with_error(quota_error);
                    }
                } else {
                    future.complete_with_error(api_error(result.error_code, &result.error_message));
                }
            } else {
                future.complete(());
            }
        }
        // Complete any unrealized id-keyed future.
        if retry_ids.is_empty() {
            for (id, future) in resp_futures.iter() {
                if !future.is_done() {
                    // // Java's `completeUnrealizedFutures` throws `new ApiException(..)`
                    // (`KafkaAdminClient.java:1748`) — the concrete base, not the
                    // `UnknownServerException` subclass it uses elsewhere for
                    // response sanity checks (`:2631`, `:2684`, `:4020`). Finding 246.
                    future.complete_with_error(Error::Api(ApiError::new(format!(
                        "The controller response did not contain a result for topic {id}"
                    ))));
                }
            }
            HandleResult::Done
        } else {
            let retry_now = resp_time.milliseconds();
            let call = get_delete_topics_with_ids_call(
                resp_mm.clone(),
                Arc::clone(&resp_futures),
                retry_ids,
                retry_quota_exceeded,
                retry_on_quota,
                retry_now,
                deadline,
                Arc::clone(&resp_time),
            );
            HandleResult::NewCall(Box::new(call))
        }
    });

    let fail_futures = Arc::clone(&futures);
    let fail_time = Arc::clone(&time);
    let handle_failure = Box::new(move |error: &Error| {
        let throttle_time_delta = (fail_time.milliseconds() - now).clamp(0, i32::MAX as i64) as i32;
        maybe_complete_quota_exceeded(
            retry_on_quota,
            error,
            &fail_futures,
            &quota_exceeded_errors,
            throttle_time_delta,
        );
        for future in fail_futures.values() {
            future.complete_with_error(error.clone());
        }
    });

    Call::new(
        "deleteTopics",
        deadline,
        NodeProvider::Controller,
        create_request,
        handle_response,
        handle_failure,
        Box::new(|| false),
    )
}

#[async_trait]
impl Admin for KafkaAdminClient {
    fn create_topics_with_options(&self, new_topics: &[NewTopic], options: CreateTopicsOptions) -> CreateTopicsResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);

        let mut handles: HashMap<String, KafkaFutureImpl<TopicMetadataAndConfig>> = HashMap::new();
        let mut topics_by_name: HashMap<String, CreatableTopic> = HashMap::new();
        for new_topic in new_topics {
            let name = new_topic.name().to_string();
            if topic_name_is_unrepresentable(&name) {
                let future: KafkaFutureImpl<TopicMetadataAndConfig> = KafkaFutureImpl::new();
                future.complete_with_error(Error::with_message(
                    Errors::InvalidTopicError,
                    format!("The given topic name '{name}' cannot be represented in a request."),
                ));
                handles.insert(name, future);
            } else if let std::collections::hash_map::Entry::Vacant(entry) = handles.entry(name.clone()) {
                entry.insert(KafkaFutureImpl::new());
                topics_by_name.insert(name, new_topic.convert_to_creatable_topic());
            }
        }
        let public: HashMap<String, KafkaFuture<TopicMetadataAndConfig>> =
            handles.iter().map(|(k, v)| (k.clone(), v.future())).collect();

        if !topics_by_name.is_empty() {
            let names: Vec<String> = topics_by_name.keys().cloned().collect();
            let call = get_create_topics_call(
                self.shared.metadata_manager.clone(),
                Arc::new(handles),
                Arc::new(topics_by_name),
                names,
                HashMap::new(),
                options.should_validate_only(),
                options.should_retry_on_quota_violation(),
                now,
                deadline,
                Arc::clone(&self.shared.time),
            );
            self.submit(call);
        }
        CreateTopicsResult::new(public)
    }

    fn delete_topics_with_options(&self, topics: TopicCollection, options: DeleteTopicsOptions) -> DeleteTopicsResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        match topics {
            TopicCollection::TopicNames(names) => {
                let mut handles: HashMap<String, KafkaFutureImpl<()>> = HashMap::new();
                let mut valid_topic_names: Vec<String> = Vec::new();
                for name in &names {
                    if topic_name_is_unrepresentable(name) {
                        let future: KafkaFutureImpl<()> = KafkaFutureImpl::new();
                        future.complete_with_error(Error::with_message(
                            Errors::InvalidTopicError,
                            format!("The given topic name '{name}' cannot be represented in a request."),
                        ));
                        handles.insert(name.clone(), future);
                    } else if let std::collections::hash_map::Entry::Vacant(entry) = handles.entry(name.clone()) {
                        entry.insert(KafkaFutureImpl::new());
                        valid_topic_names.push(name.clone());
                    }
                }
                let public: HashMap<String, KafkaFuture<()>> =
                    handles.iter().map(|(k, v)| (k.clone(), v.future())).collect();
                if !valid_topic_names.is_empty() {
                    let call = get_delete_topics_call(
                        self.shared.metadata_manager.clone(),
                        Arc::new(handles),
                        valid_topic_names,
                        HashMap::new(),
                        options.should_retry_on_quota_violation(),
                        now,
                        deadline,
                        Arc::clone(&self.shared.time),
                    );
                    self.submit(call);
                }
                DeleteTopicsResult::of_topic_names(public)
            },
            TopicCollection::TopicIds(ids) => {
                let mut handles: HashMap<Uuid, KafkaFutureImpl<()>> = HashMap::new();
                let mut valid_topic_ids: Vec<Uuid> = Vec::new();
                for id in &ids {
                    if topic_id_is_unrepresentable(*id) {
                        let future: KafkaFutureImpl<()> = KafkaFutureImpl::new();
                        future.complete_with_error(Error::with_message(
                            Errors::InvalidTopicError,
                            format!("The given topic ID '{id}' cannot be represented in a request."),
                        ));
                        handles.insert(*id, future);
                    } else if let std::collections::hash_map::Entry::Vacant(entry) = handles.entry(*id) {
                        entry.insert(KafkaFutureImpl::new());
                        valid_topic_ids.push(*id);
                    }
                }
                let public: HashMap<Uuid, KafkaFuture<()>> = handles.iter().map(|(k, v)| (*k, v.future())).collect();
                if !valid_topic_ids.is_empty() {
                    let call = get_delete_topics_with_ids_call(
                        self.shared.metadata_manager.clone(),
                        Arc::new(handles),
                        valid_topic_ids,
                        HashMap::new(),
                        options.should_retry_on_quota_violation(),
                        now,
                        deadline,
                        Arc::clone(&self.shared.time),
                    );
                    self.submit(call);
                }
                DeleteTopicsResult::of_topic_ids(public)
            },
        }
    }

    fn list_topics_with_options(&self, options: ListTopicsOptions) -> ListTopicsResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let handle: KafkaFutureImpl<HashMap<String, TopicListing>> = KafkaFutureImpl::new();
        let public = handle.future();
        let list_internal = options.should_list_internal();

        let create_request = Box::new(move |_timeout_ms: i32| {
            Ok(Box::new(metadata_request::Builder::all_topics()) as Box<dyn RequestBuilder>)
        });

        let resp_handle = handle.clone();
        let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
            let ConcreteResponse::Metadata(metadata_response) = response else {
                return HandleResult::Retry(Error::local_illegal_state("Expected a Metadata response"));
            };
            let mut topics: HashMap<String, TopicListing> = HashMap::new();
            for topic in metadata_response.topic_metadata() {
                if topic.error() != Errors::None {
                    continue;
                }
                if topic.is_internal() && !list_internal {
                    continue;
                }
                topics.insert(
                    topic.topic().to_string(),
                    TopicListing::new(topic.topic().to_string(), topic.topic_id(), topic.is_internal()),
                );
            }
            resp_handle.complete(topics);
            HandleResult::Done
        });

        let fail_handle = handle.clone();
        let handle_failure = Box::new(move |error: &Error| {
            fail_handle.complete_with_error(error.clone());
        });

        let call = Call::new(
            "listTopics",
            deadline,
            NodeProvider::LeastLoaded,
            create_request,
            handle_response,
            handle_failure,
            Box::new(|| false),
        );
        self.submit(call);
        ListTopicsResult::new(public)
    }

    fn describe_topics_with_topics_options(
        &self,
        topics: TopicCollection,
        options: DescribeTopicsOptions,
    ) -> DescribeTopicsResult {
        match topics {
            TopicCollection::TopicNames(names) => DescribeTopicsResult::of_topic_names(
                self.handle_describe_topics_by_names_with_describe_topic_partitions_api(&names, &options),
            ),
            TopicCollection::TopicIds(ids) => {
                // Describing by id uses the Metadata API in Java too
                // (handleDescribeTopicsByIds → convertTopicIdsToMetadataRequestTopic),
                // not DescribeTopicPartitions, so it is translated faithfully here.
                let mut handles: HashMap<Uuid, KafkaFutureImpl<TopicDescription>> = HashMap::new();
                let mut valid_topic_ids: Vec<Uuid> = Vec::new();
                for id in &ids {
                    if topic_id_is_unrepresentable(*id) {
                        let future: KafkaFutureImpl<TopicDescription> = KafkaFutureImpl::new();
                        future.complete_with_error(Error::with_message(
                            Errors::InvalidTopicError,
                            format!("The given topic id '{id}' cannot be represented in a request."),
                        ));
                        handles.insert(*id, future);
                    } else if let std::collections::hash_map::Entry::Vacant(entry) = handles.entry(*id) {
                        entry.insert(KafkaFutureImpl::new());
                        valid_topic_ids.push(*id);
                    }
                }
                let public: HashMap<Uuid, KafkaFuture<TopicDescription>> =
                    handles.iter().map(|(k, v)| (*k, v.future())).collect();
                if !valid_topic_ids.is_empty() {
                    let now = self.now();
                    let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
                    let call = get_describe_topics_by_ids_call(
                        Arc::new(handles),
                        valid_topic_ids,
                        options.include_authorized_operations(),
                        deadline,
                    );
                    self.submit(call);
                }
                DescribeTopicsResult::of_topic_ids(public)
            },
        }
    }

    fn create_partitions_with_options(
        &self,
        new_partitions: &HashMap<String, NewPartitions>,
        options: CreatePartitionsOptions,
    ) -> CreatePartitionsResult {
        let mut handles: HashMap<String, KafkaFutureImpl<()>> = HashMap::new();
        let mut topics_by_name: HashMap<String, CreatePartitionsTopic> = HashMap::new();
        for (topic, new_partition) in new_partitions {
            let assignments = new_partition.assignments().map(|new_assignments| {
                new_assignments
                    .iter()
                    .map(|broker_ids| {
                        let mut a = CreatePartitionsAssignment::new();
                        a.set_broker_ids(broker_ids.clone());
                        a
                    })
                    .collect::<Vec<_>>()
            });
            let mut created = CreatePartitionsTopic::new();
            created.set_name(topic.clone());
            created.set_count(new_partition.total_count());
            created.set_assignments(assignments);
            topics_by_name.insert(topic.clone(), created);
            handles.insert(topic.clone(), KafkaFutureImpl::new());
        }
        let public: HashMap<String, KafkaFuture<()>> = handles.iter().map(|(k, v)| (k.clone(), v.future())).collect();

        if !topics_by_name.is_empty() {
            let now = self.now();
            let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
            let names: Vec<String> = topics_by_name.keys().cloned().collect();
            let call = get_create_partitions_call(
                self.shared.metadata_manager.clone(),
                Arc::new(handles),
                Arc::new(topics_by_name),
                names,
                HashMap::new(),
                options.validate_only(),
                options.should_retry_on_quota_violation(),
                now,
                deadline,
                Arc::clone(&self.shared.time),
            );
            self.submit(call);
        }
        CreatePartitionsResult::new(public)
    }

    fn delete_records_with_options(
        &self,
        records_to_delete: &HashMap<TopicPartition, RecordsToDelete>,
        options: DeleteRecordsOptions,
    ) -> DeleteRecordsResult {
        let keys: std::collections::HashSet<TopicPartition> = records_to_delete.keys().cloned().collect();
        let future = DeleteRecordsHandler::new_future(keys, Arc::clone(&self.shared.partition_leader_cache));
        let result_map = future.all();

        let timeout_ms = options.timeout_ms().unwrap_or(self.shared.default_api_timeout_ms);
        let handler = DeleteRecordsHandler::new(
            records_to_delete.clone(),
            LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id)),
            timeout_ms,
        );

        let now = self.now();
        // Java calc: calcDeadlineMs(now, options.timeoutMs()) — the raw option
        // (which may be null → default), not the resolved handler timeout.
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let retry_backoff = ExponentialBackoff::new(
            self.shared.retry_backoff_ms,
            RETRY_BACKOFF_EXP_BASE,
            self.shared.retry_backoff_max_ms,
            RETRY_BACKOFF_JITTER,
        )
        .expect("ExponentialBackoff::new only fails on invalid jitter");
        let driver = AdminApiDriver::new(
            Box::new(handler),
            Box::new(future),
            deadline,
            retry_backoff,
            LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id)),
        );
        invoke_driver(driver, self.driver_context(), now);

        DeleteRecordsResult::new(result_map)
    }

    fn describe_producers_with_options(
        &self,
        partitions: &[TopicPartition],
        options: DescribeProducersOptions,
    ) -> DescribeProducersResult {
        let keys: std::collections::HashSet<TopicPartition> = partitions.iter().cloned().collect();
        let future = DescribeProducersHandler::new_future(keys, Arc::clone(&self.shared.partition_leader_cache));
        let result_map = future.all();

        let log_context = LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id));
        let handler = DescribeProducersHandler::new(options.clone(), log_context.clone());

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let retry_backoff = self.retry_backoff();
        let driver = AdminApiDriver::new(Box::new(handler), Box::new(future), deadline, retry_backoff, log_context);
        invoke_driver(driver, self.driver_context(), now);

        DescribeProducersResult::new(result_map)
    }

    fn abort_transaction_with_options(
        &self,
        spec: AbortTransactionSpec,
        options: AbortTransactionOptions,
    ) -> AbortTransactionResult {
        let keys = std::collections::HashSet::from([spec.topic_partition().clone()]);
        let future = AbortTransactionHandler::new_future(keys, Arc::clone(&self.shared.partition_leader_cache));
        let result_map = future.all();

        let log_context = LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id));
        let handler = AbortTransactionHandler::new(spec, log_context.clone());

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let retry_backoff = self.retry_backoff();
        let driver = AdminApiDriver::new(Box::new(handler), Box::new(future), deadline, retry_backoff, log_context);
        invoke_driver(driver, self.driver_context(), now);

        AbortTransactionResult::new(result_map)
    }

    fn describe_transactions_with_options(
        &self,
        transactional_ids: &[String],
        options: DescribeTransactionsOptions,
    ) -> DescribeTransactionsResult {
        let log_context = LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id));
        let future = DescribeTransactionsHandler::new_future(transactional_ids);
        let result_map = future.all();
        let handler = DescribeTransactionsHandler::new(log_context.clone());

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let retry_backoff = self.retry_backoff();
        let driver = AdminApiDriver::new(Box::new(handler), Box::new(future), deadline, retry_backoff, log_context);
        invoke_driver(driver, self.driver_context(), now);

        DescribeTransactionsResult::new(coordinator_keyed_by_id(result_map))
    }

    fn fence_producers_with_options(
        &self,
        transactional_ids: &[String],
        options: FenceProducersOptions,
    ) -> FenceProducersResult {
        let log_context = LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id));
        let future = FenceProducersHandler::new_future(transactional_ids);
        let result_map = future.all();
        let handler = FenceProducersHandler::new(&options, log_context.clone(), self.shared.request_timeout_ms);

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let retry_backoff = self.retry_backoff();
        let driver = AdminApiDriver::new(Box::new(handler), Box::new(future), deadline, retry_backoff, log_context);
        invoke_driver(driver, self.driver_context(), now);

        FenceProducersResult::new(coordinator_keyed_by_id(result_map))
    }

    fn list_transactions_with_options(&self, options: ListTransactionsOptions) -> ListTransactionsResult {
        let log_context = LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id));
        let future = ListTransactionsHandler::new_future();
        let result_future = future.all();

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let handler = ListTransactionsHandler::new(options, log_context.clone());
        let retry_backoff = self.retry_backoff();
        let driver = AdminApiDriver::new(Box::new(handler), Box::new(future), deadline, retry_backoff, log_context);
        invoke_driver(driver, self.driver_context(), now);

        ListTransactionsResult::new(result_future)
    }

    fn force_terminate_transaction_with_options(
        &self,
        transactional_id: &str,
        options: TerminateTransactionOptions,
    ) -> TerminateTransactionResult {
        // Simply leverage the existing fenceProducers implementation with a
        // single transactional id (mirrors Java's forceTerminateTransaction).
        let mut fence_options = FenceProducersOptions::new();
        if options.timeout_ms().is_some() {
            fence_options = fence_options.set_timeout_ms(options.timeout_ms());
        }
        let ids = vec![transactional_id.to_string()];
        let fence_result = self.fence_producers_with_options(&ids, fence_options);

        // Convert the result to a TerminateTransactionResult.
        let future = fence_result
            .fenced_producers()
            .get(transactional_id)
            .cloned()
            .expect("the transactional id was included in the fenceProducers request");
        TerminateTransactionResult::new(future)
    }

    fn describe_cluster_with_options(&self, options: DescribeClusterOptions) -> DescribeClusterResult {
        self.describe_cluster_with_nodes_handle(options).0
    }

    fn describe_configs_with_options(
        &self,
        config_resources: &[ConfigResource],
        options: DescribeConfigsOptions,
    ) -> DescribeConfigsResult {
        // Partition the requested config resources based on which broker they
        // must be sent to (null broker == obtainable from any broker).
        let mut node_futures: HashMap<Option<i32>, HashMap<ConfigResource, KafkaFutureImpl<Config>>> = HashMap::new();
        for resource in config_resources {
            let broker = node_for(resource);
            node_futures
                .entry(broker)
                .or_default()
                .insert(resource.clone(), KafkaFutureImpl::new());
        }

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let include_synonyms = options.include_synonyms();
        let include_documentation = options.include_documentation();

        let mut public: HashMap<ConfigResource, KafkaFuture<Config>> = HashMap::new();
        for (node, unified) in &node_futures {
            for (resource, handle) in unified {
                public.insert(resource.clone(), handle.future());
            }
            let call = get_describe_configs_call(
                *node,
                Arc::new(unified.clone()),
                include_synonyms,
                include_documentation,
                deadline,
            );
            self.submit(call);
        }

        DescribeConfigsResult::new(public)
    }

    fn incremental_alter_configs_with_options(
        &self,
        configs: &HashMap<ConfigResource, Vec<AlterConfigOp>>,
        options: AlterConfigsOptions,
    ) -> AlterConfigsResult {
        let mut all_futures: HashMap<ConfigResource, KafkaFuture<()>> = HashMap::new();
        // BROKER_LOGGER requests always go to a specific broker; a non-default
        // BROKER resource goes to that specific node; everything else goes to
        // the least loaded broker (bootstrap.controllers is unsupported here,
        // so the controller special-casing never triggers).
        let mut unified_request_resources: Vec<ConfigResource> = Vec::new();

        for resource in configs.keys() {
            let mut node = node_for(resource);
            if self.shared.metadata_manager.using_bootstrap_controllers()
                && resource.resource_type() != config_resource::Type::BrokerLogger
            {
                node = None;
            }
            if let Some(node_id) = node {
                let futures = self.submit_incremental_alter_configs(
                    configs,
                    &options,
                    std::slice::from_ref(resource),
                    NodeProvider::ConstantNodeId(node_id),
                );
                all_futures.extend(futures);
            } else {
                unified_request_resources.push(resource.clone());
            }
        }
        if !unified_request_resources.is_empty() {
            let futures = self.submit_incremental_alter_configs(
                configs,
                &options,
                &unified_request_resources,
                NodeProvider::LeastLoadedBrokerOrActiveKController,
            );
            all_futures.extend(futures);
        }

        AlterConfigsResult::new(all_futures)
    }

    fn list_config_resources_with_options(
        &self,
        config_resource_types: &HashSet<config_resource::Type>,
        options: ListConfigResourcesOptions,
    ) -> ListConfigResourcesResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let handle: KafkaFutureImpl<Vec<ConfigResource>> = KafkaFutureImpl::new();
        let public = handle.future();

        let resource_type_ids: Vec<i8> = config_resource_types.iter().map(config_resource::Type::id).collect();
        let create_request = Box::new(move |_timeout_ms: i32| {
            let mut data = ListConfigResourcesRequestData::new();
            data.set_resource_types(resource_type_ids.clone());
            Ok(Box::new(list_config_resources_request::Builder::new(data)) as Box<dyn RequestBuilder>)
        });

        let resp_handle = handle.clone();
        let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
            let ConcreteResponse::ListConfigResources(list_response) = response else {
                return HandleResult::Retry(Error::local_illegal_state("Expected a ListConfigResources response"));
            };
            let error = list_response.error();
            if error != Errors::None {
                resp_handle.complete_with_error(Error::new(error));
            } else {
                resp_handle.complete(list_response.config_resources());
            }
            HandleResult::Done
        });

        let fail_handle = handle.clone();
        let handle_failure = Box::new(move |error: &Error| {
            fail_handle.complete_with_error(error.clone());
        });

        let call = Call::new(
            "listConfigResources",
            deadline,
            NodeProvider::LeastLoaded,
            create_request,
            handle_response,
            handle_failure,
            Box::new(|| false),
        );
        self.submit(call);
        ListConfigResourcesResult::new(public)
    }

    fn describe_log_dirs_with_options(
        &self,
        brokers: &[i32],
        options: DescribeLogDirsOptions,
    ) -> DescribeLogDirsResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);

        let mut public: HashMap<i32, KafkaFuture<HashMap<String, LogDirDescription>>> = HashMap::new();
        for &broker_id in brokers {
            let handle: KafkaFutureImpl<HashMap<String, LogDirDescription>> = KafkaFutureImpl::new();
            public.insert(broker_id, handle.future());
            let call = get_describe_log_dirs_call(broker_id, handle, deadline);
            self.submit(call);
        }

        DescribeLogDirsResult::new(public)
    }

    fn alter_replica_log_dirs_with_options(
        &self,
        replica_assignment: &HashMap<TopicPartitionReplica, String>,
        options: AlterReplicaLogDirsOptions,
    ) -> AlterReplicaLogDirsResult {
        let mut futures: HashMap<TopicPartitionReplica, KafkaFutureImpl<()>> = HashMap::new();
        for replica in replica_assignment.keys() {
            futures.insert(replica.clone(), KafkaFutureImpl::new());
        }

        // Group the requested moves by destination broker, mirroring Java's
        // `replicaAssignmentByBroker`. Each broker's request carries one entry
        // per (log dir, topic) with the target partitions.
        let mut assignment_by_broker: HashMap<i32, AlterReplicaLogDirsRequestData> = HashMap::new();
        for (replica, log_dir) in replica_assignment {
            let data = assignment_by_broker
                .entry(replica.broker_id())
                .or_insert_with(AlterReplicaLogDirsRequestData::new);
            if !data.dirs.iter().any(|d| d.path == *log_dir) {
                let mut d = AlterReplicaLogDir::new();
                d.set_path(log_dir.clone());
                data.dirs.push(d);
            }
            let dir = data.dirs.iter_mut().find(|d| d.path == *log_dir).expect("dir just inserted");
            if !dir.topics.iter().any(|t| t.name == replica.topic()) {
                let mut t = AlterReplicaLogDirTopic::new();
                t.set_name(replica.topic().to_string());
                dir.topics.push(t);
            }
            let topic = dir
                .topics
                .iter_mut()
                .find(|t| t.name == replica.topic())
                .expect("topic just inserted");
            topic.partitions.push(replica.partition());
        }

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);

        let public: HashMap<TopicPartitionReplica, KafkaFuture<()>> =
            futures.iter().map(|(k, v)| (k.clone(), v.future())).collect();
        let shared = Arc::new(futures);
        for (broker_id, assignment) in assignment_by_broker {
            let call = get_alter_replica_log_dirs_call(broker_id, assignment, Arc::clone(&shared), deadline);
            self.submit(call);
        }

        AlterReplicaLogDirsResult::new(public)
    }

    fn describe_replica_log_dirs_with_options(
        &self,
        replicas: &[TopicPartitionReplica],
        options: DescribeReplicaLogDirsOptions,
    ) -> DescribeReplicaLogDirsResult {
        let mut futures: HashMap<TopicPartitionReplica, KafkaFutureImpl<ReplicaLogDirInfo>> = HashMap::new();
        for replica in replicas {
            futures.insert(replica.clone(), KafkaFutureImpl::new());
        }

        // Group the requested replicas by broker, mirroring Java's
        // `partitionsByBroker`.
        let mut partitions_by_broker: HashMap<i32, DescribeLogDirsRequestData> = HashMap::new();
        for replica in replicas {
            let data = partitions_by_broker
                .entry(replica.broker_id())
                .or_insert_with(DescribeLogDirsRequestData::new);
            let topics = data.topics.get_or_insert_with(Vec::new);
            if let Some(topic) = topics.iter_mut().find(|t| t.topic == replica.topic()) {
                topic.partitions.push(replica.partition());
            } else {
                let mut topic = DescribableLogDirTopic::new();
                topic.set_topic(replica.topic().to_string());
                topic.set_partitions(vec![replica.partition()]);
                topics.push(topic);
            }
        }

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);

        let public: HashMap<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> =
            futures.iter().map(|(k, v)| (k.clone(), v.future())).collect();
        let shared = Arc::new(futures);
        for (broker_id, request_data) in partitions_by_broker {
            // Seed the per-partition result map with the default (empty)
            // `ReplicaLogDirInfo` for every requested partition.
            let mut seed: HashMap<TopicPartition, ReplicaLogDirInfo> = HashMap::new();
            if let Some(topics) = &request_data.topics {
                for topic in topics {
                    for &partition_id in &topic.partitions {
                        seed.insert(
                            TopicPartition::new(topic.topic.clone(), partition_id),
                            ReplicaLogDirInfo::default(),
                        );
                    }
                }
            }
            let call = get_describe_replica_log_dirs_call(broker_id, request_data, seed, Arc::clone(&shared), deadline);
            self.submit(call);
        }

        DescribeReplicaLogDirsResult::new(public)
    }

    fn elect_leaders_with_options(
        &self,
        election_type: ElectionType,
        partitions: Option<HashSet<TopicPartition>>,
        options: ElectLeadersOptions,
    ) -> ElectLeadersResult {
        let handle: KafkaFutureImpl<HashMap<TopicPartition, Option<Error>>> = KafkaFutureImpl::new();
        let public = handle.future();
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);

        // Preserve the caller's null-means-all semantics: `None` requests
        // election for all partitions.
        let request_partitions: Option<Vec<TopicPartition>> = partitions.map(|set| set.into_iter().collect());

        let req_partitions = request_partitions.clone();
        let create_request = Box::new(move |timeout_ms: i32| {
            Ok(Box::new(elect_leaders_request::Builder::new(
                election_type,
                req_partitions.clone(),
                timeout_ms,
            )) as Box<dyn RequestBuilder>)
        });

        let resp_handle = handle.clone();
        let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
            let ConcreteResponse::ElectLeaders(elect_response) = response else {
                return HandleResult::Retry(Error::local_illegal_state("Expected an ElectLeaders response"));
            };
            let result = ElectLeadersResponse::elect_leaders_result(elect_response.data());
            // For version == 0 the errorCode is 0 which maps to Errors.NONE.
            let error = Errors::for_code(elect_response.data().error_code);
            if error != Errors::None {
                resp_handle.complete_with_error(Error::new(error));
                return HandleResult::Done;
            }
            resp_handle.complete(result);
            HandleResult::Done
        });

        let fail_handle = handle.clone();
        let handle_failure = Box::new(move |error: &Error| {
            fail_handle.complete_with_error(error.clone());
        });

        let call = Call::new(
            "electLeaders",
            deadline,
            NodeProvider::Controller,
            create_request,
            handle_response,
            handle_failure,
            Box::new(|| false),
        );
        self.submit(call);
        ElectLeadersResult::new(public)
    }

    fn alter_partition_reassignments_with_options(
        &self,
        reassignments: &HashMap<TopicPartition, Option<NewPartitionReassignment>>,
        options: AlterPartitionReassignmentsOptions,
    ) -> AlterPartitionReassignmentsResult {
        let mut handles: HashMap<TopicPartition, KafkaFutureImpl<()>> = HashMap::new();
        // topic -> (partition -> reassignment); BTreeMap keeps a deterministic
        // topic/partition order, mirroring Java's TreeMap.
        let mut topics_to_reassignments: std::collections::BTreeMap<
            String,
            std::collections::BTreeMap<i32, Option<NewPartitionReassignment>>,
        > = std::collections::BTreeMap::new();

        for (topic_partition, reassignment) in reassignments {
            let topic = topic_partition.topic().to_string();
            let partition = topic_partition.partition();
            let future: KafkaFutureImpl<()> = KafkaFutureImpl::new();
            handles.insert(topic_partition.clone(), future.clone());

            if topic_name_is_unrepresentable(&topic) {
                future.complete_with_error(Error::with_message(
                    Errors::InvalidTopicError,
                    format!("The given topic name '{topic}' cannot be represented in a request."),
                ));
            } else if partition < 0 {
                future.complete_with_error(Error::with_message(
                    Errors::InvalidTopicError,
                    format!("The given partition index {partition} is not valid."),
                ));
            } else {
                topics_to_reassignments
                    .entry(topic)
                    .or_default()
                    .insert(partition, reassignment.clone());
            }
        }

        let public: HashMap<TopicPartition, KafkaFuture<()>> =
            handles.iter().map(|(k, v)| (k.clone(), v.future())).collect();

        if !topics_to_reassignments.is_empty() {
            let now = self.now();
            let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
            let allow_replication_factor_change = options.allow_replication_factor_change();
            let expected_responses_count: usize =
                topics_to_reassignments.values().map(std::collections::BTreeMap::len).sum();
            let call = get_alter_partition_reassignments_call(
                self.shared.metadata_manager.clone(),
                Arc::new(handles),
                Arc::new(topics_to_reassignments),
                allow_replication_factor_change,
                expected_responses_count,
                deadline,
            );
            self.submit(call);
        }
        AlterPartitionReassignmentsResult::new(public)
    }

    fn list_partition_reassignments_with_partitions_options(
        &self,
        partitions: Option<HashSet<TopicPartition>>,
        options: ListPartitionReassignmentsOptions,
    ) -> ListPartitionReassignmentsResult {
        let handle: KafkaFutureImpl<HashMap<TopicPartition, PartitionReassignment>> = KafkaFutureImpl::new();

        // Client-side validation, mirroring Java: an unrepresentable topic name
        // or negative partition fails the whole future and is never sent.
        if let Some(partitions) = &partitions {
            for tp in partitions {
                if topic_name_is_unrepresentable(tp.topic()) {
                    handle.complete_with_error(Error::with_message(
                        Errors::InvalidTopicError,
                        format!("The given topic name '{}' cannot be represented in a request.", tp.topic()),
                    ));
                } else if tp.partition() < 0 {
                    handle.complete_with_error(Error::with_message(
                        Errors::InvalidTopicError,
                        format!("The given partition index {} is not valid.", tp.partition()),
                    ));
                }
                if handle.future().is_done() {
                    return ListPartitionReassignmentsResult::new(handle.future());
                }
            }
        }

        let public = handle.future();
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let request_partitions: Option<Vec<TopicPartition>> = partitions.map(|set| set.into_iter().collect());
        let call = get_list_partition_reassignments_call(
            self.shared.metadata_manager.clone(),
            handle,
            request_partitions,
            deadline,
        );
        self.submit(call);
        ListPartitionReassignmentsResult::new(public)
    }

    fn list_offsets_with_options(
        &self,
        topic_partition_offsets: &HashMap<TopicPartition, OffsetSpec>,
        options: ListOffsetsOptions,
    ) -> ListOffsetsResult {
        let keys: HashSet<TopicPartition> = topic_partition_offsets.keys().cloned().collect();
        let future = ListOffsetsHandler::new_future(keys, Arc::clone(&self.shared.partition_leader_cache));
        let result_map = future.all();

        let offset_queries_by_partition: HashMap<TopicPartition, i64> = topic_partition_offsets
            .iter()
            .map(|(tp, spec)| (tp.clone(), get_offset_from_spec(*spec)))
            .collect();

        let handler = ListOffsetsHandler::new(
            offset_queries_by_partition,
            options.clone(),
            LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id)),
            self.shared.default_api_timeout_ms,
        );

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let retry_backoff = ExponentialBackoff::new(
            self.shared.retry_backoff_ms,
            RETRY_BACKOFF_EXP_BASE,
            self.shared.retry_backoff_max_ms,
            RETRY_BACKOFF_JITTER,
        )
        .expect("ExponentialBackoff::new only fails on invalid jitter");
        let driver = AdminApiDriver::new(
            Box::new(handler),
            Box::new(future),
            deadline,
            retry_backoff,
            LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id)),
        );
        invoke_driver(driver, self.driver_context(), now);

        ListOffsetsResult::new(result_map)
    }

    fn list_groups_with_options(&self, options: ListGroupsOptions) -> ListGroupsResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let states: Vec<String> = options.group_states().iter().map(GroupState::to_string).collect();
        let types: Vec<String> = options.types().iter().map(GroupType::to_string).collect();
        let protocol_types: HashSet<String> = options.protocol_types().clone();

        let future = self.submit_list_groups(deadline, states, types, move |group| {
            if !protocol_types.is_empty() && !protocol_types.contains(&group.protocol_type) {
                return None;
            }
            let group_type = if group.group_type.is_empty() {
                None
            } else {
                Some(GroupType::parse(&group.group_type))
            };
            let group_state = if group.group_state.is_empty() {
                None
            } else {
                Some(GroupState::parse(&group.group_state))
            };
            Some((
                group.group_id.clone(),
                GroupListing::new(group.group_id.clone(), group_type, group.protocol_type.clone(), group_state),
            ))
        });
        ListGroupsResult::new(future)
    }

    fn describe_consumer_groups_with_options(
        &self,
        group_ids: &[String],
        options: DescribeConsumerGroupsOptions,
    ) -> DescribeConsumerGroupsResult {
        let log_context = LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id));
        let future = DescribeConsumerGroupsHandler::new_future(group_ids);
        let result_map = future.all();
        let handler = DescribeConsumerGroupsHandler::new(options.include_authorized_operations(), log_context.clone());

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let retry_backoff = self.retry_backoff();
        let driver = AdminApiDriver::new(Box::new(handler), Box::new(future), deadline, retry_backoff, log_context);
        invoke_driver(driver, self.driver_context(), now);

        DescribeConsumerGroupsResult::new(coordinator_keyed_by_id(result_map))
    }

    fn describe_classic_groups_with_options(
        &self,
        group_ids: &[String],
        options: DescribeClassicGroupsOptions,
    ) -> DescribeClassicGroupsResult {
        let log_context = LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id));
        let future = DescribeClassicGroupsHandler::new_future(group_ids);
        let result_map = future.all();
        let handler = DescribeClassicGroupsHandler::new(options.include_authorized_operations(), log_context.clone());

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let retry_backoff = self.retry_backoff();
        let driver = AdminApiDriver::new(Box::new(handler), Box::new(future), deadline, retry_backoff, log_context);
        invoke_driver(driver, self.driver_context(), now);

        DescribeClassicGroupsResult::new(coordinator_keyed_by_id(result_map))
    }

    fn list_consumer_group_offsets_with_group_specs_options(
        &self,
        group_specs: &HashMap<String, ListConsumerGroupOffsetsSpec>,
        options: ListConsumerGroupOffsetsOptions,
    ) -> ListConsumerGroupOffsetsResult {
        let log_context = LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id));
        let group_ids: Vec<String> = group_specs.keys().cloned().collect();
        let future = ListConsumerGroupOffsetsHandler::new_future(&group_ids);
        let result_map = future.all();
        let handler =
            ListConsumerGroupOffsetsHandler::new(group_specs.clone(), options.require_stable(), log_context.clone());

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let retry_backoff = self.retry_backoff();
        let driver = AdminApiDriver::new(Box::new(handler), Box::new(future), deadline, retry_backoff, log_context);
        invoke_driver(driver, self.driver_context(), now);

        ListConsumerGroupOffsetsResult::new(coordinator_keyed_by_id(result_map))
    }

    fn alter_consumer_group_offsets_with_options(
        &self,
        group_id: &str,
        offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
        options: AlterConsumerGroupOffsetsOptions,
    ) -> AlterConsumerGroupOffsetsResult {
        let log_context = LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id));
        let future = AlterConsumerGroupOffsetsHandler::new_future(group_id);
        let result_map = future.all();
        let key = CoordinatorKey::by_group_id(group_id);
        let handler = AlterConsumerGroupOffsetsHandler::new(group_id, offsets.clone(), log_context.clone());

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let retry_backoff = self.retry_backoff();
        let driver = AdminApiDriver::new(Box::new(handler), Box::new(future), deadline, retry_backoff, log_context);
        invoke_driver(driver, self.driver_context(), now);

        AlterConsumerGroupOffsetsResult::new(result_map.get(&key).expect("future exists for the group key").clone())
    }

    fn delete_consumer_group_offsets_with_options(
        &self,
        group_id: &str,
        partitions: &HashSet<TopicPartition>,
        options: DeleteConsumerGroupOffsetsOptions,
    ) -> DeleteConsumerGroupOffsetsResult {
        let log_context = LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id));
        let future = DeleteConsumerGroupOffsetsHandler::new_future(group_id);
        let result_map = future.all();
        let key = CoordinatorKey::by_group_id(group_id);
        let handler = DeleteConsumerGroupOffsetsHandler::new(group_id, partitions.clone(), log_context.clone());

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let retry_backoff = self.retry_backoff();
        let driver = AdminApiDriver::new(Box::new(handler), Box::new(future), deadline, retry_backoff, log_context);
        invoke_driver(driver, self.driver_context(), now);

        DeleteConsumerGroupOffsetsResult::new(
            result_map.get(&key).expect("future exists for the group key").clone(),
            partitions.clone(),
        )
    }

    fn delete_consumer_groups_with_options(
        &self,
        group_ids: &[String],
        options: DeleteConsumerGroupsOptions,
    ) -> DeleteConsumerGroupsResult {
        let log_context = LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id));
        let future = DeleteGroupsHandler::new_future(group_ids);
        let result_map = future.all();
        let handler = DeleteConsumerGroupsHandler::new(log_context.clone());

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let retry_backoff = self.retry_backoff();
        let driver = AdminApiDriver::new(Box::new(handler), Box::new(future), deadline, retry_backoff, log_context);
        invoke_driver(driver, self.driver_context(), now);

        DeleteConsumerGroupsResult::new(coordinator_keyed_by_id(result_map))
    }

    fn remove_members_from_consumer_group_with_options(
        &self,
        group_id: &str,
        options: RemoveMembersFromConsumerGroupOptions,
    ) -> RemoveMembersFromConsumerGroupResult {
        let log_context = LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id));
        let reason = match options.reason() {
            None | Some("") => DEFAULT_LEAVE_GROUP_REASON.to_string(),
            Some(r) => JoinGroupRequest::maybe_truncate_reason(r),
        };

        let admin_future = RemoveMembersFromConsumerGroupHandler::new_future(group_id);
        let result_map = admin_future.all();
        let key = CoordinatorKey::by_group_id(group_id);
        let group_future = result_map.get(&key).expect("future exists for the group key").clone();

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let retry_backoff = self.retry_backoff();
        let ctx = self.driver_context();

        if options.remove_all() {
            // Mirrors `getMembersFromGroup`: describe the group, then chain the
            // `LeaveGroup` driver once the membership is known.
            //
            // The describe step is issued via `describeConsumerGroups(
            // Collections.singleton(groupId))` with a *default*
            // `DescribeConsumerGroupsOptions` (Java `KafkaAdminClient.java:4172`),
            // whose `timeoutMs` is `null`. Its deadline is therefore
            // `now + defaultApiTimeoutMs`, independent of the removeMembers
            // request's `options.timeout_ms()`.
            let default_api_timeout_ms = self.shared.default_api_timeout_ms;
            let options_timeout = options.timeout_ms();
            let describe_deadline = calc_deadline_ms(now, None, default_api_timeout_ms);

            let describe_group_ids = vec![group_id.to_string()];
            let describe_future = DescribeConsumerGroupsHandler::new_future(&describe_group_ids);
            let describe_handle = describe_future.handle(&key).expect("describe future exists for the group key");
            let describe_handler = DescribeConsumerGroupsHandler::new(false, log_context.clone());
            let describe_driver = AdminApiDriver::new(
                Box::new(describe_handler),
                Box::new(describe_future),
                describe_deadline,
                retry_backoff.clone(),
                log_context.clone(),
            );
            invoke_driver(describe_driver, ctx.clone(), now);

            let group_id_owned = group_id.to_string();
            let key_for_cb = key.clone();
            describe_handle.when_complete(move |result| match result {
                Err(error) => {
                    // `new KafkaException("Encounter exception when trying to get
                    // members from group: " + groupId, ex)`
                    // (`KafkaAdminClient.java:4174`) — a bare `KafkaException`
                    // carrying the cause, NOT the inner class. Inheriting it made a
                    // `GroupAuthorizationError` cause answer `is_authorization_error()`
                    // (and so `is_fatal_error()`) where Java answers `false`, and lost
                    // the cause entirely (finding 243). "exception" is reworded to
                    // "error" per CLAUDE.md §2; the rest is Java's text verbatim.
                    admin_future.complete_with_error(HashMap::from([(
                        key_for_cb,
                        Error::kafka_message_source(
                            format!("Encounter error when trying to get members from group: {group_id_owned}"),
                            error.clone(),
                        ),
                    )]));
                },
                Ok(description) => {
                    let members: Vec<MemberIdentity> = description
                        .members()
                        .iter()
                        .map(|member| {
                            let mut identity = MemberIdentity::new();
                            match member.group_instance_id() {
                                Some(instance_id) => {
                                    identity.set_group_instance_id(Some(instance_id.to_string()));
                                },
                                None => {
                                    identity.set_member_id(member.consumer_id().to_string());
                                },
                            }
                            identity.set_reason(Some(reason.clone()));
                            identity
                        })
                        .collect();
                    let handler =
                        RemoveMembersFromConsumerGroupHandler::new(&group_id_owned, members, log_context.clone());
                    // Recompute the `LeaveGroup` deadline from the *current* time,
                    // now that the describe future has resolved. This mirrors Java's
                    // `memFuture.whenComplete(...)` (`KafkaAdminClient.java:4224-4230`)
                    // calling `invokeDriver(handler, adminFuture, options.timeoutMs())`,
                    // whose `calcDeadlineMs(time.milliseconds(), timeoutMs)` runs at
                    // this later moment — giving `LeaveGroup` a fresh full timeout
                    // window rather than the (already partially consumed) describe one.
                    let leave_now = ctx.time.milliseconds();
                    let leave_deadline = calc_deadline_ms(leave_now, options_timeout, default_api_timeout_ms);
                    let driver = AdminApiDriver::new(
                        Box::new(handler),
                        Box::new(admin_future),
                        leave_deadline,
                        retry_backoff.clone(),
                        log_context.clone(),
                    );
                    invoke_driver(driver, ctx.clone(), leave_now);
                },
            });
        } else {
            let members: Vec<MemberIdentity> = options
                .members()
                .iter()
                .map(|member| {
                    let mut identity = member.to_member_identity();
                    identity.set_reason(Some(reason.clone()));
                    identity
                })
                .collect();
            let handler = RemoveMembersFromConsumerGroupHandler::new(group_id, members, log_context.clone());
            let driver =
                AdminApiDriver::new(Box::new(handler), Box::new(admin_future), deadline, retry_backoff, log_context);
            invoke_driver(driver, ctx, now);
        }

        RemoveMembersFromConsumerGroupResult::new(group_future, options.members().clone())
    }

    fn create_acls_with_options(&self, acls: &[AclBinding], options: CreateAclsOptions) -> CreateAclsResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);

        let mut handles: HashMap<AclBinding, KafkaFutureImpl<()>> = HashMap::new();
        let mut acl_creations: Vec<AclCreation> = Vec::new();
        let mut acl_bindings_sent: Vec<AclBinding> = Vec::new();
        for acl in acls {
            if let std::collections::hash_map::Entry::Vacant(entry) = handles.entry(acl.clone()) {
                let future: KafkaFutureImpl<()> = KafkaFutureImpl::new();
                entry.insert(future.clone());
                match acl.to_filter().find_indefinite_field() {
                    None => {
                        acl_creations.push(CreateAclsRequest::acl_creation(acl));
                        acl_bindings_sent.push(acl.clone());
                    },
                    Some(indefinite) => {
                        future.complete_with_error(Error::with_message(
                            Errors::InvalidRequest,
                            format!("Invalid ACL creation: {indefinite}"),
                        ));
                    },
                }
            }
        }
        let public: HashMap<AclBinding, KafkaFuture<()>> =
            handles.iter().map(|(k, v)| (k.clone(), v.future())).collect();

        let call = get_create_acls_call(
            self.shared.metadata_manager.clone(),
            Arc::new(handles),
            acl_creations,
            acl_bindings_sent,
            deadline,
        );
        self.submit(call);
        CreateAclsResult::new(public)
    }

    fn describe_acls_with_options(
        &self,
        filter: &AclBindingFilter,
        options: DescribeAclsOptions,
    ) -> DescribeAclsResult {
        // Short-circuit on an unknown filter, mirroring
        // `KafkaAdminClient.describeAcls`: complete the future exceptionally
        // with InvalidRequestException and enqueue no Call.
        if filter.is_unknown() {
            let handle: KafkaFutureImpl<Vec<AclBinding>> = KafkaFutureImpl::new();
            handle.complete_with_error(Error::with_message(
                Errors::InvalidRequest,
                "The AclBindingFilter must not contain CreateTopicsResult::UNKNOWN elements.",
            ));
            return DescribeAclsResult::new(handle.future());
        }

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let handle: KafkaFutureImpl<Vec<AclBinding>> = KafkaFutureImpl::new();
        let public = handle.future();
        let call = get_describe_acls_call(filter.clone(), handle, deadline);
        self.submit(call);
        DescribeAclsResult::new(public)
    }

    fn delete_acls_with_options(&self, filters: &[AclBindingFilter], options: DeleteAclsOptions) -> DeleteAclsResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);

        let mut handles: HashMap<AclBindingFilter, KafkaFutureImpl<FilterResults>> = HashMap::new();
        let mut acl_binding_filters_sent: Vec<AclBindingFilter> = Vec::new();
        let mut delete_acls_filters: Vec<DeleteAclsFilter> = Vec::new();
        for filter in filters {
            if let std::collections::hash_map::Entry::Vacant(entry) = handles.entry(filter.clone()) {
                acl_binding_filters_sent.push(filter.clone());
                delete_acls_filters.push(DeleteAclsRequest::delete_acls_filter(filter));
                entry.insert(KafkaFutureImpl::new());
            }
        }
        let public: HashMap<AclBindingFilter, KafkaFuture<FilterResults>> =
            handles.iter().map(|(k, v)| (k.clone(), v.future())).collect();

        let call = get_delete_acls_call(
            self.shared.metadata_manager.clone(),
            Arc::new(handles),
            acl_binding_filters_sent,
            delete_acls_filters,
            deadline,
        );
        self.submit(call);
        DeleteAclsResult::new(public)
    }

    fn describe_client_quotas_with_options(
        &self,
        filter: &ClientQuotaFilter,
        options: DescribeClientQuotasOptions,
    ) -> DescribeClientQuotasResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let handle: KafkaFutureImpl<HashMap<ClientQuotaEntity, HashMap<String, f64>>> = KafkaFutureImpl::new();
        let public = handle.future();
        let call = get_describe_client_quotas_call(filter.clone(), handle, deadline);
        self.submit(call);
        DescribeClientQuotasResult::new(public)
    }

    fn alter_client_quotas_with_options(
        &self,
        entries: &[ClientQuotaAlteration],
        options: AlterClientQuotasOptions,
    ) -> AlterClientQuotasResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);

        // Mirrors Java: one future per entity (later entries with the same
        // entity share the single future for that entity).
        let mut handles: HashMap<ClientQuotaEntity, KafkaFutureImpl<()>> = HashMap::new();
        for entry in entries {
            handles.entry(entry.entity().clone()).or_default();
        }
        let public: HashMap<ClientQuotaEntity, KafkaFuture<()>> =
            handles.iter().map(|(k, v)| (k.clone(), v.future())).collect();

        let call = get_alter_client_quotas_call(entries.to_vec(), options.validate_only(), Arc::new(handles), deadline);
        self.submit(call);
        AlterClientQuotasResult::new(public)
    }

    fn describe_user_scram_credentials_with_users_options(
        &self,
        users: &[String],
        options: DescribeUserScramCredentialsOptions,
    ) -> DescribeUserScramCredentialsResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let handle: KafkaFutureImpl<DescribeUserScramCredentialsResponseData> = KafkaFutureImpl::new();
        let public = handle.future();
        let call = get_describe_user_scram_credentials_call(users.to_vec(), handle, deadline);
        self.submit(call);
        DescribeUserScramCredentialsResult::new(public)
    }

    fn alter_user_scram_credentials_with_options(
        &self,
        alterations: &[UserScramCredentialAlteration],
        options: AlterUserScramCredentialsOptions,
    ) -> AlterUserScramCredentialsResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);

        // Mirrors Java: one future per user.
        let mut handles: HashMap<String, KafkaFutureImpl<()>> = HashMap::new();
        for alteration in alterations {
            handles.insert(alteration.user().to_string(), KafkaFutureImpl::new());
        }

        // We track users with an illegal alteration so we can fail all their
        // alterations later; we also pre-build the wire deletions/upsertions for
        // the ones that pass validation. Building an upsertion runs PBKDF2.
        let unknown_scram_mechanism_msg = "Unknown SCRAM mechanism";
        let mut illegal: HashMap<String, Error> = HashMap::new();

        // Deletions with an empty user or an unknown mechanism are illegal.
        for alteration in alterations {
            if let UserScramCredentialAlteration::Deletion(deletion) = alteration {
                let user = deletion.user();
                if user.is_empty() {
                    illegal.insert(user.to_string(), unacceptable_credential("Username must not be empty"));
                } else if deletion.mechanism() == ScramMechanism::Unknown {
                    illegal.insert(user.to_string(), unsupported_sasl_mechanism(unknown_scram_mechanism_msg));
                }
            }
        }

        // Upsertions: validate and compute the salted password (PBKDF2) once.
        let mut user_insertions: HashMap<String, HashMap<i8, ScramCredentialUpsertion>> = HashMap::new();
        for alteration in alterations {
            let UserScramCredentialAlteration::Upsertion(upsertion) = alteration else {
                continue;
            };
            let user = upsertion.user();
            if illegal.contains_key(user) {
                continue;
            }
            if user.is_empty() {
                illegal.insert(user.to_string(), unacceptable_credential("Username must not be empty"));
                continue;
            }
            if upsertion.password().is_empty() {
                illegal.insert(user.to_string(), unacceptable_credential("Password must not be empty"));
                continue;
            }
            let mechanism = upsertion.credential_info().mechanism();
            if mechanism == ScramMechanism::Unknown {
                illegal.insert(user.to_string(), unsupported_sasl_mechanism(unknown_scram_mechanism_msg));
                continue;
            }
            match get_scram_credential_upsertion(upsertion) {
                Ok(wire) => {
                    user_insertions
                        .entry(user.to_string())
                        .or_default()
                        .insert(mechanism.r#type(), wire);
                },
                // Mirrors the NoSuchAlgorithmException branch (unknown mechanism).
                Err(e) => {
                    illegal.insert(user.to_string(), e);
                },
            }
        }

        // Pre-build the request payloads (in alteration order) for the users that
        // survived validation. The crypto is already done, so retries just reuse
        // these; mirrors Java's `userInsertions` map being computed once.
        let mut request_upsertions = Vec::new();
        for alteration in alterations {
            if let UserScramCredentialAlteration::Upsertion(upsertion) = alteration {
                if illegal.contains_key(upsertion.user()) {
                    continue;
                }
                if let Some(wire) = user_insertions
                    .get(upsertion.user())
                    .and_then(|by_mech| by_mech.get(&upsertion.credential_info().mechanism().r#type()))
                {
                    request_upsertions.push(wire.clone());
                }
            }
        }
        let mut request_deletions = Vec::new();
        for alteration in alterations {
            if let UserScramCredentialAlteration::Deletion(deletion) = alteration {
                if illegal.contains_key(deletion.user()) {
                    continue;
                }
                request_deletions.push(get_scram_credential_deletion(deletion));
            }
        }

        let handles = Arc::new(handles);
        let public: HashMap<String, KafkaFuture<()>> =
            handles.iter().map(|(user, handle)| (user.clone(), handle.future())).collect();

        let call = get_alter_user_scram_credentials_call(
            request_deletions,
            request_upsertions,
            Arc::new(illegal),
            Arc::clone(&handles),
            self.shared.metadata_manager.clone(),
            deadline,
        );
        self.submit(call);
        AlterUserScramCredentialsResult::new(public)
    }

    fn create_delegation_token_with_options(
        &self,
        options: CreateDelegationTokenOptions,
    ) -> CreateDelegationTokenResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let handle: KafkaFutureImpl<DelegationToken> = KafkaFutureImpl::new();
        let public = handle.future();
        let call = get_create_delegation_token_call(options, handle, deadline);
        self.submit(call);
        CreateDelegationTokenResult::new(public)
    }

    fn renew_delegation_token_with_options(
        &self,
        hmac: &[u8],
        options: RenewDelegationTokenOptions,
    ) -> RenewDelegationTokenResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let handle: KafkaFutureImpl<i64> = KafkaFutureImpl::new();
        let public = handle.future();
        let call = get_renew_delegation_token_call(hmac.to_vec(), options.renew_time_period_ms(), handle, deadline);
        self.submit(call);
        RenewDelegationTokenResult::new(public)
    }

    fn expire_delegation_token_with_options(
        &self,
        hmac: &[u8],
        options: ExpireDelegationTokenOptions,
    ) -> ExpireDelegationTokenResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let handle: KafkaFutureImpl<i64> = KafkaFutureImpl::new();
        let public = handle.future();
        let call = get_expire_delegation_token_call(hmac.to_vec(), options.expiry_time_period_ms(), handle, deadline);
        self.submit(call);
        ExpireDelegationTokenResult::new(public)
    }

    fn describe_delegation_token_with_options(
        &self,
        options: DescribeDelegationTokenOptions,
    ) -> DescribeDelegationTokenResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);
        let handle: KafkaFutureImpl<Vec<DelegationToken>> = KafkaFutureImpl::new();
        let public = handle.future();
        let owners = options.owners().map(<[KafkaPrincipal]>::to_vec);
        let call = get_describe_delegation_token_call(owners, handle, deadline);
        self.submit(call);
        DescribeDelegationTokenResult::new(public)
    }

    fn describe_features_with_options(&self, options: DescribeFeaturesOptions) -> DescribeFeaturesResult {
        let handle: KafkaFutureImpl<FeatureMetadata> = KafkaFutureImpl::new();
        let public = handle.future();
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);

        // Mirrors Java: a set nodeId routes to that specific broker via
        // `ConstantNodeIdProvider`, otherwise the request goes to an arbitrary
        // broker or the active controller.
        let node_provider = match options.node_id() {
            Some(node_id) => NodeProvider::ConstantNodeId(node_id),
            None => NodeProvider::LeastLoadedBrokerOrActiveKController,
        };

        let create_request = Box::new(move |_timeout_ms: i32| {
            Ok(Box::new(api_versions_request::Builder::new()) as Box<dyn RequestBuilder>)
        });

        let resp_handle = handle.clone();
        let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
            let ConcreteResponse::ApiVersions(api_versions) = response else {
                return HandleResult::Retry(Error::local_illegal_state("Expected an ApiVersions response"));
            };
            let data = api_versions.data();
            if data.error_code == Errors::None.code() {
                match create_feature_metadata(data) {
                    Ok(metadata) => resp_handle.complete(metadata),
                    Err(e) => resp_handle.complete_with_error(e),
                };
            } else {
                resp_handle.complete_with_error(Error::new(Errors::for_code(data.error_code)));
            }
            HandleResult::Done
        });

        let fail_handle = handle.clone();
        let handle_failure = Box::new(move |error: &Error| {
            fail_handle.complete_with_error(error.clone());
        });

        let call = Call::new(
            "describeFeatures",
            deadline,
            node_provider,
            create_request,
            handle_response,
            handle_failure,
            Box::new(|| false),
        );
        self.submit(call);
        DescribeFeaturesResult::new(public)
    }

    fn update_features_with_options(
        &self,
        feature_updates: &HashMap<String, FeatureUpdate>,
        options: UpdateFeaturesOptions,
    ) -> Result<UpdateFeaturesResult, Error> {
        if feature_updates.is_empty() {
            return Err(Error::local_illegal_argument("Feature updates can not be null or empty."));
        }

        let mut handles: HashMap<String, KafkaFutureImpl<()>> = HashMap::new();
        for feature in feature_updates.keys() {
            if feature.is_empty() {
                return Err(Error::local_illegal_argument("Provided feature can not be empty."));
            }
            handles.insert(feature.clone(), KafkaFutureImpl::new());
        }
        let handles = Arc::new(handles);
        let public: HashMap<String, KafkaFuture<()>> = handles
            .iter()
            .map(|(feature, handle)| (feature.clone(), handle.future()))
            .collect();

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout_ms(), self.shared.default_api_timeout_ms);

        // Snapshot the updates for the (possibly retried) request builder.
        let updates_for_request: Vec<(String, FeatureUpdate)> = feature_updates
            .iter()
            .map(|(feature, update)| (feature.clone(), *update))
            .collect();
        let validate_only = options.validate_only();
        let create_request = Box::new(move |timeout_ms: i32| {
            let mut collection = Vec::with_capacity(updates_for_request.len());
            for (feature, update) in &updates_for_request {
                let mut item = FeatureUpdateKey::new();
                item.set_feature(feature.clone());
                item.set_max_version_level(update.max_version_level());
                item.set_upgrade_type(update.upgrade_type().code());
                collection.push(item);
            }
            let mut data = UpdateFeaturesRequestData::new();
            data.set_timeout_ms(timeout_ms);
            data.set_validate_only(validate_only);
            data.set_feature_updates(collection);
            Ok(Box::new(update_features_request::Builder::new(data)) as Box<dyn RequestBuilder>)
        });

        let resp_mm = self.shared.metadata_manager.clone();
        let resp_handles = Arc::clone(&handles);
        let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
            let ConcreteResponse::UpdateFeatures(update_response) = response else {
                return HandleResult::Retry(Error::local_illegal_state("Expected an UpdateFeatures response"));
            };
            let data = update_response.data();
            let top_level_error = Errors::for_code(data.error_code);
            match top_level_error {
                Errors::None => {
                    if data.results.is_empty() {
                        // For V2 and above, NONE responses just have a top-level
                        // NONE error -- mark all the futures as completed.
                        for future in resp_handles.values() {
                            future.complete(());
                        }
                    } else {
                        for result in &data.results {
                            match resp_handles.get(&result.feature) {
                                // The server should send back a result for every
                                // feature, but we only complete known features.
                                None => {},
                                Some(future) => {
                                    let error = Errors::for_code(result.error_code);
                                    if error == Errors::None {
                                        future.complete(());
                                    } else {
                                        future.complete_with_error(api_error(result.error_code, &result.error_message));
                                    }
                                },
                            }
                        }
                        // Sanity check: the server should send back a response
                        // for every feature (mirrors completeUnrealizedFutures).
                        for (feature, future) in resp_handles.iter() {
                            if !future.is_done() {
                                // // Java's `completeUnrealizedFutures` throws `new ApiException(..)`
                                // (`KafkaAdminClient.java:1748`) — the concrete base, not the
                                // `UnknownServerException` subclass it uses elsewhere for
                                // response sanity checks (`:2631`, `:2684`, `:4020`). Finding 246.
                                future.complete_with_error(Error::Api(ApiError::new(format!(
                                    "The controller response did not contain a result for feature {feature}"
                                ))));
                            }
                        }
                    }
                },
                Errors::NotController => {
                    // Mirrors handleNotControllerError(Errors.NOT_CONTROLLER):
                    // clear the cached controller, request a metadata refresh and
                    // retry the call.
                    resp_mm.clear_controller();
                    resp_mm.request_update();
                    return HandleResult::Retry(Error::new(Errors::NotController));
                },
                _ => {
                    let error = api_error(data.error_code, &data.error_message);
                    for future in resp_handles.values() {
                        future.complete_with_error(error.clone());
                    }
                },
            }
            HandleResult::Done
        });

        let fail_handles = Arc::clone(&handles);
        let handle_failure = Box::new(move |error: &Error| {
            for future in fail_handles.values() {
                future.complete_with_error(error.clone());
            }
        });

        let call = Call::new(
            "updateFeatures",
            deadline,
            NodeProvider::Controller,
            create_request,
            handle_response,
            handle_failure,
            Box::new(|| false),
        );
        self.submit(call);
        Ok(UpdateFeaturesResult::new(public))
    }

    async fn close_with_timeout(&self, timeout: Duration) {
        // Java: `waitTimeMs = Math.min(TimeUnit.DAYS.toMillis(365), timeout.toMillis())`.
        // Its `waitTimeMs < 0` check throws `IllegalArgumentException`; a
        // `Duration` cannot be negative, so that branch is unrepresentable here.
        let wait_time_ms = timeout.as_millis().min(MAX_CLOSE_WAIT_TIME_MS as u128) as i64;
        let now = self.now();
        let new_hard_shutdown_time_ms = now.saturating_add(wait_time_ms);

        // Java publishes the deadline through a compare-and-set loop whose whole
        // purpose is monotonicity: if another `close()` already installed an
        // earlier deadline it keeps that one ("Hard shutdown time is already
        // earlier than requested"), so the deadline only ever moves forward in
        // urgency. A plain store would let `close(60s)` after `close(100ms)`
        // re-widen the poll budget that `process_pending_calls` reads on every iteration.
        //
        // Java also reassigns `newHardShutdownTimeMs = prev` on that branch, but
        // only to feed a debug log, so it has no counterpart here.
        let mut prev = Self::NO_HARD_SHUTDOWN;
        loop {
            match self.shared.shutdown.hard_shutdown_deadline_ms.compare_exchange(
                prev,
                new_hard_shutdown_time_ms,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => {
                    if actual < new_hard_shutdown_time_ms {
                        // An earlier (more urgent) deadline is already installed.
                        break;
                    }
                    prev = actual;
                },
            }
        }
        self.shared.shutdown.closing.store(true, std::sync::atomic::Ordering::Release);
        // Java calls `client.wakeup()` from inside the successful CAS arm. Here
        // the wakeup follows the `closing` store so the woken I/O task is
        // guaranteed to observe both, and it is issued on the
        // already-earlier-deadline path too (where it is a harmless no-op: the
        // `close()` that installed that deadline has already woken the task).
        self.shared.wakeup.notify_one();

        // Java ends with a *timed* join (`KafkaAdminClient.close`):
        //
        // ```java
        // if (Thread.currentThread() != thread) {
        //     thread.join(waitTimeMs);
        // }
        // ```
        //
        // The deadline installed above is only a hint to the I/O loop; the timed
        // join is the caller's guarantee, and it matters more here than in Java:
        // Java's `sendEligibleCalls` calls the non-blocking NIO
        // `client.ready(...)`, whereas ours awaits `NetworkClient::ready` →
        // `initiate_connect` → `Selector::connect`, which awaits the TCP
        // handshake and no shutdown deadline can interrupt.
        //
        // Java's `Thread.currentThread() != thread` self-deadlock guard has no
        // analogue: the I/O task owns no `Admin` handle and every per-`Call` hook
        // it runs is a sync closure, so `close()` cannot be re-entered from it.
        // Should that ever change, the timed join bounds the wait instead of
        // deadlocking, where Java skips the join entirely.
        //
        // Deliberate divergence: Java's `Thread.join(0)` means "wait forever", so
        // `close(Duration::ZERO)` there is unbounded. We treat 0 as 0, because
        // both `Admin::close`'s rustdoc and the exported C header promise a
        // return within `timeout`, and because of the uninterruptible connect
        // await above the Java behavior would be a genuine hang rather than the
        // near-immediate return it is in Java.
        let mut handle = self.shared.bg_handle.lock().unwrap().take();
        if let Some(join_handle) = handle.as_mut() {
            let joined = tokio::time::timeout(Duration::from_millis(wait_time_ms as u64), join_handle).await;
            // Java's counterpart logs when the join does not complete cleanly
            // (`KafkaAdminClient.java:707-710`, `catch (InterruptedException e)`).
            // `InterruptedException` has no Tokio analogue, but a `JoinError` — the
            // task panicked or was aborted — is the same class of "the I/O task did
            // not shut down normally" signal, and discarding it left `close()`
            // returning as if nothing had happened.
            match joined {
                Ok(Ok(())) => kafka_debug!(self.shared.log_context, "Kafka admin client closed."),
                Ok(Err(join_error)) => kafka_error!(
                    self.shared.log_context,
                    "The Kafka admin client I/O task did not exit cleanly: {}",
                    join_error
                ),
                Err(_elapsed) => {
                    // Expired: leave the task running, exactly as Java leaves the
                    // I/O thread running after an expired join, and put the handle
                    // back so a later `close()` can still join it.
                    *self.shared.bg_handle.lock().unwrap() = handle;
                },
            }
        }
    }
}

/// Builds the `describeTopics` (by name) [`Call`] using the Metadata API: the
/// fallback `describeTopicPartitions` issues for a broker that does not support
/// `DescribeTopicPartitions`.
/// Translated from `KafkaAdminClient.generateDescribeTopicsCallWithMetadataApi`.
fn get_describe_topics_by_names_call(
    futures: Arc<HashMap<String, KafkaFutureImpl<TopicDescription>>>,
    names: Vec<String>,
    include_authorized_operations: bool,
    deadline: i64,
) -> Call {
    // `supports_disabling_topic_creation` toggles the metadata request between
    // per-topic (auto-creation disabled) and all-topics on the first
    // UnsupportedVersionException, mirroring Java's downgrade.
    let supports_disabling = Arc::new(std::sync::atomic::AtomicBool::new(true));

    let req_names = names.clone();
    let req_supports = Arc::clone(&supports_disabling);
    let create_request = Box::new(move |_timeout_ms: i32| {
        if req_supports.load(std::sync::atomic::Ordering::Acquire) {
            let refs: Vec<&str> = req_names.iter().map(String::as_str).collect();
            let mut data = crate::MetadataRequestData::new();
            data.set_topics(Some(
                crate::common::requests::MetadataRequest::convert_to_metadata_request_topic(&refs),
            ));
            data.set_allow_auto_topic_creation(false);
            data.set_include_topic_authorized_operations(include_authorized_operations);
            Ok(Box::new(metadata_request::Builder::with_data(data)) as Box<dyn RequestBuilder>)
        } else {
            Ok(Box::new(metadata_request::Builder::all_topics()) as Box<dyn RequestBuilder>)
        }
    });

    let resp_futures = Arc::clone(&futures);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::Metadata(metadata_response) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected a Metadata response"));
        };
        let cluster = metadata_response.build_cluster();
        let errors = metadata_response.errors();
        for (topic_name, future) in resp_futures.iter() {
            if let Some(topic_error) = errors.get(topic_name) {
                future.complete_with_error(Error::new(*topic_error));
                continue;
            }
            if !cluster.topics().any(|t| t == topic_name.as_str()) {
                future.complete_with_error(Error::with_message(
                    Errors::UnknownTopicOrPartition,
                    format!("Topic {topic_name} not found."),
                ));
                continue;
            }
            let topic_id = cluster.topic_id(topic_name);
            let authorized_operations = metadata_response
                .topic_authorized_operations(topic_name)
                .unwrap_or(MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED);
            future.complete(topic_description_from_cluster(
                &cluster,
                topic_name,
                topic_id,
                authorized_operations,
            ));
        }
        HandleResult::Done
    });

    let fail_futures = Arc::clone(&futures);
    let handle_failure = Box::new(move |error: &Error| {
        for future in fail_futures.values() {
            future.complete_with_error(error.clone());
        }
    });

    let uv_supports = Arc::clone(&supports_disabling);
    // First UnsupportedVersion: downgrade to all-topics and retry (returns
    // true). A second one is a real failure (returns false).
    let handle_uv = Box::new(move || uv_supports.swap(false, std::sync::atomic::Ordering::AcqRel));

    Call::new(
        "describeTopics",
        deadline,
        NodeProvider::LeastLoaded,
        create_request,
        handle_response,
        handle_failure,
        handle_uv,
    )
}

/// The state [`generate_describe_topics_call_with_describe_topic_partitions_api`]'s
/// hooks share: the fields of Java's anonymous `Call` subclass
/// (`partiallyFinishedTopicDescription`) plus the `topicsRequests` map it
/// closes over. The request and response hooks are separate closures in Rust,
/// so what Java keeps in the one object is shared between them here.
struct DescribeTopicPartitionsState {
    /// Java's `topicsRequests`, a `LinkedHashMap` filled from the sorted topic
    /// names: the topics not yet fully described, in name order.
    topics_requests: BTreeSet<String>,
    /// Java's `partiallyFinishedTopicDescription`: the cursor topic of the
    /// previous page, whose partitions continue in the next one.
    partially_finished_topic_description: Option<TopicDescription>,
    /// Whether `handleUnsupportedVersionException` has issued the Metadata-API
    /// fallback. The failure hook may leave the futures to that call only once it
    /// has been issued; see the failure hook for why it can be missing.
    metadata_fallback_issued: bool,
}

/// Builds the paginated `describeTopicPartitions` [`Call`] for
/// `describeTopics` by name (KIP-966).
///
/// Translated from `KafkaAdminClient.generateDescribeTopicsCallWithDescribeTopicPartitionsApi`
/// (`KafkaAdminClient.java:2220-2325`):
///
/// - Every request names the topics not yet completed, sorted, with
///   `ResponsePartitionLimit = partitionSizeLimitPerResponse`. While a topic is
///   partially described, the request carries a cursor at that topic and its
///   next partition index.
/// - Each response completes the topics it finishes, fails the ones carrying a
///   topic error, and keeps the `NextCursor` topic as partially described. If
///   any topic is left, the call is issued again (`runnable.call(this, ..)`,
///   [`HandleResult::CallAgain`]), keeping its retry state.
/// - On `UnsupportedVersionException` it issues the Metadata-API call
///   ([`get_describe_topics_by_names_call`]) and fails itself without failing
///   the futures, which the Metadata call then completes.
///
/// `ctx` is the `runnable` Java's anonymous subclass closes over, and
/// `default_api_timeout_ms` the client field `calcDeadlineMs` reads.
fn generate_describe_topics_call_with_describe_topic_partitions_api(
    topic_names_list: Vec<String>,
    topic_futures: Arc<HashMap<String, KafkaFutureImpl<TopicDescription>>>,
    nodes: HashMap<i32, Node>,
    options: DescribeTopicsOptions,
    now: i64,
    ctx: DriverContext,
    default_api_timeout_ms: i32,
) -> Call {
    let timeout_ms = options.timeout_ms();
    let include_authorized_operations = options.include_authorized_operations();
    let partition_size_limit_per_response = options.partition_size_limit_per_response();
    let state = Arc::new(Mutex::new(DescribeTopicPartitionsState {
        topics_requests: topic_names_list.iter().cloned().collect(),
        partially_finished_topic_description: None,
        metadata_fallback_issued: false,
    }));

    let req_state = Arc::clone(&state);
    let create_request = Box::new(move |_timeout_ms: i32| {
        let state = req_state.lock().unwrap();
        let mut request = DescribeTopicPartitionsRequestData::new();
        request.set_topics(
            state
                .topics_requests
                .iter()
                .map(|topic_name| {
                    let mut topic = TopicRequest::new();
                    topic.set_name(topic_name.clone());
                    topic
                })
                .collect(),
        );
        request.set_response_partition_limit(partition_size_limit_per_response);
        if let Some(partially_finished) = &state.partially_finished_topic_description {
            // If the previous cursor points to partition 0, it will not be set
            // here. Instead, the previous cursor topic will be the first topic in
            // the request.
            let mut cursor = DescribeTopicPartitionsCursor::new();
            cursor
                .set_topic_name(partially_finished.name().to_string())
                .set_partition_index(partially_finished.partitions().len() as i32);
            request.set_cursor(Some(cursor));
        }
        Ok(Box::new(describe_topic_partitions_request::Builder::with_data(request)) as Box<dyn RequestBuilder>)
    });

    let resp_state = Arc::clone(&state);
    let resp_futures = Arc::clone(&topic_futures);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::DescribeTopicPartitions(response) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected a DescribeTopicPartitions response"));
        };
        let mut state = resp_state.lock().unwrap();
        let mut response_cursor = response.data().next_cursor.as_ref();
        // The topicDescription for the cursor topic of the current batch.
        let mut next_topic_description: Option<TopicDescription> = None;

        for topic in &response.data().topics {
            // Java looks the name up in `topicFutures` and dereferences the
            // result, so a topic the request did not name (or a null name)
            // would throw a `NullPointerException`. A broker only answers for
            // the topics it was asked about; such an entry is ignored here.
            let Some(topic_name) = topic.name.as_deref() else {
                continue;
            };
            let Some(future) = resp_futures.get(topic_name) else {
                continue;
            };
            let error = Errors::for_code(topic.error_code);
            if error != Errors::None {
                future.complete_with_error(Error::new(error));
                state.topics_requests.remove(topic_name);
                if response_cursor.is_some_and(|cursor| cursor.topic_name == topic_name) {
                    response_cursor = None;
                }
                continue;
            }

            let current_topic_description =
                topic_description_from_describe_topics_response_topic(topic, &nodes, include_authorized_operations);

            if let Some(partially_finished) = state.partially_finished_topic_description.as_mut()
                && partially_finished.name() == topic_name
            {
                // Add the partitions for the cursor topic of the previous batch.
                let mut partitions = partially_finished.partitions().to_vec();
                partitions.extend_from_slice(current_topic_description.partitions());
                *partially_finished = TopicDescription::with_authorized_operations_topic_id(
                    partially_finished.name(),
                    partially_finished.is_internal(),
                    partitions,
                    partially_finished.authorized_operations().cloned(),
                    partially_finished.topic_id(),
                );
                continue;
            }

            if response_cursor.is_some_and(|cursor| cursor.topic_name == topic_name) {
                // In the same batch of result, it may need to handle the
                // partitions for the previous cursor topic and the current
                // cursor topic. Cache the result in the nextTopicDescription.
                next_topic_description = Some(current_topic_description);
                continue;
            }

            state.topics_requests.remove(topic_name);
            future.complete(current_topic_description);
        }

        let finishes_partial = match (&state.partially_finished_topic_description, response_cursor) {
            (Some(partially_finished), Some(cursor)) => cursor.topic_name != partially_finished.name(),
            (Some(_), None) => true,
            (None, _) => false,
        };
        if finishes_partial {
            // We can't simply check nextTopicDescription != null here to close
            // the partiallyFinishedTopicDescription, because the responseCursor
            // topic may not show in the response.
            if let Some(partially_finished) = state.partially_finished_topic_description.take() {
                let topic_name = partially_finished.name().to_string();
                if let Some(future) = resp_futures.get(&topic_name) {
                    future.complete(partially_finished);
                }
                state.topics_requests.remove(&topic_name);
            }
        }
        if next_topic_description.is_some() {
            state.partially_finished_topic_description = next_topic_description;
        }

        if state.topics_requests.is_empty() {
            HandleResult::Done
        } else {
            HandleResult::CallAgain
        }
    });

    // `handleUnsupportedVersionException`'s body (`KafkaAdminClient.java:2312-2316`):
    // issue the Metadata-API call through `runnable.call`, and record that it was.
    let issue_metadata_fallback = {
        let state = Arc::clone(&state);
        let topic_futures = Arc::clone(&topic_futures);
        move || {
            state.lock().unwrap().metadata_fallback_issued = true;
            let now = ctx.time.milliseconds();
            ctx.call(get_describe_topics_by_names_call(
                Arc::clone(&topic_futures),
                topic_names_list.clone(),
                include_authorized_operations,
                calc_deadline_ms(now, timeout_ms, default_api_timeout_ms),
            ));
        }
    };
    let issue_metadata_fallback = Arc::new(issue_metadata_fallback);

    let fail_state = Arc::clone(&state);
    let fail_futures = Arc::clone(&topic_futures);
    let fail_issue_metadata_fallback = Arc::clone(&issue_metadata_fallback);
    let handle_failure = Box::new(move |error: &Error| {
        // An UnsupportedVersionException is not the user's failure: the
        // Metadata-API call issued by the hook below completes the futures
        // (`KafkaAdminClient.java:2319-2323`). Detected by code, because
        // `Error::unsupported_version` builds the generic code-35 error, as
        // `AdminClientRunnable::fail_call` does.
        if error.error() == Errors::UnsupportedVersion {
            // In Java this failure is only reached after
            // `handleUnsupportedVersionException` issued the fallback: `Call.fail`
            // skips that hook only once `runnable.closing` is set, which happens
            // when the I/O thread exits and no response is handled any more. Rust
            // sets `ShutdownSignal::closing` as soon as `close()` starts, so during
            // the close grace period `fail_call` comes straight here.
            // Issue the fallback then, as Java's hook would have: the closing gate
            // rejects it with "Cannot accept new calls when AdminClient is
            // closing.", which fails every future instead of leaving them pending.
            let issued = fail_state.lock().unwrap().metadata_fallback_issued;
            if !issued {
                fail_issue_metadata_fallback();
            }
            return;
        }
        for future in fail_futures.values() {
            future.complete_with_error(error.clone());
        }
    });

    let handle_uv = Box::new(move || {
        issue_metadata_fallback();
        false
    });

    Call::new(
        "describeTopicPartitions",
        calc_deadline_ms(now, timeout_ms, default_api_timeout_ms),
        NodeProvider::LeastLoaded,
        create_request,
        handle_response,
        handle_failure,
        handle_uv,
    )
}

/// Builds a [`TopicDescription`] from one `DescribeTopicPartitions` response
/// topic, resolving broker ids through `nodes`. The authorized operations are
/// `None` unless `include_authorized_operations` was requested.
///
/// Translated from `KafkaAdminClient.getTopicDescriptionFromDescribeTopicsResponseTopic`.
fn topic_description_from_describe_topics_response_topic(
    topic: &DescribeTopicPartitionsResponseTopic,
    nodes: &HashMap<i32, Node>,
    include_authorized_operations: bool,
) -> TopicDescription {
    let partitions = topic
        .partitions
        .iter()
        .map(|partition| DescribeTopicPartitionsResponse::partition_to_topic_partition_info(partition, nodes))
        .collect();
    let authorised_operations = if include_authorized_operations {
        AdminUtils::valid_acl_operations(topic.topic_authorized_operations)
    } else {
        None
    };
    TopicDescription::with_authorized_operations_topic_id(
        topic.name.clone().unwrap_or_default(),
        topic.is_internal,
        partitions,
        authorised_operations,
        topic.topic_id,
    )
}

/// Builds a `describeTopics` (by id) [`Call`] using the Metadata API.
/// Translated from `KafkaAdminClient.handleDescribeTopicsByIds`.
fn get_describe_topics_by_ids_call(
    futures: Arc<HashMap<Uuid, KafkaFutureImpl<TopicDescription>>>,
    ids: Vec<Uuid>,
    include_authorized_operations: bool,
    deadline: i64,
) -> Call {
    let req_ids = ids.clone();
    let create_request = Box::new(move |_timeout_ms: i32| {
        let mut data = crate::MetadataRequestData::new();
        data.set_topics(Some(
            crate::common::requests::MetadataRequest::convert_topic_ids_to_metadata_request_topic(&req_ids),
        ));
        data.set_allow_auto_topic_creation(false);
        data.set_include_topic_authorized_operations(include_authorized_operations);
        Ok(Box::new(metadata_request::Builder::with_data(data)) as Box<dyn RequestBuilder>)
    });

    let resp_futures = Arc::clone(&futures);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64, _cur_node: Option<&Node>| {
        let ConcreteResponse::Metadata(metadata_response) = response else {
            return HandleResult::Retry(Error::local_illegal_state("Expected a Metadata response"));
        };
        let cluster = metadata_response.build_cluster();
        // Java's `errorsByTopicId()` throws `IllegalStateException` on a zero topic
        // id, and `handleResponses` catches it with `call.fail(now, t)`
        // (`KafkaAdminClient.java:1394-1403`). `Retry` routes the error through
        // `fail_call` the same way; it is not retriable, so only this call fails.
        let errors = match metadata_response.errors_by_topic_id() {
            Ok(errors) => errors,
            Err(error) => return HandleResult::Retry(error),
        };
        for (topic_id, future) in resp_futures.iter() {
            let Some(topic_name) = cluster.topic_name(topic_id) else {
                future.complete_with_error(Error::with_message(
                    Errors::UnknownTopicId,
                    format!("TopicId {topic_id} not found."),
                ));
                continue;
            };
            let topic_name = topic_name.to_string();
            if let Some(topic_error) = errors.get(topic_id) {
                future.complete_with_error(Error::new(*topic_error));
                continue;
            }
            let authorized_operations = metadata_response
                .topic_authorized_operations(&topic_name)
                .unwrap_or(MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED);
            future.complete(topic_description_from_cluster(
                &cluster,
                &topic_name,
                *topic_id,
                authorized_operations,
            ));
        }
        HandleResult::Done
    });

    let fail_futures = Arc::clone(&futures);
    let handle_failure = Box::new(move |error: &Error| {
        for future in fail_futures.values() {
            future.complete_with_error(error.clone());
        }
    });

    Call::new(
        "describeTopicsWithIds",
        deadline,
        NodeProvider::LeastLoaded,
        create_request,
        handle_response,
        handle_failure,
        Box::new(|| false),
    )
}

#[cfg(test)]
impl KafkaAdminClient {
    /// Test-only constructor: wires the client over an arbitrary
    /// [`KafkaClient`] with a pre-seeded cluster and does **not** spawn the
    /// background task. The caller drives [`AdminClientRunnable::run_once`].
    ///
    /// Mirrors Java's `AdminClientUnitTestEnv`.
    pub(crate) fn create_for_test<C: KafkaClient + Send + 'static>(
        client: C,
        cluster: Cluster,
        config: &AdminClientConfig,
        time: Arc<dyn Time>,
    ) -> (Self, AdminClientRunnable<C>) {
        // Java's `AdminClientUnitTestEnv` goes through `createInternal`, which
        // resolves the id with `generateClientId` exactly like production.
        let client_id = Self::generate_client_id(config);
        let log_context = LogContext::new(format!("[AdminClient clientId={client_id}] "));
        let metadata_manager = AdminMetadataManager::new(
            config.retry_backoff_ms(),
            config.metadata_max_age_ms(),
            false,
            log_context.clone(),
        );
        metadata_manager.update(cluster, time.milliseconds());
        // Test-only: a bad `RETRY_BACKOFF_JITTER` constant is a build error in the
        // fixture, so panicking here is the right test behaviour.
        Self::build(client, metadata_manager, config, client_id, time, log_context)
            .expect("the admin retry backoff constants are valid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::mock_admin_client;
    use crate::common::utils::MockTime;
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    use crate::CreatePartitionsResponseData;
    use crate::CreateTopicsResponseData;
    use crate::DeleteRecordsResponseData;
    use crate::DeleteTopicsResponseData;
    use crate::MockClient;
    use crate::admin::MemberToRemove;
    use crate::admin::internals::AdminClientRunnable;
    use crate::common::Node;
    use crate::common::protocol::Errors;
    use crate::common::requests::RequestTestUtils;
    use crate::common::requests::metadata_response::{PartitionMetadata, TopicMetadata};
    use crate::common::requests::{CreatePartitionsResponse, DeleteRecordsResponse};
    use crate::common::requests::{CreateTopicsResponse, DeleteTopicsResponse};
    use crate::common::{TopicCollection, TopicPartition, Uuid};
    use crate::consumer::internals::ConsumerProtocol;
    use crate::create_partitions_response_data::CreatePartitionsTopicResult;
    use crate::create_topics_response_data::CreatableTopicResult;
    use crate::delete_records_response_data::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult};
    use crate::delete_topics_response_data::DeletableTopicResult;

    /// Mirrors Java `ApiError.messageWithFallback()`: the code's default text is
    /// used ONLY when the broker sent no message (null / `None`); a non-null
    /// empty message is returned verbatim, and any other message as-is.
    #[test]
    fn message_with_fallback_matches_java_apierror() {
        let code = Errors::InvalidTopicError.code();
        assert_eq!(
            KafkaAdminClient::message_with_fallback(code, &None),
            Errors::InvalidTopicError.message(),
            "null message falls back to the code's default text"
        );
        assert_eq!(
            KafkaAdminClient::message_with_fallback(code, &Some(String::new())),
            "",
            "a non-null empty message is returned verbatim, NOT the default"
        );
        assert_eq!(
            KafkaAdminClient::message_with_fallback(code, &Some("boom".to_string())),
            "boom",
            "a non-empty message is returned verbatim"
        );
    }

    /// Mirrors Java `Errors.exception(String)` (`Errors.java:462-469`), which
    /// `ApiError.exception()` delegates to: the code's default text is used ONLY
    /// when the broker sent `null`; a non-null empty message is used verbatim.
    #[test]
    fn api_error_matches_java_errors_message_fallback() {
        let code = Errors::NotController.code();
        let default_message = Errors::NotController.message();

        let absent = api_error(code, &None);
        assert_eq!(absent.error(), Errors::NotController);
        assert_eq!(
            absent.message(),
            default_message,
            "a null message falls back to the code's default text"
        );

        let empty = api_error(code, &Some(String::new()));
        assert_eq!(empty.error(), Errors::NotController, "the code survives an empty message");
        assert_eq!(
            empty.message(),
            "",
            "a non-null empty message is used verbatim, NOT the default"
        );

        let present = api_error(code, &Some("boom".to_string()));
        assert_eq!(present.error(), Errors::NotController);
        assert_eq!(present.message(), "boom");
    }

    /// Java's `new MockTime()` started at `initial_ms`, with a frozen monotonic clock.
    fn mock_time(initial_ms: i64) -> Arc<MockTime> {
        Arc::new(MockTime::with_auto_tick_ms_current_time_ms_current_high_res_time_ns(
            0, initial_ms, 0,
        ))
    }

    fn test_config() -> AdminClientConfig {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        // Bound the retry count so failure tests terminate.
        props.insert("retries".to_string(), "2".to_string());
        AdminClientConfig::new(&props).unwrap()
    }

    fn mock_cluster(num_nodes: i32, controller: i32) -> (Cluster, Vec<Node>) {
        let nodes: Vec<Node> = (0..num_nodes)
            .map(|i| Node::new(i, "localhost".to_string(), 9092 + i))
            .collect();
        let controller_node = nodes.iter().find(|n| n.id() == controller).cloned();
        let cluster = Cluster::with_invalid_topics_controller_topic_ids(
            Some("mock-cluster".to_string()),
            nodes.clone(),
            Vec::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            controller_node,
            HashMap::new(),
        );
        (cluster, nodes)
    }

    /// Builds a test environment mirroring Java's `AdminClientUnitTestEnv`.
    fn env() -> (KafkaAdminClient, AdminClientRunnable<MockClient>, Arc<MockTime>, Vec<Node>) {
        // Start at a non-zero time: MockClient's `not_throttled(0)` returns
        // false when `throttled_until_ms` is also 0, so a node never becomes
        // ready at t=0 (matching the producer's `SenderTest` which starts at
        // 1000 for the same reason).
        let time = mock_time(1000);
        let (cluster, nodes) = mock_cluster(3, 0);
        let client = MockClient::with_static_nodes(nodes.clone(), Arc::clone(&time) as Arc<dyn Time>);
        let config = test_config();
        let (admin, runnable) =
            KafkaAdminClient::create_for_test(client, cluster, &config, Arc::clone(&time) as Arc<dyn Time>);
        (admin, runnable, time, nodes)
    }

    /// Builds a test environment with extra config properties (e.g. a custom
    /// `default.api.timeout.ms` or `retry.backoff.ms`).
    fn env_with_props(
        extra: &[(&str, &str)],
    ) -> (KafkaAdminClient, AdminClientRunnable<MockClient>, Arc<MockTime>, Vec<Node>) {
        let time = mock_time(1000);
        let (cluster, nodes) = mock_cluster(3, 0);
        let client = MockClient::with_static_nodes(nodes.clone(), Arc::clone(&time) as Arc<dyn Time>);
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        for (k, v) in extra {
            props.insert((*k).to_string(), (*v).to_string());
        }
        let config = AdminClientConfig::new(&props).unwrap();
        let (admin, runnable) =
            KafkaAdminClient::create_for_test(client, cluster, &config, Arc::clone(&time) as Arc<dyn Time>);
        (admin, runnable, time, nodes)
    }

    /// Translated from `KafkaAdminClientTest.testGenerateClientId`.
    #[test]
    fn test_generate_client_id() {
        let conf = |client_id: &str| {
            let mut props = HashMap::new();
            props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
            props.insert("client.id".to_string(), client_id.to_string());
            AdminClientConfig::new(&props).unwrap()
        };
        let mut ids = HashSet::new();
        for _ in 0..10 {
            let id = KafkaAdminClient::generate_client_id(&conf(""));
            assert!(!ids.contains(&id), "Got duplicate id {id}");
            ids.insert(id);
        }
        assert_eq!("myCustomId", KafkaAdminClient::generate_client_id(&conf("myCustomId")));
    }

    /// `ConfigDef.parseType` trims `client.id`, so a blank one generates an id
    /// and a padded one is kept without its padding.
    #[test]
    fn test_generate_client_id_trims_client_id() {
        let conf = |client_id: &str| {
            let mut props = HashMap::new();
            props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
            props.insert("client.id".to_string(), client_id.to_string());
            AdminClientConfig::new(&props).unwrap()
        };
        let id = KafkaAdminClient::generate_client_id(&conf(" "));
        assert!(id.starts_with("adminclient-"), "unexpected generated id {id:?}");
        assert_eq!("myCustomId", KafkaAdminClient::generate_client_id(&conf(" myCustomId ")));
    }

    /// Beyond Java's test: the generated form is `adminclient-<n>`, and it is
    /// what the client carries (`Shared.client_id`), not the empty config
    /// value. Mirrors Java's `testMetricsReporterAutoGeneratedClientId`
    /// asserting the resolved id on the constructed client.
    #[test]
    fn test_generated_client_id_is_used_by_the_client() {
        let (admin, _runnable, _time, _nodes) = env();
        let id = admin.shared.client_id.clone();
        let n = id
            .strip_prefix("adminclient-")
            .unwrap_or_else(|| panic!("unexpected generated id {id}"));
        assert!(n.parse::<i32>().unwrap() >= 1, "unexpected generated id {id}");

        let (admin, _runnable, _time, _nodes) = env_with_props(&[("client.id", "myCustomId")]);
        assert_eq!(admin.shared.client_id, "myCustomId");
    }

    /// Like [`env_with_props`], but with a configurable broker count (mirrors
    /// Java's `mockCluster(numNodes, 0)`). Used by the group-listing broker
    /// enumeration tests that want a single broker.
    fn env_nodes_with_props(
        num_nodes: i32,
        extra: &[(&str, &str)],
    ) -> (KafkaAdminClient, AdminClientRunnable<MockClient>, Arc<MockTime>, Vec<Node>) {
        let time = mock_time(1000);
        let (cluster, nodes) = mock_cluster(num_nodes, 0);
        let client = MockClient::with_static_nodes(nodes.clone(), Arc::clone(&time) as Arc<dyn Time>);
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        for (k, v) in extra {
            props.insert((*k).to_string(), (*v).to_string());
        }
        let config = AdminClientConfig::new(&props).unwrap();
        let (admin, runnable) =
            KafkaAdminClient::create_for_test(client, cluster, &config, Arc::clone(&time) as Arc<dyn Time>);
        (admin, runnable, time, nodes)
    }

    async fn pump(runnable: &mut AdminClientRunnable<MockClient>, iters: usize) {
        for _ in 0..iters {
            runnable.run_once().await;
        }
    }

    /// Pumps `run_once` until at least one request is queued (sent but not yet
    /// responded), so a test can inspect the emitted wire request.
    async fn pump_until_request_queued(runnable: &mut AdminClientRunnable<MockClient>) {
        for _ in 0..40 {
            if runnable.client_mut().request_count() >= 1 {
                return;
            }
            runnable.run_once().await;
        }
        panic!("no request was queued after pumping");
    }

    /// Builds a multi-group `ListGroups` response.
    fn listed_groups(groups: &[(&str, &str, &str, &str)]) -> ConcreteResponse {
        use crate::ListGroupsResponseData;
        use crate::list_groups_response_data::ListedGroup;
        let wire: Vec<ListedGroup> = groups
            .iter()
            .map(|(id, protocol_type, state, group_type)| {
                let mut g = ListedGroup::new();
                g.set_group_id((*id).to_string())
                    .set_protocol_type((*protocol_type).to_string())
                    .set_group_state((*state).to_string())
                    .set_group_type((*group_type).to_string());
                g
            })
            .collect();
        let mut data = ListGroupsResponseData::new();
        data.set_groups(wire);
        ConcreteResponse::ListGroups(crate::common::requests::ListGroupsResponse::new(data))
    }

    fn create_result(name: &str, error: Errors, error_message: Option<&str>) -> CreatableTopicResult {
        let mut r = CreatableTopicResult::new();
        r.set_name(name.to_string());
        r.set_error_code(error.code());
        r.set_error_message(error_message.map(str::to_string));
        r.set_topic_id(Uuid::new(0, 7));
        r.set_num_partitions(1);
        r.set_replication_factor(1);
        r
    }

    fn create_response(results: Vec<CreatableTopicResult>) -> ConcreteResponse {
        create_response_throttled(0, results)
    }

    fn create_response_throttled(throttle_ms: i32, results: Vec<CreatableTopicResult>) -> ConcreteResponse {
        let mut data = CreateTopicsResponseData::new();
        data.set_throttle_time_ms(throttle_ms);
        data.set_topics(results);
        ConcreteResponse::CreateTopics(CreateTopicsResponse::new(data))
    }

    fn delete_result_named(name: &str, error: Errors) -> DeletableTopicResult {
        let mut r = DeletableTopicResult::new();
        r.set_name(Some(name.to_string()));
        r.set_error_code(error.code());
        r
    }

    fn delete_result_with_id(id: Uuid, error: Errors) -> DeletableTopicResult {
        let mut r = DeletableTopicResult::new();
        r.set_topic_id(id);
        r.set_error_code(error.code());
        r
    }

    fn delete_response(results: Vec<DeletableTopicResult>) -> ConcreteResponse {
        delete_response_throttled(0, results)
    }

    fn delete_response_throttled(throttle_ms: i32, results: Vec<DeletableTopicResult>) -> ConcreteResponse {
        let mut data = DeleteTopicsResponseData::new();
        data.set_throttle_time_ms(throttle_ms);
        data.set_responses(results);
        ConcreteResponse::DeleteTopics(DeleteTopicsResponse::new(data))
    }

    /// Pumps `run_once` until `done` returns true or `max_iters` is reached,
    /// mirroring Java's `TestUtils.waitForCondition` over the driven runnable.
    async fn pump_until(
        runnable: &mut AdminClientRunnable<MockClient>,
        max_iters: usize,
        mut done: impl FnMut(&mut AdminClientRunnable<MockClient>) -> bool,
    ) {
        for _ in 0..max_iters {
            if done(runnable) {
                return;
            }
            runnable.run_once().await;
        }
    }

    fn topic_meta(name: &str, internal: bool, id: Uuid, partitions: i32) -> TopicMetadata {
        let mut partition_metadata = Vec::new();
        for p in 0..partitions {
            partition_metadata.push(PartitionMetadata {
                error: Errors::None,
                topic_partition: TopicPartition::new(name.to_string(), p),
                leader_id: Some(0),
                leader_epoch: Some(0),
                replica_ids: vec![0],
                in_sync_replica_ids: vec![0],
                offline_replica_ids: vec![],
            });
        }
        TopicMetadata {
            error: Errors::None,
            topic: name.to_string(),
            topic_id: id,
            is_internal: internal,
            partition_metadata,
            authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
        }
    }

    // --- ACLs (createAcls / describeAcls / deleteAcls) -----------------------

    use crate::CreateAclsResponseData;
    use crate::DeleteAclsResponseData;
    use crate::DescribeAclsResponseData;
    use crate::common::acl::{AccessControlEntry, AccessControlEntryFilter, AclPermissionType};
    use crate::common::protocol::ApiKeys;
    use crate::common::requests::{CreateAclsResponse, DeleteAclsResponse, DescribeAclsResponse};
    use crate::common::resource::{PatternType, ResourcePattern, ResourcePatternFilter, ResourceType};
    use crate::create_acls_response_data::AclCreationResult;
    use crate::delete_acls_response_data::DeleteAclsFilterResult;

    fn acl1() -> AclBinding {
        AclBinding::new(
            ResourcePattern::new(ResourceType::Topic, "mytopic3", PatternType::Literal).unwrap(),
            AccessControlEntry::new("User:ANONYMOUS", "*", AclOperation::Describe, AclPermissionType::Allow).unwrap(),
        )
    }

    fn acl2() -> AclBinding {
        AclBinding::new(
            ResourcePattern::new(ResourceType::Topic, "mytopic4", PatternType::Literal).unwrap(),
            AccessControlEntry::new("User:ANONYMOUS", "*", AclOperation::Describe, AclPermissionType::Deny).unwrap(),
        )
    }

    fn filter1() -> AclBindingFilter {
        AclBindingFilter::new(
            ResourcePatternFilter::new(ResourceType::Any, None, PatternType::Literal),
            AccessControlEntryFilter::new(
                Some("User:ANONYMOUS".to_string()),
                None,
                AclOperation::Any,
                AclPermissionType::Any,
            ),
        )
    }

    fn filter2() -> AclBindingFilter {
        AclBindingFilter::new(
            ResourcePatternFilter::new(ResourceType::Any, None, PatternType::Literal),
            AccessControlEntryFilter::new(
                Some("User:bob".to_string()),
                None,
                AclOperation::Any,
                AclPermissionType::Any,
            ),
        )
    }

    fn unknown_filter() -> AclBindingFilter {
        AclBindingFilter::new(
            ResourcePatternFilter::new(ResourceType::Unknown, None, PatternType::Literal),
            AccessControlEntryFilter::new(
                Some("User:bob".to_string()),
                None,
                AclOperation::Any,
                AclPermissionType::Any,
            ),
        )
    }

    fn create_acls_result_ok() -> AclCreationResult {
        AclCreationResult::new()
    }

    fn create_acls_result_error(error: Errors, message: &str) -> AclCreationResult {
        let mut r = AclCreationResult::new();
        r.set_error_code(error.code());
        r.set_error_message(Some(message.to_string()));
        r
    }

    fn create_acls_response(results: Vec<AclCreationResult>) -> ConcreteResponse {
        let mut data = CreateAclsResponseData::new();
        data.set_results(results);
        ConcreteResponse::CreateAcls(CreateAclsResponse::new(data))
    }

    fn describe_acls_response(resources_from: &[AclBinding]) -> ConcreteResponse {
        let mut data = DescribeAclsResponseData::new();
        data.set_resources(DescribeAclsResponse::acls_resources(resources_from));
        ConcreteResponse::DescribeAcls(DescribeAclsResponse::new(data, ApiKeys::DESCRIBE_ACLS.latest_version()))
    }

    fn describe_acls_error_response(error: Errors, message: &str) -> ConcreteResponse {
        let mut data = DescribeAclsResponseData::new();
        data.set_error_code(error.code());
        data.set_error_message(Some(message.to_string()));
        ConcreteResponse::DescribeAcls(DescribeAclsResponse::new(data, ApiKeys::DESCRIBE_ACLS.latest_version()))
    }

    fn delete_acls_response(filter_results: Vec<DeleteAclsFilterResult>) -> ConcreteResponse {
        let mut data = DeleteAclsResponseData::new();
        data.set_throttle_time_ms(0);
        data.set_filter_results(filter_results);
        ConcreteResponse::DeleteAcls(DeleteAclsResponse::new(data, ApiKeys::DELETE_ACLS.latest_version()))
    }

    /// Translated from `KafkaAdminClientTest.testDescribeAcls`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeAcls")]
    async fn test_describe_acls() {
        let (admin, mut runnable, _time, _nodes) = env();

        // Test a call where we get back ACL1 and ACL2.
        runnable
            .client_mut()
            .prepare_response(describe_acls_response(&[acl1(), acl2()]));
        let result = admin.describe_acls_with_options(&filter1(), DescribeAclsOptions::new());
        pump(&mut runnable, 5).await;
        let mut acls = result.values().get().await.unwrap();
        acls.sort_by(|a, b| a.pattern().name().cmp(b.pattern().name()));
        assert_eq!(acls, vec![acl1(), acl2()]);

        // Test a call where we get back no results.
        runnable.client_mut().prepare_response(describe_acls_response(&[]));
        let result = admin.describe_acls_with_options(&filter2(), DescribeAclsOptions::new());
        pump(&mut runnable, 5).await;
        assert!(result.values().get().await.unwrap().is_empty());

        // Test a call where we get back an error.
        runnable
            .client_mut()
            .prepare_response(describe_acls_error_response(Errors::SecurityDisabled, "Security is disabled"));
        let result = admin.describe_acls_with_options(&filter2(), DescribeAclsOptions::new());
        pump(&mut runnable, 5).await;
        let err = result.values().get().await.unwrap_err();
        assert_eq!(err.error(), Errors::SecurityDisabled);

        // Test a call where we supply an invalid filter: completes exceptionally
        // with InvalidRequest and enqueues NO network call.
        let before = runnable.client_mut().request_count();
        let result = admin.describe_acls_with_options(&unknown_filter(), DescribeAclsOptions::new());
        assert!(result.values().is_done());
        pump(&mut runnable, 5).await;
        assert_eq!(
            runnable.client_mut().request_count(),
            before,
            "unknown filter must not enqueue a Call"
        );
        let err = result.values().get().await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidRequest);
    }

    /// Translated from `KafkaAdminClientTest.testCreateAcls`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testCreateAcls")]
    async fn test_create_acls() {
        let (admin, mut runnable, _time, _nodes) = env();

        // Test a call where we successfully create two ACLs.
        runnable
            .client_mut()
            .prepare_response(create_acls_response(vec![create_acls_result_ok(), create_acls_result_ok()]));
        let results = admin.create_acls_with_options(&[acl1(), acl2()], CreateAclsOptions::new());
        let keys: HashSet<AclBinding> = results.values().keys().cloned().collect();
        assert_eq!(keys, HashSet::from([acl1(), acl2()]));
        pump(&mut runnable, 5).await;
        for future in results.values().values() {
            future.get().await.unwrap();
        }
        results.all().get().await.unwrap();

        // Test a call where we fail to create one ACL.
        runnable.client_mut().prepare_response(create_acls_response(vec![
            create_acls_result_error(Errors::SecurityDisabled, "Security is disabled"),
            create_acls_result_ok(),
        ]));
        let results = admin.create_acls_with_options(&[acl1(), acl2()], CreateAclsOptions::new());
        pump(&mut runnable, 5).await;
        assert_eq!(
            results.values()[&acl1()].get().await.unwrap_err().error(),
            Errors::SecurityDisabled
        );
        results.values()[&acl2()].get().await.unwrap();
        assert_eq!(results.all().get().await.unwrap_err().error(), Errors::SecurityDisabled);
    }

    /// Translated from `KafkaAdminClientTest.testCreateAclsToController`.
    ///
    /// Java sets `bootstrap.controllers`, which makes
    /// `LeastLoadedBrokerOrActiveKController` route to the active controller and
    /// refresh metadata via `DescribeCluster` between the NOT_CONTROLLER attempt
    /// and the retry. On this branch `AdminMetadataManager` stubs
    /// `using_bootstrap_controllers()` to `false`, so the provider behaves like
    /// `LeastLoaded`: after clearing the controller it retries straight to a
    /// least-loaded broker (no interposed metadata call). The observable
    /// contract — retry after NOT_CONTROLLER, then success — is preserved.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testCreateAclsToController")]
    async fn test_create_acls_to_controller() {
        let (admin, mut runnable, time, _nodes) = env();
        runnable
            .client_mut()
            .prepare_response(create_acls_response(vec![create_acls_result_error(
                Errors::NotController,
                "not controller",
            )]));
        runnable
            .client_mut()
            .prepare_response(create_acls_response(vec![create_acls_result_ok()]));

        let results = admin.create_acls_with_options(&[acl1()], CreateAclsOptions::new());
        let keys: HashSet<AclBinding> = results.values().keys().cloned().collect();
        assert_eq!(keys, HashSet::from([acl1()]));
        for _ in 0..30 {
            if results.values()[&acl1()].is_done() {
                break;
            }
            runnable.run_once().await;
            time.sleep(200);
        }
        for future in results.values().values() {
            future.get().await.unwrap();
        }
        results.all().get().await.unwrap();
    }

    /// Translated from `KafkaAdminClientTest.testDeleteAcls`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDeleteAcls")]
    async fn test_delete_acls() {
        let (admin, mut runnable, _time, _nodes) = env();

        // Test a call where one filter has an error.
        let mut filter1_result = DeleteAclsFilterResult::new();
        filter1_result.set_matching_acls(vec![
            DeleteAclsResponse::matching_acl(&acl1(), Errors::None, None),
            DeleteAclsResponse::matching_acl(&acl2(), Errors::None, None),
        ]);
        let mut filter2_result = DeleteAclsFilterResult::new();
        filter2_result.set_error_code(Errors::SecurityDisabled.code());
        filter2_result.set_error_message(Some("No security".to_string()));
        runnable
            .client_mut()
            .prepare_response(delete_acls_response(vec![filter1_result, filter2_result]));
        let results = admin.delete_acls_with_options(&[filter1(), filter2()], DeleteAclsOptions::new());
        pump(&mut runnable, 5).await;
        let filter1_results = results.values()[&filter1()].get().await.unwrap();
        assert!(filter1_results.values()[0].error().is_none());
        assert_eq!(filter1_results.values()[0].binding(), Some(&acl1()));
        assert!(filter1_results.values()[1].error().is_none());
        assert_eq!(filter1_results.values()[1].binding(), Some(&acl2()));
        assert_eq!(
            results.values()[&filter2()].get().await.unwrap_err().error(),
            Errors::SecurityDisabled
        );
        assert_eq!(results.all().get().await.unwrap_err().error(), Errors::SecurityDisabled);

        // Test a call where one deletion result has an error.
        let mut err_matching = crate::delete_acls_response_data::DeleteAclsMatchingAcl::new();
        err_matching
            .set_error_code(Errors::SecurityDisabled.code())
            .set_error_message(Some("No security".to_string()))
            .set_permission_type(AclPermissionType::Allow.code())
            .set_operation(AclOperation::Alter.code())
            .set_resource_type(ResourceType::Cluster.code())
            .set_pattern_type(filter2().pattern_filter().pattern_type().code());
        let mut filter1_result = DeleteAclsFilterResult::new();
        filter1_result.set_matching_acls(vec![
            DeleteAclsResponse::matching_acl(&acl1(), Errors::None, None),
            err_matching,
        ]);
        let filter2_result = DeleteAclsFilterResult::new();
        runnable
            .client_mut()
            .prepare_response(delete_acls_response(vec![filter1_result, filter2_result]));
        let results = admin.delete_acls_with_options(&[filter1(), filter2()], DeleteAclsOptions::new());
        pump(&mut runnable, 5).await;
        assert!(results.values()[&filter2()].get().await.unwrap().values().is_empty());
        assert_eq!(results.all().get().await.unwrap_err().error(), Errors::SecurityDisabled);

        // Test a call where there are no errors.
        let mut f1 = DeleteAclsFilterResult::new();
        f1.set_matching_acls(vec![DeleteAclsResponse::matching_acl(&acl1(), Errors::None, None)]);
        let mut f2 = DeleteAclsFilterResult::new();
        f2.set_matching_acls(vec![DeleteAclsResponse::matching_acl(&acl2(), Errors::None, None)]);
        runnable.client_mut().prepare_response(delete_acls_response(vec![f1, f2]));
        let results = admin.delete_acls_with_options(&[filter1(), filter2()], DeleteAclsOptions::new());
        pump(&mut runnable, 5).await;
        let mut deleted = results.all().get().await.unwrap();
        deleted.sort_by(|a, b| a.pattern().name().cmp(b.pattern().name()));
        assert_eq!(deleted, vec![acl1(), acl2()]);
    }

    /// Translated from `KafkaAdminClientTest.testDeleteAclsToController`.
    ///
    /// As with `test_create_acls_to_controller`, `bootstrap.controllers` is
    /// stubbed off on this branch so the NOT_CONTROLLER retry goes straight to a
    /// least-loaded broker without an interposed metadata refresh.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDeleteAclsToController")]
    async fn test_delete_acls_to_controller() {
        let (admin, mut runnable, time, _nodes) = env();
        let mut not_controller = DeleteAclsFilterResult::new();
        not_controller.set_error_code(Errors::NotController.code());
        not_controller.set_error_message(Some("not controller".to_string()));
        runnable
            .client_mut()
            .prepare_response(delete_acls_response(vec![not_controller]));
        let mut ok = DeleteAclsFilterResult::new();
        ok.set_matching_acls(vec![DeleteAclsResponse::matching_acl(&acl1(), Errors::None, None)]);
        runnable.client_mut().prepare_response(delete_acls_response(vec![ok]));

        let results = admin.delete_acls_with_options(&[filter1()], DeleteAclsOptions::new());
        for _ in 0..30 {
            if results.values()[&filter1()].is_done() {
                break;
            }
            runnable.run_once().await;
            time.sleep(200);
        }
        let deleted = results.all().get().await.unwrap();
        assert_eq!(deleted, vec![acl1()]);
    }

    /// Behavior test: a binding with an indefinite (UNKNOWN) field fails only its
    /// own future; other valid bindings in the same batch still succeed.
    /// Mirrors `KafkaAdminClient.createAcls`' per-binding `findIndefiniteField`
    /// rejection.
    #[tokio::test]
    async fn test_create_acls_rejects_indefinite_binding_per_binding() {
        let (admin, mut runnable, _time, _nodes) = env();
        let bad = AclBinding::new(
            ResourcePattern::new(ResourceType::Topic, "mytopic3", PatternType::Literal).unwrap(),
            AccessControlEntry::new("User:ANONYMOUS", "*", AclOperation::Unknown, AclPermissionType::Allow).unwrap(),
        );
        // Only the valid binding is sent, so a single-result response suffices.
        runnable
            .client_mut()
            .prepare_response(create_acls_response(vec![create_acls_result_ok()]));
        let results = admin.create_acls_with_options(&[acl1(), bad.clone()], CreateAclsOptions::new());
        pump(&mut runnable, 5).await;
        results.values()[&acl1()].get().await.unwrap();
        let err = results.values()[&bad].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidRequest);
        assert!(err.message().contains("Invalid ACL creation"));
    }

    // --- client quotas (describeClientQuotas / alterClientQuotas) ------------

    use crate::common::quota::{ClientQuotaFilterComponent, Op};
    use crate::common::requests::{AlterClientQuotasResponse, DescribeClientQuotasResponse};

    /// Mirrors `KafkaAdminClientTest.newClientQuotaEntity(String...)`.
    fn new_client_quota_entity(args: &[&str]) -> ClientQuotaEntity {
        assert_eq!(args.len() % 2, 0);
        let mut entity_map = HashMap::new();
        let mut index = 0;
        while index < args.len() {
            entity_map.insert(args[index].to_string(), Some(args[index + 1].to_string()));
            index += 2;
        }
        ClientQuotaEntity::new(entity_map)
    }

    /// Translated from `KafkaAdminClientTest.testDescribeClientQuotas`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeClientQuotas")]
    async fn test_describe_client_quotas() {
        let (admin, mut runnable, _time, _nodes) = env();

        let value = "value";
        let entity1 =
            new_client_quota_entity(&[ClientQuotaEntity::USER, "user-1", ClientQuotaEntity::CLIENT_ID, value]);
        let entity2 =
            new_client_quota_entity(&[ClientQuotaEntity::USER, "user-2", ClientQuotaEntity::CLIENT_ID, value]);
        let mut response_data = HashMap::new();
        response_data.insert(entity1.clone(), HashMap::from([("consumer_byte_rate".to_string(), 10000.0)]));
        response_data.insert(entity2.clone(), HashMap::from([("producer_byte_rate".to_string(), 20000.0)]));

        runnable.client_mut().prepare_response(ConcreteResponse::DescribeClientQuotas(
            DescribeClientQuotasResponse::from_quota_entities(&response_data, 0),
        ));

        let filter =
            ClientQuotaFilter::contains(vec![ClientQuotaFilterComponent::of_entity(ClientQuotaEntity::USER, value)]);
        let result = admin.describe_client_quotas_with_options(&filter, DescribeClientQuotasOptions::new());
        pump(&mut runnable, 5).await;

        let result_data = result.entities().get().await.unwrap();
        assert_eq!(result_data.len(), 2);
        assert!(result_data.contains_key(&entity1));
        let config1 = &result_data[&entity1];
        assert_eq!(config1.len(), 1);
        assert!((config1["consumer_byte_rate"] - 10000.0).abs() < 1e-6);
        assert!(result_data.contains_key(&entity2));
        let config2 = &result_data[&entity2];
        assert_eq!(config2.len(), 1);
        assert!((config2["producer_byte_rate"] - 20000.0).abs() < 1e-6);
    }

    /// Translated from `KafkaAdminClientTest.testAlterClientQuotas`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testAlterClientQuotas")]
    async fn test_alter_client_quotas() {
        let (admin, mut runnable, _time, _nodes) = env();

        let good_entity = new_client_quota_entity(&[ClientQuotaEntity::USER, "user-1"]);
        let unauthorized_entity = new_client_quota_entity(&[ClientQuotaEntity::USER, "user-0"]);
        let invalid_entity = new_client_quota_entity(&["", "user-0"]);

        let response_data = vec![
            (
                good_entity.clone(),
                Errors::ClusterAuthorizationFailed,
                Some("Authorization failed".to_string()),
            ),
            (
                unauthorized_entity.clone(),
                Errors::ClusterAuthorizationFailed,
                Some("Authorization failed".to_string()),
            ),
            (
                invalid_entity.clone(),
                Errors::InvalidRequest,
                Some("Invalid quota entity".to_string()),
            ),
        ];
        runnable.client_mut().prepare_response(ConcreteResponse::AlterClientQuotas(
            AlterClientQuotasResponse::from_quota_entities(&response_data, 0),
        ));

        let entries = vec![
            ClientQuotaAlteration::new(good_entity.clone(), vec![Op::new("consumer_byte_rate", Some(10000.0))]),
            ClientQuotaAlteration::new(unauthorized_entity.clone(), vec![Op::new("producer_byte_rate", Some(10000.0))]),
            ClientQuotaAlteration::new(invalid_entity.clone(), vec![Op::new("producer_byte_rate", Some(100.0))]),
        ];
        let result = admin.alter_client_quotas_with_options(&entries, AlterClientQuotasOptions::new());
        pump(&mut runnable, 5).await;

        // good_entity got CLUSTER_AUTHORIZATION_FAILED in this response fixture.
        assert_eq!(
            result.values()[&good_entity].get().await.unwrap_err().error(),
            Errors::ClusterAuthorizationFailed
        );
        assert_eq!(
            result.values()[&unauthorized_entity].get().await.unwrap_err().error(),
            Errors::ClusterAuthorizationFailed
        );
        assert_eq!(
            result.values()[&invalid_entity].get().await.unwrap_err().error(),
            Errors::InvalidRequest
        );
    }

    // --- user SCRAM credentials ----------------------------------------------
    // (describeUserScramCredentials / alterUserScramCredentials)

    use crate::AlterUserScramCredentialsResponseData;
    use crate::admin::{
        AlterUserScramCredentialsOptions, DescribeUserScramCredentialsOptions, ScramCredentialInfo,
        ScramMechanism as PublicScramMechanism, UserScramCredentialAlteration, UserScramCredentialDeletion,
        UserScramCredentialUpsertion,
    };
    use crate::alter_user_scram_credentials_response_data::AlterUserScramCredentialsResult as WireAlterResult;
    use crate::common::requests::{AlterUserScramCredentialsResponse, DescribeUserScramCredentialsResponse};
    use crate::describe_user_scram_credentials_response_data::CredentialInfo as WireCredentialInfo;
    use crate::describe_user_scram_credentials_response_data::DescribeUserScramCredentialsResult as WireDescribeResult;

    /// Translated from `KafkaAdminClientTest.testDescribeUserScramCredentials`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeUserScramCredentials")]
    async fn test_describe_user_scram_credentials() {
        let user0_name = "user0";
        let user0_mechanism0 = PublicScramMechanism::ScramSha256;
        let user0_iterations0 = 4096;
        let user0_mechanism1 = PublicScramMechanism::ScramSha512;
        let user0_iterations1 = 8192;

        let user1_name = "user1";
        let user1_mechanism = PublicScramMechanism::ScramSha256;
        let user1_iterations = 4096;

        let mut ci0 = WireCredentialInfo::new();
        ci0.set_mechanism(user0_mechanism0.r#type()).set_iterations(user0_iterations0);
        let mut ci1 = WireCredentialInfo::new();
        ci1.set_mechanism(user0_mechanism1.r#type()).set_iterations(user0_iterations1);
        let mut ci_user1 = WireCredentialInfo::new();
        ci_user1
            .set_mechanism(user1_mechanism.r#type())
            .set_iterations(user1_iterations);

        let mut r0 = WireDescribeResult::new();
        r0.set_user(user0_name.to_string()).set_credential_infos(vec![ci0, ci1]);
        let mut r1 = WireDescribeResult::new();
        r1.set_user(user1_name.to_string()).set_credential_infos(vec![ci_user1]);
        let mut response_data = DescribeUserScramCredentialsResponseData::new();
        response_data.set_results(vec![r0, r1]);

        let users_requested: HashSet<String> = [user0_name.to_string(), user1_name.to_string()].into_iter().collect();

        // Mirrors the Java loop over [null, empty, [user0, null, user1]]. Rust
        // has no null in a `&[String]`, so the equivalent inputs are: empty,
        // empty, and the explicit two-user list.
        for users in [
            Vec::<String>::new(),
            Vec::new(),
            vec![user0_name.to_string(), user1_name.to_string()],
        ] {
            let (admin, mut runnable, _time, _nodes) = env();
            runnable
                .client_mut()
                .prepare_response(ConcreteResponse::DescribeUserScramCredentials(
                    DescribeUserScramCredentialsResponse::new(response_data.clone(), 0),
                ));

            let result = admin
                .describe_user_scram_credentials_with_users_options(&users, DescribeUserScramCredentialsOptions::new());
            let user0_desc_future = result.description(user0_name);
            let user1_desc_future = result.description(user1_name);
            pump(&mut runnable, 5).await;

            let description_results = result.all().get().await.unwrap();
            let users_described: HashSet<String> = result.users().get().await.unwrap().into_iter().collect();
            assert_eq!(users_requested, users_described);
            assert_eq!(users_requested, description_results.keys().cloned().collect::<HashSet<_>>());

            let desc0 = &description_results[user0_name];
            assert_eq!(desc0.name(), user0_name);
            assert_eq!(desc0.credential_infos().len(), 2);
            assert_eq!(desc0.credential_infos()[0].mechanism(), user0_mechanism0);
            assert_eq!(desc0.credential_infos()[0].iterations(), user0_iterations0);
            assert_eq!(desc0.credential_infos()[1].mechanism(), user0_mechanism1);
            assert_eq!(desc0.credential_infos()[1].iterations(), user0_iterations1);
            assert_eq!(*desc0, user0_desc_future.get().await.unwrap());

            let desc1 = &description_results[user1_name];
            assert_eq!(desc1.name(), user1_name);
            assert_eq!(desc1.credential_infos().len(), 1);
            assert_eq!(desc1.credential_infos()[0].mechanism(), user1_mechanism);
            assert_eq!(desc1.credential_infos()[0].iterations(), user1_iterations);
            assert_eq!(*desc1, user1_desc_future.get().await.unwrap());
        }
    }

    /// Translated from
    /// `KafkaAdminClientTest.testAlterUserScramCredentialsUnknownMechanism`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testAlterUserScramCredentialsUnknownMechanism")]
    async fn test_alter_user_scram_credentials_unknown_mechanism() {
        let (admin, mut runnable, _time, _nodes) = env();

        let user0_name = "user0";
        let user0_mechanism = PublicScramMechanism::Unknown;
        let user1_name = "user1";
        let user1_mechanism = PublicScramMechanism::Unknown;
        let user2_name = "user2";
        let user2_mechanism = PublicScramMechanism::ScramSha256;

        let mut result2 = WireAlterResult::new();
        result2.set_user(user2_name.to_string());
        let mut response_data = AlterUserScramCredentialsResponseData::new();
        response_data.set_results(vec![result2]);
        runnable
            .client_mut()
            .prepare_response(ConcreteResponse::AlterUserScramCredentials(
                AlterUserScramCredentialsResponse::new(response_data, 0),
            ));

        let alterations: Vec<UserScramCredentialAlteration> = vec![
            UserScramCredentialDeletion::new(user0_name, user0_mechanism).into(),
            UserScramCredentialUpsertion::with_str(
                user1_name,
                ScramCredentialInfo::new(user1_mechanism, 8192),
                "password",
            )
            .into(),
            UserScramCredentialUpsertion::with_str(
                user2_name,
                ScramCredentialInfo::new(user2_mechanism, 4096),
                "password",
            )
            .into(),
        ];
        let result =
            admin.alter_user_scram_credentials_with_options(&alterations, AlterUserScramCredentialsOptions::new());
        pump(&mut runnable, 5).await;

        let result_data = result.values();
        assert_eq!(result_data.len(), 3);
        // user0 and user1 have an unknown mechanism -> complete exceptionally.
        for user in [user0_name, user1_name] {
            assert!(result_data.contains_key(user));
            assert!(
                result_data[user].get().await.is_err(),
                "expected request for user {user} to complete with an error"
            );
        }
        assert!(result_data.contains_key(user2_name));
        result_data[user2_name].get().await.unwrap();

        assert!(
            result.all().get().await.is_err(),
            "expected all() to fail since at least one user failed"
        );
    }

    /// Java records an empty upsertion password against **that user only** —
    /// `userIllegalAlterationExceptions.put(user, new
    /// UnacceptableCredentialException(passwordMustNotBeEmptyMsg))`
    /// (`KafkaAdminClient.java:4414-4416`) — and still builds and sends every
    /// other user's alteration. No Java test covers that branch, but the C and
    /// Python bindings depend on it: their marshaling layer deliberately passes
    /// an empty password through rather than failing the whole call.
    #[tokio::test]
    async fn test_alter_user_scram_credentials_empty_password_fails_only_that_user() {
        let (admin, mut runnable, _time, _nodes) = env();

        let mut result1 = WireAlterResult::new();
        result1.set_user("user1".to_string()).set_error_code(Errors::None.code());
        let mut response_data = AlterUserScramCredentialsResponseData::new();
        response_data.set_results(vec![result1]);
        runnable
            .client_mut()
            .prepare_response(ConcreteResponse::AlterUserScramCredentials(
                AlterUserScramCredentialsResponse::new(response_data, 0),
            ));

        let alterations: Vec<UserScramCredentialAlteration> = vec![
            UserScramCredentialUpsertion::with_bytes(
                "user0",
                ScramCredentialInfo::new(PublicScramMechanism::ScramSha256, 4096),
                Vec::new(),
            )
            .into(),
            UserScramCredentialUpsertion::with_str(
                "user1",
                ScramCredentialInfo::new(PublicScramMechanism::ScramSha512, 8192),
                "password",
            )
            .into(),
        ];
        let result =
            admin.alter_user_scram_credentials_with_options(&alterations, AlterUserScramCredentialsOptions::new());
        pump(&mut runnable, 5).await;

        let result_data = result.values();
        assert_eq!(result_data.len(), 2);
        let err = result_data["user0"].get().await.unwrap_err();
        assert_eq!(err.message(), "Password must not be empty");
        // user1's alteration was still built and sent, and succeeded.
        result_data["user1"].get().await.unwrap();
    }

    /// Translated from `KafkaAdminClientTest.testAlterUserScramCredentials`.
    ///
    /// The Java test has no `throws` and runs real PBKDF2 synchronously; the
    /// Rust equivalent exercises the same crypto path (each upsertion computes a
    /// salted password via `ScramFormatter::hi`).
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testAlterUserScramCredentials")]
    async fn test_alter_user_scram_credentials() {
        let (admin, mut runnable, _time, _nodes) = env();

        let user0_name = "user0";
        let user0_mechanism0 = PublicScramMechanism::ScramSha256;
        let user0_mechanism1 = PublicScramMechanism::ScramSha512;
        let user1_name = "user1";
        let user1_mechanism0 = PublicScramMechanism::ScramSha256;
        let user2_name = "user2";
        let user2_mechanism0 = PublicScramMechanism::ScramSha512;

        let mut response_data = AlterUserScramCredentialsResponseData::new();
        response_data.set_results(
            [user0_name, user1_name, user2_name]
                .into_iter()
                .map(|u| {
                    let mut r = WireAlterResult::new();
                    r.set_user(u.to_string()).set_error_code(Errors::None.code());
                    r
                })
                .collect(),
        );
        runnable
            .client_mut()
            .prepare_response(ConcreteResponse::AlterUserScramCredentials(
                AlterUserScramCredentialsResponse::new(response_data, 0),
            ));

        let alterations: Vec<UserScramCredentialAlteration> = vec![
            UserScramCredentialDeletion::new(user0_name, user0_mechanism0).into(),
            UserScramCredentialUpsertion::with_str(
                user0_name,
                ScramCredentialInfo::new(user0_mechanism1, 8192),
                "password",
            )
            .into(),
            UserScramCredentialUpsertion::with_str(
                user1_name,
                ScramCredentialInfo::new(user1_mechanism0, 8192),
                "password",
            )
            .into(),
            UserScramCredentialDeletion::new(user2_name, user2_mechanism0).into(),
        ];
        let result =
            admin.alter_user_scram_credentials_with_options(&alterations, AlterUserScramCredentialsOptions::new());
        pump(&mut runnable, 5).await;

        let result_data = result.values();
        assert_eq!(result_data.len(), 3);
        for user in [user0_name, user1_name, user2_name] {
            assert!(result_data.contains_key(user));
            result_data[user].get().await.unwrap();
        }
    }

    // --- createTopics --------------------------------------------------------

    #[tokio::test]
    async fn test_create_topics_success() {
        let (admin, mut runnable, _time, _nodes) = env();
        let result = admin.create_topics_with_options(
            &[NewTopic::with_num_partitions_replication_factor(
                "myTopic",
                Some(1),
                Some(1),
            )],
            CreateTopicsOptions::new(),
        );
        runnable
            .client_mut()
            .prepare_response(create_response(vec![create_result("myTopic", Errors::None, None)]));
        pump(&mut runnable, 5).await;
        result.all().get().await.unwrap();
        assert_eq!(result.num_partitions("myTopic").get().await.unwrap(), 1);
        assert_eq!(result.topic_id("myTopic").get().await.unwrap(), Uuid::new(0, 7));
    }

    #[tokio::test]
    async fn test_create_topics_error_surfaces_message() {
        let (admin, mut runnable, _time, _nodes) = env();
        let result = admin.create_topics_with_options(
            &[NewTopic::with_num_partitions_replication_factor(
                "bad",
                Some(1),
                Some(1),
            )],
            CreateTopicsOptions::new(),
        );
        runnable.client_mut().prepare_response(create_response(vec![create_result(
            "bad",
            Errors::InvalidTopicError,
            Some("Topic name is invalid"),
        )]));
        pump(&mut runnable, 5).await;
        let err = result.values()["bad"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidTopicError);
        assert_eq!(err.message(), "Topic name is invalid");
    }

    #[tokio::test]
    async fn test_create_topics_partial_response_completes_unrealized() {
        let (admin, mut runnable, _time, _nodes) = env();
        let result = admin.create_topics_with_options(
            &[
                NewTopic::with_num_partitions_replication_factor("present", Some(1), Some(1)),
                NewTopic::with_num_partitions_replication_factor("missing", Some(1), Some(1)),
            ],
            CreateTopicsOptions::new(),
        );
        // Response omits "missing".
        runnable
            .client_mut()
            .prepare_response(create_response(vec![create_result("present", Errors::None, None)]));
        pump(&mut runnable, 5).await;
        result.values()["present"].get().await.unwrap();
        let err = result.values()["missing"].get().await.unwrap_err();
        assert_eq!(
            err.message(),
            "The controller response did not contain a result for topic missing"
        );
    }

    #[tokio::test]
    async fn test_create_topics_retries_on_disconnect() {
        let (admin, mut runnable, time, _nodes) = env();
        let result = admin.create_topics_with_options(
            &[NewTopic::with_num_partitions_replication_factor(
                "myTopic",
                Some(1),
                Some(1),
            )],
            CreateTopicsOptions::new(),
        );
        // First a disconnect, then a success.
        runnable
            .client_mut()
            .prepare_response_disconnected(create_response(vec![]), true);
        runnable
            .client_mut()
            .prepare_response(create_response(vec![create_result("myTopic", Errors::None, None)]));
        for _ in 0..20 {
            if result.values()["myTopic"].is_done() {
                break;
            }
            runnable.run_once().await;
            time.sleep(500);
        }
        result.all().get().await.unwrap();
    }

    /// Mirrors `KafkaAdminClientTest.testCreateTopicsRetryBackoff`: a retry must
    /// wait for the backoff to elapse before the next attempt is issued. The
    /// driven-runnable harness has no wall clock, so we assert the retry is
    /// gated by `next_allowed_try_ms` — it does not fire until the mock time
    /// advances past the backoff.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testCreateTopicsRetryBackoff")]
    async fn test_create_topics_retry_backoff() {
        let retry_backoff = 5000;
        let (admin, mut runnable, time, _nodes) = env_with_props(&[("retry.backoff.ms", &retry_backoff.to_string())]);
        let result = admin.create_topics_with_options(
            &[NewTopic::with_num_partitions_replication_factor(
                "myTopic",
                Some(1),
                Some(1),
            )],
            CreateTopicsOptions::new(),
        );
        // First attempt disconnects, second succeeds.
        runnable
            .client_mut()
            .prepare_response_disconnected(create_response(vec![]), true);
        runnable
            .client_mut()
            .prepare_response(create_response(vec![create_result("myTopic", Errors::None, None)]));

        // Drive until the first (disconnected) attempt has failed and the retry
        // is scheduled. The success response must remain unconsumed while the
        // backoff has not yet elapsed.
        pump_until(&mut runnable, 20, |r| r.client_mut().num_awaiting_responses() == 1).await;
        assert!(!result.values()["myTopic"].is_done());

        // Pump repeatedly without advancing time: the backoff gate holds and the
        // retry is not sent, so the success response stays queued.
        pump(&mut runnable, 5).await;
        assert_eq!(runnable.client_mut().num_awaiting_responses(), 1);
        assert!(!result.values()["myTopic"].is_done());

        // Advance past the (jittered) upper-bound backoff; the retry now fires.
        let upper_bound = (retry_backoff as f64 * RETRY_BACKOFF_EXP_BASE as f64 * (1.0 + RETRY_BACKOFF_JITTER)) as i64;
        time.sleep(upper_bound);
        pump_until(&mut runnable, 20, |r| r.client_mut().num_awaiting_responses() == 0).await;
        result.all().get().await.unwrap();
    }

    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testCreateTopicsHandleNotControllerException")]
    async fn test_create_topics_handle_not_controller_error() {
        let (admin, mut runnable, time, nodes) = env();
        // First attempt hits the wrong controller; then a metadata refresh
        // updates the controller; then the retry succeeds.
        runnable.client_mut().prepare_response(create_response(vec![create_result(
            "myTopic",
            Errors::NotController,
            None,
        )]));
        runnable
            .client_mut()
            .prepare_response(ConcreteResponse::Metadata(RequestTestUtils::metadata_response(
                &nodes,
                Some("mock-cluster"),
                1,
                Vec::new(),
            )));
        runnable
            .client_mut()
            .prepare_response(create_response(vec![create_result("myTopic", Errors::None, None)]));
        let result = admin.create_topics_with_options(
            &[NewTopic::with_num_partitions_replication_factor(
                "myTopic",
                Some(1),
                Some(1),
            )],
            CreateTopicsOptions::new(),
        );
        // The NOT_CONTROLLER retry is routed through the retry-backoff gate, so
        // the mock clock must advance for the retry to become eligible.
        for _ in 0..30 {
            if result.values()["myTopic"].is_done() {
                break;
            }
            runnable.run_once().await;
            time.sleep(200);
        }
        result.all().get().await.unwrap();
    }

    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testCreateTopicsRetryThrottlingExceptionWhenEnabled"
    )]
    async fn test_create_topics_retry_throttling_error_when_enabled() {
        let (admin, mut runnable, _time, _nodes) = env();
        // topic1 succeeds, topic2 is throttled (retried until success), topic3 already exists.
        runnable.client_mut().prepare_response(create_response_throttled(
            1000,
            vec![
                create_result("topic1", Errors::None, None),
                create_result("topic2", Errors::ThrottlingQuotaExceeded, None),
                create_result("topic3", Errors::TopicAlreadyExists, None),
            ],
        ));
        runnable.client_mut().prepare_response(create_response_throttled(
            1000,
            vec![create_result("topic2", Errors::ThrottlingQuotaExceeded, None)],
        ));
        runnable
            .client_mut()
            .prepare_response(create_response_throttled(0, vec![create_result("topic2", Errors::None, None)]));

        let result = admin.create_topics_with_options(
            &[
                NewTopic::with_num_partitions_replication_factor("topic1", Some(1), Some(1)),
                NewTopic::with_num_partitions_replication_factor("topic2", Some(1), Some(1)),
                NewTopic::with_num_partitions_replication_factor("topic3", Some(1), Some(1)),
            ],
            CreateTopicsOptions::new().set_retry_on_quota_violation(true),
        );
        pump_until(&mut runnable, 30, |r| r.client_mut().num_awaiting_responses() == 0).await;
        result.values()["topic1"].get().await.unwrap();
        result.values()["topic2"].get().await.unwrap();
        let err = result.values()["topic3"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::TopicAlreadyExists);
    }

    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testCreateTopicsDontRetryThrottlingExceptionWhenDisabled"
    )]
    async fn test_create_topics_dont_retry_throttling_error_when_disabled() {
        let (admin, mut runnable, _time, _nodes) = env();
        runnable.client_mut().prepare_response(create_response_throttled(
            1000,
            vec![
                create_result("topic1", Errors::None, None),
                create_result("topic2", Errors::ThrottlingQuotaExceeded, None),
                create_result("topic3", Errors::TopicAlreadyExists, None),
            ],
        ));
        let result = admin.create_topics_with_options(
            &[
                NewTopic::with_num_partitions_replication_factor("topic1", Some(1), Some(1)),
                NewTopic::with_num_partitions_replication_factor("topic2", Some(1), Some(1)),
                NewTopic::with_num_partitions_replication_factor("topic3", Some(1), Some(1)),
            ],
            CreateTopicsOptions::new().set_retry_on_quota_violation(false),
        );
        pump(&mut runnable, 5).await;
        result.values()["topic1"].get().await.unwrap();
        let err = result.values()["topic2"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::ThrottlingQuotaExceeded);
        assert_eq!(err.throttle_time_ms(), Some(1000));
        let err3 = result.values()["topic3"].get().await.unwrap_err();
        assert_eq!(err3.error(), Errors::TopicAlreadyExists);
    }

    #[tokio::test]
    async fn test_create_topics_retry_throttling_error_when_enabled_until_request_timeout() {
        let default_api_timeout: i64 = 60000;
        let (admin, mut runnable, time, _nodes) =
            env_with_props(&[("default.api.timeout.ms", &default_api_timeout.to_string())]);
        runnable.client_mut().prepare_response(create_response_throttled(
            1000,
            vec![
                create_result("topic1", Errors::None, None),
                create_result("topic2", Errors::ThrottlingQuotaExceeded, None),
                create_result("topic3", Errors::TopicAlreadyExists, None),
            ],
        ));
        runnable.client_mut().prepare_response(create_response_throttled(
            1000,
            vec![create_result("topic2", Errors::ThrottlingQuotaExceeded, None)],
        ));
        let result = admin.create_topics_with_options(
            &[
                NewTopic::with_num_partitions_replication_factor("topic1", Some(1), Some(1)),
                NewTopic::with_num_partitions_replication_factor("topic2", Some(1), Some(1)),
                NewTopic::with_num_partitions_replication_factor("topic3", Some(1), Some(1)),
            ],
            CreateTopicsOptions::new().set_retry_on_quota_violation(true),
        );
        // Consume both prepared responses; the third (retry) request stays in flight.
        pump_until(&mut runnable, 30, |r| {
            !r.client_mut().has_pending_responses() && r.client_mut().request_count() >= 1
        })
        .await;
        // Advance past the default api timeout to time out the in-flight request.
        time.sleep(default_api_timeout + 1);
        pump_until(&mut runnable, 30, |r| {
            let _ = r;
            result.values()["topic2"].is_done()
        })
        .await;
        result.values()["topic1"].get().await.unwrap();
        let err = result.values()["topic2"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::ThrottlingQuotaExceeded);
        assert_eq!(err.throttle_time_ms(), Some(0));
        let err3 = result.values()["topic3"].get().await.unwrap_err();
        assert_eq!(err3.error(), Errors::TopicAlreadyExists);
    }

    // --- deleteTopics --------------------------------------------------------

    #[tokio::test]
    async fn test_delete_topics_by_name_success() {
        let (admin, mut runnable, _time, _nodes) = env();
        let result = admin.delete_topics_with_options(
            TopicCollection::of_topic_names(vec!["myTopic".to_string()]),
            DeleteTopicsOptions::new(),
        );
        runnable
            .client_mut()
            .prepare_response(delete_response(vec![delete_result_named("myTopic", Errors::None)]));
        pump(&mut runnable, 5).await;
        result.all().get().await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_topics_by_name_error() {
        let (admin, mut runnable, _time, _nodes) = env();
        let result = admin.delete_topics_with_options(
            TopicCollection::of_topic_names(vec!["ghost".to_string()]),
            DeleteTopicsOptions::new(),
        );
        runnable.client_mut().prepare_response(delete_response(vec![delete_result_named(
            "ghost",
            Errors::UnknownTopicOrPartition,
        )]));
        pump(&mut runnable, 5).await;
        let err = result.topic_name_values().unwrap()["ghost"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnknownTopicOrPartition);
    }

    #[tokio::test]
    async fn test_delete_topics_by_id_success() {
        let (admin, mut runnable, _time, _nodes) = env();
        let id = Uuid::new(1, 2);
        let result =
            admin.delete_topics_with_options(TopicCollection::of_topic_ids(vec![id]), DeleteTopicsOptions::new());
        let mut r = DeletableTopicResult::new();
        r.set_topic_id(id);
        r.set_error_code(Errors::None.code());
        runnable.client_mut().prepare_response(delete_response(vec![r]));
        pump(&mut runnable, 5).await;
        result.all().get().await.unwrap();
    }

    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDeleteTopicsPartialResponse")]
    async fn test_delete_topics_partial_response() {
        // By name: the response omits "myOtherTopic", so its future is
        // completed by the unrealized-futures sanity check.
        let (admin, mut runnable, _time, _nodes) = env();
        let result = admin.delete_topics_with_options(
            TopicCollection::of_topic_names(vec!["myTopic".to_string(), "myOtherTopic".to_string()]),
            DeleteTopicsOptions::new(),
        );
        runnable.client_mut().prepare_response(delete_response_throttled(
            1000,
            vec![delete_result_named("myTopic", Errors::None)],
        ));
        pump(&mut runnable, 5).await;
        result.topic_name_values().unwrap()["myTopic"].get().await.unwrap();
        let err = result.topic_name_values().unwrap()["myOtherTopic"].get().await.unwrap_err();
        assert_eq!(
            err.message(),
            "The controller response did not contain a result for topic myOtherTopic"
        );

        // By id: the response omits topicId2.
        let (admin, mut runnable, _time, _nodes) = env();
        let id1 = Uuid::new(1, 1);
        let id2 = Uuid::new(2, 2);
        let result =
            admin.delete_topics_with_options(TopicCollection::of_topic_ids(vec![id1, id2]), DeleteTopicsOptions::new());
        runnable
            .client_mut()
            .prepare_response(delete_response_throttled(1000, vec![delete_result_with_id(id1, Errors::None)]));
        pump(&mut runnable, 5).await;
        result.topic_id_values().unwrap()[&id1].get().await.unwrap();
        let err = result.topic_id_values().unwrap()[&id2].get().await.unwrap_err();
        assert_eq!(
            err.message(),
            format!("The controller response did not contain a result for topic {id2}")
        );
    }

    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDeleteTopicsRetryThrottlingExceptionWhenEnabled"
    )]
    async fn test_delete_topics_retry_throttling_error_when_enabled() {
        // By name.
        let (admin, mut runnable, _time, _nodes) = env();
        runnable.client_mut().prepare_response(delete_response_throttled(
            1000,
            vec![
                delete_result_named("topic1", Errors::None),
                delete_result_named("topic2", Errors::ThrottlingQuotaExceeded),
                delete_result_named("topic3", Errors::TopicAlreadyExists),
            ],
        ));
        runnable.client_mut().prepare_response(delete_response_throttled(
            1000,
            vec![delete_result_named("topic2", Errors::ThrottlingQuotaExceeded)],
        ));
        runnable
            .client_mut()
            .prepare_response(delete_response_throttled(0, vec![delete_result_named("topic2", Errors::None)]));
        let result = admin.delete_topics_with_options(
            TopicCollection::of_topic_names(vec!["topic1".to_string(), "topic2".to_string(), "topic3".to_string()]),
            DeleteTopicsOptions::new().set_retry_on_quota_violation(true),
        );
        pump_until(&mut runnable, 30, |r| r.client_mut().num_awaiting_responses() == 0).await;
        result.topic_name_values().unwrap()["topic1"].get().await.unwrap();
        result.topic_name_values().unwrap()["topic2"].get().await.unwrap();
        let err = result.topic_name_values().unwrap()["topic3"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::TopicAlreadyExists);

        // By id.
        let (admin, mut runnable, _time, _nodes) = env();
        let id1 = Uuid::new(1, 1);
        let id2 = Uuid::new(2, 2);
        let id3 = Uuid::new(3, 3);
        runnable.client_mut().prepare_response(delete_response_throttled(
            1000,
            vec![
                delete_result_with_id(id1, Errors::None),
                delete_result_with_id(id2, Errors::ThrottlingQuotaExceeded),
                delete_result_with_id(id3, Errors::UnknownTopicId),
            ],
        ));
        runnable.client_mut().prepare_response(delete_response_throttled(
            1000,
            vec![delete_result_with_id(id2, Errors::ThrottlingQuotaExceeded)],
        ));
        runnable
            .client_mut()
            .prepare_response(delete_response_throttled(0, vec![delete_result_with_id(id2, Errors::None)]));
        let result = admin.delete_topics_with_options(
            TopicCollection::of_topic_ids(vec![id1, id2, id3]),
            DeleteTopicsOptions::new().set_retry_on_quota_violation(true),
        );
        pump_until(&mut runnable, 30, |r| r.client_mut().num_awaiting_responses() == 0).await;
        result.topic_id_values().unwrap()[&id1].get().await.unwrap();
        result.topic_id_values().unwrap()[&id2].get().await.unwrap();
        let err = result.topic_id_values().unwrap()[&id3].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnknownTopicId);
    }

    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDeleteTopicsDontRetryThrottlingExceptionWhenDisabled"
    )]
    async fn test_delete_topics_dont_retry_throttling_error_when_disabled() {
        // By name.
        let (admin, mut runnable, _time, _nodes) = env();
        runnable.client_mut().prepare_response(delete_response_throttled(
            1000,
            vec![
                delete_result_named("topic1", Errors::None),
                delete_result_named("topic2", Errors::ThrottlingQuotaExceeded),
                delete_result_named("topic3", Errors::TopicAlreadyExists),
            ],
        ));
        let result = admin.delete_topics_with_options(
            TopicCollection::of_topic_names(vec!["topic1".to_string(), "topic2".to_string(), "topic3".to_string()]),
            DeleteTopicsOptions::new().set_retry_on_quota_violation(false),
        );
        pump(&mut runnable, 5).await;
        result.topic_name_values().unwrap()["topic1"].get().await.unwrap();
        let err = result.topic_name_values().unwrap()["topic2"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::ThrottlingQuotaExceeded);
        assert_eq!(err.throttle_time_ms(), Some(1000));
        let err3 = result.topic_name_values().unwrap()["topic3"].get().await.unwrap_err();
        assert_eq!(err3.error(), Errors::TopicAlreadyExists);

        // By id.
        let (admin, mut runnable, _time, _nodes) = env();
        let id1 = Uuid::new(1, 1);
        let id2 = Uuid::new(2, 2);
        let id3 = Uuid::new(3, 3);
        runnable.client_mut().prepare_response(delete_response_throttled(
            1000,
            vec![
                delete_result_with_id(id1, Errors::None),
                delete_result_with_id(id2, Errors::ThrottlingQuotaExceeded),
                delete_result_with_id(id3, Errors::UnknownTopicId),
            ],
        ));
        let result = admin.delete_topics_with_options(
            TopicCollection::of_topic_ids(vec![id1, id2, id3]),
            DeleteTopicsOptions::new().set_retry_on_quota_violation(false),
        );
        pump(&mut runnable, 5).await;
        result.topic_id_values().unwrap()[&id1].get().await.unwrap();
        let err = result.topic_id_values().unwrap()[&id2].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::ThrottlingQuotaExceeded);
        assert_eq!(err.throttle_time_ms(), Some(1000));
        let err3 = result.topic_id_values().unwrap()[&id3].get().await.unwrap_err();
        assert_eq!(err3.error(), Errors::UnknownTopicId);
    }

    #[tokio::test]
    async fn test_delete_topics_retry_throttling_error_when_enabled_until_request_timeout() {
        let default_api_timeout: i64 = 60000;
        // By name.
        let (admin, mut runnable, time, _nodes) =
            env_with_props(&[("default.api.timeout.ms", &default_api_timeout.to_string())]);
        runnable.client_mut().prepare_response(delete_response_throttled(
            1000,
            vec![
                delete_result_named("topic1", Errors::None),
                delete_result_named("topic2", Errors::ThrottlingQuotaExceeded),
                delete_result_named("topic3", Errors::TopicAlreadyExists),
            ],
        ));
        runnable.client_mut().prepare_response(delete_response_throttled(
            1000,
            vec![delete_result_named("topic2", Errors::ThrottlingQuotaExceeded)],
        ));
        let result = admin.delete_topics_with_options(
            TopicCollection::of_topic_names(vec!["topic1".to_string(), "topic2".to_string(), "topic3".to_string()]),
            DeleteTopicsOptions::new().set_retry_on_quota_violation(true),
        );
        pump_until(&mut runnable, 30, |r| {
            !r.client_mut().has_pending_responses() && r.client_mut().request_count() >= 1
        })
        .await;
        time.sleep(default_api_timeout + 1);
        pump_until(&mut runnable, 30, |_| result.topic_name_values().unwrap()["topic2"].is_done()).await;
        result.topic_name_values().unwrap()["topic1"].get().await.unwrap();
        let err = result.topic_name_values().unwrap()["topic2"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::ThrottlingQuotaExceeded);
        assert_eq!(err.throttle_time_ms(), Some(0));
        let err3 = result.topic_name_values().unwrap()["topic3"].get().await.unwrap_err();
        assert_eq!(err3.error(), Errors::TopicAlreadyExists);

        // By id.
        let (admin, mut runnable, time, _nodes) =
            env_with_props(&[("default.api.timeout.ms", &default_api_timeout.to_string())]);
        let id1 = Uuid::new(1, 1);
        let id2 = Uuid::new(2, 2);
        let id3 = Uuid::new(3, 3);
        runnable.client_mut().prepare_response(delete_response_throttled(
            1000,
            vec![
                delete_result_with_id(id1, Errors::None),
                delete_result_with_id(id2, Errors::ThrottlingQuotaExceeded),
                delete_result_with_id(id3, Errors::UnknownTopicId),
            ],
        ));
        runnable.client_mut().prepare_response(delete_response_throttled(
            1000,
            vec![delete_result_with_id(id2, Errors::ThrottlingQuotaExceeded)],
        ));
        let result = admin.delete_topics_with_options(
            TopicCollection::of_topic_ids(vec![id1, id2, id3]),
            DeleteTopicsOptions::new().set_retry_on_quota_violation(true),
        );
        pump_until(&mut runnable, 30, |r| {
            !r.client_mut().has_pending_responses() && r.client_mut().request_count() >= 1
        })
        .await;
        time.sleep(default_api_timeout + 1);
        pump_until(&mut runnable, 30, |_| result.topic_id_values().unwrap()[&id2].is_done()).await;
        result.topic_id_values().unwrap()[&id1].get().await.unwrap();
        let err = result.topic_id_values().unwrap()[&id2].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::ThrottlingQuotaExceeded);
        assert_eq!(err.throttle_time_ms(), Some(0));
        let err3 = result.topic_id_values().unwrap()[&id3].get().await.unwrap_err();
        assert_eq!(err3.error(), Errors::UnknownTopicId);
    }

    /// `KafkaAdminClient.java:1585-1586` fails a call submitted after the I/O
    /// thread is gone with `new TimeoutException("The AdminClient thread has
    /// exited.")`, and `handleTimeoutFailure` short-circuits on
    /// `cause instanceof TimeoutException` (`:969-970`) so the user sees exactly a
    /// `TimeoutException` — i.e. a `RetriableException`.
    ///
    /// This used to be `Error::local_illegal_state`, whose `ErrorHierarchy` is empty, so
    /// **both** `is_retriable_error()` and `is_kafka_error()` answered `false`: a
    /// caller writing `if err.is_retriable_error() { retry }` behaved differently
    /// against the two clients for the identical condition.
    #[tokio::test]
    async fn a_call_submitted_after_the_io_task_exits_fails_with_a_timeout() {
        let (admin, runnable, _time, _nodes) = env();
        // Dropping the runnable drops the receiving end of the call channel, which
        // is what `runnable_call`'s `SendError` arm observes.
        drop(runnable);

        let result = admin.list_topics_with_options(ListTopicsOptions::new());
        let error = result.names().get().await.expect_err("the call cannot be delivered");

        assert_eq!(error.message(), "The AdminClient thread has exited.");
        assert!(error.is_timeout_error(), "got {error:?}");
        assert!(error.is_retriable_error(), "a timeout error is retriable: {error:?}");
        assert!(
            error.is_kafka_error(),
            "... which is an API error, which is a Kafka error: {error:?}"
        );
    }

    /// `KafkaAdminClient.java:1377-1379` cancels a call whose node disconnected with
    /// `new DisconnectException(...)`. It used to be built as
    /// `Errors::NetworkError`, so the user got protocol code 13 for a purely
    /// client-side event and a caller matching `Error::Disconnect(_)` never fired.
    ///
    /// The driver's `t instanceof DisconnectException` retry-lookup branch
    /// (`AdminApiDriver.java:265`) keys off the same class, so the wrong class was
    /// load-bearing — hence this asserts the class, not just the message.
    ///
    /// It also pins `Call.handleTimeoutFailure` (`KafkaAdminClient.java:959-964`),
    /// which is what the retry-exhausted disconnect reaches:
    ///
    /// ```java
    /// handleFailure(new TimeoutException(this + " timed out at " + now
    ///     + " after " + tries + " attempt(s)", cause));
    /// ```
    ///
    /// The Rust message used to gain an invented `"Aborted due to timeout: "`
    /// prefix, drop the `Call(...)` rendering of `this` (`:1001-1004`), and append
    /// the cause as text — leaving `Error::source()` empty where Java's
    /// `getCause()` is populated.
    #[tokio::test]
    async fn a_disconnect_fails_the_call_with_a_disconnect_error() {
        // One retry attempt only, so the call fails terminally rather than looping.
        let (admin, mut runnable, time, nodes) = env_with_props(&[("retries", "0")]);
        runnable.client_mut().prepare_response_disconnected(
            ConcreteResponse::Metadata(RequestTestUtils::metadata_response(
                &nodes,
                Some("mock-cluster"),
                0,
                Vec::new(),
            )),
            true,
        );

        let result = admin.list_topics_with_options(ListTopicsOptions::new());
        for _ in 0..30 {
            if result.names().is_done() {
                break;
            }
            runnable.run_once().await;
            time.sleep(200);
        }
        let error = result.names().get().await.expect_err("the disconnect must fail the call");

        // With `retries=0` the call is out of retries immediately, so Java's
        // `handleTimeoutFailure` wraps the cause in a `TimeoutException`.
        assert!(error.is_timeout_error(), "got {error:?}");
        assert!(
            !error.message().starts_with("Aborted due to timeout"),
            "the invented prefix has no counterpart in the Kafka tree: {}",
            error.message()
        );
        assert!(
            error.message().starts_with("Call(callName=listTopics, deadlineMs="),
            "the message must open with Java's `Call.toString()` rendering: {}",
            error.message()
        );
        assert!(
            error.message().ends_with(" attempt(s)"),
            "and end exactly where Java's does, with the cause carried separately: {}",
            error.message()
        );

        // The cause is the `DisconnectException` Java builds at
        // `KafkaAdminClient.java:1377-1379`, attached rather than stringified.
        let cause = error.source().expect("handleTimeoutFailure passes the cause through");
        assert!(
            matches!(cause, Error::Disconnect(_)),
            "expected a disconnect error; got {cause:?}"
        );
        assert!(
            cause.message().contains("being disconnected"),
            "the message keeps Java's text: {}",
            cause.message()
        );
        // `DisconnectException extends RetriableException`, so retriability is
        // unchanged relative to the old `NetworkError` — the class is the fix.
        assert!(cause.is_retriable_error(), "got {cause:?}");
    }

    /// Java wraps the admin-client constructor in `catch (Throwable exc)` and
    /// rethrows `new KafkaException("Failed to create new KafkaAdminClient", exc)`
    /// (`KafkaAdminClient.java:569-573` / `:592-595`), so a caller has one class and
    /// one message for "admin client construction failed".
    ///
    /// The three failure points used to surface in three different shapes, none of
    /// them Java's: a leaked underlying error, an `illegal_argument`
    /// (`is_kafka_error()` → `false`) with an invented message, and a panic.
    #[test]
    fn construction_failures_are_wrapped_as_a_kafka_error() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "not-a-host-port".to_string());
        let config = AdminClientConfig::new(&props).expect("the config itself parses");

        let error = KafkaAdminClient::new(config)
            .err()
            .expect("an unparseable bootstrap.servers entry must fail construction");

        assert_eq!(error.message(), "Failed to create new KafkaAdminClient");
        // Java's replacement is a bare `KafkaException`.
        assert!(error.is_kafka_error(), "Java's replacement is a Kafka error: {error:?}");
        assert!(!error.is_api_error(), "a bare Kafka error is not an API error: {error:?}");
        assert!(error.source().is_some(), "the underlying failure must be the wrapper's cause");
        // The cause is `parseAndValidateAddresses`' `ConfigException`, with its
        // single-message text.
        let cause: &Error = std::error::Error::source(&error)
            .and_then(|e| e.downcast_ref::<Error>())
            .expect("the cause is a crate Error");
        assert!(matches!(cause, Error::Config(_)), "got {cause:?}");
        assert_eq!(cause.message(), "Invalid url in bootstrap.servers: not-a-host-port");
    }

    /// `AdminClientRunnable.run`'s `finally` (`KafkaAdminClient.java:1473-1492`)
    /// fails every pending call with
    /// `TimeoutException("The AdminClient thread has exited. Call: <name>")`,
    /// **however** `processRequests` terminated.
    ///
    /// Translated as straight-line code after the loop it was not a `finally` at
    /// all: any panic inside a loop iteration skipped both `fail_all_remaining` and
    /// `client.close()`, so no outstanding `KafkaFuture` was ever completed and no
    /// socket was closed — a silent permanent hang.
    #[tokio::test]
    async fn a_panicking_io_task_still_runs_its_finally() {
        let (admin, mut runnable, time, _nodes) = env();

        // Inject a panic inside the loop body, standing in for the
        // `ClassCastException`s Java's `catch (Throwable t)` covers (the Issue-66
        // sites). An already-expired deadline makes `process_pending_calls`'s step 2
        // (`handle_timeouts` -> `fail_call` -> `handle_timeout_failure`) invoke this
        // call's failure hook on the very first iteration, so the injection is
        // deterministic. It panics only once, so the `finally`'s own re-failing of
        // the queue can be observed instead of panicking again.
        let failures = Arc::new(AtomicI64::new(0));
        let counter = Arc::clone(&failures);
        let now = time.milliseconds();
        admin.submit(Call::new(
            "panicOnPurpose",
            now - 1,
            NodeProvider::LeastLoaded,
            Box::new(|_timeout_ms| unreachable!("the call expires before it is ever sent")),
            Box::new(|_response, _now, _cur_node| HandleResult::Done),
            Box::new(move |_error| {
                if counter.fetch_add(1, Ordering::AcqRel) == 0 {
                    panic!("injected panic inside the I/O loop");
                }
            }),
            Box::new(|| false),
        ));

        // `run()` must return: the panic ends `process_requests`, and the `finally`
        // then runs. Bounded so a regression shows up as a failure, not a hang.
        tokio::time::timeout(std::time::Duration::from_secs(10), runnable.run())
            .await
            .expect("run() must reach its finally after a panic, not spin or abort");

        assert_eq!(
            failures.load(Ordering::Acquire),
            1,
            "the injected panic must actually have fired"
        );
        // `client.close()` is the tail of Java's `finally`; it only runs if the
        // whole block was reached. Straight-line code after the loop skipped it.
        assert!(
            !runnable.client_mut().active(),
            "the finally must close the client even when the loop body panicked"
        );
    }

    /// `KafkaAdminClient.java:1292-1298` wraps a `createRequest` failure as
    ///
    /// ```java
    /// new KafkaException(String.format("Internal error sending %s to %s.", call.callName, node), t)
    /// ```
    ///
    /// — a bare `KafkaException` carrying the original as its cause. It used to be
    /// an `Error::local_illegal_state` (`is_kafka_error()` → `false`) whose message had
    /// the cause text appended, so `Error::source()` was empty.
    #[tokio::test]
    async fn a_create_request_failure_is_wrapped_as_a_kafka_error() {
        let (admin, mut runnable, time, _nodes) = env_with_props(&[("retries", "0")]);

        let failure: Arc<Mutex<Option<Error>>> = Arc::new(Mutex::new(None));
        let sink = Arc::clone(&failure);
        let now = time.milliseconds();
        admin.submit(Call::new(
            "createRequestBoom",
            now + 60_000,
            NodeProvider::LeastLoaded,
            Box::new(|_timeout_ms| Err(Error::new(Errors::InvalidRequest))),
            Box::new(|_response, _now, _cur_node| HandleResult::Done),
            Box::new(move |error| {
                *sink.lock().unwrap() = Some(error.clone());
            }),
            Box::new(|| false),
        ));

        for _ in 0..30 {
            runnable.run_once().await;
            time.sleep(200);
            if failure.lock().unwrap().is_some() {
                break;
            }
        }

        let error = failure.lock().unwrap().take().expect("the call must be failed");
        assert_eq!(
            error.message(),
            "Internal error sending createRequestBoom to localhost:9092 (id: 0 rack: None isFenced: false)."
        );
        // Java's replacement is a bare `KafkaException`.
        assert!(error.is_kafka_error(), "Java's replacement is a Kafka error: {error:?}");
        assert!(!error.is_api_error(), "a bare Kafka error is not an API error: {error:?}");
        assert_eq!(
            error.source().expect("the createRequest error is the cause").error(),
            Errors::InvalidRequest,
            "the cause is attached, not stringified into the message"
        );
    }

    /// `makeBrokerMetadataCall.handleResponse` does
    /// `(MetadataResponse) abstractResponse` unguarded
    /// (`KafkaAdminClient.java:1668`) and relies on the `:1387`
    /// `catch (Throwable t)` → `call.fail(now, t)` → non-retriable →
    /// `handleFailure` → `metadataManager.updateFailed(e)`.
    ///
    /// Swallowing the mismatch ran neither `update()` nor `update_failed()` while
    /// `process_pending_calls` had already called `transition_to_update_pending`, and
    /// `metadata_fetch_delay_ms` returns `i64::MAX` in `UPDATE_PENDING` — so the
    /// client never refreshed metadata again for its whole lifetime, and
    /// `HandleResult::Done` on an internal call also re-queued the pending calls
    /// against permanently stale metadata.
    #[tokio::test]
    async fn a_wrong_typed_metadata_response_does_not_pin_the_manager_in_update_pending() {
        let (_admin, mut runnable, time, _nodes) = env_with_props(&[("retries", "0")]);

        // Force the internal metadata refresh, then answer it with the wrong type.
        runnable.metadata_manager().request_update();
        runnable.client_mut().prepare_response(ConcreteResponse::ListGroups(
            crate::common::requests::ListGroupsResponse::new(crate::ListGroupsResponseData::new()),
        ));

        for _ in 0..30 {
            runnable.run_once().await;
            time.sleep(200);
            if runnable.client_mut().num_awaiting_responses() == 0 {
                break;
            }
        }
        // The refresh must actually have gone out and consumed the wrong-typed
        // response, or this test would pass vacuously.
        assert_eq!(
            runnable.client_mut().num_awaiting_responses(),
            0,
            "the internal metadata call must have been issued and answered"
        );

        assert_ne!(
            runnable.metadata_manager().metadata_fetch_delay_ms(time.milliseconds()),
            i64::MAX,
            "the manager must leave UPDATE_PENDING and retry under backoff, as Java's \
             updateFailed(e) makes it"
        );
    }

    // --- listTopics ----------------------------------------------------------

    #[tokio::test]
    async fn test_list_topics_filters_internal_by_default() {
        let (admin, mut runnable, _time, nodes) = env();
        let result = admin.list_topics_with_options(ListTopicsOptions::new());
        let topics = vec![
            topic_meta("visible", false, Uuid::new(0, 1), 1),
            topic_meta("__consumer_offsets", true, Uuid::new(0, 2), 1),
        ];
        runnable
            .client_mut()
            .prepare_response(ConcreteResponse::Metadata(RequestTestUtils::metadata_response(
                &nodes,
                Some("mock-cluster"),
                0,
                topics,
            )));
        pump(&mut runnable, 5).await;
        let names = result.names().get().await.unwrap();
        assert!(names.contains("visible"));
        assert!(!names.contains("__consumer_offsets"));
    }

    #[tokio::test]
    async fn test_list_topics_includes_internal_when_requested() {
        let (admin, mut runnable, _time, nodes) = env();
        let result = admin.list_topics_with_options(ListTopicsOptions::new().set_list_internal(true));
        let topics = vec![topic_meta("__consumer_offsets", true, Uuid::new(0, 2), 1)];
        runnable
            .client_mut()
            .prepare_response(ConcreteResponse::Metadata(RequestTestUtils::metadata_response(
                &nodes,
                Some("mock-cluster"),
                0,
                topics,
            )));
        pump(&mut runnable, 5).await;
        let names = result.names().get().await.unwrap();
        assert!(names.contains("__consumer_offsets"));
    }

    /// Java's `calcDeadlineMs` clamps a negative option timeout to zero
    /// (`now + Math.max(0, optionTimeoutMs)`, `KafkaAdminClient.java:496-500`), so
    /// on a frozen clock a call with `timeoutMs = -1` has `deadlineMs == now`. The
    /// timeout processor only expires a call whose remaining time is `< 0`
    /// (`:1060-1061`, `:1080-1081`), so the call is sent and succeeds. Without the
    /// clamp the deadline is `now - 1` and the call expires unsent.
    #[tokio::test]
    async fn test_negative_option_timeout_is_clamped_to_zero() {
        let (admin, mut runnable, time, nodes) = env();
        let result = admin.list_topics_with_options(ListTopicsOptions::new().set_timeout_ms(Some(-1)));
        runnable
            .client_mut()
            .prepare_response(ConcreteResponse::Metadata(RequestTestUtils::metadata_response(
                &nodes,
                Some("mock-cluster"),
                0,
                vec![topic_meta("visible", false, Uuid::new(0, 1), 1)],
            )));
        pump(&mut runnable, 5).await;
        let names = result.names().get().await.expect("the call must be sent, not expired");
        assert!(names.contains("visible"));

        let now = time.milliseconds();
        assert_eq!(calc_deadline_ms(now, Some(-1), 60_000), now);
        assert_eq!(calc_deadline_ms(now, Some(i32::MIN), 60_000), now);
        assert_eq!(calc_deadline_ms(now, Some(0), 60_000), now);
        assert_eq!(calc_deadline_ms(now, Some(5), 60_000), now + 5);
        assert_eq!(calc_deadline_ms(now, None, 60_000), now + 60_000);
    }

    // --- describeTopics ------------------------------------------------------

    use crate::DescribeTopicPartitionsResponseData;
    use crate::common::requests::AbstractRequest;
    use crate::common::utils::Utils;
    use crate::describe_topic_partitions_response_data::{
        Cursor as DescribeTopicPartitionsResponseCursor, DescribeTopicPartitionsResponsePartition,
    };

    /// `KafkaAdminClientTest.addPartitionToDescribeTopicPartitionsResponse`:
    /// adds `topic_name` with one partition per index, each led by broker 0
    /// with replicas `[0, 1, 2]`, ISR `[0]`, ELR `[1]` and last-known ELR `[2]`.
    fn add_partition_to_describe_topic_partitions_response(
        data: &mut DescribeTopicPartitionsResponseData,
        topic_name: &str,
        topic_id: Uuid,
        partitions: &[i32],
    ) {
        let adding_partitions = partitions
            .iter()
            .map(|partition| {
                let mut p = DescribeTopicPartitionsResponsePartition::new();
                p.set_isr_nodes(vec![0])
                    .set_error_code(0)
                    .set_leader_epoch(0)
                    .set_leader_id(0)
                    .set_eligible_leader_replicas(Some(vec![1]))
                    .set_last_known_elr(Some(vec![2]))
                    .set_partition_index(*partition)
                    .set_replica_nodes(vec![0, 1, 2]);
                p
            })
            .collect();
        let mut topic = DescribeTopicPartitionsResponseTopic::new();
        topic
            .set_error_code(0)
            .set_topic_id(topic_id)
            .set_name(Some(topic_name.to_string()))
            .set_is_internal(false)
            .set_partitions(adding_partitions);
        data.topics.push(topic);
    }

    fn set_next_cursor(data: &mut DescribeTopicPartitionsResponseData, topic_name: &str, partition_index: i32) {
        let mut cursor = DescribeTopicPartitionsResponseCursor::new();
        cursor
            .set_topic_name(topic_name.to_string())
            .set_partition_index(partition_index);
        data.set_next_cursor(Some(cursor));
    }

    fn describe_topic_partitions_resp(data: DescribeTopicPartitionsResponseData) -> ConcreteResponse {
        ConcreteResponse::DescribeTopicPartitions(DescribeTopicPartitionsResponse::new(data))
    }

    /// The request data of a `DescribeTopicPartitions` request, or `None` for
    /// any other request (Java's `(DescribeTopicPartitionsRequestData) body.data()`
    /// cast, which a matcher only reaches for that request).
    fn describe_topic_partitions_data(request: &AbstractRequest) -> Option<&DescribeTopicPartitionsRequestData> {
        match request {
            AbstractRequest::DescribeTopicPartitions(r) => Some(r.data()),
            _ => None,
        }
    }

    /// Java's `RequestMatcher` bodies in the `DescribeTopicPartitions` tests:
    /// the request names exactly `topics`, in order, and carries `cursor`.
    fn describe_topic_partitions_matcher(
        topics: &'static [&'static str],
        cursor: Option<(&'static str, i32)>,
    ) -> crate::RequestMatcher {
        Box::new(move |body: &AbstractRequest| {
            let Some(request) = describe_topic_partitions_data(body) else {
                return false;
            };
            let names: Vec<&str> = request.topics.iter().map(|t| t.name.as_str()).collect();
            if names != topics {
                return false;
            }
            match (&request.cursor, cursor) {
                (None, None) => true,
                (Some(c), Some((topic_name, partition_index))) => {
                    c.topic_name == topic_name && c.partition_index == partition_index
                },
                _ => false,
            }
        })
    }

    /// `prepareDescribeClusterResponse(0, env.cluster().nodes(), clusterId, 2,
    /// authorizedOperations, false)`.
    fn prepare_describe_cluster(runnable: &mut AdminClientRunnable<MockClient>, nodes: &[Node], authorized_ops: i32) {
        runnable
            .client_mut()
            .prepare_response(describe_cluster_response(2, nodes, "mock-cluster", authorized_ops));
    }

    /// Pumps until every future of `result` is done.
    async fn pump_until_described(runnable: &mut AdminClientRunnable<MockClient>, result: &DescribeTopicsResult) {
        let futures: Vec<KafkaFuture<TopicDescription>> =
            result.topic_name_values().unwrap().values().cloned().collect();
        pump_until(runnable, 40, |_| futures.iter().all(KafkaFuture::is_done)).await;
    }

    /// Translated from
    /// `KafkaAdminClientTest.testDescribeTopicsWithDescribeTopicPartitionsApiBasic`.
    ///
    /// Beyond Java's assertions, it checks that the ELR / last-known-ELR ids
    /// resolve through the `describeCluster` node map, since that is what the
    /// Metadata API could never report.
    #[tokio::test]
    async fn test_describe_topics_with_describe_topic_partitions_api_basic() {
        let (admin, mut runnable, _time, nodes) = env();
        let topic_name0 = "test-0";
        let topic_name1 = "test-1";
        let topics: HashMap<&str, Uuid> =
            HashMap::from([(topic_name0, Uuid::random_uuid()), (topic_name1, Uuid::random_uuid())]);

        prepare_describe_cluster(&mut runnable, &nodes, MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED);

        let mut data_first_part = DescribeTopicPartitionsResponseData::new();
        add_partition_to_describe_topic_partitions_response(
            &mut data_first_part,
            topic_name0,
            topics[topic_name0],
            &[0],
        );
        set_next_cursor(&mut data_first_part, topic_name0, 1);
        runnable.client_mut().prepare_response_matcher(
            describe_topic_partitions_matcher(&["test-0", "test-1"], None),
            describe_topic_partitions_resp(data_first_part),
        );

        let mut data_second_part = DescribeTopicPartitionsResponseData::new();
        add_partition_to_describe_topic_partitions_response(
            &mut data_second_part,
            topic_name0,
            topics[topic_name0],
            &[1],
        );
        add_partition_to_describe_topic_partitions_response(
            &mut data_second_part,
            topic_name1,
            topics[topic_name1],
            &[0],
        );
        runnable.client_mut().prepare_response_matcher(
            describe_topic_partitions_matcher(&["test-0", "test-1"], Some(("test-0", 1))),
            describe_topic_partitions_resp(data_second_part),
        );

        let result = admin.describe_topics_with_topic_names_options(
            &[topic_name0.to_string(), topic_name1.to_string()],
            DescribeTopicsOptions::new(),
        );
        pump_until_described(&mut runnable, &result).await;
        let topic_descriptions = result.all_topic_names().unwrap().get().await.unwrap();
        assert_eq!(topic_descriptions.len(), 2);
        let topic_description = &topic_descriptions[topic_name0];
        assert_eq!(topic_description.partitions().len(), 2);
        assert_eq!(topic_description.partitions()[0].partition(), 0);
        assert_eq!(topic_description.partitions()[1].partition(), 1);
        assert_eq!(topic_description.topic_id(), topics[topic_name0]);
        let topic_description = &topic_descriptions[topic_name1];
        assert_eq!(topic_description.partitions().len(), 1);
        assert_eq!(topic_description.authorized_operations(), None);

        let partition = &topic_description.partitions()[0];
        assert_eq!(partition.leader(), Some(&nodes[0]));
        assert_eq!(partition.replicas(), &nodes[..]);
        assert_eq!(partition.isr(), &nodes[..1]);
        assert_eq!(partition.elr(), Some(&nodes[1..2]));
        assert_eq!(partition.last_known_elr(), Some(&nodes[2..3]));
        // Both pages were served; nothing else was sent.
        assert!(!runnable.client_mut().has_pending_responses());
        assert_eq!(runnable.client_mut().request_count(), 0);
    }

    /// Translated from `KafkaAdminClientTest.testDescribeTopicPartitionsApiWithAuthorizedOps`.
    #[tokio::test]
    async fn test_describe_topic_partitions_api_with_authorized_ops() {
        let (admin, mut runnable, _time, nodes) = env();
        let topic_name0 = "test-0";
        let topic_id = Uuid::random_uuid();

        let authorised_operations =
            Utils::to_32_bit_field(&HashSet::from([AclOperation::Describe.code(), AclOperation::Alter.code()]));
        prepare_describe_cluster(&mut runnable, &nodes, authorised_operations);

        let mut response_data = DescribeTopicPartitionsResponseData::new();
        let mut topic = DescribeTopicPartitionsResponseTopic::new();
        topic
            .set_error_code(0)
            .set_topic_id(topic_id)
            .set_name(Some(topic_name0.to_string()))
            .set_is_internal(false)
            .set_topic_authorized_operations(authorised_operations);
        response_data.topics.push(topic);
        runnable
            .client_mut()
            .prepare_response(describe_topic_partitions_resp(response_data));

        let result = admin.describe_topics_with_topic_names_options(
            &[topic_name0.to_string()],
            DescribeTopicsOptions::new().set_include_authorized_operations(true),
        );
        pump_until_described(&mut runnable, &result).await;
        let topic_descriptions = result.all_topic_names().unwrap().get().await.unwrap();
        let topic_description = &topic_descriptions[topic_name0];
        assert_eq!(
            topic_description.authorized_operations(),
            Some(&BTreeSet::from([AclOperation::Describe, AclOperation::Alter]))
        );
    }

    /// Translated from `KafkaAdminClientTest.testDescribeTopicPartitionsApiWithoutAuthorizedOps`.
    #[tokio::test]
    async fn test_describe_topic_partitions_api_without_authorized_ops() {
        let (admin, mut runnable, _time, nodes) = env();
        let topic_name0 = "test-0";
        let topic_id = Uuid::random_uuid();

        let authorised_operations =
            Utils::to_32_bit_field(&HashSet::from([AclOperation::Describe.code(), AclOperation::Alter.code()]));
        prepare_describe_cluster(&mut runnable, &nodes, authorised_operations);

        let mut response_data = DescribeTopicPartitionsResponseData::new();
        let mut topic = DescribeTopicPartitionsResponseTopic::new();
        topic
            .set_error_code(0)
            .set_topic_id(topic_id)
            .set_name(Some(topic_name0.to_string()))
            .set_is_internal(false)
            .set_topic_authorized_operations(authorised_operations);
        response_data.topics.push(topic);
        runnable
            .client_mut()
            .prepare_response(describe_topic_partitions_resp(response_data));

        let result = admin.describe_topics_with_topic_names_options(
            &[topic_name0.to_string()],
            DescribeTopicsOptions::new().set_include_authorized_operations(false),
        );
        pump_until_described(&mut runnable, &result).await;
        let topic_descriptions = result.all_topic_names().unwrap().get().await.unwrap();
        assert_eq!(topic_descriptions[topic_name0].authorized_operations(), None);
    }

    /// Translated from
    /// `KafkaAdminClientTest.testDescribeTopicsWithDescribeTopicPartitionsApiEdgeCase`:
    /// one page finishes the previous cursor topic and starts a new one, and the
    /// requested names are sent sorted.
    #[tokio::test]
    async fn test_describe_topics_with_describe_topic_partitions_api_edge_case() {
        let (admin, mut runnable, _time, nodes) = env();
        let topic_name0 = "test-0";
        let topic_name1 = "test-1";
        let topic_name2 = "test-2";
        let topics: HashMap<&str, Uuid> = HashMap::from([
            (topic_name0, Uuid::random_uuid()),
            (topic_name1, Uuid::random_uuid()),
            (topic_name2, Uuid::random_uuid()),
        ]);

        prepare_describe_cluster(&mut runnable, &nodes, MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED);

        let mut data_first_part = DescribeTopicPartitionsResponseData::new();
        add_partition_to_describe_topic_partitions_response(
            &mut data_first_part,
            topic_name0,
            topics[topic_name0],
            &[0],
        );
        add_partition_to_describe_topic_partitions_response(
            &mut data_first_part,
            topic_name1,
            topics[topic_name1],
            &[0],
        );
        set_next_cursor(&mut data_first_part, topic_name1, 1);
        runnable.client_mut().prepare_response_matcher(
            describe_topic_partitions_matcher(&["test-0", "test-1", "test-2"], None),
            describe_topic_partitions_resp(data_first_part),
        );

        let mut data_second_part = DescribeTopicPartitionsResponseData::new();
        add_partition_to_describe_topic_partitions_response(
            &mut data_second_part,
            topic_name1,
            topics[topic_name1],
            &[1],
        );
        add_partition_to_describe_topic_partitions_response(
            &mut data_second_part,
            topic_name2,
            topics[topic_name2],
            &[0],
        );
        set_next_cursor(&mut data_second_part, topic_name2, 1);
        runnable.client_mut().prepare_response_matcher(
            describe_topic_partitions_matcher(&["test-1", "test-2"], Some(("test-1", 1))),
            describe_topic_partitions_resp(data_second_part),
        );

        let mut data_third_part = DescribeTopicPartitionsResponseData::new();
        add_partition_to_describe_topic_partitions_response(
            &mut data_third_part,
            topic_name2,
            topics[topic_name2],
            &[1],
        );
        runnable.client_mut().prepare_response_matcher(
            describe_topic_partitions_matcher(&["test-2"], Some(("test-2", 1))),
            describe_topic_partitions_resp(data_third_part),
        );

        let result = admin.describe_topics_with_topic_names_options(
            &[
                topic_name1.to_string(),
                topic_name0.to_string(),
                topic_name2.to_string(),
            ],
            DescribeTopicsOptions::new(),
        );
        pump_until_described(&mut runnable, &result).await;
        let topic_descriptions = result.all_topic_names().unwrap().get().await.unwrap();
        assert_eq!(topic_descriptions.len(), 3);
        let topic_description = &topic_descriptions[topic_name0];
        assert_eq!(topic_description.partitions().len(), 1);
        assert_eq!(topic_description.partitions()[0].partition(), 0);
        let topic_description = &topic_descriptions[topic_name1];
        assert_eq!(topic_description.partitions().len(), 2);
        let topic_description = &topic_descriptions[topic_name2];
        assert_eq!(topic_description.partitions().len(), 2);
        assert_eq!(topic_description.authorized_operations(), None);
        assert!(!runnable.client_mut().has_pending_responses());
    }

    /// Translated from
    /// `KafkaAdminClientTest.testDescribeTopicsWithDescribeTopicPartitionsApiErrorHandling`:
    /// a topic error (29, `TOPIC_AUTHORIZATION_FAILED`) fails that topic, and so
    /// `allTopicNames()`.
    #[tokio::test]
    async fn test_describe_topics_with_describe_topic_partitions_api_error_handling() {
        let (admin, mut runnable, _time, nodes) = env();
        let topic_name0 = "test-0";
        let topic_name1 = "test-1";
        let topics: HashMap<&str, Uuid> =
            HashMap::from([(topic_name0, Uuid::random_uuid()), (topic_name1, Uuid::random_uuid())]);

        prepare_describe_cluster(&mut runnable, &nodes, MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED);

        let mut data_first_part = DescribeTopicPartitionsResponseData::new();
        add_partition_to_describe_topic_partitions_response(
            &mut data_first_part,
            topic_name0,
            topics[topic_name0],
            &[0],
        );
        let mut failed = DescribeTopicPartitionsResponseTopic::new();
        failed
            .set_error_code(29)
            .set_topic_id(Uuid::ZERO_UUID)
            .set_name(Some(topic_name1.to_string()))
            .set_is_internal(false);
        data_first_part.topics.push(failed);
        runnable.client_mut().prepare_response_matcher(
            describe_topic_partitions_matcher(&["test-0", "test-1"], None),
            describe_topic_partitions_resp(data_first_part),
        );
        let result = admin.describe_topics_with_topic_names_options(
            &[topic_name1.to_string(), topic_name0.to_string()],
            DescribeTopicsOptions::new(),
        );
        pump_until_described(&mut runnable, &result).await;

        let error = result.all_topic_names().unwrap().get().await.unwrap_err();
        assert!(matches!(error, Error::TopicAuthorization(_)), "got {error:?}");
        assert_eq!(error.message(), Errors::TopicAuthorizationFailed.message());
        // The other topic completed normally, and no further page was requested.
        let described = result.topic_name_values().unwrap()[topic_name0].get().await.unwrap();
        assert_eq!(described.partitions().len(), 1);
        assert_eq!(runnable.client_mut().request_count(), 0);
    }

    /// `partitionSizeLimitPerResponse` is the request's `ResponsePartitionLimit`
    /// on every page, and the cursor points at the next partition of the topic
    /// being paged through. Java's tests never set the option; this pins that it
    /// is read.
    #[tokio::test]
    async fn describe_topics_sends_the_partition_size_limit_on_every_page() {
        let (admin, mut runnable, _time, nodes) = env();
        let topic_id = Uuid::random_uuid();
        prepare_describe_cluster(&mut runnable, &nodes, MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED);
        for (page, cursor) in [(0, None), (1, Some(("big", 1))), (2, Some(("big", 2)))] {
            let mut data = DescribeTopicPartitionsResponseData::new();
            add_partition_to_describe_topic_partitions_response(&mut data, "big", topic_id, &[page]);
            if page < 2 {
                set_next_cursor(&mut data, "big", page + 1);
            }
            let matches_topics = describe_topic_partitions_matcher(&["big"], cursor);
            runnable.client_mut().prepare_response_matcher(
                Box::new(move |body: &AbstractRequest| {
                    matches_topics(body)
                        && describe_topic_partitions_data(body).is_some_and(|r| r.response_partition_limit == 1)
                }),
                describe_topic_partitions_resp(data),
            );
        }

        let result = admin.describe_topics_with_topic_names_options(
            &["big".to_string()],
            DescribeTopicsOptions::new().set_partition_size_limit_per_response(1),
        );
        pump_until_described(&mut runnable, &result).await;
        let described = result.topic_name_values().unwrap()["big"].get().await.unwrap();
        let partitions: Vec<i32> = described.partitions().iter().map(TopicPartitionInfo::partition).collect();
        assert_eq!(partitions, vec![0, 1, 2]);
        assert!(!runnable.client_mut().has_pending_responses());
    }

    /// On `UnsupportedVersionException` (a broker without KIP-966)
    /// `describeTopicPartitions` issues the Metadata-API `describeTopics` call
    /// and fails itself without failing the futures
    /// (`KafkaAdminClient.java:2311-2323`). The Metadata call then completes
    /// them, including its own "not found" for a topic missing from the cluster.
    #[tokio::test]
    async fn describe_topics_falls_back_to_metadata_on_unsupported_version() {
        let (admin, mut runnable, _time, nodes) = env();
        prepare_describe_cluster(&mut runnable, &nodes, MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED);
        runnable.client_mut().prepare_unsupported_version_response();
        runnable.client_mut().prepare_response_matcher(
            Box::new(|body: &AbstractRequest| match body {
                AbstractRequest::Metadata(request) => {
                    let names: Vec<&str> = request
                        .data()
                        .topics
                        .iter()
                        .flatten()
                        .filter_map(|t| t.name.as_deref())
                        .collect();
                    names == ["myTopic", "nope"] && !request.data().allow_auto_topic_creation
                },
                _ => false,
            }),
            metadata_resp(&nodes, vec![topic_meta("myTopic", false, Uuid::new(0, 9), 2)]),
        );

        let result = admin.describe_topics_with_topic_names_options(
            &["myTopic".to_string(), "nope".to_string()],
            DescribeTopicsOptions::new(),
        );
        pump_until_described(&mut runnable, &result).await;
        let my = result.topic_name_values().unwrap()["myTopic"].get().await.unwrap();
        assert_eq!(my.name(), "myTopic");
        assert_eq!(my.partitions().len(), 2);
        assert_eq!(my.topic_id(), Uuid::new(0, 9));
        // The Metadata API carries no ELR information.
        assert_eq!(my.partitions()[0].elr(), None);
        let err = result.topic_name_values().unwrap()["nope"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnknownTopicOrPartition);
        assert_eq!(err.message(), "Topic nope not found.");
        assert!(!runnable.client_mut().has_pending_responses());
    }

    /// Regression for COMMENTS.66 Issue 6: an `UnsupportedVersionException` for
    /// `describeTopicPartitions` during the `close(timeout)` grace period.
    ///
    /// In Java `Call.fail` still reaches `handleUnsupportedVersionException`
    /// then (`runnable.closing` is only set once the I/O thread exits), which
    /// issues the Metadata fallback through `runnable.call`; `call()` rejects it
    /// because the hard-shutdown deadline is set, and every topic future fails
    /// with `IllegalStateException("Cannot accept new calls when AdminClient is
    /// closing.")` (`KafkaAdminClient.java:904-920`, `:2311-2323`, `:1598-1601`).
    /// Rust's `fail_call` skips the hook while closing, so the
    /// failure hook issues the fallback itself; before that it swallowed the
    /// code-35 error and the futures never completed.
    #[tokio::test]
    async fn an_unsupported_version_during_close_fails_the_by_name_describe() {
        let (admin, mut runnable, _time, nodes) = env();
        prepare_describe_cluster(&mut runnable, &nodes, MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED);
        let result = admin.describe_topics_with_topic_names_options(&["t".to_string()], DescribeTopicsOptions::new());
        let future = result.topic_name_values().unwrap()["t"].clone();
        // describeCluster is answered, which queues describeTopicPartitions.
        pump_until(&mut runnable, 20, |r| !r.client_mut().has_pending_responses()).await;
        assert!(!future.is_done());

        // `close(30s)` with describeTopicPartitions queued. No task was spawned,
        // so this only publishes the deadline and the closing flag.
        admin.close_with_timeout(Duration::from_secs(30)).await;
        // The broker does not support DescribeTopicPartitions.
        runnable.client_mut().prepare_unsupported_version_response();
        pump(&mut runnable, 10).await;
        assert!(
            !runnable.client_mut().has_pending_responses(),
            "the UnsupportedVersion response was consumed"
        );

        let error = tokio::time::timeout(Duration::from_secs(1), future.get())
            .await
            .expect("the topic future must complete, not hang")
            .expect_err("the rejected fallback fails the topic");
        assert!(matches!(error, Error::LocalIllegalState(_)), "got {error:?}");
        assert_eq!(error.message(), "Cannot accept new calls when AdminClient is closing.");
        assert!(!runnable.has_active_external_calls_for_test());
        assert_eq!(runnable.client_mut().request_count(), 0, "no Metadata fallback was sent");
    }

    /// A failed `describeCluster` fails every topic future with its error
    /// (`completeAllExceptionally(topicFutures.values(), exception)`), and no
    /// `DescribeTopicPartitions` request is sent.
    #[tokio::test]
    async fn describe_topics_fails_every_topic_when_describe_cluster_fails() {
        let (admin, mut runnable, _time, _nodes) = env();
        let mut data = DescribeClusterResponseData::new();
        data.set_error_code(Errors::ClusterAuthorizationFailed.code());
        data.set_error_message(Some("not allowed".to_string()));
        runnable
            .client_mut()
            .prepare_response(ConcreteResponse::DescribeCluster(DescribeClusterResponse::new(data)));

        let result = admin.describe_topics_with_topic_names_options(
            &["a".to_string(), "b".to_string()],
            DescribeTopicsOptions::new(),
        );
        pump_until_described(&mut runnable, &result).await;
        for name in ["a", "b"] {
            let err = result.topic_name_values().unwrap()[name].get().await.unwrap_err();
            assert_eq!(err.error(), Errors::ClusterAuthorizationFailed, "{name}");
            assert_eq!(err.message(), "not allowed", "{name}");
        }
        pump(&mut runnable, 3).await;
        assert_eq!(runnable.client_mut().request_count(), 0);
    }

    /// Translated from `KafkaAdminClientTest.testDescribeTopicsTimeoutWhenNoBrokerResponds`:
    /// with no response, the topic future fails with a `TimeoutException` once
    /// the 200 ms `timeoutMs` passes. What times out is the `describeCluster`
    /// call (Java's `"listNodes"`), whose deadline is derived from the same
    /// option, so its name is in the message.
    #[tokio::test]
    async fn test_describe_topics_timeout_when_no_broker_responds() {
        let (admin, mut runnable, time, _nodes) =
            env_nodes_with_props(1, &[("retries", "0"), ("request.timeout.ms", "30000")]);
        let start = time.milliseconds();
        let result = admin.describe_topics_with_topic_names_options(
            &["test-topic".to_string()],
            DescribeTopicsOptions::new().set_timeout_ms(Some(200)),
        );
        let topic_description = result.topic_name_values().unwrap()["test-topic"].clone();
        pump_until_request_queued(&mut runnable).await;
        time.sleep(201);
        pump_until(&mut runnable, 20, |_| topic_description.is_done()).await;
        let error = topic_description.get().await.unwrap_err();
        assert!(matches!(error, Error::Timeout(_)), "got {error:?}");
        let now = time.milliseconds();
        assert!(now - start >= 150, "the timeout fired at {}", now - start);
        assert!(
            error.message().starts_with(&format!(
                "Call(callName=listNodes, deadlineMs={}, tries=1, nextAllowedTryMs=",
                start + 200
            )),
            "{}",
            error.message()
        );
        assert!(
            error.message().ends_with(&format!(") timed out at {now} after 1 attempt(s)")),
            "{}",
            error.message()
        );
    }

    /// The `describeTopicPartitions` call's own timeout names it, as Java's
    /// `Call("describeTopicPartitions", ..)` does. The Metadata-API path used to
    /// answer by name, so its timeouts said `describeTopics`.
    #[tokio::test]
    async fn describe_topic_partitions_timeout_names_the_call() {
        let (admin, mut runnable, time, nodes) =
            env_nodes_with_props(1, &[("retries", "0"), ("request.timeout.ms", "30000")]);
        prepare_describe_cluster(&mut runnable, &nodes, MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED);
        let result = admin.describe_topics_with_topic_names_options(
            &["test-topic".to_string()],
            DescribeTopicsOptions::new().set_timeout_ms(Some(200)),
        );
        let topic_description = result.topic_name_values().unwrap()["test-topic"].clone();
        // describeCluster is answered; describeTopicPartitions is left in flight.
        pump_until(&mut runnable, 20, |r| {
            r.client_mut()
                .requests()
                .iter()
                .any(|request| request.api_key() == &ApiKeys::DESCRIBE_TOPIC_PARTITIONS)
        })
        .await;
        time.sleep(201);
        pump_until(&mut runnable, 20, |_| topic_description.is_done()).await;
        let error = topic_description.get().await.unwrap_err();
        assert!(matches!(error, Error::Timeout(_)), "got {error:?}");
        assert!(
            error.message().starts_with("Call(callName=describeTopicPartitions, "),
            "{}",
            error.message()
        );
        let now = time.milliseconds();
        assert!(
            error.message().ends_with(&format!(") timed out at {now} after 1 attempt(s)")),
            "{}",
            error.message()
        );
    }

    /// `testInvalidTopicNames`' describe half: names that cannot be represented
    /// fail at once with Java's message, and no request — not even the
    /// `describeCluster` prerequisite — is sent.
    #[tokio::test]
    async fn describe_topics_with_only_invalid_names_sends_nothing() {
        let (admin, mut runnable, _time, _nodes) = env();
        let result = admin.describe_topics_with_topic_names_options(&[String::new()], DescribeTopicsOptions::new());
        let err = result.topic_name_values().unwrap()[""].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidTopicError);
        assert_eq!(err.message(), "The given topic name '' cannot be represented in a request.");
        pump(&mut runnable, 3).await;
        assert_eq!(runnable.client_mut().request_count(), 0);
    }

    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeTopicsByIds")]
    async fn test_describe_topics_by_ids() {
        // Valid id: the metadata response carries the topic, so it is described.
        let (admin, mut runnable, _time, nodes) = env();
        let topic_id = Uuid::new(7, 7);
        let topics = vec![topic_meta("test-topic", false, topic_id, 1)];
        runnable
            .client_mut()
            .prepare_response(ConcreteResponse::Metadata(RequestTestUtils::metadata_response(
                &nodes,
                Some("mock-cluster"),
                0,
                topics,
            )));
        let result = admin.describe_topics_with_topics_options(
            TopicCollection::of_topic_ids(vec![topic_id]),
            DescribeTopicsOptions::new(),
        );
        pump(&mut runnable, 5).await;
        let all = result.all_topic_ids().unwrap().get().await.unwrap();
        assert_eq!(all[&topic_id].name(), "test-topic");

        // Id not present in the brokers: UnknownTopicId with the Java message.
        let (admin, mut runnable, _time, nodes) = env();
        let non_exist = Uuid::new(9, 9);
        runnable
            .client_mut()
            .prepare_response(ConcreteResponse::Metadata(RequestTestUtils::metadata_response(
                &nodes,
                Some("mock-cluster"),
                0,
                Vec::new(),
            )));
        let result = admin.describe_topics_with_topics_options(
            TopicCollection::of_topic_ids(vec![non_exist]),
            DescribeTopicsOptions::new(),
        );
        pump(&mut runnable, 5).await;
        let err = result.all_topic_ids().unwrap().get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnknownTopicId);
        assert_eq!(err.message(), format!("TopicId {non_exist} not found."));

        // The zero id cannot be represented in a request; no request is sent.
        let (admin, _runnable, _time, _nodes) = env();
        let result = admin.describe_topics_with_topics_options(
            TopicCollection::of_topic_ids(vec![Uuid::ZERO_UUID]),
            DescribeTopicsOptions::new(),
        );
        let err = result.all_topic_ids().unwrap().get().await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidTopicError);
        assert_eq!(
            err.message(),
            "The given topic id 'AAAAAAAAAAAAAAAAAAAAAA' cannot be represented in a request."
        );
    }

    /// A zero topic id in a by-id Metadata response fails
    /// only that call.
    ///
    /// A 4.x broker returns the zero id for a topic deleted while a by-id
    /// `describeTopics` is being answered: it resolves the id to a name, then
    /// finds the topic gone and builds the entry with
    /// `metadataCache.getTopicId(topic)` (`KafkaApis.scala:866-871`), which is
    /// now the zero id. Java's `MetadataResponse.errorsByTopicId()` throws
    /// `IllegalStateException("Use errors() when managing topic using topic
    /// name")` (`MetadataResponse.java:118-120`), and `handleResponses` catches
    /// it with `call.fail(now, t)` (`KafkaAdminClient.java:1394-1403`). The
    /// error is not retriable, so only that call fails and the client goes on.
    /// Rust used to `assert!`, which ended the whole I/O task: the call never
    /// resolved, and every later call was rejected.
    #[tokio::test]
    async fn a_zero_topic_id_in_a_by_id_describe_fails_only_that_call() {
        let (admin, mut runnable, _time, nodes) = env();
        let requested = Uuid::new(7, 7);
        // The topic was deleted mid-request: the broker reports it under its
        // name with the zero id and an error.
        let mut deleted = topic_meta("deleted-topic", false, Uuid::ZERO_UUID, 0);
        deleted.error = Errors::UnknownTopicOrPartition;
        runnable.client_mut().prepare_response(metadata_resp(&nodes, vec![deleted]));

        let result = admin.describe_topics_with_topics_options(
            TopicCollection::of_topic_ids(vec![requested]),
            DescribeTopicsOptions::new(),
        );
        let future = result.topic_id_values().unwrap()[&requested].clone();
        pump_until(&mut runnable, 20, |_| future.is_done()).await;
        let error = future.get().await.expect_err("the zero topic id must fail the call");
        assert!(matches!(error, Error::LocalIllegalState(_)), "got {error:?}");
        assert_eq!(error.message(), "Use errors() when managing topic using topic name");

        // The client is still usable: an unrelated call on the same runnable
        // succeeds.
        runnable
            .client_mut()
            .prepare_response(metadata_resp(&nodes, vec![topic_meta("other", false, Uuid::new(8, 8), 1)]));
        let names = admin.list_topics_with_options(ListTopicsOptions::new()).names();
        pump_until(&mut runnable, 20, |_| names.is_done()).await;
        let names = names.get().await.expect("a later listTopics must succeed");
        assert_eq!(names, HashSet::from(["other".to_string()]));
    }

    #[tokio::test]
    async fn test_create_topics_response_config_metadata() {
        use crate::create_topics_response_data::CreatableTopicConfigs;
        let (admin, mut runnable, _time, _nodes) = env();
        let result = admin.create_topics_with_options(
            &[NewTopic::with_num_partitions_replication_factor(
                "myTopic",
                Some(1),
                Some(1),
            )],
            CreateTopicsOptions::new().set_validate_only(true),
        );
        let mut config = CreatableTopicConfigs::new();
        config.set_name("cleanup.policy".to_string());
        config.set_value(Some("compact".to_string()));
        config.set_read_only(true);
        config.set_is_sensitive(false);
        config.set_config_source(1); // DYNAMIC_TOPIC_CONFIG
        let mut r = create_result("myTopic", Errors::None, None);
        r.set_configs(Some(vec![config]));
        runnable.client_mut().prepare_response(create_response(vec![r]));
        pump(&mut runnable, 5).await;
        let cfg = result.config("myTopic").get().await.unwrap();
        let entry = cfg.get("cleanup.policy").expect("cleanup.policy present");
        assert_eq!(entry.value(), Some("compact"));
        assert!(entry.is_read_only());
        assert!(!entry.is_sensitive());
        assert_eq!(entry.source(), ConfigSource::DynamicTopicConfig);
    }

    #[tokio::test]
    async fn test_create_topics_invalid_name_unrepresentable() {
        let (admin, _runnable, _time, _nodes) = env();
        let result = admin.create_topics_with_options(
            &[NewTopic::with_num_partitions_replication_factor("", Some(1), Some(1))],
            CreateTopicsOptions::new(),
        );
        let err = result.values()[""].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidTopicError);
        assert_eq!(err.message(), "The given topic name '' cannot be represented in a request.");
    }

    #[tokio::test]
    async fn test_delete_topics_invalid_name_unrepresentable() {
        let (admin, _runnable, _time, _nodes) = env();
        let result = admin.delete_topics_with_options(
            TopicCollection::of_topic_names(vec![String::new()]),
            DeleteTopicsOptions::new(),
        );
        let err = result.topic_name_values().unwrap()[""].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidTopicError);
        assert_eq!(err.message(), "The given topic name '' cannot be represented in a request.");
    }

    // --- createPartitions ----------------------------------------------------

    fn create_partitions_result_item(name: &str, error: Errors, msg: Option<&str>) -> CreatePartitionsTopicResult {
        let mut r = CreatePartitionsTopicResult::new();
        r.set_name(name.to_string());
        r.set_error_code(error.code());
        r.set_error_message(msg.map(str::to_string));
        r
    }

    fn create_partitions_response(throttle_ms: i32, results: Vec<CreatePartitionsTopicResult>) -> ConcreteResponse {
        let mut data = CreatePartitionsResponseData::new();
        data.set_throttle_time_ms(throttle_ms);
        data.set_results(results);
        ConcreteResponse::CreatePartitions(CreatePartitionsResponse::new(data))
    }

    fn new_partitions_counts() -> HashMap<String, NewPartitions> {
        let mut counts = HashMap::new();
        counts.insert("my_topic".to_string(), NewPartitions::increase_to(3));
        counts.insert(
            "other_topic".to_string(),
            NewPartitions::increase_to_new_assignments(3, vec![vec![2], vec![3]]),
        );
        counts
    }

    /// Mirrors `KafkaAdminClientTest.testCreatePartitions`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testCreatePartitions")]
    async fn test_create_partitions() {
        let (admin, mut runnable, _time, _nodes) = env();
        let result = admin.create_partitions_with_options(&new_partitions_counts(), CreatePartitionsOptions::new());
        runnable.client_mut().prepare_response(create_partitions_response(
            1000,
            vec![
                create_partitions_result_item("my_topic", Errors::None, None),
                create_partitions_result_item("other_topic", Errors::InvalidTopicError, Some("some detailed reason")),
            ],
        ));
        pump(&mut runnable, 5).await;
        result.values()["my_topic"].get().await.unwrap();
        let err = result.values()["other_topic"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidTopicError);
        assert_eq!(err.message(), "some detailed reason");
    }

    /// Mirrors `KafkaAdminClientTest.testCreatePartitionsRetryThrottlingExceptionWhenEnabled`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testCreatePartitionsRetryThrottlingExceptionWhenEnabled"
    )]
    async fn test_create_partitions_retry_throttling_error_when_enabled() {
        let (admin, mut runnable, _time, _nodes) = env();
        runnable.client_mut().prepare_response(create_partitions_response(
            1000,
            vec![
                create_partitions_result_item("topic1", Errors::None, None),
                create_partitions_result_item("topic2", Errors::ThrottlingQuotaExceeded, None),
                create_partitions_result_item("topic3", Errors::TopicAlreadyExists, None),
            ],
        ));
        runnable.client_mut().prepare_response(create_partitions_response(
            1000,
            vec![create_partitions_result_item(
                "topic2",
                Errors::ThrottlingQuotaExceeded,
                None,
            )],
        ));
        runnable.client_mut().prepare_response(create_partitions_response(
            0,
            vec![create_partitions_result_item("topic2", Errors::None, None)],
        ));

        let mut counts = HashMap::new();
        counts.insert("topic1".to_string(), NewPartitions::increase_to(1));
        counts.insert("topic2".to_string(), NewPartitions::increase_to(2));
        counts.insert("topic3".to_string(), NewPartitions::increase_to(3));
        let result = admin
            .create_partitions_with_options(&counts, CreatePartitionsOptions::new().set_retry_on_quota_violation(true));

        pump_until(&mut runnable, 30, |r| r.client_mut().num_awaiting_responses() == 0).await;
        result.values()["topic1"].get().await.unwrap();
        result.values()["topic2"].get().await.unwrap();
        let err = result.values()["topic3"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::TopicAlreadyExists);
    }

    /// Mirrors `KafkaAdminClientTest.testCreatePartitionsDontRetryThrottlingExceptionWhenDisabled`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testCreatePartitionsDontRetryThrottlingExceptionWhenDisabled"
    )]
    async fn test_create_partitions_dont_retry_throttling_error_when_disabled() {
        let (admin, mut runnable, _time, _nodes) = env();
        runnable.client_mut().prepare_response(create_partitions_response(
            1000,
            vec![
                create_partitions_result_item("topic1", Errors::None, None),
                create_partitions_result_item("topic2", Errors::ThrottlingQuotaExceeded, None),
                create_partitions_result_item("topic3", Errors::TopicAlreadyExists, None),
            ],
        ));
        let mut counts = HashMap::new();
        counts.insert("topic1".to_string(), NewPartitions::increase_to(1));
        counts.insert("topic2".to_string(), NewPartitions::increase_to(2));
        counts.insert("topic3".to_string(), NewPartitions::increase_to(3));
        let result = admin.create_partitions_with_options(
            &counts,
            CreatePartitionsOptions::new().set_retry_on_quota_violation(false),
        );

        pump(&mut runnable, 5).await;
        result.values()["topic1"].get().await.unwrap();
        let err = result.values()["topic2"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::ThrottlingQuotaExceeded);
        assert_eq!(err.throttle_time_ms(), Some(1000));
        let err3 = result.values()["topic3"].get().await.unwrap_err();
        assert_eq!(err3.error(), Errors::TopicAlreadyExists);
    }

    /// Mirrors `KafkaAdminClientTest.testCreatePartitionsRetryThrottlingExceptionWhenEnabledUntilRequestTimeOut`.
    #[tokio::test]
    async fn test_create_partitions_retry_throttling_error_when_enabled_until_request_timeout() {
        let default_api_timeout: i64 = 60000;
        let (admin, mut runnable, time, _nodes) =
            env_with_props(&[("default.api.timeout.ms", &default_api_timeout.to_string())]);
        runnable.client_mut().prepare_response(create_partitions_response(
            1000,
            vec![
                create_partitions_result_item("topic1", Errors::None, None),
                create_partitions_result_item("topic2", Errors::ThrottlingQuotaExceeded, None),
                create_partitions_result_item("topic3", Errors::TopicAlreadyExists, None),
            ],
        ));
        runnable.client_mut().prepare_response(create_partitions_response(
            1000,
            vec![create_partitions_result_item(
                "topic2",
                Errors::ThrottlingQuotaExceeded,
                None,
            )],
        ));
        let mut counts = HashMap::new();
        counts.insert("topic1".to_string(), NewPartitions::increase_to(1));
        counts.insert("topic2".to_string(), NewPartitions::increase_to(2));
        counts.insert("topic3".to_string(), NewPartitions::increase_to(3));
        let result = admin
            .create_partitions_with_options(&counts, CreatePartitionsOptions::new().set_retry_on_quota_violation(true));

        pump_until(&mut runnable, 30, |r| {
            !r.client_mut().has_pending_responses() && r.client_mut().request_count() >= 1
        })
        .await;
        time.sleep(default_api_timeout + 1);
        pump_until(&mut runnable, 30, |_r| result.values()["topic2"].is_done()).await;
        result.values()["topic1"].get().await.unwrap();
        let err = result.values()["topic2"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::ThrottlingQuotaExceeded);
        assert_eq!(err.throttle_time_ms(), Some(0));
        let err3 = result.values()["topic3"].get().await.unwrap_err();
        assert_eq!(err3.error(), Errors::TopicAlreadyExists);
    }

    // --- deleteRecords -------------------------------------------------------

    fn topic_meta_error(name: &str, error: Errors) -> TopicMetadata {
        TopicMetadata {
            error,
            topic: name.to_string(),
            topic_id: Uuid::ZERO_UUID,
            is_internal: false,
            partition_metadata: Vec::new(),
            authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
        }
    }

    fn topic_meta_leaders(name: &str, leaders: &[(i32, i32)]) -> TopicMetadata {
        let partition_metadata = leaders
            .iter()
            .map(|(partition, leader)| PartitionMetadata {
                error: Errors::None,
                topic_partition: TopicPartition::new(name.to_string(), *partition),
                leader_id: Some(*leader),
                leader_epoch: Some(0),
                replica_ids: vec![*leader],
                in_sync_replica_ids: vec![*leader],
                offline_replica_ids: vec![],
            })
            .collect();
        TopicMetadata {
            error: Errors::None,
            topic: name.to_string(),
            topic_id: Uuid::ZERO_UUID,
            is_internal: false,
            partition_metadata,
            authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
        }
    }

    fn metadata_resp(nodes: &[Node], topics: Vec<TopicMetadata>) -> ConcreteResponse {
        ConcreteResponse::Metadata(RequestTestUtils::metadata_response(nodes, Some("mock-cluster"), 0, topics))
    }

    fn delete_records_partition(index: i32, error: Errors, low_watermark: i64) -> DeleteRecordsPartitionResult {
        let mut p = DeleteRecordsPartitionResult::new();
        p.set_partition_index(index);
        p.set_error_code(error.code());
        p.set_low_watermark(low_watermark);
        p
    }

    fn delete_records_resp(topic: &str, partitions: Vec<DeleteRecordsPartitionResult>) -> ConcreteResponse {
        let mut topic_result = DeleteRecordsTopicResult::new();
        topic_result.set_name(topic.to_string());
        topic_result.set_partitions(partitions);
        let mut data = DeleteRecordsResponseData::new();
        data.set_topics(vec![topic_result]);
        ConcreteResponse::DeleteRecords(DeleteRecordsResponse::new(data))
    }

    /// Mirrors `KafkaAdminClientTest.testDeleteRecords`: two retriable metadata
    /// lookups precede a successful one, then a fulfillment response carries a
    /// success, an offset-out-of-range error, an authorization failure, and a
    /// missing partition (sanity-check failure).
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDeleteRecords")]
    async fn test_delete_records() {
        let (admin, mut runnable, _time, nodes) = env();
        // Lookup retries: LEADER_NOT_AVAILABLE, then UNKNOWN_TOPIC_OR_PARTITION
        // (tolerated), then success mapping all partitions to node0.
        runnable.client_mut().prepare_response(metadata_resp(
            &nodes,
            vec![topic_meta_error("my_topic", Errors::LeaderNotAvailable)],
        ));
        runnable.client_mut().prepare_response(metadata_resp(
            &nodes,
            vec![topic_meta_error("my_topic", Errors::UnknownTopicOrPartition)],
        ));
        runnable.client_mut().prepare_response(metadata_resp(
            &nodes,
            vec![topic_meta_leaders("my_topic", &[(0, 0), (1, 0), (2, 0), (3, 0)])],
        ));
        runnable.client_mut().prepare_response(delete_records_resp(
            "my_topic",
            vec![
                delete_records_partition(0, Errors::None, 3),
                delete_records_partition(1, Errors::OffsetOutOfRange, -1),
                delete_records_partition(2, Errors::TopicAuthorizationFailed, -1),
                // partition 3 omitted → sanity-check failure
            ],
        ));

        let mut records = HashMap::new();
        records.insert(
            TopicPartition::new("my_topic", 0),
            RecordsToDelete::before_offset_with_offset(3),
        );
        records.insert(
            TopicPartition::new("my_topic", 1),
            RecordsToDelete::before_offset_with_offset(10),
        );
        records.insert(
            TopicPartition::new("my_topic", 2),
            RecordsToDelete::before_offset_with_offset(10),
        );
        records.insert(
            TopicPartition::new("my_topic", 3),
            RecordsToDelete::before_offset_with_offset(10),
        );
        let result = admin.delete_records_with_options(&records, DeleteRecordsOptions::new());

        let values = result.low_watermarks();
        pump_until(&mut runnable, 40, |_r| values.values().all(|f| f.is_done())).await;

        assert_eq!(
            values[&TopicPartition::new("my_topic", 0)].get().await.unwrap().low_watermark(),
            3
        );
        assert_eq!(
            values[&TopicPartition::new("my_topic", 1)].get().await.unwrap_err().error(),
            Errors::OffsetOutOfRange
        );
        assert_eq!(
            values[&TopicPartition::new("my_topic", 2)].get().await.unwrap_err().error(),
            Errors::TopicAuthorizationFailed
        );
        let p3_err = values[&TopicPartition::new("my_topic", 3)].get().await.unwrap_err();
        assert!(p3_err.message().contains("did not contain a result for topic partition"));
    }

    /// Mirrors `KafkaAdminClientTest.testDeleteRecordsTopicAuthorizationError`: a
    /// topic-level authorization failure during lookup fails the partition.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDeleteRecordsTopicAuthorizationError")]
    async fn test_delete_records_topic_authorization_error() {
        let (admin, mut runnable, _time, nodes) = env();
        runnable.client_mut().prepare_response(metadata_resp(
            &nodes,
            vec![topic_meta_error("foo", Errors::TopicAuthorizationFailed)],
        ));

        let mut records = HashMap::new();
        records.insert(TopicPartition::new("foo", 0), RecordsToDelete::before_offset_with_offset(10));
        let result = admin.delete_records_with_options(&records, DeleteRecordsOptions::new());

        let values = result.low_watermarks();
        pump_until(&mut runnable, 20, |_r| values.values().all(|f| f.is_done())).await;
        let err = values[&TopicPartition::new("foo", 0)].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::TopicAuthorizationFailed);
    }

    /// Mirrors `KafkaAdminClientTest.testDeleteRecordsMultipleSends`: partitions
    /// spread across two leaders produce two fulfillment requests, and each
    /// broker's result completes independently.
    ///
    /// Deviation: Java fails one broker with a pending `SaslAuthenticationException`
    /// (a connection-level failure). `MockClient` cannot simulate a pending
    /// authentication error (its `authentication_error` always returns `None`),
    /// so this port substitutes a per-partition fatal error on the second broker
    /// to exercise the same multi-broker fan-out and independent completion.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDeleteRecordsMultipleSends")]
    async fn test_delete_records_multiple_sends() {
        let (admin, mut runnable, _time, nodes) = env();
        // tp0 -> node0, tp1 -> node1.
        runnable
            .client_mut()
            .prepare_response(metadata_resp(&nodes, vec![topic_meta_leaders("foo", &[(0, 0), (1, 1)])]));
        runnable.client_mut().prepare_response_from(
            delete_records_resp("foo", vec![delete_records_partition(0, Errors::None, 3)]),
            &nodes[0],
        );
        runnable.client_mut().prepare_response_from(
            delete_records_resp("foo", vec![delete_records_partition(1, Errors::TopicAuthorizationFailed, -1)]),
            &nodes[1],
        );

        let mut records = HashMap::new();
        records.insert(TopicPartition::new("foo", 0), RecordsToDelete::before_offset_with_offset(10));
        records.insert(TopicPartition::new("foo", 1), RecordsToDelete::before_offset_with_offset(10));
        let result = admin.delete_records_with_options(&records, DeleteRecordsOptions::new());

        let values = result.low_watermarks();
        pump_until(&mut runnable, 40, |_r| values.values().all(|f| f.is_done())).await;
        assert_eq!(values[&TopicPartition::new("foo", 0)].get().await.unwrap().low_watermark(), 3);
        assert_eq!(
            values[&TopicPartition::new("foo", 1)].get().await.unwrap_err().error(),
            Errors::TopicAuthorizationFailed
        );
    }

    /// The mock's `delete_records` returns an empty result for an empty request.
    #[tokio::test]
    async fn test_mock_delete_records_empty() {
        let mock = mock_admin_client::Builder::new()
            .build()
            .expect("a fresh builder has one broker");
        let result = mock.delete_records_with_options(&HashMap::new(), DeleteRecordsOptions::new());
        assert!(result.low_watermarks().is_empty());
    }

    // --- describeProducers / abortTransaction --------------------------------

    use crate::DescribeProducersResponseData;
    use crate::WriteTxnMarkersResponseData;
    use crate::admin::ProducerState;
    use crate::admin::{AbortTransactionOptions, AbortTransactionSpec, DescribeProducersOptions};
    use crate::common::requests::{DescribeProducersResponse, WriteTxnMarkersResponse};
    use crate::describe_producers_response_data::{
        PartitionResponse as DpPartitionResponse, ProducerState as WireProducerState, TopicResponse as DpTopicResponse,
    };
    use crate::write_txn_markers_response_data::{
        WritableTxnMarkerPartitionResult, WritableTxnMarkerResult, WritableTxnMarkerTopicResult,
    };

    /// Mirrors `KafkaAdminClientTest.buildDescribeProducersResponse`.
    fn build_describe_producers_response(tp: &TopicPartition, states: &[ProducerState]) -> ConcreteResponse {
        let wire: Vec<WireProducerState> = states
            .iter()
            .map(|s| {
                let mut w = WireProducerState::new();
                w.set_producer_id(s.producer_id());
                w.set_producer_epoch(s.producer_epoch());
                w.set_last_sequence(s.last_sequence());
                w.set_last_timestamp(s.last_timestamp());
                w.set_coordinator_epoch(s.coordinator_epoch().unwrap_or(-1));
                w.set_current_txn_start_offset(s.current_transaction_start_offset().unwrap_or(-1));
                w
            })
            .collect();
        let mut partition_response = DpPartitionResponse::new();
        partition_response.set_partition_index(tp.partition());
        partition_response.set_error_code(Errors::None.code());
        partition_response.set_active_producers(wire);
        let mut topic_response = DpTopicResponse::new();
        topic_response.set_name(tp.topic().to_string());
        topic_response.set_partitions(vec![partition_response]);
        let mut data = DescribeProducersResponseData::new();
        data.set_topics(vec![topic_response]);
        ConcreteResponse::DescribeProducers(DescribeProducersResponse::new(data))
    }

    /// Mirrors `KafkaAdminClientTest.writeTxnMarkersResponse`.
    fn write_txn_markers_response(spec: &AbortTransactionSpec, error: Errors) -> ConcreteResponse {
        let mut partition = WritableTxnMarkerPartitionResult::new();
        partition.set_partition_index(spec.topic_partition().partition());
        partition.set_error_code(error.code());
        let mut topic = WritableTxnMarkerTopicResult::new();
        topic.set_name(spec.topic_partition().topic().to_string());
        topic.set_partitions(vec![partition]);
        let mut marker = WritableTxnMarkerResult::new();
        marker.set_producer_id(spec.producer_id());
        marker.set_topics(vec![topic]);
        let mut data = WriteTxnMarkersResponseData::new();
        data.set_markers(vec![marker]);
        ConcreteResponse::WriteTxnMarkers(WriteTxnMarkersResponse::new(data))
    }

    /// Mirrors `KafkaAdminClientTest.testDescribeProducers`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeProducers")]
    async fn test_describe_producers() {
        let (admin, mut runnable, time, nodes) = env();
        let tp = TopicPartition::new("foo", 0);

        // Metadata lookup maps foo-0 to node0 (the leader).
        runnable
            .client_mut()
            .prepare_response(metadata_resp(&nodes, vec![topic_meta_leaders("foo", &[(0, 0)])]));

        let expected = vec![
            ProducerState::new(12345, 15, 30, time.milliseconds(), Some(99), None),
            ProducerState::new(12345, 15, 30, time.milliseconds(), None, Some(23423)),
        ];
        runnable
            .client_mut()
            .prepare_response_from(build_describe_producers_response(&tp, &expected), &nodes[0]);

        let result = admin.describe_producers_with_options(std::slice::from_ref(&tp), DescribeProducersOptions::new());
        let partition_future = result.partition_result(&tp).unwrap();
        pump_until(&mut runnable, 40, |_r| partition_future.is_done()).await;
        let state = partition_future.get().await.unwrap();
        assert_eq!(
            state.active_producers().iter().cloned().collect::<HashSet<_>>(),
            expected.into_iter().collect::<HashSet<_>>()
        );
    }

    /// Mirrors `KafkaAdminClientTest.testDescribeProducersTimeout(boolean)`
    /// (`@ParameterizedTest` over `{true, false}` → loop).
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeProducersTimeout")]
    async fn test_describe_producers_timeout() {
        for timeout_in_metadata_lookup in [true, false] {
            let request_timeout_ms = 15000;
            let (admin, mut runnable, time, nodes) = env();
            let tp = TopicPartition::new("foo", 0);

            if !timeout_in_metadata_lookup {
                runnable
                    .client_mut()
                    .prepare_response(metadata_resp(&nodes, vec![topic_meta_leaders("foo", &[(0, 0)])]));
            }

            let options = DescribeProducersOptions::new().set_timeout_ms(Some(request_timeout_ms));
            let result = admin.describe_producers_with_options(std::slice::from_ref(&tp), options);
            let all = result.all();
            // Drain whatever is prepared, then confirm the request has not
            // completed before the timeout elapses.
            pump(&mut runnable, 5).await;
            assert!(
                !all.is_done(),
                "future completed before timeout (metadata_lookup={timeout_in_metadata_lookup})"
            );

            time.sleep(request_timeout_ms as i64 + 1);
            drive_until(&mut runnable, &time, 40, || all.is_done()).await;
            assert!(matches!(all.get().await.unwrap_err(), Error::Timeout(_)));
        }
    }

    /// Mirrors `KafkaAdminClientTest.testDescribeProducersRetryAfterDisconnect`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeProducersRetryAfterDisconnect")]
    async fn test_describe_producers_retry_after_disconnect() {
        let (admin, mut runnable, time, nodes) = env_with_props(&[("retry.backoff.ms", "100")]);
        let tp = TopicPartition::new("foo", 0);

        // Lookup maps to node0; the fulfillment disconnects; a fresh lookup maps
        // to node1; the retried fulfillment succeeds.
        runnable
            .client_mut()
            .prepare_response(metadata_resp(&nodes, vec![topic_meta_leaders("foo", &[(0, 0)])]));

        let expected = vec![
            ProducerState::new(12345, 15, 30, time.milliseconds(), Some(99), None),
            ProducerState::new(12345, 15, 30, time.milliseconds(), None, Some(23423)),
        ];
        runnable
            .client_mut()
            .prepare_response_disconnected(build_describe_producers_response(&tp, &expected), true);
        runnable
            .client_mut()
            .prepare_response(metadata_resp(&nodes, vec![topic_meta_leaders("foo", &[(0, 1)])]));
        runnable
            .client_mut()
            .prepare_response_from(build_describe_producers_response(&tp, &expected), &nodes[1]);

        let result = admin.describe_producers_with_options(std::slice::from_ref(&tp), DescribeProducersOptions::new());
        let partition_future = result.partition_result(&tp).unwrap();
        drive_until(&mut runnable, &time, 60, || partition_future.is_done()).await;
        let state = partition_future.get().await.unwrap();
        assert_eq!(
            state.active_producers().iter().cloned().collect::<HashSet<_>>(),
            expected.into_iter().collect::<HashSet<_>>()
        );
    }

    /// Mirrors `KafkaAdminClientTest.testAbortTransaction`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testAbortTransaction")]
    async fn test_abort_transaction() {
        let (admin, mut runnable, _time, nodes) = env();
        let tp = TopicPartition::new("foo", 13);
        let spec = AbortTransactionSpec::new(tp.clone(), 12345, 15, 200);

        runnable
            .client_mut()
            .prepare_response(metadata_resp(&nodes, vec![topic_meta_leaders("foo", &[(13, 0)])]));
        runnable
            .client_mut()
            .prepare_response_from(write_txn_markers_response(&spec, Errors::None), &nodes[0]);

        let result = admin.abort_transaction_with_options(spec, AbortTransactionOptions::new());
        let all = result.all();
        pump_until(&mut runnable, 40, |_r| all.is_done()).await;
        all.get().await.unwrap();
    }

    /// Mirrors `KafkaAdminClientTest.testAbortTransactionFindLeaderAfterDisconnect`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testAbortTransactionFindLeaderAfterDisconnect")]
    async fn test_abort_transaction_find_leader_after_disconnect() {
        let (admin, mut runnable, time, nodes) = env_with_props(&[("retry.backoff.ms", "100")]);
        let tp = TopicPartition::new("foo", 13);
        let spec = AbortTransactionSpec::new(tp.clone(), 12345, 15, 200);

        runnable
            .client_mut()
            .prepare_response(metadata_resp(&nodes, vec![topic_meta_leaders("foo", &[(13, 0)])]));
        runnable
            .client_mut()
            .prepare_response_disconnected(write_txn_markers_response(&spec, Errors::None), true);
        runnable
            .client_mut()
            .prepare_response(metadata_resp(&nodes, vec![topic_meta_leaders("foo", &[(13, 1)])]));
        runnable
            .client_mut()
            .prepare_response_from(write_txn_markers_response(&spec, Errors::None), &nodes[1]);

        let result = admin.abort_transaction_with_options(spec, AbortTransactionOptions::new());
        let all = result.all();
        drive_until(&mut runnable, &time, 60, || all.is_done()).await;
        all.get().await.unwrap();
    }

    /// The mock's `describe_producers` mirrors Java's `UnsupportedOperationException`.
    #[tokio::test]
    async fn test_mock_describe_producers_unsupported() {
        let mock = mock_admin_client::Builder::new()
            .build()
            .expect("a fresh builder has one broker");
        let tp = TopicPartition::new("foo", 0);
        let result = mock.describe_producers_with_options(std::slice::from_ref(&tp), DescribeProducersOptions::new());
        let err = result.partition_result(&tp).unwrap().get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnsupportedVersion);
    }

    /// The mock's `abort_transaction` mirrors Java's `UnsupportedOperationException`.
    #[tokio::test]
    async fn test_mock_abort_transaction_unsupported() {
        let mock = mock_admin_client::Builder::new()
            .build()
            .expect("a fresh builder has one broker");
        let spec = AbortTransactionSpec::new(TopicPartition::new("foo", 0), 1, 1, 1);
        let result = mock.abort_transaction_with_options(spec, AbortTransactionOptions::new());
        assert_eq!(result.all().get().await.unwrap_err().error(), Errors::UnsupportedVersion);
    }

    // --- describeTransactions / fenceProducers -------------------------------

    use crate::DescribeTransactionsResponseData;
    use crate::InitProducerIdResponseData;
    use crate::admin::{DescribeTransactionsOptions, FenceProducersOptions, TransactionDescription, TransactionState};
    use crate::common::requests::InitProducerIdResponse;
    use crate::describe_transactions_response_data::TransactionState as WireTxnState;

    fn describe_txn_state(
        transactional_id: &str,
        state: &str,
        producer_id: i64,
        producer_epoch: i16,
        timeout_ms: i32,
        start_time_ms: i64,
    ) -> WireTxnState {
        let mut s = WireTxnState::new();
        s.set_error_code(Errors::None.code());
        s.set_transactional_id(transactional_id.to_string());
        s.set_transaction_state(state.to_string());
        s.set_producer_id(producer_id);
        s.set_producer_epoch(producer_epoch);
        s.set_transaction_timeout_ms(timeout_ms);
        s.set_transaction_start_time_ms(start_time_ms);
        s
    }

    fn describe_transactions_resp(states: Vec<WireTxnState>) -> ConcreteResponse {
        let mut data = DescribeTransactionsResponseData::new();
        data.set_transaction_states(states);
        ConcreteResponse::DescribeTransactions(crate::common::requests::DescribeTransactionsResponse::new(data))
    }

    fn describe_txn_error_state(transactional_id: &str, error: Errors) -> WireTxnState {
        let mut s = WireTxnState::new();
        s.set_error_code(error.code());
        s.set_transactional_id(transactional_id.to_string());
        s
    }

    fn init_producer_id_resp(error: Errors, producer_id: i64, producer_epoch: i16) -> ConcreteResponse {
        let mut data = InitProducerIdResponseData::new();
        data.set_error_code(error.code());
        data.set_producer_id(producer_id);
        data.set_producer_epoch(producer_epoch);
        ConcreteResponse::InitProducerId(InitProducerIdResponse::new(data))
    }

    /// Mirrors `KafkaAdminClientTest.testDescribeTransactions`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeTransactions")]
    async fn test_describe_transactions() {
        let (admin, mut runnable, _time, nodes) = env();
        let transactional_id = "foo";
        let coordinator = &nodes[0];

        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(transactional_id, coordinator)]));
        runnable.client_mut().prepare_response_from(
            describe_transactions_resp(vec![describe_txn_state(
                transactional_id,
                "CompleteCommit",
                12345,
                15,
                10000,
                -1,
            )]),
            coordinator,
        );

        let result = admin.describe_transactions_with_options(&["foo".to_string()], DescribeTransactionsOptions::new());
        let future = result.description(transactional_id).unwrap();
        pump_until(&mut runnable, 40, |_r| future.is_done()).await;
        let expected = TransactionDescription::new(
            coordinator.id(),
            TransactionState::CompleteCommit,
            12345,
            15,
            10000,
            None,
            HashSet::new(),
        );
        assert_eq!(future.get().await.unwrap(), expected);
    }

    /// Mirrors `KafkaAdminClientTest.testRetryDescribeTransactionsAfterNotCoordinatorError`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testRetryDescribeTransactionsAfterNotCoordinatorError"
    )]
    async fn test_retry_describe_transactions_after_not_coordinator_error() {
        let (admin, mut runnable, time, nodes) = env_with_props(&[("retry.backoff.ms", "100")]);
        let transactional_id = "foo";
        let coordinator1 = &nodes[0];
        let coordinator2 = &nodes[1];

        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(transactional_id, coordinator1)]));
        runnable.client_mut().prepare_response_from(
            describe_transactions_resp(vec![describe_txn_error_state(transactional_id, Errors::NotCoordinator)]),
            coordinator1,
        );
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(transactional_id, coordinator2)]));
        runnable.client_mut().prepare_response_from(
            describe_transactions_resp(vec![describe_txn_state(
                transactional_id,
                "CompleteCommit",
                12345,
                15,
                10000,
                -1,
            )]),
            coordinator2,
        );

        let result = admin.describe_transactions_with_options(&["foo".to_string()], DescribeTransactionsOptions::new());
        let future = result.description(transactional_id).unwrap();
        drive_until(&mut runnable, &time, 60, || future.is_done()).await;
        let expected = TransactionDescription::new(
            coordinator2.id(),
            TransactionState::CompleteCommit,
            12345,
            15,
            10000,
            None,
            HashSet::new(),
        );
        assert_eq!(future.get().await.unwrap(), expected);
    }

    /// A driver call (here the `FindCoordinator` lookup of `describeTransactions`)
    /// honours `retries`: `AdminClientRunnable.enqueue` fails a call whose
    /// `tries` exceed `maxRetries` with `TimeoutException("Exceeded maxRetries
    /// after " + tries + " tries.")` (`KafkaAdminClient.java:1563-1568`). With
    /// `retries=2` and a coordinator that is never available, Java 4.3.1 sends
    /// exactly three `FindCoordinator` requests (tries 0, 1, 2) and fails the key
    /// when the driver offers the fourth.
    #[tokio::test]
    async fn test_driver_lookup_retries_are_bounded_by_max_retries() {
        let (admin, mut runnable, time, _nodes) = env_with_props(&[("retries", "2"), ("retry.backoff.ms", "10")]);
        let find_coordinator_requests = Arc::new(AtomicUsize::new(0));
        // More error responses than Java consumes, so a fourth request would be
        // answered (and counted) rather than left waiting.
        for _ in 0..5 {
            let counter = Arc::clone(&find_coordinator_requests);
            runnable.client_mut().prepare_response_matcher(
                Box::new(move |body: &AbstractRequest| {
                    let is_find_coordinator = matches!(body, AbstractRequest::FindCoordinator(_));
                    if is_find_coordinator {
                        counter.fetch_add(1, Ordering::SeqCst);
                    }
                    is_find_coordinator
                }),
                find_coordinator_error_resp("foo", Errors::CoordinatorNotAvailable),
            );
        }

        let result = admin.describe_transactions_with_options(&["foo".to_string()], DescribeTransactionsOptions::new());
        let future = result.description("foo").unwrap();
        drive_until(&mut runnable, &time, 200, || future.is_done()).await;

        assert!(
            future.is_done(),
            "the lookup must fail once out of retries; {} FindCoordinator requests were sent",
            find_coordinator_requests.load(Ordering::SeqCst)
        );
        let err = future.get().await.expect_err("the lookup runs out of retries");
        assert!(matches!(err, Error::Timeout(_)), "got {err:?}");
        assert_eq!(err.message(), "Exceeded maxRetries after 3 tries.");
        assert_eq!(find_coordinator_requests.load(Ordering::SeqCst), 3);
    }

    /// Mirrors `KafkaAdminClientTest.testFenceProducers`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testFenceProducers")]
    async fn test_fence_producers() {
        let (admin, mut runnable, time, nodes) = env_with_props(&[("retry.backoff.ms", "100")]);
        let transactional_id = "copyCat";
        let coordinator = &nodes[0];

        // Retriable FindCoordinator error, then success, then a coordinator-load
        // InitProducerId retry, then a coordinator-moved retry, then success.
        runnable
            .client_mut()
            .prepare_response(find_coordinator_error_resp(transactional_id, Errors::CoordinatorNotAvailable));
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(transactional_id, coordinator)]));
        runnable
            .client_mut()
            .prepare_response_from(init_producer_id_resp(Errors::CoordinatorLoadInProgress, 0, 0), coordinator);
        runnable
            .client_mut()
            .prepare_response_from(init_producer_id_resp(Errors::NotCoordinator, 0, 0), coordinator);
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(transactional_id, coordinator)]));
        runnable
            .client_mut()
            .prepare_response_from(init_producer_id_resp(Errors::None, 4761, 489), coordinator);

        let result = admin.fence_producers_with_options(&["copyCat".to_string()], FenceProducersOptions::new());
        let all = result.all();
        drive_until(&mut runnable, &time, 80, || all.is_done()).await;
        all.get().await.unwrap();
        assert_eq!(result.producer_id(transactional_id).unwrap().get().await.unwrap(), 4761);
        assert_eq!(result.epoch_id(transactional_id).unwrap().get().await.unwrap(), 489);
    }

    /// The mock's `describe_transactions` mirrors Java's `UnsupportedOperationException`.
    #[tokio::test]
    async fn test_mock_describe_transactions_unsupported() {
        let mock = mock_admin_client::Builder::new()
            .build()
            .expect("a fresh builder has one broker");
        let result = mock.describe_transactions_with_options(&["t".to_string()], DescribeTransactionsOptions::new());
        assert_eq!(
            result.description("t").unwrap().get().await.unwrap_err().error(),
            Errors::UnsupportedVersion
        );
    }

    /// The mock's `fence_producers` mirrors Java's `UnsupportedOperationException`.
    #[tokio::test]
    async fn test_mock_fence_producers_unsupported() {
        let mock = mock_admin_client::Builder::new()
            .build()
            .expect("a fresh builder has one broker");
        let result = mock.fence_producers_with_options(&["t".to_string()], FenceProducersOptions::new());
        assert_eq!(result.all().get().await.unwrap_err().error(), Errors::UnsupportedVersion);
    }

    // --- listTransactions / forceTerminateTransaction ------------------------

    use crate::ListTransactionsResponseData;
    use crate::admin::{ListTransactionsOptions, TerminateTransactionOptions, TransactionListing};
    use crate::list_transactions_response_data::TransactionState as WireListTxnState;

    fn list_transactions_resp(listing: &TransactionListing) -> ConcreteResponse {
        let mut s = WireListTxnState::new();
        s.set_transactional_id(listing.transactional_id().to_string());
        s.set_producer_id(listing.producer_id());
        s.set_transaction_state(listing.state().to_string());
        let mut data = ListTransactionsResponseData::new();
        data.set_error_code(Errors::None.code());
        data.set_transaction_states(vec![s]);
        ConcreteResponse::ListTransactions(crate::common::requests::ListTransactionsResponse::new(data))
    }

    /// Mirrors `KafkaAdminClientTest.testListTransactions`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testListTransactions")]
    async fn test_list_transactions() {
        let (admin, mut runnable, _time, nodes) = env();
        // The all-brokers lookup returns every broker; then each broker answers
        // its own `ListTransactions` request with one listing (indexed by id).
        runnable.client_mut().prepare_response(metadata_resp(&nodes, vec![]));

        let expected = [
            TransactionListing::new("foo", 12345, TransactionState::Ongoing),
            TransactionListing::new("bar", 98765, TransactionState::PrepareAbort),
            TransactionListing::new("baz", 13579, TransactionState::CompleteCommit),
        ];
        for node in &nodes {
            runnable
                .client_mut()
                .prepare_response_from(list_transactions_resp(&expected[node.id() as usize]), node);
        }

        let result = admin.list_transactions_with_options(ListTransactionsOptions::new());
        let all = result.all();
        pump_until(&mut runnable, 60, |_r| all.is_done()).await;
        assert_eq!(
            all.get().await.unwrap().into_iter().collect::<HashSet<_>>(),
            expected.into_iter().collect::<HashSet<_>>()
        );
    }

    /// Java's `listTransactions()` is `listTransactions(new ListTransactionsOptions())`
    /// (`Admin.java`), whose `filteredDuration` starts at `-1L`
    /// (`ListTransactionsOptions.java:33`), and `ListTransactionsHandler.buildBatchedRequest`
    /// copies it into `DurationFilter`. So the no-arg call asks every broker with
    /// `DurationFilter = -1` (no duration filter), not `0`.
    #[tokio::test]
    async fn test_list_transactions_without_options_sends_no_duration_filter() {
        let (admin, mut runnable, _time, nodes) = env();
        runnable.client_mut().prepare_response(metadata_resp(&nodes, vec![]));

        let duration_filters = Arc::new(Mutex::new(Vec::new()));
        for node in &nodes {
            let seen = Arc::clone(&duration_filters);
            runnable.client_mut().prepare_response_from_matcher(
                Box::new(move |body: &AbstractRequest| match body {
                    AbstractRequest::ListTransactions(request) => {
                        seen.lock().unwrap().push(request.data().duration_filter);
                        true
                    },
                    _ => false,
                }),
                list_transactions_resp(&TransactionListing::new(
                    format!("txn-{}", node.id()),
                    i64::from(node.id()),
                    TransactionState::Ongoing,
                )),
                node,
            );
        }

        let result = admin.list_transactions();
        let all = result.all();
        pump_until(&mut runnable, 60, |_r| all.is_done()).await;
        assert_eq!(all.get().await.unwrap().len(), nodes.len());
        assert_eq!(*duration_filters.lock().unwrap(), vec![-1; nodes.len()]);
    }

    /// Mirrors `KafkaAdminClientTest.testForceTerminateTransaction`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testForceTerminateTransaction")]
    async fn test_force_terminate_transaction() {
        let (admin, mut runnable, _time, nodes) = env();
        let transactional_id = "testForceTerminate";
        let coordinator = &nodes[0];
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(transactional_id, coordinator)]));
        runnable
            .client_mut()
            .prepare_response_from(init_producer_id_resp(Errors::None, 5678, 123), coordinator);

        let result =
            admin.force_terminate_transaction_with_options(transactional_id, TerminateTransactionOptions::new());
        let future = result.result();
        pump_until(&mut runnable, 40, |_r| future.is_done()).await;
        future.get().await.unwrap();
    }

    /// Mirrors `KafkaAdminClientTest.testForceTerminateTransactionWithError`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testForceTerminateTransactionWithError")]
    async fn test_force_terminate_transaction_with_error() {
        let (admin, mut runnable, _time, nodes) = env();
        let transactional_id = "testForceTerminateError";
        let coordinator = &nodes[0];
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(transactional_id, coordinator)]));
        runnable.client_mut().prepare_response_from(
            init_producer_id_resp(Errors::TransactionalIdAuthorizationFailed, 0, 0),
            coordinator,
        );

        let result =
            admin.force_terminate_transaction_with_options(transactional_id, TerminateTransactionOptions::new());
        let future = result.result();
        pump_until(&mut runnable, 40, |_r| future.is_done()).await;
        assert_eq!(
            future.get().await.unwrap_err().error(),
            Errors::TransactionalIdAuthorizationFailed
        );
    }

    /// Mirrors `KafkaAdminClientTest.testForceTerminateTransactionWithCustomTimeout`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testForceTerminateTransactionWithCustomTimeout")]
    async fn test_force_terminate_transaction_with_custom_timeout() {
        let (admin, mut runnable, _time, nodes) = env();
        let transactional_id = "testForceTerminateTimeout";
        let coordinator = &nodes[0];
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(transactional_id, coordinator)]));
        runnable
            .client_mut()
            .prepare_response_from(init_producer_id_resp(Errors::None, 9012, 456), coordinator);

        let options = TerminateTransactionOptions::new().set_timeout_ms(Some(10000));
        let result = admin.force_terminate_transaction_with_options(transactional_id, options);
        let future = result.result();
        pump_until(&mut runnable, 40, |_r| future.is_done()).await;
        future.get().await.unwrap();
    }

    /// The mock's `list_transactions` mirrors Java's `UnsupportedOperationException`.
    #[tokio::test]
    async fn test_mock_list_transactions_unsupported() {
        let mock = mock_admin_client::Builder::new()
            .build()
            .expect("a fresh builder has one broker");
        let result = mock.list_transactions_with_options(ListTransactionsOptions::new());
        assert_eq!(result.all().get().await.unwrap_err().error(), Errors::UnsupportedVersion);
    }

    /// The mock's `force_terminate_transaction` mirrors Java's `UnsupportedOperationException`.
    #[tokio::test]
    async fn test_mock_force_terminate_transaction_unsupported() {
        let mock = mock_admin_client::Builder::new()
            .build()
            .expect("a fresh builder has one broker");
        let result = mock.force_terminate_transaction_with_options("t", TerminateTransactionOptions::new());
        assert_eq!(result.result().get().await.unwrap_err().error(), Errors::UnsupportedVersion);
    }

    // --- describeCluster -----------------------------------------------------

    use crate::DescribeClusterResponseData;
    use crate::DescribeConfigsResponseData;
    use crate::IncrementalAlterConfigsResponseData;
    use crate::ListConfigResourcesResponseData;
    use crate::admin::{
        AlterConfigOp, AlterConfigsOptions, DescribeClusterOptions, DescribeConfigsOptions, ListConfigResourcesOptions,
        OpType,
    };
    use crate::common::acl::AclOperation;
    use crate::common::config::{ConfigResource, config_resource};
    use crate::common::requests::{
        DescribeClusterResponse, DescribeConfigsResponse, IncrementalAlterConfigsResponse, ListConfigResourcesResponse,
    };
    use crate::describe_cluster_response_data::DescribeClusterBroker;
    use crate::describe_configs_response_data::DescribeConfigsResult as WireDescribeConfigsResult;
    use crate::incremental_alter_configs_response_data::AlterConfigsResourceResponse;
    use crate::list_config_resources_response_data::ConfigResource as WireConfigResource;

    fn describe_cluster_response(
        controller_id: i32,
        brokers: &[Node],
        cluster_id: &str,
        authorized_ops: i32,
    ) -> ConcreteResponse {
        let mut data = DescribeClusterResponseData::new();
        data.set_error_code(Errors::None.code());
        data.set_controller_id(controller_id);
        data.set_cluster_id(cluster_id.to_string());
        data.set_cluster_authorized_operations(authorized_ops);
        let wire_brokers = brokers
            .iter()
            .map(|n| {
                let mut b = DescribeClusterBroker::new();
                b.set_broker_id(n.id());
                b.set_host(n.host().to_string());
                b.set_port(n.port());
                b.set_rack(n.rack().map(str::to_string));
                b
            })
            .collect();
        data.set_brokers(wire_brokers);
        ConcreteResponse::DescribeCluster(DescribeClusterResponse::new(data))
    }

    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeCluster")]
    async fn test_describe_cluster() {
        let (admin, mut runnable, _time, nodes) = env();
        let cluster_id = "mock-cluster";

        // First call: authorized operations omitted, controller id 2.
        runnable.client_mut().prepare_response(describe_cluster_response(
            2,
            &nodes,
            cluster_id,
            MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
        ));
        let result = admin.describe_cluster_with_options(DescribeClusterOptions::new());
        pump(&mut runnable, 5).await;
        assert_eq!(result.cluster_id().get().await.unwrap(), cluster_id);
        let got: HashSet<Node> = result.nodes().get().await.unwrap().into_iter().collect();
        assert_eq!(got, nodes.iter().cloned().collect());
        assert_eq!(result.controller().get().await.unwrap().unwrap().id(), 2);
        assert_eq!(result.authorized_operations().get().await.unwrap(), None);

        // Second call: authorized operations DESCRIBE|ALTER, controller id 1.
        let ops = (1 << AclOperation::Describe.code()) | (1 << AclOperation::Alter.code());
        runnable
            .client_mut()
            .prepare_response(describe_cluster_response(1, &nodes, cluster_id, ops));
        let result2 = admin.describe_cluster_with_options(DescribeClusterOptions::new());
        pump(&mut runnable, 5).await;
        assert_eq!(result2.controller().get().await.unwrap().unwrap().id(), 1);
        let expected: BTreeSet<AclOperation> = [AclOperation::Describe, AclOperation::Alter].into_iter().collect();
        assert_eq!(result2.authorized_operations().get().await.unwrap(), Some(expected));
    }

    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeClusterHandleError")]
    async fn test_describe_cluster_handle_error() {
        let (admin, mut runnable, _time, _nodes) = env();
        let error_message = "my error";
        let mut data = DescribeClusterResponseData::new();
        data.set_error_code(Errors::InvalidRequest.code());
        data.set_error_message(Some(error_message.to_string()));
        runnable
            .client_mut()
            .prepare_response(ConcreteResponse::DescribeCluster(DescribeClusterResponse::new(data)));

        let result = admin.describe_cluster_with_options(DescribeClusterOptions::new());
        pump(&mut runnable, 5).await;
        for err in [
            result.cluster_id().get().await.unwrap_err(),
            result.controller().get().await.unwrap_err(),
            result.nodes().get().await.unwrap_err(),
            result.authorized_operations().get().await.unwrap_err(),
        ] {
            assert_eq!(err.error(), Errors::InvalidRequest);
            assert!(err.message().contains(error_message), "message was: {}", err.message());
        }
    }

    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeClusterFailBack")]
    async fn test_describe_cluster_fail_back() {
        let (admin, mut runnable, _time, nodes) = env();
        let cluster_id = "mock-cluster";
        // Reject the DescribeCluster request with an unsupported version, then
        // answer the Metadata fallback.
        runnable.client_mut().prepare_unsupported_version_response();
        runnable
            .client_mut()
            .prepare_response(ConcreteResponse::Metadata(RequestTestUtils::metadata_response(
                &nodes,
                Some(cluster_id),
                2,
                Vec::new(),
            )));

        let result = admin.describe_cluster_with_options(DescribeClusterOptions::new());
        pump(&mut runnable, 8).await;
        assert_eq!(result.cluster_id().get().await.unwrap(), cluster_id);
        let got: HashSet<Node> = result.nodes().get().await.unwrap().into_iter().collect();
        assert_eq!(got, nodes.iter().cloned().collect());
        assert_eq!(result.controller().get().await.unwrap().unwrap().id(), 2);
        assert_eq!(result.authorized_operations().get().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_describe_cluster_unsupported_version_for_fenced_brokers() {
        let (admin, mut runnable, _time, _nodes) = env();
        // includeFencedBrokers=true: an UnsupportedVersion must NOT fall back to
        // the Metadata request; it propagates as UnsupportedVersion.
        runnable.client_mut().prepare_unsupported_version_response();
        let result =
            admin.describe_cluster_with_options(DescribeClusterOptions::new().set_include_fenced_brokers(true));
        pump(&mut runnable, 8).await;
        let err = result.nodes().get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnsupportedVersion);
    }

    // --- describeConfigs -----------------------------------------------------

    fn describe_configs_result(name: &str, type_id: i8, error: Errors) -> WireDescribeConfigsResult {
        let mut r = WireDescribeConfigsResult::new();
        r.set_resource_name(name.to_string());
        r.set_resource_type(type_id);
        r.set_error_code(error.code());
        r.set_configs(Vec::new());
        r
    }

    fn describe_configs_response(results: Vec<WireDescribeConfigsResult>) -> ConcreteResponse {
        let mut data = DescribeConfigsResponseData::new();
        data.set_results(results);
        ConcreteResponse::DescribeConfigs(DescribeConfigsResponse::new(data))
    }

    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeBrokerConfigs")]
    async fn test_describe_broker_configs() {
        let (admin, mut runnable, _time, nodes) = env();
        let broker0 = ConfigResource::new(config_resource::Type::Broker, "0".to_string());
        let broker1 = ConfigResource::new(config_resource::Type::Broker, "1".to_string());
        runnable.client_mut().prepare_response_from(
            describe_configs_response(vec![describe_configs_result(
                "0",
                config_resource::Type::Broker.id(),
                Errors::None,
            )]),
            &nodes[0],
        );
        runnable.client_mut().prepare_response_from(
            describe_configs_response(vec![describe_configs_result(
                "1",
                config_resource::Type::Broker.id(),
                Errors::None,
            )]),
            &nodes[1],
        );
        let result =
            admin.describe_configs_with_options(&[broker0.clone(), broker1.clone()], DescribeConfigsOptions::new());
        pump(&mut runnable, 8).await;
        let keys: HashSet<ConfigResource> = result.values().keys().cloned().collect();
        assert_eq!(keys, [broker0.clone(), broker1.clone()].into_iter().collect());
        result.values().get(&broker0).unwrap().get().await.unwrap();
        result.values().get(&broker1).unwrap().get().await.unwrap();
    }

    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeBrokerAndLogConfigs")]
    async fn test_describe_broker_and_log_configs() {
        let (admin, mut runnable, _time, nodes) = env();
        let broker = ConfigResource::new(config_resource::Type::Broker, "0".to_string());
        let broker_logger = ConfigResource::new(config_resource::Type::BrokerLogger, "0".to_string());
        // Both broker and broker-logger resources for node 0 go to node 0 in one
        // request.
        runnable.client_mut().prepare_response_from(
            describe_configs_response(vec![
                describe_configs_result("0", config_resource::Type::Broker.id(), Errors::None),
                describe_configs_result("0", config_resource::Type::BrokerLogger.id(), Errors::None),
            ]),
            &nodes[0],
        );
        let result = admin
            .describe_configs_with_options(&[broker.clone(), broker_logger.clone()], DescribeConfigsOptions::new());
        pump(&mut runnable, 8).await;
        let keys: HashSet<ConfigResource> = result.values().keys().cloned().collect();
        assert_eq!(keys, [broker.clone(), broker_logger.clone()].into_iter().collect());
        result.values().get(&broker).unwrap().get().await.unwrap();
        result.values().get(&broker_logger).unwrap().get().await.unwrap();
    }

    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeConfigsPartialResponse")]
    async fn test_describe_configs_partial_response() {
        let (admin, mut runnable, _time, _nodes) = env();
        let topic = ConfigResource::new(config_resource::Type::Topic, "topic".to_string());
        let topic2 = ConfigResource::new(config_resource::Type::Topic, "topic2".to_string());
        // The (single, least-loaded) response only contains `topic`.
        runnable
            .client_mut()
            .prepare_response(describe_configs_response(vec![describe_configs_result(
                "topic",
                config_resource::Type::Topic.id(),
                Errors::None,
            )]));
        let result =
            admin.describe_configs_with_options(&[topic.clone(), topic2.clone()], DescribeConfigsOptions::new());
        pump(&mut runnable, 8).await;
        let keys: HashSet<ConfigResource> = result.values().keys().cloned().collect();
        assert_eq!(keys, [topic.clone(), topic2.clone()].into_iter().collect());
        result.values().get(&topic).unwrap().get().await.unwrap();
        assert!(result.values().get(&topic2).unwrap().get().await.is_err());
    }

    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeConfigsUnrequested")]
    async fn test_describe_configs_unrequested() {
        let (admin, mut runnable, _time, _nodes) = env();
        let topic = ConfigResource::new(config_resource::Type::Topic, "topic".to_string());
        // Response contains an extra, unrequested resource; it is ignored.
        runnable.client_mut().prepare_response(describe_configs_response(vec![
            describe_configs_result("topic", config_resource::Type::Topic.id(), Errors::None),
            describe_configs_result("unrequested", config_resource::Type::Topic.id(), Errors::None),
        ]));
        let result = admin.describe_configs_with_options(std::slice::from_ref(&topic), DescribeConfigsOptions::new());
        pump(&mut runnable, 8).await;
        let keys: HashSet<ConfigResource> = result.values().keys().cloned().collect();
        assert_eq!(keys, [topic.clone()].into_iter().collect());
        result.values().get(&topic).unwrap().get().await.unwrap();
    }

    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeClientMetricsConfigs")]
    async fn test_describe_client_metrics_configs() {
        let (admin, mut runnable, _time, _nodes) = env();
        let sub1 = ConfigResource::new(config_resource::Type::ClientMetrics, "sub1".to_string());
        let sub2 = ConfigResource::new(config_resource::Type::ClientMetrics, "sub2".to_string());
        runnable.client_mut().prepare_response(describe_configs_response(vec![
            describe_configs_result("sub1", config_resource::Type::ClientMetrics.id(), Errors::None),
            describe_configs_result("sub2", config_resource::Type::ClientMetrics.id(), Errors::None),
        ]));
        let result = admin.describe_configs_with_options(&[sub1.clone(), sub2.clone()], DescribeConfigsOptions::new());
        pump(&mut runnable, 8).await;
        let keys: HashSet<ConfigResource> = result.values().keys().cloned().collect();
        assert_eq!(keys, [sub1.clone(), sub2.clone()].into_iter().collect());
        result.values().get(&sub1).unwrap().get().await.unwrap();
        result.values().get(&sub2).unwrap().get().await.unwrap();
    }

    // --- incrementalAlterConfigs ---------------------------------------------

    fn alter_configs_resource_response(
        name: &str,
        type_id: i8,
        error: Errors,
        message: &str,
    ) -> AlterConfigsResourceResponse {
        let mut r = AlterConfigsResourceResponse::new();
        r.set_resource_name(name.to_string());
        r.set_resource_type(type_id);
        r.set_error_code(error.code());
        r.set_error_message(Some(message.to_string()));
        r
    }

    fn incremental_alter_configs_response(responses: Vec<AlterConfigsResourceResponse>) -> ConcreteResponse {
        let mut data = IncrementalAlterConfigsResponseData::new();
        data.set_responses(responses);
        ConcreteResponse::IncrementalAlterConfigs(IncrementalAlterConfigsResponse::new(data))
    }

    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testIncrementalAlterConfigs")]
    async fn test_incremental_alter_configs() {
        let (admin, mut runnable, _time, _nodes) = env();

        let broker_resource = ConfigResource::new(config_resource::Type::Broker, String::new());
        let topic_resource = ConfigResource::new(config_resource::Type::Topic, "topic1".to_string());
        let metric_resource = ConfigResource::new(config_resource::Type::ClientMetrics, "metric1".to_string());
        let group_resource = ConfigResource::new(config_resource::Type::Group, "group1".to_string());

        // Error scenario: all four resources are least-loaded-routed (default
        // broker, topic, client-metrics, group all have node_for == None), so a
        // single request fails per-resource.
        runnable.client_mut().prepare_response(incremental_alter_configs_response(vec![
            alter_configs_resource_response(
                "",
                config_resource::Type::Broker.id(),
                Errors::ClusterAuthorizationFailed,
                "authorization error",
            ),
            alter_configs_resource_response(
                "metric1",
                config_resource::Type::ClientMetrics.id(),
                Errors::InvalidRequest,
                "Subscription is not allowed",
            ),
            alter_configs_resource_response(
                "topic1",
                config_resource::Type::Topic.id(),
                Errors::InvalidRequest,
                "Config value append is not allowed for config",
            ),
            alter_configs_resource_response(
                "group1",
                config_resource::Type::Group.id(),
                Errors::InvalidConfig,
                "Unknown group config name: group.initial.rebalance.delay.ms",
            ),
        ]));

        let op1 = AlterConfigOp::new(
            ConfigEntry::new("log.segment.bytes".to_string(), Some("1073741".to_string())),
            OpType::Set,
        );
        let op2 = AlterConfigOp::new(
            ConfigEntry::new("compression.type".to_string(), Some("gzip".to_string())),
            OpType::Append,
        );
        let op3 = AlterConfigOp::new(
            ConfigEntry::new("interval.ms".to_string(), Some("1000".to_string())),
            OpType::Append,
        );
        let op4 = AlterConfigOp::new(
            ConfigEntry::new("group.initial.rebalance.delay.ms".to_string(), Some("1000".to_string())),
            OpType::Set,
        );

        let mut configs = HashMap::new();
        configs.insert(broker_resource.clone(), vec![op1.clone()]);
        configs.insert(topic_resource.clone(), vec![op2]);
        configs.insert(metric_resource.clone(), vec![op3.clone()]);
        configs.insert(group_resource.clone(), vec![op4.clone()]);

        let result = admin.incremental_alter_configs_with_options(&configs, AlterConfigsOptions::new());
        pump(&mut runnable, 8).await;
        assert_eq!(
            result.values().get(&broker_resource).unwrap().get().await.unwrap_err().error(),
            Errors::ClusterAuthorizationFailed
        );
        assert_eq!(
            result.values().get(&topic_resource).unwrap().get().await.unwrap_err().error(),
            Errors::InvalidRequest
        );
        assert_eq!(
            result.values().get(&metric_resource).unwrap().get().await.unwrap_err().error(),
            Errors::InvalidRequest
        );
        assert_eq!(
            result.values().get(&group_resource).unwrap().get().await.unwrap_err().error(),
            Errors::InvalidConfig
        );

        // Success scenario.
        runnable.client_mut().prepare_response(incremental_alter_configs_response(vec![
            alter_configs_resource_response("", config_resource::Type::Broker.id(), Errors::None, ""),
            alter_configs_resource_response("metric1", config_resource::Type::ClientMetrics.id(), Errors::None, ""),
            alter_configs_resource_response("group1", config_resource::Type::Group.id(), Errors::None, ""),
        ]));
        let mut success = HashMap::new();
        success.insert(broker_resource, vec![op1]);
        success.insert(metric_resource, vec![op3]);
        success.insert(group_resource, vec![op4]);
        let result = admin.incremental_alter_configs_with_options(&success, AlterConfigsOptions::new());
        pump(&mut runnable, 8).await;
        result.all().get().await.unwrap();
    }

    // --- listConfigResources -------------------------------------------------

    fn list_config_resources_response(error: Errors, resources: &[(&str, i8)]) -> ConcreteResponse {
        let mut data = ListConfigResourcesResponseData::new();
        data.set_error_code(error.code());
        let wire = resources
            .iter()
            .map(|(name, type_id)| {
                let mut r = WireConfigResource::new();
                r.set_resource_name((*name).to_string());
                r.set_resource_type(*type_id);
                r
            })
            .collect();
        data.set_config_resources(wire);
        ConcreteResponse::ListConfigResources(ListConfigResourcesResponse::new(data))
    }

    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testListConfigResources")]
    async fn test_list_config_resources() {
        let (admin, mut runnable, _time, _nodes) = env();
        let expected = [
            ("client-metrics", config_resource::Type::ClientMetrics.id()),
            ("1", config_resource::Type::Broker.id()),
            ("1", config_resource::Type::BrokerLogger.id()),
            ("topic", config_resource::Type::Topic.id()),
            ("group", config_resource::Type::Group.id()),
        ];
        runnable
            .client_mut()
            .prepare_response(list_config_resources_response(Errors::None, &expected));
        let result = admin.list_config_resources_with_options(&HashSet::new(), ListConfigResourcesOptions::new());
        pump(&mut runnable, 5).await;
        let listed = result.all().get().await.unwrap();
        assert_eq!(listed.len(), expected.len());
        let expected_set: HashSet<ConfigResource> = expected
            .iter()
            .map(|(name, type_id)| ConfigResource::new(config_resource::Type::for_id(*type_id), (*name).to_string()))
            .collect();
        assert_eq!(listed.into_iter().collect::<HashSet<_>>(), expected_set);
    }

    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testListConfigResourcesEmpty")]
    async fn test_list_config_resources_empty() {
        let (admin, mut runnable, _time, _nodes) = env();
        runnable
            .client_mut()
            .prepare_response(list_config_resources_response(Errors::None, &[]));
        let result = admin.list_config_resources_with_options(&HashSet::new(), ListConfigResourcesOptions::new());
        pump(&mut runnable, 5).await;
        assert!(result.all().get().await.unwrap().is_empty());
    }

    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testListConfigResourcesNotSupported")]
    async fn test_list_config_resources_not_supported() {
        let (admin, mut runnable, _time, _nodes) = env();
        runnable
            .client_mut()
            .prepare_response(list_config_resources_response(Errors::UnsupportedVersion, &[]));
        let mut types = HashSet::new();
        types.insert(config_resource::Type::Unknown);
        let result = admin.list_config_resources_with_options(&types, ListConfigResourcesOptions::new());
        pump(&mut runnable, 5).await;
        let err = result.all().get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnsupportedVersion);
    }

    // Branch (2) coverage at the `Call` bridge: the `maybe_retry` hook installed
    // by `new_driver_call` turns a disconnect into a lookup retry driven through
    // the `AdminApiDriver` and reports `MaybeRetryOutcome::Handled`, instead of
    // re-queueing the call against the (dead) fulfillment node. Mirrors the hook
    // side of `AdminApiDriverTest.testRetryLookupAfterDisconnect`.
    #[test]
    fn driver_call_maybe_retry_disconnect_redrives_lookup() {
        use crate::admin::internals::admin_api_driver::test_support::{
            TestContext, completed, mapped, placeholder_response,
        };

        let mut ctx = TestContext::dynamic_mapped(&["foo"]);
        let now = ctx.now;

        // Drive the initial lookup so `foo` maps to broker 1.
        ctx.expect_lookup(&["foo"], mapped(&[("foo", 1)]));
        let lookup_specs = ctx.driver.poll();
        assert_eq!(lookup_specs.len(), 1);
        ctx.driver.on_response(
            now,
            &lookup_specs[0].scope,
            &lookup_specs[0].keys,
            &placeholder_response(),
            Node::no_node(),
        );
        assert_eq!(ctx.driver.key_to_broker_id(&"foo".to_string()), Some(1));

        // Obtain the fulfillment spec targeting broker 1.
        ctx.expect_request(&["foo"], completed(&[("foo", 15)]));
        let mut fulfill_specs = ctx.driver.poll();
        assert_eq!(fulfill_specs.len(), 1);
        let spec = fulfill_specs.remove(0);
        assert_eq!(spec.scope.destination_broker_id(), Some(1));

        // Wrap the spec in a real driver `Call` and fire a disconnect through it.
        let driver = Arc::new(Mutex::new(ctx.driver));
        let (tx, mut rx) = mpsc::unbounded_channel();
        let drv_ctx = DriverContext {
            tx,
            wakeup: Arc::new(Notify::new()),
            shutdown: Arc::new(ShutdownSignal::new()),
            using_bootstrap_controllers: false,
            max_retries: i32::MAX,
            time: mock_time(now),
            log_context: LogContext::new("[test] "),
        };
        let mut call = new_driver_call(Arc::clone(&driver), spec, drv_ctx);

        let outcome = call.maybe_retry(
            &Error::Disconnect(crate::common::errors::DisconnectError::new("disconnected")),
            now,
        );
        assert!(matches!(outcome, MaybeRetryOutcome::Handled));

        // `foo` was unmapped and a fresh lookup call was enqueued (targeting a
        // least-loaded broker, not the disconnected fulfillment node).
        assert_eq!(driver.lock().unwrap().key_to_broker_id(&"foo".to_string()), None);
        let follow_up = rx.try_recv().expect("a lookup call should have been enqueued");
        assert!(matches!(follow_up.node_provider, NodeProvider::LeastLoaded));
    }

    // A non-disconnect error falls through to the default `Requeue` outcome, so
    // the runnable re-queues the call honoring backoff/retries; the driver state
    // is untouched and nothing new is enqueued.
    #[test]
    fn driver_call_maybe_retry_non_network_requeues() {
        use crate::admin::internals::admin_api_driver::test_support::{TestContext, completed};

        let ctx = TestContext::static_mapped(&[("foo", 0)]);
        let now = ctx.now;
        ctx.expect_request(&["foo"], completed(&[("foo", 15)]));

        let mut driver = ctx.driver;
        let mut specs = driver.poll();
        assert_eq!(specs.len(), 1);
        let spec = specs.remove(0);

        let driver = Arc::new(Mutex::new(driver));
        let (tx, mut rx) = mpsc::unbounded_channel();
        let drv_ctx = DriverContext {
            tx,
            wakeup: Arc::new(Notify::new()),
            shutdown: Arc::new(ShutdownSignal::new()),
            using_bootstrap_controllers: false,
            max_retries: i32::MAX,
            time: mock_time(now),
            log_context: LogContext::new("[test] "),
        };
        let mut call = new_driver_call(Arc::clone(&driver), spec, drv_ctx);

        let outcome = call.maybe_retry(&Error::new(Errors::UnknownServerError), now);
        assert!(matches!(outcome, MaybeRetryOutcome::Requeue));
        assert_eq!(driver.lock().unwrap().key_to_broker_id(&"foo".to_string()), Some(0));
        assert!(rx.try_recv().is_err());
    }

    // --- describeLogDirs / alterReplicaLogDirs / describeReplicaLogDirs -------

    use crate::AlterReplicaLogDirsResponseData;
    use crate::DescribeLogDirsResponseData;
    use crate::admin::{AlterReplicaLogDirsOptions, DescribeLogDirsOptions, DescribeReplicaLogDirsOptions};
    use crate::alter_replica_log_dirs_response_data::{
        AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
    };
    use crate::common::TopicPartitionReplica;
    use crate::common::requests::AlterReplicaLogDirsResponse;
    use crate::describe_log_dirs_response_data::{
        DescribeLogDirsPartition, DescribeLogDirsResult as WireDescribeLogDirsResult, DescribeLogDirsTopic,
    };

    fn describe_log_dirs_topics(
        partition_size: i64,
        offset_lag: i64,
        topic: &str,
        partition: i32,
        is_future: bool,
    ) -> Vec<DescribeLogDirsTopic> {
        let mut p = DescribeLogDirsPartition::new();
        p.set_partition_index(partition);
        p.set_partition_size(partition_size);
        p.set_is_future_key(is_future);
        p.set_offset_lag(offset_lag);
        let mut t = DescribeLogDirsTopic::new();
        t.set_name(topic.to_string());
        t.set_partitions(vec![p]);
        vec![t]
    }

    fn describe_log_dirs_result(
        error: Errors,
        log_dir: &str,
        topics: Vec<DescribeLogDirsTopic>,
    ) -> WireDescribeLogDirsResult {
        let mut r = WireDescribeLogDirsResult::new();
        r.set_error_code(error.code());
        r.set_log_dir(log_dir.to_string());
        r.set_topics(topics);
        r
    }

    fn describe_log_dirs_response(results: Vec<WireDescribeLogDirsResult>) -> ConcreteResponse {
        let mut data = DescribeLogDirsResponseData::new();
        data.set_results(results);
        ConcreteResponse::DescribeLogDirs(DescribeLogDirsResponse::new(data))
    }

    fn describe_log_dirs_single(
        error: Errors,
        log_dir: &str,
        tp: &TopicPartition,
        partition_size: i64,
        offset_lag: i64,
    ) -> ConcreteResponse {
        describe_log_dirs_response(vec![describe_log_dirs_result(
            error,
            log_dir,
            describe_log_dirs_topics(partition_size, offset_lag, tp.topic(), tp.partition(), false),
        )])
    }

    fn describe_log_dirs_single_with_bytes(
        error: Errors,
        log_dir: &str,
        tp: &TopicPartition,
        partition_size: i64,
        offset_lag: i64,
        total_bytes: i64,
        usable_bytes: i64,
    ) -> ConcreteResponse {
        let mut r = describe_log_dirs_result(
            error,
            log_dir,
            describe_log_dirs_topics(partition_size, offset_lag, tp.topic(), tp.partition(), false),
        );
        r.set_total_bytes(total_bytes);
        r.set_usable_bytes(usable_bytes);
        describe_log_dirs_response(vec![r])
    }

    #[expect(clippy::too_many_arguments)]
    fn describe_log_dirs_single_cordoned(
        error: Errors,
        log_dir: &str,
        tp: &TopicPartition,
        partition_size: i64,
        offset_lag: i64,
        total_bytes: i64,
        usable_bytes: i64,
        is_cordoned: bool,
    ) -> ConcreteResponse {
        let mut r = describe_log_dirs_result(
            error,
            log_dir,
            describe_log_dirs_topics(partition_size, offset_lag, tp.topic(), tp.partition(), false),
        );
        r.set_total_bytes(total_bytes);
        r.set_usable_bytes(usable_bytes);
        r.set_is_cordoned(is_cordoned);
        describe_log_dirs_response(vec![r])
    }

    fn empty_describe_log_dirs_response(error: Option<Errors>) -> ConcreteResponse {
        let mut data = DescribeLogDirsResponseData::new();
        if let Some(e) = error {
            data.set_error_code(e.code());
        }
        ConcreteResponse::DescribeLogDirs(DescribeLogDirsResponse::new(data))
    }

    fn replica_describe_log_dirs_result(
        tpr: &TopicPartitionReplica,
        log_dir: &str,
        partition_size: i64,
        offset_lag: i64,
        is_future: bool,
    ) -> WireDescribeLogDirsResult {
        let mut r = WireDescribeLogDirsResult::new();
        r.set_error_code(Errors::None.code());
        r.set_log_dir(log_dir.to_string());
        r.set_topics(describe_log_dirs_topics(
            partition_size,
            offset_lag,
            tpr.topic(),
            tpr.partition(),
            is_future,
        ));
        r
    }

    fn alter_log_dirs_response(error: Errors, topic: &str, partitions: &[i32]) -> ConcreteResponse {
        let mut topic_result = AlterReplicaLogDirTopicResult::new();
        topic_result.set_topic_name(topic.to_string());
        topic_result.set_partitions(
            partitions
                .iter()
                .map(|&partition_id| {
                    let mut p = AlterReplicaLogDirPartitionResult::new();
                    p.set_partition_index(partition_id);
                    p.set_error_code(error.code());
                    p
                })
                .collect(),
        );
        let mut data = AlterReplicaLogDirsResponseData::new();
        data.set_results(vec![topic_result]);
        ConcreteResponse::AlterReplicaLogDirs(AlterReplicaLogDirsResponse::new(data))
    }

    /// Mirrors `KafkaAdminClientTest.testDescribeLogDirs`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeLogDirs")]
    async fn test_describe_log_dirs() {
        let log_dir = "/var/data/kafka";
        let tp = TopicPartition::new("topic", 12);
        let partition_size = 1234567890;
        let offset_lag = 24;
        let (admin, mut runnable, _time, nodes) = env();

        runnable.client_mut().prepare_response_from(
            describe_log_dirs_single(Errors::None, log_dir, &tp, partition_size, offset_lag),
            &nodes[0],
        );
        let result = admin.describe_log_dirs_with_options(&[0], DescribeLogDirsOptions::new());
        pump_until(&mut runnable, 10, |_r| result.descriptions()[&0].is_done()).await;

        let descriptions = result.descriptions();
        assert_eq!(descriptions.keys().copied().collect::<HashSet<_>>(), HashSet::from([0]));
        let map = descriptions[&0].get().await.unwrap();
        assert_description_contains(&map, log_dir, &tp, partition_size, offset_lag, None, None);
        let all = result.all_descriptions().get().await.unwrap();
        assert_eq!(all.keys().copied().collect::<HashSet<_>>(), HashSet::from([0]));
        assert_description_contains(&all[&0], log_dir, &tp, partition_size, offset_lag, None, None);

        // Empty results when not authorized with version < 3.
        runnable
            .client_mut()
            .prepare_response_from(empty_describe_log_dirs_response(None), &nodes[0]);
        let error_result = admin.describe_log_dirs_with_options(&[0], DescribeLogDirsOptions::new());
        pump_until(&mut runnable, 10, |_r| error_result.descriptions()[&0].is_done()).await;
        let err = error_result.all_descriptions().get().await.unwrap_err();
        assert_eq!(err.error(), Errors::ClusterAuthorizationFailed);

        // Empty results with an error with version >= 3.
        runnable
            .client_mut()
            .prepare_response_from(empty_describe_log_dirs_response(Some(Errors::UnknownServerError)), &nodes[0]);
        let error_result2 = admin.describe_log_dirs_with_options(&[0], DescribeLogDirsOptions::new());
        pump_until(&mut runnable, 10, |_r| error_result2.descriptions()[&0].is_done()).await;
        let err2 = error_result2.all_descriptions().get().await.unwrap_err();
        assert_eq!(err2.error(), Errors::UnknownServerError);
    }

    fn assert_description_contains(
        map: &HashMap<String, LogDirDescription>,
        log_dir: &str,
        tp: &TopicPartition,
        partition_size: i64,
        offset_lag: i64,
        total_bytes: Option<i64>,
        usable_bytes: Option<i64>,
    ) {
        assert_eq!(
            map.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from([log_dir.to_string()])
        );
        let desc = &map[log_dir];
        assert!(desc.error().is_none());
        let infos = desc.replica_infos();
        assert_eq!(infos.keys().cloned().collect::<HashSet<_>>(), HashSet::from([tp.clone()]));
        assert_eq!(infos[tp].size(), partition_size);
        assert_eq!(infos[tp].offset_lag(), offset_lag);
        assert!(!infos[tp].is_future());
        assert_eq!(desc.total_bytes(), total_bytes);
        assert_eq!(desc.usable_bytes(), usable_bytes);
        assert!(!desc.is_cordoned());
    }

    /// Mirrors `KafkaAdminClientTest.testDescribeLogDirsWithVolumeBytes`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeLogDirsWithVolumeBytes")]
    async fn test_describe_log_dirs_with_volume_bytes() {
        let log_dir = "/var/data/kafka";
        let tp = TopicPartition::new("topic", 12);
        let partition_size = 1234567890;
        let offset_lag = 24;
        let total_bytes = 123;
        let usable_bytes = 456;
        let (admin, mut runnable, _time, nodes) = env();

        runnable.client_mut().prepare_response_from(
            describe_log_dirs_single_with_bytes(
                Errors::None,
                log_dir,
                &tp,
                partition_size,
                offset_lag,
                total_bytes,
                usable_bytes,
            ),
            &nodes[0],
        );
        let result = admin.describe_log_dirs_with_options(&[0], DescribeLogDirsOptions::new());
        pump_until(&mut runnable, 10, |_r| result.descriptions()[&0].is_done()).await;
        let map = result.descriptions()[&0].get().await.unwrap();
        assert_description_contains(
            &map,
            log_dir,
            &tp,
            partition_size,
            offset_lag,
            Some(total_bytes),
            Some(usable_bytes),
        );
        let all = result.all_descriptions().get().await.unwrap();
        assert_description_contains(
            &all[&0],
            log_dir,
            &tp,
            partition_size,
            offset_lag,
            Some(total_bytes),
            Some(usable_bytes),
        );
    }

    /// Mirrors `KafkaAdminClientTest.testDescribeLogDirsWithCordonedDir`
    /// (KIP-1066).
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeLogDirsWithCordonedDir")]
    async fn test_describe_log_dirs_with_cordoned_dir() {
        let log_dir = "/var/data/kafka";
        let tp = TopicPartition::new("topic", 12);
        let (admin, mut runnable, _time, nodes) = env();

        runnable.client_mut().prepare_response_from(
            describe_log_dirs_single_cordoned(Errors::None, log_dir, &tp, 123, -1, -1, -1, true),
            &nodes[0],
        );
        let result = admin.describe_log_dirs_with_options(&[0], DescribeLogDirsOptions::new());
        pump_until(&mut runnable, 10, |_r| result.descriptions()[&0].is_done()).await;

        let descriptions = result.descriptions();
        assert_eq!(descriptions.keys().copied().collect::<HashSet<_>>(), HashSet::from([0]));
        let map = descriptions[&0].get().await.unwrap();
        assert_eq!(
            map.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from([log_dir.to_string()])
        );
        assert!(map[log_dir].is_cordoned());
        assert_eq!(
            map[log_dir].replica_infos().keys().cloned().collect::<HashSet<_>>(),
            HashSet::from([tp.clone()])
        );

        let all = result.all_descriptions().get().await.unwrap();
        assert_eq!(all.keys().copied().collect::<HashSet<_>>(), HashSet::from([0]));
        let all_map = &all[&0];
        assert_eq!(
            all_map.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from([log_dir.to_string()])
        );
        assert!(all_map[log_dir].is_cordoned());
        assert_eq!(
            all_map[log_dir].replica_infos().keys().cloned().collect::<HashSet<_>>(),
            HashSet::from([tp.clone()])
        );
    }

    /// Mirrors `KafkaAdminClientTest.testDescribeLogDirsOfflineDir`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeLogDirsOfflineDir")]
    async fn test_describe_log_dirs_offline_dir() {
        let log_dir = "/var/data/kafka";
        let (admin, mut runnable, _time, nodes) = env();
        runnable.client_mut().prepare_response_from(
            describe_log_dirs_response(vec![describe_log_dirs_result(Errors::KafkaStorageError, log_dir, Vec::new())]),
            &nodes[0],
        );
        let result = admin.describe_log_dirs_with_options(&[0], DescribeLogDirsOptions::new());
        pump_until(&mut runnable, 10, |_r| result.descriptions()[&0].is_done()).await;
        let map = result.descriptions()[&0].get().await.unwrap();
        assert_eq!(
            map.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from([log_dir.to_string()])
        );
        assert_eq!(map[log_dir].error().unwrap().error(), Errors::KafkaStorageError);
        assert!(map[log_dir].replica_infos().is_empty());
    }

    /// Mirrors `KafkaAdminClientTest.testDescribeLogDirsPartialFailure`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeLogDirsPartialFailure")]
    async fn test_describe_log_dirs_partial_failure() {
        let default_api_timeout: i64 = 60000;
        let (admin, mut runnable, time, nodes) = env_with_props(&[
            ("default.api.timeout.ms", &default_api_timeout.to_string()),
            ("retries", "0"),
        ]);
        // Provide only node 1's response.
        runnable.client_mut().prepare_response_from(
            describe_log_dirs_response(vec![describe_log_dirs_result(Errors::None, "/data", Vec::new())]),
            &nodes[1],
        );
        let result = admin.describe_log_dirs_with_options(&[0, 1], DescribeLogDirsOptions::new());
        pump_until(&mut runnable, 30, |r| !r.client_mut().has_pending_responses()).await;
        time.sleep(default_api_timeout + 1);
        pump_until(&mut runnable, 30, |_r| {
            result.descriptions()[&0].is_done() && result.descriptions()[&1].is_done()
        })
        .await;
        assert!(matches!(result.descriptions()[&0].get().await.unwrap_err(), Error::Timeout(_)));
        assert!(result.descriptions()[&1].get().await.is_ok());
    }

    /// Mirrors `KafkaAdminClientTest.testDescribeReplicaLogDirs`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeReplicaLogDirs")]
    async fn test_describe_replica_log_dirs() {
        let tpr1 = TopicPartitionReplica::new("topic", 12, 1);
        let tpr2 = TopicPartitionReplica::new("topic", 12, 2);
        let (admin, mut runnable, _time, nodes) = env();

        let broker1log0 = "/var/data/kafka0";
        let broker1log1 = "/var/data/kafka1";
        let broker2log0 = "/var/data/kafka2";
        runnable.client_mut().prepare_response_from(
            describe_log_dirs_response(vec![
                replica_describe_log_dirs_result(&tpr1, broker1log0, 987654321, 24, false),
                replica_describe_log_dirs_result(&tpr1, broker1log1, 123456789, 4321, true),
            ]),
            &nodes[1],
        );
        runnable.client_mut().prepare_response_from(
            describe_log_dirs_response(vec![describe_log_dirs_result(
                Errors::KafkaStorageError,
                broker2log0,
                Vec::new(),
            )]),
            &nodes[2],
        );

        let result = admin.describe_replica_log_dirs_with_options(
            &[tpr1.clone(), tpr2.clone()],
            DescribeReplicaLogDirsOptions::new(),
        );
        pump_until(&mut runnable, 20, |_r| {
            result.values()[&tpr1].is_done() && result.values()[&tpr2].is_done()
        })
        .await;

        assert_eq!(
            result.values().keys().cloned().collect::<HashSet<_>>(),
            HashSet::from([tpr1.clone(), tpr2.clone()])
        );
        let info1 = result.values()[&tpr1].get().await.unwrap();
        assert_eq!(info1.current_replica_log_dir(), Some(broker1log0));
        assert_eq!(info1.current_replica_offset_lag(), 24);
        assert_eq!(info1.future_replica_log_dir(), Some(broker1log1));
        assert_eq!(info1.future_replica_offset_lag(), 4321);

        let info2 = result.values()[&tpr2].get().await.unwrap();
        assert_eq!(info2.current_replica_log_dir(), None);
        assert_eq!(info2.current_replica_offset_lag(), -1);
        assert_eq!(info2.future_replica_log_dir(), None);
        assert_eq!(info2.future_replica_offset_lag(), -1);
    }

    /// Mirrors `KafkaAdminClientTest.testDescribeReplicaLogDirsUnexpected`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeReplicaLogDirsUnexpected")]
    async fn test_describe_replica_log_dirs_unexpected() {
        let expected = TopicPartitionReplica::new("topic", 12, 1);
        let unexpected = TopicPartitionReplica::new("topic", 12, 2);
        let (admin, mut runnable, _time, nodes) = env();

        let broker1log0 = "/var/data/kafka0";
        let broker1log1 = "/var/data/kafka1";
        runnable.client_mut().prepare_response_from(
            describe_log_dirs_response(vec![
                replica_describe_log_dirs_result(&expected, broker1log0, 987654321, 24, false),
                replica_describe_log_dirs_result(&unexpected, broker1log1, 123456789, 4321, true),
            ]),
            &nodes[1],
        );

        let result = admin.describe_replica_log_dirs_with_options(
            std::slice::from_ref(&expected),
            DescribeReplicaLogDirsOptions::new(),
        );
        pump_until(&mut runnable, 20, |_r| result.values()[&expected].is_done()).await;

        assert_eq!(
            result.values().keys().cloned().collect::<HashSet<_>>(),
            HashSet::from([expected.clone()])
        );
        let info = result.values()[&expected].get().await.unwrap();
        assert_eq!(info.current_replica_log_dir(), Some(broker1log0));
        assert_eq!(info.current_replica_offset_lag(), 24);
        assert_eq!(info.future_replica_log_dir(), Some(broker1log1));
        assert_eq!(info.future_replica_offset_lag(), 4321);
    }

    /// Mirrors `KafkaAdminClientTest.testDescribeReplicaLogDirsWithNonExistReplica`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeReplicaLogDirsWithNonExistReplica")]
    async fn test_describe_replica_log_dirs_with_non_exist_replica() {
        let broker_id = 0;
        let tpr1 = TopicPartitionReplica::new("topic1", 12, broker_id);
        let tpr2 = TopicPartitionReplica::new("topic2", 12, broker_id);
        let (admin, mut runnable, _time, nodes) = env();

        let log_dir = "/var/data/kafka0";
        let offset_lag = 1;
        runnable.client_mut().prepare_response_from(
            describe_log_dirs_response(vec![replica_describe_log_dirs_result(
                &tpr1, log_dir, 123456, offset_lag, false,
            )]),
            &nodes[broker_id as usize],
        );

        let result = admin.describe_replica_log_dirs_with_options(
            &[tpr1.clone(), tpr2.clone()],
            DescribeReplicaLogDirsOptions::new(),
        );
        pump_until(&mut runnable, 20, |_r| {
            result.values()[&tpr1].is_done() && result.values()[&tpr2].is_done()
        })
        .await;

        let info1 = result.values()[&tpr1].get().await.unwrap();
        assert_eq!(info1.current_replica_log_dir(), Some(log_dir));
        assert_eq!(info1.future_replica_log_dir(), None);
        assert_eq!(info1.current_replica_offset_lag(), offset_lag);
        assert_eq!(info1.future_replica_offset_lag(), -1);
        let info2 = result.values()[&tpr2].get().await.unwrap();
        assert_eq!(info2.current_replica_log_dir(), None);
        assert_eq!(info2.future_replica_log_dir(), None);
        assert_eq!(info2.current_replica_offset_lag(), -1);
        assert_eq!(info2.future_replica_offset_lag(), -1);
    }

    /// Mirrors `KafkaAdminClientTest.testAlterReplicaLogDirsSuccess`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testAlterReplicaLogDirsSuccess")]
    async fn test_alter_replica_log_dirs_success() {
        let (admin, mut runnable, _time, nodes) = env();
        runnable
            .client_mut()
            .prepare_response_from(alter_log_dirs_response(Errors::None, "topic", &[0]), &nodes[0]);
        runnable
            .client_mut()
            .prepare_response_from(alter_log_dirs_response(Errors::None, "topic", &[0]), &nodes[1]);

        let tpr0 = TopicPartitionReplica::new("topic", 0, 0);
        let tpr1 = TopicPartitionReplica::new("topic", 0, 1);
        let assignment = HashMap::from([
            (tpr0.clone(), "/data0".to_string()),
            (tpr1.clone(), "/data1".to_string()),
        ]);
        let result = admin.alter_replica_log_dirs_with_options(&assignment, AlterReplicaLogDirsOptions::new());
        pump_until(&mut runnable, 20, |_r| {
            result.values()[&tpr0].is_done() && result.values()[&tpr1].is_done()
        })
        .await;
        result.values()[&tpr0].get().await.unwrap();
        result.values()[&tpr1].get().await.unwrap();
    }

    /// Mirrors `KafkaAdminClientTest.testAlterReplicaLogDirsLogDirNotFound`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testAlterReplicaLogDirsLogDirNotFound")]
    async fn test_alter_replica_log_dirs_log_dir_not_found() {
        let (admin, mut runnable, _time, nodes) = env();
        runnable
            .client_mut()
            .prepare_response_from(alter_log_dirs_response(Errors::None, "topic", &[0]), &nodes[0]);
        runnable
            .client_mut()
            .prepare_response_from(alter_log_dirs_response(Errors::LogDirNotFound, "topic", &[0]), &nodes[1]);

        let tpr0 = TopicPartitionReplica::new("topic", 0, 0);
        let tpr1 = TopicPartitionReplica::new("topic", 0, 1);
        let assignment = HashMap::from([
            (tpr0.clone(), "/data0".to_string()),
            (tpr1.clone(), "/data1".to_string()),
        ]);
        let result = admin.alter_replica_log_dirs_with_options(&assignment, AlterReplicaLogDirsOptions::new());
        pump_until(&mut runnable, 20, |_r| {
            result.values()[&tpr0].is_done() && result.values()[&tpr1].is_done()
        })
        .await;
        result.values()[&tpr0].get().await.unwrap();
        let err = result.values()[&tpr1].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::LogDirNotFound);
    }

    /// Mirrors `KafkaAdminClientTest.testAlterReplicaLogDirsUnrequested`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testAlterReplicaLogDirsUnrequested")]
    async fn test_alter_replica_log_dirs_unrequested() {
        let (admin, mut runnable, _time, nodes) = env();
        // Response contains partitions 1 and 2, but only 1 was requested.
        runnable
            .client_mut()
            .prepare_response_from(alter_log_dirs_response(Errors::None, "topic", &[1, 2]), &nodes[0]);

        let tpr1 = TopicPartitionReplica::new("topic", 1, 0);
        let assignment = HashMap::from([(tpr1.clone(), "/data1".to_string())]);
        let result = admin.alter_replica_log_dirs_with_options(&assignment, AlterReplicaLogDirsOptions::new());
        pump_until(&mut runnable, 20, |_r| result.values()[&tpr1].is_done()).await;
        result.values()[&tpr1].get().await.unwrap();
    }

    /// Mirrors `KafkaAdminClientTest.testAlterReplicaLogDirsPartialResponse`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testAlterReplicaLogDirsPartialResponse")]
    async fn test_alter_replica_log_dirs_partial_response() {
        let (admin, mut runnable, _time, nodes) = env();
        // Response contains only partition 1; partition 2 was also requested.
        runnable
            .client_mut()
            .prepare_response_from(alter_log_dirs_response(Errors::None, "topic", &[1]), &nodes[0]);

        let tpr1 = TopicPartitionReplica::new("topic", 1, 0);
        let tpr2 = TopicPartitionReplica::new("topic", 2, 0);
        let assignment = HashMap::from([
            (tpr1.clone(), "/data1".to_string()),
            (tpr2.clone(), "/data1".to_string()),
        ]);
        let result = admin.alter_replica_log_dirs_with_options(&assignment, AlterReplicaLogDirsOptions::new());
        pump_until(&mut runnable, 20, |_r| {
            result.values()[&tpr1].is_done() && result.values()[&tpr2].is_done()
        })
        .await;
        result.values()[&tpr1].get().await.unwrap();
        // The sanity check completes the unrequested-in-response future with an
        // UnknownServerError (mirrors `completeUnrealizedFutures`).
        let err = result.values()[&tpr2].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnknownServerError);
    }

    /// Mirrors `KafkaAdminClientTest.testAlterReplicaLogDirsPartialFailure`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testAlterReplicaLogDirsPartialFailure")]
    async fn test_alter_replica_log_dirs_partial_failure() {
        let default_api_timeout: i64 = 60000;
        let (admin, mut runnable, time, nodes) = env_with_props(&[
            ("default.api.timeout.ms", &default_api_timeout.to_string()),
            ("retries", "0"),
        ]);
        // Provide only node 1's response.
        runnable
            .client_mut()
            .prepare_response_from(alter_log_dirs_response(Errors::None, "topic", &[2]), &nodes[1]);

        let tpr1 = TopicPartitionReplica::new("topic", 1, 0);
        let tpr2 = TopicPartitionReplica::new("topic", 2, 1);
        let assignment = HashMap::from([
            (tpr1.clone(), "/data1".to_string()),
            (tpr2.clone(), "/data1".to_string()),
        ]);
        let result = admin.alter_replica_log_dirs_with_options(&assignment, AlterReplicaLogDirsOptions::new());
        pump_until(&mut runnable, 30, |r| !r.client_mut().has_pending_responses()).await;
        time.sleep(default_api_timeout + 1);
        pump_until(&mut runnable, 30, |_r| {
            result.values()[&tpr1].is_done() && result.values()[&tpr2].is_done()
        })
        .await;
        assert!(matches!(result.values()[&tpr1].get().await.unwrap_err(), Error::Timeout(_)));
        result.values()[&tpr2].get().await.unwrap();
    }

    // --- MockAdminClient log-dir methods -------------------------------------

    fn mock_topic_partition_info(partition: i32, leader: &Node, replicas: Vec<Node>) -> TopicPartitionInfo {
        TopicPartitionInfo::with_elr_last_known_elr(
            partition,
            Some(leader.clone()),
            replicas,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    }

    #[tokio::test]
    async fn test_mock_describe_log_dirs_reports_topic_replicas() {
        let mock = mock_admin_client::Builder::new()
            .set_num_brokers(2)
            .and_then(mock_admin_client::Builder::build)
            .expect("num_brokers is at least 1");
        let leader = Node::new(0, "localhost".to_string(), 1000);
        let replicas = vec![
            Node::new(0, "localhost".to_string(), 1000),
            Node::new(1, "localhost".to_string(), 1001),
        ];
        mock.add_topic(false, "topic", vec![mock_topic_partition_info(0, &leader, replicas)], None)
            .expect("seeding a topic with known brokers succeeds");

        let result = mock.describe_log_dirs_with_options(&[0, 1], DescribeLogDirsOptions::new());
        let broker0 = result.descriptions()[&0].get().await.unwrap();
        assert!(broker0.contains_key("/tmp/kafka-logs"));
        let infos = broker0["/tmp/kafka-logs"].replica_infos();
        assert!(infos.contains_key(&TopicPartition::new("topic", 0)));
        // Broker 1 is a replica for the partition too.
        let broker1 = result.descriptions()[&1].get().await.unwrap();
        assert!(
            broker1["/tmp/kafka-logs"]
                .replica_infos()
                .contains_key(&TopicPartition::new("topic", 0))
        );
    }

    #[tokio::test]
    async fn test_mock_alter_and_describe_replica_log_dirs() {
        let mock = mock_admin_client::Builder::new()
            .build()
            .expect("a fresh builder has one broker");
        mock.set_broker_log_dirs(0, vec!["/data0".to_string(), "/data1".to_string()])
            .expect("broker 0 exists");
        let leader = Node::new(0, "localhost".to_string(), 1000);
        mock.add_topic(
            false,
            "topic",
            vec![mock_topic_partition_info(0, &leader, vec![leader.clone()])],
            None,
        )
        .expect("seeding a topic with known brokers succeeds");

        // Before any move, current log dir is the seeded first broker log dir.
        let tpr = TopicPartitionReplica::new("topic", 0, 0);
        let before = mock
            .describe_replica_log_dirs_with_options(std::slice::from_ref(&tpr), DescribeReplicaLogDirsOptions::new());
        let info = before.values()[&tpr].get().await.unwrap();
        assert_eq!(info.current_replica_log_dir(), Some("/data0"));
        assert_eq!(info.future_replica_log_dir(), None);

        // Move to /data1; describe should reflect the pending move.
        let assignment = HashMap::from([(tpr.clone(), "/data1".to_string())]);
        let alter = mock.alter_replica_log_dirs_with_options(&assignment, AlterReplicaLogDirsOptions::new());
        alter.values()[&tpr].get().await.unwrap();
        let after = mock
            .describe_replica_log_dirs_with_options(std::slice::from_ref(&tpr), DescribeReplicaLogDirsOptions::new());
        let moved = after.values()[&tpr].get().await.unwrap();
        assert_eq!(moved.current_replica_log_dir(), Some("/data0"));
        assert_eq!(moved.future_replica_log_dir(), Some("/data1"));
    }

    #[tokio::test]
    async fn test_mock_alter_replica_log_dirs_offline_dir() {
        let mock = mock_admin_client::Builder::new()
            .build()
            .expect("a fresh builder has one broker");
        let leader = Node::new(0, "localhost".to_string(), 1000);
        mock.add_topic(
            false,
            "topic",
            vec![mock_topic_partition_info(0, &leader, vec![leader.clone()])],
            None,
        )
        .expect("seeding a topic with known brokers succeeds");
        let tpr = TopicPartitionReplica::new("topic", 0, 0);
        // "/nope" is not among the broker's log dirs -> KafkaStorageError.
        let assignment = HashMap::from([(tpr.clone(), "/nope".to_string())]);
        let result = mock.alter_replica_log_dirs_with_options(&assignment, AlterReplicaLogDirsOptions::new());
        let err = result.values()[&tpr].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::KafkaStorageError);
    }

    #[tokio::test]
    async fn test_mock_alter_replica_log_dirs_negative_partition_does_not_panic() {
        let mock = mock_admin_client::Builder::new()
            .build()
            .expect("a fresh builder has one broker");
        mock.set_broker_log_dirs(0, vec!["/data0".to_string()])
            .expect("broker 0 exists");
        let leader = Node::new(0, "localhost".to_string(), 1000);
        mock.add_topic(
            false,
            "topic",
            vec![mock_topic_partition_info(0, &leader, vec![leader.clone()])],
            None,
        )
        .expect("seeding a topic with known brokers succeeds");

        // A negative partition number has no constructor-time validation
        // (mirroring Java's equally unvalidated TopicPartitionReplica), so it
        // must be handled the same way an unknown replica is, not panic.
        let tpr = TopicPartitionReplica::new("topic", -1, 0);
        let assignment = HashMap::from([(tpr.clone(), "/data0".to_string())]);
        let result = mock.alter_replica_log_dirs_with_options(&assignment, AlterReplicaLogDirsOptions::new());
        let err = result.values()[&tpr].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::ReplicaNotAvailable);
        assert!(err.message().starts_with("Can't find"), "message was: {}", err.message());
    }

    #[tokio::test]
    async fn test_mock_describe_replica_log_dirs_negative_partition_does_not_panic() {
        let mock = mock_admin_client::Builder::new()
            .build()
            .expect("a fresh builder has one broker");
        mock.set_broker_log_dirs(0, vec!["/data0".to_string()])
            .expect("broker 0 exists");
        let leader = Node::new(0, "localhost".to_string(), 1000);
        mock.add_topic(
            false,
            "topic",
            vec![mock_topic_partition_info(0, &leader, vec![leader.clone()])],
            None,
        )
        .expect("seeding a topic with known brokers succeeds");

        let tpr = TopicPartitionReplica::new("topic", -1, 0);
        let result = mock
            .describe_replica_log_dirs_with_options(std::slice::from_ref(&tpr), DescribeReplicaLogDirsOptions::new());
        let info = result.values()[&tpr].get().await.unwrap();
        assert_eq!(info, ReplicaLogDirInfo::default());
    }

    // --- electLeaders / (alter|list)PartitionReassignments / listOffsets -------
    //
    // `ElectLeadersResponse`, `ElectionType`, and the `*Options` / POJO types are
    // already in scope via `use super::*`. Only the wire *data* structs and the
    // `ListOffsetsResponse` wrapper need importing here.

    use crate::AlterPartitionReassignmentsResponseData;
    use crate::ElectLeadersResponseData;
    use crate::ListOffsetsResponseData;
    use crate::ListPartitionReassignmentsResponseData;
    use crate::alter_partition_reassignments_response_data::{
        ReassignablePartitionResponse, ReassignableTopicResponse,
    };
    use crate::common::requests::{
        AlterPartitionReassignmentsResponse, ListOffsetsResponse, ListPartitionReassignmentsResponse,
    };
    use crate::elect_leaders_response_data::{PartitionResult, ReplicaElectionResult};
    use crate::list_partition_reassignments_response_data::{OngoingPartitionReassignment, OngoingTopicReassignment};

    fn elect_leaders_resp(top_error: Errors, results: Vec<ReplicaElectionResult>) -> ConcreteResponse {
        let mut data = ElectLeadersResponseData::new();
        data.set_error_code(top_error.code());
        data.set_replica_election_results(results);
        ConcreteResponse::ElectLeaders(ElectLeadersResponse::with_data(data))
    }

    fn election_result(topic: &str, partitions: &[(i32, Errors, Option<&str>)]) -> ReplicaElectionResult {
        let mut result = ReplicaElectionResult::new();
        result.set_topic(topic.to_string());
        for (partition, error, message) in partitions {
            let mut pr = PartitionResult::new();
            pr.set_partition_id(*partition);
            pr.set_error_code(error.code());
            pr.set_error_message(message.map(str::to_string));
            result.partition_result.push(pr);
        }
        result
    }

    /// Mirrors `KafkaAdminClientTest.testElectLeaders`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testElectLeaders")]
    async fn test_elect_leaders() {
        for election_type in ElectionType::values() {
            let (admin, mut runnable, time, _nodes) = env();
            let topic1 = TopicPartition::new("topic", 0);
            let topic2 = TopicPartition::new("topic", 2);

            // A call where both partitions fail with ClusterAuthorizationFailed.
            runnable.client_mut().prepare_response(elect_leaders_resp(
                Errors::None,
                vec![election_result(
                    "topic",
                    &[
                        (0, Errors::ClusterAuthorizationFailed, Some("no")),
                        (2, Errors::ClusterAuthorizationFailed, Some("no")),
                    ],
                )],
            ));
            let partitions: HashSet<TopicPartition> = [topic1.clone(), topic2.clone()].into_iter().collect();
            let result =
                admin.elect_leaders_with_options(election_type, Some(partitions.clone()), ElectLeadersOptions::new());
            pump(&mut runnable, 5).await;
            let map = result.partitions().get().await.unwrap();
            assert_eq!(map[&topic2].as_ref().unwrap().error(), Errors::ClusterAuthorizationFailed);

            // A call where there are no errors.
            runnable.client_mut().prepare_response(elect_leaders_resp(
                Errors::None,
                vec![election_result(
                    "topic",
                    &[(0, Errors::None, None), (2, Errors::None, None)],
                )],
            ));
            let result =
                admin.elect_leaders_with_options(election_type, Some(partitions.clone()), ElectLeadersOptions::new());
            pump(&mut runnable, 5).await;
            let map = result.partitions().get().await.unwrap();
            assert!(map[&topic1].is_none());
            assert!(map[&topic2].is_none());

            // A call that times out (no response prepared).
            let result = admin.elect_leaders_with_options(
                election_type,
                Some(partitions),
                ElectLeadersOptions::new().set_timeout_ms(Some(100)),
            );
            pump_until(&mut runnable, 5, |r| r.client_mut().request_count() >= 1).await;
            time.sleep(200);
            pump_until(&mut runnable, 30, |_r| result.partitions().is_done()).await;
            let err = result.partitions().get().await.unwrap_err();
            assert!(matches!(err, Error::Timeout(_)));
        }
    }

    // --- describeFeatures / updateFeatures -------------------------------------

    use crate::ApiVersionsResponseData;
    use crate::admin::UpgradeType;
    use crate::api_message_type::ListenerType;
    use crate::api_versions_response_data::SupportedFeatureKey;
    use crate::common::requests::{ApiVersionsResponse, UpdateFeaturesResponse, api_versions_response};

    /// Mirrors `KafkaAdminClientTest.defaultFeatureMetadata`.
    fn default_feature_metadata() -> FeatureMetadata {
        let mut finalized = HashMap::new();
        finalized.insert("test_feature_1".to_string(), FinalizedVersionRange::new(2, 2).unwrap());
        let mut supported = HashMap::new();
        supported.insert("test_feature_1".to_string(), SupportedVersionRange::new(1, 5).unwrap());
        FeatureMetadata::new(finalized, Some(1), supported)
    }

    /// Mirrors `KafkaAdminClientTest.prepareApiVersionsResponseForDescribeFeatures`.
    fn api_versions_feature_response(error: Errors) -> ConcreteResponse {
        if error == Errors::None {
            let mut supported = SupportedFeatureKey::new();
            supported.set_name("test_feature_1".to_string());
            supported.set_min_version(1);
            supported.set_max_version(5);
            let mut finalized = HashMap::new();
            finalized.insert("test_feature_1".to_string(), 2i16);
            let response = api_versions_response::Builder::new()
                .set_api_versions(ApiVersionsResponse::filter_apis(ListenerType::Broker, false, false))
                .set_supported_features(vec![supported])
                .set_finalized_features(finalized)
                .set_finalized_features_epoch(1)
                .build();
            ConcreteResponse::ApiVersions(response)
        } else {
            let mut data = ApiVersionsResponseData::new();
            data.set_throttle_time_ms(0);
            data.set_error_code(error.code());
            ConcreteResponse::ApiVersions(ApiVersionsResponse::new(data))
        }
    }

    /// Builds an `UpdateFeatures` response echoing the given feature names when
    /// the top-level error is NONE (mirrors `UpdateFeaturesResponse.createWithErrors`).
    fn update_features_response(top_error: Errors, message: Option<&str>, updates: &[&str]) -> ConcreteResponse {
        let set: BTreeSet<String> = updates.iter().map(|s| (*s).to_string()).collect();
        ConcreteResponse::UpdateFeatures(UpdateFeaturesResponse::create_with_errors(
            top_error,
            message.map(str::to_string),
            &set,
            0,
        ))
    }

    /// Mirrors `KafkaAdminClientTest.makeTestFeatureUpdates`.
    fn make_test_feature_updates() -> HashMap<String, FeatureUpdate> {
        let mut map = HashMap::new();
        map.insert(
            "test_feature_1".to_string(),
            FeatureUpdate::new(2, UpgradeType::Upgrade).unwrap(),
        );
        map.insert(
            "test_feature_2".to_string(),
            FeatureUpdate::new(3, UpgradeType::SafeDowngrade).unwrap(),
        );
        map
    }

    /// Mirrors `KafkaAdminClientTest.testDescribeFeaturesSuccess`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeFeaturesSuccess")]
    async fn test_describe_features_success() {
        let (admin, mut runnable, _time, _nodes) = env();
        runnable
            .client_mut()
            .prepare_response(api_versions_feature_response(Errors::None));
        let result = admin.describe_features_with_options(DescribeFeaturesOptions::new().set_timeout_ms(Some(10000)));
        pump(&mut runnable, 5).await;
        let metadata = result.feature_metadata().get().await.unwrap();
        assert_eq!(metadata, default_feature_metadata());
    }

    /// Mirrors `KafkaAdminClientTest.testDescribeFeaturesFailure`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeFeaturesFailure")]
    async fn test_describe_features_failure() {
        let (admin, mut runnable, _time, _nodes) = env();
        runnable
            .client_mut()
            .prepare_response(api_versions_feature_response(Errors::InvalidRequest));
        let result = admin.describe_features_with_options(DescribeFeaturesOptions::new().set_timeout_ms(Some(10000)));
        pump(&mut runnable, 5).await;
        let err = result.feature_metadata().get().await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidRequest);
    }

    /// Mirrors `KafkaAdminClientTest.testDescribeFeaturesWithNodeSuccess` — a set
    /// `nodeId` routes the request to that broker via `ConstantNodeIdProvider`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeFeaturesWithNodeSuccess")]
    async fn test_describe_features_with_node_success() {
        let (admin, mut runnable, _time, nodes) = env();
        runnable
            .client_mut()
            .prepare_response_from(api_versions_feature_response(Errors::None), &nodes[0]);
        let result = admin
            .describe_features_with_options(DescribeFeaturesOptions::new().set_timeout_ms(Some(10000)).set_node_id(0));
        pump(&mut runnable, 5).await;
        let metadata = result.feature_metadata().get().await.unwrap();
        assert_eq!(metadata, default_feature_metadata());
    }

    /// Mirrors `KafkaAdminClientTest.testDescribeFeaturesWithNodeFailure` — the
    /// response is prepared for node 1 but the request targets node 0, so it is
    /// never answered and the future times out.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeFeaturesWithNodeFailure")]
    async fn test_describe_features_with_node_failure() {
        let (admin, mut runnable, time, nodes) = env();
        runnable
            .client_mut()
            .prepare_response_from(api_versions_feature_response(Errors::None), &nodes[1]);
        let result = admin
            .describe_features_with_options(DescribeFeaturesOptions::new().set_timeout_ms(Some(1000)).set_node_id(0));
        pump_until(&mut runnable, 5, |r| r.client_mut().request_count() >= 1).await;
        time.sleep(2000);
        pump_until(&mut runnable, 30, |_r| result.feature_metadata().is_done()).await;
        assert!(result.feature_metadata().get().await.is_err());
    }

    /// Drives `KafkaAdminClientTest.testUpdateFeaturesDuringSuccess` — a
    /// `@ParameterizedTest` over `@ValueSource(shorts = {1, 2})`. v1 responses
    /// carry per-feature results; v2+ carry only a top-level NONE.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testUpdateFeaturesDuringSuccess")]
    async fn test_update_features_during_success() {
        for version in [1i16, 2] {
            let (admin, mut runnable, _time, _nodes) = env();
            let features: Vec<&str> = if version <= 1 {
                vec!["test_feature_1", "test_feature_2"]
            } else {
                Vec::new()
            };
            runnable
                .client_mut()
                .prepare_response(update_features_response(Errors::None, None, &features));
            let updates = make_test_feature_updates();
            let result = admin
                .update_features_with_options(&updates, UpdateFeaturesOptions::new().set_timeout_ms(Some(10000)))
                .unwrap();
            pump(&mut runnable, 5).await;
            for future in result.values().values() {
                future.get().await.unwrap();
            }
        }
    }

    /// Mirrors `KafkaAdminClientTest.testUpdateFeaturesTopLevelError`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testUpdateFeaturesTopLevelError")]
    async fn test_update_features_top_level_error() {
        let (admin, mut runnable, _time, _nodes) = env();
        runnable
            .client_mut()
            .prepare_response(update_features_response(Errors::InvalidRequest, None, &[]));
        let updates = make_test_feature_updates();
        let result = admin
            .update_features_with_options(&updates, UpdateFeaturesOptions::new().set_timeout_ms(Some(10000)))
            .unwrap();
        pump(&mut runnable, 5).await;
        for future in result.values().values() {
            let err = future.get().await.unwrap_err();
            assert_eq!(err.error(), Errors::InvalidRequest);
            // The top-level error carried no message, so it falls back to the
            // error code's default message (mirrors ApiError.exception()).
            assert_eq!(err.message(), Errors::InvalidRequest.message());
        }
    }

    /// Drives `KafkaAdminClientTest.testUpdateFeaturesHandleNotControllerException`
    /// — a `@ParameterizedTest` over `@ValueSource(shorts = {1, 2})`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testUpdateFeaturesHandleNotControllerException")]
    async fn test_update_features_handle_not_controller_error() {
        for version in [1i16, 2] {
            let (admin, mut runnable, time, nodes) = env();
            // First attempt hits the wrong controller.
            runnable
                .client_mut()
                .prepare_response(update_features_response(Errors::NotController, None, &[]));
            // Then a metadata refresh updates the controller to node 1.
            runnable
                .client_mut()
                .prepare_response(ConcreteResponse::Metadata(RequestTestUtils::metadata_response(
                    &nodes,
                    Some("mock-cluster"),
                    1,
                    Vec::new(),
                )));
            // Then the retry succeeds.
            let features: Vec<&str> = if version <= 1 {
                vec!["test_feature_1", "test_feature_2"]
            } else {
                Vec::new()
            };
            runnable
                .client_mut()
                .prepare_response(update_features_response(Errors::None, None, &features));
            let updates = make_test_feature_updates();
            let result = admin
                .update_features_with_options(&updates, UpdateFeaturesOptions::new().set_timeout_ms(Some(10000)))
                .unwrap();
            // The NOT_CONTROLLER retry is gated by retry-backoff, so advance the
            // mock clock until every future resolves.
            for _ in 0..30 {
                if result.values().values().all(|f| f.is_done()) {
                    break;
                }
                runnable.run_once().await;
                time.sleep(200);
            }
            result.all().get().await.unwrap();
        }
    }

    /// Mirrors `KafkaAdminClientTest.testUpdateFeaturesShouldFailRequestForEmptyUpdates`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testUpdateFeaturesShouldFailRequestForEmptyUpdates"
    )]
    async fn test_update_features_should_fail_request_for_empty_updates() {
        let (admin, _runnable, _time, _nodes) = env();
        let err = admin
            .update_features_with_options(&HashMap::new(), UpdateFeaturesOptions::new())
            .unwrap_err();
        assert_eq!(err.message(), "Feature updates can not be null or empty.");
    }

    /// Mirrors `KafkaAdminClientTest.testUpdateFeaturesShouldFailRequestForInvalidFeatureName`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testUpdateFeaturesShouldFailRequestForInvalidFeatureName"
    )]
    async fn test_update_features_should_fail_request_for_invalid_feature_name() {
        let (admin, _runnable, _time, _nodes) = env();
        let mut updates = HashMap::new();
        updates.insert("feature".to_string(), FeatureUpdate::new(2, UpgradeType::Upgrade).unwrap());
        updates.insert(String::new(), FeatureUpdate::new(2, UpgradeType::Upgrade).unwrap());
        let err = admin
            .update_features_with_options(&updates, UpdateFeaturesOptions::new())
            .unwrap_err();
        assert_eq!(err.message(), "Provided feature can not be empty.");
    }

    // `testUpdateFeaturesShouldFailRequestInClientWhenDowngradeFlagIsNotSetDuringDeletion`
    // is a `FeatureUpdate` constructor test; it lives in `feature_update.rs`
    // (`new_rejects_deletion_with_upgrade_flag`).

    fn alter_reassignments_resp(
        top_error: Errors,
        error_message: Option<&str>,
        responses: Vec<ReassignableTopicResponse>,
    ) -> ConcreteResponse {
        let mut data = AlterPartitionReassignmentsResponseData::new();
        data.set_error_code(top_error.code());
        data.set_error_message(error_message.map(str::to_string));
        data.set_responses(responses);
        ConcreteResponse::AlterPartitionReassignments(AlterPartitionReassignmentsResponse::new(data))
    }

    fn reassignable_topic_response(
        name: &str,
        partitions: &[(i32, Errors, Option<&str>)],
    ) -> ReassignableTopicResponse {
        let mut topic = ReassignableTopicResponse::new();
        topic.set_name(name.to_string());
        for (index, error, message) in partitions {
            let mut p = ReassignablePartitionResponse::new();
            p.set_partition_index(*index);
            p.set_error_code(error.code());
            p.set_error_message(message.map(str::to_string));
            topic.partitions.push(p);
        }
        topic
    }

    fn reassignments_input() -> HashMap<TopicPartition, Option<NewPartitionReassignment>> {
        let mut reassignments = HashMap::new();
        reassignments.insert(TopicPartition::new("A", 0), None);
        reassignments.insert(
            TopicPartition::new("B", 0),
            Some(NewPartitionReassignment::new(vec![1, 2, 3]).unwrap()),
        );
        reassignments
    }

    /// Mirrors the too-few-responses scenario of `testAlterPartitionReassignments`.
    #[tokio::test]
    async fn test_alter_partition_reassignments_too_few_responses() {
        let (admin, mut runnable, _time, _nodes) = env();
        // The server returns a result for A-0 only (expected 2).
        runnable.client_mut().prepare_response(alter_reassignments_resp(
            Errors::None,
            None,
            vec![reassignable_topic_response("A", &[(0, Errors::None, None)])],
        ));
        let result = admin.alter_partition_reassignments_with_options(
            &reassignments_input(),
            AlterPartitionReassignmentsOptions::new(),
        );
        pump(&mut runnable, 5).await;
        let all_err = result.all().get().await.unwrap_err();
        assert_eq!(all_err.error(), Errors::UnknownServerError);
        let a0_err = result.values()[&TopicPartition::new("A", 0)].get().await.unwrap_err();
        assert_eq!(a0_err.error(), Errors::UnknownServerError);
    }

    /// Mirrors the partition-level and top-level error scenarios of
    /// `testAlterPartitionReassignments`.
    #[tokio::test]
    async fn test_alter_partition_reassignments_errors() {
        // Partition-level error: A-0 fails, B-0 succeeds.
        let (admin, mut runnable, _time, _nodes) = env();
        runnable.client_mut().prepare_response(alter_reassignments_resp(
            Errors::None,
            None,
            vec![
                reassignable_topic_response("A", &[(0, Errors::InvalidReplicaAssignment, Some("bad"))]),
                reassignable_topic_response("B", &[(0, Errors::None, None)]),
            ],
        ));
        let result = admin.alter_partition_reassignments_with_options(
            &reassignments_input(),
            AlterPartitionReassignmentsOptions::new(),
        );
        pump(&mut runnable, 5).await;
        assert_eq!(
            result.values()[&TopicPartition::new("A", 0)].get().await.unwrap_err().error(),
            Errors::InvalidReplicaAssignment
        );
        result.values()[&TopicPartition::new("B", 0)].get().await.unwrap();

        // Top-level error: the custom message propagates to every future.
        let (admin, mut runnable, _time, _nodes) = env();
        let error_message = "this is custom error message";
        runnable.client_mut().prepare_response(alter_reassignments_resp(
            Errors::ClusterAuthorizationFailed,
            Some(error_message),
            vec![
                reassignable_topic_response("A", &[(0, Errors::None, None)]),
                reassignable_topic_response("B", &[(0, Errors::None, None)]),
            ],
        ));
        let result = admin.alter_partition_reassignments_with_options(
            &reassignments_input(),
            AlterPartitionReassignmentsOptions::new(),
        );
        pump(&mut runnable, 5).await;
        let all_err = result.all().get().await.unwrap_err();
        assert_eq!(all_err.error(), Errors::ClusterAuthorizationFailed);
        assert_eq!(all_err.message(), error_message);
        assert_eq!(
            result.values()[&TopicPartition::new("A", 0)].get().await.unwrap_err().message(),
            error_message
        );
    }

    /// A null `ErrorMessage` must leave the error code's own text in place.
    ///
    /// Java builds these with `Errors.exception(errorMessage)`, which returns the
    /// pre-built exception (default text) when the message is null. Real brokers
    /// send null here: cancelling a reassignment with nothing in flight answers
    /// `NO_REASSIGNMENT_IN_PROGRESS` with no message, and the user must still see
    /// "No partition reassignment is in progress." — not an empty string.
    #[tokio::test]
    async fn test_alter_partition_reassignments_null_error_message_keeps_default_text() {
        let default_text = Errors::NoReassignmentInProgress.message();
        assert!(!default_text.is_empty(), "the code must have default text to preserve");

        // Top-level error with a null message (the observed broker behaviour).
        let (admin, mut runnable, _time, _nodes) = env();
        runnable.client_mut().prepare_response(alter_reassignments_resp(
            Errors::NoReassignmentInProgress,
            None,
            vec![
                reassignable_topic_response("A", &[(0, Errors::None, None)]),
                reassignable_topic_response("B", &[(0, Errors::None, None)]),
            ],
        ));
        let result = admin.alter_partition_reassignments_with_options(
            &reassignments_input(),
            AlterPartitionReassignmentsOptions::new(),
        );
        pump(&mut runnable, 5).await;
        let all_err = result.all().get().await.unwrap_err();
        assert_eq!(all_err.error(), Errors::NoReassignmentInProgress);
        assert_eq!(all_err.message(), default_text);
        let a0_err = result.values()[&TopicPartition::new("A", 0)].get().await.unwrap_err();
        assert_eq!(a0_err.message(), default_text);

        // Partition-level error with a null message goes through the same helper.
        let (admin, mut runnable, _time, _nodes) = env();
        runnable.client_mut().prepare_response(alter_reassignments_resp(
            Errors::None,
            None,
            vec![
                reassignable_topic_response("A", &[(0, Errors::NoReassignmentInProgress, None)]),
                reassignable_topic_response("B", &[(0, Errors::None, None)]),
            ],
        ));
        let result = admin.alter_partition_reassignments_with_options(
            &reassignments_input(),
            AlterPartitionReassignmentsOptions::new(),
        );
        pump(&mut runnable, 5).await;
        let a0_err = result.values()[&TopicPartition::new("A", 0)].get().await.unwrap_err();
        assert_eq!(a0_err.error(), Errors::NoReassignmentInProgress);
        assert_eq!(a0_err.message(), default_text);
        result.values()[&TopicPartition::new("B", 0)].get().await.unwrap();

        // An empty-but-present message is kept verbatim, as Java's null-only
        // check does — this is the distinction `unwrap_or_default()` erased.
        let (admin, mut runnable, _time, _nodes) = env();
        runnable.client_mut().prepare_response(alter_reassignments_resp(
            Errors::None,
            None,
            vec![
                reassignable_topic_response("A", &[(0, Errors::NoReassignmentInProgress, Some(""))]),
                reassignable_topic_response("B", &[(0, Errors::None, None)]),
            ],
        ));
        let result = admin.alter_partition_reassignments_with_options(
            &reassignments_input(),
            AlterPartitionReassignmentsOptions::new(),
        );
        pump(&mut runnable, 5).await;
        assert_eq!(
            result.values()[&TopicPartition::new("A", 0)].get().await.unwrap_err().message(),
            ""
        );
    }

    /// The `listPartitionReassignments` counterpart of
    /// `test_alter_partition_reassignments_null_error_message_keeps_default_text`.
    #[tokio::test]
    async fn test_list_partition_reassignments_null_error_message_keeps_default_text() {
        let default_text = Errors::ClusterAuthorizationFailed.message();
        assert!(!default_text.is_empty(), "the code must have default text to preserve");

        let (admin, mut runnable, _time, _nodes) = env();
        let mut data = ListPartitionReassignmentsResponseData::new();
        data.set_error_code(Errors::ClusterAuthorizationFailed.code());
        data.set_error_message(None);
        runnable
            .client_mut()
            .prepare_response(ConcreteResponse::ListPartitionReassignments(
                ListPartitionReassignmentsResponse::new(data),
            ));

        let result =
            admin.list_partition_reassignments_with_partitions_options(None, ListPartitionReassignmentsOptions::new());
        pump(&mut runnable, 5).await;
        let err = result.reassignments().get().await.unwrap_err();
        assert_eq!(err.error(), Errors::ClusterAuthorizationFailed);
        assert_eq!(err.message(), default_text);
    }

    /// Mirrors the unrepresentable-topic scenario of `testAlterPartitionReassignments`.
    #[tokio::test]
    async fn test_alter_partition_reassignments_unrepresentable() {
        let (admin, mut runnable, _time, _nodes) = env();
        let invalid_topic = TopicPartition::new("", 0);
        let invalid_partition = TopicPartition::new("ABC", -1);
        let valid = TopicPartition::new("A", 0);
        let mut reassignments = HashMap::new();
        reassignments.insert(
            invalid_partition.clone(),
            Some(NewPartitionReassignment::new(vec![1, 2, 3]).unwrap()),
        );
        reassignments.insert(
            invalid_topic.clone(),
            Some(NewPartitionReassignment::new(vec![1, 2, 3]).unwrap()),
        );
        reassignments.insert(valid.clone(), Some(NewPartitionReassignment::new(vec![1, 2, 3]).unwrap()));

        runnable.client_mut().prepare_response(alter_reassignments_resp(
            Errors::None,
            None,
            vec![reassignable_topic_response("A", &[(0, Errors::None, None)])],
        ));
        let result =
            admin.alter_partition_reassignments_with_options(&reassignments, AlterPartitionReassignmentsOptions::new());
        pump(&mut runnable, 5).await;
        assert_eq!(
            result.values()[&invalid_topic].get().await.unwrap_err().error(),
            Errors::InvalidTopicError
        );
        assert_eq!(
            result.values()[&invalid_partition].get().await.unwrap_err().error(),
            Errors::InvalidTopicError
        );
        result.values()[&valid].get().await.unwrap();
    }

    /// Mirrors the NOT_CONTROLLER scenario of `testAlterPartitionReassignments`.
    #[tokio::test]
    async fn test_alter_partition_reassignments_not_controller() {
        let (admin, mut runnable, time, nodes) = env();
        runnable.client_mut().prepare_response(alter_reassignments_resp(
            Errors::NotController,
            Some(Errors::NotController.message()),
            vec![
                reassignable_topic_response("A", &[(0, Errors::None, None)]),
                reassignable_topic_response("B", &[(0, Errors::None, None)]),
            ],
        ));
        runnable
            .client_mut()
            .prepare_response(ConcreteResponse::Metadata(RequestTestUtils::metadata_response(
                &nodes,
                Some("mock-cluster"),
                1,
                Vec::new(),
            )));
        runnable.client_mut().prepare_response(alter_reassignments_resp(
            Errors::None,
            None,
            vec![
                reassignable_topic_response("A", &[(0, Errors::None, None)]),
                reassignable_topic_response("B", &[(0, Errors::None, None)]),
            ],
        ));
        let result = admin.alter_partition_reassignments_with_options(
            &reassignments_input(),
            AlterPartitionReassignmentsOptions::new(),
        );
        for _ in 0..30 {
            if result.all().is_done() {
                break;
            }
            runnable.run_once().await;
            time.sleep(200);
        }
        result.all().get().await.unwrap();
        result.values()[&TopicPartition::new("A", 0)].get().await.unwrap();
        result.values()[&TopicPartition::new("B", 0)].get().await.unwrap();
    }

    fn ongoing_topic(name: &str, partition: i32) -> OngoingTopicReassignment {
        let mut pr = OngoingPartitionReassignment::new();
        pr.set_partition_index(partition);
        pr.set_replicas(vec![1, 2, 3, 4, 5, 6]);
        pr.set_adding_replicas(vec![4, 5, 6]);
        pr.set_removing_replicas(vec![1, 2, 3]);
        let mut topic = OngoingTopicReassignment::new();
        topic.set_name(name.to_string());
        topic.set_partitions(vec![pr]);
        topic
    }

    fn list_reassignments_resp(top_error: Errors, topics: Vec<OngoingTopicReassignment>) -> ConcreteResponse {
        let mut data = ListPartitionReassignmentsResponseData::new();
        data.set_error_code(top_error.code());
        data.set_error_message(Some(top_error.message().to_string()));
        data.set_topics(topics);
        ConcreteResponse::ListPartitionReassignments(ListPartitionReassignmentsResponse::new(data))
    }

    /// Mirrors `KafkaAdminClientTest.testListPartitionReassignments`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testListPartitionReassignments")]
    async fn test_list_partition_reassignments() {
        let tp1 = TopicPartition::new("A", 0);
        let tp2 = TopicPartition::new("B", 0);

        // 1. NOT_CONTROLLER handling then success.
        let (admin, mut runnable, time, nodes) = env();
        runnable
            .client_mut()
            .prepare_response(list_reassignments_resp(Errors::NotController, Vec::new()));
        runnable
            .client_mut()
            .prepare_response(ConcreteResponse::Metadata(RequestTestUtils::metadata_response(
                &nodes,
                Some("mock-cluster"),
                1,
                Vec::new(),
            )));
        runnable.client_mut().prepare_response(list_reassignments_resp(
            Errors::None,
            vec![ongoing_topic("A", 0), ongoing_topic("B", 0)],
        ));
        let result =
            admin.list_partition_reassignments_with_partitions_options(None, ListPartitionReassignmentsOptions::new());
        for _ in 0..30 {
            if result.reassignments().is_done() {
                break;
            }
            runnable.run_once().await;
            time.sleep(200);
        }
        result.reassignments().get().await.unwrap();

        // 2. UNKNOWN_TOPIC_OR_PARTITION.
        let (admin, mut runnable, _time, _nodes) = env();
        runnable
            .client_mut()
            .prepare_response(list_reassignments_resp(Errors::UnknownTopicOrPartition, Vec::new()));
        let partitions: HashSet<TopicPartition> = [tp1.clone(), tp2.clone()].into_iter().collect();
        let result = admin.list_partition_reassignments_with_partitions_options(
            Some(partitions),
            ListPartitionReassignmentsOptions::new(),
        );
        pump(&mut runnable, 5).await;
        assert_eq!(
            result.reassignments().get().await.unwrap_err().error(),
            Errors::UnknownTopicOrPartition
        );

        // 3. Success — the ongoing reassignments are mapped per partition.
        let (admin, mut runnable, _time, _nodes) = env();
        runnable.client_mut().prepare_response(list_reassignments_resp(
            Errors::None,
            vec![ongoing_topic("A", 0), ongoing_topic("B", 0)],
        ));
        let result =
            admin.list_partition_reassignments_with_partitions_options(None, ListPartitionReassignmentsOptions::new());
        pump(&mut runnable, 5).await;
        let reassignments = result.reassignments().get().await.unwrap();
        assert_eq!(reassignments[&tp1].adding_replicas(), &[4, 5, 6]);
        assert_eq!(reassignments[&tp1].removing_replicas(), &[1, 2, 3]);
        assert_eq!(reassignments[&tp1].replicas(), &[1, 2, 3, 4, 5, 6]);
        assert_eq!(reassignments[&tp2].replicas(), &[1, 2, 3, 4, 5, 6]);
    }

    fn list_offsets_resp_from(results: &[(TopicPartition, Errors, i64, i64, i32)]) -> ConcreteResponse {
        let mut data = ListOffsetsResponseData::new();
        let topics = results
            .iter()
            .map(|(tp, error, timestamp, offset, epoch)| {
                ListOffsetsResponse::singleton_list_offsets_topic_response(tp, *error, *timestamp, *offset, *epoch)
            })
            .collect();
        data.set_topics(topics);
        ConcreteResponse::ListOffsets(ListOffsetsResponse::new(data))
    }

    /// Mirrors `KafkaAdminClientTest.testListOffsets`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testListOffsets")]
    async fn test_list_offsets() {
        let (admin, mut runnable, _time, nodes) = env();
        let tp0 = TopicPartition::new("foo", 0);
        let tp1 = TopicPartition::new("bar", 0);
        let tp2 = TopicPartition::new("baz", 0);
        let tp3 = TopicPartition::new("qux", 0);
        // Lookup: all partitions lead by node0.
        runnable.client_mut().prepare_response(metadata_resp(
            &nodes,
            vec![
                topic_meta_leaders("foo", &[(0, 0)]),
                topic_meta_leaders("bar", &[(0, 0)]),
                topic_meta_leaders("baz", &[(0, 0)]),
                topic_meta_leaders("qux", &[(0, 0)]),
            ],
        ));
        runnable.client_mut().prepare_response(list_offsets_resp_from(&[
            (tp0.clone(), Errors::None, -1, 123, 321),
            (tp1.clone(), Errors::None, -1, 234, 432),
            (tp2.clone(), Errors::None, 123456789, 345, 543),
            (tp3.clone(), Errors::None, 234567890, 456, 654),
        ]));

        let mut partitions = HashMap::new();
        partitions.insert(tp0.clone(), OffsetSpec::latest());
        partitions.insert(tp1.clone(), OffsetSpec::earliest());
        partitions.insert(tp2.clone(), OffsetSpec::for_timestamp(1_000_000));
        partitions.insert(tp3.clone(), OffsetSpec::max_timestamp());
        let result = admin.list_offsets_with_options(&partitions, ListOffsetsOptions::new());
        pump_until(&mut runnable, 40, |_r| result.all().is_done()).await;

        let offsets = result.all().get().await.unwrap();
        assert_eq!(offsets[&tp0].offset(), 123);
        assert_eq!(offsets[&tp0].leader_epoch(), Some(321));
        assert_eq!(offsets[&tp0].timestamp(), -1);
        assert_eq!(offsets[&tp1].offset(), 234);
        assert_eq!(offsets[&tp2].offset(), 345);
        assert_eq!(offsets[&tp2].timestamp(), 123456789);
        assert_eq!(offsets[&tp3].offset(), 456);
        assert_eq!(offsets[&tp3].timestamp(), 234567890);
        assert_eq!(result.partition_result(&tp0).unwrap().get().await.unwrap().offset(), 123);
        // A partition that was not attempted yields an error.
        assert!(result.partition_result(&TopicPartition::new("unknown", 0)).is_err());
    }

    /// Mirrors `KafkaAdminClientTest.testListOffsetsNonRetriableErrors`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testListOffsetsNonRetriableErrors")]
    async fn test_list_offsets_non_retriable_errors() {
        let (admin, mut runnable, _time, nodes) = env();
        let tp0 = TopicPartition::new("foo", 0);
        runnable
            .client_mut()
            .prepare_response(metadata_resp(&nodes, vec![topic_meta_leaders("foo", &[(0, 0)])]));
        runnable.client_mut().prepare_response(list_offsets_resp_from(&[(
            tp0.clone(),
            Errors::TopicAuthorizationFailed,
            -1,
            -1,
            -1,
        )]));
        let mut partitions = HashMap::new();
        partitions.insert(tp0.clone(), OffsetSpec::latest());
        let result = admin.list_offsets_with_options(&partitions, ListOffsetsOptions::new());
        pump_until(&mut runnable, 40, |_r| result.all().is_done()).await;
        assert_eq!(result.all().get().await.unwrap_err().error(), Errors::TopicAuthorizationFailed);
    }

    /// Mirrors `KafkaAdminClientTest.testListOffsetsRetriableErrors`: a
    /// LEADER_NOT_AVAILABLE partition triggers a metadata re-lookup then a retry.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testListOffsetsRetriableErrors")]
    async fn test_list_offsets_retriable_errors() {
        let (admin, mut runnable, time, nodes) = env();
        let tp0 = TopicPartition::new("foo", 0);
        let tp1 = TopicPartition::new("foo", 1);
        let tp2 = TopicPartition::new("bar", 0);
        // Lookup: foo-0/foo-1 -> node0, bar-0 -> node1.
        runnable.client_mut().prepare_response(metadata_resp(
            &nodes,
            vec![
                topic_meta_leaders("foo", &[(0, 0), (1, 0)]),
                topic_meta_leaders("bar", &[(0, 1)]),
            ],
        ));
        // node0 fulfillment: foo-0 LEADER_NOT_AVAILABLE (re-lookup), foo-1 ok.
        runnable.client_mut().prepare_response_from(
            list_offsets_resp_from(&[
                (tp0.clone(), Errors::LeaderNotAvailable, -1, 123, 321),
                (tp1.clone(), Errors::None, -1, 987, 789),
            ]),
            &nodes[0],
        );
        // node1 fulfillment: bar-0 ok.
        runnable
            .client_mut()
            .prepare_response_from(list_offsets_resp_from(&[(tp2.clone(), Errors::None, -1, 456, 654)]), &nodes[1]);
        // metadata re-lookup for the unmapped foo-0.
        runnable
            .client_mut()
            .prepare_response(metadata_resp(&nodes, vec![topic_meta_leaders("foo", &[(0, 0), (1, 0)])]));
        // node0 fulfillment retry: foo-0 ok.
        runnable
            .client_mut()
            .prepare_response_from(list_offsets_resp_from(&[(tp0.clone(), Errors::None, -1, 345, 543)]), &nodes[0]);

        let mut partitions = HashMap::new();
        partitions.insert(tp0.clone(), OffsetSpec::latest());
        partitions.insert(tp1.clone(), OffsetSpec::latest());
        partitions.insert(tp2.clone(), OffsetSpec::latest());
        let result = admin.list_offsets_with_options(&partitions, ListOffsetsOptions::new());
        for _ in 0..60 {
            if result.all().is_done() {
                break;
            }
            runnable.run_once().await;
            time.sleep(100);
        }
        let offsets = result.all().get().await.unwrap();
        assert_eq!(offsets[&tp0].offset(), 345);
        assert_eq!(offsets[&tp1].offset(), 987);
        assert_eq!(offsets[&tp2].offset(), 456);
    }

    /// Mirrors `KafkaAdminClientTest.testListOffsetsMaxTimestampUnsupportedSingleOffsetSpec`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testListOffsetsMaxTimestampUnsupportedSingleOffsetSpec"
    )]
    async fn test_list_offsets_max_timestamp_unsupported_single_offset_spec() {
        let (admin, mut runnable, _time, nodes) = env();
        let tp0 = TopicPartition::new("foo", 0);
        runnable
            .client_mut()
            .prepare_response(metadata_resp(&nodes, vec![topic_meta_leaders("foo", &[(0, 0)])]));
        runnable.client_mut().prepare_unsupported_version_response();
        let mut partitions = HashMap::new();
        partitions.insert(tp0.clone(), OffsetSpec::max_timestamp());
        let result = admin.list_offsets_with_options(&partitions, ListOffsetsOptions::new());
        pump_until(&mut runnable, 40, |_r| result.all().is_done()).await;
        assert_eq!(result.all().get().await.unwrap_err().error(), Errors::UnsupportedVersion);
    }

    /// Mirrors `KafkaAdminClientTest.testListOffsetsMaxTimestampUnsupportedMultipleOffsetSpec`:
    /// only the MAX_TIMESTAMP partition fails; the other is retried and succeeds.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testListOffsetsMaxTimestampUnsupportedMultipleOffsetSpec"
    )]
    async fn test_list_offsets_max_timestamp_unsupported_multiple_offset_spec() {
        let (admin, mut runnable, time, nodes) = env();
        let tp0 = TopicPartition::new("foo", 0);
        let tp1 = TopicPartition::new("foo", 1);
        runnable
            .client_mut()
            .prepare_response(metadata_resp(&nodes, vec![topic_meta_leaders("foo", &[(0, 0), (1, 0)])]));
        runnable.client_mut().prepare_unsupported_version_response();
        // Retry for the non-max partition succeeds.
        runnable
            .client_mut()
            .prepare_response_from(list_offsets_resp_from(&[(tp1.clone(), Errors::None, -1, 345, 543)]), &nodes[0]);
        let mut partitions = HashMap::new();
        partitions.insert(tp0.clone(), OffsetSpec::max_timestamp());
        partitions.insert(tp1.clone(), OffsetSpec::latest());
        let result = admin.list_offsets_with_options(&partitions, ListOffsetsOptions::new());
        for _ in 0..60 {
            if result.partition_result(&tp0).unwrap().is_done() && result.partition_result(&tp1).unwrap().is_done() {
                break;
            }
            runnable.run_once().await;
            time.sleep(100);
        }
        assert_eq!(
            result.partition_result(&tp0).unwrap().get().await.unwrap_err().error(),
            Errors::UnsupportedVersion
        );
        assert_eq!(result.partition_result(&tp1).unwrap().get().await.unwrap().offset(), 345);
    }

    /// Mirrors `KafkaAdminClientTest.testListOffsetsPartialResponse`: the leader
    /// omits a result for one partition, which fails the sanity check.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testListOffsetsPartialResponse")]
    async fn test_list_offsets_partial_response() {
        let (admin, mut runnable, _time, nodes) = env();
        let tp0 = TopicPartition::new("foo", 0);
        let tp1 = TopicPartition::new("foo", 1);
        runnable
            .client_mut()
            .prepare_response(metadata_resp(&nodes, vec![topic_meta_leaders("foo", &[(0, 0), (1, 0)])]));
        // Only tp0 is present; tp1 is omitted.
        runnable
            .client_mut()
            .prepare_response(list_offsets_resp_from(&[(tp0.clone(), Errors::None, -2, 123, 456)]));
        let mut partitions = HashMap::new();
        partitions.insert(tp0.clone(), OffsetSpec::latest());
        partitions.insert(tp1.clone(), OffsetSpec::latest());
        let result = admin.list_offsets_with_options(&partitions, ListOffsetsOptions::new());
        pump_until(&mut runnable, 40, |_r| result.all().is_done()).await;
        assert!(result.partition_result(&tp0).unwrap().get().await.is_ok());
        assert!(result.partition_result(&tp1).unwrap().get().await.is_err());
        assert!(result.all().get().await.is_err());
    }

    /// KAFKA-20673. Reproduces the scenario where the partition-leader cache
    /// holds an entry pointing at a broker that has since left the cluster (for
    /// example after a broker is recycled with a new id). The cached leader
    /// sends the request straight to the fulfillment stage, but the admin client
    /// can never route it because the broker is no longer in the metadata.
    /// Without re-running the lookup, the call would sit unassigned until the
    /// request deadline expires and fail with "Timed out waiting for a node
    /// assignment". The admin client should instead re-resolve the leader and
    /// complete the request.
    ///
    /// Mirrors `KafkaAdminClientTest.testListOffsetsRetriesLookupWhenCachedLeaderLeavesCluster`.
    /// Java drops node1 by letting the periodic broker-info metadata refresh
    /// (driven by `metadata.max.age.ms=50`) observe a shrunk cluster; the Rust
    /// pump harness does not drive that refresh automatically, so the test
    /// reproduces the same observable precondition — node1 absent from the ready
    /// metadata — by updating the shared `AdminMetadataManager` cluster directly
    /// between the two calls. Everything else (cache seeding on the first call,
    /// the stale fulfillment fast-path on the second, the re-lookup, and the
    /// completion) exercises the production code path unchanged.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testListOffsetsRetriesLookupWhenCachedLeaderLeavesCluster"
    )]
    async fn test_list_offsets_retries_lookup_when_cached_leader_leaves_cluster() {
        // node0 and node1 both exist initially; foo-0 is led by node1.
        // A large metadata.max.age.ms + retry.backoff.ms keeps the periodic
        // broker-info metadata refresh from firing during the pump (the
        // `request_update` that the stale ConstantNodeId provider schedules
        // would otherwise consume the prepared topic-metadata response), so
        // node1's departure is driven solely by the direct metadata update
        // below.
        let (admin, mut runnable, time, nodes) =
            env_with_props(&[("metadata.max.age.ms", "300000"), ("retry.backoff.ms", "300000")]);
        let node0 = nodes[0].clone();
        let node1 = nodes[1].clone();
        let tp0 = TopicPartition::new("foo", 0);

        // First call: the lookup resolves foo-0 to node1 (and caches it), then
        // the offsets fetch succeeds on node1.
        runnable
            .client_mut()
            .prepare_response(metadata_resp(&nodes, vec![topic_meta_leaders("foo", &[(0, 1)])]));
        runnable
            .client_mut()
            .prepare_response_from(list_offsets_resp_from(&[(tp0.clone(), Errors::None, -1, 100, 5)]), &node1);

        let mut partitions = HashMap::new();
        partitions.insert(tp0.clone(), OffsetSpec::latest());
        let first = admin.list_offsets_with_options(&partitions, ListOffsetsOptions::new());
        pump_until(&mut runnable, 40, |_r| first.all().is_done()).await;
        assert_eq!(first.all().get().await.unwrap()[&tp0].offset(), 100);

        // node1 leaves the cluster: foo-0 is now led by node0, and node1 is gone
        // from the admin client's metadata. The partition-leader cache still
        // points foo-0 at node1.
        let shrunk = Cluster::with_invalid_topics_controller_topic_ids(
            Some("mock-cluster".to_string()),
            vec![node0.clone()],
            Vec::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            Some(node0.clone()),
            HashMap::new(),
        );
        admin.shared.metadata_manager.update(shrunk, admin.shared.time.milliseconds());

        // Second call: the cache sends foo-0 straight to fulfillment on node1,
        // which is gone. The admin client must re-resolve the leader (now node0)
        // via a fresh lookup rather than getting stuck until the deadline.
        runnable.client_mut().prepare_response(metadata_resp(
            std::slice::from_ref(&node0),
            vec![topic_meta_leaders("foo", &[(0, 0)])],
        ));
        runnable
            .client_mut()
            .prepare_response_from(list_offsets_resp_from(&[(tp0.clone(), Errors::None, -1, 200, 5)]), &node0);

        let second = admin.list_offsets_with_options(&partitions, ListOffsetsOptions::new());
        for _ in 0..60 {
            if second.all().is_done() {
                break;
            }
            runnable.run_once().await;
            time.sleep(20);
        }
        assert!(
            second.all().is_done(),
            "second listOffsets did not recover after the cached leader left the cluster"
        );
        assert_eq!(second.all().get().await.unwrap()[&tp0].offset(), 200);
    }

    // Skipped `KafkaAdminClientTest` listOffsets slices (with rationale):
    // - testListOffsetsEarliestLocalSpecMinVersion / testListOffsetsLatestTierSpecSpecMinVersion
    //   / testListOffsetsEarliestPendingUploadSpecSpecMinVersion only assert the
    //   `oldestAllowedVersion()` of the built request (8 / 9 / 11). That version
    //   selection lives entirely in `ListOffsetsHandler::build_batched_request`
    //   and is covered directly by `list_offsets_handler::tests::build_request_allowed_versions`
    //   and the `list_offsets_request` builder tests — re-driving it end-to-end
    //   through the network mock would add no coverage.

    /// The mock's `list_offsets` serves seeded beginning/end offsets and rejects
    /// timestamp specs.
    #[tokio::test]
    async fn test_mock_list_offsets() {
        let mock = mock_admin_client::Builder::new()
            .build()
            .expect("a fresh builder has one broker");
        let earliest = TopicPartition::new("t", 0);
        let latest = TopicPartition::new("t", 1);
        let ts = TopicPartition::new("t", 2);
        mock.update_beginning_offsets(HashMap::from([(earliest.clone(), 5)]));
        mock.update_end_offsets(HashMap::from([(latest.clone(), 99)]));
        let mut partitions = HashMap::new();
        partitions.insert(earliest.clone(), OffsetSpec::earliest());
        partitions.insert(latest.clone(), OffsetSpec::latest());
        partitions.insert(ts.clone(), OffsetSpec::for_timestamp(123));
        let result = mock.list_offsets_with_options(&partitions, ListOffsetsOptions::new());
        assert_eq!(result.partition_result(&earliest).unwrap().get().await.unwrap().offset(), 5);
        assert_eq!(result.partition_result(&latest).unwrap().get().await.unwrap().offset(), 99);
        assert!(result.partition_result(&ts).unwrap().get().await.is_err());
    }

    /// The mock's `alter_partition_reassignments` / `list_partition_reassignments`
    /// track reassignments against added topics.
    #[tokio::test]
    async fn test_mock_partition_reassignments() {
        let mock = mock_admin_client::Builder::new()
            .set_num_brokers(3)
            .and_then(mock_admin_client::Builder::build)
            .expect("num_brokers is at least 1");
        let leader = Node::new(0, "localhost".to_string(), 1000);
        let replicas = vec![
            Node::new(0, "localhost".to_string(), 1000),
            Node::new(1, "localhost".to_string(), 1001),
        ];
        mock.add_topic(false, "topic", vec![mock_topic_partition_info(0, &leader, replicas)], None)
            .expect("seeding a topic with known brokers succeeds");
        let tp = TopicPartition::new("topic", 0);
        let mut reassignments = HashMap::new();
        reassignments.insert(tp.clone(), Some(NewPartitionReassignment::new(vec![1, 2]).unwrap()));
        let result =
            mock.alter_partition_reassignments_with_options(&reassignments, AlterPartitionReassignmentsOptions::new());
        result.values()[&tp].get().await.unwrap();

        let listed = mock
            .list_partition_reassignments_with_partitions_options(None, ListPartitionReassignmentsOptions::new())
            .reassignments()
            .get()
            .await
            .unwrap();
        assert!(listed.contains_key(&tp));
        // target [1,2] vs current replicas [0,1]: adding 2, removing 0.
        assert_eq!(listed[&tp].adding_replicas(), &[2]);
        assert_eq!(listed[&tp].removing_replicas(), &[0]);
    }

    /// The mock's `elect_leaders` mirrors Java's `UnsupportedOperationException`.
    #[tokio::test]
    async fn test_mock_elect_leaders_unsupported() {
        let mock = mock_admin_client::Builder::new()
            .build()
            .expect("a fresh builder has one broker");
        let result = mock.elect_leaders_with_options(ElectionType::Preferred, None, ElectLeadersOptions::new());
        let err = result.partitions().get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnsupportedVersion);
    }

    // ---- Group listing / describe (Tier 2 Phase 1) ----

    fn listed_group(group_id: &str, protocol_type: &str, state: &str, group_type: &str) -> ConcreteResponse {
        use crate::ListGroupsResponseData;
        use crate::list_groups_response_data::ListedGroup;
        let mut g = ListedGroup::new();
        g.set_group_id(group_id.to_string())
            .set_protocol_type(protocol_type.to_string())
            .set_group_state(state.to_string())
            .set_group_type(group_type.to_string());
        let mut data = ListGroupsResponseData::new();
        data.set_groups(vec![g]);
        ConcreteResponse::ListGroups(crate::common::requests::ListGroupsResponse::new(data))
    }

    fn empty_list_groups_resp() -> ConcreteResponse {
        use crate::ListGroupsResponseData;
        ConcreteResponse::ListGroups(crate::common::requests::ListGroupsResponse::new(ListGroupsResponseData::new()))
    }

    fn find_coordinator_resp(entries: &[(&str, &Node)]) -> ConcreteResponse {
        use crate::FindCoordinatorResponseData;
        use crate::find_coordinator_response_data::Coordinator;
        let coordinators: Vec<Coordinator> = entries
            .iter()
            .map(|(key, node)| {
                let mut c = Coordinator::new();
                c.set_key(key.to_string())
                    .set_error_code(Errors::None.code())
                    .set_node_id(node.id())
                    .set_host(node.host().to_string())
                    .set_port(node.port());
                c
            })
            .collect();
        let mut data = FindCoordinatorResponseData::new();
        data.set_coordinators(coordinators);
        ConcreteResponse::FindCoordinator(crate::common::requests::FindCoordinatorResponse::new(data))
    }

    fn consumer_group_describe_resp(group_id: &str) -> ConcreteResponse {
        use crate::ConsumerGroupDescribeResponseData;
        use crate::consumer_group_describe_response_data::DescribedGroup;
        let mut group = DescribedGroup::new();
        group
            .set_group_id(group_id.to_string())
            .set_group_state("Stable".to_string())
            .set_group_epoch(5)
            .set_assignment_epoch(5)
            .set_assignor_name("uniform".to_string());
        let mut data = ConsumerGroupDescribeResponseData::new();
        data.set_groups(vec![group]);
        ConcreteResponse::ConsumerGroupDescribe(crate::common::requests::ConsumerGroupDescribeResponse::new(data))
    }

    fn consumer_group_describe_error_resp(group_id: &str, error: Errors, message: Option<&str>) -> ConcreteResponse {
        use crate::ConsumerGroupDescribeResponseData;
        use crate::consumer_group_describe_response_data::DescribedGroup;
        let mut group = DescribedGroup::new();
        group
            .set_group_id(group_id.to_string())
            .set_error_code(error.code())
            .set_error_message(message.map(str::to_string));
        let mut data = ConsumerGroupDescribeResponseData::new();
        data.set_groups(vec![group]);
        ConcreteResponse::ConsumerGroupDescribe(crate::common::requests::ConsumerGroupDescribeResponse::new(data))
    }

    fn describe_groups_error_resp(group_id: &str, error: Errors, message: Option<&str>) -> ConcreteResponse {
        use crate::DescribeGroupsResponseData;
        use crate::describe_groups_response_data::DescribedGroup;
        let mut group = DescribedGroup::new();
        group
            .set_group_id(group_id.to_string())
            .set_error_code(error.code())
            .set_error_message(message.map(str::to_string));
        let mut data = DescribeGroupsResponseData::new();
        data.set_groups(vec![group]);
        ConcreteResponse::DescribeGroups(crate::common::requests::DescribeGroupsResponse::new(data))
    }

    /// A `FindCoordinator` response carrying a single erroring coordinator for
    /// `key` (mirrors Java's `prepareFindCoordinatorResponse(error, key, Node.noNode())`
    /// for the retriable-error retry path).
    fn find_coordinator_error_resp(key: &str, error: Errors) -> ConcreteResponse {
        use crate::FindCoordinatorResponseData;
        use crate::find_coordinator_response_data::Coordinator;
        let mut c = Coordinator::new();
        c.set_key(key.to_string())
            .set_error_code(error.code())
            .set_node_id(-1)
            .set_host(String::new())
            .set_port(-1);
        let mut data = FindCoordinatorResponseData::new();
        data.set_coordinators(vec![c]);
        ConcreteResponse::FindCoordinator(crate::common::requests::FindCoordinatorResponse::new(data))
    }

    /// A `DescribeGroups` member for a classic group (mirrors
    /// `DescribeGroupsResponseData.DescribedGroupMember`).
    fn described_member(
        member_id: &str,
        group_instance_id: Option<&str>,
        client_id: &str,
        client_host: &str,
        member_assignment: Vec<u8>,
    ) -> crate::describe_groups_response_data::DescribedGroupMember {
        let mut m = crate::describe_groups_response_data::DescribedGroupMember::new();
        m.set_member_id(member_id.to_string())
            .set_group_instance_id(group_instance_id.map(str::to_string))
            .set_client_id(client_id.to_string())
            .set_client_host(client_host.to_string())
            .set_member_assignment(member_assignment);
        m
    }

    /// A full (non-error) `DescribeGroups` response for the given groups.
    fn describe_groups_full_resp(
        groups: Vec<crate::describe_groups_response_data::DescribedGroup>,
    ) -> ConcreteResponse {
        use crate::DescribeGroupsResponseData;
        let mut data = DescribeGroupsResponseData::new();
        data.set_groups(groups);
        ConcreteResponse::DescribeGroups(crate::common::requests::DescribeGroupsResponse::new(data))
    }

    /// Broker enumeration: `list_groups` fans out one `ListGroups` per broker and
    /// unions the results.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testListGroups")]
    async fn test_list_groups() {
        let (admin, mut runnable, _time, nodes) = env();
        runnable.client_mut().prepare_response(metadata_resp(&nodes, Vec::new()));
        runnable
            .client_mut()
            .prepare_response_from(listed_group("g1", "consumer", "Stable", "Consumer"), &nodes[0]);
        runnable
            .client_mut()
            .prepare_response_from(listed_group("g2", "consumer", "Stable", "Consumer"), &nodes[1]);
        runnable.client_mut().prepare_response_from(empty_list_groups_resp(), &nodes[2]);

        let result = admin.list_groups_with_options(ListGroupsOptions::new());
        pump_until(&mut runnable, 40, |_r| result.valid().is_done()).await;

        let mut ids: Vec<String> = result
            .valid()
            .get()
            .await
            .unwrap()
            .iter()
            .map(|g| g.group_id().to_string())
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["g1".to_string(), "g2".to_string()]);
        assert!(result.errors().get().await.unwrap().is_empty());
    }

    /// `list_groups`' protocol-type filter excludes non-matching groups.
    #[tokio::test]
    async fn test_list_groups_filters_protocol_type() {
        let (admin, mut runnable, _time, nodes) = env();
        runnable.client_mut().prepare_response(metadata_resp(&nodes, Vec::new()));
        runnable
            .client_mut()
            .prepare_response_from(listed_group("g1", "consumer", "Stable", "Consumer"), &nodes[0]);
        runnable
            .client_mut()
            .prepare_response_from(listed_group("connect", "connect", "Stable", "Classic"), &nodes[1]);
        runnable.client_mut().prepare_response_from(empty_list_groups_resp(), &nodes[2]);

        let options = ListGroupsOptions::new().with_protocol_types(HashSet::from(["consumer".to_string()]));
        let result = admin.list_groups_with_options(options);
        pump_until(&mut runnable, 40, |_r| result.valid().is_done()).await;

        let ids: Vec<String> = result
            .valid()
            .get()
            .await
            .unwrap()
            .iter()
            .map(|g| g.group_id().to_string())
            .collect();
        assert_eq!(ids, vec!["g1".to_string()]);
    }

    /// Translated from `KafkaAdminClientTest.testListConsumerGroups`.
    ///
    /// `list_groups(for_consumer_groups())` fans out one `ListGroups` per broker:
    /// an empty metadata response is retried, retriable per-broker errors are
    /// retried, `connector` groups are filtered out by the protocol-type filter,
    /// and a fatal broker error is surfaced through `all()` / `errors()` while
    /// `valid()` still carries the three consumer groups.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testListConsumerGroups")]
    async fn test_list_consumer_groups() {
        fn list_groups_error(error: Errors) -> ConcreteResponse {
            use crate::ListGroupsResponseData;
            let mut data = ListGroupsResponseData::new();
            data.set_error_code(error.code());
            ConcreteResponse::ListGroups(crate::common::requests::ListGroupsResponse::new(data))
        }

        let (admin, mut runnable, time, nodes) = env_nodes_with_props(4, &[("retries", "2")]);
        let consumer = ConsumerProtocol::PROTOCOL_TYPE;

        // Empty metadata response should be retried
        runnable.client_mut().prepare_response(metadata_resp(&[], Vec::new()));
        runnable.client_mut().prepare_response(metadata_resp(&nodes, Vec::new()));

        runnable.client_mut().prepare_response_from(
            listed_groups(&[
                ("group-1", consumer, "Stable", ""),
                ("group-connect-1", "connector", "Stable", ""),
            ]),
            &nodes[0],
        );
        // handle retriable errors
        runnable
            .client_mut()
            .prepare_response_from(list_groups_error(Errors::CoordinatorNotAvailable), &nodes[1]);
        runnable
            .client_mut()
            .prepare_response_from(list_groups_error(Errors::CoordinatorLoadInProgress), &nodes[1]);
        runnable.client_mut().prepare_response_from(
            listed_groups(&[
                ("group-2", consumer, "Stable", ""),
                ("group-connect-2", "connector", "Stable", ""),
            ]),
            &nodes[1],
        );
        runnable.client_mut().prepare_response_from(
            listed_groups(&[
                ("group-3", consumer, "Stable", ""),
                ("group-connect-3", "connector", "Stable", ""),
            ]),
            &nodes[2],
        );
        // fatal error
        runnable
            .client_mut()
            .prepare_response_from(list_groups_error(Errors::UnknownServerError), &nodes[3]);

        let result = admin.list_groups_with_options(ListGroupsOptions::for_consumer_groups());
        for _ in 0..80 {
            if result.all().is_done() {
                break;
            }
            runnable.run_once().await;
            time.sleep(100);
        }
        assert_eq!(result.all().get().await.unwrap_err().error(), Errors::UnknownServerError);

        let listings = result.valid().get().await.unwrap();
        assert_eq!(listings.len(), 3);
        let mut group_ids = HashSet::new();
        for listing in &listings {
            group_ids.insert(listing.group_id().to_string());
            assert!(listing.group_state().is_some());
        }
        assert_eq!(
            group_ids,
            HashSet::from(["group-1".to_string(), "group-2".to_string(), "group-3".to_string()])
        );
        assert_eq!(result.errors().get().await.unwrap().len(), 1);
    }

    /// Translated from `KafkaAdminClientTest.testListGroupsWithTypes`.
    ///
    /// Asserts the emitted `ListGroups` request carries the types filter
    /// derived from `ListGroupsOptions::with_types`, then that both listings are
    /// returned.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testListGroupsWithTypes")]
    async fn test_list_groups_with_types() {
        use crate::common::requests::AbstractRequest;

        let (admin, mut runnable, _time, nodes) = env_nodes_with_props(1, &[]);
        runnable.client_mut().prepare_response(metadata_resp(&nodes, Vec::new()));

        let options = ListGroupsOptions::new().with_types(HashSet::from([GroupType::Consumer]));
        let result = admin.list_groups_with_options(options);
        pump_until_request_queued(&mut runnable).await;

        // The single per-broker ListGroups request carries the types filter.
        {
            let reqs = runnable.client_mut().requests_mut();
            assert_eq!(reqs.len(), 1);
            match reqs[0].request_builder_mut().build().unwrap() {
                AbstractRequest::ListGroups(req) => {
                    assert!(req.data().states_filter.is_empty());
                    assert_eq!(req.data().types_filter, vec![GroupType::Consumer.to_string()]);
                },
                other => panic!("expected a ListGroups request, got {other:?}"),
            }
        }

        runnable.client_mut().respond_from(
            listed_groups(&[
                ("group-1", ConsumerProtocol::PROTOCOL_TYPE, "Stable", "Consumer"),
                ("group-2", "", "Empty", "Consumer"),
            ]),
            &nodes[0],
        );
        pump_until(&mut runnable, 40, |_r| result.valid().is_done()).await;

        let mut ids: Vec<String> = result
            .valid()
            .get()
            .await
            .unwrap()
            .iter()
            .map(|g| g.group_id().to_string())
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["group-1".to_string(), "group-2".to_string()]);
        assert!(result.errors().get().await.unwrap().is_empty());
    }

    /// Translated from `KafkaAdminClientTest.testListGroupsWithTypesOlderBrokerVersion`.
    ///
    /// A `SHARE`/`CONSUMER`-only types filter surfaces `UnsupportedVersion`
    /// (the broker's older `ListGroups` version cannot express it), while a
    /// `CLASSIC`-only filter is silently omitted at the older version and
    /// succeeds. The Rust `MockClient` does not negotiate API versions, so the
    /// broker-side downgrade is modeled two ways: the omit path is verified by
    /// building the emitted request at v4 (mirroring the negotiated version) and
    /// asserting the types filter is dropped, and the reject path is modeled
    /// with `prepare_unsupported_version_response` (the same version-mismatch
    /// response the real `NetworkClient` produces when the builder throws).
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testListGroupsWithTypesOlderBrokerVersion")]
    async fn test_list_groups_with_types_older_broker_version() {
        use crate::common::requests::AbstractRequest;

        let (admin, mut runnable, _time, nodes) = env_nodes_with_props(1, &[]);

        // A SHARE-only filter cannot be omitted, so it surfaces UnsupportedVersion.
        runnable.client_mut().prepare_response(metadata_resp(&nodes, Vec::new()));
        runnable.client_mut().prepare_unsupported_version_response();
        let result =
            admin.list_groups_with_options(ListGroupsOptions::new().with_types(HashSet::from([GroupType::Share])));
        pump_until(&mut runnable, 40, |_r| result.all().is_done()).await;
        assert_eq!(result.all().get().await.unwrap_err().error(), Errors::UnsupportedVersion);

        // A CLASSIC-only filter is omitted on an older broker and succeeds.
        runnable.client_mut().prepare_response(metadata_resp(&nodes, Vec::new()));
        let result =
            admin.list_groups_with_options(ListGroupsOptions::new().with_types(HashSet::from([GroupType::Classic])));
        pump_until_request_queued(&mut runnable).await;
        {
            let reqs = runnable.client_mut().requests_mut();
            assert_eq!(reqs.len(), 1);
            // At v5 the request still carries the classic types filter ...
            match reqs[0].request_builder_mut().build().unwrap() {
                AbstractRequest::ListGroups(req) => {
                    assert_eq!(req.data().types_filter, vec![GroupType::Classic.to_string()]);
                },
                other => panic!("expected a ListGroups request, got {other:?}"),
            }
            // ... but building at the older broker's v4 omits it (the request
            // succeeds against the older broker with an empty filter).
            match reqs[0].request_builder_mut().build_version(4).unwrap() {
                AbstractRequest::ListGroups(req) => assert!(req.data().types_filter.is_empty()),
                other => panic!("expected a ListGroups request, got {other:?}"),
            }
        }
        runnable.client_mut().respond_from(
            listed_groups(&[("group-1", ConsumerProtocol::PROTOCOL_TYPE, "Stable", "")]),
            &nodes[0],
        );
        pump_until(&mut runnable, 40, |_r| result.all().is_done()).await;
        let listings = result.all().get().await.unwrap();
        assert_eq!(listings.len(), 1);
        assert_eq!(listings[0].group_id(), "group-1");

        // A CONSUMER-only filter (without classic) also surfaces UnsupportedVersion.
        runnable.client_mut().prepare_response(metadata_resp(&nodes, Vec::new()));
        runnable.client_mut().prepare_unsupported_version_response();
        let result =
            admin.list_groups_with_options(ListGroupsOptions::new().with_types(HashSet::from([GroupType::Consumer])));
        pump_until(&mut runnable, 40, |_r| result.all().is_done()).await;
        assert_eq!(result.all().get().await.unwrap_err().error(), Errors::UnsupportedVersion);
    }

    /// Translated from `KafkaAdminClientTest.testListConsumerGroupsWithStates`.
    ///
    /// `for_consumer_groups()` derives a `[Classic, Consumer]` types filter; this
    /// asserts that filter reaches the wire request, then that both consumer
    /// groups are returned.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testListConsumerGroupsWithStates")]
    async fn test_list_consumer_groups_with_states() {
        use crate::common::requests::AbstractRequest;

        let (admin, mut runnable, _time, nodes) = env_nodes_with_props(1, &[]);
        runnable.client_mut().prepare_response(metadata_resp(&nodes, Vec::new()));

        let result = admin.list_groups_with_options(ListGroupsOptions::for_consumer_groups());
        pump_until_request_queued(&mut runnable).await;
        {
            let reqs = runnable.client_mut().requests_mut();
            match reqs[0].request_builder_mut().build().unwrap() {
                AbstractRequest::ListGroups(req) => {
                    let mut types = req.data().types_filter.clone();
                    types.sort();
                    assert_eq!(types, vec![GroupType::Classic.to_string(), GroupType::Consumer.to_string()]);
                },
                other => panic!("expected a ListGroups request, got {other:?}"),
            }
        }

        runnable.client_mut().respond_from(
            listed_groups(&[
                ("group-1", ConsumerProtocol::PROTOCOL_TYPE, "Stable", ""),
                ("group-2", "", "Empty", ""),
            ]),
            &nodes[0],
        );
        pump_until(&mut runnable, 40, |_r| result.valid().is_done()).await;
        let mut ids: Vec<String> = result
            .valid()
            .get()
            .await
            .unwrap()
            .iter()
            .map(|g| g.group_id().to_string())
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["group-1".to_string(), "group-2".to_string()]);
        assert!(result.errors().get().await.unwrap().is_empty());
    }

    /// Translated from
    /// `KafkaAdminClientTest.testListConsumerGroupsWithTypesOlderBrokerVersion`.
    ///
    /// A states filter reaches a v4 broker (states are v4+), and a `SHARE` types
    /// filter surfaces `UnsupportedVersion` against the older broker.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testListConsumerGroupsWithTypesOlderBrokerVersion"
    )]
    async fn test_list_consumer_groups_with_types_older_broker_version() {
        use crate::common::requests::AbstractRequest;

        let (admin, mut runnable, _time, nodes) = env_nodes_with_props(1, &[]);

        // States filter with no types filter is fine at v4.
        runnable.client_mut().prepare_response(metadata_resp(&nodes, Vec::new()));
        let result = admin
            .list_groups_with_options(ListGroupsOptions::new().in_group_states(HashSet::from([GroupState::Stable])));
        pump_until_request_queued(&mut runnable).await;
        {
            let reqs = runnable.client_mut().requests_mut();
            match reqs[0].request_builder_mut().build_version(4).unwrap() {
                AbstractRequest::ListGroups(req) => {
                    assert_eq!(req.data().states_filter, vec![GroupState::Stable.to_string()]);
                    assert!(req.data().types_filter.is_empty());
                },
                other => panic!("expected a ListGroups request, got {other:?}"),
            }
        }
        runnable.client_mut().respond_from(
            listed_groups(&[("group-1", ConsumerProtocol::PROTOCOL_TYPE, "Stable", "")]),
            &nodes[0],
        );
        pump_until(&mut runnable, 40, |_r| result.all().is_done()).await;
        assert_eq!(result.all().get().await.unwrap().len(), 1);

        // A SHARE types filter cannot be set against the older broker.
        runnable.client_mut().prepare_response(metadata_resp(&nodes, Vec::new()));
        runnable.client_mut().prepare_unsupported_version_response();
        let result =
            admin.list_groups_with_options(ListGroupsOptions::new().with_types(HashSet::from([GroupType::Share])));
        pump_until(&mut runnable, 40, |_r| result.all().is_done()).await;
        assert_eq!(result.all().get().await.unwrap_err().error(), Errors::UnsupportedVersion);
    }

    /// Translated from `KafkaAdminClientTest.testListConsumerGroupsMetadataFailure`.
    ///
    /// An empty metadata response leaves no brokers to send `ListGroups` to; with
    /// `retries=0` the metadata call fails terminally and `handle_failure` wraps
    /// it as "Failed to find brokers to send ListGroups".
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testListConsumerGroupsMetadataFailure")]
    async fn test_list_consumer_groups_metadata_failure() {
        let (admin, mut runnable, time, _nodes) = env_nodes_with_props(3, &[("retries", "0")]);
        // Empty broker list → no brokers to send to.
        runnable.client_mut().prepare_response(metadata_resp(&[], Vec::new()));

        let result = admin.list_groups_with_options(ListGroupsOptions::for_consumer_groups());
        for _ in 0..40 {
            if result.all().is_done() {
                break;
            }
            runnable.run_once().await;
            time.sleep(100);
        }
        let err = result.all().get().await.unwrap_err();
        // Java hardcodes "ListGroups" in the message (`KafkaAdminClient.java:3565`),
        // and `testListConsumerGroupsMetadataFailure` / `testListGroupsMetadataFailure`
        // assert only `KafkaException.class`. The Rust message used to substitute
        // the lower-cased Rust call name and append the cause's text (finding 243).
        assert_eq!(err.message(), "Failed to find brokers to send ListGroups");
        assert!(err.is_kafka_error(), "Java's assertFutureThrows(KafkaException.class): {err:?}");
        assert!(!err.is_api_error(), "a bare KafkaException is not an ApiException: {err:?}");
    }

    /// Translated from `KafkaAdminClientTest.testListGroupsMetadataFailure`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testListGroupsMetadataFailure")]
    async fn test_list_groups_metadata_failure() {
        let (admin, mut runnable, time, _nodes) = env_nodes_with_props(3, &[("retries", "0")]);
        runnable.client_mut().prepare_response(metadata_resp(&[], Vec::new()));

        let result = admin.list_groups_with_options(ListGroupsOptions::new());
        for _ in 0..40 {
            if result.all().is_done() {
                break;
            }
            runnable.run_once().await;
            time.sleep(100);
        }
        let err = result.all().get().await.unwrap_err();
        // Java hardcodes "ListGroups" in the message (`KafkaAdminClient.java:3565`),
        // and `testListConsumerGroupsMetadataFailure` / `testListGroupsMetadataFailure`
        // assert only `KafkaException.class`. The Rust message used to substitute
        // the lower-cased Rust call name and append the cause's text (finding 243).
        assert_eq!(err.message(), "Failed to find brokers to send ListGroups");
        assert!(err.is_kafka_error(), "Java's assertFutureThrows(KafkaException.class): {err:?}");
        assert!(!err.is_api_error(), "a bare KafkaException is not an ApiException: {err:?}");
    }

    /// `describe_consumer_groups` finds the coordinator then describes the group
    /// with the KIP-848 `ConsumerGroupDescribe` API.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeConsumerGroups")]
    async fn test_describe_consumer_groups() {
        let (admin, mut runnable, _time, nodes) = env();
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[("g1", &nodes[0])]));
        runnable
            .client_mut()
            .prepare_response_from(consumer_group_describe_resp("g1"), &nodes[0]);

        let result =
            admin.describe_consumer_groups_with_options(&["g1".to_string()], DescribeConsumerGroupsOptions::new());
        let future = result.described_groups()["g1"].clone();
        pump_until(&mut runnable, 40, |_r| future.is_done()).await;

        let description = future.get().await.unwrap();
        assert_eq!(description.group_id(), "g1");
        assert_eq!(description.group_type(), GroupType::Consumer);
        assert_eq!(description.group_state(), GroupState::Stable);
        assert_eq!(description.partition_assignor(), "uniform");
        // `coordinator()` is the Node the fulfillment request was routed to, and
        // Java hands the handler `Call.curNode()` — a fully resolved broker. The
        // whole endpoint must survive, not just the id: a fabricated
        // `Node::new(id, "", -1)` would still satisfy an id-only assertion.
        let coordinator = description.coordinator().expect("coordinator present");
        assert_eq!(coordinator.id(), 0);
        assert_eq!(coordinator.host(), "localhost");
        assert_eq!(coordinator.port(), 9092);
        assert_eq!(coordinator, &nodes[0]);
    }

    /// A `describe_consumer_groups` on a nonexistent group id surfaces
    /// `GROUP_ID_NOT_FOUND` after the classic fallback also reports it, keeping
    /// the more-informative `ConsumerGroupDescribe` message.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeConsumerGroupsGroupIdNotFound")]
    async fn test_describe_consumer_groups_group_id_not_found() {
        let (admin, mut runnable, time, nodes) = env();
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[("missing", &nodes[0])]));
        runnable.client_mut().prepare_response_from(
            consumer_group_describe_error_resp("missing", Errors::GroupIdNotFound, Some("informative message")),
            &nodes[0],
        );
        // Fallback: classic DescribeGroups also reports GROUP_ID_NOT_FOUND.
        runnable.client_mut().prepare_response_from(
            describe_groups_error_resp("missing", Errors::GroupIdNotFound, Some("terse message")),
            &nodes[0],
        );

        let result =
            admin.describe_consumer_groups_with_options(&["missing".to_string()], DescribeConsumerGroupsOptions::new());
        let future = result.described_groups()["missing"].clone();
        // The classic-API fallback is a driver retry gated on the retry backoff,
        // so the mock clock must advance for the second (DescribeGroups) request.
        for _ in 0..60 {
            if future.is_done() {
                break;
            }
            runnable.run_once().await;
            time.sleep(200);
        }

        let err = future.get().await.unwrap_err();
        assert_eq!(err.error(), Errors::GroupIdNotFound);
        assert_eq!(err.message(), "informative message");
    }

    /// Mirrors `testDescribeGroupsWithBothUnsupportedApis`: the first
    /// `ConsumerGroupDescribe` request fails with `UNSUPPORTED_VERSION`, the
    /// driver falls back to the classic `DescribeGroups` request, and when that
    /// too fails with `UNSUPPORTED_VERSION` the group future surfaces the
    /// `UnsupportedVersionException`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeGroupsWithBothUnsupportedApis")]
    async fn test_describe_groups_with_both_unsupported_apis() {
        let (admin, mut runnable, time, nodes) = env();
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[("g1", &nodes[0])]));
        // The first request sent is a ConsumerGroupDescribe request. Fail it to
        // fall back to the classic version.
        runnable.client_mut().prepare_unsupported_version_response();
        // Fail the classic DescribeGroups fallback as well.
        runnable.client_mut().prepare_unsupported_version_response();

        let result =
            admin.describe_consumer_groups_with_options(&["g1".to_string()], DescribeConsumerGroupsOptions::new());
        let future = result.described_groups()["g1"].clone();
        // The classic-API fallback is a driver retry gated on the retry backoff,
        // so the mock clock must advance for the second request to be sent.
        for _ in 0..60 {
            if future.is_done() {
                break;
            }
            runnable.run_once().await;
            time.sleep(200);
        }

        let err = future.get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnsupportedVersion);
    }

    /// Translated from `KafkaAdminClientTest.testDescribeClassicGroups`.
    ///
    /// Exercises the full classic-describe path: retriable `FindCoordinator`
    /// errors are retried, retriable/coordinator-moved `DescribeGroups` errors
    /// trigger a re-lookup, and the final response's two members have their
    /// assignment bytes decoded via `ConsumerProtocol::deserialize_assignment`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeClassicGroups")]
    async fn test_describe_classic_groups() {
        use crate::common::ClassicGroupState;
        use crate::consumer::consumer_partition_assignor::Assignment;
        use crate::consumer::internals::ConsumerProtocol;
        use crate::describe_groups_response_data::DescribedGroup;

        // Default retries (i32::MAX) with a small backoff so the retry/re-lookup
        // sequence completes; `env()`'s `retries=2` is too few for this chain.
        let (admin, mut runnable, time, nodes) = env_with_props(&[("retry.backoff.ms", "10")]);
        {
            let c = runnable.client_mut();
            // Retriable FindCoordinatorResponse errors should be retried.
            c.prepare_response(find_coordinator_error_resp("group-0", Errors::CoordinatorNotAvailable));
            c.prepare_response(find_coordinator_error_resp("group-0", Errors::CoordinatorLoadInProgress));
            c.prepare_response(find_coordinator_resp(&[("group-0", &nodes[0])]));
            // Retriable DescribeGroups error should be retried.
            c.prepare_response(describe_groups_error_resp("group-0", Errors::CoordinatorLoadInProgress, None));
            // NOT_COORDINATOR: the coordinator moved, so re-run the lookup.
            c.prepare_response(describe_groups_error_resp("group-0", Errors::NotCoordinator, None));
            c.prepare_response(find_coordinator_resp(&[("group-0", &nodes[0])]));
            // COORDINATOR_NOT_AVAILABLE: same, re-run the lookup.
            c.prepare_response(describe_groups_error_resp("group-0", Errors::CoordinatorNotAvailable, None));
            c.prepare_response(find_coordinator_resp(&[("group-0", &nodes[0])]));

            // Final good response: two members sharing one 3-partition assignment.
            let topic_partitions = vec![
                TopicPartition::new("my_topic", 0),
                TopicPartition::new("my_topic", 1),
                TopicPartition::new("my_topic", 2),
            ];
            let assignment_bytes = ConsumerProtocol::serialize_assignment(&Assignment::new(topic_partitions)).unwrap();
            let member_one = described_member("0", None, "clientId0", "clientHost", assignment_bytes.clone());
            let member_two = described_member("1", Some("static"), "clientId1", "clientHost", assignment_bytes.clone());
            let mut group = DescribedGroup::new();
            group
                .set_group_id("group-0".to_string())
                .set_protocol_type(ConsumerProtocol::PROTOCOL_TYPE.to_string())
                .set_group_state(ClassicGroupState::Stable.to_string())
                .set_members(vec![member_one, member_two]);
            c.prepare_response(describe_groups_full_resp(vec![group]));
        }

        let result =
            admin.describe_classic_groups_with_options(&["group-0".to_string()], DescribeClassicGroupsOptions::new());
        let future = result.described_groups()["group-0"].clone();
        for _ in 0..300 {
            if future.is_done() {
                break;
            }
            runnable.run_once().await;
            time.sleep(50);
        }

        let description = future.get().await.unwrap();
        assert_eq!(result.described_groups().len(), 1);
        assert_eq!(description.group_id(), "group-0");
        assert_eq!(description.state(), ClassicGroupState::Stable);
        assert_eq!(description.members().len(), 2);

        let expected_partitions: HashSet<TopicPartition> = [
            TopicPartition::new("my_topic", 0),
            TopicPartition::new("my_topic", 1),
            TopicPartition::new("my_topic", 2),
        ]
        .into_iter()
        .collect();
        for member in description.members() {
            assert_eq!(member.assignment().topic_partitions(), &expected_partitions);
        }
        // The static member carries its group instance id.
        let member_ids: Vec<&str> = description.members().iter().map(|m| m.consumer_id()).collect();
        assert!(member_ids.contains(&"0"));
        assert!(member_ids.contains(&"1"));
        let static_member = description.members().iter().find(|m| m.consumer_id() == "1").unwrap();
        assert_eq!(static_member.group_instance_id(), Some("static"));
        // Same contract as `test_describe_consumer_groups`: the coordinator
        // reaching public API is the resolved broker, endpoint included. This
        // path re-ran the lookup twice, so it also covers a re-mapped coordinator.
        assert_eq!(description.coordinator(), Some(&nodes[0]));
    }

    /// Translated from
    /// `KafkaAdminClientTest.testDescribeClassicGroupsWithAuthorizedOperationsOmitted`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeClassicGroupsWithAuthorizedOperationsOmitted"
    )]
    async fn test_describe_classic_groups_with_authorized_operations_omitted() {
        use crate::describe_groups_response_data::DescribedGroup;

        let (admin, mut runnable, _time, nodes) = env();
        {
            let c = runnable.client_mut();
            c.prepare_response(find_coordinator_resp(&[("group-0", &nodes[0])]));
            let mut group = DescribedGroup::new();
            group
                .set_group_id("group-0".to_string())
                .set_protocol_type(String::new())
                .set_authorized_operations(MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED);
            c.prepare_response_from(describe_groups_full_resp(vec![group]), &nodes[0]);
        }

        let result =
            admin.describe_classic_groups_with_options(&["group-0".to_string()], DescribeClassicGroupsOptions::new());
        let future = result.described_groups()["group-0"].clone();
        pump_until(&mut runnable, 40, |_r| future.is_done()).await;

        let description = future.get().await.unwrap();
        // Java asserts `assertNull(groupDescription.authorizedOperations())`: the
        // omitted sentinel means "not reported", which is not the same as a
        // broker reporting an empty set.
        assert_eq!(description.authorized_operations(), None);
    }

    /// Translated from `KafkaAdminClientTest.testDescribeMultipleClassicGroups`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDescribeMultipleClassicGroups")]
    async fn test_describe_multiple_classic_groups() {
        use crate::common::ClassicGroupState;
        use crate::consumer::consumer_partition_assignor::Assignment;
        use crate::consumer::internals::ConsumerProtocol;
        use crate::describe_groups_response_data::DescribedGroup;

        let (admin, mut runnable, _time, nodes) = env();
        {
            let c = runnable.client_mut();
            // Both group ids resolve to the same coordinator.
            c.prepare_response(find_coordinator_resp(&[("group-0", &nodes[0]), ("group-1", &nodes[0])]));

            let topic_partitions = vec![
                TopicPartition::new("my_topic", 0),
                TopicPartition::new("my_topic", 1),
                TopicPartition::new("my_topic", 2),
            ];
            let assignment_bytes = ConsumerProtocol::serialize_assignment(&Assignment::new(topic_partitions)).unwrap();

            let mut group0 = DescribedGroup::new();
            group0
                .set_group_id("group-0".to_string())
                .set_protocol_type(ConsumerProtocol::PROTOCOL_TYPE.to_string())
                .set_group_state(ClassicGroupState::Stable.to_string())
                .set_members(vec![
                    described_member("0", None, "clientId0", "clientHost", assignment_bytes.clone()),
                    described_member("1", None, "clientId1", "clientHost", assignment_bytes.clone()),
                ]);
            let mut group1 = DescribedGroup::new();
            group1
                .set_group_id("group-1".to_string())
                .set_protocol_type("other".to_string())
                .set_group_state(ClassicGroupState::Stable.to_string())
                .set_members(vec![
                    described_member("0", None, "clientId0", "clientHost", Vec::new()),
                    described_member("1", None, "clientId1", "clientHost", Vec::new()),
                ]);
            // Both groups map to one coordinator, so the batched handler sends a
            // single DescribeGroups request for both ids.
            c.prepare_response_from(describe_groups_full_resp(vec![group0, group1]), &nodes[0]);
        }

        let result = admin.describe_classic_groups_with_options(
            &["group-0".to_string(), "group-1".to_string()],
            DescribeClassicGroupsOptions::new(),
        );
        let g0 = result.described_groups()["group-0"].clone();
        let g1 = result.described_groups()["group-1"].clone();
        pump_until(&mut runnable, 60, |_r| g0.is_done() && g1.is_done()).await;

        assert_eq!(result.described_groups().len(), 2);
        let keys: HashSet<String> = result.described_groups().keys().cloned().collect();
        assert_eq!(keys, HashSet::from(["group-0".to_string(), "group-1".to_string()]));
        assert!(g0.get().await.is_ok());
        assert!(g1.get().await.is_ok());
    }

    /// Seeds a group config in the mock (the only way Java's mock populates its
    /// `groupConfigs` keyset is via `incrementalAlterConfigs` on a GROUP
    /// resource).
    async fn seed_mock_group(mock: &crate::admin::MockAdminClient, group_id: &str) {
        use crate::admin::{AlterConfigOp, ConfigEntry, OpType};
        let resource = ConfigResource::new(config_resource::Type::Group, group_id.to_string());
        let ops = vec![AlterConfigOp::new(
            ConfigEntry::new("consumer.session.timeout.ms".to_string(), Some("45000".to_string())),
            OpType::Set,
        )];
        let mut configs = HashMap::new();
        configs.insert(resource.clone(), ops);
        mock.incremental_alter_configs_with_options(&configs, AlterConfigsOptions::new())
            .all()
            .get()
            .await
            .unwrap();
    }

    /// The mock's `list_groups` returns one CONSUMER/STABLE listing per seeded
    /// group config (mirrors Java's mock).
    #[tokio::test]
    async fn test_mock_list_groups() {
        let mock = mock_admin_client::Builder::new()
            .build()
            .expect("a fresh builder has one broker");
        seed_mock_group(&mock, "g1").await;
        let result = mock.list_groups_with_options(ListGroupsOptions::new());
        let listings = result.valid().get().await.unwrap();
        assert_eq!(listings.len(), 1);
        assert_eq!(listings[0].group_id(), "g1");
        assert_eq!(listings[0].group_type(), Some(GroupType::Consumer));
        assert_eq!(listings[0].group_state(), Some(GroupState::Stable));
    }

    /// The mock's `describe_consumer_groups` mirrors Java's
    /// `UnsupportedOperationException`.
    #[tokio::test]
    async fn test_mock_describe_consumer_groups_unsupported() {
        let mock = mock_admin_client::Builder::new()
            .build()
            .expect("a fresh builder has one broker");
        let result =
            mock.describe_consumer_groups_with_options(&["g1".to_string()], DescribeConsumerGroupsOptions::new());
        let err = result.described_groups()["g1"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnsupportedVersion);
    }

    /// The mock's `describe_classic_groups` mirrors Java's
    /// `UnsupportedOperationException`.
    #[tokio::test]
    async fn test_mock_describe_classic_groups_unsupported() {
        let mock = mock_admin_client::Builder::new()
            .build()
            .expect("a fresh builder has one broker");
        let result =
            mock.describe_classic_groups_with_options(&["g1".to_string()], DescribeClassicGroupsOptions::new());
        let err = result.described_groups()["g1"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnsupportedVersion);
    }

    // --- consumer group offsets (list / alter / delete) ---------------------

    const GROUP_ID: &str = "group0";

    /// Drives `run_once` (advancing the mock clock so retry-backoff-gated retries
    /// fire) until `done` returns true or `max_iters` is reached.
    async fn drive_until(
        runnable: &mut AdminClientRunnable<MockClient>,
        time: &MockTime,
        max_iters: usize,
        done: impl Fn() -> bool,
    ) {
        for _ in 0..max_iters {
            if done() {
                return;
            }
            runnable.run_once().await;
            time.sleep(50);
        }
    }

    /// Builds an environment whose driver retries essentially unbounded, so
    /// multi-step retry chains complete (Java's default `retries` is high, but
    /// the Rust `test_config` caps it at 2).
    fn offsets_env(num_nodes: i32) -> (KafkaAdminClient, AdminClientRunnable<MockClient>, Arc<MockTime>, Vec<Node>) {
        env_nodes_with_props(num_nodes, &[("retries", "2147483647"), ("retry.backoff.ms", "10")])
    }

    fn offset_fetch_group_error(group: &str, error: Errors) -> ConcreteResponse {
        let mut g = crate::offset_fetch_response_data::OffsetFetchResponseGroup::new();
        g.set_group_id(group.to_string()).set_error_code(error.code());
        let mut data = crate::OffsetFetchResponseData::new();
        data.set_groups(vec![g]);
        ConcreteResponse::OffsetFetch(crate::common::requests::OffsetFetchResponse::new(
            data,
            crate::common::protocol::ApiKeys::OFFSET_FETCH.latest_version(),
        ))
    }

    /// Builds a full (per-partition) `OffsetFetch` response for one group.
    fn offset_fetch_full(group: &str, topic: &str, partitions: &[(i32, i64)]) -> ConcreteResponse {
        use crate::OffsetFetchResponseData;
        use crate::offset_fetch_response_data::{
            OffsetFetchResponseGroup, OffsetFetchResponsePartitions, OffsetFetchResponseTopics,
        };
        let wire_partitions: Vec<OffsetFetchResponsePartitions> = partitions
            .iter()
            .map(|(index, offset)| {
                let mut p = OffsetFetchResponsePartitions::new();
                p.set_partition_index(*index).set_committed_offset(*offset);
                p
            })
            .collect();
        let mut wire_topic = OffsetFetchResponseTopics::new();
        wire_topic.set_name(topic.to_string()).set_partitions(wire_partitions);
        let mut g = OffsetFetchResponseGroup::new();
        g.set_group_id(group.to_string()).set_topics(vec![wire_topic]);
        let mut data = OffsetFetchResponseData::new();
        data.set_groups(vec![g]);
        ConcreteResponse::OffsetFetch(crate::common::requests::OffsetFetchResponse::new(
            data,
            crate::common::protocol::ApiKeys::OFFSET_FETCH.latest_version(),
        ))
    }

    fn offset_commit_resp(entries: &[(TopicPartition, Errors)]) -> ConcreteResponse {
        let map: HashMap<TopicPartition, Errors> = entries.iter().cloned().collect();
        ConcreteResponse::OffsetCommit(
            crate::common::requests::OffsetCommitResponse::with_throttle_time_ms_response_data(0, &map),
        )
    }

    fn offset_delete_top_level(error: Errors) -> ConcreteResponse {
        let mut data = crate::OffsetDeleteResponseData::new();
        data.set_error_code(error.code());
        ConcreteResponse::OffsetDelete(crate::common::requests::OffsetDeleteResponse::new(data))
    }

    fn offset_delete_partition(topic: &str, partition: i32, error: Errors) -> ConcreteResponse {
        use crate::OffsetDeleteResponseData;
        use crate::offset_delete_response_data::{OffsetDeleteResponsePartition, OffsetDeleteResponseTopic};
        let mut p = OffsetDeleteResponsePartition::new();
        p.set_partition_index(partition).set_error_code(error.code());
        let mut t = OffsetDeleteResponseTopic::new();
        t.set_name(topic.to_string()).set_partitions(vec![p]);
        let mut data = OffsetDeleteResponseData::new();
        data.set_error_code(Errors::None.code());
        data.set_topics(vec![t]);
        ConcreteResponse::OffsetDelete(crate::common::requests::OffsetDeleteResponse::new(data))
    }

    /// An old (v<=3) single-coordinator `FindCoordinator` response — the form a
    /// non-batched `FindCoordinator` request receives. Mirrors Java's
    /// `prepareOldFindCoordinatorResponse`; the empty coordinator key binds the
    /// response to whichever single key requested it.
    fn old_find_coordinator_resp(node: &Node) -> ConcreteResponse {
        use crate::FindCoordinatorResponseData;
        let mut data = FindCoordinatorResponseData::new();
        data.set_error_code(Errors::None.code())
            .set_node_id(node.id())
            .set_host(node.host().to_string())
            .set_port(node.port());
        ConcreteResponse::FindCoordinator(crate::common::requests::FindCoordinatorResponse::new(data))
    }

    /// An old (single-coordinator) `FindCoordinator` error response, mirroring
    /// `prepareOldFindCoordinatorResponse(error, Node.noNode())`.
    fn old_find_coordinator_error_resp(error: Errors) -> ConcreteResponse {
        use crate::FindCoordinatorResponseData;
        let mut data = FindCoordinatorResponseData::new();
        data.set_error_code(error.code())
            .set_node_id(-1)
            .set_host(String::new())
            .set_port(-1);
        ConcreteResponse::FindCoordinator(crate::common::requests::FindCoordinatorResponse::new(data))
    }

    fn single_spec(partitions: &[TopicPartition]) -> HashMap<String, ListConsumerGroupOffsetsSpec> {
        HashMap::from([(
            GROUP_ID.to_string(),
            ListConsumerGroupOffsetsSpec::new().set_topic_partitions(Some(partitions.to_vec())),
        )])
    }

    /// Translated from `testListConsumerGroupOffsets`: retriable FindCoordinator
    /// and OffsetFetch errors are retried, and the final response's negative
    /// offset maps to `None`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testListConsumerGroupOffsets")]
    async fn test_list_consumer_group_offsets() {
        let (admin, mut runnable, time, nodes) = offsets_env(1);
        let tp0 = TopicPartition::new("my_topic", 0);
        let tp1 = TopicPartition::new("my_topic", 1);
        let tp2 = TopicPartition::new("my_topic", 2);
        let tp3 = TopicPartition::new("my_topic", 3);

        // Retriable FindCoordinator error is retried.
        runnable
            .client_mut()
            .prepare_response(find_coordinator_error_resp(GROUP_ID, Errors::CoordinatorNotAvailable));
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
        // Retriable OffsetFetch error is retried.
        runnable
            .client_mut()
            .prepare_response(offset_fetch_group_error(GROUP_ID, Errors::CoordinatorLoadInProgress));
        // NOT_COORDINATOR / COORDINATOR_NOT_AVAILABLE trigger a re-lookup.
        runnable
            .client_mut()
            .prepare_response(offset_fetch_group_error(GROUP_ID, Errors::NotCoordinator));
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
        runnable
            .client_mut()
            .prepare_response(offset_fetch_group_error(GROUP_ID, Errors::CoordinatorNotAvailable));
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
        runnable.client_mut().prepare_response(offset_fetch_full(
            GROUP_ID,
            "my_topic",
            &[(0, 10), (1, 0), (2, 20), (3, -1)],
        ));

        let result = admin.list_consumer_group_offsets_with_group_specs_options(
            &single_spec(&[tp0.clone(), tp1.clone(), tp2.clone(), tp3.clone()]),
            ListConsumerGroupOffsetsOptions::new(),
        );
        let future = result.partitions_to_offset_and_metadata().unwrap();
        drive_until(&mut runnable, &time, 80, || future.is_done()).await;

        let offsets = future.get().await.unwrap();
        assert_eq!(offsets.len(), 4);
        assert_eq!(offsets.get(&tp0).unwrap().as_ref().unwrap().offset(), 10);
        assert_eq!(offsets.get(&tp1).unwrap().as_ref().unwrap().offset(), 0);
        assert_eq!(offsets.get(&tp2).unwrap().as_ref().unwrap().offset(), 20);
        assert!(offsets.contains_key(&tp3));
        assert_eq!(offsets.get(&tp3).unwrap(), &None);
    }

    /// Translated from `testListConsumerGroupOffsetsNonRetriableErrors`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testListConsumerGroupOffsetsNonRetriableErrors")]
    async fn test_list_consumer_group_offsets_non_retriable_errors() {
        let (admin, mut runnable, time, nodes) = offsets_env(1);
        for error in [
            Errors::GroupAuthorizationFailed,
            Errors::InvalidGroupId,
            Errors::GroupIdNotFound,
            Errors::UnknownMemberId,
            Errors::StaleMemberEpoch,
        ] {
            runnable
                .client_mut()
                .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
            runnable
                .client_mut()
                .prepare_response(offset_fetch_group_error(GROUP_ID, error));

            let result = admin.list_consumer_group_offsets_with_group_specs_options(
                &single_spec(&[TopicPartition::new("t", 0)]),
                ListConsumerGroupOffsetsOptions::new(),
            );
            let future = result.partitions_to_offset_and_metadata().unwrap();
            drive_until(&mut runnable, &time, 40, || future.is_done()).await;
            assert_eq!(future.get().await.unwrap_err().error(), error);
        }
    }

    fn batched_specs() -> HashMap<String, ListConsumerGroupOffsetsSpec> {
        HashMap::from([
            (
                "groupA".to_string(),
                ListConsumerGroupOffsetsSpec::new().set_topic_partitions(Some(vec![TopicPartition::new("A", 1)])),
            ),
            (
                "groupB".to_string(),
                ListConsumerGroupOffsetsSpec::new().set_topic_partitions(Some(vec![TopicPartition::new("B", 2)])),
            ),
        ])
    }

    fn offset_fetch_multi(groups: &[(&str, &str, i32)]) -> ConcreteResponse {
        use crate::OffsetFetchResponseData;
        use crate::offset_fetch_response_data::{
            OffsetFetchResponseGroup, OffsetFetchResponsePartitions, OffsetFetchResponseTopics,
        };
        let wire_groups: Vec<OffsetFetchResponseGroup> = groups
            .iter()
            .map(|(group, topic, partition)| {
                let mut p = OffsetFetchResponsePartitions::new();
                p.set_partition_index(*partition).set_committed_offset(10);
                let mut t = OffsetFetchResponseTopics::new();
                t.set_name((*topic).to_string()).set_partitions(vec![p]);
                let mut g = OffsetFetchResponseGroup::new();
                g.set_group_id((*group).to_string()).set_topics(vec![t]);
                g
            })
            .collect();
        let mut data = OffsetFetchResponseData::new();
        data.set_groups(wire_groups);
        ConcreteResponse::OffsetFetch(crate::common::requests::OffsetFetchResponse::new(
            data,
            crate::common::protocol::ApiKeys::OFFSET_FETCH.latest_version(),
        ))
    }

    /// Translated from `testBatchedListConsumerGroupOffsets`: two groups behind a
    /// single (batched) FindCoordinator and OffsetFetch.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testBatchedListConsumerGroupOffsets")]
    async fn test_batched_list_consumer_group_offsets() {
        let (admin, mut runnable, time, nodes) = offsets_env(1);
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[("groupA", &nodes[0]), ("groupB", &nodes[0])]));
        runnable
            .client_mut()
            .prepare_response(offset_fetch_multi(&[("groupA", "A", 1), ("groupB", "B", 2)]));

        let result = admin.list_consumer_group_offsets_with_group_specs_options(
            &batched_specs(),
            ListConsumerGroupOffsetsOptions::new(),
        );
        let all = result.all();
        drive_until(&mut runnable, &time, 40, || all.is_done()).await;

        let map = all.get().await.unwrap();
        assert_eq!(map.len(), 2);
        // Each group's per-partition offsets match the requested spec.
        for group in ["groupA", "groupB"] {
            let future = result.partitions_to_offset_and_metadata_for_group(group).unwrap();
            assert_eq!(future.get().await.unwrap().len(), 1);
        }
    }

    /// Translated from `testBatchedListConsumerGroupOffsetsWithNoFindCoordinatorBatching`:
    /// a `NoBatchedFindCoordinatorsException` disables batching, after which the
    /// groups are looked up individually.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testBatchedListConsumerGroupOffsetsWithNoFindCoordinatorBatching"
    )]
    async fn test_batched_list_consumer_group_offsets_with_no_find_coordinator_batching() {
        let (admin, mut runnable, time, nodes) = offsets_env(1);
        // The batched FindCoordinator build fails (broker only supports v3) —
        // mirror the version-mismatch the real NetworkClient produces for
        // `NoBatchedFindCoordinatorsException`.
        runnable.client_mut().prepare_version_mismatch_response(
            "Cannot create a v3 FindCoordinator request because we require features supported only in 4 or later.",
        );
        // After disabling batching, each group is looked up individually with the
        // old single-coordinator FindCoordinator form.
        runnable.client_mut().prepare_response(old_find_coordinator_resp(&nodes[0]));
        runnable.client_mut().prepare_response(old_find_coordinator_resp(&nodes[0]));
        runnable
            .client_mut()
            .prepare_response(offset_fetch_multi(&[("groupA", "A", 1), ("groupB", "B", 2)]));
        runnable
            .client_mut()
            .prepare_response(offset_fetch_multi(&[("groupA", "A", 1), ("groupB", "B", 2)]));

        let result = admin.list_consumer_group_offsets_with_group_specs_options(
            &batched_specs(),
            ListConsumerGroupOffsetsOptions::new(),
        );
        let all = result.all();
        drive_until(&mut runnable, &time, 80, || all.is_done()).await;
        assert_eq!(all.get().await.unwrap().len(), 2);
    }

    /// Translated from `testBatchedListConsumerGroupOffsetsWithNoOffsetFetchBatching`:
    /// a `NoBatchedOffsetFetchRequestException` disables batching, after which
    /// both FindCoordinator and OffsetFetch are re-sent per group.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testBatchedListConsumerGroupOffsetsWithNoOffsetFetchBatching"
    )]
    async fn test_batched_list_consumer_group_offsets_with_no_offset_fetch_batching() {
        let (admin, mut runnable, time, nodes) = offsets_env(1);
        // Batched FindCoordinator succeeds, but the batched OffsetFetch build
        // fails (broker only supports v7) — a `NoBatchedOffsetFetchRequestException`.
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[("groupA", &nodes[0]), ("groupB", &nodes[0])]));
        runnable.client_mut().prepare_version_mismatch_response(
            "Broker does not support batching groups for fetch offset request on version 7",
        );
        // After disabling batching, FindCoordinator (old single-coordinator form)
        // + OffsetFetch are re-sent per group.
        runnable.client_mut().prepare_response(old_find_coordinator_resp(&nodes[0]));
        runnable.client_mut().prepare_response(old_find_coordinator_resp(&nodes[0]));
        runnable
            .client_mut()
            .prepare_response(offset_fetch_multi(&[("groupA", "A", 1), ("groupB", "B", 2)]));
        runnable
            .client_mut()
            .prepare_response(offset_fetch_multi(&[("groupA", "A", 1), ("groupB", "B", 2)]));

        let result = admin.list_consumer_group_offsets_with_group_specs_options(
            &batched_specs(),
            ListConsumerGroupOffsetsOptions::new(),
        );
        let all = result.all();
        drive_until(&mut runnable, &time, 80, || all.is_done()).await;
        assert_eq!(all.get().await.unwrap().len(), 2);
    }

    /// Translated from `KafkaAdminClientTest.testListConsumerGroupOffsetsOptionsWithBatchedApi`
    /// (helper `verifyListConsumerGroupOffsetsOptions`): the `requireStable`
    /// option and the request timeout propagate to the built `OffsetFetch` wire
    /// request, and the group id / topic / partition indexes map through.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testListConsumerGroupOffsetsOptionsWithBatchedApi"
    )]
    async fn test_list_consumer_group_offsets_options_with_batched_api() {
        use crate::common::requests::AbstractRequest;

        // Java uses mockCluster(3, 0) with RETRIES_CONFIG = "0".
        let (admin, mut runnable, _time, nodes) = env_with_props(&[("retries", "0")]);
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));

        let options = ListConsumerGroupOffsetsOptions::new()
            .set_require_stable(true)
            .set_timeout_ms(Some(300));
        let _result = admin.list_consumer_group_offsets_with_group_specs_options(
            &single_spec(&[TopicPartition::new("A", 0)]),
            options,
        );

        // Pump until the `OffsetFetch` request is queued. The `FindCoordinator`
        // request is matched to the prepared response at send time (so it never
        // enters the queue); the first request left in the queue is the
        // `OffsetFetch` request built after the coordinator is resolved.
        pump_until_request_queued(&mut runnable).await;

        let reqs = runnable.client_mut().requests_mut();
        assert_eq!(reqs.len(), 1);
        let client_request = &mut reqs[0];
        // The `ListConsumerGroupOffsetsOptions.timeoutMs(300)` propagates to the
        // sent request's timeout (Java asserts clientRequest.requestTimeoutMs()).
        assert_eq!(client_request.request_timeout_ms(), 300);
        match client_request.request_builder_mut().build().unwrap() {
            AbstractRequest::OffsetFetch(req) => {
                let data = req.data();
                // The core contract this test pins: requireStable(true) reaches
                // the wire.
                assert!(data.require_stable);
                let group_ids: Vec<String> = data.groups.iter().map(|g| g.group_id.clone()).collect();
                assert_eq!(group_ids, vec![GROUP_ID.to_string()]);
                let group = &data.groups[0];
                let topics = group.topics.as_ref().expect("topics present");
                let topic_names: Vec<String> = topics.iter().map(|t| t.name.clone()).collect();
                assert_eq!(topic_names, vec!["A".to_string()]);
                assert_eq!(topics[0].partition_indexes, vec![0]);
            },
            other => panic!("expected an OffsetFetch request, got {other:?}"),
        }
    }

    fn offsets_to_alter() -> HashMap<TopicPartition, OffsetAndMetadata> {
        HashMap::from([
            (TopicPartition::new("foo", 0), OffsetAndMetadata::new(123).unwrap()),
            (TopicPartition::new("bar", 0), OffsetAndMetadata::new(456).unwrap()),
        ])
    }

    /// Translated from `testAlterConsumerGroupOffsets` (happy path).
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testAlterConsumerGroupOffsets")]
    async fn test_alter_consumer_group_offsets() {
        let (admin, mut runnable, time, nodes) = offsets_env(1);
        let tp1 = TopicPartition::new("foo", 0);
        let tp2 = TopicPartition::new("bar", 0);
        let tp3 = TopicPartition::new("foobar", 0);
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
        runnable
            .client_mut()
            .prepare_response(offset_commit_resp(&[(tp1.clone(), Errors::None), (tp2.clone(), Errors::None)]));

        let result = admin.alter_consumer_group_offsets_with_options(
            GROUP_ID,
            &offsets_to_alter(),
            AlterConsumerGroupOffsetsOptions::new(),
        );
        let all = result.all();
        drive_until(&mut runnable, &time, 40, || all.is_done()).await;

        assert_eq!(all.get().await.unwrap(), ());
        assert_eq!(result.partition_result(&tp1).get().await.unwrap(), ());
        assert_eq!(result.partition_result(&tp2).get().await.unwrap(), ());
        // A partition not in the request fails with IllegalArgument.
        assert!(matches!(
            result.partition_result(&tp3).get().await.unwrap_err(),
            Error::LocalIllegalArgument(_)
        ));
    }

    /// Translated from `testOffsetCommitWithMultipleErrors`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testOffsetCommitWithMultipleErrors")]
    async fn test_offset_commit_with_multiple_errors() {
        let (admin, mut runnable, time, nodes) = offsets_env(1);
        let foo0 = TopicPartition::new("foo", 0);
        let foo1 = TopicPartition::new("foo", 1);
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
        runnable.client_mut().prepare_response(offset_commit_resp(&[
            (foo0.clone(), Errors::None),
            (foo1.clone(), Errors::UnknownTopicOrPartition),
        ]));

        let offsets = HashMap::from([
            (foo0.clone(), OffsetAndMetadata::new(123).unwrap()),
            (foo1.clone(), OffsetAndMetadata::new(456).unwrap()),
        ]);
        let result = admin.alter_consumer_group_offsets_with_options(
            GROUP_ID,
            &offsets,
            AlterConsumerGroupOffsetsOptions::new(),
        );
        let all = result.all();
        drive_until(&mut runnable, &time, 40, || all.is_done()).await;

        assert_eq!(result.partition_result(&foo0).get().await.unwrap(), ());
        assert_eq!(
            result.partition_result(&foo1).get().await.unwrap_err().error(),
            Errors::UnknownTopicOrPartition
        );
        assert_eq!(all.get().await.unwrap_err().error(), Errors::UnknownTopicOrPartition);
    }

    /// Translated from `testAlterConsumerGroupOffsetsNonRetriableErrors`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testAlterConsumerGroupOffsetsNonRetriableErrors"
    )]
    async fn test_alter_consumer_group_offsets_non_retriable_errors() {
        let (admin, mut runnable, time, nodes) = offsets_env(1);
        let tp1 = TopicPartition::new("foo", 0);
        for error in [
            Errors::GroupAuthorizationFailed,
            Errors::InvalidGroupId,
            Errors::GroupIdNotFound,
            Errors::StaleMemberEpoch,
        ] {
            runnable
                .client_mut()
                .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
            runnable
                .client_mut()
                .prepare_response(offset_commit_resp(&[(tp1.clone(), error)]));

            let offsets = HashMap::from([(tp1.clone(), OffsetAndMetadata::new(123).unwrap())]);
            let result = admin.alter_consumer_group_offsets_with_options(
                GROUP_ID,
                &offsets,
                AlterConsumerGroupOffsetsOptions::new(),
            );
            let all = result.all();
            drive_until(&mut runnable, &time, 40, || all.is_done()).await;
            assert_eq!(all.get().await.unwrap_err().error(), error);
            assert_eq!(result.partition_result(&tp1).get().await.unwrap_err().error(), error);
        }
    }

    /// Translated from `testAlterConsumerGroupOffsetsFindCoordinatorNonRetriableErrors`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testAlterConsumerGroupOffsetsFindCoordinatorNonRetriableErrors"
    )]
    async fn test_alter_consumer_group_offsets_find_coordinator_non_retriable_errors() {
        let (admin, mut runnable, time, _nodes) = offsets_env(1);
        let tp1 = TopicPartition::new("foo", 0);
        runnable
            .client_mut()
            .prepare_response(find_coordinator_error_resp(GROUP_ID, Errors::GroupAuthorizationFailed));

        let offsets = HashMap::from([(tp1.clone(), OffsetAndMetadata::new(123).unwrap())]);
        let result = admin.alter_consumer_group_offsets_with_options(
            GROUP_ID,
            &offsets,
            AlterConsumerGroupOffsetsOptions::new(),
        );
        let all = result.all();
        drive_until(&mut runnable, &time, 40, || all.is_done()).await;
        assert_eq!(all.get().await.unwrap_err().error(), Errors::GroupAuthorizationFailed);
        assert_eq!(
            result.partition_result(&tp1).get().await.unwrap_err().error(),
            Errors::GroupAuthorizationFailed
        );
    }

    /// Translated from `testDeleteConsumerGroupOffsets` (happy path with one
    /// partition-level `GROUP_SUBSCRIBED_TO_TOPIC`).
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDeleteConsumerGroupOffsets")]
    async fn test_delete_consumer_group_offsets() {
        let (admin, mut runnable, time, nodes) = offsets_env(1);
        let tp1 = TopicPartition::new("foo", 0);
        let tp2 = TopicPartition::new("bar", 0);
        let tp3 = TopicPartition::new("foobar", 0);
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
        // Two topics: foo -> NONE, bar -> GROUP_SUBSCRIBED_TO_TOPIC.
        {
            use crate::OffsetDeleteResponseData;
            use crate::offset_delete_response_data::{OffsetDeleteResponsePartition, OffsetDeleteResponseTopic};
            let mut foo_p = OffsetDeleteResponsePartition::new();
            foo_p.set_partition_index(0).set_error_code(Errors::None.code());
            let mut foo_t = OffsetDeleteResponseTopic::new();
            foo_t.set_name("foo".to_string()).set_partitions(vec![foo_p]);
            let mut bar_p = OffsetDeleteResponsePartition::new();
            bar_p
                .set_partition_index(0)
                .set_error_code(Errors::GroupSubscribedToTopic.code());
            let mut bar_t = OffsetDeleteResponseTopic::new();
            bar_t.set_name("bar".to_string()).set_partitions(vec![bar_p]);
            let mut data = OffsetDeleteResponseData::new();
            data.set_topics(vec![foo_t, bar_t]);
            runnable.client_mut().prepare_response(ConcreteResponse::OffsetDelete(
                crate::common::requests::OffsetDeleteResponse::new(data),
            ));
        }

        let result = admin.delete_consumer_group_offsets_with_options(
            GROUP_ID,
            &HashSet::from([tp1.clone(), tp2.clone()]),
            DeleteConsumerGroupOffsetsOptions::new(),
        );
        let all = result.all();
        drive_until(&mut runnable, &time, 40, || all.is_done()).await;

        assert_eq!(result.partition_result(&tp1).unwrap().get().await.unwrap(), ());
        assert_eq!(all.get().await.unwrap_err().error(), Errors::GroupSubscribedToTopic);
        assert_eq!(
            result.partition_result(&tp2).unwrap().get().await.unwrap_err().error(),
            Errors::GroupSubscribedToTopic
        );
        // A partition not in the request fails synchronously with IllegalArgument.
        assert!(matches!(result.partition_result(&tp3), Err(Error::LocalIllegalArgument(_))));
    }

    /// Translated from `testDeleteConsumerGroupOffsetsNonRetriableErrors`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDeleteConsumerGroupOffsetsNonRetriableErrors"
    )]
    async fn test_delete_consumer_group_offsets_non_retriable_errors() {
        let (admin, mut runnable, time, nodes) = offsets_env(1);
        let tp1 = TopicPartition::new("foo", 0);
        for error in [
            Errors::GroupAuthorizationFailed,
            Errors::InvalidGroupId,
            Errors::GroupIdNotFound,
        ] {
            runnable
                .client_mut()
                .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
            runnable.client_mut().prepare_response(offset_delete_top_level(error));

            let result = admin.delete_consumer_group_offsets_with_options(
                GROUP_ID,
                &HashSet::from([tp1.clone()]),
                DeleteConsumerGroupOffsetsOptions::new(),
            );
            let all = result.all();
            drive_until(&mut runnable, &time, 40, || all.is_done()).await;
            assert_eq!(all.get().await.unwrap_err().error(), error);
            assert_eq!(result.partition_result(&tp1).unwrap().get().await.unwrap_err().error(), error);
        }
    }

    /// Translated from `testDeleteConsumerGroupOffsetsFindCoordinatorNonRetriableErrors`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDeleteConsumerGroupOffsetsFindCoordinatorNonRetriableErrors"
    )]
    async fn test_delete_consumer_group_offsets_find_coordinator_non_retriable_errors() {
        let (admin, mut runnable, time, _nodes) = offsets_env(1);
        let tp1 = TopicPartition::new("foo", 0);
        runnable
            .client_mut()
            .prepare_response(find_coordinator_error_resp(GROUP_ID, Errors::GroupAuthorizationFailed));

        let result = admin.delete_consumer_group_offsets_with_options(
            GROUP_ID,
            &HashSet::from([tp1.clone()]),
            DeleteConsumerGroupOffsetsOptions::new(),
        );
        let all = result.all();
        drive_until(&mut runnable, &time, 40, || all.is_done()).await;
        assert_eq!(all.get().await.unwrap_err().error(), Errors::GroupAuthorizationFailed);
        assert_eq!(
            result.partition_result(&tp1).unwrap().get().await.unwrap_err().error(),
            Errors::GroupAuthorizationFailed
        );
    }

    /// Translated from `testDeleteConsumerGroupOffsetsRetriableErrors`: retriable
    /// group errors are retried (with re-lookup for coordinator-moved errors).
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDeleteConsumerGroupOffsetsRetriableErrors")]
    async fn test_delete_consumer_group_offsets_retriable_errors() {
        let (admin, mut runnable, time, nodes) = offsets_env(1);
        let tp1 = TopicPartition::new("foo", 0);
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
        runnable
            .client_mut()
            .prepare_response(offset_delete_top_level(Errors::CoordinatorLoadInProgress));
        runnable
            .client_mut()
            .prepare_response(offset_delete_top_level(Errors::NotCoordinator));
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
        runnable
            .client_mut()
            .prepare_response(offset_delete_top_level(Errors::CoordinatorNotAvailable));
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
        runnable
            .client_mut()
            .prepare_response(offset_delete_partition("foo", 0, Errors::None));

        let result = admin.delete_consumer_group_offsets_with_options(
            GROUP_ID,
            &HashSet::from([tp1.clone()]),
            DeleteConsumerGroupOffsetsOptions::new(),
        );
        let all = result.all();
        drive_until(&mut runnable, &time, 80, || all.is_done()).await;
        assert_eq!(all.get().await.unwrap(), ());
        assert_eq!(result.partition_result(&tp1).unwrap().get().await.unwrap(), ());
    }

    // --- deleteConsumerGroups / removeMembersFromConsumerGroup --------------

    /// A `DeleteGroups` response with one result per (group, error) entry.
    fn delete_groups_resp(entries: &[(&str, Errors)]) -> ConcreteResponse {
        use crate::DeleteGroupsResponseData;
        use crate::delete_groups_response_data::DeletableGroupResult;
        let results: Vec<DeletableGroupResult> = entries
            .iter()
            .map(|(group_id, error)| {
                let mut r = DeletableGroupResult::new();
                r.set_group_id((*group_id).to_string()).set_error_code(error.code());
                r
            })
            .collect();
        let mut data = DeleteGroupsResponseData::new();
        data.set_results(results);
        ConcreteResponse::DeleteGroups(crate::common::requests::DeleteGroupsResponse::new(data))
    }

    /// A `LeaveGroup` response carrying only a top-level error.
    fn leave_group_top_level(error: Errors) -> ConcreteResponse {
        use crate::LeaveGroupResponseData;
        let mut data = LeaveGroupResponseData::new();
        data.set_error_code(error.code());
        ConcreteResponse::LeaveGroup(crate::common::requests::LeaveGroupResponse::new(data))
    }

    /// A successful `LeaveGroup` response with one member response per
    /// `(group.instance.id, error)` entry (member id echoed as empty, as the
    /// broker does for a static member removed by instance id).
    fn leave_group_members_resp(members: &[(&str, Errors)]) -> ConcreteResponse {
        use crate::LeaveGroupResponseData;
        use crate::leave_group_response_data::MemberResponse;
        let member_responses: Vec<MemberResponse> = members
            .iter()
            .map(|(instance_id, error)| {
                let mut m = MemberResponse::new();
                m.set_group_instance_id(Some((*instance_id).to_string()))
                    .set_error_code(error.code());
                m
            })
            .collect();
        let mut data = LeaveGroupResponseData::new();
        data.set_error_code(Errors::None.code()).set_members(member_responses);
        ConcreteResponse::LeaveGroup(crate::common::requests::LeaveGroupResponse::new(data))
    }

    /// A `ConsumerGroupDescribe` response listing static members (used by the
    /// `removeAll` describe path). Each member carries a `group.instance.id`.
    fn consumer_group_describe_members_resp(group_id: &str, instance_ids: &[&str]) -> ConcreteResponse {
        use crate::ConsumerGroupDescribeResponseData;
        use crate::consumer_group_describe_response_data::{DescribedGroup, Member};
        let members: Vec<Member> = instance_ids
            .iter()
            .enumerate()
            .map(|(i, instance_id)| {
                let mut m = Member::new();
                m.set_member_id(format!("member-{i}"))
                    .set_instance_id(Some((*instance_id).to_string()));
                m
            })
            .collect();
        let mut group = DescribedGroup::new();
        group
            .set_group_id(group_id.to_string())
            .set_group_state("Stable".to_string())
            .set_group_epoch(5)
            .set_assignment_epoch(5)
            .set_assignor_name("uniform".to_string())
            .set_members(members);
        let mut data = ConsumerGroupDescribeResponseData::new();
        data.set_groups(vec![group]);
        ConcreteResponse::ConsumerGroupDescribe(crate::common::requests::ConsumerGroupDescribeResponse::new(data))
    }

    fn members_to_remove(instance_ids: &[&str]) -> RemoveMembersFromConsumerGroupOptions {
        RemoveMembersFromConsumerGroupOptions::new(instance_ids.iter().map(|id| MemberToRemove::new(*id))).unwrap()
    }

    /// Translated from `testDeleteConsumerGroupsNumRetries`: with `retries=0`, a
    /// `NOT_COORDINATOR` re-lookup exhausts the retry budget and the deletion
    /// times out.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDeleteConsumerGroupsNumRetries")]
    async fn test_delete_consumer_groups_num_retries() {
        let default_api_timeout: i64 = 60000;
        let (admin, mut runnable, time, nodes) = env_nodes_with_props(
            3,
            &[
                ("default.api.timeout.ms", &default_api_timeout.to_string()),
                ("retries", "0"),
            ],
        );
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[("groupId", &nodes[0])]));
        runnable
            .client_mut()
            .prepare_response(delete_groups_resp(&[("groupId", Errors::NotCoordinator)]));
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[("groupId", &nodes[0])]));

        let result =
            admin.delete_consumer_groups_with_options(&["groupId".to_string()], DeleteConsumerGroupsOptions::new());
        let all = result.all();
        pump_until(&mut runnable, 40, |r| !r.client_mut().has_pending_responses()).await;
        time.sleep(default_api_timeout + 1);
        drive_until(&mut runnable, &time, 40, || all.is_done()).await;
        assert!(matches!(all.get().await.unwrap_err(), Error::Timeout(_)));
    }

    /// Translated from `testDeleteConsumerGroupsWithOlderBroker`: retriable
    /// `FindCoordinator` errors are retried, non-retriable ones fail, and
    /// coordinator-moved `DeleteGroups` errors trigger a re-lookup. Uses the old
    /// (single-coordinator) `FindCoordinator` response form throughout.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testDeleteConsumerGroupsWithOlderBroker")]
    async fn test_delete_consumer_groups_with_older_broker() {
        let (admin, mut runnable, time, nodes) = env_nodes_with_props(1, &[("retries", "2147483647")]);

        // Retriable FindCoordinator errors are retried, then a good coordinator.
        runnable
            .client_mut()
            .prepare_response(old_find_coordinator_error_resp(Errors::CoordinatorNotAvailable));
        runnable
            .client_mut()
            .prepare_response(old_find_coordinator_error_resp(Errors::CoordinatorLoadInProgress));
        runnable.client_mut().prepare_response(old_find_coordinator_resp(&nodes[0]));
        runnable
            .client_mut()
            .prepare_response(delete_groups_resp(&[("groupId", Errors::None)]));

        let result =
            admin.delete_consumer_groups_with_options(&["groupId".to_string()], DeleteConsumerGroupsOptions::new());
        let deleted = result.deleted_groups()["groupId"].clone();
        drive_until(&mut runnable, &time, 80, || deleted.is_done()).await;
        assert_eq!(deleted.get().await.unwrap(), ());

        // A non-retriable FindCoordinator error surfaces.
        runnable
            .client_mut()
            .prepare_response(old_find_coordinator_error_resp(Errors::GroupAuthorizationFailed));
        let error_result =
            admin.delete_consumer_groups_with_options(&["groupId".to_string()], DeleteConsumerGroupsOptions::new());
        let error_deleted = error_result.deleted_groups()["groupId"].clone();
        drive_until(&mut runnable, &time, 80, || error_deleted.is_done()).await;
        assert_eq!(error_deleted.get().await.unwrap_err().error(), Errors::GroupAuthorizationFailed);

        // Retriable DeleteGroups errors (load-in-progress, then coordinator moved)
        // are retried, with a re-lookup for the coordinator-moved errors.
        runnable.client_mut().prepare_response(old_find_coordinator_resp(&nodes[0]));
        runnable
            .client_mut()
            .prepare_response(delete_groups_resp(&[("groupId", Errors::CoordinatorLoadInProgress)]));
        runnable
            .client_mut()
            .prepare_response(delete_groups_resp(&[("groupId", Errors::NotCoordinator)]));
        runnable.client_mut().prepare_response(old_find_coordinator_resp(&nodes[0]));
        runnable
            .client_mut()
            .prepare_response(delete_groups_resp(&[("groupId", Errors::CoordinatorNotAvailable)]));
        runnable.client_mut().prepare_response(old_find_coordinator_resp(&nodes[0]));
        runnable
            .client_mut()
            .prepare_response(delete_groups_resp(&[("groupId", Errors::None)]));

        let retry_result =
            admin.delete_consumer_groups_with_options(&["groupId".to_string()], DeleteConsumerGroupsOptions::new());
        let retry_deleted = retry_result.deleted_groups()["groupId"].clone();
        drive_until(&mut runnable, &time, 120, || retry_deleted.is_done()).await;
        assert_eq!(retry_deleted.get().await.unwrap(), ());
    }

    /// Translated from `testRemoveMembersFromGroupNumRetries`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testRemoveMembersFromGroupNumRetries")]
    async fn test_remove_members_from_group_num_retries() {
        let default_api_timeout: i64 = 60000;
        let (admin, mut runnable, time, nodes) = env_nodes_with_props(
            3,
            &[
                ("default.api.timeout.ms", &default_api_timeout.to_string()),
                ("retries", "0"),
            ],
        );
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
        runnable
            .client_mut()
            .prepare_response(leave_group_top_level(Errors::NotCoordinator));
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));

        let result = admin.remove_members_from_consumer_group_with_options(
            GROUP_ID,
            members_to_remove(&["instance-1", "instance-2"]),
        );
        let all = result.all();
        pump_until(&mut runnable, 40, |r| !r.client_mut().has_pending_responses()).await;
        time.sleep(default_api_timeout + 1);
        drive_until(&mut runnable, &time, 40, || all.is_done()).await;
        assert!(matches!(all.get().await.unwrap_err(), Error::Timeout(_)));
    }

    /// Translated from `testRemoveMembersFromGroupRetriableErrors`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testRemoveMembersFromGroupRetriableErrors")]
    async fn test_remove_members_from_group_retriable_errors() {
        let (admin, mut runnable, time, nodes) = offsets_env(1);
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
        runnable
            .client_mut()
            .prepare_response(leave_group_top_level(Errors::CoordinatorLoadInProgress));
        runnable
            .client_mut()
            .prepare_response(leave_group_top_level(Errors::NotCoordinator));
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
        runnable
            .client_mut()
            .prepare_response(leave_group_top_level(Errors::CoordinatorNotAvailable));
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
        runnable
            .client_mut()
            .prepare_response(leave_group_members_resp(&[("instance-1", Errors::None)]));

        let member = MemberToRemove::new("instance-1");
        let result =
            admin.remove_members_from_consumer_group_with_options(GROUP_ID, members_to_remove(&["instance-1"]));
        let all = result.all();
        drive_until(&mut runnable, &time, 120, || all.is_done()).await;
        assert_eq!(all.get().await.unwrap(), ());
        assert_eq!(result.member_result(&member).unwrap().get().await.unwrap(), ());
    }

    /// Translated from `testRemoveMembersFromGroupNonRetriableErrors`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testRemoveMembersFromGroupNonRetriableErrors")]
    async fn test_remove_members_from_group_non_retriable_errors() {
        let (admin, mut runnable, time, nodes) = offsets_env(1);
        for error in [
            Errors::GroupAuthorizationFailed,
            Errors::InvalidGroupId,
            Errors::GroupIdNotFound,
        ] {
            runnable
                .client_mut()
                .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
            runnable.client_mut().prepare_response(leave_group_top_level(error));

            let member = MemberToRemove::new("instance-1");
            let result =
                admin.remove_members_from_consumer_group_with_options(GROUP_ID, members_to_remove(&["instance-1"]));
            let all = result.all();
            drive_until(&mut runnable, &time, 60, || all.is_done()).await;
            assert_eq!(all.get().await.unwrap_err().error(), error);
            assert_eq!(result.member_result(&member).unwrap().get().await.unwrap_err().error(), error);
        }
    }

    /// Translated from `testRemoveMembersFromGroup`: member-level error, then a
    /// missing member, then success, and finally the two `removeAll` scenarios.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testRemoveMembersFromGroup")]
    async fn test_remove_members_from_group() {
        let (admin, mut runnable, time, nodes) = offsets_env(1);
        let instance_one = "instance-1";
        let instance_two = "instance-2";
        let member_one = MemberToRemove::new(instance_one);
        let member_two = MemberToRemove::new(instance_two);

        // Inject one member-level error (instance-1 -> UNKNOWN_MEMBER_ID).
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
        runnable.client_mut().prepare_response(leave_group_members_resp(&[
            (instance_one, Errors::UnknownMemberId),
            (instance_two, Errors::None),
        ]));

        let member_level_error_result = admin.remove_members_from_consumer_group_with_options(
            GROUP_ID,
            members_to_remove(&[instance_one, instance_two]),
        );
        let all = member_level_error_result.all();
        drive_until(&mut runnable, &time, 60, || all.is_done()).await;
        assert_eq!(all.get().await.unwrap_err().error(), Errors::UnknownMemberId);
        assert_eq!(
            member_level_error_result
                .member_result(&member_one)
                .unwrap()
                .get()
                .await
                .unwrap_err()
                .error(),
            Errors::UnknownMemberId
        );
        assert_eq!(
            member_level_error_result
                .member_result(&member_two)
                .unwrap()
                .get()
                .await
                .unwrap(),
            ()
        );

        // Return with a missing member (instance-1 absent from the response).
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
        runnable
            .client_mut()
            .prepare_response(leave_group_members_resp(&[(instance_two, Errors::None)]));

        let missing_member_result = admin.remove_members_from_consumer_group_with_options(
            GROUP_ID,
            members_to_remove(&[instance_one, instance_two]),
        );
        let missing_all = missing_member_result.all();
        drive_until(&mut runnable, &time, 60, || missing_all.is_done()).await;
        assert!(matches!(missing_all.get().await.unwrap_err(), Error::LocalIllegalArgument(_)));
        assert!(matches!(
            missing_member_result
                .member_result(&member_one)
                .unwrap()
                .get()
                .await
                .unwrap_err(),
            Error::LocalIllegalArgument(_)
        ));
        assert_eq!(
            missing_member_result.member_result(&member_two).unwrap().get().await.unwrap(),
            ()
        );

        // Return with success for both members.
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
        runnable.client_mut().prepare_response(leave_group_members_resp(&[
            (instance_two, Errors::None),
            (instance_one, Errors::None),
        ]));
        let no_error_result = admin.remove_members_from_consumer_group_with_options(
            GROUP_ID,
            members_to_remove(&[instance_one, instance_two]),
        );
        let no_error_all = no_error_result.all();
        drive_until(&mut runnable, &time, 60, || no_error_all.is_done()).await;
        assert_eq!(no_error_all.get().await.unwrap(), ());
        assert_eq!(no_error_result.member_result(&member_one).unwrap().get().await.unwrap(), ());
        assert_eq!(no_error_result.member_result(&member_two).unwrap().get().await.unwrap(), ());

        // removeAll with a partial failure: describe the group, then remove all
        // members but one reports UNKNOWN_MEMBER_ID.
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
        runnable
            .client_mut()
            .prepare_response(consumer_group_describe_members_resp(GROUP_ID, &[instance_one, instance_two]));
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
        runnable.client_mut().prepare_response(leave_group_members_resp(&[
            (instance_one, Errors::UnknownMemberId),
            (instance_two, Errors::None),
        ]));
        let partial_failure_result = admin.remove_members_from_consumer_group_with_options(
            GROUP_ID,
            RemoveMembersFromConsumerGroupOptions::default(),
        );
        let partial_all = partial_failure_result.all();
        drive_until(&mut runnable, &time, 80, || partial_all.is_done()).await;
        assert_eq!(partial_all.get().await.unwrap_err().error(), Errors::UnknownMemberId);

        // removeAll with success.
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
        runnable
            .client_mut()
            .prepare_response(consumer_group_describe_members_resp(GROUP_ID, &[instance_one, instance_two]));
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
        runnable.client_mut().prepare_response(leave_group_members_resp(&[
            (instance_two, Errors::None),
            (instance_one, Errors::None),
        ]));
        let success_result = admin.remove_members_from_consumer_group_with_options(
            GROUP_ID,
            RemoveMembersFromConsumerGroupOptions::default(),
        );
        let success_all = success_result.all();
        drive_until(&mut runnable, &time, 80, || success_all.is_done()).await;
        assert_eq!(success_all.get().await.unwrap(), ());
    }

    /// Regression test for the `removeAll` describe step's timeout source.
    ///
    /// Java issues the describe via `describeConsumerGroups(Collections
    /// .singleton(groupId))` with a *default* `DescribeConsumerGroupsOptions`
    /// (its `timeoutMs` is `null`), so the describe driver's deadline is
    /// `now + defaultApiTimeoutMs` — INDEPENDENT of the removeMembers request's
    /// `options.timeout_ms()` (`KafkaAdminClient.java:4172`). This asserts the first
    /// describe request (the coordinator lookup) carries the default-API-timeout
    /// budget, not the small `options.timeout_ms()` budget, and so fails against
    /// code that ties the describe deadline to `options.timeout_ms()`.
    #[tokio::test]
    async fn test_remove_all_describe_uses_default_api_timeout() {
        use crate::common::protocol::ApiKeys;
        // request.timeout.ms (30000) > default.api.timeout.ms (20000) so the
        // describe budget is observable uncapped; options.timeout (5000) is
        // smaller still, so the buggy and fixed budgets are distinguishable.
        let (admin, mut runnable, _time, _nodes) = env_with_props(&[
            ("request.timeout.ms", "30000"),
            ("default.api.timeout.ms", "20000"),
            ("retries", "2147483647"),
            ("retry.backoff.ms", "10"),
        ]);
        // No responses prepared: the describe coordinator-lookup request is sent
        // but stays queued (unanswered), ready for inspection.
        let options = RemoveMembersFromConsumerGroupOptions::default().set_timeout_ms(Some(5000));
        assert!(options.remove_all());
        let _result = admin.remove_members_from_consumer_group_with_options(GROUP_ID, options);

        pump_until(&mut runnable, 40, |r| r.client_mut().request_count() >= 1).await;

        let requests = runnable.client_mut().requests();
        assert_eq!(requests.len(), 1, "only the describe coordinator lookup should be queued");
        let describe_lookup = &requests[0];
        assert_eq!(describe_lookup.api_key(), &ApiKeys::FIND_COORDINATOR);
        // now == 1000, describe deadline == now + default.api.timeout.ms == 21000,
        // budget == min(request.timeout.ms=30000, 21000-1000) == 20000. The buggy
        // code (describe deadline tied to options.timeout=5000) would instead
        // yield min(30000, 5000) == 5000.
        assert_eq!(
            describe_lookup.request_timeout_ms(),
            20000,
            "describe step must use default.api.timeout.ms (20000), not options.timeout (5000)"
        );
    }

    /// Regression test for the `removeAll` `LeaveGroup` deadline recomputation.
    ///
    /// Java computes the `LeaveGroup` driver's deadline INSIDE
    /// `memFuture.whenComplete(...)`, after the describe future resolves
    /// (`KafkaAdminClient.java:4224-4230` → `invokeDriver(..., options.timeoutMs())`
    /// → `calcDeadlineMs(time.milliseconds(), ...)`), so `LeaveGroup` gets a
    /// fresh full timeout window starting when describe completes. Here the mock
    /// clock is advanced past the ORIGINAL (call-time) `options.timeout` window
    /// before describe completes: the fix recomputes the LeaveGroup deadline from
    /// the post-describe time (so its coordinator lookup is issued with a fresh
    /// 5000ms budget), whereas code that baked the pre-describe deadline would
    /// have the LeaveGroup window already expired and issue no request at all.
    #[tokio::test]
    async fn test_remove_all_leave_group_deadline_computed_after_describe() {
        use crate::common::protocol::ApiKeys;
        // default.api.timeout.ms is large so the describe step survives the clock
        // advance (the describe-timeout fix is a prerequisite); options.timeout
        // (5000) is smaller than request.timeout.ms so the LeaveGroup budget is
        // observable uncapped.
        let (admin, mut runnable, time, nodes) = env_with_props(&[
            ("request.timeout.ms", "30000"),
            ("default.api.timeout.ms", "60000"),
            ("retries", "2147483647"),
            ("retry.backoff.ms", "10"),
        ]);
        // Answer the describe (coordinator lookup + ConsumerGroupDescribe). Do NOT
        // prepare the LeaveGroup coordinator lookup, so it stays queued for
        // inspection.
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
        runnable
            .client_mut()
            .prepare_response(consumer_group_describe_members_resp(GROUP_ID, &["instance-1"]));

        let options = RemoveMembersFromConsumerGroupOptions::default().set_timeout_ms(Some(5000));
        assert!(options.remove_all());
        let _result = admin.remove_members_from_consumer_group_with_options(GROUP_ID, options);

        // Advance the clock to 10000 — past the pre-describe LeaveGroup deadline
        // (call-time 1000 + options.timeout 5000 == 6000) — before describe is
        // driven. With the fix, the LeaveGroup deadline is recomputed from the
        // post-describe time (10000 + 5000 == 15000). Without it, the baked-in
        // window is already expired, describe/LeaveGroup time out, and no
        // LeaveGroup coordinator lookup is ever issued.
        time.sleep(9000);

        pump_until(&mut runnable, 40, |r| {
            r.client_mut()
                .requests()
                .iter()
                .any(|req| req.api_key() == &ApiKeys::FIND_COORDINATOR)
        })
        .await;

        let leave_lookup = runnable
            .client_mut()
            .requests()
            .iter()
            .find(|req| req.api_key() == &ApiKeys::FIND_COORDINATOR)
            .expect("the LeaveGroup coordinator lookup should be queued with a fresh deadline");
        // now == 10000, fresh LeaveGroup deadline == 10000 + options.timeout 5000
        // == 15000, budget == min(request.timeout.ms=30000, 15000-10000) == 5000.
        assert_eq!(
            leave_lookup.request_timeout_ms(),
            5000,
            "LeaveGroup deadline must be recomputed fresh after describe completes"
        );
    }

    /// Drives one `removeMembersFromConsumerGroup` with the given `reason`,
    /// asserting the emitted `LeaveGroup` request carried `expected_reason` on
    /// every member. Mirrors the private Java helper
    /// `testRemoveMembersFromGroup(reason, expectedReason)` — Java attaches a
    /// request-matcher predicate to the prepared response; our `MockClient` has
    /// no such matcher, so we inspect the emitted (queued, unanswered) request
    /// directly, which is the established request-inspection pattern.
    async fn assert_remove_members_reason(reason: Option<&str>, expected_reason: &str) {
        use crate::common::requests::AbstractRequest;
        let (admin, mut runnable, _time, nodes) = offsets_env(3);
        // Answer only FindCoordinator so the LeaveGroup request is sent but stays
        // queued (no prepared response), ready for inspection.
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));

        let mut options = members_to_remove(&["instance-1", "instance-2"]);
        if let Some(reason) = reason {
            options.set_reason(reason);
        }
        let _result = admin.remove_members_from_consumer_group_with_options(GROUP_ID, options);

        pump_until(&mut runnable, 40, |r| {
            r.client_mut()
                .requests_mut()
                .iter_mut()
                .any(|req| matches!(req.request_builder_mut().build(), Ok(AbstractRequest::LeaveGroup(_))))
        })
        .await;

        let leave_request = runnable
            .client_mut()
            .requests_mut()
            .iter_mut()
            .find_map(|req| match req.request_builder_mut().build() {
                Ok(AbstractRequest::LeaveGroup(r)) => Some(r),
                _ => None,
            })
            .expect("a LeaveGroup request should be queued");
        assert!(!leave_request.data().members.is_empty());
        for member in &leave_request.data().members {
            assert_eq!(member.reason.as_deref(), Some(expected_reason));
        }
    }

    /// Translated from `testRemoveMembersFromGroupReason`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testRemoveMembersFromGroupReason")]
    async fn test_remove_members_from_group_reason() {
        assert_remove_members_reason(Some("testing remove members reason"), "testing remove members reason").await;
    }

    /// Translated from `testRemoveMembersFromGroupTruncatesReason`: a reason
    /// longer than 255 chars is truncated to exactly 255 on the wire.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testRemoveMembersFromGroupTruncatesReason")]
    async fn test_remove_members_from_group_truncates_reason() {
        let reason = "Very looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong reason that is 271 characters long to make sure that length limit logic handles the scenario nicely";
        assert_eq!(reason.chars().count(), 271);
        let truncated: String = reason.chars().take(255).collect();
        assert_remove_members_reason(Some(reason), &truncated).await;
    }

    /// Translated from `testRemoveMembersFromGroupDefaultReason`: a null or empty
    /// reason falls back to the default reason.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.admin.KafkaAdminClientTest#testRemoveMembersFromGroupDefaultReason")]
    async fn test_remove_members_from_group_default_reason() {
        assert_remove_members_reason(None, DEFAULT_LEAVE_GROUP_REASON).await;
        assert_remove_members_reason(Some(""), DEFAULT_LEAVE_GROUP_REASON).await;
    }

    /// A [`MockClient`] whose `authentication_error` reports a failure, mirroring
    /// Java's `MockClient.authenticationException(node)` being non-null after
    /// `createPendingAuthenticationError`. `MockClient` (outside this module)
    /// always answers `None`, and the trait method is the only way the admin
    /// runnable learns about an authentication failure
    /// (`KafkaAdminClient.java:1373`).
    struct AuthFailingClient {
        inner: MockClient,
        error: Option<Error>,
    }

    impl AuthFailingClient {
        /// `error` is the object the channel would have raised, so the test can
        /// pick the *subclass* — which is the whole point of the regression it
        /// backs.
        fn new(inner: MockClient, error: Error) -> Self {
            Self { inner, error: Some(error) }
        }
    }

    impl KafkaClient for AuthFailingClient {
        fn authentication_error(&self, _node: &Node) -> Option<Error> {
            self.error.clone()
        }

        fn is_ready(&self, node: &Node, now: i64) -> bool {
            self.inner.is_ready(node, now)
        }

        fn ready(&mut self, node: &Node, now: i64) -> impl std::future::Future<Output = bool> + Send {
            self.inner.ready(node, now)
        }

        fn connection_delay(&self, node: &Node, now: i64) -> i64 {
            self.inner.connection_delay(node, now)
        }

        fn poll_delay_ms(&self, node: &Node, now: i64) -> i64 {
            self.inner.poll_delay_ms(node, now)
        }

        fn connection_failed(&self, node: &Node) -> bool {
            self.inner.connection_failed(node)
        }

        fn send(&mut self, request: crate::ClientRequest, now: i64) {
            self.inner.send(request, now)
        }

        fn poll(
            &mut self,
            timeout: i64,
            now: i64,
        ) -> impl std::future::Future<Output = Vec<crate::ClientResponse>> + Send {
            self.inner.poll(timeout, now)
        }

        fn disconnect(&mut self, node_id: &str) -> impl std::future::Future<Output = ()> + Send {
            self.inner.disconnect(node_id)
        }

        fn close_connection(&mut self, node_id: &str) -> impl std::future::Future<Output = ()> + Send {
            self.inner.close_connection(node_id)
        }

        fn least_loaded_node(&self, now: i64) -> crate::LeastLoadedNode {
            self.inner.least_loaded_node(now)
        }

        fn in_flight_request_count(&self) -> i32 {
            self.inner.in_flight_request_count()
        }

        fn has_in_flight_requests(&self) -> bool {
            self.inner.has_in_flight_requests()
        }

        fn in_flight_request_count_for_node(&self, node_id: &str) -> usize {
            self.inner.in_flight_request_count_for_node(node_id)
        }

        fn has_in_flight_requests_for_node(&self, node_id: &str) -> bool {
            self.inner.has_in_flight_requests_for_node(node_id)
        }

        fn has_ready_nodes(&self, now: i64) -> bool {
            self.inner.has_ready_nodes(now)
        }

        fn wakeup(&self) {
            self.inner.wakeup()
        }

        fn wakeup_handle(&self) -> Arc<Notify> {
            self.inner.wakeup_handle()
        }

        fn wakeup_notify(&self) -> Arc<Notify> {
            self.inner.wakeup_notify()
        }

        fn new_client_request(
            &mut self,
            node_id: &str,
            request_builder: Box<dyn crate::common::requests::RequestBuilder>,
            created_time_ms: i64,
            expect_response: bool,
        ) -> crate::ClientRequest {
            self.inner
                .new_client_request(node_id, request_builder, created_time_ms, expect_response)
        }

        fn new_client_request_with_timeout(
            &mut self,
            node_id: &str,
            request_builder: Box<dyn crate::common::requests::RequestBuilder>,
            created_time_ms: i64,
            expect_response: bool,
            request_timeout_ms: i32,
            callback: Option<crate::RequestCompletionHandler>,
        ) -> crate::ClientRequest {
            self.inner.new_client_request_with_timeout(
                node_id,
                request_builder,
                created_time_ms,
                expect_response,
                request_timeout_ms,
                callback,
            )
        }

        fn initiate_close(&self) {
            self.inner.initiate_close()
        }

        fn active(&self) -> bool {
            self.inner.active()
        }

        fn close(&mut self) -> impl std::future::Future<Output = ()> + Send {
            self.inner.close()
        }
    }

    /// Regression for finding 242. Java propagates the exception **object** —
    /// `AuthenticationException authException = client.authenticationException(call.curNode());
    /// if (authException != null) call.fail(now, authException);`
    /// (`KafkaAdminClient.java:1373-1376`) — so whichever `AuthenticationException`
    /// subclass the channel raised is what the admin future fails with. Rust
    /// hardcoded `Errors::SaslAuthenticationFailed`, reporting code 58 for a TLS
    /// certificate rejection (`KafkaChannel.java:463-467`'s
    /// `catch (SslAuthenticationException e)`), on a connection that never
    /// performed a SASL exchange.
    ///
    /// It also pins finding 231's other half: the reason must be the
    /// authenticator's own text, with no second class prefix baked into
    /// `message()`.
    #[tokio::test]
    async fn an_admin_authentication_failure_is_not_reported_as_sasl() {
        let time = mock_time(1000);
        let (cluster, nodes) = mock_cluster(3, 0);
        let inner = MockClient::with_static_nodes(nodes.clone(), Arc::clone(&time) as Arc<dyn Time>);
        let client = AuthFailingClient::new(
            inner,
            Error::SslAuthentication(crate::common::errors::SslAuthenticationError::new(
                "SSL handshake failed: certificate rejected",
            )),
        );
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        props.insert("retries".to_string(), "0".to_string());
        let config = AdminClientConfig::new(&props).unwrap();
        let (admin, mut runnable) =
            KafkaAdminClient::create_for_test(client, cluster, &config, Arc::clone(&time) as Arc<dyn Time>);

        runnable
            .client_mut()
            .inner
            .prepare_response_disconnected(metadata_resp(&nodes, Vec::new()), true);

        let result = admin.list_topics_with_options(ListTopicsOptions::new());
        let names = result.names();
        for _ in 0..40 {
            if names.is_done() {
                break;
            }
            runnable.run_once().await;
            time.sleep(200);
        }
        let error = names.get().await.expect_err("the authentication failure must fail the call");

        // `retries=0` means the call is out of retries, so Java's
        // `handleTimeoutFailure` wraps the cause; the authentication error is the
        // cause. Unwrap it if present, otherwise the error itself is it.
        let auth = error.source().unwrap_or(&error);
        assert!(auth.is_authentication_error(), "must classify as authentication: {auth:?}");
        assert_ne!(
            auth.error(),
            Errors::SaslAuthenticationFailed,
            "an SSL rejection must not be reported as SASL_AUTHENTICATION_FAILED: {auth:?}"
        );
        // The class itself, not just the code: Java hands the object through, so a
        // caller matching on the subclass — which is how it tells a certificate
        // rejection from a rejected credential — still fires.
        assert!(
            matches!(auth, Error::SslAuthentication(_)),
            "the subclass the channel raised must survive: {auth:?}"
        );
        assert_eq!(
            auth.message(),
            "SSL handshake failed: certificate rejected",
            "the authenticator's bare text: no second class prefix (finding 231)"
        );
        assert!(
            crate::common::requests::RequestUtils::is_fatal_error(auth),
            "RequestUtils.isFatalException answers true for an AuthenticationException: {auth:?}"
        );
    }

    /// Regression for finding 243(a). `handleFailure` on the `findAllBrokers`
    /// metadata call builds
    /// `new KafkaException("Failed to find brokers to send ListGroups", throwable)`
    /// (`KafkaAdminClient.java:3565`) — a **bare** `KafkaException`, which is a
    /// *sibling* of `ApiException`, not a subclass. So:
    ///
    ///   * `is_kafka_error()` is `true`, `is_api_error()` / `is_retriable_error()`
    ///     are `false`;
    ///   * the cause is reachable through `getCause()`;
    ///   * the message is fixed — it does NOT carry the cause's text.
    ///
    /// Reusing the inner error's code made the wrapper *inherit* the inner class,
    /// so a metadata timeout came back retriable and causeless.
    #[tokio::test]
    async fn list_groups_metadata_failure_is_a_bare_kafka_error() {
        // retries=0 so the disconnected metadata call fails terminally instead of
        // looping.
        let (admin, mut runnable, time, nodes) = env_with_props(&[("retries", "0")]);
        runnable
            .client_mut()
            .prepare_response_disconnected(metadata_resp(&nodes, Vec::new()), true);

        let result = admin.list_groups_with_options(ListGroupsOptions::new());
        let errors = result.errors();
        drive_until(&mut runnable, &time, 60, || errors.is_done()).await;

        let reported = errors.get().await.expect("the metadata failure is reported through errors()");
        assert_eq!(reported.len(), 1, "one wrapped error, got {reported:?}");
        let error = &reported[0];

        assert_eq!(
            error.message(),
            "Failed to find brokers to send ListGroups",
            "Java's message is fixed: no lower-cased call name, no appended cause text"
        );
        assert!(error.is_kafka_error(), "a bare KafkaException is a Kafka error: {error:?}");
        assert!(
            !error.is_api_error(),
            "a bare KafkaException is a SIBLING of ApiException, not a subclass: {error:?}"
        );
        assert!(
            !error.is_retriable_error(),
            "the wrapper must not inherit the inner TimeoutException's retriability: {error:?}"
        );
        assert_eq!(
            error.error(),
            Errors::UnknownServerError,
            "a client-built KafkaException has no wire code"
        );
        let cause = error.source().expect("Java passes the throwable as the cause");
        assert!(
            cause.is_timeout_error(),
            "expected the metadata timeout as the cause; got {cause:?}"
        );
    }

    /// Regression for finding 243(b). `getMembersFromGroup`'s `whenComplete`
    /// builds `new KafkaException("Encounter exception when trying to get members
    /// from group: " + groupId, ex)` (`KafkaAdminClient.java:4174`) — again a bare
    /// `KafkaException` carrying the cause, NOT the inner class. Inheriting the
    /// inner class turned a `GroupAuthorizationException` cause into an
    /// `is_authorization_error()` (and therefore `is_fatal_error()`) wrapper where
    /// Java answers `false` to both, and dropped the cause.
    ///
    /// ("exception" is reworded to "error" in the Rust message per CLAUDE.md §2.)
    #[tokio::test]
    async fn remove_all_describe_failure_is_a_bare_kafka_error() {
        let (admin, mut runnable, time, nodes) = offsets_env(1);
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));
        runnable.client_mut().prepare_response(consumer_group_describe_error_resp(
            GROUP_ID,
            Errors::GroupAuthorizationFailed,
            None,
        ));

        let result = admin.remove_members_from_consumer_group_with_options(
            GROUP_ID,
            RemoveMembersFromConsumerGroupOptions::default(),
        );
        let all = result.all();
        drive_until(&mut runnable, &time, 80, || all.is_done()).await;
        let error = all.get().await.expect_err("the describe step must fail the removeAll");

        assert_eq!(
            error.message(),
            format!("Encounter error when trying to get members from group: {GROUP_ID}")
        );
        assert!(error.is_kafka_error(), "got {error:?}");
        assert!(!error.is_api_error(), "a bare KafkaException is not an ApiException: {error:?}");
        assert!(
            !error.is_authorization_error(),
            "the wrapper must not inherit GroupAuthorizationException: {error:?}"
        );
        assert!(
            !crate::common::requests::RequestUtils::is_fatal_error(&error),
            "and therefore must not be fatal: {error:?}"
        );
        let cause = error.source().expect("Java passes `ex` as the cause");
        assert_eq!(cause.error(), Errors::GroupAuthorizationFailed, "got {cause:?}");
        assert!(cause.is_authorization_error(), "the cause keeps its own class: {cause:?}");
    }

    /// Regression for finding 246. `completeUnrealizedFutures` throws
    /// `new ApiException(messageFormatter.apply(key))`
    /// (`KafkaAdminClient.java:1744-1749`) — the concrete base class. It was
    /// spelled `Errors::UnknownServerError`, which resolves to the
    /// `UnknownServerException` *subclass*, so `Display` read
    /// `"UnknownServerError: .."` and `matches!(e, Error::Api(_))` never matched.
    ///
    /// Java uses `new UnknownServerException(..)` for its *other* response sanity
    /// checks (`:2631`, `:2684`, `:4020`), so this is not a blanket sweep.
    #[tokio::test]
    async fn an_unrealized_future_fails_with_a_bare_api_error() {
        let (admin, mut runnable, _time, _nodes) = env();
        let result = admin.create_topics_with_options(
            &[NewTopic::with_num_partitions_replication_factor(
                "myTopic",
                Some(1),
                Some(1),
            )],
            CreateTopicsOptions::new(),
        );
        // The broker answers with no per-topic result at all, so the requested
        // topic's future is left unrealized (`completeUnrealizedFutures`).
        runnable.client_mut().prepare_response(create_response(vec![]));
        for _ in 0..20 {
            if result.values()["myTopic"].is_done() {
                break;
            }
            runnable.run_once().await;
        }

        let error = result.values()["myTopic"]
            .get()
            .await
            .expect_err("an unrealized future must fail");
        assert!(
            matches!(error, Error::Api(_)),
            "Java throws the concrete `ApiException`, not a subclass: {error:?}"
        );
        assert_eq!(
            error.message(),
            "The controller response did not contain a result for topic myTopic"
        );
        assert_eq!(
            error.to_string(),
            "ApiError: The controller response did not contain a result for topic myTopic",
            "Display names the class Java threw"
        );
        assert!(error.is_api_error(), "got {error:?}");
        assert!(error.is_kafka_error(), "an ApiException is a KafkaException: {error:?}");
        assert!(
            !error.is_retriable_error(),
            "the concrete ApiException is not retriable: {error:?}"
        );
    }

    /// Regression for finding 247(a). Java rejects a call submitted once the
    /// client is closing with
    /// `new IllegalStateException("Cannot accept new calls when AdminClient is
    /// closing.")` (`KafkaAdminClient.java:1601`). The Rust text had drifted to
    /// "The AdminClient is closed.", which is not the sanctioned
    /// "exception"->"error" rewording.
    #[tokio::test]
    async fn a_call_submitted_while_closing_uses_javas_message() {
        let (admin, _runnable, _time, _nodes) = env();
        admin.close_with_timeout(Duration::from_millis(0)).await;

        let result = admin.list_topics_with_options(ListTopicsOptions::new());
        let error = result
            .names()
            .get()
            .await
            .expect_err("a call submitted while closing must fail");

        assert_eq!(error.message(), "Cannot accept new calls when AdminClient is closing.");
        // Java throws an `IllegalStateException`, which is outside the
        // `KafkaException` hierarchy entirely.
        assert!(matches!(error, Error::LocalIllegalState(_)), "got {error:?}");
        assert!(!error.is_kafka_error(), "got {error:?}");
    }

    /// Regression for finding 247. `handleNotControllerError` rethrows
    /// `error.exception()` for the code it matched
    /// (`KafkaAdminClient.java:4145-4153`), so a `NOT_LEADER_OR_FOLLOWER` seen by a
    /// `bootstrap.controllers` client surfaces as `NotLeaderOrFollower`. Rust
    /// always reported `NotController`.
    #[test]
    fn handle_not_controller_error_reports_the_matched_code() {
        let with_controllers = AdminMetadataManager::new(100, 1000, true, LogContext::empty());
        let without_controllers = AdminMetadataManager::new(100, 1000, false, LogContext::empty());

        let not_controller = HashMap::from([(Errors::NotController, 1)]);
        let not_leader = HashMap::from([(Errors::NotLeaderOrFollower, 1)]);

        assert_eq!(
            handle_not_controller_error(&without_controllers, &not_controller).map(|e| e.error()),
            Some(Errors::NotController)
        );
        assert_eq!(
            handle_not_controller_error(&with_controllers, &not_controller).map(|e| e.error()),
            Some(Errors::NotController)
        );
        assert_eq!(
            handle_not_controller_error(&with_controllers, &not_leader).map(|e| e.error()),
            Some(Errors::NotLeaderOrFollower),
            "Java rethrows the error built for the code it matched (`KafkaAdminClient.java:4152`)"
        );
        assert!(
            handle_not_controller_error(&without_controllers, &not_leader).is_none(),
            "the NOT_LEADER_OR_FOLLOWER arm is gated on usingBootstrapControllers"
        );
    }

    // --- shutdown ------------------------------------------------------------

    /// Java's `threadShouldExit` consults `hasActiveExternalCalls()`, which
    /// skips every `Call` with `internal == true`
    /// (`KafkaAdminClient.java:1419-1441`). The metadata refresh
    /// (`makeMetadataCall`) is internal and is recreated on every backoff
    /// expiry, so counting it would keep the I/O task alive for as long as the
    /// bootstrap brokers stay unreachable: `close(timeout)` would block for the
    /// whole timeout, and Java's no-argument `Admin.close()` — which passes
    /// `Duration.ofMillis(Long.MAX_VALUE)` — would never return at all.
    #[tokio::test]
    async fn close_exits_the_io_task_while_only_the_internal_metadata_call_is_active() {
        let (admin, mut runnable, time, _nodes) = env();
        // Force the refresh the production client performs on its own once the
        // seeded metadata expires (or fails), then let phase 4 create the
        // internal call. The mock has no prepared response, so the call stays
        // active indefinitely — exactly the unreachable-broker situation.
        admin.shared.metadata_manager.request_update();
        time.sleep(1_000);
        pump(&mut runnable, 2).await;
        assert!(
            runnable.has_active_calls_for_test(),
            "the internal metadata refresh call should be active"
        );
        assert!(
            !runnable.has_active_external_calls_for_test(),
            "the metadata refresh call is internal, so it is not an active external call"
        );

        // Java's no-argument `Admin.close()`: no reachable hard deadline.
        admin.shared.shutdown.closing.store(true, Ordering::Release);
        admin
            .shared
            .shutdown
            .hard_shutdown_deadline_ms
            .store(i64::MAX, Ordering::Release);

        assert!(
            runnable.should_exit_for_test(time.milliseconds()),
            "close() must not wait on an internal call: the I/O task has to exit at once"
        );
    }

    /// The other half of the contract: an **external** call does hold the loop
    /// open until the hard-shutdown deadline, so `close(timeout)` still gives a
    /// user-submitted RPC its chance to finish.
    #[tokio::test]
    async fn close_waits_for_an_active_external_call_until_the_hard_deadline() {
        let (admin, mut runnable, time, _nodes) = env();
        let _result = admin.list_topics_with_options(ListTopicsOptions::new());
        pump(&mut runnable, 1).await;
        assert!(
            runnable.has_active_external_calls_for_test(),
            "the submitted listTopics call should be an active external call"
        );

        let now = time.milliseconds();
        admin.shared.shutdown.closing.store(true, Ordering::Release);
        admin
            .shared
            .shutdown
            .hard_shutdown_deadline_ms
            .store(now + 30_000, Ordering::Release);
        assert!(
            !runnable.should_exit_for_test(now),
            "an active external call keeps the I/O task alive until the hard deadline"
        );

        // Once the hard deadline passes, the task exits and aborts the call.
        assert!(runnable.should_exit_for_test(now + 30_000));
    }

    /// A [`KafkaClient`] wrapper whose `poll` records the timeout it was handed
    /// and, once armed, advances the mock clock by that timeout — i.e. it
    /// behaves like a real `poll` that finds nothing on the socket and waits the
    /// whole budget it was given. That makes the wait the I/O loop *would* have
    /// performed observable on the mock clock (`MockClient::poll` ignores its
    /// timeout, so no assertion on real elapsed time is possible).
    ///
    /// Recording the argument mirrors Mockito's
    /// `verify(client).poll(captor.capture(), anyLong())`; the consumer tests
    /// use the same wrapper shape (`CountingClient` in
    /// `consumer/internals/consumer_network_thread.rs`).
    struct WaitingClient {
        inner: MockClient,
        time: Arc<MockTime>,
        poll_timeouts: Arc<Mutex<Vec<i64>>>,
        advance_clock: Arc<std::sync::atomic::AtomicBool>,
        stuck: Arc<std::sync::atomic::AtomicBool>,
        park_once: Arc<std::sync::atomic::AtomicBool>,
        parked: Arc<Notify>,
    }

    impl WaitingClient {
        fn new(inner: MockClient, time: Arc<MockTime>) -> Self {
            Self {
                inner,
                time,
                poll_timeouts: Arc::new(Mutex::new(Vec::new())),
                advance_clock: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                stuck: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                park_once: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                parked: Arc::new(Notify::new()),
            }
        }

        /// Once armed, the next `poll` parks until the client's
        /// [`wakeup_notify`](KafkaClient::wakeup_notify) handle fires, then
        /// behaves normally. That handle is the same `Notify` production
        /// `submit()` and `close()` poke (it is what `KafkaAdminClient::build`
        /// stores as `Shared::wakeup`), so the park ends exactly when a real
        /// selector poll would be woken (DoD #12). Only the first poll after
        /// arming parks, so the loop can finish its work afterwards.
        fn park_once(&self) -> Arc<std::sync::atomic::AtomicBool> {
            Arc::clone(&self.park_once)
        }

        /// Signalled (with a stored permit) when `poll` has entered the park
        /// armed by [`park_once`](Self::park_once).
        fn parked(&self) -> Arc<Notify> {
            Arc::clone(&self.parked)
        }

        fn poll_timeouts(&self) -> Arc<Mutex<Vec<i64>>> {
            Arc::clone(&self.poll_timeouts)
        }

        fn advance_clock(&self) -> Arc<std::sync::atomic::AtomicBool> {
            Arc::clone(&self.advance_clock)
        }

        /// Once armed, `poll` never returns, so the I/O loop can never reach
        /// `should_exit` again. It stands in for any `await` inside a
        /// `process_pending_calls` phase that no shutdown deadline can interrupt — in
        /// production the unbounded `socket.connect(...).await` that
        /// `send_eligible_calls` reaches through `client.ready(...)`.
        fn stuck(&self) -> Arc<std::sync::atomic::AtomicBool> {
            Arc::clone(&self.stuck)
        }
    }

    impl KafkaClient for WaitingClient {
        fn is_ready(&self, node: &Node, now: i64) -> bool {
            self.inner.is_ready(node, now)
        }
        async fn ready(&mut self, node: &Node, now: i64) -> bool {
            self.inner.ready(node, now).await
        }
        fn connection_delay(&self, node: &Node, now: i64) -> i64 {
            self.inner.connection_delay(node, now)
        }
        fn poll_delay_ms(&self, node: &Node, now: i64) -> i64 {
            self.inner.poll_delay_ms(node, now)
        }
        fn connection_failed(&self, node: &Node) -> bool {
            self.inner.connection_failed(node)
        }
        fn authentication_error(&self, node: &Node) -> Option<Error> {
            self.inner.authentication_error(node)
        }
        fn send(&mut self, request: crate::ClientRequest, now: i64) {
            self.inner.send(request, now)
        }
        async fn poll(&mut self, timeout: i64, now: i64) -> Vec<crate::ClientResponse> {
            self.poll_timeouts.lock().unwrap().push(timeout);
            if self.stuck.load(Ordering::Acquire) {
                std::future::pending::<()>().await;
            }
            if self.park_once.swap(false, Ordering::AcqRel) {
                let wakeup = self.inner.wakeup_notify();
                // Created before `parked` is signalled, so a wakeup issued the
                // moment the test resumes is not lost: `notify_one` either wakes
                // this waiter or leaves a permit it consumes on first poll.
                let woken = wakeup.notified();
                self.parked.notify_one();
                woken.await;
            }
            let now = if self.advance_clock.load(Ordering::Acquire) {
                self.time.sleep(timeout);
                self.time.milliseconds()
            } else {
                now
            };
            self.inner.poll(timeout, now).await
        }
        async fn disconnect(&mut self, node_id: &str) {
            self.inner.disconnect(node_id).await
        }
        async fn close_connection(&mut self, node_id: &str) {
            self.inner.close_connection(node_id).await
        }
        fn least_loaded_node(&self, now: i64) -> crate::LeastLoadedNode {
            self.inner.least_loaded_node(now)
        }
        fn in_flight_request_count(&self) -> i32 {
            self.inner.in_flight_request_count()
        }
        fn has_in_flight_requests(&self) -> bool {
            self.inner.has_in_flight_requests()
        }
        fn in_flight_request_count_for_node(&self, node_id: &str) -> usize {
            self.inner.in_flight_request_count_for_node(node_id)
        }
        fn has_in_flight_requests_for_node(&self, node_id: &str) -> bool {
            self.inner.has_in_flight_requests_for_node(node_id)
        }
        fn has_ready_nodes(&self, now: i64) -> bool {
            self.inner.has_ready_nodes(now)
        }
        fn wakeup(&self) {
            self.inner.wakeup()
        }
        fn wakeup_handle(&self) -> Arc<Notify> {
            self.inner.wakeup_handle()
        }
        fn wakeup_notify(&self) -> Arc<Notify> {
            self.inner.wakeup_notify()
        }
        fn new_client_request(
            &mut self,
            node_id: &str,
            request_builder: Box<dyn RequestBuilder>,
            created_time_ms: i64,
            expect_response: bool,
        ) -> crate::ClientRequest {
            self.inner
                .new_client_request(node_id, request_builder, created_time_ms, expect_response)
        }
        fn new_client_request_with_timeout(
            &mut self,
            node_id: &str,
            request_builder: Box<dyn RequestBuilder>,
            created_time_ms: i64,
            expect_response: bool,
            request_timeout_ms: i32,
            callback: Option<crate::RequestCompletionHandler>,
        ) -> crate::ClientRequest {
            self.inner.new_client_request_with_timeout(
                node_id,
                request_builder,
                created_time_ms,
                expect_response,
                request_timeout_ms,
                callback,
            )
        }
        fn initiate_close(&self) {
            self.inner.initiate_close()
        }
        fn active(&self) -> bool {
            self.inner.active()
        }
        async fn close(&mut self) {
            self.inner.close().await
        }
    }

    /// Java bounds every `client.poll(...)` by the time left until the
    /// hard-shutdown deadline once `close()` has been called
    /// (`KafkaAdminClient.java:1512-1515`):
    ///
    /// ```java
    /// long pollTimeout = Math.min(1200000, timeoutProcessor.nextTimeoutMs());
    /// if (curHardShutdownTimeMs != INVALID_SHUTDOWN_TIME) {
    ///     pollTimeout = Math.min(pollTimeout, curHardShutdownTimeMs - now);
    /// }
    /// ```
    ///
    /// Without that clamp an in-flight **external** call keeps `should_exit`
    /// false (which is correct — see the test above), while the poll itself
    /// waits on the far larger call deadline
    /// (`default.api.timeout.ms`) or, in production, on `NetworkClient`'s own
    /// `request.timeout.ms` cap. `close(100ms)` would then block for tens of
    /// seconds, and because the FFI `close` is a `block_on`, C and Python
    /// callers would see the same overrun.
    #[tokio::test]
    async fn close_bounds_the_poll_timeout_by_the_hard_shutdown_deadline() {
        let time = mock_time(1000);
        let (cluster, nodes) = mock_cluster(3, 0);
        let client = WaitingClient::new(
            MockClient::with_static_nodes(nodes.clone(), Arc::clone(&time) as Arc<dyn Time>),
            Arc::clone(&time),
        );
        let poll_timeouts = client.poll_timeouts();
        let advance_clock = client.advance_clock();
        let config = test_config();
        let (admin, mut runnable) =
            KafkaAdminClient::create_for_test(client, cluster, &config, Arc::clone(&time) as Arc<dyn Time>);

        // Put an external RPC in flight. The mock has no prepared response, so
        // the call sits in `correlation_id_to_calls`: `pending_calls` is empty,
        // hence no `retry_backoff_ms` floor, and the only contributors left to
        // the poll timeout are the call deadline (`default.api.timeout.ms`) and
        // `metadata.max.age.ms`.
        let _result = admin.list_topics_with_options(ListTopicsOptions::new());
        for _ in 0..40 {
            if runnable.client_mut().inner.request_count() >= 1 {
                break;
            }
            runnable.run_once().await;
        }
        assert!(
            runnable.client_mut().inner.request_count() >= 1,
            "the listTopics request should have been sent"
        );
        assert!(
            runnable.has_active_external_calls_for_test(),
            "the in-flight listTopics call should be an active external call"
        );

        // `close(Duration::from_millis(100))`.
        let now = time.milliseconds();
        let hard_deadline = now + 100;
        admin.shared.shutdown.closing.store(true, Ordering::Release);
        admin
            .shared
            .shutdown
            .hard_shutdown_deadline_ms
            .store(hard_deadline, Ordering::Release);

        // From here on the client waits out every timeout it is given, like a
        // real one polling an idle socket.
        poll_timeouts.lock().unwrap().clear();
        advance_clock.store(true, Ordering::Release);

        runnable.run().await;

        let timeouts = poll_timeouts.lock().unwrap().clone();
        assert!(
            !timeouts.is_empty(),
            "the run loop should have polled at least once after close()"
        );
        for timeout in &timeouts {
            assert!(
                *timeout <= 100,
                "every poll after close() must be clamped to the remaining shutdown budget \
                 (100 ms), got {timeouts:?}"
            );
        }
        let waited = time.milliseconds() - now;
        assert!(
            waited <= 100,
            "close(100ms) must not overrun its deadline: the I/O task waited {waited}ms"
        );
    }

    /// The deadline clamped in the test above is only a *hint* to the I/O loop.
    /// The caller's guarantee is Java's **timed** join at the end of
    /// `KafkaAdminClient.close(Duration)`:
    ///
    /// ```java
    /// if (Thread.currentThread() != thread) {
    ///     thread.join(waitTimeMs);   // returns after waitTimeMs regardless
    /// }
    /// ```
    ///
    /// It matters more in Rust than in Java: Java's `sendEligibleCalls` calls the
    /// non-blocking NIO `client.ready(...)` and cannot block, whereas ours awaits
    /// `NetworkClient::ready` → `initiate_connect` → `Selector::connect`, which
    /// awaits the TCP handshake with no timeout of its own. An unbounded
    /// `handle.await` would then hand a `close(50ms)` caller — including the C
    /// and Python layers, which `block_on` it — a wait bounded only by the OS
    /// connect timeout.
    #[tokio::test]
    async fn close_returns_within_its_timeout_even_when_the_io_task_cannot_exit() {
        let time = mock_time(1000);
        let (cluster, nodes) = mock_cluster(3, 0);
        let client = WaitingClient::new(
            MockClient::with_static_nodes(nodes.clone(), Arc::clone(&time) as Arc<dyn Time>),
            Arc::clone(&time),
        );
        let stuck = client.stuck();
        let config = test_config();
        let (admin, runnable) =
            KafkaAdminClient::create_for_test(client, cluster, &config, Arc::clone(&time) as Arc<dyn Time>);

        // From its first poll on, the I/O task is parked forever: it can neither
        // finish work nor re-evaluate `should_exit`, so nothing but the timed
        // join can end the wait.
        stuck.store(true, Ordering::Release);
        admin.spawn(runnable);
        // An active external call, so `should_exit` could not short-circuit on
        // "all work has been completed" even if the task did run again.
        let _result = admin.list_topics_with_options(ListTopicsOptions::new());

        let started = std::time::Instant::now();
        let returned =
            tokio::time::timeout(Duration::from_secs(5), admin.close_with_timeout(Duration::from_millis(50))).await;
        assert!(
            returned.is_ok(),
            "close(50ms) must return even though the I/O task can never exit; it was still \
             blocked after 5s"
        );
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(1),
            "close(50ms) must be bounded by its timeout, but it returned only after {elapsed:?}"
        );
    }

    /// A call submitted just before `close()` must still run. Java's
    /// `processRequests` drains `newCalls` and checks `threadShouldExit` back to
    /// back at the top of every iteration (`KafkaAdminClient.java:1498-1504`):
    ///
    /// ```java
    /// // Copy newCalls into pendingCalls.
    /// drainNewCalls();
    ///
    /// // Check if the AdminClient thread should shut down.
    /// long curHardShutdownTimeMs = hardShutdownTimeMs.get();
    /// if ((curHardShutdownTimeMs != INVALID_SHUTDOWN_TIME) && threadShouldExit(now, curHardShutdownTimeMs))
    ///     break;
    /// ```
    ///
    /// so a call that arrived while the thread was parked in `client.poll` is in
    /// `pendingCalls` — an active external call — when the exit decision is
    /// made, and `close(timeout)` waits for it. When the exit check instead ran
    /// after the whole iteration (network poll included), the call was still in
    /// the submission channel, invisible to `has_active_external_calls`, and
    /// `fail_all_remaining` failed it with "The AdminClient thread has exited."
    ///
    /// The loop is the real spawned `run()`, parked in its network poll on
    /// `client.wakeup_notify()` — the `Notify` production `submit()` and
    /// `close()` poke — so it wakes exactly as a production selector would.
    #[tokio::test]
    async fn a_call_submitted_just_before_close_is_completed() {
        let time = mock_time(1000);
        let (cluster, nodes) = mock_cluster(3, 0);
        let client = WaitingClient::new(
            MockClient::with_static_nodes(nodes.clone(), Arc::clone(&time) as Arc<dyn Time>),
            Arc::clone(&time),
        );
        let park_once = client.park_once();
        let parked = client.parked();
        let config = test_config();
        let (admin, mut runnable) =
            KafkaAdminClient::create_for_test(client, cluster, &config, Arc::clone(&time) as Arc<dyn Time>);
        runnable.client_mut().inner.prepare_response(create_response(vec![create_result(
            "myTopic",
            Errors::None,
            None,
        )]));

        // Park the I/O task in its first network poll, before anything is
        // submitted.
        park_once.store(true, Ordering::Release);
        admin.spawn(runnable);
        tokio::time::timeout(Duration::from_secs(5), parked.notified())
            .await
            .expect("the I/O task should park in its network poll");

        // Submitted while the task is parked: the call sits in the submission
        // channel until the task next drains it.
        let result = admin.create_topics_with_options(
            &[NewTopic::with_num_partitions_replication_factor(
                "myTopic",
                Some(1),
                Some(1),
            )],
            CreateTopicsOptions::new(),
        );
        tokio::time::timeout(Duration::from_secs(10), admin.close_with_timeout(Duration::from_secs(30)))
            .await
            .expect("close(30s) should return once the create has completed");

        let value = result.values()["myTopic"].get().await;
        assert!(
            value.is_ok(),
            "a createTopics submitted before close() must complete, as in Java; got {value:?}"
        );
        result
            .all()
            .get()
            .await
            .expect("all() of a completed createTopics must succeed");
    }

    /// Steps `runnable` until exactly one `InitProducerId` request is in flight
    /// and returns the number queued, so a `fenceProducers` test can act while
    /// the driver holds the next key's fulfillment back.
    async fn pump_until_one_init_producer_id_in_flight(runnable: &mut AdminClientRunnable<MockClient>) -> usize {
        let count = |runnable: &mut AdminClientRunnable<MockClient>| {
            runnable
                .client_mut()
                .requests()
                .iter()
                .filter(|r| *r.api_key() == crate::common::protocol::ApiKeys::INIT_PRODUCER_ID)
                .count()
        };
        for _ in 0..40 {
            if count(runnable) >= 1 {
                break;
            }
            runnable.run_once().await;
        }
        // A few more iterations: the driver must NOT issue the second key's
        // request while the first is outstanding (`AdminApiDriver.java:381-383`).
        for _ in 0..5 {
            runnable.run_once().await;
        }
        count(runnable)
    }

    /// Every key of a driver RPC resolves on `close()`, even
    /// one whose fulfillment request had not been issued yet.
    ///
    /// The driver issues at most one fulfillment request per broker at a time
    /// (`AdminApiDriver.java:381-383`), so with one broker `b`'s `InitProducerId`
    /// is only created once `a`'s completes or fails. `close(0)` makes the I/O
    /// task exit with `a`'s call in flight; the `finally` fails it, and its
    /// failure hook asks the driver for the next request. Java routes that
    /// through `runnable.call(..)` (`KafkaAdminClient.java:5110`), which rejects
    /// it once the shutdown deadline is set (`:1599-1601`), so `b` fails with
    /// "Cannot accept new calls when AdminClient is closing." Rust used to send
    /// it straight into the submission channel after the final drain; the
    /// runnable was then dropped with it inside and `b` never resolved.
    #[tokio::test]
    async fn close_resolves_a_driver_key_whose_fulfillment_was_not_yet_issued() {
        let (admin, mut runnable, _time, nodes) = env_nodes_with_props(1, &[]);
        let coordinator = &nodes[0];
        // One batched lookup maps both transactional ids to the only broker.
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[("a", coordinator), ("b", coordinator)]));

        let result =
            admin.fence_producers_with_options(&["a".to_string(), "b".to_string()], FenceProducersOptions::new());
        assert_eq!(
            pump_until_one_init_producer_id_in_flight(&mut runnable).await,
            1,
            "exactly one key's InitProducerId must be in flight; the other's is held back"
        );

        // `close(Duration.ZERO)`: no task was spawned, so this only publishes the
        // closing gate and a hard deadline of `now`. The loop then exits at once
        // with the in-flight call still outstanding.
        admin.close_with_timeout(Duration::ZERO).await;
        tokio::time::timeout(Duration::from_secs(10), runnable.run())
            .await
            .expect("run() must exit once the hard deadline has passed");
        // Dropping the runnable drops anything still queued in its channel, as
        // the end of the I/O task does in production.
        drop(runnable);

        let a = result.producer_id("a").expect("a is a requested key");
        let b = result.producer_id("b").expect("b is a requested key");
        assert!(
            a.is_done() && b.is_done(),
            "every key must resolve on close(), including the one whose fulfillment was not yet \
             issued (a: {}, b: {})",
            a.is_done(),
            b.is_done()
        );

        // Which key the driver issued first follows its map order, so identify
        // the two outcomes by their errors rather than by name.
        let mut messages = vec![
            a.get().await.expect_err("close(0) aborts the fence").to_string(),
            b.get().await.expect_err("close(0) aborts the fence").to_string(),
        ];
        messages.sort();
        assert_eq!(
            messages,
            vec![
                "LocalIllegalStateError: Cannot accept new calls when AdminClient is closing.".to_string(),
                "TimeoutError: The AdminClient thread has exited. Call: fenceProducer(api=INIT_PRODUCER_ID)"
                    .to_string(),
            ],
            "the in-flight key times out with the exiting task; the unissued one is rejected by the \
             closing gate with Java's IllegalStateException text"
        );
    }

    /// A driver RPC issued after `close()` fails with Java's
    /// `IllegalStateException("Cannot accept new calls when AdminClient is
    /// closing.")` (`KafkaAdminClient.java:1599-1601`, reached through
    /// `invokeDriver` → `maybeSendRequests` → `runnable.call`), exactly like a
    /// plain call. It used to bypass the gate: with the I/O task gone the
    /// channel send failed and the key got a retriable
    /// `TimeoutException("The AdminClient thread has exited.")` instead.
    #[tokio::test]
    async fn a_driver_rpc_issued_after_close_fails_with_the_closing_error() {
        let (admin, mut runnable, _time, _nodes) = env();
        admin.close_with_timeout(Duration::ZERO).await;
        tokio::time::timeout(Duration::from_secs(10), runnable.run())
            .await
            .expect("with no work the I/O task exits as soon as close() is called");
        drop(runnable);

        let result = admin.fence_producers_with_options(&["a".to_string()], FenceProducersOptions::new());
        let future = result.producer_id("a").expect("a is a requested key");
        assert!(future.is_done(), "a call rejected by the closing gate resolves at once");
        let error = future.get().await.expect_err("a call issued after close() must fail");
        assert!(matches!(error, Error::LocalIllegalState(_)), "got {error:?}");
        assert_eq!(error.message(), "Cannot accept new calls when AdminClient is closing.");
        assert!(
            !error.is_retriable_error(),
            "Java's IllegalStateException is not retriable, unlike the TimeoutException it replaced: {error:?}"
        );
    }

    /// A follow-up call issued during the I/O task's
    /// shutdown tail resolves instead of hanging, even when the loop ended
    /// without `close()` having set the closing gate.
    ///
    /// Java's `finally` starts with `closing = true` (`KafkaAdminClient.java:1474`),
    /// so `enqueue` rejects anything submitted from then on with
    /// `TimeoutException("The AdminClient thread has exited.")` (`:1576-1586`).
    /// Here the loop ends in a panic, so only that flag can stop the follow-up
    /// the driver issues when the `finally` fails the in-flight key: without it
    /// the call was queued after the final drain and dropped with the runnable.
    #[tokio::test]
    async fn a_follow_up_issued_in_the_shutdown_tail_after_a_panic_resolves() {
        let (admin, mut runnable, time, nodes) = env_nodes_with_props(1, &[]);
        let coordinator = &nodes[0];
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[("a", coordinator), ("b", coordinator)]));
        let result =
            admin.fence_producers_with_options(&["a".to_string(), "b".to_string()], FenceProducersOptions::new());
        assert_eq!(pump_until_one_init_producer_id_in_flight(&mut runnable).await, 1);

        // End the loop with a panic (see `a_panicking_io_task_still_runs_its_finally`):
        // an already-expired call whose failure hook panics once.
        let now = time.milliseconds();
        let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hook_fired = Arc::clone(&fired);
        admin.submit(Call::new(
            "panicOnPurpose",
            now - 1,
            NodeProvider::LeastLoaded,
            Box::new(|_timeout_ms| unreachable!("the call expires before it is ever sent")),
            Box::new(|_response, _now, _cur_node| HandleResult::Done),
            Box::new(move |_error| {
                if !hook_fired.swap(true, Ordering::AcqRel) {
                    panic!("injected panic inside the I/O loop");
                }
            }),
            Box::new(|| false),
        ));
        tokio::time::timeout(Duration::from_secs(10), runnable.run())
            .await
            .expect("run() must reach its finally after a panic");
        assert!(fired.load(Ordering::Acquire), "the injected panic must actually have fired");
        assert_eq!(
            admin.shared.shutdown.hard_shutdown_deadline_ms.load(Ordering::Acquire),
            KafkaAdminClient::NO_HARD_SHUTDOWN,
            "precondition: close() was never called, so the closing gate is not set"
        );
        drop(runnable);

        let a = result.producer_id("a").expect("a is a requested key");
        let b = result.producer_id("b").expect("b is a requested key");
        assert!(a.is_done() && b.is_done(), "every key must resolve after the task exits");
        // The two keys reach the text by different paths, and the exact messages
        // tell them apart: the in-flight key is failed by `fail_all_remaining`
        // (with the call rendered after it), while the follow-up is rejected by
        // the closed channel (bare, as Java's `enqueue` builds it). A follow-up
        // that was queued and failed by a later drain would carry the suffix.
        let mut messages = Vec::new();
        for future in [a, b] {
            let error = future.get().await.expect_err("the task exited before the fence finished");
            assert!(error.is_timeout_error(), "got {error:?}");
            messages.push(error.message().to_string());
        }
        messages.sort();
        assert_eq!(
            messages,
            vec![
                "The AdminClient thread has exited.".to_string(),
                "The AdminClient thread has exited. Call: fenceProducer(api=INIT_PRODUCER_ID)".to_string(),
            ]
        );
    }

    /// Regression for COMMENTS.66.md Issue 2: a quota-retry follow-up passes the
    /// same `runnable.call` gate as every other new call.
    ///
    /// Java resubmits the throttled topics with `runnable.call(call, now)`
    /// (`KafkaAdminClient.java:1880`), which rejects the retry once `close()` has
    /// set the hard-shutdown deadline (`:1599-1601`). The retry's
    /// `handleFailure` leaves a non-timeout cause alone
    /// (`maybeCompleteQuotaExceededException`), so the topic fails with
    /// `IllegalStateException("Cannot accept new calls when AdminClient is
    /// closing.")`. Rust used to push the retry straight into `pending_calls`,
    /// where it kept the loop alive and was sent again during `close()`.
    #[tokio::test]
    async fn a_quota_retry_during_close_is_rejected_by_the_closing_gate() {
        let (admin, mut runnable, _time, _nodes) = env();
        let result = admin.create_topics_with_options(
            &[NewTopic::with_num_partitions_replication_factor(
                "topic1",
                Some(1),
                Some(1),
            )],
            CreateTopicsOptions::new().set_retry_on_quota_violation(true),
        );
        pump_until_request_queued(&mut runnable).await;

        // `close(30s)` with the createTopics request in flight. No task was
        // spawned, so this only publishes the deadline and the closing flag.
        admin.close_with_timeout(Duration::from_secs(30)).await;
        // The controller answers with a quota violation, which asks for a retry.
        runnable.client_mut().respond(create_response_throttled(
            1000,
            vec![create_result("topic1", Errors::ThrottlingQuotaExceeded, None)],
        ));
        runnable.run_once().await;

        let future = &result.values()["topic1"];
        assert!(
            future.is_done(),
            "the quota retry must be rejected at once, not queued to be sent again during close()"
        );
        let error = future.get().await.expect_err("the rejected retry fails the topic");
        assert!(matches!(error, Error::LocalIllegalState(_)), "got {error:?}");
        assert_eq!(error.message(), "Cannot accept new calls when AdminClient is closing.");
        assert!(
            !runnable.has_active_external_calls_for_test(),
            "the rejected retry must not stay queued"
        );
    }

    /// Java publishes the hard-shutdown deadline through a compare-and-set loop
    /// that only ever moves it *earlier* (`KafkaAdminClient.close`: "Hard
    /// shutdown time is already earlier than requested"). A plain store would let
    /// a later, more relaxed `close()` re-widen the poll budget that `process_pending_calls`
    /// reads on every iteration — stretching the wait of a caller already parked
    /// in the join above.
    #[tokio::test]
    async fn close_never_widens_an_existing_hard_shutdown_deadline() {
        let (admin, _runnable, time, _nodes) = env();
        let now = time.milliseconds();
        let deadline = || admin.shared.shutdown.hard_shutdown_deadline_ms.load(Ordering::Acquire);

        // No task was spawned, so each `close()` here only publishes the deadline.
        admin.close_with_timeout(Duration::from_millis(100)).await;
        assert_eq!(deadline(), now + 100, "the first close() installs its own deadline");

        admin.close_with_timeout(Duration::from_secs(60)).await;
        assert_eq!(
            deadline(),
            now + 100,
            "a later, more relaxed close() must keep the earlier deadline"
        );

        admin.close_with_timeout(Duration::from_millis(10)).await;
        assert_eq!(deadline(), now + 10, "a more urgent close() does move the deadline earlier");
    }

    /// Java caps the wait at a year ("Limit the timeout to a year"), which also
    /// keeps the deadline it derives finite — the no-argument `Admin.close()`
    /// passes `Duration.ofMillis(Long.MAX_VALUE)`.
    #[tokio::test]
    async fn close_clamps_the_wait_to_a_year() {
        let (admin, _runnable, time, _nodes) = env();
        let now = time.milliseconds();
        admin.close_with_timeout(Duration::from_millis(i64::MAX as u64)).await;
        assert_eq!(
            admin.shared.shutdown.hard_shutdown_deadline_ms.load(Ordering::Acquire),
            now + MAX_CLOSE_WAIT_TIME_MS
        );
    }
}
