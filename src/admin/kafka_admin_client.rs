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
//! # `describeTopics` deviation
//!
//! Java 4.2 describes topics **by name** via the KIP-966
//! `DescribeTopicPartitions` API (with cursor pagination), falling back to the
//! Metadata API (`generateDescribeTopicsCallWithMetadataApi`) on
//! `UnsupportedVersionException`. This port uses the **Metadata-API path
//! directly** for `describe_topics` by name, avoiding the
//! `DescribeTopicPartitions` cursor-pagination machinery and its
//! `describeCluster` prerequisite. This is a documented, intentional Phase-1
//! deviation: the observable per-topic result (description or
//! `UnknownTopicOrPartitionError`) is identical for the common case, and the
//! `DescribeTopicPartitions` wire type is deferred to a later tier.
//!
//! Describing topics **by id** (`handleDescribeTopicsByIds`) already uses the
//! Metadata API in Java (`convertTopicIdsToMetadataRequestTopic`), not
//! `DescribeTopicPartitions`, so it is translated faithfully here with no
//! deferral.
//!
//! `bootstrap.controllers` (KIP-919) is unsupported in Phase 1, so the metadata
//! refresh always uses the broker `Metadata` API (never `DescribeCluster`), and
//! controller/least-loaded node selection never needs the
//! `LeastLoadedBrokerOrActiveKController` provider.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::ApiVersions;
use crate::DefaultHostResolver;
use crate::alter_replica_log_dirs_request_data::{
    AlterReplicaLogDir, AlterReplicaLogDirTopic, AlterReplicaLogDirsRequestData,
};
use crate::alter_user_scram_credentials_request_data::{
    AlterUserScramCredentialsRequestData, ScramCredentialDeletion, ScramCredentialUpsertion,
};
use crate::client_utils;
use crate::common::acl::{AclBinding, AclBindingFilter, AclOperation};
use crate::common::config::{ConfigResource, ConfigResourceType};
use crate::common::kafka_future::KafkaFutureImpl;
use crate::common::network::Selector;
use crate::common::network::channel_builders;
use crate::common::protocol::Errors;
use crate::common::quota::{ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter};
use crate::common::requests::metadata_response::{AUTHORIZED_OPERATIONS_OMITTED, NO_CONTROLLER_ID};
use crate::common::requests::{
    AlterClientQuotasRequestBuilder, AlterReplicaLogDirsRequestBuilder, AlterUserScramCredentialsRequestBuilder,
    ConcreteResponse, CreateAclsRequest, CreateAclsRequestBuilder, CreateDelegationTokenRequestBuilder,
    CreatePartitionsRequestBuilder, CreateTopicsRequestBuilder, DeleteAclsRequest, DeleteAclsRequestBuilder,
    DeleteAclsResponse, DeleteTopicsRequestBuilder, DescribeAclsRequestBuilder, DescribeAclsResponse,
    DescribeClientQuotasRequestBuilder, DescribeClusterRequestBuilder, DescribeConfigsRequestBuilder,
    DescribeDelegationTokenRequestBuilder, DescribeLogDirsRequestBuilder, DescribeLogDirsResponse,
    DescribeUserScramCredentialsRequestBuilder, ENDPOINT_TYPE_BROKER, ENDPOINT_TYPE_CONTROLLER,
    ExpireDelegationTokenRequestBuilder, IncrementalAlterConfigsRequestBuilder, ListConfigResourcesRequestBuilder,
    ListGroupsRequestBuilder, MetadataRequestBuilder, RenewDelegationTokenRequestBuilder, RequestBuilder,
};
use crate::common::security::SecurityProtocol;
use crate::common::security::auth::KafkaPrincipal;
use crate::common::security::scram::internals::{ScramFormatter, ScramMechanism as InternalScramMechanism};
use crate::common::security::token::delegation::{DelegationToken, TokenInformation};
use crate::common::utils::{ExponentialBackoff, LogContext};
use crate::common::{
    Cluster, GroupState, GroupType, KafkaError, KafkaFuture, TopicCollection, TopicPartition, TopicPartitionInfo, Uuid,
};
use crate::consumer::OffsetAndMetadata;
use crate::consumer::internals::consumer_protocol::PROTOCOL_TYPE;
use crate::create_acls_request_data::{AclCreation, CreateAclsRequestData};
use crate::create_partitions_request_data::{
    CreatePartitionsAssignment, CreatePartitionsRequestData, CreatePartitionsTopic,
};
use crate::create_topics_request_data::{CreatableTopic, CreateTopicsRequestData};
use crate::delete_acls_request_data::{DeleteAclsFilter, DeleteAclsRequestData};
use crate::delete_topics_request_data::{DeleteTopicState, DeleteTopicsRequestData};
use crate::describe_cluster_request_data::DescribeClusterRequestData;
use crate::describe_configs_request_data::{DescribeConfigsRequestData, DescribeConfigsResource};
use crate::describe_log_dirs_request_data::{DescribableLogDirTopic, DescribeLogDirsRequestData};
use crate::describe_user_scram_credentials_request_data::{DescribeUserScramCredentialsRequestData, UserName};
use crate::describe_user_scram_credentials_response_data::DescribeUserScramCredentialsResponseData;
use crate::incremental_alter_configs_request_data::{
    AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequestData,
};
use crate::kafka_client::KafkaClient;
use crate::list_config_resources_request_data::ListConfigResourcesRequestData;
use crate::list_groups_request_data::ListGroupsRequestData;
use crate::metadata_recovery_strategy::MetadataRecoveryStrategy;
use crate::network_client::NetworkClient;

use super::internals::abort_transaction_handler::AbortTransactionHandler;
use super::internals::admin_api_driver::{AdminApiDriver, RequestSpec};
use super::internals::admin_api_future::AdminApiFuture;
use super::internals::admin_client_runnable::{AdminClientRunnable, NO_HARD_SHUTDOWN, ShutdownSignal};
use super::internals::admin_metadata_manager::AdminMetadataManager;
use super::internals::admin_utils::valid_acl_operations;
use super::internals::alter_consumer_group_offsets_handler::AlterConsumerGroupOffsetsHandler;
use super::internals::call::{Call, HandleResult, MaybeRetryOutcome, NodeProvider};
use super::internals::coordinator_key::CoordinatorKey;
use super::internals::delete_consumer_group_offsets_handler::DeleteConsumerGroupOffsetsHandler;
use super::internals::delete_consumer_groups_handler::DeleteConsumerGroupsHandler;
use super::internals::delete_groups_handler::DeleteGroupsHandler;
use super::internals::delete_records_handler::DeleteRecordsHandler;
use super::internals::describe_classic_groups_handler::DescribeClassicGroupsHandler;
use super::internals::describe_consumer_groups_handler::DescribeConsumerGroupsHandler;
use super::internals::describe_producers_handler::DescribeProducersHandler;
use super::internals::describe_transactions_handler::DescribeTransactionsHandler;
use super::internals::fence_producers_handler::FenceProducersHandler;
use super::internals::list_consumer_group_offsets_handler::ListConsumerGroupOffsetsHandler;
use super::internals::list_offsets_handler::ListOffsetsHandler;
use super::internals::list_transactions_handler::ListTransactionsHandler;
use super::internals::partition_leader_cache::PartitionLeaderCache;
use super::internals::remove_members_from_consumer_group_handler::RemoveMembersFromConsumerGroupHandler;
use super::records_to_delete::RecordsToDelete;
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
    AlterReplicaLogDirsResult, Config, ConfigEntry, ConfigSource, ConfigSynonym, ConfigType, CreateAclsOptions,
    CreateAclsResult, CreateDelegationTokenOptions, CreateDelegationTokenResult, CreatePartitionsOptions,
    CreatePartitionsResult, CreateTopicsOptions, CreateTopicsResult, DeleteAclsOptions, DeleteAclsResult,
    DeleteConsumerGroupOffsetsOptions, DeleteConsumerGroupOffsetsResult, DeleteConsumerGroupsOptions,
    DeleteConsumerGroupsResult, DeleteRecordsOptions, DeleteRecordsResult, DeleteTopicsOptions, DeleteTopicsResult,
    DescribeAclsOptions, DescribeAclsResult, DescribeClassicGroupsOptions, DescribeClassicGroupsResult,
    DescribeClientQuotasOptions, DescribeClientQuotasResult, DescribeClusterOptions, DescribeClusterResult,
    DescribeConfigsOptions, DescribeConfigsResult, DescribeConsumerGroupsOptions, DescribeConsumerGroupsResult,
    DescribeDelegationTokenOptions, DescribeDelegationTokenResult, DescribeLogDirsOptions, DescribeLogDirsResult,
    DescribeReplicaLogDirsOptions, DescribeReplicaLogDirsResult, DescribeTopicsOptions, DescribeTopicsResult,
    ElectLeadersOptions, ElectLeadersResult, ExpireDelegationTokenOptions, ExpireDelegationTokenResult, FilterResult,
    FilterResults, GroupListing, ListConfigResourcesOptions, ListConfigResourcesResult,
    ListConsumerGroupOffsetsOptions, ListConsumerGroupOffsetsResult, ListConsumerGroupOffsetsSpec, ListGroupsOptions,
    ListGroupsResult, ListOffsetsOptions, ListOffsetsResult, ListPartitionReassignmentsOptions,
    ListPartitionReassignmentsResult, ListTopicsOptions, ListTopicsResult, LogDirDescription, NewPartitionReassignment,
    NewPartitions, NewTopic, OffsetSpec, PartitionReassignment, RemoveMembersFromConsumerGroupOptions,
    RemoveMembersFromConsumerGroupResult, RenewDelegationTokenOptions, RenewDelegationTokenResult, ReplicaInfo,
    ReplicaLogDirInfo, TopicDescription, TopicListing, TopicMetadataAndConfig,
};
use super::{
    AlterUserScramCredentialsOptions, AlterUserScramCredentialsResult, DescribeUserScramCredentialsOptions,
    DescribeUserScramCredentialsResult, ScramMechanism, UserScramCredentialAlteration, UserScramCredentialDeletion,
    UserScramCredentialUpsertion,
};
#[allow(deprecated)]
use super::{
    ClientMetricsResourceListing, ConsumerGroupListing, ListClientMetricsResourcesOptions,
    ListClientMetricsResourcesResult, ListConsumerGroupsOptions, ListConsumerGroupsResult,
};
use super::{
    DescribeFeaturesOptions, DescribeFeaturesResult, FeatureMetadata, FeatureUpdate, FinalizedVersionRange,
    SupportedVersionRange, UpdateFeaturesOptions, UpdateFeaturesResult,
};
use crate::alter_partition_reassignments_request_data::{
    AlterPartitionReassignmentsRequestData, ReassignablePartition, ReassignableTopic,
};
use crate::api_versions_response_data::ApiVersionsResponseData;
use crate::common::TopicPartitionReplica;
use crate::common::requests::{
    AlterPartitionReassignmentsRequestBuilder, ApiVersionsRequestBuilder, ElectLeadersRequestBuilder,
    ElectLeadersResponse, ListPartitionReassignmentsRequestBuilder, UpdateFeaturesRequestBuilder,
    maybe_truncate_reason,
};
use crate::common::{ElectionType, Node};
use crate::create_delegation_token_request_data::{CreatableRenewers, CreateDelegationTokenRequestData};
use crate::expire_delegation_token_request_data::ExpireDelegationTokenRequestData;
use crate::leave_group_request_data::MemberIdentity;
use crate::list_partition_reassignments_request_data::{
    ListPartitionReassignmentsRequestData, ListPartitionReassignmentsTopics,
};
use crate::renew_delegation_token_request_data::RenewDelegationTokenRequestData;
use crate::update_features_request_data::{FeatureUpdateKey, UpdateFeaturesRequestData};
use std::collections::{BTreeSet, HashSet};

/// The default reason sent in a `LeaveGroup` request when an admin removes a
/// member without providing one. Mirrors
/// `KafkaAdminClient.DEFAULT_LEAVE_GROUP_REASON`.
const DEFAULT_LEAVE_GROUP_REASON: &str = "member was removed by an admin";

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
    #[allow(dead_code)]
    client_id: String,
    default_api_timeout_ms: i32,
    /// The `request.timeout.ms` config, used as the default transaction timeout
    /// for `fenceProducers` (mirrors `KafkaAdminClient.requestTimeoutMs`).
    request_timeout_ms: i32,
    admin_tx: mpsc::UnboundedSender<Call>,
    wakeup: Arc<Notify>,
    shutdown: Arc<ShutdownSignal>,
    metadata_manager: AdminMetadataManager,
    time_provider: Arc<dyn Fn() -> i64 + Send + Sync>,
    bg_handle: Mutex<Option<JoinHandle<()>>>,
    /// Retry-backoff parameters for the `AdminApiDriver` (mirrors the fields
    /// passed to the driver in `invokeDriver`).
    retry_backoff_ms: i64,
    retry_backoff_max_ms: i64,
    /// Cache of partition-to-leader mappings shared across driver-backed calls
    /// (`deleteRecords`), mirroring `KafkaAdminClient.partitionLeaderCache`.
    partition_leader_cache: Arc<PartitionLeaderCache>,
}

/// The administrative client for Kafka.
///
/// Corresponds to `org.apache.kafka.clients.admin.KafkaAdminClient`.
pub struct KafkaAdminClient {
    shared: Arc<Shared>,
}

impl KafkaAdminClient {
    /// Creates a network-backed admin client from configuration, spawning the
    /// background I/O task.
    ///
    /// Mirrors `KafkaAdminClient.createInternal`. Phase 1 supports the PLAINTEXT
    /// security protocol only.
    ///
    /// # Errors
    ///
    /// Returns an error if the bootstrap addresses cannot be resolved or the
    /// channel builder cannot be created.
    pub fn from_config(config: AdminClientConfig) -> Result<Self, KafkaError> {
        let log_context = LogContext::new(format!("[AdminClient clientId={}] ", config.client_id()));

        let bootstrap: Vec<String> = config.bootstrap_servers().to_vec();
        let addresses = client_utils::parse_and_validate_addresses(&bootstrap)?;

        let time_provider: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64
        });

        let metadata_manager = AdminMetadataManager::new(
            config.retry_backoff_ms(),
            config.metadata_max_age_ms(),
            false, // bootstrap.controllers unsupported in Phase 1
            log_context.clone(),
        );
        // Seed with the bootstrap cluster so the first metadata refresh has
        // nodes to talk to (mirrors Java's constructor `metadataManager.update`).
        let now = (time_provider)();
        metadata_manager.update(Cluster::bootstrap(&addresses), now);

        let channel_builder = channel_builders::client_channel_builder(
            SecurityProtocol::Plaintext,
            None,
            None,
            None,
            config.client_id(),
            log_context.clone(),
        )
        .map_err(|e| KafkaError::illegal_argument(format!("Failed to create channel builder: {e}")))?;
        let selector = Selector::with_defaults_and_log_context(
            config.connections_max_idle_ms(),
            channel_builder,
            log_context.clone(),
        );
        let api_versions = Arc::new(ApiVersions::new());

        let client = NetworkClient::with_metadata_updater(
            selector,
            metadata_manager.updater(),
            config.client_id(),
            100, // max in-flight requests per connection (admin sends <= 1 per node)
            config.reconnect_backoff_ms(),
            config.reconnect_backoff_max_ms(),
            crate::common::network::selectable::USE_DEFAULT_BUFFER_SIZE,
            crate::common::network::selectable::USE_DEFAULT_BUFFER_SIZE,
            config.request_timeout_ms(),
            config.socket_connection_setup_timeout_ms(),
            config.socket_connection_setup_timeout_ms(),
            true, // discover_broker_versions
            api_versions,
            DefaultHostResolver::new(),
            MetadataRecoveryStrategy::None,
            log_context.clone(),
        );

        let (admin, runnable) = Self::build(client, metadata_manager, &config, time_provider, log_context);
        admin.spawn(runnable);
        Ok(admin)
    }

    /// Wires up the shared state and the (not-yet-running) background runnable.
    fn build<C: KafkaClient + Send + 'static>(
        client: C,
        metadata_manager: AdminMetadataManager,
        config: &AdminClientConfig,
        time_provider: Arc<dyn Fn() -> i64 + Send + Sync>,
        log_context: LogContext,
    ) -> (Self, AdminClientRunnable<C>) {
        let (admin_tx, admin_rx) = mpsc::unbounded_channel();
        let wakeup = client.wakeup_notify();
        let shutdown = Arc::new(ShutdownSignal::new());
        let retry_backoff = ExponentialBackoff::new(
            config.retry_backoff_ms(),
            RETRY_BACKOFF_EXP_BASE,
            config.retry_backoff_max_ms(),
            RETRY_BACKOFF_JITTER,
        )
        .expect("ExponentialBackoff::new only fails on invalid jitter");

        let runnable = AdminClientRunnable::new(
            client,
            metadata_manager.clone(),
            admin_rx,
            retry_backoff,
            config.retry_backoff_ms(),
            config.retries(),
            config.request_timeout_ms(),
            Arc::clone(&time_provider),
            Arc::clone(&shutdown),
            log_context,
        );

        let shared = Shared {
            client_id: config.client_id().to_string(),
            default_api_timeout_ms: config.default_api_timeout_ms(),
            request_timeout_ms: config.request_timeout_ms(),
            admin_tx,
            wakeup,
            shutdown,
            metadata_manager,
            time_provider,
            bg_handle: Mutex::new(None),
            retry_backoff_ms: config.retry_backoff_ms(),
            retry_backoff_max_ms: config.retry_backoff_max_ms(),
            partition_leader_cache: Arc::new(PartitionLeaderCache::new()),
        };
        (Self { shared: Arc::new(shared) }, runnable)
    }

    /// Spawns the background task.
    fn spawn<C: KafkaClient + Send + 'static>(&self, mut runnable: AdminClientRunnable<C>) {
        let handle = tokio::task::spawn(async move {
            runnable.run().await;
        });
        *self.shared.bg_handle.lock().unwrap() = Some(handle);
    }

    /// Submits a call to the background task, failing it immediately if the
    /// client is closed. Mirrors `AdminClientRunnable.call` / `enqueue`.
    fn submit(&self, call: Call) {
        if self.shared.shutdown.closing.load(std::sync::atomic::Ordering::Acquire) {
            let mut call = call;
            call.handle_failure(&KafkaError::illegal_state("The AdminClient is closed."));
            return;
        }
        // Mirrors KafkaAdminClient.call: reject calls whose endpoint is
        // incompatible with a `bootstrap.controllers` client (KIP-919).
        if self.shared.metadata_manager.using_bootstrap_controllers() && !call.node_provider.supports_use_controllers()
        {
            let mut call = call;
            call.handle_failure(&KafkaError::unsupported_version(
                "This Admin API is not supported when communicating directly with the controller quorum.",
            ));
            return;
        }
        match self.shared.admin_tx.send(call) {
            Ok(()) => self.shared.wakeup.notify_one(),
            Err(mpsc::error::SendError(mut call)) => {
                call.handle_failure(&KafkaError::illegal_state("The AdminClient thread has exited."));
            },
        }
    }

    fn now(&self) -> i64 {
        (self.shared.time_provider)()
    }

    /// Builds the context used to submit `AdminApiDriver`-generated calls
    /// (mirrors the closure over `runnable` in `KafkaAdminClient.maybeSendRequests`).
    fn driver_context(&self) -> DriverContext {
        DriverContext {
            tx: self.shared.admin_tx.clone(),
            wakeup: Arc::clone(&self.shared.wakeup),
            time_provider: Arc::clone(&self.shared.time_provider),
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

    /// Drives a `listGroups` / `listConsumerGroups` broker-enumeration RPC: a
    /// `findAllBrokers` metadata call whose response fans out one per-broker
    /// `ListGroups` call, all feeding a shared [`ListGroupsResults`] accumulator.
    ///
    /// `maybe_add` maps a wire `ListedGroup` to an optional keyed listing
    /// (returning `None` filters the group out). Mirrors the shared structure of
    /// `KafkaAdminClient.listGroups` / `listConsumerGroups`.
    fn submit_list_groups<L, F>(
        &self,
        call_name: &'static str,
        deadline: i64,
        states_filter: Vec<String>,
        types_filter: Vec<String>,
        maybe_add: F,
    ) -> KafkaFuture<Vec<Result<L, KafkaError>>>
    where
        L: Clone + Send + Sync + 'static,
        F: Fn(&crate::list_groups_response_data::ListedGroup) -> Option<(String, L)> + Clone + Send + Sync + 'static,
    {
        let all: KafkaFutureImpl<Vec<Result<L, KafkaError>>> = KafkaFutureImpl::new();
        let public = all.future();
        let ctx = self.driver_context();

        let fail_all = all.clone();
        let handle_failure = Box::new(move |error: &KafkaError| {
            // Mirrors Java: wrap in a KafkaException("Failed to find brokers ...").
            let wrapped = KafkaError::with_message(
                error.error(),
                format!("Failed to find brokers to send {call_name}: {}", error.message()),
            );
            fail_all.complete(vec![Err(wrapped)]);
        });

        let create_request = Box::new(move |_timeout_ms: i32| {
            // Empty topic list (just the broker list), matching Java's
            // MetadataRequest with setTopics(emptyList).setAllowAutoTopicCreation(true).
            Ok(Box::new(MetadataRequestBuilder::new(Some(&[]), true)) as Box<dyn RequestBuilder>)
        });

        let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
            let ConcreteResponse::Metadata(metadata_response) = response else {
                return HandleResult::Retry(KafkaError::illegal_state("Expected a Metadata response"));
            };
            let nodes: Vec<Node> = metadata_response.brokers().to_vec();
            if nodes.is_empty() {
                // Java throws StaleMetadataException (retriable) so the metadata
                // fetch is retried; there is no dedicated StaleMetadata error code
                // in Rust, so we surface a retriable metadata error to trigger the
                // same retry.
                return HandleResult::Retry(KafkaError::with_message(
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
                    Ok(Box::new(ListGroupsRequestBuilder::new(data)) as Box<dyn RequestBuilder>)
                });

                let resp_results = Arc::clone(&results);
                let resp_node = node.clone();
                let resp_add = maybe_add.clone();
                let handle_list_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
                    let ConcreteResponse::ListGroups(list_response) = response else {
                        return HandleResult::Retry(KafkaError::illegal_state("Expected a ListGroups response"));
                    };
                    let error = Errors::for_code(list_response.data().error_code);
                    if error == Errors::CoordinatorLoadInProgress || error == Errors::CoordinatorNotAvailable {
                        // Retriable at the broker level: retry this per-broker call.
                        return HandleResult::Retry(KafkaError::new(error));
                    }
                    let mut results = resp_results.lock().unwrap();
                    if error != Errors::None {
                        results.add_error(&KafkaError::new(error), &resp_node);
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
                let handle_list_failure = Box::new(move |error: &KafkaError| {
                    let mut results = fail_results.lock().unwrap();
                    results.add_error(error, &fail_node);
                    results.complete_node(node_id);
                });

                let list_call = Call::new(
                    call_name,
                    deadline,
                    NodeProvider::ConstantNodeId(node_id),
                    create_list_request,
                    handle_list_response,
                    handle_list_failure,
                    Box::new(|| false),
                );
                match ctx.tx.send(list_call) {
                    Ok(()) => ctx.wakeup.notify_one(),
                    Err(mpsc::error::SendError(mut call)) => {
                        call.handle_failure(&KafkaError::illegal_state("The AdminClient task has exited."));
                    },
                }
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
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
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
            Ok(Box::new(IncrementalAlterConfigsRequestBuilder::from_data(request_data.clone()))
                as Box<dyn RequestBuilder>)
        });

        let handles = Arc::new(handles);
        let resp_mm = self.shared.metadata_manager.clone();
        let resp_handles = Arc::clone(&handles);
        let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
            let ConcreteResponse::IncrementalAlterConfigs(alter_response) = response else {
                return HandleResult::Retry(KafkaError::illegal_state("Expected an IncrementalAlterConfigs response"));
            };
            if let Some(err) = handle_not_controller_error(&resp_mm, &alter_response.error_counts()) {
                return HandleResult::Retry(err);
            }
            let errors = alter_response.errors_by_resource();
            for (resource, future) in resp_handles.iter() {
                match errors.get(resource) {
                    Some((code, message)) if *code != Errors::None.code() => {
                        future.complete_exceptionally(api_error(*code, message));
                    },
                    _ => {
                        future.complete(());
                    },
                }
            }
            HandleResult::Done
        });

        let fail_handles = Arc::clone(&handles);
        let handle_failure = Box::new(move |error: &KafkaError| {
            for future in fail_handles.values() {
                future.complete_exceptionally(error.clone());
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
    time_provider: Arc<dyn Fn() -> i64 + Send + Sync>,
}

/// Kicks off a driver-backed RPC: polls the driver for its initial requests and
/// submits them. Mirrors `KafkaAdminClient.invokeDriver` + the initial
/// `maybeSendRequests`.
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
fn maybe_send_requests<K, V>(driver: &Arc<Mutex<AdminApiDriver<K, V>>>, ctx: &DriverContext, _now: i64)
where
    K: Clone + Eq + std::hash::Hash + std::fmt::Display + Send + 'static,
    V: Send + 'static,
{
    let specs = driver.lock().unwrap().poll();
    for spec in specs {
        let call = new_driver_call(Arc::clone(driver), spec, ctx.clone());
        match ctx.tx.send(call) {
            Ok(()) => ctx.wakeup.notify_one(),
            Err(mpsc::error::SendError(mut call)) => {
                call.handle_failure(&KafkaError::illegal_state("The AdminClient thread has exited."));
            },
        }
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
    // A minimal node used only for the handler's `broker.id()` in log/sanity
    // messages; the real endpoint is resolved by the node provider on send.
    let node = Node::new(scope.destination_broker_id().unwrap_or(-1), String::new(), -1);

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
            .ok_or_else(|| KafkaError::illegal_state("AdminApiDriver produced no request on retry")),
    });

    let hr_driver = Arc::clone(&driver);
    let hr_ctx = ctx.clone();
    let hr_scope = scope.clone();
    let hr_keys = keys.clone();
    let hr_node = node.clone();
    let handle_response = Box::new(move |response: &ConcreteResponse, now: i64| {
        hr_driver
            .lock()
            .unwrap()
            .on_response(now, &hr_scope, &hr_keys, response, &hr_node);
        maybe_send_requests(&hr_driver, &hr_ctx, now);
        HandleResult::Done
    });

    let hf_driver = Arc::clone(&driver);
    let hf_ctx = ctx.clone();
    let hf_scope = scope.clone();
    let hf_keys = keys.clone();
    let hf_time = Arc::clone(&ctx.time_provider);
    let handle_failure = Box::new(move |error: &KafkaError| {
        let now = (hf_time)();
        hf_driver.lock().unwrap().on_failure(now, &hf_scope, &hf_keys, error);
        maybe_send_requests(&hf_driver, &hf_ctx, now);
    });

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

    // maybeRetry override: a disconnect retries lookup via the driver rather
    // than re-sending to the (possibly dead) node. Mirrors `newCall.maybeRetry`.
    let mr_driver = Arc::clone(&driver);
    let mr_ctx = ctx.clone();
    let mr_scope = scope;
    let mr_keys = keys;
    call.set_maybe_retry_fn(Box::new(move |error: &KafkaError, now: i64| {
        if error.error() == Errors::NetworkException {
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
/// `KafkaAdminClient.calcDeadlineMs`.
fn calc_deadline_ms(now: i64, option_timeout: Option<i32>, default_api_timeout_ms: i32) -> i64 {
    now + option_timeout.unwrap_or(default_api_timeout_ms) as i64
}

/// Re-keys a `CoordinatorKey`-keyed future map by the coordinator key's id
/// value (the group id), mirroring Java's
/// `future.all().entrySet().stream().collect(toMap(e -> e.getKey().idValue, ...))`.
fn coordinator_keyed_by_id<V: Send + 'static>(
    map: HashMap<CoordinatorKey, KafkaFuture<V>>,
) -> HashMap<String, KafkaFuture<V>> {
    map.into_iter().map(|(key, future)| (key.id_value, future)).collect()
}

/// Accumulates the per-broker results of a `listGroups` / `listConsumerGroups`
/// broker-enumeration RPC, completing the combined future once every broker has
/// reported. Mirrors `KafkaAdminClient.ListGroupsResults` /
/// `ListConsumerGroupsResults` (generic over the listing type `L`).
struct ListGroupsResults<L: Clone + Send + Sync + 'static> {
    errors: Vec<KafkaError>,
    listings: HashMap<String, L>,
    remaining: HashSet<i32>,
    future: KafkaFutureImpl<Vec<Result<L, KafkaError>>>,
}

impl<L: Clone + Send + Sync + 'static> ListGroupsResults<L> {
    /// Creates the accumulator for the given broker node ids, completing the
    /// future immediately if there are no brokers.
    fn new(node_ids: HashSet<i32>, future: KafkaFutureImpl<Vec<Result<L, KafkaError>>>) -> Arc<Mutex<Self>> {
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
    fn add_error(&mut self, error: &KafkaError, node: &Node) {
        let message = error.message();
        let wrapped = if message.is_empty() {
            KafkaError::with_message(error.error(), format!("Error listing groups on {node}"))
        } else {
            KafkaError::with_message(error.error(), format!("Error listing groups on {node}: {message}"))
        };
        self.errors.push(wrapped);
    }

    /// Records a listing keyed by group id.
    fn add_listing(&mut self, group_id: String, listing: L) {
        self.listings.insert(group_id, listing);
    }

    /// Marks a broker done and completes the future if it was the last one.
    fn complete_node(&mut self, node_id: i32) {
        self.remaining.remove(&node_id);
        self.try_complete();
    }

    fn try_complete(&mut self) {
        if self.remaining.is_empty() {
            let mut results: Vec<Result<L, KafkaError>> = self.listings.values().cloned().map(Ok).collect();
            results.extend(self.errors.iter().cloned().map(Err));
            self.future.complete(results);
        }
    }
}

/// Builds a `KafkaError` from a wire error code and optional message, mirroring
/// Java's `ApiError.exception()`.
fn api_error(code: i16, message: &Option<String>) -> KafkaError {
    let error = Errors::for_code(code);
    match message {
        Some(m) if !m.is_empty() => KafkaError::with_message(error, m.clone()),
        _ => KafkaError::new(error),
    }
}

/// Builds a [`FeatureMetadata`] from an `ApiVersionsResponse`'s data, mirroring
/// the `createFeatureMetadata` closure inside `KafkaAdminClient.describeFeatures`.
///
/// # Errors
///
/// Returns an error if a finalized/supported version range from the response is
/// invalid (mirrors Java's constructor throwing `IllegalArgumentException`).
fn create_feature_metadata(data: &ApiVersionsResponseData) -> Result<FeatureMetadata, KafkaError> {
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
fn topic_name_is_unrepresentable(topic_name: &str) -> bool {
    topic_name.is_empty()
}

/// Returns `true` if a topic id cannot be represented in an RPC (the zero id).
/// Mirrors `KafkaAdminClient.topicIdIsUnrepresentable`.
fn topic_id_is_unrepresentable(topic_id: Uuid) -> bool {
    topic_id == Uuid::ZERO_UUID
}

/// Returns the response error message with a fallback to the error code's
/// default message. Mirrors Java's `ApiError.messageWithFallback`.
fn message_with_fallback(code: i16, message: &Option<String>) -> String {
    match message {
        Some(m) if !m.is_empty() => m.clone(),
        _ => Errors::for_code(code).message().to_string(),
    }
}

/// Completes any future that was retried due to a quota-exceeded error with the
/// carried [`ThrottlingQuotaExceeded`](KafkaError::ThrottlingQuotaExceeded)
/// error (reduced by the elapsed throttle time) when the request ultimately
/// timed out. Mirrors `KafkaAdminClient.maybeCompleteQuotaExceededException`.
fn maybe_complete_quota_exceeded<K, T>(
    should_retry_on_quota_violation: bool,
    error: &KafkaError,
    futures: &HashMap<K, KafkaFutureImpl<T>>,
    quota_exceeded_exceptions: &HashMap<K, KafkaError>,
    throttle_time_delta: i32,
) where
    K: std::hash::Hash + Eq,
    T: Clone + Send + Sync + 'static,
{
    if should_retry_on_quota_violation && matches!(error, KafkaError::Timeout(_)) {
        for (key, quota_error) in quota_exceeded_exceptions {
            if let Some(future) = futures.get(key) {
                let throttle = quota_error.throttle_time_ms().unwrap_or(0);
                future.complete_exceptionally(KafkaError::throttling_quota_exceeded(
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
        Ok(Box::new(CreateAclsRequestBuilder::from_data(data)) as Box<dyn RequestBuilder>)
    });

    let resp_mm = mm.clone();
    let resp_futures = Arc::clone(&futures);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
        let ConcreteResponse::CreateAcls(create_response) = response else {
            return HandleResult::Retry(KafkaError::illegal_state("Expected a CreateAcls response"));
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
                    future.complete_exceptionally(KafkaError::with_message(
                        Errors::UnknownServerError,
                        format!("The broker reported no creation result for the given ACL: {binding}"),
                    ));
                },
                Some(creation) => {
                    if Errors::for_code(creation.error_code) != Errors::None {
                        future.complete_exceptionally(api_error(creation.error_code, &creation.error_message));
                    } else {
                        future.complete(());
                    }
                },
            }
        }
        HandleResult::Done
    });

    let fail_futures = Arc::clone(&futures);
    let handle_failure = Box::new(move |error: &KafkaError| {
        for future in fail_futures.values() {
            future.complete_exceptionally(error.clone());
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
        Ok(Box::new(DescribeAclsRequestBuilder::from_filter(&filter)) as Box<dyn RequestBuilder>)
    });

    let resp_handle = handle.clone();
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
        let ConcreteResponse::DescribeAcls(describe_response) = response else {
            return HandleResult::Retry(KafkaError::illegal_state("Expected a DescribeAcls response"));
        };
        if Errors::for_code(describe_response.error_code()) != Errors::None {
            resp_handle.complete_exceptionally(api_error(
                describe_response.error_code(),
                &describe_response.error_message().map(str::to_string),
            ));
        } else {
            match DescribeAclsResponse::acl_bindings(describe_response.acls()) {
                Ok(bindings) => {
                    resp_handle.complete(bindings);
                },
                Err(e) => {
                    resp_handle.complete_exceptionally(e);
                },
            }
        }
        HandleResult::Done
    });

    let fail_handle = handle.clone();
    let handle_failure = Box::new(move |error: &KafkaError| {
        fail_handle.complete_exceptionally(error.clone());
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
        Ok(Box::new(DescribeClientQuotasRequestBuilder::from_filter(&filter)) as Box<dyn RequestBuilder>)
    });

    let resp_handle = handle.clone();
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
        let ConcreteResponse::DescribeClientQuotas(describe_response) = response else {
            return HandleResult::Retry(KafkaError::illegal_state("Expected a DescribeClientQuotas response"));
        };
        // Mirrors DescribeClientQuotasResponse.complete: error first, else the
        // decoded entity map.
        if Errors::for_code(describe_response.error_code()) != Errors::None {
            resp_handle.complete_exceptionally(api_error(
                describe_response.error_code(),
                &describe_response.error_message().map(str::to_string),
            ));
        } else {
            resp_handle.complete(describe_response.entities());
        }
        HandleResult::Done
    });

    let fail_handle = handle.clone();
    let handle_failure = Box::new(move |error: &KafkaError| {
        fail_handle.complete_exceptionally(error.clone());
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
        Ok(Box::new(AlterClientQuotasRequestBuilder::new(&request_entries, validate_only)) as Box<dyn RequestBuilder>)
    });

    let resp_futures = Arc::clone(&futures);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
        let ConcreteResponse::AlterClientQuotas(alter_response) = response else {
            return HandleResult::Retry(KafkaError::illegal_state("Expected an AlterClientQuotas response"));
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
                    future.complete_exceptionally(e);
                },
            }
        }
        HandleResult::Done
    });

    let fail_futures = Arc::clone(&futures);
    let handle_failure = Box::new(move |error: &KafkaError| {
        for future in fail_futures.values() {
            future.complete_exceptionally(error.clone());
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
        Ok(Box::new(DescribeUserScramCredentialsRequestBuilder::from_data(data)) as Box<dyn RequestBuilder>)
    });

    let resp_handle = handle.clone();
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
        let ConcreteResponse::DescribeUserScramCredentials(describe_response) = response else {
            return HandleResult::Retry(KafkaError::illegal_state("Expected a DescribeUserScramCredentials response"));
        };
        // Mirrors handleResponse: a message-level error fails the whole future,
        // otherwise the raw data is handed to the *Result view helpers.
        let data = describe_response.data();
        let message_level_error_code = data.error_code;
        if message_level_error_code != Errors::None.code() {
            resp_handle.complete_exceptionally(api_error(message_level_error_code, &data.error_message));
        } else {
            resp_handle.complete(data.clone());
        }
        HandleResult::Done
    });

    let fail_handle = handle.clone();
    let handle_failure = Box::new(move |error: &KafkaError| {
        fail_handle.complete_exceptionally(error.clone());
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
    illegal: Arc<HashMap<String, KafkaError>>,
    futures: Arc<HashMap<String, KafkaFutureImpl<()>>>,
    metadata_manager: AdminMetadataManager,
    deadline: i64,
) -> Call {
    let create_request = Box::new(move |_timeout_ms: i32| {
        let mut data = AlterUserScramCredentialsRequestData::new();
        data.set_upsertions(upsertions.clone()).set_deletions(deletions.clone());
        Ok(Box::new(AlterUserScramCredentialsRequestBuilder::from_data(data)) as Box<dyn RequestBuilder>)
    });

    let resp_mm = metadata_manager;
    let resp_illegal = Arc::clone(&illegal);
    let resp_futures = Arc::clone(&futures);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
        let ConcreteResponse::AlterUserScramCredentials(alter_response) = response else {
            return HandleResult::Retry(KafkaError::illegal_state("Expected an AlterUserScramCredentials response"));
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
                future.complete_exceptionally(error.clone());
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
                        future.complete_exceptionally(api_error(result.error_code, &result.error_message));
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
                future.complete_exceptionally(KafkaError::with_message(
                    Errors::UnknownServerError,
                    format!("The broker response did not contain a result for user {user}"),
                ));
            }
        }
        HandleResult::Done
    });

    let fail_futures = Arc::clone(&futures);
    let handle_failure = Box::new(move |error: &KafkaError| {
        for future in fail_futures.values() {
            future.complete_exceptionally(error.clone());
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
fn get_scram_credential_upsertion(
    upsertion: &UserScramCredentialUpsertion,
) -> Result<ScramCredentialUpsertion, KafkaError> {
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
fn get_scram_credential_deletion(deletion: &UserScramCredentialDeletion) -> ScramCredentialDeletion {
    let mut wire = ScramCredentialDeletion::new();
    wire.set_name(deletion.user().to_string())
        .set_mechanism(deletion.mechanism().r#type());
    wire
}

/// Mirrors `new UnacceptableCredentialException(message)`.
fn unacceptable_credential(message: &str) -> KafkaError {
    KafkaError::with_message(Errors::UnacceptableCredential, message)
}

/// Mirrors `new UnsupportedSaslMechanismException(message)`.
fn unsupported_sasl_mechanism(message: &str) -> KafkaError {
    KafkaError::with_message(Errors::UnsupportedSaslMechanism, message)
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
    let renewer_principals = options.get_renewers().to_vec();
    let owner = options.get_owner().cloned();
    let max_lifetime_ms = options.get_max_lifetime_ms();

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
        Ok(Box::new(CreateDelegationTokenRequestBuilder::from_data(data)) as Box<dyn RequestBuilder>)
    });

    let resp_handle = handle.clone();
    let resp_renewers = options.get_renewers().to_vec();
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
        let ConcreteResponse::CreateDelegationToken(create_response) = response else {
            return HandleResult::Retry(KafkaError::illegal_state("Expected a CreateDelegationToken response"));
        };
        // Mirrors CreateDelegationToken handleResponse: error first, else build
        // the TokenInformation / DelegationToken from the response data using
        // the requested renewers.
        if create_response.has_error() {
            resp_handle.complete_exceptionally(KafkaError::new(create_response.error()));
        } else {
            let data = create_response.data();
            let token_info = TokenInformation::with_requester(
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
    let handle_failure = Box::new(move |error: &KafkaError| {
        fail_handle.complete_exceptionally(error.clone());
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
        Ok(Box::new(RenewDelegationTokenRequestBuilder::from_data(data)) as Box<dyn RequestBuilder>)
    });

    let resp_handle = handle.clone();
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
        let ConcreteResponse::RenewDelegationToken(renew_response) = response else {
            return HandleResult::Retry(KafkaError::illegal_state("Expected a RenewDelegationToken response"));
        };
        if renew_response.has_error() {
            resp_handle.complete_exceptionally(KafkaError::new(renew_response.error()));
        } else {
            resp_handle.complete(renew_response.expiry_timestamp());
        }
        HandleResult::Done
    });

    let fail_handle = handle.clone();
    let handle_failure = Box::new(move |error: &KafkaError| {
        fail_handle.complete_exceptionally(error.clone());
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
        Ok(Box::new(ExpireDelegationTokenRequestBuilder::from_data(data)) as Box<dyn RequestBuilder>)
    });

    let resp_handle = handle.clone();
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
        let ConcreteResponse::ExpireDelegationToken(expire_response) = response else {
            return HandleResult::Retry(KafkaError::illegal_state("Expected an ExpireDelegationToken response"));
        };
        if expire_response.has_error() {
            resp_handle.complete_exceptionally(KafkaError::new(expire_response.error()));
        } else {
            resp_handle.complete(expire_response.expiry_timestamp());
        }
        HandleResult::Done
    });

    let fail_handle = handle.clone();
    let handle_failure = Box::new(move |error: &KafkaError| {
        fail_handle.complete_exceptionally(error.clone());
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
        Ok(Box::new(DescribeDelegationTokenRequestBuilder::from_owners(owners.as_deref())) as Box<dyn RequestBuilder>)
    });

    let resp_handle = handle.clone();
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
        let ConcreteResponse::DescribeDelegationToken(describe_response) = response else {
            return HandleResult::Retry(KafkaError::illegal_state("Expected a DescribeDelegationToken response"));
        };
        if describe_response.has_error() {
            resp_handle.complete_exceptionally(KafkaError::new(describe_response.error()));
        } else {
            resp_handle.complete(describe_response.tokens());
        }
        HandleResult::Done
    });

    let fail_handle = handle.clone();
    let handle_failure = Box::new(move |error: &KafkaError| {
        fail_handle.complete_exceptionally(error.clone());
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
        Ok(Box::new(DeleteAclsRequestBuilder::from_data(data)) as Box<dyn RequestBuilder>)
    });

    let resp_mm = mm.clone();
    let resp_futures = Arc::clone(&futures);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
        let ConcreteResponse::DeleteAcls(delete_response) = response else {
            return HandleResult::Retry(KafkaError::illegal_state("Expected a DeleteAcls response"));
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
                    future.complete_exceptionally(KafkaError::with_message(
                        Errors::UnknownServerError,
                        "The broker reported no deletion result for the given filter.",
                    ));
                },
                Some(filter_result) => {
                    if Errors::for_code(filter_result.error_code) != Errors::None {
                        future
                            .complete_exceptionally(api_error(filter_result.error_code, &filter_result.error_message));
                    } else {
                        let mut results = Vec::new();
                        for matching_acl in &filter_result.matching_acls {
                            let binding = DeleteAclsResponse::acl_binding(matching_acl).ok();
                            let exception = if Errors::for_code(matching_acl.error_code) != Errors::None {
                                Some(api_error(matching_acl.error_code, &matching_acl.error_message))
                            } else {
                                None
                            };
                            results.push(FilterResult::new(binding, exception));
                        }
                        future.complete(FilterResults::new(results));
                    }
                },
            }
        }
        HandleResult::Done
    });

    let fail_futures = Arc::clone(&futures);
    let handle_failure = Box::new(move |error: &KafkaError| {
        for future in fail_futures.values() {
            future.complete_exceptionally(error.clone());
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
fn handle_not_controller_error(mm: &AdminMetadataManager, error_counts: &HashMap<Errors, i32>) -> Option<KafkaError> {
    let has_not_controller = error_counts.contains_key(&Errors::NotController)
        || (mm.using_bootstrap_controllers() && error_counts.contains_key(&Errors::NotLeaderOrFollower));
    if has_not_controller {
        mm.clear_controller();
        mm.request_update();
        Some(KafkaError::new(Errors::NotController))
    } else {
        None
    }
}

/// Maps an [`OffsetSpec`] to the wire-protocol timestamp sentinel used by
/// `ListOffsets`.
///
/// Mirrors `KafkaAdminClient.getOffsetFromSpec`.
fn get_offset_from_spec(offset_spec: OffsetSpec) -> i64 {
    use crate::common::requests::list_offsets_request::{
        EARLIEST_LOCAL_TIMESTAMP, EARLIEST_PENDING_UPLOAD_TIMESTAMP, EARLIEST_TIMESTAMP, LATEST_TIERED_TIMESTAMP,
        LATEST_TIMESTAMP, MAX_TIMESTAMP,
    };
    match offset_spec {
        OffsetSpec::Timestamp(timestamp) => timestamp,
        OffsetSpec::Earliest => EARLIEST_TIMESTAMP,
        OffsetSpec::MaxTimestamp => MAX_TIMESTAMP,
        OffsetSpec::EarliestLocal => EARLIEST_LOCAL_TIMESTAMP,
        OffsetSpec::LatestTiered => LATEST_TIERED_TIMESTAMP,
        OffsetSpec::EarliestPendingUpload => EARLIEST_PENDING_UPLOAD_TIMESTAMP,
        OffsetSpec::Latest => LATEST_TIMESTAMP,
    }
}

/// Builds the `alterPartitionReassignments` controller call.
///
/// Mirrors the anonymous `Call` in `KafkaAdminClient.alterPartitionReassignments`.
#[allow(clippy::type_complexity)]
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
        Ok(Box::new(AlterPartitionReassignmentsRequestBuilder::from_data(data)) as Box<dyn RequestBuilder>)
    });

    let resp_mm = mm.clone();
    let resp_futures = Arc::clone(&futures);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
        let ConcreteResponse::AlterPartitionReassignments(alter_response) = response else {
            return HandleResult::Retry(KafkaError::illegal_state("Expected an AlterPartitionReassignments response"));
        };
        let data = alter_response.data();
        let mut errors: HashMap<TopicPartition, Option<KafkaError>> = HashMap::new();
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
                            errors.insert(
                                tp,
                                Some(KafkaError::with_message(
                                    partition_error,
                                    part_response.error_message.clone().unwrap_or_default(),
                                )),
                            );
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
                        errors.insert(
                            tp,
                            Some(KafkaError::with_message(
                                top_level_error,
                                data.error_message.clone().unwrap_or_default(),
                            )),
                        );
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
            let error = KafkaError::with_message(
                Errors::UnknownServerError,
                format!(
                    "The server returned too {quantifier} results.Expected {expected_responses_count} but received {received_responses_count}"
                ),
            );
            for future in resp_futures.values() {
                future.complete_exceptionally(error.clone());
            }
            return HandleResult::Done;
        }

        for (tp, exception) in errors {
            let Some(future) = resp_futures.get(&tp) else {
                continue;
            };
            match exception {
                None => {
                    future.complete(());
                },
                Some(error) => {
                    future.complete_exceptionally(error);
                },
            }
        }
        HandleResult::Done
    });

    let fail_futures = Arc::clone(&futures);
    let handle_failure = Box::new(move |error: &KafkaError| {
        for future in fail_futures.values() {
            future.complete_exceptionally(error.clone());
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
        Ok(Box::new(ListPartitionReassignmentsRequestBuilder::from_data(list_data)) as Box<dyn RequestBuilder>)
    });

    let resp_mm = mm.clone();
    let resp_handle = handle.clone();
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
        let ConcreteResponse::ListPartitionReassignments(list_response) = response else {
            return HandleResult::Retry(KafkaError::illegal_state("Expected a ListPartitionReassignments response"));
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
                resp_handle.complete_exceptionally(KafkaError::with_message(
                    error,
                    data.error_message.clone().unwrap_or_default(),
                ));
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
    let handle_failure = Box::new(move |error: &KafkaError| {
        fail_handle.complete_exceptionally(error.clone());
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
fn node_for(resource: &ConfigResource) -> Option<i32> {
    if (resource.resource_type() == ConfigResourceType::Broker && !resource.is_default())
        || resource.resource_type() == ConfigResourceType::BrokerLogger
    {
        // Java parses `Integer.valueOf(resource.name())`; a non-numeric name
        // would throw. Here a parse failure degrades to "any broker" rather
        // than panicking on a recoverable path (CLAUDE.md §10).
        resource.name().parse::<i32>().ok()
    } else {
        None
    }
}

/// Decodes a 32-bit authorized-operations field into an optional set of valid
/// [`AclOperation`]s, returning `None` when the field is omitted.
///
/// Mirrors `AdminUtils.validAclOperations`, which returns `null` when the
/// operations are omitted (Java's `describeCluster` completes the future with
/// that `null`).
fn valid_acl_operations_or_null(authorized_operations: i32) -> Option<BTreeSet<AclOperation>> {
    if authorized_operations == AUTHORIZED_OPERATIONS_OMITTED {
        None
    } else {
        Some(valid_acl_operations(authorized_operations))
    }
}

/// Converts a `DescribeConfigsResult` wire result into a [`Config`], mirroring
/// `KafkaAdminClient.describeConfigResult`.
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
        ConfigEntry::with_metadata(
            config.name.clone(),
            config.value.clone(),
            ConfigSource::for_id(config.config_source),
            config.is_sensitive,
            config.read_only,
            synonyms,
            ConfigType::for_id(config.config_type),
            config.documentation.clone(),
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
        Ok(Box::new(DescribeConfigsRequestBuilder::from_data(data)) as Box<dyn RequestBuilder>)
    });

    let resp_unified = Arc::clone(&unified);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
        let ConcreteResponse::DescribeConfigs(describe_response) = response else {
            return HandleResult::Retry(KafkaError::illegal_state("Expected a DescribeConfigs response"));
        };
        for (config_resource, result) in describe_response.result_map() {
            let Some(future) = resp_unified.get(&config_resource) else {
                // A config in the response that was not in the request; Java
                // logs a warning and ignores it.
                continue;
            };
            if result.error_code != Errors::None.code() {
                future.complete_exceptionally(api_error(result.error_code, &result.error_message));
            } else {
                future.complete(describe_config_result(result));
            }
        }
        // Complete any future for which the node did not return a result.
        for (resource, future) in resp_unified.iter() {
            if !future.is_done() {
                future.complete_exceptionally(KafkaError::with_message(
                    Errors::UnknownServerError,
                    format!("The node response did not contain a result for config resource {resource}"),
                ));
            }
        }
        HandleResult::Done
    });

    let fail_unified = Arc::clone(&unified);
    let handle_failure = Box::new(move |error: &KafkaError| {
        for future in fail_unified.values() {
            future.complete_exceptionally(error.clone());
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
            future.complete_exceptionally(KafkaError::with_message(Errors::UnknownServerError, message(name)));
        }
    }
}

// ---------------------------------------------------------------------------
// describeLogDirs / alterReplicaLogDirs / describeReplicaLogDirs
// ---------------------------------------------------------------------------

/// Maps a protocol error code to an optional error, mirroring Java's
/// `Errors.forCode(code).exception()` which returns `null` for `NONE`.
fn api_exception(error_code: i16) -> Option<KafkaError> {
    let error = Errors::for_code(error_code);
    (error != Errors::None).then(|| KafkaError::new(error))
}

/// Builds a map from log-directory path to [`LogDirDescription`] from a
/// `DescribeLogDirs` response. Mirrors `KafkaAdminClient.logDirDescriptions`.
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
            LogDirDescription::with_volume_bytes(
                api_exception(log_dir_result.error_code),
                replica_info_map,
                log_dir_result.total_bytes,
                log_dir_result.usable_bytes,
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
        Ok(Box::new(DescribeLogDirsRequestBuilder::from_data(data)) as Box<dyn RequestBuilder>)
    });

    let resp_handle = handle.clone();
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
        let ConcreteResponse::DescribeLogDirs(resp) = response else {
            return HandleResult::Retry(KafkaError::illegal_state("Expected a DescribeLogDirs response"));
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
            resp_handle.complete_exceptionally(KafkaError::new(error));
        }
        HandleResult::Done
    });

    let fail_handle = handle;
    let handle_failure = Box::new(move |error: &KafkaError| {
        fail_handle.complete_exceptionally(error.clone());
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
        Ok(Box::new(AlterReplicaLogDirsRequestBuilder::from_data(assignment.clone())) as Box<dyn RequestBuilder>)
    });

    let resp_futures = Arc::clone(&futures);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
        let ConcreteResponse::AlterReplicaLogDirs(resp) = response else {
            return HandleResult::Retry(KafkaError::illegal_state("Expected an AlterReplicaLogDirs response"));
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
                            future
                                .complete_exceptionally(KafkaError::new(Errors::for_code(partition_result.error_code)));
                        }
                    },
                }
            }
        }
        // The server should send back a result for every replica. Do a sanity
        // check anyway (mirrors `completeUnrealizedFutures`).
        for (replica, future) in resp_futures.iter() {
            if replica.broker_id() == broker_id && !future.is_done() {
                future.complete_exceptionally(KafkaError::with_message(
                    Errors::UnknownServerError,
                    format!("The response from broker {broker_id} did not contain a result for replica {replica}"),
                ));
            }
        }
        HandleResult::Done
    });

    let fail_futures = Arc::clone(&futures);
    let handle_failure = Box::new(move |error: &KafkaError| {
        // Only completes the futures of brokerId.
        for (replica, future) in fail_futures.iter() {
            if replica.broker_id() == broker_id {
                future.complete_exceptionally(error.clone());
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
        Ok(Box::new(DescribeLogDirsRequestBuilder::from_data(request_data.clone())) as Box<dyn RequestBuilder>)
    });

    let resp_futures = Arc::clone(&futures);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
        let ConcreteResponse::DescribeLogDirs(resp) = response else {
            return HandleResult::Retry(KafkaError::illegal_state("Expected a DescribeLogDirs response"));
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
                let illegal = KafkaError::illegal_state(format!(
                    "The error {:?} for log directory {log_dir} in the response from broker {broker_id} is illegal",
                    error.error()
                ));
                for future in resp_futures.values() {
                    future.complete_exceptionally(illegal.clone());
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
    let handle_failure = Box::new(move |error: &KafkaError| {
        for future in fail_futures.values() {
            future.complete_exceptionally(error.clone());
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
            TopicPartitionInfo::with_leader_replicas_isr(
                p.partition(),
                leader,
                p.replicas().to_vec(),
                p.in_sync_replicas().to_vec(),
            )
        })
        .collect();
    partitions.sort_by_key(|p| p.partition());
    TopicDescription::with_authorized_operations(
        topic_name,
        is_internal,
        partitions,
        valid_acl_operations(authorized_operations),
        topic_id,
    )
}

// ---------------------------------------------------------------------------
// createTopics
// ---------------------------------------------------------------------------

/// Builds a `createTopics` [`Call`]. Free function so the quota-retry path can
/// rebuild a fresh call with the same futures. Translated from
/// `KafkaAdminClient.getCreateTopicsCall`.
#[allow(clippy::too_many_arguments)]
fn get_create_topics_call(
    mm: AdminMetadataManager,
    futures: Arc<HashMap<String, KafkaFutureImpl<TopicMetadataAndConfig>>>,
    topics_by_name: Arc<HashMap<String, CreatableTopic>>,
    names: Vec<String>,
    quota_exceeded_exceptions: HashMap<String, KafkaError>,
    validate_only: bool,
    retry_on_quota: bool,
    now: i64,
    deadline: i64,
    time_provider: Arc<dyn Fn() -> i64 + Send + Sync>,
) -> Call {
    let req_names = names.clone();
    let req_topics = Arc::clone(&topics_by_name);
    let create_request = Box::new(move |timeout_ms: i32| {
        let mut data = CreateTopicsRequestData::new();
        let topics: Vec<CreatableTopic> = req_names.iter().filter_map(|n| req_topics.get(n).cloned()).collect();
        data.set_topics(topics);
        data.set_timeout_ms(timeout_ms);
        data.set_validate_only(validate_only);
        Ok(Box::new(CreateTopicsRequestBuilder::from_data(data)) as Box<dyn RequestBuilder>)
    });

    let resp_mm = mm.clone();
    let resp_futures = Arc::clone(&futures);
    let resp_topics = Arc::clone(&topics_by_name);
    let resp_time = Arc::clone(&time_provider);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
        let ConcreteResponse::CreateTopics(create_response) = response else {
            return HandleResult::Retry(KafkaError::illegal_state("Expected a CreateTopics response"));
        };
        if let Some(err) = handle_not_controller_error(&resp_mm, &create_response.error_counts()) {
            return HandleResult::Retry(err);
        }
        let throttle_time_ms = create_response.throttle_time_ms();
        let mut retry_names: Vec<String> = Vec::new();
        let mut retry_quota_exceeded: HashMap<String, KafkaError> = HashMap::new();
        for result in &create_response.data().topics {
            let Some(future) = resp_futures.get(&result.name) else {
                continue;
            };
            let error = Errors::for_code(result.error_code);
            if error != Errors::None {
                if error == Errors::ThrottlingQuotaExceeded {
                    let quota_error = KafkaError::throttling_quota_exceeded(
                        throttle_time_ms,
                        message_with_fallback(result.error_code, &result.error_message),
                    );
                    if retry_on_quota {
                        retry_names.push(result.name.clone());
                        retry_quota_exceeded.insert(result.name.clone(), quota_error);
                    } else {
                        future.complete_exceptionally(quota_error);
                    }
                } else {
                    future.complete_exceptionally(api_error(result.error_code, &result.error_message));
                }
            } else if result.topic_config_error_code != Errors::None.code() {
                future.complete(TopicMetadataAndConfig::with_error(KafkaError::new(Errors::for_code(
                    result.topic_config_error_code,
                ))));
            } else if result.num_partitions == crate::admin::create_topics_result::UNKNOWN {
                future.complete(TopicMetadataAndConfig::with_error(KafkaError::unsupported_version(
                    "Topic metadata and configs in CreateTopics response not supported",
                )));
            } else {
                let config = result
                    .configs
                    .as_ref()
                    .map(|configs| {
                        Config::new(configs.iter().map(|c| {
                            ConfigEntry::with_metadata(
                                c.name.clone(),
                                c.value.clone(),
                                ConfigSource::for_id(c.config_source),
                                c.is_sensitive,
                                c.read_only,
                                Vec::new(),
                                ConfigType::Unknown,
                                None,
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
            let retry_now = (resp_time)();
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
    let fail_time = Arc::clone(&time_provider);
    let handle_failure = Box::new(move |error: &KafkaError| {
        // If there were any topics retried due to a quota exceeded exception,
        // propagate the initial error back to the caller if the request timed
        // out (mirrors maybeCompleteQuotaExceededException).
        let throttle_time_delta = ((fail_time)() - now).clamp(0, i32::MAX as i64) as i32;
        maybe_complete_quota_exceeded(
            retry_on_quota,
            error,
            &fail_futures,
            &quota_exceeded_exceptions,
            throttle_time_delta,
        );
        for future in fail_futures.values() {
            future.complete_exceptionally(error.clone());
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
#[allow(clippy::too_many_arguments)]
fn get_create_partitions_call(
    mm: AdminMetadataManager,
    futures: Arc<HashMap<String, KafkaFutureImpl<()>>>,
    topics_by_name: Arc<HashMap<String, CreatePartitionsTopic>>,
    names: Vec<String>,
    quota_exceeded_exceptions: HashMap<String, KafkaError>,
    validate_only: bool,
    retry_on_quota: bool,
    now: i64,
    deadline: i64,
    time_provider: Arc<dyn Fn() -> i64 + Send + Sync>,
) -> Call {
    let req_names = names.clone();
    let req_topics = Arc::clone(&topics_by_name);
    let create_request = Box::new(move |timeout_ms: i32| {
        let mut data = CreatePartitionsRequestData::new();
        let topics: Vec<CreatePartitionsTopic> = req_names.iter().filter_map(|n| req_topics.get(n).cloned()).collect();
        data.set_topics(topics);
        data.set_timeout_ms(timeout_ms);
        data.set_validate_only(validate_only);
        Ok(Box::new(CreatePartitionsRequestBuilder::from_data(data)) as Box<dyn RequestBuilder>)
    });

    let resp_mm = mm.clone();
    let resp_futures = Arc::clone(&futures);
    let resp_topics = Arc::clone(&topics_by_name);
    let resp_time = Arc::clone(&time_provider);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
        let ConcreteResponse::CreatePartitions(create_response) = response else {
            return HandleResult::Retry(KafkaError::illegal_state("Expected a CreatePartitions response"));
        };
        if let Some(err) = handle_not_controller_error(&resp_mm, &create_response.error_counts()) {
            return HandleResult::Retry(err);
        }
        let throttle_time_ms = create_response.throttle_time_ms();
        let mut retry_names: Vec<String> = Vec::new();
        let mut retry_quota_exceeded: HashMap<String, KafkaError> = HashMap::new();
        for result in &create_response.data().results {
            let Some(future) = resp_futures.get(&result.name) else {
                continue;
            };
            let error = Errors::for_code(result.error_code);
            if error != Errors::None {
                if error == Errors::ThrottlingQuotaExceeded {
                    let quota_error = KafkaError::throttling_quota_exceeded(
                        throttle_time_ms,
                        message_with_fallback(result.error_code, &result.error_message),
                    );
                    if retry_on_quota {
                        retry_names.push(result.name.clone());
                        retry_quota_exceeded.insert(result.name.clone(), quota_error);
                    } else {
                        future.complete_exceptionally(quota_error);
                    }
                } else {
                    future.complete_exceptionally(api_error(result.error_code, &result.error_message));
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
            let retry_now = (resp_time)();
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
    let fail_time = Arc::clone(&time_provider);
    let handle_failure = Box::new(move |error: &KafkaError| {
        let throttle_time_delta = ((fail_time)() - now).clamp(0, i32::MAX as i64) as i32;
        maybe_complete_quota_exceeded(
            retry_on_quota,
            error,
            &fail_futures,
            &quota_exceeded_exceptions,
            throttle_time_delta,
        );
        for future in fail_futures.values() {
            future.complete_exceptionally(error.clone());
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
#[allow(clippy::too_many_arguments)]
fn get_delete_topics_call(
    mm: AdminMetadataManager,
    futures: Arc<HashMap<String, KafkaFutureImpl<()>>>,
    names: Vec<String>,
    quota_exceeded_exceptions: HashMap<String, KafkaError>,
    retry_on_quota: bool,
    now: i64,
    deadline: i64,
    time_provider: Arc<dyn Fn() -> i64 + Send + Sync>,
) -> Call {
    let req_names = names.clone();
    let create_request = Box::new(move |timeout_ms: i32| {
        let mut data = DeleteTopicsRequestData::new();
        data.set_topic_names(req_names.clone());
        data.set_timeout_ms(timeout_ms);
        Ok(Box::new(DeleteTopicsRequestBuilder::from_data(data)) as Box<dyn RequestBuilder>)
    });

    let resp_mm = mm.clone();
    let resp_futures = Arc::clone(&futures);
    let resp_time = Arc::clone(&time_provider);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
        let ConcreteResponse::DeleteTopics(delete_response) = response else {
            return HandleResult::Retry(KafkaError::illegal_state("Expected a DeleteTopics response"));
        };
        if let Some(err) = handle_not_controller_error(&resp_mm, &delete_response.error_counts()) {
            return HandleResult::Retry(err);
        }
        let throttle_time_ms = delete_response.throttle_time_ms();
        let mut retry_names: Vec<String> = Vec::new();
        let mut retry_quota_exceeded: HashMap<String, KafkaError> = HashMap::new();
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
                    let quota_error = KafkaError::throttling_quota_exceeded(
                        throttle_time_ms,
                        message_with_fallback(result.error_code, &result.error_message),
                    );
                    if retry_on_quota {
                        retry_names.push(name.clone());
                        retry_quota_exceeded.insert(name.clone(), quota_error);
                    } else {
                        future.complete_exceptionally(quota_error);
                    }
                } else {
                    future.complete_exceptionally(api_error(result.error_code, &result.error_message));
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
            let retry_now = (resp_time)();
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
    let fail_time = Arc::clone(&time_provider);
    let handle_failure = Box::new(move |error: &KafkaError| {
        let throttle_time_delta = ((fail_time)() - now).clamp(0, i32::MAX as i64) as i32;
        maybe_complete_quota_exceeded(
            retry_on_quota,
            error,
            &fail_futures,
            &quota_exceeded_exceptions,
            throttle_time_delta,
        );
        for future in fail_futures.values() {
            future.complete_exceptionally(error.clone());
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
#[allow(clippy::too_many_arguments)]
fn get_delete_topics_with_ids_call(
    mm: AdminMetadataManager,
    futures: Arc<HashMap<Uuid, KafkaFutureImpl<()>>>,
    ids: Vec<Uuid>,
    quota_exceeded_exceptions: HashMap<Uuid, KafkaError>,
    retry_on_quota: bool,
    now: i64,
    deadline: i64,
    time_provider: Arc<dyn Fn() -> i64 + Send + Sync>,
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
        Ok(Box::new(DeleteTopicsRequestBuilder::from_data(data)) as Box<dyn RequestBuilder>)
    });

    let resp_mm = mm.clone();
    let resp_futures = Arc::clone(&futures);
    let resp_time = Arc::clone(&time_provider);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
        let ConcreteResponse::DeleteTopics(delete_response) = response else {
            return HandleResult::Retry(KafkaError::illegal_state("Expected a DeleteTopics response"));
        };
        if let Some(err) = handle_not_controller_error(&resp_mm, &delete_response.error_counts()) {
            return HandleResult::Retry(err);
        }
        let throttle_time_ms = delete_response.throttle_time_ms();
        let mut retry_ids: Vec<Uuid> = Vec::new();
        let mut retry_quota_exceeded: HashMap<Uuid, KafkaError> = HashMap::new();
        for result in &delete_response.data().responses {
            let Some(future) = resp_futures.get(&result.topic_id) else {
                continue;
            };
            let error = Errors::for_code(result.error_code);
            if error != Errors::None {
                if error == Errors::ThrottlingQuotaExceeded {
                    let quota_error = KafkaError::throttling_quota_exceeded(
                        throttle_time_ms,
                        message_with_fallback(result.error_code, &result.error_message),
                    );
                    if retry_on_quota {
                        retry_ids.push(result.topic_id);
                        retry_quota_exceeded.insert(result.topic_id, quota_error);
                    } else {
                        future.complete_exceptionally(quota_error);
                    }
                } else {
                    future.complete_exceptionally(api_error(result.error_code, &result.error_message));
                }
            } else {
                future.complete(());
            }
        }
        // Complete any unrealized id-keyed future.
        if retry_ids.is_empty() {
            for (id, future) in resp_futures.iter() {
                if !future.is_done() {
                    future.complete_exceptionally(KafkaError::with_message(
                        Errors::UnknownServerError,
                        format!("The controller response did not contain a result for topic {id}"),
                    ));
                }
            }
            HandleResult::Done
        } else {
            let retry_now = (resp_time)();
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
    let fail_time = Arc::clone(&time_provider);
    let handle_failure = Box::new(move |error: &KafkaError| {
        let throttle_time_delta = ((fail_time)() - now).clamp(0, i32::MAX as i64) as i32;
        maybe_complete_quota_exceeded(
            retry_on_quota,
            error,
            &fail_futures,
            &quota_exceeded_exceptions,
            throttle_time_delta,
        );
        for future in fail_futures.values() {
            future.complete_exceptionally(error.clone());
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
    fn create_topics(&self, new_topics: &[NewTopic], options: CreateTopicsOptions) -> CreateTopicsResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);

        let mut handles: HashMap<String, KafkaFutureImpl<TopicMetadataAndConfig>> = HashMap::new();
        let mut topics_by_name: HashMap<String, CreatableTopic> = HashMap::new();
        for new_topic in new_topics {
            let name = new_topic.name().to_string();
            if topic_name_is_unrepresentable(&name) {
                let future: KafkaFutureImpl<TopicMetadataAndConfig> = KafkaFutureImpl::new();
                future.complete_exceptionally(KafkaError::with_message(
                    Errors::InvalidTopicException,
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
                Arc::clone(&self.shared.time_provider),
            );
            self.submit(call);
        }
        CreateTopicsResult::new(public)
    }

    fn delete_topics(&self, topics: TopicCollection, options: DeleteTopicsOptions) -> DeleteTopicsResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        match topics {
            TopicCollection::TopicNames(names) => {
                let mut handles: HashMap<String, KafkaFutureImpl<()>> = HashMap::new();
                let mut valid_topic_names: Vec<String> = Vec::new();
                for name in &names {
                    if topic_name_is_unrepresentable(name) {
                        let future: KafkaFutureImpl<()> = KafkaFutureImpl::new();
                        future.complete_exceptionally(KafkaError::with_message(
                            Errors::InvalidTopicException,
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
                        Arc::clone(&self.shared.time_provider),
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
                        future.complete_exceptionally(KafkaError::with_message(
                            Errors::InvalidTopicException,
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
                        Arc::clone(&self.shared.time_provider),
                    );
                    self.submit(call);
                }
                DeleteTopicsResult::of_topic_ids(public)
            },
        }
    }

    fn list_topics(&self, options: ListTopicsOptions) -> ListTopicsResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let handle: KafkaFutureImpl<HashMap<String, TopicListing>> = KafkaFutureImpl::new();
        let public = handle.future();
        let list_internal = options.should_list_internal();

        let create_request = Box::new(move |_timeout_ms: i32| {
            Ok(Box::new(MetadataRequestBuilder::all_topics()) as Box<dyn RequestBuilder>)
        });

        let resp_handle = handle.clone();
        let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
            let ConcreteResponse::Metadata(metadata_response) = response else {
                return HandleResult::Retry(KafkaError::illegal_state("Expected a Metadata response"));
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
        let handle_failure = Box::new(move |error: &KafkaError| {
            fail_handle.complete_exceptionally(error.clone());
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

    fn describe_topics(&self, topics: TopicCollection, options: DescribeTopicsOptions) -> DescribeTopicsResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        match topics {
            TopicCollection::TopicNames(names) => {
                let mut handles: HashMap<String, KafkaFutureImpl<TopicDescription>> = HashMap::new();
                let mut valid_topic_names: Vec<String> = Vec::new();
                for name in &names {
                    if topic_name_is_unrepresentable(name) {
                        let future: KafkaFutureImpl<TopicDescription> = KafkaFutureImpl::new();
                        future.complete_exceptionally(KafkaError::with_message(
                            Errors::InvalidTopicException,
                            format!("The given topic name '{name}' cannot be represented in a request."),
                        ));
                        handles.insert(name.clone(), future);
                    } else if let std::collections::hash_map::Entry::Vacant(entry) = handles.entry(name.clone()) {
                        entry.insert(KafkaFutureImpl::new());
                        valid_topic_names.push(name.clone());
                    }
                }
                let public: HashMap<String, KafkaFuture<TopicDescription>> =
                    handles.iter().map(|(k, v)| (k.clone(), v.future())).collect();
                if !valid_topic_names.is_empty() {
                    let call = get_describe_topics_by_names_call(
                        Arc::new(handles),
                        valid_topic_names,
                        options.should_include_authorized_operations(),
                        deadline,
                    );
                    self.submit(call);
                }
                DescribeTopicsResult::of_topic_names(public)
            },
            TopicCollection::TopicIds(ids) => {
                // Describing by id uses the Metadata API in Java too
                // (handleDescribeTopicsByIds → convertTopicIdsToMetadataRequestTopic),
                // not DescribeTopicPartitions, so it is translated faithfully here.
                let mut handles: HashMap<Uuid, KafkaFutureImpl<TopicDescription>> = HashMap::new();
                let mut valid_topic_ids: Vec<Uuid> = Vec::new();
                for id in &ids {
                    if topic_id_is_unrepresentable(*id) {
                        let future: KafkaFutureImpl<TopicDescription> = KafkaFutureImpl::new();
                        future.complete_exceptionally(KafkaError::with_message(
                            Errors::InvalidTopicException,
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
                    let call = get_describe_topics_by_ids_call(
                        Arc::new(handles),
                        valid_topic_ids,
                        options.should_include_authorized_operations(),
                        deadline,
                    );
                    self.submit(call);
                }
                DescribeTopicsResult::of_topic_ids(public)
            },
        }
    }

    fn create_partitions(
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
            let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
            let names: Vec<String> = topics_by_name.keys().cloned().collect();
            let call = get_create_partitions_call(
                self.shared.metadata_manager.clone(),
                Arc::new(handles),
                Arc::new(topics_by_name),
                names,
                HashMap::new(),
                options.should_validate_only(),
                options.should_retry_on_quota_violation(),
                now,
                deadline,
                Arc::clone(&self.shared.time_provider),
            );
            self.submit(call);
        }
        CreatePartitionsResult::new(public)
    }

    fn delete_records(
        &self,
        records_to_delete: &HashMap<TopicPartition, RecordsToDelete>,
        options: DeleteRecordsOptions,
    ) -> DeleteRecordsResult {
        let keys: std::collections::HashSet<TopicPartition> = records_to_delete.keys().cloned().collect();
        let future = DeleteRecordsHandler::new_future(keys, Arc::clone(&self.shared.partition_leader_cache));
        let result_map = future.all();

        let timeout_ms = options.timeout().unwrap_or(self.shared.default_api_timeout_ms);
        let handler = DeleteRecordsHandler::new(
            records_to_delete.clone(),
            LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id)),
            timeout_ms,
        );

        let now = self.now();
        // Java calc: calcDeadlineMs(now, options.timeoutMs()) — the raw option
        // (which may be null → default), not the resolved handler timeout.
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
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

    fn describe_producers(
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
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let retry_backoff = self.retry_backoff();
        let driver = AdminApiDriver::new(Box::new(handler), Box::new(future), deadline, retry_backoff, log_context);
        invoke_driver(driver, self.driver_context(), now);

        DescribeProducersResult::new(result_map)
    }

    fn abort_transaction(
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
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let retry_backoff = self.retry_backoff();
        let driver = AdminApiDriver::new(Box::new(handler), Box::new(future), deadline, retry_backoff, log_context);
        invoke_driver(driver, self.driver_context(), now);

        AbortTransactionResult::new(result_map)
    }

    fn describe_transactions(
        &self,
        transactional_ids: &[String],
        options: DescribeTransactionsOptions,
    ) -> DescribeTransactionsResult {
        let log_context = LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id));
        let future = DescribeTransactionsHandler::new_future(transactional_ids);
        let result_map = future.all();
        let handler = DescribeTransactionsHandler::new(log_context.clone());

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let retry_backoff = self.retry_backoff();
        let driver = AdminApiDriver::new(Box::new(handler), Box::new(future), deadline, retry_backoff, log_context);
        invoke_driver(driver, self.driver_context(), now);

        DescribeTransactionsResult::new(coordinator_keyed_by_id(result_map))
    }

    fn fence_producers(&self, transactional_ids: &[String], options: FenceProducersOptions) -> FenceProducersResult {
        let log_context = LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id));
        let future = FenceProducersHandler::new_future(transactional_ids);
        let result_map = future.all();
        let handler = FenceProducersHandler::new(&options, log_context.clone(), self.shared.request_timeout_ms);

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let retry_backoff = self.retry_backoff();
        let driver = AdminApiDriver::new(Box::new(handler), Box::new(future), deadline, retry_backoff, log_context);
        invoke_driver(driver, self.driver_context(), now);

        FenceProducersResult::new(coordinator_keyed_by_id(result_map))
    }

    fn list_transactions(&self, options: ListTransactionsOptions) -> ListTransactionsResult {
        let log_context = LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id));
        let future = ListTransactionsHandler::new_future();
        let result_future = future.all();

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let handler = ListTransactionsHandler::new(options, log_context.clone());
        let retry_backoff = self.retry_backoff();
        let driver = AdminApiDriver::new(Box::new(handler), Box::new(future), deadline, retry_backoff, log_context);
        invoke_driver(driver, self.driver_context(), now);

        ListTransactionsResult::new(result_future)
    }

    fn force_terminate_transaction(
        &self,
        transactional_id: &str,
        options: TerminateTransactionOptions,
    ) -> TerminateTransactionResult {
        // Simply leverage the existing fenceProducers implementation with a
        // single transactional id (mirrors Java's forceTerminateTransaction).
        let mut fence_options = FenceProducersOptions::new();
        if options.timeout().is_some() {
            fence_options = fence_options.timeout_ms(options.timeout());
        }
        let ids = vec![transactional_id.to_string()];
        let fence_result = self.fence_producers(&ids, fence_options);

        // Convert the result to a TerminateTransactionResult.
        let future = fence_result
            .fenced_producers()
            .get(transactional_id)
            .cloned()
            .expect("the transactional id was included in the fenceProducers request");
        TerminateTransactionResult::new(future)
    }

    fn describe_cluster(&self, options: DescribeClusterOptions) -> DescribeClusterResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);

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
        let include_authorized_operations = options.should_include_authorized_operations();
        let include_fenced_brokers = options.should_include_fenced_brokers();

        let req_use_metadata = Arc::clone(&use_metadata_request);
        let req_mm = mm.clone();
        let create_request = Box::new(move |_timeout_ms: i32| {
            if req_use_metadata.load(std::sync::atomic::Ordering::Acquire) {
                // Only requests node information; allow_auto_topic_creation=true
                // simplifies communication with older brokers.
                let mut data = crate::metadata_request_data::MetadataRequestData::new();
                data.set_topics(Some(Vec::new()));
                data.set_allow_auto_topic_creation(true);
                data.set_include_cluster_authorized_operations(include_authorized_operations);
                Ok(Box::new(MetadataRequestBuilder::from_data(data)) as Box<dyn RequestBuilder>)
            } else {
                if req_mm.using_bootstrap_controllers() && include_fenced_brokers {
                    return Err(KafkaError::illegal_argument(
                        "Cannot request fenced brokers from controller endpoint",
                    ));
                }
                let endpoint_type = if req_mm.using_bootstrap_controllers() {
                    ENDPOINT_TYPE_CONTROLLER
                } else {
                    ENDPOINT_TYPE_BROKER
                };
                let mut data = DescribeClusterRequestData::new();
                data.set_include_cluster_authorized_operations(include_authorized_operations);
                data.set_endpoint_type(endpoint_type);
                data.set_include_fenced_brokers(include_fenced_brokers);
                Ok(Box::new(DescribeClusterRequestBuilder::from_data(data)) as Box<dyn RequestBuilder>)
            }
        });

        let resp_use_metadata = Arc::clone(&use_metadata_request);
        let resp_nodes = nodes_handle.clone();
        let resp_controller = controller_handle.clone();
        let resp_cluster_id = cluster_id_handle.clone();
        let resp_authorized = authorized_ops_handle.clone();
        let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
            if resp_use_metadata.load(std::sync::atomic::Ordering::Acquire) {
                let ConcreteResponse::Metadata(metadata_response) = response else {
                    return HandleResult::Retry(KafkaError::illegal_state("Expected a Metadata response"));
                };
                resp_nodes.complete(metadata_response.brokers().to_vec());
                let controller = metadata_response.controller().filter(|c| c.id() != NO_CONTROLLER_ID).cloned();
                resp_controller.complete(controller);
                resp_cluster_id.complete(metadata_response.cluster_id().unwrap_or_default().to_string());
                resp_authorized
                    .complete(valid_acl_operations_or_null(metadata_response.cluster_authorized_operations()));
            } else {
                let ConcreteResponse::DescribeCluster(describe_response) = response else {
                    return HandleResult::Retry(KafkaError::illegal_state("Expected a DescribeCluster response"));
                };
                let error = Errors::for_code(describe_response.data().error_code);
                if error != Errors::None {
                    // Mirrors Java's `handleFailure(error.exception(errorMessage))`:
                    // fail all four futures directly rather than retrying.
                    let err = api_error(describe_response.data().error_code, &describe_response.data().error_message);
                    resp_nodes.complete_exceptionally(err.clone());
                    resp_controller.complete_exceptionally(err.clone());
                    resp_cluster_id.complete_exceptionally(err.clone());
                    resp_authorized.complete_exceptionally(err);
                    return HandleResult::Done;
                }
                let nodes = describe_response.nodes();
                let controller_id = describe_response.data().controller_id;
                resp_nodes.complete(nodes.values().cloned().collect());
                // Controller is None if the controller id is NO_CONTROLLER_ID.
                resp_controller.complete(nodes.get(&controller_id).cloned());
                resp_cluster_id.complete(describe_response.data().cluster_id.clone());
                resp_authorized.complete(valid_acl_operations_or_null(
                    describe_response.data().cluster_authorized_operations,
                ));
            }
            HandleResult::Done
        });

        let fail_nodes = nodes_handle.clone();
        let fail_controller = controller_handle.clone();
        let fail_cluster_id = cluster_id_handle.clone();
        let fail_authorized = authorized_ops_handle.clone();
        let handle_failure = Box::new(move |error: &KafkaError| {
            fail_nodes.complete_exceptionally(error.clone());
            fail_controller.complete_exceptionally(error.clone());
            fail_cluster_id.complete_exceptionally(error.clone());
            fail_authorized.complete_exceptionally(error.clone());
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
        public
    }

    fn describe_configs(
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
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let include_synonyms = options.should_include_synonyms();
        let include_documentation = options.should_include_documentation();

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

    fn incremental_alter_configs(
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
                && resource.resource_type() != ConfigResourceType::BrokerLogger
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

    fn list_config_resources(
        &self,
        config_resource_types: &HashSet<ConfigResourceType>,
        options: ListConfigResourcesOptions,
    ) -> ListConfigResourcesResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let handle: KafkaFutureImpl<Vec<ConfigResource>> = KafkaFutureImpl::new();
        let public = handle.future();

        let resource_type_ids: Vec<i8> = config_resource_types.iter().map(ConfigResourceType::id).collect();
        let create_request = Box::new(move |_timeout_ms: i32| {
            let mut data = ListConfigResourcesRequestData::new();
            data.set_resource_types(resource_type_ids.clone());
            Ok(Box::new(ListConfigResourcesRequestBuilder::from_data(data)) as Box<dyn RequestBuilder>)
        });

        let resp_handle = handle.clone();
        let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
            let ConcreteResponse::ListConfigResources(list_response) = response else {
                return HandleResult::Retry(KafkaError::illegal_state("Expected a ListConfigResources response"));
            };
            let error = list_response.error();
            if error != Errors::None {
                resp_handle.complete_exceptionally(KafkaError::new(error));
            } else {
                resp_handle.complete(list_response.config_resources());
            }
            HandleResult::Done
        });

        let fail_handle = handle.clone();
        let handle_failure = Box::new(move |error: &KafkaError| {
            fail_handle.complete_exceptionally(error.clone());
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

    #[allow(deprecated)]
    fn list_client_metrics_resources(
        &self,
        options: ListClientMetricsResourcesOptions,
    ) -> ListClientMetricsResourcesResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let handle: KafkaFutureImpl<Vec<ClientMetricsResourceListing>> = KafkaFutureImpl::new();
        let public = handle.future();

        // Reuse the `ListConfigResources` wire path, filtered to the
        // `CLIENT_METRICS` resource type (mirrors Java's
        // `ListConfigResourcesRequest.Builder` seeded with
        // `List.of(ConfigResource.Type.CLIENT_METRICS.id())`).
        let create_request = Box::new(move |_timeout_ms: i32| {
            let mut data = ListConfigResourcesRequestData::new();
            data.set_resource_types(vec![ConfigResourceType::ClientMetrics.id()]);
            Ok(Box::new(ListConfigResourcesRequestBuilder::from_data(data)) as Box<dyn RequestBuilder>)
        });

        let resp_handle = handle.clone();
        let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
            let ConcreteResponse::ListConfigResources(list_response) = response else {
                return HandleResult::Retry(KafkaError::illegal_state("Expected a ListConfigResources response"));
            };
            let error = list_response.error();
            if error != Errors::None {
                resp_handle.complete_exceptionally(KafkaError::new(error));
            } else {
                let listings: Vec<ClientMetricsResourceListing> = list_response
                    .config_resources()
                    .into_iter()
                    .filter(|resource| resource.resource_type() == ConfigResourceType::ClientMetrics)
                    .map(|resource| ClientMetricsResourceListing::new(resource.name()))
                    .collect();
                resp_handle.complete(listings);
            }
            HandleResult::Done
        });

        let fail_handle = handle.clone();
        let handle_failure = Box::new(move |error: &KafkaError| {
            fail_handle.complete_exceptionally(error.clone());
        });

        let call = Call::new(
            "listClientMetricsResources",
            deadline,
            NodeProvider::LeastLoaded,
            create_request,
            handle_response,
            handle_failure,
            Box::new(|| false),
        );
        self.submit(call);
        ListClientMetricsResourcesResult::new(public)
    }

    fn describe_log_dirs(&self, brokers: &[i32], options: DescribeLogDirsOptions) -> DescribeLogDirsResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);

        let mut public: HashMap<i32, KafkaFuture<HashMap<String, LogDirDescription>>> = HashMap::new();
        for &broker_id in brokers {
            let handle: KafkaFutureImpl<HashMap<String, LogDirDescription>> = KafkaFutureImpl::new();
            public.insert(broker_id, handle.future());
            let call = get_describe_log_dirs_call(broker_id, handle, deadline);
            self.submit(call);
        }

        DescribeLogDirsResult::new(public)
    }

    fn alter_replica_log_dirs(
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
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);

        let public: HashMap<TopicPartitionReplica, KafkaFuture<()>> =
            futures.iter().map(|(k, v)| (k.clone(), v.future())).collect();
        let shared = Arc::new(futures);
        for (broker_id, assignment) in assignment_by_broker {
            let call = get_alter_replica_log_dirs_call(broker_id, assignment, Arc::clone(&shared), deadline);
            self.submit(call);
        }

        AlterReplicaLogDirsResult::new(public)
    }

    fn describe_replica_log_dirs(
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
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);

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

    fn elect_leaders(
        &self,
        election_type: ElectionType,
        partitions: Option<HashSet<TopicPartition>>,
        options: ElectLeadersOptions,
    ) -> ElectLeadersResult {
        let handle: KafkaFutureImpl<HashMap<TopicPartition, Option<KafkaError>>> = KafkaFutureImpl::new();
        let public = handle.future();
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);

        // Preserve the caller's null-means-all semantics: `None` requests
        // election for all partitions.
        let request_partitions: Option<Vec<TopicPartition>> = partitions.map(|set| set.into_iter().collect());

        let req_partitions = request_partitions.clone();
        let create_request = Box::new(move |timeout_ms: i32| {
            Ok(Box::new(ElectLeadersRequestBuilder::new(
                election_type,
                req_partitions.clone(),
                timeout_ms,
            )) as Box<dyn RequestBuilder>)
        });

        let resp_handle = handle.clone();
        let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
            let ConcreteResponse::ElectLeaders(elect_response) = response else {
                return HandleResult::Retry(KafkaError::illegal_state("Expected an ElectLeaders response"));
            };
            let result = ElectLeadersResponse::elect_leaders_result(elect_response.data());
            // For version == 0 the errorCode is 0 which maps to Errors.NONE.
            let error = Errors::for_code(elect_response.data().error_code);
            if error != Errors::None {
                resp_handle.complete_exceptionally(KafkaError::new(error));
                return HandleResult::Done;
            }
            resp_handle.complete(result);
            HandleResult::Done
        });

        let fail_handle = handle.clone();
        let handle_failure = Box::new(move |error: &KafkaError| {
            fail_handle.complete_exceptionally(error.clone());
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

    fn alter_partition_reassignments(
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
                future.complete_exceptionally(KafkaError::with_message(
                    Errors::InvalidTopicException,
                    format!("The given topic name '{topic}' cannot be represented in a request."),
                ));
            } else if partition < 0 {
                future.complete_exceptionally(KafkaError::with_message(
                    Errors::InvalidTopicException,
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
            let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
            let allow_replication_factor_change = options.should_allow_replication_factor_change();
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

    fn list_partition_reassignments(
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
                    handle.complete_exceptionally(KafkaError::with_message(
                        Errors::InvalidTopicException,
                        format!("The given topic name '{}' cannot be represented in a request.", tp.topic()),
                    ));
                } else if tp.partition() < 0 {
                    handle.complete_exceptionally(KafkaError::with_message(
                        Errors::InvalidTopicException,
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
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
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

    fn list_offsets(
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
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
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

    fn list_groups(&self, options: ListGroupsOptions) -> ListGroupsResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let states: Vec<String> = options.group_states().iter().map(GroupState::to_string).collect();
        let types: Vec<String> = options.types().iter().map(GroupType::to_string).collect();
        let protocol_types: HashSet<String> = options.protocol_types().clone();

        let future = self.submit_list_groups("listGroups", deadline, states, types, move |group| {
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

    #[allow(deprecated)]
    fn list_consumer_groups(&self, options: ListConsumerGroupsOptions) -> ListConsumerGroupsResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let states: Vec<String> = options.group_states().iter().map(GroupState::to_string).collect();
        let types: Vec<String> = options.types().iter().map(GroupType::to_string).collect();

        let future = self.submit_list_groups("listConsumerGroups", deadline, states, types, move |group| {
            if group.protocol_type != PROTOCOL_TYPE && !group.protocol_type.is_empty() {
                return None;
            }
            let group_state = if group.group_state.is_empty() {
                None
            } else {
                Some(GroupState::parse(&group.group_state))
            };
            let group_type = if group.group_type.is_empty() {
                None
            } else {
                Some(GroupType::parse(&group.group_type))
            };
            Some((
                group.group_id.clone(),
                ConsumerGroupListing::new(
                    group.group_id.clone(),
                    group_state,
                    group_type,
                    group.protocol_type.is_empty(),
                ),
            ))
        });
        ListConsumerGroupsResult::new(future)
    }

    fn describe_consumer_groups(
        &self,
        group_ids: &[String],
        options: DescribeConsumerGroupsOptions,
    ) -> DescribeConsumerGroupsResult {
        let log_context = LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id));
        let future = DescribeConsumerGroupsHandler::new_future(group_ids);
        let result_map = future.all();
        let handler =
            DescribeConsumerGroupsHandler::new(options.should_include_authorized_operations(), log_context.clone());

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let retry_backoff = self.retry_backoff();
        let driver = AdminApiDriver::new(Box::new(handler), Box::new(future), deadline, retry_backoff, log_context);
        invoke_driver(driver, self.driver_context(), now);

        DescribeConsumerGroupsResult::new(coordinator_keyed_by_id(result_map))
    }

    fn describe_classic_groups(
        &self,
        group_ids: &[String],
        options: DescribeClassicGroupsOptions,
    ) -> DescribeClassicGroupsResult {
        let log_context = LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id));
        let future = DescribeClassicGroupsHandler::new_future(group_ids);
        let result_map = future.all();
        let handler =
            DescribeClassicGroupsHandler::new(options.should_include_authorized_operations(), log_context.clone());

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let retry_backoff = self.retry_backoff();
        let driver = AdminApiDriver::new(Box::new(handler), Box::new(future), deadline, retry_backoff, log_context);
        invoke_driver(driver, self.driver_context(), now);

        DescribeClassicGroupsResult::new(coordinator_keyed_by_id(result_map))
    }

    fn list_consumer_group_offsets(
        &self,
        group_specs: &HashMap<String, ListConsumerGroupOffsetsSpec>,
        options: ListConsumerGroupOffsetsOptions,
    ) -> ListConsumerGroupOffsetsResult {
        let log_context = LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id));
        let group_ids: Vec<String> = group_specs.keys().cloned().collect();
        let future = ListConsumerGroupOffsetsHandler::new_future(&group_ids);
        let result_map = future.all();
        let handler = ListConsumerGroupOffsetsHandler::new(
            group_specs.clone(),
            options.should_require_stable(),
            log_context.clone(),
        );

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let retry_backoff = self.retry_backoff();
        let driver = AdminApiDriver::new(Box::new(handler), Box::new(future), deadline, retry_backoff, log_context);
        invoke_driver(driver, self.driver_context(), now);

        ListConsumerGroupOffsetsResult::new(coordinator_keyed_by_id(result_map))
    }

    fn alter_consumer_group_offsets(
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
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let retry_backoff = self.retry_backoff();
        let driver = AdminApiDriver::new(Box::new(handler), Box::new(future), deadline, retry_backoff, log_context);
        invoke_driver(driver, self.driver_context(), now);

        AlterConsumerGroupOffsetsResult::new(result_map.get(&key).expect("future exists for the group key").clone())
    }

    fn delete_consumer_group_offsets(
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
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let retry_backoff = self.retry_backoff();
        let driver = AdminApiDriver::new(Box::new(handler), Box::new(future), deadline, retry_backoff, log_context);
        invoke_driver(driver, self.driver_context(), now);

        DeleteConsumerGroupOffsetsResult::new(
            result_map.get(&key).expect("future exists for the group key").clone(),
            partitions.clone(),
        )
    }

    fn delete_consumer_groups(
        &self,
        group_ids: &[String],
        options: DeleteConsumerGroupsOptions,
    ) -> DeleteConsumerGroupsResult {
        let log_context = LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id));
        let future = DeleteGroupsHandler::new_future(group_ids);
        let result_map = future.all();
        let handler = DeleteConsumerGroupsHandler::new(log_context.clone());

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let retry_backoff = self.retry_backoff();
        let driver = AdminApiDriver::new(Box::new(handler), Box::new(future), deadline, retry_backoff, log_context);
        invoke_driver(driver, self.driver_context(), now);

        DeleteConsumerGroupsResult::new(coordinator_keyed_by_id(result_map))
    }

    fn remove_members_from_consumer_group(
        &self,
        group_id: &str,
        options: RemoveMembersFromConsumerGroupOptions,
    ) -> RemoveMembersFromConsumerGroupResult {
        let log_context = LogContext::new(format!("[AdminClient clientId={}] ", self.shared.client_id));
        let reason = match options.reason_value() {
            None | Some("") => DEFAULT_LEAVE_GROUP_REASON.to_string(),
            Some(r) => maybe_truncate_reason(r),
        };

        let admin_future = RemoveMembersFromConsumerGroupHandler::new_future(group_id);
        let result_map = admin_future.all();
        let key = CoordinatorKey::by_group_id(group_id);
        let group_future = result_map.get(&key).expect("future exists for the group key").clone();

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
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
            // request's `options.timeout()`.
            let default_api_timeout_ms = self.shared.default_api_timeout_ms;
            let options_timeout = options.timeout();
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
                    admin_future.complete_exceptionally(HashMap::from([(
                        key_for_cb,
                        KafkaError::with_message(
                            error.error(),
                            format!("Encounter exception when trying to get members from group: {group_id_owned}"),
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
                    let leave_now = (ctx.time_provider)();
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

    fn create_acls(&self, acls: &[AclBinding], options: CreateAclsOptions) -> CreateAclsResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);

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
                        future.complete_exceptionally(KafkaError::with_message(
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

    fn describe_acls(&self, filter: &AclBindingFilter, options: DescribeAclsOptions) -> DescribeAclsResult {
        // Short-circuit on an unknown filter, mirroring
        // `KafkaAdminClient.describeAcls`: complete the future exceptionally
        // with InvalidRequestException and enqueue no Call.
        if filter.is_unknown() {
            let handle: KafkaFutureImpl<Vec<AclBinding>> = KafkaFutureImpl::new();
            handle.complete_exceptionally(KafkaError::with_message(
                Errors::InvalidRequest,
                "The AclBindingFilter must not contain UNKNOWN elements.",
            ));
            return DescribeAclsResult::new(handle.future());
        }

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let handle: KafkaFutureImpl<Vec<AclBinding>> = KafkaFutureImpl::new();
        let public = handle.future();
        let call = get_describe_acls_call(filter.clone(), handle, deadline);
        self.submit(call);
        DescribeAclsResult::new(public)
    }

    fn delete_acls(&self, filters: &[AclBindingFilter], options: DeleteAclsOptions) -> DeleteAclsResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);

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

    fn describe_client_quotas(
        &self,
        filter: &ClientQuotaFilter,
        options: DescribeClientQuotasOptions,
    ) -> DescribeClientQuotasResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let handle: KafkaFutureImpl<HashMap<ClientQuotaEntity, HashMap<String, f64>>> = KafkaFutureImpl::new();
        let public = handle.future();
        let call = get_describe_client_quotas_call(filter.clone(), handle, deadline);
        self.submit(call);
        DescribeClientQuotasResult::new(public)
    }

    fn alter_client_quotas(
        &self,
        entries: &[ClientQuotaAlteration],
        options: AlterClientQuotasOptions,
    ) -> AlterClientQuotasResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);

        // Mirrors Java: one future per entity (later entries with the same
        // entity share the single future for that entity).
        let mut handles: HashMap<ClientQuotaEntity, KafkaFutureImpl<()>> = HashMap::new();
        for entry in entries {
            handles.entry(entry.entity().clone()).or_default();
        }
        let public: HashMap<ClientQuotaEntity, KafkaFuture<()>> =
            handles.iter().map(|(k, v)| (k.clone(), v.future())).collect();

        let call =
            get_alter_client_quotas_call(entries.to_vec(), options.is_validate_only(), Arc::new(handles), deadline);
        self.submit(call);
        AlterClientQuotasResult::new(public)
    }

    fn describe_user_scram_credentials(
        &self,
        users: &[String],
        options: DescribeUserScramCredentialsOptions,
    ) -> DescribeUserScramCredentialsResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let handle: KafkaFutureImpl<DescribeUserScramCredentialsResponseData> = KafkaFutureImpl::new();
        let public = handle.future();
        let call = get_describe_user_scram_credentials_call(users.to_vec(), handle, deadline);
        self.submit(call);
        DescribeUserScramCredentialsResult::new(public)
    }

    fn alter_user_scram_credentials(
        &self,
        alterations: &[UserScramCredentialAlteration],
        options: AlterUserScramCredentialsOptions,
    ) -> AlterUserScramCredentialsResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);

        // Mirrors Java: one future per user.
        let mut handles: HashMap<String, KafkaFutureImpl<()>> = HashMap::new();
        for alteration in alterations {
            handles.insert(alteration.user().to_string(), KafkaFutureImpl::new());
        }

        // We track users with an illegal alteration so we can fail all their
        // alterations later; we also pre-build the wire deletions/upsertions for
        // the ones that pass validation. Building an upsertion runs PBKDF2.
        let unknown_scram_mechanism_msg = "Unknown SCRAM mechanism";
        let mut illegal: HashMap<String, KafkaError> = HashMap::new();

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

    fn create_delegation_token(&self, options: CreateDelegationTokenOptions) -> CreateDelegationTokenResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let handle: KafkaFutureImpl<DelegationToken> = KafkaFutureImpl::new();
        let public = handle.future();
        let call = get_create_delegation_token_call(options, handle, deadline);
        self.submit(call);
        CreateDelegationTokenResult::new(public)
    }

    fn renew_delegation_token(&self, hmac: &[u8], options: RenewDelegationTokenOptions) -> RenewDelegationTokenResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let handle: KafkaFutureImpl<i64> = KafkaFutureImpl::new();
        let public = handle.future();
        let call = get_renew_delegation_token_call(hmac.to_vec(), options.get_renew_time_period_ms(), handle, deadline);
        self.submit(call);
        RenewDelegationTokenResult::new(public)
    }

    fn expire_delegation_token(
        &self,
        hmac: &[u8],
        options: ExpireDelegationTokenOptions,
    ) -> ExpireDelegationTokenResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let handle: KafkaFutureImpl<i64> = KafkaFutureImpl::new();
        let public = handle.future();
        let call =
            get_expire_delegation_token_call(hmac.to_vec(), options.get_expiry_time_period_ms(), handle, deadline);
        self.submit(call);
        ExpireDelegationTokenResult::new(public)
    }

    fn describe_delegation_token(&self, options: DescribeDelegationTokenOptions) -> DescribeDelegationTokenResult {
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);
        let handle: KafkaFutureImpl<Vec<DelegationToken>> = KafkaFutureImpl::new();
        let public = handle.future();
        let owners = options.get_owners().map(<[KafkaPrincipal]>::to_vec);
        let call = get_describe_delegation_token_call(owners, handle, deadline);
        self.submit(call);
        DescribeDelegationTokenResult::new(public)
    }

    fn describe_features(&self, options: DescribeFeaturesOptions) -> DescribeFeaturesResult {
        let handle: KafkaFutureImpl<FeatureMetadata> = KafkaFutureImpl::new();
        let public = handle.future();
        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);

        // Mirrors Java: a set nodeId routes to that specific broker via
        // `ConstantNodeIdProvider`, otherwise the request goes to an arbitrary
        // broker or the active controller.
        let node_provider = match options.get_node_id() {
            Some(node_id) => NodeProvider::ConstantNodeId(node_id),
            None => NodeProvider::LeastLoadedBrokerOrActiveKController,
        };

        let create_request =
            Box::new(move |_timeout_ms: i32| Ok(Box::new(ApiVersionsRequestBuilder::new()) as Box<dyn RequestBuilder>));

        let resp_handle = handle.clone();
        let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
            let ConcreteResponse::ApiVersions(api_versions) = response else {
                return HandleResult::Retry(KafkaError::illegal_state("Expected an ApiVersions response"));
            };
            let data = api_versions.data();
            if data.error_code == Errors::None.code() {
                match create_feature_metadata(data) {
                    Ok(metadata) => resp_handle.complete(metadata),
                    Err(e) => resp_handle.complete_exceptionally(e),
                };
            } else {
                resp_handle.complete_exceptionally(KafkaError::new(Errors::for_code(data.error_code)));
            }
            HandleResult::Done
        });

        let fail_handle = handle.clone();
        let handle_failure = Box::new(move |error: &KafkaError| {
            fail_handle.complete_exceptionally(error.clone());
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

    fn update_features(
        &self,
        feature_updates: &HashMap<String, FeatureUpdate>,
        options: UpdateFeaturesOptions,
    ) -> Result<UpdateFeaturesResult, KafkaError> {
        if feature_updates.is_empty() {
            return Err(KafkaError::illegal_argument("Feature updates can not be null or empty."));
        }

        let mut handles: HashMap<String, KafkaFutureImpl<()>> = HashMap::new();
        for feature in feature_updates.keys() {
            if feature.is_empty() {
                return Err(KafkaError::illegal_argument("Provided feature can not be empty."));
            }
            handles.insert(feature.clone(), KafkaFutureImpl::new());
        }
        let handles = Arc::new(handles);
        let public: HashMap<String, KafkaFuture<()>> = handles
            .iter()
            .map(|(feature, handle)| (feature.clone(), handle.future()))
            .collect();

        let now = self.now();
        let deadline = calc_deadline_ms(now, options.timeout(), self.shared.default_api_timeout_ms);

        // Snapshot the updates for the (possibly retried) request builder.
        let updates_for_request: Vec<(String, FeatureUpdate)> = feature_updates
            .iter()
            .map(|(feature, update)| (feature.clone(), *update))
            .collect();
        let validate_only = options.get_validate_only();
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
            Ok(Box::new(UpdateFeaturesRequestBuilder::from_data(data)) as Box<dyn RequestBuilder>)
        });

        let resp_mm = self.shared.metadata_manager.clone();
        let resp_handles = Arc::clone(&handles);
        let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
            let ConcreteResponse::UpdateFeatures(update_response) = response else {
                return HandleResult::Retry(KafkaError::illegal_state("Expected an UpdateFeatures response"));
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
                                        future.complete_exceptionally(api_error(
                                            result.error_code,
                                            &result.error_message,
                                        ));
                                    }
                                },
                            }
                        }
                        // Sanity check: the server should send back a response
                        // for every feature (mirrors completeUnrealizedFutures).
                        for (feature, future) in resp_handles.iter() {
                            if !future.is_done() {
                                future.complete_exceptionally(KafkaError::with_message(
                                    Errors::UnknownServerError,
                                    format!("The controller response did not contain a result for feature {feature}"),
                                ));
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
                    return HandleResult::Retry(KafkaError::new(Errors::NotController));
                },
                _ => {
                    let error = api_error(data.error_code, &data.error_message);
                    for future in resp_handles.values() {
                        future.complete_exceptionally(error.clone());
                    }
                },
            }
            HandleResult::Done
        });

        let fail_handles = Arc::clone(&handles);
        let handle_failure = Box::new(move |error: &KafkaError| {
            for future in fail_handles.values() {
                future.complete_exceptionally(error.clone());
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

    async fn close(&self, timeout: Duration) {
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
        // re-widen the poll budget that `run_once` reads on every iteration.
        //
        // Java also reassigns `newHardShutdownTimeMs = prev` on that branch, but
        // only to feed a debug log, so it has no counterpart here.
        let mut prev = NO_HARD_SHUTDOWN;
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
            let joined = tokio::time::timeout(Duration::from_millis(wait_time_ms as u64), join_handle)
                .await
                .is_ok();
            if !joined {
                // Expired: leave the task running, exactly as Java leaves the
                // I/O thread running after an expired join, and put the handle
                // back so a later `close()` can still join it.
                *self.shared.bg_handle.lock().unwrap() = handle;
            }
        }
    }
}

/// Builds a `describeTopics` (by name) [`Call`] using the Metadata API.
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
            let mut data = crate::metadata_request_data::MetadataRequestData::new();
            data.set_topics(Some(
                crate::common::requests::MetadataRequest::convert_to_metadata_request_topic(&refs),
            ));
            data.set_allow_auto_topic_creation(false);
            data.set_include_topic_authorized_operations(include_authorized_operations);
            Ok(Box::new(MetadataRequestBuilder::from_data(data)) as Box<dyn RequestBuilder>)
        } else {
            Ok(Box::new(MetadataRequestBuilder::all_topics()) as Box<dyn RequestBuilder>)
        }
    });

    let resp_futures = Arc::clone(&futures);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
        let ConcreteResponse::Metadata(metadata_response) = response else {
            return HandleResult::Retry(KafkaError::illegal_state("Expected a Metadata response"));
        };
        let cluster = metadata_response.build_cluster();
        let errors = metadata_response.errors();
        for (topic_name, future) in resp_futures.iter() {
            if let Some(topic_error) = errors.get(topic_name) {
                future.complete_exceptionally(KafkaError::new(*topic_error));
                continue;
            }
            if !cluster.topics().any(|t| t == topic_name.as_str()) {
                future.complete_exceptionally(KafkaError::with_message(
                    Errors::UnknownTopicOrPartition,
                    format!("Topic {topic_name} not found."),
                ));
                continue;
            }
            let topic_id = cluster.topic_id(topic_name);
            let authorized_operations = metadata_response
                .topic_authorized_operations(topic_name)
                .unwrap_or(crate::common::requests::metadata_response::AUTHORIZED_OPERATIONS_OMITTED);
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
    let handle_failure = Box::new(move |error: &KafkaError| {
        for future in fail_futures.values() {
            future.complete_exceptionally(error.clone());
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
        let mut data = crate::metadata_request_data::MetadataRequestData::new();
        data.set_topics(Some(
            crate::common::requests::MetadataRequest::convert_topic_ids_to_metadata_request_topic(&req_ids),
        ));
        data.set_allow_auto_topic_creation(false);
        data.set_include_topic_authorized_operations(include_authorized_operations);
        Ok(Box::new(MetadataRequestBuilder::from_data(data)) as Box<dyn RequestBuilder>)
    });

    let resp_futures = Arc::clone(&futures);
    let handle_response = Box::new(move |response: &ConcreteResponse, _now: i64| {
        let ConcreteResponse::Metadata(metadata_response) = response else {
            return HandleResult::Retry(KafkaError::illegal_state("Expected a Metadata response"));
        };
        let cluster = metadata_response.build_cluster();
        let errors = metadata_response.errors_by_topic_id();
        for (topic_id, future) in resp_futures.iter() {
            let Some(topic_name) = cluster.topic_name(topic_id) else {
                future.complete_exceptionally(KafkaError::with_message(
                    Errors::UnknownTopicId,
                    format!("TopicId {topic_id} not found."),
                ));
                continue;
            };
            let topic_name = topic_name.to_string();
            if let Some(topic_error) = errors.get(topic_id) {
                future.complete_exceptionally(KafkaError::new(*topic_error));
                continue;
            }
            let authorized_operations = metadata_response
                .topic_authorized_operations(&topic_name)
                .unwrap_or(crate::common::requests::metadata_response::AUTHORIZED_OPERATIONS_OMITTED);
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
    let handle_failure = Box::new(move |error: &KafkaError| {
        for future in fail_futures.values() {
            future.complete_exceptionally(error.clone());
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
        time_provider: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> (Self, AdminClientRunnable<C>) {
        let log_context = LogContext::new(format!("[AdminClient clientId={}] ", config.client_id()));
        let metadata_manager = AdminMetadataManager::new(
            config.retry_backoff_ms(),
            config.metadata_max_age_ms(),
            false,
            log_context.clone(),
        );
        metadata_manager.update(cluster, (time_provider)());
        Self::build(client, metadata_manager, config, time_provider, log_context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::{AtomicI64, Ordering};

    use crate::admin::MemberToRemove;
    use crate::admin::internals::admin_client_runnable::AdminClientRunnable;
    use crate::common::Node;
    use crate::common::protocol::Errors;
    use crate::common::requests::metadata_response::{AUTHORIZED_OPERATIONS_OMITTED, PartitionMetadata, TopicMetadata};
    use crate::common::requests::request_test_utils;
    use crate::common::requests::{CreatePartitionsResponse, DeleteRecordsResponse};
    use crate::common::requests::{CreateTopicsResponse, DeleteTopicsResponse};
    use crate::common::{TopicCollection, TopicPartition, Uuid};
    use crate::create_partitions_response_data::{CreatePartitionsResponseData, CreatePartitionsTopicResult};
    use crate::create_topics_response_data::{CreatableTopicResult, CreateTopicsResponseData};
    use crate::delete_records_response_data::{
        DeleteRecordsPartitionResult, DeleteRecordsResponseData, DeleteRecordsTopicResult,
    };
    use crate::delete_topics_response_data::{DeletableTopicResult, DeleteTopicsResponseData};
    use crate::mock_client::MockClient;

    /// A mutable mock clock so retry/backoff tests can advance time.
    struct MockTime {
        now: AtomicI64,
    }

    impl MockTime {
        fn new(initial: i64) -> Arc<Self> {
            Arc::new(Self { now: AtomicI64::new(initial) })
        }
        fn provider(self: &Arc<Self>) -> Arc<dyn Fn() -> i64 + Send + Sync> {
            let t = Arc::clone(self);
            Arc::new(move || t.now.load(Ordering::Acquire))
        }
        fn sleep(&self, ms: i64) {
            self.now.fetch_add(ms, Ordering::AcqRel);
        }
    }

    fn test_config() -> AdminClientConfig {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        // Bound the retry count so failure tests terminate.
        props.insert("retries".to_string(), "2".to_string());
        AdminClientConfig::from_properties(&props).unwrap()
    }

    fn mock_cluster(num_nodes: i32, controller: i32) -> (Cluster, Vec<Node>) {
        let nodes: Vec<Node> = (0..num_nodes)
            .map(|i| Node::new(i, "localhost".to_string(), 9092 + i))
            .collect();
        let controller_node = nodes.iter().find(|n| n.id() == controller).cloned();
        let cluster = Cluster::new(
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
        let time = MockTime::new(1000);
        let (cluster, nodes) = mock_cluster(3, 0);
        let client = MockClient::new(nodes.clone(), time.provider());
        let config = test_config();
        let (admin, runnable) = KafkaAdminClient::create_for_test(client, cluster, &config, time.provider());
        (admin, runnable, time, nodes)
    }

    /// Builds a test environment with extra config properties (e.g. a custom
    /// `default.api.timeout.ms` or `retry.backoff.ms`).
    fn env_with_props(
        extra: &[(&str, &str)],
    ) -> (KafkaAdminClient, AdminClientRunnable<MockClient>, Arc<MockTime>, Vec<Node>) {
        let time = MockTime::new(1000);
        let (cluster, nodes) = mock_cluster(3, 0);
        let client = MockClient::new(nodes.clone(), time.provider());
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        for (k, v) in extra {
            props.insert((*k).to_string(), (*v).to_string());
        }
        let config = AdminClientConfig::from_properties(&props).unwrap();
        let (admin, runnable) = KafkaAdminClient::create_for_test(client, cluster, &config, time.provider());
        (admin, runnable, time, nodes)
    }

    /// Like [`env_with_props`], but with a configurable broker count (mirrors
    /// Java's `mockCluster(numNodes, 0)`). Used by the group-listing broker
    /// enumeration tests that want a single broker.
    fn env_nodes_with_props(
        num_nodes: i32,
        extra: &[(&str, &str)],
    ) -> (KafkaAdminClient, AdminClientRunnable<MockClient>, Arc<MockTime>, Vec<Node>) {
        let time = MockTime::new(1000);
        let (cluster, nodes) = mock_cluster(num_nodes, 0);
        let client = MockClient::new(nodes.clone(), time.provider());
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        for (k, v) in extra {
            props.insert((*k).to_string(), (*v).to_string());
        }
        let config = AdminClientConfig::from_properties(&props).unwrap();
        let (admin, runnable) = KafkaAdminClient::create_for_test(client, cluster, &config, time.provider());
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
        use crate::list_groups_response_data::{ListGroupsResponseData, ListedGroup};
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
            authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        }
    }

    // --- ACLs (createAcls / describeAcls / deleteAcls) -----------------------

    use crate::common::acl::{AccessControlEntry, AccessControlEntryFilter, AclPermissionType};
    use crate::common::protocol::ApiKeys;
    use crate::common::requests::{CreateAclsResponse, DeleteAclsResponse, DescribeAclsResponse};
    use crate::common::resource::{PatternType, ResourcePattern, ResourcePatternFilter, ResourceType};
    use crate::create_acls_response_data::{AclCreationResult, CreateAclsResponseData};
    use crate::delete_acls_response_data::{DeleteAclsFilterResult, DeleteAclsResponseData};
    use crate::describe_acls_response_data::DescribeAclsResponseData;

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
    async fn test_describe_acls() {
        let (admin, mut runnable, _time, _nodes) = env();

        // Test a call where we get back ACL1 and ACL2.
        runnable
            .client_mut()
            .prepare_response(describe_acls_response(&[acl1(), acl2()]));
        let result = admin.describe_acls(&filter1(), DescribeAclsOptions::new());
        pump(&mut runnable, 5).await;
        let mut acls = result.values().get().await.unwrap();
        acls.sort_by(|a, b| a.pattern().name().cmp(b.pattern().name()));
        assert_eq!(acls, vec![acl1(), acl2()]);

        // Test a call where we get back no results.
        runnable.client_mut().prepare_response(describe_acls_response(&[]));
        let result = admin.describe_acls(&filter2(), DescribeAclsOptions::new());
        pump(&mut runnable, 5).await;
        assert!(result.values().get().await.unwrap().is_empty());

        // Test a call where we get back an error.
        runnable
            .client_mut()
            .prepare_response(describe_acls_error_response(Errors::SecurityDisabled, "Security is disabled"));
        let result = admin.describe_acls(&filter2(), DescribeAclsOptions::new());
        pump(&mut runnable, 5).await;
        let err = result.values().get().await.unwrap_err();
        assert_eq!(err.error(), Errors::SecurityDisabled);

        // Test a call where we supply an invalid filter: completes exceptionally
        // with InvalidRequest and enqueues NO network call.
        let before = runnable.client_mut().request_count();
        let result = admin.describe_acls(&unknown_filter(), DescribeAclsOptions::new());
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
    async fn test_create_acls() {
        let (admin, mut runnable, _time, _nodes) = env();

        // Test a call where we successfully create two ACLs.
        runnable
            .client_mut()
            .prepare_response(create_acls_response(vec![create_acls_result_ok(), create_acls_result_ok()]));
        let results = admin.create_acls(&[acl1(), acl2()], CreateAclsOptions::new());
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
        let results = admin.create_acls(&[acl1(), acl2()], CreateAclsOptions::new());
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

        let results = admin.create_acls(&[acl1()], CreateAclsOptions::new());
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
        let results = admin.delete_acls(&[filter1(), filter2()], DeleteAclsOptions::new());
        pump(&mut runnable, 5).await;
        let filter1_results = results.values()[&filter1()].get().await.unwrap();
        assert!(filter1_results.values()[0].exception().is_none());
        assert_eq!(filter1_results.values()[0].binding(), Some(&acl1()));
        assert!(filter1_results.values()[1].exception().is_none());
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
        let results = admin.delete_acls(&[filter1(), filter2()], DeleteAclsOptions::new());
        pump(&mut runnable, 5).await;
        assert!(results.values()[&filter2()].get().await.unwrap().values().is_empty());
        assert_eq!(results.all().get().await.unwrap_err().error(), Errors::SecurityDisabled);

        // Test a call where there are no errors.
        let mut f1 = DeleteAclsFilterResult::new();
        f1.set_matching_acls(vec![DeleteAclsResponse::matching_acl(&acl1(), Errors::None, None)]);
        let mut f2 = DeleteAclsFilterResult::new();
        f2.set_matching_acls(vec![DeleteAclsResponse::matching_acl(&acl2(), Errors::None, None)]);
        runnable.client_mut().prepare_response(delete_acls_response(vec![f1, f2]));
        let results = admin.delete_acls(&[filter1(), filter2()], DeleteAclsOptions::new());
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

        let results = admin.delete_acls(&[filter1()], DeleteAclsOptions::new());
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
        let results = admin.create_acls(&[acl1(), bad.clone()], CreateAclsOptions::new());
        pump(&mut runnable, 5).await;
        results.values()[&acl1()].get().await.unwrap();
        let err = results.values()[&bad].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidRequest);
        assert!(err.message().contains("Invalid ACL creation"));
    }

    // --- client quotas (describeClientQuotas / alterClientQuotas) ------------

    use crate::common::quota::client_quota_entity::{CLIENT_ID, USER};
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
    async fn test_describe_client_quotas() {
        let (admin, mut runnable, _time, _nodes) = env();

        let value = "value";
        let entity1 = new_client_quota_entity(&[USER, "user-1", CLIENT_ID, value]);
        let entity2 = new_client_quota_entity(&[USER, "user-2", CLIENT_ID, value]);
        let mut response_data = HashMap::new();
        response_data.insert(entity1.clone(), HashMap::from([("consumer_byte_rate".to_string(), 10000.0)]));
        response_data.insert(entity2.clone(), HashMap::from([("producer_byte_rate".to_string(), 20000.0)]));

        runnable.client_mut().prepare_response(ConcreteResponse::DescribeClientQuotas(
            DescribeClientQuotasResponse::from_quota_entities(&response_data, 0),
        ));

        let filter = ClientQuotaFilter::contains(vec![ClientQuotaFilterComponent::of_entity(USER, value)]);
        let result = admin.describe_client_quotas(&filter, DescribeClientQuotasOptions::new());
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
    async fn test_alter_client_quotas() {
        let (admin, mut runnable, _time, _nodes) = env();

        let good_entity = new_client_quota_entity(&[USER, "user-1"]);
        let unauthorized_entity = new_client_quota_entity(&[USER, "user-0"]);
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
        let result = admin.alter_client_quotas(&entries, AlterClientQuotasOptions::new());
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

    use crate::admin::{
        AlterUserScramCredentialsOptions, DescribeUserScramCredentialsOptions, ScramCredentialInfo,
        ScramMechanism as PublicScramMechanism, UserScramCredentialAlteration, UserScramCredentialDeletion,
        UserScramCredentialUpsertion,
    };
    use crate::alter_user_scram_credentials_response_data::{
        AlterUserScramCredentialsResponseData, AlterUserScramCredentialsResult as WireAlterResult,
    };
    use crate::common::requests::{AlterUserScramCredentialsResponse, DescribeUserScramCredentialsResponse};
    use crate::describe_user_scram_credentials_response_data::CredentialInfo as WireCredentialInfo;
    use crate::describe_user_scram_credentials_response_data::DescribeUserScramCredentialsResult as WireDescribeResult;

    /// Translated from `KafkaAdminClientTest.testDescribeUserScramCredentials`.
    #[tokio::test]
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

            let result = admin.describe_user_scram_credentials(&users, DescribeUserScramCredentialsOptions::new());
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
            UserScramCredentialUpsertion::new(user1_name, ScramCredentialInfo::new(user1_mechanism, 8192), "password")
                .into(),
            UserScramCredentialUpsertion::new(user2_name, ScramCredentialInfo::new(user2_mechanism, 4096), "password")
                .into(),
        ];
        let result = admin.alter_user_scram_credentials(&alterations, AlterUserScramCredentialsOptions::new());
        pump(&mut runnable, 5).await;

        let result_data = result.values();
        assert_eq!(result_data.len(), 3);
        // user0 and user1 have an unknown mechanism -> complete exceptionally.
        for user in [user0_name, user1_name] {
            assert!(result_data.contains_key(user));
            assert!(
                result_data[user].get().await.is_err(),
                "expected request for user {user} to complete exceptionally"
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
            UserScramCredentialUpsertion::with_password_bytes(
                "user0",
                ScramCredentialInfo::new(PublicScramMechanism::ScramSha256, 4096),
                Vec::new(),
            )
            .into(),
            UserScramCredentialUpsertion::new(
                "user1",
                ScramCredentialInfo::new(PublicScramMechanism::ScramSha512, 8192),
                "password",
            )
            .into(),
        ];
        let result = admin.alter_user_scram_credentials(&alterations, AlterUserScramCredentialsOptions::new());
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
            UserScramCredentialUpsertion::new(user0_name, ScramCredentialInfo::new(user0_mechanism1, 8192), "password")
                .into(),
            UserScramCredentialUpsertion::new(user1_name, ScramCredentialInfo::new(user1_mechanism0, 8192), "password")
                .into(),
            UserScramCredentialDeletion::new(user2_name, user2_mechanism0).into(),
        ];
        let result = admin.alter_user_scram_credentials(&alterations, AlterUserScramCredentialsOptions::new());
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
        let result = admin.create_topics(&[NewTopic::new("myTopic", 1, 1)], CreateTopicsOptions::new());
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
        let result = admin.create_topics(&[NewTopic::new("bad", 1, 1)], CreateTopicsOptions::new());
        runnable.client_mut().prepare_response(create_response(vec![create_result(
            "bad",
            Errors::InvalidTopicException,
            Some("Topic name is invalid"),
        )]));
        pump(&mut runnable, 5).await;
        let err = result.values()["bad"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidTopicException);
        assert_eq!(err.message(), "Topic name is invalid");
    }

    #[tokio::test]
    async fn test_create_topics_partial_response_completes_unrealized() {
        let (admin, mut runnable, _time, _nodes) = env();
        let result = admin.create_topics(
            &[NewTopic::new("present", 1, 1), NewTopic::new("missing", 1, 1)],
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
        let result = admin.create_topics(&[NewTopic::new("myTopic", 1, 1)], CreateTopicsOptions::new());
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
    async fn test_create_topics_retry_backoff() {
        let retry_backoff = 5000;
        let (admin, mut runnable, time, _nodes) = env_with_props(&[("retry.backoff.ms", &retry_backoff.to_string())]);
        let result = admin.create_topics(&[NewTopic::new("myTopic", 1, 1)], CreateTopicsOptions::new());
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
    async fn test_create_topics_handle_not_controller_exception() {
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
            .prepare_response(ConcreteResponse::Metadata(request_test_utils::metadata_response(
                &nodes,
                Some("mock-cluster"),
                1,
                Vec::new(),
            )));
        runnable
            .client_mut()
            .prepare_response(create_response(vec![create_result("myTopic", Errors::None, None)]));
        let result = admin.create_topics(&[NewTopic::new("myTopic", 1, 1)], CreateTopicsOptions::new());
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
    async fn test_create_topics_retry_throttling_exception_when_enabled() {
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

        let result = admin.create_topics(
            &[
                NewTopic::new("topic1", 1, 1),
                NewTopic::new("topic2", 1, 1),
                NewTopic::new("topic3", 1, 1),
            ],
            CreateTopicsOptions::new().retry_on_quota_violation(true),
        );
        pump_until(&mut runnable, 30, |r| r.client_mut().num_awaiting_responses() == 0).await;
        result.values()["topic1"].get().await.unwrap();
        result.values()["topic2"].get().await.unwrap();
        let err = result.values()["topic3"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::TopicAlreadyExists);
    }

    #[tokio::test]
    async fn test_create_topics_dont_retry_throttling_exception_when_disabled() {
        let (admin, mut runnable, _time, _nodes) = env();
        runnable.client_mut().prepare_response(create_response_throttled(
            1000,
            vec![
                create_result("topic1", Errors::None, None),
                create_result("topic2", Errors::ThrottlingQuotaExceeded, None),
                create_result("topic3", Errors::TopicAlreadyExists, None),
            ],
        ));
        let result = admin.create_topics(
            &[
                NewTopic::new("topic1", 1, 1),
                NewTopic::new("topic2", 1, 1),
                NewTopic::new("topic3", 1, 1),
            ],
            CreateTopicsOptions::new().retry_on_quota_violation(false),
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
    async fn test_create_topics_retry_throttling_exception_when_enabled_until_request_timeout() {
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
        let result = admin.create_topics(
            &[
                NewTopic::new("topic1", 1, 1),
                NewTopic::new("topic2", 1, 1),
                NewTopic::new("topic3", 1, 1),
            ],
            CreateTopicsOptions::new().retry_on_quota_violation(true),
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
        let result = admin.delete_topics(
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
        let result = admin.delete_topics(
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
        let result = admin.delete_topics(TopicCollection::of_topic_ids(vec![id]), DeleteTopicsOptions::new());
        let mut r = DeletableTopicResult::new();
        r.set_topic_id(id);
        r.set_error_code(Errors::None.code());
        runnable.client_mut().prepare_response(delete_response(vec![r]));
        pump(&mut runnable, 5).await;
        result.all().get().await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_topics_partial_response() {
        // By name: the response omits "myOtherTopic", so its future is
        // completed by the unrealized-futures sanity check.
        let (admin, mut runnable, _time, _nodes) = env();
        let result = admin.delete_topics(
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
        let result = admin.delete_topics(TopicCollection::of_topic_ids(vec![id1, id2]), DeleteTopicsOptions::new());
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
    async fn test_delete_topics_retry_throttling_exception_when_enabled() {
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
        let result = admin.delete_topics(
            TopicCollection::of_topic_names(vec!["topic1".to_string(), "topic2".to_string(), "topic3".to_string()]),
            DeleteTopicsOptions::new().retry_on_quota_violation(true),
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
        let result = admin.delete_topics(
            TopicCollection::of_topic_ids(vec![id1, id2, id3]),
            DeleteTopicsOptions::new().retry_on_quota_violation(true),
        );
        pump_until(&mut runnable, 30, |r| r.client_mut().num_awaiting_responses() == 0).await;
        result.topic_id_values().unwrap()[&id1].get().await.unwrap();
        result.topic_id_values().unwrap()[&id2].get().await.unwrap();
        let err = result.topic_id_values().unwrap()[&id3].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnknownTopicId);
    }

    #[tokio::test]
    async fn test_delete_topics_dont_retry_throttling_exception_when_disabled() {
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
        let result = admin.delete_topics(
            TopicCollection::of_topic_names(vec!["topic1".to_string(), "topic2".to_string(), "topic3".to_string()]),
            DeleteTopicsOptions::new().retry_on_quota_violation(false),
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
        let result = admin.delete_topics(
            TopicCollection::of_topic_ids(vec![id1, id2, id3]),
            DeleteTopicsOptions::new().retry_on_quota_violation(false),
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
    async fn test_delete_topics_retry_throttling_exception_when_enabled_until_request_timeout() {
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
        let result = admin.delete_topics(
            TopicCollection::of_topic_names(vec!["topic1".to_string(), "topic2".to_string(), "topic3".to_string()]),
            DeleteTopicsOptions::new().retry_on_quota_violation(true),
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
        let result = admin.delete_topics(
            TopicCollection::of_topic_ids(vec![id1, id2, id3]),
            DeleteTopicsOptions::new().retry_on_quota_violation(true),
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

    // --- listTopics ----------------------------------------------------------

    #[tokio::test]
    async fn test_list_topics_filters_internal_by_default() {
        let (admin, mut runnable, _time, nodes) = env();
        let result = admin.list_topics(ListTopicsOptions::new());
        let topics = vec![
            topic_meta("visible", false, Uuid::new(0, 1), 1),
            topic_meta("__consumer_offsets", true, Uuid::new(0, 2), 1),
        ];
        runnable
            .client_mut()
            .prepare_response(ConcreteResponse::Metadata(request_test_utils::metadata_response(
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
        let result = admin.list_topics(ListTopicsOptions::new().list_internal(true));
        let topics = vec![topic_meta("__consumer_offsets", true, Uuid::new(0, 2), 1)];
        runnable
            .client_mut()
            .prepare_response(ConcreteResponse::Metadata(request_test_utils::metadata_response(
                &nodes,
                Some("mock-cluster"),
                0,
                topics,
            )));
        pump(&mut runnable, 5).await;
        let names = result.names().get().await.unwrap();
        assert!(names.contains("__consumer_offsets"));
    }

    // --- describeTopics ------------------------------------------------------

    #[tokio::test]
    async fn test_describe_topics_success() {
        let (admin, mut runnable, _time, nodes) = env();
        let result = admin.describe_topics(
            TopicCollection::of_topic_names(vec!["myTopic".to_string()]),
            DescribeTopicsOptions::new(),
        );
        let topics = vec![topic_meta("myTopic", false, Uuid::new(0, 9), 2)];
        runnable
            .client_mut()
            .prepare_response(ConcreteResponse::Metadata(request_test_utils::metadata_response(
                &nodes,
                Some("mock-cluster"),
                0,
                topics,
            )));
        pump(&mut runnable, 5).await;
        let desc = result.all_topic_names().unwrap().get().await.unwrap();
        let my = &desc["myTopic"];
        assert_eq!(my.name(), "myTopic");
        assert_eq!(my.partitions().len(), 2);
        assert_eq!(my.topic_id(), Uuid::new(0, 9));
    }

    #[tokio::test]
    async fn test_describe_topics_unknown_topic() {
        let (admin, mut runnable, _time, nodes) = env();
        let result = admin.describe_topics(
            TopicCollection::of_topic_names(vec!["nope".to_string()]),
            DescribeTopicsOptions::new(),
        );
        // Response contains a different topic, so "nope" is absent from the cluster.
        let topics = vec![topic_meta("other", false, Uuid::new(0, 3), 1)];
        runnable
            .client_mut()
            .prepare_response(ConcreteResponse::Metadata(request_test_utils::metadata_response(
                &nodes,
                Some("mock-cluster"),
                0,
                topics,
            )));
        pump(&mut runnable, 5).await;
        let err = result.topic_name_values().unwrap()["nope"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnknownTopicOrPartition);
        assert_eq!(err.message(), "Topic nope not found.");
    }

    #[tokio::test]
    async fn test_describe_topics_by_ids() {
        // Valid id: the metadata response carries the topic, so it is described.
        let (admin, mut runnable, _time, nodes) = env();
        let topic_id = Uuid::new(7, 7);
        let topics = vec![topic_meta("test-topic", false, topic_id, 1)];
        runnable
            .client_mut()
            .prepare_response(ConcreteResponse::Metadata(request_test_utils::metadata_response(
                &nodes,
                Some("mock-cluster"),
                0,
                topics,
            )));
        let result = admin.describe_topics(TopicCollection::of_topic_ids(vec![topic_id]), DescribeTopicsOptions::new());
        pump(&mut runnable, 5).await;
        let all = result.all_topic_ids().unwrap().get().await.unwrap();
        assert_eq!(all[&topic_id].name(), "test-topic");

        // Id not present in the brokers: UnknownTopicId with the Java message.
        let (admin, mut runnable, _time, nodes) = env();
        let non_exist = Uuid::new(9, 9);
        runnable
            .client_mut()
            .prepare_response(ConcreteResponse::Metadata(request_test_utils::metadata_response(
                &nodes,
                Some("mock-cluster"),
                0,
                Vec::new(),
            )));
        let result =
            admin.describe_topics(TopicCollection::of_topic_ids(vec![non_exist]), DescribeTopicsOptions::new());
        pump(&mut runnable, 5).await;
        let err = result.all_topic_ids().unwrap().get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnknownTopicId);
        assert_eq!(err.message(), format!("TopicId {non_exist} not found."));

        // The zero id cannot be represented in a request; no request is sent.
        let (admin, _runnable, _time, _nodes) = env();
        let result = admin.describe_topics(
            TopicCollection::of_topic_ids(vec![Uuid::ZERO_UUID]),
            DescribeTopicsOptions::new(),
        );
        let err = result.all_topic_ids().unwrap().get().await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidTopicException);
        assert_eq!(
            err.message(),
            "The given topic id 'AAAAAAAAAAAAAAAAAAAAAA' cannot be represented in a request."
        );
    }

    #[tokio::test]
    async fn test_create_topics_response_config_metadata() {
        use crate::create_topics_response_data::CreatableTopicConfigs;
        let (admin, mut runnable, _time, _nodes) = env();
        let result = admin.create_topics(
            &[NewTopic::new("myTopic", 1, 1)],
            CreateTopicsOptions::new().validate_only(true),
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
        let result = admin.create_topics(&[NewTopic::new("", 1, 1)], CreateTopicsOptions::new());
        let err = result.values()[""].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidTopicException);
        assert_eq!(err.message(), "The given topic name '' cannot be represented in a request.");
    }

    #[tokio::test]
    async fn test_delete_topics_invalid_name_unrepresentable() {
        let (admin, _runnable, _time, _nodes) = env();
        let result =
            admin.delete_topics(TopicCollection::of_topic_names(vec![String::new()]), DeleteTopicsOptions::new());
        let err = result.topic_name_values().unwrap()[""].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidTopicException);
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
            NewPartitions::increase_to_with_assignments(3, vec![vec![2], vec![3]]),
        );
        counts
    }

    /// Mirrors `KafkaAdminClientTest.testCreatePartitions`.
    #[tokio::test]
    async fn test_create_partitions() {
        let (admin, mut runnable, _time, _nodes) = env();
        let result = admin.create_partitions(&new_partitions_counts(), CreatePartitionsOptions::new());
        runnable.client_mut().prepare_response(create_partitions_response(
            1000,
            vec![
                create_partitions_result_item("my_topic", Errors::None, None),
                create_partitions_result_item(
                    "other_topic",
                    Errors::InvalidTopicException,
                    Some("some detailed reason"),
                ),
            ],
        ));
        pump(&mut runnable, 5).await;
        result.values()["my_topic"].get().await.unwrap();
        let err = result.values()["other_topic"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidTopicException);
        assert_eq!(err.message(), "some detailed reason");
    }

    /// Mirrors `KafkaAdminClientTest.testCreatePartitionsRetryThrottlingExceptionWhenEnabled`.
    #[tokio::test]
    async fn test_create_partitions_retry_throttling_exception_when_enabled() {
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
        let result = admin.create_partitions(&counts, CreatePartitionsOptions::new().retry_on_quota_violation(true));

        pump_until(&mut runnable, 30, |r| r.client_mut().num_awaiting_responses() == 0).await;
        result.values()["topic1"].get().await.unwrap();
        result.values()["topic2"].get().await.unwrap();
        let err = result.values()["topic3"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::TopicAlreadyExists);
    }

    /// Mirrors `KafkaAdminClientTest.testCreatePartitionsDontRetryThrottlingExceptionWhenDisabled`.
    #[tokio::test]
    async fn test_create_partitions_dont_retry_throttling_exception_when_disabled() {
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
        let result = admin.create_partitions(&counts, CreatePartitionsOptions::new().retry_on_quota_violation(false));

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
    async fn test_create_partitions_retry_throttling_exception_when_enabled_until_request_timeout() {
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
        let result = admin.create_partitions(&counts, CreatePartitionsOptions::new().retry_on_quota_violation(true));

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
            authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
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
            authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        }
    }

    fn metadata_resp(nodes: &[Node], topics: Vec<TopicMetadata>) -> ConcreteResponse {
        ConcreteResponse::Metadata(request_test_utils::metadata_response(nodes, Some("mock-cluster"), 0, topics))
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
        records.insert(TopicPartition::new("my_topic", 0), RecordsToDelete::before_offset(3));
        records.insert(TopicPartition::new("my_topic", 1), RecordsToDelete::before_offset(10));
        records.insert(TopicPartition::new("my_topic", 2), RecordsToDelete::before_offset(10));
        records.insert(TopicPartition::new("my_topic", 3), RecordsToDelete::before_offset(10));
        let result = admin.delete_records(&records, DeleteRecordsOptions::new());

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
    async fn test_delete_records_topic_authorization_error() {
        let (admin, mut runnable, _time, nodes) = env();
        runnable.client_mut().prepare_response(metadata_resp(
            &nodes,
            vec![topic_meta_error("foo", Errors::TopicAuthorizationFailed)],
        ));

        let mut records = HashMap::new();
        records.insert(TopicPartition::new("foo", 0), RecordsToDelete::before_offset(10));
        let result = admin.delete_records(&records, DeleteRecordsOptions::new());

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
    async fn test_delete_records_multiple_sends() {
        let (admin, mut runnable, _time, nodes) = env();
        // tp0 -> node0, tp1 -> node1.
        runnable
            .client_mut()
            .prepare_response(metadata_resp(&nodes, vec![topic_meta_leaders("foo", &[(0, 0), (1, 1)])]));
        runnable.client_mut().prepare_response_for_node(
            delete_records_resp("foo", vec![delete_records_partition(0, Errors::None, 3)]),
            &nodes[0],
        );
        runnable.client_mut().prepare_response_for_node(
            delete_records_resp("foo", vec![delete_records_partition(1, Errors::TopicAuthorizationFailed, -1)]),
            &nodes[1],
        );

        let mut records = HashMap::new();
        records.insert(TopicPartition::new("foo", 0), RecordsToDelete::before_offset(10));
        records.insert(TopicPartition::new("foo", 1), RecordsToDelete::before_offset(10));
        let result = admin.delete_records(&records, DeleteRecordsOptions::new());

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
        use crate::admin::MockAdminClient;
        let mock = MockAdminClient::create(1);
        let result = mock.delete_records(&HashMap::new(), DeleteRecordsOptions::new());
        assert!(result.low_watermarks().is_empty());
    }

    // --- describeProducers / abortTransaction --------------------------------

    use crate::admin::producer_state::ProducerState;
    use crate::admin::{AbortTransactionOptions, AbortTransactionSpec, DescribeProducersOptions};
    use crate::common::requests::{DescribeProducersResponse, WriteTxnMarkersResponse};
    use crate::describe_producers_response_data::{
        DescribeProducersResponseData, PartitionResponse as DpPartitionResponse, ProducerState as WireProducerState,
        TopicResponse as DpTopicResponse,
    };
    use crate::write_txn_markers_response_data::{
        WritableTxnMarkerPartitionResult, WritableTxnMarkerResult, WritableTxnMarkerTopicResult,
        WriteTxnMarkersResponseData,
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
    async fn test_describe_producers() {
        let (admin, mut runnable, time, nodes) = env();
        let tp = TopicPartition::new("foo", 0);

        // Metadata lookup maps foo-0 to node0 (the leader).
        runnable
            .client_mut()
            .prepare_response(metadata_resp(&nodes, vec![topic_meta_leaders("foo", &[(0, 0)])]));

        let expected = vec![
            ProducerState::new(12345, 15, 30, time.provider()(), Some(99), None),
            ProducerState::new(12345, 15, 30, time.provider()(), None, Some(23423)),
        ];
        runnable
            .client_mut()
            .prepare_response_for_node(build_describe_producers_response(&tp, &expected), &nodes[0]);

        let result = admin.describe_producers(std::slice::from_ref(&tp), DescribeProducersOptions::new());
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

            let options = DescribeProducersOptions::new().timeout_ms(Some(request_timeout_ms));
            let result = admin.describe_producers(std::slice::from_ref(&tp), options);
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
            assert!(matches!(all.get().await.unwrap_err(), KafkaError::Timeout(_)));
        }
    }

    /// Mirrors `KafkaAdminClientTest.testDescribeProducersRetryAfterDisconnect`.
    #[tokio::test]
    async fn test_describe_producers_retry_after_disconnect() {
        let (admin, mut runnable, time, nodes) = env_with_props(&[("retry.backoff.ms", "100")]);
        let tp = TopicPartition::new("foo", 0);

        // Lookup maps to node0; the fulfillment disconnects; a fresh lookup maps
        // to node1; the retried fulfillment succeeds.
        runnable
            .client_mut()
            .prepare_response(metadata_resp(&nodes, vec![topic_meta_leaders("foo", &[(0, 0)])]));

        let expected = vec![
            ProducerState::new(12345, 15, 30, time.provider()(), Some(99), None),
            ProducerState::new(12345, 15, 30, time.provider()(), None, Some(23423)),
        ];
        runnable
            .client_mut()
            .prepare_response_disconnected(build_describe_producers_response(&tp, &expected), true);
        runnable
            .client_mut()
            .prepare_response(metadata_resp(&nodes, vec![topic_meta_leaders("foo", &[(0, 1)])]));
        runnable
            .client_mut()
            .prepare_response_for_node(build_describe_producers_response(&tp, &expected), &nodes[1]);

        let result = admin.describe_producers(std::slice::from_ref(&tp), DescribeProducersOptions::new());
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
    async fn test_abort_transaction() {
        let (admin, mut runnable, _time, nodes) = env();
        let tp = TopicPartition::new("foo", 13);
        let spec = AbortTransactionSpec::new(tp.clone(), 12345, 15, 200);

        runnable
            .client_mut()
            .prepare_response(metadata_resp(&nodes, vec![topic_meta_leaders("foo", &[(13, 0)])]));
        runnable
            .client_mut()
            .prepare_response_for_node(write_txn_markers_response(&spec, Errors::None), &nodes[0]);

        let result = admin.abort_transaction(spec, AbortTransactionOptions::new());
        let all = result.all();
        pump_until(&mut runnable, 40, |_r| all.is_done()).await;
        all.get().await.unwrap();
    }

    /// Mirrors `KafkaAdminClientTest.testAbortTransactionFindLeaderAfterDisconnect`.
    #[tokio::test]
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
            .prepare_response_for_node(write_txn_markers_response(&spec, Errors::None), &nodes[1]);

        let result = admin.abort_transaction(spec, AbortTransactionOptions::new());
        let all = result.all();
        drive_until(&mut runnable, &time, 60, || all.is_done()).await;
        all.get().await.unwrap();
    }

    /// The mock's `describe_producers` mirrors Java's `UnsupportedOperationException`.
    #[tokio::test]
    async fn test_mock_describe_producers_unsupported() {
        use crate::admin::MockAdminClient;
        let mock = MockAdminClient::create(1);
        let tp = TopicPartition::new("foo", 0);
        let result = mock.describe_producers(std::slice::from_ref(&tp), DescribeProducersOptions::new());
        let err = result.partition_result(&tp).unwrap().get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnsupportedVersion);
    }

    /// The mock's `abort_transaction` mirrors Java's `UnsupportedOperationException`.
    #[tokio::test]
    async fn test_mock_abort_transaction_unsupported() {
        use crate::admin::MockAdminClient;
        let mock = MockAdminClient::create(1);
        let spec = AbortTransactionSpec::new(TopicPartition::new("foo", 0), 1, 1, 1);
        let result = mock.abort_transaction(spec, AbortTransactionOptions::new());
        assert_eq!(result.all().get().await.unwrap_err().error(), Errors::UnsupportedVersion);
    }

    // --- describeTransactions / fenceProducers -------------------------------

    use crate::admin::{DescribeTransactionsOptions, FenceProducersOptions, TransactionDescription, TransactionState};
    use crate::common::requests::InitProducerIdResponse;
    use crate::describe_transactions_response_data::{
        DescribeTransactionsResponseData, TransactionState as WireTxnState,
    };
    use crate::init_producer_id_response_data::InitProducerIdResponseData;

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
    async fn test_describe_transactions() {
        let (admin, mut runnable, _time, nodes) = env();
        let transactional_id = "foo";
        let coordinator = &nodes[0];

        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(transactional_id, coordinator)]));
        runnable.client_mut().prepare_response_for_node(
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

        let result = admin.describe_transactions(&["foo".to_string()], DescribeTransactionsOptions::new());
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
    async fn test_retry_describe_transactions_after_not_coordinator_error() {
        let (admin, mut runnable, time, nodes) = env_with_props(&[("retry.backoff.ms", "100")]);
        let transactional_id = "foo";
        let coordinator1 = &nodes[0];
        let coordinator2 = &nodes[1];

        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(transactional_id, coordinator1)]));
        runnable.client_mut().prepare_response_for_node(
            describe_transactions_resp(vec![describe_txn_error_state(transactional_id, Errors::NotCoordinator)]),
            coordinator1,
        );
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(transactional_id, coordinator2)]));
        runnable.client_mut().prepare_response_for_node(
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

        let result = admin.describe_transactions(&["foo".to_string()], DescribeTransactionsOptions::new());
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

    /// Mirrors `KafkaAdminClientTest.testFenceProducers`.
    #[tokio::test]
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
            .prepare_response_for_node(init_producer_id_resp(Errors::CoordinatorLoadInProgress, 0, 0), coordinator);
        runnable
            .client_mut()
            .prepare_response_for_node(init_producer_id_resp(Errors::NotCoordinator, 0, 0), coordinator);
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(transactional_id, coordinator)]));
        runnable
            .client_mut()
            .prepare_response_for_node(init_producer_id_resp(Errors::None, 4761, 489), coordinator);

        let result = admin.fence_producers(&["copyCat".to_string()], FenceProducersOptions::new());
        let all = result.all();
        drive_until(&mut runnable, &time, 80, || all.is_done()).await;
        all.get().await.unwrap();
        assert_eq!(result.producer_id(transactional_id).unwrap().get().await.unwrap(), 4761);
        assert_eq!(result.epoch_id(transactional_id).unwrap().get().await.unwrap(), 489);
    }

    /// The mock's `describe_transactions` mirrors Java's `UnsupportedOperationException`.
    #[tokio::test]
    async fn test_mock_describe_transactions_unsupported() {
        use crate::admin::MockAdminClient;
        let mock = MockAdminClient::create(1);
        let result = mock.describe_transactions(&["t".to_string()], DescribeTransactionsOptions::new());
        assert_eq!(
            result.description("t").unwrap().get().await.unwrap_err().error(),
            Errors::UnsupportedVersion
        );
    }

    /// The mock's `fence_producers` mirrors Java's `UnsupportedOperationException`.
    #[tokio::test]
    async fn test_mock_fence_producers_unsupported() {
        use crate::admin::MockAdminClient;
        let mock = MockAdminClient::create(1);
        let result = mock.fence_producers(&["t".to_string()], FenceProducersOptions::new());
        assert_eq!(result.all().get().await.unwrap_err().error(), Errors::UnsupportedVersion);
    }

    // --- listTransactions / forceTerminateTransaction ------------------------

    use crate::admin::{ListTransactionsOptions, TerminateTransactionOptions, TransactionListing};
    use crate::list_transactions_response_data::{ListTransactionsResponseData, TransactionState as WireListTxnState};

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
                .prepare_response_for_node(list_transactions_resp(&expected[node.id() as usize]), node);
        }

        let result = admin.list_transactions(ListTransactionsOptions::new());
        let all = result.all();
        pump_until(&mut runnable, 60, |_r| all.is_done()).await;
        assert_eq!(
            all.get().await.unwrap().into_iter().collect::<HashSet<_>>(),
            expected.into_iter().collect::<HashSet<_>>()
        );
    }

    /// Mirrors `KafkaAdminClientTest.testForceTerminateTransaction`.
    #[tokio::test]
    async fn test_force_terminate_transaction() {
        let (admin, mut runnable, _time, nodes) = env();
        let transactional_id = "testForceTerminate";
        let coordinator = &nodes[0];
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(transactional_id, coordinator)]));
        runnable
            .client_mut()
            .prepare_response_for_node(init_producer_id_resp(Errors::None, 5678, 123), coordinator);

        let result = admin.force_terminate_transaction(transactional_id, TerminateTransactionOptions::new());
        let future = result.result();
        pump_until(&mut runnable, 40, |_r| future.is_done()).await;
        future.get().await.unwrap();
    }

    /// Mirrors `KafkaAdminClientTest.testForceTerminateTransactionWithError`.
    #[tokio::test]
    async fn test_force_terminate_transaction_with_error() {
        let (admin, mut runnable, _time, nodes) = env();
        let transactional_id = "testForceTerminateError";
        let coordinator = &nodes[0];
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(transactional_id, coordinator)]));
        runnable.client_mut().prepare_response_for_node(
            init_producer_id_resp(Errors::TransactionalIdAuthorizationFailed, 0, 0),
            coordinator,
        );

        let result = admin.force_terminate_transaction(transactional_id, TerminateTransactionOptions::new());
        let future = result.result();
        pump_until(&mut runnable, 40, |_r| future.is_done()).await;
        assert_eq!(
            future.get().await.unwrap_err().error(),
            Errors::TransactionalIdAuthorizationFailed
        );
    }

    /// Mirrors `KafkaAdminClientTest.testForceTerminateTransactionWithCustomTimeout`.
    #[tokio::test]
    async fn test_force_terminate_transaction_with_custom_timeout() {
        let (admin, mut runnable, _time, nodes) = env();
        let transactional_id = "testForceTerminateTimeout";
        let coordinator = &nodes[0];
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(transactional_id, coordinator)]));
        runnable
            .client_mut()
            .prepare_response_for_node(init_producer_id_resp(Errors::None, 9012, 456), coordinator);

        let options = TerminateTransactionOptions::new().timeout_ms(Some(10000));
        let result = admin.force_terminate_transaction(transactional_id, options);
        let future = result.result();
        pump_until(&mut runnable, 40, |_r| future.is_done()).await;
        future.get().await.unwrap();
    }

    /// The mock's `list_transactions` mirrors Java's `UnsupportedOperationException`.
    #[tokio::test]
    async fn test_mock_list_transactions_unsupported() {
        use crate::admin::MockAdminClient;
        let mock = MockAdminClient::create(1);
        let result = mock.list_transactions(ListTransactionsOptions::new());
        assert_eq!(result.all().get().await.unwrap_err().error(), Errors::UnsupportedVersion);
    }

    /// The mock's `force_terminate_transaction` mirrors Java's `UnsupportedOperationException`.
    #[tokio::test]
    async fn test_mock_force_terminate_transaction_unsupported() {
        use crate::admin::MockAdminClient;
        let mock = MockAdminClient::create(1);
        let result = mock.force_terminate_transaction("t", TerminateTransactionOptions::new());
        assert_eq!(result.result().get().await.unwrap_err().error(), Errors::UnsupportedVersion);
    }

    // --- describeCluster -----------------------------------------------------

    use crate::admin::{
        AlterConfigOp, AlterConfigsOptions, DescribeClusterOptions, DescribeConfigsOptions, ListConfigResourcesOptions,
        OpType,
    };
    use crate::common::acl::AclOperation;
    use crate::common::config::{ConfigResource, ConfigResourceType};
    use crate::common::requests::{
        DescribeClusterResponse, DescribeConfigsResponse, IncrementalAlterConfigsResponse, ListConfigResourcesResponse,
    };
    use crate::describe_cluster_response_data::{DescribeClusterBroker, DescribeClusterResponseData};
    use crate::describe_configs_response_data::{
        DescribeConfigsResponseData, DescribeConfigsResult as WireDescribeConfigsResult,
    };
    use crate::incremental_alter_configs_response_data::{
        AlterConfigsResourceResponse, IncrementalAlterConfigsResponseData,
    };
    use crate::list_config_resources_response_data::{
        ConfigResource as WireConfigResource, ListConfigResourcesResponseData,
    };

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
    async fn test_describe_cluster() {
        let (admin, mut runnable, _time, nodes) = env();
        let cluster_id = "mock-cluster";

        // First call: authorized operations omitted, controller id 2.
        runnable.client_mut().prepare_response(describe_cluster_response(
            2,
            &nodes,
            cluster_id,
            AUTHORIZED_OPERATIONS_OMITTED,
        ));
        let result = admin.describe_cluster(DescribeClusterOptions::new());
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
        let result2 = admin.describe_cluster(DescribeClusterOptions::new());
        pump(&mut runnable, 5).await;
        assert_eq!(result2.controller().get().await.unwrap().unwrap().id(), 1);
        let expected: BTreeSet<AclOperation> = [AclOperation::Describe, AclOperation::Alter].into_iter().collect();
        assert_eq!(result2.authorized_operations().get().await.unwrap(), Some(expected));
    }

    #[tokio::test]
    async fn test_describe_cluster_handle_error() {
        let (admin, mut runnable, _time, _nodes) = env();
        let error_message = "my error";
        let mut data = DescribeClusterResponseData::new();
        data.set_error_code(Errors::InvalidRequest.code());
        data.set_error_message(Some(error_message.to_string()));
        runnable
            .client_mut()
            .prepare_response(ConcreteResponse::DescribeCluster(DescribeClusterResponse::new(data)));

        let result = admin.describe_cluster(DescribeClusterOptions::new());
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
    async fn test_describe_cluster_fail_back() {
        let (admin, mut runnable, _time, nodes) = env();
        let cluster_id = "mock-cluster";
        // Reject the DescribeCluster request with an unsupported version, then
        // answer the Metadata fallback.
        runnable.client_mut().prepare_unsupported_version_response();
        runnable
            .client_mut()
            .prepare_response(ConcreteResponse::Metadata(request_test_utils::metadata_response(
                &nodes,
                Some(cluster_id),
                2,
                Vec::new(),
            )));

        let result = admin.describe_cluster(DescribeClusterOptions::new());
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
        let result = admin.describe_cluster(DescribeClusterOptions::new().include_fenced_brokers(true));
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
    async fn test_describe_broker_configs() {
        let (admin, mut runnable, _time, nodes) = env();
        let broker0 = ConfigResource::new(ConfigResourceType::Broker, "0".to_string());
        let broker1 = ConfigResource::new(ConfigResourceType::Broker, "1".to_string());
        runnable.client_mut().prepare_response_for_node(
            describe_configs_response(vec![describe_configs_result(
                "0",
                ConfigResourceType::Broker.id(),
                Errors::None,
            )]),
            &nodes[0],
        );
        runnable.client_mut().prepare_response_for_node(
            describe_configs_response(vec![describe_configs_result(
                "1",
                ConfigResourceType::Broker.id(),
                Errors::None,
            )]),
            &nodes[1],
        );
        let result = admin.describe_configs(&[broker0.clone(), broker1.clone()], DescribeConfigsOptions::new());
        pump(&mut runnable, 8).await;
        let keys: HashSet<ConfigResource> = result.values().keys().cloned().collect();
        assert_eq!(keys, [broker0.clone(), broker1.clone()].into_iter().collect());
        result.values().get(&broker0).unwrap().get().await.unwrap();
        result.values().get(&broker1).unwrap().get().await.unwrap();
    }

    #[tokio::test]
    async fn test_describe_broker_and_log_configs() {
        let (admin, mut runnable, _time, nodes) = env();
        let broker = ConfigResource::new(ConfigResourceType::Broker, "0".to_string());
        let broker_logger = ConfigResource::new(ConfigResourceType::BrokerLogger, "0".to_string());
        // Both broker and broker-logger resources for node 0 go to node 0 in one
        // request.
        runnable.client_mut().prepare_response_for_node(
            describe_configs_response(vec![
                describe_configs_result("0", ConfigResourceType::Broker.id(), Errors::None),
                describe_configs_result("0", ConfigResourceType::BrokerLogger.id(), Errors::None),
            ]),
            &nodes[0],
        );
        let result = admin.describe_configs(&[broker.clone(), broker_logger.clone()], DescribeConfigsOptions::new());
        pump(&mut runnable, 8).await;
        let keys: HashSet<ConfigResource> = result.values().keys().cloned().collect();
        assert_eq!(keys, [broker.clone(), broker_logger.clone()].into_iter().collect());
        result.values().get(&broker).unwrap().get().await.unwrap();
        result.values().get(&broker_logger).unwrap().get().await.unwrap();
    }

    #[tokio::test]
    async fn test_describe_configs_partial_response() {
        let (admin, mut runnable, _time, _nodes) = env();
        let topic = ConfigResource::new(ConfigResourceType::Topic, "topic".to_string());
        let topic2 = ConfigResource::new(ConfigResourceType::Topic, "topic2".to_string());
        // The (single, least-loaded) response only contains `topic`.
        runnable
            .client_mut()
            .prepare_response(describe_configs_response(vec![describe_configs_result(
                "topic",
                ConfigResourceType::Topic.id(),
                Errors::None,
            )]));
        let result = admin.describe_configs(&[topic.clone(), topic2.clone()], DescribeConfigsOptions::new());
        pump(&mut runnable, 8).await;
        let keys: HashSet<ConfigResource> = result.values().keys().cloned().collect();
        assert_eq!(keys, [topic.clone(), topic2.clone()].into_iter().collect());
        result.values().get(&topic).unwrap().get().await.unwrap();
        assert!(result.values().get(&topic2).unwrap().get().await.is_err());
    }

    #[tokio::test]
    async fn test_describe_configs_unrequested() {
        let (admin, mut runnable, _time, _nodes) = env();
        let topic = ConfigResource::new(ConfigResourceType::Topic, "topic".to_string());
        // Response contains an extra, unrequested resource; it is ignored.
        runnable.client_mut().prepare_response(describe_configs_response(vec![
            describe_configs_result("topic", ConfigResourceType::Topic.id(), Errors::None),
            describe_configs_result("unrequested", ConfigResourceType::Topic.id(), Errors::None),
        ]));
        let result = admin.describe_configs(std::slice::from_ref(&topic), DescribeConfigsOptions::new());
        pump(&mut runnable, 8).await;
        let keys: HashSet<ConfigResource> = result.values().keys().cloned().collect();
        assert_eq!(keys, [topic.clone()].into_iter().collect());
        result.values().get(&topic).unwrap().get().await.unwrap();
    }

    #[tokio::test]
    async fn test_describe_client_metrics_configs() {
        let (admin, mut runnable, _time, _nodes) = env();
        let sub1 = ConfigResource::new(ConfigResourceType::ClientMetrics, "sub1".to_string());
        let sub2 = ConfigResource::new(ConfigResourceType::ClientMetrics, "sub2".to_string());
        runnable.client_mut().prepare_response(describe_configs_response(vec![
            describe_configs_result("sub1", ConfigResourceType::ClientMetrics.id(), Errors::None),
            describe_configs_result("sub2", ConfigResourceType::ClientMetrics.id(), Errors::None),
        ]));
        let result = admin.describe_configs(&[sub1.clone(), sub2.clone()], DescribeConfigsOptions::new());
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
    async fn test_incremental_alter_configs() {
        let (admin, mut runnable, _time, _nodes) = env();

        let broker_resource = ConfigResource::new(ConfigResourceType::Broker, String::new());
        let topic_resource = ConfigResource::new(ConfigResourceType::Topic, "topic1".to_string());
        let metric_resource = ConfigResource::new(ConfigResourceType::ClientMetrics, "metric1".to_string());
        let group_resource = ConfigResource::new(ConfigResourceType::Group, "group1".to_string());

        // Error scenario: all four resources are least-loaded-routed (default
        // broker, topic, client-metrics, group all have node_for == None), so a
        // single request fails per-resource.
        runnable.client_mut().prepare_response(incremental_alter_configs_response(vec![
            alter_configs_resource_response(
                "",
                ConfigResourceType::Broker.id(),
                Errors::ClusterAuthorizationFailed,
                "authorization error",
            ),
            alter_configs_resource_response(
                "metric1",
                ConfigResourceType::ClientMetrics.id(),
                Errors::InvalidRequest,
                "Subscription is not allowed",
            ),
            alter_configs_resource_response(
                "topic1",
                ConfigResourceType::Topic.id(),
                Errors::InvalidRequest,
                "Config value append is not allowed for config",
            ),
            alter_configs_resource_response(
                "group1",
                ConfigResourceType::Group.id(),
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

        let result = admin.incremental_alter_configs(&configs, AlterConfigsOptions::new());
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
            alter_configs_resource_response("", ConfigResourceType::Broker.id(), Errors::None, ""),
            alter_configs_resource_response("metric1", ConfigResourceType::ClientMetrics.id(), Errors::None, ""),
            alter_configs_resource_response("group1", ConfigResourceType::Group.id(), Errors::None, ""),
        ]));
        let mut success = HashMap::new();
        success.insert(broker_resource, vec![op1]);
        success.insert(metric_resource, vec![op3]);
        success.insert(group_resource, vec![op4]);
        let result = admin.incremental_alter_configs(&success, AlterConfigsOptions::new());
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
    async fn test_list_config_resources() {
        let (admin, mut runnable, _time, _nodes) = env();
        let expected = [
            ("client-metrics", ConfigResourceType::ClientMetrics.id()),
            ("1", ConfigResourceType::Broker.id()),
            ("1", ConfigResourceType::BrokerLogger.id()),
            ("topic", ConfigResourceType::Topic.id()),
            ("group", ConfigResourceType::Group.id()),
        ];
        runnable
            .client_mut()
            .prepare_response(list_config_resources_response(Errors::None, &expected));
        let result = admin.list_config_resources(&HashSet::new(), ListConfigResourcesOptions::new());
        pump(&mut runnable, 5).await;
        let listed = result.all().get().await.unwrap();
        assert_eq!(listed.len(), expected.len());
        let expected_set: HashSet<ConfigResource> = expected
            .iter()
            .map(|(name, type_id)| ConfigResource::new(ConfigResourceType::for_id(*type_id), (*name).to_string()))
            .collect();
        assert_eq!(listed.into_iter().collect::<HashSet<_>>(), expected_set);
    }

    #[tokio::test]
    async fn test_list_config_resources_empty() {
        let (admin, mut runnable, _time, _nodes) = env();
        runnable
            .client_mut()
            .prepare_response(list_config_resources_response(Errors::None, &[]));
        let result = admin.list_config_resources(&HashSet::new(), ListConfigResourcesOptions::new());
        pump(&mut runnable, 5).await;
        assert!(result.all().get().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_list_config_resources_not_supported() {
        let (admin, mut runnable, _time, _nodes) = env();
        runnable
            .client_mut()
            .prepare_response(list_config_resources_response(Errors::UnsupportedVersion, &[]));
        let mut types = HashSet::new();
        types.insert(ConfigResourceType::Unknown);
        let result = admin.list_config_resources(&types, ListConfigResourcesOptions::new());
        pump(&mut runnable, 5).await;
        let err = result.all().get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnsupportedVersion);
    }

    // --- listClientMetricsResources ------------------------------------------

    /// Translated from `KafkaAdminClientTest.testListClientMetricsResources`.
    #[tokio::test]
    #[allow(deprecated)]
    async fn test_list_client_metrics_resources() {
        use crate::admin::{ClientMetricsResourceListing, ListClientMetricsResourcesOptions};
        let (admin, mut runnable, _time, _nodes) = env();
        let client_metrics_id = ConfigResourceType::ClientMetrics.id();
        let expected: HashSet<ClientMetricsResourceListing> = [
            ClientMetricsResourceListing::new("one"),
            ClientMetricsResourceListing::new("two"),
        ]
        .into_iter()
        .collect();
        runnable.client_mut().prepare_response(list_config_resources_response(
            Errors::None,
            &[("one", client_metrics_id), ("two", client_metrics_id)],
        ));
        let result = admin.list_client_metrics_resources(ListClientMetricsResourcesOptions::new());
        pump(&mut runnable, 5).await;
        let listed = result.all().get().await.unwrap();
        assert_eq!(listed.into_iter().collect::<HashSet<_>>(), expected);
    }

    /// Translated from `KafkaAdminClientTest.testListClientMetricsResourcesEmpty`.
    #[tokio::test]
    #[allow(deprecated)]
    async fn test_list_client_metrics_resources_empty() {
        use crate::admin::ListClientMetricsResourcesOptions;
        let (admin, mut runnable, _time, _nodes) = env();
        runnable
            .client_mut()
            .prepare_response(list_config_resources_response(Errors::None, &[]));
        let result = admin.list_client_metrics_resources(ListClientMetricsResourcesOptions::new());
        pump(&mut runnable, 5).await;
        assert!(result.all().get().await.unwrap().is_empty());
    }

    /// Translated from `KafkaAdminClientTest.testListClientMetricsResourcesNotSupported`.
    #[tokio::test]
    #[allow(deprecated)]
    async fn test_list_client_metrics_resources_not_supported() {
        use crate::admin::ListClientMetricsResourcesOptions;
        let (admin, mut runnable, _time, _nodes) = env();
        runnable
            .client_mut()
            .prepare_response(list_config_resources_response(Errors::UnsupportedVersion, &[]));
        let result = admin.list_client_metrics_resources(ListClientMetricsResourcesOptions::new());
        pump(&mut runnable, 5).await;
        let err = result.all().get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnsupportedVersion);
        assert_eq!(err.message(), "The version of API is not supported.");
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
        let drv_ctx = DriverContext { tx, wakeup: Arc::new(Notify::new()), time_provider: Arc::new(move || now) };
        let mut call = new_driver_call(Arc::clone(&driver), spec, drv_ctx);

        let outcome = call.maybe_retry(&KafkaError::new(Errors::NetworkException), now);
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
        let drv_ctx = DriverContext { tx, wakeup: Arc::new(Notify::new()), time_provider: Arc::new(move || now) };
        let mut call = new_driver_call(Arc::clone(&driver), spec, drv_ctx);

        let outcome = call.maybe_retry(&KafkaError::new(Errors::UnknownServerError), now);
        assert!(matches!(outcome, MaybeRetryOutcome::Requeue));
        assert_eq!(driver.lock().unwrap().key_to_broker_id(&"foo".to_string()), Some(0));
        assert!(rx.try_recv().is_err());
    }

    // --- describeLogDirs / alterReplicaLogDirs / describeReplicaLogDirs -------

    use crate::admin::{
        AlterReplicaLogDirsOptions, DescribeLogDirsOptions, DescribeReplicaLogDirsOptions, MockAdminClient,
    };
    use crate::alter_replica_log_dirs_response_data::{
        AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult, AlterReplicaLogDirsResponseData,
    };
    use crate::common::TopicPartitionReplica;
    use crate::common::requests::AlterReplicaLogDirsResponse;
    use crate::describe_log_dirs_response_data::{
        DescribeLogDirsPartition, DescribeLogDirsResponseData, DescribeLogDirsResult as WireDescribeLogDirsResult,
        DescribeLogDirsTopic,
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
    async fn test_describe_log_dirs() {
        let log_dir = "/var/data/kafka";
        let tp = TopicPartition::new("topic", 12);
        let partition_size = 1234567890;
        let offset_lag = 24;
        let (admin, mut runnable, _time, nodes) = env();

        runnable.client_mut().prepare_response_for_node(
            describe_log_dirs_single(Errors::None, log_dir, &tp, partition_size, offset_lag),
            &nodes[0],
        );
        let result = admin.describe_log_dirs(&[0], DescribeLogDirsOptions::new());
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
            .prepare_response_for_node(empty_describe_log_dirs_response(None), &nodes[0]);
        let error_result = admin.describe_log_dirs(&[0], DescribeLogDirsOptions::new());
        pump_until(&mut runnable, 10, |_r| error_result.descriptions()[&0].is_done()).await;
        let err = error_result.all_descriptions().get().await.unwrap_err();
        assert_eq!(err.error(), Errors::ClusterAuthorizationFailed);

        // Empty results with an error with version >= 3.
        runnable
            .client_mut()
            .prepare_response_for_node(empty_describe_log_dirs_response(Some(Errors::UnknownServerError)), &nodes[0]);
        let error_result2 = admin.describe_log_dirs(&[0], DescribeLogDirsOptions::new());
        pump_until(&mut runnable, 10, |_r| error_result2.descriptions()[&0].is_done()).await;
        let err2 = error_result2.all_descriptions().get().await.unwrap_err();
        assert_eq!(err2.error(), Errors::UnknownServerError);
    }

    #[allow(clippy::too_many_arguments)]
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
    }

    /// Mirrors `KafkaAdminClientTest.testDescribeLogDirsWithVolumeBytes`.
    #[tokio::test]
    async fn test_describe_log_dirs_with_volume_bytes() {
        let log_dir = "/var/data/kafka";
        let tp = TopicPartition::new("topic", 12);
        let partition_size = 1234567890;
        let offset_lag = 24;
        let total_bytes = 123;
        let usable_bytes = 456;
        let (admin, mut runnable, _time, nodes) = env();

        runnable.client_mut().prepare_response_for_node(
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
        let result = admin.describe_log_dirs(&[0], DescribeLogDirsOptions::new());
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

    /// Mirrors `KafkaAdminClientTest.testDescribeLogDirsOfflineDir`.
    #[tokio::test]
    async fn test_describe_log_dirs_offline_dir() {
        let log_dir = "/var/data/kafka";
        let (admin, mut runnable, _time, nodes) = env();
        runnable.client_mut().prepare_response_for_node(
            describe_log_dirs_response(vec![describe_log_dirs_result(Errors::KafkaStorageError, log_dir, Vec::new())]),
            &nodes[0],
        );
        let result = admin.describe_log_dirs(&[0], DescribeLogDirsOptions::new());
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
    async fn test_describe_log_dirs_partial_failure() {
        let default_api_timeout: i64 = 60000;
        let (admin, mut runnable, time, nodes) = env_with_props(&[
            ("default.api.timeout.ms", &default_api_timeout.to_string()),
            ("retries", "0"),
        ]);
        // Provide only node 1's response.
        runnable.client_mut().prepare_response_for_node(
            describe_log_dirs_response(vec![describe_log_dirs_result(Errors::None, "/data", Vec::new())]),
            &nodes[1],
        );
        let result = admin.describe_log_dirs(&[0, 1], DescribeLogDirsOptions::new());
        pump_until(&mut runnable, 30, |r| !r.client_mut().has_pending_responses()).await;
        time.sleep(default_api_timeout + 1);
        pump_until(&mut runnable, 30, |_r| {
            result.descriptions()[&0].is_done() && result.descriptions()[&1].is_done()
        })
        .await;
        assert!(matches!(
            result.descriptions()[&0].get().await.unwrap_err(),
            KafkaError::Timeout(_)
        ));
        assert!(result.descriptions()[&1].get().await.is_ok());
    }

    /// Mirrors `KafkaAdminClientTest.testDescribeReplicaLogDirs`.
    #[tokio::test]
    async fn test_describe_replica_log_dirs() {
        let tpr1 = TopicPartitionReplica::new("topic", 12, 1);
        let tpr2 = TopicPartitionReplica::new("topic", 12, 2);
        let (admin, mut runnable, _time, nodes) = env();

        let broker1log0 = "/var/data/kafka0";
        let broker1log1 = "/var/data/kafka1";
        let broker2log0 = "/var/data/kafka2";
        runnable.client_mut().prepare_response_for_node(
            describe_log_dirs_response(vec![
                replica_describe_log_dirs_result(&tpr1, broker1log0, 987654321, 24, false),
                replica_describe_log_dirs_result(&tpr1, broker1log1, 123456789, 4321, true),
            ]),
            &nodes[1],
        );
        runnable.client_mut().prepare_response_for_node(
            describe_log_dirs_response(vec![describe_log_dirs_result(
                Errors::KafkaStorageError,
                broker2log0,
                Vec::new(),
            )]),
            &nodes[2],
        );

        let result =
            admin.describe_replica_log_dirs(&[tpr1.clone(), tpr2.clone()], DescribeReplicaLogDirsOptions::new());
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
    async fn test_describe_replica_log_dirs_unexpected() {
        let expected = TopicPartitionReplica::new("topic", 12, 1);
        let unexpected = TopicPartitionReplica::new("topic", 12, 2);
        let (admin, mut runnable, _time, nodes) = env();

        let broker1log0 = "/var/data/kafka0";
        let broker1log1 = "/var/data/kafka1";
        runnable.client_mut().prepare_response_for_node(
            describe_log_dirs_response(vec![
                replica_describe_log_dirs_result(&expected, broker1log0, 987654321, 24, false),
                replica_describe_log_dirs_result(&unexpected, broker1log1, 123456789, 4321, true),
            ]),
            &nodes[1],
        );

        let result =
            admin.describe_replica_log_dirs(std::slice::from_ref(&expected), DescribeReplicaLogDirsOptions::new());
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
    async fn test_describe_replica_log_dirs_with_non_exist_replica() {
        let broker_id = 0;
        let tpr1 = TopicPartitionReplica::new("topic1", 12, broker_id);
        let tpr2 = TopicPartitionReplica::new("topic2", 12, broker_id);
        let (admin, mut runnable, _time, nodes) = env();

        let log_dir = "/var/data/kafka0";
        let offset_lag = 1;
        runnable.client_mut().prepare_response_for_node(
            describe_log_dirs_response(vec![replica_describe_log_dirs_result(
                &tpr1, log_dir, 123456, offset_lag, false,
            )]),
            &nodes[broker_id as usize],
        );

        let result =
            admin.describe_replica_log_dirs(&[tpr1.clone(), tpr2.clone()], DescribeReplicaLogDirsOptions::new());
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
    async fn test_alter_replica_log_dirs_success() {
        let (admin, mut runnable, _time, nodes) = env();
        runnable
            .client_mut()
            .prepare_response_for_node(alter_log_dirs_response(Errors::None, "topic", &[0]), &nodes[0]);
        runnable
            .client_mut()
            .prepare_response_for_node(alter_log_dirs_response(Errors::None, "topic", &[0]), &nodes[1]);

        let tpr0 = TopicPartitionReplica::new("topic", 0, 0);
        let tpr1 = TopicPartitionReplica::new("topic", 0, 1);
        let assignment = HashMap::from([
            (tpr0.clone(), "/data0".to_string()),
            (tpr1.clone(), "/data1".to_string()),
        ]);
        let result = admin.alter_replica_log_dirs(&assignment, AlterReplicaLogDirsOptions::new());
        pump_until(&mut runnable, 20, |_r| {
            result.values()[&tpr0].is_done() && result.values()[&tpr1].is_done()
        })
        .await;
        result.values()[&tpr0].get().await.unwrap();
        result.values()[&tpr1].get().await.unwrap();
    }

    /// Mirrors `KafkaAdminClientTest.testAlterReplicaLogDirsLogDirNotFound`.
    #[tokio::test]
    async fn test_alter_replica_log_dirs_log_dir_not_found() {
        let (admin, mut runnable, _time, nodes) = env();
        runnable
            .client_mut()
            .prepare_response_for_node(alter_log_dirs_response(Errors::None, "topic", &[0]), &nodes[0]);
        runnable
            .client_mut()
            .prepare_response_for_node(alter_log_dirs_response(Errors::LogDirNotFound, "topic", &[0]), &nodes[1]);

        let tpr0 = TopicPartitionReplica::new("topic", 0, 0);
        let tpr1 = TopicPartitionReplica::new("topic", 0, 1);
        let assignment = HashMap::from([
            (tpr0.clone(), "/data0".to_string()),
            (tpr1.clone(), "/data1".to_string()),
        ]);
        let result = admin.alter_replica_log_dirs(&assignment, AlterReplicaLogDirsOptions::new());
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
    async fn test_alter_replica_log_dirs_unrequested() {
        let (admin, mut runnable, _time, nodes) = env();
        // Response contains partitions 1 and 2, but only 1 was requested.
        runnable
            .client_mut()
            .prepare_response_for_node(alter_log_dirs_response(Errors::None, "topic", &[1, 2]), &nodes[0]);

        let tpr1 = TopicPartitionReplica::new("topic", 1, 0);
        let assignment = HashMap::from([(tpr1.clone(), "/data1".to_string())]);
        let result = admin.alter_replica_log_dirs(&assignment, AlterReplicaLogDirsOptions::new());
        pump_until(&mut runnable, 20, |_r| result.values()[&tpr1].is_done()).await;
        result.values()[&tpr1].get().await.unwrap();
    }

    /// Mirrors `KafkaAdminClientTest.testAlterReplicaLogDirsPartialResponse`.
    #[tokio::test]
    async fn test_alter_replica_log_dirs_partial_response() {
        let (admin, mut runnable, _time, nodes) = env();
        // Response contains only partition 1; partition 2 was also requested.
        runnable
            .client_mut()
            .prepare_response_for_node(alter_log_dirs_response(Errors::None, "topic", &[1]), &nodes[0]);

        let tpr1 = TopicPartitionReplica::new("topic", 1, 0);
        let tpr2 = TopicPartitionReplica::new("topic", 2, 0);
        let assignment = HashMap::from([
            (tpr1.clone(), "/data1".to_string()),
            (tpr2.clone(), "/data1".to_string()),
        ]);
        let result = admin.alter_replica_log_dirs(&assignment, AlterReplicaLogDirsOptions::new());
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
    async fn test_alter_replica_log_dirs_partial_failure() {
        let default_api_timeout: i64 = 60000;
        let (admin, mut runnable, time, nodes) = env_with_props(&[
            ("default.api.timeout.ms", &default_api_timeout.to_string()),
            ("retries", "0"),
        ]);
        // Provide only node 1's response.
        runnable
            .client_mut()
            .prepare_response_for_node(alter_log_dirs_response(Errors::None, "topic", &[2]), &nodes[1]);

        let tpr1 = TopicPartitionReplica::new("topic", 1, 0);
        let tpr2 = TopicPartitionReplica::new("topic", 2, 1);
        let assignment = HashMap::from([
            (tpr1.clone(), "/data1".to_string()),
            (tpr2.clone(), "/data1".to_string()),
        ]);
        let result = admin.alter_replica_log_dirs(&assignment, AlterReplicaLogDirsOptions::new());
        pump_until(&mut runnable, 30, |r| !r.client_mut().has_pending_responses()).await;
        time.sleep(default_api_timeout + 1);
        pump_until(&mut runnable, 30, |_r| {
            result.values()[&tpr1].is_done() && result.values()[&tpr2].is_done()
        })
        .await;
        assert!(matches!(
            result.values()[&tpr1].get().await.unwrap_err(),
            KafkaError::Timeout(_)
        ));
        result.values()[&tpr2].get().await.unwrap();
    }

    // --- MockAdminClient log-dir methods -------------------------------------

    fn mock_topic_partition_info(partition: i32, leader: &Node, replicas: Vec<Node>) -> TopicPartitionInfo {
        TopicPartitionInfo::new(partition, Some(leader.clone()), replicas, Vec::new(), Vec::new(), Vec::new())
    }

    #[tokio::test]
    async fn test_mock_describe_log_dirs_reports_topic_replicas() {
        let mock = MockAdminClient::create(2);
        let leader = Node::new(0, "localhost".to_string(), 1000);
        let replicas = vec![
            Node::new(0, "localhost".to_string(), 1000),
            Node::new(1, "localhost".to_string(), 1001),
        ];
        mock.add_topic(false, "topic", vec![mock_topic_partition_info(0, &leader, replicas)], None);

        let result = mock.describe_log_dirs(&[0, 1], DescribeLogDirsOptions::new());
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
        let mock = MockAdminClient::create(1);
        mock.set_broker_log_dirs(0, vec!["/data0".to_string(), "/data1".to_string()]);
        let leader = Node::new(0, "localhost".to_string(), 1000);
        mock.add_topic(
            false,
            "topic",
            vec![mock_topic_partition_info(0, &leader, vec![leader.clone()])],
            None,
        );

        // Before any move, current log dir is the seeded first broker log dir.
        let tpr = TopicPartitionReplica::new("topic", 0, 0);
        let before = mock.describe_replica_log_dirs(std::slice::from_ref(&tpr), DescribeReplicaLogDirsOptions::new());
        let info = before.values()[&tpr].get().await.unwrap();
        assert_eq!(info.current_replica_log_dir(), Some("/data0"));
        assert_eq!(info.future_replica_log_dir(), None);

        // Move to /data1; describe should reflect the pending move.
        let assignment = HashMap::from([(tpr.clone(), "/data1".to_string())]);
        let alter = mock.alter_replica_log_dirs(&assignment, AlterReplicaLogDirsOptions::new());
        alter.values()[&tpr].get().await.unwrap();
        let after = mock.describe_replica_log_dirs(std::slice::from_ref(&tpr), DescribeReplicaLogDirsOptions::new());
        let moved = after.values()[&tpr].get().await.unwrap();
        assert_eq!(moved.current_replica_log_dir(), Some("/data0"));
        assert_eq!(moved.future_replica_log_dir(), Some("/data1"));
    }

    #[tokio::test]
    async fn test_mock_alter_replica_log_dirs_offline_dir() {
        let mock = MockAdminClient::create(1);
        let leader = Node::new(0, "localhost".to_string(), 1000);
        mock.add_topic(
            false,
            "topic",
            vec![mock_topic_partition_info(0, &leader, vec![leader.clone()])],
            None,
        );
        let tpr = TopicPartitionReplica::new("topic", 0, 0);
        // "/nope" is not among the broker's log dirs -> KafkaStorageError.
        let assignment = HashMap::from([(tpr.clone(), "/nope".to_string())]);
        let result = mock.alter_replica_log_dirs(&assignment, AlterReplicaLogDirsOptions::new());
        let err = result.values()[&tpr].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::KafkaStorageError);
    }

    // --- electLeaders / (alter|list)PartitionReassignments / listOffsets -------
    //
    // `ElectLeadersResponse`, `ElectionType`, and the `*Options` / POJO types are
    // already in scope via `use super::*`. Only the wire *data* structs and the
    // `ListOffsetsResponse` wrapper need importing here.

    use crate::alter_partition_reassignments_response_data::{
        AlterPartitionReassignmentsResponseData, ReassignablePartitionResponse, ReassignableTopicResponse,
    };
    use crate::common::requests::{
        AlterPartitionReassignmentsResponse, ListOffsetsResponse, ListPartitionReassignmentsResponse,
    };
    use crate::elect_leaders_response_data::{ElectLeadersResponseData, PartitionResult, ReplicaElectionResult};
    use crate::list_offsets_response_data::ListOffsetsResponseData;
    use crate::list_partition_reassignments_response_data::{
        ListPartitionReassignmentsResponseData, OngoingPartitionReassignment, OngoingTopicReassignment,
    };

    fn elect_leaders_resp(top_error: Errors, results: Vec<ReplicaElectionResult>) -> ConcreteResponse {
        let mut data = ElectLeadersResponseData::new();
        data.set_error_code(top_error.code());
        data.set_replica_election_results(results);
        ConcreteResponse::ElectLeaders(ElectLeadersResponse::new(data))
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
            let result = admin.elect_leaders(election_type, Some(partitions.clone()), ElectLeadersOptions::new());
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
            let result = admin.elect_leaders(election_type, Some(partitions.clone()), ElectLeadersOptions::new());
            pump(&mut runnable, 5).await;
            let map = result.partitions().get().await.unwrap();
            assert!(map[&topic1].is_none());
            assert!(map[&topic2].is_none());

            // A call that times out (no response prepared).
            let result = admin.elect_leaders(
                election_type,
                Some(partitions),
                ElectLeadersOptions::new().timeout_ms(Some(100)),
            );
            pump_until(&mut runnable, 5, |r| r.client_mut().request_count() >= 1).await;
            time.sleep(200);
            pump_until(&mut runnable, 30, |_r| result.partitions().is_done()).await;
            let err = result.partitions().get().await.unwrap_err();
            assert!(matches!(err, KafkaError::Timeout(_)));
        }
    }

    // --- describeFeatures / updateFeatures -------------------------------------

    use crate::admin::UpgradeType;
    use crate::api_message_type::ListenerType;
    use crate::api_versions_response_data::{ApiVersionsResponseData, SupportedFeatureKey};
    use crate::common::requests::{ApiVersionsResponse, ApiVersionsResponseBuilder, UpdateFeaturesResponse};

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
            let response = ApiVersionsResponseBuilder::new()
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
    async fn test_describe_features_success() {
        let (admin, mut runnable, _time, _nodes) = env();
        runnable
            .client_mut()
            .prepare_response(api_versions_feature_response(Errors::None));
        let result = admin.describe_features(DescribeFeaturesOptions::new().timeout_ms(Some(10000)));
        pump(&mut runnable, 5).await;
        let metadata = result.feature_metadata().get().await.unwrap();
        assert_eq!(metadata, default_feature_metadata());
    }

    /// Mirrors `KafkaAdminClientTest.testDescribeFeaturesFailure`.
    #[tokio::test]
    async fn test_describe_features_failure() {
        let (admin, mut runnable, _time, _nodes) = env();
        runnable
            .client_mut()
            .prepare_response(api_versions_feature_response(Errors::InvalidRequest));
        let result = admin.describe_features(DescribeFeaturesOptions::new().timeout_ms(Some(10000)));
        pump(&mut runnable, 5).await;
        let err = result.feature_metadata().get().await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidRequest);
    }

    /// Mirrors `KafkaAdminClientTest.testDescribeFeaturesWithNodeSuccess` — a set
    /// `nodeId` routes the request to that broker via `ConstantNodeIdProvider`.
    #[tokio::test]
    async fn test_describe_features_with_node_success() {
        let (admin, mut runnable, _time, nodes) = env();
        runnable
            .client_mut()
            .prepare_response_for_node(api_versions_feature_response(Errors::None), &nodes[0]);
        let result = admin.describe_features(DescribeFeaturesOptions::new().timeout_ms(Some(10000)).node_id(0));
        pump(&mut runnable, 5).await;
        let metadata = result.feature_metadata().get().await.unwrap();
        assert_eq!(metadata, default_feature_metadata());
    }

    /// Mirrors `KafkaAdminClientTest.testDescribeFeaturesWithNodeFailure` — the
    /// response is prepared for node 1 but the request targets node 0, so it is
    /// never answered and the future times out.
    #[tokio::test]
    async fn test_describe_features_with_node_failure() {
        let (admin, mut runnable, time, nodes) = env();
        runnable
            .client_mut()
            .prepare_response_for_node(api_versions_feature_response(Errors::None), &nodes[1]);
        let result = admin.describe_features(DescribeFeaturesOptions::new().timeout_ms(Some(1000)).node_id(0));
        pump_until(&mut runnable, 5, |r| r.client_mut().request_count() >= 1).await;
        time.sleep(2000);
        pump_until(&mut runnable, 30, |_r| result.feature_metadata().is_done()).await;
        assert!(result.feature_metadata().get().await.is_err());
    }

    /// Drives `KafkaAdminClientTest.testUpdateFeaturesDuringSuccess` — a
    /// `@ParameterizedTest` over `@ValueSource(shorts = {1, 2})`. v1 responses
    /// carry per-feature results; v2+ carry only a top-level NONE.
    #[tokio::test]
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
                .update_features(&updates, UpdateFeaturesOptions::new().timeout_ms(Some(10000)))
                .unwrap();
            pump(&mut runnable, 5).await;
            for future in result.values().values() {
                future.get().await.unwrap();
            }
        }
    }

    /// Mirrors `KafkaAdminClientTest.testUpdateFeaturesTopLevelError`.
    #[tokio::test]
    async fn test_update_features_top_level_error() {
        let (admin, mut runnable, _time, _nodes) = env();
        runnable
            .client_mut()
            .prepare_response(update_features_response(Errors::InvalidRequest, None, &[]));
        let updates = make_test_feature_updates();
        let result = admin
            .update_features(&updates, UpdateFeaturesOptions::new().timeout_ms(Some(10000)))
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
    async fn test_update_features_handle_not_controller_exception() {
        for version in [1i16, 2] {
            let (admin, mut runnable, time, nodes) = env();
            // First attempt hits the wrong controller.
            runnable
                .client_mut()
                .prepare_response(update_features_response(Errors::NotController, None, &[]));
            // Then a metadata refresh updates the controller to node 1.
            runnable
                .client_mut()
                .prepare_response(ConcreteResponse::Metadata(request_test_utils::metadata_response(
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
                .update_features(&updates, UpdateFeaturesOptions::new().timeout_ms(Some(10000)))
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
    async fn test_update_features_should_fail_request_for_empty_updates() {
        let (admin, _runnable, _time, _nodes) = env();
        let err = admin
            .update_features(&HashMap::new(), UpdateFeaturesOptions::new())
            .unwrap_err();
        assert_eq!(err.message(), "Feature updates can not be null or empty.");
    }

    /// Mirrors `KafkaAdminClientTest.testUpdateFeaturesShouldFailRequestForInvalidFeatureName`.
    #[tokio::test]
    async fn test_update_features_should_fail_request_for_invalid_feature_name() {
        let (admin, _runnable, _time, _nodes) = env();
        let mut updates = HashMap::new();
        updates.insert("feature".to_string(), FeatureUpdate::new(2, UpgradeType::Upgrade).unwrap());
        updates.insert(String::new(), FeatureUpdate::new(2, UpgradeType::Upgrade).unwrap());
        let err = admin.update_features(&updates, UpdateFeaturesOptions::new()).unwrap_err();
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
        let result =
            admin.alter_partition_reassignments(&reassignments_input(), AlterPartitionReassignmentsOptions::new());
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
        let result =
            admin.alter_partition_reassignments(&reassignments_input(), AlterPartitionReassignmentsOptions::new());
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
        let result =
            admin.alter_partition_reassignments(&reassignments_input(), AlterPartitionReassignmentsOptions::new());
        pump(&mut runnable, 5).await;
        let all_err = result.all().get().await.unwrap_err();
        assert_eq!(all_err.error(), Errors::ClusterAuthorizationFailed);
        assert_eq!(all_err.message(), error_message);
        assert_eq!(
            result.values()[&TopicPartition::new("A", 0)].get().await.unwrap_err().message(),
            error_message
        );
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
        let result = admin.alter_partition_reassignments(&reassignments, AlterPartitionReassignmentsOptions::new());
        pump(&mut runnable, 5).await;
        assert_eq!(
            result.values()[&invalid_topic].get().await.unwrap_err().error(),
            Errors::InvalidTopicException
        );
        assert_eq!(
            result.values()[&invalid_partition].get().await.unwrap_err().error(),
            Errors::InvalidTopicException
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
            .prepare_response(ConcreteResponse::Metadata(request_test_utils::metadata_response(
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
        let result =
            admin.alter_partition_reassignments(&reassignments_input(), AlterPartitionReassignmentsOptions::new());
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
            .prepare_response(ConcreteResponse::Metadata(request_test_utils::metadata_response(
                &nodes,
                Some("mock-cluster"),
                1,
                Vec::new(),
            )));
        runnable.client_mut().prepare_response(list_reassignments_resp(
            Errors::None,
            vec![ongoing_topic("A", 0), ongoing_topic("B", 0)],
        ));
        let result = admin.list_partition_reassignments(None, ListPartitionReassignmentsOptions::new());
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
        let result = admin.list_partition_reassignments(Some(partitions), ListPartitionReassignmentsOptions::new());
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
        let result = admin.list_partition_reassignments(None, ListPartitionReassignmentsOptions::new());
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
        let result = admin.list_offsets(&partitions, ListOffsetsOptions::new());
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
        let result = admin.list_offsets(&partitions, ListOffsetsOptions::new());
        pump_until(&mut runnable, 40, |_r| result.all().is_done()).await;
        assert_eq!(result.all().get().await.unwrap_err().error(), Errors::TopicAuthorizationFailed);
    }

    /// Mirrors `KafkaAdminClientTest.testListOffsetsRetriableErrors`: a
    /// LEADER_NOT_AVAILABLE partition triggers a metadata re-lookup then a retry.
    #[tokio::test]
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
        runnable.client_mut().prepare_response_for_node(
            list_offsets_resp_from(&[
                (tp0.clone(), Errors::LeaderNotAvailable, -1, 123, 321),
                (tp1.clone(), Errors::None, -1, 987, 789),
            ]),
            &nodes[0],
        );
        // node1 fulfillment: bar-0 ok.
        runnable
            .client_mut()
            .prepare_response_for_node(list_offsets_resp_from(&[(tp2.clone(), Errors::None, -1, 456, 654)]), &nodes[1]);
        // metadata re-lookup for the unmapped foo-0.
        runnable
            .client_mut()
            .prepare_response(metadata_resp(&nodes, vec![topic_meta_leaders("foo", &[(0, 0), (1, 0)])]));
        // node0 fulfillment retry: foo-0 ok.
        runnable
            .client_mut()
            .prepare_response_for_node(list_offsets_resp_from(&[(tp0.clone(), Errors::None, -1, 345, 543)]), &nodes[0]);

        let mut partitions = HashMap::new();
        partitions.insert(tp0.clone(), OffsetSpec::latest());
        partitions.insert(tp1.clone(), OffsetSpec::latest());
        partitions.insert(tp2.clone(), OffsetSpec::latest());
        let result = admin.list_offsets(&partitions, ListOffsetsOptions::new());
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
    async fn test_list_offsets_max_timestamp_unsupported_single_offset_spec() {
        let (admin, mut runnable, _time, nodes) = env();
        let tp0 = TopicPartition::new("foo", 0);
        runnable
            .client_mut()
            .prepare_response(metadata_resp(&nodes, vec![topic_meta_leaders("foo", &[(0, 0)])]));
        runnable.client_mut().prepare_unsupported_version_response();
        let mut partitions = HashMap::new();
        partitions.insert(tp0.clone(), OffsetSpec::max_timestamp());
        let result = admin.list_offsets(&partitions, ListOffsetsOptions::new());
        pump_until(&mut runnable, 40, |_r| result.all().is_done()).await;
        assert_eq!(result.all().get().await.unwrap_err().error(), Errors::UnsupportedVersion);
    }

    /// Mirrors `KafkaAdminClientTest.testListOffsetsMaxTimestampUnsupportedMultipleOffsetSpec`:
    /// only the MAX_TIMESTAMP partition fails; the other is retried and succeeds.
    #[tokio::test]
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
            .prepare_response_for_node(list_offsets_resp_from(&[(tp1.clone(), Errors::None, -1, 345, 543)]), &nodes[0]);
        let mut partitions = HashMap::new();
        partitions.insert(tp0.clone(), OffsetSpec::max_timestamp());
        partitions.insert(tp1.clone(), OffsetSpec::latest());
        let result = admin.list_offsets(&partitions, ListOffsetsOptions::new());
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
        let result = admin.list_offsets(&partitions, ListOffsetsOptions::new());
        pump_until(&mut runnable, 40, |_r| result.all().is_done()).await;
        assert!(result.partition_result(&tp0).unwrap().get().await.is_ok());
        assert!(result.partition_result(&tp1).unwrap().get().await.is_err());
        assert!(result.all().get().await.is_err());
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
        use crate::admin::MockAdminClient;
        let mock = MockAdminClient::create(1);
        let earliest = TopicPartition::new("t", 0);
        let latest = TopicPartition::new("t", 1);
        let ts = TopicPartition::new("t", 2);
        mock.update_beginning_offsets(HashMap::from([(earliest.clone(), 5)]));
        mock.update_end_offsets(HashMap::from([(latest.clone(), 99)]));
        let mut partitions = HashMap::new();
        partitions.insert(earliest.clone(), OffsetSpec::earliest());
        partitions.insert(latest.clone(), OffsetSpec::latest());
        partitions.insert(ts.clone(), OffsetSpec::for_timestamp(123));
        let result = mock.list_offsets(&partitions, ListOffsetsOptions::new());
        assert_eq!(result.partition_result(&earliest).unwrap().get().await.unwrap().offset(), 5);
        assert_eq!(result.partition_result(&latest).unwrap().get().await.unwrap().offset(), 99);
        assert!(result.partition_result(&ts).unwrap().get().await.is_err());
    }

    /// The mock's `alter_partition_reassignments` / `list_partition_reassignments`
    /// track reassignments against added topics.
    #[tokio::test]
    async fn test_mock_partition_reassignments() {
        use crate::admin::MockAdminClient;
        let mock = MockAdminClient::create(3);
        let leader = Node::new(0, "localhost".to_string(), 1000);
        let replicas = vec![
            Node::new(0, "localhost".to_string(), 1000),
            Node::new(1, "localhost".to_string(), 1001),
        ];
        mock.add_topic(false, "topic", vec![mock_topic_partition_info(0, &leader, replicas)], None);
        let tp = TopicPartition::new("topic", 0);
        let mut reassignments = HashMap::new();
        reassignments.insert(tp.clone(), Some(NewPartitionReassignment::new(vec![1, 2]).unwrap()));
        let result = mock.alter_partition_reassignments(&reassignments, AlterPartitionReassignmentsOptions::new());
        result.values()[&tp].get().await.unwrap();

        let listed = mock
            .list_partition_reassignments(None, ListPartitionReassignmentsOptions::new())
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
        use crate::admin::MockAdminClient;
        let mock = MockAdminClient::create(1);
        let result = mock.elect_leaders(ElectionType::Preferred, None, ElectLeadersOptions::new());
        let err = result.partitions().get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnsupportedVersion);
    }

    // ---- Group listing / describe (Tier 2 Phase 1) ----

    fn listed_group(group_id: &str, protocol_type: &str, state: &str, group_type: &str) -> ConcreteResponse {
        use crate::list_groups_response_data::{ListGroupsResponseData, ListedGroup};
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
        use crate::list_groups_response_data::ListGroupsResponseData;
        ConcreteResponse::ListGroups(crate::common::requests::ListGroupsResponse::new(ListGroupsResponseData::new()))
    }

    fn find_coordinator_resp(entries: &[(&str, &Node)]) -> ConcreteResponse {
        use crate::find_coordinator_response_data::{Coordinator, FindCoordinatorResponseData};
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
        use crate::consumer_group_describe_response_data::{ConsumerGroupDescribeResponseData, DescribedGroup};
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
        use crate::consumer_group_describe_response_data::{ConsumerGroupDescribeResponseData, DescribedGroup};
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
        use crate::describe_groups_response_data::{DescribeGroupsResponseData, DescribedGroup};
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
        use crate::find_coordinator_response_data::{Coordinator, FindCoordinatorResponseData};
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
        use crate::describe_groups_response_data::DescribeGroupsResponseData;
        let mut data = DescribeGroupsResponseData::new();
        data.set_groups(groups);
        ConcreteResponse::DescribeGroups(crate::common::requests::DescribeGroupsResponse::new(data))
    }

    /// Broker enumeration: `list_groups` fans out one `ListGroups` per broker and
    /// unions the results.
    #[tokio::test]
    async fn test_list_groups() {
        let (admin, mut runnable, _time, nodes) = env();
        runnable.client_mut().prepare_response(metadata_resp(&nodes, Vec::new()));
        runnable
            .client_mut()
            .prepare_response_for_node(listed_group("g1", "consumer", "Stable", "Consumer"), &nodes[0]);
        runnable
            .client_mut()
            .prepare_response_for_node(listed_group("g2", "consumer", "Stable", "Consumer"), &nodes[1]);
        runnable
            .client_mut()
            .prepare_response_for_node(empty_list_groups_resp(), &nodes[2]);

        let result = admin.list_groups(ListGroupsOptions::new());
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
            .prepare_response_for_node(listed_group("g1", "consumer", "Stable", "Consumer"), &nodes[0]);
        runnable
            .client_mut()
            .prepare_response_for_node(listed_group("connect", "connect", "Stable", "Classic"), &nodes[1]);
        runnable
            .client_mut()
            .prepare_response_for_node(empty_list_groups_resp(), &nodes[2]);

        let options = ListGroupsOptions::new().with_protocol_types(HashSet::from(["consumer".to_string()]));
        let result = admin.list_groups(options);
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

    /// Broker enumeration: `list_consumer_groups` fans out per broker.
    #[tokio::test]
    #[allow(deprecated)]
    async fn test_list_consumer_groups() {
        let (admin, mut runnable, _time, nodes) = env();
        runnable.client_mut().prepare_response(metadata_resp(&nodes, Vec::new()));
        runnable
            .client_mut()
            .prepare_response_for_node(listed_group("g1", "consumer", "Stable", "Consumer"), &nodes[0]);
        runnable
            .client_mut()
            .prepare_response_for_node(listed_group("connect", "connect", "Stable", "Classic"), &nodes[1]);
        runnable
            .client_mut()
            .prepare_response_for_node(empty_list_groups_resp(), &nodes[2]);

        let result = admin.list_consumer_groups(ListConsumerGroupsOptions::new());
        pump_until(&mut runnable, 40, |_r| result.valid().is_done()).await;

        // Only the consumer-protocol group is retained.
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

    /// Translated from `KafkaAdminClientTest.testListGroupsWithTypes`.
    ///
    /// Asserts the emitted `ListGroups` request carries the types filter
    /// derived from `ListGroupsOptions::with_types`, then that both listings are
    /// returned.
    #[tokio::test]
    async fn test_list_groups_with_types() {
        use crate::common::requests::ConcreteRequest;

        let (admin, mut runnable, _time, nodes) = env_nodes_with_props(1, &[]);
        runnable.client_mut().prepare_response(metadata_resp(&nodes, Vec::new()));

        let options = ListGroupsOptions::new().with_types(HashSet::from([GroupType::Consumer]));
        let result = admin.list_groups(options);
        pump_until_request_queued(&mut runnable).await;

        // The single per-broker ListGroups request carries the types filter.
        {
            let reqs = runnable.client_mut().requests_mut();
            assert_eq!(reqs.len(), 1);
            match reqs[0].request_builder_mut().build().unwrap() {
                ConcreteRequest::ListGroups(req) => {
                    assert!(req.data().states_filter.is_empty());
                    assert_eq!(req.data().types_filter, vec![GroupType::Consumer.to_string()]);
                },
                other => panic!("expected a ListGroups request, got {other:?}"),
            }
        }

        runnable.client_mut().respond_from(
            listed_groups(&[
                ("group-1", PROTOCOL_TYPE, "Stable", "Consumer"),
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
    async fn test_list_groups_with_types_older_broker_version() {
        use crate::common::requests::ConcreteRequest;

        let (admin, mut runnable, _time, nodes) = env_nodes_with_props(1, &[]);

        // A SHARE-only filter cannot be omitted, so it surfaces UnsupportedVersion.
        runnable.client_mut().prepare_response(metadata_resp(&nodes, Vec::new()));
        runnable.client_mut().prepare_unsupported_version_response();
        let result = admin.list_groups(ListGroupsOptions::new().with_types(HashSet::from([GroupType::Share])));
        pump_until(&mut runnable, 40, |_r| result.all().is_done()).await;
        assert_eq!(result.all().get().await.unwrap_err().error(), Errors::UnsupportedVersion);

        // A CLASSIC-only filter is omitted on an older broker and succeeds.
        runnable.client_mut().prepare_response(metadata_resp(&nodes, Vec::new()));
        let result = admin.list_groups(ListGroupsOptions::new().with_types(HashSet::from([GroupType::Classic])));
        pump_until_request_queued(&mut runnable).await;
        {
            let reqs = runnable.client_mut().requests_mut();
            assert_eq!(reqs.len(), 1);
            // At v5 the request still carries the classic types filter ...
            match reqs[0].request_builder_mut().build().unwrap() {
                ConcreteRequest::ListGroups(req) => {
                    assert_eq!(req.data().types_filter, vec![GroupType::Classic.to_string()]);
                },
                other => panic!("expected a ListGroups request, got {other:?}"),
            }
            // ... but building at the older broker's v4 omits it (the request
            // succeeds against the older broker with an empty filter).
            match reqs[0].request_builder_mut().build_version(4).unwrap() {
                ConcreteRequest::ListGroups(req) => assert!(req.data().types_filter.is_empty()),
                other => panic!("expected a ListGroups request, got {other:?}"),
            }
        }
        runnable
            .client_mut()
            .respond_from(listed_groups(&[("group-1", PROTOCOL_TYPE, "Stable", "")]), &nodes[0]);
        pump_until(&mut runnable, 40, |_r| result.all().is_done()).await;
        let listings = result.all().get().await.unwrap();
        assert_eq!(listings.len(), 1);
        assert_eq!(listings[0].group_id(), "group-1");

        // A CONSUMER-only filter (without classic) also surfaces UnsupportedVersion.
        runnable.client_mut().prepare_response(metadata_resp(&nodes, Vec::new()));
        runnable.client_mut().prepare_unsupported_version_response();
        let result = admin.list_groups(ListGroupsOptions::new().with_types(HashSet::from([GroupType::Consumer])));
        pump_until(&mut runnable, 40, |_r| result.all().is_done()).await;
        assert_eq!(result.all().get().await.unwrap_err().error(), Errors::UnsupportedVersion);
    }

    /// Translated from `KafkaAdminClientTest.testListConsumerGroupsWithStates`.
    ///
    /// `for_consumer_groups()` derives a `[Classic, Consumer]` types filter; this
    /// asserts that filter reaches the wire request, then that both consumer
    /// groups are returned.
    #[tokio::test]
    async fn test_list_consumer_groups_with_states() {
        use crate::common::requests::ConcreteRequest;

        let (admin, mut runnable, _time, nodes) = env_nodes_with_props(1, &[]);
        runnable.client_mut().prepare_response(metadata_resp(&nodes, Vec::new()));

        let result = admin.list_groups(ListGroupsOptions::for_consumer_groups());
        pump_until_request_queued(&mut runnable).await;
        {
            let reqs = runnable.client_mut().requests_mut();
            match reqs[0].request_builder_mut().build().unwrap() {
                ConcreteRequest::ListGroups(req) => {
                    let mut types = req.data().types_filter.clone();
                    types.sort();
                    assert_eq!(types, vec![GroupType::Classic.to_string(), GroupType::Consumer.to_string()]);
                },
                other => panic!("expected a ListGroups request, got {other:?}"),
            }
        }

        runnable.client_mut().respond_from(
            listed_groups(&[("group-1", PROTOCOL_TYPE, "Stable", ""), ("group-2", "", "Empty", "")]),
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
    async fn test_list_consumer_groups_with_types_older_broker_version() {
        use crate::common::requests::ConcreteRequest;

        let (admin, mut runnable, _time, nodes) = env_nodes_with_props(1, &[]);

        // States filter with no types filter is fine at v4.
        runnable.client_mut().prepare_response(metadata_resp(&nodes, Vec::new()));
        let result = admin.list_groups(ListGroupsOptions::new().in_group_states(HashSet::from([GroupState::Stable])));
        pump_until_request_queued(&mut runnable).await;
        {
            let reqs = runnable.client_mut().requests_mut();
            match reqs[0].request_builder_mut().build_version(4).unwrap() {
                ConcreteRequest::ListGroups(req) => {
                    assert_eq!(req.data().states_filter, vec![GroupState::Stable.to_string()]);
                    assert!(req.data().types_filter.is_empty());
                },
                other => panic!("expected a ListGroups request, got {other:?}"),
            }
        }
        runnable
            .client_mut()
            .respond_from(listed_groups(&[("group-1", PROTOCOL_TYPE, "Stable", "")]), &nodes[0]);
        pump_until(&mut runnable, 40, |_r| result.all().is_done()).await;
        assert_eq!(result.all().get().await.unwrap().len(), 1);

        // A SHARE types filter cannot be set against the older broker.
        runnable.client_mut().prepare_response(metadata_resp(&nodes, Vec::new()));
        runnable.client_mut().prepare_unsupported_version_response();
        let result = admin.list_groups(ListGroupsOptions::new().with_types(HashSet::from([GroupType::Share])));
        pump_until(&mut runnable, 40, |_r| result.all().is_done()).await;
        assert_eq!(result.all().get().await.unwrap_err().error(), Errors::UnsupportedVersion);
    }

    /// Translated from the deprecated
    /// `KafkaAdminClientTest.testListConsumerGroupsWithStates` /
    /// `...WithTypes` variants: the deprecated `list_consumer_groups` API also
    /// carries the states/types filter to the wire request.
    #[tokio::test]
    #[allow(deprecated)]
    async fn test_list_consumer_groups_deprecated_with_states_and_types() {
        use crate::common::requests::ConcreteRequest;

        let (admin, mut runnable, _time, nodes) = env_nodes_with_props(1, &[]);
        runnable.client_mut().prepare_response(metadata_resp(&nodes, Vec::new()));

        let options = ListConsumerGroupsOptions::new()
            .in_group_states(HashSet::from([GroupState::Stable]))
            .with_types(HashSet::from([GroupType::Consumer]));
        let result = admin.list_consumer_groups(options);
        pump_until_request_queued(&mut runnable).await;
        {
            let reqs = runnable.client_mut().requests_mut();
            match reqs[0].request_builder_mut().build().unwrap() {
                ConcreteRequest::ListGroups(req) => {
                    assert_eq!(req.data().states_filter, vec![GroupState::Stable.to_string()]);
                    assert_eq!(req.data().types_filter, vec![GroupType::Consumer.to_string()]);
                },
                other => panic!("expected a ListGroups request, got {other:?}"),
            }
        }
        runnable
            .client_mut()
            .respond_from(listed_groups(&[("group-1", PROTOCOL_TYPE, "Stable", "Consumer")]), &nodes[0]);
        pump_until(&mut runnable, 40, |_r| result.valid().is_done()).await;
        assert_eq!(result.valid().get().await.unwrap().len(), 1);
    }

    /// Translated from the deprecated
    /// `KafkaAdminClientTest.testListConsumerGroupsWithTypesOlderBrokerVersion`:
    /// a SHARE types filter surfaces `UnsupportedVersion` through the deprecated
    /// API's future.
    #[tokio::test]
    #[allow(deprecated)]
    async fn test_list_consumer_groups_deprecated_older_broker_version() {
        let (admin, mut runnable, _time, nodes) = env_nodes_with_props(1, &[]);
        runnable.client_mut().prepare_response(metadata_resp(&nodes, Vec::new()));
        runnable.client_mut().prepare_unsupported_version_response();

        let options = ListConsumerGroupsOptions::new().with_types(HashSet::from([GroupType::Share]));
        let result = admin.list_consumer_groups(options);
        pump_until(&mut runnable, 40, |_r| result.all().is_done()).await;
        assert_eq!(result.all().get().await.unwrap_err().error(), Errors::UnsupportedVersion);
    }

    /// Translated from `KafkaAdminClientTest.testListConsumerGroupsMetadataFailure`.
    ///
    /// An empty metadata response leaves no brokers to send `ListGroups` to; with
    /// `retries=0` the metadata call fails terminally and `handle_failure` wraps
    /// it as "Failed to find brokers to send listConsumerGroups".
    #[tokio::test]
    #[allow(deprecated)]
    async fn test_list_consumer_groups_metadata_failure() {
        let (admin, mut runnable, time, nodes) = env_nodes_with_props(3, &[("retries", "0")]);
        // Empty broker list → no brokers to send to.
        runnable.client_mut().prepare_response(metadata_resp(&[], Vec::new()));

        let result = admin.list_consumer_groups(ListConsumerGroupsOptions::new());
        for _ in 0..40 {
            if result.all().is_done() {
                break;
            }
            runnable.run_once().await;
            time.sleep(100);
        }
        let err = result.all().get().await.unwrap_err();
        assert!(
            err.message().contains("Failed to find brokers to send listConsumerGroups"),
            "got: {}",
            err.message()
        );
        let _ = &nodes;
    }

    /// The `list_groups` metadata-failure counterpart of
    /// `testListConsumerGroupsMetadataFailure` (Java exercises the shared
    /// `findAllBrokers` path from both entry points).
    #[tokio::test]
    async fn test_list_groups_metadata_failure() {
        let (admin, mut runnable, time, _nodes) = env_nodes_with_props(3, &[("retries", "0")]);
        runnable.client_mut().prepare_response(metadata_resp(&[], Vec::new()));

        let result = admin.list_groups(ListGroupsOptions::new());
        for _ in 0..40 {
            if result.all().is_done() {
                break;
            }
            runnable.run_once().await;
            time.sleep(100);
        }
        let err = result.all().get().await.unwrap_err();
        assert!(
            err.message().contains("Failed to find brokers to send listGroups"),
            "got: {}",
            err.message()
        );
    }

    /// `describe_consumer_groups` finds the coordinator then describes the group
    /// with the KIP-848 `ConsumerGroupDescribe` API.
    #[tokio::test]
    async fn test_describe_consumer_groups() {
        let (admin, mut runnable, _time, nodes) = env();
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[("g1", &nodes[0])]));
        runnable
            .client_mut()
            .prepare_response_for_node(consumer_group_describe_resp("g1"), &nodes[0]);

        let result = admin.describe_consumer_groups(&["g1".to_string()], DescribeConsumerGroupsOptions::new());
        let future = result.described_groups()["g1"].clone();
        pump_until(&mut runnable, 40, |_r| future.is_done()).await;

        let description = future.get().await.unwrap();
        assert_eq!(description.group_id(), "g1");
        assert_eq!(description.group_type(), GroupType::Consumer);
        assert_eq!(description.group_state(), GroupState::Stable);
        assert_eq!(description.partition_assignor(), "uniform");
        // The driver identifies the coordinator by broker id (the Node it routed
        // the fulfillment request to).
        assert_eq!(description.coordinator().map(Node::id), Some(0));
    }

    /// A `describe_consumer_groups` on a nonexistent group id surfaces
    /// `GROUP_ID_NOT_FOUND` after the classic fallback also reports it, keeping
    /// the more-informative `ConsumerGroupDescribe` message.
    #[tokio::test]
    async fn test_describe_consumer_groups_group_id_not_found() {
        let (admin, mut runnable, time, nodes) = env();
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[("missing", &nodes[0])]));
        runnable.client_mut().prepare_response_for_node(
            consumer_group_describe_error_resp("missing", Errors::GroupIdNotFound, Some("informative message")),
            &nodes[0],
        );
        // Fallback: classic DescribeGroups also reports GROUP_ID_NOT_FOUND.
        runnable.client_mut().prepare_response_for_node(
            describe_groups_error_resp("missing", Errors::GroupIdNotFound, Some("terse message")),
            &nodes[0],
        );

        let result = admin.describe_consumer_groups(&["missing".to_string()], DescribeConsumerGroupsOptions::new());
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

        let result = admin.describe_consumer_groups(&["g1".to_string()], DescribeConsumerGroupsOptions::new());
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
    async fn test_describe_classic_groups() {
        use crate::common::ClassicGroupState;
        use crate::consumer::consumer_partition_assignor::Assignment;
        use crate::consumer::internals::consumer_protocol::ConsumerProtocol;
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
            let assignment_bytes =
                ConsumerProtocol::serialize_assignment(&Assignment::with_partitions(topic_partitions)).unwrap();
            let member_one = described_member("0", None, "clientId0", "clientHost", assignment_bytes.clone());
            let member_two = described_member("1", Some("static"), "clientId1", "clientHost", assignment_bytes.clone());
            let mut group = DescribedGroup::new();
            group
                .set_group_id("group-0".to_string())
                .set_protocol_type(PROTOCOL_TYPE.to_string())
                .set_group_state(ClassicGroupState::Stable.to_string())
                .set_members(vec![member_one, member_two]);
            c.prepare_response(describe_groups_full_resp(vec![group]));
        }

        let result = admin.describe_classic_groups(&["group-0".to_string()], DescribeClassicGroupsOptions::new());
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
    }

    /// Translated from
    /// `KafkaAdminClientTest.testDescribeClassicGroupsWithAuthorizedOperationsOmitted`.
    #[tokio::test]
    async fn test_describe_classic_groups_with_authorized_operations_omitted() {
        use crate::common::requests::metadata_response::AUTHORIZED_OPERATIONS_OMITTED;
        use crate::describe_groups_response_data::DescribedGroup;

        let (admin, mut runnable, _time, nodes) = env();
        {
            let c = runnable.client_mut();
            c.prepare_response(find_coordinator_resp(&[("group-0", &nodes[0])]));
            let mut group = DescribedGroup::new();
            group
                .set_group_id("group-0".to_string())
                .set_protocol_type(String::new())
                .set_authorized_operations(AUTHORIZED_OPERATIONS_OMITTED);
            c.prepare_response_for_node(describe_groups_full_resp(vec![group]), &nodes[0]);
        }

        let result = admin.describe_classic_groups(&["group-0".to_string()], DescribeClassicGroupsOptions::new());
        let future = result.described_groups()["group-0"].clone();
        pump_until(&mut runnable, 40, |_r| future.is_done()).await;

        let description = future.get().await.unwrap();
        // Omitted authorized operations decode to an empty set (Java returns null).
        assert!(description.authorized_operations().is_empty());
    }

    /// Translated from `KafkaAdminClientTest.testDescribeMultipleClassicGroups`.
    #[tokio::test]
    async fn test_describe_multiple_classic_groups() {
        use crate::common::ClassicGroupState;
        use crate::consumer::consumer_partition_assignor::Assignment;
        use crate::consumer::internals::consumer_protocol::ConsumerProtocol;
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
            let assignment_bytes =
                ConsumerProtocol::serialize_assignment(&Assignment::with_partitions(topic_partitions)).unwrap();

            let mut group0 = DescribedGroup::new();
            group0
                .set_group_id("group-0".to_string())
                .set_protocol_type(PROTOCOL_TYPE.to_string())
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
            c.prepare_response_for_node(describe_groups_full_resp(vec![group0, group1]), &nodes[0]);
        }

        let result = admin.describe_classic_groups(
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
        let resource = ConfigResource::new(ConfigResourceType::Group, group_id.to_string());
        let ops = vec![AlterConfigOp::new(
            ConfigEntry::new("consumer.session.timeout.ms".to_string(), Some("45000".to_string())),
            OpType::Set,
        )];
        let mut configs = HashMap::new();
        configs.insert(resource.clone(), ops);
        mock.incremental_alter_configs(&configs, AlterConfigsOptions::new())
            .all()
            .get()
            .await
            .unwrap();
    }

    /// The mock's `list_groups` returns one CONSUMER/STABLE listing per seeded
    /// group config (mirrors Java's mock).
    #[tokio::test]
    async fn test_mock_list_groups() {
        use crate::admin::MockAdminClient;
        let mock = MockAdminClient::create(1);
        seed_mock_group(&mock, "g1").await;
        let result = mock.list_groups(ListGroupsOptions::new());
        let listings = result.valid().get().await.unwrap();
        assert_eq!(listings.len(), 1);
        assert_eq!(listings[0].group_id(), "g1");
        assert_eq!(listings[0].group_type(), Some(GroupType::Consumer));
        assert_eq!(listings[0].group_state(), Some(GroupState::Stable));
    }

    /// The mock's `list_consumer_groups` returns one listing per seeded group.
    #[tokio::test]
    #[allow(deprecated)]
    async fn test_mock_list_consumer_groups() {
        use crate::admin::MockAdminClient;
        let mock = MockAdminClient::create(1);
        seed_mock_group(&mock, "g1").await;
        let result = mock.list_consumer_groups(ListConsumerGroupsOptions::new());
        let listings = result.valid().get().await.unwrap();
        assert_eq!(listings.len(), 1);
        assert_eq!(listings[0].group_id(), "g1");
        assert!(!listings[0].is_simple_consumer_group());
    }

    /// The mock's `describe_consumer_groups` mirrors Java's
    /// `UnsupportedOperationException`.
    #[tokio::test]
    async fn test_mock_describe_consumer_groups_unsupported() {
        use crate::admin::MockAdminClient;
        let mock = MockAdminClient::create(1);
        let result = mock.describe_consumer_groups(&["g1".to_string()], DescribeConsumerGroupsOptions::new());
        let err = result.described_groups()["g1"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnsupportedVersion);
    }

    /// The mock's `describe_classic_groups` mirrors Java's
    /// `UnsupportedOperationException`.
    #[tokio::test]
    async fn test_mock_describe_classic_groups_unsupported() {
        use crate::admin::MockAdminClient;
        let mock = MockAdminClient::create(1);
        let result = mock.describe_classic_groups(&["g1".to_string()], DescribeClassicGroupsOptions::new());
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
        let mut data = crate::offset_fetch_response_data::OffsetFetchResponseData::new();
        data.set_groups(vec![g]);
        ConcreteResponse::OffsetFetch(crate::common::requests::OffsetFetchResponse::new(
            data,
            crate::common::protocol::ApiKeys::OFFSET_FETCH.latest_version(),
        ))
    }

    /// Builds a full (per-partition) `OffsetFetch` response for one group.
    fn offset_fetch_full(group: &str, topic: &str, partitions: &[(i32, i64)]) -> ConcreteResponse {
        use crate::offset_fetch_response_data::{
            OffsetFetchResponseData, OffsetFetchResponseGroup, OffsetFetchResponsePartitions, OffsetFetchResponseTopics,
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
        ConcreteResponse::OffsetCommit(crate::common::requests::OffsetCommitResponse::from_response_data(0, &map))
    }

    fn offset_delete_top_level(error: Errors) -> ConcreteResponse {
        let mut data = crate::offset_delete_response_data::OffsetDeleteResponseData::new();
        data.set_error_code(error.code());
        ConcreteResponse::OffsetDelete(crate::common::requests::OffsetDeleteResponse::new(data))
    }

    fn offset_delete_partition(topic: &str, partition: i32, error: Errors) -> ConcreteResponse {
        use crate::offset_delete_response_data::{
            OffsetDeleteResponseData, OffsetDeleteResponsePartition, OffsetDeleteResponseTopic,
        };
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
        use crate::find_coordinator_response_data::FindCoordinatorResponseData;
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
        use crate::find_coordinator_response_data::FindCoordinatorResponseData;
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
            ListConsumerGroupOffsetsSpec::new().topic_partitions(Some(partitions.to_vec())),
        )])
    }

    /// Translated from `testListConsumerGroupOffsets`: retriable FindCoordinator
    /// and OffsetFetch errors are retried, and the final response's negative
    /// offset maps to `None`.
    #[tokio::test]
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

        let result = admin.list_consumer_group_offsets(
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

            let result = admin.list_consumer_group_offsets(
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
                ListConsumerGroupOffsetsSpec::new().topic_partitions(Some(vec![TopicPartition::new("A", 1)])),
            ),
            (
                "groupB".to_string(),
                ListConsumerGroupOffsetsSpec::new().topic_partitions(Some(vec![TopicPartition::new("B", 2)])),
            ),
        ])
    }

    fn offset_fetch_multi(groups: &[(&str, &str, i32)]) -> ConcreteResponse {
        use crate::offset_fetch_response_data::{
            OffsetFetchResponseData, OffsetFetchResponseGroup, OffsetFetchResponsePartitions, OffsetFetchResponseTopics,
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
    async fn test_batched_list_consumer_group_offsets() {
        let (admin, mut runnable, time, nodes) = offsets_env(1);
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[("groupA", &nodes[0]), ("groupB", &nodes[0])]));
        runnable
            .client_mut()
            .prepare_response(offset_fetch_multi(&[("groupA", "A", 1), ("groupB", "B", 2)]));

        let result = admin.list_consumer_group_offsets(&batched_specs(), ListConsumerGroupOffsetsOptions::new());
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

        let result = admin.list_consumer_group_offsets(&batched_specs(), ListConsumerGroupOffsetsOptions::new());
        let all = result.all();
        drive_until(&mut runnable, &time, 80, || all.is_done()).await;
        assert_eq!(all.get().await.unwrap().len(), 2);
    }

    /// Translated from `testBatchedListConsumerGroupOffsetsWithNoOffsetFetchBatching`:
    /// a `NoBatchedOffsetFetchRequestException` disables batching, after which
    /// both FindCoordinator and OffsetFetch are re-sent per group.
    #[tokio::test]
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

        let result = admin.list_consumer_group_offsets(&batched_specs(), ListConsumerGroupOffsetsOptions::new());
        let all = result.all();
        drive_until(&mut runnable, &time, 80, || all.is_done()).await;
        assert_eq!(all.get().await.unwrap().len(), 2);
    }

    /// Translated from `KafkaAdminClientTest.testListConsumerGroupOffsetsOptionsWithBatchedApi`
    /// (helper `verifyListConsumerGroupOffsetsOptions`): the `requireStable`
    /// option and the request timeout propagate to the built `OffsetFetch` wire
    /// request, and the group id / topic / partition indexes map through.
    #[tokio::test]
    async fn test_list_consumer_group_offsets_options_with_batched_api() {
        use crate::common::requests::ConcreteRequest;

        // Java uses mockCluster(3, 0) with RETRIES_CONFIG = "0".
        let (admin, mut runnable, _time, nodes) = env_with_props(&[("retries", "0")]);
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));

        let options = ListConsumerGroupOffsetsOptions::new()
            .require_stable(true)
            .timeout_ms(Some(300));
        let _result = admin.list_consumer_group_offsets(&single_spec(&[TopicPartition::new("A", 0)]), options);

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
            ConcreteRequest::OffsetFetch(req) => {
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

        let result =
            admin.alter_consumer_group_offsets(GROUP_ID, &offsets_to_alter(), AlterConsumerGroupOffsetsOptions::new());
        let all = result.all();
        drive_until(&mut runnable, &time, 40, || all.is_done()).await;

        assert_eq!(all.get().await.unwrap(), ());
        assert_eq!(result.partition_result(&tp1).get().await.unwrap(), ());
        assert_eq!(result.partition_result(&tp2).get().await.unwrap(), ());
        // A partition not in the request fails with IllegalArgument.
        assert!(matches!(
            result.partition_result(&tp3).get().await.unwrap_err(),
            KafkaError::IllegalArgument(_)
        ));
    }

    /// Translated from `testOffsetCommitWithMultipleErrors`.
    #[tokio::test]
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
        let result = admin.alter_consumer_group_offsets(GROUP_ID, &offsets, AlterConsumerGroupOffsetsOptions::new());
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
            let result =
                admin.alter_consumer_group_offsets(GROUP_ID, &offsets, AlterConsumerGroupOffsetsOptions::new());
            let all = result.all();
            drive_until(&mut runnable, &time, 40, || all.is_done()).await;
            assert_eq!(all.get().await.unwrap_err().error(), error);
            assert_eq!(result.partition_result(&tp1).get().await.unwrap_err().error(), error);
        }
    }

    /// Translated from `testAlterConsumerGroupOffsetsFindCoordinatorNonRetriableErrors`.
    #[tokio::test]
    async fn test_alter_consumer_group_offsets_find_coordinator_non_retriable_errors() {
        let (admin, mut runnable, time, _nodes) = offsets_env(1);
        let tp1 = TopicPartition::new("foo", 0);
        runnable
            .client_mut()
            .prepare_response(find_coordinator_error_resp(GROUP_ID, Errors::GroupAuthorizationFailed));

        let offsets = HashMap::from([(tp1.clone(), OffsetAndMetadata::new(123).unwrap())]);
        let result = admin.alter_consumer_group_offsets(GROUP_ID, &offsets, AlterConsumerGroupOffsetsOptions::new());
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
            use crate::offset_delete_response_data::{
                OffsetDeleteResponseData, OffsetDeleteResponsePartition, OffsetDeleteResponseTopic,
            };
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

        let result = admin.delete_consumer_group_offsets(
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
        assert!(matches!(result.partition_result(&tp3), Err(KafkaError::IllegalArgument(_))));
    }

    /// Translated from `testDeleteConsumerGroupOffsetsNonRetriableErrors`.
    #[tokio::test]
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

            let result = admin.delete_consumer_group_offsets(
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
    async fn test_delete_consumer_group_offsets_find_coordinator_non_retriable_errors() {
        let (admin, mut runnable, time, _nodes) = offsets_env(1);
        let tp1 = TopicPartition::new("foo", 0);
        runnable
            .client_mut()
            .prepare_response(find_coordinator_error_resp(GROUP_ID, Errors::GroupAuthorizationFailed));

        let result = admin.delete_consumer_group_offsets(
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

        let result = admin.delete_consumer_group_offsets(
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
        use crate::delete_groups_response_data::{DeletableGroupResult, DeleteGroupsResponseData};
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
        use crate::leave_group_response_data::LeaveGroupResponseData;
        let mut data = LeaveGroupResponseData::new();
        data.set_error_code(error.code());
        ConcreteResponse::LeaveGroup(crate::common::requests::LeaveGroupResponse::new(data))
    }

    /// A successful `LeaveGroup` response with one member response per
    /// `(group.instance.id, error)` entry (member id echoed as empty, as the
    /// broker does for a static member removed by instance id).
    fn leave_group_members_resp(members: &[(&str, Errors)]) -> ConcreteResponse {
        use crate::leave_group_response_data::{LeaveGroupResponseData, MemberResponse};
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
        use crate::consumer_group_describe_response_data::{ConsumerGroupDescribeResponseData, DescribedGroup, Member};
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

        let result = admin.delete_consumer_groups(&["groupId".to_string()], DeleteConsumerGroupsOptions::new());
        let all = result.all();
        pump_until(&mut runnable, 40, |r| !r.client_mut().has_pending_responses()).await;
        time.sleep(default_api_timeout + 1);
        drive_until(&mut runnable, &time, 40, || all.is_done()).await;
        assert!(matches!(all.get().await.unwrap_err(), KafkaError::Timeout(_)));
    }

    /// Translated from `testDeleteConsumerGroupsWithOlderBroker`: retriable
    /// `FindCoordinator` errors are retried, non-retriable ones fail, and
    /// coordinator-moved `DeleteGroups` errors trigger a re-lookup. Uses the old
    /// (single-coordinator) `FindCoordinator` response form throughout.
    #[tokio::test]
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

        let result = admin.delete_consumer_groups(&["groupId".to_string()], DeleteConsumerGroupsOptions::new());
        let deleted = result.deleted_groups()["groupId"].clone();
        drive_until(&mut runnable, &time, 80, || deleted.is_done()).await;
        assert_eq!(deleted.get().await.unwrap(), ());

        // A non-retriable FindCoordinator error surfaces.
        runnable
            .client_mut()
            .prepare_response(old_find_coordinator_error_resp(Errors::GroupAuthorizationFailed));
        let error_result = admin.delete_consumer_groups(&["groupId".to_string()], DeleteConsumerGroupsOptions::new());
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

        let retry_result = admin.delete_consumer_groups(&["groupId".to_string()], DeleteConsumerGroupsOptions::new());
        let retry_deleted = retry_result.deleted_groups()["groupId"].clone();
        drive_until(&mut runnable, &time, 120, || retry_deleted.is_done()).await;
        assert_eq!(retry_deleted.get().await.unwrap(), ());
    }

    /// Translated from `testRemoveMembersFromGroupNumRetries`.
    #[tokio::test]
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

        let result =
            admin.remove_members_from_consumer_group(GROUP_ID, members_to_remove(&["instance-1", "instance-2"]));
        let all = result.all();
        pump_until(&mut runnable, 40, |r| !r.client_mut().has_pending_responses()).await;
        time.sleep(default_api_timeout + 1);
        drive_until(&mut runnable, &time, 40, || all.is_done()).await;
        assert!(matches!(all.get().await.unwrap_err(), KafkaError::Timeout(_)));
    }

    /// Translated from `testRemoveMembersFromGroupRetriableErrors`.
    #[tokio::test]
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
        let result = admin.remove_members_from_consumer_group(GROUP_ID, members_to_remove(&["instance-1"]));
        let all = result.all();
        drive_until(&mut runnable, &time, 120, || all.is_done()).await;
        assert_eq!(all.get().await.unwrap(), ());
        assert_eq!(result.member_result(&member).unwrap().get().await.unwrap(), ());
    }

    /// Translated from `testRemoveMembersFromGroupNonRetriableErrors`.
    #[tokio::test]
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
            let result = admin.remove_members_from_consumer_group(GROUP_ID, members_to_remove(&["instance-1"]));
            let all = result.all();
            drive_until(&mut runnable, &time, 60, || all.is_done()).await;
            assert_eq!(all.get().await.unwrap_err().error(), error);
            assert_eq!(result.member_result(&member).unwrap().get().await.unwrap_err().error(), error);
        }
    }

    /// Translated from `testRemoveMembersFromGroup`: member-level error, then a
    /// missing member, then success, and finally the two `removeAll` scenarios.
    #[tokio::test]
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

        let member_level_error_result =
            admin.remove_members_from_consumer_group(GROUP_ID, members_to_remove(&[instance_one, instance_two]));
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

        let missing_member_result =
            admin.remove_members_from_consumer_group(GROUP_ID, members_to_remove(&[instance_one, instance_two]));
        let missing_all = missing_member_result.all();
        drive_until(&mut runnable, &time, 60, || missing_all.is_done()).await;
        assert!(matches!(missing_all.get().await.unwrap_err(), KafkaError::IllegalArgument(_)));
        assert!(matches!(
            missing_member_result
                .member_result(&member_one)
                .unwrap()
                .get()
                .await
                .unwrap_err(),
            KafkaError::IllegalArgument(_)
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
        let no_error_result =
            admin.remove_members_from_consumer_group(GROUP_ID, members_to_remove(&[instance_one, instance_two]));
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
        let partial_failure_result =
            admin.remove_members_from_consumer_group(GROUP_ID, RemoveMembersFromConsumerGroupOptions::default());
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
        let success_result =
            admin.remove_members_from_consumer_group(GROUP_ID, RemoveMembersFromConsumerGroupOptions::default());
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
    /// `options.timeout()` (`KafkaAdminClient.java:4172`). This asserts the first
    /// describe request (the coordinator lookup) carries the default-API-timeout
    /// budget, not the small `options.timeout()` budget, and so fails against
    /// code that ties the describe deadline to `options.timeout()`.
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
        let options = RemoveMembersFromConsumerGroupOptions::default().timeout_ms(Some(5000));
        assert!(options.remove_all());
        let _result = admin.remove_members_from_consumer_group(GROUP_ID, options);

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

        let options = RemoveMembersFromConsumerGroupOptions::default().timeout_ms(Some(5000));
        assert!(options.remove_all());
        let _result = admin.remove_members_from_consumer_group(GROUP_ID, options);

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
        use crate::common::requests::ConcreteRequest;
        let (admin, mut runnable, _time, nodes) = offsets_env(3);
        // Answer only FindCoordinator so the LeaveGroup request is sent but stays
        // queued (no prepared response), ready for inspection.
        runnable
            .client_mut()
            .prepare_response(find_coordinator_resp(&[(GROUP_ID, &nodes[0])]));

        let mut options = members_to_remove(&["instance-1", "instance-2"]);
        if let Some(reason) = reason {
            options.reason(reason);
        }
        let _result = admin.remove_members_from_consumer_group(GROUP_ID, options);

        pump_until(&mut runnable, 40, |r| {
            r.client_mut()
                .requests_mut()
                .iter_mut()
                .any(|req| matches!(req.request_builder_mut().build(), Ok(ConcreteRequest::LeaveGroup(_))))
        })
        .await;

        let leave_request = runnable
            .client_mut()
            .requests_mut()
            .iter_mut()
            .find_map(|req| match req.request_builder_mut().build() {
                Ok(ConcreteRequest::LeaveGroup(r)) => Some(r),
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
    async fn test_remove_members_from_group_reason() {
        assert_remove_members_reason(Some("testing remove members reason"), "testing remove members reason").await;
    }

    /// Translated from `testRemoveMembersFromGroupTruncatesReason`: a reason
    /// longer than 255 chars is truncated to exactly 255 on the wire.
    #[tokio::test]
    async fn test_remove_members_from_group_truncates_reason() {
        let reason = "Very looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong reason that is 271 characters long to make sure that length limit logic handles the scenario nicely";
        assert_eq!(reason.chars().count(), 271);
        let truncated: String = reason.chars().take(255).collect();
        assert_remove_members_reason(Some(reason), &truncated).await;
    }

    /// Translated from `testRemoveMembersFromGroupDefaultReason`: a null or empty
    /// reason falls back to the default reason.
    #[tokio::test]
    async fn test_remove_members_from_group_default_reason() {
        assert_remove_members_reason(None, DEFAULT_LEAVE_GROUP_REASON).await;
        assert_remove_members_reason(Some(""), DEFAULT_LEAVE_GROUP_REASON).await;
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
            runnable.should_exit_for_test(time.now.load(Ordering::Acquire)),
            "close() must not wait on an internal call: the I/O task has to exit at once"
        );
    }

    /// The other half of the contract: an **external** call does hold the loop
    /// open until the hard-shutdown deadline, so `close(timeout)` still gives a
    /// user-submitted RPC its chance to finish.
    #[tokio::test]
    async fn close_waits_for_an_active_external_call_until_the_hard_deadline() {
        let (admin, mut runnable, time, _nodes) = env();
        let _result = admin.list_topics(ListTopicsOptions::new());
        pump(&mut runnable, 1).await;
        assert!(
            runnable.has_active_external_calls_for_test(),
            "the submitted listTopics call should be an active external call"
        );

        let now = time.now.load(Ordering::Acquire);
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
    }

    impl WaitingClient {
        fn new(inner: MockClient, time: Arc<MockTime>) -> Self {
            Self {
                inner,
                time,
                poll_timeouts: Arc::new(Mutex::new(Vec::new())),
                advance_clock: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                stuck: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            }
        }

        fn poll_timeouts(&self) -> Arc<Mutex<Vec<i64>>> {
            Arc::clone(&self.poll_timeouts)
        }

        fn advance_clock(&self) -> Arc<std::sync::atomic::AtomicBool> {
            Arc::clone(&self.advance_clock)
        }

        /// Once armed, `poll` never returns, so the I/O loop can never reach
        /// `should_exit` again. It stands in for any `await` inside a
        /// `run_once` phase that no shutdown deadline can interrupt — in
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
        fn authentication_error(&self, node: &Node) -> Option<String> {
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
            let now = if self.advance_clock.load(Ordering::Acquire) {
                self.time.sleep(timeout);
                self.time.now.load(Ordering::Acquire)
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
    /// (`KafkaAdminClient.java:1500-1502`):
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
        let time = MockTime::new(1000);
        let (cluster, nodes) = mock_cluster(3, 0);
        let client = WaitingClient::new(MockClient::new(nodes.clone(), time.provider()), Arc::clone(&time));
        let poll_timeouts = client.poll_timeouts();
        let advance_clock = client.advance_clock();
        let config = test_config();
        let (admin, mut runnable) = KafkaAdminClient::create_for_test(client, cluster, &config, time.provider());

        // Put an external RPC in flight. The mock has no prepared response, so
        // the call sits in `correlation_id_to_calls`: `pending_calls` is empty,
        // hence no `retry_backoff_ms` floor, and the only contributors left to
        // the poll timeout are the call deadline (`default.api.timeout.ms`) and
        // `metadata.max.age.ms`.
        let _result = admin.list_topics(ListTopicsOptions::new());
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
        let now = time.now.load(Ordering::Acquire);
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
        let waited = time.now.load(Ordering::Acquire) - now;
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
        let time = MockTime::new(1000);
        let (cluster, nodes) = mock_cluster(3, 0);
        let client = WaitingClient::new(MockClient::new(nodes.clone(), time.provider()), Arc::clone(&time));
        let stuck = client.stuck();
        let config = test_config();
        let (admin, runnable) = KafkaAdminClient::create_for_test(client, cluster, &config, time.provider());

        // From its first poll on, the I/O task is parked forever: it can neither
        // finish work nor re-evaluate `should_exit`, so nothing but the timed
        // join can end the wait.
        stuck.store(true, Ordering::Release);
        admin.spawn(runnable);
        // An active external call, so `should_exit` could not short-circuit on
        // "all work has been completed" even if the task did run again.
        let _result = admin.list_topics(ListTopicsOptions::new());

        let started = std::time::Instant::now();
        let returned = tokio::time::timeout(Duration::from_secs(5), admin.close(Duration::from_millis(50))).await;
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

    /// Java publishes the hard-shutdown deadline through a compare-and-set loop
    /// that only ever moves it *earlier* (`KafkaAdminClient.close`: "Hard
    /// shutdown time is already earlier than requested"). A plain store would let
    /// a later, more relaxed `close()` re-widen the poll budget that `run_once`
    /// reads on every iteration — stretching the wait of a caller already parked
    /// in the join above.
    #[tokio::test]
    async fn close_never_widens_an_existing_hard_shutdown_deadline() {
        let (admin, _runnable, time, _nodes) = env();
        let now = time.now.load(Ordering::Acquire);
        let deadline = || admin.shared.shutdown.hard_shutdown_deadline_ms.load(Ordering::Acquire);

        // No task was spawned, so each `close()` here only publishes the deadline.
        admin.close(Duration::from_millis(100)).await;
        assert_eq!(deadline(), now + 100, "the first close() installs its own deadline");

        admin.close(Duration::from_secs(60)).await;
        assert_eq!(
            deadline(),
            now + 100,
            "a later, more relaxed close() must keep the earlier deadline"
        );

        admin.close(Duration::from_millis(10)).await;
        assert_eq!(deadline(), now + 10, "a more urgent close() does move the deadline earlier");
    }

    /// Java caps the wait at a year ("Limit the timeout to a year"), which also
    /// keeps the deadline it derives finite — the no-argument `Admin.close()`
    /// passes `Duration.ofMillis(Long.MAX_VALUE)`.
    #[tokio::test]
    async fn close_clamps_the_wait_to_a_year() {
        let (admin, _runnable, time, _nodes) = env();
        let now = time.now.load(Ordering::Acquire);
        admin.close(Duration::from_millis(i64::MAX as u64)).await;
        assert_eq!(
            admin.shared.shutdown.hard_shutdown_deadline_ms.load(Ordering::Acquire),
            now + MAX_CLOSE_WAIT_TIME_MS
        );
    }
}
