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
use crate::client_utils;
use crate::common::kafka_future::KafkaFutureImpl;
use crate::common::network::Selector;
use crate::common::network::channel_builders;
use crate::common::protocol::Errors;
use crate::common::requests::{
    ConcreteResponse, CreateTopicsRequestBuilder, DeleteTopicsRequestBuilder, MetadataRequestBuilder, RequestBuilder,
};
use crate::common::security::SecurityProtocol;
use crate::common::utils::{ExponentialBackoff, LogContext};
use crate::common::{Cluster, KafkaError, KafkaFuture, TopicCollection, TopicPartitionInfo, Uuid};
use crate::create_topics_request_data::{CreatableTopic, CreateTopicsRequestData};
use crate::delete_topics_request_data::{DeleteTopicState, DeleteTopicsRequestData};
use crate::kafka_client::KafkaClient;
use crate::metadata_recovery_strategy::MetadataRecoveryStrategy;
use crate::network_client::NetworkClient;

use super::internals::admin_client_runnable::{AdminClientRunnable, ShutdownSignal};
use super::internals::admin_metadata_manager::AdminMetadataManager;
use super::internals::admin_utils::valid_acl_operations;
use super::internals::call::{Call, HandleResult, NodeProvider};
use super::{
    Admin, AdminClientConfig, Config, ConfigEntry, ConfigSource, ConfigType, CreateTopicsOptions, CreateTopicsResult,
    DeleteTopicsOptions, DeleteTopicsResult, DescribeTopicsOptions, DescribeTopicsResult, ListTopicsOptions,
    ListTopicsResult, NewTopic, TopicDescription, TopicListing, TopicMetadataAndConfig,
};

/// The `RETRY_BACKOFF_EXP_BASE` used by the admin retry backoff (Java constant).
const RETRY_BACKOFF_EXP_BASE: i32 = 2;
/// The `RETRY_BACKOFF_JITTER` used by the admin retry backoff (Java constant).
const RETRY_BACKOFF_JITTER: f64 = 0.2;

/// State shared between the `KafkaAdminClient` handle and (indirectly) the
/// background task.
struct Shared {
    #[allow(dead_code)]
    client_id: String,
    default_api_timeout_ms: i32,
    admin_tx: mpsc::UnboundedSender<Call>,
    wakeup: Arc<Notify>,
    shutdown: Arc<ShutdownSignal>,
    metadata_manager: AdminMetadataManager,
    time_provider: Arc<dyn Fn() -> i64 + Send + Sync>,
    bg_handle: Mutex<Option<JoinHandle<()>>>,
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
            admin_tx,
            wakeup,
            shutdown,
            metadata_manager,
            time_provider,
            bg_handle: Mutex::new(None),
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
}

/// Computes the absolute deadline for a call. Mirrors
/// `KafkaAdminClient.calcDeadlineMs`.
fn calc_deadline_ms(now: i64, option_timeout: Option<i32>, default_api_timeout_ms: i32) -> i64 {
    now + option_timeout.unwrap_or(default_api_timeout_ms) as i64
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

    async fn close(&self, timeout: Duration) {
        let now = self.now();
        let deadline = now.saturating_add(timeout.as_millis() as i64);
        self.shared
            .shutdown
            .hard_shutdown_deadline_ms
            .store(deadline, std::sync::atomic::Ordering::Release);
        self.shared.shutdown.closing.store(true, std::sync::atomic::Ordering::Release);
        self.shared.wakeup.notify_one();
        let handle = self.shared.bg_handle.lock().unwrap().take();
        if let Some(handle) = handle {
            let _ = handle.await;
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

    use crate::admin::internals::admin_client_runnable::AdminClientRunnable;
    use crate::common::Node;
    use crate::common::protocol::Errors;
    use crate::common::requests::metadata_response::{AUTHORIZED_OPERATIONS_OMITTED, PartitionMetadata, TopicMetadata};
    use crate::common::requests::request_test_utils;
    use crate::common::requests::{CreateTopicsResponse, DeleteTopicsResponse};
    use crate::common::{TopicCollection, TopicPartition, Uuid};
    use crate::create_topics_response_data::{CreatableTopicResult, CreateTopicsResponseData};
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

    async fn pump(runnable: &mut AdminClientRunnable<MockClient>, iters: usize) {
        for _ in 0..iters {
            runnable.run_once().await;
        }
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
}
