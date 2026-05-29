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

//! `OffsetsRequestManager` — drives `ListOffsets` and `OffsetsForLeaderEpoch`
//! requests required to reset and validate partition positions.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.OffsetsRequestManager`.
//!
//! ## Scope
//!
//! This translation covers the **reset** (`reset_positions_if_needed`),
//! **validate** (`validate_positions_if_needed`) and
//! **init-with-committed-offsets**
//! ([`Self::init_with_committed_offsets_if_needed`]) code paths that Java's
//! `updateFetchPositions` orchestrates. The remaining Java methods that
//! depend on `CommitRequestManager` (`fetch_offsets` for `list_offsets`,
//! `update_fetch_positions`, `prepare_fetch_offsets_requests`) are
//! deferred to Phase 10 commits 3b/3c.
//!
//! ## Concurrency
//!
//! All mutable state lives on `OffsetsRequestManager` itself, which the
//! consumer's bg task owns exclusively (consumer-threading.md §10). The
//! `Arc<Mutex<SubscriptionState>>` is borrowed for the brief critical
//! sections; no lock is held across an `.await` (CLAUDE.md §9.6).
//!
//! ## Pending-completion handling
//!
//! Java composes `CompletableFuture<...>` chains to attach response
//! handlers. The Rust translation enqueues an `UnsentRequest`, then —
//! when the bg task dispatches it — takes its
//! [`oneshot::Receiver<Result<ClientResponse, KafkaError>>`] and
//! `spawn`s a small task that awaits the response and forwards the
//! result back into the manager via a `mpsc` channel. The manager
//! drains the channel on its next `poll`, applying the success/failure
//! handler from `OffsetFetcherUtilsState`.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, oneshot};

use crate::api_versions::ApiVersions;
use crate::client_response::ClientResponse;
use crate::common::cluster_resource::ClusterResource;
use crate::common::cluster_resource_listener::ClusterResourceListener;
use crate::common::requests::{
    ConcreteResponse, ListOffsetsRequestBuilder, OffsetsForLeaderEpochResponse,
    list_offsets_request::CONSUMER_REPLICA_ID,
};
use crate::common::{IsolationLevel, KafkaError, TopicPartition};
use crate::consumer::OffsetAndMetadata;
use crate::list_offsets_request_data::ListOffsetsPartition;

use super::auto_offset_reset_strategy::AutoOffsetResetStrategy;
use super::commit_request_manager::CommitRequestManager;
use super::consumer_metadata::ConsumerMetadata;
use super::network_client_delegate::{PollResult, UnsentRequest};
use super::offset_fetcher_utils::{
    OffsetFetcherUtilsState, has_usable_offset_for_leader_epoch_version, regroup_fetch_positions_by_leader,
    regroup_partition_map_by_node,
};
use super::offsets_for_leader_epoch_client::OffsetsForLeaderEpochClient;
use super::request_manager::RequestManager;
use super::subscription_state::{FetchPosition, SubscriptionState};

/// Tracks pending request completions that need to be processed on the
/// next `poll()` call.
enum PendingCompletion {
    ListOffsetsForReset {
        reset_timestamps: HashMap<TopicPartition, ListOffsetsPartition>,
        partition_strategies: HashMap<TopicPartition, AutoOffsetResetStrategy>,
        result: Result<ClientResponse, KafkaError>,
    },
    OffsetsForLeaderEpoch {
        fetch_positions: HashMap<TopicPartition, FetchPosition>,
        result: Result<ClientResponse, KafkaError>,
    },
}

/// The KIP-848 `OffsetsRequestManager`. Drives `ListOffsets` (for offset
/// reset) and `OffsetsForLeaderEpoch` (for position validation) requests.
///
/// Java: `org.apache.kafka.clients.consumer.internals.OffsetsRequestManager`
/// (which extends `RequestManager` and `ClusterResourceListener`). The
/// Rust translation factors the `ClusterResourceListener` callback into a
/// separate handle struct so we can register the callback with
/// `Metadata::add_cluster_update_listener` without binding the listener's
/// lifetime to the manager's `&mut`.
pub(crate) struct OffsetsRequestManager {
    subscription_state: Arc<Mutex<SubscriptionState>>,
    metadata: Arc<ConsumerMetadata>,
    isolation_level: IsolationLevel,
    request_timeout_ms: i64,
    /// Java: `defaultApiTimeoutMs`. Used by
    /// [`Self::init_with_committed_offsets_if_needed`] to compute the
    /// internal `fetchCommittedDeadlineMs` (Java
    /// `OffsetsRequestManager.java:379`).
    default_api_timeout_ms: i64,
    api_versions: Arc<ApiVersions>,
    offset_fetcher_utils: Arc<OffsetFetcherUtilsState>,
    /// Optional handle to the commit manager — `None` when the consumer
    /// has no group. Java's `OffsetsRequestManager` accepts the commit
    /// manager as a non-null parameter but the
    /// `initWithCommittedOffsetsIfNeeded` path explicitly returns early
    /// when no group is configured; this Rust translation collapses both
    /// cases into `None`.
    commit_request_manager: Option<Arc<CommitRequestManager>>,
    /// The in-flight `OffsetFetch` triggered by the most-recent
    /// [`Self::init_with_committed_offsets_if_needed`] call. Java:
    /// `pendingOffsetFetchEvent`. Reused by subsequent calls when the
    /// initializing-partition set matches, so a poll that times out
    /// while an `OffsetFetch` is still in flight does not waste the
    /// outstanding request. Cleared when the underlying fetch resolves.
    pending_offset_fetch_event: Arc<Mutex<Option<PendingFetchCommittedRequest>>>,

    /// Requests built but not yet drained by `poll`. Java:
    /// `requestsToSend`.
    requests_to_send: Vec<UnsentRequest>,
    /// Completions waiting to be applied on the next `poll` call.
    /// `Mutex` because the receiver-side `tokio::spawn` writes into it
    /// from another task.
    pending_completions_rx: mpsc::UnboundedReceiver<PendingCompletion>,
    pending_completions_tx: mpsc::UnboundedSender<PendingCompletion>,
    closing: bool,
}

/// In-flight `OffsetFetch` carrying the set of initializing partitions
/// that triggered it plus any additional `oneshot::Sender`s that have
/// piggy-backed on the same request (the "reuse" path).
///
/// Mirrors Java's `OffsetsRequestManager.PendingFetchCommittedRequest`.
/// Java holds a `CompletableFuture` whose downstream `whenComplete`
/// handlers form a chain; Rust uses a `Vec<oneshot::Sender>` because
/// `oneshot::Receiver` is single-consumer.
struct PendingFetchCommittedRequest {
    requested_partitions: HashSet<TopicPartition>,
    /// Senders waiting for the in-flight fetch to resolve. The first
    /// entry is for the caller that initially issued the fetch; each
    /// subsequent reuse adds another sender. When the fetch completes,
    /// the driver task drains the vec and forwards the result (or its
    /// `()` ack on success — offsets are written into the
    /// `SubscriptionState` as side effects) to every sender.
    waiters: Vec<oneshot::Sender<Result<(), KafkaError>>>,
}

impl OffsetsRequestManager {
    /// Constructs a new `OffsetsRequestManager`.
    ///
    /// Mirrors Java's constructor minus the `LogContext` parameter
    /// (implicit in Rust through `log`). `commit_request_manager` is
    /// `Option<Arc<CommitRequestManager>>` so test fixtures and the
    /// (out-of-scope) group-less assignor path can pass `None`; Java
    /// requires the commit manager non-null and short-circuits the
    /// `initWithCommittedOffsetsIfNeeded` path internally.
    #[allow(clippy::too_many_arguments)] // Mirrors Java constructor argument list.
    pub(crate) fn new(
        subscription_state: Arc<Mutex<SubscriptionState>>,
        metadata: Arc<ConsumerMetadata>,
        isolation_level: IsolationLevel,
        retry_backoff_ms: i64,
        request_timeout_ms: i64,
        default_api_timeout_ms: i64,
        api_versions: Arc<ApiVersions>,
        commit_request_manager: Option<Arc<CommitRequestManager>>,
    ) -> Self {
        let offset_fetcher_utils = Arc::new(OffsetFetcherUtilsState::new(
            metadata.clone(),
            subscription_state.clone(),
            api_versions.clone(),
            retry_backoff_ms,
        ));
        let (pending_completions_tx, pending_completions_rx) = mpsc::unbounded_channel();
        let manager = Self {
            subscription_state,
            metadata: metadata.clone(),
            isolation_level,
            request_timeout_ms,
            default_api_timeout_ms,
            api_versions,
            offset_fetcher_utils,
            commit_request_manager,
            pending_offset_fetch_event: Arc::new(Mutex::new(None)),
            requests_to_send: Vec::new(),
            pending_completions_rx,
            pending_completions_tx,
            closing: false,
        };
        // Register the cluster metadata update callback. The listener
        // re-issues any deferred requests on metadata change; in this
        // minimal translation that's a no-op (no `requests_to_retry`
        // queue is modelled because `fetch_offsets` is deferred), so we
        // just install a stateless handle.
        metadata
            .metadata_arc()
            .add_cluster_update_listener(Box::new(OffsetsClusterListener {}));
        manager
    }

    /// Reset offsets for all assigned partitions that require it. Offsets
    /// are reset with timestamps according to the configured reset
    /// strategy. This generates `ListOffsets` requests, enqueued for the
    /// next `poll` call.
    ///
    /// Mirrors Java's `resetPositionsIfNeeded()`.
    ///
    /// # Errors
    ///
    /// Propagates the cached reset-positions exception from a previous
    /// call (e.g. `TopicAuthorizationException`), or any
    /// `NoOffsetForPartitionException` raised when a partition needs
    /// reset but no strategy is configured.
    pub(crate) fn reset_positions_if_needed(&mut self, current_time_ms: i64) -> Result<(), KafkaError> {
        let partition_strategies = self
            .offset_fetcher_utils
            .get_offset_reset_strategy_for_partitions(current_time_ms)?;
        if partition_strategies.is_empty() {
            return Ok(());
        }
        self.send_list_offsets_requests_and_reset_positions(partition_strategies, current_time_ms);
        Ok(())
    }

    /// Validate positions for all assigned partitions that have a leader
    /// change pending validation. Generates `OffsetsForLeaderEpoch`
    /// requests grouped by leader, enqueued for the next `poll` call.
    ///
    /// Mirrors Java's `validatePositionsIfNeeded()`.
    ///
    /// # Errors
    ///
    /// Propagates the cached validate-positions exception from a previous
    /// call (e.g. a saved `LogTruncationException`).
    pub(crate) fn validate_positions_if_needed(&mut self, current_time_ms: i64) -> Result<(), KafkaError> {
        let partitions_to_validate = self
            .offset_fetcher_utils
            .refresh_and_get_partitions_to_validate(current_time_ms)?;
        if partitions_to_validate.is_empty() {
            return Ok(());
        }
        self.send_offsets_for_leader_epoch_requests_and_validate_positions(partitions_to_validate, current_time_ms);
        Ok(())
    }

    /// Mirrors Java's `sendListOffsetsRequestsAndResetPositions`.
    ///
    /// Groups partitions by current leader and builds one `ListOffsets`
    /// request per leader. Each request's response receiver is wired
    /// into a tokio task that forwards the result into the manager's
    /// `pending_completions` channel; `poll` drains the channel and
    /// applies success/failure handlers via `OffsetFetcherUtilsState`.
    fn send_list_offsets_requests_and_reset_positions(
        &mut self,
        partition_strategies: HashMap<TopicPartition, AutoOffsetResetStrategy>,
        current_time_ms: i64,
    ) {
        let now_ms = current_time_ms;
        // Build per-partition `ListOffsetsPartition` carrying the strategy's
        // wire timestamp.
        let mut timestamps_to_search: HashMap<TopicPartition, ListOffsetsPartition> = HashMap::new();
        for (tp, strategy) in &partition_strategies {
            if let Some(ts) = strategy.timestamp() {
                let mut part = ListOffsetsPartition::new();
                part.set_partition_index(tp.partition());
                part.set_timestamp(ts);
                timestamps_to_search.insert(tp.clone(), part);
            }
        }

        // Group by current leader, dropping entries without one — those
        // will be retried on the next metadata update.
        let by_node = regroup_partition_map_by_node(&self.metadata, &timestamps_to_search);

        for (node, reset_timestamps) in by_node {
            // Java: subscriptionState.setNextAllowedRetry(...) to back off
            // any duplicate sends while this is in flight.
            {
                let partitions: std::collections::HashSet<TopicPartition> = reset_timestamps.keys().cloned().collect();
                let mut subs = self.subscription_state.lock().expect("SubscriptionState mutex poisoned");
                subs.set_next_allowed_retry(&partitions, now_ms + self.request_timeout_ms);
            }

            let mut builder = ListOffsetsRequestBuilder::for_consumer(false, self.isolation_level);
            let topics = crate::common::requests::ListOffsetsRequest::to_list_offsets_topics(&reset_timestamps);
            builder.set_target_times(topics);
            builder.set_timeout_ms(self.request_timeout_ms as i32);
            // Override the wire replica id (Builder::new used
            // CONSUMER_REPLICA_ID above; this is a no-op assert but
            // documents intent).
            debug_assert_eq!(CONSUMER_REPLICA_ID, -1);

            let mut unsent = UnsentRequest::new(Box::new(builder), Some(node));
            // Take the receiver and spawn a forwarder.
            let response_rx = unsent.take_response_receiver().expect("receiver fresh");
            let tx = self.pending_completions_tx.clone();
            let strategies = partition_strategies.clone();
            let timestamps = reset_timestamps.clone();
            tokio::spawn(async move {
                let result = match response_rx.await {
                    Ok(r) => r,
                    Err(_) => Err(KafkaError::new(crate::common::protocol::Errors::NetworkException)),
                };
                let _ = tx.send(PendingCompletion::ListOffsetsForReset {
                    reset_timestamps: timestamps,
                    partition_strategies: strategies,
                    result,
                });
            });
            self.requests_to_send.push(unsent);
        }
    }

    /// Mirrors Java's `sendOffsetsForLeaderEpochRequestsAndValidatePositions`.
    fn send_offsets_for_leader_epoch_requests_and_validate_positions(
        &mut self,
        partitions_to_validate: HashMap<TopicPartition, FetchPosition>,
        current_time_ms: i64,
    ) {
        let now_ms = current_time_ms;
        let regrouped = regroup_fetch_positions_by_leader(&partitions_to_validate);

        for (node, fetch_positions) in regrouped {
            if node.is_empty() {
                self.metadata.metadata_arc().request_update(true);
                continue;
            }
            let node_versions = self.api_versions.get(node.id_string());
            let Some(versions) = node_versions else {
                // Java schedules a non-blocking try_connect. The bg task
                // owns the network client; we emit a debug log to
                // surface this and continue.
                log::debug!("No API versions for node {}, deferring OffsetsForLeaderEpoch", node);
                continue;
            };
            if !has_usable_offset_for_leader_epoch_version(&versions) {
                log::debug!(
                    "Skipping validation of fetch offsets for partitions {:?} since the broker does not support the \
                     required protocol version (introduced in Kafka 2.3)",
                    fetch_positions.keys()
                );
                let mut subs = self.subscription_state.lock().expect("SubscriptionState mutex poisoned");
                for partition in fetch_positions.keys() {
                    let _ = subs.complete_validation(partition);
                }
                continue;
            }
            {
                let partitions: std::collections::HashSet<TopicPartition> = fetch_positions.keys().cloned().collect();
                let mut subs = self.subscription_state.lock().expect("SubscriptionState mutex poisoned");
                subs.set_next_allowed_retry(&partitions, now_ms + self.request_timeout_ms);
            }

            let builder = OffsetsForLeaderEpochClient::prepare_request(&fetch_positions);
            let mut unsent = UnsentRequest::new(Box::new(builder), Some(node));
            let response_rx = unsent.take_response_receiver().expect("receiver fresh");
            let tx = self.pending_completions_tx.clone();
            let positions = fetch_positions.clone();
            tokio::spawn(async move {
                let result = match response_rx.await {
                    Ok(r) => r,
                    Err(_) => Err(KafkaError::new(crate::common::protocol::Errors::NetworkException)),
                };
                let _ = tx.send(PendingCompletion::OffsetsForLeaderEpoch { fetch_positions: positions, result });
            });
            self.requests_to_send.push(unsent);
        }
    }

    /// Refresh fetch positions for any partition that still requires
    /// one, using committed offsets retrieved from the group
    /// coordinator. Mirrors Java's
    /// `OffsetsRequestManager.initWithCommittedOffsetsIfNeeded`.
    ///
    /// The Java contract:
    ///
    /// 1. Empty `initializing_partitions` ⇒ completed `Ok(())`.
    /// 2. Otherwise, if there is an in-flight `OffsetFetch` whose
    ///    requested-partition set matches the new request, **reuse it**:
    ///    register the caller's `oneshot::Sender` on the existing
    ///    pending event. Java:
    ///    `OffsetsRequestManager.canReusePendingOffsetFetchEvent`.
    /// 3. Otherwise, build a new `OffsetFetch` via the commit manager,
    ///    using `max(deadline_ms, current_time_ms + default_api_timeout_ms)`
    ///    as the request deadline so the OffsetFetch outlives the
    ///    triggering `updateFetchPositions` deadline if needed.
    /// 4. On fetch completion: apply offsets via [`Self::refresh_offsets`]
    ///    (mirrors Java's `refreshOffsets` + `ConsumerUtils.refreshCommittedOffsets`)
    ///    and fan the result out to every waiter on the pending event.
    pub(crate) fn init_with_committed_offsets_if_needed(
        &self,
        initializing_partitions: HashSet<TopicPartition>,
        deadline_ms: i64,
        current_time_ms: i64,
    ) -> oneshot::Receiver<Result<(), KafkaError>> {
        let (tx, rx) = oneshot::channel();
        if initializing_partitions.is_empty() {
            let _ = tx.send(Ok(()));
            return rx;
        }
        log::debug!("Refreshing committed offsets for partitions {:?}", initializing_partitions);

        // Reuse path — same partition set still in flight.
        {
            let mut guard = self
                .pending_offset_fetch_event
                .lock()
                .expect("pending_offset_fetch_event mutex poisoned");
            if let Some(pending) = guard.as_mut()
                && pending.requested_partitions == initializing_partitions
            {
                pending.waiters.push(tx);
                return rx;
            }
        }

        // No commit manager (e.g. consumer has no group): nothing to do,
        // resolve immediately. Java reaches the same outcome by
        // short-circuiting before `commitRequestManager.fetchOffsets` is
        // called.
        let Some(commit_rm) = self.commit_request_manager.as_ref() else {
            let _ = tx.send(Ok(()));
            return rx;
        };

        // New fetch path. The deadline is the later of the caller's
        // `deadlineMs` and the internal default-api-timeout slack so the
        // underlying OffsetFetch is not abandoned solely because the
        // triggering event's deadline is short.
        let fetch_committed_deadline_ms = deadline_ms.max(current_time_ms.saturating_add(self.default_api_timeout_ms));
        let inner_rx =
            commit_rm.fetch_offsets(initializing_partitions.clone(), fetch_committed_deadline_ms, current_time_ms);

        {
            let mut guard = self
                .pending_offset_fetch_event
                .lock()
                .expect("pending_offset_fetch_event mutex poisoned");
            *guard =
                Some(PendingFetchCommittedRequest { requested_partitions: initializing_partitions, waiters: vec![tx] });
        }

        // Drive the in-flight fetch: when it resolves, apply the offsets
        // to the subscription state and fan the result out to every
        // waiter that registered on this pending event.
        let pending_slot = Arc::clone(&self.pending_offset_fetch_event);
        let subscription_state = Arc::clone(&self.subscription_state);
        let metadata = Arc::clone(&self.metadata);
        tokio::spawn(async move {
            let fetch_result = match inner_rx.await {
                Ok(r) => r,
                Err(_) => Err(KafkaError::new(crate::common::protocol::Errors::NetworkException)),
            };

            // Take the waiters out of the pending slot and clear it
            // (Java: `pendingOffsetFetchEvent = null` inside the
            // `whenComplete` callback).
            let waiters = {
                let mut guard = pending_slot.lock().expect("pending_offset_fetch_event mutex poisoned");
                guard.take().map(|p| p.waiters).unwrap_or_default()
            };

            // Apply the committed offsets to the subscription state.
            let result_for_waiters = match fetch_result {
                Ok(offsets) => {
                    refresh_offsets(&offsets, subscription_state.as_ref(), metadata.as_ref());
                    Ok(())
                },
                Err(err) => {
                    log::error!("Error fetching committed offsets to update positions: {}", err);
                    Err(err)
                },
            };

            // Fan the result out to every waiter. Use `clone` because
            // `KafkaError` is cloneable but the `Result` we send is
            // by-value per sender.
            for waiter in waiters {
                let payload = match &result_for_waiters {
                    Ok(()) => Ok(()),
                    Err(err) => Err(err.clone()),
                };
                let _ = waiter.send(payload);
            }
        });

        rx
    }

    /// Drains pending completions and forwards them to the
    /// `OffsetFetcherUtilsState` handlers.
    fn drain_pending_completions(&mut self, current_time_ms: i64) -> Result<(), KafkaError> {
        let now_ms = current_time_ms;
        while let Ok(completion) = self.pending_completions_rx.try_recv() {
            match completion {
                PendingCompletion::ListOffsetsForReset { reset_timestamps, partition_strategies, result } => {
                    match result {
                        Ok(client_response) => {
                            if let Some(list_offsets_response) = downcast_list_offsets(&client_response) {
                                match self.offset_fetcher_utils.handle_list_offset_response(list_offsets_response) {
                                    Ok(result) => {
                                        // Reset-path: do NOT call update_subscription_state.
                                        // The fetched offsets are EARLIEST/LATEST per the reset
                                        // strategy, NOT HW/LSO — using them to update HW/LSO
                                        // would corrupt the lag metric. Java's reset path
                                        // (OffsetsRequestManager.java:613-650) does not call
                                        // updateSubscriptionState either; that's only for the
                                        // multi-node fetchOffsets flow (Java:570-573), which
                                        // is currently deferred.
                                        self.offset_fetcher_utils.on_successful_response_for_resetting_positions(
                                            &result,
                                            &partition_strategies,
                                            now_ms,
                                        )?;
                                    },
                                    Err(err) => {
                                        self.offset_fetcher_utils.on_failed_response_for_resetting_positions(
                                            &reset_timestamps,
                                            KafkaError::topic_authorization(err.unauthorized_topics.clone()),
                                            now_ms,
                                        );
                                    },
                                }
                            } else {
                                log::debug!("ListOffsets response had unexpected body type, ignoring");
                            }
                        },
                        Err(err) => {
                            self.offset_fetcher_utils.on_failed_response_for_resetting_positions(
                                &reset_timestamps,
                                err,
                                now_ms,
                            );
                        },
                    }
                },
                PendingCompletion::OffsetsForLeaderEpoch { fetch_positions, result } => match result {
                    Ok(client_response) => {
                        if let Some(response) = downcast_offsets_for_leader_epoch(&client_response) {
                            match OffsetsForLeaderEpochClient::handle_response(&fetch_positions, response) {
                                Ok(epoch_result) => {
                                    let truncations =
                                        self.offset_fetcher_utils.on_successful_response_for_validating_positions(
                                            &fetch_positions,
                                            &epoch_result,
                                            now_ms,
                                        );
                                    if !truncations.is_empty() {
                                        let fetch_offsets: HashMap<TopicPartition, i64> = truncations
                                            .iter()
                                            .map(|t| (t.topic_partition.clone(), t.fetch_position.offset))
                                            .collect();
                                        let divergent_offsets: HashMap<
                                            TopicPartition,
                                            crate::consumer::OffsetAndMetadata,
                                        > = truncations
                                            .iter()
                                            .filter_map(|t| {
                                                t.divergent_offset_opt
                                                    .as_ref()
                                                    .map(|d| (t.topic_partition.clone(), d.clone()))
                                            })
                                            .collect();
                                        let log_truncation =
                                            KafkaError::from(crate::consumer::errors::ConsumerError::log_truncation(
                                                fetch_offsets,
                                                divergent_offsets,
                                            ));
                                        self.offset_fetcher_utils.maybe_set_validate_error(log_truncation);
                                    }
                                },
                                Err(err) => {
                                    self.offset_fetcher_utils.on_failed_response_for_validating_positions(
                                        &fetch_positions,
                                        KafkaError::topic_authorization(err.unauthorized_topics.clone()),
                                        now_ms,
                                    );
                                },
                            }
                        } else {
                            log::debug!("OffsetsForLeaderEpoch response had unexpected body type, ignoring");
                        }
                    },
                    Err(err) => {
                        self.offset_fetcher_utils.on_failed_response_for_validating_positions(
                            &fetch_positions,
                            err,
                            now_ms,
                        );
                    },
                },
            }
        }
        Ok(())
    }
}

/// Helper to downcast a `ClientResponse` to a `ListOffsetsResponse`.
fn downcast_list_offsets(response: &ClientResponse) -> Option<&crate::common::requests::ListOffsetsResponse> {
    match response.response_body() {
        Some(ConcreteResponse::ListOffsets(r)) => Some(r),
        _ => None,
    }
}

/// Helper to downcast a `ClientResponse` to an `OffsetsForLeaderEpochResponse`.
fn downcast_offsets_for_leader_epoch(response: &ClientResponse) -> Option<&OffsetsForLeaderEpochResponse> {
    match response.response_body() {
        Some(ConcreteResponse::OffsetsForLeaderEpoch(r)) => Some(r),
        _ => None,
    }
}

/// Apply committed offsets to the subscription state for partitions
/// that are still initializing. Mirrors Java's
/// `ConsumerUtils.refreshCommittedOffsets` composed with
/// `OffsetsRequestManager.offsetsForInitializingPartitions` (filter to
/// initializing) and `OffsetsRequestManager.refreshOffsets` (success
/// branch).
///
/// Filtering ensures we do not overwrite a position that the user set
/// manually via `seek` between the time the OffsetFetch was issued and
/// the response arrived; Java does the same in
/// `OffsetsRequestManager.offsetsForInitializingPartitions`.
fn refresh_offsets(
    offsets: &HashMap<TopicPartition, Option<OffsetAndMetadata>>,
    subscription_state: &Mutex<SubscriptionState>,
    metadata: &ConsumerMetadata,
) {
    // Snapshot the currently initializing partitions before acquiring
    // the write lock (Java reads via subscriptionState.initializingPartitions()).
    let currently_initializing = {
        let guard = subscription_state.lock().expect("SubscriptionState mutex poisoned");
        guard.initializing_partitions()
    };

    let metadata_arc = metadata.metadata_arc();
    let mut subs = subscription_state.lock().expect("SubscriptionState mutex poisoned");
    for (tp, oam) in offsets {
        let Some(oam) = oam.as_ref() else {
            // No committed offset for this partition — Java's
            // `refreshCommittedOffsets` skips `null` values.
            continue;
        };
        if !currently_initializing.contains(tp) {
            continue;
        }
        // Update the last-seen epoch first, then seek if still assigned.
        if let Some(epoch) = oam.leader_epoch() {
            let _ = metadata_arc.update_last_seen_epoch_if_newer(tp, epoch);
        }
        if !subs.is_assigned(tp) {
            // Partition was unassigned between request and response —
            // skip the seek (Java: same isAssigned guard).
            continue;
        }
        let leader_and_epoch = metadata_arc.current_leader(tp);
        let position = FetchPosition::with_leader(oam.offset(), oam.leader_epoch(), leader_and_epoch);
        let _ = subs.seek_unvalidated(tp, position);
    }
}

impl RequestManager for OffsetsRequestManager {
    /// Drains completed responses then returns the queued
    /// requests. Java: `poll(long currentTimeMs)`.
    fn poll(&mut self, current_time_ms: i64) -> PollResult {
        // Drain any completions first, applying success/failure handlers.
        // Errors here are stored on the OffsetFetcherUtilsState for the
        // next call to surface; we don't propagate them through `poll`
        // because Java's poll signature returns only a `PollResult`.
        if let Err(err) = self.drain_pending_completions(current_time_ms) {
            log::error!("Error draining pending offset completions: {}", err);
        }
        let unsent = std::mem::take(&mut self.requests_to_send);
        if unsent.is_empty() {
            PollResult::empty()
        } else {
            PollResult::with_requests(unsent)
        }
    }

    fn signal_close(&mut self) {
        self.closing = true;
    }
}

/// Stateless `ClusterResourceListener` registered on the consumer's
/// `Metadata`. The full Java implementation re-issues deferred
/// `ListOffsets` requests on metadata change; this translation defers
/// the request-replay path until `fetch_offsets` is translated (it
/// depends on `CommitRequestManager`, out of scope for Phase 7d).
struct OffsetsClusterListener {}

impl ClusterResourceListener for OffsetsClusterListener {
    fn on_update(&self, _cluster_resource: &ClusterResource) {
        // No-op: the deferred-request replay logic from Java's
        // `onUpdate` lives on the `requests_to_retry` queue, which this
        // translation does not yet model. Once `fetch_offsets` (Phase
        // 8+) lands and starts producing retry entries, this listener
        // MUST drain them and re-prepare requests.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_versions::ApiVersions;
    use crate::common::internals::ClusterResourceListeners;
    use crate::consumer::ConsumerConfig;
    use crate::consumer::internals::subscription_state::SubscriptionState;

    /// Returns a fresh manager wired against a no-op metadata / subscription state.
    fn new_manager() -> OffsetsRequestManager {
        let config = ConsumerConfig::from_properties(&std::collections::HashMap::from([
            ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
            ("group.id".to_string(), "g".to_string()),
        ]))
        .expect("config");
        let subscription_state = Arc::new(Mutex::new(SubscriptionState::new(
            crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy::EARLIEST,
        )));
        let metadata =
            ConsumerMetadata::from_config(&config, subscription_state.clone(), ClusterResourceListeners::new());
        OffsetsRequestManager::new(
            subscription_state,
            Arc::new(metadata),
            IsolationLevel::ReadUncommitted,
            100,
            30_000,
            60_000,
            Arc::new(ApiVersions::new()),
            None,
        )
    }

    /// Verifies that a freshly-constructed manager has nothing to poll.
    #[tokio::test]
    async fn empty_manager_poll_returns_empty() {
        let mut mgr = new_manager();
        let result = RequestManager::poll(&mut mgr, 0);
        assert_eq!(result.time_until_next_poll_ms, PollResult::WAIT_FOREVER);
        assert!(result.unsent_requests.is_empty());
    }

    /// Verifies that `reset_positions_if_needed` is a no-op when no
    /// partition is awaiting reset.
    #[tokio::test]
    async fn reset_positions_if_needed_with_no_resets_is_noop() {
        let mut mgr = new_manager();
        let result = mgr.reset_positions_if_needed(0);
        assert!(result.is_ok());
        assert_eq!(RequestManager::poll(&mut mgr, 0).unsent_requests.len(), 0);
    }

    /// Verifies that `validate_positions_if_needed` is a no-op when no
    /// partition needs validation.
    #[tokio::test]
    async fn validate_positions_if_needed_with_nothing_to_validate_is_noop() {
        let mut mgr = new_manager();
        let result = mgr.validate_positions_if_needed(0);
        assert!(result.is_ok());
        assert_eq!(RequestManager::poll(&mut mgr, 0).unsent_requests.len(), 0);
    }

    /// Behavior-equivalent of Java's
    /// `testResetPositionsSendNoRequestIfNoPartitionsNeedingReset` — no
    /// partition needs reset, so no requests are enqueued.
    ///
    /// Java uses a Mockito stub on `subscriptionState.partitionsNeedingReset`
    /// to return an empty set; the Rust translation uses the real
    /// `SubscriptionState` and gets the same outcome by not assigning
    /// any partitions awaiting reset.
    #[tokio::test]
    async fn reset_positions_send_no_request_if_no_partitions_needing_reset() {
        let mut mgr = new_manager();
        // No partitions are assigned, so partitions_needing_reset returns an empty set.
        mgr.reset_positions_if_needed(0).expect("ok");
        assert_eq!(RequestManager::poll(&mut mgr, 0).unsent_requests.len(), 0);
    }

    /// Verifies `signal_close` flips the `closing` flag so subsequent
    /// `poll` calls can choose to short-circuit. Java does not have a
    /// direct test for this — it's exercised implicitly through the bg
    /// task shutdown path — but pinning the behaviour here protects the
    /// translation contract.
    #[tokio::test]
    async fn signal_close_sets_closing_flag() {
        let mut mgr = new_manager();
        assert!(!mgr.closing);
        RequestManager::signal_close(&mut mgr);
        assert!(mgr.closing);
    }

    // -----------------------------------------------------------------
    //   init_with_committed_offsets_if_needed (relocated from
    //   CommitRequestManager — Phase 10 commit 3a/N)
    // -----------------------------------------------------------------

    use crate::consumer::OffsetAndMetadata;
    use crate::consumer::internals::commit_request_manager::CommitRequestManager;
    use crate::metadata::LeaderAndEpoch;

    /// Build an `OffsetsRequestManager` plus a backing
    /// `CommitRequestManager` so the init-with-committed path can be
    /// exercised end-to-end (the spawned driver task expects a real
    /// commit manager to fulfil the fetch oneshot).
    fn new_manager_with_commit() -> (OffsetsRequestManager, Arc<CommitRequestManager>, Arc<Mutex<SubscriptionState>>) {
        let config = ConsumerConfig::from_properties(&std::collections::HashMap::from([
            ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
            ("group.id".to_string(), "g".to_string()),
        ]))
        .expect("config");
        let subscription_state = Arc::new(Mutex::new(SubscriptionState::new(
            crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy::EARLIEST,
        )));
        let metadata = Arc::new(ConsumerMetadata::from_config(
            &config,
            subscription_state.clone(),
            ClusterResourceListeners::new(),
        ));
        let commit_rm = Arc::new(CommitRequestManager::new(
            &config,
            metadata.clone(),
            subscription_state.clone(),
            "g",
            None,
            0,
        ));
        let mgr = OffsetsRequestManager::new(
            subscription_state.clone(),
            metadata,
            IsolationLevel::ReadUncommitted,
            100,
            30_000,
            60_000,
            Arc::new(ApiVersions::new()),
            Some(commit_rm.clone()),
        );
        (mgr, commit_rm, subscription_state)
    }

    /// Java parity:
    /// `OffsetsRequestManager.initWithCommittedOffsetsIfNeeded` returns
    /// `CompletableFuture.completedFuture(null)` when the
    /// initializing-partition set is empty.
    #[tokio::test(flavor = "current_thread")]
    async fn init_with_committed_offsets_empty_partitions_resolves_immediately() {
        let mgr = new_manager();
        let rx = mgr.init_with_committed_offsets_if_needed(HashSet::new(), i64::MAX, 0);
        let result = rx.await.expect("oneshot");
        assert!(result.is_ok());
    }

    /// Java models the commit manager as a constructor-required
    /// argument, but the
    /// `initWithCommittedOffsetsIfNeeded` chain effectively short-circuits
    /// when there is nothing to fetch from. The Rust translation collapses
    /// "no group" into `commit_request_manager = None`, and the method
    /// resolves immediately with `Ok(())`.
    #[tokio::test(flavor = "current_thread")]
    async fn init_with_committed_offsets_no_commit_manager_resolves_immediately() {
        let mgr = new_manager(); // no commit manager
        let mut partitions = HashSet::new();
        partitions.insert(TopicPartition::new("t".to_string(), 0));
        let rx = mgr.init_with_committed_offsets_if_needed(partitions, i64::MAX, 0);
        let result = rx.await.expect("oneshot");
        assert!(result.is_ok());
    }

    /// First call with non-empty initializing partitions issues an
    /// `OffsetFetch` request on the commit manager (visible as an entry
    /// on the commit manager's `unsent_offset_fetches` queue).
    #[tokio::test(flavor = "current_thread")]
    async fn init_with_committed_offsets_enqueues_fetch_offsets_request() {
        let (mgr, commit_rm, _) = new_manager_with_commit();
        let mut partitions = HashSet::new();
        partitions.insert(TopicPartition::new("t".to_string(), 0));
        let _rx = mgr.init_with_committed_offsets_if_needed(partitions.clone(), i64::MAX, 0);
        // The commit manager's pending queue should have one fetch
        // request enqueued by the spawned chain.
        let guard = commit_rm.inner_state_for_test();
        assert_eq!(guard, 1, "fetch_offsets should have enqueued one OffsetFetch request");
    }

    /// Java parity:
    /// `OffsetsRequestManager.canReusePendingOffsetFetchEvent` returns
    /// true when the partition set matches an in-flight fetch. A second
    /// call with the same partitions must NOT issue another
    /// `OffsetFetch` — both callers wait on the same underlying request.
    #[tokio::test(flavor = "current_thread")]
    async fn init_with_committed_offsets_reuses_pending_fetch() {
        let (mgr, commit_rm, _) = new_manager_with_commit();
        let mut partitions = HashSet::new();
        partitions.insert(TopicPartition::new("t".to_string(), 0));

        let _rx1 = mgr.init_with_committed_offsets_if_needed(partitions.clone(), i64::MAX, 0);
        let _rx2 = mgr.init_with_committed_offsets_if_needed(partitions.clone(), i64::MAX, 0);

        // Only one OffsetFetch should have been enqueued — the second
        // call piggy-backs on the pending event.
        let guard = commit_rm.inner_state_for_test();
        assert_eq!(
            guard, 1,
            "second call should reuse the pending OffsetFetch, not issue a new one"
        );
    }

    /// Java parity: a second call with a DIFFERENT partition set bypasses
    /// the reuse path (`canReusePendingOffsetFetchEvent` returns false)
    /// and issues a new `OffsetFetch`. Note that Java's exact behavior
    /// here is to overwrite the pending event slot; the new request
    /// replaces the old one. We accept the same: the new fetch is
    /// enqueued.
    #[tokio::test(flavor = "current_thread")]
    async fn init_with_committed_offsets_does_not_reuse_for_different_partitions() {
        let (mgr, commit_rm, _) = new_manager_with_commit();
        let mut partitions1 = HashSet::new();
        partitions1.insert(TopicPartition::new("t".to_string(), 0));
        let mut partitions2 = HashSet::new();
        partitions2.insert(TopicPartition::new("t".to_string(), 1));

        let _rx1 = mgr.init_with_committed_offsets_if_needed(partitions1, i64::MAX, 0);
        let _rx2 = mgr.init_with_committed_offsets_if_needed(partitions2, i64::MAX, 0);

        let guard = commit_rm.inner_state_for_test();
        assert_eq!(guard, 2, "different partition sets should each issue their own OffsetFetch");
    }

    /// Java parity:
    /// `ConsumerUtils.refreshCommittedOffsets` — for each entry in the
    /// offsets map that maps to a currently-assigned, currently-initializing
    /// partition, set the position via `seek_unvalidated`. The Rust
    /// helper is a free function so we can drive it independently of the
    /// in-flight fetch.
    #[tokio::test(flavor = "current_thread")]
    async fn refresh_offsets_seeks_unvalidated_for_initializing_assigned_partitions() {
        let (_mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        let tp = TopicPartition::new("t".to_string(), 0);

        // Assign tp without a position so it is "initializing".
        {
            let mut subs = subscription_state.lock().unwrap();
            let mut set = HashSet::new();
            set.insert(tp.clone());
            subs.assign_from_user(set).expect("assign");
            assert!(subs.initializing_partitions().contains(&tp));
            assert!(subs.is_assigned(&tp));
        }

        let metadata = Arc::new(ConsumerMetadata::from_config(
            &ConsumerConfig::from_properties(&std::collections::HashMap::from([
                ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
                ("group.id".to_string(), "g".to_string()),
            ]))
            .expect("config"),
            subscription_state.clone(),
            ClusterResourceListeners::new(),
        ));

        let mut offsets: HashMap<TopicPartition, Option<OffsetAndMetadata>> = HashMap::new();
        offsets.insert(tp.clone(), Some(OffsetAndMetadata::new(10).expect("offset metadata")));

        super::refresh_offsets(&offsets, subscription_state.as_ref(), metadata.as_ref());

        // The position should now be set to offset 10.
        let subs = subscription_state.lock().unwrap();
        let position = subs.position(&tp).expect("position lookup").expect("position present");
        assert_eq!(position.offset, 10);
    }

    /// Java parity:
    /// `OffsetsRequestManager.offsetsForInitializingPartitions` — filter
    /// out offsets for partitions that are no longer initializing (e.g.
    /// the user called `seek` manually between request and response).
    #[tokio::test(flavor = "current_thread")]
    async fn refresh_offsets_skips_partitions_no_longer_initializing() {
        let (_mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        let tp = TopicPartition::new("t".to_string(), 0);

        // Assign tp AND give it a manual position — it is no longer
        // initializing.
        {
            let mut subs = subscription_state.lock().unwrap();
            let mut set = HashSet::new();
            set.insert(tp.clone());
            subs.assign_from_user(set).expect("assign");
            let pos = FetchPosition::with_leader(99, None, LeaderAndEpoch::no_leader_or_epoch());
            subs.seek_unvalidated(&tp, pos).expect("seek");
            assert!(!subs.initializing_partitions().contains(&tp));
        }

        let metadata = Arc::new(ConsumerMetadata::from_config(
            &ConsumerConfig::from_properties(&std::collections::HashMap::from([
                ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
                ("group.id".to_string(), "g".to_string()),
            ]))
            .expect("config"),
            subscription_state.clone(),
            ClusterResourceListeners::new(),
        ));

        let mut offsets: HashMap<TopicPartition, Option<OffsetAndMetadata>> = HashMap::new();
        offsets.insert(tp.clone(), Some(OffsetAndMetadata::new(10).expect("offset metadata")));
        super::refresh_offsets(&offsets, subscription_state.as_ref(), metadata.as_ref());

        // The position should NOT have been overwritten to 10.
        let subs = subscription_state.lock().unwrap();
        let position = subs.position(&tp).expect("position lookup").expect("position present");
        assert_eq!(position.offset, 99);
    }
}
