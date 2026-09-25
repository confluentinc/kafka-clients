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

//! Internal consumer types (org.apache.kafka.clients.consumer.internals).
//!
//! Per CLAUDE.md §2, types in this module use `pub(crate)` visibility.
//! Stable user-facing re-exports live in `crate::consumer`.

mod abstract_fetch;
mod abstract_heartbeat_request_manager;
pub(crate) mod abstract_membership_manager;
mod async_consumer_metrics;
pub(crate) mod auto_offset_reset_strategy;
pub(crate) mod commit_request_manager;
mod completed_fetch;
mod consumer_heartbeat_request_manager;
mod consumer_interceptors;
mod consumer_membership_manager;
mod consumer_metadata;
mod consumer_network_thread;
mod consumer_protocol;
pub(crate) use consumer_protocol::ConsumerProtocol;
mod consumer_rebalance_listener_invoker;
mod consumer_rebalance_metrics_manager;
mod consumer_utils;
mod coordinator_request_manager;
mod deserializers;
pub(crate) mod events;
mod fetch_buffer;
mod fetch_collector;
mod fetch_config;
mod fetch_metrics_aggregator;
mod fetch_metrics_manager;
mod fetch_metrics_registry;
mod fetch_request_manager;
mod fetch_utils;
mod heartbeat_metrics_manager;
mod heartbeat_request_state;
mod kafka_consumer_metrics;
mod member_state;
mod member_state_listener;
pub(crate) mod network_client_delegate;
mod offset_and_timestamp_internal;
mod offset_commit_callback_invoker;
mod offset_commit_metrics_manager;
pub(crate) mod offset_fetcher_utils;
mod offsets_for_leader_epoch_client;
pub(crate) mod offsets_request_manager;
mod positions_validator;
mod rebalance_callback_metrics_manager;
mod request_manager;
mod request_managers;
mod request_state;
mod sensor_builder;
pub(crate) mod subscription_state;
mod timed_request_state;
pub(crate) mod topic_metadata_request_manager;
mod wakeup_trigger;

// `pub use`, not `pub(crate) use`: `AutoOffsetResetStrategy` / `StrategyType` are
// public API (`consumer::AutoOffsetResetStrategy`, consumer-threading.md §20) even
// though they live in an `internals` package, so the re-export must carry that
// visibility for `consumer/mod.rs` to forward it.
pub(crate) use abstract_fetch::AbstractFetch;
pub(crate) use abstract_heartbeat_request_manager::{
    AbstractHeartbeatRequestManager, HeartbeatErrorAction, HeartbeatFailureAction,
};
pub(crate) use abstract_membership_manager::{AbstractMembershipManager, LocalAssignment};
pub(crate) use async_consumer_metrics::AsyncConsumerMetrics;
pub use auto_offset_reset_strategy::{AutoOffsetResetStrategy, StrategyType};
pub(crate) use commit_request_manager::CommitRequestManager;
pub(crate) use completed_fetch::CompletedFetch;
pub(crate) use consumer_heartbeat_request_manager::ConsumerHeartbeatRequestManager;
pub(crate) use consumer_interceptors::ConsumerInterceptors;
pub(crate) use consumer_membership_manager::ConsumerMembershipManager;
pub(crate) use consumer_metadata::ConsumerMetadata;
pub(crate) use consumer_network_thread::{ConsumerNetworkThread, SystemThreadTime, ThreadTime};
pub(crate) use consumer_rebalance_listener_invoker::ConsumerRebalanceListenerInvoker;
pub(crate) use consumer_rebalance_metrics_manager::ConsumerRebalanceMetricsManager;
pub(crate) use consumer_utils::ConsumerUtils;
pub(crate) use coordinator_request_manager::CoordinatorRequestManager;
pub(crate) use deserializers::Deserializers;
pub(crate) use fetch_buffer::FetchBuffer;
pub(crate) use fetch_collector::{FetchCollector, FetchCollectorTime, SystemFetchCollectorTime};
pub(crate) use fetch_config::FetchConfig;
pub(crate) use fetch_metrics_aggregator::FetchMetricsAggregator;
pub(crate) use fetch_metrics_manager::FetchMetricsManager;
pub(crate) use fetch_metrics_registry::FetchMetricsRegistry;
pub(crate) use fetch_request_manager::{FetchRequestManager, IsUnavailableFn, MaybeAuthFailureFn};
pub(crate) use fetch_utils::FetchUtils;
pub(crate) use heartbeat_metrics_manager::HeartbeatMetricsManager;
pub(crate) use heartbeat_request_state::HeartbeatRequestState;
pub(crate) use kafka_consumer_metrics::KafkaConsumerMetrics;
pub(crate) use member_state::MemberState;
pub(crate) use member_state_listener::MemberStateListener;
pub(crate) use network_client_delegate::{NetworkClientDelegate, PollResult, UnsentRequest};
pub(crate) use offset_and_timestamp_internal::OffsetAndTimestampInternal;
pub(crate) use offset_commit_callback_invoker::{AutoCommitInterceptorHook, OffsetCommitCallbackInvoker};
pub(crate) use offset_commit_metrics_manager::OffsetCommitMetricsManager;
pub(crate) use offset_fetcher_utils::{ListOffsetData, ListOffsetResult, OffsetFetcherUtils};
pub(crate) use offsets_for_leader_epoch_client::{OffsetForEpochResult, OffsetsForLeaderEpochClient};
pub(crate) use offsets_request_manager::OffsetsRequestManager;
pub(crate) use positions_validator::PositionsValidator;
pub(crate) use rebalance_callback_metrics_manager::RebalanceCallbackMetricsManager;
pub(crate) use request_manager::RequestManager;
pub(crate) use request_managers::RequestManagers;
pub(crate) use request_state::RequestState;
pub(crate) use sensor_builder::SensorBuilder;
pub(crate) use subscription_state::{FetchPosition, LogTruncation, SubscriptionState};
pub(crate) use timed_request_state::TimedRequestState;
pub(crate) use topic_metadata_request_manager::TopicMetadataRequestManager;
pub(crate) use wakeup_trigger::WakeupTrigger;
