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

//! Internal machinery for the admin client.
//!
//! Corresponds to `org.apache.kafka.clients.admin.internals` plus the private
//! `Call` / `AdminClientRunnable` machinery inside `KafkaAdminClient`. All of it
//! is `pub(crate)` per the `internal`-package naming rule.

mod abort_transaction_handler;
pub(crate) mod admin_api_driver;
pub(crate) mod admin_api_future;
pub(crate) mod admin_api_handler;
pub(crate) mod admin_api_lookup_strategy;
mod admin_client_runnable;
mod admin_metadata_manager;
mod admin_utils;
pub(crate) mod all_brokers_strategy;
mod alter_consumer_group_offsets_handler;
mod api_request_scope;
mod call;
mod coordinator_key;
mod coordinator_strategy;
mod delete_consumer_group_offsets_handler;
mod delete_consumer_groups_handler;
mod delete_groups_handler;
mod delete_records_handler;
mod describe_classic_groups_handler;
mod describe_consumer_groups_handler;
mod describe_producers_handler;
mod describe_transactions_handler;
mod fence_producers_handler;
mod list_consumer_group_offsets_handler;
mod list_offsets_handler;
mod list_transactions_handler;
mod partition_leader_cache;
pub(crate) mod partition_leader_strategy;
#[cfg(test)]
mod partition_leader_strategy_integration_test;
mod remove_members_from_consumer_group_handler;
mod static_broker_strategy;

pub(crate) use abort_transaction_handler::AbortTransactionHandler;
pub(crate) use admin_api_driver::{AdminApiDriver, RequestSpec};
// `UNKNOWN_BROKER_ID` cannot become `AdminApiFuture::UNKNOWN_BROKER_ID` (Java's
// `AdminApiFuture.java:30`, an interface field): an associated const makes a
// trait not dyn-compatible (E0038) and `AdminApiDriver` holds
// `Box<dyn AdminApiFuture<K, V>>`. It stays module-level and is reached through
// this re-export instead.
pub(crate) use admin_api_future::{AdminApiFuture, SimpleAdminApiFuture, UNKNOWN_BROKER_ID};
pub(crate) use admin_api_handler::{AdminApiHandler, ApiResult, RequestAndKeys};
pub(crate) use admin_api_lookup_strategy::{AdminApiLookupStrategy, LookupResult};
pub(crate) use admin_client_runnable::{AdminClientRunnable, ShutdownSignal};
pub(crate) use admin_metadata_manager::AdminMetadataManager;
pub(crate) use admin_utils::AdminUtils;
pub(crate) use all_brokers_strategy::{AllBrokersFuture, AllBrokersStrategy, BrokerKey};
pub(crate) use alter_consumer_group_offsets_handler::AlterConsumerGroupOffsetsHandler;
pub(crate) use api_request_scope::ApiRequestScope;
pub(crate) use call::{Call, HandleResult, MaybeRetryOutcome, NodeProvider};
pub(crate) use coordinator_key::CoordinatorKey;
pub(crate) use coordinator_strategy::CoordinatorStrategy;
pub(crate) use delete_consumer_group_offsets_handler::DeleteConsumerGroupOffsetsHandler;
pub(crate) use delete_consumer_groups_handler::DeleteConsumerGroupsHandler;
pub(crate) use delete_groups_handler::DeleteGroupsHandler;
pub(crate) use delete_records_handler::DeleteRecordsHandler;
pub(crate) use describe_classic_groups_handler::DescribeClassicGroupsHandler;
pub(crate) use describe_consumer_groups_handler::DescribeConsumerGroupsHandler;
pub(crate) use describe_producers_handler::DescribeProducersHandler;
pub(crate) use describe_transactions_handler::DescribeTransactionsHandler;
pub(crate) use fence_producers_handler::FenceProducersHandler;
pub(crate) use list_consumer_group_offsets_handler::ListConsumerGroupOffsetsHandler;
pub(crate) use list_offsets_handler::ListOffsetsHandler;
pub(crate) use list_transactions_handler::ListTransactionsHandler;
pub(crate) use partition_leader_cache::PartitionLeaderCache;
pub(crate) use partition_leader_strategy::{PartitionLeaderFuture, PartitionLeaderStrategy};
pub(crate) use remove_members_from_consumer_group_handler::RemoveMembersFromConsumerGroupHandler;
pub(crate) use static_broker_strategy::StaticBrokerStrategy;
