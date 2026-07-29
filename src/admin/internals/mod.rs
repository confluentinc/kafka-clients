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

pub(crate) mod admin_api_driver;
pub(crate) mod admin_api_future;
pub(crate) mod admin_api_handler;
pub(crate) mod admin_api_lookup_strategy;
pub(crate) mod admin_client_runnable;
pub(crate) mod admin_metadata_manager;
pub(crate) mod admin_utils;
pub(crate) mod alter_consumer_group_offsets_handler;
pub(crate) mod api_request_scope;
pub(crate) mod call;
pub(crate) mod coordinator_key;
pub(crate) mod coordinator_strategy;
pub(crate) mod delete_consumer_group_offsets_handler;
pub(crate) mod delete_consumer_groups_handler;
pub(crate) mod delete_groups_handler;
pub(crate) mod delete_records_handler;
pub(crate) mod describe_classic_groups_handler;
pub(crate) mod describe_consumer_groups_handler;
pub(crate) mod list_consumer_group_offsets_handler;
pub(crate) mod list_offsets_handler;
pub(crate) mod partition_leader_cache;
pub(crate) mod partition_leader_strategy;
pub(crate) mod remove_members_from_consumer_group_handler;
