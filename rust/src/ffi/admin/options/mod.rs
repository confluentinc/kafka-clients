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

//! C bindings for `org.apache.kafka.clients.admin.*Options` (CLAUDE.md §4): one
//! `kafka_admin_<Rpc>Options_t` per Java class, re-exported by `crate::admin::options`
//! on the Rust side and declared here file by file to mirror it.

pub(crate) mod abort_transaction_options;
pub(crate) mod alter_client_quotas_options;
pub(crate) mod alter_configs_options;
pub(crate) mod alter_consumer_group_offsets_options;
pub(crate) mod alter_partition_reassignments_options;
pub(crate) mod alter_replica_log_dirs_options;
pub(crate) mod alter_user_scram_credentials_options;
pub(crate) mod create_acls_options;
pub(crate) mod create_delegation_token_options;
pub(crate) mod create_partitions_options;
pub(crate) mod create_topics_options;
pub(crate) mod delete_acls_options;
pub(crate) mod delete_consumer_group_offsets_options;
pub(crate) mod delete_consumer_groups_options;
pub(crate) mod delete_records_options;
pub(crate) mod delete_topics_options;
pub(crate) mod describe_acls_options;
pub(crate) mod describe_classic_groups_options;
pub(crate) mod describe_client_quotas_options;
pub(crate) mod describe_cluster_options;
pub(crate) mod describe_configs_options;
pub(crate) mod describe_consumer_groups_options;
pub(crate) mod describe_delegation_token_options;
pub(crate) mod describe_features_options;
pub(crate) mod describe_log_dirs_options;
pub(crate) mod describe_producers_options;
pub(crate) mod describe_replica_log_dirs_options;
pub(crate) mod describe_topics_options;
pub(crate) mod describe_transactions_options;
pub(crate) mod describe_user_scram_credentials_options;
pub(crate) mod elect_leaders_options;
pub(crate) mod expire_delegation_token_options;
pub(crate) mod fence_producers_options;
pub(crate) mod list_config_resources_options;
pub(crate) mod list_consumer_group_offsets_options;
pub(crate) mod list_groups_options;
pub(crate) mod list_offsets_options;
pub(crate) mod list_partition_reassignments_options;
pub(crate) mod list_topics_options;
pub(crate) mod list_transactions_options;
pub(crate) mod remove_members_from_consumer_group_options;
pub(crate) mod renew_delegation_token_options;
pub(crate) mod terminate_transaction_options;
pub(crate) mod update_features_options;
