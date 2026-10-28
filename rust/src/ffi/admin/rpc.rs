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

//! The `Admin` RPC invokers: one `kafka_admin_Admin_<method>` per method of
//! the Rust `Admin` trait (CLAUDE.md §4 rule 9), excluding `close`, which
//! lives in `crate::ffi::admin` with the callback pump.
//!
//! Every RPC is non-blocking on both sides (`admin-client.md` §1): the
//! function returns the Java `*Result` handle at once and the caller awaits
//! the `kafka_common_KafkaFuture_t`s it exposes. Inputs are borrowed and
//! copied during the call; `kafka_List_t` / `kafka_Map_t` element types are
//! those of the Rust signature (a `kafka_List_t` of
//! `const kafka_admin_NewTopic_t *` for `&[NewTopic]`, a `kafka_Map_t` of
//! `const char *` to `const kafka_admin_NewPartitions_t *` for
//! `&HashMap<String, NewPartitions>`, a `kafka_Map_t` of
//! `const kafka_common_config_ConfigResource_t *` to `kafka_List_t *` of
//! `const kafka_admin_AlterConfigOp_t *` for `incremental_alter_configs`, ...),
//! scalars boxed (`const int32_t *` for `&[i32]`), `NULL` standing for a Rust
//! `None` (the optional partition set of `elect_leaders` and
//! `list_partition_reassignments_with_partitions_options`, a `None` value in
//! `alter_partition_reassignments`). An options pointer is the
//! `kafka_admin_<Rpc>Options_t` of the RPC, copied as well. A null list or
//! map where the Rust side takes a non-optional collection reads as empty.

use std::collections::{HashMap, HashSet};
use std::ffi::{c_char, c_void};

use crate::common::TopicPartition;
use crate::ffi::admin::abort_transaction_spec::{abort_transaction_spec_ref, kafka_admin_AbortTransactionSpec_t};
use crate::ffi::admin::alter_config_op::alter_config_op_ref;
use crate::ffi::admin::feature_update::feature_update_ref;
use crate::ffi::admin::list_consumer_group_offsets_spec::list_consumer_group_offsets_spec_ref;
use crate::ffi::admin::new_partition_reassignment::new_partition_reassignment_ref;
use crate::ffi::admin::new_partitions::new_partitions_ref;
use crate::ffi::admin::new_topic::new_topic_ref;
use crate::ffi::admin::offset_spec::offset_spec_value_of;
use crate::ffi::admin::options::{
    abort_transaction_options::{abort_transaction_options_ref, kafka_admin_AbortTransactionOptions_t},
    alter_client_quotas_options::{alter_client_quotas_options_ref, kafka_admin_AlterClientQuotasOptions_t},
    alter_configs_options::{alter_configs_options_ref, kafka_admin_AlterConfigsOptions_t},
    alter_consumer_group_offsets_options::{
        alter_consumer_group_offsets_options_ref, kafka_admin_AlterConsumerGroupOffsetsOptions_t,
    },
    alter_partition_reassignments_options::{
        alter_partition_reassignments_options_ref, kafka_admin_AlterPartitionReassignmentsOptions_t,
    },
    alter_replica_log_dirs_options::{alter_replica_log_dirs_options_ref, kafka_admin_AlterReplicaLogDirsOptions_t},
    alter_user_scram_credentials_options::{
        alter_user_scram_credentials_options_ref, kafka_admin_AlterUserScramCredentialsOptions_t,
    },
    create_acls_options::{create_acls_options_ref, kafka_admin_CreateAclsOptions_t},
    create_delegation_token_options::{
        create_delegation_token_options_ref, kafka_admin_CreateDelegationTokenOptions_t,
    },
    create_partitions_options::{create_partitions_options_ref, kafka_admin_CreatePartitionsOptions_t},
    create_topics_options::{create_topics_options_ref, kafka_admin_CreateTopicsOptions_t},
    delete_acls_options::{delete_acls_options_ref, kafka_admin_DeleteAclsOptions_t},
    delete_consumer_group_offsets_options::{
        delete_consumer_group_offsets_options_ref, kafka_admin_DeleteConsumerGroupOffsetsOptions_t,
    },
    delete_consumer_groups_options::{delete_consumer_groups_options_ref, kafka_admin_DeleteConsumerGroupsOptions_t},
    delete_records_options::{delete_records_options_ref, kafka_admin_DeleteRecordsOptions_t},
    delete_topics_options::{delete_topics_options_ref, kafka_admin_DeleteTopicsOptions_t},
    describe_acls_options::{describe_acls_options_ref, kafka_admin_DescribeAclsOptions_t},
    describe_classic_groups_options::{
        describe_classic_groups_options_ref, kafka_admin_DescribeClassicGroupsOptions_t,
    },
    describe_client_quotas_options::{describe_client_quotas_options_ref, kafka_admin_DescribeClientQuotasOptions_t},
    describe_cluster_options::{describe_cluster_options_ref, kafka_admin_DescribeClusterOptions_t},
    describe_configs_options::{describe_configs_options_ref, kafka_admin_DescribeConfigsOptions_t},
    describe_consumer_groups_options::{
        describe_consumer_groups_options_ref, kafka_admin_DescribeConsumerGroupsOptions_t,
    },
    describe_delegation_token_options::{
        describe_delegation_token_options_ref, kafka_admin_DescribeDelegationTokenOptions_t,
    },
    describe_features_options::{describe_features_options_ref, kafka_admin_DescribeFeaturesOptions_t},
    describe_log_dirs_options::{describe_log_dirs_options_ref, kafka_admin_DescribeLogDirsOptions_t},
    describe_producers_options::{describe_producers_options_ref, kafka_admin_DescribeProducersOptions_t},
    describe_replica_log_dirs_options::{
        describe_replica_log_dirs_options_ref, kafka_admin_DescribeReplicaLogDirsOptions_t,
    },
    describe_topics_options::{describe_topics_options_ref, kafka_admin_DescribeTopicsOptions_t},
    describe_transactions_options::{describe_transactions_options_ref, kafka_admin_DescribeTransactionsOptions_t},
    describe_user_scram_credentials_options::{
        describe_user_scram_credentials_options_ref, kafka_admin_DescribeUserScramCredentialsOptions_t,
    },
    elect_leaders_options::{elect_leaders_options_ref, kafka_admin_ElectLeadersOptions_t},
    expire_delegation_token_options::{
        expire_delegation_token_options_ref, kafka_admin_ExpireDelegationTokenOptions_t,
    },
    fence_producers_options::{fence_producers_options_ref, kafka_admin_FenceProducersOptions_t},
    list_config_resources_options::{kafka_admin_ListConfigResourcesOptions_t, list_config_resources_options_ref},
    list_consumer_group_offsets_options::{
        kafka_admin_ListConsumerGroupOffsetsOptions_t, list_consumer_group_offsets_options_ref,
    },
    list_groups_options::{kafka_admin_ListGroupsOptions_t, list_groups_options_ref},
    list_offsets_options::{kafka_admin_ListOffsetsOptions_t, list_offsets_options_ref},
    list_partition_reassignments_options::{
        kafka_admin_ListPartitionReassignmentsOptions_t, list_partition_reassignments_options_ref,
    },
    list_topics_options::{kafka_admin_ListTopicsOptions_t, list_topics_options_ref},
    list_transactions_options::{kafka_admin_ListTransactionsOptions_t, list_transactions_options_ref},
    remove_members_from_consumer_group_options::{
        kafka_admin_RemoveMembersFromConsumerGroupOptions_t, remove_members_from_consumer_group_options_ref,
    },
    renew_delegation_token_options::{kafka_admin_RenewDelegationTokenOptions_t, renew_delegation_token_options_ref},
    terminate_transaction_options::{kafka_admin_TerminateTransactionOptions_t, terminate_transaction_options_ref},
    unregister_controller_options::{kafka_admin_UnregisterControllerOptions_t, unregister_controller_options_ref},
    update_features_options::{kafka_admin_UpdateFeaturesOptions_t, update_features_options_ref},
};
use crate::ffi::admin::records_to_delete::records_to_delete_ref;
use crate::ffi::admin::user_scram_credential_alteration::user_scram_credential_alteration_ref;
use crate::ffi::admin::{
    abort_transaction_result::{box_abort_transaction_result, kafka_admin_AbortTransactionResult_t},
    alter_client_quotas_result::{box_alter_client_quotas_result, kafka_admin_AlterClientQuotasResult_t},
    alter_configs_result::{box_alter_configs_result, kafka_admin_AlterConfigsResult_t},
    alter_consumer_group_offsets_result::{
        box_alter_consumer_group_offsets_result, kafka_admin_AlterConsumerGroupOffsetsResult_t,
    },
    alter_partition_reassignments_result::{
        box_alter_partition_reassignments_result, kafka_admin_AlterPartitionReassignmentsResult_t,
    },
    alter_replica_log_dirs_result::{box_alter_replica_log_dirs_result, kafka_admin_AlterReplicaLogDirsResult_t},
    alter_user_scram_credentials_result::{
        box_alter_user_scram_credentials_result, kafka_admin_AlterUserScramCredentialsResult_t,
    },
    create_acls_result::{box_create_acls_result, kafka_admin_CreateAclsResult_t},
    create_delegation_token_result::{box_create_delegation_token_result, kafka_admin_CreateDelegationTokenResult_t},
    create_partitions_result::{box_create_partitions_result, kafka_admin_CreatePartitionsResult_t},
    create_topics_result::{box_create_topics_result, kafka_admin_CreateTopicsResult_t},
    delete_acls_result::{box_delete_acls_result, kafka_admin_DeleteAclsResult_t},
    delete_consumer_group_offsets_result::{
        box_delete_consumer_group_offsets_result, kafka_admin_DeleteConsumerGroupOffsetsResult_t,
    },
    delete_consumer_groups_result::{box_delete_consumer_groups_result, kafka_admin_DeleteConsumerGroupsResult_t},
    delete_records_result::{box_delete_records_result, kafka_admin_DeleteRecordsResult_t},
    delete_topics_result::{box_delete_topics_result, kafka_admin_DeleteTopicsResult_t},
    describe_acls_result::{box_describe_acls_result, kafka_admin_DescribeAclsResult_t},
    describe_classic_groups_result::{box_describe_classic_groups_result, kafka_admin_DescribeClassicGroupsResult_t},
    describe_client_quotas_result::{box_describe_client_quotas_result, kafka_admin_DescribeClientQuotasResult_t},
    describe_cluster_result::{box_describe_cluster_result, kafka_admin_DescribeClusterResult_t},
    describe_configs_result::{box_describe_configs_result, kafka_admin_DescribeConfigsResult_t},
    describe_consumer_groups_result::{
        box_describe_consumer_groups_result, kafka_admin_DescribeConsumerGroupsResult_t,
    },
    describe_delegation_token_result::{
        box_describe_delegation_token_result, kafka_admin_DescribeDelegationTokenResult_t,
    },
    describe_features_result::{box_describe_features_result, kafka_admin_DescribeFeaturesResult_t},
    describe_log_dirs_result::{box_describe_log_dirs_result, kafka_admin_DescribeLogDirsResult_t},
    describe_producers_result::{box_describe_producers_result, kafka_admin_DescribeProducersResult_t},
    describe_replica_log_dirs_result::{
        box_describe_replica_log_dirs_result, kafka_admin_DescribeReplicaLogDirsResult_t,
    },
    describe_topics_result::{box_describe_topics_result, kafka_admin_DescribeTopicsResult_t},
    describe_transactions_result::{box_describe_transactions_result, kafka_admin_DescribeTransactionsResult_t},
    describe_user_scram_credentials_result::{
        box_describe_user_scram_credentials_result, kafka_admin_DescribeUserScramCredentialsResult_t,
    },
    elect_leaders_result::{box_elect_leaders_result, kafka_admin_ElectLeadersResult_t},
    expire_delegation_token_result::{box_expire_delegation_token_result, kafka_admin_ExpireDelegationTokenResult_t},
    fence_producers_result::{box_fence_producers_result, kafka_admin_FenceProducersResult_t},
    list_config_resources_result::{box_list_config_resources_result, kafka_admin_ListConfigResourcesResult_t},
    list_consumer_group_offsets_result::{
        box_list_consumer_group_offsets_result, kafka_admin_ListConsumerGroupOffsetsResult_t,
    },
    list_groups_result::{box_list_groups_result, kafka_admin_ListGroupsResult_t},
    list_offsets_result::{box_list_offsets_result, kafka_admin_ListOffsetsResult_t},
    list_partition_reassignments_result::{
        box_list_partition_reassignments_result, kafka_admin_ListPartitionReassignmentsResult_t,
    },
    list_topics_result::{box_list_topics_result, kafka_admin_ListTopicsResult_t},
    list_transactions_result::{box_list_transactions_result, kafka_admin_ListTransactionsResult_t},
    remove_members_from_consumer_group_result::{
        box_remove_members_from_consumer_group_result, kafka_admin_RemoveMembersFromConsumerGroupResult_t,
    },
    renew_delegation_token_result::{box_renew_delegation_token_result, kafka_admin_RenewDelegationTokenResult_t},
    terminate_transaction_result::{box_terminate_transaction_result, kafka_admin_TerminateTransactionResult_t},
    unregister_controller_result::{box_unregister_controller_result, kafka_admin_UnregisterControllerResult_t},
    update_features_result::{box_update_features_result, kafka_admin_UpdateFeaturesResult_t},
};
use crate::ffi::admin::{client_ref, kafka_admin_Admin_t};
use crate::ffi::common::acl::acl_binding::acl_binding_ref;
use crate::ffi::common::acl::acl_binding_filter::{acl_binding_filter_ref, kafka_common_acl_AclBindingFilter_t};
use crate::ffi::common::config::config_resource::{config_resource_ref, type_of};
use crate::ffi::common::deliver_value;
use crate::ffi::common::election_type::{kafka_common_ElectionType_t, value_of as election_type_value_of};
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::common::quota::client_quota_alteration::client_quota_alteration_ref;
use crate::ffi::common::quota::client_quota_filter::{client_quota_filter_ref, kafka_common_quota_ClientQuotaFilter_t};
use crate::ffi::common::topic_collection::{kafka_common_TopicCollection_t, topic_collection_ref};
use crate::ffi::common::topic_partition::{list_topic_partitions, topic_partition_ref};
use crate::ffi::common::topic_partition_replica::topic_partition_replica_ref;
use crate::ffi::consumer::offset_and_metadata::map_offset_and_metadata;
use crate::ffi::ffi_guard;
use crate::ffi::util::{
    c_str_to_string, kafka_Bytes_t, kafka_List_t, kafka_Map_t, list_elements, list_strings, map_entries,
};

// ---------------------------------------------------------------------------
// Input readers
// ---------------------------------------------------------------------------

/// The elements of a borrowed list, each converted by `read`; `NULL` reads as
/// empty. `read` is called on the list's elements only, once each.
///
/// # Safety
///
/// `list` must be `NULL` or a valid list handle, alive and not mutated during
/// the call.
unsafe fn list_refs<T>(list: *const kafka_List_t, read: impl Fn(*mut c_void) -> T) -> Vec<T> {
    if list.is_null() {
        return Vec::new();
    }
    // SAFETY: `list` is non-null (checked above) and, per this function's `# Safety`, a valid
    // list handle, alive and unmutated during the call, as `list_elements` requires; the slice is
    // dropped before the call returns.
    unsafe { list_elements(list) }.iter().map(|&p| read(p)).collect()
}

/// The entries of a borrowed map, keys and values converted; `NULL` reads as
/// empty. `read_key` is called on the map's keys only and `read_value` on
/// its values only, once each.
///
/// # Safety
///
/// `map` must be `NULL` or a valid map handle, alive and not mutated during
/// the call.
unsafe fn keyed<K, V>(
    map: *const kafka_Map_t,
    read_key: impl Fn(*mut c_void) -> K,
    read_value: impl Fn(*mut c_void) -> V,
) -> HashMap<K, V>
where
    K: std::hash::Hash + Eq,
{
    if map.is_null() {
        return HashMap::new();
    }
    // SAFETY: `map` is non-null (checked above) and, per this function's `# Safety`, a valid map
    // handle, alive and unmutated during the call, as `map_entries` requires; the slice is
    // dropped before the call returns.
    unsafe { map_entries(map) }
        .iter()
        .map(|&(k, v)| (read_key(k), read_value(v)))
        .collect()
}

/// A borrowed map of `const char *` keys. `read_value` is called on the
/// map's values only, once each.
///
/// # Safety
///
/// `map` must be `NULL` or a valid map handle whose keys are `NULL` or
/// NUL-terminated strings, alive and not mutated during the call.
unsafe fn string_keyed<V>(map: *const kafka_Map_t, read_value: impl Fn(*mut c_void) -> V) -> HashMap<String, V> {
    // SAFETY: Per this function's `# Safety`, `map` is null or a valid map handle, alive and
    // unmutated during the call, as `keyed` requires, and `keyed` calls the key reader on its
    // keys only, each null or a NUL-terminated string, as `c_str_to_string` requires.
    unsafe { keyed(map, |k| c_str_to_string(k as *const c_char), read_value) }
}

/// A borrowed map of `const kafka_common_TopicPartition_t *` keys.
/// `read_value` is called on the map's values only, once each.
///
/// # Safety
///
/// `map` must be `NULL` or a valid map handle whose keys are valid
/// topic-partition handles, all alive and not mutated during the call.
unsafe fn tp_keyed<V>(map: *const kafka_Map_t, read_value: impl Fn(*mut c_void) -> V) -> HashMap<TopicPartition, V> {
    // SAFETY: Per this function's `# Safety`, `map` is null or a valid map handle, alive and
    // unmutated during the call, as `keyed` requires, and `keyed` calls the key reader on its
    // keys only, each a valid topic-partition handle alive while it is cloned, as
    // `topic_partition_ref` requires.
    unsafe { keyed(map, |k| topic_partition_ref(k as *const _).clone(), read_value) }
}

/// A borrowed list of `const kafka_common_TopicPartition_t *`, `NULL` for
/// Java's null (`Optional.empty()`).
///
/// # Safety
///
/// `list` must be `NULL` or a valid list whose elements are topic-partition
/// handles, all alive and not mutated during the call.
unsafe fn optional_tp_set(list: *const kafka_List_t) -> Option<HashSet<TopicPartition>> {
    if list.is_null() {
        None
    } else {
        // SAFETY: Per this function's `# Safety`, `list` is a valid list of topic-partition
        // handles, alive and unmutated during the call, as `list_topic_partitions` requires; the
        // values are cloned.
        Some(unsafe { list_topic_partitions(list) }.into_iter().collect())
    }
}

/// The bytes behind a `kafka_Bytes_t`; a `NULL` `data` reads as empty.
///
/// # Safety
///
/// `bytes.data` must be `NULL` or point at `bytes.len` readable bytes, not
/// mutated while `bytes` stays borrowed.
unsafe fn bytes_of(bytes: &kafka_Bytes_t) -> &[u8] {
    if bytes.data.is_null() {
        &[]
    } else {
        // SAFETY: `bytes.data` is non-null (checked above) and, per this function's `# Safety`,
        // points at `bytes.len` readable bytes, unmutated while `bytes` stays borrowed, which
        // bounds the returned slice's lifetime; a negative length reads as 0, and `u8` has no
        // alignment requirement.
        unsafe { std::slice::from_raw_parts(bytes.data, usize::try_from(bytes.len).unwrap_or(0)) }
    }
}

// ---------------------------------------------------------------------------
// Invokers
// ---------------------------------------------------------------------------

/// Create a batch of new topics with the default options.
///
/// This is a convenience method for `kafka_admin_Admin_create_topics_with_options` with default
/// options. See the overload for more details.
///
/// This operation is supported by brokers with version 0.10.1.0 or higher.
///
/// # Parameters
///
/// - `new_topics` - The new topics to create.
///
/// # Returns
///
/// The CreateTopicsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `new_topics` is a `kafka_List_t` of `const kafka_admin_NewTopic_t *`, each live; `NULL` reads
///   as an empty list.
///
/// The result handle is owned by the caller, freed with `kafka_admin_CreateTopicsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_create_topics(
    self_: *const kafka_admin_Admin_t,
    new_topics: *const kafka_List_t,
) -> *mut kafka_admin_CreateTopicsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `new_topics` is null or a valid list, alive and
    // unmutated during this call, as `list_refs` requires, and `list_refs` passes the closure its
    // elements only, each a live `const kafka_admin_NewTopic_t *` handle no other call mutates
    // meanwhile, as `new_topic_ref` requires; each value is cloned before the call returns.
    let new_topics_ = unsafe { list_refs(new_topics, |p| new_topic_ref(p as *const _).clone()) };
    let result = client.call(|admin| admin.create_topics(&new_topics_));
    box_create_topics_result(result, &client.future_ctx())
}

/// Create a batch of new topics.
///
/// This operation is not transactional so it may succeed for some topics while fail for others.
///
/// It may take several seconds after `CreateTopicsResult` returns success for all the brokers to
/// become aware that the topics have been created. During this time,
/// `kafka_admin_Admin_list_topics` and `kafka_admin_Admin_describe_topics_with_topic_names` may not
/// return information about the new topics.
///
/// This operation is supported by brokers with version 0.10.1.0 or higher. The validateOnly option
/// is supported from version 0.10.2.0.
///
/// # Parameters
///
/// - `new_topics` - The new topics to create.
/// - `options` - The options to use when creating the new topics.
///
/// # Returns
///
/// The CreateTopicsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `new_topics` is a `kafka_List_t` of `const kafka_admin_NewTopic_t *`, each live; `NULL` reads
///   as an empty list.
///
/// The result handle is owned by the caller, freed with `kafka_admin_CreateTopicsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_create_topics_with_options(
    self_: *const kafka_admin_Admin_t,
    new_topics: *const kafka_List_t,
    options: *const kafka_admin_CreateTopicsOptions_t,
) -> *mut kafka_admin_CreateTopicsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `new_topics` is null or a valid list, alive and
    // unmutated during this call, as `list_refs` requires, and `list_refs` passes the closure its
    // elements only, each a live `const kafka_admin_NewTopic_t *` handle no other call mutates
    // meanwhile, as `new_topic_ref` requires; each value is cloned before the call returns.
    let new_topics_ = unsafe { list_refs(new_topics, |p| new_topic_ref(p as *const _).clone()) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_CreateTopicsOptions_t` handle that no other call mutates during this one, so no
    // setter runs on it while it is cloned, as `create_topics_options_ref` requires.
    let options_ = unsafe { create_topics_options_ref(options) }.clone();
    let result = client.call(|admin| admin.create_topics_with_options(&new_topics_, options_));
    box_create_topics_result(result, &client.future_ctx())
}

/// This is a convenience method for `kafka_admin_Admin_delete_topics_with_topic_collection_options`
/// with default options. See the overload for more details.
///
/// This operation is supported by brokers with version 0.10.1.0 or higher.
///
/// # Parameters
///
/// - `topics` - The topic names to delete, a `kafka_List_t` of `const char *`.
///
/// # Returns
///
/// The DeleteTopicsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with `kafka_admin_DeleteTopicsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_delete_topics_with_string_slice(
    self_: *const kafka_admin_Admin_t,
    topics: *const kafka_List_t,
) -> *mut kafka_admin_DeleteTopicsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `topics` is null or a valid list of NUL-terminated
    // `const char *`, unmutated during this call, as `list_strings` requires; the strings are
    // copied.
    let topics_ = unsafe { list_strings(topics) };
    let result = client.call(|admin| admin.delete_topics_with_string_slice(&topics_));
    box_delete_topics_result(result, &client.future_ctx())
}

/// This is a convenience method for `kafka_admin_Admin_delete_topics_with_topic_collection_options`
/// with default options. See the overload for more details.
///
/// This operation is supported by brokers with version 0.10.1.0 or higher.
///
/// # Parameters
///
/// - `topics` - The topic names to delete, a `kafka_List_t` of `const char *`.
/// - `options` - The options to use when deleting the topics.
///
/// # Returns
///
/// The DeleteTopicsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with `kafka_admin_DeleteTopicsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_delete_topics_with_string_slice_options(
    self_: *const kafka_admin_Admin_t,
    topics: *const kafka_List_t,
    options: *const kafka_admin_DeleteTopicsOptions_t,
) -> *mut kafka_admin_DeleteTopicsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `topics` is null or a valid list of NUL-terminated
    // `const char *`, unmutated during this call, as `list_strings` requires; the strings are
    // copied.
    let topics_ = unsafe { list_strings(topics) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_DeleteTopicsOptions_t` handle that no other call mutates during this one, so no
    // setter runs on it while it is cloned, as `delete_topics_options_ref` requires.
    let options_ = unsafe { delete_topics_options_ref(options) }.clone();
    let result = client.call(|admin| admin.delete_topics_with_string_slice_options(&topics_, options_));
    box_delete_topics_result(result, &client.future_ctx())
}

/// This is a convenience method for `kafka_admin_Admin_delete_topics_with_topic_collection_options`
/// with default options. See the overload for more details.
///
/// When using topic IDs, this operation is supported by brokers with inter-broker protocol 2.8 or
/// higher. When using topic names, this operation is supported by brokers with version 0.10.1.0 or
/// higher.
///
/// # Parameters
///
/// - `topics` - The topics to delete.
///
/// # Returns
///
/// The DeleteTopicsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with `kafka_admin_DeleteTopicsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_delete_topics_with_topic_collection(
    self_: *const kafka_admin_Admin_t,
    topics: *const kafka_common_TopicCollection_t,
) -> *mut kafka_admin_DeleteTopicsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `topics` is a live `kafka_common_TopicCollection_t`
    // handle, unmutated during this call, as `topic_collection_ref` requires; it is cloned before
    // the call returns.
    let topics_ = unsafe { topic_collection_ref(topics) }.clone();
    let result = client.call(|admin| admin.delete_topics_with_topic_collection(topics_));
    box_delete_topics_result(result, &client.future_ctx())
}

/// Delete a batch of topics.
///
/// This operation is not transactional so it may succeed for some topics while fail for others.
///
/// It may take several seconds after the `DeleteTopicsResult` returns success for all the brokers
/// to become aware that the topics are gone. During this time, `kafka_admin_Admin_list_topics` and
/// `kafka_admin_Admin_describe_topics_with_topic_names` may continue to return information about
/// the deleted topics.
///
/// If delete.topic.enable is set to false on the brokers, an error will be returned to the client
/// indicating that topic deletion is disabled.
///
/// When using topic IDs, this operation is supported by brokers with inter-broker protocol 2.8 or
/// higher. When using topic names, this operation is supported by brokers with version 0.10.1.0 or
/// higher.
///
/// # Parameters
///
/// - `topics` - The topics to delete.
/// - `options` - The options to use when deleting the topics.
///
/// # Returns
///
/// The DeleteTopicsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with `kafka_admin_DeleteTopicsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_delete_topics_with_topic_collection_options(
    self_: *const kafka_admin_Admin_t,
    topics: *const kafka_common_TopicCollection_t,
    options: *const kafka_admin_DeleteTopicsOptions_t,
) -> *mut kafka_admin_DeleteTopicsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `topics` is a live `kafka_common_TopicCollection_t`
    // handle, unmutated during this call, as `topic_collection_ref` requires; it is cloned before
    // the call returns.
    let topics_ = unsafe { topic_collection_ref(topics) }.clone();
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_DeleteTopicsOptions_t` handle that no other call mutates during this one, so no
    // setter runs on it while it is cloned, as `delete_topics_options_ref` requires.
    let options_ = unsafe { delete_topics_options_ref(options) }.clone();
    let result = client.call(|admin| admin.delete_topics_with_topic_collection_options(topics_, options_));
    box_delete_topics_result(result, &client.future_ctx())
}

/// List the topics available in the cluster with the default options.
///
/// This is a convenience method for `kafka_admin_Admin_list_topics_with_options` with default
/// options. See the overload for more details.
///
/// # Returns
///
/// The ListTopicsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. The result handle is owned by the caller, freed with
/// `kafka_admin_ListTopicsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_list_topics(
    self_: *const kafka_admin_Admin_t,
) -> *mut kafka_admin_ListTopicsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    let result = client.call(|admin| admin.list_topics());
    box_list_topics_result(result, &client.future_ctx())
}

/// List the topics available in the cluster.
///
/// # Parameters
///
/// - `options` - The options to use when listing the topics.
///
/// # Returns
///
/// The ListTopicsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with `kafka_admin_ListTopicsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_list_topics_with_options(
    self_: *const kafka_admin_Admin_t,
    options: *const kafka_admin_ListTopicsOptions_t,
) -> *mut kafka_admin_ListTopicsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `options` is a live `kafka_admin_ListTopicsOptions_t`
    // handle that no other call mutates during this one, so no setter runs on it while it is
    // cloned, as `list_topics_options_ref` requires.
    let options_ = unsafe { list_topics_options_ref(options) }.clone();
    let result = client.call(|admin| admin.list_topics_with_options(options_));
    box_list_topics_result(result, &client.future_ctx())
}

/// Describe some topics in the cluster, with the default options.
///
/// This is a convenience method for `kafka_admin_Admin_describe_topics_with_topic_names_options`
/// with default options. See the overload for more details.
///
/// # Parameters
///
/// - `topic_names` - The names of the topics to describe.
///
/// # Returns
///
/// The DescribeTopicsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `topic_names` is a `kafka_List_t` of NUL-terminated `const char *`; `NULL` reads as an empty
///   list and a `NULL` element as an empty string.
///
/// The result handle is owned by the caller, freed with `kafka_admin_DescribeTopicsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_topics_with_topic_names(
    self_: *const kafka_admin_Admin_t,
    topic_names: *const kafka_List_t,
) -> *mut kafka_admin_DescribeTopicsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `topic_names` is null or a valid list of
    // NUL-terminated `const char *`, unmutated during this call, as `list_strings` requires; the
    // strings are copied.
    let topic_names_ = unsafe { list_strings(topic_names) };
    let result = client.call(|admin| admin.describe_topics_with_topic_names(&topic_names_));
    box_describe_topics_result(result, &client.future_ctx())
}

/// Describe some topics in the cluster.
///
/// # Parameters
///
/// - `topic_names` - The names of the topics to describe.
/// - `options` - The options to use when describing the topic.
///
/// # Returns
///
/// The DescribeTopicsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `topic_names` is a `kafka_List_t` of NUL-terminated `const char *`; `NULL` reads as an empty
///   list and a `NULL` element as an empty string.
///
/// The result handle is owned by the caller, freed with `kafka_admin_DescribeTopicsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_topics_with_topic_names_options(
    self_: *const kafka_admin_Admin_t,
    topic_names: *const kafka_List_t,
    options: *const kafka_admin_DescribeTopicsOptions_t,
) -> *mut kafka_admin_DescribeTopicsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `topic_names` is null or a valid list of
    // NUL-terminated `const char *`, unmutated during this call, as `list_strings` requires; the
    // strings are copied.
    let topic_names_ = unsafe { list_strings(topic_names) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_DescribeTopicsOptions_t` handle that no other call mutates during this one, so
    // no setter runs on it while it is cloned, as `describe_topics_options_ref` requires.
    let options_ = unsafe { describe_topics_options_ref(options) }.clone();
    let result = client.call(|admin| admin.describe_topics_with_topic_names_options(&topic_names_, options_));
    box_describe_topics_result(result, &client.future_ctx())
}

/// This is a convenience method for `kafka_admin_Admin_describe_topics_with_topics_options` with
/// default options. See the overload for more details.
///
/// When using topic IDs, this operation is supported by brokers with version 3.1.0 or higher.
///
/// # Parameters
///
/// - `topics` - The topics to describe.
///
/// # Returns
///
/// The DescribeTopicsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with `kafka_admin_DescribeTopicsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_topics_with_topics(
    self_: *const kafka_admin_Admin_t,
    topics: *const kafka_common_TopicCollection_t,
) -> *mut kafka_admin_DescribeTopicsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `topics` is a live `kafka_common_TopicCollection_t`
    // handle, unmutated during this call, as `topic_collection_ref` requires; it is cloned before
    // the call returns.
    let topics_ = unsafe { topic_collection_ref(topics) }.clone();
    let result = client.call(|admin| admin.describe_topics_with_topics(topics_));
    box_describe_topics_result(result, &client.future_ctx())
}

/// Describe some topics in the cluster. When using topic IDs, this operation is supported by
/// brokers with version 3.1.0 or higher.
///
/// # Parameters
///
/// - `topics` - The topics to describe.
/// - `options` - The options to use when describing the topics.
///
/// # Returns
///
/// The DescribeTopicsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with `kafka_admin_DescribeTopicsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_topics_with_topics_options(
    self_: *const kafka_admin_Admin_t,
    topics: *const kafka_common_TopicCollection_t,
    options: *const kafka_admin_DescribeTopicsOptions_t,
) -> *mut kafka_admin_DescribeTopicsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `topics` is a live `kafka_common_TopicCollection_t`
    // handle, unmutated during this call, as `topic_collection_ref` requires; it is cloned before
    // the call returns.
    let topics_ = unsafe { topic_collection_ref(topics) }.clone();
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_DescribeTopicsOptions_t` handle that no other call mutates during this one, so
    // no setter runs on it while it is cloned, as `describe_topics_options_ref` requires.
    let options_ = unsafe { describe_topics_options_ref(options) }.clone();
    let result = client.call(|admin| admin.describe_topics_with_topics_options(topics_, options_));
    box_describe_topics_result(result, &client.future_ctx())
}

/// Increase the number of partitions of the topics given as the keys of `new_partitions` according
/// to the corresponding values. **If partitions are increased for a topic that has a key, the
/// partition logic or ordering of the messages will be affected.**
///
/// This is a convenience method for `kafka_admin_Admin_create_partitions_with_options` with default
/// options. See the overload for more details.
///
/// # Parameters
///
/// - `new_partitions` - The topics which should have new partitions created, and corresponding
///   parameters for the created partitions.
///
/// # Returns
///
/// The CreatePartitionsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `new_partitions` is a `kafka_Map_t` of NUL-terminated `const char *` keys to `const
///   kafka_admin_NewPartitions_t *` values, each live; `NULL` reads as an empty map and a `NULL`
///   key as an empty string.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_CreatePartitionsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_create_partitions(
    self_: *const kafka_admin_Admin_t,
    new_partitions: *const kafka_Map_t,
) -> *mut kafka_admin_CreatePartitionsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `new_partitions` is null or a valid map whose keys
    // are null or NUL-terminated strings, alive and unmutated during this call, as `string_keyed`
    // requires, and `string_keyed` passes the closure its values only, each a live
    // `const kafka_admin_NewPartitions_t *` handle no other call mutates meanwhile, as
    // `new_partitions_ref` requires; each value is cloned before the call returns.
    let new_partitions_ = unsafe { string_keyed(new_partitions, |p| new_partitions_ref(p as *const _).clone()) };
    let result = client.call(|admin| admin.create_partitions(&new_partitions_));
    box_create_partitions_result(result, &client.future_ctx())
}

/// Increase the number of partitions of the topics given as the keys of `new_partitions` according
/// to the corresponding values. **If partitions are increased for a topic that has a key, the
/// partition logic or ordering of the messages will be affected.**
///
/// This operation is not transactional so it may succeed for some topics while fail for others.
///
/// It may take several seconds after this method returns success for all the brokers to become
/// aware that the partitions have been created. During this time,
/// `kafka_admin_Admin_describe_topics_with_topic_names` may not return information about the new
/// partitions.
///
/// This operation is supported by brokers with version 1.0.0 or higher.
///
/// The following errors can be anticipated when calling `kafka_common_KafkaFuture_get` on the
/// futures obtained from the values() method of the returned `CreatePartitionsResult`
///
/// - `kafka_common_Error_is_authorization_error` if the authenticated user is not authorized to
///   alter the topic
/// - `kafka_common_Error_is_timeout_error` if the request was not completed in within the given
///   `kafka_admin_CreatePartitionsOptions_timeout_ms`.
/// - `kafka_common_Error_is_reassignment_in_progress_error` if a partition reassignment is
///   currently in progress
/// - `kafka_common_Error_is_broker_not_available_error` if the requested
///   `kafka_admin_NewPartitions_assignments` contain a broker that is currently unavailable.
/// - `kafka_common_Error_is_invalid_replication_factor_error` if no
///   `kafka_admin_NewPartitions_assignments` are given and it is impossible for the broker to
///   assign replicas with the topics replication factor.
/// - Subclasses of `kafka_common_Error_is_kafka_error` if the request is invalid in some way.
/// - `kafka_common_Error_is_invalid_partitions_error` if the requested partition count is less than
///   or equal to the current partition count.
///
/// # Parameters
///
/// - `new_partitions` - The topics which should have new partitions created, and corresponding
///   parameters for the created partitions.
/// - `options` - The options to use when creating the new partitions.
///
/// # Returns
///
/// The CreatePartitionsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `new_partitions` is a `kafka_Map_t` of NUL-terminated `const char *` keys to `const
///   kafka_admin_NewPartitions_t *` values, each live; `NULL` reads as an empty map and a `NULL`
///   key as an empty string.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_CreatePartitionsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_create_partitions_with_options(
    self_: *const kafka_admin_Admin_t,
    new_partitions: *const kafka_Map_t,
    options: *const kafka_admin_CreatePartitionsOptions_t,
) -> *mut kafka_admin_CreatePartitionsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `new_partitions` is null or a valid map whose keys
    // are null or NUL-terminated strings, alive and unmutated during this call, as `string_keyed`
    // requires, and `string_keyed` passes the closure its values only, each a live
    // `const kafka_admin_NewPartitions_t *` handle no other call mutates meanwhile, as
    // `new_partitions_ref` requires; each value is cloned before the call returns.
    let new_partitions_ = unsafe { string_keyed(new_partitions, |p| new_partitions_ref(p as *const _).clone()) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_CreatePartitionsOptions_t` handle that no other call mutates during this one, so
    // no setter runs on it while it is cloned, as `create_partitions_options_ref` requires.
    let options_ = unsafe { create_partitions_options_ref(options) }.clone();
    let result = client.call(|admin| admin.create_partitions_with_options(&new_partitions_, options_));
    box_create_partitions_result(result, &client.future_ctx())
}

/// Delete records whose offset is smaller than the given offset of the corresponding partition.
///
/// This is a convenience method for `kafka_admin_Admin_delete_records_with_options` with default
/// options. See the overload for more details.
///
/// This operation is supported by brokers with version 0.11.0.0 or higher.
///
/// # Parameters
///
/// - `records_to_delete` - The topic partitions and related offsets from which records deletion
///   starts.
///
/// # Returns
///
/// The DeleteRecordsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `records_to_delete` is a `kafka_Map_t` of `const kafka_common_TopicPartition_t *` keys to
///   `const kafka_admin_RecordsToDelete_t *` values, each live; `NULL` reads as an empty map.
///
/// The result handle is owned by the caller, freed with `kafka_admin_DeleteRecordsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_delete_records(
    self_: *const kafka_admin_Admin_t,
    records_to_delete: *const kafka_Map_t,
) -> *mut kafka_admin_DeleteRecordsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `records_to_delete` is null or a valid map whose keys
    // are live `const kafka_common_TopicPartition_t *` handles, alive and unmutated during this
    // call, as `tp_keyed` requires, and `tp_keyed` passes the closure its values only, each a live
    // `const kafka_admin_RecordsToDelete_t *` handle no other call mutates meanwhile, as
    // `records_to_delete_ref` requires; each value is cloned before the call returns.
    let records_to_delete_ = unsafe { tp_keyed(records_to_delete, |p| records_to_delete_ref(p as *const _).clone()) };
    let result = client.call(|admin| admin.delete_records(&records_to_delete_));
    box_delete_records_result(result, &client.future_ctx())
}

/// Delete records whose offset is smaller than the given offset of the corresponding partition.
///
/// This operation is supported by brokers with version 0.11.0.0 or higher.
///
/// # Parameters
///
/// - `records_to_delete` - The topic partitions and related offsets from which records deletion
///   starts.
/// - `options` - The options to use when deleting records.
///
/// # Returns
///
/// The DeleteRecordsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `records_to_delete` is a `kafka_Map_t` of `const kafka_common_TopicPartition_t *` keys to
///   `const kafka_admin_RecordsToDelete_t *` values, each live; `NULL` reads as an empty map.
///
/// The result handle is owned by the caller, freed with `kafka_admin_DeleteRecordsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_delete_records_with_options(
    self_: *const kafka_admin_Admin_t,
    records_to_delete: *const kafka_Map_t,
    options: *const kafka_admin_DeleteRecordsOptions_t,
) -> *mut kafka_admin_DeleteRecordsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `records_to_delete` is null or a valid map whose keys
    // are live `const kafka_common_TopicPartition_t *` handles, alive and unmutated during this
    // call, as `tp_keyed` requires, and `tp_keyed` passes the closure its values only, each a live
    // `const kafka_admin_RecordsToDelete_t *` handle no other call mutates meanwhile, as
    // `records_to_delete_ref` requires; each value is cloned before the call returns.
    let records_to_delete_ = unsafe { tp_keyed(records_to_delete, |p| records_to_delete_ref(p as *const _).clone()) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_DeleteRecordsOptions_t` handle that no other call mutates during this one, so no
    // setter runs on it while it is cloned, as `delete_records_options_ref` requires.
    let options_ = unsafe { delete_records_options_ref(options) }.clone();
    let result = client.call(|admin| admin.delete_records_with_options(&records_to_delete_, options_));
    box_delete_records_result(result, &client.future_ctx())
}

/// Describe producer state on a set of topic partitions. See
/// `kafka_admin_Admin_describe_producers_with_options` for more details.
///
/// # Parameters
///
/// - `partitions` - The set of partitions to query
///
/// # Returns
///
/// The result
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `partitions` is a `kafka_List_t` of `const kafka_common_TopicPartition_t *`, each live; `NULL`
///   reads as an empty list.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_DescribeProducersResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_producers(
    self_: *const kafka_admin_Admin_t,
    partitions: *const kafka_List_t,
) -> *mut kafka_admin_DescribeProducersResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list of
    // `const kafka_common_TopicPartition_t *` handles, all live and unmutated during this call, as
    // `list_topic_partitions` requires; the values are cloned.
    let partitions_ = unsafe { list_topic_partitions(partitions) };
    let result = client.call(|admin| admin.describe_producers(&partitions_));
    box_describe_producers_result(result, &client.future_ctx())
}

/// Describe active producer state on a set of topic partitions. Unless a specific broker is
/// requested through `kafka_admin_DescribeProducersOptions_broker_id`, this will query the
/// partition leader to find the producer state.
///
/// # Parameters
///
/// - `partitions` - The set of partitions to query
/// - `options` - Options to control the method behavior
///
/// # Returns
///
/// The result
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `partitions` is a `kafka_List_t` of `const kafka_common_TopicPartition_t *`, each live; `NULL`
///   reads as an empty list.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_DescribeProducersResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_producers_with_options(
    self_: *const kafka_admin_Admin_t,
    partitions: *const kafka_List_t,
    options: *const kafka_admin_DescribeProducersOptions_t,
) -> *mut kafka_admin_DescribeProducersResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list of
    // `const kafka_common_TopicPartition_t *` handles, all live and unmutated during this call, as
    // `list_topic_partitions` requires; the values are cloned.
    let partitions_ = unsafe { list_topic_partitions(partitions) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_DescribeProducersOptions_t` handle that no other call mutates during this one,
    // so no setter runs on it while it is cloned, as `describe_producers_options_ref` requires.
    let options_ = unsafe { describe_producers_options_ref(options) }.clone();
    let result = client.call(|admin| admin.describe_producers_with_options(&partitions_, options_));
    box_describe_producers_result(result, &client.future_ctx())
}

/// Forcefully abort a transaction which is open on a topic partition. See
/// `kafka_admin_Admin_abort_transaction_with_options` for more details.
///
/// # Parameters
///
/// - `spec` - The transaction specification including topic partition and producer details
///
/// # Returns
///
/// The result
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with `kafka_admin_AbortTransactionResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_abort_transaction(
    self_: *const kafka_admin_Admin_t,
    spec: *const kafka_admin_AbortTransactionSpec_t,
) -> *mut kafka_admin_AbortTransactionResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `spec` is a live `kafka_admin_AbortTransactionSpec_t`
    // handle, unmutated during this call, as `abort_transaction_spec_ref` requires; it is cloned
    // before the call returns.
    let spec_ = unsafe { abort_transaction_spec_ref(spec) }.clone();
    let result = client.call(|admin| admin.abort_transaction(spec_));
    box_abort_transaction_result(result, &client.future_ctx())
}

/// Forcefully abort a transaction which is open on a topic partition. This will send a
/// `WriteTxnMarkers` request to the partition leader in order to abort the transaction. This
/// requires administrative privileges.
///
/// # Parameters
///
/// - `spec` - The transaction specification including topic partition and producer details
/// - `options` - Options to control the method behavior (including filters)
///
/// # Returns
///
/// The result
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with `kafka_admin_AbortTransactionResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_abort_transaction_with_options(
    self_: *const kafka_admin_Admin_t,
    spec: *const kafka_admin_AbortTransactionSpec_t,
    options: *const kafka_admin_AbortTransactionOptions_t,
) -> *mut kafka_admin_AbortTransactionResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `spec` is a live `kafka_admin_AbortTransactionSpec_t`
    // handle, unmutated during this call, as `abort_transaction_spec_ref` requires; it is cloned
    // before the call returns.
    let spec_ = unsafe { abort_transaction_spec_ref(spec) }.clone();
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_AbortTransactionOptions_t` handle that no other call mutates during this one, so
    // no setter runs on it while it is cloned, as `abort_transaction_options_ref` requires.
    let options_ = unsafe { abort_transaction_options_ref(options) }.clone();
    let result = client.call(|admin| admin.abort_transaction_with_options(spec_, options_));
    box_abort_transaction_result(result, &client.future_ctx())
}

/// Unregister a controller. This is a convenience method for
/// `kafka_admin_Admin_unregister_controller_with_options`
///
/// # Parameters
///
/// - `controller_id` - the controller id to unregister.
///
/// # Returns
///
/// the `UnregisterControllerResult` containing the result
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with
/// `kafka_admin_UnregisterControllerResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_unregister_controller(
    self_: *const kafka_admin_Admin_t,
    controller_id: i32,
) -> *mut kafka_admin_UnregisterControllerResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    let result = client.call(|admin| admin.unregister_controller(controller_id));
    box_unregister_controller_result(result, &client.future_ctx())
}

/// Unregister a controller. The following errors can be anticipated when calling
/// `kafka_common_KafkaFuture_get` on the future from the returned `UnregisterControllerResult`:
///
/// - `kafka_common_Error_is_timeout_error` If the request timed out before the unregister operation
///   could finish.
/// - `kafka_common_Error_is_unsupported_version_error` If the software is too old to support the
///   unregistration API.
/// - `kafka_common_Error_is_controller_id_not_registered_error` If the requested controller id is
///   not currently registered.
/// - `kafka_common_Error_is_not_controller_error` If the request does not arrive at the active
///   controller.
/// - `kafka_common_Error_is_invalid_request_error` If the request tries to unregister the current
///   active controller id.
///
/// # Parameters
///
/// - `controller_id` - the controller id to unregister.
/// - `options` - the options to use.
///
/// # Returns
///
/// the `UnregisterControllerResult` containing the result
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with
/// `kafka_admin_UnregisterControllerResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_unregister_controller_with_options(
    self_: *const kafka_admin_Admin_t,
    controller_id: i32,
    options: *const kafka_admin_UnregisterControllerOptions_t,
) -> *mut kafka_admin_UnregisterControllerResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_UnregisterControllerOptions_t` handle that no other call mutates during this
    // one, so no setter runs on it while it is cloned, as `unregister_controller_options_ref`
    // requires.
    let options_ = unsafe { unregister_controller_options_ref(options) }.clone();
    let result = client.call(|admin| admin.unregister_controller_with_options(controller_id, options_));
    box_unregister_controller_result(result, &client.future_ctx())
}

/// Describe the state of a set of transactional IDs. See
/// `kafka_admin_Admin_describe_transactions_with_options` for more details.
///
/// # Parameters
///
/// - `transactional_ids` - The set of transactional IDs to query
///
/// # Returns
///
/// The result
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `transactional_ids` is a `kafka_List_t` of NUL-terminated `const char *`; `NULL` reads as an
///   empty list and a `NULL` element as an empty string.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_DescribeTransactionsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_transactions(
    self_: *const kafka_admin_Admin_t,
    transactional_ids: *const kafka_List_t,
) -> *mut kafka_admin_DescribeTransactionsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `transactional_ids` is null or a valid list of
    // NUL-terminated `const char *`, unmutated during this call, as `list_strings` requires; the
    // strings are copied.
    let transactional_ids_ = unsafe { list_strings(transactional_ids) };
    let result = client.call(|admin| admin.describe_transactions(&transactional_ids_));
    box_describe_transactions_result(result, &client.future_ctx())
}

/// Describe the state of a set of transactional IDs from the respective transaction coordinators,
/// which are dynamically discovered.
///
/// # Parameters
///
/// - `transactional_ids` - The set of transactional IDs to query
/// - `options` - Options to control the method behavior
///
/// # Returns
///
/// The result
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `transactional_ids` is a `kafka_List_t` of NUL-terminated `const char *`; `NULL` reads as an
///   empty list and a `NULL` element as an empty string.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_DescribeTransactionsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_transactions_with_options(
    self_: *const kafka_admin_Admin_t,
    transactional_ids: *const kafka_List_t,
    options: *const kafka_admin_DescribeTransactionsOptions_t,
) -> *mut kafka_admin_DescribeTransactionsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `transactional_ids` is null or a valid list of
    // NUL-terminated `const char *`, unmutated during this call, as `list_strings` requires; the
    // strings are copied.
    let transactional_ids_ = unsafe { list_strings(transactional_ids) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_DescribeTransactionsOptions_t` handle that no other call mutates during this
    // one, so no setter runs on it while it is cloned, as `describe_transactions_options_ref`
    // requires.
    let options_ = unsafe { describe_transactions_options_ref(options) }.clone();
    let result = client.call(|admin| admin.describe_transactions_with_options(&transactional_ids_, options_));
    box_describe_transactions_result(result, &client.future_ctx())
}

/// Fence out all active producers that use any of the provided transactional IDs, with the default
/// options.
///
/// This is a convenience method for `kafka_admin_Admin_fence_producers_with_options` with default
/// options. See the overload for more details.
///
/// # Parameters
///
/// - `transactional_ids` - The IDs of the producers to fence.
///
/// # Returns
///
/// The FenceProducersResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `transactional_ids` is a `kafka_List_t` of NUL-terminated `const char *`; `NULL` reads as an
///   empty list and a `NULL` element as an empty string.
///
/// The result handle is owned by the caller, freed with `kafka_admin_FenceProducersResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_fence_producers(
    self_: *const kafka_admin_Admin_t,
    transactional_ids: *const kafka_List_t,
) -> *mut kafka_admin_FenceProducersResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `transactional_ids` is null or a valid list of
    // NUL-terminated `const char *`, unmutated during this call, as `list_strings` requires; the
    // strings are copied.
    let transactional_ids_ = unsafe { list_strings(transactional_ids) };
    let result = client.call(|admin| admin.fence_producers(&transactional_ids_));
    box_fence_producers_result(result, &client.future_ctx())
}

/// Fence out all active producers that use any of the provided transactional IDs.
///
/// # Parameters
///
/// - `transactional_ids` - The IDs of the producers to fence.
/// - `options` - The options to use when fencing the producers.
///
/// # Returns
///
/// The FenceProducersResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `transactional_ids` is a `kafka_List_t` of NUL-terminated `const char *`; `NULL` reads as an
///   empty list and a `NULL` element as an empty string.
///
/// The result handle is owned by the caller, freed with `kafka_admin_FenceProducersResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_fence_producers_with_options(
    self_: *const kafka_admin_Admin_t,
    transactional_ids: *const kafka_List_t,
    options: *const kafka_admin_FenceProducersOptions_t,
) -> *mut kafka_admin_FenceProducersResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `transactional_ids` is null or a valid list of
    // NUL-terminated `const char *`, unmutated during this call, as `list_strings` requires; the
    // strings are copied.
    let transactional_ids_ = unsafe { list_strings(transactional_ids) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_FenceProducersOptions_t` handle that no other call mutates during this one, so
    // no setter runs on it while it is cloned, as `fence_producers_options_ref` requires.
    let options_ = unsafe { fence_producers_options_ref(options) }.clone();
    let result = client.call(|admin| admin.fence_producers_with_options(&transactional_ids_, options_));
    box_fence_producers_result(result, &client.future_ctx())
}

/// List active transactions in the cluster. See `kafka_admin_Admin_list_transactions_with_options`
/// for more details.
///
/// # Returns
///
/// The result
///
/// # Safety
///
/// `self_` must be a live handle or view. The result handle is owned by the caller, freed with
/// `kafka_admin_ListTransactionsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_list_transactions(
    self_: *const kafka_admin_Admin_t,
) -> *mut kafka_admin_ListTransactionsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    let result = client.call(|admin| admin.list_transactions());
    box_list_transactions_result(result, &client.future_ctx())
}

/// List active transactions in the cluster. This will query all potential transaction coordinators
/// in the cluster and collect the state of all transactions. Users should typically attempt to
/// reduce the size of the result set using
/// `kafka_admin_ListTransactionsOptions_filter_producer_ids` or
/// `kafka_admin_ListTransactionsOptions_filter_states` or
/// `kafka_admin_ListTransactionsOptions_filter_on_duration`.
///
/// # Parameters
///
/// - `options` - Options to control the method behavior (including filters)
///
/// # Returns
///
/// The result
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with `kafka_admin_ListTransactionsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_list_transactions_with_options(
    self_: *const kafka_admin_Admin_t,
    options: *const kafka_admin_ListTransactionsOptions_t,
) -> *mut kafka_admin_ListTransactionsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_ListTransactionsOptions_t` handle that no other call mutates during this one, so
    // no setter runs on it while it is cloned, as `list_transactions_options_ref` requires.
    let options_ = unsafe { list_transactions_options_ref(options) }.clone();
    let result = client.call(|admin| admin.list_transactions_with_options(options_));
    box_list_transactions_result(result, &client.future_ctx())
}

/// Force terminate a transaction for the given transactional ID with the default options.
///
/// This is a convenience method for `kafka_admin_Admin_force_terminate_transaction_with_options`
/// with default options.
///
/// # Parameters
///
/// - `transactional_id` - The ID of the transaction to terminate.
///
/// # Returns
///
/// The TerminateTransactionResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with
/// `kafka_admin_TerminateTransactionResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_force_terminate_transaction(
    self_: *const kafka_admin_Admin_t,
    transactional_id: *const c_char,
) -> *mut kafka_admin_TerminateTransactionResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `transactional_id` is null or a NUL-terminated
    // string, as `c_str_to_string` requires; it is copied.
    let transactional_id_ = unsafe { c_str_to_string(transactional_id) };
    let result = client.call(|admin| admin.force_terminate_transaction(&transactional_id_));
    box_terminate_transaction_result(result, &client.future_ctx())
}

/// Force terminate a transaction for the given transactional ID. This operation aborts any ongoing
/// transaction associated with the transactional ID. It's similar to fenceProducers but only
/// targets a single transactional ID to handle long-running transactions when 2PC is enabled.
///
/// # Parameters
///
/// - `transactional_id` - The ID of the transaction to terminate.
/// - `options` - The options to use when terminating the transaction.
///
/// # Returns
///
/// The TerminateTransactionResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with
/// `kafka_admin_TerminateTransactionResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_force_terminate_transaction_with_options(
    self_: *const kafka_admin_Admin_t,
    transactional_id: *const c_char,
    options: *const kafka_admin_TerminateTransactionOptions_t,
) -> *mut kafka_admin_TerminateTransactionResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `transactional_id` is null or a NUL-terminated
    // string, as `c_str_to_string` requires; it is copied.
    let transactional_id_ = unsafe { c_str_to_string(transactional_id) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_TerminateTransactionOptions_t` handle that no other call mutates during this
    // one, so no setter runs on it while it is cloned, as `terminate_transaction_options_ref`
    // requires.
    let options_ = unsafe { terminate_transaction_options_ref(options) }.clone();
    let result = client.call(|admin| admin.force_terminate_transaction_with_options(&transactional_id_, options_));
    box_terminate_transaction_result(result, &client.future_ctx())
}

/// Get information about the nodes in the cluster, using the default options.
///
/// This is a convenience method for `kafka_admin_Admin_describe_cluster_with_options` with default
/// options. See the overload for more details.
///
/// # Returns
///
/// The DescribeClusterResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. The result handle is owned by the caller, freed with
/// `kafka_admin_DescribeClusterResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_cluster(
    self_: *const kafka_admin_Admin_t,
) -> *mut kafka_admin_DescribeClusterResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    let result = client.call(|admin| admin.describe_cluster());
    box_describe_cluster_result(result, &client.future_ctx())
}

/// Get information about the nodes in the cluster.
///
/// To obtain broker cluster information, you must configure `bootstrap.servers`. To obtain
/// controller cluster information, you must configure `bootstrap.controllers`.
///
/// # Parameters
///
/// - `options` - The options to use when getting information about the cluster.
///
/// # Returns
///
/// The DescribeClusterResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with `kafka_admin_DescribeClusterResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_cluster_with_options(
    self_: *const kafka_admin_Admin_t,
    options: *const kafka_admin_DescribeClusterOptions_t,
) -> *mut kafka_admin_DescribeClusterResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_DescribeClusterOptions_t` handle that no other call mutates during this one, so
    // no setter runs on it while it is cloned, as `describe_cluster_options_ref` requires.
    let options_ = unsafe { describe_cluster_options_ref(options) }.clone();
    let result = client.call(|admin| admin.describe_cluster_with_options(options_));
    box_describe_cluster_result(result, &client.future_ctx())
}

/// Get the configuration for the specified resources with the default options.
///
/// This is a convenience method for `kafka_admin_Admin_describe_configs_with_options` with default
/// options. See the overload for more details.
///
/// This operation is supported by brokers with version 0.11.0.0 or higher.
///
/// # Parameters
///
/// - `config_resources` - The resources (topic and broker resource types are currently supported)
///
/// # Returns
///
/// The DescribeConfigsResult
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `config_resources` is a `kafka_List_t` of `const kafka_common_config_ConfigResource_t *`, each
///   live; `NULL` reads as an empty list.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_DescribeConfigsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_configs(
    self_: *const kafka_admin_Admin_t,
    config_resources: *const kafka_List_t,
) -> *mut kafka_admin_DescribeConfigsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `config_resources` is null or a valid list, alive and
    // unmutated during this call, as `list_refs` requires, and `list_refs` passes the closure its
    // elements only, each a live `const kafka_common_config_ConfigResource_t *` handle no other
    // call mutates meanwhile, as `config_resource_ref` requires; each value is cloned before the
    // call returns.
    let config_resources_ = unsafe { list_refs(config_resources, |p| config_resource_ref(p as *const _).clone()) };
    let result = client.call(|admin| admin.describe_configs(&config_resources_));
    box_describe_configs_result(result, &client.future_ctx())
}

/// Get the configuration for the specified resources.
///
/// The returned configuration includes default values and the `kafka_admin_ConfigEntry_is_default`
/// method can be used to distinguish them from user supplied values.
///
/// The value of config entries where `kafka_admin_ConfigEntry_is_sensitive` is true is always
/// `NULL` so that sensitive information is not disclosed.
///
/// Config entries where `kafka_admin_ConfigEntry_is_read_only` is true cannot be updated.
///
/// The different behavior of nonexistent resource:
///
/// - `kafka_common_config_ConfigResource_Type_BROKER`: fails with
///   `kafka_common_Error_is_timeout_error`
/// - `kafka_common_config_ConfigResource_Type_TOPIC`: fails with
///   `kafka_common_Error_is_unknown_topic_or_partition_error`
/// - `kafka_common_config_ConfigResource_Type_GROUP`: just return default configs even if the
///   target group is nonexistent
/// - `kafka_common_config_ConfigResource_Type_BROKER_LOGGER`: fails with
///   `kafka_common_Error_is_timeout_error`
/// - `kafka_common_config_ConfigResource_Type_CLIENT_METRICS`: will return empty configs
///
/// Note that you cannot describe broker configs or broker logger using `bootstrap.controllers`, and
/// you cannot describe controller configs or controller logger using `bootstrap.servers`.
///
/// This operation is supported by brokers with version 0.11.0.0 or higher.
///
/// # Parameters
///
/// - `config_resources` - The resources (topic and broker resource types are currently supported)
/// - `options` - The options to use when describing configs
///
/// # Returns
///
/// The DescribeConfigsResult
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `config_resources` is a `kafka_List_t` of `const kafka_common_config_ConfigResource_t *`, each
///   live; `NULL` reads as an empty list.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_DescribeConfigsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_configs_with_options(
    self_: *const kafka_admin_Admin_t,
    config_resources: *const kafka_List_t,
    options: *const kafka_admin_DescribeConfigsOptions_t,
) -> *mut kafka_admin_DescribeConfigsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `config_resources` is null or a valid list, alive and
    // unmutated during this call, as `list_refs` requires, and `list_refs` passes the closure its
    // elements only, each a live `const kafka_common_config_ConfigResource_t *` handle no other
    // call mutates meanwhile, as `config_resource_ref` requires; each value is cloned before the
    // call returns.
    let config_resources_ = unsafe { list_refs(config_resources, |p| config_resource_ref(p as *const _).clone()) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_DescribeConfigsOptions_t` handle that no other call mutates during this one, so
    // no setter runs on it while it is cloned, as `describe_configs_options_ref` requires.
    let options_ = unsafe { describe_configs_options_ref(options) }.clone();
    let result = client.call(|admin| admin.describe_configs_with_options(&config_resources_, options_));
    box_describe_configs_result(result, &client.future_ctx())
}

/// Incrementally updates the configuration for the specified resources with default options.
///
/// This is a convenience method for `kafka_admin_Admin_incremental_alter_configs_with_options` with
/// default options. See the overload for more details.
///
/// This operation is supported by brokers with version 2.3.0 or higher.
///
/// # Parameters
///
/// - `configs` - The resources with their configs
///
/// # Returns
///
/// The AlterConfigsResult
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `configs` is a `kafka_Map_t` of `const kafka_common_config_ConfigResource_t *` keys to `const
///   kafka_List_t *` values, each a list of `const kafka_admin_AlterConfigOp_t *`; every handle is
///   live, and a `NULL` map or list reads as empty.
///
/// The result handle is owned by the caller, freed with `kafka_admin_AlterConfigsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_incremental_alter_configs(
    self_: *const kafka_admin_Admin_t,
    configs: *const kafka_Map_t,
) -> *mut kafka_admin_AlterConfigsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `configs` is null or a valid map, alive and unmutated
    // during this call, as `keyed` requires. `keyed` passes the key closure its keys only, each a
    // live `const kafka_common_config_ConfigResource_t *` handle no other call mutates meanwhile,
    // as `config_resource_ref` requires, and the value closure its values only, each a valid
    // `kafka_List_t *` likewise alive and unmutated, as `list_refs` requires, whose elements, the
    // only pointers `list_refs` passes on, are live `const kafka_admin_AlterConfigOp_t *` handles
    // no other call mutates, as `alter_config_op_ref` requires. Each value is cloned before the
    // call returns.
    let configs_ = unsafe {
        keyed(
            configs,
            |k| config_resource_ref(k as *const _).clone(),
            |v| list_refs(v as *const kafka_List_t, |p| alter_config_op_ref(p as *const _).clone()),
        )
    };
    let result = client.call(|admin| admin.incremental_alter_configs(&configs_));
    box_alter_configs_result(result, &client.future_ctx())
}

/// Incrementally update the configuration for the specified resources.
///
/// Updates are not transactional so they may succeed for some resources while fail for others. The
/// configs for a particular resource are updated atomically.
///
/// The following errors can be anticipated when calling `kafka_common_KafkaFuture_get` on the
/// futures obtained from the returned `AlterConfigsResult`:
///
/// - `kafka_common_Error_is_cluster_authorization_error` if the authenticated user didn't have
///   alter access to the cluster.
/// - `kafka_common_Error_is_topic_authorization_error` if the authenticated user didn't have alter
///   access to the Topic.
/// - `kafka_common_Error_is_unknown_topic_or_partition_error` if the Topic doesn't exist.
/// - `kafka_common_Error_is_invalid_request_error` if the request details are invalid. e.g., a
///   configuration key was specified more than once for a resource
///
/// This operation is supported by brokers with version 2.3.0 or higher.
///
/// # Parameters
///
/// - `configs` - The resources with their configs
/// - `options` - The options to use when altering configs
///
/// # Returns
///
/// The AlterConfigsResult
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `configs` is a `kafka_Map_t` of `const kafka_common_config_ConfigResource_t *` keys to `const
///   kafka_List_t *` values, each a list of `const kafka_admin_AlterConfigOp_t *`; every handle is
///   live, and a `NULL` map or list reads as empty.
///
/// The result handle is owned by the caller, freed with `kafka_admin_AlterConfigsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_incremental_alter_configs_with_options(
    self_: *const kafka_admin_Admin_t,
    configs: *const kafka_Map_t,
    options: *const kafka_admin_AlterConfigsOptions_t,
) -> *mut kafka_admin_AlterConfigsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `configs` is null or a valid map, alive and unmutated
    // during this call, as `keyed` requires. `keyed` passes the key closure its keys only, each a
    // live `const kafka_common_config_ConfigResource_t *` handle no other call mutates meanwhile,
    // as `config_resource_ref` requires, and the value closure its values only, each a valid
    // `kafka_List_t *` likewise alive and unmutated, as `list_refs` requires, whose elements, the
    // only pointers `list_refs` passes on, are live `const kafka_admin_AlterConfigOp_t *` handles
    // no other call mutates, as `alter_config_op_ref` requires. Each value is cloned before the
    // call returns.
    let configs_ = unsafe {
        keyed(
            configs,
            |k| config_resource_ref(k as *const _).clone(),
            |v| list_refs(v as *const kafka_List_t, |p| alter_config_op_ref(p as *const _).clone()),
        )
    };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_AlterConfigsOptions_t` handle that no other call mutates during this one, so no
    // setter runs on it while it is cloned, as `alter_configs_options_ref` requires.
    let options_ = unsafe { alter_configs_options_ref(options) }.clone();
    let result = client.call(|admin| admin.incremental_alter_configs_with_options(&configs_, options_));
    box_alter_configs_result(result, &client.future_ctx())
}

/// List all configuration resources available in the cluster with the default options.
///
/// This is a convenience method for `kafka_admin_Admin_list_config_resources_with_options` with
/// default options. See the overload for more details.
///
/// # Returns
///
/// The ListConfigurationResourcesResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. The result handle is owned by the caller, freed with
/// `kafka_admin_ListConfigResourcesResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_list_config_resources(
    self_: *const kafka_admin_Admin_t,
) -> *mut kafka_admin_ListConfigResourcesResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    let result = client.call(|admin| admin.list_config_resources());
    box_list_config_resources_result(result, &client.future_ctx())
}

/// List the configuration resources available in the cluster which matches config resource type. If
/// no config resource types are specified, all configuration resources will be listed.
///
/// # Parameters
///
/// - `config_resource_types` - The set of configuration resource types to list.
/// - `options` - The options to use when listing the configuration resources.
///
/// # Returns
///
/// The ListConfigurationResourcesResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `config_resource_types` is a `kafka_List_t` of `const
///   kafka_common_config_ConfigResource_Type_t *` singletons; `NULL` reads as an empty list.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_ListConfigResourcesResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_list_config_resources_with_options(
    self_: *const kafka_admin_Admin_t,
    config_resource_types: *const kafka_List_t,
    options: *const kafka_admin_ListConfigResourcesOptions_t,
) -> *mut kafka_admin_ListConfigResourcesResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `config_resource_types` is null or a valid list,
    // alive and unmutated during this call, as `list_refs` requires, and `list_refs` passes the
    // closure its elements only, each one of the `kafka_common_config_ConfigResource_Type_t`
    // singletons, as `type_of` requires; each value is copied out.
    let config_resource_types_ = unsafe { list_refs(config_resource_types, |p| type_of(p as *const _)) }
        .into_iter()
        .collect::<HashSet<_>>();
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_ListConfigResourcesOptions_t` handle that no other call mutates during this one,
    // so no setter runs on it while it is cloned, as `list_config_resources_options_ref` requires.
    let options_ = unsafe { list_config_resources_options_ref(options) }.clone();
    let result = client.call(|admin| admin.list_config_resources_with_options(&config_resource_types_, options_));
    box_list_config_resources_result(result, &client.future_ctx())
}

/// Query the information of all log directories on the given set of brokers
///
/// This is a convenience method for `kafka_admin_Admin_describe_log_dirs_with_options` with default
/// options. See the overload for more details.
///
/// This operation is supported by brokers with version 1.0.0 or higher.
///
/// # Parameters
///
/// - `brokers` - A list of brokers
///
/// # Returns
///
/// The DescribeLogDirsResult
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `brokers` is a `kafka_List_t` of `const int32_t *`, each pointing at a live value; `NULL`
///   reads as an empty list.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_DescribeLogDirsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_log_dirs(
    self_: *const kafka_admin_Admin_t,
    brokers: *const kafka_List_t,
) -> *mut kafka_admin_DescribeLogDirsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `brokers` is null or a valid list, alive and
    // unmutated during this call, as `list_refs` requires, and `list_refs` passes the closure its
    // elements only, each a live `const int32_t *` pointing at an `int32_t` no other call writes
    // meanwhile, so reading it is sound.
    let brokers_ = unsafe { list_refs(brokers, |p| *(p as *const i32)) };
    let result = client.call(|admin| admin.describe_log_dirs(&brokers_));
    box_describe_log_dirs_result(result, &client.future_ctx())
}

/// Query the information of all log directories on the given set of brokers
///
/// This operation is supported by brokers with version 1.0.0 or higher.
///
/// # Parameters
///
/// - `brokers` - A list of brokers
/// - `options` - The options to use when querying log dir info
///
/// # Returns
///
/// The DescribeLogDirsResult
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `brokers` is a `kafka_List_t` of `const int32_t *`, each pointing at a live value; `NULL`
///   reads as an empty list.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_DescribeLogDirsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_log_dirs_with_options(
    self_: *const kafka_admin_Admin_t,
    brokers: *const kafka_List_t,
    options: *const kafka_admin_DescribeLogDirsOptions_t,
) -> *mut kafka_admin_DescribeLogDirsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `brokers` is null or a valid list, alive and
    // unmutated during this call, as `list_refs` requires, and `list_refs` passes the closure its
    // elements only, each a live `const int32_t *` pointing at an `int32_t` no other call writes
    // meanwhile, so reading it is sound.
    let brokers_ = unsafe { list_refs(brokers, |p| *(p as *const i32)) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_DescribeLogDirsOptions_t` handle that no other call mutates during this one, so
    // no setter runs on it while it is cloned, as `describe_log_dirs_options_ref` requires.
    let options_ = unsafe { describe_log_dirs_options_ref(options) }.clone();
    let result = client.call(|admin| admin.describe_log_dirs_with_options(&brokers_, options_));
    box_describe_log_dirs_result(result, &client.future_ctx())
}

/// Change the log directory for the specified replicas. If the replica does not exist on the
/// broker, the result shows REPLICA_NOT_AVAILABLE for the given replica and the replica will be
/// created in the given log directory on the broker when it is created later. If the replica
/// already exists on the broker, the replica will be moved to the given log directory if it is not
/// already there. For detailed result, inspect the returned `AlterReplicaLogDirsResult` instance.
///
/// This operation is not transactional so it may succeed for some replicas while fail for others.
///
/// This is a convenience method for `kafka_admin_Admin_alter_replica_log_dirs_with_options` with
/// default options. See the overload for more details.
///
/// This operation is supported by brokers with version 1.1.0 or higher.
///
/// # Parameters
///
/// - `replica_assignment` - The replicas with their log directory absolute path
///
/// # Returns
///
/// The AlterReplicaLogDirsResult
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `replica_assignment` is a `kafka_Map_t` of `const kafka_common_TopicPartitionReplica_t *`
///   keys, each live, to NUL-terminated `const char *` log directory values; `NULL` reads as an
///   empty map and a `NULL` value as an empty string.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_AlterReplicaLogDirsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_alter_replica_log_dirs(
    self_: *const kafka_admin_Admin_t,
    replica_assignment: *const kafka_Map_t,
) -> *mut kafka_admin_AlterReplicaLogDirsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `replica_assignment` is null or a valid map, alive
    // and unmutated during this call, as `keyed` requires. `keyed` passes the key closure its keys
    // only, each a live `const kafka_common_TopicPartitionReplica_t *` handle no other call mutates
    // meanwhile, as `topic_partition_replica_ref` requires, and the value closure its values only,
    // each null or a NUL-terminated string, as `c_str_to_string` requires. Each key is cloned and
    // each string copied before the call returns.
    let replica_assignment_ = unsafe {
        keyed(
            replica_assignment,
            |k| topic_partition_replica_ref(k as *const _).clone(),
            |v| c_str_to_string(v as *const c_char),
        )
    };
    let result = client.call(|admin| admin.alter_replica_log_dirs(&replica_assignment_));
    box_alter_replica_log_dirs_result(result, &client.future_ctx())
}

/// Change the log directory for the specified replicas. If the replica does not exist on the
/// broker, the result shows REPLICA_NOT_AVAILABLE for the given replica and the replica will be
/// created in the given log directory on the broker when it is created later. If the replica
/// already exists on the broker, the replica will be moved to the given log directory if it is not
/// already there. For detailed result, inspect the returned `AlterReplicaLogDirsResult` instance.
///
/// This operation is not transactional so it may succeed for some replicas while fail for others.
///
/// This operation is supported by brokers with version 1.1.0 or higher.
///
/// # Parameters
///
/// - `replica_assignment` - The replicas with their log directory absolute path
/// - `options` - The options to use when changing replica dir
///
/// # Returns
///
/// The AlterReplicaLogDirsResult
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `replica_assignment` is a `kafka_Map_t` of `const kafka_common_TopicPartitionReplica_t *`
///   keys, each live, to NUL-terminated `const char *` log directory values; `NULL` reads as an
///   empty map and a `NULL` value as an empty string.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_AlterReplicaLogDirsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_alter_replica_log_dirs_with_options(
    self_: *const kafka_admin_Admin_t,
    replica_assignment: *const kafka_Map_t,
    options: *const kafka_admin_AlterReplicaLogDirsOptions_t,
) -> *mut kafka_admin_AlterReplicaLogDirsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `replica_assignment` is null or a valid map, alive
    // and unmutated during this call, as `keyed` requires. `keyed` passes the key closure its keys
    // only, each a live `const kafka_common_TopicPartitionReplica_t *` handle no other call mutates
    // meanwhile, as `topic_partition_replica_ref` requires, and the value closure its values only,
    // each null or a NUL-terminated string, as `c_str_to_string` requires. Each key is cloned and
    // each string copied before the call returns.
    let replica_assignment_ = unsafe {
        keyed(
            replica_assignment,
            |k| topic_partition_replica_ref(k as *const _).clone(),
            |v| c_str_to_string(v as *const c_char),
        )
    };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_AlterReplicaLogDirsOptions_t` handle that no other call mutates during this one,
    // so no setter runs on it while it is cloned, as `alter_replica_log_dirs_options_ref` requires.
    let options_ = unsafe { alter_replica_log_dirs_options_ref(options) }.clone();
    let result = client.call(|admin| admin.alter_replica_log_dirs_with_options(&replica_assignment_, options_));
    box_alter_replica_log_dirs_result(result, &client.future_ctx())
}

/// Query the replica log directory information for the specified replicas.
///
/// This is a convenience method for `kafka_admin_Admin_describe_replica_log_dirs_with_options` with
/// default options. See the overload for more details.
///
/// This operation is supported by brokers with version 1.0.0 or higher.
///
/// # Parameters
///
/// - `replicas` - The replicas to query
///
/// # Returns
///
/// The DescribeReplicaLogDirsResult
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `replicas` is a `kafka_List_t` of `const kafka_common_TopicPartitionReplica_t *`, each live;
///   `NULL` reads as an empty list.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_DescribeReplicaLogDirsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_replica_log_dirs(
    self_: *const kafka_admin_Admin_t,
    replicas: *const kafka_List_t,
) -> *mut kafka_admin_DescribeReplicaLogDirsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `replicas` is null or a valid list, alive and
    // unmutated during this call, as `list_refs` requires, and `list_refs` passes the closure its
    // elements only, each a live `const kafka_common_TopicPartitionReplica_t *` handle no other
    // call mutates meanwhile, as `topic_partition_replica_ref` requires; each value is cloned
    // before the call returns.
    let replicas_ = unsafe { list_refs(replicas, |p| topic_partition_replica_ref(p as *const _).clone()) };
    let result = client.call(|admin| admin.describe_replica_log_dirs(&replicas_));
    box_describe_replica_log_dirs_result(result, &client.future_ctx())
}

/// Query the replica log directory information for the specified replicas.
///
/// This operation is supported by brokers with version 1.0.0 or higher.
///
/// # Parameters
///
/// - `replicas` - The replicas to query
/// - `options` - The options to use when querying replica log dir info
///
/// # Returns
///
/// The DescribeReplicaLogDirsResult
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `replicas` is a `kafka_List_t` of `const kafka_common_TopicPartitionReplica_t *`, each live;
///   `NULL` reads as an empty list.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_DescribeReplicaLogDirsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_replica_log_dirs_with_options(
    self_: *const kafka_admin_Admin_t,
    replicas: *const kafka_List_t,
    options: *const kafka_admin_DescribeReplicaLogDirsOptions_t,
) -> *mut kafka_admin_DescribeReplicaLogDirsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `replicas` is null or a valid list, alive and
    // unmutated during this call, as `list_refs` requires, and `list_refs` passes the closure its
    // elements only, each a live `const kafka_common_TopicPartitionReplica_t *` handle no other
    // call mutates meanwhile, as `topic_partition_replica_ref` requires; each value is cloned
    // before the call returns.
    let replicas_ = unsafe { list_refs(replicas, |p| topic_partition_replica_ref(p as *const _).clone()) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_DescribeReplicaLogDirsOptions_t` handle that no other call mutates during this
    // one, so no setter runs on it while it is cloned, as `describe_replica_log_dirs_options_ref`
    // requires.
    let options_ = unsafe { describe_replica_log_dirs_options_ref(options) }.clone();
    let result = client.call(|admin| admin.describe_replica_log_dirs_with_options(&replicas_, options_));
    box_describe_replica_log_dirs_result(result, &client.future_ctx())
}

/// Elect a replica as leader for topic partitions.
///
/// This is a convenience method for `kafka_admin_Admin_elect_leaders_with_options` with default
/// options.
///
/// # Parameters
///
/// - `election_type` - The type of election to conduct.
/// - `partitions` - The topics and partitions for which to conduct elections.
///
/// # Returns
///
/// The ElectLeadersResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `partitions` is a `kafka_List_t` of `const kafka_common_TopicPartition_t *`, each live, or
///   `NULL` for Java's absent partition set (`null` / `Optional.empty()`), which is not the same as
///   an empty list.
///
/// The result handle is owned by the caller, freed with `kafka_admin_ElectLeadersResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_elect_leaders(
    self_: *const kafka_admin_Admin_t,
    election_type: *const kafka_common_ElectionType_t,
    partitions: *const kafka_List_t,
) -> *mut kafka_admin_ElectLeadersResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `election_type` is one of the
    // `kafka_common_ElectionType_t` singletons, as `value_of` requires.
    let election_type_ = unsafe { election_type_value_of(election_type) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null, standing for `None`, or a valid
    // list of `const kafka_common_TopicPartition_t *` handles, all live and unmutated during this
    // call, as `optional_tp_set` requires.
    let partitions_ = unsafe { optional_tp_set(partitions) };
    let result = client.call(|admin| admin.elect_leaders(election_type_, partitions_));
    box_elect_leaders_result(result, &client.future_ctx())
}

/// Elect a replica as leader for the given `partitions`, or for all partitions if the argument to
/// `partitions` is `NULL`.
///
/// This operation is not transactional so it may succeed for some partitions while fail for others.
///
/// It may take several seconds after this method returns success for all the brokers in the cluster
/// to become aware that the partitions have new leaders. During this time,
/// `kafka_admin_Admin_describe_topics_with_topic_names` may not return information about the
/// partitions' new leaders.
///
/// This operation is supported by brokers with version 2.2.0 or later if preferred election is use;
/// otherwise the brokers most be 2.4.0 or higher.
///
/// The following errors can be anticipated when calling `kafka_common_KafkaFuture_get` on the
/// future obtained from the returned `ElectLeadersResult`:
///
/// - `kafka_common_Error_is_cluster_authorization_error` if the authenticated user didn't have
///   alter access to the cluster.
/// - `kafka_common_Error_is_unknown_topic_or_partition_error` if the topic or partition did not
///   exist within the cluster.
/// - `kafka_common_Error_is_invalid_topic_error` if the topic was already queued for deletion.
/// - `kafka_common_Error_is_not_controller_error` if the request was sent to a broker that was not
///   the controller for the cluster.
/// - `kafka_common_Error_is_timeout_error` if the request timed out before the election was
///   complete.
/// - `kafka_common_Error_is_leader_not_available_error` if the preferred leader was not alive or
///   not in the ISR.
///
/// # Parameters
///
/// - `election_type` - The type of election to conduct.
/// - `partitions` - The topics and partitions for which to conduct elections.
/// - `options` - The options to use when electing the leaders.
///
/// # Returns
///
/// The ElectLeadersResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `partitions` is a `kafka_List_t` of `const kafka_common_TopicPartition_t *`, each live, or
///   `NULL` for Java's absent partition set (`null` / `Optional.empty()`), which is not the same as
///   an empty list.
///
/// The result handle is owned by the caller, freed with `kafka_admin_ElectLeadersResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_elect_leaders_with_options(
    self_: *const kafka_admin_Admin_t,
    election_type: *const kafka_common_ElectionType_t,
    partitions: *const kafka_List_t,
    options: *const kafka_admin_ElectLeadersOptions_t,
) -> *mut kafka_admin_ElectLeadersResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `election_type` is one of the
    // `kafka_common_ElectionType_t` singletons, as `value_of` requires.
    let election_type_ = unsafe { election_type_value_of(election_type) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null, standing for `None`, or a valid
    // list of `const kafka_common_TopicPartition_t *` handles, all live and unmutated during this
    // call, as `optional_tp_set` requires.
    let partitions_ = unsafe { optional_tp_set(partitions) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_ElectLeadersOptions_t` handle that no other call mutates during this one, so no
    // setter runs on it while it is cloned, as `elect_leaders_options_ref` requires.
    let options_ = unsafe { elect_leaders_options_ref(options) }.clone();
    let result = client.call(|admin| admin.elect_leaders_with_options(election_type_, partitions_, options_));
    box_elect_leaders_result(result, &client.future_ctx())
}

/// Change the reassignments for one or more partitions. Providing `NULL` will revert the
/// reassignment for the associated partition. This is a convenience method for
/// `kafka_admin_Admin_alter_partition_reassignments_with_options` with default options. See the
/// overload for more details.
///
/// # Parameters
///
/// - `reassignments` - The reassignments to add, modify, or remove. See `NewPartitionReassignment`.
///
/// # Returns
///
/// The result.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `reassignments` is a `kafka_Map_t` of `const kafka_common_TopicPartition_t *` keys to `const
///   kafka_admin_NewPartitionReassignment_t *` values, each key live; a `NULL` value is Java's
///   `Optional.empty()`, which cancels that partition's reassignment, and a `NULL` map reads as an
///   empty map.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_AlterPartitionReassignmentsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_alter_partition_reassignments(
    self_: *const kafka_admin_Admin_t,
    reassignments: *const kafka_Map_t,
) -> *mut kafka_admin_AlterPartitionReassignmentsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `reassignments` is null or a valid map whose keys are
    // live `const kafka_common_TopicPartition_t *` handles, alive and unmutated during this call,
    // as `tp_keyed` requires, and `tp_keyed` passes the closure its values only, each null or a
    // live `const kafka_admin_NewPartitionReassignment_t *` handle no other call mutates meanwhile,
    // and the closure reads only the non-null ones, as `new_partition_reassignment_ref` requires;
    // each value is cloned before the call returns.
    let reassignments_ = unsafe {
        tp_keyed(reassignments, |p| {
            (!p.is_null()).then(|| new_partition_reassignment_ref(p as *const _).clone())
        })
    };
    let result = client.call(|admin| admin.alter_partition_reassignments(&reassignments_));
    box_alter_partition_reassignments_result(result, &client.future_ctx())
}

/// Change the reassignments for one or more partitions. Providing `NULL` will revert the
/// reassignment for the associated partition.
///
/// The following errors can be anticipated when calling `kafka_common_KafkaFuture_get` on the
/// futures obtained from the returned `AlterPartitionReassignmentsResult`:
///
/// - `kafka_common_Error_is_cluster_authorization_error` If the authenticated user didn't have
///   alter access to the cluster.
/// - `kafka_common_Error_is_unknown_topic_or_partition_error` If the topic or partition does not
///   exist within the cluster.
/// - `kafka_common_Error_is_timeout_error` if the request timed out before the controller could
///   record the new assignments.
/// - `kafka_common_Error_is_invalid_replica_assignment_error` If the specified assignment was not
///   valid.
/// - `kafka_common_Error_is_invalid_replication_factor_error` If the replication factor was changed
///   in an invalid way. Only thrown when
///   `kafka_admin_AlterPartitionReassignmentsOptions_allow_replication_factor_change` is set to
///   false and the request is attempting to alter reassignments (not cancel)
/// - `kafka_common_Error_is_unsupported_version_error` If
///   `kafka_admin_AlterPartitionReassignmentsOptions_allow_replication_factor_change` was changed
///   outside the default and the server does not support the option (e.g due to an old Kafka
///   version).
/// - `kafka_common_Error_is_no_reassignment_in_progress_error` If there was an attempt to cancel a
///   reassignment for a partition which was not being reassigned.
///
/// # Parameters
///
/// - `reassignments` - The reassignments to add, modify, or remove. See `NewPartitionReassignment`.
/// - `options` - The options to use.
///
/// # Returns
///
/// The result.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `reassignments` is a `kafka_Map_t` of `const kafka_common_TopicPartition_t *` keys to `const
///   kafka_admin_NewPartitionReassignment_t *` values, each key live; a `NULL` value is Java's
///   `Optional.empty()`, which cancels that partition's reassignment, and a `NULL` map reads as an
///   empty map.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_AlterPartitionReassignmentsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_alter_partition_reassignments_with_options(
    self_: *const kafka_admin_Admin_t,
    reassignments: *const kafka_Map_t,
    options: *const kafka_admin_AlterPartitionReassignmentsOptions_t,
) -> *mut kafka_admin_AlterPartitionReassignmentsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `reassignments` is null or a valid map whose keys are
    // live `const kafka_common_TopicPartition_t *` handles, alive and unmutated during this call,
    // as `tp_keyed` requires, and `tp_keyed` passes the closure its values only, each null or a
    // live `const kafka_admin_NewPartitionReassignment_t *` handle no other call mutates meanwhile,
    // and the closure reads only the non-null ones, as `new_partition_reassignment_ref` requires;
    // each value is cloned before the call returns.
    let reassignments_ = unsafe {
        tp_keyed(reassignments, |p| {
            (!p.is_null()).then(|| new_partition_reassignment_ref(p as *const _).clone())
        })
    };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_AlterPartitionReassignmentsOptions_t` handle that no other call mutates during
    // this one, so no setter runs on it while it is cloned, as
    // `alter_partition_reassignments_options_ref` requires.
    let options_ = unsafe { alter_partition_reassignments_options_ref(options) }.clone();
    let result = client.call(|admin| admin.alter_partition_reassignments_with_options(&reassignments_, options_));
    box_alter_partition_reassignments_result(result, &client.future_ctx())
}

/// List all of the current partition reassignments.
///
/// This is a convenience method for `kafka_admin_Admin_list_partition_reassignments_with_options`
/// with default options. See the overload for more details.
///
/// # Returns
///
/// The result.
///
/// # Safety
///
/// `self_` must be a live handle or view. The result handle is owned by the caller, freed with
/// `kafka_admin_ListPartitionReassignmentsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_list_partition_reassignments(
    self_: *const kafka_admin_Admin_t,
) -> *mut kafka_admin_ListPartitionReassignmentsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    let result = client.call(|admin| admin.list_partition_reassignments());
    box_list_partition_reassignments_result(result, &client.future_ctx())
}

/// List the current reassignments for the given partitions.
///
/// This is a convenience method for
/// `kafka_admin_Admin_list_partition_reassignments_with_partitions_options` with default options.
/// See the overload for more details.
///
/// # Parameters
///
/// - `partitions` - The topic partitions to list reassignments for.
///
/// # Returns
///
/// The result.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `partitions` is a `kafka_List_t` of `const kafka_common_TopicPartition_t *`, each live; `NULL`
///   reads as an empty list.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_ListPartitionReassignmentsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_list_partition_reassignments_with_partitions(
    self_: *const kafka_admin_Admin_t,
    partitions: *const kafka_List_t,
) -> *mut kafka_admin_ListPartitionReassignmentsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list of
    // `const kafka_common_TopicPartition_t *` handles, all live and unmutated during this call, as
    // `list_topic_partitions` requires; the values are cloned.
    let partitions_ = unsafe { list_topic_partitions(partitions) }.into_iter().collect::<HashSet<_>>();
    let result = client.call(|admin| admin.list_partition_reassignments_with_partitions(partitions_));
    box_list_partition_reassignments_result(result, &client.future_ctx())
}

/// List all of the current partition reassignments.
///
/// The following errors can be anticipated when calling `kafka_common_KafkaFuture_get` on the
/// futures obtained from the returned `ListPartitionReassignmentsResult`:
///
/// - `kafka_common_Error_is_cluster_authorization_error` If the authenticated user doesn't have
///   alter access to the cluster.
/// - `kafka_common_Error_is_unknown_topic_or_partition_error` If a given topic or partition does
///   not exist.
/// - `kafka_common_Error_is_timeout_error` If the request timed out before the controller could
///   list the current reassignments.
///
/// # Parameters
///
/// - `options` - The options to use.
///
/// # Returns
///
/// The result.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with
/// `kafka_admin_ListPartitionReassignmentsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_list_partition_reassignments_with_options(
    self_: *const kafka_admin_Admin_t,
    options: *const kafka_admin_ListPartitionReassignmentsOptions_t,
) -> *mut kafka_admin_ListPartitionReassignmentsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_ListPartitionReassignmentsOptions_t` handle that no other call mutates during
    // this one, so no setter runs on it while it is cloned, as
    // `list_partition_reassignments_options_ref` requires.
    let options_ = unsafe { list_partition_reassignments_options_ref(options) }.clone();
    let result = client.call(|admin| admin.list_partition_reassignments_with_options(options_));
    box_list_partition_reassignments_result(result, &client.future_ctx())
}

/// List the current reassignments for the given partitions.
///
/// The following errors can be anticipated when calling `kafka_common_KafkaFuture_get` on the
/// futures obtained from the returned `ListPartitionReassignmentsResult`:
///
/// - `kafka_common_Error_is_cluster_authorization_error` If the authenticated user doesn't have
///   alter access to the cluster.
/// - `kafka_common_Error_is_unknown_topic_or_partition_error` If a given topic or partition does
///   not exist.
/// - `kafka_common_Error_is_timeout_error` If the request timed out before the controller could
///   list the current reassignments.
///
/// # Parameters
///
/// - `partitions` - the partitions we want to get reassignment for, or `NULL` if we want to get the
///   reassignments for all partitions in the cluster
/// - `options` - The options to use.
///
/// # Returns
///
/// The result.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `partitions` is a `kafka_List_t` of `const kafka_common_TopicPartition_t *`, each live, or
///   `NULL` for Java's absent partition set (`null` / `Optional.empty()`), which is not the same as
///   an empty list.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_ListPartitionReassignmentsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_list_partition_reassignments_with_partitions_options(
    self_: *const kafka_admin_Admin_t,
    partitions: *const kafka_List_t,
    options: *const kafka_admin_ListPartitionReassignmentsOptions_t,
) -> *mut kafka_admin_ListPartitionReassignmentsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null, standing for `None`, or a valid
    // list of `const kafka_common_TopicPartition_t *` handles, all live and unmutated during this
    // call, as `optional_tp_set` requires.
    let partitions_ = unsafe { optional_tp_set(partitions) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_ListPartitionReassignmentsOptions_t` handle that no other call mutates during
    // this one, so no setter runs on it while it is cloned, as
    // `list_partition_reassignments_options_ref` requires.
    let options_ = unsafe { list_partition_reassignments_options_ref(options) }.clone();
    let result = client.call(|admin| admin.list_partition_reassignments_with_partitions_options(partitions_, options_));
    box_list_partition_reassignments_result(result, &client.future_ctx())
}

/// List offset for the specified partitions and OffsetSpec. This operation enables to find the
/// beginning offset, end offset as well as the offset matching a timestamp in partitions.
///
/// This is a convenience method for `kafka_admin_Admin_list_offsets_with_options`
///
/// # Parameters
///
/// - `topic_partition_offsets` - The mapping from partition to the OffsetSpec to look up.
///
/// # Returns
///
/// The ListOffsetsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `topic_partition_offsets` is a `kafka_Map_t` of `const kafka_common_TopicPartition_t *` keys
///   to `const kafka_admin_OffsetSpec_t *` values, each live; `NULL` reads as an empty map.
///
/// The result handle is owned by the caller, freed with `kafka_admin_ListOffsetsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_list_offsets(
    self_: *const kafka_admin_Admin_t,
    topic_partition_offsets: *const kafka_Map_t,
) -> *mut kafka_admin_ListOffsetsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `topic_partition_offsets` is null or a valid map
    // whose keys are live `const kafka_common_TopicPartition_t *` handles, alive and unmutated
    // during this call, as `tp_keyed` requires, and `tp_keyed` passes the closure its values only,
    // each a `const kafka_admin_OffsetSpec_t *` singleton, or an owned handle not yet destroyed
    // that no other call frees meanwhile, as `offset_spec_value_of` requires; each value is cloned
    // out, so the borrow ends within the call.
    let topic_partition_offsets_ =
        unsafe { tp_keyed(topic_partition_offsets, |p| offset_spec_value_of(p as *const _).clone()) };
    let result = client.call(|admin| admin.list_offsets(&topic_partition_offsets_));
    box_list_offsets_result(result, &client.future_ctx())
}

/// List offset for the specified partitions. This operation enables to find the beginning offset,
/// end offset as well as the offset matching a timestamp in partitions.
///
/// # Parameters
///
/// - `topic_partition_offsets` - The mapping from partition to the OffsetSpec to look up.
/// - `options` - The options to use when retrieving the offsets
///
/// # Returns
///
/// The ListOffsetsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `topic_partition_offsets` is a `kafka_Map_t` of `const kafka_common_TopicPartition_t *` keys
///   to `const kafka_admin_OffsetSpec_t *` values, each live; `NULL` reads as an empty map.
///
/// The result handle is owned by the caller, freed with `kafka_admin_ListOffsetsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_list_offsets_with_options(
    self_: *const kafka_admin_Admin_t,
    topic_partition_offsets: *const kafka_Map_t,
    options: *const kafka_admin_ListOffsetsOptions_t,
) -> *mut kafka_admin_ListOffsetsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `topic_partition_offsets` is null or a valid map
    // whose keys are live `const kafka_common_TopicPartition_t *` handles, alive and unmutated
    // during this call, as `tp_keyed` requires, and `tp_keyed` passes the closure its values only,
    // each a `const kafka_admin_OffsetSpec_t *` singleton, or an owned handle not yet destroyed
    // that no other call frees meanwhile, as `offset_spec_value_of` requires; each value is cloned
    // out, so the borrow ends within the call.
    let topic_partition_offsets_ =
        unsafe { tp_keyed(topic_partition_offsets, |p| offset_spec_value_of(p as *const _).clone()) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_ListOffsetsOptions_t` handle that no other call mutates during this one, so no
    // setter runs on it while it is cloned, as `list_offsets_options_ref` requires.
    let options_ = unsafe { list_offsets_options_ref(options) }.clone();
    let result = client.call(|admin| admin.list_offsets_with_options(&topic_partition_offsets_, options_));
    box_list_offsets_result(result, &client.future_ctx())
}

/// List the groups available in the cluster with the default options.
///
/// This is a convenience method for `kafka_admin_Admin_list_groups_with_options` with default
/// options. See the overload for more details.
///
/// # Returns
///
/// The ListGroupsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. The result handle is owned by the caller, freed with
/// `kafka_admin_ListGroupsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_list_groups(
    self_: *const kafka_admin_Admin_t,
) -> *mut kafka_admin_ListGroupsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    let result = client.call(|admin| admin.list_groups());
    box_list_groups_result(result, &client.future_ctx())
}

/// List the groups available in the cluster.
///
/// # Parameters
///
/// - `options` - The options to use when listing the groups.
///
/// # Returns
///
/// The ListGroupsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with `kafka_admin_ListGroupsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_list_groups_with_options(
    self_: *const kafka_admin_Admin_t,
    options: *const kafka_admin_ListGroupsOptions_t,
) -> *mut kafka_admin_ListGroupsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `options` is a live `kafka_admin_ListGroupsOptions_t`
    // handle that no other call mutates during this one, so no setter runs on it while it is
    // cloned, as `list_groups_options_ref` requires.
    let options_ = unsafe { list_groups_options_ref(options) }.clone();
    let result = client.call(|admin| admin.list_groups_with_options(options_));
    box_list_groups_result(result, &client.future_ctx())
}

/// Describe some consumer groups in the cluster, with the default options.
///
/// This is a convenience method for `kafka_admin_Admin_describe_consumer_groups_with_options` with
/// default options. See the overload for more details.
///
/// # Parameters
///
/// - `group_ids` - The IDs of the groups to describe.
///
/// # Returns
///
/// The DescribeConsumerGroupsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `group_ids` is a `kafka_List_t` of NUL-terminated `const char *`; `NULL` reads as an empty
///   list and a `NULL` element as an empty string.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_DescribeConsumerGroupsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_consumer_groups(
    self_: *const kafka_admin_Admin_t,
    group_ids: *const kafka_List_t,
) -> *mut kafka_admin_DescribeConsumerGroupsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `group_ids` is null or a valid list of NUL-terminated
    // `const char *`, unmutated during this call, as `list_strings` requires; the strings are
    // copied.
    let group_ids_ = unsafe { list_strings(group_ids) };
    let result = client.call(|admin| admin.describe_consumer_groups(&group_ids_));
    box_describe_consumer_groups_result(result, &client.future_ctx())
}

/// Describe some consumer groups in the cluster.
///
/// # Parameters
///
/// - `group_ids` - The IDs of the groups to describe.
/// - `options` - The options to use when describing the groups.
///
/// # Returns
///
/// The DescribeConsumerGroupsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `group_ids` is a `kafka_List_t` of NUL-terminated `const char *`; `NULL` reads as an empty
///   list and a `NULL` element as an empty string.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_DescribeConsumerGroupsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_consumer_groups_with_options(
    self_: *const kafka_admin_Admin_t,
    group_ids: *const kafka_List_t,
    options: *const kafka_admin_DescribeConsumerGroupsOptions_t,
) -> *mut kafka_admin_DescribeConsumerGroupsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `group_ids` is null or a valid list of NUL-terminated
    // `const char *`, unmutated during this call, as `list_strings` requires; the strings are
    // copied.
    let group_ids_ = unsafe { list_strings(group_ids) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_DescribeConsumerGroupsOptions_t` handle that no other call mutates during this
    // one, so no setter runs on it while it is cloned, as `describe_consumer_groups_options_ref`
    // requires.
    let options_ = unsafe { describe_consumer_groups_options_ref(options) }.clone();
    let result = client.call(|admin| admin.describe_consumer_groups_with_options(&group_ids_, options_));
    box_describe_consumer_groups_result(result, &client.future_ctx())
}

/// Describe some classic groups in the cluster, with the default options.
///
/// This is a convenience method for `kafka_admin_Admin_describe_classic_groups_with_options` with
/// default options. See the overload for more details.
///
/// # Parameters
///
/// - `group_ids` - The IDs of the groups to describe.
///
/// # Returns
///
/// The DescribeClassicGroupsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `group_ids` is a `kafka_List_t` of NUL-terminated `const char *`; `NULL` reads as an empty
///   list and a `NULL` element as an empty string.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_DescribeClassicGroupsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_classic_groups(
    self_: *const kafka_admin_Admin_t,
    group_ids: *const kafka_List_t,
) -> *mut kafka_admin_DescribeClassicGroupsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `group_ids` is null or a valid list of NUL-terminated
    // `const char *`, unmutated during this call, as `list_strings` requires; the strings are
    // copied.
    let group_ids_ = unsafe { list_strings(group_ids) };
    let result = client.call(|admin| admin.describe_classic_groups(&group_ids_));
    box_describe_classic_groups_result(result, &client.future_ctx())
}

/// Describe some classic groups in the cluster.
///
/// # Parameters
///
/// - `group_ids` - The IDs of the groups to describe.
/// - `options` - The options to use when describing the groups.
///
/// # Returns
///
/// The DescribeClassicGroupsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `group_ids` is a `kafka_List_t` of NUL-terminated `const char *`; `NULL` reads as an empty
///   list and a `NULL` element as an empty string.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_DescribeClassicGroupsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_classic_groups_with_options(
    self_: *const kafka_admin_Admin_t,
    group_ids: *const kafka_List_t,
    options: *const kafka_admin_DescribeClassicGroupsOptions_t,
) -> *mut kafka_admin_DescribeClassicGroupsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `group_ids` is null or a valid list of NUL-terminated
    // `const char *`, unmutated during this call, as `list_strings` requires; the strings are
    // copied.
    let group_ids_ = unsafe { list_strings(group_ids) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_DescribeClassicGroupsOptions_t` handle that no other call mutates during this
    // one, so no setter runs on it while it is cloned, as `describe_classic_groups_options_ref`
    // requires.
    let options_ = unsafe { describe_classic_groups_options_ref(options) }.clone();
    let result = client.call(|admin| admin.describe_classic_groups_with_options(&group_ids_, options_));
    box_describe_classic_groups_result(result, &client.future_ctx())
}

/// List the consumer group offsets available in the cluster with the default options.
///
/// This is a convenience method for
/// `kafka_admin_Admin_list_consumer_group_offsets_with_group_id_options` to list offsets of all
/// partitions of one group with default options.
///
/// # Parameters
///
/// - `group_id` - The group to list the offsets of.
///
/// # Returns
///
/// The ListConsumerGroupOffsetsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with
/// `kafka_admin_ListConsumerGroupOffsetsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_list_consumer_group_offsets_with_group_id(
    self_: *const kafka_admin_Admin_t,
    group_id: *const c_char,
) -> *mut kafka_admin_ListConsumerGroupOffsetsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `group_id` is null or a NUL-terminated string, as
    // `c_str_to_string` requires; it is copied.
    let group_id_ = unsafe { c_str_to_string(group_id) };
    let result = client.call(|admin| admin.list_consumer_group_offsets_with_group_id(&group_id_));
    box_list_consumer_group_offsets_result(result, &client.future_ctx())
}

/// List the consumer group offsets available in the cluster.
///
/// # Parameters
///
/// - `group_id` - The group to list the offsets of.
/// - `options` - The options to use when listing the consumer group offsets.
///
/// # Returns
///
/// The ListConsumerGroupOffsetsResult
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with
/// `kafka_admin_ListConsumerGroupOffsetsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_list_consumer_group_offsets_with_group_id_options(
    self_: *const kafka_admin_Admin_t,
    group_id: *const c_char,
    options: *const kafka_admin_ListConsumerGroupOffsetsOptions_t,
) -> *mut kafka_admin_ListConsumerGroupOffsetsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `group_id` is null or a NUL-terminated string, as
    // `c_str_to_string` requires; it is copied.
    let group_id_ = unsafe { c_str_to_string(group_id) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_ListConsumerGroupOffsetsOptions_t` handle that no other call mutates during this
    // one, so no setter runs on it while it is cloned, as `list_consumer_group_offsets_options_ref`
    // requires.
    let options_ = unsafe { list_consumer_group_offsets_options_ref(options) }.clone();
    let result = client.call(|admin| admin.list_consumer_group_offsets_with_group_id_options(&group_id_, options_));
    box_list_consumer_group_offsets_result(result, &client.future_ctx())
}

/// List the consumer group offsets available in the cluster for the specified groups with the
/// default options.
///
/// This is a convenience method for
/// `kafka_admin_Admin_list_consumer_group_offsets_with_group_specs_options` with default options.
///
/// # Parameters
///
/// - `group_specs` - Map of consumer group ids to a spec that specifies the topic partitions of the
///   group to list offsets for.
///
/// # Returns
///
/// The ListConsumerGroupOffsetsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `group_specs` is a `kafka_Map_t` of NUL-terminated `const char *` keys to `const
///   kafka_admin_ListConsumerGroupOffsetsSpec_t *` values, each live; `NULL` reads as an empty map
///   and a `NULL` key as an empty string.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_ListConsumerGroupOffsetsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_list_consumer_group_offsets_with_group_specs(
    self_: *const kafka_admin_Admin_t,
    group_specs: *const kafka_Map_t,
) -> *mut kafka_admin_ListConsumerGroupOffsetsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `group_specs` is null or a valid map whose keys are
    // null or NUL-terminated strings, alive and unmutated during this call, as `string_keyed`
    // requires, and `string_keyed` passes the closure its values only, each a live
    // `const kafka_admin_ListConsumerGroupOffsetsSpec_t *` handle no other call mutates meanwhile,
    // as `list_consumer_group_offsets_spec_ref` requires; each value is cloned before the call
    // returns.
    let group_specs_ =
        unsafe { string_keyed(group_specs, |p| list_consumer_group_offsets_spec_ref(p as *const _).clone()) };
    let result = client.call(|admin| admin.list_consumer_group_offsets_with_group_specs(&group_specs_));
    box_list_consumer_group_offsets_result(result, &client.future_ctx())
}

/// List the consumer group offsets available in the cluster for the specified consumer groups.
///
/// # Parameters
///
/// - `group_specs` - Map of consumer group ids to a spec that specifies the topic partitions of the
///   group to list offsets for.
/// - `options` - The options to use when listing the consumer group offsets.
///
/// # Returns
///
/// The ListConsumerGroupOffsetsResult
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `group_specs` is a `kafka_Map_t` of NUL-terminated `const char *` keys to `const
///   kafka_admin_ListConsumerGroupOffsetsSpec_t *` values, each live; `NULL` reads as an empty map
///   and a `NULL` key as an empty string.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_ListConsumerGroupOffsetsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_list_consumer_group_offsets_with_group_specs_options(
    self_: *const kafka_admin_Admin_t,
    group_specs: *const kafka_Map_t,
    options: *const kafka_admin_ListConsumerGroupOffsetsOptions_t,
) -> *mut kafka_admin_ListConsumerGroupOffsetsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `group_specs` is null or a valid map whose keys are
    // null or NUL-terminated strings, alive and unmutated during this call, as `string_keyed`
    // requires, and `string_keyed` passes the closure its values only, each a live
    // `const kafka_admin_ListConsumerGroupOffsetsSpec_t *` handle no other call mutates meanwhile,
    // as `list_consumer_group_offsets_spec_ref` requires; each value is cloned before the call
    // returns.
    let group_specs_ =
        unsafe { string_keyed(group_specs, |p| list_consumer_group_offsets_spec_ref(p as *const _).clone()) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_ListConsumerGroupOffsetsOptions_t` handle that no other call mutates during this
    // one, so no setter runs on it while it is cloned, as `list_consumer_group_offsets_options_ref`
    // requires.
    let options_ = unsafe { list_consumer_group_offsets_options_ref(options) }.clone();
    let result =
        client.call(|admin| admin.list_consumer_group_offsets_with_group_specs_options(&group_specs_, options_));
    box_list_consumer_group_offsets_result(result, &client.future_ctx())
}

/// Alters offsets for the specified group. In order to succeed, the group must be empty.
///
/// This is a convenience method for `kafka_admin_Admin_alter_consumer_group_offsets_with_options`
/// with default options. See the overload for more details.
///
/// # Parameters
///
/// - `group_id` - The group for which to alter offsets.
/// - `offsets` - A map of offsets by partition with associated metadata.
///
/// # Returns
///
/// The AlterOffsetsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `offsets` is a `kafka_Map_t` of `const kafka_common_TopicPartition_t *` keys to `const
///   kafka_consumer_OffsetAndMetadata_t *` values, each live; `NULL` reads as an empty map.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_AlterConsumerGroupOffsetsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_alter_consumer_group_offsets(
    self_: *const kafka_admin_Admin_t,
    group_id: *const c_char,
    offsets: *const kafka_Map_t,
) -> *mut kafka_admin_AlterConsumerGroupOffsetsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `group_id` is null or a NUL-terminated string, as
    // `c_str_to_string` requires; it is copied.
    let group_id_ = unsafe { c_str_to_string(group_id) };
    // SAFETY: Per this function's `# Safety`, `offsets` is null or a valid map of
    // `const kafka_common_TopicPartition_t *` keys to `const kafka_consumer_OffsetAndMetadata_t *`
    // values, all live, that no other call mutates during this one, as `map_offset_and_metadata`
    // requires; the entries are cloned.
    let offsets_ = unsafe { map_offset_and_metadata(offsets) };
    let result = client.call(|admin| admin.alter_consumer_group_offsets(&group_id_, &offsets_));
    box_alter_consumer_group_offsets_result(result, &client.future_ctx())
}

/// Alters offsets for the specified group. In order to succeed, the group must be empty.
///
/// This operation is not transactional so it may succeed for some partitions while fail for others.
///
/// # Parameters
///
/// - `group_id` - The group for which to alter offsets.
/// - `offsets` - A map of offsets by partition with associated metadata. Partitions not specified
///   in the map are ignored.
/// - `options` - The options to use when altering the offsets.
///
/// # Returns
///
/// The AlterOffsetsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `offsets` is a `kafka_Map_t` of `const kafka_common_TopicPartition_t *` keys to `const
///   kafka_consumer_OffsetAndMetadata_t *` values, each live; `NULL` reads as an empty map.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_AlterConsumerGroupOffsetsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_alter_consumer_group_offsets_with_options(
    self_: *const kafka_admin_Admin_t,
    group_id: *const c_char,
    offsets: *const kafka_Map_t,
    options: *const kafka_admin_AlterConsumerGroupOffsetsOptions_t,
) -> *mut kafka_admin_AlterConsumerGroupOffsetsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `group_id` is null or a NUL-terminated string, as
    // `c_str_to_string` requires; it is copied.
    let group_id_ = unsafe { c_str_to_string(group_id) };
    // SAFETY: Per this function's `# Safety`, `offsets` is null or a valid map of
    // `const kafka_common_TopicPartition_t *` keys to `const kafka_consumer_OffsetAndMetadata_t *`
    // values, all live, that no other call mutates during this one, as `map_offset_and_metadata`
    // requires; the entries are cloned.
    let offsets_ = unsafe { map_offset_and_metadata(offsets) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_AlterConsumerGroupOffsetsOptions_t` handle that no other call mutates during
    // this one, so no setter runs on it while it is cloned, as
    // `alter_consumer_group_offsets_options_ref` requires.
    let options_ = unsafe { alter_consumer_group_offsets_options_ref(options) }.clone();
    let result = client.call(|admin| admin.alter_consumer_group_offsets_with_options(&group_id_, &offsets_, options_));
    box_alter_consumer_group_offsets_result(result, &client.future_ctx())
}

/// Delete committed offsets for a set of partitions in a consumer group with the default options.
/// This will succeed at the partition level only if the group is not actively subscribed to the
/// corresponding topic.
///
/// # Parameters
///
/// - `group_id` - The group to delete the offsets of.
/// - `partitions` - The partitions whose committed offsets to delete.
///
/// # Returns
///
/// The DeleteConsumerGroupOffsetsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `partitions` is a `kafka_List_t` of `const kafka_common_TopicPartition_t *`, each live; `NULL`
///   reads as an empty list.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_DeleteConsumerGroupOffsetsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_delete_consumer_group_offsets(
    self_: *const kafka_admin_Admin_t,
    group_id: *const c_char,
    partitions: *const kafka_List_t,
) -> *mut kafka_admin_DeleteConsumerGroupOffsetsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `group_id` is null or a NUL-terminated string, as
    // `c_str_to_string` requires; it is copied.
    let group_id_ = unsafe { c_str_to_string(group_id) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list of
    // `const kafka_common_TopicPartition_t *` handles, all live and unmutated during this call, as
    // `list_topic_partitions` requires; the values are cloned.
    let partitions_ = unsafe { list_topic_partitions(partitions) }.into_iter().collect::<HashSet<_>>();
    let result = client.call(|admin| admin.delete_consumer_group_offsets(&group_id_, &partitions_));
    box_delete_consumer_group_offsets_result(result, &client.future_ctx())
}

/// Delete committed offsets for a set of partitions in a consumer group. This will succeed at the
/// partition level only if the group is not actively subscribed to the corresponding topic.
///
/// # Parameters
///
/// - `group_id` - The group to delete the offsets of.
/// - `partitions` - The partitions whose committed offsets to delete.
/// - `options` - The options to use when deleting offsets in a consumer group.
///
/// # Returns
///
/// The DeleteConsumerGroupOffsetsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `partitions` is a `kafka_List_t` of `const kafka_common_TopicPartition_t *`, each live; `NULL`
///   reads as an empty list.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_DeleteConsumerGroupOffsetsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_delete_consumer_group_offsets_with_options(
    self_: *const kafka_admin_Admin_t,
    group_id: *const c_char,
    partitions: *const kafka_List_t,
    options: *const kafka_admin_DeleteConsumerGroupOffsetsOptions_t,
) -> *mut kafka_admin_DeleteConsumerGroupOffsetsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `group_id` is null or a NUL-terminated string, as
    // `c_str_to_string` requires; it is copied.
    let group_id_ = unsafe { c_str_to_string(group_id) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list of
    // `const kafka_common_TopicPartition_t *` handles, all live and unmutated during this call, as
    // `list_topic_partitions` requires; the values are cloned.
    let partitions_ = unsafe { list_topic_partitions(partitions) }.into_iter().collect::<HashSet<_>>();
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_DeleteConsumerGroupOffsetsOptions_t` handle that no other call mutates during
    // this one, so no setter runs on it while it is cloned, as
    // `delete_consumer_group_offsets_options_ref` requires.
    let options_ = unsafe { delete_consumer_group_offsets_options_ref(options) }.clone();
    let result =
        client.call(|admin| admin.delete_consumer_group_offsets_with_options(&group_id_, &partitions_, options_));
    box_delete_consumer_group_offsets_result(result, &client.future_ctx())
}

/// Delete consumer groups from the cluster with the default options.
///
/// # Parameters
///
/// - `group_ids` - The ids of the groups to delete.
///
/// # Returns
///
/// The DeleteConsumerGroupResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `group_ids` is a `kafka_List_t` of NUL-terminated `const char *`; `NULL` reads as an empty
///   list and a `NULL` element as an empty string.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_DeleteConsumerGroupsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_delete_consumer_groups(
    self_: *const kafka_admin_Admin_t,
    group_ids: *const kafka_List_t,
) -> *mut kafka_admin_DeleteConsumerGroupsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `group_ids` is null or a valid list of NUL-terminated
    // `const char *`, unmutated during this call, as `list_strings` requires; the strings are
    // copied.
    let group_ids_ = unsafe { list_strings(group_ids) };
    let result = client.call(|admin| admin.delete_consumer_groups(&group_ids_));
    box_delete_consumer_groups_result(result, &client.future_ctx())
}

/// Delete consumer groups from the cluster.
///
/// # Parameters
///
/// - `group_ids` - The ids of the groups to delete.
/// - `options` - The options to use when deleting a consumer group.
///
/// # Returns
///
/// The DeleteConsumerGroupsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `group_ids` is a `kafka_List_t` of NUL-terminated `const char *`; `NULL` reads as an empty
///   list and a `NULL` element as an empty string.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_DeleteConsumerGroupsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_delete_consumer_groups_with_options(
    self_: *const kafka_admin_Admin_t,
    group_ids: *const kafka_List_t,
    options: *const kafka_admin_DeleteConsumerGroupsOptions_t,
) -> *mut kafka_admin_DeleteConsumerGroupsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `group_ids` is null or a valid list of NUL-terminated
    // `const char *`, unmutated during this call, as `list_strings` requires; the strings are
    // copied.
    let group_ids_ = unsafe { list_strings(group_ids) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_DeleteConsumerGroupsOptions_t` handle that no other call mutates during this
    // one, so no setter runs on it while it is cloned, as `delete_consumer_groups_options_ref`
    // requires.
    let options_ = unsafe { delete_consumer_groups_options_ref(options) }.clone();
    let result = client.call(|admin| admin.delete_consumer_groups_with_options(&group_ids_, options_));
    box_delete_consumer_groups_result(result, &client.future_ctx())
}

/// Remove members from the consumer group by given member identities.
///
/// For possible error codes, refer to `LeaveGroupResponse`.
///
/// # Parameters
///
/// - `group_id` - The ID of the group to remove member from.
/// - `options` - The options to carry removing members' information.
///
/// # Returns
///
/// The MembershipChangeResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with
/// `kafka_admin_RemoveMembersFromConsumerGroupResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_remove_members_from_consumer_group_with_options(
    self_: *const kafka_admin_Admin_t,
    group_id: *const c_char,
    options: *const kafka_admin_RemoveMembersFromConsumerGroupOptions_t,
) -> *mut kafka_admin_RemoveMembersFromConsumerGroupResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `group_id` is null or a NUL-terminated string, as
    // `c_str_to_string` requires; it is copied.
    let group_id_ = unsafe { c_str_to_string(group_id) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_RemoveMembersFromConsumerGroupOptions_t` handle that no other call mutates
    // during this one, so no setter runs on it while it is cloned, as
    // `remove_members_from_consumer_group_options_ref` requires.
    let options_ = unsafe { remove_members_from_consumer_group_options_ref(options) }.clone();
    let result = client.call(|admin| admin.remove_members_from_consumer_group_with_options(&group_id_, options_));
    box_remove_members_from_consumer_group_result(result, &client.future_ctx())
}

/// This is a convenience method for `kafka_admin_Admin_create_acls_with_options` with default
/// options. See the overload for more details.
///
/// This operation is supported by brokers with version 0.11.0.0 or higher.
///
/// # Parameters
///
/// - `acls` - The ACLs to create
///
/// # Returns
///
/// The CreateAclsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `acls` is a `kafka_List_t` of `const kafka_common_acl_AclBinding_t *`, each live; `NULL` reads
///   as an empty list.
///
/// The result handle is owned by the caller, freed with `kafka_admin_CreateAclsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_create_acls(
    self_: *const kafka_admin_Admin_t,
    acls: *const kafka_List_t,
) -> *mut kafka_admin_CreateAclsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `acls` is null or a valid list, alive and unmutated
    // during this call, as `list_refs` requires, and `list_refs` passes the closure its elements
    // only, each a live `const kafka_common_acl_AclBinding_t *` handle no other call mutates
    // meanwhile, as `acl_binding_ref` requires; each value is cloned before the call returns.
    let acls_ = unsafe { list_refs(acls, |p| acl_binding_ref(p as *const _).clone()) };
    let result = client.call(|admin| admin.create_acls(&acls_));
    box_create_acls_result(result, &client.future_ctx())
}

/// Creates access control lists (ACLs) which are bound to specific resources.
///
/// This operation is not transactional so it may succeed for some ACLs while fail for others.
///
/// If you attempt to add an ACL that duplicates an existing ACL, no error will be raised, but no
/// changes will be made.
///
/// This operation is supported by brokers with version 0.11.0.0 or higher.
///
/// # Parameters
///
/// - `acls` - The ACLs to create
/// - `options` - The options to use when creating the ACLs.
///
/// # Returns
///
/// The CreateAclsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `acls` is a `kafka_List_t` of `const kafka_common_acl_AclBinding_t *`, each live; `NULL` reads
///   as an empty list.
///
/// The result handle is owned by the caller, freed with `kafka_admin_CreateAclsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_create_acls_with_options(
    self_: *const kafka_admin_Admin_t,
    acls: *const kafka_List_t,
    options: *const kafka_admin_CreateAclsOptions_t,
) -> *mut kafka_admin_CreateAclsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `acls` is null or a valid list, alive and unmutated
    // during this call, as `list_refs` requires, and `list_refs` passes the closure its elements
    // only, each a live `const kafka_common_acl_AclBinding_t *` handle no other call mutates
    // meanwhile, as `acl_binding_ref` requires; each value is cloned before the call returns.
    let acls_ = unsafe { list_refs(acls, |p| acl_binding_ref(p as *const _).clone()) };
    // SAFETY: Per this function's `# Safety`, `options` is a live `kafka_admin_CreateAclsOptions_t`
    // handle that no other call mutates during this one, so no setter runs on it while it is
    // cloned, as `create_acls_options_ref` requires.
    let options_ = unsafe { create_acls_options_ref(options) }.clone();
    let result = client.call(|admin| admin.create_acls_with_options(&acls_, options_));
    box_create_acls_result(result, &client.future_ctx())
}

/// This is a convenience method for `kafka_admin_Admin_describe_acls_with_options` with default
/// options. See the overload for more details.
///
/// This operation is supported by brokers with version 0.11.0.0 or higher.
///
/// # Parameters
///
/// - `filter` - The filter to use.
///
/// # Returns
///
/// The DescribeAclsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with `kafka_admin_DescribeAclsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_acls(
    self_: *const kafka_admin_Admin_t,
    filter: *const kafka_common_acl_AclBindingFilter_t,
) -> *mut kafka_admin_DescribeAclsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `filter` is a live
    // `kafka_common_acl_AclBindingFilter_t` handle, unmutated during this call, as
    // `acl_binding_filter_ref` requires; the reference is used only within this call.
    let filter_ = unsafe { acl_binding_filter_ref(filter) };
    let result = client.call(|admin| admin.describe_acls(filter_));
    box_describe_acls_result(result, &client.future_ctx())
}

/// Lists access control lists (ACLs) according to the supplied filter.
///
/// Note: it may take some time for changes made by `createAcls` or `deleteAcls` to be reflected in
/// the output of `describeAcls`.
///
/// This operation is supported by brokers with version 0.11.0.0 or higher.
///
/// # Parameters
///
/// - `filter` - The filter to use.
/// - `options` - The options to use when listing the ACLs.
///
/// # Returns
///
/// The DescribeAclsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with `kafka_admin_DescribeAclsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_acls_with_options(
    self_: *const kafka_admin_Admin_t,
    filter: *const kafka_common_acl_AclBindingFilter_t,
    options: *const kafka_admin_DescribeAclsOptions_t,
) -> *mut kafka_admin_DescribeAclsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `filter` is a live
    // `kafka_common_acl_AclBindingFilter_t` handle, unmutated during this call, as
    // `acl_binding_filter_ref` requires; the reference is used only within this call.
    let filter_ = unsafe { acl_binding_filter_ref(filter) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_DescribeAclsOptions_t` handle that no other call mutates during this one, so no
    // setter runs on it while it is cloned, as `describe_acls_options_ref` requires.
    let options_ = unsafe { describe_acls_options_ref(options) }.clone();
    let result = client.call(|admin| admin.describe_acls_with_options(filter_, options_));
    box_describe_acls_result(result, &client.future_ctx())
}

/// This is a convenience method for `kafka_admin_Admin_delete_acls_with_options` with default
/// options. See the overload for more details.
///
/// This operation is supported by brokers with version 0.11.0.0 or higher.
///
/// # Parameters
///
/// - `filters` - The filters to use.
///
/// # Returns
///
/// The DeleteAclsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `filters` is a `kafka_List_t` of `const kafka_common_acl_AclBindingFilter_t *`, each live;
///   `NULL` reads as an empty list.
///
/// The result handle is owned by the caller, freed with `kafka_admin_DeleteAclsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_delete_acls(
    self_: *const kafka_admin_Admin_t,
    filters: *const kafka_List_t,
) -> *mut kafka_admin_DeleteAclsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `filters` is null or a valid list, alive and
    // unmutated during this call, as `list_refs` requires, and `list_refs` passes the closure its
    // elements only, each a live `const kafka_common_acl_AclBindingFilter_t *` handle no other call
    // mutates meanwhile, as `acl_binding_filter_ref` requires; each value is cloned before the call
    // returns.
    let filters_ = unsafe { list_refs(filters, |p| acl_binding_filter_ref(p as *const _).clone()) };
    let result = client.call(|admin| admin.delete_acls(&filters_));
    box_delete_acls_result(result, &client.future_ctx())
}

/// Deletes access control lists (ACLs) according to the supplied filters.
///
/// This operation is not transactional so it may succeed for some ACLs while fail for others.
///
/// This operation is supported by brokers with version 0.11.0.0 or higher.
///
/// # Parameters
///
/// - `filters` - The filters to use.
/// - `options` - The options to use when deleting the ACLs.
///
/// # Returns
///
/// The DeleteAclsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `filters` is a `kafka_List_t` of `const kafka_common_acl_AclBindingFilter_t *`, each live;
///   `NULL` reads as an empty list.
///
/// The result handle is owned by the caller, freed with `kafka_admin_DeleteAclsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_delete_acls_with_options(
    self_: *const kafka_admin_Admin_t,
    filters: *const kafka_List_t,
    options: *const kafka_admin_DeleteAclsOptions_t,
) -> *mut kafka_admin_DeleteAclsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `filters` is null or a valid list, alive and
    // unmutated during this call, as `list_refs` requires, and `list_refs` passes the closure its
    // elements only, each a live `const kafka_common_acl_AclBindingFilter_t *` handle no other call
    // mutates meanwhile, as `acl_binding_filter_ref` requires; each value is cloned before the call
    // returns.
    let filters_ = unsafe { list_refs(filters, |p| acl_binding_filter_ref(p as *const _).clone()) };
    // SAFETY: Per this function's `# Safety`, `options` is a live `kafka_admin_DeleteAclsOptions_t`
    // handle that no other call mutates during this one, so no setter runs on it while it is
    // cloned, as `delete_acls_options_ref` requires.
    let options_ = unsafe { delete_acls_options_ref(options) }.clone();
    let result = client.call(|admin| admin.delete_acls_with_options(&filters_, options_));
    box_delete_acls_result(result, &client.future_ctx())
}

/// Describes all entities matching the provided filter that have at least one client quota
/// configuration value defined.
///
/// This is a convenience method for `kafka_admin_Admin_describe_client_quotas_with_options` with
/// default options. See the overload for more details.
///
/// This operation is supported by brokers with version 2.6.0 or higher.
///
/// # Parameters
///
/// - `filter` - the filter to apply to match entities
///
/// # Returns
///
/// the DescribeClientQuotasResult containing the result
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with
/// `kafka_admin_DescribeClientQuotasResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_client_quotas(
    self_: *const kafka_admin_Admin_t,
    filter: *const kafka_common_quota_ClientQuotaFilter_t,
) -> *mut kafka_admin_DescribeClientQuotasResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `filter` is a live
    // `kafka_common_quota_ClientQuotaFilter_t` handle, unmutated during this call, as
    // `client_quota_filter_ref` requires; the reference is used only within this call.
    let filter_ = unsafe { client_quota_filter_ref(filter) };
    let result = client.call(|admin| admin.describe_client_quotas(filter_));
    box_describe_client_quotas_result(result, &client.future_ctx())
}

/// Describes all entities matching the provided filter that have at least one client quota
/// configuration value defined.
///
/// The following errors can be anticipated when calling `kafka_common_KafkaFuture_get` on the
/// future from the returned `DescribeClientQuotasResult`:
///
/// - `kafka_common_Error_is_cluster_authorization_error` If the authenticated user didn't have
///   describe access to the cluster.
/// - `kafka_common_Error_is_invalid_request_error` If the request details are invalid. e.g., an
///   invalid entity type was specified.
/// - `kafka_common_Error_is_timeout_error` If the request timed out before the describe could
///   finish.
///
/// This operation is supported by brokers with version 2.6.0 or higher.
///
/// # Parameters
///
/// - `filter` - the filter to apply to match entities
/// - `options` - the options to use
///
/// # Returns
///
/// the DescribeClientQuotasResult containing the result
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with
/// `kafka_admin_DescribeClientQuotasResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_client_quotas_with_options(
    self_: *const kafka_admin_Admin_t,
    filter: *const kafka_common_quota_ClientQuotaFilter_t,
    options: *const kafka_admin_DescribeClientQuotasOptions_t,
) -> *mut kafka_admin_DescribeClientQuotasResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `filter` is a live
    // `kafka_common_quota_ClientQuotaFilter_t` handle, unmutated during this call, as
    // `client_quota_filter_ref` requires; the reference is used only within this call.
    let filter_ = unsafe { client_quota_filter_ref(filter) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_DescribeClientQuotasOptions_t` handle that no other call mutates during this
    // one, so no setter runs on it while it is cloned, as `describe_client_quotas_options_ref`
    // requires.
    let options_ = unsafe { describe_client_quotas_options_ref(options) }.clone();
    let result = client.call(|admin| admin.describe_client_quotas_with_options(filter_, options_));
    box_describe_client_quotas_result(result, &client.future_ctx())
}

/// Alters client quota configurations with the specified alterations.
///
/// This is a convenience method for `kafka_admin_Admin_alter_client_quotas_with_options` with
/// default options. See the overload for more details.
///
/// This operation is supported by brokers with version 2.6.0 or higher.
///
/// # Parameters
///
/// - `entries` - the alterations to perform
///
/// # Returns
///
/// the AlterClientQuotasResult containing the result
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `entries` is a `kafka_List_t` of `const kafka_common_quota_ClientQuotaAlteration_t *`, each
///   live; `NULL` reads as an empty list.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_AlterClientQuotasResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_alter_client_quotas(
    self_: *const kafka_admin_Admin_t,
    entries: *const kafka_List_t,
) -> *mut kafka_admin_AlterClientQuotasResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `entries` is null or a valid list, alive and
    // unmutated during this call, as `list_refs` requires, and `list_refs` passes the closure its
    // elements only, each a live `const kafka_common_quota_ClientQuotaAlteration_t *` handle no
    // other call mutates meanwhile, as `client_quota_alteration_ref` requires; each value is cloned
    // before the call returns.
    let entries_ = unsafe { list_refs(entries, |p| client_quota_alteration_ref(p as *const _).clone()) };
    let result = client.call(|admin| admin.alter_client_quotas(&entries_));
    box_alter_client_quotas_result(result, &client.future_ctx())
}

/// Alters client quota configurations with the specified alterations.
///
/// Alterations for a single entity are atomic, but across entities is not guaranteed. The resulting
/// per-entity error code should be evaluated to resolve the success or failure of all updates.
///
/// The following errors can be anticipated when calling `kafka_common_KafkaFuture_get` on the
/// futures obtained from the returned `AlterClientQuotasResult`:
///
/// - `kafka_common_Error_is_cluster_authorization_error` If the authenticated user didn't have
///   alter access to the cluster.
/// - `kafka_common_Error_is_invalid_request_error` If the request details are invalid. e.g., a
///   configuration key was specified more than once for an entity.
/// - `kafka_common_Error_is_timeout_error` If the request timed out before the alterations could
///   finish. It cannot be guaranteed whether the update succeed or not.
///
/// This operation is supported by brokers with version 2.6.0 or higher.
///
/// # Parameters
///
/// - `entries` - the alterations to perform
/// - `options` - The options to use when altering the quotas.
///
/// # Returns
///
/// the AlterClientQuotasResult containing the result
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `entries` is a `kafka_List_t` of `const kafka_common_quota_ClientQuotaAlteration_t *`, each
///   live; `NULL` reads as an empty list.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_AlterClientQuotasResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_alter_client_quotas_with_options(
    self_: *const kafka_admin_Admin_t,
    entries: *const kafka_List_t,
    options: *const kafka_admin_AlterClientQuotasOptions_t,
) -> *mut kafka_admin_AlterClientQuotasResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `entries` is null or a valid list, alive and
    // unmutated during this call, as `list_refs` requires, and `list_refs` passes the closure its
    // elements only, each a live `const kafka_common_quota_ClientQuotaAlteration_t *` handle no
    // other call mutates meanwhile, as `client_quota_alteration_ref` requires; each value is cloned
    // before the call returns.
    let entries_ = unsafe { list_refs(entries, |p| client_quota_alteration_ref(p as *const _).clone()) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_AlterClientQuotasOptions_t` handle that no other call mutates during this one,
    // so no setter runs on it while it is cloned, as `alter_client_quotas_options_ref` requires.
    let options_ = unsafe { alter_client_quotas_options_ref(options) }.clone();
    let result = client.call(|admin| admin.alter_client_quotas_with_options(&entries_, options_));
    box_alter_client_quotas_result(result, &client.future_ctx())
}

/// Describe all SASL/SCRAM credentials.
///
/// This is a convenience method for
/// `kafka_admin_Admin_describe_user_scram_credentials_with_users_options`
///
/// # Returns
///
/// The DescribeUserScramCredentialsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. The result handle is owned by the caller, freed with
/// `kafka_admin_DescribeUserScramCredentialsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_user_scram_credentials(
    self_: *const kafka_admin_Admin_t,
) -> *mut kafka_admin_DescribeUserScramCredentialsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    let result = client.call(|admin| admin.describe_user_scram_credentials());
    box_describe_user_scram_credentials_result(result, &client.future_ctx())
}

/// Describe SASL/SCRAM credentials for the given users.
///
/// This is a convenience method for
/// `kafka_admin_Admin_describe_user_scram_credentials_with_users_options`
///
/// # Parameters
///
/// - `users` - the users for which credentials are to be described; all users' credentials are
///   described if empty.
///
/// # Returns
///
/// The DescribeUserScramCredentialsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `users` is a `kafka_List_t` of NUL-terminated `const char *`; `NULL` reads as an empty list
///   and a `NULL` element as an empty string.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_DescribeUserScramCredentialsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_user_scram_credentials_with_users(
    self_: *const kafka_admin_Admin_t,
    users: *const kafka_List_t,
) -> *mut kafka_admin_DescribeUserScramCredentialsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `users` is null or a valid list of NUL-terminated
    // `const char *`, unmutated during this call, as `list_strings` requires; the strings are
    // copied.
    let users_ = unsafe { list_strings(users) };
    let result = client.call(|admin| admin.describe_user_scram_credentials_with_users(&users_));
    box_describe_user_scram_credentials_result(result, &client.future_ctx())
}

/// Describe SASL/SCRAM credentials.
///
/// The following errors can be anticipated when calling `kafka_common_KafkaFuture_get` on the
/// futures from the returned `DescribeUserScramCredentialsResult`:
///
/// - `kafka_common_Error_is_cluster_authorization_error` If the authenticated user didn't have
///   describe access to the cluster.
/// - `kafka_common_Error_is_resource_not_found_error` If the user did not exist/had no SCRAM
///   credentials.
/// - `kafka_common_Error_is_duplicate_resource_error` If the user was requested to be described
///   more than once in the original request.
/// - `kafka_common_Error_is_timeout_error` If the request timed out before the describe operation
///   could finish.
///
/// This operation is supported by brokers with version 2.7.0 or higher.
///
/// # Parameters
///
/// - `users` - the users for which credentials are to be described; all users' credentials are
///   described if empty.
/// - `options` - The options to use when describing the credentials
///
/// # Returns
///
/// The DescribeUserScramCredentialsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `users` is a `kafka_List_t` of NUL-terminated `const char *`; `NULL` reads as an empty list
///   and a `NULL` element as an empty string.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_DescribeUserScramCredentialsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_user_scram_credentials_with_users_options(
    self_: *const kafka_admin_Admin_t,
    users: *const kafka_List_t,
    options: *const kafka_admin_DescribeUserScramCredentialsOptions_t,
) -> *mut kafka_admin_DescribeUserScramCredentialsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `users` is null or a valid list of NUL-terminated
    // `const char *`, unmutated during this call, as `list_strings` requires; the strings are
    // copied.
    let users_ = unsafe { list_strings(users) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_DescribeUserScramCredentialsOptions_t` handle that no other call mutates during
    // this one, so no setter runs on it while it is cloned, as
    // `describe_user_scram_credentials_options_ref` requires.
    let options_ = unsafe { describe_user_scram_credentials_options_ref(options) }.clone();
    let result = client.call(|admin| admin.describe_user_scram_credentials_with_users_options(&users_, options_));
    box_describe_user_scram_credentials_result(result, &client.future_ctx())
}

/// Alter SASL/SCRAM credentials for the given users.
///
/// This is a convenience method for `kafka_admin_Admin_alter_user_scram_credentials_with_options`
///
/// # Parameters
///
/// - `alterations` - the alterations to be applied
///
/// # Returns
///
/// The AlterUserScramCredentialsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `alterations` is a `kafka_List_t` of `const kafka_admin_UserScramCredentialAlteration_t *`,
///   each live; `NULL` reads as an empty list.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_AlterUserScramCredentialsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_alter_user_scram_credentials(
    self_: *const kafka_admin_Admin_t,
    alterations: *const kafka_List_t,
) -> *mut kafka_admin_AlterUserScramCredentialsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `alterations` is null or a valid list, alive and
    // unmutated during this call, as `list_refs` requires, and `list_refs` passes the closure its
    // elements only, each a live `const kafka_admin_UserScramCredentialAlteration_t *` handle no
    // other call mutates meanwhile, as `user_scram_credential_alteration_ref` requires; each value
    // is cloned before the call returns.
    let alterations_ =
        unsafe { list_refs(alterations, |p| user_scram_credential_alteration_ref(p as *const _).clone()) };
    let result = client.call(|admin| admin.alter_user_scram_credentials(&alterations_));
    box_alter_user_scram_credentials_result(result, &client.future_ctx())
}

/// Alter SASL/SCRAM credentials.
///
/// The following errors can be anticipated when calling `kafka_common_KafkaFuture_get` any of the
/// futures from the returned `AlterUserScramCredentialsResult`:
///
/// - `kafka_common_Error_is_not_controller_error` If the request is not sent to the Controller
///   broker.
/// - `kafka_common_Error_is_cluster_authorization_error` If the authenticated user didn't have
///   alter access to the cluster.
/// - `kafka_common_Error_is_unsupported_by_authentication_error` If the user authenticated with a
///   delegation token.
/// - `kafka_common_Error_is_unsupported_sasl_mechanism_error` If the requested SCRAM mechanism is
///   unrecognized or otherwise unsupported.
/// - `kafka_common_Error_is_unacceptable_credential_error` If the username is empty or the
///   requested number of iterations is too small or too large.
/// - `kafka_common_Error_is_timeout_error` If the request timed out before the describe could
///   finish.
///
/// This operation is supported by brokers with version 2.7.0 or higher.
///
/// # Parameters
///
/// - `alterations` - the alterations to be applied
/// - `options` - The options to use when altering the credentials
///
/// # Returns
///
/// The AlterUserScramCredentialsResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `alterations` is a `kafka_List_t` of `const kafka_admin_UserScramCredentialAlteration_t *`,
///   each live; `NULL` reads as an empty list.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_AlterUserScramCredentialsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_alter_user_scram_credentials_with_options(
    self_: *const kafka_admin_Admin_t,
    alterations: *const kafka_List_t,
    options: *const kafka_admin_AlterUserScramCredentialsOptions_t,
) -> *mut kafka_admin_AlterUserScramCredentialsResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `alterations` is null or a valid list, alive and
    // unmutated during this call, as `list_refs` requires, and `list_refs` passes the closure its
    // elements only, each a live `const kafka_admin_UserScramCredentialAlteration_t *` handle no
    // other call mutates meanwhile, as `user_scram_credential_alteration_ref` requires; each value
    // is cloned before the call returns.
    let alterations_ =
        unsafe { list_refs(alterations, |p| user_scram_credential_alteration_ref(p as *const _).clone()) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_AlterUserScramCredentialsOptions_t` handle that no other call mutates during
    // this one, so no setter runs on it while it is cloned, as
    // `alter_user_scram_credentials_options_ref` requires.
    let options_ = unsafe { alter_user_scram_credentials_options_ref(options) }.clone();
    let result = client.call(|admin| admin.alter_user_scram_credentials_with_options(&alterations_, options_));
    box_alter_user_scram_credentials_result(result, &client.future_ctx())
}

/// Create a Delegation Token.
///
/// This is a convenience method for `kafka_admin_Admin_create_delegation_token_with_options` with
/// default options. See the overload for more details.
///
/// # Returns
///
/// The CreateDelegationTokenResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. The result handle is owned by the caller, freed with
/// `kafka_admin_CreateDelegationTokenResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_create_delegation_token(
    self_: *const kafka_admin_Admin_t,
) -> *mut kafka_admin_CreateDelegationTokenResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    let result = client.call(|admin| admin.create_delegation_token());
    box_create_delegation_token_result(result, &client.future_ctx())
}

/// Create a Delegation Token.
///
/// This operation is supported by brokers with version 1.1.0 or higher.
///
/// The following errors can be anticipated when calling `kafka_common_KafkaFuture_get` on the
/// futures obtained from the delegationToken() method of the returned `CreateDelegationTokenResult`
///
/// - `kafka_common_Error_is_unsupported_by_authentication_error` If the request sent on
///   PLAINTEXT/1-way SSL channels or delegation token authenticated channels.
/// - `kafka_common_Error_is_invalid_principal_type_error` if the renewers principal type is not
///   supported.
/// - `kafka_common_Error_is_delegation_token_disabled_error` if the delegation token feature is
///   disabled.
/// - `kafka_common_Error_is_timeout_error` if the request was not completed in within the given
///   `kafka_admin_CreateDelegationTokenOptions_timeout_ms`.
///
/// # Parameters
///
/// - `options` - The options to use when creating delegation token.
///
/// # Returns
///
/// The CreateDelegationTokenResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with
/// `kafka_admin_CreateDelegationTokenResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_create_delegation_token_with_options(
    self_: *const kafka_admin_Admin_t,
    options: *const kafka_admin_CreateDelegationTokenOptions_t,
) -> *mut kafka_admin_CreateDelegationTokenResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_CreateDelegationTokenOptions_t` handle that no other call mutates during this
    // one, so no setter runs on it while it is cloned, as `create_delegation_token_options_ref`
    // requires.
    let options_ = unsafe { create_delegation_token_options_ref(options) }.clone();
    let result = client.call(|admin| admin.create_delegation_token_with_options(options_));
    box_create_delegation_token_result(result, &client.future_ctx())
}

/// Renew a Delegation Token.
///
/// This is a convenience method for `kafka_admin_Admin_renew_delegation_token_with_options` with
/// default options. See the overload for more details.
///
/// # Parameters
///
/// - `hmac` - HMAC of the Delegation token
///
/// # Returns
///
/// The RenewDelegationTokenResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `hmac` is a `kafka_Bytes_t` whose `data` points at `len` readable bytes; a `NULL` `data` reads
///   as empty.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_RenewDelegationTokenResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_renew_delegation_token(
    self_: *const kafka_admin_Admin_t,
    hmac: kafka_Bytes_t,
) -> *mut kafka_admin_RenewDelegationTokenResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `hmac.data` is null or points at `hmac.len` readable
    // bytes, unmutated during this call, as `bytes_of` requires; the slice borrows `hmac`, a local
    // of this call, so it is not used after the call returns.
    let hmac_ = unsafe { bytes_of(&hmac) };
    let result = client.call(|admin| admin.renew_delegation_token(hmac_));
    box_renew_delegation_token_result(result, &client.future_ctx())
}

/// Renew a Delegation Token.
///
/// This operation is supported by brokers with version 1.1.0 or higher.
///
/// The following errors can be anticipated when calling `kafka_common_KafkaFuture_get` on the
/// futures obtained from the expiryTimestamp() method of the returned `RenewDelegationTokenResult`
///
/// - `kafka_common_Error_is_unsupported_by_authentication_error` If the request sent on
///   PLAINTEXT/1-way SSL channels or delegation token authenticated channels.
/// - `kafka_common_Error_is_delegation_token_disabled_error` if the delegation token feature is
///   disabled.
/// - `kafka_common_Error_is_delegation_token_not_found_error` if the delegation token is not found
///   on server.
/// - `kafka_common_Error_is_delegation_token_owner_mismatch_error` if the authenticated user is not
///   owner/renewer of the token.
/// - `kafka_common_Error_is_delegation_token_expired_error` if the delegation token is expired.
/// - `kafka_common_Error_is_timeout_error` if the request was not completed in within the given
///   `kafka_admin_RenewDelegationTokenOptions_timeout_ms`.
///
/// # Parameters
///
/// - `hmac` - HMAC of the Delegation token
/// - `options` - The options to use when renewing delegation token.
///
/// # Returns
///
/// The RenewDelegationTokenResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `hmac` is a `kafka_Bytes_t` whose `data` points at `len` readable bytes; a `NULL` `data` reads
///   as empty.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_RenewDelegationTokenResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_renew_delegation_token_with_options(
    self_: *const kafka_admin_Admin_t,
    hmac: kafka_Bytes_t,
    options: *const kafka_admin_RenewDelegationTokenOptions_t,
) -> *mut kafka_admin_RenewDelegationTokenResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `hmac.data` is null or points at `hmac.len` readable
    // bytes, unmutated during this call, as `bytes_of` requires; the slice borrows `hmac`, a local
    // of this call, so it is not used after the call returns.
    let hmac_ = unsafe { bytes_of(&hmac) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_RenewDelegationTokenOptions_t` handle that no other call mutates during this
    // one, so no setter runs on it while it is cloned, as `renew_delegation_token_options_ref`
    // requires.
    let options_ = unsafe { renew_delegation_token_options_ref(options) }.clone();
    let result = client.call(|admin| admin.renew_delegation_token_with_options(hmac_, options_));
    box_renew_delegation_token_result(result, &client.future_ctx())
}

/// Expire a Delegation Token.
///
/// This is a convenience method for `kafka_admin_Admin_expire_delegation_token_with_options` with
/// default options. This will expire the token immediately. See the overload for more details.
///
/// # Parameters
///
/// - `hmac` - HMAC of the Delegation token
///
/// # Returns
///
/// The ExpireDelegationTokenResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `hmac` is a `kafka_Bytes_t` whose `data` points at `len` readable bytes; a `NULL` `data` reads
///   as empty.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_ExpireDelegationTokenResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_expire_delegation_token(
    self_: *const kafka_admin_Admin_t,
    hmac: kafka_Bytes_t,
) -> *mut kafka_admin_ExpireDelegationTokenResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `hmac.data` is null or points at `hmac.len` readable
    // bytes, unmutated during this call, as `bytes_of` requires; the slice borrows `hmac`, a local
    // of this call, so it is not used after the call returns.
    let hmac_ = unsafe { bytes_of(&hmac) };
    let result = client.call(|admin| admin.expire_delegation_token(hmac_));
    box_expire_delegation_token_result(result, &client.future_ctx())
}

/// Expire a Delegation Token.
///
/// This operation is supported by brokers with version 1.1.0 or higher.
///
/// The following errors can be anticipated when calling `kafka_common_KafkaFuture_get` on the
/// futures obtained from the expiryTimestamp() method of the returned `ExpireDelegationTokenResult`
///
/// - `kafka_common_Error_is_unsupported_by_authentication_error` If the request sent on
///   PLAINTEXT/1-way SSL channels or delegation token authenticated channels.
/// - `kafka_common_Error_is_delegation_token_disabled_error` if the delegation token feature is
///   disabled.
/// - `kafka_common_Error_is_delegation_token_not_found_error` if the delegation token is not found
///   on server.
/// - `kafka_common_Error_is_delegation_token_owner_mismatch_error` if the authenticated user is not
///   owner/renewer of the requested token.
/// - `kafka_common_Error_is_delegation_token_expired_error` if the delegation token is expired.
/// - `kafka_common_Error_is_timeout_error` if the request was not completed in within the given
///   `kafka_admin_ExpireDelegationTokenOptions_timeout_ms`.
///
/// # Parameters
///
/// - `hmac` - HMAC of the Delegation token
/// - `options` - The options to use when expiring delegation token.
///
/// # Returns
///
/// The ExpireDelegationTokenResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. The inputs are
/// borrowed and copied during the call:
///
/// - `hmac` is a `kafka_Bytes_t` whose `data` points at `len` readable bytes; a `NULL` `data` reads
///   as empty.
///
/// The result handle is owned by the caller, freed with
/// `kafka_admin_ExpireDelegationTokenResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_expire_delegation_token_with_options(
    self_: *const kafka_admin_Admin_t,
    hmac: kafka_Bytes_t,
    options: *const kafka_admin_ExpireDelegationTokenOptions_t,
) -> *mut kafka_admin_ExpireDelegationTokenResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `hmac.data` is null or points at `hmac.len` readable
    // bytes, unmutated during this call, as `bytes_of` requires; the slice borrows `hmac`, a local
    // of this call, so it is not used after the call returns.
    let hmac_ = unsafe { bytes_of(&hmac) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_ExpireDelegationTokenOptions_t` handle that no other call mutates during this
    // one, so no setter runs on it while it is cloned, as `expire_delegation_token_options_ref`
    // requires.
    let options_ = unsafe { expire_delegation_token_options_ref(options) }.clone();
    let result = client.call(|admin| admin.expire_delegation_token_with_options(hmac_, options_));
    box_expire_delegation_token_result(result, &client.future_ctx())
}

/// Describe the Delegation Tokens.
///
/// This is a convenience method for `kafka_admin_Admin_describe_delegation_token_with_options` with
/// default options. This will return all the user owned tokens and tokens where user have Describe
/// permission. See the overload for more details.
///
/// # Returns
///
/// The DescribeDelegationTokenResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. The result handle is owned by the caller, freed with
/// `kafka_admin_DescribeDelegationTokenResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_delegation_token(
    self_: *const kafka_admin_Admin_t,
) -> *mut kafka_admin_DescribeDelegationTokenResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    let result = client.call(|admin| admin.describe_delegation_token());
    box_describe_delegation_token_result(result, &client.future_ctx())
}

/// Describe the Delegation Tokens.
///
/// This operation is supported by brokers with version 1.1.0 or higher.
///
/// The following errors can be anticipated when calling `kafka_common_KafkaFuture_get` on the
/// futures obtained from the delegationTokens() method of the returned
/// `DescribeDelegationTokenResult`
///
/// - `kafka_common_Error_is_unsupported_by_authentication_error` If the request sent on
///   PLAINTEXT/1-way SSL channels or delegation token authenticated channels.
/// - `kafka_common_Error_is_delegation_token_disabled_error` if the delegation token feature is
///   disabled.
/// - `kafka_common_Error_is_timeout_error` if the request was not completed in within the given
///   `kafka_admin_DescribeDelegationTokenOptions_timeout_ms`.
///
/// # Parameters
///
/// - `options` - The options to use when describing delegation tokens.
///
/// # Returns
///
/// The DescribeDelegationTokenResult.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with
/// `kafka_admin_DescribeDelegationTokenResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_delegation_token_with_options(
    self_: *const kafka_admin_Admin_t,
    options: *const kafka_admin_DescribeDelegationTokenOptions_t,
) -> *mut kafka_admin_DescribeDelegationTokenResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_DescribeDelegationTokenOptions_t` handle that no other call mutates during this
    // one, so no setter runs on it while it is cloned, as `describe_delegation_token_options_ref`
    // requires.
    let options_ = unsafe { describe_delegation_token_options_ref(options) }.clone();
    let result = client.call(|admin| admin.describe_delegation_token_with_options(options_));
    box_describe_delegation_token_result(result, &client.future_ctx())
}

/// Describes finalized as well as supported features.
///
/// This is a convenience method for `kafka_admin_Admin_describe_features_with_options` with default
/// options. See the overload for more details.
///
/// # Returns
///
/// the `DescribeFeaturesResult` containing the result
///
/// # Safety
///
/// `self_` must be a live handle or view. The result handle is owned by the caller, freed with
/// `kafka_admin_DescribeFeaturesResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_features(
    self_: *const kafka_admin_Admin_t,
) -> *mut kafka_admin_DescribeFeaturesResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    let result = client.call(|admin| admin.describe_features());
    box_describe_features_result(result, &client.future_ctx())
}

/// Describes finalized as well as supported features. The request is issued to any random broker.
///
/// The following errors can be anticipated when calling `kafka_common_KafkaFuture_get` on the
/// future from the returned `DescribeFeaturesResult`:
///
/// - `kafka_common_Error_is_timeout_error` If the request timed out before the describe operation
///   could finish.
///
/// # Parameters
///
/// - `options` - the options to use
///
/// # Returns
///
/// the `DescribeFeaturesResult` containing the result
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a string NUL-terminated
/// and an enum one of its singletons. The inputs are borrowed and copied during the call. The
/// result handle is owned by the caller, freed with `kafka_admin_DescribeFeaturesResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_describe_features_with_options(
    self_: *const kafka_admin_Admin_t,
    options: *const kafka_admin_DescribeFeaturesOptions_t,
) -> *mut kafka_admin_DescribeFeaturesResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_DescribeFeaturesOptions_t` handle that no other call mutates during this one, so
    // no setter runs on it while it is cloned, as `describe_features_options_ref` requires.
    let options_ = unsafe { describe_features_options_ref(options) }.clone();
    let result = client.call(|admin| admin.describe_features_with_options(options_));
    box_describe_features_result(result, &client.future_ctx())
}

/// Applies specified updates to finalized features.
///
/// This is a convenience method for `kafka_admin_Admin_update_features_with_options` with default
/// options. See the overload for more details.
///
/// # Parameters
///
/// - `feature_updates` - the map of finalized feature name to `FeatureUpdate`
///
/// # Returns
///
/// the `UpdateFeaturesResult` containing the result
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_argument_error` if `feature_updates` is empty or any
///   feature name is blank.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. `out_err` must be a
/// valid slot pointing at `NULL`. The inputs are borrowed and copied during the call:
///
/// - `feature_updates` is a `kafka_Map_t` of NUL-terminated `const char *` keys to `const
///   kafka_admin_FeatureUpdate_t *` values, each live; `NULL` reads as an empty map and a `NULL`
///   key as an empty string.
///
/// The result handle is owned by the caller, freed with `kafka_admin_UpdateFeaturesResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_update_features(
    self_: *const kafka_admin_Admin_t,
    feature_updates: *const kafka_Map_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_admin_UpdateFeaturesResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `feature_updates` is null or a valid map whose keys
    // are null or NUL-terminated strings, alive and unmutated during this call, as `string_keyed`
    // requires, and `string_keyed` passes the closure its values only, each a live
    // `const kafka_admin_FeatureUpdate_t *` handle no other call mutates meanwhile, as
    // `feature_update_ref` requires; each value is cloned before the call returns.
    let feature_updates_ = unsafe { string_keyed(feature_updates, |p| feature_update_ref(p as *const _).clone()) };
    let result = client.call(|admin| admin.update_features(&feature_updates_));
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |r| box_update_features_result(r, &client.future_ctx())) }
}

/// Applies specified updates to finalized features. This operation is not transactional so some
/// updates may succeed while the rest may fail.
///
/// The API takes in a map of finalized feature names to `FeatureUpdate` that needs to be applied.
/// Each entry in the map specifies the finalized feature to be added or updated or deleted, along
/// with the new max feature version level value. This request is issued only to the controller
/// since the API is only served by the controller. The return value contains an error code for each
/// supplied `FeatureUpdate`, and the code indicates if the update succeeded or failed in the
/// controller.
///
/// - Downgrading a feature version level is not a common operation and should only be performed
///   when necessary. It is permitted only if the `FeatureUpdate` specifies the `upgradeType` as
///   either `kafka_admin_FeatureUpdate_UpgradeType_SAFE_DOWNGRADE` or
///   `kafka_admin_FeatureUpdate_UpgradeType_UNSAFE_DOWNGRADE`.
/// - `SAFE_DOWNGRADE`: Allows downgrades that do not lead to metadata loss.
/// - `UNSAFE_DOWNGRADE`: Permits downgrades that might result in metadata loss.
///
/// Note that even with these settings, certain downgrades may still be rejected by the controller
/// if they are considered unsafe or impossible.
///
/// - Deleting a finalized feature version is also not a common operation. To delete a feature, set
///   the `maxVersionLevel` to zero and specify the `upgradeType` as either
///   `kafka_admin_FeatureUpdate_UpgradeType_SAFE_DOWNGRADE` or
///   `kafka_admin_FeatureUpdate_UpgradeType_UNSAFE_DOWNGRADE`.
/// - The `kafka_admin_FeatureUpdate_UpgradeType_UPGRADE` type cannot be used when the
///   `maxVersionLevel` is zero. Attempting to do so will result in an
///   `kafka_common_Error_is_local_illegal_argument_error`.
///
/// The following errors can be anticipated when calling `kafka_common_KafkaFuture_get` on the
/// futures obtained from the returned `UpdateFeaturesResult`:
///
/// - `kafka_common_Error_is_cluster_authorization_error` If the authenticated user didn't have
///   alter access to the cluster.
/// - `kafka_common_Error_is_invalid_request_error` If the request details are invalid. e.g., a
///   non-existing finalized feature is attempted to be deleted or downgraded.
/// - `kafka_common_Error_is_timeout_error` If the request timed out before the updates could
///   finish. It cannot be guaranteed whether the updates succeeded or not.
/// - `kafka_common_Error_is_feature_update_failed_error` This means there was an unexpected error
///   encountered when the update was applied on the controller. There is no guarantee on whether
///   the update succeeded or failed. The best way to find out is to issue a
///   `kafka_admin_Admin_describe_features` request.
///
/// This operation is supported by brokers with version 2.7.0 or higher.
///
/// # Parameters
///
/// - `feature_updates` - the map of finalized feature name to `FeatureUpdate`
/// - `options` - the options to use
///
/// # Returns
///
/// the `UpdateFeaturesResult` containing the result
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_argument_error` if `feature_updates` is empty or any
///   feature name is blank.
///
/// # Translation notes
///
/// Java throws that `IllegalArgumentException` from the call; here it is stored in `*out_err`.
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be live and not mutated by any
/// other call for the duration of this one, unless its entry below allows `NULL`, and of its
/// parameter's type: a string NUL-terminated and an enum one of its singletons. `out_err` must be a
/// valid slot pointing at `NULL`. The inputs are borrowed and copied during the call:
///
/// - `feature_updates` is a `kafka_Map_t` of NUL-terminated `const char *` keys to `const
///   kafka_admin_FeatureUpdate_t *` values, each live; `NULL` reads as an empty map and a `NULL`
///   key as an empty string.
///
/// The result handle is owned by the caller, freed with `kafka_admin_UpdateFeaturesResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_update_features_with_options(
    self_: *const kafka_admin_Admin_t,
    feature_updates: *const kafka_Map_t,
    options: *const kafka_admin_UpdateFeaturesOptions_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_admin_UpdateFeaturesResult_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle or view, alive for this
    // call, which is as long as `client` is used, as `client_ref` requires.
    let client = unsafe { client_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `feature_updates` is null or a valid map whose keys
    // are null or NUL-terminated strings, alive and unmutated during this call, as `string_keyed`
    // requires, and `string_keyed` passes the closure its values only, each a live
    // `const kafka_admin_FeatureUpdate_t *` handle no other call mutates meanwhile, as
    // `feature_update_ref` requires; each value is cloned before the call returns.
    let feature_updates_ = unsafe { string_keyed(feature_updates, |p| feature_update_ref(p as *const _).clone()) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_UpdateFeaturesOptions_t` handle that no other call mutates during this one, so
    // no setter runs on it while it is cloned, as `update_features_options_ref` requires.
    let options_ = unsafe { update_features_options_ref(options) }.clone();
    let result = client.call(|admin| admin.update_features_with_options(&feature_updates_, options_));
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid slot,
    // as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |r| box_update_features_result(r, &client.future_ctx())) }
}
