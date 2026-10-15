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

/// The elements of a borrowed list, each converted by `read`; null reads as
/// empty. `read` is called on the list's elements only, once each.
///
/// # Safety
///
/// `list` must be null or a valid list handle, alive and not mutated during
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

/// The entries of a borrowed map, keys and values converted; null reads as
/// empty. `read_key` is called on the map's keys only and `read_value` on
/// its values only, once each.
///
/// # Safety
///
/// `map` must be null or a valid map handle, alive and not mutated during
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
/// `map` must be null or a valid map handle whose keys are null or
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
/// `map` must be null or a valid map handle whose keys are valid
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
/// `list` must be null or a valid list whose elements are topic-partition
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

/// The bytes behind a `kafka_Bytes_t`; a null `data` reads as empty.
///
/// # Safety
///
/// `bytes.data` must be null or point at `bytes.len` readable bytes, not
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

/// `Admin.create_topics`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_CreateTopicsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.create_topics_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_CreateTopicsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.delete_topics`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DeleteTopicsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_delete_topics(
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
    let result = client.call(|admin| admin.delete_topics(topics_));
    box_delete_topics_result(result, &client.future_ctx())
}

/// `Admin.delete_topics_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DeleteTopicsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Admin_delete_topics_with_options(
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
    let result = client.call(|admin| admin.delete_topics_with_options(topics_, options_));
    box_delete_topics_result(result, &client.future_ctx())
}

/// `Admin.list_topics`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_ListTopicsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.list_topics_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_ListTopicsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_topics_with_topic_names`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeTopicsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_topics_with_topic_names_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeTopicsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_topics_with_topics`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeTopicsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_topics_with_topics_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeTopicsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.create_partitions`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_CreatePartitionsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.create_partitions_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_CreatePartitionsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.delete_records`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DeleteRecordsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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
    // `records_to_delete_ref` requires; each value is copied out before the call returns.
    let records_to_delete_ = unsafe { tp_keyed(records_to_delete, |p| *records_to_delete_ref(p as *const _)) };
    let result = client.call(|admin| admin.delete_records(&records_to_delete_));
    box_delete_records_result(result, &client.future_ctx())
}

/// `Admin.delete_records_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DeleteRecordsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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
    // `records_to_delete_ref` requires; each value is copied out before the call returns.
    let records_to_delete_ = unsafe { tp_keyed(records_to_delete, |p| *records_to_delete_ref(p as *const _)) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_DeleteRecordsOptions_t` handle that no other call mutates during this one, so no
    // setter runs on it while it is cloned, as `delete_records_options_ref` requires.
    let options_ = unsafe { delete_records_options_ref(options) }.clone();
    let result = client.call(|admin| admin.delete_records_with_options(&records_to_delete_, options_));
    box_delete_records_result(result, &client.future_ctx())
}

/// `Admin.describe_producers`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeProducersResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_producers_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeProducersResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.abort_transaction`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_AbortTransactionResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.abort_transaction_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_AbortTransactionResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.unregister_controller`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_UnregisterControllerResult_destroy`.
///
/// # Safety
///
/// `self_` must be a live handle or view.
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

/// `Admin.unregister_controller_with_options`: a non-blocking call returning the owned result
/// handle, freed with `kafka_admin_UnregisterControllerResult_destroy`; the options are borrowed
/// and copied during the call.
///
/// # Safety
///
/// `self_` must be a live handle or view, and `options` a live handle not mutated by any other
/// call for the duration of this one.
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

/// `Admin.describe_transactions`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeTransactionsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_transactions_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeTransactionsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.fence_producers`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_FenceProducersResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.fence_producers_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_FenceProducersResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.list_transactions`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_ListTransactionsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.list_transactions_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_ListTransactionsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.force_terminate_transaction`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_TerminateTransactionResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.force_terminate_transaction_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_TerminateTransactionResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_cluster`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeClusterResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_cluster_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeClusterResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_configs`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeConfigsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_configs_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeConfigsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.incremental_alter_configs`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_AlterConfigsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.incremental_alter_configs_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_AlterConfigsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.list_config_resources`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_ListConfigResourcesResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.list_config_resources_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_ListConfigResourcesResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_log_dirs`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeLogDirsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_log_dirs_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeLogDirsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.alter_replica_log_dirs`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_AlterReplicaLogDirsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.alter_replica_log_dirs_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_AlterReplicaLogDirsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_replica_log_dirs`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeReplicaLogDirsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_replica_log_dirs_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeReplicaLogDirsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.elect_leaders`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_ElectLeadersResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.elect_leaders_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_ElectLeadersResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.alter_partition_reassignments`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_AlterPartitionReassignmentsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.alter_partition_reassignments_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_AlterPartitionReassignmentsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.list_partition_reassignments`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_ListPartitionReassignmentsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.list_partition_reassignments_with_partitions`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_ListPartitionReassignmentsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.list_partition_reassignments_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_ListPartitionReassignmentsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.list_partition_reassignments_with_partitions_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_ListPartitionReassignmentsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.list_offsets`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_ListOffsetsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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
    // that no other call frees meanwhile, as `offset_spec_value_of` requires; each value is copied
    // out.
    let topic_partition_offsets_ =
        unsafe { tp_keyed(topic_partition_offsets, |p| offset_spec_value_of(p as *const _)) };
    let result = client.call(|admin| admin.list_offsets(&topic_partition_offsets_));
    box_list_offsets_result(result, &client.future_ctx())
}

/// `Admin.list_offsets_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_ListOffsetsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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
    // that no other call frees meanwhile, as `offset_spec_value_of` requires; each value is copied
    // out.
    let topic_partition_offsets_ =
        unsafe { tp_keyed(topic_partition_offsets, |p| offset_spec_value_of(p as *const _)) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_ListOffsetsOptions_t` handle that no other call mutates during this one, so no
    // setter runs on it while it is cloned, as `list_offsets_options_ref` requires.
    let options_ = unsafe { list_offsets_options_ref(options) }.clone();
    let result = client.call(|admin| admin.list_offsets_with_options(&topic_partition_offsets_, options_));
    box_list_offsets_result(result, &client.future_ctx())
}

/// `Admin.list_groups`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_ListGroupsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.list_groups_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_ListGroupsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_consumer_groups`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeConsumerGroupsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_consumer_groups_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeConsumerGroupsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_classic_groups`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeClassicGroupsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_classic_groups_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeClassicGroupsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.list_consumer_group_offsets_with_group_id`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_ListConsumerGroupOffsetsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.list_consumer_group_offsets_with_group_id_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_ListConsumerGroupOffsetsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.list_consumer_group_offsets_with_group_specs`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_ListConsumerGroupOffsetsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.list_consumer_group_offsets_with_group_specs_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_ListConsumerGroupOffsetsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.alter_consumer_group_offsets`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_AlterConsumerGroupOffsetsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.alter_consumer_group_offsets_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_AlterConsumerGroupOffsetsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.delete_consumer_group_offsets`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DeleteConsumerGroupOffsetsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.delete_consumer_group_offsets_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DeleteConsumerGroupOffsetsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.delete_consumer_groups`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DeleteConsumerGroupsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.delete_consumer_groups_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DeleteConsumerGroupsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.remove_members_from_consumer_group_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_RemoveMembersFromConsumerGroupResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.create_acls`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_CreateAclsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.create_acls_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_CreateAclsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_acls`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeAclsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_acls_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeAclsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.delete_acls`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DeleteAclsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.delete_acls_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DeleteAclsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_client_quotas`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeClientQuotasResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_client_quotas_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeClientQuotasResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.alter_client_quotas`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_AlterClientQuotasResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.alter_client_quotas_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_AlterClientQuotasResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_user_scram_credentials`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeUserScramCredentialsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_user_scram_credentials_with_users`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeUserScramCredentialsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_user_scram_credentials_with_users_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeUserScramCredentialsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.alter_user_scram_credentials`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_AlterUserScramCredentialsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.alter_user_scram_credentials_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_AlterUserScramCredentialsResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.create_delegation_token`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_CreateDelegationTokenResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.create_delegation_token_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_CreateDelegationTokenResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.renew_delegation_token`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_RenewDelegationTokenResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
/// `hmac.data` must be null or point at `hmac.len` readable bytes, not
/// mutated during the call.
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

/// `Admin.renew_delegation_token_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_RenewDelegationTokenResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
/// `hmac.data` must be null or point at `hmac.len` readable bytes, not
/// mutated during the call.
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

/// `Admin.expire_delegation_token`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_ExpireDelegationTokenResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
/// `hmac.data` must be null or point at `hmac.len` readable bytes, not
/// mutated during the call.
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

/// `Admin.expire_delegation_token_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_ExpireDelegationTokenResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
/// `hmac.data` must be null or point at `hmac.len` readable bytes, not
/// mutated during the call.
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

/// `Admin.describe_delegation_token`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeDelegationTokenResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_delegation_token_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeDelegationTokenResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_features`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeFeaturesResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.describe_features_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_DescribeFeaturesResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any
/// other call for the duration of this one, and of its parameter's type: a
/// string NUL-terminated, an enum one of its singletons, and a list or map
/// holding elements of the types the module docs give, each element
/// meeting these same conditions.
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

/// `Admin.update_features`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_UpdateFeaturesResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any other
/// call for the duration of this one, and of its parameter's type: a string
/// NUL-terminated, an enum one of its singletons, and a list or map holding
/// elements of the types the module docs give, each element meeting these same
/// conditions. `out_err` must be a valid slot pointing at null.
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
    // `feature_update_ref` requires; each value is copied out before the call returns.
    let feature_updates_ = unsafe { string_keyed(feature_updates, |p| *feature_update_ref(p as *const _)) };
    let result = client.call(|admin| admin.update_features(&feature_updates_));
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |r| box_update_features_result(r, &client.future_ctx())) }
}

/// `Admin.update_features_with_options`: a non-blocking call returning the owned result handle,
/// freed with `kafka_admin_UpdateFeaturesResult_destroy`; the inputs are borrowed and
/// copied during the call (see the module docs for the container element
/// types).
///
/// # Safety
///
/// `self_` must be a live handle or view. Every other pointer must be null
/// where the module docs allow it, or else live and not mutated by any other
/// call for the duration of this one, and of its parameter's type: a string
/// NUL-terminated, an enum one of its singletons, and a list or map holding
/// elements of the types the module docs give, each element meeting these same
/// conditions. `out_err` must be a valid slot pointing at null.
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
    // `feature_update_ref` requires; each value is copied out before the call returns.
    let feature_updates_ = unsafe { string_keyed(feature_updates, |p| *feature_update_ref(p as *const _)) };
    // SAFETY: Per this function's `# Safety`, `options` is a live
    // `kafka_admin_UpdateFeaturesOptions_t` handle that no other call mutates during this one, so
    // no setter runs on it while it is cloned, as `update_features_options_ref` requires.
    let options_ = unsafe { update_features_options_ref(options) }.clone();
    let result = client.call(|admin| admin.update_features_with_options(&feature_updates_, options_));
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid slot,
    // as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |r| box_update_features_result(r, &client.future_ctx())) }
}
