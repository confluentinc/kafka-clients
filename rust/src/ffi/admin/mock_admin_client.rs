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

//! `kafka_admin_MockAdminClient_t`:
//! `org.apache.kafka.clients.admin.MockAdminClient` (CLAUDE.md §4), the
//! in-memory test double. Public in Rust through the `public-in-rust` tag
//! (`admin-client.md` §9), so it has C bindings.
//!
//! The class handle is built by `kafka_admin_MockAdminClient_create`, freed
//! with `kafka_admin_MockAdminClient_destroy`, and driven as an `Admin`
//! through the borrowed `kafka_admin_MockAdminClient__as_Admin` view; the
//! mock-specific seeding methods are inherent methods on the class handle,
//! as in Rust.

use std::collections::{BTreeMap, HashMap};
use std::ffi::c_char;

use crate::admin::MockAdminClient;
use crate::ffi::admin::{AdminClassHandle, error_slot, kafka_admin_Admin_t, out_slot};
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::common::topic_partition::map_topic_partition_i64;
use crate::ffi::common::topic_partition_info::topic_partition_info_ref;
use crate::ffi::util::{
    c_str_to_string, kafka_List_t, kafka_Map_t, list_elements, list_strings, map_entries, map_strings,
};

/// Opaque handle to a [`MockAdminClient`].
#[repr(C)]
#[doc(alias = "public-in-rust")]
pub struct kafka_admin_MockAdminClient_t {
    _private: [u8; 0],
}

/// The mock behind a class handle.
///
/// # Safety
///
/// `self_` must be a live handle from `kafka_admin_MockAdminClient_create`.
unsafe fn mock_ref<'a>(self_: *const kafka_admin_MockAdminClient_t) -> &'a MockAdminClient {
    unsafe { &*(self_ as *const AdminClassHandle) }
        .mock()
        .expect("a MockAdminClient handle is built by new_mock")
}

/// Reads a `kafka_Map_t` of `const char *` to `const int16_t *`.
///
/// # Safety
///
/// `map` must be null or a live map of C strings to `int16_t` values.
unsafe fn map_string_i16(map: *const kafka_Map_t) -> HashMap<String, i16> {
    if map.is_null() {
        return HashMap::new();
    }
    unsafe { map_entries(map) }
        .iter()
        .map(|&(k, v)| (unsafe { c_str_to_string(k as *const c_char) }, unsafe { *(v as *const i16) }))
        .collect()
}

/// `MockAdminClient.create(int numBrokers)`: a mock with `numBrokers`
/// brokers `localhost:1000 + id`, owned by the caller
/// ([`kafka_admin_MockAdminClient_destroy`]). Fails with Java's
/// `IllegalArgumentException` message for a non-positive count.
///
/// # Safety
///
/// `out_create` must be a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_create(
    num_brokers: i32,
    out_create: *mut *mut kafka_admin_MockAdminClient_t,
) -> *mut kafka_common_Error_t {
    let result = AdminClassHandle::new_mock(|| MockAdminClient::create(num_brokers));
    unsafe {
        out_slot(result, out_create, |handle| {
            Box::into_raw(handle) as *mut kafka_admin_MockAdminClient_t
        })
    }
}

/// The `Admin` view of the mock (CLAUDE.md §4 rule 3): borrowed, valid until
/// the class handle is destroyed, never passed to `kafka_admin_Admin_destroy`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient__as_Admin(
    self_: *const kafka_admin_MockAdminClient_t,
) -> *const kafka_admin_Admin_t {
    unsafe { &*(self_ as *const AdminClassHandle) }.as_admin()
}

/// `MockAdminClient.Builder.featureLevels` / `minSupportedFeatureLevels` /
/// `maxSupportedFeatureLevels`: seeds the three feature-level maps
/// (`kafka_Map_t` of `const char *` to `const int16_t *`, borrowed and copied
/// during the call).
///
/// # Safety
///
/// `self_` must be a live handle; the maps null or live.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_set_feature_levels(
    self_: *const kafka_admin_MockAdminClient_t,
    feature_levels: *const kafka_Map_t,
    min_supported_feature_levels: *const kafka_Map_t,
    max_supported_feature_levels: *const kafka_Map_t,
) {
    unsafe {
        mock_ref(self_).set_feature_levels(
            map_string_i16(feature_levels),
            map_string_i16(min_supported_feature_levels),
            map_string_i16(max_supported_feature_levels),
        )
    }
}

/// `MockAdminClient.updateBeginningOffsets(Map<TopicPartition, Long>)`:
/// `new_offsets` is a `kafka_Map_t` of `const kafka_common_TopicPartition_t *`
/// to `const int64_t *`, borrowed and copied during the call.
///
/// # Safety
///
/// `self_` must be a live handle and `new_offsets` null or a live map.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_update_beginning_offsets(
    self_: *const kafka_admin_MockAdminClient_t,
    new_offsets: *const kafka_Map_t,
) {
    unsafe { mock_ref(self_).update_beginning_offsets(map_topic_partition_i64(new_offsets)) }
}

/// `MockAdminClient.updateEndOffsets(Map<TopicPartition, Long>)`; the map
/// as in [`kafka_admin_MockAdminClient_update_beginning_offsets`].
///
/// # Safety
///
/// `self_` must be a live handle and `new_offsets` null or a live map.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_update_end_offsets(
    self_: *const kafka_admin_MockAdminClient_t,
    new_offsets: *const kafka_Map_t,
) {
    unsafe { mock_ref(self_).update_end_offsets(map_topic_partition_i64(new_offsets)) }
}

/// `MockAdminClient.updateConsumerGroupOffsets(Map<TopicPartition, Long>)`;
/// the map as in [`kafka_admin_MockAdminClient_update_beginning_offsets`].
///
/// # Safety
///
/// `self_` must be a live handle and `new_offsets` null or a live map.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_update_consumer_group_offsets(
    self_: *const kafka_admin_MockAdminClient_t,
    new_offsets: *const kafka_Map_t,
) {
    unsafe { mock_ref(self_).update_consumer_group_offsets(map_topic_partition_i64(new_offsets)) }
}

/// `MockAdminClient.Builder.brokerLogDirs`: the log directories of
/// `broker_id` (`kafka_List_t` of `const char *`, copied during the call);
/// fails with Java's message for an unknown broker.
///
/// # Safety
///
/// `self_` must be a live handle and `log_dirs` null or a live list.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_set_broker_log_dirs(
    self_: *const kafka_admin_MockAdminClient_t,
    broker_id: i32,
    log_dirs: *const kafka_List_t,
) -> *mut kafka_common_Error_t {
    error_slot(unsafe { mock_ref(self_).set_broker_log_dirs(broker_id, list_strings(log_dirs)) })
}

/// `MockAdminClient.addTopic(boolean internal, String name,
/// List<TopicPartitionInfo> partitions, Map<String, String> configs)`:
/// `partitions` is a `kafka_List_t` of `const kafka_common_TopicPartitionInfo_t *`
/// and `configs` a `kafka_Map_t` of `const char *` to `const char *`, `NULL`
/// for Java's null; both borrowed and copied during the call. Fails with
/// Java's message when the topic exists or a partition names an unknown
/// broker.
///
/// # Safety
///
/// `self_` must be a live handle, `name` a C string, `partitions` null or a
/// live list and `configs` null or a live map.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_add_topic(
    self_: *const kafka_admin_MockAdminClient_t,
    internal: i8,
    name: *const c_char,
    partitions: *const kafka_List_t,
    configs: *const kafka_Map_t,
) -> *mut kafka_common_Error_t {
    let name = unsafe { c_str_to_string(name) };
    let partitions = if partitions.is_null() {
        Vec::new()
    } else {
        unsafe { list_elements(partitions) }
            .iter()
            .map(|&p| unsafe { topic_partition_info_ref(p as *const _) }.clone())
            .collect()
    };
    let configs: Option<BTreeMap<String, String>> = if configs.is_null() {
        None
    } else {
        Some(unsafe { map_strings(configs) }.into_iter().collect())
    };
    error_slot(unsafe { mock_ref(self_) }.add_topic(internal != 0, &name, partitions, configs))
}

/// `MockAdminClient.markTopicForDeletion(String name)`; fails with Java's
/// message for an unknown topic.
///
/// # Safety
///
/// `self_` must be a live handle and `name` a C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_mark_topic_for_deletion(
    self_: *const kafka_admin_MockAdminClient_t,
    name: *const c_char,
) -> *mut kafka_common_Error_t {
    let name = unsafe { c_str_to_string(name) };
    error_slot(unsafe { mock_ref(self_) }.mark_topic_for_deletion(&name))
}

/// `MockAdminClient.timeoutNextRequest(int numberOfRequests)`: the next
/// `number_of_requests` RPCs fail their futures with a `TimeoutException`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_timeout_next_request(
    self_: *const kafka_admin_MockAdminClient_t,
    number_of_requests: i32,
) {
    unsafe { mock_ref(self_) }.timeout_next_request(number_of_requests)
}

/// Frees the mock (see the module docs of `crate::ffi::admin` for what
/// `_destroy` does); null is a no-op. Views from
/// [`kafka_admin_MockAdminClient__as_Admin`] become invalid.
///
/// # Safety
///
/// `self_` must be null or a handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_destroy(self_: *mut kafka_admin_MockAdminClient_t) {
    if !self_.is_null() {
        unsafe { Box::from_raw(self_ as *mut AdminClassHandle) }.destroy();
    }
}
