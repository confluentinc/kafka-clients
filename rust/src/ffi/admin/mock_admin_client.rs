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
use crate::ffi::admin::{AdminClassHandle, kafka_admin_Admin_t};
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::common::topic_partition::map_topic_partition_i64;
use crate::ffi::common::topic_partition_info::topic_partition_info_ref;
use crate::ffi::common::{deliver_unit, deliver_value};
use crate::ffi::ffi_guard;
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
/// `self_` must be a live handle from `kafka_admin_MockAdminClient_create`,
/// kept alive for `'a`.
unsafe fn mock_ref<'a>(self_: *const kafka_admin_MockAdminClient_t) -> &'a MockAdminClient {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle from
    // `kafka_admin_MockAdminClient_create`, kept alive for `'a`: the `Box<AdminClassHandle>` that
    // function delivers, so the shared reference is to a live value. Nothing takes a `&mut` to the
    // handle while it is live: only its destroy takes it back, after the handle's last use.
    unsafe { &*(self_ as *const AdminClassHandle) }
        .mock()
        .expect("a MockAdminClient handle is built by new_mock")
}

/// Reads a `kafka_Map_t` of `const char *` to `const int16_t *`.
///
/// # Safety
///
/// `map` must be null or a live map of C strings to `const int16_t *`
/// values, each pointing at an `int16_t`.
unsafe fn map_string_i16(map: *const kafka_Map_t) -> HashMap<String, i16> {
    if map.is_null() {
        return HashMap::new();
    }
    // SAFETY: `map` is non-null (checked above) and, per this function's `# Safety`, a live map,
    // unmutated during the call since `kafka_Map_put`'s `# Safety` excludes another call using the
    // map at the same time, as `map_entries` requires; the slice is dropped before the call returns.
    unsafe { map_entries(map) }
        .iter()
        // SAFETY: Per this function's `# Safety`, each key is a C string, as `c_str_to_string`
        // requires, and each value a live `const int16_t *` pointing at an `int16_t` no other call
        // writes meanwhile, so reading it is sound.
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
/// `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_create(
    num_brokers: i32,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_admin_MockAdminClient_t {
    let result = AdminClassHandle::new_mock(|| MockAdminClient::create(num_brokers));
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid slot, as `deliver_value` requires.
    unsafe {
        deliver_value(result, out_err, |handle| {
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient__as_Admin(
    self_: *const kafka_admin_MockAdminClient_t,
) -> *const kafka_admin_Admin_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, i.e. the
    // `Box<AdminClassHandle>` `kafka_admin_MockAdminClient_create` delivered, so the shared
    // reference, used only to compute the view's address, is to a live value no `&mut` aliases.
    unsafe { &*(self_ as *const AdminClassHandle) }.as_admin()
}

/// `MockAdminClient.Builder.featureLevels` / `minSupportedFeatureLevels` /
/// `maxSupportedFeatureLevels`: seeds the three feature-level maps
/// (`kafka_Map_t` of `const char *` to `const int16_t *`, borrowed and copied
/// during the call).
///
/// # Safety
///
/// `self_` must be a live handle; the maps null or live maps of the types
/// above.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_set_feature_levels(
    self_: *const kafka_admin_MockAdminClient_t,
    feature_levels: *const kafka_Map_t,
    min_supported_feature_levels: *const kafka_Map_t,
    max_supported_feature_levels: *const kafka_Map_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle from
    // `kafka_admin_MockAdminClient_create`, kept alive for this call, as `mock_ref` requires, and
    // each map null or a live map of C strings to `const int16_t *` values, as `map_string_i16`
    // requires.
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
/// `self_` must be a live handle and `new_offsets` null or a live map of
/// the types above, each key and value live.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_update_beginning_offsets(
    self_: *const kafka_admin_MockAdminClient_t,
    new_offsets: *const kafka_Map_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle from
    // `kafka_admin_MockAdminClient_create`, kept alive for this call, as `mock_ref` requires, and
    // `new_offsets` null or a live map of `const kafka_common_TopicPartition_t *` handles to
    // `const int64_t *` values, as `map_topic_partition_i64` requires; the entries are copied.
    unsafe { mock_ref(self_).update_beginning_offsets(map_topic_partition_i64(new_offsets)) }
}

/// `MockAdminClient.updateEndOffsets(Map<TopicPartition, Long>)`; the map
/// as in [`kafka_admin_MockAdminClient_update_beginning_offsets`].
///
/// # Safety
///
/// `self_` must be a live handle and `new_offsets` null or a live map of
/// the types above, each key and value live.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_update_end_offsets(
    self_: *const kafka_admin_MockAdminClient_t,
    new_offsets: *const kafka_Map_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle from
    // `kafka_admin_MockAdminClient_create`, kept alive for this call, as `mock_ref` requires, and
    // `new_offsets` null or a live map of `const kafka_common_TopicPartition_t *` handles to
    // `const int64_t *` values, as `map_topic_partition_i64` requires; the entries are copied.
    unsafe { mock_ref(self_).update_end_offsets(map_topic_partition_i64(new_offsets)) }
}

/// `MockAdminClient.updateConsumerGroupOffsets(Map<TopicPartition, Long>)`;
/// the map as in [`kafka_admin_MockAdminClient_update_beginning_offsets`].
///
/// # Safety
///
/// `self_` must be a live handle and `new_offsets` null or a live map of
/// the types above, each key and value live.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_update_consumer_group_offsets(
    self_: *const kafka_admin_MockAdminClient_t,
    new_offsets: *const kafka_Map_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle from
    // `kafka_admin_MockAdminClient_create`, kept alive for this call, as `mock_ref` requires, and
    // `new_offsets` null or a live map of `const kafka_common_TopicPartition_t *` handles to
    // `const int64_t *` values, as `map_topic_partition_i64` requires; the entries are copied.
    unsafe { mock_ref(self_).update_consumer_group_offsets(map_topic_partition_i64(new_offsets)) }
}

/// `MockAdminClient.Builder.brokerLogDirs`: the log directories of
/// `broker_id` (`kafka_List_t` of `const char *`, copied during the call);
/// fails with Java's message for an unknown broker.
///
/// # Safety
///
/// `self_` must be a live handle and `log_dirs` null or a live list of C
/// strings. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_set_broker_log_dirs(
    self_: *const kafka_admin_MockAdminClient_t,
    broker_id: i32,
    log_dirs: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle from
    // `kafka_admin_MockAdminClient_create`, kept alive for this call, as `mock_ref` requires, and
    // `log_dirs` null or a live list of C strings, as `list_strings` requires; they are copied.
    unsafe { deliver_unit(mock_ref(self_).set_broker_log_dirs(broker_id, list_strings(log_dirs)), out_err) }
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
/// live list and `configs` null or a live map, of the element types above, each
/// element live. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_add_topic(
    self_: *const kafka_admin_MockAdminClient_t,
    internal: i8,
    name: *const c_char,
    partitions: *const kafka_List_t,
    configs: *const kafka_Map_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `name` is a C string, as `c_str_to_string` requires;
    // it is copied.
    let name = unsafe { c_str_to_string(name) };
    let partitions = if partitions.is_null() {
        Vec::new()
    } else {
        // SAFETY: `partitions` is non-null (checked above) and, per this function's `# Safety`, a
        // live list, unmutated during the call since `kafka_List_add`'s `# Safety` excludes another
        // call using the list at the same time, as `list_elements` requires; the slice is dropped
        // before the call returns.
        unsafe { list_elements(partitions) }
            .iter()
            // SAFETY: Per this function's `# Safety`, each element is a live
            // `const kafka_common_TopicPartitionInfo_t *` handle, alive while it is cloned, as
            // `topic_partition_info_ref` requires.
            .map(|&p| unsafe { topic_partition_info_ref(p as *const _) }.clone())
            .collect()
    };
    let configs: Option<BTreeMap<String, String>> = if configs.is_null() {
        None
    } else {
        // SAFETY: Per this function's `# Safety`, `configs` (non-null, checked above) is a live map
        // of `const char *` keys and values, i.e. NUL-terminated strings, as `map_strings`
        // requires; they are copied.
        Some(unsafe { map_strings(configs) }.into_iter().collect())
    };
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle from
    // `kafka_admin_MockAdminClient_create`, kept alive for this call, as `mock_ref` requires.
    let result = unsafe { mock_ref(self_) }.add_topic(internal != 0, &name, partitions, configs);
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe { deliver_unit(result, out_err) }
}

/// `MockAdminClient.markTopicForDeletion(String name)`; fails with Java's
/// message for an unknown topic.
///
/// # Safety
///
/// `self_` must be a live handle and `name` a C string. `out_err` must be a
/// valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_mark_topic_for_deletion(
    self_: *const kafka_admin_MockAdminClient_t,
    name: *const c_char,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `name` is a C string, as `c_str_to_string` requires;
    // it is copied.
    let name = unsafe { c_str_to_string(name) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle from
    // `kafka_admin_MockAdminClient_create`, kept alive for this call, as `mock_ref` requires.
    let result = unsafe { mock_ref(self_) }.mark_topic_for_deletion(&name);
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe { deliver_unit(result, out_err) }
}

/// `MockAdminClient.timeoutNextRequest(int numberOfRequests)`: the next
/// `number_of_requests` RPCs fail their futures with a `TimeoutException`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_timeout_next_request(
    self_: *const kafka_admin_MockAdminClient_t,
    number_of_requests: i32,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle from
    // `kafka_admin_MockAdminClient_create`, kept alive for this call, as `mock_ref` requires.
    unsafe { mock_ref(self_) }.timeout_next_request(number_of_requests)
}

/// `MockAdminClient.Builder.usingRaftController(boolean)`, as a setter on the
/// constructed mock: whether the mocked cluster runs a KRaft controller
/// quorum, which decides whether `unregister_controller` succeeds. `0` is
/// `false` (the default), any other value `true`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_set_using_raft_controller(
    self_: *const kafka_admin_MockAdminClient_t,
    using_raft_controller: i8,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle from
    // `kafka_admin_MockAdminClient_create`, kept alive for this call, as `mock_ref` requires.
    unsafe { mock_ref(self_) }.set_using_raft_controller(using_raft_controller != 0)
}

/// Frees the mock (see the module docs of `crate::ffi::admin` for what
/// `_destroy` does); null is a no-op. Views from
/// [`kafka_admin_MockAdminClient__as_Admin`] become invalid.
///
/// # Safety
///
/// `self_` must be null or a handle not yet destroyed, that no other call
/// uses at the same time, directly or through a view; neither it nor its
/// views are used afterwards, by the callbacks this call runs included.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_destroy(self_: *mut kafka_admin_MockAdminClient_t) {
    if !self_.is_null() {
        // SAFETY: Per this function's `# Safety`, `self_` (non-null, checked above) is a handle not
        // yet destroyed, i.e. the `Box<AdminClassHandle>` `kafka_admin_MockAdminClient_create`
        // delivered, that no other call uses at the same time, directly or through a view, so
        // taking it back frees it exactly once, through `destroy`, which awaits the tasks still
        // holding the client. Neither it nor its views are used afterwards, by the callbacks
        // `destroy` runs included, as the doc requires.
        unsafe { Box::from_raw(self_ as *mut AdminClassHandle) }.destroy();
    }
}
