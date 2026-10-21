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
//! The class handle is built by one of Java's constructors,
//! `kafka_admin_MockAdminClient_new` and
//! `kafka_admin_MockAdminClient_with_brokers_controller`, or by
//! `kafka_admin_MockAdminClient_Builder_build`, from the
//! `kafka_admin_MockAdminClient_Builder_t` that `kafka_admin_MockAdminClient_create`
//! returns, as Java's static `create()` returns a `Builder`. It is freed with
//! `kafka_admin_MockAdminClient_destroy`, and driven as an `Admin` through the
//! borrowed `kafka_admin_MockAdminClient__as_Admin` view; the mock-specific
//! seeding methods are inherent methods on the class handle, as in Rust.
//!
//! The builder handle (Java's nested `MockAdminClient.Builder`) is owned by
//! the caller and freed with `kafka_admin_MockAdminClient_Builder_destroy`. Its
//! setters change the handle in place, as Java's return `this`: an infallible
//! one returns nothing, a fallible one returns the same handle (a borrowed
//! pointer, not a second owner) on success and `NULL` with the error in
//! `out_err` on failure, leaving the builder as it was before the call (Java
//! throws before it assigns anything for a negative broker count or an
//! out-of-range controller index). `_build` leaves the builder usable, as
//! Java's `build()` does, so one builder can build several mocks.

use std::collections::{BTreeMap, HashMap};
use std::ffi::c_char;

use crate::admin::MockAdminClient;
use crate::admin::mock_admin_client::Builder;
use crate::common::Error;
use crate::ffi::admin::{AdminClassHandle, kafka_admin_Admin_t};
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::common::node::{kafka_common_Node_t, list_nodes, node_ref};
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
/// `self_` must be a live handle from a mock constructor,
/// kept alive for `'a`.
unsafe fn mock_ref<'a>(self_: *const kafka_admin_MockAdminClient_t) -> &'a MockAdminClient {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle from a mock constructor,
    // kept alive for `'a`: the `Box<AdminClassHandle>` `box_mock` delivers, so the shared reference
    // is to a live value. Nothing takes a `&mut` to the
    // handle while it is live: only its destroy takes it back, after the handle's last use.
    unsafe { &*(self_ as *const AdminClassHandle) }
        .mock()
        .expect("a MockAdminClient handle is built by from_mock")
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

/// `MockAdminClient.create()`: a fresh [`kafka_admin_MockAdminClient_Builder_t`]
/// with one broker, `Node(0, "localhost", 1000)`, as Java's static `create()`
/// returns `new Builder()`. Owned by the caller, freed with
/// [`kafka_admin_MockAdminClient_Builder_destroy`]; the same as
/// [`kafka_admin_MockAdminClient_Builder_new`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_MockAdminClient_create() -> *mut kafka_admin_MockAdminClient_Builder_t {
    box_builder(MockAdminClient::create())
}

/// `new MockAdminClient()`: a mock with one broker, `Node.noNode()`, which is
/// also the controller. Owned by the caller, freed with
/// [`kafka_admin_MockAdminClient_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_MockAdminClient_new() -> *mut kafka_admin_MockAdminClient_t {
    box_mock(MockAdminClient::new())
}

/// `new MockAdminClient(List<Node> brokers, Node controller)`: `brokers` is a
/// `kafka_List_t` of `const kafka_common_Node_t *` and `controller` a node
/// handle, both borrowed and copied during the call. The mock is owned by the
/// caller and freed with [`kafka_admin_MockAdminClient_destroy`]. Fails, with
/// `NULL` returned, with Java's message "The controller node must be in the
/// list of brokers" when `controller` is not one of `brokers`.
///
/// # Safety
///
/// `brokers` must be null or a valid list of valid node handles, `controller`
/// a valid node handle, and `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_with_brokers_controller(
    brokers: *const kafka_List_t,
    controller: *const kafka_common_Node_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_admin_MockAdminClient_t {
    // SAFETY: Per this function's `# Safety`, `brokers` is null or a valid list of node handles,
    // as `list_nodes` requires; the nodes are copied.
    let brokers = unsafe { list_nodes(brokers) };
    // SAFETY: Per this function's `# Safety`, `controller` is a valid node handle, alive for this
    // call, as `node_ref` requires; the node is copied, so the borrow ends here.
    let controller = unsafe { node_ref(controller) }.clone();
    let result = MockAdminClient::with_brokers_controller(brokers, controller);
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid slot, as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, box_mock) }
}

/// Hands `mock` to C as an owned class handle.
fn box_mock(mock: MockAdminClient) -> *mut kafka_admin_MockAdminClient_t {
    Box::into_raw(AdminClassHandle::from_mock(mock)) as *mut kafka_admin_MockAdminClient_t
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
    // `Box<AdminClassHandle>` `box_mock` delivered, so the shared
    // reference, used only to compute the view's address, is to a live value no `&mut` aliases.
    unsafe { &*(self_ as *const AdminClassHandle) }.as_admin()
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
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle from a mock
    // constructor, kept alive for this call, as `mock_ref` requires, and
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
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle from a mock
    // constructor, kept alive for this call, as `mock_ref` requires, and
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
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle from a mock
    // constructor, kept alive for this call, as `mock_ref` requires, and
    // `new_offsets` null or a live map of `const kafka_common_TopicPartition_t *` handles to
    // `const int64_t *` values, as `map_topic_partition_i64` requires; the entries are copied.
    unsafe { mock_ref(self_).update_consumer_group_offsets(map_topic_partition_i64(new_offsets)) }
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
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle from a mock
    // constructor, kept alive for this call, as `mock_ref` requires.
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
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle from a mock
    // constructor, kept alive for this call, as `mock_ref` requires.
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
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle from a mock
    // constructor, kept alive for this call, as `mock_ref` requires.
    unsafe { mock_ref(self_) }.timeout_next_request(number_of_requests)
}

/// `MockAdminClient.controller(Node controller)`: makes `controller` (a node
/// handle, borrowed and copied during the call) the controller
/// `describe_cluster` reports. Fails with Java's message "The controller node
/// must be in the list of brokers" when it is not one of the mock's brokers,
/// leaving the controller unchanged.
///
/// # Safety
///
/// `self_` must be a live handle, `controller` a valid node handle, and
/// `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_set_controller(
    self_: *const kafka_admin_MockAdminClient_t,
    controller: *const kafka_common_Node_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `controller` is a valid node handle, alive for this
    // call, as `node_ref` requires; the node is copied, so the borrow ends here.
    let controller = unsafe { node_ref(controller) }.clone();
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle from a mock constructor,
    // kept alive for this call, as `mock_ref` requires.
    let result = unsafe { mock_ref(self_) }.set_controller(controller);
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe { deliver_unit(result, out_err) }
}

/// `MockAdminClient.setFetchesRemainingUntilVisible(String topicName, int
/// fetchesRemainingUntilVisible)`: hides the topic from the next
/// `fetches_remaining_until_visible` fetches (`list_topics`, `describe_topics`
/// and topic `describe_configs`). Fails with Java's message
/// `"No such topic as <name>"` for an unknown topic.
///
/// # Safety
///
/// `self_` must be a live handle, `topic_name` a C string, and `out_err` a
/// valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_set_fetches_remaining_until_visible(
    self_: *const kafka_admin_MockAdminClient_t,
    topic_name: *const c_char,
    fetches_remaining_until_visible: i32,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `topic_name` is a C string, as `c_str_to_string`
    // requires; it is copied.
    let topic_name = unsafe { c_str_to_string(topic_name) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle from a mock constructor,
    // kept alive for this call, as `mock_ref` requires.
    let result =
        unsafe { mock_ref(self_) }.set_fetches_remaining_until_visible(&topic_name, fetches_remaining_until_visible);
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe { deliver_unit(result, out_err) }
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
        // yet destroyed, i.e. the `Box<AdminClassHandle>` `box_mock` delivered, that no other call uses at the same time, directly or through a view, so
        // taking it back frees it exactly once, through `destroy`, which awaits the tasks still
        // holding the client. Neither it nor its views are used afterwards, by the callbacks
        // `destroy` runs included, as the doc requires.
        unsafe { Box::from_raw(self_ as *mut AdminClassHandle) }.destroy();
    }
}

// --- MockAdminClient.Builder ------------------------------------------------

/// Opaque handle to a [`Builder`], Java's nested `MockAdminClient.Builder`.
/// Owned by the caller, freed with [`kafka_admin_MockAdminClient_Builder_destroy`].
#[repr(C)]
#[doc(alias = "public-in-rust")]
pub struct kafka_admin_MockAdminClient_Builder_t {
    _private: [u8; 0],
}

/// Hands `builder` to C as an owned handle.
fn box_builder(builder: Builder) -> *mut kafka_admin_MockAdminClient_Builder_t {
    Box::into_raw(Box::new(builder)) as *mut kafka_admin_MockAdminClient_Builder_t
}

/// The builder behind a handle, exclusively.
///
/// # Safety
///
/// `self_` must be a live builder handle that no other call uses at the same
/// time, kept alive for `'a`.
unsafe fn builder_mut<'a>(self_: *mut kafka_admin_MockAdminClient_Builder_t) -> &'a mut Builder {
    // SAFETY: Per this function's `# Safety`, `self_` is a live builder handle, i.e. the
    // `Box::into_raw` of a `Builder` in `box_builder`, the only constructor, kept alive for `'a`
    // and used by no other call meanwhile, so the exclusive reference aliases nothing.
    unsafe { &mut *(self_ as *mut Builder) }
}

/// Applies the infallible fluent setter `set` to the builder in place.
///
/// # Safety
///
/// As [`builder_mut`], for this call.
unsafe fn update(self_: *mut kafka_admin_MockAdminClient_Builder_t, set: impl FnOnce(Builder) -> Builder) {
    // SAFETY: Per this function's `# Safety`, `self_` is as `builder_mut` requires.
    let builder = unsafe { builder_mut(self_) };
    *builder = set(std::mem::take(builder));
}

/// Applies the fallible fluent setter `set` to the builder: on success the
/// builder is replaced and `self_` returned; on failure the builder is left
/// as it was, the error stored in `*out_err` and `NULL` returned.
///
/// # Safety
///
/// As [`builder_mut`], for this call; `out_err` a valid slot pointing at null.
unsafe fn try_update(
    self_: *mut kafka_admin_MockAdminClient_Builder_t,
    out_err: *mut *mut kafka_common_Error_t,
    set: impl FnOnce(Builder) -> Result<Builder, Error>,
) -> *mut kafka_admin_MockAdminClient_Builder_t {
    // SAFETY: Per this function's `# Safety`, `self_` is as `builder_mut` requires.
    let builder = unsafe { builder_mut(self_) };
    // The Rust setter consumes the builder and drops it on failure; a copy keeps the
    // C handle's state for the error path.
    let result = set(builder.clone()).map(|updated| *builder = updated);
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |()| self_) }
}

/// `new MockAdminClient.Builder()`: a builder with one broker,
/// `Node(0, "localhost", 1000)`, and [`MockAdminClient::DEFAULT_LOG_DIRS`].
/// Owned by the caller, freed with [`kafka_admin_MockAdminClient_Builder_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_MockAdminClient_Builder_new() -> *mut kafka_admin_MockAdminClient_Builder_t {
    box_builder(Builder::new())
}

/// `Builder.clusterId(String clusterId)`: `cluster_id` is copied.
///
/// # Safety
///
/// `self_` must be a live builder handle that no other call uses at the same
/// time, and `cluster_id` a C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_Builder_set_cluster_id(
    self_: *mut kafka_admin_MockAdminClient_Builder_t,
    cluster_id: *const c_char,
) {
    // SAFETY: Per this function's `# Safety`, `cluster_id` is a C string, as `c_str_to_string`
    // requires; it is copied.
    let cluster_id = unsafe { c_str_to_string(cluster_id) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live builder handle no other call uses
    // at the same time, as `update` requires.
    unsafe { update(self_, |b| b.set_cluster_id(&cluster_id)) }
}

/// `Builder.brokers(List<Node> brokers)`: `brokers` is a `kafka_List_t` of
/// `const kafka_common_Node_t *`, copied during the call. As in Java, the
/// broker count is first set with `numBrokers(brokers.size())`, which pads or
/// truncates the log directories, and the list is then replaced. Returns
/// `self_` (borrowed, still owned by the caller); on failure `NULL`, the
/// builder unchanged: the one reachable failure is a log-dir list shorter
/// than `brokers` after [`kafka_admin_MockAdminClient_Builder_set_broker_log_dirs`],
/// with the JDK's message "toIndex = n" (Java's `subList`).
///
/// # Safety
///
/// `self_` must be a live builder handle that no other call uses at the same
/// time, `brokers` null or a valid list of valid node handles, and `out_err`
/// a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_Builder_set_brokers(
    self_: *mut kafka_admin_MockAdminClient_Builder_t,
    brokers: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_admin_MockAdminClient_Builder_t {
    // SAFETY: Per this function's `# Safety`, `brokers` is null or a valid list of node handles,
    // as `list_nodes` requires; the nodes are copied.
    let brokers = unsafe { list_nodes(brokers) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live builder handle no other call uses
    // at the same time and `out_err` a valid slot, as `try_update` requires.
    unsafe { try_update(self_, out_err, |b| b.set_brokers(brokers)) }
}

/// `Builder.numBrokers(int numBrokers)`: shrinking keeps the first brokers
/// and log-dir lists, growing adds `Node(id, "localhost", 1000 + id)` with the
/// default log dirs. Returns `self_` (borrowed, still owned by the caller); on
/// failure `NULL`, the builder unchanged, with the JDK's `subList` message:
/// "fromIndex(0) > toIndex(n)" for a negative count, "toIndex = n" when
/// shrinking past a shorter log-dir list.
///
/// # Safety
///
/// `self_` must be a live builder handle that no other call uses at the same
/// time, and `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_Builder_set_num_brokers(
    self_: *mut kafka_admin_MockAdminClient_Builder_t,
    num_brokers: i32,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_admin_MockAdminClient_Builder_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live builder handle no other call uses
    // at the same time and `out_err` a valid slot, as `try_update` requires.
    unsafe { try_update(self_, out_err, |b| b.set_num_brokers(num_brokers)) }
}

/// `Builder.controller(int index)`: makes the broker at `index`, read at this
/// call as Java's `brokers.get(index)`, the controller. Returns `self_`
/// (borrowed, still owned by the caller); on failure `NULL`, the builder
/// unchanged, with the JDK's message "Index i out of bounds for length n".
///
/// # Safety
///
/// `self_` must be a live builder handle that no other call uses at the same
/// time, and `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_Builder_set_controller(
    self_: *mut kafka_admin_MockAdminClient_Builder_t,
    index: i32,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_admin_MockAdminClient_Builder_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live builder handle no other call uses
    // at the same time and `out_err` a valid slot, as `try_update` requires.
    unsafe { try_update(self_, out_err, |b| b.set_controller(index)) }
}

/// `Builder.brokerLogDirs(List<List<String>> brokerLogDirs)`: a `kafka_List_t`
/// of `const kafka_List_t *`, one per broker id, each of `const char *`; all
/// copied during the call. As in Java, the length is not checked against the
/// broker count.
///
/// # Safety
///
/// `self_` must be a live builder handle that no other call uses at the same
/// time, and `broker_log_dirs` null or a valid list whose elements are null
/// or valid lists of C strings.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_Builder_set_broker_log_dirs(
    self_: *mut kafka_admin_MockAdminClient_Builder_t,
    broker_log_dirs: *const kafka_List_t,
) {
    // SAFETY: Per this function's `# Safety`, `broker_log_dirs` is null or a valid list,
    // unmutated during the call since `kafka_List_add`'s `# Safety` excludes another call using the
    // list at the same time, as `list_elements` requires; the slice is dropped before the call
    // returns.
    let broker_log_dirs: Vec<Vec<String>> = unsafe { list_elements(broker_log_dirs) }
        .iter()
        // SAFETY: Per this function's `# Safety`, each element is null or a valid list of C
        // strings, as `list_strings` requires; the strings are copied.
        .map(|&dirs| unsafe { list_strings(dirs as *const kafka_List_t) })
        .collect();
    // SAFETY: Per this function's `# Safety`, `self_` is a live builder handle no other call uses
    // at the same time, as `update` requires.
    unsafe { update(self_, |b| b.set_broker_log_dirs(broker_log_dirs)) }
}

/// `Builder.defaultReplicationFactor(int defaultReplicationFactor)`: the
/// replication factor `create_topics` uses when a topic gives none; `build`
/// narrows it to 16 bits with Java's wrapping `shortValue()`.
///
/// # Safety
///
/// `self_` must be a live builder handle that no other call uses at the same
/// time.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_Builder_set_default_replication_factor(
    self_: *mut kafka_admin_MockAdminClient_Builder_t,
    default_replication_factor: i32,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live builder handle no other call uses
    // at the same time, as `update` requires.
    unsafe { update(self_, |b| b.set_default_replication_factor(default_replication_factor)) }
}

/// `Builder.usingRaftController(boolean usingRaftController)`: `0` is `false`
/// (the default), any other value `true`.
///
/// # Safety
///
/// `self_` must be a live builder handle that no other call uses at the same
/// time.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_Builder_set_using_raft_controller(
    self_: *mut kafka_admin_MockAdminClient_Builder_t,
    using_raft_controller: i8,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live builder handle no other call uses
    // at the same time, as `update` requires.
    unsafe { update(self_, |b| b.set_using_raft_controller(using_raft_controller != 0)) }
}

/// `Builder.defaultPartitions(short numPartitions)`: the partition count
/// `create_topics` uses when a topic gives none (default 1).
///
/// # Safety
///
/// `self_` must be a live builder handle that no other call uses at the same
/// time.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_Builder_set_default_partitions(
    self_: *mut kafka_admin_MockAdminClient_Builder_t,
    num_partitions: i16,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live builder handle no other call uses
    // at the same time, as `update` requires.
    unsafe { update(self_, |b| b.set_default_partitions(num_partitions)) }
}

/// `Builder.featureLevels(Map<String, Short> featureLevels)`: a `kafka_Map_t`
/// of `const char *` to `const int16_t *`, copied during the call; null reads
/// as empty.
///
/// # Safety
///
/// `self_` must be a live builder handle that no other call uses at the same
/// time, and `feature_levels` null or a live map of the types above.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_Builder_set_feature_levels(
    self_: *mut kafka_admin_MockAdminClient_Builder_t,
    feature_levels: *const kafka_Map_t,
) {
    // SAFETY: Per this function's `# Safety`, `feature_levels` is null or a live map of C strings
    // to `const int16_t *` values, as `map_string_i16` requires; the entries are copied.
    let feature_levels = unsafe { map_string_i16(feature_levels) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live builder handle no other call uses
    // at the same time, as `update` requires.
    unsafe { update(self_, |b| b.set_feature_levels(feature_levels)) }
}

/// `Builder.minSupportedFeatureLevels(Map<String, Short>)`; the map as in
/// [`kafka_admin_MockAdminClient_Builder_set_feature_levels`].
///
/// # Safety
///
/// `self_` must be a live builder handle that no other call uses at the same
/// time, and `min_supported_feature_levels` null or a live map of C strings to
/// `const int16_t *`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_Builder_set_min_supported_feature_levels(
    self_: *mut kafka_admin_MockAdminClient_Builder_t,
    min_supported_feature_levels: *const kafka_Map_t,
) {
    // SAFETY: Per this function's `# Safety`, `min_supported_feature_levels` is null or a live map
    // of C strings to `const int16_t *` values, as `map_string_i16` requires; the entries are
    // copied.
    let levels = unsafe { map_string_i16(min_supported_feature_levels) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live builder handle no other call uses
    // at the same time, as `update` requires.
    unsafe { update(self_, |b| b.set_min_supported_feature_levels(levels)) }
}

/// `Builder.maxSupportedFeatureLevels(Map<String, Short>)`; the map as in
/// [`kafka_admin_MockAdminClient_Builder_set_feature_levels`].
///
/// # Safety
///
/// `self_` must be a live builder handle that no other call uses at the same
/// time, and `max_supported_feature_levels` null or a live map of C strings to
/// `const int16_t *`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_Builder_set_max_supported_feature_levels(
    self_: *mut kafka_admin_MockAdminClient_Builder_t,
    max_supported_feature_levels: *const kafka_Map_t,
) {
    // SAFETY: Per this function's `# Safety`, `max_supported_feature_levels` is null or a live map
    // of C strings to `const int16_t *` values, as `map_string_i16` requires; the entries are
    // copied.
    let levels = unsafe { map_string_i16(max_supported_feature_levels) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live builder handle no other call uses
    // at the same time, as `update` requires.
    unsafe { update(self_, |b| b.set_max_supported_feature_levels(levels)) }
}

/// `Builder.defaultGroupConfigs(Map<String, String> defaultGroupConfigs)`: a
/// `kafka_Map_t` of `const char *` to `const char *`, copied during the call;
/// null reads as empty. The defaults are overlaid onto every group's configs.
///
/// # Safety
///
/// `self_` must be a live builder handle that no other call uses at the same
/// time, and `default_group_configs` null or a valid map of C strings to C
/// strings.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_Builder_set_default_group_configs(
    self_: *mut kafka_admin_MockAdminClient_Builder_t,
    default_group_configs: *const kafka_Map_t,
) {
    // SAFETY: Per this function's `# Safety`, `default_group_configs` is null or a valid map of
    // `const char *` keys and values, i.e. NUL-terminated strings, as `map_strings` requires; they
    // are copied.
    let configs: HashMap<String, String> = unsafe { map_strings(default_group_configs) }.into_iter().collect();
    // SAFETY: Per this function's `# Safety`, `self_` is a live builder handle no other call uses
    // at the same time, as `update` requires.
    unsafe { update(self_, |b| b.set_default_group_configs(configs)) }
}

/// `Builder.build()`: a mock from the builder's current settings, owned by
/// the caller and freed with [`kafka_admin_MockAdminClient_destroy`]. The
/// builder is left as it is and stays the caller's to destroy, as a Java
/// builder may build again. Defaults: the controller is the first broker, the
/// default partition count 1 and the default replication factor
/// `min(brokers, 3)`. Fails, with `NULL` returned, with the JDK's "Index 0 out
/// of bounds for length 0" when there are no brokers and no controller was
/// set (Java's `brokers.get(0)`), and with Java's "The controller node must be
/// in the list of brokers" when the controller is no longer one of the
/// brokers.
///
/// # Safety
///
/// `self_` must be a live builder handle that no other call uses at the same
/// time, and `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_Builder_build(
    self_: *mut kafka_admin_MockAdminClient_Builder_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_admin_MockAdminClient_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live builder handle no other call uses
    // at the same time, kept alive for this call, as `builder_mut` requires.
    let result = unsafe { builder_mut(self_) }.clone().build();
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid slot, as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, box_mock) }
}

/// Frees a builder handle; null is a no-op. Mocks it built are unaffected.
///
/// # Safety
///
/// `self_` must be null or an owned builder handle not yet destroyed, that no
/// other call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MockAdminClient_Builder_destroy(
    self_: *mut kafka_admin_MockAdminClient_Builder_t,
) {
    if !self_.is_null() {
        // SAFETY: Per this function's `# Safety`, `self_` (non-null, checked above) is an owned
        // builder handle not yet destroyed, i.e. the `Box::into_raw` of a `Builder` in
        // `box_builder`, that no other call uses and that is not used afterwards, so taking it back
        // and dropping it frees it exactly once.
        unsafe { drop(Box::from_raw(self_ as *mut Builder)) };
    }
}
