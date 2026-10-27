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

//! `kafka_admin_ListOffsetsResult_t` and its nested
//! `kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t`:
//! `org.apache.kafka.clients.admin.ListOffsetsResult` (CLAUDE.md §4).

use std::collections::HashMap;
use std::ffi::{c_char, c_void};

use crate::admin::{ListOffsetsResult, ListOffsetsResultInfo};
use crate::common::TopicPartition;
use crate::ffi::admin::{
    FutureCtx, ResultHandle, box_result, destroy_map_element, destroy_result, resolved_future, result_ref,
};
use crate::ffi::common::deliver_value;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::common::topic_partition::{
    box_topic_partition, kafka_common_TopicPartition_destroy, kafka_common_TopicPartition_t, topic_partition_key_eq,
    topic_partition_ref,
};
use crate::ffi::ffi_guard;
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::{box_map, into_c_string, kafka_Map_t, map_entries};

// ---------------------------------------------------------------------------
// ListOffsetsResult.ListOffsetsResultInfo
// ---------------------------------------------------------------------------

/// Opaque handle to a [`ListOffsetsResultInfo`], owned and freed with
/// [`kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_destroy`].
#[repr(C)]
pub struct kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t {
    _private: [u8; 0],
}

/// Hands `info` to C as an owned handle.
pub(crate) fn box_list_offsets_result_info(
    info: ListOffsetsResultInfo,
) -> *mut kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t {
    Box::into_raw(Box::new(info)) as *mut kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `info` must be a live list-offsets-result-info handle, kept alive for
/// `'a`.
pub(crate) unsafe fn list_offsets_result_info_ref<'a>(
    info: *const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t,
) -> &'a ListOffsetsResultInfo {
    // SAFETY: Per this function's `# Safety`, `info` is a live list-offsets-result-info handle kept
    // alive for `'a`. Every `kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t` is the
    // `Box::into_raw` of a `Box<ListOffsetsResultInfo>` (`box_list_offsets_result_info`, the only
    // place one is made, for an owned handle or a value a map or future owns), so it points at a
    // live `ListOffsetsResultInfo`. Nothing takes a `&mut` to it: only the destroy takes the box
    // back, and its `# Safety` excludes any other use at the same time or afterwards.
    unsafe { &*(info as *const ListOffsetsResultInfo) }
}

/// Frees a `kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t *` element
/// of an owned container or future.
///
/// # Safety
///
/// `element` must be an owned list-offsets-result-info handle not yet
/// destroyed, that no other call uses at the same time; it is not used
/// afterwards.
pub(crate) unsafe fn destroy_list_offsets_result_info_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned list-offsets-result-info handle
    // not yet destroyed, that no other call uses at the same time and that is not used afterwards,
    // as `kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_destroy` requires.
    unsafe {
        kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_destroy(
            element as *mut kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t,
        )
    }
}

/// Creates the offset of a partition with its timestamp and leader epoch.
///
/// # Parameters
///
/// - `offset` - the offset
/// - `timestamp` - the timestamp
/// - `leader_epoch` - the leader epoch, `-1` when unknown
///
/// # Safety
///
/// The returned handle is owned and freed with
/// `kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_new(
    offset: i64,
    timestamp: i64,
    leader_epoch: i32,
) -> *mut kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t {
    let leader_epoch = if leader_epoch == -1 { None } else { Some(leader_epoch) };
    box_list_offsets_result_info(ListOffsetsResultInfo::new(offset, timestamp, leader_epoch))
}

/// Returns the offset.
///
/// # Safety
///
/// `self_` must be a live list-offsets-result-info handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_offset(
    self_: *const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live list-offsets-result-info handle,
    // kept alive for this call, as `list_offsets_result_info_ref` requires; the borrow ends with
    // the getter.
    unsafe { list_offsets_result_info_ref(self_) }.offset()
}

/// Returns the timestamp.
///
/// # Safety
///
/// `self_` must be a live list-offsets-result-info handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_timestamp(
    self_: *const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live list-offsets-result-info handle,
    // kept alive for this call, as `list_offsets_result_info_ref` requires; the borrow ends with
    // the getter.
    unsafe { list_offsets_result_info_ref(self_) }.timestamp()
}

/// Returns `-1` for Java's empty `Optional`.
///
/// A caught Rust panic returns -2, which is no leader epoch.
///
/// # Safety
///
/// `self_` must be a live list-offsets-result-info handle.
#[ffi_guard(fallback = -2)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_leader_epoch(
    self_: *const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live list-offsets-result-info handle,
    // kept alive for this call, as `list_offsets_result_info_ref` requires; the borrow ends with
    // the getter.
    unsafe { list_offsets_result_info_ref(self_) }.leader_epoch().unwrap_or(-1)
}

/// Returns as a string.
///
/// # Safety
///
/// `self_` must be a live list-offsets-result-info handle.
///
/// The returned string is owned by the caller, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_to_string(
    self_: *const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a live list-offsets-result-info handle,
    // kept alive for this call, as `list_offsets_result_info_ref` requires; the borrow ends with
    // `to_string`.
    into_c_string(&unsafe { list_offsets_result_info_ref(self_) }.to_string())
}

/// Frees an owned list-offsets-result-info handle; `NULL` is a no-op.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned handle not yet destroyed, that no other
/// call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_destroy(
    self_: *mut kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, that no other call uses at the same time and that is not used
        // afterwards. Every such handle is the `Box::into_raw` of a `Box<ListOffsetsResultInfo>`
        // (`box_list_offsets_result_info`), so `from_raw` reclaims that allocation, once.
        drop(unsafe { Box::from_raw(self_ as *mut ListOffsetsResultInfo) });
    }
}

// ---------------------------------------------------------------------------
// ListOffsetsResult
// ---------------------------------------------------------------------------

/// Opaque handle to a [`ListOffsetsResult`], owned and freed with
/// [`kafka_admin_ListOffsetsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_ListOffsetsResult_t {
    _private: [u8; 0],
}

/// Frees a `kafka_common_TopicPartition_t *` key of an owned map.
///
/// # Safety
///
/// `element` must be an owned topic-partition handle not yet destroyed.
unsafe fn destroy_topic_partition_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned topic-partition handle not yet
    // destroyed, as `kafka_common_TopicPartition_destroy` requires.
    unsafe { kafka_common_TopicPartition_destroy(element as *mut kafka_common_TopicPartition_t) }
}

/// Java's `Map<TopicPartition, ListOffsetsResultInfo>` as an owned map of
/// owned `kafka_common_TopicPartition_t *` keys (compared by value) to owned
/// `kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t *`, ordered by
/// topic then partition.
fn infos_map(map: HashMap<TopicPartition, ListOffsetsResultInfo>) -> *mut c_void {
    let mut entries: Vec<(TopicPartition, ListOffsetsResultInfo)> = map.into_iter().collect();
    entries.sort_by(|(a, _), (b, _)| a.topic().cmp(b.topic()).then(a.partition().cmp(&b.partition())));
    let entries = entries
        .into_iter()
        .map(|(tp, i)| {
            (
                box_topic_partition(tp) as *mut c_void,
                box_list_offsets_result_info(i) as *mut c_void,
            )
        })
        .collect();
    box_map(
        entries,
        Some(destroy_topic_partition_element),
        Some(destroy_list_offsets_result_info_element),
        Some(topic_partition_key_eq),
    ) as *mut c_void
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_list_offsets_result(
    result: ListOffsetsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_ListOffsetsResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle, kept alive for `'a`.
unsafe fn handle<'a>(self_: *const kafka_admin_ListOffsetsResult_t) -> &'a ResultHandle<ListOffsetsResult> {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle kept alive for `'a`,
    // and every `kafka_admin_ListOffsetsResult_t` is built by `box_result::<ListOffsetsResult, _>`
    // (in `box_list_offsets_result`), as `result_ref` requires.
    unsafe { result_ref(self_) }
}

/// Return a future which can be used to check the result for a given partition.
///
/// # Parameters
///
/// - `partition` - the partition, borrowed for the call
///
/// # Returns
///
/// A future whose `get` yields a `kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t *` owned
/// by the future; `NULL` on failure.
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_argument_error` - `partition` was not part of the request
///
/// # Safety
///
/// `self_` must be a live result handle, `partition` a valid topic-partition
/// handle and `out_err` a valid slot pointing at `NULL`. The returned future is
/// owned and freed with `kafka_common_KafkaFuture_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResult_partition_result(
    self_: *const kafka_admin_ListOffsetsResult_t,
    partition: *const kafka_common_TopicPartition_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    // SAFETY: Per this function's `# Safety`, `partition` is a valid topic-partition handle, as
    // `topic_partition_ref` requires; the caller keeps it alive for this call, which the borrow
    // does not outlive.
    let partition = unsafe { topic_partition_ref(partition) };
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_value` requires. For `handle_future`, `destroy_list_offsets_result_info_element` frees exactly the
    // handle `box_list_offsets_result_info` makes, and the handle holds only owned data and no
    // thread-bound state, so it may be freed from any thread and read from several at once, as
    // `handle_future` requires.
    unsafe {
        deliver_value(h.result.partition_result(partition), out_err, |f| {
            h.ctx.handle_future(
                &f,
                |i| box_list_offsets_result_info(i) as *mut c_void,
                destroy_list_offsets_result_info_element,
            )
        })
    }
}

/// Return a future which succeeds only if offsets for all specified partitions have been
/// successfully retrieved.
///
/// # Returns
///
/// A future; its `get` delivers a `kafka_Map_t *` (owned by the future) of
/// `kafka_common_TopicPartition_t *` keys, ordered by topic then partition and compared by value in
/// `kafka_Map_get`, to `kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t *` values.
///
/// # Safety
///
/// `self_` must be a live result handle.
///
/// The returned future is owned by the caller, freed with `kafka_common_KafkaFuture_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResult_all(
    self_: *const kafka_admin_ListOffsetsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    // SAFETY: `destroy_map_element` frees exactly the map `infos_map` makes through `box_map`, and
    // the handle holds only owned data and no thread-bound state, so it may be freed from any
    // thread and read from several at once, as `handle_future` requires.
    unsafe { h.ctx.handle_future(&h.result.all(), infos_map, destroy_map_element) }
}

/// Creates a result from a map of partition to the future of its offset.
///
/// # Parameters
///
/// - `futures` - a map of `const kafka_common_TopicPartition_t *` to `const
///   kafka_common_KafkaFuture_t *` whose value is a
///   `kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t *`, copied during the call
///
/// # Safety
///
/// `futures` must be `NULL` or a valid map whose keys are valid topic-partition handles and whose
/// values are valid future handles, each successful one's value a valid
/// `kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t *`. The caller keeps ownership of the
/// map, its keys, its futures and their values. Each future should already be complete
/// (`kafka_common_KafkaFuture_is_done`): its outcome is copied as it stands, and one still pending
/// stands as one failed with `kafka_common_Error_is_local_illegal_argument_error`. The returned
/// handle is owned and freed with `kafka_admin_ListOffsetsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResult_new(
    futures: *const kafka_Map_t,
) -> *mut kafka_admin_ListOffsetsResult_t {
    // SAFETY: Per this function's `# Safety`, `futures` is null or a valid map handle. The borrow
    // of its entries ends with this call, while the caller keeps the map alive, and nothing mutates
    // it meanwhile: `kafka_Map_put`, its one mutator, requires that no other call uses the map at
    // the same time. That is what `map_entries` requires.
    let futures = unsafe { map_entries(futures) }
        .iter()
        .map(|&(k, v)| {
            (
                // SAFETY: Per this function's `# Safety`, the key `k` is a valid topic-partition
                // handle, which the caller keeps alive for this call, as `topic_partition_ref`
                // requires; the borrow ends with the `clone`.
                unsafe { topic_partition_ref(k as *const kafka_common_TopicPartition_t) }.clone(),
                // SAFETY: Per this function's `# Safety`, the value `v` is a valid future handle,
                // alive for this call, and a successful one's value a valid `kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t *`
                // borrowed from it, as `list_offsets_result_info_ref` requires; the value is cloned, so the borrow
                // ends within the call, as `resolved_future` requires.
                unsafe {
                    resolved_future(v as *const kafka_common_KafkaFuture_t, "ListOffsetsResult", |value| {
                        list_offsets_result_info_ref(
                            value as *const kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_t,
                        )
                        .clone()
                    })
                },
            )
        })
        .collect();
    box_result(ListOffsetsResult::new(futures), &FutureCtx::detached())
}

/// Frees a result handle; `NULL` is a no-op. Futures already taken from it
/// stay valid.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned result handle not yet destroyed, that
/// no other call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListOffsetsResult_destroy(self_: *mut kafka_admin_ListOffsetsResult_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned result handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards; every
    // `kafka_admin_ListOffsetsResult_t` is built by `box_result::<ListOffsetsResult, _>`, as
    // `destroy_result` requires.
    unsafe { destroy_result::<ListOffsetsResult, _>(self_) }
}
