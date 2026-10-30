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

//! C bindings for `org.apache.kafka.clients.admin.DeleteRecordsResult`.

use std::ffi::c_void;

use crate::admin::{DeleteRecordsResult, DeletedRecords};
use crate::common::{KafkaFuture, TopicPartition};
use crate::ffi::admin::deleted_records::{
    box_deleted_records, deleted_records_ref, destroy_deleted_records_element, kafka_admin_DeletedRecords_t,
};
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, resolved_future, result_ref};
use crate::ffi::common::topic_partition::{
    box_topic_partition, kafka_common_TopicPartition_destroy, kafka_common_TopicPartition_t, topic_partition_key_eq,
    topic_partition_ref,
};
use crate::ffi::ffi_guard;
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::{kafka_Map_t, map_entries};

/// Opaque handle to a [`DeleteRecordsResult`], owned by the caller and
/// freed with [`kafka_admin_DeleteRecordsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_DeleteRecordsResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C, binding the futures it exposes to `ctx`.
pub(crate) fn box_delete_records_result(
    result: DeleteRecordsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_DeleteRecordsResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle, kept alive for `'a`.
unsafe fn handle<'a>(self_: *const kafka_admin_DeleteRecordsResult_t) -> &'a ResultHandle<DeleteRecordsResult> {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle kept alive for `'a`,
    // and every `kafka_admin_DeleteRecordsResult_t` is built by
    // `box_result::<DeleteRecordsResult, _>` (in `box_delete_records_result`), as `result_ref`
    // requires.
    unsafe { result_ref(self_) }
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

/// Return a map from topic partition to futures which can be used to check the status of individual
/// deletions.
///
/// # Returns
///
/// A map, sorted by topic then partition, of owned `kafka_common_TopicPartition_t *` keys (compared
/// by value in `kafka_Map_get`) to owned `kafka_common_KafkaFuture_t *` whose `get` delivers a
/// `kafka_admin_DeletedRecords_t *` owned by the future.
///
/// # Safety
///
/// `self_` must be a live result handle.
///
/// The returned map is owned by the caller, freed with `kafka_Map_destroy`, which frees the keys
/// and the futures.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteRecordsResult_low_watermarks(
    self_: *const kafka_admin_DeleteRecordsResult_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    let mut entries: Vec<(&TopicPartition, &KafkaFuture<DeletedRecords>)> = h.result.low_watermarks().iter().collect();
    entries.sort_by(|a, b| (a.0.topic(), a.0.partition()).cmp(&(b.0.topic(), b.0.partition())));
    h.ctx.keyed_future_map(
        entries,
        |tp| box_topic_partition(tp.clone()) as *mut c_void,
        destroy_topic_partition_element,
        topic_partition_key_eq,
        |ctx, future| {
            // SAFETY: `destroy_deleted_records_element` frees exactly the handle
            // `box_deleted_records` makes, and the handle holds only owned data and no thread-bound
            // state, so it may be freed from any thread and read from several at once, as
            // `handle_future` requires.
            unsafe {
                ctx.handle_future(
                    future,
                    |records| box_deleted_records(records) as *mut c_void,
                    destroy_deleted_records_element,
                )
            }
        },
    )
}

/// Return a future which succeeds only if all the records deletions succeed.
///
/// # Returns
///
/// A `KafkaFuture<Void>` (its `get` delivers `NULL`) that succeeds once every deletion succeeded.
///
/// # Safety
///
/// `self_` must be a live result handle.
///
/// The returned future is owned by the caller, freed with `kafka_common_KafkaFuture_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteRecordsResult_all(
    self_: *const kafka_admin_DeleteRecordsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    h.ctx.void_future(&h.result.all())
}

/// Creates a result from a map of partition to the future of its deleted records.
///
/// # Parameters
///
/// - `futures` - a map of `const kafka_common_TopicPartition_t *` to `const
///   kafka_common_KafkaFuture_t *` whose value is a `kafka_admin_DeletedRecords_t *`, copied during
///   the call
///
/// # Safety
///
/// `futures` must be `NULL` or a valid map whose keys are valid topic-partition handles and whose
/// values are valid future handles, each successful one's value a valid
/// `kafka_admin_DeletedRecords_t *`. The caller keeps ownership of the map, its keys, its futures
/// and their values. Each future should already be complete (`kafka_common_KafkaFuture_is_done`):
/// its outcome is copied as it stands, and one still pending stands as one failed with
/// `kafka_common_Error_is_local_illegal_argument_error`. The returned handle is owned and freed
/// with `kafka_admin_DeleteRecordsResult_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteRecordsResult_new(
    futures: *const kafka_Map_t,
) -> *mut kafka_admin_DeleteRecordsResult_t {
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
                // alive for this call, and a successful one's value a valid `kafka_admin_DeletedRecords_t *`
                // borrowed from it, as `deleted_records_ref` requires; the value is cloned, so the borrow
                // ends within the call, as `resolved_future` requires.
                unsafe {
                    resolved_future(v as *const kafka_common_KafkaFuture_t, "DeleteRecordsResult", |value| {
                        deleted_records_ref(value as *const kafka_admin_DeletedRecords_t).clone()
                    })
                },
            )
        })
        .collect();
    box_result(DeleteRecordsResult::new(futures), &FutureCtx::detached())
}

/// Frees a result handle; `NULL` is a no-op. Maps and futures taken from the
/// result stay valid until they are destroyed themselves.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned result handle not yet destroyed, that
/// no other call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteRecordsResult_destroy(self_: *mut kafka_admin_DeleteRecordsResult_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned result handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards; every
    // `kafka_admin_DeleteRecordsResult_t` is built by `box_result::<DeleteRecordsResult, _>`, as
    // `destroy_result` requires.
    unsafe { destroy_result::<DeleteRecordsResult, _>(self_) }
}
