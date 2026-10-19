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

//! C bindings for `org.apache.kafka.clients.admin.DeleteTopicsResult`.
//!
//! A result is keyed by topic id or by topic name, never both: the accessor
//! for the other key returns `NULL`, as Java's returns `null`. Java's
//! constructor is protected and its `ofTopicIds` / `ofTopicNames` factories
//! package-private, so C gets no constructor.

use std::ffi::c_void;
use std::ptr;

use crate::admin::DeleteTopicsResult;
use crate::common::{KafkaFuture, Uuid};
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, result_ref};
use crate::ffi::common::uuid::{box_uuid, kafka_common_Uuid_destroy, kafka_common_Uuid_t, uuid_of};
use crate::ffi::ffi_guard;
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::kafka_Map_t;

/// Opaque handle to a [`DeleteTopicsResult`], owned by the caller and freed
/// with [`kafka_admin_DeleteTopicsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_DeleteTopicsResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C, binding the futures it exposes to `ctx`.
pub(crate) fn box_delete_topics_result(
    result: DeleteTopicsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_DeleteTopicsResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle, kept alive for `'a`.
unsafe fn handle<'a>(self_: *const kafka_admin_DeleteTopicsResult_t) -> &'a ResultHandle<DeleteTopicsResult> {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle kept alive for `'a`,
    // and every `kafka_admin_DeleteTopicsResult_t` is built by
    // `box_result::<DeleteTopicsResult, _>` (in `box_delete_topics_result`), as `result_ref`
    // requires.
    unsafe { result_ref(self_) }
}

/// Frees a `kafka_common_Uuid_t *` key of an owned map.
///
/// # Safety
///
/// `element` must be an owned uuid handle not yet destroyed.
unsafe fn destroy_uuid_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned uuid handle not yet destroyed,
    // as `kafka_common_Uuid_destroy` requires.
    unsafe { kafka_common_Uuid_destroy(element as *mut kafka_common_Uuid_t) }
}

/// Compares two `kafka_common_Uuid_t *` keys by value.
///
/// # Safety
///
/// `a` and `b` must be valid uuid handles, alive for this call.
unsafe fn uuid_key_eq(a: *mut c_void, b: *mut c_void) -> bool {
    // SAFETY: Per this function's `# Safety`, `a` and `b` are valid uuid handles, alive for this
    // call, as `uuid_of` requires.
    unsafe { uuid_of(a as *const kafka_common_Uuid_t) == uuid_of(b as *const kafka_common_Uuid_t) }
}

/// `DeleteTopicsResult.topicIdValues()`: an owned map, sorted by topic id,
/// of owned `kafka_common_Uuid_t *` keys (compared by value in
/// `kafka_Map_get`) to owned `kafka_common_KafkaFuture_t *`
/// (`KafkaFuture<Void>`: `get` delivers `NULL`), freed with
/// `kafka_Map_destroy` (which frees the keys and the futures); or `NULL`
/// when the result is keyed by topic name (Java's `null`).
///
/// # Safety
///
/// `self_` must be a live result handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteTopicsResult_topic_id_values(
    self_: *const kafka_admin_DeleteTopicsResult_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    match h.result.topic_id_values() {
        Some(values) => {
            let mut entries: Vec<(&Uuid, &KafkaFuture<()>)> = values.iter().collect();
            entries.sort_by_key(|(uuid, _)| **uuid);
            h.ctx.keyed_future_map(
                entries,
                |uuid| box_uuid(*uuid) as *mut c_void,
                destroy_uuid_element,
                uuid_key_eq,
                FutureCtx::void_future,
            )
        },
        None => ptr::null_mut(),
    }
}

/// `DeleteTopicsResult.topicNameValues()`: an owned map, sorted by topic
/// name, of owned `char *` keys to owned `kafka_common_KafkaFuture_t *`
/// (`KafkaFuture<Void>`: `get` delivers `NULL`), freed with
/// `kafka_Map_destroy` (which frees the keys and the futures); or `NULL`
/// when the result is keyed by topic id (Java's `null`).
///
/// # Safety
///
/// `self_` must be a live result handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteTopicsResult_topic_name_values(
    self_: *const kafka_admin_DeleteTopicsResult_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    match h.result.topic_name_values() {
        Some(values) => h.ctx.string_keyed_future_map(values.iter(), FutureCtx::void_future),
        None => ptr::null_mut(),
    }
}

/// `DeleteTopicsResult.all()`: an owned `KafkaFuture<Void>` (its `get`
/// delivers `NULL`) that succeeds once every topic was deleted, freed with
/// `kafka_common_KafkaFuture_destroy`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteTopicsResult_all(
    self_: *const kafka_admin_DeleteTopicsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    h.ctx.void_future(&h.result.all())
}

/// Frees a result handle; null is a no-op. Maps and futures taken from the
/// result stay valid until they are destroyed themselves.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed, that
/// no other call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteTopicsResult_destroy(self_: *mut kafka_admin_DeleteTopicsResult_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned result handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards; every
    // `kafka_admin_DeleteTopicsResult_t` is built by `box_result::<DeleteTopicsResult, _>`, as
    // `destroy_result` requires.
    unsafe { destroy_result::<DeleteTopicsResult, _>(self_) }
}
