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
//! The Rust result is an enum (keyed by topic id or by topic name, never
//! both), so besides the Java methods it has the `_e` enumerator type,
//! `__enum`, and one factory per variant (CLAUDE.md §4, "Enums").

use std::collections::HashMap;
use std::ffi::{c_char, c_void};
use std::ptr;

use crate::admin::DeleteTopicsResult;
use crate::common::{Error, KafkaFuture, Uuid};
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, result_ref};
use crate::ffi::common::take_error;
use crate::ffi::common::uuid::{box_uuid, kafka_common_Uuid_destroy, kafka_common_Uuid_t, uuid_of};
use crate::ffi::kafka_future::{
    kafka_common_KafkaFuture_get, kafka_common_KafkaFuture_is_done, kafka_common_KafkaFuture_t,
};
use crate::ffi::util::{c_str_to_string, kafka_Map_t, map_entries};

/// Opaque handle to a [`DeleteTopicsResult`], owned by the caller and freed
/// with [`kafka_admin_DeleteTopicsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_DeleteTopicsResult_t {
    _private: [u8; 0],
}

/// The variants of [`DeleteTopicsResult`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_admin_DeleteTopicsResult_e {
    by_topic_id,
    by_topic_name,
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
/// `self_` must be a live result handle.
unsafe fn handle<'a>(self_: *const kafka_admin_DeleteTopicsResult_t) -> &'a ResultHandle<DeleteTopicsResult> {
    unsafe { result_ref(self_) }
}

/// Frees a `kafka_common_Uuid_t *` key of an owned map.
///
/// # Safety
///
/// `element` must be an owned `kafka_common_Uuid_t *`.
unsafe fn destroy_uuid_element(element: *mut c_void) {
    unsafe { kafka_common_Uuid_destroy(element as *mut kafka_common_Uuid_t) }
}

/// Compares two `kafka_common_Uuid_t *` keys by value.
///
/// # Safety
///
/// `a` and `b` must be valid `kafka_common_Uuid_t *`.
unsafe fn uuid_key_eq(a: *mut c_void, b: *mut c_void) -> bool {
    unsafe { uuid_of(a as *const kafka_common_Uuid_t) == uuid_of(b as *const kafka_common_Uuid_t) }
}

/// The Rust `KafkaFuture<Void>` standing for a C future that is already
/// complete (see [`kafka_admin_DeleteTopicsResult_by_topic_id`]): its
/// outcome is copied; a future that is not yet complete yields one failed
/// with the translation of `IllegalArgumentException`.
///
/// # Safety
///
/// `future` must be a valid future handle.
unsafe fn resolved_void_future(future: *const kafka_common_KafkaFuture_t) -> KafkaFuture<()> {
    if unsafe { kafka_common_KafkaFuture_is_done(future) } == 0 {
        return KafkaFuture::completed_future(Err(Error::local_illegal_argument(
            "The futures passed to DeleteTopicsResult must already be complete",
        )));
    }
    let mut value: *mut c_void = ptr::null_mut();
    let error = unsafe { kafka_common_KafkaFuture_get(future, &mut value) };
    KafkaFuture::completed_future(unsafe { take_error(error) }.map_or(Ok(()), Err))
}

/// `DeleteTopicsResult.ofTopicIds(Map<Uuid, KafkaFuture<Void>> topicIdFutures)`:
/// `value` is a borrowed map of `const kafka_common_Uuid_t *` to
/// `const kafka_common_KafkaFuture_t *`, copied during the call; the caller
/// keeps ownership of the map, its keys and its futures. Only futures that
/// are already complete (`kafka_common_KafkaFuture_is_done`) can be copied
/// across the boundary: the outcome of each is taken as it stands, and a
/// future still pending stands as one failed with the translation of
/// `IllegalArgumentException`. Owned, freed with
/// [`kafka_admin_DeleteTopicsResult_destroy`].
///
/// # Safety
///
/// `value` must be null or a valid map whose keys are uuid handles and
/// whose values are future handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteTopicsResult_by_topic_id(
    value: *const kafka_Map_t,
) -> *mut kafka_admin_DeleteTopicsResult_t {
    let futures: HashMap<Uuid, KafkaFuture<()>> = unsafe { map_entries(value) }
        .iter()
        .map(|&(k, v)| {
            (unsafe { uuid_of(k as *const kafka_common_Uuid_t) }, unsafe {
                resolved_void_future(v as *const kafka_common_KafkaFuture_t)
            })
        })
        .collect();
    box_delete_topics_result(DeleteTopicsResult::of_topic_ids(futures), &FutureCtx::detached())
}

/// `DeleteTopicsResult.ofTopicNames(Map<String, KafkaFuture<Void>> nameFutures)`:
/// `value` is a borrowed map of `const char *` to
/// `const kafka_common_KafkaFuture_t *`, copied during the call; the caller
/// keeps ownership of the map, its keys and its futures. Only futures that
/// are already complete (`kafka_common_KafkaFuture_is_done`) can be copied
/// across the boundary: the outcome of each is taken as it stands, and a
/// future still pending stands as one failed with the translation of
/// `IllegalArgumentException`. Owned, freed with
/// [`kafka_admin_DeleteTopicsResult_destroy`].
///
/// # Safety
///
/// `value` must be null or a valid map whose keys are NUL-terminated
/// strings and whose values are future handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteTopicsResult_by_topic_name(
    value: *const kafka_Map_t,
) -> *mut kafka_admin_DeleteTopicsResult_t {
    let futures: HashMap<String, KafkaFuture<()>> = unsafe { map_entries(value) }
        .iter()
        .map(|&(k, v)| {
            (unsafe { c_str_to_string(k as *const c_char) }, unsafe {
                resolved_void_future(v as *const kafka_common_KafkaFuture_t)
            })
        })
        .collect();
    box_delete_topics_result(DeleteTopicsResult::of_topic_names(futures), &FutureCtx::detached())
}

/// The variant of a result, for a `switch`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteTopicsResult__enum(
    self_: *const kafka_admin_DeleteTopicsResult_t,
) -> kafka_admin_DeleteTopicsResult_e {
    match unsafe { handle(self_) }.result {
        DeleteTopicsResult::ByTopicId(_) => kafka_admin_DeleteTopicsResult_e::by_topic_id,
        DeleteTopicsResult::ByTopicName(_) => kafka_admin_DeleteTopicsResult_e::by_topic_name,
    }
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteTopicsResult_topic_id_values(
    self_: *const kafka_admin_DeleteTopicsResult_t,
) -> *mut kafka_Map_t {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteTopicsResult_topic_name_values(
    self_: *const kafka_admin_DeleteTopicsResult_t,
) -> *mut kafka_Map_t {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteTopicsResult_all(
    self_: *const kafka_admin_DeleteTopicsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.void_future(&h.result.all())
}

/// Frees a result handle; null is a no-op. Maps and futures taken from the
/// result stay valid until they are destroyed themselves.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteTopicsResult_destroy(self_: *mut kafka_admin_DeleteTopicsResult_t) {
    unsafe { destroy_result::<DeleteTopicsResult, _>(self_) }
}
