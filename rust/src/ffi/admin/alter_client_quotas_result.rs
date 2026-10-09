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

//! C bindings for `org.apache.kafka.clients.admin.AlterClientQuotasResult`.

use std::collections::HashMap;
use std::ffi::c_void;
use std::ptr;

use crate::admin::AlterClientQuotasResult;
use crate::common::quota::ClientQuotaEntity;
use crate::common::{Error, KafkaFuture};
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, result_ref};
use crate::ffi::common::quota::client_quota_entity::{
    box_client_quota_entity, client_quota_entity_ref, kafka_common_quota_ClientQuotaEntity_destroy,
    kafka_common_quota_ClientQuotaEntity_t, sorted_entries,
};
use crate::ffi::common::take_error;
use crate::ffi::kafka_future::{
    kafka_common_KafkaFuture_get, kafka_common_KafkaFuture_is_done, kafka_common_KafkaFuture_t,
};
use crate::ffi::util::{kafka_Map_t, map_entries};

/// Opaque handle to an [`AlterClientQuotasResult`], owned by the caller and
/// freed with [`kafka_admin_AlterClientQuotasResult_destroy`].
#[repr(C)]
pub struct kafka_admin_AlterClientQuotasResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C, binding the futures it exposes to `ctx`.
pub(crate) fn box_alter_client_quotas_result(
    result: AlterClientQuotasResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_AlterClientQuotasResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle.
unsafe fn handle<'a>(self_: *const kafka_admin_AlterClientQuotasResult_t) -> &'a ResultHandle<AlterClientQuotasResult> {
    unsafe { result_ref(self_) }
}

/// Frees a `kafka_common_quota_ClientQuotaEntity_t *` key of an owned map.
///
/// # Safety
///
/// `element` must be an owned `kafka_common_quota_ClientQuotaEntity_t *`.
unsafe fn destroy_client_quota_entity_element(element: *mut c_void) {
    unsafe { kafka_common_quota_ClientQuotaEntity_destroy(element as *mut kafka_common_quota_ClientQuotaEntity_t) }
}

/// Compares two `kafka_common_quota_ClientQuotaEntity_t *` keys by value.
///
/// # Safety
///
/// `a` and `b` must be valid `kafka_common_quota_ClientQuotaEntity_t *`.
unsafe fn client_quota_entity_key_eq(a: *mut c_void, b: *mut c_void) -> bool {
    unsafe {
        client_quota_entity_ref(a as *const kafka_common_quota_ClientQuotaEntity_t)
            == client_quota_entity_ref(b as *const kafka_common_quota_ClientQuotaEntity_t)
    }
}

/// The sort key of an entity: its entries in type order (its `toString()`
/// prints an unordered map, so it cannot serve).
fn entity_sort_key(entity: &ClientQuotaEntity) -> Vec<(String, Option<String>)> {
    sorted_entries(entity)
        .into_iter()
        .map(|(t, n)| (t.to_string(), n.map(str::to_string)))
        .collect()
}

/// The Rust `KafkaFuture<Void>` standing for a C future that is already
/// complete (see [`kafka_admin_AlterClientQuotasResult_new`]): its outcome
/// is copied; a future that is not yet complete yields one failed with the
/// translation of `IllegalArgumentException`.
///
/// # Safety
///
/// `future` must be a valid future handle.
unsafe fn resolved_void_future(future: *const kafka_common_KafkaFuture_t) -> KafkaFuture<()> {
    if unsafe { kafka_common_KafkaFuture_is_done(future) } == 0 {
        return KafkaFuture::completed_future(Err(Error::local_illegal_argument(
            "The futures passed to AlterClientQuotasResult must already be complete",
        )));
    }
    let mut value: *mut c_void = ptr::null_mut();
    let error = unsafe { kafka_common_KafkaFuture_get(future, &mut value) };
    KafkaFuture::completed_future(unsafe { take_error(error) }.map_or(Ok(()), Err))
}

/// `new AlterClientQuotasResult(Map<ClientQuotaEntity, KafkaFuture<Void>> futures)`:
/// `futures` is a borrowed map of `const kafka_common_quota_ClientQuotaEntity_t *`
/// to `const kafka_common_KafkaFuture_t *`, copied during the call; the
/// caller keeps ownership of the map, its keys and its futures. Only futures
/// that are already complete (`kafka_common_KafkaFuture_is_done`) can be
/// copied across the boundary: the outcome of each is taken as it stands,
/// and a future still pending stands as one failed with the translation of
/// `IllegalArgumentException`. Owned, freed with
/// [`kafka_admin_AlterClientQuotasResult_destroy`].
///
/// # Safety
///
/// `futures` must be null or a valid map whose keys are client-quota-entity
/// handles and whose values are future handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterClientQuotasResult_new(
    futures: *const kafka_Map_t,
) -> *mut kafka_admin_AlterClientQuotasResult_t {
    let futures: HashMap<ClientQuotaEntity, KafkaFuture<()>> = unsafe { map_entries(futures) }
        .iter()
        .map(|&(k, v)| {
            (
                unsafe { client_quota_entity_ref(k as *const kafka_common_quota_ClientQuotaEntity_t) }.clone(),
                unsafe { resolved_void_future(v as *const kafka_common_KafkaFuture_t) },
            )
        })
        .collect();
    box_alter_client_quotas_result(AlterClientQuotasResult::new(futures), &FutureCtx::detached())
}

/// `AlterClientQuotasResult.values()`: an owned map, sorted by the entity's
/// entries, of owned `kafka_common_quota_ClientQuotaEntity_t *` keys
/// (compared by value in `kafka_Map_get`) to owned
/// `kafka_common_KafkaFuture_t *` (`KafkaFuture<Void>`: `get` delivers
/// `NULL`). Freed with `kafka_Map_destroy`, which frees the keys and the
/// futures.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterClientQuotasResult_values(
    self_: *const kafka_admin_AlterClientQuotasResult_t,
) -> *mut kafka_Map_t {
    let h = unsafe { handle(self_) };
    let mut entries: Vec<(&ClientQuotaEntity, &KafkaFuture<()>)> = h.result.values().iter().collect();
    entries.sort_by_cached_key(|(entity, _)| entity_sort_key(entity));
    h.ctx.keyed_future_map(
        entries,
        |entity| box_client_quota_entity(entity.clone()) as *mut c_void,
        destroy_client_quota_entity_element,
        client_quota_entity_key_eq,
        FutureCtx::void_future,
    )
}

/// `AlterClientQuotasResult.all()`: an owned `KafkaFuture<Void>` (its `get`
/// delivers `NULL`) that succeeds once every quota alteration succeeded,
/// freed with `kafka_common_KafkaFuture_destroy`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterClientQuotasResult_all(
    self_: *const kafka_admin_AlterClientQuotasResult_t,
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
pub unsafe extern "C" fn kafka_admin_AlterClientQuotasResult_destroy(
    self_: *mut kafka_admin_AlterClientQuotasResult_t,
) {
    unsafe { destroy_result::<AlterClientQuotasResult, _>(self_) }
}
