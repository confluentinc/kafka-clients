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

//! C bindings for `org.apache.kafka.clients.admin.DescribeClientQuotasResult`.

use std::collections::{BTreeMap, HashMap};
use std::ffi::{c_char, c_void};
use std::ptr;

use crate::admin::DescribeClientQuotasResult;
use crate::common::quota::ClientQuotaEntity;
use crate::common::{Error, KafkaFuture};
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_map_element, destroy_result, result_ref};
use crate::ffi::common::quota::client_quota_entity::{
    box_client_quota_entity, client_quota_entity_ref, kafka_common_quota_ClientQuotaEntity_destroy,
    kafka_common_quota_ClientQuotaEntity_t, sorted_entries,
};
use crate::ffi::common::take_error;
use crate::ffi::kafka_future::{
    kafka_common_KafkaFuture_destroy, kafka_common_KafkaFuture_get, kafka_common_KafkaFuture_is_done,
    kafka_common_KafkaFuture_t,
};
use crate::ffi::util::{box_map, box_string_keyed_map, c_str_to_string, destroy_boxed, kafka_Map_t, map_entries};

/// The value of the result's future: `Map<ClientQuotaEntity, Map<String, Double>>`.
type Entities = HashMap<ClientQuotaEntity, HashMap<String, f64>>;

/// Opaque handle to a [`DescribeClientQuotasResult`], owned by the caller
/// and freed with [`kafka_admin_DescribeClientQuotasResult_destroy`].
#[repr(C)]
pub struct kafka_admin_DescribeClientQuotasResult_t {
    _private: [u8; 0],
}

/// What the handle holds: Java's `entities()` returns the result's one
/// future itself, so the C future is built once and borrowed out, owned by
/// the handle (the Rust future lives on inside it).
struct DescribeClientQuotasResultInner {
    entities: *mut kafka_common_KafkaFuture_t,
}

impl Drop for DescribeClientQuotasResultInner {
    fn drop(&mut self) {
        // SAFETY: `entities` is the owned future handle built in
        // `box_describe_client_quotas_result`, destroyed exactly here.
        unsafe { kafka_common_KafkaFuture_destroy(self.entities) }
    }
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

/// The C shape of the future's value: an owned map, sorted by the entity's
/// entries, of owned `kafka_common_quota_ClientQuotaEntity_t *` (compared
/// by value in `kafka_Map_get`) to owned `kafka_Map_t *` of owned `char *`
/// quota names (sorted) to owned `double *` quota values.
fn box_entities(entities: Entities) -> *mut kafka_Map_t {
    let mut outer: Vec<(ClientQuotaEntity, HashMap<String, f64>)> = entities.into_iter().collect();
    outer.sort_by_cached_key(|(entity, _)| entity_sort_key(entity));
    let entries = outer
        .into_iter()
        .map(|(entity, quotas)| {
            let sorted: BTreeMap<String, f64> = quotas.into_iter().collect();
            let quotas = box_string_keyed_map(
                sorted
                    .into_iter()
                    .map(|(name, value)| (name, Box::into_raw(Box::new(value)) as *mut c_void)),
                Some(destroy_boxed::<f64>),
            );
            (box_client_quota_entity(entity) as *mut c_void, quotas as *mut c_void)
        })
        .collect();
    box_map(
        entries,
        Some(destroy_client_quota_entity_element),
        Some(destroy_map_element),
        Some(client_quota_entity_key_eq),
    )
}

/// Reads the C shape built by [`box_entities`] back; null reads as empty.
///
/// # Safety
///
/// `map` must be null or a map of client-quota-entity handles to maps of
/// NUL-terminated strings to `double *`.
unsafe fn entities_of(map: *const kafka_Map_t) -> Entities {
    unsafe { map_entries(map) }
        .iter()
        .map(|&(entity, quotas)| {
            let entity = unsafe { client_quota_entity_ref(entity as *const kafka_common_quota_ClientQuotaEntity_t) };
            let quotas = unsafe { map_entries(quotas as *const kafka_Map_t) }
                .iter()
                .map(|&(name, value)| {
                    (unsafe { c_str_to_string(name as *const c_char) }, unsafe {
                        *(value as *const f64)
                    })
                })
                .collect();
            (entity.clone(), quotas)
        })
        .collect()
}

/// Hands `result` to C, binding the future it exposes to `ctx`.
pub(crate) fn box_describe_client_quotas_result(
    result: DescribeClientQuotasResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_DescribeClientQuotasResult_t {
    let entities = ctx.handle_future(
        result.entities(),
        |entities| box_entities(entities) as *mut c_void,
        destroy_map_element,
    );
    box_result(DescribeClientQuotasResultInner { entities }, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle.
unsafe fn handle<'a>(
    self_: *const kafka_admin_DescribeClientQuotasResult_t,
) -> &'a ResultHandle<DescribeClientQuotasResultInner> {
    unsafe { result_ref(self_) }
}

/// `new DescribeClientQuotasResult(KafkaFuture<Map<ClientQuotaEntity, Map<String, Double>>> entities)`:
/// `entities` is a borrowed future whose value has the shape
/// [`kafka_admin_DescribeClientQuotasResult_entities`] documents; the value
/// is copied during the call and the caller keeps ownership of the future
/// and of everything the value points at. Only a future that is already
/// complete (`kafka_common_KafkaFuture_is_done`) can be copied across the
/// boundary: its outcome is taken as it stands, and a future still pending
/// stands as one failed with the translation of `IllegalArgumentException`.
/// Owned, freed with [`kafka_admin_DescribeClientQuotasResult_destroy`].
///
/// # Safety
///
/// `entities` must be a valid future handle whose value, when it has one,
/// is null or a map of the documented shape.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClientQuotasResult_new(
    entities: *const kafka_common_KafkaFuture_t,
) -> *mut kafka_admin_DescribeClientQuotasResult_t {
    let result: Result<Entities, Error> = if unsafe { kafka_common_KafkaFuture_is_done(entities) } == 0 {
        Err(Error::local_illegal_argument(
            "The future passed to DescribeClientQuotasResult must already be complete",
        ))
    } else {
        let mut value: *mut c_void = ptr::null_mut();
        let error = unsafe { kafka_common_KafkaFuture_get(entities, &mut value) };
        match unsafe { take_error(error) } {
            Some(error) => Err(error),
            None => Ok(unsafe { entities_of(value as *const kafka_Map_t) }),
        }
    };
    box_describe_client_quotas_result(
        DescribeClientQuotasResult::new(KafkaFuture::completed_future(result)),
        &FutureCtx::detached(),
    )
}

/// `DescribeClientQuotasResult.entities()`: the future of the matching
/// quotas, borrowed from the result handle (valid until it is destroyed,
/// never passed to `kafka_common_KafkaFuture_destroy`). Its `get` delivers
/// a `kafka_Map_t *` owned by the future: `kafka_common_quota_ClientQuotaEntity_t *`
/// keys (sorted by entry, compared by value in `kafka_Map_get`) to
/// `kafka_Map_t *` values of `char *` quota names (sorted) to `double *`
/// quota values.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClientQuotasResult_entities(
    self_: *const kafka_admin_DescribeClientQuotasResult_t,
) -> *const kafka_common_KafkaFuture_t {
    unsafe { handle(self_) }.result.entities
}

/// Frees a result handle and the future borrowed from it; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeClientQuotasResult_destroy(
    self_: *mut kafka_admin_DescribeClientQuotasResult_t,
) {
    unsafe { destroy_result::<DescribeClientQuotasResultInner, _>(self_) }
}
