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

//! `kafka_common_quota_ClientQuotaAlteration_t` and its nested
//! `kafka_common_quota_ClientQuotaAlteration_Op_t`:
//! `org.apache.kafka.common.quota.ClientQuotaAlteration` and
//! `ClientQuotaAlteration.Op` (CLAUDE.md §4 rule 7).

use std::ffi::{CString, c_char, c_void};

use crate::common::quota::{ClientQuotaAlteration, Op};
use crate::ffi::common::quota::client_quota_entity::{
    client_quota_entity_ptr, client_quota_entity_ref, kafka_common_quota_ClientQuotaEntity_t,
};
use crate::ffi::util::{
    box_list, c_str_to_string, destroy_boxed, into_c_string, kafka_List_t, list_elements, owned_c_string,
};

/// Opaque handle to a [`ClientQuotaAlteration`].
///
/// Points at the alteration itself: the entity getter borrows the entity's own
/// handle and the other getters return owned values.
#[repr(C)]
pub struct kafka_common_quota_ClientQuotaAlteration_t {
    _private: [u8; 0],
}

/// Opaque handle to a [`ClientQuotaAlteration.Op`](Op).
#[repr(C)]
pub struct kafka_common_quota_ClientQuotaAlteration_Op_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_quota_ClientQuotaAlteration_Op_t`] points at: the op
/// plus the NUL-terminated key its getter borrows out.
pub(crate) struct OpInner {
    op: Op,
    key_c: CString,
}

impl OpInner {
    pub(crate) fn new(op: Op) -> Self {
        let key_c = owned_c_string(op.key());
        Self { op, key_c }
    }
}

unsafe fn op_inner_ref<'a>(op: *const kafka_common_quota_ClientQuotaAlteration_Op_t) -> &'a OpInner {
    unsafe { &*(op as *const OpInner) }
}

/// The op behind a handle.
///
/// # Safety
///
/// `op` must be a valid op handle.
pub(crate) unsafe fn op_ref<'a>(op: *const kafka_common_quota_ClientQuotaAlteration_Op_t) -> &'a Op {
    &unsafe { op_inner_ref(op) }.op
}

/// Hands `op` to C as an owned handle, freed with
/// [`kafka_common_quota_ClientQuotaAlteration_Op_destroy`].
pub(crate) fn box_op(op: Op) -> *mut kafka_common_quota_ClientQuotaAlteration_Op_t {
    Box::into_raw(Box::new(OpInner::new(op))) as *mut kafka_common_quota_ClientQuotaAlteration_Op_t
}

/// The alteration behind a handle.
///
/// # Safety
///
/// `alteration` must be a valid alteration handle.
pub(crate) unsafe fn client_quota_alteration_ref<'a>(
    alteration: *const kafka_common_quota_ClientQuotaAlteration_t,
) -> &'a ClientQuotaAlteration {
    unsafe { &*(alteration as *const ClientQuotaAlteration) }
}

/// Hands `alteration` to C as an owned handle, freed with
/// [`kafka_common_quota_ClientQuotaAlteration_destroy`].
pub(crate) fn box_client_quota_alteration(
    alteration: ClientQuotaAlteration,
) -> *mut kafka_common_quota_ClientQuotaAlteration_t {
    Box::into_raw(Box::new(alteration)) as *mut kafka_common_quota_ClientQuotaAlteration_t
}

/// `new Op(String key, Double value)`: `value` is the quota to set, or `NAN`
/// for Java's `null`, which removes the quota. Owned, freed with
/// [`kafka_common_quota_ClientQuotaAlteration_Op_destroy`]; `key` is copied.
///
/// # Safety
///
/// `key` must be a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaAlteration_Op_new(
    key: *const c_char,
    value: f64,
) -> *mut kafka_common_quota_ClientQuotaAlteration_Op_t {
    box_op(Op::new(unsafe { c_str_to_string(key) }, (!value.is_nan()).then_some(value)))
}

/// `key()`: the quota type, borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid op handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaAlteration_Op_key(
    self_: *const kafka_common_quota_ClientQuotaAlteration_Op_t,
) -> *const c_char {
    unsafe { op_inner_ref(self_) }.key_c.as_ptr()
}

/// `value()`: the quota to set, or `NAN` for Java's `null` (remove).
///
/// # Safety
///
/// `self_` must be a valid op handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaAlteration_Op_value(
    self_: *const kafka_common_quota_ClientQuotaAlteration_Op_t,
) -> f64 {
    unsafe { op_ref(self_) }.value().unwrap_or(f64::NAN)
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid op handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaAlteration_Op_to_string(
    self_: *const kafka_common_quota_ClientQuotaAlteration_Op_t,
) -> *mut c_char {
    into_c_string(&unsafe { op_ref(self_) }.to_string())
}

/// Frees an owned op handle. Null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned op handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaAlteration_Op_destroy(
    self_: *mut kafka_common_quota_ClientQuotaAlteration_Op_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut OpInner) });
    }
}

/// `new ClientQuotaAlteration(ClientQuotaEntity entity, Collection<Op> ops)`:
/// `entity` and the borrowed `kafka_common_quota_ClientQuotaAlteration_Op_t`
/// handles in `ops` are copied during the call. Owned, freed with
/// [`kafka_common_quota_ClientQuotaAlteration_destroy`].
///
/// # Safety
///
/// `entity` must be a valid entity handle and `ops` null or a valid list of
/// op handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaAlteration_new(
    entity: *const kafka_common_quota_ClientQuotaEntity_t,
    ops: *const kafka_List_t,
) -> *mut kafka_common_quota_ClientQuotaAlteration_t {
    let ops = unsafe { list_elements(ops) }
        .iter()
        .map(|&op| unsafe { op_ref(op as *const kafka_common_quota_ClientQuotaAlteration_Op_t) }.clone())
        .collect();
    box_client_quota_alteration(ClientQuotaAlteration::new(
        unsafe { client_quota_entity_ref(entity) }.clone(),
        ops,
    ))
}

/// `entity()`: borrowed from the handle; never destroyed.
///
/// # Safety
///
/// `self_` must be a valid alteration handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaAlteration_entity(
    self_: *const kafka_common_quota_ClientQuotaAlteration_t,
) -> *const kafka_common_quota_ClientQuotaEntity_t {
    client_quota_entity_ptr(unsafe { client_quota_alteration_ref(self_) }.entity())
}

/// `ops()`: an owned list of owned
/// `kafka_common_quota_ClientQuotaAlteration_Op_t` copies, in the
/// alteration's order. Freed with `kafka_List_destroy`, which also frees the
/// ops.
///
/// # Safety
///
/// `self_` must be a valid alteration handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaAlteration_ops(
    self_: *const kafka_common_quota_ClientQuotaAlteration_t,
) -> *mut kafka_List_t {
    let elements = unsafe { client_quota_alteration_ref(self_) }
        .ops()
        .iter()
        .map(|op| box_op(op.clone()) as *mut c_void)
        .collect();
    box_list(elements, Some(destroy_boxed::<OpInner>))
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid alteration handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaAlteration_to_string(
    self_: *const kafka_common_quota_ClientQuotaAlteration_t,
) -> *mut c_char {
    into_c_string(&unsafe { client_quota_alteration_ref(self_) }.to_string())
}

/// Frees an owned alteration handle. Null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned alteration handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaAlteration_destroy(
    self_: *mut kafka_common_quota_ClientQuotaAlteration_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut ClientQuotaAlteration) });
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::common::quota::ClientQuotaEntity;
    use crate::ffi::common::quota::client_quota_entity::box_client_quota_entity;
    use crate::ffi::common::quota::client_quota_entity::kafka_common_quota_ClientQuotaEntity_destroy;
    use crate::ffi::util::{
        kafka_List_add, kafka_List_destroy, kafka_List_get, kafka_List_new, kafka_List_size, kafka_string_destroy,
    };

    #[test]
    fn op_keeps_nan_apart_from_a_value() {
        let key = CString::new("producer_byte_rate").unwrap();
        unsafe {
            let set = kafka_common_quota_ClientQuotaAlteration_Op_new(key.as_ptr(), 1024.0);
            assert_eq!(*op_ref(set), Op::new("producer_byte_rate", Some(1024.0)));
            assert_eq!(
                CStr::from_ptr(kafka_common_quota_ClientQuotaAlteration_Op_key(set)).to_str(),
                Ok("producer_byte_rate")
            );
            assert_eq!(kafka_common_quota_ClientQuotaAlteration_Op_value(set), 1024.0);
            let s = kafka_common_quota_ClientQuotaAlteration_Op_to_string(set);
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                Op::new("producer_byte_rate", Some(1024.0)).to_string()
            );
            kafka_string_destroy(s);
            kafka_common_quota_ClientQuotaAlteration_Op_destroy(set);

            // NAN is Java's null: the op removes the quota.
            let remove = kafka_common_quota_ClientQuotaAlteration_Op_new(key.as_ptr(), f64::NAN);
            assert_eq!(*op_ref(remove), Op::new("producer_byte_rate", None));
            assert!(kafka_common_quota_ClientQuotaAlteration_Op_value(remove).is_nan());
            kafka_common_quota_ClientQuotaAlteration_Op_destroy(remove);
            kafka_common_quota_ClientQuotaAlteration_Op_destroy(ptr::null_mut());
        }
    }

    #[test]
    fn alteration_copies_its_inputs_and_borrows_its_entity() {
        let rate = CString::new("producer_byte_rate").unwrap();
        let consumer = CString::new("consumer_byte_rate").unwrap();
        let entity = ClientQuotaEntity::new(HashMap::from([("user".to_string(), Some("alice".to_string()))]));
        let expected = ClientQuotaAlteration::new(
            entity.clone(),
            vec![
                Op::new("producer_byte_rate", Some(1.0)),
                Op::new("consumer_byte_rate", None),
            ],
        );
        unsafe {
            let entity_handle = box_client_quota_entity(entity.clone());
            let op0 = kafka_common_quota_ClientQuotaAlteration_Op_new(rate.as_ptr(), 1.0);
            let op1 = kafka_common_quota_ClientQuotaAlteration_Op_new(consumer.as_ptr(), f64::NAN);
            let ops = kafka_List_new();
            kafka_List_add(ops, op0 as *mut c_void);
            kafka_List_add(ops, op1 as *mut c_void);
            let alteration = kafka_common_quota_ClientQuotaAlteration_new(entity_handle, ops);
            // Everything was copied: the inputs go first.
            kafka_List_destroy(ops);
            kafka_common_quota_ClientQuotaAlteration_Op_destroy(op0);
            kafka_common_quota_ClientQuotaAlteration_Op_destroy(op1);
            kafka_common_quota_ClientQuotaEntity_destroy(entity_handle);

            assert_eq!(*client_quota_alteration_ref(alteration), expected);
            assert_eq!(
                *client_quota_entity_ref(kafka_common_quota_ClientQuotaAlteration_entity(alteration)),
                entity
            );
            let copies = kafka_common_quota_ClientQuotaAlteration_ops(alteration);
            assert_eq!(kafka_List_size(copies), 2);
            assert_eq!(
                *op_ref(kafka_List_get(copies, 1) as *const kafka_common_quota_ClientQuotaAlteration_Op_t),
                Op::new("consumer_byte_rate", None)
            );
            kafka_List_destroy(copies);
            let s = kafka_common_quota_ClientQuotaAlteration_to_string(alteration);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), expected.to_string());
            kafka_string_destroy(s);
            kafka_common_quota_ClientQuotaAlteration_destroy(alteration);
            kafka_common_quota_ClientQuotaAlteration_destroy(ptr::null_mut());
        }
    }
}
