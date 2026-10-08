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

//! `kafka_admin_AlterConfigOp_t`:
//! `org.apache.kafka.clients.admin.AlterConfigOp` with its nested enum
//! `kafka_admin_AlterConfigOp_OpType_t` (CLAUDE.md §4, "Nested types" and
//! "Enums"). The config-entry getter returns a borrowed handle valid as long
//! as the operation.

#![expect(non_camel_case_types)]

use std::ffi::c_char;

use crate::admin::{AlterConfigOp, OpType};
use crate::ffi::admin::config_entry::{ConfigEntryInner, config_entry_ref, kafka_admin_ConfigEntry_t};
use crate::ffi::util::into_c_string;

// ---------------------------------------------------------------------------
// AlterConfigOp.OpType
// ---------------------------------------------------------------------------

/// Opaque handle to an [`OpType`] singleton.
#[repr(C)]
pub struct kafka_admin_AlterConfigOp_OpType_t {
    _private: [u8; 0],
}

/// The values of [`OpType`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_admin_AlterConfigOp_OpType_e {
    set,
    delete,
    append,
    subtract,
}

/// One static instance per value, indexed by
/// [`kafka_admin_AlterConfigOp_OpType_e`].
static VARIANTS: [OpType; 4] = [OpType::Set, OpType::Delete, OpType::Append, OpType::Subtract];

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn enum_of(op_type: OpType) -> kafka_admin_AlterConfigOp_OpType_e {
    match op_type {
        OpType::Set => kafka_admin_AlterConfigOp_OpType_e::set,
        OpType::Delete => kafka_admin_AlterConfigOp_OpType_e::delete,
        OpType::Append => kafka_admin_AlterConfigOp_OpType_e::append,
        OpType::Subtract => kafka_admin_AlterConfigOp_OpType_e::subtract,
    }
}

/// The borrowed singleton standing for `op_type`.
pub(crate) fn op_type_singleton(op_type: OpType) -> *const kafka_admin_AlterConfigOp_OpType_t {
    &VARIANTS[enum_of(op_type) as usize] as *const OpType as *const kafka_admin_AlterConfigOp_OpType_t
}

/// The value behind a singleton.
///
/// # Safety
///
/// `op_type` must be a singleton returned by this module.
pub(crate) unsafe fn op_type_value_of(op_type: *const kafka_admin_AlterConfigOp_OpType_t) -> OpType {
    unsafe { *(op_type as *const OpType) }
}

/// `OpType.SET`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_AlterConfigOp_OpType_set() -> *const kafka_admin_AlterConfigOp_OpType_t {
    op_type_singleton(OpType::Set)
}

/// `OpType.DELETE`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_AlterConfigOp_OpType_delete() -> *const kafka_admin_AlterConfigOp_OpType_t {
    op_type_singleton(OpType::Delete)
}

/// `OpType.APPEND`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_AlterConfigOp_OpType_append() -> *const kafka_admin_AlterConfigOp_OpType_t {
    op_type_singleton(OpType::Append)
}

/// `OpType.SUBTRACT`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_AlterConfigOp_OpType_subtract() -> *const kafka_admin_AlterConfigOp_OpType_t {
    op_type_singleton(OpType::Subtract)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterConfigOp_OpType__enum(
    self_: *const kafka_admin_AlterConfigOp_OpType_t,
) -> kafka_admin_AlterConfigOp_OpType_e {
    enum_of(unsafe { op_type_value_of(self_) })
}

/// `OpType.id()`: the wire-protocol byte of the operation.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterConfigOp_OpType_id(self_: *const kafka_admin_AlterConfigOp_OpType_t) -> i8 {
    unsafe { op_type_value_of(self_) }.id()
}

/// `OpType.forId(byte id)`: the borrowed singleton with that wire byte, or
/// `NULL` for an id Java does not know (Java returns null).
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_AlterConfigOp_OpType_for_id(id: i8) -> *const kafka_admin_AlterConfigOp_OpType_t {
    OpType::for_id(id).map_or(std::ptr::null(), op_type_singleton)
}

// ---------------------------------------------------------------------------
// AlterConfigOp
// ---------------------------------------------------------------------------

/// Opaque handle to an [`AlterConfigOp`].
#[repr(C)]
pub struct kafka_admin_AlterConfigOp_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_AlterConfigOp_t`] points at: the operation plus the
/// handle of its config entry, which the getter borrows out.
pub(crate) struct AlterConfigOpInner {
    op: AlterConfigOp,
    config_entry: ConfigEntryInner,
}

impl AlterConfigOpInner {
    pub(crate) fn new(op: AlterConfigOp) -> Self {
        let config_entry = ConfigEntryInner::new(op.config_entry().clone());
        Self { op, config_entry }
    }
}

unsafe fn inner_ref<'a>(op: *const kafka_admin_AlterConfigOp_t) -> &'a AlterConfigOpInner {
    unsafe { &*(op as *const AlterConfigOpInner) }
}

/// The operation behind a handle.
///
/// # Safety
///
/// `op` must be a valid alter-config-op handle.
pub(crate) unsafe fn alter_config_op_ref<'a>(op: *const kafka_admin_AlterConfigOp_t) -> &'a AlterConfigOp {
    &unsafe { inner_ref(op) }.op
}

/// Hands `op` to C as an owned handle, freed with
/// [`kafka_admin_AlterConfigOp_destroy`].
pub(crate) fn box_alter_config_op(op: AlterConfigOp) -> *mut kafka_admin_AlterConfigOp_t {
    Box::into_raw(Box::new(AlterConfigOpInner::new(op))) as *mut kafka_admin_AlterConfigOp_t
}

/// `new AlterConfigOp(ConfigEntry configEntry, OpType operationType)`: the
/// entry is copied, the caller keeps its handle. Owned, freed with
/// [`kafka_admin_AlterConfigOp_destroy`].
///
/// # Safety
///
/// `config_entry` must be a valid config-entry handle and `operation_type`
/// an op-type singleton.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterConfigOp_new(
    config_entry: *const kafka_admin_ConfigEntry_t,
    operation_type: *const kafka_admin_AlterConfigOp_OpType_t,
) -> *mut kafka_admin_AlterConfigOp_t {
    box_alter_config_op(AlterConfigOp::new(unsafe { config_entry_ref(config_entry) }.clone(), unsafe {
        op_type_value_of(operation_type)
    }))
}

/// `configEntry()`: a borrowed handle valid as long as the operation, never
/// passed to `kafka_admin_ConfigEntry_destroy`.
///
/// # Safety
///
/// `self_` must be a valid alter-config-op handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterConfigOp_config_entry(
    self_: *const kafka_admin_AlterConfigOp_t,
) -> *const kafka_admin_ConfigEntry_t {
    unsafe { inner_ref(self_) }.config_entry.as_ptr()
}

/// `opType()`: a borrowed singleton.
///
/// # Safety
///
/// `self_` must be a valid alter-config-op handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterConfigOp_op_type(
    self_: *const kafka_admin_AlterConfigOp_t,
) -> *const kafka_admin_AlterConfigOp_OpType_t {
    op_type_singleton(unsafe { alter_config_op_ref(self_) }.op_type())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid alter-config-op handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterConfigOp_to_string(self_: *const kafka_admin_AlterConfigOp_t) -> *mut c_char {
    into_c_string(&unsafe { alter_config_op_ref(self_) }.to_string())
}

/// Frees an owned operation handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned operation handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterConfigOp_destroy(self_: *mut kafka_admin_AlterConfigOp_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut AlterConfigOpInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::admin::ConfigEntry;
    use crate::ffi::admin::config_entry::{
        box_config_entry, kafka_admin_ConfigEntry_destroy, kafka_admin_ConfigEntry_name,
    };
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn op_type_singletons_ids_and_for_id() {
        for (index, &op_type) in VARIANTS.iter().enumerate() {
            let handle = op_type_singleton(op_type);
            unsafe {
                assert_eq!(op_type_value_of(handle), op_type);
                assert_eq!(kafka_admin_AlterConfigOp_OpType__enum(handle) as usize, index);
                assert_eq!(
                    kafka_admin_AlterConfigOp_OpType_for_id(kafka_admin_AlterConfigOp_OpType_id(handle)),
                    handle
                );
            }
        }
        assert_eq!(kafka_admin_AlterConfigOp_OpType_subtract(), op_type_singleton(OpType::Subtract));
        assert!(kafka_admin_AlterConfigOp_OpType_for_id(7).is_null());
    }

    #[test]
    fn op_copies_its_entry_and_borrows_it_out() {
        let entry = ConfigEntry::new("retention.ms".to_string(), Some("1000".to_string()));
        let entry_handle = box_config_entry(entry.clone());
        unsafe {
            let op = kafka_admin_AlterConfigOp_new(entry_handle, kafka_admin_AlterConfigOp_OpType_append());
            kafka_admin_ConfigEntry_destroy(entry_handle);
            let expected = AlterConfigOp::new(entry, OpType::Append);
            assert_eq!(*alter_config_op_ref(op), expected);
            assert_eq!(kafka_admin_AlterConfigOp_op_type(op), kafka_admin_AlterConfigOp_OpType_append());
            assert_eq!(
                CStr::from_ptr(kafka_admin_ConfigEntry_name(kafka_admin_AlterConfigOp_config_entry(op)))
                    .to_str()
                    .unwrap(),
                "retention.ms"
            );
            let s = kafka_admin_AlterConfigOp_to_string(op);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), expected.to_string());
            kafka_string_destroy(s);
            kafka_admin_AlterConfigOp_destroy(op);
            kafka_admin_AlterConfigOp_destroy(ptr::null_mut());
        }
    }
}
