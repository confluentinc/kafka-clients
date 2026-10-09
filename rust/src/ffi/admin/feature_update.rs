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

//! `kafka_admin_FeatureUpdate_t`:
//! `org.apache.kafka.clients.admin.FeatureUpdate` with its nested enum
//! `kafka_admin_FeatureUpdate_UpgradeType_t` (CLAUDE.md §4, "Nested types"
//! and "Enums").

#![expect(non_camel_case_types)]

use std::ffi::c_char;

use crate::admin::{FeatureUpdate, UpgradeType};
use crate::ffi::admin::out_slot;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::util::into_c_string;

// ---------------------------------------------------------------------------
// FeatureUpdate.UpgradeType
// ---------------------------------------------------------------------------

/// Opaque handle to an [`UpgradeType`] singleton.
#[repr(C)]
pub struct kafka_admin_FeatureUpdate_UpgradeType_t {
    _private: [u8; 0],
}

/// The values of [`UpgradeType`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_admin_FeatureUpdate_UpgradeType_e {
    kafka_admin_FeatureUpdate_UpgradeType_UNKNOWN,
    kafka_admin_FeatureUpdate_UpgradeType_UPGRADE,
    kafka_admin_FeatureUpdate_UpgradeType_SAFE_DOWNGRADE,
    kafka_admin_FeatureUpdate_UpgradeType_UNSAFE_DOWNGRADE,
}

/// One static instance per value, indexed by
/// [`kafka_admin_FeatureUpdate_UpgradeType_e`].
static VARIANTS: [UpgradeType; 4] = [
    UpgradeType::Unknown,
    UpgradeType::Upgrade,
    UpgradeType::SafeDowngrade,
    UpgradeType::UnsafeDowngrade,
];

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn enum_of(upgrade_type: UpgradeType) -> kafka_admin_FeatureUpdate_UpgradeType_e {
    match upgrade_type {
        UpgradeType::Unknown => kafka_admin_FeatureUpdate_UpgradeType_e::kafka_admin_FeatureUpdate_UpgradeType_UNKNOWN,
        UpgradeType::Upgrade => kafka_admin_FeatureUpdate_UpgradeType_e::kafka_admin_FeatureUpdate_UpgradeType_UPGRADE,
        UpgradeType::SafeDowngrade => {
            kafka_admin_FeatureUpdate_UpgradeType_e::kafka_admin_FeatureUpdate_UpgradeType_SAFE_DOWNGRADE
        },
        UpgradeType::UnsafeDowngrade => {
            kafka_admin_FeatureUpdate_UpgradeType_e::kafka_admin_FeatureUpdate_UpgradeType_UNSAFE_DOWNGRADE
        },
    }
}

/// The borrowed singleton standing for `upgrade_type`.
pub(crate) fn upgrade_type_singleton(upgrade_type: UpgradeType) -> *const kafka_admin_FeatureUpdate_UpgradeType_t {
    &VARIANTS[enum_of(upgrade_type) as usize] as *const UpgradeType as *const kafka_admin_FeatureUpdate_UpgradeType_t
}

/// The value behind a singleton.
///
/// # Safety
///
/// `upgrade_type` must be a singleton returned by this module.
pub(crate) unsafe fn upgrade_type_value_of(
    upgrade_type: *const kafka_admin_FeatureUpdate_UpgradeType_t,
) -> UpgradeType {
    unsafe { *(upgrade_type as *const UpgradeType) }
}

/// `UpgradeType.UNKNOWN`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_FeatureUpdate_UpgradeType_unknown() -> *const kafka_admin_FeatureUpdate_UpgradeType_t {
    upgrade_type_singleton(UpgradeType::Unknown)
}

/// `UpgradeType.UPGRADE`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_FeatureUpdate_UpgradeType_upgrade() -> *const kafka_admin_FeatureUpdate_UpgradeType_t {
    upgrade_type_singleton(UpgradeType::Upgrade)
}

/// `UpgradeType.SAFE_DOWNGRADE`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_FeatureUpdate_UpgradeType_safe_downgrade()
-> *const kafka_admin_FeatureUpdate_UpgradeType_t {
    upgrade_type_singleton(UpgradeType::SafeDowngrade)
}

/// `UpgradeType.UNSAFE_DOWNGRADE`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_FeatureUpdate_UpgradeType_unsafe_downgrade()
-> *const kafka_admin_FeatureUpdate_UpgradeType_t {
    upgrade_type_singleton(UpgradeType::UnsafeDowngrade)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FeatureUpdate_UpgradeType__enum(
    self_: *const kafka_admin_FeatureUpdate_UpgradeType_t,
) -> kafka_admin_FeatureUpdate_UpgradeType_e {
    enum_of(unsafe { upgrade_type_value_of(self_) })
}

/// `UpgradeType.code()`: the wire-protocol byte of the upgrade type.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FeatureUpdate_UpgradeType_code(
    self_: *const kafka_admin_FeatureUpdate_UpgradeType_t,
) -> i8 {
    unsafe { upgrade_type_value_of(self_) }.code()
}

/// `UpgradeType.fromCode(int code)`: the borrowed singleton with that wire
/// code, `UNKNOWN` for a code Java does not know.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_FeatureUpdate_UpgradeType_from_code(
    code: i32,
) -> *const kafka_admin_FeatureUpdate_UpgradeType_t {
    upgrade_type_singleton(UpgradeType::from_code(code))
}

// ---------------------------------------------------------------------------
// FeatureUpdate
// ---------------------------------------------------------------------------

/// Opaque handle to a [`FeatureUpdate`].
#[repr(C)]
pub struct kafka_admin_FeatureUpdate_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_FeatureUpdate_t`] points at.
pub(crate) struct FeatureUpdateInner {
    update: FeatureUpdate,
}

/// The update behind a handle.
///
/// # Safety
///
/// `update` must be a valid feature-update handle.
pub(crate) unsafe fn feature_update_ref<'a>(update: *const kafka_admin_FeatureUpdate_t) -> &'a FeatureUpdate {
    &unsafe { &*(update as *const FeatureUpdateInner) }.update
}

/// Hands `update` to C as an owned handle, freed with
/// [`kafka_admin_FeatureUpdate_destroy`].
pub(crate) fn box_feature_update(update: FeatureUpdate) -> *mut kafka_admin_FeatureUpdate_t {
    Box::into_raw(Box::new(FeatureUpdateInner { update })) as *mut kafka_admin_FeatureUpdate_t
}

/// `new FeatureUpdate(short maxVersionLevel, UpgradeType upgradeType)`:
/// delivers the owned handle through `out_new` (freed with
/// [`kafka_admin_FeatureUpdate_destroy`]), or returns the owned
/// `IllegalArgumentException` translation when the level is negative or a
/// deletion (level 0) is requested with `UPGRADE`.
///
/// # Safety
///
/// `upgrade_type` must be an upgrade-type singleton and `out_new` a valid
/// slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FeatureUpdate_new(
    max_version_level: i16,
    upgrade_type: *const kafka_admin_FeatureUpdate_UpgradeType_t,
    out_new: *mut *mut kafka_admin_FeatureUpdate_t,
) -> *mut kafka_common_Error_t {
    let result = FeatureUpdate::new(max_version_level, unsafe { upgrade_type_value_of(upgrade_type) });
    unsafe { out_slot(result, out_new, box_feature_update) }
}

/// `maxVersionLevel()`.
///
/// # Safety
///
/// `self_` must be a valid feature-update handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FeatureUpdate_max_version_level(self_: *const kafka_admin_FeatureUpdate_t) -> i16 {
    unsafe { feature_update_ref(self_) }.max_version_level()
}

/// `upgradeType()`: a borrowed singleton.
///
/// # Safety
///
/// `self_` must be a valid feature-update handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FeatureUpdate_upgrade_type(
    self_: *const kafka_admin_FeatureUpdate_t,
) -> *const kafka_admin_FeatureUpdate_UpgradeType_t {
    upgrade_type_singleton(unsafe { feature_update_ref(self_) }.upgrade_type())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid feature-update handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FeatureUpdate_to_string(self_: *const kafka_admin_FeatureUpdate_t) -> *mut c_char {
    into_c_string(&unsafe { feature_update_ref(self_) }.to_string())
}

/// Frees an owned update handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned update handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FeatureUpdate_destroy(self_: *mut kafka_admin_FeatureUpdate_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut FeatureUpdateInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::ffi::common::{error_ref, kafka_common_Error_destroy};
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn upgrade_type_singletons_codes_and_from_code() {
        for (index, &upgrade_type) in VARIANTS.iter().enumerate() {
            let handle = upgrade_type_singleton(upgrade_type);
            unsafe {
                assert_eq!(upgrade_type_value_of(handle), upgrade_type);
                assert_eq!(kafka_admin_FeatureUpdate_UpgradeType__enum(handle) as usize, index);
                assert_eq!(
                    kafka_admin_FeatureUpdate_UpgradeType_from_code(i32::from(
                        kafka_admin_FeatureUpdate_UpgradeType_code(handle)
                    )),
                    handle
                );
            }
        }
        assert_eq!(
            kafka_admin_FeatureUpdate_UpgradeType_safe_downgrade(),
            upgrade_type_singleton(UpgradeType::SafeDowngrade)
        );
        assert_eq!(
            kafka_admin_FeatureUpdate_UpgradeType_from_code(42),
            kafka_admin_FeatureUpdate_UpgradeType_unknown()
        );
    }

    #[test]
    fn new_validates_like_java() {
        let mut update = ptr::null_mut();
        unsafe {
            let error = kafka_admin_FeatureUpdate_new(0, kafka_admin_FeatureUpdate_UpgradeType_upgrade(), &mut update);
            assert!(!error.is_null());
            assert!(error_ref(error).error.is_local_illegal_argument_error());
            kafka_common_Error_destroy(error);

            assert!(
                kafka_admin_FeatureUpdate_new(2, kafka_admin_FeatureUpdate_UpgradeType_upgrade(), &mut update)
                    .is_null()
            );
            let expected = FeatureUpdate::new(2, UpgradeType::Upgrade).unwrap();
            assert_eq!(*feature_update_ref(update), expected);
            assert_eq!(kafka_admin_FeatureUpdate_max_version_level(update), 2);
            assert_eq!(
                kafka_admin_FeatureUpdate_upgrade_type(update),
                kafka_admin_FeatureUpdate_UpgradeType_upgrade()
            );
            let s = kafka_admin_FeatureUpdate_to_string(update);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), expected.to_string());
            kafka_string_destroy(s);
            kafka_admin_FeatureUpdate_destroy(update);
            kafka_admin_FeatureUpdate_destroy(ptr::null_mut());
        }
    }
}
