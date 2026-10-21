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

//! `kafka_consumer_CloseOptions_t`:
//! `org.apache.kafka.clients.consumer.CloseOptions`, with its nested enum
//! `kafka_consumer_CloseOptions_GroupMembershipOperation_t` (CLAUDE.md §4,
//! "Nested types" and "Enums").
//!
//! Rust's fluent setters take `self` by value and return it; C mutates the
//! handle in place, so `with_timeout(self, ms)` on the handle is Java's
//! `options.withTimeout(timeout)` with the result stored back.

#![expect(non_camel_case_types)]

use crate::ffi::util::into_c_string;
use std::ffi::c_char;
use std::time::Duration;

use crate::consumer::{CloseOptions, GroupMembershipOperation};
use crate::ffi::ffi_guard;

/// Opaque handle to a [`CloseOptions`].
#[repr(C)]
pub struct kafka_consumer_CloseOptions_t {
    _private: [u8; 0],
}

/// Opaque handle to a [`GroupMembershipOperation`] singleton.
#[repr(C)]
pub struct kafka_consumer_CloseOptions_GroupMembershipOperation_t {
    _private: [u8; 0],
}

/// The values of [`GroupMembershipOperation`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_consumer_CloseOptions_GroupMembershipOperation_e {
    kafka_consumer_CloseOptions_GroupMembershipOperation_LEAVE_GROUP,
    kafka_consumer_CloseOptions_GroupMembershipOperation_REMAIN_IN_GROUP,
    kafka_consumer_CloseOptions_GroupMembershipOperation_DEFAULT,
}

/// One static instance per value, indexed by
/// [`kafka_consumer_CloseOptions_GroupMembershipOperation_e`].
static VARIANTS: [GroupMembershipOperation; 3] = [
    GroupMembershipOperation::LeaveGroup,
    GroupMembershipOperation::RemainInGroup,
    GroupMembershipOperation::Default,
];

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn enum_of(operation: GroupMembershipOperation) -> kafka_consumer_CloseOptions_GroupMembershipOperation_e {
    match operation {
        GroupMembershipOperation::LeaveGroup => kafka_consumer_CloseOptions_GroupMembershipOperation_e::kafka_consumer_CloseOptions_GroupMembershipOperation_LEAVE_GROUP,
        GroupMembershipOperation::RemainInGroup => {
            kafka_consumer_CloseOptions_GroupMembershipOperation_e::kafka_consumer_CloseOptions_GroupMembershipOperation_REMAIN_IN_GROUP
        },
        GroupMembershipOperation::Default => kafka_consumer_CloseOptions_GroupMembershipOperation_e::kafka_consumer_CloseOptions_GroupMembershipOperation_DEFAULT,
    }
}

/// The borrowed singleton standing for `operation`.
fn singleton(operation: GroupMembershipOperation) -> *const kafka_consumer_CloseOptions_GroupMembershipOperation_t {
    &VARIANTS[enum_of(operation) as usize] as *const GroupMembershipOperation
        as *const kafka_consumer_CloseOptions_GroupMembershipOperation_t
}

/// The value behind a singleton.
///
/// # Safety
///
/// `operation` must be a singleton returned by this module.
unsafe fn value_of(
    operation: *const kafka_consumer_CloseOptions_GroupMembershipOperation_t,
) -> GroupMembershipOperation {
    // SAFETY: Per this function's `# Safety`, `operation` is a singleton returned by this module,
    // i.e. a pointer to an element of the `static` `VARIANTS` array (`singleton`), so it points at
    // a live, initialised `GroupMembershipOperation`, which is `Copy` and read out here.
    unsafe { *(operation as *const GroupMembershipOperation) }
}

/// The options behind a handle.
///
/// # Safety
///
/// `options` must be a valid close-options handle, kept alive and not modified
/// for `'a`.
pub(crate) unsafe fn close_options_ref<'a>(options: *const kafka_consumer_CloseOptions_t) -> &'a CloseOptions {
    // SAFETY: Per this function's `# Safety`, `options` is a valid close-options handle kept alive
    // and unmodified for `'a`: a `Box<CloseOptions>` from `Box::into_raw` in
    // `kafka_consumer_CloseOptions_new_with_timeout` or
    // `kafka_consumer_CloseOptions_with_operation`, the only constructors, so the shared reference
    // is to a live `CloseOptions`.
    unsafe { &*(options as *const CloseOptions) }
}

/// A Java `Duration` in milliseconds; a negative value is clamped to zero,
/// the way Java's timer treats an already-elapsed deadline.
fn duration_ms(timeout: i64) -> Duration {
    Duration::from_millis(u64::try_from(timeout).unwrap_or(0))
}

/// `GroupMembershipOperation.LEAVE_GROUP`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_consumer_CloseOptions_GroupMembershipOperation_leave_group()
-> *const kafka_consumer_CloseOptions_GroupMembershipOperation_t {
    singleton(GroupMembershipOperation::LeaveGroup)
}

/// `GroupMembershipOperation.REMAIN_IN_GROUP`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_consumer_CloseOptions_GroupMembershipOperation_remain_in_group()
-> *const kafka_consumer_CloseOptions_GroupMembershipOperation_t {
    singleton(GroupMembershipOperation::RemainInGroup)
}

/// `GroupMembershipOperation.DEFAULT`: static members remain in the group,
/// dynamic members leave.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_consumer_CloseOptions_GroupMembershipOperation_default()
-> *const kafka_consumer_CloseOptions_GroupMembershipOperation_t {
    singleton(GroupMembershipOperation::Default)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// A caught Rust panic returns `kafka_consumer_CloseOptions_GroupMembershipOperation_LEAVE_GROUP`,
/// the first enumerator: the enum has no `UNKNOWN` constant, so the panic cannot be told apart from
/// that value, and is only logged.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard(fallback = kafka_consumer_CloseOptions_GroupMembershipOperation_e::kafka_consumer_CloseOptions_GroupMembershipOperation_LEAVE_GROUP)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_CloseOptions_GroupMembershipOperation__enum(
    self_: *const kafka_consumer_CloseOptions_GroupMembershipOperation_t,
) -> kafka_consumer_CloseOptions_GroupMembershipOperation_e {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `value_of` requires; the value is copied out.
    enum_of(unsafe { value_of(self_) })
}

/// `toString()`, Java's inherited `Enum.toString()`: the constant's name, as an owned string freed
/// with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_CloseOptions_GroupMembershipOperation_to_string(
    self_: *const kafka_consumer_CloseOptions_GroupMembershipOperation_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `value_of` requires; the value is copied out and its name copied into an owned string.
    into_c_string(&unsafe { value_of(self_) }.to_string())
}

/// `CloseOptions.timeout(Duration timeout)`, the static factory: an owned
/// handle with `GroupMembershipOperation.DEFAULT`, freed with
/// [`kafka_consumer_CloseOptions_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_consumer_CloseOptions_new_with_timeout(timeout: i64) -> *mut kafka_consumer_CloseOptions_t {
    Box::into_raw(Box::new(CloseOptions::new_with_timeout(duration_ms(timeout)))) as *mut kafka_consumer_CloseOptions_t
}

/// `CloseOptions.groupMembershipOperation(GroupMembershipOperation)`, the
/// static factory: an owned handle with no timeout (Java's default of
/// `Long.MAX_VALUE` milliseconds applies at `close`).
///
/// # Safety
///
/// `operation` must be a singleton returned by this module.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_CloseOptions_with_operation(
    operation: *const kafka_consumer_CloseOptions_GroupMembershipOperation_t,
) -> *mut kafka_consumer_CloseOptions_t {
    // SAFETY: Per this function's `# Safety`, `operation` is a singleton returned by this module,
    // as `value_of` requires; the value is copied out.
    Box::into_raw(Box::new(CloseOptions::with_operation(unsafe { value_of(operation) })))
        as *mut kafka_consumer_CloseOptions_t
}

/// `withTimeout(Duration timeout)`, applied in place.
///
/// # Safety
///
/// `self_` must be a valid handle that no other call uses at the same time.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_CloseOptions_with_timeout(
    self_: *mut kafka_consumer_CloseOptions_t,
    timeout: i64,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, i.e. a `Box<CloseOptions>`
    // from one of the two constructors, and no other call uses it at the same time, so the
    // exclusive reference is sound for this call, its only use.
    let slot = unsafe { &mut *(self_ as *mut CloseOptions) };
    *slot = slot.clone().with_timeout(duration_ms(timeout));
}

/// `withGroupMembershipOperation(GroupMembershipOperation)`, applied in
/// place.
///
/// # Safety
///
/// `self_` must be a valid handle that no other call uses at the same time,
/// and `operation` a singleton returned by this module.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_CloseOptions_with_group_membership_operation(
    self_: *mut kafka_consumer_CloseOptions_t,
    operation: *const kafka_consumer_CloseOptions_GroupMembershipOperation_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, i.e. a `Box<CloseOptions>`
    // from one of the two constructors, and no other call uses it at the same time, so the
    // exclusive reference is sound for this call, its only use.
    let slot = unsafe { &mut *(self_ as *mut CloseOptions) };
    // SAFETY: Per this function's `# Safety`, `operation` is a singleton returned by this module,
    // as `value_of` requires; the value is copied out.
    *slot = slot.clone().with_group_membership_operation(unsafe { value_of(operation) });
}

/// `timeout()`: milliseconds, or `-1` when none was set.
///
/// A caught Rust panic returns -2, which is no close timeout: Java's `close` rejects a negative one.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[ffi_guard(fallback = -2)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_CloseOptions_timeout(self_: *const kafka_consumer_CloseOptions_t) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, and no setter runs on it
    // meanwhile, since the setters' `# Safety` excludes any other call using the handle at the same
    // time, so `close_options_ref`'s requirement holds for this expression, the only use of the
    // reference.
    unsafe { close_options_ref(self_) }
        .timeout()
        .map_or(-1, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// `groupMembershipOperation()`: a borrowed singleton.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_CloseOptions_group_membership_operation(
    self_: *const kafka_consumer_CloseOptions_t,
) -> *const kafka_consumer_CloseOptions_GroupMembershipOperation_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, and no setter runs on it
    // meanwhile, since the setters' `# Safety` excludes any other call using the handle at the same
    // time, so `close_options_ref`'s requirement holds for this expression, the only use of the
    // reference.
    singleton(unsafe { close_options_ref(self_) }.group_membership_operation())
}

/// Frees a handle; a null pointer is a no-op.
///
/// # Safety
///
/// `self_` must be null or a valid handle not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_CloseOptions_destroy(self_: *mut kafka_consumer_CloseOptions_t) {
    if !self_.is_null() {
        // SAFETY: Per this function's `# Safety`, `self_` (non-null, checked above) is a valid
        // handle not used afterwards. Every close-options handle is a `Box<CloseOptions>` from
        // `Box::into_raw` in one of the two constructors, owned by the caller, so taking it back
        // and dropping it frees it exactly once.
        unsafe { drop(Box::from_raw(self_ as *mut CloseOptions)) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factories_and_in_place_setters() {
        let options = kafka_consumer_CloseOptions_new_with_timeout(1500);
        // SAFETY: Test code: every options handle comes from one of the two constructors and is
        // destroyed once, at the end of its block, and every operation handle is a singleton of
        // this module, never freed.
        unsafe {
            assert_eq!(kafka_consumer_CloseOptions_timeout(options), 1500);
            assert_eq!(
                kafka_consumer_CloseOptions_group_membership_operation(options),
                kafka_consumer_CloseOptions_GroupMembershipOperation_default()
            );
            kafka_consumer_CloseOptions_with_group_membership_operation(
                options,
                kafka_consumer_CloseOptions_GroupMembershipOperation_remain_in_group(),
            );
            kafka_consumer_CloseOptions_with_timeout(options, 10);
            assert_eq!(kafka_consumer_CloseOptions_timeout(options), 10);
            assert_eq!(
                close_options_ref(options).group_membership_operation(),
                GroupMembershipOperation::RemainInGroup
            );
            kafka_consumer_CloseOptions_destroy(options);

            let by_op = kafka_consumer_CloseOptions_with_operation(
                kafka_consumer_CloseOptions_GroupMembershipOperation_leave_group(),
            );
            assert_eq!(kafka_consumer_CloseOptions_timeout(by_op), -1);
            assert_eq!(
                kafka_consumer_CloseOptions_GroupMembershipOperation__enum(
                    kafka_consumer_CloseOptions_group_membership_operation(by_op)
                ),
                kafka_consumer_CloseOptions_GroupMembershipOperation_e::kafka_consumer_CloseOptions_GroupMembershipOperation_LEAVE_GROUP
            );
            kafka_consumer_CloseOptions_destroy(by_op);
        }
    }
}
