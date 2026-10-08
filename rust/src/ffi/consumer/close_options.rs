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

use std::time::Duration;

use crate::consumer::{CloseOptions, GroupMembershipOperation};

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
    leave_group,
    remain_in_group,
    default,
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
        GroupMembershipOperation::LeaveGroup => kafka_consumer_CloseOptions_GroupMembershipOperation_e::leave_group,
        GroupMembershipOperation::RemainInGroup => {
            kafka_consumer_CloseOptions_GroupMembershipOperation_e::remain_in_group
        },
        GroupMembershipOperation::Default => kafka_consumer_CloseOptions_GroupMembershipOperation_e::default,
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
    unsafe { *(operation as *const GroupMembershipOperation) }
}

/// The options behind a handle.
///
/// # Safety
///
/// `options` must be a valid close-options handle.
pub(crate) unsafe fn close_options_ref<'a>(options: *const kafka_consumer_CloseOptions_t) -> &'a CloseOptions {
    unsafe { &*(options as *const CloseOptions) }
}

/// A Java `Duration` in milliseconds; a negative value is clamped to zero,
/// the way Java's timer treats an already-elapsed deadline.
fn duration_ms(timeout: i64) -> Duration {
    Duration::from_millis(u64::try_from(timeout).unwrap_or(0))
}

/// `GroupMembershipOperation.LEAVE_GROUP`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_consumer_CloseOptions_GroupMembershipOperation_leave_group()
-> *const kafka_consumer_CloseOptions_GroupMembershipOperation_t {
    singleton(GroupMembershipOperation::LeaveGroup)
}

/// `GroupMembershipOperation.REMAIN_IN_GROUP`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_consumer_CloseOptions_GroupMembershipOperation_remain_in_group()
-> *const kafka_consumer_CloseOptions_GroupMembershipOperation_t {
    singleton(GroupMembershipOperation::RemainInGroup)
}

/// `GroupMembershipOperation.DEFAULT`: static members remain in the group,
/// dynamic members leave.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_consumer_CloseOptions_GroupMembershipOperation_default()
-> *const kafka_consumer_CloseOptions_GroupMembershipOperation_t {
    singleton(GroupMembershipOperation::Default)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_CloseOptions_GroupMembershipOperation__enum(
    self_: *const kafka_consumer_CloseOptions_GroupMembershipOperation_t,
) -> kafka_consumer_CloseOptions_GroupMembershipOperation_e {
    enum_of(unsafe { value_of(self_) })
}

/// `CloseOptions.timeout(Duration timeout)`, the static factory: an owned
/// handle with `GroupMembershipOperation.DEFAULT`, freed with
/// [`kafka_consumer_CloseOptions_destroy`].
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_CloseOptions_with_operation(
    operation: *const kafka_consumer_CloseOptions_GroupMembershipOperation_t,
) -> *mut kafka_consumer_CloseOptions_t {
    Box::into_raw(Box::new(CloseOptions::with_operation(unsafe { value_of(operation) })))
        as *mut kafka_consumer_CloseOptions_t
}

/// `withTimeout(Duration timeout)`, applied in place.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_CloseOptions_with_timeout(
    self_: *mut kafka_consumer_CloseOptions_t,
    timeout: i64,
) {
    let slot = unsafe { &mut *(self_ as *mut CloseOptions) };
    *slot = slot.clone().with_timeout(duration_ms(timeout));
}

/// `withGroupMembershipOperation(GroupMembershipOperation)`, applied in
/// place.
///
/// # Safety
///
/// `self_` must be a valid handle and `operation` a singleton returned by
/// this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_CloseOptions_with_group_membership_operation(
    self_: *mut kafka_consumer_CloseOptions_t,
    operation: *const kafka_consumer_CloseOptions_GroupMembershipOperation_t,
) {
    let slot = unsafe { &mut *(self_ as *mut CloseOptions) };
    *slot = slot.clone().with_group_membership_operation(unsafe { value_of(operation) });
}

/// `timeout()`: milliseconds, or `-1` when none was set.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_CloseOptions_timeout(self_: *const kafka_consumer_CloseOptions_t) -> i64 {
    unsafe { close_options_ref(self_) }
        .timeout()
        .map_or(-1, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// `groupMembershipOperation()`: a borrowed singleton.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_CloseOptions_group_membership_operation(
    self_: *const kafka_consumer_CloseOptions_t,
) -> *const kafka_consumer_CloseOptions_GroupMembershipOperation_t {
    singleton(unsafe { close_options_ref(self_) }.group_membership_operation())
}

/// Frees a handle; a null pointer is a no-op.
///
/// # Safety
///
/// `self_` must be null or a valid handle not used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_CloseOptions_destroy(self_: *mut kafka_consumer_CloseOptions_t) {
    if !self_.is_null() {
        unsafe { drop(Box::from_raw(self_ as *mut CloseOptions)) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factories_and_in_place_setters() {
        let options = kafka_consumer_CloseOptions_new_with_timeout(1500);
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
                kafka_consumer_CloseOptions_GroupMembershipOperation_e::leave_group
            );
            kafka_consumer_CloseOptions_destroy(by_op);
        }
    }
}
