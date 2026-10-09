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

//! `kafka_admin_ListConsumerGroupOffsetsOptions_t`: `org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions`
//! (CLAUDE.md §4).
//!
//! Rust's fluent setters take `self` by value and return it; C mutates the
//! handle in place, so `set_x(self, x)` on the handle is Java's
//! `options.x(x)` with the result stored back.

use crate::admin::ListConsumerGroupOffsetsOptions;

/// Opaque handle to a [`ListConsumerGroupOffsetsOptions`], owned by the caller and freed with
/// [`kafka_admin_ListConsumerGroupOffsetsOptions_destroy`].
#[repr(C)]
pub struct kafka_admin_ListConsumerGroupOffsetsOptions_t {
    _private: [u8; 0],
}

/// The options behind a handle.
///
/// # Safety
///
/// `options` must be a live handle.
pub(crate) unsafe fn list_consumer_group_offsets_options_ref<'a>(
    options: *const kafka_admin_ListConsumerGroupOffsetsOptions_t,
) -> &'a ListConsumerGroupOffsetsOptions {
    unsafe { &*(options as *const ListConsumerGroupOffsetsOptions) }
}

/// Mutable access to the options behind a handle, for the in-place setters.
///
/// # Safety
///
/// `options` must be a live handle and no other reference to it may be live.
unsafe fn options_mut<'a>(
    options: *mut kafka_admin_ListConsumerGroupOffsetsOptions_t,
) -> &'a mut ListConsumerGroupOffsetsOptions {
    unsafe { &mut *(options as *mut ListConsumerGroupOffsetsOptions) }
}

fn boxed(options: ListConsumerGroupOffsetsOptions) -> *mut kafka_admin_ListConsumerGroupOffsetsOptions_t {
    Box::into_raw(Box::new(options)) as *mut kafka_admin_ListConsumerGroupOffsetsOptions_t
}

/// `new ListConsumerGroupOffsetsOptions()`: the defaults, with the client's default API timeout.
/// Owned, freed with [`kafka_admin_ListConsumerGroupOffsetsOptions_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ListConsumerGroupOffsetsOptions_new() -> *mut kafka_admin_ListConsumerGroupOffsetsOptions_t
{
    boxed(ListConsumerGroupOffsetsOptions::new())
}

/// `AbstractOptions.timeoutMs(Integer timeoutMs)`: `-1` (any negative value)
/// stands for Java's `null`, the client's default API timeout.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConsumerGroupOffsetsOptions_set_timeout_ms(
    self_: *mut kafka_admin_ListConsumerGroupOffsetsOptions_t,
    timeout_ms: i32,
) {
    let options = unsafe { options_mut(self_) };
    *options = std::mem::take(options).set_timeout_ms((timeout_ms >= 0).then_some(timeout_ms));
}

/// `AbstractOptions.timeoutMs()`: the timeout in milliseconds, `-1` when the
/// client's default API timeout applies.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConsumerGroupOffsetsOptions_timeout_ms(
    self_: *const kafka_admin_ListConsumerGroupOffsetsOptions_t,
) -> i32 {
    unsafe { list_consumer_group_offsets_options_ref(self_) }
        .timeout_ms()
        .unwrap_or(-1)
}

/// `ListConsumerGroupOffsetsOptions.requireStable(boolean requireStable)`: whether the broker must return stable offsets, retrying while a transaction is in progress.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConsumerGroupOffsetsOptions_set_require_stable(
    self_: *mut kafka_admin_ListConsumerGroupOffsetsOptions_t,
    require_stable: i8,
) {
    let options = unsafe { options_mut(self_) };
    *options = std::mem::take(options).set_require_stable(require_stable != 0);
}

/// `ListConsumerGroupOffsetsOptions.requireStable()`: whether the broker must return stable offsets.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConsumerGroupOffsetsOptions_require_stable(
    self_: *const kafka_admin_ListConsumerGroupOffsetsOptions_t,
) -> i8 {
    i8::from(unsafe { list_consumer_group_offsets_options_ref(self_) }.require_stable())
}

/// Frees a handle returned by this module; a no-op on `NULL`.
///
/// # Safety
///
/// `self_` must be `NULL` or a handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConsumerGroupOffsetsOptions_destroy(
    self_: *mut kafka_admin_ListConsumerGroupOffsetsOptions_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut ListConsumerGroupOffsetsOptions) });
    }
}
