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

//! `kafka_admin_DescribeFeaturesOptions_t`: `org.apache.kafka.clients.admin.DescribeFeaturesOptions`
//! (CLAUDE.md §4).
//!
//! Rust's fluent setters take `self` by value and return it; C mutates the
//! handle in place, so `set_x(self, x)` on the handle is Java's
//! `options.x(x)` with the result stored back.

use crate::admin::DescribeFeaturesOptions;

/// Opaque handle to a [`DescribeFeaturesOptions`], owned by the caller and freed with
/// [`kafka_admin_DescribeFeaturesOptions_destroy`].
#[repr(C)]
pub struct kafka_admin_DescribeFeaturesOptions_t {
    _private: [u8; 0],
}

/// The options behind a handle.
///
/// # Safety
///
/// `options` must be a live handle.
pub(crate) unsafe fn describe_features_options_ref<'a>(
    options: *const kafka_admin_DescribeFeaturesOptions_t,
) -> &'a DescribeFeaturesOptions {
    unsafe { &*(options as *const DescribeFeaturesOptions) }
}

/// Mutable access to the options behind a handle, for the in-place setters.
///
/// # Safety
///
/// `options` must be a live handle and no other reference to it may be live.
unsafe fn options_mut<'a>(options: *mut kafka_admin_DescribeFeaturesOptions_t) -> &'a mut DescribeFeaturesOptions {
    unsafe { &mut *(options as *mut DescribeFeaturesOptions) }
}

fn boxed(options: DescribeFeaturesOptions) -> *mut kafka_admin_DescribeFeaturesOptions_t {
    Box::into_raw(Box::new(options)) as *mut kafka_admin_DescribeFeaturesOptions_t
}

/// `new DescribeFeaturesOptions()`: the defaults, with the client's default API timeout.
/// Owned, freed with [`kafka_admin_DescribeFeaturesOptions_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_DescribeFeaturesOptions_new() -> *mut kafka_admin_DescribeFeaturesOptions_t {
    boxed(DescribeFeaturesOptions::new())
}

/// `AbstractOptions.timeoutMs(Integer timeoutMs)`: `-1` (any negative value)
/// stands for Java's `null`, the client's default API timeout.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeFeaturesOptions_set_timeout_ms(
    self_: *mut kafka_admin_DescribeFeaturesOptions_t,
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
pub unsafe extern "C" fn kafka_admin_DescribeFeaturesOptions_timeout_ms(
    self_: *const kafka_admin_DescribeFeaturesOptions_t,
) -> i32 {
    unsafe { describe_features_options_ref(self_) }.timeout_ms().unwrap_or(-1)
}

/// `DescribeFeaturesOptions.nodeId(int nodeId)`: the node the request is sent to (Java's `Optional` is always present once set).
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeFeaturesOptions_set_node_id(
    self_: *mut kafka_admin_DescribeFeaturesOptions_t,
    node_id: i32,
) {
    let options = unsafe { options_mut(self_) };
    *options = std::mem::take(options).set_node_id(node_id);
}

/// `DescribeFeaturesOptions.nodeId()`: the node the request is sent to, `-1` when unset (Java's empty `Optional`: an arbitrary node).
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeFeaturesOptions_node_id(
    self_: *const kafka_admin_DescribeFeaturesOptions_t,
) -> i32 {
    unsafe { describe_features_options_ref(self_) }.node_id().unwrap_or(-1)
}

/// Frees a handle returned by this module; a no-op on `NULL`.
///
/// # Safety
///
/// `self_` must be `NULL` or a handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeFeaturesOptions_destroy(
    self_: *mut kafka_admin_DescribeFeaturesOptions_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut DescribeFeaturesOptions) });
    }
}
