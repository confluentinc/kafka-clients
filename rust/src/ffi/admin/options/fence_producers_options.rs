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

//! `kafka_admin_FenceProducersOptions_t`: `org.apache.kafka.clients.admin.FenceProducersOptions`
//! (CLAUDE.md §4).
//!
//! Rust's fluent setters take `self` by value and return it; C mutates the
//! handle in place, so `set_x(self, x)` on the handle is Java's
//! `options.x(x)` with the result stored back.

use std::ffi::c_char;

use crate::admin::FenceProducersOptions;
use crate::ffi::ffi_guard;
use crate::ffi::util::into_c_string;

/// Opaque handle to a [`FenceProducersOptions`], owned by the caller and freed with
/// [`kafka_admin_FenceProducersOptions_destroy`].
#[repr(C)]
pub struct kafka_admin_FenceProducersOptions_t {
    _private: [u8; 0],
}

/// The options behind a handle.
///
/// # Safety
///
/// `options` must be a live handle, kept alive for `'a`, during which no
/// setter runs on it.
pub(crate) unsafe fn fence_producers_options_ref<'a>(
    options: *const kafka_admin_FenceProducersOptions_t,
) -> &'a FenceProducersOptions {
    // SAFETY: Per this function's `# Safety`, `options` is a live handle kept alive for `'a`,
    // during which no setter runs on it. Every `kafka_admin_FenceProducersOptions_t` is the
    // `Box::into_raw` of a `Box<FenceProducersOptions>` (`boxed`), so the shared reference is to a
    // live `FenceProducersOptions` that nothing mutates while it is used.
    unsafe { &*(options as *const FenceProducersOptions) }
}

/// Mutable access to the options behind a handle, for the in-place setters.
///
/// # Safety
///
/// `options` must be a live handle, kept alive for `'a`, to which no other
/// reference is live meanwhile.
unsafe fn options_mut<'a>(options: *mut kafka_admin_FenceProducersOptions_t) -> &'a mut FenceProducersOptions {
    // SAFETY: Per this function's `# Safety`, `options` is a live handle kept alive for `'a`, to
    // which no other reference is live meanwhile. Every `kafka_admin_FenceProducersOptions_t` is
    // the `Box::into_raw` of a `Box<FenceProducersOptions>` (`boxed`), so the exclusive reference
    // is to a live `FenceProducersOptions`.
    unsafe { &mut *(options as *mut FenceProducersOptions) }
}

fn boxed(options: FenceProducersOptions) -> *mut kafka_admin_FenceProducersOptions_t {
    Box::into_raw(Box::new(options)) as *mut kafka_admin_FenceProducersOptions_t
}

/// `new FenceProducersOptions()`: the defaults, with the client's default API timeout.
///
/// # Safety
///
/// The returned handle is owned by the caller, freed with
/// `kafka_admin_FenceProducersOptions_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_FenceProducersOptions_new() -> *mut kafka_admin_FenceProducersOptions_t {
    boxed(FenceProducersOptions::new())
}

/// Set the timeout in milliseconds for this operation or `-1` if the default API timeout for the
/// AdminClient should be used.
///
/// # Parameters
///
/// - `timeout_ms` - the timeout in milliseconds; any negative value stands for Java's `null`, the
///   default API timeout.
///
/// # Safety
///
/// `self_` must be a live handle that no other call uses at the same time.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FenceProducersOptions_set_timeout_ms(
    self_: *mut kafka_admin_FenceProducersOptions_t,
    timeout_ms: i32,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle that no other call uses at
    // the same time, so no other reference to it is live while `options` is used, as `options_mut`
    // requires.
    let options = unsafe { options_mut(self_) };
    *options = std::mem::take(options).set_timeout_ms((timeout_ms >= 0).then_some(timeout_ms));
}

/// The timeout in milliseconds for this operation or `-1` if the default API timeout for the
/// AdminClient should be used.
///
/// # Returns
///
/// The timeout in milliseconds, or `-1` for the default API timeout.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FenceProducersOptions_timeout_ms(
    self_: *const kafka_admin_FenceProducersOptions_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call;
    // no setter runs on it meanwhile, since the setters' `# Safety` excludes any other call using
    // the handle at the same time. That is what `fence_producers_options_ref` requires.
    unsafe { fence_producers_options_ref(self_) }.timeout_ms().unwrap_or(-1)
}

/// Returns the options' `toString()` form.
///
/// # Safety
///
/// `self_` must be a live handle. The string is owned by the caller, freed with
/// `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FenceProducersOptions_to_string(
    self_: *const kafka_admin_FenceProducersOptions_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call;
    // no setter runs on it meanwhile, since the setters' `# Safety` excludes any other call using
    // the handle at the same time. That is what `fence_producers_options_ref` requires.
    into_c_string(&unsafe { fence_producers_options_ref(self_) }.to_string())
}

/// Frees a handle returned by this module; a no-op on `NULL`.
///
/// # Safety
///
/// `self_` must be `NULL` or a handle not yet destroyed, that no other call
/// uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FenceProducersOptions_destroy(self_: *mut kafka_admin_FenceProducersOptions_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, a handle
        // not yet destroyed, that no other call uses at the same time and that is not used
        // afterwards. Every `kafka_admin_FenceProducersOptions_t` is the `Box::into_raw` of a
        // `Box<FenceProducersOptions>` (`boxed`), so `from_raw` reclaims that allocation, once.
        drop(unsafe { Box::from_raw(self_ as *mut FenceProducersOptions) });
    }
}
