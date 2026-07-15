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

//! Consumer-generic FFI marshaling shared across consumer FFI surfaces.
//!
//! Both the share consumer and a future regular-consumer FFI hand back the same
//! Rust types — a zero-copy [`ConsumerRecords`](crate::consumer::ConsumerRecords)
//! batch and small collection results such as the topic-subscription set. Their
//! C marshaling lives here so it is written once and reused verbatim, keeping
//! the per-surface files (`share_consumer.rs`, and later `consumer.rs`) limited
//! to surface-specific entry points.
//!
//! The opaque types keep the `kafka_consumer_` prefix because the underlying
//! Rust types belong to `org.apache.kafka.clients.consumer`.

// FFI function names follow the kafka_<TypeName>_<method> convention with
// PascalCase type names, which intentionally differs from Rust's snake_case.
#![allow(non_snake_case, non_camel_case_types)]

use std::ffi::{CString, c_char};

// ---------------------------------------------------------------------------
// StringList — a Set<String> result (e.g. the topic subscription)
// ---------------------------------------------------------------------------

/// Opaque handle to a `Set<String>` result (owns cached, NUL-terminated
/// strings).
#[repr(C)]
pub struct kafka_consumer_StringList_t {
    _private: [u8; 0],
}

/// Owns the result strings as cached `CString`s for stable indexed access.
struct StringListInner {
    items: Vec<CString>,
}

/// Boxes an iterator of strings into an opaque string-list handle. Any interior
/// NUL byte truncates that entry (an empty string on failure), which cannot
/// happen for the topic names this carries.
pub(crate) fn box_string_list(strings: impl IntoIterator<Item = String>) -> *mut kafka_consumer_StringList_t {
    let items = strings
        .into_iter()
        .map(|s| CString::new(s.as_bytes()).unwrap_or_default())
        .collect();
    Box::into_raw(Box::new(StringListInner { items })) as *mut kafka_consumer_StringList_t
}

/// Returns the number of strings.
///
/// # Safety
///
/// `list` must be a valid string-list handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_StringList_count(list: *const kafka_consumer_StringList_t) -> i32 {
    if list.is_null() {
        return 0;
    }
    unsafe { &*(list as *const StringListInner) }.items.len() as i32
}

/// Returns the string at `index` as a NUL-terminated C string (owned by the
/// handle, valid until it is destroyed), or null if out of range.
///
/// # Safety
///
/// `list` must be a valid string-list handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_StringList_get(
    list: *const kafka_consumer_StringList_t,
    index: i32,
) -> *const c_char {
    if list.is_null() || index < 0 {
        return std::ptr::null();
    }
    match unsafe { &*(list as *const StringListInner) }.items.get(index as usize) {
        Some(s) => s.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Destroys a string-list handle. Safe with null (no-op).
///
/// # Safety
///
/// `list` must be null or a valid string-list handle. After this call the
/// pointer (and any string pointers obtained from it) are invalid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_StringList_destroy(list: *mut kafka_consumer_StringList_t) {
    if !list.is_null() {
        unsafe { drop(Box::from_raw(list as *mut StringListInner)) };
    }
}
