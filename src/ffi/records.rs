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

use crate::common::header::{Header, RecordHeader};
use crate::consumer::{ConsumerRecord, ConsumerRecords};

// Records marshaled through the FFI are monomorphized over refcounted
// `bytes::Bytes` keys/values: each record's key/value is a zero-copy slice of
// the owning fetch buffer. The C side borrows ptr+len; the boxed batch keeps the
// buffer alive.
type Bytes = bytes::Bytes;

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

// ---------------------------------------------------------------------------
// ConsumerRecords + ConsumerRecord marshaling (zero-copy)
//
// The polled batch owns exactly one buffer per record's key/value bytes; every
// C accessor below borrows slices from it, so no key/value/header bytes are
// copied on the receive path. Record pointers (and their byte slices) stay valid
// only until the owning records handle is destroyed.
// ---------------------------------------------------------------------------

/// Opaque handle to a polled batch of records (owns the buffers).
#[repr(C)]
pub struct kafka_consumer_ConsumerRecords_t {
    _private: [u8; 0],
}

/// Opaque handle to a single consumer record (borrows from the owning batch).
#[repr(C)]
pub struct kafka_consumer_ConsumerRecord_t {
    _private: [u8; 0],
}

/// Owns the polled batch plus a flat index into its records (insertion order).
/// Record getters borrow from `records`; the flat pointers are valid until the
/// handle is destroyed.
struct ConsumerRecordsInner {
    /// Owns the record buffers.
    records: ConsumerRecords<Bytes, Bytes>,
    /// Index → record pointer (insertion order). Pointers borrow into `records`
    /// and stay valid because `records` is boxed and never moved.
    flat: Vec<*const ConsumerRecord<Bytes, Bytes>>,
}

/// Boxes a polled batch into an opaque records handle, building the flat index
/// in insertion order. Zero-copy: the records (and their key/value buffers) are
/// moved into the box, not cloned.
pub(crate) fn box_records(records: ConsumerRecords<Bytes, Bytes>) -> *mut kafka_consumer_ConsumerRecords_t {
    // Box `records` first so its address is stable, then collect borrowed
    // pointers into it.
    let mut inner = Box::new(ConsumerRecordsInner { records, flat: Vec::new() });
    let flat: Vec<*const ConsumerRecord<Bytes, Bytes>> = (&inner.records)
        .into_iter()
        .map(|r| r as *const ConsumerRecord<Bytes, Bytes>)
        .collect();
    inner.flat = flat;
    Box::into_raw(inner) as *mut kafka_consumer_ConsumerRecords_t
}

/// Casts a records handle to its inner type.
///
/// # Safety
///
/// `records` must be a valid handle from [`box_records`].
unsafe fn records_ref(records: *const kafka_consumer_ConsumerRecords_t) -> &'static ConsumerRecordsInner {
    unsafe { &*(records as *const ConsumerRecordsInner) }
}

/// Returns the number of records in the batch.
///
/// # Safety
///
/// `records` must be a valid records handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecords_count(records: *const kafka_consumer_ConsumerRecords_t) -> i32 {
    if records.is_null() {
        return 0;
    }
    unsafe { records_ref(records) }.flat.len() as i32
}

/// Returns whether the batch is empty.
///
/// # Safety
///
/// `records` must be a valid records handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecords_is_empty(
    records: *const kafka_consumer_ConsumerRecords_t,
) -> bool {
    if records.is_null() {
        return true;
    }
    unsafe { records_ref(records) }.flat.is_empty()
}

/// Returns the record at index `index` (borrowed; valid until the records handle
/// is destroyed), or null if out of range.
///
/// # Safety
///
/// `records` must be a valid records handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecords_get(
    records: *const kafka_consumer_ConsumerRecords_t,
    index: i32,
) -> *const kafka_consumer_ConsumerRecord_t {
    if records.is_null() || index < 0 {
        return std::ptr::null();
    }
    let inner = unsafe { records_ref(records) };
    match inner.flat.get(index as usize) {
        Some(&ptr) => ptr as *const kafka_consumer_ConsumerRecord_t,
        None => std::ptr::null(),
    }
}

/// Destroys a records handle, freeing the owned batch. Safe with null (no-op).
///
/// # Safety
///
/// `records` must be null or a valid records handle. After this call the pointer
/// (and any record pointers obtained from it) are invalid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecords_destroy(records: *mut kafka_consumer_ConsumerRecords_t) {
    if !records.is_null() {
        unsafe {
            drop(Box::from_raw(records as *mut ConsumerRecordsInner));
        }
    }
}

/// Casts a record handle to its inner type. Exposed to the consumer FFI surfaces
/// so `acknowledge`-style entry points can borrow the polled record.
///
/// # Safety
///
/// `record` must be a valid record pointer obtained from
/// [`kafka_consumer_ConsumerRecords_get`], and the owning batch must still be
/// alive.
pub(crate) unsafe fn record_ref(
    record: *const kafka_consumer_ConsumerRecord_t,
) -> &'static ConsumerRecord<Bytes, Bytes> {
    unsafe { &*(record as *const ConsumerRecord<Bytes, Bytes>) }
}

/// Returns the partition of a record.
///
/// # Safety
///
/// `record` must be a valid record pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_partition(
    record: *const kafka_consumer_ConsumerRecord_t,
) -> i32 {
    unsafe { record_ref(record) }.partition()
}

/// Returns the offset of a record.
///
/// # Safety
///
/// `record` must be a valid record pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_offset(record: *const kafka_consumer_ConsumerRecord_t) -> i64 {
    unsafe { record_ref(record) }.offset()
}

/// Returns the timestamp of a record (milliseconds since epoch, or
/// `NO_TIMESTAMP` = -1).
///
/// # Safety
///
/// `record` must be a valid record pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_timestamp(
    record: *const kafka_consumer_ConsumerRecord_t,
) -> i64 {
    unsafe { record_ref(record) }.timestamp()
}

/// Returns the topic name of a record as a (ptr, len) pair (NOT NUL-terminated).
/// `out_len` receives the byte length. The pointer borrows into the batch and is
/// valid until the records handle is destroyed.
///
/// # Safety
///
/// `record` must be a valid record pointer; `out_len` must be a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_topic(
    record: *const kafka_consumer_ConsumerRecord_t,
    out_len: *mut i32,
) -> *const c_char {
    let rec = unsafe { record_ref(record) };
    let topic = rec.topic();
    if !out_len.is_null() {
        unsafe { *out_len = topic.len() as i32 };
    }
    topic.as_ptr() as *const c_char
}

/// Returns the key bytes of a record as a (ptr, len) pair, or (null, -1) if the
/// key is absent. The pointer borrows into the batch (zero-copy) and is valid
/// until the records handle is destroyed.
///
/// # Safety
///
/// `record` must be a valid record pointer; `out_len` must be a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_key(
    record: *const kafka_consumer_ConsumerRecord_t,
    out_len: *mut i32,
) -> *const u8 {
    let rec = unsafe { record_ref(record) };
    match rec.key() {
        Some(k) => {
            if !out_len.is_null() {
                unsafe { *out_len = k.len() as i32 };
            }
            k.as_ptr()
        },
        None => {
            if !out_len.is_null() {
                unsafe { *out_len = -1 };
            }
            std::ptr::null()
        },
    }
}

/// Returns the value bytes of a record as a (ptr, len) pair, or (null, -1) if the
/// value is absent. The pointer borrows into the batch (zero-copy) and is valid
/// until the records handle is destroyed.
///
/// # Safety
///
/// `record` must be a valid record pointer; `out_len` must be a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_value(
    record: *const kafka_consumer_ConsumerRecord_t,
    out_len: *mut i32,
) -> *const u8 {
    let rec = unsafe { record_ref(record) };
    match rec.value() {
        Some(v) => {
            if !out_len.is_null() {
                unsafe { *out_len = v.len() as i32 };
            }
            v.as_ptr()
        },
        None => {
            if !out_len.is_null() {
                unsafe { *out_len = -1 };
            }
            std::ptr::null()
        },
    }
}

/// Returns the timestamp type of a record as its numeric id (`-1` =
/// NoTimestampType, `0` = CreateTime, `1` = LogAppendTime).
///
/// # Safety
///
/// `record` must be a valid record pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_timestamp_type(
    record: *const kafka_consumer_ConsumerRecord_t,
) -> i32 {
    unsafe { record_ref(record) }.timestamp_type().id()
}

/// Returns the serialized key size in bytes, or `-1` if the key is null.
///
/// # Safety
///
/// `record` must be a valid record pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_serialized_key_size(
    record: *const kafka_consumer_ConsumerRecord_t,
) -> i32 {
    unsafe { record_ref(record) }.serialized_key_size()
}

/// Returns the serialized value size in bytes, or `-1` if the value is null.
///
/// # Safety
///
/// `record` must be a valid record pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_serialized_value_size(
    record: *const kafka_consumer_ConsumerRecord_t,
) -> i32 {
    unsafe { record_ref(record) }.serialized_value_size()
}

/// Returns the leader epoch of a record. Writes the epoch to `*out_epoch` and
/// returns `true` if present; returns `false` (leaving `*out_epoch` untouched) if
/// absent.
///
/// # Safety
///
/// `record` must be a valid record pointer; `out_epoch` a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_leader_epoch(
    record: *const kafka_consumer_ConsumerRecord_t,
    out_epoch: *mut i32,
) -> bool {
    match unsafe { record_ref(record) }.leader_epoch() {
        Some(epoch) => {
            if !out_epoch.is_null() {
                unsafe { *out_epoch = epoch };
            }
            true
        },
        None => false,
    }
}

/// Returns the delivery count of a record (KIP-932 share consumer). Writes the
/// count to `*out_count` and returns `true` if present; returns `false` if
/// absent.
///
/// # Safety
///
/// `record` must be a valid record pointer; `out_count` a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_delivery_count(
    record: *const kafka_consumer_ConsumerRecord_t,
    out_count: *mut i32,
) -> bool {
    match unsafe { record_ref(record) }.delivery_count() {
        Some(count) => {
            if !out_count.is_null() {
                unsafe { *out_count = count as i32 };
            }
            true
        },
        None => false,
    }
}

/// Returns the number of headers attached to a record.
///
/// # Safety
///
/// `record` must be a valid record pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_header_count(
    record: *const kafka_consumer_ConsumerRecord_t,
) -> i32 {
    unsafe { record_ref(record) }.headers().into_iter().count() as i32
}

/// Returns the borrowed header at `index` (insertion order), or `None` if out of
/// range.
fn header_at(rec: &ConsumerRecord<Bytes, Bytes>, index: i32) -> Option<&RecordHeader> {
    if index < 0 {
        return None;
    }
    rec.headers().into_iter().nth(index as usize)
}

/// Returns the key of the header at `index` as a (ptr, len) pair (NOT
/// NUL-terminated), or (null, -1) if out of range. `out_len` receives the byte
/// length. The pointer borrows into the batch.
///
/// # Safety
///
/// `record` must be a valid record pointer; `out_len` a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_header_key(
    record: *const kafka_consumer_ConsumerRecord_t,
    index: i32,
    out_len: *mut i32,
) -> *const c_char {
    let rec = unsafe { record_ref(record) };
    match header_at(rec, index) {
        Some(header) => {
            let key = header.key();
            if !out_len.is_null() {
                unsafe { *out_len = key.len() as i32 };
            }
            key.as_ptr() as *const c_char
        },
        None => {
            if !out_len.is_null() {
                unsafe { *out_len = -1 };
            }
            std::ptr::null()
        },
    }
}

/// Returns the value of the header at `index` as a (ptr, len) pair, or (null, -1)
/// if out of range or the header value is null. `out_len` receives the byte
/// length. The pointer borrows into the batch (zero-copy).
///
/// # Safety
///
/// `record` must be a valid record pointer; `out_len` a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_header_value(
    record: *const kafka_consumer_ConsumerRecord_t,
    index: i32,
    out_len: *mut i32,
) -> *const u8 {
    let rec = unsafe { record_ref(record) };
    match header_at(rec, index).and_then(|h| h.value()) {
        Some(value) => {
            if !out_len.is_null() {
                unsafe { *out_len = value.len() as i32 };
            }
            value.as_ptr()
        },
        None => {
            if !out_len.is_null() {
                unsafe { *out_len = -1 };
            }
            std::ptr::null()
        },
    }
}
