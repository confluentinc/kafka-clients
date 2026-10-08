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

//! `kafka_producer_ProducerRecord_t`:
//! `org.apache.kafka.clients.producer.ProducerRecord<K, V>` (CLAUDE.md §4),
//! with the `ProducerRecordOptions` / `ProducerRecordOptionsBuilder` pair
//! that stands for the constructor overloads beyond three parameters (§2).
//!
//! `K` and `V` are `void *` (§4, "Generic types"): a record stores the
//! pointers the application passes and hands them, unchanged, to the
//! producer's serializers and partitioner. `NULL` is Java's `null`. When the
//! producer was built without a serializer the `void *` is a
//! `kafka_Bytes_t *` whose bytes are read while `send` runs; a record never
//! copies or frees what its pointers address, so the application keeps them
//! valid until the send that takes the record has registered it (returned,
//! for the blocking `send`; invoked its completion callback, for `send_cb`).
//!
//! Headers are copied into the record (Java's `ProducerRecord` holds its own
//! `RecordHeaders`), so the handle passed to a constructor stays the
//! caller's.

use std::ffi::{CString, c_char, c_void};

use crate::common::Error;
use crate::common::header::RecordHeaders;
use crate::ffi::common::header::internals::record_headers::{
    RecordHeadersInner, box_record_headers, kafka_common_header_internals_RecordHeaders_t, record_headers_ref,
};
use crate::ffi::common::{box_error, kafka_common_Error_t};
use crate::ffi::util::{GenericValue, c_str_to_string, into_c_string, owned_c_string};
use crate::producer::{ProducerRecord, ProducerRecordOptions, ProducerRecordOptionsBuilder};

/// The record type every producer handle works with.
pub(crate) type GenericRecord = ProducerRecord<GenericValue, GenericValue>;

/// Opaque handle to a [`ProducerRecord`] over `void *` key and value.
#[repr(C)]
pub struct kafka_producer_ProducerRecord_t {
    _private: [u8; 0],
}

pub(crate) struct ProducerRecordInner {
    record: GenericRecord,
    topic_c: CString,
    /// The headers getter's view: a copy of the record's (immutable) headers
    /// behind a stable address.
    headers: Box<RecordHeadersInner>,
}

impl ProducerRecordInner {
    fn new(record: GenericRecord) -> Self {
        let topic_c = owned_c_string(record.topic());
        let headers = RecordHeadersInner::boxed(record.headers().clone());
        Self { record, topic_c, headers }
    }
}

/// Boxes a record into an owned handle.
pub(crate) fn box_producer_record(record: GenericRecord) -> *mut kafka_producer_ProducerRecord_t {
    Box::into_raw(Box::new(ProducerRecordInner::new(record))) as *mut kafka_producer_ProducerRecord_t
}

/// The record behind a handle.
///
/// # Safety
///
/// `record` must be a live handle.
pub(crate) unsafe fn producer_record_ref<'a>(record: *const kafka_producer_ProducerRecord_t) -> &'a GenericRecord {
    &unsafe { inner(record) }.record
}

unsafe fn inner<'a>(record: *const kafka_producer_ProducerRecord_t) -> &'a ProducerRecordInner {
    unsafe { &*(record as *const ProducerRecordInner) }
}

fn generic(ptr: *const c_void) -> Option<GenericValue> {
    (!ptr.is_null()).then(|| GenericValue::new(ptr as *mut c_void))
}

fn generic_ptr(value: Option<&GenericValue>) -> *mut c_void {
    value.map_or(std::ptr::null_mut(), |v| v.as_ptr())
}

fn optional_partition(partition: i32) -> Option<i32> {
    (partition >= 0).then_some(partition)
}

fn optional_timestamp(timestamp: i64) -> Option<i64> {
    (timestamp >= 0).then_some(timestamp)
}

unsafe fn deliver(
    result: Result<GenericRecord, Error>,
    out: *mut *mut kafka_producer_ProducerRecord_t,
) -> *mut kafka_common_Error_t {
    match result {
        Ok(record) => {
            unsafe { *out = box_producer_record(record) };
            std::ptr::null_mut()
        },
        Err(error) => box_error(error),
    }
}

// ---------------------------------------------------------------------------
// ProducerRecord
// ---------------------------------------------------------------------------

/// `new ProducerRecord(String topic, V value)`: owned, freed with
/// [`kafka_producer_ProducerRecord_destroy`].
///
/// # Safety
///
/// `topic` must be a NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecord_new(
    topic: *const c_char,
    value: *const c_void,
) -> *mut kafka_producer_ProducerRecord_t {
    box_producer_record(ProducerRecord::new(unsafe { c_str_to_string(topic) }, generic(value)))
}

/// `new ProducerRecord(String topic, K key, V value)`.
///
/// # Safety
///
/// `topic` must be a NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecord_with_key(
    topic: *const c_char,
    key: *const c_void,
    value: *const c_void,
) -> *mut kafka_producer_ProducerRecord_t {
    box_producer_record(ProducerRecord::with_key(
        unsafe { c_str_to_string(topic) },
        generic(key),
        generic(value),
    ))
}

/// `new ProducerRecord(String topic, Integer partition, K key, V value)`:
/// `partition < 0` is Java's `null`. Fails with the translation of Java's
/// `IllegalArgumentException` the constructor throws.
///
/// # Safety
///
/// `topic` must be a NUL-terminated string and `out_with_partition_key` a
/// valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecord_with_partition_key(
    topic: *const c_char,
    partition: i32,
    key: *const c_void,
    value: *const c_void,
    out_with_partition_key: *mut *mut kafka_producer_ProducerRecord_t,
) -> *mut kafka_common_Error_t {
    let result = ProducerRecord::with_partition_key(
        unsafe { c_str_to_string(topic) },
        optional_partition(partition),
        generic(key),
        generic(value),
    )
    .map_err(|e| Error::local_illegal_argument(e.message()));
    unsafe { deliver(result, out_with_partition_key) }
}

/// `new ProducerRecord(String topic, Integer partition, K key, V value,
/// Iterable<Header> headers)`: the headers are copied, `partition < 0` is
/// Java's `null`.
///
/// # Safety
///
/// `topic` must be a NUL-terminated string, `headers` a live handle and
/// `out_with_partition_key_headers` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecord_with_partition_key_headers(
    topic: *const c_char,
    partition: i32,
    key: *const c_void,
    value: *const c_void,
    headers: *const kafka_common_header_internals_RecordHeaders_t,
    out_with_partition_key_headers: *mut *mut kafka_producer_ProducerRecord_t,
) -> *mut kafka_common_Error_t {
    let result = ProducerRecord::with_partition_key_headers(
        unsafe { c_str_to_string(topic) },
        optional_partition(partition),
        generic(key),
        generic(value),
        unsafe { record_headers_ref(headers) }.clone(),
    )
    .map_err(|e| Error::local_illegal_argument(e.message()));
    unsafe { deliver(result, out_with_partition_key_headers) }
}

/// `new ProducerRecord(String topic, Integer partition, Long timestamp, K
/// key, V value)`: `partition < 0` and `timestamp < 0` are Java's `null`
/// (Java rejects a negative timestamp, so no value is lost).
///
/// # Safety
///
/// `topic` must be a NUL-terminated string and
/// `out_with_partition_timestamp_key` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecord_with_partition_timestamp_key(
    topic: *const c_char,
    partition: i32,
    timestamp: i64,
    key: *const c_void,
    value: *const c_void,
    out_with_partition_timestamp_key: *mut *mut kafka_producer_ProducerRecord_t,
) -> *mut kafka_common_Error_t {
    let result = ProducerRecord::with_partition_timestamp_key(
        unsafe { c_str_to_string(topic) },
        optional_partition(partition),
        optional_timestamp(timestamp),
        generic(key),
        generic(value),
    )
    .map_err(|e| Error::local_illegal_argument(e.message()));
    unsafe { deliver(result, out_with_partition_timestamp_key) }
}

/// `ProducerRecord::with_options`: the six-parameter constructor through a
/// built [`kafka_producer_ProducerRecordOptions_t`], which stays the
/// caller's.
///
/// # Safety
///
/// `options` must be a live handle and `out_with_options` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecord_with_options(
    options: *const kafka_producer_ProducerRecordOptions_t,
    out_with_options: *mut *mut kafka_producer_ProducerRecord_t,
) -> *mut kafka_common_Error_t {
    let options = unsafe { options_ref(options) }.clone();
    unsafe {
        deliver(
            ProducerRecord::with_options(options).map_err(|e| Error::local_illegal_argument(e.message())),
            out_with_options,
        )
    }
}

/// `ProducerRecord.topic()`: borrowed, valid until the handle is destroyed.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecord_topic(
    self_: *const kafka_producer_ProducerRecord_t,
) -> *const c_char {
    unsafe { inner(self_) }.topic_c.as_ptr()
}

/// `ProducerRecord.headers()`: a borrowed view valid until the handle is
/// destroyed.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecord_headers(
    self_: *const kafka_producer_ProducerRecord_t,
) -> *const kafka_common_header_internals_RecordHeaders_t {
    &*unsafe { inner(self_) }.headers as *const RecordHeadersInner
        as *const kafka_common_header_internals_RecordHeaders_t
}

/// `ProducerRecord.key()`: the application's `void *`, `NULL` for `null`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecord_key(
    self_: *const kafka_producer_ProducerRecord_t,
) -> *mut c_void {
    generic_ptr(unsafe { producer_record_ref(self_) }.key())
}

/// `ProducerRecord.value()`: the application's `void *`, `NULL` for `null`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecord_value(
    self_: *const kafka_producer_ProducerRecord_t,
) -> *mut c_void {
    generic_ptr(unsafe { producer_record_ref(self_) }.value())
}

/// `ProducerRecord.timestamp()`: `-1` for `null`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecord_timestamp(self_: *const kafka_producer_ProducerRecord_t) -> i64 {
    unsafe { producer_record_ref(self_) }.timestamp().unwrap_or(-1)
}

/// `ProducerRecord.partition()`: `-1` for `null`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecord_partition(self_: *const kafka_producer_ProducerRecord_t) -> i32 {
    unsafe { producer_record_ref(self_) }.partition().unwrap_or(-1)
}

/// `ProducerRecord.toString()`: owned, freed with `kafka_string_destroy`.
/// Key and value print as the addresses they are.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecord_to_string(
    self_: *const kafka_producer_ProducerRecord_t,
) -> *mut c_char {
    into_c_string(&unsafe { producer_record_ref(self_) }.to_string())
}

/// `ProducerRecord::into_parts`: consumes the record, delivering each part
/// through its slot (`NULL` slots are skipped). `out_topic` is owned by the
/// caller (`kafka_string_destroy`), `out_headers` too
/// (`kafka_common_header_internals_RecordHeaders_destroy`); `out_partition`
/// and `out_timestamp` receive `-1` for `null`. The handle is freed.
///
/// # Safety
///
/// `self_` must be a live handle; every non-null slot must be valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecord_into_parts(
    self_: *mut kafka_producer_ProducerRecord_t,
    out_topic: *mut *mut c_char,
    out_partition: *mut i32,
    out_timestamp: *mut i64,
    out_headers: *mut *mut kafka_common_header_internals_RecordHeaders_t,
    out_key: *mut *mut c_void,
    out_value: *mut *mut c_void,
) {
    let inner = unsafe { Box::from_raw(self_ as *mut ProducerRecordInner) };
    let (topic, partition, timestamp, headers, key, value) = inner.record.into_parts();
    unsafe {
        if !out_topic.is_null() {
            *out_topic = into_c_string(&topic);
        }
        if !out_partition.is_null() {
            *out_partition = partition.unwrap_or(-1);
        }
        if !out_timestamp.is_null() {
            *out_timestamp = timestamp.unwrap_or(-1);
        }
        if !out_headers.is_null() {
            *out_headers = box_record_headers(headers);
        }
        if !out_key.is_null() {
            *out_key = generic_ptr(key.as_ref());
        }
        if !out_value.is_null() {
            *out_value = generic_ptr(value.as_ref());
        }
    }
}

/// Frees the handle; null is a no-op. The key and value pointers are the
/// application's and are not freed.
///
/// # Safety
///
/// `self_` must be null or a handle not yet destroyed or consumed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecord_destroy(self_: *mut kafka_producer_ProducerRecord_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut ProducerRecordInner) });
    }
}

// ---------------------------------------------------------------------------
// ProducerRecordOptions / ProducerRecordOptionsBuilder
// ---------------------------------------------------------------------------

/// Opaque handle to a built `ProducerRecordOptions`.
// Rust-only: the `ProducerRecordOptions` struct CLAUDE.md §2 mandates for the
// overloaded constructors; Java has no such class.
#[doc(alias = "rust-only")]
#[repr(C)]
pub struct kafka_producer_ProducerRecordOptions_t {
    _private: [u8; 0],
}

/// Opaque handle to a `ProducerRecordOptionsBuilder`.
// Rust-only: builds `ProducerRecordOptions` (CLAUDE.md §2)
#[doc(alias = "rust-only")]
#[repr(C)]
pub struct kafka_producer_ProducerRecordOptionsBuilder_t {
    _private: [u8; 0],
}

type GenericOptions = ProducerRecordOptions<GenericValue, GenericValue>;
type GenericBuilder = ProducerRecordOptionsBuilder<GenericValue, GenericValue>;

unsafe fn options_ref<'a>(options: *const kafka_producer_ProducerRecordOptions_t) -> &'a GenericOptions {
    unsafe { &*(options as *const GenericOptions) }
}

/// The Rust builder consumes and returns itself on every setter; the C
/// handle keeps it in an `Option` so a setter can take it, apply the call and
/// put it back.
unsafe fn with_builder(
    builder: *mut kafka_producer_ProducerRecordOptionsBuilder_t,
    f: impl FnOnce(GenericBuilder) -> GenericBuilder,
) {
    let slot = unsafe { &mut *(builder as *mut Option<GenericBuilder>) };
    if let Some(builder) = slot.take() {
        *slot = Some(f(builder));
    }
}

/// `ProducerRecordOptionsBuilder::new()`: owned, freed with
/// [`kafka_producer_ProducerRecordOptionsBuilder_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_producer_ProducerRecordOptionsBuilder_new() -> *mut kafka_producer_ProducerRecordOptionsBuilder_t
{
    Box::into_raw(Box::new(Some(GenericBuilder::new()))) as *mut kafka_producer_ProducerRecordOptionsBuilder_t
}

/// `set_topic`: mandatory.
///
/// # Safety
///
/// `self_` must be a live handle and `topic` a NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecordOptionsBuilder_set_topic(
    self_: *mut kafka_producer_ProducerRecordOptionsBuilder_t,
    topic: *const c_char,
) {
    let topic = unsafe { c_str_to_string(topic) };
    unsafe { with_builder(self_, |b| b.set_topic(topic)) };
}

/// `set_partition`: `partition < 0` is Java's `null`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecordOptionsBuilder_set_partition(
    self_: *mut kafka_producer_ProducerRecordOptionsBuilder_t,
    partition: i32,
) {
    unsafe { with_builder(self_, |b| b.set_partition(optional_partition(partition))) };
}

/// `set_timestamp`: `timestamp < 0` is Java's `null`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecordOptionsBuilder_set_timestamp(
    self_: *mut kafka_producer_ProducerRecordOptionsBuilder_t,
    timestamp: i64,
) {
    unsafe { with_builder(self_, |b| b.set_timestamp(optional_timestamp(timestamp))) };
}

/// `set_key`: the application's `void *`, `NULL` for `null`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecordOptionsBuilder_set_key(
    self_: *mut kafka_producer_ProducerRecordOptionsBuilder_t,
    key: *const c_void,
) {
    unsafe { with_builder(self_, |b| b.set_key(generic(key))) };
}

/// `set_value`: mandatory (`NULL` is the `null` value, still "set").
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecordOptionsBuilder_set_value(
    self_: *mut kafka_producer_ProducerRecordOptionsBuilder_t,
    value: *const c_void,
) {
    unsafe {
        with_builder(self_, |b| {
            b.set_value(
                Some(generic(value))
                    .map(|v| v.unwrap_or(GenericValue::new(std::ptr::null_mut())))
                    .filter(|v| !v.is_null()),
            )
        })
    };
}

/// `set_headers`: copied; `NULL` leaves the record with empty headers.
///
/// # Safety
///
/// `self_` must be a live handle and `headers` null or a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecordOptionsBuilder_set_headers(
    self_: *mut kafka_producer_ProducerRecordOptionsBuilder_t,
    headers: *const kafka_common_header_internals_RecordHeaders_t,
) {
    let headers: Option<RecordHeaders> = (!headers.is_null()).then(|| unsafe { record_headers_ref(headers) }.clone());
    unsafe { with_builder(self_, |b| b.set_headers(headers)) };
}

/// `build`: validates the mandatory parameters (`topic`, `value`) and
/// delivers the options, owned by the caller
/// ([`kafka_producer_ProducerRecordOptions_destroy`]). The builder is
/// consumed: a second `build` on the same handle fails as if nothing was set.
///
/// # Safety
///
/// `self_` must be a live handle and `out_build` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecordOptionsBuilder_build(
    self_: *mut kafka_producer_ProducerRecordOptionsBuilder_t,
    out_build: *mut *mut kafka_producer_ProducerRecordOptions_t,
) -> *mut kafka_common_Error_t {
    let slot = unsafe { &mut *(self_ as *mut Option<GenericBuilder>) };
    let builder = slot.take().unwrap_or_else(GenericBuilder::new);
    match builder.build() {
        Ok(options) => {
            unsafe { *out_build = Box::into_raw(Box::new(options)) as *mut kafka_producer_ProducerRecordOptions_t };
            std::ptr::null_mut()
        },
        Err(error) => box_error(error),
    }
}

/// Frees the builder; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or a handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecordOptionsBuilder_destroy(
    self_: *mut kafka_producer_ProducerRecordOptionsBuilder_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut Option<GenericBuilder>) });
    }
}

/// Frees built options; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or a handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerRecordOptions_destroy(
    self_: *mut kafka_producer_ProducerRecordOptions_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut GenericOptions) });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::header::Headers;
    use crate::ffi::common::header::internals::record_headers::{
        kafka_common_header_internals_RecordHeaders_destroy, kafka_common_header_internals_RecordHeaders_new,
    };
    use crate::ffi::common::{kafka_common_Error_destroy, kafka_common_Error_message};
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn constructors_keep_the_pointers_and_map_null_to_minus_one() {
        let topic = CString::new("topic").unwrap();
        let key = 1i32;
        let value = 2i32;
        unsafe {
            let record = kafka_producer_ProducerRecord_new(topic.as_ptr(), &value as *const i32 as *const c_void);
            assert_eq!(c_str_to_string(kafka_producer_ProducerRecord_topic(record)), "topic");
            assert!(kafka_producer_ProducerRecord_key(record).is_null());
            assert_eq!(kafka_producer_ProducerRecord_value(record), &value as *const i32 as *mut c_void);
            assert_eq!(kafka_producer_ProducerRecord_partition(record), -1);
            assert_eq!(kafka_producer_ProducerRecord_timestamp(record), -1);
            let headers = kafka_producer_ProducerRecord_headers(record);
            assert!(record_headers_ref(headers).to_array().is_empty());
            kafka_producer_ProducerRecord_destroy(record);

            let record = kafka_producer_ProducerRecord_with_key(
                topic.as_ptr(),
                &key as *const i32 as *const c_void,
                std::ptr::null(),
            );
            assert_eq!(kafka_producer_ProducerRecord_key(record), &key as *const i32 as *mut c_void);
            assert!(kafka_producer_ProducerRecord_value(record).is_null());
            kafka_producer_ProducerRecord_destroy(record);

            let mut out = std::ptr::null_mut();
            let error = kafka_producer_ProducerRecord_with_partition_timestamp_key(
                topic.as_ptr(),
                3,
                42,
                std::ptr::null(),
                &value as *const i32 as *const c_void,
                &raw mut out,
            );
            assert!(error.is_null());
            assert_eq!(kafka_producer_ProducerRecord_partition(out), 3);
            assert_eq!(kafka_producer_ProducerRecord_timestamp(out), 42);
            let text = kafka_producer_ProducerRecord_to_string(out);
            let text_s = c_str_to_string(text);
            assert!(text_s.starts_with("ProducerRecord(topic=topic, partition=Some(3)"), "{text_s}");
            kafka_string_destroy(text);
            kafka_producer_ProducerRecord_destroy(out);
            kafka_producer_ProducerRecord_destroy(std::ptr::null_mut());
        }
    }

    #[test]
    fn invalid_arguments_fail_with_javas_message() {
        let topic = CString::new("topic").unwrap();
        let mut out = std::ptr::null_mut();
        unsafe {
            let error = kafka_producer_ProducerRecord_with_partition_timestamp_key(
                topic.as_ptr(),
                -1,
                -5,
                std::ptr::null(),
                std::ptr::null(),
                &raw mut out,
            );
            // A negative timestamp is `null` on the C side, so this succeeds...
            assert!(error.is_null());
            kafka_producer_ProducerRecord_destroy(out);

            // ...while a `null` topic is Java's IllegalArgumentException.
            let record =
                ProducerRecord::<GenericValue, GenericValue>::with_partition_key(String::new(), None, None, None);
            assert!(
                record.is_ok(),
                "the Rust side accepts an empty topic; only null is rejected in Java"
            );
            let empty = CString::new("").unwrap();
            let error = kafka_producer_ProducerRecord_with_partition_key(
                empty.as_ptr(),
                -1,
                std::ptr::null(),
                std::ptr::null(),
                &raw mut out,
            );
            assert!(error.is_null());
            kafka_producer_ProducerRecord_destroy(out);
        }
    }

    #[test]
    fn options_builder_validates_and_copies_headers() {
        let topic = CString::new("topic").unwrap();
        let value = 2i32;
        unsafe {
            let builder = kafka_producer_ProducerRecordOptionsBuilder_new();
            let mut options = std::ptr::null_mut();
            let error = kafka_producer_ProducerRecordOptionsBuilder_build(builder, &raw mut options);
            assert!(!error.is_null());
            assert_eq!(
                c_str_to_string(kafka_common_Error_message(error)),
                "ProducerRecordOptionsBuilder::build: mandatory parameter `topic` was not set"
            );
            kafka_common_Error_destroy(error);
            kafka_producer_ProducerRecordOptionsBuilder_destroy(builder);

            let headers = kafka_common_header_internals_RecordHeaders_new();
            let builder = kafka_producer_ProducerRecordOptionsBuilder_new();
            kafka_producer_ProducerRecordOptionsBuilder_set_topic(builder, topic.as_ptr());
            kafka_producer_ProducerRecordOptionsBuilder_set_partition(builder, 1);
            kafka_producer_ProducerRecordOptionsBuilder_set_timestamp(builder, 7);
            kafka_producer_ProducerRecordOptionsBuilder_set_key(builder, std::ptr::null());
            kafka_producer_ProducerRecordOptionsBuilder_set_value(builder, &value as *const i32 as *const c_void);
            kafka_producer_ProducerRecordOptionsBuilder_set_headers(builder, headers);
            let error = kafka_producer_ProducerRecordOptionsBuilder_build(builder, &raw mut options);
            assert!(error.is_null());
            kafka_producer_ProducerRecordOptionsBuilder_destroy(builder);
            kafka_common_header_internals_RecordHeaders_destroy(headers);

            let mut record = std::ptr::null_mut();
            let error = kafka_producer_ProducerRecord_with_options(options, &raw mut record);
            assert!(error.is_null());
            assert_eq!(kafka_producer_ProducerRecord_partition(record), 1);
            assert_eq!(kafka_producer_ProducerRecord_timestamp(record), 7);
            assert_eq!(kafka_producer_ProducerRecord_value(record), &value as *const i32 as *mut c_void);

            // The options stay usable: a second record from the same handle.
            let mut second = std::ptr::null_mut();
            assert!(kafka_producer_ProducerRecord_with_options(options, &raw mut second).is_null());
            kafka_producer_ProducerRecord_destroy(second);
            kafka_producer_ProducerRecordOptions_destroy(options);
            kafka_producer_ProducerRecordOptions_destroy(std::ptr::null_mut());

            let mut out_topic = std::ptr::null_mut();
            let mut out_partition = 0;
            let mut out_timestamp = 0;
            let mut out_headers = std::ptr::null_mut();
            let mut out_key = std::ptr::null_mut();
            let mut out_value = std::ptr::null_mut();
            kafka_producer_ProducerRecord_into_parts(
                record,
                &raw mut out_topic,
                &raw mut out_partition,
                &raw mut out_timestamp,
                &raw mut out_headers,
                &raw mut out_key,
                &raw mut out_value,
            );
            assert_eq!(c_str_to_string(out_topic), "topic");
            assert_eq!(out_partition, 1);
            assert_eq!(out_timestamp, 7);
            assert!(record_headers_ref(out_headers).to_array().is_empty());
            assert!(out_key.is_null());
            assert_eq!(out_value, &value as *const i32 as *mut c_void);
            kafka_string_destroy(out_topic);
            kafka_common_header_internals_RecordHeaders_destroy(out_headers);
        }
    }
}
