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

//! `kafka_consumer_ConsumerRecord_t`:
//! `org.apache.kafka.clients.consumer.ConsumerRecord<K, V>`, with the
//! Rust-only `ConsumerRecordOptions` / `ConsumerRecordOptionsBuilder` that
//! stand for the constructor overloads with more than three parameters
//! (CLAUDE.md §2).
//!
//! `K` and `V` are `void *` (CLAUDE.md §4, "Generic types"): what
//! `kafka_consumer_ConsumerRecord_key` / `_value` return is whatever the
//! deserializer the consumer was created with produced. With a `NULL`
//! deserializer it is a `kafka_Bytes_t *` the record owns, pointing into the
//! fetch buffer the records handle keeps alive (zero-copy, CLAUDE.md §14),
//! freed when the record is; with a C deserializer it is the C side's own
//! value, never freed by Rust. A record built from C with
//! [`kafka_consumer_ConsumerRecord_new`] or `_with_options` never owns its
//! key or value.

use std::ffi::{CString, c_char, c_void};
use std::sync::OnceLock;

use crate::consumer::ConsumerRecord;
use crate::consumer::{ConsumerRecordOptions, ConsumerRecordOptionsBuilder};
use crate::ffi::common::header::internals::record_headers::{
    RecordHeadersInner, kafka_common_header_internals_RecordHeaders_t, record_headers_ref,
};
use crate::ffi::common::record::timestamp_type::{self, kafka_common_record_TimestampType_t};
use crate::ffi::common::{box_error, kafka_common_Error_t};
use crate::ffi::util::{
    GenericValue, c_str_to_string, into_c_string, kafka_Bytes_destroy, kafka_Bytes_t, owned_c_string,
};

/// A record whose key and value are the C caller's `void *`s.
pub(crate) type GenericRecord = ConsumerRecord<GenericValue, GenericValue>;

/// Opaque handle to a [`ConsumerRecord`].
#[repr(C)]
pub struct kafka_consumer_ConsumerRecord_t {
    _private: [u8; 0],
}

/// Opaque handle to a [`ConsumerRecordOptions`].
#[repr(C)]
// the Options struct standing for the constructor overloads with more than three parameters (CLAUDE.md §2)
#[doc(alias = "rust-only")]
pub struct kafka_consumer_ConsumerRecordOptions_t {
    _private: [u8; 0],
}

/// Opaque handle to a [`ConsumerRecordOptionsBuilder`].
#[repr(C)]
// the builder of the Options struct standing for the constructor overloads (CLAUDE.md §2)
#[doc(alias = "rust-only")]
pub struct kafka_consumer_ConsumerRecordOptionsBuilder_t {
    _private: [u8; 0],
}

/// What a record handle points at: the record, a NUL-terminated copy of its
/// topic, the lazily built headers view and whether the `void *`s are
/// `kafka_Bytes_t *` this record owns (see the module docs).
pub(crate) struct ConsumerRecordInner {
    record: GenericRecord,
    topic_c: CString,
    headers: OnceLock<Box<RecordHeadersInner>>,
    owns_key: bool,
    owns_value: bool,
}

impl ConsumerRecordInner {
    pub(crate) fn new(record: GenericRecord, owns_key: bool, owns_value: bool) -> Self {
        let topic_c = owned_c_string(record.topic());
        Self { record, topic_c, headers: OnceLock::new(), owns_key, owns_value }
    }

    /// The handle pointer of this boxed record.
    pub(crate) fn as_ptr(&self) -> *const kafka_consumer_ConsumerRecord_t {
        self as *const Self as *const kafka_consumer_ConsumerRecord_t
    }
}

impl Drop for ConsumerRecordInner {
    fn drop(&mut self) {
        // SAFETY: an owned `void *` is a `kafka_Bytes_t *` built by the
        // passthrough deserializer and handed out by this record alone.
        if self.owns_key
            && let Some(key) = self.record.key()
        {
            unsafe { kafka_Bytes_destroy(key.as_ptr() as *mut kafka_Bytes_t) };
        }
        if self.owns_value
            && let Some(value) = self.record.value()
        {
            unsafe { kafka_Bytes_destroy(value.as_ptr() as *mut kafka_Bytes_t) };
        }
    }
}

/// Hands `record` to C as an owned handle, freed with
/// [`kafka_consumer_ConsumerRecord_destroy`].
pub(crate) fn box_consumer_record(
    record: GenericRecord,
    owns_key: bool,
    owns_value: bool,
) -> *mut kafka_consumer_ConsumerRecord_t {
    Box::into_raw(Box::new(ConsumerRecordInner::new(record, owns_key, owns_value)))
        as *mut kafka_consumer_ConsumerRecord_t
}

/// The record behind a handle.
///
/// # Safety
///
/// `record` must be a valid record handle.
pub(crate) unsafe fn consumer_record_ref<'a>(record: *const kafka_consumer_ConsumerRecord_t) -> &'a GenericRecord {
    &unsafe { &*(record as *const ConsumerRecordInner) }.record
}

unsafe fn inner_ref<'a>(record: *const kafka_consumer_ConsumerRecord_t) -> &'a ConsumerRecordInner {
    unsafe { &*(record as *const ConsumerRecordInner) }
}

/// Java's `null` for a null pointer.
fn generic(ptr: *const c_void) -> Option<GenericValue> {
    (!ptr.is_null()).then(|| GenericValue::new(ptr as *mut c_void))
}

fn generic_ptr(value: Option<&GenericValue>) -> *mut c_void {
    value.map_or(std::ptr::null_mut(), |v| v.as_ptr())
}

// ---------------------------------------------------------------------------
// ConsumerRecord
// ---------------------------------------------------------------------------

/// `new ConsumerRecord(String topic, int partition, long offset, K key, V value)`:
/// an owned handle freed with [`kafka_consumer_ConsumerRecord_destroy`];
/// `key` and `value` stay the caller's (null is Java `null`).
///
/// # Safety
///
/// `topic` must be a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_new(
    topic: *const c_char,
    partition: i32,
    offset: i64,
    key: *const c_void,
    value: *const c_void,
) -> *mut kafka_consumer_ConsumerRecord_t {
    let topic = unsafe { c_str_to_string(topic) };
    box_consumer_record(
        GenericRecord::new(topic, partition, offset, generic(key), generic(value)),
        false,
        false,
    )
}

/// The constructor overloads with more than three parameters, through the
/// Rust-only options (CLAUDE.md §2): an owned handle; the options stay the
/// caller's, and so do the key and value they carry.
///
/// # Safety
///
/// `options` must be a valid options handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_with_options(
    options: *const kafka_consumer_ConsumerRecordOptions_t,
) -> *mut kafka_consumer_ConsumerRecord_t {
    let options = unsafe { options_ref(options) }.clone();
    box_consumer_record(GenericRecord::with_options(options), false, false)
}

/// `topic()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_topic(
    self_: *const kafka_consumer_ConsumerRecord_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }.topic_c.as_ptr()
}

/// `partition()`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_partition(self_: *const kafka_consumer_ConsumerRecord_t) -> i32 {
    unsafe { consumer_record_ref(self_) }.partition()
}

/// `offset()`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_offset(self_: *const kafka_consumer_ConsumerRecord_t) -> i64 {
    unsafe { consumer_record_ref(self_) }.offset()
}

/// `timestamp()`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_timestamp(self_: *const kafka_consumer_ConsumerRecord_t) -> i64 {
    unsafe { consumer_record_ref(self_) }.timestamp()
}

/// `timestampType()`: a borrowed singleton.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_timestamp_type(
    self_: *const kafka_consumer_ConsumerRecord_t,
) -> *const kafka_common_record_TimestampType_t {
    timestamp_type::singleton(unsafe { consumer_record_ref(self_) }.timestamp_type())
}

/// `serializedKeySize()`: `-1` for a null key.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_serialized_key_size(
    self_: *const kafka_consumer_ConsumerRecord_t,
) -> i32 {
    unsafe { consumer_record_ref(self_) }.serialized_key_size()
}

/// `serializedValueSize()`: `-1` for a null value.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_serialized_value_size(
    self_: *const kafka_consumer_ConsumerRecord_t,
) -> i32 {
    unsafe { consumer_record_ref(self_) }.serialized_value_size()
}

/// `key()`: the deserialized key (see the module docs), `NULL` for Java
/// `null`; borrowed from the record.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_key(
    self_: *const kafka_consumer_ConsumerRecord_t,
) -> *mut c_void {
    generic_ptr(unsafe { consumer_record_ref(self_) }.key())
}

/// `value()`: the deserialized value (see the module docs), `NULL` for Java
/// `null`; borrowed from the record.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_value(
    self_: *const kafka_consumer_ConsumerRecord_t,
) -> *mut c_void {
    generic_ptr(unsafe { consumer_record_ref(self_) }.value())
}

/// `headers()`: borrowed from the record, valid until it is destroyed.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_headers(
    self_: *const kafka_consumer_ConsumerRecord_t,
) -> *const kafka_common_header_internals_RecordHeaders_t {
    let inner = unsafe { inner_ref(self_) };
    inner
        .headers
        .get_or_init(|| RecordHeadersInner::boxed(inner.record.headers().clone()))
        .as_ptr()
}

/// `leaderEpoch()`: the epoch, or `-1` for `Optional.empty()`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_leader_epoch(
    self_: *const kafka_consumer_ConsumerRecord_t,
) -> i32 {
    unsafe { consumer_record_ref(self_) }.leader_epoch().unwrap_or(-1)
}

/// `deliveryCount()`: the count, or `-1` for `Optional.empty()`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_delivery_count(
    self_: *const kafka_consumer_ConsumerRecord_t,
) -> i16 {
    unsafe { consumer_record_ref(self_) }.delivery_count().unwrap_or(-1)
}

/// Java `toString()`: an owned string freed with `kafka_string_destroy`. The
/// key and value print as the `void *` addresses Rust holds for them.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_to_string(
    self_: *const kafka_consumer_ConsumerRecord_t,
) -> *mut c_char {
    into_c_string(&unsafe { consumer_record_ref(self_) }.to_string())
}

/// Frees a handle (and the `kafka_Bytes_t`s it owns, see the module docs); a
/// null pointer is a no-op.
///
/// # Safety
///
/// `self_` must be null or a valid owned handle not used afterwards; a
/// record borrowed from a records handle is never passed here.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecord_destroy(self_: *mut kafka_consumer_ConsumerRecord_t) {
    if !self_.is_null() {
        unsafe { drop(Box::from_raw(self_ as *mut ConsumerRecordInner)) };
    }
}

// ---------------------------------------------------------------------------
// ConsumerRecordOptions / ConsumerRecordOptionsBuilder
// ---------------------------------------------------------------------------

type GenericOptions = ConsumerRecordOptions<GenericValue, GenericValue>;
type GenericBuilder = ConsumerRecordOptionsBuilder<GenericValue, GenericValue>;

/// The options behind a handle.
///
/// # Safety
///
/// `options` must be a valid options handle.
unsafe fn options_ref<'a>(options: *const kafka_consumer_ConsumerRecordOptions_t) -> &'a GenericOptions {
    unsafe { &*(options as *const GenericOptions) }
}

/// Applies a by-value fluent setter to the builder behind a handle. The
/// slot is empty only after `build`, which consumes the builder; a setter
/// after that is a no-op.
unsafe fn with_builder(
    builder: *mut kafka_consumer_ConsumerRecordOptionsBuilder_t,
    f: impl FnOnce(GenericBuilder) -> GenericBuilder,
) {
    let slot = unsafe { &mut *(builder as *mut Option<GenericBuilder>) };
    if let Some(b) = slot.take() {
        *slot = Some(f(b));
    }
}

/// `ConsumerRecordOptionsBuilder::new()`: an owned handle freed with
/// [`kafka_consumer_ConsumerRecordOptionsBuilder_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_consumer_ConsumerRecordOptionsBuilder_new() -> *mut kafka_consumer_ConsumerRecordOptionsBuilder_t
{
    Box::into_raw(Box::new(Some(GenericBuilder::new()))) as *mut kafka_consumer_ConsumerRecordOptionsBuilder_t
}

/// `set_topic` (mandatory).
///
/// # Safety
///
/// `self_` must be a valid builder handle and `topic` a valid NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecordOptionsBuilder_set_topic(
    self_: *mut kafka_consumer_ConsumerRecordOptionsBuilder_t,
    topic: *const c_char,
) {
    let topic = unsafe { c_str_to_string(topic) };
    unsafe { with_builder(self_, |b| b.set_topic(topic)) }
}

/// `set_partition` (mandatory).
///
/// # Safety
///
/// `self_` must be a valid builder handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecordOptionsBuilder_set_partition(
    self_: *mut kafka_consumer_ConsumerRecordOptionsBuilder_t,
    partition: i32,
) {
    unsafe { with_builder(self_, |b| b.set_partition(partition)) }
}

/// `set_offset` (mandatory).
///
/// # Safety
///
/// `self_` must be a valid builder handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecordOptionsBuilder_set_offset(
    self_: *mut kafka_consumer_ConsumerRecordOptionsBuilder_t,
    offset: i64,
) {
    unsafe { with_builder(self_, |b| b.set_offset(offset)) }
}

/// `set_timestamp`.
///
/// # Safety
///
/// `self_` must be a valid builder handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecordOptionsBuilder_set_timestamp(
    self_: *mut kafka_consumer_ConsumerRecordOptionsBuilder_t,
    timestamp: i64,
) {
    unsafe { with_builder(self_, |b| b.set_timestamp(timestamp)) }
}

/// `set_timestamp_type`.
///
/// # Safety
///
/// `self_` must be a valid builder handle and `timestamp_type` a singleton
/// of `kafka_common_record_TimestampType_t`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecordOptionsBuilder_set_timestamp_type(
    self_: *mut kafka_consumer_ConsumerRecordOptionsBuilder_t,
    timestamp_type: *const kafka_common_record_TimestampType_t,
) {
    let timestamp_type = unsafe { timestamp_type::value_of(timestamp_type) };
    unsafe { with_builder(self_, |b| b.set_timestamp_type(timestamp_type)) }
}

/// `set_serialized_key_size`.
///
/// # Safety
///
/// `self_` must be a valid builder handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecordOptionsBuilder_set_serialized_key_size(
    self_: *mut kafka_consumer_ConsumerRecordOptionsBuilder_t,
    serialized_key_size: i32,
) {
    unsafe { with_builder(self_, |b| b.set_serialized_key_size(serialized_key_size)) }
}

/// `set_serialized_value_size`.
///
/// # Safety
///
/// `self_` must be a valid builder handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecordOptionsBuilder_set_serialized_value_size(
    self_: *mut kafka_consumer_ConsumerRecordOptionsBuilder_t,
    serialized_value_size: i32,
) {
    unsafe { with_builder(self_, |b| b.set_serialized_value_size(serialized_value_size)) }
}

/// `set_key`: the caller's `void *`, null for Java `null`.
///
/// # Safety
///
/// `self_` must be a valid builder handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecordOptionsBuilder_set_key(
    self_: *mut kafka_consumer_ConsumerRecordOptionsBuilder_t,
    key: *const c_void,
) {
    unsafe { with_builder(self_, |b| b.set_key(generic(key))) }
}

/// `set_value`: the caller's `void *`, null for Java `null`.
///
/// # Safety
///
/// `self_` must be a valid builder handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecordOptionsBuilder_set_value(
    self_: *mut kafka_consumer_ConsumerRecordOptionsBuilder_t,
    value: *const c_void,
) {
    unsafe { with_builder(self_, |b| b.set_value(generic(value))) }
}

/// `set_headers`: copied during the call.
///
/// # Safety
///
/// `self_` must be a valid builder handle and `headers` a valid record-headers
/// handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecordOptionsBuilder_set_headers(
    self_: *mut kafka_consumer_ConsumerRecordOptionsBuilder_t,
    headers: *const kafka_common_header_internals_RecordHeaders_t,
) {
    let headers = unsafe { record_headers_ref(headers) }.clone();
    unsafe { with_builder(self_, |b| b.set_headers(headers)) }
}

/// `set_leader_epoch`: a negative value is `Optional.empty()`.
///
/// # Safety
///
/// `self_` must be a valid builder handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecordOptionsBuilder_set_leader_epoch(
    self_: *mut kafka_consumer_ConsumerRecordOptionsBuilder_t,
    leader_epoch: i32,
) {
    unsafe { with_builder(self_, |b| b.set_leader_epoch((leader_epoch >= 0).then_some(leader_epoch))) }
}

/// `set_delivery_count`: a negative value is `Optional.empty()`.
///
/// # Safety
///
/// `self_` must be a valid builder handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecordOptionsBuilder_set_delivery_count(
    self_: *mut kafka_consumer_ConsumerRecordOptionsBuilder_t,
    delivery_count: i16,
) {
    unsafe { with_builder(self_, |b| b.set_delivery_count((delivery_count >= 0).then_some(delivery_count))) }
}

/// `build()`: validates the mandatory fields and delivers the options,
/// owned by the caller ([`kafka_consumer_ConsumerRecordOptions_destroy`]),
/// or returns the `IllegalArgumentError`. The builder is consumed either
/// way and only waits to be destroyed.
///
/// # Safety
///
/// `self_` must be a valid builder handle and `out_build` a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecordOptionsBuilder_build(
    self_: *mut kafka_consumer_ConsumerRecordOptionsBuilder_t,
    out_build: *mut *mut kafka_consumer_ConsumerRecordOptions_t,
) -> *mut kafka_common_Error_t {
    let slot = unsafe { &mut *(self_ as *mut Option<GenericBuilder>) };
    let Some(builder) = slot.take() else {
        return box_error(crate::common::Error::local_illegal_state(
            "ConsumerRecordOptionsBuilder already built",
        ));
    };
    match builder.build() {
        Ok(options) => {
            unsafe { *out_build = Box::into_raw(Box::new(options)) as *mut kafka_consumer_ConsumerRecordOptions_t };
            std::ptr::null_mut()
        },
        Err(error) => box_error(error),
    }
}

/// Frees a builder handle; a null pointer is a no-op.
///
/// # Safety
///
/// `self_` must be null or a valid builder handle not used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecordOptionsBuilder_destroy(
    self_: *mut kafka_consumer_ConsumerRecordOptionsBuilder_t,
) {
    if !self_.is_null() {
        unsafe { drop(Box::from_raw(self_ as *mut Option<GenericBuilder>)) };
    }
}

/// Frees an options handle; a null pointer is a no-op.
///
/// # Safety
///
/// `self_` must be null or a valid options handle not used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecordOptions_destroy(
    self_: *mut kafka_consumer_ConsumerRecordOptions_t,
) {
    if !self_.is_null() {
        unsafe { drop(Box::from_raw(self_ as *mut GenericOptions)) };
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::common::record::TimestampType;
    use crate::ffi::common::{error_ref, kafka_common_Error_destroy};
    use crate::ffi::util::box_bytes;

    #[test]
    fn plain_constructor_and_getters() {
        let mut key = 1i32;
        let record = unsafe {
            kafka_consumer_ConsumerRecord_new(
                c"t".as_ptr(),
                3,
                9,
                &mut key as *mut i32 as *const c_void,
                std::ptr::null(),
            )
        };
        unsafe {
            assert_eq!(
                CStr::from_ptr(kafka_consumer_ConsumerRecord_topic(record)).to_str().unwrap(),
                "t"
            );
            assert_eq!(kafka_consumer_ConsumerRecord_partition(record), 3);
            assert_eq!(kafka_consumer_ConsumerRecord_offset(record), 9);
            assert_eq!(kafka_consumer_ConsumerRecord_key(record), &mut key as *mut i32 as *mut c_void);
            assert!(kafka_consumer_ConsumerRecord_value(record).is_null());
            assert_eq!(kafka_consumer_ConsumerRecord_leader_epoch(record), -1);
            assert_eq!(kafka_consumer_ConsumerRecord_delivery_count(record), -1);
            assert_eq!(
                kafka_consumer_ConsumerRecord_timestamp_type(record),
                timestamp_type::singleton(TimestampType::NoTimestampType)
            );
            assert!(!kafka_consumer_ConsumerRecord_headers(record).is_null());
            kafka_consumer_ConsumerRecord_destroy(record);
        }
    }

    #[test]
    fn builder_validates_and_builds() {
        let builder = kafka_consumer_ConsumerRecordOptionsBuilder_new();
        let mut options = std::ptr::null_mut();
        // Missing mandatory fields: the Rust builder's IllegalArgumentError.
        let error = unsafe { kafka_consumer_ConsumerRecordOptionsBuilder_build(builder, &mut options) };
        assert!(!error.is_null());
        assert!(unsafe { error_ref(error) }.error.is_local_illegal_argument_error());
        unsafe {
            kafka_common_Error_destroy(error);
            kafka_consumer_ConsumerRecordOptionsBuilder_destroy(builder);
        }

        let builder = kafka_consumer_ConsumerRecordOptionsBuilder_new();
        unsafe {
            kafka_consumer_ConsumerRecordOptionsBuilder_set_topic(builder, c"t".as_ptr());
            kafka_consumer_ConsumerRecordOptionsBuilder_set_partition(builder, 1);
            kafka_consumer_ConsumerRecordOptionsBuilder_set_offset(builder, 2);
            kafka_consumer_ConsumerRecordOptionsBuilder_set_timestamp(builder, 3);
            kafka_consumer_ConsumerRecordOptionsBuilder_set_timestamp_type(
                builder,
                timestamp_type::singleton(TimestampType::LogAppendTime),
            );
            kafka_consumer_ConsumerRecordOptionsBuilder_set_leader_epoch(builder, 4);
            kafka_consumer_ConsumerRecordOptionsBuilder_set_delivery_count(builder, 5);
            // Key and value are mandatory, as in Rust: Java `null` is an
            // explicit `NULL`.
            kafka_consumer_ConsumerRecordOptionsBuilder_set_key(builder, std::ptr::null());
            kafka_consumer_ConsumerRecordOptionsBuilder_set_value(builder, std::ptr::null());
            assert!(kafka_consumer_ConsumerRecordOptionsBuilder_build(builder, &mut options).is_null());
            kafka_consumer_ConsumerRecordOptionsBuilder_destroy(builder);
            let record = kafka_consumer_ConsumerRecord_with_options(options);
            kafka_consumer_ConsumerRecordOptions_destroy(options);
            assert_eq!(kafka_consumer_ConsumerRecord_timestamp(record), 3);
            assert_eq!(
                kafka_consumer_ConsumerRecord_timestamp_type(record),
                timestamp_type::singleton(TimestampType::LogAppendTime)
            );
            assert_eq!(kafka_consumer_ConsumerRecord_leader_epoch(record), 4);
            assert_eq!(kafka_consumer_ConsumerRecord_delivery_count(record), 5);
            kafka_consumer_ConsumerRecord_destroy(record);
        }
    }

    #[test]
    fn owned_bytes_are_freed_with_the_record() {
        // Exercised for leaks under Miri/valgrind; here it checks the owned
        // path hands out the very pointer the deserializer produced.
        let key = box_bytes(bytes::Bytes::from_static(b"k"));
        let record = box_consumer_record(
            GenericRecord::new("t", 0, 0, Some(GenericValue::new(key as *mut c_void)), None),
            true,
            true,
        );
        unsafe {
            assert_eq!(kafka_consumer_ConsumerRecord_key(record) as *mut kafka_Bytes_t, key);
            assert_eq!((*key).as_slice(), Some(&b"k"[..]));
            kafka_consumer_ConsumerRecord_destroy(record);
        }
    }
}
