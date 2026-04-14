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

//! C FFI functions for the Kafka producer.
//!
//! This module provides `extern "C"` functions that expose the [`Producer`]
//! trait through opaque pointer handles, suitable for use from C, C++, or
//! any language with C FFI support.
//!
//! # Design
//!
//! - **Opaque handles**: [`CProducer`], [`CFutureRecordMetadata`], and
//!   [`CRecordMetadata`] are opaque types. Callers receive and pass raw
//!   pointers to these types; the internal layout is hidden.
//!
//! - **Fixed-width types**: All struct fields and function parameters use
//!   `i32`, `i64`, `bool`, and pointers — never `usize` or `size_t`. This
//!   ensures identical struct layouts on 32-bit and 64-bit platforms.
//!
//! - **Error codes**: Functions return `i32` error codes. `0` means success.
//!   Non-zero values correspond to [`Errors`] enum discriminants.
//!
//! - **Null safety**: All functions check for null pointers and return
//!   appropriate error codes (or do nothing for void functions).
//!
//! [`Producer`]: crate::clients::producer::Producer
//! [`Errors`]: crate::common::protocol::Errors

use std::ffi::{CStr, CString, c_char};
use std::sync::Mutex;

use crate::clients::producer::Producer;
use crate::clients::producer::future_record_metadata::FutureRecordMetadata;
use crate::clients::producer::mock_producer::MockProducer;
use crate::clients::producer::record::ProducerRecord;
use crate::clients::producer::record_metadata::RecordMetadata;
use crate::common::kafka_error::KafkaError;
use crate::common::protocol::Errors;

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// Internal enum wrapping all supported producer implementations.
///
/// Using an enum instead of `dyn Any` makes the FFI layer type-safe and avoids
/// downcasting. New producer kinds (e.g., `Real(KafkaProducer)`) can be added
/// as variants.
enum ProducerKind {
    /// A mock producer for testing.
    Mock(MockProducer),
}

/// Internal wrapper that pairs [`RecordMetadata`] with a [`CString`] for the
/// topic name, so that [`kafka_record_metadata_topic`] can return a valid
/// `*const c_char` that lives as long as the handle.
struct CRecordMetadataInner {
    metadata: RecordMetadata,
    /// Cached CString for the topic, created once at construction time.
    topic_cstring: CString,
}

// ---------------------------------------------------------------------------
// Opaque handle types
// ---------------------------------------------------------------------------

/// Opaque producer handle exposed to C callers.
///
/// Internally wraps a `Box<Mutex<ProducerKind>>`. The `Mutex` provides
/// thread-safe access matching Java's `synchronized` methods.
#[repr(C)]
pub struct CProducer {
    _private: [u8; 0],
}

/// Opaque future handle for a pending send result.
///
/// Internally wraps a `Box<FutureRecordMetadata>`.
#[repr(C)]
pub struct CFutureRecordMetadata {
    _private: [u8; 0],
}

/// Opaque record metadata handle returned after a successful send.
///
/// Internally wraps a `Box<CRecordMetadataInner>`.
#[repr(C)]
pub struct CRecordMetadata {
    _private: [u8; 0],
}

/// A single record in a batch send call.
///
/// All fields use fixed-width types for cross-platform FFI portability.
///
/// # Field Conventions
///
/// - `partition`: Use `-1` for "no partition specified" (let the producer choose).
/// - `key_len`: Use `-1` to indicate no key. When `>= 0`, `key` must point to
///   a valid buffer of that length.
/// - `value_len`: Use `-1` to indicate no value. When `>= 0`, `value` must
///   point to a valid buffer of that length.
#[repr(C)]
pub struct CProducerRecord {
    /// Null-terminated UTF-8 topic name.
    pub topic: *const c_char,
    /// Partition number, or -1 for unset.
    pub partition: i32,
    /// Pointer to key bytes, or null if no key.
    pub key: *const u8,
    /// Key length in bytes, or -1 for no key.
    pub key_len: i32,
    /// Pointer to value bytes, or null if no value.
    pub value: *const u8,
    /// Value length in bytes, or -1 for no value.
    pub value_len: i32,
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Success return code.
const SUCCESS: i32 = 0;

/// Casts a `*mut CProducer` to a reference to the internal `Mutex<ProducerKind>`.
///
/// # Safety
///
/// The pointer must be non-null and must have been created by
/// [`kafka_producer_new_mock`] (or a future constructor).
unsafe fn producer_ref(producer: *mut CProducer) -> &'static Mutex<ProducerKind> {
    unsafe { &*(producer as *mut Mutex<ProducerKind>) }
}

/// Casts a `*mut CFutureRecordMetadata` to a mutable reference to
/// `FutureRecordMetadata`.
///
/// # Safety
///
/// The pointer must be non-null and must have been created by a send function.
unsafe fn future_ref(future: *mut CFutureRecordMetadata) -> &'static mut FutureRecordMetadata {
    unsafe { &mut *(future as *mut FutureRecordMetadata) }
}

/// Casts a `*const CRecordMetadata` to a reference to `CRecordMetadataInner`.
///
/// # Safety
///
/// The pointer must be non-null and must have been created by
/// [`kafka_future_get`].
unsafe fn metadata_ref(metadata: *const CRecordMetadata) -> &'static CRecordMetadataInner {
    unsafe { &*(metadata as *const CRecordMetadataInner) }
}

/// Calls `Producer::send` on the given `ProducerKind`.
fn producer_send(kind: &ProducerKind, record: ProducerRecord) -> Result<FutureRecordMetadata, KafkaError> {
    match kind {
        ProducerKind::Mock(mock) => mock.send(record),
    }
}

/// Builds a [`ProducerRecord`] from raw C FFI parameters.
///
/// # Safety
///
/// - `topic` must be a valid, non-null, null-terminated C string.
/// - `key` must be valid for `key_len` bytes if `key_len >= 0`.
/// - `value` must be valid for `value_len` bytes if `value_len >= 0`.
unsafe fn build_record(
    topic: *const c_char,
    partition: i32,
    key: *const u8,
    key_len: i32,
    value: *const u8,
    value_len: i32,
) -> Result<ProducerRecord, i32> {
    let topic_str = unsafe { CStr::from_ptr(topic) }.to_string_lossy();
    let mut record = ProducerRecord::new(topic_str.as_ref()).map_err(|e| i32::from(e.code()))?;

    if partition >= 0 {
        record = record.with_partition(partition).map_err(|e| i32::from(e.code()))?;
    }

    if key_len >= 0 {
        if key.is_null() {
            return Err(i32::from(Errors::InvalidRequest.code()));
        }
        let key_slice = unsafe { std::slice::from_raw_parts(key, key_len as usize) };
        record = record.with_key(key_slice.to_vec());
    }

    if value_len >= 0 {
        if value.is_null() {
            return Err(i32::from(Errors::InvalidRequest.code()));
        }
        let value_slice = unsafe { std::slice::from_raw_parts(value, value_len as usize) };
        record = record.with_value(value_slice.to_vec());
    }

    Ok(record)
}

/// Wraps a [`FutureRecordMetadata`] into a heap-allocated opaque pointer.
fn box_future(future: FutureRecordMetadata) -> *mut CFutureRecordMetadata {
    Box::into_raw(Box::new(future)) as *mut CFutureRecordMetadata
}

/// Wraps a [`RecordMetadata`] into a heap-allocated opaque pointer, including
/// a cached [`CString`] for the topic name.
fn box_metadata(metadata: RecordMetadata) -> *mut CRecordMetadata {
    // Construct the CString eagerly. If the topic contains an interior NUL
    // (which should never happen for valid Kafka topic names), replace it
    // with a fallback.
    let topic_cstring = CString::new(metadata.topic()).unwrap_or_else(|_| CString::new("").unwrap());
    let inner = CRecordMetadataInner { metadata, topic_cstring };
    Box::into_raw(Box::new(inner)) as *mut CRecordMetadata
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/// Creates a new mock producer.
///
/// # Parameters
///
/// - `auto_complete`: If `true`, sends complete immediately. If `false`, the
///   caller must use [`kafka_mock_producer_complete_next`] or
///   [`kafka_mock_producer_error_next`] to resolve sends.
///
/// # Returns
///
/// A non-null opaque producer handle, or null on failure (should not happen
/// for mock producers).
///
/// # Safety
///
/// The returned handle must eventually be freed with [`kafka_producer_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_producer_new_mock(auto_complete: bool) -> *mut CProducer {
    let kind = ProducerKind::Mock(MockProducer::with_auto_complete(auto_complete));
    let boxed = Box::new(Mutex::new(kind));
    Box::into_raw(boxed) as *mut CProducer
}

/// Destroys a producer handle, freeing all associated resources.
///
/// Safe to call with a null pointer (no-op in that case).
///
/// # Safety
///
/// - `producer` must be null or a valid handle from a `kafka_producer_new_*`
///   function.
/// - After this call, the pointer is invalid and must not be used.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_destroy(producer: *mut CProducer) {
    if !producer.is_null() {
        unsafe {
            drop(Box::from_raw(producer as *mut Mutex<ProducerKind>));
        }
    }
}

// ---------------------------------------------------------------------------
// Send
// ---------------------------------------------------------------------------

/// Sends a single record through the producer.
///
/// # Parameters
///
/// - `producer`: Non-null producer handle.
/// - `topic`: Non-null, null-terminated UTF-8 topic name.
/// - `partition`: Partition number, or `-1` for no partition hint.
/// - `key`: Pointer to key bytes, or null if `key_len` is `-1`.
/// - `key_len`: Key length in bytes, or `-1` for no key.
/// - `value`: Pointer to value bytes, or null if `value_len` is `-1`.
/// - `value_len`: Value length in bytes, or `-1` for no value.
/// - `out_future`: Non-null pointer where the future handle will be written.
///
/// # Returns
///
/// `0` on success, non-zero error code on failure.
///
/// # Safety
///
/// - `producer` must be a valid handle.
/// - `topic` must be a valid C string.
/// - `key` must be valid for `key_len` bytes if `key_len >= 0`.
/// - `value` must be valid for `value_len` bytes if `value_len >= 0`.
/// - `out_future` must be a valid, non-null pointer to a `*mut CFutureRecordMetadata`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_send(
    producer: *mut CProducer,
    topic: *const c_char,
    partition: i32,
    key: *const u8,
    key_len: i32,
    value: *const u8,
    value_len: i32,
    out_future: *mut *mut CFutureRecordMetadata,
) -> i32 {
    if producer.is_null() || topic.is_null() || out_future.is_null() {
        return i32::from(Errors::InvalidRequest.code());
    }

    let record = match unsafe { build_record(topic, partition, key, key_len, value, value_len) } {
        Ok(r) => r,
        Err(code) => return code,
    };

    let producer_mtx = unsafe { producer_ref(producer) };
    let guard = producer_mtx.lock().unwrap();
    match producer_send(&guard, record) {
        Ok(future) => {
            unsafe {
                *out_future = box_future(future);
            }
            SUCCESS
        },
        Err(e) => i32::from(e.code()),
    }
}

/// Sends a batch of records through the producer.
///
/// Iterates over `records[0..count]`, calling send for each. Writes futures
/// to `out_futures[0..count]`. On the first error, stops and returns the
/// error code; futures for successfully sent records prior to the error are
/// still valid and must be destroyed by the caller.
///
/// # Parameters
///
/// - `producer`: Non-null producer handle.
/// - `records`: Non-null pointer to an array of [`CProducerRecord`].
/// - `count`: Number of records in the array (must be `>= 0`).
/// - `out_futures`: Non-null pointer to an array of `*mut CFutureRecordMetadata`
///   with at least `count` entries. Caller must allocate this array.
///
/// # Returns
///
/// `0` on success, non-zero error code on first failure.
///
/// # Safety
///
/// - `records` must point to at least `count` valid [`CProducerRecord`] structs.
/// - `out_futures` must point to at least `count` writable pointer slots.
/// - Each `CProducerRecord.topic` must be a valid C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_send_batch(
    producer: *mut CProducer,
    records: *const CProducerRecord,
    count: i32,
    out_futures: *mut *mut CFutureRecordMetadata,
) -> i32 {
    if producer.is_null() || records.is_null() || out_futures.is_null() || count < 0 {
        return i32::from(Errors::InvalidRequest.code());
    }

    let producer_mtx = unsafe { producer_ref(producer) };
    let guard = producer_mtx.lock().unwrap();
    let count = count as usize;

    for i in 0..count {
        let rec = unsafe { &*records.add(i) };

        if rec.topic.is_null() {
            return i32::from(Errors::InvalidRequest.code());
        }

        let record =
            match unsafe { build_record(rec.topic, rec.partition, rec.key, rec.key_len, rec.value, rec.value_len) } {
                Ok(r) => r,
                Err(code) => return code,
            };

        match producer_send(&guard, record) {
            Ok(future) => unsafe {
                *out_futures.add(i) = box_future(future);
            },
            Err(e) => return i32::from(e.code()),
        }
    }

    SUCCESS
}

// ---------------------------------------------------------------------------
// Future
// ---------------------------------------------------------------------------

/// Checks if a future has resolved.
///
/// This eagerly polls the underlying channel, so it can return `true` even
/// without having been awaited. This matches Java's `Future.isDone()`.
///
/// # Parameters
///
/// - `future`: Non-null future handle.
///
/// # Returns
///
/// `true` if the future has resolved (success or error), `false` if still
/// pending. Returns `false` if `future` is null.
///
/// # Safety
///
/// `future` must be a valid handle from a send function, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_future_is_done(future: *mut CFutureRecordMetadata) -> bool {
    if future.is_null() {
        return false;
    }
    let f = unsafe { future_ref(future) };
    f.is_done()
}

/// Blocks until the future resolves and writes the result to `*out_metadata`.
///
/// On success, `*out_metadata` is set to a valid [`CRecordMetadata`] handle
/// that must be freed with [`kafka_record_metadata_destroy`].
///
/// On failure, `*out_metadata` is set to null and the error code is returned.
///
/// # Parameters
///
/// - `future`: Non-null future handle.
/// - `out_metadata`: Non-null pointer where the metadata handle will be written.
///
/// # Returns
///
/// `0` on success, non-zero error code on failure.
///
/// # Safety
///
/// - `future` must be a valid handle.
/// - `out_metadata` must be a valid, non-null pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_future_get(
    future: *mut CFutureRecordMetadata,
    out_metadata: *mut *mut CRecordMetadata,
) -> i32 {
    if future.is_null() || out_metadata.is_null() {
        return i32::from(Errors::InvalidRequest.code());
    }

    let f = unsafe { future_ref(future) };

    // Create a single-threaded tokio runtime to block on the async get().
    // We use new_current_thread() which only requires the "rt" feature,
    // not "rt-multi-thread".
    let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(_) => {
            unsafe {
                *out_metadata = std::ptr::null_mut();
            }
            return i32::from(Errors::UnknownServerError.code());
        },
    };

    match rt.block_on(f.get()) {
        Ok(metadata) => {
            unsafe {
                *out_metadata = box_metadata(metadata);
            }
            SUCCESS
        },
        Err(e) => {
            unsafe {
                *out_metadata = std::ptr::null_mut();
            }
            i32::from(e.code())
        },
    }
}

/// Destroys a future handle, freeing all associated resources.
///
/// Safe to call with a null pointer (no-op).
///
/// # Safety
///
/// - `future` must be null or a valid handle from a send function.
/// - After this call, the pointer is invalid and must not be used.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_future_destroy(future: *mut CFutureRecordMetadata) {
    if !future.is_null() {
        unsafe {
            drop(Box::from_raw(future as *mut FutureRecordMetadata));
        }
    }
}

// ---------------------------------------------------------------------------
// RecordMetadata
// ---------------------------------------------------------------------------

/// Returns the offset of the record.
///
/// # Parameters
///
/// - `metadata`: Non-null metadata handle.
///
/// # Returns
///
/// The offset, or `-1` if the metadata handle is null.
///
/// # Safety
///
/// `metadata` must be a valid handle from [`kafka_future_get`], or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_record_metadata_offset(metadata: *const CRecordMetadata) -> i64 {
    if metadata.is_null() {
        return -1;
    }
    unsafe { metadata_ref(metadata) }.metadata.offset()
}

/// Returns the topic name as a null-terminated C string.
///
/// The returned pointer is valid until [`kafka_record_metadata_destroy`] is
/// called on the same handle.
///
/// # Parameters
///
/// - `metadata`: Non-null metadata handle.
///
/// # Returns
///
/// A `*const c_char` pointing to the topic name, or null if the metadata
/// handle is null.
///
/// # Safety
///
/// `metadata` must be a valid handle from [`kafka_future_get`], or null.
/// The returned pointer must not be used after the metadata is destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_record_metadata_topic(metadata: *const CRecordMetadata) -> *const c_char {
    if metadata.is_null() {
        return std::ptr::null();
    }
    unsafe { metadata_ref(metadata) }.topic_cstring.as_ptr()
}

/// Returns the partition number of the record.
///
/// # Parameters
///
/// - `metadata`: Non-null metadata handle.
///
/// # Returns
///
/// The partition number, or `-1` if the metadata handle is null.
///
/// # Safety
///
/// `metadata` must be a valid handle from [`kafka_future_get`], or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_record_metadata_partition(metadata: *const CRecordMetadata) -> i32 {
    if metadata.is_null() {
        return -1;
    }
    unsafe { metadata_ref(metadata) }.metadata.partition()
}

/// Destroys a record metadata handle, freeing all associated resources.
///
/// Safe to call with a null pointer (no-op).
///
/// # Safety
///
/// - `metadata` must be null or a valid handle from [`kafka_future_get`].
/// - After this call, the pointer is invalid and must not be used.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_record_metadata_destroy(metadata: *mut CRecordMetadata) {
    if !metadata.is_null() {
        unsafe {
            drop(Box::from_raw(metadata as *mut CRecordMetadataInner));
        }
    }
}

// ---------------------------------------------------------------------------
// Producer operations
// ---------------------------------------------------------------------------

/// Flushes all pending records.
///
/// # Parameters
///
/// - `producer`: Non-null producer handle.
///
/// # Returns
///
/// `0` on success, non-zero error code on failure.
///
/// # Safety
///
/// `producer` must be a valid handle, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_flush(producer: *mut CProducer) -> i32 {
    if producer.is_null() {
        return i32::from(Errors::InvalidRequest.code());
    }

    let producer_mtx = unsafe { producer_ref(producer) };
    let guard = producer_mtx.lock().unwrap();
    match &*guard {
        ProducerKind::Mock(mock) => match mock.flush() {
            Ok(()) => SUCCESS,
            Err(e) => i32::from(e.code()),
        },
    }
}

/// Closes the producer.
///
/// After closing, further send calls will fail.
///
/// # Safety
///
/// `producer` must be a valid handle, or null (no-op).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_close(producer: *mut CProducer) {
    if producer.is_null() {
        return;
    }

    let producer_mtx = unsafe { producer_ref(producer) };
    let guard = producer_mtx.lock().unwrap();
    match &*guard {
        ProducerKind::Mock(mock) => {
            let _ = mock.close();
        },
    }
}

// ---------------------------------------------------------------------------
// Mock-specific operations
// ---------------------------------------------------------------------------

/// Completes the next pending send successfully.
///
/// Only valid for mock producers. Returns `false` if there are no pending
/// completions or if the producer is null or not a mock.
///
/// # Safety
///
/// `producer` must be a valid handle, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_mock_producer_complete_next(producer: *mut CProducer) -> bool {
    if producer.is_null() {
        return false;
    }

    let producer_mtx = unsafe { producer_ref(producer) };
    let guard = producer_mtx.lock().unwrap();
    match &*guard {
        ProducerKind::Mock(mock) => mock.complete_next(),
    }
}

/// Completes the next pending send with an error.
///
/// # Parameters
///
/// - `producer`: Non-null producer handle (must be a mock producer).
/// - `error_code`: Kafka error code (e.g., `2` for `CorruptMessage`).
/// - `error_message`: Optional null-terminated error message, or null to use
///   the default message for the error code.
///
/// # Returns
///
/// `true` if there was a pending completion, `false` otherwise.
///
/// # Safety
///
/// - `producer` must be a valid handle.
/// - `error_message` must be a valid C string or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_mock_producer_error_next(
    producer: *mut CProducer,
    error_code: i32,
    error_message: *const c_char,
) -> bool {
    if producer.is_null() {
        return false;
    }

    let error_enum = Errors::for_code(error_code as i16);
    let error = if error_message.is_null() {
        KafkaError::new(error_enum)
    } else {
        let msg = unsafe { CStr::from_ptr(error_message) }.to_string_lossy();
        KafkaError::with_message(error_enum, msg.as_ref())
    };

    let producer_mtx = unsafe { producer_ref(producer) };
    let guard = producer_mtx.lock().unwrap();
    match &*guard {
        ProducerKind::Mock(mock) => mock.error_next(error),
    }
}

/// Returns the number of records in the sent history.
///
/// # Parameters
///
/// - `producer`: Non-null producer handle (must be a mock producer).
///
/// # Returns
///
/// The number of sent records, or `0` if the producer is null or not a mock.
///
/// # Safety
///
/// `producer` must be a valid handle, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_mock_producer_history_count(producer: *const CProducer) -> i32 {
    if producer.is_null() {
        return 0;
    }

    let producer_mtx = unsafe { &*(producer as *const Mutex<ProducerKind>) };
    let guard = producer_mtx.lock().unwrap();
    match &*guard {
        ProducerKind::Mock(mock) => {
            let count = mock.history().len();
            // Clamp to i32::MAX to avoid overflow (extremely unlikely in practice).
            count.min(i32::MAX as usize) as i32
        },
    }
}

/// Clears the sent history and pending completions.
///
/// # Safety
///
/// `producer` must be a valid handle, or null (no-op).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_mock_producer_clear(producer: *mut CProducer) {
    if producer.is_null() {
        return;
    }

    let producer_mtx = unsafe { producer_ref(producer) };
    let guard = producer_mtx.lock().unwrap();
    match &*guard {
        ProducerKind::Mock(mock) => mock.clear(),
    }
}

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Returns a human-readable description for a Kafka error code.
///
/// The returned pointer points to a static string and is valid for the
/// lifetime of the program. It must not be freed by the caller.
///
/// # Parameters
///
/// - `error_code`: A Kafka error code (e.g., `0` for no error, `2` for
///   `CorruptMessage`).
///
/// # Returns
///
/// A `*const c_char` pointing to a null-terminated UTF-8 error description.
/// Returns a pointer to an empty string for error code `0` (no error).
#[unsafe(no_mangle)]
pub extern "C" fn kafka_error_message(error_code: i32) -> *const c_char {
    let error = Errors::for_code(error_code as i16);
    error_message_ptr(error)
}

/// Returns a static, null-terminated C string pointer for the given error.
///
/// Uses a `std::sync::LazyLock` table to create `CString`s exactly once.
fn error_message_ptr(error: Errors) -> *const c_char {
    use std::sync::LazyLock;

    // Build a lookup table of all error messages as CStrings.
    // Indexed by (code + 1) to handle code -1 (UnknownServerError).
    // Range: -1..=133 -> index 0..=134
    static ERROR_MESSAGES: LazyLock<Vec<CString>> = LazyLock::new(|| {
        let mut table = Vec::with_capacity(135);
        for code in -1..=133_i16 {
            let error = Errors::for_code(code);
            let msg = error.message();
            // All Kafka error messages are valid ASCII/UTF-8 without NUL bytes.
            let cstring = CString::new(msg).unwrap_or_else(|_| CString::new("").unwrap());
            table.push(cstring);
        }
        table
    });

    let code = error.code();
    let index = (i32::from(code) + 1) as usize;
    if index < ERROR_MESSAGES.len() {
        ERROR_MESSAGES[index].as_ptr()
    } else {
        // Unknown code -- return the UnknownServerError message (index 0).
        ERROR_MESSAGES[0].as_ptr()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Lifecycle tests ----------------------------------------------------

    #[test]
    fn test_create_and_destroy_mock_producer() {
        let producer = kafka_producer_new_mock(true);
        assert!(!producer.is_null());
        unsafe {
            kafka_producer_destroy(producer);
        }
    }

    #[test]
    fn test_destroy_null_is_noop() {
        unsafe {
            kafka_producer_destroy(std::ptr::null_mut());
        }
    }

    // -- Send tests ---------------------------------------------------------

    #[test]
    fn test_send_auto_complete() {
        let producer = kafka_producer_new_mock(true);
        let topic = CString::new("test-topic").unwrap();
        let key = b"key";
        let value = b"value";
        let mut future: *mut CFutureRecordMetadata = std::ptr::null_mut();

        unsafe {
            let result = kafka_producer_send(
                producer,
                topic.as_ptr(),
                -1,
                key.as_ptr(),
                key.len() as i32,
                value.as_ptr(),
                value.len() as i32,
                &mut future,
            );
            assert_eq!(result, SUCCESS);
            assert!(!future.is_null());
            assert!(kafka_future_is_done(future));

            kafka_future_destroy(future);
            kafka_producer_destroy(producer);
        }
    }

    #[test]
    fn test_send_manual_complete() {
        let producer = kafka_producer_new_mock(false);
        let topic = CString::new("test-topic").unwrap();
        let mut future: *mut CFutureRecordMetadata = std::ptr::null_mut();

        unsafe {
            let result = kafka_producer_send(
                producer,
                topic.as_ptr(),
                -1,
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut future,
            );
            assert_eq!(result, SUCCESS);
            assert!(!future.is_null());
            assert!(!kafka_future_is_done(future));

            // Complete it
            assert!(kafka_mock_producer_complete_next(producer));
            assert!(kafka_future_is_done(future));

            kafka_future_destroy(future);
            kafka_producer_destroy(producer);
        }
    }

    #[test]
    fn test_send_null_producer() {
        let topic = CString::new("topic").unwrap();
        let mut future: *mut CFutureRecordMetadata = std::ptr::null_mut();

        unsafe {
            let result = kafka_producer_send(
                std::ptr::null_mut(),
                topic.as_ptr(),
                -1,
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut future,
            );
            assert_ne!(result, SUCCESS);
        }
    }

    #[test]
    fn test_send_null_topic() {
        let producer = kafka_producer_new_mock(true);
        let mut future: *mut CFutureRecordMetadata = std::ptr::null_mut();

        unsafe {
            let result = kafka_producer_send(
                producer,
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut future,
            );
            assert_ne!(result, SUCCESS);
            kafka_producer_destroy(producer);
        }
    }

    #[test]
    fn test_send_null_out_future() {
        let producer = kafka_producer_new_mock(true);
        let topic = CString::new("topic").unwrap();

        unsafe {
            let result = kafka_producer_send(
                producer,
                topic.as_ptr(),
                -1,
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                std::ptr::null_mut(),
            );
            assert_ne!(result, SUCCESS);
            kafka_producer_destroy(producer);
        }
    }

    #[test]
    fn test_send_with_partition() {
        let producer = kafka_producer_new_mock(true);
        let topic = CString::new("topic").unwrap();
        let mut future: *mut CFutureRecordMetadata = std::ptr::null_mut();

        unsafe {
            let result = kafka_producer_send(
                producer,
                topic.as_ptr(),
                3,
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut future,
            );
            assert_eq!(result, SUCCESS);

            // Get metadata and verify partition
            let mut metadata: *mut CRecordMetadata = std::ptr::null_mut();
            let get_result = kafka_future_get(future, &mut metadata);
            assert_eq!(get_result, SUCCESS);
            assert!(!metadata.is_null());
            assert_eq!(kafka_record_metadata_partition(metadata), 3);

            kafka_record_metadata_destroy(metadata);
            kafka_future_destroy(future);
            kafka_producer_destroy(producer);
        }
    }

    // -- Batch send tests ---------------------------------------------------

    #[test]
    fn test_send_batch() {
        let producer = kafka_producer_new_mock(true);
        let topic1 = CString::new("topic1").unwrap();
        let topic2 = CString::new("topic2").unwrap();
        let key = b"key";
        let value = b"value";

        let records = [
            CProducerRecord {
                topic: topic1.as_ptr(),
                partition: -1,
                key: key.as_ptr(),
                key_len: key.len() as i32,
                value: value.as_ptr(),
                value_len: value.len() as i32,
            },
            CProducerRecord {
                topic: topic2.as_ptr(),
                partition: 1,
                key: std::ptr::null(),
                key_len: -1,
                value: std::ptr::null(),
                value_len: -1,
            },
        ];

        let mut futures: [*mut CFutureRecordMetadata; 2] = [std::ptr::null_mut(), std::ptr::null_mut()];

        unsafe {
            let result = kafka_producer_send_batch(producer, records.as_ptr(), 2, futures.as_mut_ptr());
            assert_eq!(result, SUCCESS);

            for f in &futures {
                assert!(!f.is_null());
                assert!(kafka_future_is_done(*f));
            }

            // Check history count
            assert_eq!(kafka_mock_producer_history_count(producer as *const _), 2);

            for f in &futures {
                kafka_future_destroy(*f);
            }
            kafka_producer_destroy(producer);
        }
    }

    #[test]
    fn test_send_batch_null_params() {
        let producer = kafka_producer_new_mock(true);
        let topic = CString::new("topic").unwrap();
        let records = [CProducerRecord {
            topic: topic.as_ptr(),
            partition: -1,
            key: std::ptr::null(),
            key_len: -1,
            value: std::ptr::null(),
            value_len: -1,
        }];
        let mut futures: [*mut CFutureRecordMetadata; 1] = [std::ptr::null_mut()];

        unsafe {
            // Null producer
            assert_ne!(
                kafka_producer_send_batch(std::ptr::null_mut(), records.as_ptr(), 1, futures.as_mut_ptr(),),
                SUCCESS
            );

            // Null records
            assert_ne!(
                kafka_producer_send_batch(producer, std::ptr::null(), 1, futures.as_mut_ptr(),),
                SUCCESS
            );

            // Null out_futures
            assert_ne!(
                kafka_producer_send_batch(producer, records.as_ptr(), 1, std::ptr::null_mut(),),
                SUCCESS
            );

            // Negative count
            assert_ne!(
                kafka_producer_send_batch(producer, records.as_ptr(), -1, futures.as_mut_ptr(),),
                SUCCESS
            );

            kafka_producer_destroy(producer);
        }
    }

    #[test]
    fn test_send_batch_zero_count() {
        let producer = kafka_producer_new_mock(true);
        let mut futures: *mut CFutureRecordMetadata = std::ptr::null_mut();

        unsafe {
            // Zero count with non-null pointers is valid -- no records sent.
            // Use a dummy non-null pointer for records since count is 0.
            let dummy_record = CProducerRecord {
                topic: std::ptr::null(),
                partition: -1,
                key: std::ptr::null(),
                key_len: -1,
                value: std::ptr::null(),
                value_len: -1,
            };
            let result = kafka_producer_send_batch(producer, &dummy_record, 0, &mut futures);
            assert_eq!(result, SUCCESS);
            assert_eq!(kafka_mock_producer_history_count(producer as *const _), 0);

            kafka_producer_destroy(producer);
        }
    }

    // -- Future tests -------------------------------------------------------

    #[test]
    fn test_future_get_success() {
        let producer = kafka_producer_new_mock(true);
        let topic = CString::new("my-topic").unwrap();
        let mut future: *mut CFutureRecordMetadata = std::ptr::null_mut();

        unsafe {
            kafka_producer_send(
                producer,
                topic.as_ptr(),
                0,
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut future,
            );

            let mut metadata: *mut CRecordMetadata = std::ptr::null_mut();
            let result = kafka_future_get(future, &mut metadata);
            assert_eq!(result, SUCCESS);
            assert!(!metadata.is_null());

            assert_eq!(kafka_record_metadata_offset(metadata), 0);
            assert_eq!(kafka_record_metadata_partition(metadata), 0);

            // Check topic
            let topic_ptr = kafka_record_metadata_topic(metadata);
            assert!(!topic_ptr.is_null());
            let topic_str = CStr::from_ptr(topic_ptr).to_str().unwrap();
            assert_eq!(topic_str, "my-topic");

            kafka_record_metadata_destroy(metadata);
            kafka_future_destroy(future);
            kafka_producer_destroy(producer);
        }
    }

    #[test]
    fn test_future_get_error() {
        let producer = kafka_producer_new_mock(false);
        let topic = CString::new("topic").unwrap();
        let mut future: *mut CFutureRecordMetadata = std::ptr::null_mut();

        unsafe {
            kafka_producer_send(
                producer,
                topic.as_ptr(),
                -1,
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut future,
            );

            let err_msg = CString::new("test error").unwrap();
            kafka_mock_producer_error_next(producer, i32::from(Errors::CorruptMessage.code()), err_msg.as_ptr());

            let mut metadata: *mut CRecordMetadata = std::ptr::null_mut();
            let result = kafka_future_get(future, &mut metadata);
            assert_ne!(result, SUCCESS);
            assert!(metadata.is_null());

            kafka_future_destroy(future);
            kafka_producer_destroy(producer);
        }
    }

    #[test]
    fn test_future_is_done_null() {
        unsafe {
            assert!(!kafka_future_is_done(std::ptr::null_mut()));
        }
    }

    #[test]
    fn test_future_get_null_params() {
        unsafe {
            let mut metadata: *mut CRecordMetadata = std::ptr::null_mut();
            assert_ne!(kafka_future_get(std::ptr::null_mut(), &mut metadata), SUCCESS);
        }
    }

    #[test]
    fn test_future_destroy_null() {
        unsafe {
            kafka_future_destroy(std::ptr::null_mut());
        }
    }

    // -- RecordMetadata tests -----------------------------------------------

    #[test]
    fn test_metadata_null_returns_defaults() {
        unsafe {
            assert_eq!(kafka_record_metadata_offset(std::ptr::null()), -1);
            assert_eq!(kafka_record_metadata_partition(std::ptr::null()), -1);
            assert!(kafka_record_metadata_topic(std::ptr::null()).is_null());
        }
    }

    #[test]
    fn test_metadata_destroy_null() {
        unsafe {
            kafka_record_metadata_destroy(std::ptr::null_mut());
        }
    }

    // -- Flush and close tests ----------------------------------------------

    #[test]
    fn test_flush() {
        let producer = kafka_producer_new_mock(false);
        let topic = CString::new("topic").unwrap();
        let mut future: *mut CFutureRecordMetadata = std::ptr::null_mut();

        unsafe {
            kafka_producer_send(
                producer,
                topic.as_ptr(),
                -1,
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut future,
            );

            assert!(!kafka_future_is_done(future));

            let result = kafka_producer_flush(producer);
            assert_eq!(result, SUCCESS);

            assert!(kafka_future_is_done(future));

            kafka_future_destroy(future);
            kafka_producer_destroy(producer);
        }
    }

    #[test]
    fn test_flush_null() {
        unsafe {
            assert_ne!(kafka_producer_flush(std::ptr::null_mut()), SUCCESS);
        }
    }

    #[test]
    fn test_close_and_send_fails() {
        let producer = kafka_producer_new_mock(true);
        let topic = CString::new("topic").unwrap();

        unsafe {
            kafka_producer_close(producer);

            let mut future: *mut CFutureRecordMetadata = std::ptr::null_mut();
            let result = kafka_producer_send(
                producer,
                topic.as_ptr(),
                -1,
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut future,
            );
            assert_ne!(result, SUCCESS);

            kafka_producer_destroy(producer);
        }
    }

    #[test]
    fn test_close_null() {
        unsafe {
            kafka_producer_close(std::ptr::null_mut());
        }
    }

    // -- Mock-specific tests ------------------------------------------------

    #[test]
    fn test_mock_complete_next_no_pending() {
        let producer = kafka_producer_new_mock(false);
        unsafe {
            assert!(!kafka_mock_producer_complete_next(producer));
            kafka_producer_destroy(producer);
        }
    }

    #[test]
    fn test_mock_complete_next_null() {
        unsafe {
            assert!(!kafka_mock_producer_complete_next(std::ptr::null_mut()));
        }
    }

    #[test]
    fn test_mock_error_next_no_pending() {
        let producer = kafka_producer_new_mock(false);
        let msg = CString::new("err").unwrap();
        unsafe {
            assert!(!kafka_mock_producer_error_next(producer, 2, msg.as_ptr()));
            kafka_producer_destroy(producer);
        }
    }

    #[test]
    fn test_mock_error_next_null_message() {
        let producer = kafka_producer_new_mock(false);
        let topic = CString::new("topic").unwrap();
        let mut future: *mut CFutureRecordMetadata = std::ptr::null_mut();

        unsafe {
            kafka_producer_send(
                producer,
                topic.as_ptr(),
                -1,
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut future,
            );

            // Error with null message -- should use default message.
            assert!(kafka_mock_producer_error_next(
                producer,
                i32::from(Errors::CorruptMessage.code()),
                std::ptr::null(),
            ));

            let mut metadata: *mut CRecordMetadata = std::ptr::null_mut();
            let result = kafka_future_get(future, &mut metadata);
            assert_ne!(result, SUCCESS);
            assert!(metadata.is_null());

            kafka_future_destroy(future);
            kafka_producer_destroy(producer);
        }
    }

    #[test]
    fn test_mock_error_next_null_producer() {
        let msg = CString::new("err").unwrap();
        unsafe {
            assert!(!kafka_mock_producer_error_next(std::ptr::null_mut(), 2, msg.as_ptr()));
        }
    }

    #[test]
    fn test_mock_history_count() {
        let producer = kafka_producer_new_mock(true);
        let topic = CString::new("topic").unwrap();

        unsafe {
            assert_eq!(kafka_mock_producer_history_count(producer as *const _), 0);

            let mut f1: *mut CFutureRecordMetadata = std::ptr::null_mut();
            kafka_producer_send(
                producer,
                topic.as_ptr(),
                -1,
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut f1,
            );
            assert_eq!(kafka_mock_producer_history_count(producer as *const _), 1);

            let mut f2: *mut CFutureRecordMetadata = std::ptr::null_mut();
            kafka_producer_send(
                producer,
                topic.as_ptr(),
                -1,
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut f2,
            );
            assert_eq!(kafka_mock_producer_history_count(producer as *const _), 2);

            kafka_future_destroy(f1);
            kafka_future_destroy(f2);
            kafka_producer_destroy(producer);
        }
    }

    #[test]
    fn test_mock_history_count_null() {
        unsafe {
            assert_eq!(kafka_mock_producer_history_count(std::ptr::null()), 0);
        }
    }

    #[test]
    fn test_mock_clear() {
        let producer = kafka_producer_new_mock(true);
        let topic = CString::new("topic").unwrap();

        unsafe {
            let mut f: *mut CFutureRecordMetadata = std::ptr::null_mut();
            kafka_producer_send(producer, topic.as_ptr(), -1, std::ptr::null(), -1, std::ptr::null(), -1, &mut f);
            assert_eq!(kafka_mock_producer_history_count(producer as *const _), 1);

            kafka_mock_producer_clear(producer);
            assert_eq!(kafka_mock_producer_history_count(producer as *const _), 0);

            kafka_future_destroy(f);
            kafka_producer_destroy(producer);
        }
    }

    #[test]
    fn test_mock_clear_null() {
        unsafe {
            kafka_mock_producer_clear(std::ptr::null_mut());
        }
    }

    // -- Error message tests ------------------------------------------------

    #[test]
    fn test_error_message_known_codes() {
        unsafe {
            // Success (0)
            let msg = kafka_error_message(0);
            assert!(!msg.is_null());
            let s = CStr::from_ptr(msg).to_str().unwrap();
            assert!(s.is_empty(), "Error code 0 should return empty string");

            // CorruptMessage (2)
            let msg = kafka_error_message(2);
            assert!(!msg.is_null());
            let s = CStr::from_ptr(msg).to_str().unwrap();
            assert!(s.contains("CRC checksum"));

            // UnknownServerError (-1)
            let msg = kafka_error_message(-1);
            assert!(!msg.is_null());
            let s = CStr::from_ptr(msg).to_str().unwrap();
            assert!(s.contains("unexpected error"));
        }
    }

    #[test]
    fn test_error_message_unknown_code() {
        unsafe {
            let msg = kafka_error_message(9999);
            assert!(!msg.is_null());
            let s = CStr::from_ptr(msg).to_str().unwrap();
            // Unknown codes map to UnknownServerError
            assert!(s.contains("unexpected error"));
        }
    }

    // -- Integration-style round-trip tests ---------------------------------

    #[test]
    fn test_full_send_get_destroy_cycle() {
        let producer = kafka_producer_new_mock(true);
        let topic = CString::new("round-trip").unwrap();
        let key = b"my-key";
        let value = b"my-value";

        unsafe {
            // Send
            let mut future: *mut CFutureRecordMetadata = std::ptr::null_mut();
            let send_result = kafka_producer_send(
                producer,
                topic.as_ptr(),
                2,
                key.as_ptr(),
                key.len() as i32,
                value.as_ptr(),
                value.len() as i32,
                &mut future,
            );
            assert_eq!(send_result, SUCCESS);

            // Get metadata
            let mut metadata: *mut CRecordMetadata = std::ptr::null_mut();
            let get_result = kafka_future_get(future, &mut metadata);
            assert_eq!(get_result, SUCCESS);

            // Verify metadata
            assert_eq!(kafka_record_metadata_offset(metadata), 0);
            assert_eq!(kafka_record_metadata_partition(metadata), 2);
            let topic_ptr = kafka_record_metadata_topic(metadata);
            let topic_str = CStr::from_ptr(topic_ptr).to_str().unwrap();
            assert_eq!(topic_str, "round-trip");

            // History
            assert_eq!(kafka_mock_producer_history_count(producer as *const _), 1);

            // Clean up
            kafka_record_metadata_destroy(metadata);
            kafka_future_destroy(future);
            kafka_producer_destroy(producer);
        }
    }

    #[test]
    fn test_multiple_sends_incrementing_offsets() {
        let producer = kafka_producer_new_mock(true);
        let topic = CString::new("topic").unwrap();

        unsafe {
            for expected_offset in 0..3_i64 {
                let mut future: *mut CFutureRecordMetadata = std::ptr::null_mut();
                kafka_producer_send(
                    producer,
                    topic.as_ptr(),
                    0,
                    std::ptr::null(),
                    -1,
                    std::ptr::null(),
                    -1,
                    &mut future,
                );

                let mut metadata: *mut CRecordMetadata = std::ptr::null_mut();
                let result = kafka_future_get(future, &mut metadata);
                assert_eq!(result, SUCCESS);
                assert_eq!(kafka_record_metadata_offset(metadata), expected_offset);

                kafka_record_metadata_destroy(metadata);
                kafka_future_destroy(future);
            }

            kafka_producer_destroy(producer);
        }
    }

    #[test]
    fn test_send_after_close_returns_error() {
        let producer = kafka_producer_new_mock(true);
        let topic = CString::new("topic").unwrap();

        unsafe {
            kafka_producer_close(producer);

            let mut future: *mut CFutureRecordMetadata = std::ptr::null_mut();
            let result = kafka_producer_send(
                producer,
                topic.as_ptr(),
                -1,
                std::ptr::null(),
                -1,
                std::ptr::null(),
                -1,
                &mut future,
            );
            assert_ne!(result, SUCCESS, "Send after close should fail");
            assert!(future.is_null(), "Future should be null on error");

            kafka_producer_destroy(producer);
        }
    }

    #[test]
    fn test_flush_after_close_returns_error() {
        let producer = kafka_producer_new_mock(true);

        unsafe {
            kafka_producer_close(producer);

            let result = kafka_producer_flush(producer);
            assert_ne!(result, SUCCESS, "Flush after close should fail");

            kafka_producer_destroy(producer);
        }
    }

    #[test]
    fn test_send_only_key() {
        let producer = kafka_producer_new_mock(true);
        let topic = CString::new("topic").unwrap();
        let key = b"only-key";
        let mut future: *mut CFutureRecordMetadata = std::ptr::null_mut();

        unsafe {
            let result = kafka_producer_send(
                producer,
                topic.as_ptr(),
                -1,
                key.as_ptr(),
                key.len() as i32,
                std::ptr::null(),
                -1,
                &mut future,
            );
            assert_eq!(result, SUCCESS);
            assert!(kafka_future_is_done(future));

            kafka_future_destroy(future);
            kafka_producer_destroy(producer);
        }
    }

    #[test]
    fn test_send_only_value() {
        let producer = kafka_producer_new_mock(true);
        let topic = CString::new("topic").unwrap();
        let value = b"only-value";
        let mut future: *mut CFutureRecordMetadata = std::ptr::null_mut();

        unsafe {
            let result = kafka_producer_send(
                producer,
                topic.as_ptr(),
                -1,
                std::ptr::null(),
                -1,
                value.as_ptr(),
                value.len() as i32,
                &mut future,
            );
            assert_eq!(result, SUCCESS);
            assert!(kafka_future_is_done(future));

            kafka_future_destroy(future);
            kafka_producer_destroy(producer);
        }
    }
}
