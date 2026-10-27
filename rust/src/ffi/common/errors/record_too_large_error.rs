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

//! `kafka_common_RecordTooLargeError_t`:
//! `org.apache.kafka.common.errors.RecordTooLargeException`.

use std::ffi::c_char;
use std::ptr;

use crate::common::Error;
use crate::common::errors::RecordTooLargeError;
use crate::ffi::common::errors::{Payload, PayloadClass, take_source};
use crate::ffi::common::topic_partition::{map_topic_partition_i64, topic_partition_i64_map};
use crate::ffi::common::{error_ref, kafka_common_Error_t};
use crate::ffi::ffi_guard;
use crate::ffi::util::{c_str_to_string, kafka_Map_t};

/// Opaque handle to a [`RecordTooLargeError`].
#[repr(C)]
pub struct kafka_common_RecordTooLargeError_t {
    _private: [u8; 0],
}

impl PayloadClass for RecordTooLargeError {
    fn message(&self) -> &str {
        RecordTooLargeError::message(self)
    }

    fn source(&self) -> Option<&Error> {
        RecordTooLargeError::source(self)
    }
}

/// Returns the payload of a `RecordTooLargeException` error, or `NULL` when the error is another
/// class.
///
/// # Parameters
///
/// - `error` - The error handle.
///
/// # Safety
///
/// `error` must be a valid error handle.
/// The returned view is borrowed, valid as long as `error`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_record_too_large_error(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_RecordTooLargeError_t {
    // SAFETY: Per this function's `# Safety`, `error` is a valid error handle: non-null and live,
    // an owned one from `box_error` or one borrowed from a live owner, as `error_ref` requires. The
    // reference is used only within this call. The view returned is a
    // `Payload<RecordTooLargeError>` cloned from the variant's payload into the `ErrorInner`'s
    // payload slot, so it lives as long as the error handle: until the error, or the owner it is
    // borrowed from, is destroyed.
    let inner = unsafe { error_ref(error) };
    match &inner.error {
        Error::RecordTooLarge(e) => inner.payload_view(e) as *const kafka_common_RecordTooLargeError_t,
        _ => ptr::null(),
    }
}

/// `new RecordTooLargeException(String message)`.
///
/// # Parameters
///
/// - `message` - The detail message.
///
/// # Safety
///
/// `message` must be a valid NUL-terminated string. The returned handle is owned, freed with
/// `kafka_common_RecordTooLargeError_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordTooLargeError_new(
    message: *const c_char,
) -> *mut kafka_common_RecordTooLargeError_t {
    // SAFETY: Per this function's `# Safety`, `message` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    Payload::boxed(RecordTooLargeError::new(unsafe { c_str_to_string(message) }))
}

/// `new RecordTooLargeException()` with Java's default message.
///
/// # Safety
///
/// The returned handle is owned, freed with `kafka_common_RecordTooLargeError_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_RecordTooLargeError_with_default_message() -> *mut kafka_common_RecordTooLargeError_t {
    Payload::boxed(RecordTooLargeError::with_default_message())
}

/// `new RecordTooLargeException(String message, Throwable cause)`.
///
/// # Parameters
///
/// - `message` - The detail message.
/// - `source` - The cause of the error.
///
/// # Safety
///
/// `message` must be a valid NUL-terminated string and `source` an owned
/// error handle not yet destroyed; `source` is consumed, so the caller must not destroy it
/// afterwards. The returned handle is owned, freed with `kafka_common_RecordTooLargeError_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordTooLargeError_with_source(
    message: *const c_char,
    source: *mut kafka_common_Error_t,
) -> *mut kafka_common_RecordTooLargeError_t {
    // SAFETY: Per this function's `# Safety`, `message` is a valid NUL-terminated string, as
    // `c_str_to_string` requires, copied into an owned `String`; and `source` an owned error handle
    // not yet destroyed, as `take_source` requires, which the constructor consumes, so the caller
    // does not use it again.
    Payload::boxed(RecordTooLargeError::with_source(unsafe { c_str_to_string(message) }, unsafe {
        take_source(source)
    }))
}

/// `new RecordTooLargeException(String message, Map<TopicPartition, Long>
/// recordTooLargePartitions)`.
///
/// # Parameters
///
/// - `message` - The detail message.
/// - `record_too_large_partitions` - The partitions whose records were too large, with their
///   offsets: `const kafka_common_TopicPartition_t *` keys and `const int64_t *` values, or
///   `NULL`.
///
/// # Safety
///
/// `message` must be a valid NUL-terminated string and
/// `record_too_large_partitions` `NULL` or a valid map of the stated types; both are copied. The
/// returned handle is owned, freed with `kafka_common_RecordTooLargeError_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordTooLargeError_with_record_too_large_partitions(
    message: *const c_char,
    record_too_large_partitions: *const kafka_Map_t,
) -> *mut kafka_common_RecordTooLargeError_t {
    Payload::boxed(RecordTooLargeError::with_record_too_large_partitions(
        // SAFETY: Per this function's `# Safety`, `message` is a valid NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(message) },
        // SAFETY: Per this function's `# Safety`, `record_too_large_partitions` is null or a valid
        // map of the stated types, `kafka_common_TopicPartition_t *` keys and `int64_t *` values,
        // as `map_topic_partition_i64` requires; the entries are copied into an owned map, so
        // nothing borrows it after this call.
        unsafe { map_topic_partition_i64(record_too_large_partitions) },
    ))
}

/// Returns `recordTooLargePartitions()`: a map of `kafka_common_TopicPartition_t *` to
/// `int64_t *`, ordered by topic then partition, or `NULL` when Java's field is null (not an
/// empty map).
///
/// # Safety
///
/// `self_` must be a valid record-too-large handle. The returned map is owned, freed with
/// `kafka_Map_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordTooLargeError_record_too_large_partitions(
    self_: *const kafka_common_RecordTooLargeError_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid record-too-large handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). The borrow lasts for this call only: the field is
    // copied into the owned container returned.
    unsafe { Payload::<RecordTooLargeError>::from_ptr(self_) }
        .value()
        .record_too_large_partitions()
        .map_or(ptr::null_mut(), topic_partition_i64_map)
}

/// Returns the detail message of the error.
///
/// # Safety
///
/// `self_` must be a valid record-too-large handle.
/// The returned string is borrowed, valid until `self_` is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordTooLargeError_message(
    self_: *const kafka_common_RecordTooLargeError_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid record-too-large handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). `message_ptr` points at the payload's own C
    // string, so the string lives as long as the handle; that is the rustdoc's "borrowed from the
    // handle".
    unsafe { Payload::<RecordTooLargeError>::from_ptr(self_) }.message_ptr()
}

/// Returns the cause of the error, or `NULL` if there is none.
///
/// # Safety
///
/// `self_` must be a valid record-too-large handle.
/// The returned handle is borrowed, valid until `self_` is destroyed.
#[ffi_guard(fallback = ::core::ptr::null())]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordTooLargeError_source(
    self_: *const kafka_common_RecordTooLargeError_t,
) -> *const kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid record-too-large handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). `source_ptr` returns the cause handle the
    // payload's `source` slot owns, so it lives as long as the handle; that is the rustdoc's
    // "borrowed from the handle".
    unsafe { Payload::<RecordTooLargeError>::from_ptr(self_) }.source_ptr()
}

/// Returns the text form of the error.
///
/// # Safety
///
/// `self_` must be a valid record-too-large handle.
/// The returned string is owned, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordTooLargeError_to_string(
    self_: *const kafka_common_RecordTooLargeError_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid record-too-large handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). The borrow lasts for this call only: `to_c_string`
    // copies the text into an owned string.
    unsafe { Payload::<RecordTooLargeError>::from_ptr(self_) }.to_c_string()
}

/// Frees an owned handle; `NULL` is a no-op. A view borrowed from a
/// `kafka_common_Error_t` is never passed here.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordTooLargeError_destroy(self_: *mut kafka_common_RecordTooLargeError_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned handle not yet destroyed,
    // as `Payload::destroy` requires. Owned `kafka_common_RecordTooLargeError_t` handles come only
    // from this class's constructors, which box a `Payload<RecordTooLargeError>`, and a view is
    // never passed here.
    unsafe { Payload::<RecordTooLargeError>::destroy(self_) }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ffi::{CStr, CString};

    use super::*;
    use crate::common::TopicPartition;
    use crate::ffi::common::{box_error, kafka_common_Error_destroy, kafka_common_Error_message};
    use crate::ffi::util::{kafka_Map_destroy, kafka_Map_size, kafka_Map_value};

    #[test]
    fn partitions_map_is_nullable_and_round_trips() {
        let mut partitions = HashMap::new();
        partitions.insert(TopicPartition::new("t", 0), 999i64);
        let error = box_error(Error::RecordTooLarge(RecordTooLargeError::with_record_too_large_partitions(
            "m", partitions,
        )));
        let message = CString::new("big").unwrap();
        // SAFETY: Test code: `box_error` and this module's constructors and getters make every
        // handle used. Each owned handle (the errors, the payloads, the maps, the string) is
        // destroyed once, `cause` is consumed by `with_source` and not used again, the views are
        // used only while their error is alive, and `message` outlives the calls that read it.
        unsafe {
            let view = kafka_common_Error_record_too_large_error(error);
            assert!(!view.is_null());
            let map = kafka_common_RecordTooLargeError_record_too_large_partitions(view);
            assert!(!map.is_null());
            assert_eq!(kafka_Map_size(map), 1);
            assert_eq!(*(kafka_Map_value(map, 0) as *const i64), 999);

            // Feeding the map back builds an equal value.
            let rebuilt = kafka_common_RecordTooLargeError_with_record_too_large_partitions(message.as_ptr(), map);
            kafka_Map_destroy(map);
            let again = kafka_common_RecordTooLargeError_record_too_large_partitions(rebuilt);
            assert_eq!(kafka_Map_size(again), 1);
            kafka_Map_destroy(again);
            assert_eq!(
                CStr::from_ptr(kafka_common_RecordTooLargeError_message(rebuilt))
                    .to_str()
                    .unwrap(),
                "big"
            );
            kafka_common_RecordTooLargeError_destroy(rebuilt);
            kafka_common_Error_destroy(error);

            // Java's field defaults to null: the getter returns null, not an
            // empty map.
            let no_partitions = box_error(Error::RecordTooLarge(RecordTooLargeError::new("m")));
            assert!(
                kafka_common_RecordTooLargeError_record_too_large_partitions(
                    kafka_common_Error_record_too_large_error(no_partitions)
                )
                .is_null()
            );
            kafka_common_Error_destroy(no_partitions);

            let other = box_error(Error::kafka_message("other"));
            assert!(kafka_common_Error_record_too_large_error(other).is_null());
            kafka_common_Error_destroy(other);

            let cause = box_error(Error::kafka_message("cause"));
            let with_source = kafka_common_RecordTooLargeError_with_source(message.as_ptr(), cause);
            let source = kafka_common_RecordTooLargeError_source(with_source);
            assert_eq!(CStr::from_ptr(kafka_common_Error_message(source)).to_str().unwrap(), "cause");
            let s = kafka_common_RecordTooLargeError_to_string(with_source);
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                RecordTooLargeError::with_source("big", Error::kafka_message("cause")).to_string()
            );
            crate::ffi::util::kafka_string_destroy(s);
            kafka_common_RecordTooLargeError_destroy(with_source);
            kafka_common_RecordTooLargeError_destroy(kafka_common_RecordTooLargeError_new(message.as_ptr()));
            kafka_common_RecordTooLargeError_destroy(kafka_common_RecordTooLargeError_with_default_message());
            kafka_common_RecordTooLargeError_destroy(ptr::null_mut());
        }
    }
}
