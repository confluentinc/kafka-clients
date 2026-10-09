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

/// The payload of a `RecordTooLargeException` error, borrowed from the error
/// handle, or null when the error is another class. The suffix stays because
/// `kafka_common_Error_record_too_large` is the `Error` factory.
///
/// # Safety
///
/// `error` must be a valid error handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_record_too_large_error(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_RecordTooLargeError_t {
    let inner = unsafe { error_ref(error) };
    match &inner.error {
        Error::RecordTooLarge(e) => inner.payload_view(e) as *const kafka_common_RecordTooLargeError_t,
        _ => ptr::null(),
    }
}

/// `new RecordTooLargeException(String message)`. Owned, freed with
/// [`kafka_common_RecordTooLargeError_destroy`].
///
/// # Safety
///
/// `message` must be a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordTooLargeError_new(
    message: *const c_char,
) -> *mut kafka_common_RecordTooLargeError_t {
    Payload::boxed(RecordTooLargeError::new(unsafe { c_str_to_string(message) }))
}

/// `new RecordTooLargeException()` with Java's default message.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_RecordTooLargeError_with_default_message() -> *mut kafka_common_RecordTooLargeError_t {
    Payload::boxed(RecordTooLargeError::with_default_message())
}

/// `new RecordTooLargeException(String message, Throwable cause)`; `source`
/// is consumed and must not be destroyed by the caller afterwards.
///
/// # Safety
///
/// `message` must be a valid NUL-terminated string and `source` an owned
/// error handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordTooLargeError_with_source(
    message: *const c_char,
    source: *mut kafka_common_Error_t,
) -> *mut kafka_common_RecordTooLargeError_t {
    Payload::boxed(RecordTooLargeError::with_source(unsafe { c_str_to_string(message) }, unsafe {
        take_source(source)
    }))
}

/// `new RecordTooLargeException(String message, Map<TopicPartition, Long>
/// recordTooLargePartitions)`; the map holds
/// `const kafka_common_TopicPartition_t *` keys and `const int64_t *` values,
/// copied.
///
/// # Safety
///
/// `message` must be a valid NUL-terminated string and
/// `record_too_large_partitions` null or a valid map of the stated types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordTooLargeError_with_record_too_large_partitions(
    message: *const c_char,
    record_too_large_partitions: *const kafka_Map_t,
) -> *mut kafka_common_RecordTooLargeError_t {
    Payload::boxed(RecordTooLargeError::with_record_too_large_partitions(
        unsafe { c_str_to_string(message) },
        unsafe { map_topic_partition_i64(record_too_large_partitions) },
    ))
}

/// `recordTooLargePartitions()`: an owned map of
/// `kafka_common_TopicPartition_t *` to `int64_t *`, ordered by topic then
/// partition and freed with `kafka_Map_destroy`, or null when Java's field
/// is null (not an empty map).
///
/// # Safety
///
/// `self_` must be a valid record-too-large handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordTooLargeError_record_too_large_partitions(
    self_: *const kafka_common_RecordTooLargeError_t,
) -> *mut kafka_Map_t {
    unsafe { Payload::<RecordTooLargeError>::from_ptr(self_) }
        .value()
        .record_too_large_partitions()
        .map_or(ptr::null_mut(), topic_partition_i64_map)
}

/// `getMessage()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid record-too-large handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordTooLargeError_message(
    self_: *const kafka_common_RecordTooLargeError_t,
) -> *const c_char {
    unsafe { Payload::<RecordTooLargeError>::from_ptr(self_) }.message_ptr()
}

/// `getCause()`: borrowed from the handle, or null.
///
/// # Safety
///
/// `self_` must be a valid record-too-large handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordTooLargeError_source(
    self_: *const kafka_common_RecordTooLargeError_t,
) -> *const kafka_common_Error_t {
    unsafe { Payload::<RecordTooLargeError>::from_ptr(self_) }.source_ptr()
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid record-too-large handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordTooLargeError_to_string(
    self_: *const kafka_common_RecordTooLargeError_t,
) -> *mut c_char {
    unsafe { Payload::<RecordTooLargeError>::from_ptr(self_) }.to_c_string()
}

/// Frees an owned handle; null is a no-op. A view borrowed from a
/// `kafka_common_Error_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordTooLargeError_destroy(self_: *mut kafka_common_RecordTooLargeError_t) {
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
