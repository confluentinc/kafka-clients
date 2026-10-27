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

//! `kafka_common_ConsumerOffsetOutOfRangeError_t`:
//! `org.apache.kafka.clients.consumer.OffsetOutOfRangeException`.

use std::ffi::c_char;
use std::ptr;

use crate::common::Error;
use crate::consumer::ConsumerOffsetOutOfRangeError;
use crate::ffi::common::errors::{Payload, PayloadClass};
use crate::ffi::common::topic_partition::{
    map_topic_partition_i64, sorted_topic_partition_list, topic_partition_i64_map,
};
use crate::ffi::common::{error_ref, kafka_common_Error_t};
use crate::ffi::ffi_guard;
use crate::ffi::util::{c_str_to_string, kafka_List_t, kafka_Map_t};

/// Opaque handle to a [`ConsumerOffsetOutOfRangeError`].
#[repr(C)]
pub struct kafka_common_ConsumerOffsetOutOfRangeError_t {
    _private: [u8; 0],
}

impl PayloadClass for ConsumerOffsetOutOfRangeError {
    fn message(&self) -> &str {
        ConsumerOffsetOutOfRangeError::message(self)
    }

    fn source(&self) -> Option<&Error> {
        ConsumerOffsetOutOfRangeError::source(self)
    }
}

/// Returns the payload of an `OffsetOutOfRangeException` error, or `NULL` when the error is another
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
pub unsafe extern "C" fn kafka_common_Error_consumer_offset_out_of_range(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_ConsumerOffsetOutOfRangeError_t {
    // SAFETY: Per this function's `# Safety`, `error` is a valid error handle: non-null and live,
    // an owned one from `box_error` or one borrowed from a live owner, as `error_ref` requires. The
    // reference is used only within this call. The view returned is a
    // `Payload<ConsumerOffsetOutOfRangeError>` cloned from the variant's payload into the
    // `ErrorInner`'s payload slot, so it lives as long as the error handle: until the error, or the
    // owner it is borrowed from, is destroyed.
    let inner = unsafe { error_ref(error) };
    match &inner.error {
        Error::ConsumerOffsetOutOfRange(e) => {
            inner.payload_view(e) as *const kafka_common_ConsumerOffsetOutOfRangeError_t
        },
        _ => ptr::null(),
    }
}

/// `new OffsetOutOfRangeException(Map<TopicPartition, Long>
/// offsetOutOfRangePartitions)`.
///
/// # Parameters
///
/// - `offset_out_of_range_partitions` - Map of partition to the out-of-range offset, with
///   `const kafka_common_TopicPartition_t *` keys and `const int64_t *` values
///
/// # Safety
///
/// `offset_out_of_range_partitions` must be `NULL` or a valid map of the
/// stated types; it is copied. The returned handle is owned, freed with
/// `kafka_common_ConsumerOffsetOutOfRangeError_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerOffsetOutOfRangeError_new(
    offset_out_of_range_partitions: *const kafka_Map_t,
) -> *mut kafka_common_ConsumerOffsetOutOfRangeError_t {
    // SAFETY: Per this function's `# Safety`, `offset_out_of_range_partitions` is null or a valid
    // map of the stated types, `kafka_common_TopicPartition_t *` keys and `int64_t *` values, as
    // `map_topic_partition_i64` requires; the entries are copied into an owned map, so nothing
    // borrows it after this call.
    Payload::boxed(ConsumerOffsetOutOfRangeError::new(unsafe {
        map_topic_partition_i64(offset_out_of_range_partitions)
    }))
}

/// `new OffsetOutOfRangeException(String message, Map<TopicPartition, Long>
/// offsetOutOfRangePartitions)`.
///
/// # Parameters
///
/// - `message` - The detail message
/// - `offset_out_of_range_partitions` - Map of partition to the out-of-range offset
///
/// # Safety
///
/// `message` must be a valid NUL-terminated string and
/// `offset_out_of_range_partitions` `NULL` or a valid map of the stated types; it is copied.
///
/// The returned handle is owned by the caller, freed with
/// `kafka_common_ConsumerOffsetOutOfRangeError_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerOffsetOutOfRangeError_with_message(
    message: *const c_char,
    offset_out_of_range_partitions: *const kafka_Map_t,
) -> *mut kafka_common_ConsumerOffsetOutOfRangeError_t {
    Payload::boxed(ConsumerOffsetOutOfRangeError::with_message(
        // SAFETY: Per this function's `# Safety`, `message` is a valid NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(message) },
        // SAFETY: Per this function's `# Safety`, `offset_out_of_range_partitions` is null or a
        // valid map of the stated types, `kafka_common_TopicPartition_t *` keys and `int64_t *`
        // values, as `map_topic_partition_i64` requires; the entries are copied into an owned map,
        // so nothing borrows it after this call.
        unsafe { map_topic_partition_i64(offset_out_of_range_partitions) },
    ))
}

/// `partitions()`: a list of `kafka_common_TopicPartition_t *`, ordered by topic then partition.
///
/// # Returns
///
/// The set of partitions with invalid offsets
///
/// # Safety
///
/// `self_` must be a valid offset-out-of-range handle. The returned list is owned, freed with
/// `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerOffsetOutOfRangeError_partitions(
    self_: *const kafka_common_ConsumerOffsetOutOfRangeError_t,
) -> *mut kafka_List_t {
    sorted_topic_partition_list(
        // SAFETY: Per this function's `# Safety`, `self_` is a valid offset-out-of-range handle: a
        // view whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr`
        // requires (no call mutates a payload of this class). The borrow lasts for this call only:
        // the field is copied into the owned container returned.
        unsafe { Payload::<ConsumerOffsetOutOfRangeError>::from_ptr(self_) }
            .value()
            .partitions(),
    )
}

/// Returns the map of partition to the out-of-range offset (`offsetOutOfRangePartitions()`):
/// `kafka_common_TopicPartition_t *` to `int64_t *`, ordered by topic then partition.
///
/// # Safety
///
/// `self_` must be a valid offset-out-of-range handle. The returned map is owned, freed with
/// `kafka_Map_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerOffsetOutOfRangeError_offset_out_of_range_partitions(
    self_: *const kafka_common_ConsumerOffsetOutOfRangeError_t,
) -> *mut kafka_Map_t {
    topic_partition_i64_map(
        // SAFETY: Per this function's `# Safety`, `self_` is a valid offset-out-of-range handle: a
        // view whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr`
        // requires (no call mutates a payload of this class). The borrow lasts for this call only:
        // the field is copied into the owned container returned.
        unsafe { Payload::<ConsumerOffsetOutOfRangeError>::from_ptr(self_) }
            .value()
            .offset_out_of_range_partitions(),
    )
}

/// Returns the detail message of the error.
///
/// # Safety
///
/// `self_` must be a valid offset-out-of-range handle.
/// The returned string is borrowed, valid until `self_` is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerOffsetOutOfRangeError_message(
    self_: *const kafka_common_ConsumerOffsetOutOfRangeError_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid offset-out-of-range handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). `message_ptr` points at the payload's own C
    // string, so the string lives as long as the handle; that is the rustdoc's "borrowed from the
    // handle".
    unsafe { Payload::<ConsumerOffsetOutOfRangeError>::from_ptr(self_) }.message_ptr()
}

/// Returns the cause of the error, or `NULL` if there is none.
///
/// # Safety
///
/// `self_` must be a valid offset-out-of-range handle.
/// The returned handle is borrowed, valid until `self_` is destroyed.
#[ffi_guard(fallback = ::core::ptr::null())]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerOffsetOutOfRangeError_source(
    self_: *const kafka_common_ConsumerOffsetOutOfRangeError_t,
) -> *const kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid offset-out-of-range handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). `source_ptr` returns the cause handle the
    // payload's `source` slot owns, so it lives as long as the handle; that is the rustdoc's
    // "borrowed from the handle".
    unsafe { Payload::<ConsumerOffsetOutOfRangeError>::from_ptr(self_) }.source_ptr()
}

/// Returns the text form of the error.
///
/// # Safety
///
/// `self_` must be a valid offset-out-of-range handle.
/// The returned string is owned, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerOffsetOutOfRangeError_to_string(
    self_: *const kafka_common_ConsumerOffsetOutOfRangeError_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid offset-out-of-range handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). The borrow lasts for this call only: `to_c_string`
    // copies the text into an owned string.
    unsafe { Payload::<ConsumerOffsetOutOfRangeError>::from_ptr(self_) }.to_c_string()
}

/// Frees an owned handle; `NULL` is a no-op. A view borrowed from a
/// `kafka_common_Error_t` is never passed here.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerOffsetOutOfRangeError_destroy(
    self_: *mut kafka_common_ConsumerOffsetOutOfRangeError_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned handle not yet destroyed,
    // as `Payload::destroy` requires. Owned `kafka_common_ConsumerOffsetOutOfRangeError_t` handles
    // come only from this class's constructors, which box a
    // `Payload<ConsumerOffsetOutOfRangeError>`, and a view is never passed here.
    unsafe { Payload::<ConsumerOffsetOutOfRangeError>::destroy(self_) }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ffi::{CStr, CString};

    use super::*;
    use crate::common::TopicPartition;
    use crate::ffi::common::topic_partition::kafka_common_TopicPartition_partition;
    use crate::ffi::common::{box_error, kafka_common_Error_destroy};
    use crate::ffi::util::{
        kafka_List_destroy, kafka_List_get, kafka_List_size, kafka_Map_destroy, kafka_Map_size, kafka_Map_value,
    };

    #[test]
    fn offsets_cross_as_a_topic_partition_map() {
        let mut offsets = HashMap::new();
        offsets.insert(TopicPartition::new("t", 2), 77i64);
        offsets.insert(TopicPartition::new("t", 0), 5i64);
        let error = box_error(Error::ConsumerOffsetOutOfRange(ConsumerOffsetOutOfRangeError::new(offsets)));
        let message = CString::new("oor").unwrap();
        // SAFETY: Test code: `box_error` and this module's constructors and getters make every
        // handle used. Each owned handle (the error, the payloads, the list and maps, the string)
        // is destroyed once, the view is used only while its error is alive, and `message` outlives
        // the call that reads it.
        unsafe {
            let view = kafka_common_Error_consumer_offset_out_of_range(error);
            assert!(!view.is_null());
            let map = kafka_common_ConsumerOffsetOutOfRangeError_offset_out_of_range_partitions(view);
            assert_eq!(kafka_Map_size(map), 2);
            assert_eq!(*(kafka_Map_value(map, 0) as *const i64), 5);
            assert_eq!(*(kafka_Map_value(map, 1) as *const i64), 77);
            let list = kafka_common_ConsumerOffsetOutOfRangeError_partitions(view);
            assert_eq!(kafka_List_size(list), 2);
            assert_eq!(kafka_common_TopicPartition_partition(kafka_List_get(list, 1) as *const _), 2);
            kafka_List_destroy(list);

            // Feeding the map back builds an equal value.
            let rebuilt = kafka_common_ConsumerOffsetOutOfRangeError_with_message(message.as_ptr(), map);
            kafka_Map_destroy(map);
            assert_eq!(
                CStr::from_ptr(kafka_common_ConsumerOffsetOutOfRangeError_message(rebuilt))
                    .to_str()
                    .unwrap(),
                "oor"
            );
            let again = kafka_common_ConsumerOffsetOutOfRangeError_offset_out_of_range_partitions(rebuilt);
            assert_eq!(kafka_Map_size(again), 2);
            kafka_Map_destroy(again);
            assert!(kafka_common_ConsumerOffsetOutOfRangeError_source(rebuilt).is_null());
            let s = kafka_common_ConsumerOffsetOutOfRangeError_to_string(rebuilt);
            assert!(!s.is_null());
            crate::ffi::util::kafka_string_destroy(s);
            kafka_common_ConsumerOffsetOutOfRangeError_destroy(rebuilt);
            kafka_common_Error_destroy(error);

            let other = box_error(Error::kafka_message("other"));
            assert!(kafka_common_Error_consumer_offset_out_of_range(other).is_null());
            kafka_common_Error_destroy(other);

            kafka_common_ConsumerOffsetOutOfRangeError_destroy(kafka_common_ConsumerOffsetOutOfRangeError_new(
                ptr::null(),
            ));
            kafka_common_ConsumerOffsetOutOfRangeError_destroy(ptr::null_mut());
        }
    }
}
