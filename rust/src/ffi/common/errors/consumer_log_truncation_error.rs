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

//! `kafka_common_ConsumerLogTruncationError_t`:
//! `org.apache.kafka.clients.consumer.LogTruncationException`.

use std::collections::HashMap;
use std::ffi::{c_char, c_void};
use std::ptr;

use crate::common::{Error, TopicPartition};
use crate::consumer::{ConsumerLogTruncationError, OffsetAndMetadata};
use crate::ffi::common::errors::{Payload, PayloadClass};
use crate::ffi::common::topic_partition::{
    TopicPartitionInner, box_topic_partition, kafka_common_TopicPartition_t, map_topic_partition_i64,
    sorted_topic_partition_list, topic_partition_i64_map, topic_partition_key_eq, topic_partition_ref,
};
use crate::ffi::common::{error_ref, kafka_common_Error_t};
use crate::ffi::consumer::{
    OffsetAndMetadataInner, box_offset_and_metadata, kafka_consumer_OffsetAndMetadata_t, offset_and_metadata_ref,
};
use crate::ffi::ffi_guard;
use crate::ffi::util::{box_map, c_str_to_string, destroy_boxed, kafka_List_t, kafka_Map_t, map_entries};

/// Opaque handle to a [`ConsumerLogTruncationError`].
#[repr(C)]
pub struct kafka_common_ConsumerLogTruncationError_t {
    _private: [u8; 0],
}

impl PayloadClass for ConsumerLogTruncationError {
    fn message(&self) -> &str {
        ConsumerLogTruncationError::message(self)
    }

    fn source(&self) -> Option<&Error> {
        ConsumerLogTruncationError::source(self)
    }
}

/// Hands a Java `Map<TopicPartition, OffsetAndMetadata>` to C as an owned
/// map: keys are `kafka_common_TopicPartition_t *`, values
/// `kafka_consumer_OffsetAndMetadata_t *`, entries ordered by topic then
/// partition, and `kafka_Map_get` compares keys by value.
pub(crate) fn topic_partition_offset_and_metadata_map(
    map: &HashMap<TopicPartition, OffsetAndMetadata>,
) -> *mut kafka_Map_t {
    let mut entries: Vec<(&TopicPartition, &OffsetAndMetadata)> = map.iter().collect();
    entries.sort_by(|(a, _), (b, _)| a.topic().cmp(b.topic()).then(a.partition().cmp(&b.partition())));
    let entries = entries
        .into_iter()
        .map(|(tp, oam)| {
            (
                box_topic_partition(tp.clone()) as *mut c_void,
                box_offset_and_metadata(oam.clone()) as *mut c_void,
            )
        })
        .collect();
    box_map(
        entries,
        Some(destroy_boxed::<TopicPartitionInner>),
        Some(destroy_boxed::<OffsetAndMetadataInner>),
        Some(topic_partition_key_eq),
    )
}

/// Reads a map of `const kafka_common_TopicPartition_t *` keys and
/// `const kafka_consumer_OffsetAndMetadata_t *` values; null reads as empty.
///
/// # Safety
///
/// `map` must be null or a valid map of the stated key and value handles.
pub(crate) unsafe fn map_topic_partition_offset_and_metadata(
    map: *const kafka_Map_t,
) -> HashMap<TopicPartition, OffsetAndMetadata> {
    // SAFETY: Per this function's `# Safety`, `map` is null or a valid map, alive for this call,
    // and `kafka_Map_put` forbids any other call from using it meanwhile, so it is not mutated
    // while borrowed, as `map_entries` requires. The entries are cloned out before return.
    unsafe { map_entries(map) }
        .iter()
        .map(|&(k, v)| {
            (
                // SAFETY: Per this function's `# Safety`, every key is a valid topic-partition
                // handle, as `topic_partition_ref` requires; it is cloned, so the borrow lasts for
                // this call only.
                unsafe { topic_partition_ref(k as *const kafka_common_TopicPartition_t) }.clone(),
                // SAFETY: Per this function's `# Safety`, every value is a valid
                // offset-and-metadata handle, as `offset_and_metadata_ref` requires; it is cloned,
                // so the borrow lasts for this call only.
                unsafe { offset_and_metadata_ref(v as *const kafka_consumer_OffsetAndMetadata_t) }.clone(),
            )
        })
        .collect()
}

/// The payload of a `LogTruncationException` error, borrowed from the error
/// handle, or null when the error is another class.
///
/// # Safety
///
/// `error` must be a valid error handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_consumer_log_truncation(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_ConsumerLogTruncationError_t {
    // SAFETY: Per this function's `# Safety`, `error` is a valid error handle: non-null and live,
    // an owned one from `box_error` or one borrowed from a live owner, as `error_ref` requires. The
    // reference is used only within this call. The view returned is a
    // `Payload<ConsumerLogTruncationError>` cloned from the variant's payload into the
    // `ErrorInner`'s payload slot, so it lives as long as the error handle: until the error, or the
    // owner it is borrowed from, is destroyed.
    let inner = unsafe { error_ref(error) };
    match &inner.error {
        Error::ConsumerLogTruncation(e) => {
            inner.payload_view(e.as_ref()) as *const kafka_common_ConsumerLogTruncationError_t
        },
        _ => ptr::null(),
    }
}

/// `new LogTruncationException(Map<TopicPartition, Long>
/// offsetOutOfRangePartitions, Map<TopicPartition, OffsetAndMetadata>
/// divergentOffsets)`, with Java's default message. The first map holds
/// `const kafka_common_TopicPartition_t *` keys and `const int64_t *`
/// values, the second the same keys and
/// `const kafka_consumer_OffsetAndMetadata_t *` values; both are copied.
/// Owned, freed with [`kafka_common_ConsumerLogTruncationError_destroy`].
///
/// # Safety
///
/// Each map must be null or a valid map of the stated types.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerLogTruncationError_new(
    offset_out_of_range_partitions: *const kafka_Map_t,
    divergent_offsets: *const kafka_Map_t,
) -> *mut kafka_common_ConsumerLogTruncationError_t {
    Payload::boxed(ConsumerLogTruncationError::new(
        // SAFETY: Per this function's `# Safety`, `offset_out_of_range_partitions` is null or a
        // valid map of the stated types, `kafka_common_TopicPartition_t *` keys and `int64_t *`
        // values, as `map_topic_partition_i64` requires; the entries are copied into an owned map,
        // so nothing borrows it after this call.
        unsafe { map_topic_partition_i64(offset_out_of_range_partitions) },
        // SAFETY: Per this function's `# Safety`, `divergent_offsets` is null or a valid map of the
        // stated types, `kafka_common_TopicPartition_t *` keys and
        // `kafka_consumer_OffsetAndMetadata_t *` values, as
        // `map_topic_partition_offset_and_metadata` requires; the entries are cloned into an owned
        // map, so nothing borrows it after this call.
        unsafe { map_topic_partition_offset_and_metadata(divergent_offsets) },
    ))
}

/// `new LogTruncationException(String message, Map<TopicPartition, Long>
/// offsetOutOfRangePartitions, Map<TopicPartition, OffsetAndMetadata>
/// divergentOffsets)`.
///
/// # Safety
///
/// `message` must be a valid NUL-terminated string and each map null or a
/// valid map of the stated types.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerLogTruncationError_with_message(
    message: *const c_char,
    offset_out_of_range_partitions: *const kafka_Map_t,
    divergent_offsets: *const kafka_Map_t,
) -> *mut kafka_common_ConsumerLogTruncationError_t {
    Payload::boxed(ConsumerLogTruncationError::with_message(
        // SAFETY: Per this function's `# Safety`, `message` is a valid NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(message) },
        // SAFETY: Per this function's `# Safety`, `offset_out_of_range_partitions` is null or a
        // valid map of the stated types, `kafka_common_TopicPartition_t *` keys and `int64_t *`
        // values, as `map_topic_partition_i64` requires; the entries are copied into an owned map,
        // so nothing borrows it after this call.
        unsafe { map_topic_partition_i64(offset_out_of_range_partitions) },
        // SAFETY: Per this function's `# Safety`, `divergent_offsets` is null or a valid map of the
        // stated types, `kafka_common_TopicPartition_t *` keys and
        // `kafka_consumer_OffsetAndMetadata_t *` values, as
        // `map_topic_partition_offset_and_metadata` requires; the entries are cloned into an owned
        // map, so nothing borrows it after this call.
        unsafe { map_topic_partition_offset_and_metadata(divergent_offsets) },
    ))
}

/// `partitions()`: an owned list of `kafka_common_TopicPartition_t *`,
/// ordered by topic then partition, freed with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid log-truncation handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerLogTruncationError_partitions(
    self_: *const kafka_common_ConsumerLogTruncationError_t,
) -> *mut kafka_List_t {
    sorted_topic_partition_list(
        // SAFETY: Per this function's `# Safety`, `self_` is a valid log-truncation handle: a view
        // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr`
        // requires (no call mutates a payload of this class). The borrow lasts for this call only:
        // the field is copied into the owned container returned.
        unsafe { Payload::<ConsumerLogTruncationError>::from_ptr(self_) }
            .value()
            .partitions(),
    )
}

/// `offsetOutOfRangePartitions()`: an owned map of
/// `kafka_common_TopicPartition_t *` to `int64_t *`, ordered by topic then
/// partition, freed with `kafka_Map_destroy`.
///
/// # Safety
///
/// `self_` must be a valid log-truncation handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerLogTruncationError_offset_out_of_range_partitions(
    self_: *const kafka_common_ConsumerLogTruncationError_t,
) -> *mut kafka_Map_t {
    topic_partition_i64_map(
        // SAFETY: Per this function's `# Safety`, `self_` is a valid log-truncation handle: a view
        // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr`
        // requires (no call mutates a payload of this class). The borrow lasts for this call only:
        // the field is copied into the owned container returned.
        unsafe { Payload::<ConsumerLogTruncationError>::from_ptr(self_) }
            .value()
            .offset_out_of_range_partitions(),
    )
}

/// `divergentOffsets()`: an owned map of `kafka_common_TopicPartition_t *` to
/// `kafka_consumer_OffsetAndMetadata_t *`, ordered by topic then partition,
/// freed with `kafka_Map_destroy`.
///
/// # Safety
///
/// `self_` must be a valid log-truncation handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerLogTruncationError_divergent_offsets(
    self_: *const kafka_common_ConsumerLogTruncationError_t,
) -> *mut kafka_Map_t {
    topic_partition_offset_and_metadata_map(
        // SAFETY: Per this function's `# Safety`, `self_` is a valid log-truncation handle: a view
        // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr`
        // requires (no call mutates a payload of this class). The borrow lasts for this call only:
        // the field is copied into the owned container returned.
        unsafe { Payload::<ConsumerLogTruncationError>::from_ptr(self_) }
            .value()
            .divergent_offsets(),
    )
}

/// `getMessage()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid log-truncation handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerLogTruncationError_message(
    self_: *const kafka_common_ConsumerLogTruncationError_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid log-truncation handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). `message_ptr` points at the payload's own C
    // string, so the string lives as long as the handle; that is the rustdoc's "borrowed from the
    // handle".
    unsafe { Payload::<ConsumerLogTruncationError>::from_ptr(self_) }.message_ptr()
}

/// `getCause()`: borrowed from the handle, or null.
///
/// # Safety
///
/// `self_` must be a valid log-truncation handle.
#[ffi_guard(fallback = ::core::ptr::null())]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerLogTruncationError_source(
    self_: *const kafka_common_ConsumerLogTruncationError_t,
) -> *const kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid log-truncation handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). `source_ptr` returns the cause handle the
    // payload's `source` slot owns, so it lives as long as the handle; that is the rustdoc's
    // "borrowed from the handle".
    unsafe { Payload::<ConsumerLogTruncationError>::from_ptr(self_) }.source_ptr()
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid log-truncation handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerLogTruncationError_to_string(
    self_: *const kafka_common_ConsumerLogTruncationError_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid log-truncation handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). The borrow lasts for this call only: `to_c_string`
    // copies the text into an owned string.
    unsafe { Payload::<ConsumerLogTruncationError>::from_ptr(self_) }.to_c_string()
}

/// Frees an owned handle; null is a no-op. A view borrowed from a
/// `kafka_common_Error_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerLogTruncationError_destroy(
    self_: *mut kafka_common_ConsumerLogTruncationError_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned handle not yet destroyed,
    // as `Payload::destroy` requires. Owned `kafka_common_ConsumerLogTruncationError_t` handles
    // come only from this class's constructors, which box a `Payload<ConsumerLogTruncationError>`,
    // and a view is never passed here.
    unsafe { Payload::<ConsumerLogTruncationError>::destroy(self_) }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};

    use super::*;
    use crate::ffi::common::{box_error, kafka_common_Error_destroy};
    use crate::ffi::consumer::offset_and_metadata::kafka_consumer_OffsetAndMetadata_offset;
    use crate::ffi::util::{kafka_List_destroy, kafka_List_size, kafka_Map_destroy, kafka_Map_size, kafka_Map_value};

    #[test]
    fn both_maps_cross_and_round_trip() {
        let mut offsets = HashMap::new();
        offsets.insert(TopicPartition::new("t", 0), 10i64);
        let mut divergent = HashMap::new();
        divergent.insert(TopicPartition::new("t", 1), OffsetAndMetadata::new(5).unwrap());
        let error = box_error(Error::ConsumerLogTruncation(Box::new(ConsumerLogTruncationError::new(
            offsets, divergent,
        ))));
        let message = CString::new("truncated").unwrap();
        // SAFETY: Test code: `box_error` and this module's constructors and getters make every
        // handle used. Each owned handle (the error, the payloads, the lists and maps the getters
        // return, the string) is destroyed once, the view is used only while its error is alive,
        // and `message` outlives the call that reads it.
        unsafe {
            let view = kafka_common_Error_consumer_log_truncation(error);
            assert!(!view.is_null());
            // Java's `partitions()` is inherited from `OffsetOutOfRangeException`:
            // only the out-of-range keys, not the divergent ones.
            let partitions = kafka_common_ConsumerLogTruncationError_partitions(view);
            assert_eq!(kafka_List_size(partitions), 1);
            kafka_List_destroy(partitions);
            let oor = kafka_common_ConsumerLogTruncationError_offset_out_of_range_partitions(view);
            assert_eq!(kafka_Map_size(oor), 1);
            assert_eq!(*(kafka_Map_value(oor, 0) as *const i64), 10);
            let divergent = kafka_common_ConsumerLogTruncationError_divergent_offsets(view);
            assert_eq!(kafka_Map_size(divergent), 1);
            assert_eq!(
                kafka_consumer_OffsetAndMetadata_offset(kafka_Map_value(divergent, 0) as *const _),
                5
            );

            // Feeding both maps back builds an equal value.
            let rebuilt = kafka_common_ConsumerLogTruncationError_with_message(message.as_ptr(), oor, divergent);
            kafka_Map_destroy(oor);
            kafka_Map_destroy(divergent);
            assert_eq!(
                CStr::from_ptr(kafka_common_ConsumerLogTruncationError_message(rebuilt))
                    .to_str()
                    .unwrap(),
                "truncated"
            );
            let again = kafka_common_ConsumerLogTruncationError_divergent_offsets(rebuilt);
            assert_eq!(kafka_Map_size(again), 1);
            kafka_Map_destroy(again);
            assert!(kafka_common_ConsumerLogTruncationError_source(rebuilt).is_null());
            let s = kafka_common_ConsumerLogTruncationError_to_string(rebuilt);
            assert!(!s.is_null());
            crate::ffi::util::kafka_string_destroy(s);
            kafka_common_ConsumerLogTruncationError_destroy(rebuilt);
            kafka_common_Error_destroy(error);

            let other = box_error(Error::kafka_message("other"));
            assert!(kafka_common_Error_consumer_log_truncation(other).is_null());
            kafka_common_Error_destroy(other);

            let empty = kafka_common_ConsumerLogTruncationError_new(ptr::null(), ptr::null());
            assert_eq!(
                CStr::from_ptr(kafka_common_ConsumerLogTruncationError_message(empty))
                    .to_str()
                    .unwrap(),
                ConsumerLogTruncationError::new(HashMap::new(), HashMap::new()).message()
            );
            kafka_common_ConsumerLogTruncationError_destroy(empty);
            kafka_common_ConsumerLogTruncationError_destroy(ptr::null_mut());
        }
    }
}
