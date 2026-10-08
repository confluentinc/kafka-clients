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
    unsafe { map_entries(map) }
        .iter()
        .map(|&(k, v)| {
            (
                unsafe { topic_partition_ref(k as *const kafka_common_TopicPartition_t) }.clone(),
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_consumer_log_truncation(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_ConsumerLogTruncationError_t {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerLogTruncationError_new(
    offset_out_of_range_partitions: *const kafka_Map_t,
    divergent_offsets: *const kafka_Map_t,
) -> *mut kafka_common_ConsumerLogTruncationError_t {
    Payload::boxed(ConsumerLogTruncationError::new(
        unsafe { map_topic_partition_i64(offset_out_of_range_partitions) },
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerLogTruncationError_with_message(
    message: *const c_char,
    offset_out_of_range_partitions: *const kafka_Map_t,
    divergent_offsets: *const kafka_Map_t,
) -> *mut kafka_common_ConsumerLogTruncationError_t {
    Payload::boxed(ConsumerLogTruncationError::with_message(
        unsafe { c_str_to_string(message) },
        unsafe { map_topic_partition_i64(offset_out_of_range_partitions) },
        unsafe { map_topic_partition_offset_and_metadata(divergent_offsets) },
    ))
}

/// `partitions()`: an owned list of `kafka_common_TopicPartition_t *`,
/// ordered by topic then partition, freed with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid log-truncation handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerLogTruncationError_partitions(
    self_: *const kafka_common_ConsumerLogTruncationError_t,
) -> *mut kafka_List_t {
    sorted_topic_partition_list(
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerLogTruncationError_offset_out_of_range_partitions(
    self_: *const kafka_common_ConsumerLogTruncationError_t,
) -> *mut kafka_Map_t {
    topic_partition_i64_map(
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerLogTruncationError_divergent_offsets(
    self_: *const kafka_common_ConsumerLogTruncationError_t,
) -> *mut kafka_Map_t {
    topic_partition_offset_and_metadata_map(
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerLogTruncationError_message(
    self_: *const kafka_common_ConsumerLogTruncationError_t,
) -> *const c_char {
    unsafe { Payload::<ConsumerLogTruncationError>::from_ptr(self_) }.message_ptr()
}

/// `getCause()`: borrowed from the handle, or null.
///
/// # Safety
///
/// `self_` must be a valid log-truncation handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerLogTruncationError_source(
    self_: *const kafka_common_ConsumerLogTruncationError_t,
) -> *const kafka_common_Error_t {
    unsafe { Payload::<ConsumerLogTruncationError>::from_ptr(self_) }.source_ptr()
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid log-truncation handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerLogTruncationError_to_string(
    self_: *const kafka_common_ConsumerLogTruncationError_t,
) -> *mut c_char {
    unsafe { Payload::<ConsumerLogTruncationError>::from_ptr(self_) }.to_c_string()
}

/// Frees an owned handle; null is a no-op. A view borrowed from a
/// `kafka_common_Error_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerLogTruncationError_destroy(
    self_: *mut kafka_common_ConsumerLogTruncationError_t,
) {
    unsafe { Payload::<ConsumerLogTruncationError>::destroy(self_) }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};

    use super::*;
    use crate::ffi::common::{box_error, kafka_common_Error_destroy};
    use crate::ffi::consumer::kafka_consumer_OffsetAndMetadata_offset;
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
