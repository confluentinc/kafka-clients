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

//! `kafka_consumer_ConsumerRecords_t`:
//! `org.apache.kafka.clients.consumer.ConsumerRecords<K, V>`, what `poll`
//! returns.
//!
//! The handle pre-wraps every record so the `records(...)` views hand out
//! borrowed `kafka_consumer_ConsumerRecord_t *`s that stay valid, and stable,
//! until the records handle is destroyed; those lists own nothing and a
//! borrowed record is never passed to `kafka_consumer_ConsumerRecord_destroy`.
//! With a `NULL` deserializer the records own the `kafka_Bytes_t`s behind
//! their keys and values, which point into the fetch buffer the records
//! keep alive: destroying the records handle releases the buffer.

use std::collections::HashMap;
use std::ffi::{c_char, c_void};

use indexmap::IndexMap;

use crate::common::TopicPartition;
use crate::consumer::{ConsumerRecords, OffsetAndMetadata};
use crate::ffi::common::topic_partition::{kafka_common_TopicPartition_t, topic_partition_list, topic_partition_ref};
use crate::ffi::consumer::consumer_record::{
    ConsumerRecordInner, GenericRecord, consumer_record_ref, kafka_consumer_ConsumerRecord_t,
};
use crate::ffi::consumer::offset_and_metadata::{map_offset_and_metadata, offset_and_metadata_map};
use crate::ffi::util::{box_list, c_str_to_string, kafka_List_t, kafka_Map_t, list_elements, map_entries};

/// Opaque handle to a [`ConsumerRecords`].
#[repr(C)]
pub struct kafka_consumer_ConsumerRecords_t {
    _private: [u8; 0],
}

/// What the handle points at: the records per partition, in the order
/// `partitions()` yields, each already wrapped as a handle, plus the next
/// offsets. The vectors are never resized after construction, so the record
/// pointers handed out by `records()` stay valid until the handle is
/// destroyed.
pub(crate) struct ConsumerRecordsInner {
    partitions: Vec<(TopicPartition, Vec<ConsumerRecordInner>)>,
    next_offsets: HashMap<TopicPartition, OffsetAndMetadata>,
}

impl ConsumerRecordsInner {
    fn new(
        records: ConsumerRecords<crate::ffi::util::GenericValue, crate::ffi::util::GenericValue>,
        owns_key: bool,
        owns_value: bool,
    ) -> Self {
        let next_offsets = records.next_offsets().clone();
        let layout: Vec<(TopicPartition, usize)> = records
            .partitions()
            .map(|tp| (tp.clone(), records.records_with_partition(tp).len()))
            .collect();
        // The owned iterator yields the partitions in the same order as
        // `partitions()`, so the records are moved out, never copied.
        let mut records = records.into_iter();
        let partitions = layout
            .into_iter()
            .map(|(tp, count)| {
                let wrapped = records
                    .by_ref()
                    .take(count)
                    .map(|record| ConsumerRecordInner::new(record, owns_key, owns_value))
                    .collect();
                (tp, wrapped)
            })
            .collect();
        Self { partitions, next_offsets }
    }

    fn count(&self) -> usize {
        self.partitions.iter().map(|(_, records)| records.len()).sum()
    }

    fn borrowed_list<'a>(records: impl Iterator<Item = &'a ConsumerRecordInner>) -> *mut kafka_List_t {
        box_list(records.map(|r| r.as_ptr() as *mut c_void).collect(), None)
    }
}

/// Hands `records` to C as an owned handle, freed with
/// [`kafka_consumer_ConsumerRecords_destroy`]; `owns_key` / `owns_value` say
/// whether the `void *`s are `kafka_Bytes_t`s the records own (a `NULL`
/// deserializer on that side).
pub(crate) fn box_consumer_records(
    records: ConsumerRecords<crate::ffi::util::GenericValue, crate::ffi::util::GenericValue>,
    owns_key: bool,
    owns_value: bool,
) -> *mut kafka_consumer_ConsumerRecords_t {
    Box::into_raw(Box::new(ConsumerRecordsInner::new(records, owns_key, owns_value)))
        as *mut kafka_consumer_ConsumerRecords_t
}

unsafe fn inner_ref<'a>(records: *const kafka_consumer_ConsumerRecords_t) -> &'a ConsumerRecordsInner {
    unsafe { &*(records as *const ConsumerRecordsInner) }
}

/// `ConsumerRecords.empty()`: an owned handle with no records.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_consumer_ConsumerRecords_empty() -> *mut kafka_consumer_ConsumerRecords_t {
    box_consumer_records(ConsumerRecords::empty(), false, false)
}

/// `new ConsumerRecords(Map<TopicPartition, List<ConsumerRecord<K, V>>> records,
/// Map<TopicPartition, OffsetAndMetadata> nextOffsets)`: `records` maps
/// `kafka_common_TopicPartition_t *` to `kafka_List_t *` of
/// `kafka_consumer_ConsumerRecord_t *`; both maps and everything in them are
/// copied during the call and stay the caller's. The copied records never
/// own their keys or values (see the module docs).
///
/// # Safety
///
/// `records` and `next_offsets` must be null or valid maps of the documented
/// element types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecords_with_next_offsets(
    records: *const kafka_Map_t,
    next_offsets: *const kafka_Map_t,
) -> *mut kafka_consumer_ConsumerRecords_t {
    let mut by_partition: IndexMap<TopicPartition, Vec<GenericRecord>> = IndexMap::new();
    for &(tp, list) in unsafe { map_entries(records) } {
        let tp = unsafe { topic_partition_ref(tp as *const kafka_common_TopicPartition_t) }.clone();
        let records = unsafe { list_elements(list as *const kafka_List_t) }
            .iter()
            .map(|&record| unsafe { consumer_record_ref(record as *const kafka_consumer_ConsumerRecord_t) }.clone())
            .collect();
        by_partition.insert(tp, records);
    }
    let next_offsets = unsafe { map_offset_and_metadata(next_offsets) };
    box_consumer_records(ConsumerRecords::with_next_offsets(by_partition, next_offsets), false, false)
}

/// `nextOffsets()`: an owned map of owned `kafka_common_TopicPartition_t *`
/// to owned `kafka_consumer_OffsetAndMetadata_t *`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecords_next_offsets(
    self_: *const kafka_consumer_ConsumerRecords_t,
) -> *mut kafka_Map_t {
    offset_and_metadata_map(&unsafe { inner_ref(self_) }.next_offsets)
}

/// `partitions()`: an owned list of owned `kafka_common_TopicPartition_t *`,
/// in the order the records were fetched.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecords_partitions(
    self_: *const kafka_consumer_ConsumerRecords_t,
) -> *mut kafka_List_t {
    topic_partition_list(unsafe { inner_ref(self_) }.partitions.iter().map(|(tp, _)| tp.clone()))
}

/// `records(TopicPartition partition)`: an owned list of borrowed
/// `const kafka_consumer_ConsumerRecord_t *`, valid until this handle is
/// destroyed (see the module docs); empty for an unknown partition.
///
/// # Safety
///
/// `self_` must be a valid handle and `partition` a valid topic-partition
/// handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecords_records_with_partition(
    self_: *const kafka_consumer_ConsumerRecords_t,
    partition: *const kafka_common_TopicPartition_t,
) -> *mut kafka_List_t {
    let inner = unsafe { inner_ref(self_) };
    let partition = unsafe { topic_partition_ref(partition) };
    ConsumerRecordsInner::borrowed_list(
        inner
            .partitions
            .iter()
            .filter(|(tp, _)| tp == partition)
            .flat_map(|(_, records)| records.iter()),
    )
}

/// `records(String topic)`: an owned list of borrowed
/// `const kafka_consumer_ConsumerRecord_t *` across the topic's partitions,
/// valid until this handle is destroyed.
///
/// # Safety
///
/// `self_` must be a valid handle and `topic` a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecords_records_with_topic(
    self_: *const kafka_consumer_ConsumerRecords_t,
    topic: *const c_char,
) -> *mut kafka_List_t {
    let inner = unsafe { inner_ref(self_) };
    let topic = unsafe { c_str_to_string(topic) };
    ConsumerRecordsInner::borrowed_list(
        inner
            .partitions
            .iter()
            .filter(|(tp, _)| tp.topic() == topic)
            .flat_map(|(_, records)| records.iter()),
    )
}

/// `isEmpty()`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecords_is_empty(self_: *const kafka_consumer_ConsumerRecords_t) -> i8 {
    i8::from(unsafe { inner_ref(self_) }.count() == 0)
}

/// `count()`: the number of records across all partitions.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecords_count(self_: *const kafka_consumer_ConsumerRecords_t) -> i32 {
    i32::try_from(unsafe { inner_ref(self_) }.count()).unwrap_or(i32::MAX)
}

/// Frees a handle and every record it holds; a null pointer is a no-op.
///
/// # Safety
///
/// `self_` must be null or a valid handle not used afterwards, nor any
/// record borrowed from it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerRecords_destroy(self_: *mut kafka_consumer_ConsumerRecords_t) {
    if !self_.is_null() {
        unsafe { drop(Box::from_raw(self_ as *mut ConsumerRecordsInner)) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::common::topic_partition::{box_topic_partition, kafka_common_TopicPartition_destroy};
    use crate::ffi::consumer::consumer_record::{
        kafka_consumer_ConsumerRecord_destroy, kafka_consumer_ConsumerRecord_new, kafka_consumer_ConsumerRecord_offset,
    };
    use crate::ffi::util::{
        GenericValue, kafka_List_add, kafka_List_destroy, kafka_List_get, kafka_List_new, kafka_List_size,
        kafka_Map_destroy, kafka_Map_new, kafka_Map_put, kafka_Map_size,
    };

    fn sample() -> ConsumerRecords<GenericValue, GenericValue> {
        let mut records = IndexMap::new();
        records.insert(
            TopicPartition::new("b", 0),
            vec![
                GenericRecord::new("b", 0, 1, None, None),
                GenericRecord::new("b", 0, 2, None, None),
            ],
        );
        records.insert(TopicPartition::new("a", 0), vec![GenericRecord::new("a", 0, 7, None, None)]);
        let mut next = HashMap::new();
        next.insert(TopicPartition::new("b", 0), OffsetAndMetadata::new(3).unwrap());
        ConsumerRecords::with_next_offsets(records, next)
    }

    #[test]
    fn views_keep_fetch_order_and_borrow_records() {
        let records = box_consumer_records(sample(), false, false);
        unsafe {
            assert_eq!(kafka_consumer_ConsumerRecords_count(records), 3);
            assert_eq!(kafka_consumer_ConsumerRecords_is_empty(records), 0);
            let partitions = kafka_consumer_ConsumerRecords_partitions(records);
            assert_eq!(kafka_List_size(partitions), 2);
            assert_eq!(topic_partition_ref(kafka_List_get(partitions, 0) as *const _).topic(), "b");
            kafka_List_destroy(partitions);

            let tp = box_topic_partition(TopicPartition::new("b", 0));
            let b = kafka_consumer_ConsumerRecords_records_with_partition(records, tp);
            assert_eq!(kafka_List_size(b), 2);
            let first = kafka_List_get(b, 0) as *const kafka_consumer_ConsumerRecord_t;
            assert_eq!(kafka_consumer_ConsumerRecord_offset(first), 1);
            kafka_List_destroy(b);
            // The same borrowed pointer comes back from the topic view.
            let by_topic = kafka_consumer_ConsumerRecords_records_with_topic(records, c"b".as_ptr());
            assert_eq!(kafka_List_get(by_topic, 0) as *const kafka_consumer_ConsumerRecord_t, first);
            kafka_List_destroy(by_topic);
            kafka_common_TopicPartition_destroy(tp);

            let next = kafka_consumer_ConsumerRecords_next_offsets(records);
            assert_eq!(kafka_Map_size(next), 1);
            kafka_Map_destroy(next);
            kafka_consumer_ConsumerRecords_destroy(records);
        }
    }

    #[test]
    fn built_from_c_copies_the_records() {
        unsafe {
            let tp = box_topic_partition(TopicPartition::new("t", 1));
            let record = kafka_consumer_ConsumerRecord_new(c"t".as_ptr(), 1, 5, std::ptr::null(), std::ptr::null());
            let list = kafka_List_new();
            kafka_List_add(list, record as *mut c_void);
            let map = kafka_Map_new();
            kafka_Map_put(map, tp as *mut c_void, list as *mut c_void);
            let records = kafka_consumer_ConsumerRecords_with_next_offsets(map, std::ptr::null());
            // The inputs are the caller's and can go first.
            kafka_Map_destroy(map);
            kafka_List_destroy(list);
            kafka_consumer_ConsumerRecord_destroy(record);
            let view = kafka_consumer_ConsumerRecords_records_with_partition(records, tp);
            assert_eq!(kafka_List_size(view), 1);
            assert_eq!(kafka_consumer_ConsumerRecord_offset(kafka_List_get(view, 0) as *const _), 5);
            kafka_List_destroy(view);
            kafka_common_TopicPartition_destroy(tp);
            let empty = kafka_consumer_ConsumerRecords_empty();
            assert_eq!(kafka_consumer_ConsumerRecords_is_empty(empty), 1);
            kafka_consumer_ConsumerRecords_destroy(empty);
            kafka_consumer_ConsumerRecords_destroy(records);
        }
    }
}
