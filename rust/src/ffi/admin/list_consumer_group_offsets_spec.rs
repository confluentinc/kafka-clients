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

//! `kafka_admin_ListConsumerGroupOffsetsSpec_t`:
//! `org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec`
//! (CLAUDE.md §4). Rust's fluent setter takes `self` by value and returns
//! it; C mutates the handle in place.

use std::ffi::c_char;

use crate::admin::ListConsumerGroupOffsetsSpec;
use crate::ffi::common::topic_partition::{list_topic_partitions, topic_partition_list};
use crate::ffi::util::{into_c_string, kafka_List_t};

/// Opaque handle to a [`ListConsumerGroupOffsetsSpec`].
#[repr(C)]
pub struct kafka_admin_ListConsumerGroupOffsetsSpec_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_ListConsumerGroupOffsetsSpec_t`] points at.
pub(crate) struct ListConsumerGroupOffsetsSpecInner {
    spec: ListConsumerGroupOffsetsSpec,
}

/// The spec behind a handle.
///
/// # Safety
///
/// `spec` must be a valid list-consumer-group-offsets-spec handle.
pub(crate) unsafe fn list_consumer_group_offsets_spec_ref<'a>(
    spec: *const kafka_admin_ListConsumerGroupOffsetsSpec_t,
) -> &'a ListConsumerGroupOffsetsSpec {
    &unsafe { &*(spec as *const ListConsumerGroupOffsetsSpecInner) }.spec
}

/// Hands `spec` to C as an owned handle, freed with
/// [`kafka_admin_ListConsumerGroupOffsetsSpec_destroy`].
pub(crate) fn box_list_consumer_group_offsets_spec(
    spec: ListConsumerGroupOffsetsSpec,
) -> *mut kafka_admin_ListConsumerGroupOffsetsSpec_t {
    Box::into_raw(Box::new(ListConsumerGroupOffsetsSpecInner { spec }))
        as *mut kafka_admin_ListConsumerGroupOffsetsSpec_t
}

/// `new ListConsumerGroupOffsetsSpec()`: no partitions set, so all of the
/// group's partitions are listed. Owned, freed with
/// [`kafka_admin_ListConsumerGroupOffsetsSpec_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ListConsumerGroupOffsetsSpec_new() -> *mut kafka_admin_ListConsumerGroupOffsetsSpec_t {
    box_list_consumer_group_offsets_spec(ListConsumerGroupOffsetsSpec::new())
}

/// `topicPartitions(Collection<TopicPartition> topicPartitions)`, the
/// setter, applied in place: a borrowed list of
/// `const kafka_common_TopicPartition_t *`, copied; `NULL` is Java's null
/// and lists all of the group's partitions.
///
/// # Safety
///
/// `self_` must be a valid spec handle and `topic_partitions` null or a
/// valid list of topic-partition handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConsumerGroupOffsetsSpec_set_topic_partitions(
    self_: *mut kafka_admin_ListConsumerGroupOffsetsSpec_t,
    topic_partitions: *const kafka_List_t,
) {
    let topic_partitions = (!topic_partitions.is_null()).then(|| unsafe { list_topic_partitions(topic_partitions) });
    let inner = unsafe { &mut *(self_ as *mut ListConsumerGroupOffsetsSpecInner) };
    inner.spec = inner.spec.clone().set_topic_partitions(topic_partitions);
}

/// `topicPartitions()`, the getter: an owned list of owned
/// `kafka_common_TopicPartition_t *` copies in the order given, freed
/// together with `kafka_List_destroy`; `NULL` when none were set (Java
/// returns null).
///
/// # Safety
///
/// `self_` must be a valid spec handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConsumerGroupOffsetsSpec_topic_partitions(
    self_: *const kafka_admin_ListConsumerGroupOffsetsSpec_t,
) -> *mut kafka_List_t {
    unsafe { list_consumer_group_offsets_spec_ref(self_) }
        .topic_partitions()
        .map_or(std::ptr::null_mut(), |tps| topic_partition_list(tps.iter().cloned()))
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid spec handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConsumerGroupOffsetsSpec_to_string(
    self_: *const kafka_admin_ListConsumerGroupOffsetsSpec_t,
) -> *mut c_char {
    into_c_string(&unsafe { list_consumer_group_offsets_spec_ref(self_) }.to_string())
}

/// Frees an owned spec handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned spec handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ListConsumerGroupOffsetsSpec_destroy(
    self_: *mut kafka_admin_ListConsumerGroupOffsetsSpec_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut ListConsumerGroupOffsetsSpecInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, c_void};
    use std::ptr;

    use super::*;
    use crate::common::TopicPartition;
    use crate::ffi::common::topic_partition::{
        box_topic_partition, kafka_common_TopicPartition_destroy, kafka_common_TopicPartition_t, topic_partition_ref,
    };
    use crate::ffi::util::{
        kafka_List_add, kafka_List_destroy, kafka_List_get, kafka_List_new, kafka_List_size, kafka_string_destroy,
    };

    #[test]
    fn setter_copies_the_list_and_null_clears_it() {
        let spec = kafka_admin_ListConsumerGroupOffsetsSpec_new();
        unsafe {
            assert!(kafka_admin_ListConsumerGroupOffsetsSpec_topic_partitions(spec).is_null());
            let tp = TopicPartition::new("orders", 1);
            let tp_handle = box_topic_partition(tp.clone());
            let list = kafka_List_new();
            kafka_List_add(list, tp_handle as *mut c_void);
            kafka_admin_ListConsumerGroupOffsetsSpec_set_topic_partitions(spec, list);
            kafka_List_destroy(list);
            kafka_common_TopicPartition_destroy(tp_handle);

            let expected = ListConsumerGroupOffsetsSpec::new().set_topic_partitions(Some(vec![tp.clone()]));
            assert_eq!(*list_consumer_group_offsets_spec_ref(spec), expected);
            let out = kafka_admin_ListConsumerGroupOffsetsSpec_topic_partitions(spec);
            assert_eq!(kafka_List_size(out), 1);
            assert_eq!(
                *topic_partition_ref(kafka_List_get(out, 0) as *const kafka_common_TopicPartition_t),
                tp
            );
            kafka_List_destroy(out);
            let s = kafka_admin_ListConsumerGroupOffsetsSpec_to_string(spec);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), expected.to_string());
            kafka_string_destroy(s);

            kafka_admin_ListConsumerGroupOffsetsSpec_set_topic_partitions(spec, ptr::null());
            assert!(kafka_admin_ListConsumerGroupOffsetsSpec_topic_partitions(spec).is_null());
            kafka_admin_ListConsumerGroupOffsetsSpec_destroy(spec);
            kafka_admin_ListConsumerGroupOffsetsSpec_destroy(ptr::null_mut());
        }
    }
}
