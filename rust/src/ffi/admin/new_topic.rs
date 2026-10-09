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

//! `kafka_admin_NewTopic_t`: `org.apache.kafka.clients.admin.NewTopic`
//! (CLAUDE.md §4). Java's `Optional<Integer>` / `Optional<Short>` cross as
//! the scalar with `-1` for an absent value, which is also what Java's
//! getters return (`CreateTopicsRequest.NO_NUM_PARTITIONS` /
//! `NO_REPLICATION_FACTOR`). A `Map<Integer, List<Integer>>` crosses as a
//! `kafka_Map_t` of `int32_t *` keys to `kafka_List_t *` values holding
//! `int32_t *` elements.

use std::collections::BTreeMap;
use std::ffi::{CString, c_char, c_void};

use crate::admin::NewTopic;
use crate::ffi::util::{
    box_list, box_map, box_string_map, c_str_to_string, destroy_boxed, into_c_string, kafka_List_destroy, kafka_List_t,
    kafka_Map_t, list_elements, map_entries, map_strings, owned_c_string,
};

/// Opaque handle to a [`NewTopic`].
#[repr(C)]
pub struct kafka_admin_NewTopic_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_NewTopic_t`] points at: the topic plus the
/// NUL-terminated name the string getter borrows out.
pub(crate) struct NewTopicInner {
    topic: NewTopic,
    name_c: CString,
}

impl NewTopicInner {
    pub(crate) fn new(topic: NewTopic) -> Self {
        let name_c = owned_c_string(topic.name());
        Self { topic, name_c }
    }
}

unsafe fn inner_ref<'a>(topic: *const kafka_admin_NewTopic_t) -> &'a NewTopicInner {
    unsafe { &*(topic as *const NewTopicInner) }
}

/// The topic behind a handle.
///
/// # Safety
///
/// `topic` must be a valid new-topic handle.
pub(crate) unsafe fn new_topic_ref<'a>(topic: *const kafka_admin_NewTopic_t) -> &'a NewTopic {
    &unsafe { inner_ref(topic) }.topic
}

/// Hands `topic` to C as an owned handle, freed with
/// [`kafka_admin_NewTopic_destroy`].
pub(crate) fn box_new_topic(topic: NewTopic) -> *mut kafka_admin_NewTopic_t {
    Box::into_raw(Box::new(NewTopicInner::new(topic))) as *mut kafka_admin_NewTopic_t
}

/// Reads a borrowed list of `const int32_t *` into values; null reads as empty.
unsafe fn list_i32(list: *const kafka_List_t) -> Vec<i32> {
    unsafe { list_elements(list) }
        .iter()
        .map(|&element| unsafe { *(element as *const i32) })
        .collect()
}

/// Hands `values` to C as an owned list of `int32_t *`.
fn box_i32_list(values: &[i32]) -> *mut kafka_List_t {
    let elements = values.iter().map(|&v| Box::into_raw(Box::new(v)) as *mut c_void).collect();
    box_list(elements, Some(destroy_boxed::<i32>))
}

/// Frees a `kafka_List_t *` element of an owned container.
unsafe fn destroy_list_element(element: *mut c_void) {
    unsafe { kafka_List_destroy(element as *mut kafka_List_t) };
}

/// Compares two `int32_t *` keys by value.
unsafe fn i32_key_eq(a: *mut c_void, b: *mut c_void) -> bool {
    unsafe { *(a as *const i32) == *(b as *const i32) }
}

/// Reads a borrowed map of `const int32_t *` to `const kafka_List_t *` of
/// `const int32_t *`; null reads as empty.
unsafe fn map_replicas_assignments(map: *const kafka_Map_t) -> BTreeMap<i32, Vec<i32>> {
    unsafe { map_entries(map) }
        .iter()
        .map(|&(k, v)| (unsafe { *(k as *const i32) }, unsafe { list_i32(v as *const kafka_List_t) }))
        .collect()
}

/// `new NewTopic(String name, Optional<Integer> numPartitions,
/// Optional<Short> replicationFactor)`: `-1` stands for an empty optional
/// (the broker default). Owned, freed with [`kafka_admin_NewTopic_destroy`].
///
/// # Safety
///
/// `name` must be a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewTopic_with_num_partitions_replication_factor(
    name: *const c_char,
    num_partitions: i32,
    replication_factor: i16,
) -> *mut kafka_admin_NewTopic_t {
    box_new_topic(NewTopic::with_num_partitions_replication_factor(
        unsafe { c_str_to_string(name) },
        (num_partitions >= 0).then_some(num_partitions),
        (replication_factor >= 0).then_some(replication_factor),
    ))
}

/// `new NewTopic(String name, Map<Integer, List<Integer>> replicasAssignments)`:
/// a borrowed map of `const int32_t *` partition ids to `const kafka_List_t *`
/// of `const int32_t *` broker ids, copied (`NULL` reads as empty). Owned,
/// freed with [`kafka_admin_NewTopic_destroy`].
///
/// # Safety
///
/// `name` must be a valid NUL-terminated string and `replicas_assignments`
/// null or a valid map of that shape.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewTopic_with_replicas_assignments(
    name: *const c_char,
    replicas_assignments: *const kafka_Map_t,
) -> *mut kafka_admin_NewTopic_t {
    box_new_topic(NewTopic::with_replicas_assignments(unsafe { c_str_to_string(name) }, unsafe {
        map_replicas_assignments(replicas_assignments)
    }))
}

/// `name()`: a borrowed string valid as long as the topic, never passed to
/// `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid new-topic handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewTopic_name(self_: *const kafka_admin_NewTopic_t) -> *const c_char {
    unsafe { inner_ref(self_) }.name_c.as_ptr()
}

/// `numPartitions()`: `-1` (`CreateTopicsRequest.NO_NUM_PARTITIONS`) when
/// the broker default applies.
///
/// # Safety
///
/// `self_` must be a valid new-topic handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewTopic_num_partitions(self_: *const kafka_admin_NewTopic_t) -> i32 {
    unsafe { new_topic_ref(self_) }.num_partitions()
}

/// `replicationFactor()`: `-1` (`CreateTopicsRequest.NO_REPLICATION_FACTOR`)
/// when the broker default applies.
///
/// # Safety
///
/// `self_` must be a valid new-topic handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewTopic_replication_factor(self_: *const kafka_admin_NewTopic_t) -> i16 {
    unsafe { new_topic_ref(self_) }.replication_factor()
}

/// `replicasAssignments()`: an owned map of owned `int32_t *` partition ids
/// (in ascending order, `kafka_Map_get` comparing by value) to owned
/// `kafka_List_t *` of owned `int32_t *` broker ids, freed together with
/// `kafka_Map_destroy`; `NULL` when the topic was created with partition
/// count and replication factor instead (Java returns null).
///
/// # Safety
///
/// `self_` must be a valid new-topic handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewTopic_replicas_assignments(
    self_: *const kafka_admin_NewTopic_t,
) -> *mut kafka_Map_t {
    match unsafe { new_topic_ref(self_) }.replicas_assignments() {
        None => std::ptr::null_mut(),
        Some(assignments) => {
            let entries = assignments
                .iter()
                .map(|(&partition, brokers)| {
                    (
                        Box::into_raw(Box::new(partition)) as *mut c_void,
                        box_i32_list(brokers) as *mut c_void,
                    )
                })
                .collect();
            box_map(
                entries,
                Some(destroy_boxed::<i32>),
                Some(destroy_list_element),
                Some(i32_key_eq),
            )
        },
    }
}

/// `configs(Map<String, String> configs)`, the setter, applied in place: a
/// borrowed map of `const char *` to `const char *`, copied (`NULL` reads as
/// empty).
///
/// # Safety
///
/// `self_` must be a valid new-topic handle and `configs` null or a valid
/// map of NUL-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewTopic_set_configs(
    self_: *mut kafka_admin_NewTopic_t,
    configs: *const kafka_Map_t,
) {
    let configs: BTreeMap<String, String> = unsafe { map_strings(configs) }.into_iter().collect();
    let inner = unsafe { &mut *(self_ as *mut NewTopicInner) };
    inner.topic = inner.topic.clone().set_configs(configs);
}

/// `configs()`, the getter: an owned map of owned `char *` names to owned
/// `char *` values in ascending name order, freed together with
/// `kafka_Map_destroy`; `NULL` when no configs were set (Java returns null).
///
/// # Safety
///
/// `self_` must be a valid new-topic handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewTopic_configs(self_: *const kafka_admin_NewTopic_t) -> *mut kafka_Map_t {
    unsafe { new_topic_ref(self_) }
        .configs()
        .map_or(std::ptr::null_mut(), box_string_map)
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid new-topic handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewTopic_to_string(self_: *const kafka_admin_NewTopic_t) -> *mut c_char {
    into_c_string(&unsafe { new_topic_ref(self_) }.to_string())
}

/// Frees an owned topic handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned topic handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_NewTopic_destroy(self_: *mut kafka_admin_NewTopic_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut NewTopicInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::ffi::util::{
        kafka_List_add, kafka_List_get, kafka_List_new, kafka_List_size, kafka_Map_destroy, kafka_Map_get,
        kafka_Map_key, kafka_Map_new, kafka_Map_put, kafka_Map_size, kafka_Map_value, kafka_string_destroy,
    };

    #[test]
    fn counts_constructor_maps_minus_one_to_defaults() {
        unsafe {
            let topic = kafka_admin_NewTopic_with_num_partitions_replication_factor(c"orders".as_ptr(), 3, -1);
            assert_eq!(
                *new_topic_ref(topic),
                NewTopic::with_num_partitions_replication_factor("orders", Some(3), None)
            );
            assert_eq!(CStr::from_ptr(kafka_admin_NewTopic_name(topic)).to_str().unwrap(), "orders");
            assert_eq!(kafka_admin_NewTopic_num_partitions(topic), 3);
            assert_eq!(kafka_admin_NewTopic_replication_factor(topic), -1);
            assert!(kafka_admin_NewTopic_replicas_assignments(topic).is_null());
            assert!(kafka_admin_NewTopic_configs(topic).is_null());

            let configs = kafka_Map_new();
            kafka_Map_put(
                configs,
                c"retention.ms".as_ptr() as *mut c_void,
                c"1000".as_ptr() as *mut c_void,
            );
            kafka_admin_NewTopic_set_configs(topic, configs);
            kafka_Map_destroy(configs);
            let configs = kafka_admin_NewTopic_configs(topic);
            assert_eq!(kafka_Map_size(configs), 1);
            assert_eq!(
                CStr::from_ptr(kafka_Map_get(configs, c"retention.ms".as_ptr() as *mut c_void) as *const c_char)
                    .to_str()
                    .unwrap(),
                "1000"
            );
            kafka_Map_destroy(configs);
            let s = kafka_admin_NewTopic_to_string(topic);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), new_topic_ref(topic).to_string());
            kafka_string_destroy(s);
            kafka_admin_NewTopic_destroy(topic);
            kafka_admin_NewTopic_destroy(ptr::null_mut());
        }
    }

    #[test]
    fn assignments_constructor_copies_and_returns_the_map() {
        unsafe {
            let brokers = kafka_List_new();
            let (mut b1, mut b2) = (1_i32, 2_i32);
            kafka_List_add(brokers, &mut b1 as *mut i32 as *mut c_void);
            kafka_List_add(brokers, &mut b2 as *mut i32 as *mut c_void);
            let assignments = kafka_Map_new();
            let mut partition = 0_i32;
            kafka_Map_put(assignments, &mut partition as *mut i32 as *mut c_void, brokers as *mut c_void);
            let topic = kafka_admin_NewTopic_with_replicas_assignments(c"orders".as_ptr(), assignments);
            kafka_Map_destroy(assignments);
            kafka_List_destroy(brokers);

            let expected = NewTopic::with_replicas_assignments("orders", BTreeMap::from([(0, vec![1, 2])]));
            assert_eq!(*new_topic_ref(topic), expected);
            assert_eq!(kafka_admin_NewTopic_num_partitions(topic), -1);
            let map = kafka_admin_NewTopic_replicas_assignments(topic);
            assert_eq!(kafka_Map_size(map), 1);
            assert_eq!(*(kafka_Map_key(map, 0) as *const i32), 0);
            let mut lookup = 0_i32;
            let list = kafka_Map_get(map, &mut lookup as *mut i32 as *mut c_void) as *const kafka_List_t;
            assert_eq!(list, kafka_Map_value(map, 0) as *const kafka_List_t);
            assert_eq!(kafka_List_size(list), 2);
            assert_eq!(*(kafka_List_get(list, 1) as *const i32), 2);
            kafka_Map_destroy(map);
            kafka_admin_NewTopic_destroy(topic);
        }
    }
}
