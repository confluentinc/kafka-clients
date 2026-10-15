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

//! `kafka_common_TopicPartition_t`: `org.apache.kafka.common.TopicPartition`
//! (CLAUDE.md §4).
//!
//! The handle owns a [`TopicPartition`] plus the NUL-terminated copy of its
//! topic that the borrowed string getter hands out. The module also hosts the
//! crate-internal helpers every other surface uses to move topic-partitions
//! across the boundary: single handles, `kafka_List_t`s of them and the
//! `kafka_Map_t` shape of Java's `Map<TopicPartition, Long>`.

use std::collections::HashMap;
use std::ffi::{CString, c_char, c_void};

use crate::common::TopicPartition;
use crate::ffi::ffi_guard;
use crate::ffi::util::{
    box_list, box_map, c_str_to_string, destroy_boxed, into_c_string, kafka_List_t, kafka_Map_t, list_elements,
    map_entries, owned_c_string,
};

/// Opaque handle to a [`TopicPartition`].
#[repr(C)]
pub struct kafka_common_TopicPartition_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_TopicPartition_t`] points at: the value plus the
/// NUL-terminated topic the string getter borrows out.
pub(crate) struct TopicPartitionInner {
    tp: TopicPartition,
    topic_c: CString,
}

impl TopicPartitionInner {
    pub(crate) fn new(tp: TopicPartition) -> Self {
        let topic_c = owned_c_string(tp.topic());
        Self { tp, topic_c }
    }

    pub(crate) fn topic_partition(&self) -> &TopicPartition {
        &self.tp
    }

    /// The NUL-terminated topic, valid as long as `self`.
    pub(crate) fn topic_ptr(&self) -> *const c_char {
        self.topic_c.as_ptr()
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_TopicPartition_t {
        self as *const Self as *const kafka_common_TopicPartition_t
    }
}

/// Hands `tp` to C as an owned handle, freed with
/// [`kafka_common_TopicPartition_destroy`].
pub(crate) fn box_topic_partition(tp: TopicPartition) -> *mut kafka_common_TopicPartition_t {
    Box::into_raw(Box::new(TopicPartitionInner::new(tp))) as *mut kafka_common_TopicPartition_t
}

/// The topic-partition behind a handle.
///
/// # Safety
///
/// `tp` must be a valid topic-partition handle, alive for `'a`.
pub(crate) unsafe fn topic_partition_ref<'a>(tp: *const kafka_common_TopicPartition_t) -> &'a TopicPartition {
    // SAFETY: Per this function's `# Safety`, `tp` is a valid topic-partition handle alive for
    // `'a`, so it points at a live `TopicPartitionInner`: the `Box::into_raw` of
    // `box_topic_partition`, or one a parent embeds and hands out through
    // `TopicPartitionInner::as_ptr`, which lives as long as that parent. No call mutates a
    // `TopicPartitionInner`, so the shared reference is sound for `'a`.
    unsafe { &*(tp as *const TopicPartitionInner) }.topic_partition()
}

/// Hands `tps` to C as an owned list of `kafka_common_TopicPartition_t *`,
/// in iteration order.
pub(crate) fn topic_partition_list(tps: impl IntoIterator<Item = TopicPartition>) -> *mut kafka_List_t {
    let elements = tps.into_iter().map(|tp| box_topic_partition(tp) as *mut c_void).collect();
    box_list(elements, Some(destroy_boxed::<TopicPartitionInner>))
}

/// Hands `tps` to C as an owned list ordered by topic, then partition: the
/// order a `HashSet`/`HashMap` of topic-partitions lacks and a C test needs.
pub(crate) fn sorted_topic_partition_list<'a>(tps: impl IntoIterator<Item = &'a TopicPartition>) -> *mut kafka_List_t {
    let mut tps: Vec<TopicPartition> = tps.into_iter().cloned().collect();
    tps.sort_by(|a, b| a.topic().cmp(b.topic()).then(a.partition().cmp(&b.partition())));
    topic_partition_list(tps)
}

/// Reads a list of `const kafka_common_TopicPartition_t *` into owned
/// values; null reads as empty.
///
/// # Safety
///
/// `list` must be null or a valid list whose elements are topic-partition
/// handles.
pub(crate) unsafe fn list_topic_partitions(list: *const kafka_List_t) -> Vec<TopicPartition> {
    // SAFETY: Per this function's `# Safety`, `list` is null or a valid list handle, as
    // `list_elements` requires; the slice is used only within this call, while nothing mutates the
    // list.
    unsafe { list_elements(list) }
        .iter()
        // SAFETY: Per this function's `# Safety`, every element of `list` is a topic-partition
        // handle, as `topic_partition_ref` requires; the value is cloned, so the borrow lasts for
        // this call only.
        .map(|&element| unsafe { topic_partition_ref(element as *const kafka_common_TopicPartition_t) }.clone())
        .collect()
}

/// Compares two `kafka_common_TopicPartition_t` keys by value, for
/// `kafka_Map_get` on a Rust-built map keyed by topic-partition.
///
/// # Safety
///
/// `a` and `b` must each be null or a valid topic-partition handle.
pub(crate) unsafe fn topic_partition_key_eq(a: *mut c_void, b: *mut c_void) -> bool {
    if a.is_null() || b.is_null() {
        return a == b;
    }
    // SAFETY: `a` and `b` are non-null (checked above) and, per this function's `# Safety`, valid
    // topic-partition handles, as `topic_partition_ref` requires; both references are used only for
    // the comparison.
    unsafe {
        topic_partition_ref(a as *const kafka_common_TopicPartition_t)
            == topic_partition_ref(b as *const kafka_common_TopicPartition_t)
    }
}

/// Hands a Java `Map<TopicPartition, Long>` to C as an owned map: keys are
/// `kafka_common_TopicPartition_t *`, values `int64_t *`, entries ordered by
/// topic then partition, and `kafka_Map_get` compares keys by value.
pub(crate) fn topic_partition_i64_map(map: &HashMap<TopicPartition, i64>) -> *mut kafka_Map_t {
    let mut entries: Vec<(&TopicPartition, i64)> = map.iter().map(|(tp, &v)| (tp, v)).collect();
    entries.sort_by(|(a, _), (b, _)| a.topic().cmp(b.topic()).then(a.partition().cmp(&b.partition())));
    let entries = entries
        .into_iter()
        .map(|(tp, v)| {
            (
                box_topic_partition(tp.clone()) as *mut c_void,
                Box::into_raw(Box::new(v)) as *mut c_void,
            )
        })
        .collect();
    box_map(
        entries,
        Some(destroy_boxed::<TopicPartitionInner>),
        Some(destroy_boxed::<i64>),
        Some(topic_partition_key_eq),
    )
}

/// Reads a map of `const kafka_common_TopicPartition_t *` keys and
/// `const int64_t *` values; null reads as empty.
///
/// # Safety
///
/// `map` must be null or a valid map whose keys are topic-partition handles
/// and whose values point at `int64_t`s.
pub(crate) unsafe fn map_topic_partition_i64(map: *const kafka_Map_t) -> HashMap<TopicPartition, i64> {
    // SAFETY: Per this function's `# Safety`, `map` is null or a valid map handle, as `map_entries`
    // requires; the slice is used only within this call, while nothing mutates the map.
    unsafe { map_entries(map) }
        .iter()
        .map(|&(k, v)| {
            // SAFETY: Per this function's `# Safety`, every key of `map` is a topic-partition
            // handle, as `topic_partition_ref` requires; the value is cloned, so the borrow lasts
            // for this call only.
            let tp = unsafe { topic_partition_ref(k as *const kafka_common_TopicPartition_t) }.clone();
            let offset = if v.is_null() {
                0
            } else {
                // SAFETY: `v` is non-null (checked above) and, per this function's `# Safety`,
                // points at an `int64_t`, so the read is sound.
                unsafe { *(v as *const i64) }
            };
            (tp, offset)
        })
        .collect()
}

/// `new TopicPartition(String topic, int partition)`, as an owned handle
/// freed with [`kafka_common_TopicPartition_destroy`].
///
/// # Safety
///
/// `topic` must be a valid NUL-terminated string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartition_new(
    topic: *const c_char,
    partition: i32,
) -> *mut kafka_common_TopicPartition_t {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    box_topic_partition(TopicPartition::new(unsafe { c_str_to_string(topic) }, partition))
}

/// `topic()`: the topic, borrowed from the handle and valid until it is
/// destroyed.
///
/// # Safety
///
/// `self_` must be a valid topic-partition handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartition_topic(
    self_: *const kafka_common_TopicPartition_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid topic-partition handle, so it
    // points at a live `TopicPartitionInner`: the `Box::into_raw` of `box_topic_partition`, or one
    // a parent embeds and hands out through `TopicPartitionInner::as_ptr`, which lives as long as
    // that parent. No call mutates a `TopicPartitionInner`, so the shared reference is sound for
    // this call. The string returned is the inner's own `topic_c`, so it lives as long as the
    // handle; that is the rustdoc's "borrowed from the handle and valid until it is destroyed".
    unsafe { &*(self_ as *const TopicPartitionInner) }.topic_ptr()
}

/// `partition()`.
///
/// # Safety
///
/// `self_` must be a valid topic-partition handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartition_partition(self_: *const kafka_common_TopicPartition_t) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid topic-partition handle, as
    // `topic_partition_ref` requires; the reference is used only within this call.
    unsafe { topic_partition_ref(self_) }.partition()
}

/// `toString()`: `topic-partition`, as an owned string freed with
/// `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid topic-partition handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartition_to_string(
    self_: *const kafka_common_TopicPartition_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid topic-partition handle, as
    // `topic_partition_ref` requires; the reference is used only within this call, and the text is
    // copied into an owned string.
    into_c_string(&unsafe { topic_partition_ref(self_) }.to_string())
}

/// Frees an owned topic-partition handle. Null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned topic-partition handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartition_destroy(self_: *mut kafka_common_TopicPartition_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // topic-partition handle not yet destroyed, i.e. the `Box::into_raw` of
        // `box_topic_partition`, so `Box::from_raw` reclaims it exactly once. A borrowed handle
        // from `TopicPartitionInner::as_ptr` is never passed here.
        drop(unsafe { Box::from_raw(self_ as *mut TopicPartitionInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::ffi::util::{kafka_List_destroy, kafka_List_get, kafka_List_size, kafka_Map_destroy, kafka_Map_get};

    #[test]
    fn handle_round_trips_topic_partition_and_display() {
        let topic = CString::new("orders").unwrap();
        // SAFETY: Test code: `topic` outlives the constructor, which copies it.
        let tp = unsafe { kafka_common_TopicPartition_new(topic.as_ptr(), 7) };
        // SAFETY: Test code: `tp` comes from `kafka_common_TopicPartition_new` and is destroyed
        // once, after the topic it lends and the string from `to_string`, itself destroyed once,
        // are read.
        unsafe {
            assert_eq!(
                CStr::from_ptr(kafka_common_TopicPartition_topic(tp)).to_str().unwrap(),
                "orders"
            );
            assert_eq!(kafka_common_TopicPartition_partition(tp), 7);
            let s = kafka_common_TopicPartition_to_string(tp);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), "orders-7");
            crate::ffi::util::kafka_string_destroy(s);
            kafka_common_TopicPartition_destroy(tp);
            kafka_common_TopicPartition_destroy(ptr::null_mut());
        }
    }

    #[test]
    fn sorted_list_orders_by_topic_then_partition() {
        let tps = [
            TopicPartition::new("b", 0),
            TopicPartition::new("a", 2),
            TopicPartition::new("a", 1),
        ];
        let list = sorted_topic_partition_list(tps.iter());
        // SAFETY: Test code: `list` comes from `sorted_topic_partition_list`, whose elements are
        // topic-partition handles, and is destroyed once, after the references read from it are
        // last used.
        unsafe {
            assert_eq!(kafka_List_size(list), 3);
            let read = |i| {
                let tp = topic_partition_ref(kafka_List_get(list, i) as *const kafka_common_TopicPartition_t);
                (tp.topic().to_string(), tp.partition())
            };
            assert_eq!(read(0), ("a".to_string(), 1));
            assert_eq!(read(1), ("a".to_string(), 2));
            assert_eq!(read(2), ("b".to_string(), 0));
            assert_eq!(list_topic_partitions(list).len(), 3);
            kafka_List_destroy(list);
        }
    }

    #[test]
    fn i64_map_round_trips_and_compares_keys_by_value() {
        let mut map = HashMap::new();
        map.insert(TopicPartition::new("t", 1), 10_i64);
        map.insert(TopicPartition::new("t", 0), 5_i64);
        let c_map = topic_partition_i64_map(&map);
        // A fresh handle with the same value finds the entry: keys compare by
        // value, not by pointer.
        let probe = box_topic_partition(TopicPartition::new("t", 1));
        // SAFETY: Test code: `c_map` comes from `topic_partition_i64_map`, whose keys are
        // topic-partition handles and whose values point at `i64`s, and is destroyed once, after
        // the value read from it; `probe` comes from `box_topic_partition` and is destroyed once,
        // after the lookup.
        unsafe {
            let value = kafka_Map_get(c_map, probe as *mut c_void) as *const i64;
            assert_eq!(*value, 10);
            assert_eq!(map_topic_partition_i64(c_map), map);
            assert!(map_topic_partition_i64(ptr::null()).is_empty());
            kafka_common_TopicPartition_destroy(probe);
            kafka_Map_destroy(c_map);
        }
    }
}
