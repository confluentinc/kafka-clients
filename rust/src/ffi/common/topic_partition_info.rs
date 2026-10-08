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

//! `kafka_common_TopicPartitionInfo_t`:
//! `org.apache.kafka.common.TopicPartitionInfo` (CLAUDE.md §4).
//!
//! Java's `elr()` / `lastKnownElr()` are null for a partition built with the
//! four-argument constructor and an empty list for one the broker reported
//! without eligible leader replicas; the C getters keep the distinction by
//! returning null or an empty `kafka_List_t`.

use std::ffi::c_char;
use std::ptr;

use crate::common::TopicPartitionInfo;
use crate::ffi::common::node::{
    NodeInner, kafka_common_Node_t, list_nodes, node_list, optional_node, optional_node_ptr,
};
use crate::ffi::util::{into_c_string, kafka_List_t};

/// Opaque handle to a [`TopicPartitionInfo`].
#[repr(C)]
pub struct kafka_common_TopicPartitionInfo_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_TopicPartitionInfo_t`] points at: the value plus a
/// cached leader so `leader()` is a borrowed node valid as long as the
/// handle.
pub(crate) struct TopicPartitionInfoInner {
    info: TopicPartitionInfo,
    leader: Option<NodeInner>,
}

impl TopicPartitionInfoInner {
    pub(crate) fn new(info: TopicPartitionInfo) -> Self {
        let leader = info.leader().cloned().map(NodeInner::new);
        Self { info, leader }
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_TopicPartitionInfo_t {
        self as *const Self as *const kafka_common_TopicPartitionInfo_t
    }
}

unsafe fn inner_ref<'a>(info: *const kafka_common_TopicPartitionInfo_t) -> &'a TopicPartitionInfoInner {
    unsafe { &*(info as *const TopicPartitionInfoInner) }
}

fn boxed(info: TopicPartitionInfo) -> *mut kafka_common_TopicPartitionInfo_t {
    Box::into_raw(Box::new(TopicPartitionInfoInner::new(info))) as *mut kafka_common_TopicPartitionInfo_t
}

/// `new TopicPartitionInfo(int partition, Node leader, List<Node> replicas,
/// List<Node> isr)`, leaving `elr` and `lastKnownElr` null. `leader` is
/// nullable; the lists hold `const kafka_common_Node_t *` and are copied.
/// Owned, freed with [`kafka_common_TopicPartitionInfo_destroy`].
///
/// # Safety
///
/// `leader` must be null or a valid node handle and each list null or a
/// valid list of node handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionInfo_new(
    partition: i32,
    leader: *const kafka_common_Node_t,
    replicas: *const kafka_List_t,
    isr: *const kafka_List_t,
) -> *mut kafka_common_TopicPartitionInfo_t {
    boxed(TopicPartitionInfo::new(
        partition,
        unsafe { optional_node(leader) },
        unsafe { list_nodes(replicas) },
        unsafe { list_nodes(isr) },
    ))
}

/// `new TopicPartitionInfo(int partition, Node leader, List<Node> replicas,
/// List<Node> isr, List<Node> elr, List<Node> lastKnownElr)`. A null `elr`
/// or `last_known_elr` list is an empty one, as Java's constructor requires
/// non-null lists.
///
/// # Safety
///
/// As [`kafka_common_TopicPartitionInfo_new`], plus `elr` and
/// `last_known_elr` null or valid lists of node handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionInfo_with_elr_last_known_elr(
    partition: i32,
    leader: *const kafka_common_Node_t,
    replicas: *const kafka_List_t,
    isr: *const kafka_List_t,
    elr: *const kafka_List_t,
    last_known_elr: *const kafka_List_t,
) -> *mut kafka_common_TopicPartitionInfo_t {
    boxed(TopicPartitionInfo::with_elr_last_known_elr(
        partition,
        unsafe { optional_node(leader) },
        unsafe { list_nodes(replicas) },
        unsafe { list_nodes(isr) },
        unsafe { list_nodes(elr) },
        unsafe { list_nodes(last_known_elr) },
    ))
}

/// `partition()`.
///
/// # Safety
///
/// `self_` must be a valid topic-partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionInfo_partition(
    self_: *const kafka_common_TopicPartitionInfo_t,
) -> i32 {
    unsafe { inner_ref(self_) }.info.partition()
}

/// `leader()`: borrowed from the handle, or null when there is no leader.
///
/// # Safety
///
/// `self_` must be a valid topic-partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionInfo_leader(
    self_: *const kafka_common_TopicPartitionInfo_t,
) -> *const kafka_common_Node_t {
    optional_node_ptr(unsafe { inner_ref(self_) }.leader.as_ref())
}

/// `replicas()`: an owned list of `kafka_common_Node_t *` copies, freed with
/// `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid topic-partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionInfo_replicas(
    self_: *const kafka_common_TopicPartitionInfo_t,
) -> *mut kafka_List_t {
    node_list(unsafe { inner_ref(self_) }.info.replicas())
}

/// `isr()`: an owned list of `kafka_common_Node_t *` copies.
///
/// # Safety
///
/// `self_` must be a valid topic-partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionInfo_isr(
    self_: *const kafka_common_TopicPartitionInfo_t,
) -> *mut kafka_List_t {
    node_list(unsafe { inner_ref(self_) }.info.isr())
}

/// `elr()`: an owned list of `kafka_common_Node_t *` copies, or null when the
/// broker did not report eligible leader replicas (Java's null).
///
/// # Safety
///
/// `self_` must be a valid topic-partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionInfo_elr(
    self_: *const kafka_common_TopicPartitionInfo_t,
) -> *mut kafka_List_t {
    unsafe { inner_ref(self_) }.info.elr().map_or(ptr::null_mut(), node_list)
}

/// `lastKnownElr()`: an owned list of `kafka_common_Node_t *` copies, or null
/// when the broker did not report it (Java's null).
///
/// # Safety
///
/// `self_` must be a valid topic-partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionInfo_last_known_elr(
    self_: *const kafka_common_TopicPartitionInfo_t,
) -> *mut kafka_List_t {
    unsafe { inner_ref(self_) }
        .info
        .last_known_elr()
        .map_or(ptr::null_mut(), node_list)
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid topic-partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionInfo_to_string(
    self_: *const kafka_common_TopicPartitionInfo_t,
) -> *mut c_char {
    into_c_string(&unsafe { inner_ref(self_) }.info.to_string())
}

/// Frees an owned topic-partition-info handle. Null is a no-op; a handle
/// borrowed from a `kafka_admin_TopicDescription_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionInfo_destroy(self_: *mut kafka_common_TopicPartitionInfo_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut TopicPartitionInfoInner) });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Node;
    use crate::ffi::util::{kafka_List_destroy, kafka_List_size};

    /// Java's `elr()` / `lastKnownElr()` are null for a partition built with
    /// the four-argument constructor and an empty list for a reported-but-empty
    /// set; the C getters keep the two apart as a null vs an empty list.
    #[test]
    fn absent_elr_is_null_and_reported_empty_is_an_empty_list() {
        let absent = TopicPartitionInfoInner::new(TopicPartitionInfo::new(0, None, vec![], vec![]));
        let reported_empty = TopicPartitionInfoInner::new(TopicPartitionInfo::with_elr_last_known_elr(
            0,
            None,
            vec![],
            vec![],
            vec![],
            vec![],
        ));
        let reported = TopicPartitionInfoInner::new(TopicPartitionInfo::with_elr_last_known_elr(
            0,
            None,
            vec![],
            vec![],
            vec![Node::new(1, "h1".to_string(), 9091)],
            vec![
                Node::new(2, "h2".to_string(), 9092),
                Node::new(3, "h3".to_string(), 9093),
            ],
        ));
        unsafe {
            assert!(kafka_common_TopicPartitionInfo_elr(absent.as_ptr()).is_null());
            assert!(kafka_common_TopicPartitionInfo_last_known_elr(absent.as_ptr()).is_null());
            assert!(kafka_common_TopicPartitionInfo_leader(absent.as_ptr()).is_null());

            let elr = kafka_common_TopicPartitionInfo_elr(reported_empty.as_ptr());
            assert!(!elr.is_null());
            assert_eq!(kafka_List_size(elr), 0);
            kafka_List_destroy(elr);
            let last_known = kafka_common_TopicPartitionInfo_last_known_elr(reported_empty.as_ptr());
            assert!(!last_known.is_null());
            assert_eq!(kafka_List_size(last_known), 0);
            kafka_List_destroy(last_known);

            // Distinct lengths, so swapping the two accessors fails.
            let elr = kafka_common_TopicPartitionInfo_elr(reported.as_ptr());
            assert_eq!(kafka_List_size(elr), 1);
            kafka_List_destroy(elr);
            let last_known = kafka_common_TopicPartitionInfo_last_known_elr(reported.as_ptr());
            assert_eq!(kafka_List_size(last_known), 2);
            kafka_List_destroy(last_known);
        }
    }

    #[test]
    fn constructors_build_owned_handles() {
        unsafe {
            let plain = kafka_common_TopicPartitionInfo_new(4, ptr::null(), ptr::null(), ptr::null());
            assert_eq!(kafka_common_TopicPartitionInfo_partition(plain), 4);
            assert!(kafka_common_TopicPartitionInfo_elr(plain).is_null());
            kafka_common_TopicPartitionInfo_destroy(plain);

            let with_elr = kafka_common_TopicPartitionInfo_with_elr_last_known_elr(
                4,
                ptr::null(),
                ptr::null(),
                ptr::null(),
                ptr::null(),
                ptr::null(),
            );
            let elr = kafka_common_TopicPartitionInfo_elr(with_elr);
            assert!(!elr.is_null());
            kafka_List_destroy(elr);
            kafka_common_TopicPartitionInfo_destroy(with_elr);
            kafka_common_TopicPartitionInfo_destroy(ptr::null_mut());
        }
    }
}
