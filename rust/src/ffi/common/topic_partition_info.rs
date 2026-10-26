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
use crate::ffi::ffi_guard;
use crate::ffi::util::{into_c_string, kafka_List_t};

/// Opaque handle to a [`TopicPartitionInfo`].
#[repr(C)]
pub struct kafka_common_TopicPartitionInfo_t {
    _private: [u8; 0],
}

/// What a `kafka_common_TopicPartitionInfo_t` points at: the value plus a
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
    // handed out by the admin result handles once the admin slice lands
    #[cfg_attr(not(test), expect(dead_code))]
    pub(crate) fn as_ptr(&self) -> *const kafka_common_TopicPartitionInfo_t {
        self as *const Self as *const kafka_common_TopicPartitionInfo_t
    }
}

/// What a handle points at.
///
/// # Safety
///
/// `info` must be a valid topic-partition-info handle, alive for `'a`.
unsafe fn inner_ref<'a>(info: *const kafka_common_TopicPartitionInfo_t) -> &'a TopicPartitionInfoInner {
    // SAFETY: Per this function's `# Safety`, `info` is a valid topic-partition-info handle alive
    // for `'a`, so it points at a live `TopicPartitionInfoInner`: the `Box::into_raw` of `boxed`,
    // or one a holder hands out through `TopicPartitionInfoInner::as_ptr`, which lives as long as
    // that holder (no holder does yet outside the tests). No call mutates a
    // `TopicPartitionInfoInner`, so the shared reference is sound for `'a`.
    unsafe { &*(info as *const TopicPartitionInfoInner) }
}

fn boxed(info: TopicPartitionInfo) -> *mut kafka_common_TopicPartitionInfo_t {
    Box::into_raw(Box::new(TopicPartitionInfoInner::new(info))) as *mut kafka_common_TopicPartitionInfo_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `info` must be a valid topic-partition-info handle, alive for `'a`.
pub(crate) unsafe fn topic_partition_info_ref<'a>(
    info: *const kafka_common_TopicPartitionInfo_t,
) -> &'a TopicPartitionInfo {
    // SAFETY: Per this function's `# Safety`, `info` is a valid topic-partition-info handle alive
    // for `'a`, as `inner_ref` requires.
    &unsafe { inner_ref(info) }.info
}

/// Hands `info` to C as an owned handle, freed with
/// `kafka_common_TopicPartitionInfo_destroy`.
pub(crate) fn box_topic_partition_info(info: TopicPartitionInfo) -> *mut kafka_common_TopicPartitionInfo_t {
    boxed(info)
}

/// Create an instance of this class with the provided parameters, with no eligible leader replicas.
///
/// # Parameters
///
/// - `partition` - the partition id
/// - `leader` - the leader of the partition or `NULL` if there is none.
/// - `replicas` - the replicas of the partition in the same order as the replica assignment (the
///   preferred replica is the head of the list)
/// - `isr` - the in-sync replicas
///
/// # Safety
///
/// `leader` must be `NULL` or a valid node handle and each list `NULL` or a
/// valid list of node handles; all are copied. The returned handle is owned, freed with
/// `kafka_common_TopicPartitionInfo_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionInfo_new(
    partition: i32,
    leader: *const kafka_common_Node_t,
    replicas: *const kafka_List_t,
    isr: *const kafka_List_t,
) -> *mut kafka_common_TopicPartitionInfo_t {
    boxed(TopicPartitionInfo::new(
        partition,
        // SAFETY: Per this function's `# Safety`, `leader` is null or a valid node handle, as
        // `optional_node` requires; the node is cloned, so nothing borrows it after this call.
        unsafe { optional_node(leader) },
        // SAFETY: Per this function's `# Safety`, `replicas` is null or a valid list of node
        // handles, as `list_nodes` requires; the nodes are cloned, so nothing borrows the list
        // after this call.
        unsafe { list_nodes(replicas) },
        // SAFETY: Per this function's `# Safety`, `isr` is null or a valid list of node handles, as
        // `list_nodes` requires; the nodes are cloned, so nothing borrows the list after this call.
        unsafe { list_nodes(isr) },
    ))
}

/// Create an instance of this class with the provided parameters.
///
/// # Parameters
///
/// - `partition` - the partition id
/// - `leader` - the leader of the partition or `NULL` if there is none.
/// - `replicas` - the replicas of the partition in the same order as the replica assignment (the
///   preferred replica is the head of the list)
/// - `isr` - the in-sync replicas
/// - `elr` - the eligible leader replicas
/// - `last_known_elr` - the last known eligible leader replicas.
///
/// # Safety
///
/// As `kafka_common_TopicPartitionInfo_new`, plus `elr` and
/// `last_known_elr` `NULL` or valid lists of node handles; a `NULL` list reads as empty.
#[ffi_guard]
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
        // SAFETY: Per this function's `# Safety`, `leader` is null or a valid node handle, as
        // `optional_node` requires; the node is cloned, so nothing borrows it after this call.
        unsafe { optional_node(leader) },
        // SAFETY: Per this function's `# Safety`, `replicas` is null or a valid list of node
        // handles, as `list_nodes` requires; the nodes are cloned, so nothing borrows the list
        // after this call.
        unsafe { list_nodes(replicas) },
        // SAFETY: Per this function's `# Safety`, `isr` is null or a valid list of node handles, as
        // `list_nodes` requires; the nodes are cloned, so nothing borrows the list after this call.
        unsafe { list_nodes(isr) },
        // SAFETY: Per this function's `# Safety`, `elr` is null or a valid list of node handles, as
        // `list_nodes` requires; the nodes are cloned, so nothing borrows the list after this call.
        unsafe { list_nodes(elr) },
        // SAFETY: Per this function's `# Safety`, `last_known_elr` is null or a valid list of node
        // handles, as `list_nodes` requires; the nodes are cloned, so nothing borrows the list
        // after this call.
        unsafe { list_nodes(last_known_elr) },
    ))
}

/// Return the partition id.
///
/// # Returns
///
/// The partition id.
///
/// # Safety
///
/// `self_` must be a valid topic-partition-info handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionInfo_partition(
    self_: *const kafka_common_TopicPartitionInfo_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid topic-partition-info handle, as
    // `inner_ref` requires; the reference is used only within this call.
    unsafe { inner_ref(self_) }.info.partition()
}

/// Return the leader of the partition or `NULL` if there is none.
///
/// # Returns
///
/// The leader, or `NULL`.
///
/// # Safety
///
/// `self_` must be a valid topic-partition-info handle.
/// The returned handle is borrowed from the handle, valid until it is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionInfo_leader(
    self_: *const kafka_common_TopicPartitionInfo_t,
) -> *const kafka_common_Node_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid topic-partition-info handle, as
    // `inner_ref` requires. The node returned is the inner's own cached `leader`, so it lives as
    // long as the handle; that is the rustdoc's "borrowed from the handle".
    optional_node_ptr(unsafe { inner_ref(self_) }.leader.as_ref())
}

/// Return the replicas of the partition in the same order as the replica assignment. The preferred replica is the
/// head of the list.
///
/// Brokers with version lower than 0.11.0.0 return the replicas in unspecified order due to a bug.
///
/// # Returns
///
/// The replicas.
///
/// # Safety
///
/// `self_` must be a valid topic-partition-info handle.
/// The returned list of `kafka_common_Node_t *` copies is owned, freed with `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionInfo_replicas(
    self_: *const kafka_common_TopicPartitionInfo_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid topic-partition-info handle, as
    // `inner_ref` requires; the nodes are cloned into the owned list returned, so the borrow lasts
    // for this call only.
    node_list(unsafe { inner_ref(self_) }.info.replicas())
}

/// Return the in-sync replicas of the partition. Note that the ordering of the result is unspecified.
///
/// # Returns
///
/// The in-sync replicas.
///
/// # Safety
///
/// `self_` must be a valid topic-partition-info handle.
/// The returned list of `kafka_common_Node_t *` copies is owned, freed with `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionInfo_isr(
    self_: *const kafka_common_TopicPartitionInfo_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid topic-partition-info handle, as
    // `inner_ref` requires; the nodes are cloned into the owned list returned, so the borrow lasts
    // for this call only.
    node_list(unsafe { inner_ref(self_) }.info.isr())
}

/// Return the eligible leader replicas of the partition. Note that the ordering of the result is unspecified.
///
/// # Returns
///
/// The eligible leader replicas, or `NULL` when the broker did not report them.
///
/// # Safety
///
/// `self_` must be a valid topic-partition-info handle.
/// The returned list of `kafka_common_Node_t *` copies is owned, freed with `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionInfo_elr(
    self_: *const kafka_common_TopicPartitionInfo_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid topic-partition-info handle, as
    // `inner_ref` requires; the nodes are cloned into the owned list returned, so the borrow lasts
    // for this call only.
    unsafe { inner_ref(self_) }.info.elr().map_or(ptr::null_mut(), node_list)
}

/// Return the last known eligible leader replicas of the partition. Note that the ordering of the result is
/// unspecified.
///
/// # Returns
///
/// The last known eligible leader replicas, or `NULL` when the broker did not report them.
///
/// # Safety
///
/// `self_` must be a valid topic-partition-info handle.
/// The returned list of `kafka_common_Node_t *` copies is owned, freed with `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionInfo_last_known_elr(
    self_: *const kafka_common_TopicPartitionInfo_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid topic-partition-info handle, as
    // `inner_ref` requires; the nodes are cloned into the owned list returned, so the borrow lasts
    // for this call only.
    unsafe { inner_ref(self_) }
        .info
        .last_known_elr()
        .map_or(ptr::null_mut(), node_list)
}

/// Returns `(partition=<id>, leader=<leader>, replicas=<nodes>, isr=<nodes>, elr=<nodes>, lastKnownElr=<nodes>)`, a
/// `NULL` elr list printed as `N/A`.
///
/// # Safety
///
/// `self_` must be a valid topic-partition-info handle.
/// The returned string is owned, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionInfo_to_string(
    self_: *const kafka_common_TopicPartitionInfo_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid topic-partition-info handle, as
    // `inner_ref` requires; the reference is used only within this call, and the text is copied
    // into an owned string.
    into_c_string(&unsafe { inner_ref(self_) }.info.to_string())
}

/// Frees an owned topic-partition-info handle. Null is a no-op; a handle
/// borrowed from a `kafka_admin_TopicDescription_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionInfo_destroy(self_: *mut kafka_common_TopicPartitionInfo_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, i.e. the `Box::into_raw` of `boxed`, so `Box::from_raw`
        // reclaims it exactly once, together with the leader node it caches.
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
        // SAFETY: Test code: every handle passed is a view from `TopicPartitionInfoInner::as_ptr`
        // on a local inner that outlives the calls; each list returned is destroyed once, after it
        // is read.
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
        // SAFETY: Test code: the null lists and leader are accepted by the constructors; each
        // handle comes from a constructor and is destroyed once, after the calls reading it, and
        // the `elr` list is destroyed once, after it is read.
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
