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

//! `kafka_common_PartitionInfo_t`: `org.apache.kafka.common.PartitionInfo`
//! (CLAUDE.md §4).
//!
//! The handle owns a [`PartitionInfo`], the NUL-terminated topic its string
//! getter borrows out and a cached [`NodeInner`] for the leader, so
//! `leader()` is a borrowed `kafka_common_Node_t` valid as long as the
//! handle. The replica lists are `java.util.List<Node>`s and cross as owned
//! `kafka_List_t`s of node copies.

use std::ffi::{CString, c_char, c_void};

use crate::common::PartitionInfo;
use crate::ffi::common::node::{
    NodeInner, kafka_common_Node_t, list_nodes, node_list, optional_node, optional_node_ptr,
};
use crate::ffi::ffi_guard;
use crate::ffi::util::{
    box_list, c_str_to_string, destroy_boxed, into_c_string, kafka_List_t, list_elements, owned_c_string,
};

/// Opaque handle to a [`PartitionInfo`].
#[repr(C)]
pub struct kafka_common_PartitionInfo_t {
    _private: [u8; 0],
}

/// What a `kafka_common_PartitionInfo_t` points at.
pub(crate) struct PartitionInfoInner {
    info: PartitionInfo,
    topic_c: CString,
    leader: Option<NodeInner>,
}

impl PartitionInfoInner {
    pub(crate) fn new(info: PartitionInfo) -> Self {
        let topic_c = owned_c_string(info.topic());
        let leader = info.leader().cloned().map(NodeInner::new);
        Self { info, topic_c, leader }
    }

    pub(crate) fn info(&self) -> &PartitionInfo {
        &self.info
    }

    /// A borrowed handle on the cached leader, null when there is none.
    pub(crate) fn leader_ptr(&self) -> *const kafka_common_Node_t {
        optional_node_ptr(self.leader.as_ref())
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_PartitionInfo_t {
        self as *const Self as *const kafka_common_PartitionInfo_t
    }
}

/// Hands `info` to C as an owned handle, freed with
/// `kafka_common_PartitionInfo_destroy`.
pub(crate) fn box_partition_info(info: PartitionInfo) -> *mut kafka_common_PartitionInfo_t {
    Box::into_raw(Box::new(PartitionInfoInner::new(info))) as *mut kafka_common_PartitionInfo_t
}

/// The partition info behind a handle.
///
/// # Safety
///
/// `info` must be a valid partition-info handle, alive for `'a`.
pub(crate) unsafe fn partition_info_ref<'a>(info: *const kafka_common_PartitionInfo_t) -> &'a PartitionInfo {
    // SAFETY: Per this function's `# Safety`, `info` is a valid partition-info handle alive for
    // `'a`, so it points at a live `PartitionInfoInner`: the `Box::into_raw` of
    // `box_partition_info`, or one a `kafka_common_Cluster_t` stores and hands out through
    // `PartitionInfoInner::as_ptr`, which lives as long as that cluster. No call mutates a
    // `PartitionInfoInner`, so the shared reference is sound for `'a`.
    unsafe { &*(info as *const PartitionInfoInner) }.info()
}

/// What a handle points at.
///
/// # Safety
///
/// `info` must be a valid partition-info handle, alive for `'a`.
unsafe fn inner_ref<'a>(info: *const kafka_common_PartitionInfo_t) -> &'a PartitionInfoInner {
    // SAFETY: Per this function's `# Safety`, `info` is a valid partition-info handle alive for
    // `'a`, so it points at a live `PartitionInfoInner`: the `Box::into_raw` of
    // `box_partition_info`, or one a `kafka_common_Cluster_t` stores and hands out through
    // `PartitionInfoInner::as_ptr`, which lives as long as that cluster. No call mutates a
    // `PartitionInfoInner`, so the shared reference is sound for `'a`.
    unsafe { &*(info as *const PartitionInfoInner) }
}

/// Hands copies of `infos` to C as an owned list of
/// `kafka_common_PartitionInfo_t *`.
pub(crate) fn partition_info_list(infos: &[PartitionInfo]) -> *mut kafka_List_t {
    let elements = infos
        .iter()
        .map(|info| box_partition_info(info.clone()) as *mut c_void)
        .collect();
    box_list(elements, Some(destroy_boxed::<PartitionInfoInner>))
}

/// Reads a list of `const kafka_common_PartitionInfo_t *` into owned copies;
/// null reads as empty.
///
/// # Safety
///
/// `list` must be null or a valid list whose elements are partition-info
/// handles.
pub(crate) unsafe fn list_partition_infos(list: *const kafka_List_t) -> Vec<PartitionInfo> {
    // SAFETY: Per this function's `# Safety`, `list` is null or a valid list handle, as
    // `list_elements` requires; the slice is used only within this call, while nothing mutates the
    // list.
    unsafe { list_elements(list) }
        .iter()
        // SAFETY: Per this function's `# Safety`, every element of `list` is a partition-info
        // handle, as `partition_info_ref` requires; the value is cloned, so the borrow lasts for
        // this call only.
        .map(|&element| unsafe { partition_info_ref(element as *const kafka_common_PartitionInfo_t) }.clone())
        .collect()
}

/// Creates a partition info with no offline replicas.
///
/// # Parameters
///
/// - `topic` - the topic name
/// - `partition` - the partition id
/// - `leader` - the leader node, or `NULL` if there is none
/// - `replicas` - the replicas, preferred replica first
/// - `in_sync_replicas` - the in-sync replicas
///
/// # Safety
///
/// `topic` must be a valid NUL-terminated string, `leader` `NULL` or a valid
/// node handle, and each list `NULL` or a valid list of node handles; all are copied. The
/// returned handle is owned, freed with `kafka_common_PartitionInfo_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_new(
    topic: *const c_char,
    partition: i32,
    leader: *const kafka_common_Node_t,
    replicas: *const kafka_List_t,
    in_sync_replicas: *const kafka_List_t,
) -> *mut kafka_common_PartitionInfo_t {
    box_partition_info(PartitionInfo::new(
        // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(topic) },
        partition,
        // SAFETY: Per this function's `# Safety`, `leader` is null or a valid node handle, as
        // `optional_node` requires; the node is cloned, so nothing borrows it after this call.
        unsafe { optional_node(leader) },
        // SAFETY: Per this function's `# Safety`, `replicas` is null or a valid list of node
        // handles, as `list_nodes` requires; the nodes are cloned, so nothing borrows the list
        // after this call.
        unsafe { list_nodes(replicas) },
        // SAFETY: Per this function's `# Safety`, `in_sync_replicas` is null or a valid list of
        // node handles, as `list_nodes` requires; the nodes are cloned, so nothing borrows the list
        // after this call.
        unsafe { list_nodes(in_sync_replicas) },
    ))
}

/// Creates a partition info.
///
/// # Parameters
///
/// - `topic` - the topic name
/// - `partition` - the partition id
/// - `leader` - the leader node, or `NULL` if there is none
/// - `replicas` - the replicas, preferred replica first
/// - `in_sync_replicas` - the in-sync replicas
/// - `offline_replicas` - the offline replicas
///
/// # Safety
///
/// As `kafka_common_PartitionInfo_new`, plus `offline_replicas` `NULL` or a
/// valid list of node handles.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_with_offline_replicas(
    topic: *const c_char,
    partition: i32,
    leader: *const kafka_common_Node_t,
    replicas: *const kafka_List_t,
    in_sync_replicas: *const kafka_List_t,
    offline_replicas: *const kafka_List_t,
) -> *mut kafka_common_PartitionInfo_t {
    box_partition_info(PartitionInfo::with_offline_replicas(
        // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(topic) },
        partition,
        // SAFETY: Per this function's `# Safety`, `leader` is null or a valid node handle, as
        // `optional_node` requires; the node is cloned, so nothing borrows it after this call.
        unsafe { optional_node(leader) },
        // SAFETY: Per this function's `# Safety`, `replicas` is null or a valid list of node
        // handles, as `list_nodes` requires; the nodes are cloned, so nothing borrows the list
        // after this call.
        unsafe { list_nodes(replicas) },
        // SAFETY: Per this function's `# Safety`, `in_sync_replicas` is null or a valid list of
        // node handles, as `list_nodes` requires; the nodes are cloned, so nothing borrows the list
        // after this call.
        unsafe { list_nodes(in_sync_replicas) },
        // SAFETY: Per this function's `# Safety`, `offline_replicas` is null or a valid list of
        // node handles, as `list_nodes` requires; the nodes are cloned, so nothing borrows the list
        // after this call.
        unsafe { list_nodes(offline_replicas) },
    ))
}

/// Returns the topic name
///
/// # Safety
///
/// `self_` must be a valid partition-info handle.
/// The string is borrowed from the handle, valid until it is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_topic(self_: *const kafka_common_PartitionInfo_t) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid partition-info handle, as
    // `inner_ref` requires. The string returned is the inner's own `topic_c`, so it lives as long
    // as the handle; that is the rustdoc's "borrowed from the handle, valid until it is destroyed".
    unsafe { inner_ref(self_) }.topic_c.as_ptr()
}

/// Returns the partition id.
///
/// # Safety
///
/// `self_` must be a valid partition-info handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_partition(self_: *const kafka_common_PartitionInfo_t) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid partition-info handle, as
    // `partition_info_ref` requires; the reference is used only within this call.
    unsafe { partition_info_ref(self_) }.partition()
}

/// Returns the node currently acting as a leader for this partition or `NULL` if there is no leader
///
/// # Safety
///
/// `self_` must be a valid partition-info handle.
/// The returned handle is borrowed from the handle, valid until it is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_leader(
    self_: *const kafka_common_PartitionInfo_t,
) -> *const kafka_common_Node_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid partition-info handle, as
    // `inner_ref` requires. The node returned is the inner's own cached `leader`, so it lives as
    // long as the handle; that is the rustdoc's "borrowed from the handle".
    unsafe { inner_ref(self_) }.leader_ptr()
}

/// Returns the complete set of replicas for this partition regardless of whether they are alive or up-to-date.
/// The preferred replica is the head of the list.
///
/// # Safety
///
/// `self_` must be a valid partition-info handle.
/// The returned list of `kafka_common_Node_t *` copies is owned, freed with `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_replicas(
    self_: *const kafka_common_PartitionInfo_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid partition-info handle, as
    // `partition_info_ref` requires; the nodes are cloned into the owned list returned, so the
    // borrow lasts for this call only.
    node_list(unsafe { partition_info_ref(self_) }.replicas())
}

/// Returns the subset of the replicas that are in sync, that is caught-up to the leader and ready to take over as
/// leader if the leader should fail
///
/// # Safety
///
/// `self_` must be a valid partition-info handle.
/// The returned list of `kafka_common_Node_t *` copies is owned, freed with `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_in_sync_replicas(
    self_: *const kafka_common_PartitionInfo_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid partition-info handle, as
    // `partition_info_ref` requires; the nodes are cloned into the owned list returned, so the
    // borrow lasts for this call only.
    node_list(unsafe { partition_info_ref(self_) }.in_sync_replicas())
}

/// Returns the subset of the replicas that are offline
///
/// # Safety
///
/// `self_` must be a valid partition-info handle.
/// The returned list of `kafka_common_Node_t *` copies is owned, freed with `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_offline_replicas(
    self_: *const kafka_common_PartitionInfo_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid partition-info handle, as
    // `partition_info_ref` requires; the nodes are cloned into the owned list returned, so the
    // borrow lasts for this call only.
    node_list(unsafe { partition_info_ref(self_) }.offline_replicas())
}

/// Returns `Partition(topic = <topic>, partition = <partition>, leader = <leader id or none>,
/// replicas = <ids>, isr = <ids>, offlineReplicas = <ids>)`.
///
/// # Safety
///
/// `self_` must be a valid partition-info handle.
/// The returned string is owned, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_to_string(
    self_: *const kafka_common_PartitionInfo_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid partition-info handle, as
    // `partition_info_ref` requires; the reference is used only within this call, and the text is
    // copied into an owned string.
    into_c_string(&unsafe { partition_info_ref(self_) }.to_string())
}

/// Frees an owned partition-info handle. Null is a no-op; a handle borrowed
/// from a `kafka_common_Cluster_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned partition-info handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_destroy(self_: *mut kafka_common_PartitionInfo_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // partition-info handle not yet destroyed, i.e. the `Box::into_raw` of
        // `box_partition_info`, so `Box::from_raw` reclaims it exactly once. A handle borrowed from
        // a cluster is never passed here, as the rustdoc states.
        drop(unsafe { Box::from_raw(self_ as *mut PartitionInfoInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::common::Node;
    use crate::ffi::common::node::{kafka_common_Node_destroy, kafka_common_Node_id, kafka_common_Node_new};
    use crate::ffi::util::{kafka_List_add, kafka_List_destroy, kafka_List_get, kafka_List_new, kafka_List_size};

    #[test]
    fn constructor_copies_nodes_and_getters_return_owned_lists() {
        let topic = CString::new("t").unwrap();
        let host = CString::new("h").unwrap();
        // SAFETY: Test code: `topic` and `host` outlive the constructors, which copy them; the
        // nodes come from `kafka_common_Node_new` and the lists from `kafka_List_new`, which borrow
        // the nodes and are destroyed before them, after the constructor copies both; `info` is
        // destroyed once, after the views it lends and the owned lists from its getters, each
        // destroyed once, are read.
        unsafe {
            let leader = kafka_common_Node_new(1, host.as_ptr(), 1);
            let follower = kafka_common_Node_new(2, host.as_ptr(), 2);
            let replicas = kafka_List_new();
            kafka_List_add(replicas, leader as *mut c_void);
            kafka_List_add(replicas, follower as *mut c_void);
            let isr = kafka_List_new();
            kafka_List_add(isr, leader as *mut c_void);

            let info = kafka_common_PartitionInfo_new(topic.as_ptr(), 3, leader, replicas, isr);
            // The C-built lists and nodes are the caller's: freeing them now
            // must not affect the handle, which copied what it needed.
            kafka_List_destroy(replicas);
            kafka_List_destroy(isr);
            kafka_common_Node_destroy(leader);
            kafka_common_Node_destroy(follower);

            assert_eq!(CStr::from_ptr(kafka_common_PartitionInfo_topic(info)).to_str().unwrap(), "t");
            assert_eq!(kafka_common_PartitionInfo_partition(info), 3);
            assert_eq!(kafka_common_Node_id(kafka_common_PartitionInfo_leader(info)), 1);
            let replicas = kafka_common_PartitionInfo_replicas(info);
            assert_eq!(kafka_List_size(replicas), 2);
            assert_eq!(
                kafka_common_Node_id(kafka_List_get(replicas, 1) as *const kafka_common_Node_t),
                2
            );
            kafka_List_destroy(replicas);
            let offline = kafka_common_PartitionInfo_offline_replicas(info);
            assert_eq!(kafka_List_size(offline), 0);
            kafka_List_destroy(offline);
            kafka_common_PartitionInfo_destroy(info);
            kafka_common_PartitionInfo_destroy(ptr::null_mut());
        }
    }

    #[test]
    fn leader_is_nullable_and_list_round_trips() {
        let infos = [
            PartitionInfo::new("t".to_string(), 0, None, vec![], vec![]),
            PartitionInfo::new("t".to_string(), 1, Some(Node::new(1, "h".to_string(), 1)), vec![], vec![]),
        ];
        let list = partition_info_list(&infos);
        // SAFETY: Test code: `list` comes from `partition_info_list`, whose elements are
        // partition-info handles, and is destroyed once, after the calls reading its elements.
        unsafe {
            assert!(kafka_common_PartitionInfo_leader(kafka_List_get(list, 0) as *const _).is_null());
            assert!(!kafka_common_PartitionInfo_leader(kafka_List_get(list, 1) as *const _).is_null());
            let copies = list_partition_infos(list);
            assert_eq!(copies.len(), 2);
            assert!(
                copies
                    .iter()
                    .zip(&infos)
                    .all(|(a, b)| a.topic() == b.topic() && a.partition() == b.partition())
            );
            kafka_List_destroy(list);
        }
    }
}
