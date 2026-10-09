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
use crate::ffi::util::{
    box_list, c_str_to_string, destroy_boxed, into_c_string, kafka_List_t, list_elements, owned_c_string,
};

/// Opaque handle to a [`PartitionInfo`].
#[repr(C)]
pub struct kafka_common_PartitionInfo_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_PartitionInfo_t`] points at.
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
/// [`kafka_common_PartitionInfo_destroy`].
pub(crate) fn box_partition_info(info: PartitionInfo) -> *mut kafka_common_PartitionInfo_t {
    Box::into_raw(Box::new(PartitionInfoInner::new(info))) as *mut kafka_common_PartitionInfo_t
}

/// The partition info behind a handle.
///
/// # Safety
///
/// `info` must be a valid partition-info handle.
pub(crate) unsafe fn partition_info_ref<'a>(info: *const kafka_common_PartitionInfo_t) -> &'a PartitionInfo {
    unsafe { &*(info as *const PartitionInfoInner) }.info()
}

unsafe fn inner_ref<'a>(info: *const kafka_common_PartitionInfo_t) -> &'a PartitionInfoInner {
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
    unsafe { list_elements(list) }
        .iter()
        .map(|&element| unsafe { partition_info_ref(element as *const kafka_common_PartitionInfo_t) }.clone())
        .collect()
}

/// `new PartitionInfo(String topic, int partition, Node leader, Node[]
/// replicas, Node[] inSyncReplicas)`, with no offline replicas. `leader` is
/// nullable; the lists hold `const kafka_common_Node_t *` and are copied.
/// Owned, freed with [`kafka_common_PartitionInfo_destroy`].
///
/// # Safety
///
/// `topic` must be a valid NUL-terminated string, `leader` null or a valid
/// node handle, and each list null or a valid list of node handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_new(
    topic: *const c_char,
    partition: i32,
    leader: *const kafka_common_Node_t,
    replicas: *const kafka_List_t,
    in_sync_replicas: *const kafka_List_t,
) -> *mut kafka_common_PartitionInfo_t {
    box_partition_info(PartitionInfo::new(
        unsafe { c_str_to_string(topic) },
        partition,
        unsafe { optional_node(leader) },
        unsafe { list_nodes(replicas) },
        unsafe { list_nodes(in_sync_replicas) },
    ))
}

/// `new PartitionInfo(String topic, int partition, Node leader, Node[]
/// replicas, Node[] inSyncReplicas, Node[] offlineReplicas)`.
///
/// # Safety
///
/// As [`kafka_common_PartitionInfo_new`], plus `offline_replicas` null or a
/// valid list of node handles.
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
        unsafe { c_str_to_string(topic) },
        partition,
        unsafe { optional_node(leader) },
        unsafe { list_nodes(replicas) },
        unsafe { list_nodes(in_sync_replicas) },
        unsafe { list_nodes(offline_replicas) },
    ))
}

/// `topic()`: borrowed from the handle, valid until it is destroyed.
///
/// # Safety
///
/// `self_` must be a valid partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_topic(self_: *const kafka_common_PartitionInfo_t) -> *const c_char {
    unsafe { inner_ref(self_) }.topic_c.as_ptr()
}

/// `partition()`.
///
/// # Safety
///
/// `self_` must be a valid partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_partition(self_: *const kafka_common_PartitionInfo_t) -> i32 {
    unsafe { partition_info_ref(self_) }.partition()
}

/// `leader()`: the leader node, borrowed from the handle, or null when the
/// partition has none.
///
/// # Safety
///
/// `self_` must be a valid partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_leader(
    self_: *const kafka_common_PartitionInfo_t,
) -> *const kafka_common_Node_t {
    unsafe { inner_ref(self_) }.leader_ptr()
}

/// `replicas()`: an owned list of `kafka_common_Node_t *` copies, freed with
/// `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_replicas(
    self_: *const kafka_common_PartitionInfo_t,
) -> *mut kafka_List_t {
    node_list(unsafe { partition_info_ref(self_) }.replicas())
}

/// `inSyncReplicas()`: an owned list of `kafka_common_Node_t *` copies.
///
/// # Safety
///
/// `self_` must be a valid partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_in_sync_replicas(
    self_: *const kafka_common_PartitionInfo_t,
) -> *mut kafka_List_t {
    node_list(unsafe { partition_info_ref(self_) }.in_sync_replicas())
}

/// `offlineReplicas()`: an owned list of `kafka_common_Node_t *` copies.
///
/// # Safety
///
/// `self_` must be a valid partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_offline_replicas(
    self_: *const kafka_common_PartitionInfo_t,
) -> *mut kafka_List_t {
    node_list(unsafe { partition_info_ref(self_) }.offline_replicas())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid partition-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_to_string(
    self_: *const kafka_common_PartitionInfo_t,
) -> *mut c_char {
    into_c_string(&unsafe { partition_info_ref(self_) }.to_string())
}

/// Frees an owned partition-info handle. Null is a no-op; a handle borrowed
/// from a `kafka_common_Cluster_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned partition-info handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_PartitionInfo_destroy(self_: *mut kafka_common_PartitionInfo_t) {
    if !self_.is_null() {
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
