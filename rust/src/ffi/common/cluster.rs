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

//! `kafka_common_Cluster_t`: `org.apache.kafka.common.Cluster` (CLAUDE.md
//! §4).
//!
//! A cluster is immutable, so the handle caches a C view of everything its
//! borrowed getters return: one `kafka_common_Node_t` per node and for the
//! controller, one `kafka_common_PartitionInfo_t` per partition, the
//! NUL-terminated topic name of each topic id and the cluster resource. The
//! getters returning Java collections build owned `kafka_List_t`s of copies.

use std::collections::HashMap;
use std::ffi::{CString, c_char};
use std::net::{Ipv4Addr, SocketAddr};
use std::ptr;

use crate::ClientUtils;
use crate::common::{Cluster, PartitionInfo, TopicPartition, Uuid};
use crate::ffi::common::cluster_resource::{ClusterResourceInner, kafka_common_ClusterResource_t};
use crate::ffi::common::node::{
    NodeInner, kafka_common_Node_t, list_nodes, node_list, optional_node, optional_node_ptr,
};
use crate::ffi::common::partition_info::{
    PartitionInfoInner, kafka_common_PartitionInfo_t, list_partition_infos, partition_info_list, partition_info_ref,
};
use crate::ffi::common::topic_partition::{kafka_common_TopicPartition_t, topic_partition_ref};
use crate::ffi::common::uuid::{box_uuid, kafka_common_Uuid_t, uuid_list, uuid_of};
use crate::ffi::ffi_guard;
use crate::ffi::util::{
    c_str_to_option, c_str_to_string, into_c_string, kafka_List_t, kafka_Map_t, list_string_set, list_strings,
    map_entries, owned_c_string, sorted_string_list,
};

/// Opaque handle to a [`Cluster`].
#[repr(C)]
pub struct kafka_common_Cluster_t {
    _private: [u8; 0],
}

/// What a `kafka_common_Cluster_t` points at.
pub(crate) struct ClusterInner {
    cluster: Cluster,
    nodes: Vec<NodeInner>,
    controller: Option<NodeInner>,
    partitions: HashMap<TopicPartition, PartitionInfoInner>,
    topic_names_c: HashMap<Uuid, CString>,
    cluster_resource: ClusterResourceInner,
}

impl ClusterInner {
    pub(crate) fn new(cluster: Cluster) -> Self {
        let nodes = cluster.nodes().iter().cloned().map(NodeInner::new).collect();
        let controller = cluster.controller().cloned().map(NodeInner::new);
        let partitions = cluster
            .topics()
            .flat_map(|topic| cluster.partitions_for_topic(topic))
            .map(|info| {
                (
                    TopicPartition::new(info.topic(), info.partition()),
                    PartitionInfoInner::new(info.clone()),
                )
            })
            .collect();
        let topic_names_c = cluster
            .topic_ids()
            .map(|id| (*id, owned_c_string(cluster.topic_name(id).unwrap_or_default())))
            .collect();
        let cluster_resource = ClusterResourceInner::new(cluster.cluster_resource().clone());
        Self { cluster, nodes, controller, partitions, topic_names_c, cluster_resource }
    }

    fn node_ptr_by_id(&self, id: i32) -> *const kafka_common_Node_t {
        self.nodes
            .iter()
            .find(|node| node.node().id() == id)
            .map_or(ptr::null(), NodeInner::as_ptr)
    }
}

/// What a handle points at.
///
/// # Safety
///
/// `cluster` must be a valid cluster handle, alive for `'a`.
unsafe fn inner_ref<'a>(cluster: *const kafka_common_Cluster_t) -> &'a ClusterInner {
    // SAFETY: Per this function's `# Safety`, `cluster` is a valid cluster handle alive for `'a`,
    // so it points at a live `ClusterInner`, the `Box::into_raw` of `box_cluster`, the only
    // function that makes one. No call mutates a `ClusterInner`, so the shared reference is sound
    // for `'a`.
    unsafe { &*(cluster as *const ClusterInner) }
}

/// Hands `cluster` to C as an owned handle, freed with
/// `kafka_common_Cluster_destroy`.
pub(crate) fn box_cluster(cluster: Cluster) -> *mut kafka_common_Cluster_t {
    Box::into_raw(Box::new(ClusterInner::new(cluster))) as *mut kafka_common_Cluster_t
}

/// The cluster behind a handle.
///
/// # Safety
///
/// `cluster` must be a valid cluster handle, alive for `'a`.
pub(crate) unsafe fn cluster_ref<'a>(cluster: *const kafka_common_Cluster_t) -> &'a Cluster {
    // SAFETY: Per this function's `# Safety`, `cluster` is a valid cluster handle alive for `'a`,
    // so it points at a live `ClusterInner`, the `Box::into_raw` of `box_cluster`, the only
    // function that makes one. No call mutates a `ClusterInner`, so the shared reference is sound
    // for `'a`.
    &unsafe { &*(cluster as *const ClusterInner) }.cluster
}

/// Create a new cluster with the given id, nodes and partitions
///
/// # Parameters
///
/// - `cluster_id` - the cluster id, or `NULL`
/// - `nodes` - The nodes in the cluster
/// - `partitions` - Information about a subset of the topic-partitions this cluster hosts
/// - `unauthorized_topics` - the topics the client is not authorized to access
/// - `internal_topics` - the internal topics
///
/// # Safety
///
/// `cluster_id` must be `NULL` or a valid NUL-terminated string. Each list must be `NULL` or a
/// valid list: `nodes` of `const kafka_common_Node_t *`, `partitions` of
/// `const kafka_common_PartitionInfo_t *`, the topic sets of `const char *`; all are copied. The
/// returned handle is owned, freed with `kafka_common_Cluster_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_new(
    cluster_id: *const c_char,
    nodes: *const kafka_List_t,
    partitions: *const kafka_List_t,
    unauthorized_topics: *const kafka_List_t,
    internal_topics: *const kafka_List_t,
) -> *mut kafka_common_Cluster_t {
    box_cluster(Cluster::new(
        // SAFETY: Per this function's `# Safety`, `cluster_id` is null or a valid NUL-terminated
        // string, as `c_str_to_option` requires; it is copied into an owned value, so nothing
        // borrows it after this call.
        unsafe { c_str_to_option(cluster_id) },
        // SAFETY: Per this function's `# Safety`, `nodes` is null or a valid list of node handles,
        // as `list_nodes` requires; the nodes are copied, so nothing borrows the list after this
        // call.
        unsafe { list_nodes(nodes) },
        // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list of
        // partition-info handles, as `list_partition_infos` requires; the infos are copied, so
        // nothing borrows the list after this call.
        unsafe { list_partition_infos(partitions) },
        // SAFETY: Per this function's `# Safety`, `unauthorized_topics` is null or a valid list of
        // NUL-terminated strings, as `list_string_set` requires; the strings are copied, so nothing
        // borrows the list after this call.
        unsafe { list_string_set(unauthorized_topics) },
        // SAFETY: Per this function's `# Safety`, `internal_topics` is null or a valid list of
        // NUL-terminated strings, as `list_string_set` requires; the strings are copied, so nothing
        // borrows the list after this call.
        unsafe { list_string_set(internal_topics) },
    ))
}

/// Create a new cluster with the given id, nodes and partitions
///
/// # Parameters
///
/// - `cluster_id` - the cluster id, or `NULL`
/// - `nodes` - The nodes in the cluster
/// - `partitions` - Information about a subset of the topic-partitions this cluster hosts
/// - `unauthorized_topics` - the topics the client is not authorized to access
/// - `internal_topics` - the internal topics
/// - `controller` - the controller node, or `NULL`
///
/// # Safety
///
/// As `kafka_common_Cluster_new`, plus `controller` `NULL` or a valid node
/// handle, copied.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_with_controller(
    cluster_id: *const c_char,
    nodes: *const kafka_List_t,
    partitions: *const kafka_List_t,
    unauthorized_topics: *const kafka_List_t,
    internal_topics: *const kafka_List_t,
    controller: *const kafka_common_Node_t,
) -> *mut kafka_common_Cluster_t {
    box_cluster(Cluster::with_controller(
        // SAFETY: Per this function's `# Safety`, through `kafka_common_Cluster_new`'s,
        // `cluster_id` is null or a valid NUL-terminated string, as `c_str_to_option` requires; it
        // is copied into an owned value, so nothing borrows it after this call.
        unsafe { c_str_to_option(cluster_id) },
        // SAFETY: Per this function's `# Safety`, through `kafka_common_Cluster_new`'s, `nodes` is
        // null or a valid list of node handles, as `list_nodes` requires; the nodes are copied, so
        // nothing borrows the list after this call.
        unsafe { list_nodes(nodes) },
        // SAFETY: Per this function's `# Safety`, through `kafka_common_Cluster_new`'s,
        // `partitions` is null or a valid list of partition-info handles, as `list_partition_infos`
        // requires; the infos are copied, so nothing borrows the list after this call.
        unsafe { list_partition_infos(partitions) },
        // SAFETY: Per this function's `# Safety`, through `kafka_common_Cluster_new`'s,
        // `unauthorized_topics` is null or a valid list of NUL-terminated strings, as
        // `list_string_set` requires; the strings are copied, so nothing borrows the list after
        // this call.
        unsafe { list_string_set(unauthorized_topics) },
        // SAFETY: Per this function's `# Safety`, through `kafka_common_Cluster_new`'s,
        // `internal_topics` is null or a valid list of NUL-terminated strings, as `list_string_set`
        // requires; the strings are copied, so nothing borrows the list after this call.
        unsafe { list_string_set(internal_topics) },
        // SAFETY: Per this function's `# Safety`, `controller` is null or a valid node handle, as
        // `optional_node` requires; the node is copied, so nothing borrows it after this call.
        unsafe { optional_node(controller) },
    ))
}

/// Create a new cluster with the given id, nodes and partitions
///
/// # Parameters
///
/// - `cluster_id` - the cluster id, or `NULL`
/// - `nodes` - The nodes in the cluster
/// - `partitions` - Information about a subset of the topic-partitions this cluster hosts
/// - `unauthorized_topics` - the topics the client is not authorized to access
/// - `invalid_topics` - the topics with an invalid name
/// - `internal_topics` - the internal topics
/// - `controller` - the controller node, or `NULL`
///
/// # Safety
///
/// As `kafka_common_Cluster_with_controller`, plus `invalid_topics` `NULL` or
/// a valid list of strings.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_with_invalid_topics_controller(
    cluster_id: *const c_char,
    nodes: *const kafka_List_t,
    partitions: *const kafka_List_t,
    unauthorized_topics: *const kafka_List_t,
    invalid_topics: *const kafka_List_t,
    internal_topics: *const kafka_List_t,
    controller: *const kafka_common_Node_t,
) -> *mut kafka_common_Cluster_t {
    box_cluster(Cluster::with_invalid_topics_controller(
        // SAFETY: Per this function's `# Safety`, through `kafka_common_Cluster_with_controller`'s,
        // `cluster_id` is null or a valid NUL-terminated string, as `c_str_to_option` requires; it
        // is copied into an owned value, so nothing borrows it after this call.
        unsafe { c_str_to_option(cluster_id) },
        // SAFETY: Per this function's `# Safety`, through `kafka_common_Cluster_with_controller`'s,
        // `nodes` is null or a valid list of node handles, as `list_nodes` requires; the nodes are
        // copied, so nothing borrows the list after this call.
        unsafe { list_nodes(nodes) },
        // SAFETY: Per this function's `# Safety`, through `kafka_common_Cluster_with_controller`'s,
        // `partitions` is null or a valid list of partition-info handles, as `list_partition_infos`
        // requires; the infos are copied, so nothing borrows the list after this call.
        unsafe { list_partition_infos(partitions) },
        // SAFETY: Per this function's `# Safety`, through `kafka_common_Cluster_with_controller`'s,
        // `unauthorized_topics` is null or a valid list of NUL-terminated strings, as
        // `list_string_set` requires; the strings are copied, so nothing borrows the list after
        // this call.
        unsafe { list_string_set(unauthorized_topics) },
        // SAFETY: Per this function's `# Safety`, `invalid_topics` is null or a valid list of
        // NUL-terminated strings, as `list_string_set` requires; the strings are copied, so nothing
        // borrows the list after this call.
        unsafe { list_string_set(invalid_topics) },
        // SAFETY: Per this function's `# Safety`, through `kafka_common_Cluster_with_controller`'s,
        // `internal_topics` is null or a valid list of NUL-terminated strings, as `list_string_set`
        // requires; the strings are copied, so nothing borrows the list after this call.
        unsafe { list_string_set(internal_topics) },
        // SAFETY: Per this function's `# Safety`, through `kafka_common_Cluster_with_controller`'s,
        // `controller` is null or a valid node handle, as `optional_node` requires; the node is
        // copied, so nothing borrows it after this call.
        unsafe { optional_node(controller) },
    ))
}

/// Create a new cluster with the given id, nodes, partitions and topicIds
///
/// # Parameters
///
/// - `cluster_id` - the cluster id, or `NULL`
/// - `nodes` - The nodes in the cluster
/// - `partitions` - Information about a subset of the topic-partitions this cluster hosts
/// - `unauthorized_topics` - the topics the client is not authorized to access
/// - `invalid_topics` - the topics with an invalid name
/// - `internal_topics` - the internal topics
/// - `controller` - the controller node, or `NULL`
/// - `topic_ids` - the topic ids by topic name
///
/// # Safety
///
/// As `kafka_common_Cluster_with_invalid_topics_controller`, plus
/// `topic_ids` `NULL` or a valid map of `const char *` topic names to
/// `const kafka_common_Uuid_t *`, copied.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_with_invalid_topics_controller_topic_ids(
    cluster_id: *const c_char,
    nodes: *const kafka_List_t,
    partitions: *const kafka_List_t,
    unauthorized_topics: *const kafka_List_t,
    invalid_topics: *const kafka_List_t,
    internal_topics: *const kafka_List_t,
    controller: *const kafka_common_Node_t,
    topic_ids: *const kafka_Map_t,
) -> *mut kafka_common_Cluster_t {
    // SAFETY: Per this function's `# Safety`, `topic_ids` is null or a valid map, which the caller
    // keeps alive and does not mutate for this call, as `map_entries` requires; the borrow of its
    // entries ends when they are collected.
    let topic_ids = unsafe { map_entries(topic_ids) }
        .iter()
        .map(|&(name, id)| {
            // SAFETY: Per this function's `# Safety`, every key of `topic_ids` is a valid
            // NUL-terminated string, as `c_str_to_string` requires, and every value a valid uuid
            // handle, as `uuid_of` requires; both are copied into owned values, so nothing borrows
            // the map after this call.
            (unsafe { c_str_to_string(name as *const c_char) }, unsafe {
                uuid_of(id as *const kafka_common_Uuid_t)
            })
        })
        .collect();
    box_cluster(Cluster::with_invalid_topics_controller_topic_ids(
        // SAFETY: Per this function's `# Safety`, through
        // `kafka_common_Cluster_with_invalid_topics_controller`'s, `cluster_id` is null or a valid
        // NUL-terminated string, as `c_str_to_option` requires; it is copied into an owned value,
        // so nothing borrows it after this call.
        unsafe { c_str_to_option(cluster_id) },
        // SAFETY: Per this function's `# Safety`, through
        // `kafka_common_Cluster_with_invalid_topics_controller`'s, `nodes` is null or a valid list
        // of node handles, as `list_nodes` requires; the nodes are copied, so nothing borrows the
        // list after this call.
        unsafe { list_nodes(nodes) },
        // SAFETY: Per this function's `# Safety`, through
        // `kafka_common_Cluster_with_invalid_topics_controller`'s, `partitions` is null or a valid
        // list of partition-info handles, as `list_partition_infos` requires; the infos are copied,
        // so nothing borrows the list after this call.
        unsafe { list_partition_infos(partitions) },
        // SAFETY: Per this function's `# Safety`, through
        // `kafka_common_Cluster_with_invalid_topics_controller`'s, `unauthorized_topics` is null or
        // a valid list of NUL-terminated strings, as `list_string_set` requires; the strings are
        // copied, so nothing borrows the list after this call.
        unsafe { list_string_set(unauthorized_topics) },
        // SAFETY: Per this function's `# Safety`, through
        // `kafka_common_Cluster_with_invalid_topics_controller`'s, `invalid_topics` is null or a
        // valid list of NUL-terminated strings, as `list_string_set` requires; the strings are
        // copied, so nothing borrows the list after this call.
        unsafe { list_string_set(invalid_topics) },
        // SAFETY: Per this function's `# Safety`, through
        // `kafka_common_Cluster_with_invalid_topics_controller`'s, `internal_topics` is null or a
        // valid list of NUL-terminated strings, as `list_string_set` requires; the strings are
        // copied, so nothing borrows the list after this call.
        unsafe { list_string_set(internal_topics) },
        // SAFETY: Per this function's `# Safety`, through
        // `kafka_common_Cluster_with_invalid_topics_controller`'s, `controller` is null or a valid
        // node handle, as `optional_node` requires; the node is copied, so nothing borrows it after
        // this call.
        unsafe { optional_node(controller) },
        topic_ids,
    ))
}

/// Create an empty cluster instance with no nodes and no topic-partitions.
///
/// # Returns
///
/// The empty cluster.
///
/// # Safety
///
/// The returned handle is owned, freed with `kafka_common_Cluster_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_Cluster_empty() -> *mut kafka_common_Cluster_t {
    box_cluster(Cluster::empty())
}

/// Create a "bootstrap" cluster using the given list of host/ports
///
/// # Parameters
///
/// - `addresses` - The addresses
///
/// # Returns
///
/// A cluster for these hosts/ports
///
/// # Safety
///
/// `addresses` must be `NULL` or a valid list of NUL-terminated strings of the form `host:port`,
/// as Java's `Utils.getHost` / `Utils.getPort` accept them (an optional `scheme://` prefix and
/// `[ipv6]` brackets included), each with a port in `0..=65535`. This precondition is not
/// checked: a malformed element yields a node with the whole string as host and port `0`. The
/// returned handle is owned, freed with `kafka_common_Cluster_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_bootstrap(addresses: *const kafka_List_t) -> *mut kafka_common_Cluster_t {
    // SAFETY: Per this function's `# Safety`, `addresses` is null or a valid list of NUL-terminated
    // strings, as `list_strings` requires; the strings are copied, so nothing borrows the list
    // after this call.
    let addresses: Vec<(String, SocketAddr)> = unsafe { list_strings(addresses) }
        .iter()
        .map(|address| {
            let (host, port) = ClientUtils::parse_host_port(address)
                .and_then(|(host, port)| port.parse::<u16>().ok().map(|port| (host, port)))
                .unwrap_or((address.as_str(), 0));
            (host.to_string(), SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), port))
        })
        .collect();
    box_cluster(Cluster::bootstrap(&addresses))
}

/// Return a copy of this cluster combined with `partitions`.
///
/// # Parameters
///
/// - `partitions` - the partitions to add, a map of `const kafka_common_TopicPartition_t *` to
///   `const kafka_common_PartitionInfo_t *`, copied
///
/// # Returns
///
/// The combined cluster.
///
/// # Safety
///
/// `self_` must be a valid cluster handle and `partitions` `NULL` or a valid
/// map of the stated key and value types. The returned handle is owned, freed with
/// `kafka_common_Cluster_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_with_partitions(
    self_: *const kafka_common_Cluster_t,
    partitions: *const kafka_Map_t,
) -> *mut kafka_common_Cluster_t {
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid map, which the caller
    // keeps alive and does not mutate for this call, as `map_entries` requires; the borrow of its
    // entries ends when they are collected.
    let partitions: HashMap<TopicPartition, PartitionInfo> = unsafe { map_entries(partitions) }
        .iter()
        .map(|&(tp, info)| {
            (
                // SAFETY: Per this function's `# Safety`, every key of `partitions` is a valid
                // topic-partition handle, as `topic_partition_ref` requires; it is cloned, so
                // nothing borrows it after this call.
                unsafe { topic_partition_ref(tp as *const kafka_common_TopicPartition_t) }.clone(),
                // SAFETY: Per this function's `# Safety`, every value of `partitions` is a valid
                // partition-info handle, as `partition_info_ref` requires; it is cloned, so nothing
                // borrows it after this call.
                unsafe { partition_info_ref(info as *const kafka_common_PartitionInfo_t) }.clone(),
            )
        })
        .collect();
    // SAFETY: Per this function's `# Safety`, `self_` is a valid cluster handle, as `inner_ref`
    // requires; the reference is used only within this call, and what it reads is copied into the
    // new cluster returned.
    box_cluster(unsafe { inner_ref(self_) }.cluster.with_partitions(partitions))
}

/// # Returns
///
/// The known set of nodes
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
/// The returned list of `kafka_common_Node_t *` copies is owned, freed with `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_nodes(self_: *const kafka_common_Cluster_t) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid cluster handle, as `inner_ref`
    // requires; the reference is used only within this call, and what it reads is copied into the
    // owned list returned.
    node_list(unsafe { inner_ref(self_) }.cluster.nodes())
}

/// Get the node by the node id (or `NULL` if the node is not online or does not exist)
///
/// # Parameters
///
/// - `id` - The id of the node
///
/// # Returns
///
/// The node, or `NULL` if the node is not online or does not exist
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
/// The returned handle is borrowed from the cluster handle, valid until it is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_node_by_id(
    self_: *const kafka_common_Cluster_t,
    id: i32,
) -> *const kafka_common_Node_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid cluster handle, as `inner_ref`
    // requires; the reference is used only within this call. The handle returned is a view on the
    // `NodeInner` the cluster caches, so it lives as long as the cluster handle; that is the
    // rustdoc's "borrowed from the handle".
    unsafe { inner_ref(self_) }.node_ptr_by_id(id)
}

/// Get the node by node id if the replica for the given partition is online
///
/// # Parameters
///
/// - `partition` - The TopicPartition
/// - `id` - The node id
///
/// # Returns
///
/// the node, or `NULL` if it is not online
///
/// # Safety
///
/// `self_` must be a valid cluster handle and `partition` a valid
/// topic-partition handle.
/// The returned handle is borrowed from the cluster handle, valid until it is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_node_if_online(
    self_: *const kafka_common_Cluster_t,
    partition: *const kafka_common_TopicPartition_t,
    id: i32,
) -> *const kafka_common_Node_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid cluster handle, as `inner_ref`
    // requires; the reference is used only within this call; the node handle returned is a view on
    // a `NodeInner` the cluster caches (`node_ptr_by_id`), so it lives as long as the cluster
    // handle; that is the rustdoc's "borrowed from the handle".
    let inner = unsafe { inner_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `partition` is a valid topic-partition handle, as
    // `topic_partition_ref` requires; it is used only for the lookup.
    match inner.cluster.node_if_online(unsafe { topic_partition_ref(partition) }, id) {
        Some(node) => inner.node_ptr_by_id(node.id()),
        None => ptr::null(),
    }
}

/// Get the current leader for the given topic-partition
///
/// # Parameters
///
/// - `topic_partition` - The topic and partition we want to know the leader for
///
/// # Returns
///
/// The node that is the leader for this topic-partition, or `NULL` if there is currently no leader
///
/// # Safety
///
/// `self_` must be a valid cluster handle and `topic_partition` a valid
/// topic-partition handle.
/// The returned handle is borrowed from the cluster handle, valid until it is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_leader_for(
    self_: *const kafka_common_Cluster_t,
    topic_partition: *const kafka_common_TopicPartition_t,
) -> *const kafka_common_Node_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid cluster handle, as `inner_ref`
    // requires; the reference is used only within this call. The handle returned is a view on the
    // leader `NodeInner` of a `PartitionInfoInner` the cluster caches, so it lives as long as the
    // cluster handle; that is the rustdoc's "borrowed from the handle".
    unsafe { inner_ref(self_) }
        .partitions
        // SAFETY: Per this function's `# Safety`, `topic_partition` is a valid topic-partition
        // handle, as `topic_partition_ref` requires; it is used only for the lookup.
        .get(unsafe { topic_partition_ref(topic_partition) })
        .map_or(ptr::null(), PartitionInfoInner::leader_ptr)
}

/// Get the metadata for the specified partition
///
/// # Parameters
///
/// - `topic_partition` - The topic and partition to fetch info for
///
/// # Returns
///
/// The metadata about the given topic and partition, or `NULL` if none is found
///
/// # Safety
///
/// `self_` must be a valid cluster handle and `topic_partition` a valid
/// topic-partition handle.
/// The returned handle is borrowed from the cluster handle, valid until it is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_partition(
    self_: *const kafka_common_Cluster_t,
    topic_partition: *const kafka_common_TopicPartition_t,
) -> *const kafka_common_PartitionInfo_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid cluster handle, as `inner_ref`
    // requires; the reference is used only within this call. The handle returned is a view on the
    // `PartitionInfoInner` the cluster caches, so it lives as long as the cluster handle; that is
    // the rustdoc's "borrowed from the handle".
    unsafe { inner_ref(self_) }
        .partitions
        // SAFETY: Per this function's `# Safety`, `topic_partition` is a valid topic-partition
        // handle, as `topic_partition_ref` requires; it is used only for the lookup.
        .get(unsafe { topic_partition_ref(topic_partition) })
        .map_or(ptr::null(), PartitionInfoInner::as_ptr)
}

/// Get the list of partitions for this topic
///
/// # Parameters
///
/// - `topic` - The topic name
///
/// # Returns
///
/// A list of partitions
///
/// # Safety
///
/// `self_` must be a valid cluster handle and `topic` a valid NUL-terminated
/// string.
/// The returned list of `kafka_common_PartitionInfo_t *` copies is owned, freed with `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_partitions_for_topic(
    self_: *const kafka_common_Cluster_t,
    topic: *const c_char,
) -> *mut kafka_List_t {
    partition_info_list(
        // SAFETY: Per this function's `# Safety`, `self_` is a valid cluster handle, as `inner_ref`
        // requires; the reference is used only within this call, and what it reads is copied into
        // the owned list returned.
        unsafe { inner_ref(self_) }
            .cluster
            // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string, as
            // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows
            // it after this call.
            .partitions_for_topic(&unsafe { c_str_to_string(topic) }),
    )
}

/// Get the number of partitions for the given topic.
///
/// # Parameters
///
/// - `topic` - The topic to get the number of partitions for
///
/// # Returns
///
/// The number of partitions or -1 if there is no corresponding metadata
///
/// # Translation notes
///
/// A caught Rust panic returns -2, which is no partition count.
///
/// # Safety
///
/// `self_` must be a valid cluster handle and `topic` a valid NUL-terminated
/// string.
#[ffi_guard(fallback = -2)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_partition_count_for_topic(
    self_: *const kafka_common_Cluster_t,
    topic: *const c_char,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid cluster handle, as `inner_ref`
    // requires; the reference is used only within this call, and only a count is read.
    unsafe { inner_ref(self_) }
        .cluster
        // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        .partition_count_for_topic(&unsafe { c_str_to_string(topic) })
        .map_or(-1, |count| count as i32)
}

/// Get the list of available partitions for this topic
///
/// # Parameters
///
/// - `topic` - The topic name
///
/// # Returns
///
/// A list of partitions
///
/// # Safety
///
/// `self_` must be a valid cluster handle and `topic` a valid NUL-terminated
/// string.
/// The returned list of `kafka_common_PartitionInfo_t *` copies is owned, freed with `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_available_partitions_for_topic(
    self_: *const kafka_common_Cluster_t,
    topic: *const c_char,
) -> *mut kafka_List_t {
    partition_info_list(
        // SAFETY: Per this function's `# Safety`, `self_` is a valid cluster handle, as `inner_ref`
        // requires; the reference is used only within this call, and what it reads is copied into
        // the owned list returned.
        unsafe { inner_ref(self_) }
            .cluster
            // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string, as
            // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows
            // it after this call.
            .available_partitions_for_topic(&unsafe { c_str_to_string(topic) }),
    )
}

/// Get the list of partitions whose leader is this node
///
/// # Parameters
///
/// - `node_id` - The node id
///
/// # Returns
///
/// A list of partitions
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
/// The returned list of `kafka_common_PartitionInfo_t *` copies is owned, freed with `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_partitions_for_node(
    self_: *const kafka_common_Cluster_t,
    node_id: i32,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid cluster handle, as `inner_ref`
    // requires; the reference is used only within this call, and what it reads is copied into the
    // owned list returned.
    partition_info_list(unsafe { inner_ref(self_) }.cluster.partitions_for_node(node_id))
}

/// Get all topics.
///
/// # Returns
///
/// a set of all topics
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
/// The returned list of `char *`, sorted, is owned, freed with `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_topics(self_: *const kafka_common_Cluster_t) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid cluster handle, as `inner_ref`
    // requires; the reference is used only within this call, and what it reads is copied into the
    // owned list returned.
    sorted_string_list(unsafe { inner_ref(self_) }.cluster.topics())
}

/// Returns the topics the client is not authorized to access, sorted.
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
/// The returned list of `char *` is owned, freed with `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_unauthorized_topics(
    self_: *const kafka_common_Cluster_t,
) -> *mut kafka_List_t {
    sorted_string_list(
        // SAFETY: Per this function's `# Safety`, `self_` is a valid cluster handle, as `inner_ref`
        // requires; the reference is used only within this call, and what it reads is copied into
        // the owned list returned.
        unsafe { inner_ref(self_) }
            .cluster
            .unauthorized_topics()
            .iter()
            .map(String::as_str),
    )
}

/// Returns the topics with an invalid name, sorted.
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
/// The returned list of `char *` is owned, freed with `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_invalid_topics(
    self_: *const kafka_common_Cluster_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid cluster handle, as `inner_ref`
    // requires; the reference is used only within this call, and what it reads is copied into the
    // owned list returned.
    sorted_string_list(unsafe { inner_ref(self_) }.cluster.invalid_topics().iter().map(String::as_str))
}

/// Returns the internal topics, sorted.
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
/// The returned list of `char *` is owned, freed with `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_internal_topics(
    self_: *const kafka_common_Cluster_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid cluster handle, as `inner_ref`
    // requires; the reference is used only within this call, and what it reads is copied into the
    // owned list returned.
    sorted_string_list(unsafe { inner_ref(self_) }.cluster.internal_topics().iter().map(String::as_str))
}

/// Returns 1 if this is a bootstrap cluster, else 0.
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_is_bootstrap_configured(self_: *const kafka_common_Cluster_t) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid cluster handle, as `inner_ref`
    // requires; the reference is used only within this call, and only a flag is read.
    unsafe { inner_ref(self_) }.cluster.is_bootstrap_configured() as i8
}

/// Returns the cluster's id resource.
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
/// The returned handle is borrowed from it, valid until it is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_cluster_resource(
    self_: *const kafka_common_Cluster_t,
) -> *const kafka_common_ClusterResource_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid cluster handle, as `inner_ref`
    // requires; the reference is used only within this call. The handle returned is a view on the
    // `ClusterResourceInner` the cluster caches, so it lives as long as the cluster handle; that is
    // the rustdoc's "borrowed from the handle, valid until it is destroyed".
    unsafe { inner_ref(self_) }.cluster_resource.as_ptr()
}

/// Returns the controller node, or `NULL` when unknown.
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
/// The returned handle is borrowed from it, valid until it is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_controller(
    self_: *const kafka_common_Cluster_t,
) -> *const kafka_common_Node_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid cluster handle, as `inner_ref`
    // requires; the reference is used only within this call. The handle returned is a view on the
    // controller `NodeInner` the cluster caches, so it lives as long as the cluster handle; that is
    // the rustdoc's "borrowed from the handle".
    optional_node_ptr(unsafe { inner_ref(self_) }.controller.as_ref())
}

/// Returns the ids of the known topics, sorted by their bits.
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
/// The returned list of `kafka_common_Uuid_t *` is owned, freed with `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_topic_ids(self_: *const kafka_common_Cluster_t) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid cluster handle, as `inner_ref`
    // requires; the reference is used only within this call, and the ids are copied out.
    let mut ids: Vec<Uuid> = unsafe { inner_ref(self_) }.cluster.topic_ids().copied().collect();
    ids.sort_unstable_by_key(|id| (id.most_significant_bits(), id.least_significant_bits()));
    uuid_list(ids)
}

/// Returns the id of `topic`, or `Uuid.ZERO_UUID` for an unknown topic.
///
/// # Parameters
///
/// - `topic` - the topic name
///
/// # Safety
///
/// `self_` must be a valid cluster handle and `topic` a valid NUL-terminated
/// string. The returned handle is owned, freed with `kafka_common_Uuid_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_topic_id(
    self_: *const kafka_common_Cluster_t,
    topic: *const c_char,
) -> *mut kafka_common_Uuid_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid cluster handle, as `inner_ref`
    // requires; the reference is used only within this call, and `topic` is a valid NUL-terminated
    // string, as `c_str_to_string` requires; both are read only within this call, and the id is
    // copied into the owned handle returned.
    box_uuid(unsafe { inner_ref(self_) }.cluster.topic_id(&unsafe { c_str_to_string(topic) }))
}

/// Returns the name of the topic with id `topic_id`, or `NULL` for an unknown id.
///
/// # Parameters
///
/// - `topic_id` - the topic id
///
/// # Safety
///
/// `self_` must be a valid cluster handle and `topic_id` a valid uuid
/// handle. The string is borrowed from the cluster handle, valid until it is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_topic_name(
    self_: *const kafka_common_Cluster_t,
    topic_id: *const kafka_common_Uuid_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid cluster handle, as `inner_ref`
    // requires; the reference is used only within this call. The string returned is a
    // NUL-terminated topic name the cluster caches, so it lives as long as the cluster handle; that
    // is the rustdoc's "borrowed from the handle".
    unsafe { inner_ref(self_) }
        .topic_names_c
        // SAFETY: Per this function's `# Safety`, `topic_id` is a valid uuid handle, as `uuid_of`
        // requires; the uuid is copied out for the lookup.
        .get(&unsafe { uuid_of(topic_id) })
        .map_or(ptr::null(), |name| name.as_ptr())
}

/// Returns `Cluster(id = <id>, nodes = <nodes>, partitions = <partitions>, controller = <controller>)`.
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
/// The returned string is owned, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_to_string(self_: *const kafka_common_Cluster_t) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid cluster handle, as `inner_ref`
    // requires; the reference is used only within this call, and the text is copied into an owned
    // string.
    into_c_string(&unsafe { inner_ref(self_) }.cluster.to_string())
}

/// Frees an owned cluster handle. Null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned cluster handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_destroy(self_: *mut kafka_common_Cluster_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // cluster handle not yet destroyed, i.e. the `Box::into_raw` of `box_cluster`, so
        // `Box::from_raw` reclaims it exactly once.
        drop(unsafe { Box::from_raw(self_ as *mut ClusterInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, c_void};

    use super::*;
    use crate::common::Node;
    use crate::ffi::common::cluster_resource::kafka_common_ClusterResource_cluster_id;
    use crate::ffi::common::node::{kafka_common_Node_id, kafka_common_Node_port};
    use crate::ffi::common::partition_info::kafka_common_PartitionInfo_partition;
    use crate::ffi::common::topic_partition::{box_topic_partition, kafka_common_TopicPartition_destroy};
    use crate::ffi::common::uuid::kafka_common_Uuid_destroy;
    use crate::ffi::util::{
        kafka_List_add, kafka_List_destroy, kafka_List_get, kafka_List_new, kafka_List_size, kafka_Map_destroy,
        kafka_Map_new, kafka_Map_put, kafka_string_destroy,
    };

    fn sample() -> Cluster {
        let n1 = Node::new(1, "h1".to_string(), 9091);
        let n2 = Node::new(2, "h2".to_string(), 9092);
        let partitions = vec![
            PartitionInfo::with_offline_replicas(
                "t".to_string(),
                0,
                Some(n1.clone()),
                vec![n1.clone(), n2.clone()],
                vec![n1.clone()],
                vec![n2.clone()],
            ),
            PartitionInfo::new("t".to_string(), 1, None, vec![n2.clone()], vec![]),
        ];
        Cluster::with_invalid_topics_controller_topic_ids(
            Some("cid".to_string()),
            vec![n1.clone(), n2],
            partitions,
            ["ua".to_string()].into_iter().collect(),
            ["inv".to_string()].into_iter().collect(),
            ["__int".to_string()].into_iter().collect(),
            Some(n1),
            [("t".to_string(), Uuid::new(7, 8))].into_iter().collect(),
        )
    }

    #[test]
    fn borrowed_getters_resolve_through_the_cached_views() {
        let cluster = box_cluster(sample());
        let topic = CString::new("t").unwrap();
        let missing = CString::new("missing").unwrap();
        // SAFETY: Test code: `cluster` comes from `box_cluster` and is destroyed once, after every
        // borrowed view read from it; the `CString`s outlive the calls reading them; each
        // topic-partition and uuid handle comes from its constructor and is destroyed once, after
        // the calls using it; every owned list and string is destroyed once, after it is read;
        // destroying null is a no-op.
        unsafe {
            assert_eq!(kafka_common_Node_id(kafka_common_Cluster_controller(cluster)), 1);
            assert_eq!(kafka_common_Node_port(kafka_common_Cluster_node_by_id(cluster, 2)), 9092);
            assert!(kafka_common_Cluster_node_by_id(cluster, 3).is_null());

            let tp0 = box_topic_partition(TopicPartition::new("t", 0));
            let tp1 = box_topic_partition(TopicPartition::new("t", 1));
            assert_eq!(kafka_common_Node_id(kafka_common_Cluster_leader_for(cluster, tp0)), 1);
            assert!(kafka_common_Cluster_leader_for(cluster, tp1).is_null());
            assert_eq!(
                kafka_common_PartitionInfo_partition(kafka_common_Cluster_partition(cluster, tp1)),
                1
            );
            // Node 2 is an offline replica of t-0, so it is not online there.
            assert!(kafka_common_Cluster_node_if_online(cluster, tp0, 2).is_null());
            assert_eq!(kafka_common_Node_id(kafka_common_Cluster_node_if_online(cluster, tp0, 1)), 1);
            kafka_common_TopicPartition_destroy(tp0);
            kafka_common_TopicPartition_destroy(tp1);

            assert_eq!(kafka_common_Cluster_partition_count_for_topic(cluster, topic.as_ptr()), 2);
            assert_eq!(kafka_common_Cluster_partition_count_for_topic(cluster, missing.as_ptr()), -1);
            let available = kafka_common_Cluster_available_partitions_for_topic(cluster, topic.as_ptr());
            assert_eq!(kafka_List_size(available), 1);
            kafka_List_destroy(available);
            let for_node = kafka_common_Cluster_partitions_for_node(cluster, 1);
            assert_eq!(kafka_List_size(for_node), 1);
            kafka_List_destroy(for_node);

            assert_eq!(
                CStr::from_ptr(kafka_common_ClusterResource_cluster_id(kafka_common_Cluster_cluster_resource(
                    cluster
                )))
                .to_str()
                .unwrap(),
                "cid"
            );
            assert_eq!(kafka_common_Cluster_is_bootstrap_configured(cluster), 0);

            for (getter, expected) in [
                (kafka_common_Cluster_topics as unsafe extern "C" fn(_) -> _, "t"),
                (kafka_common_Cluster_unauthorized_topics, "ua"),
                (kafka_common_Cluster_invalid_topics, "inv"),
                (kafka_common_Cluster_internal_topics, "__int"),
            ] {
                let list = getter(cluster);
                assert_eq!(kafka_List_size(list), 1);
                assert_eq!(
                    CStr::from_ptr(kafka_List_get(list, 0) as *const c_char).to_str().unwrap(),
                    expected
                );
                kafka_List_destroy(list);
            }

            let id = kafka_common_Cluster_topic_id(cluster, topic.as_ptr());
            assert_eq!(uuid_of(id), Uuid::new(7, 8));
            assert_eq!(
                CStr::from_ptr(kafka_common_Cluster_topic_name(cluster, id)).to_str().unwrap(),
                "t"
            );
            kafka_common_Uuid_destroy(id);
            let ids = kafka_common_Cluster_topic_ids(cluster);
            assert_eq!(kafka_List_size(ids), 1);
            kafka_List_destroy(ids);
            let zero = box_uuid(Uuid::new(0, 0));
            assert!(kafka_common_Cluster_topic_name(cluster, zero).is_null());
            kafka_common_Uuid_destroy(zero);

            // `Cluster::new` shuffles its nodes as Java does, so two `sample()`
            // instances print in different orders: compare with the wrapped one.
            let s = kafka_common_Cluster_to_string(cluster);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), inner_ref(cluster).cluster.to_string());
            kafka_string_destroy(s);
            kafka_common_Cluster_destroy(cluster);
            kafka_common_Cluster_destroy(ptr::null_mut());
        }
    }

    #[test]
    fn constructors_copy_their_inputs() {
        let cid = CString::new("cid").unwrap();
        let ua = CString::new("ua").unwrap();
        let t = CString::new("t").unwrap();
        // SAFETY: Test code: the `CString`s outlive every call reading them; each list and map
        // comes from `kafka_List_new` or `kafka_Map_new`, borrows handles and strings that outlive
        // it, and is destroyed once, after the constructors copy it; the node, partition-info, uuid
        // and topic-partition handles come from their constructors and are each destroyed once,
        // after the constructors copy them; each cluster is destroyed once, after it is read.
        unsafe {
            let node = crate::ffi::common::node::box_node(Node::new(1, "h".to_string(), 1));
            let nodes = kafka_List_new();
            kafka_List_add(nodes, node as *mut c_void);
            let info = crate::ffi::common::partition_info::box_partition_info(PartitionInfo::new(
                "t".to_string(),
                0,
                None,
                vec![],
                vec![],
            ));
            let partitions = kafka_List_new();
            kafka_List_add(partitions, info as *mut c_void);
            let unauthorized = kafka_List_new();
            kafka_List_add(unauthorized, ua.as_ptr() as *mut c_void);
            let uuid = box_uuid(Uuid::new(1, 2));
            let topic_ids = kafka_Map_new();
            kafka_Map_put(topic_ids, t.as_ptr() as *mut c_void, uuid as *mut c_void);

            let plain = kafka_common_Cluster_new(cid.as_ptr(), nodes, partitions, unauthorized, ptr::null());
            let with_controller =
                kafka_common_Cluster_with_controller(cid.as_ptr(), nodes, partitions, unauthorized, ptr::null(), node);
            let with_ids = kafka_common_Cluster_with_invalid_topics_controller_topic_ids(
                ptr::null(),
                nodes,
                partitions,
                unauthorized,
                ptr::null(),
                ptr::null(),
                ptr::null(),
                topic_ids,
            );
            kafka_List_destroy(nodes);
            kafka_List_destroy(partitions);
            kafka_List_destroy(unauthorized);
            kafka_Map_destroy(topic_ids);
            crate::ffi::common::node::kafka_common_Node_destroy(node);
            crate::ffi::common::partition_info::kafka_common_PartitionInfo_destroy(info);
            kafka_common_Uuid_destroy(uuid);

            assert!(kafka_common_Cluster_controller(plain).is_null());
            assert_eq!(kafka_common_Node_id(kafka_common_Cluster_controller(with_controller)), 1);
            assert_eq!(kafka_common_Cluster_partition_count_for_topic(plain, t.as_ptr()), 1);
            let id = kafka_common_Cluster_topic_id(with_ids, t.as_ptr());
            assert_eq!(uuid_of(id), Uuid::new(1, 2));
            kafka_common_Uuid_destroy(id);
            assert!(kafka_common_ClusterResource_cluster_id(kafka_common_Cluster_cluster_resource(with_ids)).is_null());

            // `with_partitions` adds t-1 to a copy and leaves the original alone.
            let tp = box_topic_partition(TopicPartition::new("t", 1));
            let extra = crate::ffi::common::partition_info::box_partition_info(PartitionInfo::new(
                "t".to_string(),
                1,
                None,
                vec![],
                vec![],
            ));
            let additions = kafka_Map_new();
            kafka_Map_put(additions, tp as *mut c_void, extra as *mut c_void);
            let combined = kafka_common_Cluster_with_partitions(plain, additions);
            kafka_Map_destroy(additions);
            kafka_common_TopicPartition_destroy(tp);
            crate::ffi::common::partition_info::kafka_common_PartitionInfo_destroy(extra);
            assert_eq!(kafka_common_Cluster_partition_count_for_topic(combined, t.as_ptr()), 2);
            assert_eq!(kafka_common_Cluster_partition_count_for_topic(plain, t.as_ptr()), 1);

            for cluster in [plain, with_controller, with_ids, combined] {
                kafka_common_Cluster_destroy(cluster);
            }
        }
    }

    #[test]
    fn empty_and_bootstrap_follow_java() {
        let a = CString::new("h1:9091").unwrap();
        let b = CString::new("[::1]:9092").unwrap();
        // SAFETY: Test code: the `CString`s outlive the calls reading them; `addresses` comes from
        // `kafka_List_new`, borrows them and is destroyed once, after
        // `kafka_common_Cluster_bootstrap` copies it; each cluster is destroyed once, after the
        // node views borrowed from it are read; each owned list is destroyed once, after it is
        // read.
        unsafe {
            let empty = kafka_common_Cluster_empty();
            let nodes = kafka_common_Cluster_nodes(empty);
            assert_eq!(kafka_List_size(nodes), 0);
            kafka_List_destroy(nodes);
            assert_eq!(kafka_common_Cluster_is_bootstrap_configured(empty), 0);
            kafka_common_Cluster_destroy(empty);

            let addresses = kafka_List_new();
            kafka_List_add(addresses, a.as_ptr() as *mut c_void);
            kafka_List_add(addresses, b.as_ptr() as *mut c_void);
            let bootstrap = kafka_common_Cluster_bootstrap(addresses);
            kafka_List_destroy(addresses);
            assert_eq!(kafka_common_Cluster_is_bootstrap_configured(bootstrap), 1);
            let nodes = kafka_common_Cluster_nodes(bootstrap);
            assert_eq!(kafka_List_size(nodes), 2);
            kafka_List_destroy(nodes);
            // Bootstrap nodes get ids -1, -2, ... and the parsed port. The
            // node list is shuffled as in Java, so look the nodes up by id.
            let first = kafka_common_Cluster_node_by_id(bootstrap, -1);
            assert_eq!(kafka_common_Node_id(first), -1);
            assert_eq!(kafka_common_Node_port(first), 9091);
            let second = kafka_common_Cluster_node_by_id(bootstrap, -2);
            assert_eq!(kafka_common_Node_id(second), -2);
            assert_eq!(kafka_common_Node_port(second), 9092);
            kafka_common_Cluster_destroy(bootstrap);
        }
    }
}
