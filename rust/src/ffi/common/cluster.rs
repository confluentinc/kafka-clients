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
use crate::ffi::util::{
    c_str_to_option, c_str_to_string, into_c_string, kafka_List_t, kafka_Map_t, list_string_set, list_strings,
    map_entries, owned_c_string, sorted_string_list,
};

/// Opaque handle to a [`Cluster`].
#[repr(C)]
pub struct kafka_common_Cluster_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_Cluster_t`] points at.
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

unsafe fn inner_ref<'a>(cluster: *const kafka_common_Cluster_t) -> &'a ClusterInner {
    unsafe { &*(cluster as *const ClusterInner) }
}

/// Hands `cluster` to C as an owned handle, freed with
/// [`kafka_common_Cluster_destroy`].
pub(crate) fn box_cluster(cluster: Cluster) -> *mut kafka_common_Cluster_t {
    Box::into_raw(Box::new(ClusterInner::new(cluster))) as *mut kafka_common_Cluster_t
}

/// `new Cluster(String clusterId, Collection<Node> nodes,
/// Collection<PartitionInfo> partitions, Set<String> unauthorizedTopics,
/// Set<String> internalTopics)`. `cluster_id` is nullable; `nodes` holds
/// `const kafka_common_Node_t *`, `partitions`
/// `const kafka_common_PartitionInfo_t *` and the topic sets `const char *`,
/// all copied. Owned, freed with [`kafka_common_Cluster_destroy`].
///
/// # Safety
///
/// `cluster_id` must be null or a valid NUL-terminated string and each list
/// null or a valid list of the stated element type.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_new(
    cluster_id: *const c_char,
    nodes: *const kafka_List_t,
    partitions: *const kafka_List_t,
    unauthorized_topics: *const kafka_List_t,
    internal_topics: *const kafka_List_t,
) -> *mut kafka_common_Cluster_t {
    box_cluster(Cluster::new(
        unsafe { c_str_to_option(cluster_id) },
        unsafe { list_nodes(nodes) },
        unsafe { list_partition_infos(partitions) },
        unsafe { list_string_set(unauthorized_topics) },
        unsafe { list_string_set(internal_topics) },
    ))
}

/// `new Cluster(String clusterId, Collection<Node> nodes,
/// Collection<PartitionInfo> partitions, Set<String> unauthorizedTopics,
/// Set<String> internalTopics, Node controller)`; `controller` is nullable.
///
/// # Safety
///
/// As [`kafka_common_Cluster_new`], plus `controller` null or a valid node
/// handle.
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
        unsafe { c_str_to_option(cluster_id) },
        unsafe { list_nodes(nodes) },
        unsafe { list_partition_infos(partitions) },
        unsafe { list_string_set(unauthorized_topics) },
        unsafe { list_string_set(internal_topics) },
        unsafe { optional_node(controller) },
    ))
}

/// `new Cluster(String clusterId, Collection<Node> nodes,
/// Collection<PartitionInfo> partitions, Set<String> unauthorizedTopics,
/// Set<String> invalidTopics, Set<String> internalTopics, Node controller)`.
///
/// # Safety
///
/// As [`kafka_common_Cluster_with_controller`], plus `invalid_topics` null or
/// a valid list of strings.
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
        unsafe { c_str_to_option(cluster_id) },
        unsafe { list_nodes(nodes) },
        unsafe { list_partition_infos(partitions) },
        unsafe { list_string_set(unauthorized_topics) },
        unsafe { list_string_set(invalid_topics) },
        unsafe { list_string_set(internal_topics) },
        unsafe { optional_node(controller) },
    ))
}

/// `new Cluster(String clusterId, Collection<Node> nodes,
/// Collection<PartitionInfo> partitions, Set<String> unauthorizedTopics,
/// Set<String> invalidTopics, Set<String> internalTopics, Node controller,
/// Map<String, Uuid> topicIds)`. `topic_ids` maps `const char *` topic names
/// to `const kafka_common_Uuid_t *`, copied.
///
/// # Safety
///
/// As [`kafka_common_Cluster_with_invalid_topics_controller`], plus
/// `topic_ids` null or a valid map of the stated key and value types.
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
    let topic_ids = unsafe { map_entries(topic_ids) }
        .iter()
        .map(|&(name, id)| {
            (unsafe { c_str_to_string(name as *const c_char) }, unsafe {
                uuid_of(id as *const kafka_common_Uuid_t)
            })
        })
        .collect();
    box_cluster(Cluster::with_invalid_topics_controller_topic_ids(
        unsafe { c_str_to_option(cluster_id) },
        unsafe { list_nodes(nodes) },
        unsafe { list_partition_infos(partitions) },
        unsafe { list_string_set(unauthorized_topics) },
        unsafe { list_string_set(invalid_topics) },
        unsafe { list_string_set(internal_topics) },
        unsafe { optional_node(controller) },
        topic_ids,
    ))
}

/// `Cluster.empty()`: a cluster with no nodes and no topic-partitions.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_Cluster_empty() -> *mut kafka_common_Cluster_t {
    box_cluster(Cluster::empty())
}

/// `Cluster.bootstrap(List<InetSocketAddress> addresses)`: `addresses` holds
/// `const char *` elements of the form `host:port`, as accepted by Java's
/// `Utils.getHost` / `Utils.getPort` (an optional `scheme://` prefix and
/// `[ipv6]` brackets included). Each element must be well formed with a port
/// in `0..=65535`; this is a programming precondition, not checked
/// (CLAUDE.md §4), and a malformed element yields a node with the whole
/// string as host and port `0`.
///
/// # Safety
///
/// `addresses` must be null or a valid list of NUL-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_bootstrap(addresses: *const kafka_List_t) -> *mut kafka_common_Cluster_t {
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

/// `withPartitions(Map<TopicPartition, PartitionInfo> partitions)`: a new
/// owned cluster combining this one with `partitions`, a map of
/// `const kafka_common_TopicPartition_t *` to
/// `const kafka_common_PartitionInfo_t *` (copied).
///
/// # Safety
///
/// `self_` must be a valid cluster handle and `partitions` null or a valid
/// map of the stated key and value types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_with_partitions(
    self_: *const kafka_common_Cluster_t,
    partitions: *const kafka_Map_t,
) -> *mut kafka_common_Cluster_t {
    let partitions: HashMap<TopicPartition, PartitionInfo> = unsafe { map_entries(partitions) }
        .iter()
        .map(|&(tp, info)| {
            (
                unsafe { topic_partition_ref(tp as *const kafka_common_TopicPartition_t) }.clone(),
                unsafe { partition_info_ref(info as *const kafka_common_PartitionInfo_t) }.clone(),
            )
        })
        .collect();
    box_cluster(unsafe { inner_ref(self_) }.cluster.with_partitions(partitions))
}

/// `nodes()`: an owned list of `kafka_common_Node_t *` copies, freed with
/// `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_nodes(self_: *const kafka_common_Cluster_t) -> *mut kafka_List_t {
    node_list(unsafe { inner_ref(self_) }.cluster.nodes())
}

/// `nodeById(int id)`: borrowed from the handle, or null when unknown.
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_node_by_id(
    self_: *const kafka_common_Cluster_t,
    id: i32,
) -> *const kafka_common_Node_t {
    unsafe { inner_ref(self_) }.node_ptr_by_id(id)
}

/// `nodeIfOnline(TopicPartition partition, int id)`: the node, borrowed from
/// the handle, when it exists and is not an offline replica of `partition`;
/// null otherwise.
///
/// # Safety
///
/// `self_` must be a valid cluster handle and `partition` a valid
/// topic-partition handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_node_if_online(
    self_: *const kafka_common_Cluster_t,
    partition: *const kafka_common_TopicPartition_t,
    id: i32,
) -> *const kafka_common_Node_t {
    let inner = unsafe { inner_ref(self_) };
    match inner.cluster.node_if_online(unsafe { topic_partition_ref(partition) }, id) {
        Some(node) => inner.node_ptr_by_id(node.id()),
        None => ptr::null(),
    }
}

/// `leaderFor(TopicPartition topicPartition)`: borrowed from the handle, or
/// null when the partition is unknown or has no leader.
///
/// # Safety
///
/// `self_` must be a valid cluster handle and `topic_partition` a valid
/// topic-partition handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_leader_for(
    self_: *const kafka_common_Cluster_t,
    topic_partition: *const kafka_common_TopicPartition_t,
) -> *const kafka_common_Node_t {
    unsafe { inner_ref(self_) }
        .partitions
        .get(unsafe { topic_partition_ref(topic_partition) })
        .map_or(ptr::null(), PartitionInfoInner::leader_ptr)
}

/// `partition(TopicPartition topicPartition)`: borrowed from the handle, or
/// null when unknown.
///
/// # Safety
///
/// `self_` must be a valid cluster handle and `topic_partition` a valid
/// topic-partition handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_partition(
    self_: *const kafka_common_Cluster_t,
    topic_partition: *const kafka_common_TopicPartition_t,
) -> *const kafka_common_PartitionInfo_t {
    unsafe { inner_ref(self_) }
        .partitions
        .get(unsafe { topic_partition_ref(topic_partition) })
        .map_or(ptr::null(), PartitionInfoInner::as_ptr)
}

/// `partitionsForTopic(String topic)`: an owned list of
/// `kafka_common_PartitionInfo_t *` copies, freed with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid cluster handle and `topic` a valid NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_partitions_for_topic(
    self_: *const kafka_common_Cluster_t,
    topic: *const c_char,
) -> *mut kafka_List_t {
    partition_info_list(
        unsafe { inner_ref(self_) }
            .cluster
            .partitions_for_topic(&unsafe { c_str_to_string(topic) }),
    )
}

/// `partitionCountForTopic(String topic)`: the count, or `-1` for Java's
/// null (an unknown topic).
///
/// # Safety
///
/// `self_` must be a valid cluster handle and `topic` a valid NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_partition_count_for_topic(
    self_: *const kafka_common_Cluster_t,
    topic: *const c_char,
) -> i32 {
    unsafe { inner_ref(self_) }
        .cluster
        .partition_count_for_topic(&unsafe { c_str_to_string(topic) })
        .map_or(-1, |count| count as i32)
}

/// `availablePartitionsForTopic(String topic)`: an owned list of
/// `kafka_common_PartitionInfo_t *` copies of the partitions with a leader.
///
/// # Safety
///
/// `self_` must be a valid cluster handle and `topic` a valid NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_available_partitions_for_topic(
    self_: *const kafka_common_Cluster_t,
    topic: *const c_char,
) -> *mut kafka_List_t {
    partition_info_list(
        unsafe { inner_ref(self_) }
            .cluster
            .available_partitions_for_topic(&unsafe { c_str_to_string(topic) }),
    )
}

/// `partitionsForNode(int nodeId)`: an owned list of
/// `kafka_common_PartitionInfo_t *` copies of the partitions led by the node.
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_partitions_for_node(
    self_: *const kafka_common_Cluster_t,
    node_id: i32,
) -> *mut kafka_List_t {
    partition_info_list(unsafe { inner_ref(self_) }.cluster.partitions_for_node(node_id))
}

/// `topics()`: an owned, sorted list of `char *`, freed with
/// `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_topics(self_: *const kafka_common_Cluster_t) -> *mut kafka_List_t {
    sorted_string_list(unsafe { inner_ref(self_) }.cluster.topics())
}

/// `unauthorizedTopics()`: an owned, sorted list of `char *`.
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_unauthorized_topics(
    self_: *const kafka_common_Cluster_t,
) -> *mut kafka_List_t {
    sorted_string_list(
        unsafe { inner_ref(self_) }
            .cluster
            .unauthorized_topics()
            .iter()
            .map(String::as_str),
    )
}

/// `invalidTopics()`: an owned, sorted list of `char *`.
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_invalid_topics(
    self_: *const kafka_common_Cluster_t,
) -> *mut kafka_List_t {
    sorted_string_list(unsafe { inner_ref(self_) }.cluster.invalid_topics().iter().map(String::as_str))
}

/// `internalTopics()`: an owned, sorted list of `char *`.
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_internal_topics(
    self_: *const kafka_common_Cluster_t,
) -> *mut kafka_List_t {
    sorted_string_list(unsafe { inner_ref(self_) }.cluster.internal_topics().iter().map(String::as_str))
}

/// `isBootstrapConfigured()`.
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_is_bootstrap_configured(self_: *const kafka_common_Cluster_t) -> i8 {
    unsafe { inner_ref(self_) }.cluster.is_bootstrap_configured() as i8
}

/// `clusterResource()`: borrowed from the handle, valid until it is
/// destroyed.
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_cluster_resource(
    self_: *const kafka_common_Cluster_t,
) -> *const kafka_common_ClusterResource_t {
    unsafe { inner_ref(self_) }.cluster_resource.as_ptr()
}

/// `controller()`: borrowed from the handle, or null when unknown.
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_controller(
    self_: *const kafka_common_Cluster_t,
) -> *const kafka_common_Node_t {
    optional_node_ptr(unsafe { inner_ref(self_) }.controller.as_ref())
}

/// `topicIds()`: an owned list of `kafka_common_Uuid_t *`, sorted by bits so
/// the order is deterministic, freed with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_topic_ids(self_: *const kafka_common_Cluster_t) -> *mut kafka_List_t {
    let mut ids: Vec<Uuid> = unsafe { inner_ref(self_) }.cluster.topic_ids().copied().collect();
    ids.sort_unstable_by_key(|id| (id.most_significant_bits(), id.least_significant_bits()));
    uuid_list(ids)
}

/// `topicId(String topic)`: an owned uuid handle, `Uuid.ZERO_UUID` for an
/// unknown topic, freed with `kafka_common_Uuid_destroy`.
///
/// # Safety
///
/// `self_` must be a valid cluster handle and `topic` a valid NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_topic_id(
    self_: *const kafka_common_Cluster_t,
    topic: *const c_char,
) -> *mut kafka_common_Uuid_t {
    box_uuid(unsafe { inner_ref(self_) }.cluster.topic_id(&unsafe { c_str_to_string(topic) }))
}

/// `topicName(Uuid topicId)`: borrowed from the handle, or null for an
/// unknown id.
///
/// # Safety
///
/// `self_` must be a valid cluster handle and `topic_id` a valid uuid
/// handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_topic_name(
    self_: *const kafka_common_Cluster_t,
    topic_id: *const kafka_common_Uuid_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }
        .topic_names_c
        .get(&unsafe { uuid_of(topic_id) })
        .map_or(ptr::null(), |name| name.as_ptr())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid cluster handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_to_string(self_: *const kafka_common_Cluster_t) -> *mut c_char {
    into_c_string(&unsafe { inner_ref(self_) }.cluster.to_string())
}

/// Frees an owned cluster handle. Null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned cluster handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Cluster_destroy(self_: *mut kafka_common_Cluster_t) {
    if !self_.is_null() {
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
