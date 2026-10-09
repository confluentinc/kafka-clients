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

//! `kafka_producer_RoundRobinPartitioner_t`:
//! `org.apache.kafka.clients.producer.RoundRobinPartitioner` (CLAUDE.md §4).
//!
//! The class handle exists so a C caller can configure the built-in
//! partitioner without implementing `kafka_producer_Partitioner_t`: its
//! `__as_Partitioner` view is what `kafka_producer_ProducerConfig_set_partitioner`
//! and the mock options builder take.

use std::sync::{Arc, Mutex, OnceLock};

use crate::ffi::common::metrics::Interface;
use crate::ffi::producer::partitioner::{SharedPartitioner, kafka_producer_Partitioner_t, partitioner_view};
use crate::producer::RoundRobinPartitioner;

/// Opaque handle to a [`RoundRobinPartitioner`].
#[repr(C)]
pub struct kafka_producer_RoundRobinPartitioner_t {
    _private: [u8; 0],
}

struct RoundRobinPartitionerInner {
    imp: Arc<SharedPartitioner>,
    as_partitioner: OnceLock<Interface<SharedPartitioner>>,
}

unsafe fn inner<'a>(self_: *const kafka_producer_RoundRobinPartitioner_t) -> &'a RoundRobinPartitionerInner {
    unsafe { &*(self_ as *const RoundRobinPartitionerInner) }
}

/// `new RoundRobinPartitioner()`: owned, freed with
/// [`kafka_producer_RoundRobinPartitioner_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_producer_RoundRobinPartitioner_new() -> *mut kafka_producer_RoundRobinPartitioner_t {
    let imp: Arc<SharedPartitioner> = Arc::new(Mutex::new(RoundRobinPartitioner::new()));
    Box::into_raw(Box::new(RoundRobinPartitionerInner { imp, as_partitioner: OnceLock::new() }))
        as *mut kafka_producer_RoundRobinPartitioner_t
}

/// The class as a `Partitioner`: a borrowed view valid until the class
/// handle is destroyed, never passed to `kafka_producer_Partitioner_destroy`
/// (CLAUDE.md §4 rule 3). A `*mut kafka_producer_Partitioner_t` parameter
/// that receives it shares the implementation and leaves the class handle
/// in place, which the caller keeps alive until the producer is destroyed.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RoundRobinPartitioner__as_Partitioner(
    self_: *mut kafka_producer_RoundRobinPartitioner_t,
) -> *mut kafka_producer_Partitioner_t {
    let inner = unsafe { inner(self_) };
    let view = inner.as_partitioner.get_or_init(|| partitioner_view(Arc::clone(&inner.imp)));
    view as *const Interface<SharedPartitioner> as *mut kafka_producer_Partitioner_t
}

/// Frees the handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or a handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RoundRobinPartitioner_destroy(
    self_: *mut kafka_producer_RoundRobinPartitioner_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut RoundRobinPartitionerInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;

    use super::*;
    use crate::common::{Cluster, Node, PartitionInfo};
    use crate::ffi::common::cluster::{box_cluster, kafka_common_Cluster_destroy};
    use crate::ffi::producer::partitioner::{kafka_producer_Partitioner_partition, take_partitioner};
    use crate::ffi::util::kafka_Bytes_t;

    fn cluster() -> Cluster {
        let node = Node::new(1, "localhost".to_string(), 9092);
        Cluster::new(
            None,
            vec![node.clone()],
            (0..3)
                .map(|p| PartitionInfo::new("topic".to_string(), p, Some(node.clone()), vec![], vec![]))
                .collect(),
            Default::default(),
            Default::default(),
        )
    }

    #[test]
    fn view_is_cached_and_round_robins_through_the_invoker() {
        let handle = kafka_producer_RoundRobinPartitioner_new();
        let cluster = box_cluster(cluster());
        let topic = CString::new("topic").unwrap();
        unsafe {
            let view = kafka_producer_RoundRobinPartitioner__as_Partitioner(handle);
            assert_eq!(view, kafka_producer_RoundRobinPartitioner__as_Partitioner(handle));
            let partition = |view| {
                kafka_producer_Partitioner_partition(
                    view,
                    topic.as_ptr(),
                    std::ptr::null(),
                    kafka_Bytes_t::NULL,
                    std::ptr::null(),
                    kafka_Bytes_t::NULL,
                    cluster,
                )
            };
            let mut seen: Vec<i32> = (0..3).map(|_| partition(view)).collect();
            seen.sort_unstable();
            assert_eq!(seen, vec![0, 1, 2], "three sends round-robin over the three partitions");

            // Taking the view shares the implementation and leaves the class
            // handle alive.
            let shared = take_partitioner(view);
            assert_eq!(Arc::strong_count(&shared), 3, "class handle, cached view and the taken Arc");
            drop(shared);
            kafka_common_Cluster_destroy(cluster);
            kafka_producer_RoundRobinPartitioner_destroy(handle);
            kafka_producer_RoundRobinPartitioner_destroy(std::ptr::null_mut());
        }
    }
}
