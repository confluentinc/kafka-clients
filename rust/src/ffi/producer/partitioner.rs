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

//! `kafka_producer_Partitioner_t`: the
//! `org.apache.kafka.clients.producer.Partitioner` interface (CLAUDE.md §4,
//! "Traits").
//!
//! `K` and `V` are `void *` (§4, "Generic types"): `partition` receives the
//! record's key and value exactly as the application passed them to the
//! `ProducerRecord`, beside their serialized bytes (borrowed for the call,
//! `data == NULL` standing for Java's `null`) and a `kafka_common_Cluster_t`
//! view of the metadata, also borrowed for the call.
//!
//! `configure` takes `&mut self`, so a handle guards its implementation with
//! a mutex (see [`SharedPartitioner`]) and that invoker takes a `*mut`
//! handle. The producer calls `partition` for every record through the same
//! mutex: a C partitioner costs one uncontended lock per record on top of the
//! Rust built-in ones, which never take it.

#![expect(non_camel_case_types)]

use std::collections::HashMap;
use std::ffi::{c_char, c_void};
use std::sync::{Arc, Mutex, PoisonError};

use crate::common::Cluster;
use crate::ffi::common::cluster::{box_cluster, kafka_common_Cluster_destroy, kafka_common_Cluster_t};
use crate::ffi::common::metrics::Interface;
use crate::ffi::ffi_guard;
use crate::ffi::util::{
    GenericValue, box_string_map, c_str_to_string, kafka_Bytes_t, kafka_Map_destroy, kafka_Map_t, map_strings,
    owned_c_string,
};
use crate::producer::Partitioner;

/// Opaque handle to a [`Partitioner`] implementation over the `void *`
/// representation.
#[repr(C)]
pub struct kafka_producer_Partitioner_t {
    _private: [u8; 0],
}

/// What every partitioner handle points at: the implementation behind the
/// mutex that serializes `configure` (`&mut self`) with the other methods.
pub(crate) type SharedPartitioner = Mutex<dyn Partitioner<GenericValue, GenericValue>>;

/// `void configure(Map<String, ?> configs)` of a C implementation: the
/// Java default is a no-op, so the pointer is nullable (CLAUDE.md §4 rule 3).
/// `configs` holds the producer's string configuration and is borrowed for
/// the call.
pub type kafka_producer_Partitioner_configure_fn_t =
    Option<unsafe extern "C" fn(self_: *mut c_void, _configs: *const kafka_Map_t)>;

/// `int partition(String topic, Object key, byte[] keyBytes, Object value,
/// byte[] valueBytes, Cluster cluster)` of a C implementation. `key` and
/// `value` are the record's `void *`s (`NULL` for Java's `null`), the byte
/// arrays their serialized form, and `cluster` a view borrowed for the call.
pub type kafka_producer_Partitioner_partition_fn_t = unsafe extern "C" fn(
    self_: *mut c_void,
    topic: *const c_char,
    key: *const c_void,
    key_bytes: kafka_Bytes_t,
    value: *const c_void,
    value_bytes: kafka_Bytes_t,
    cluster: *const kafka_common_Cluster_t,
) -> i32;

/// `void close()` of a C implementation; nullable, the Java default being a
/// no-op.
pub type kafka_producer_Partitioner_close_fn_t = Option<unsafe extern "C" fn(self_: *mut c_void)>;

/// The shared implementation behind a handle.
///
/// # Safety
///
/// `partitioner` must be a live handle, kept alive for `'a`.
pub(crate) unsafe fn partitioner_ref<'a>(partitioner: *const kafka_producer_Partitioner_t) -> &'a SharedPartitioner {
    // SAFETY: Per this function's `# Safety`, `partitioner` is a live handle kept alive for `'a`:
    // an owned one, the `Interface::owned` box of `kafka_producer_Partitioner_new`, or the view a
    // class handle caches (`kafka_producer_RoundRobinPartitioner__as_Partitioner`), as
    // `Interface::from_ptr` requires. The implementation returned is borrowed from the handle, so
    // it lives as long as the handle does.
    unsafe { Interface::<SharedPartitioner>::from_ptr(partitioner as *const Interface<SharedPartitioner>) }.get()
}

/// Takes the implementation a `*mut` parameter received: an owned handle is
/// freed and its implementation moved out, a view (`__as_Partitioner`) shares
/// its implementation and stays with its class handle.
///
/// # Safety
///
/// `partitioner` must be a live handle; an owned one is freed by this call
/// and must not be used again.
pub(crate) unsafe fn take_partitioner(partitioner: *mut kafka_producer_Partitioner_t) -> Arc<SharedPartitioner> {
    // SAFETY: Per this function's `# Safety`, `partitioner` is a live handle of this interface,
    // owned (from `kafka_producer_Partitioner_new`) or a cached `__as_Partitioner` view, and an
    // owned one is not used again after this call frees it, as `Interface::take` requires.
    unsafe { Interface::take(partitioner as *mut Interface<SharedPartitioner>) }
}

/// Builds a view handle over a shared implementation; the view belongs to
/// the class handle that caches it and is never destroyed on its own.
pub(crate) fn partitioner_view(imp: Arc<SharedPartitioner>) -> Interface<SharedPartitioner> {
    Interface::view(imp)
}

/// Adapts a shared implementation to the by-value `Box<dyn Partitioner>` the
/// Rust producer and mock take, so the same C partitioner can be handed to
/// several producers (Java shares the instance the same way).
pub(crate) struct SharedPartitionerAdapter(pub(crate) Arc<SharedPartitioner>);

impl Partitioner<GenericValue, GenericValue> for SharedPartitionerAdapter {
    fn configure(&mut self, configs: &HashMap<String, String>) {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).configure(configs);
    }

    fn partition(
        &self,
        topic: &str,
        key: Option<&GenericValue>,
        key_bytes: Option<&[u8]>,
        value: Option<&GenericValue>,
        value_bytes: Option<&[u8]>,
        cluster: &Cluster,
    ) -> i32 {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).partition(
            topic,
            key,
            key_bytes,
            value,
            value_bytes,
            cluster,
        )
    }

    fn close(&self) {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).close();
    }
}

/// A C implementation of [`Partitioner`] registered through
/// [`kafka_producer_Partitioner_new`].
struct CPartitioner {
    self_: *mut c_void,
    configure: kafka_producer_Partitioner_configure_fn_t,
    partition: kafka_producer_Partitioner_partition_fn_t,
    close: kafka_producer_Partitioner_close_fn_t,
}

// SAFETY: `self_` and the function pointers are what the C caller registered through
// `kafka_producer_Partitioner_new`, whose rustdoc lets the functions be called from any thread
// (the calling thread of a blocking entry point or a runtime worker of a `_cb` one), so the C
// caller is responsible for the thread-safety of `self_`; the function pointers are plain `Copy`
// values. `CPartitioner` never dereferences `self_`; it only passes it back to those functions.
unsafe impl Send for CPartitioner {}
// SAFETY: `Sync` is required by the `Partitioner` trait. A `CPartitioner` is built only inside the
// `SharedPartitioner` mutex of `kafka_producer_Partitioner_new`, so its methods never run
// concurrently, as that function's rustdoc states; a `&CPartitioner` exposes only the opaque
// `self_` and the `Copy` function pointers, and the type never dereferences `self_`.
unsafe impl Sync for CPartitioner {}

fn value_ptr(value: Option<&GenericValue>) -> *const c_void {
    value.map_or(std::ptr::null(), |v| v.as_ptr() as *const c_void)
}

impl Partitioner<GenericValue, GenericValue> for CPartitioner {
    fn configure(&mut self, configs: &HashMap<String, String>) {
        let Some(configure) = self.configure else {
            return;
        };
        let map = box_string_map(configs.iter());
        // SAFETY: `configure` and `self.self_` are the pair the C caller registered through
        // `kafka_producer_Partitioner_new`, whose rustdoc keeps `self_` alive while the handle is
        // live and until every producer it was given to is destroyed, which covers this call: it
        // runs on the implementation the handle or such a producer holds. `map` is the owned
        // `box_string_map` built above, alive for the call, which borrows it as
        // `kafka_producer_Partitioner_configure_fn_t` documents.
        unsafe { configure(self.self_, map) };
        // SAFETY: `map` is the owned map `box_string_map` built above, which the C function only
        // borrowed for the call; it is destroyed here, exactly once.
        unsafe { kafka_Map_destroy(map) };
    }

    fn partition(
        &self,
        topic: &str,
        key: Option<&GenericValue>,
        key_bytes: Option<&[u8]>,
        value: Option<&GenericValue>,
        value_bytes: Option<&[u8]>,
        cluster: &Cluster,
    ) -> i32 {
        let topic = owned_c_string(topic);
        // A `kafka_common_Cluster_t` is a snapshot built from the `Cluster`,
        // so it is rebuilt per call; the Rust built-in partitioners never pay
        // this.
        let cluster = box_cluster(cluster.clone());
        // SAFETY: `self.partition` and `self.self_` are the pair the C caller registered through
        // `kafka_producer_Partitioner_new`, whose rustdoc keeps `self_` alive while the handle is
        // live and until every producer it was given to is destroyed, which covers this call. The
        // arguments are borrowed for the call, as the `_fn_t` rustdoc states: `topic` is a
        // `CString` local, `key` and `value` the record's `void *`s this method was given, the byte
        // views borrow `key_bytes` and `value_bytes`, which outlive the call, and `cluster` is the
        // owned `box_cluster` handle built above.
        let partition = unsafe {
            (self.partition)(
                self.self_,
                topic.as_ptr(),
                value_ptr(key),
                kafka_Bytes_t::from_option(key_bytes),
                value_ptr(value),
                kafka_Bytes_t::from_option(value_bytes),
                cluster,
            )
        };
        // SAFETY: `cluster` is the owned `box_cluster` handle built above, which the C function
        // only borrowed for the call; it is destroyed here, exactly once.
        unsafe { kafka_common_Cluster_destroy(cluster) };
        partition
    }

    fn close(&self) {
        if let Some(close) = self.close {
            // SAFETY: `close` and `self.self_` are the pair the C caller registered through
            // `kafka_producer_Partitioner_new`, whose rustdoc keeps `self_` alive while the handle
            // is live and until every producer it was given to is destroyed, which covers this
            // call.
            unsafe { close(self.self_) };
        }
    }
}

/// Registers a C implementation of `Partitioner`.
///
/// The caller owns `self_` and keeps it alive while the handle is live and
/// until every producer the handle was given to has been destroyed, then
/// frees it; the handle itself is consumed by the `*mut` parameter that
/// receives it or freed with [`kafka_producer_Partitioner_destroy`].
/// `configure` and `close` may be `NULL` for the Java default (a no-op).
/// The functions may be called from any thread, but the calls through one
/// handle never overlap: the handle serialises them with a mutex.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_producer_Partitioner_new(
    self_: *mut c_void,
    configure: kafka_producer_Partitioner_configure_fn_t,
    partition: kafka_producer_Partitioner_partition_fn_t,
    close: kafka_producer_Partitioner_close_fn_t,
) -> *mut kafka_producer_Partitioner_t {
    let imp: Arc<SharedPartitioner> = Arc::new(Mutex::new(CPartitioner { self_, configure, partition, close }));
    Interface::owned(imp) as *mut kafka_producer_Partitioner_t
}

/// `Partitioner.configure(Map)`: forwards the string configuration to the
/// implementation.
///
/// # Safety
///
/// `self_` must be a live handle and `configs` a live map of C strings.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Partitioner_configure(
    self_: *mut kafka_producer_Partitioner_t,
    _configs: *const kafka_Map_t,
) {
    // SAFETY: Per this function's `# Safety`, `_configs` (the `configs` of the `# Safety`) is a
    // live map of C strings, as `map_strings` requires; its entries are copied into owned
    // `String`s.
    let configs: HashMap<String, String> = unsafe { map_strings(_configs) }.into_iter().collect();
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `partitioner_ref`
    // requires; the reference is used only within this call.
    unsafe { partitioner_ref(self_) }
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .configure(&configs);
}

/// `Partitioner.partition(...)`: `key` and `value` are the record's
/// `void *`s (`NULL` for `null`), the byte arrays are borrowed for the call
/// (`data == NULL` for `null`) and `cluster` is a live cluster handle.
///
/// # Safety
///
/// `self_` and `cluster` must be live handles; `topic` a NUL-terminated
/// string; `key_bytes` and `value_bytes` must each have a null `data` or
/// point to `len` readable bytes not written during the call.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Partitioner_partition(
    self_: *const kafka_producer_Partitioner_t,
    topic: *const c_char,
    key: *const c_void,
    key_bytes: kafka_Bytes_t,
    value: *const c_void,
    value_bytes: kafka_Bytes_t,
    cluster: *const kafka_common_Cluster_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    let topic = unsafe { c_str_to_string(topic) };
    let key = (!key.is_null()).then(|| GenericValue::new(key as *mut c_void));
    let value = (!value.is_null()).then(|| GenericValue::new(value as *mut c_void));
    // SAFETY: Per this function's `# Safety`, `cluster` is a live handle, as `cluster_ref`
    // requires; the reference is used only within this call.
    let cluster = unsafe { crate::ffi::common::cluster::cluster_ref(cluster) };
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `partitioner_ref`
    // requires; the reference is used only within this call.
    unsafe { partitioner_ref(self_) }
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .partition(
            &topic,
            key.as_ref(),
            // SAFETY: Per this function's `# Safety`, `key_bytes` has a null `data` or points to
            // `len` readable bytes not written during the call, as `kafka_Bytes_t::as_slice`
            // requires; the slice is used only within this call.
            unsafe { key_bytes.as_slice() },
            value.as_ref(),
            // SAFETY: Per this function's `# Safety`, `value_bytes` has a null `data` or points to
            // `len` readable bytes not written during the call, as `kafka_Bytes_t::as_slice`
            // requires; the slice is used only within this call.
            unsafe { value_bytes.as_slice() },
            cluster,
        )
}

/// `Partitioner.close()`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Partitioner_close(self_: *const kafka_producer_Partitioner_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `partitioner_ref`
    // requires; the reference is used only within this call.
    unsafe { partitioner_ref(self_) }
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .close();
}

/// Frees an owned handle built by [`kafka_producer_Partitioner_new`]; null
/// is a no-op. Never pass an `__as_Partitioner` view.
///
/// # Safety
///
/// `self_` must be null or an owned handle not consumed or destroyed before.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Partitioner_destroy(self_: *mut kafka_producer_Partitioner_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned handle not consumed or
    // destroyed before, never an `__as_Partitioner` view (as the rustdoc states), which is what
    // `Interface::destroy` requires.
    unsafe { Interface::<SharedPartitioner>::destroy(self_ as *mut Interface<SharedPartitioner>) };
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::sync::atomic::{AtomicI32, Ordering};

    use super::*;
    use crate::common::{Node, PartitionInfo};
    use crate::ffi::common::cluster::kafka_common_Cluster_node_by_id;
    use crate::ffi::util::{kafka_Map_get, kafka_Map_size};

    struct Recorder {
        configured: AtomicI32,
        closed: AtomicI32,
        last_key_len: AtomicI32,
        last_node: AtomicI32,
    }

    unsafe extern "C" fn configure(self_: *mut c_void, configs: *const kafka_Map_t) {
        // SAFETY: Test code: `self_` is the test's `Recorder`, which outlives every call.
        let recorder = unsafe { &*(self_ as *const Recorder) };
        let key = CString::new("bootstrap.servers").unwrap();
        // SAFETY: Test code: `configs` is the map `configure` borrows for this call, and `key` a
        // live `CString`.
        let value = unsafe { kafka_Map_get(configs, key.as_ptr() as *mut c_void) };
        assert!(!value.is_null(), "the configuration map is handed to configure");
        // SAFETY: Test code: `configs` is the map `configure` borrows for this call.
        assert_eq!(unsafe { kafka_Map_size(configs) }, 1);
        recorder.configured.fetch_add(1, Ordering::SeqCst);
    }

    unsafe extern "C" fn partition(
        self_: *mut c_void,
        topic: *const c_char,
        key: *const c_void,
        key_bytes: kafka_Bytes_t,
        value: *const c_void,
        value_bytes: kafka_Bytes_t,
        cluster: *const kafka_common_Cluster_t,
    ) -> i32 {
        // SAFETY: Test code: `self_` is the test's `Recorder`, which outlives every call.
        let recorder = unsafe { &*(self_ as *const Recorder) };
        // SAFETY: Test code: `topic` is the NUL-terminated string borrowed for this call.
        assert_eq!(unsafe { c_str_to_string(topic) }, "topic");
        // SAFETY: Test code: the test passes the address of a live `i32` as `key`.
        assert_eq!(unsafe { *(key as *const i32) }, 7);
        assert!(value.is_null());
        // SAFETY: Test code: `key_bytes` borrows a static byte string.
        assert_eq!(unsafe { key_bytes.as_slice() }, Some(&b"ab"[..]));
        // SAFETY: Test code: `value_bytes` is the null view.
        assert_eq!(unsafe { value_bytes.as_slice() }, None);
        recorder.last_key_len.store(key_bytes.len, Ordering::SeqCst);
        // SAFETY: Test code: `cluster` is the view borrowed for this call.
        let node = unsafe { kafka_common_Cluster_node_by_id(cluster, 1) };
        recorder.last_node.store(i32::from(!node.is_null()), Ordering::SeqCst);
        3
    }

    unsafe extern "C" fn close(self_: *mut c_void) {
        // SAFETY: Test code: `self_` is the test's `Recorder`, which outlives every call.
        unsafe { &*(self_ as *const Recorder) }.closed.fetch_add(1, Ordering::SeqCst);
    }

    fn cluster() -> Cluster {
        let node = Node::new(1, "localhost".to_string(), 9092);
        Cluster::new(
            None,
            vec![node.clone()],
            vec![PartitionInfo::new("topic".to_string(), 0, Some(node), vec![], vec![])],
            Default::default(),
            Default::default(),
        )
    }

    #[test]
    fn c_implementation_is_reached_through_the_invokers() {
        let recorder = Recorder {
            configured: AtomicI32::new(0),
            closed: AtomicI32::new(0),
            last_key_len: AtomicI32::new(-1),
            last_node: AtomicI32::new(0),
        };
        let handle = kafka_producer_Partitioner_new(
            &recorder as *const Recorder as *mut c_void,
            Some(configure),
            partition,
            Some(close),
        );
        let configs = box_string_map([("bootstrap.servers", "localhost:9092")]);
        let topic = CString::new("topic").unwrap();
        let key = 7i32;
        let cluster = box_cluster(cluster());
        // SAFETY: Test code: `handle` comes from `kafka_producer_Partitioner_new` over the test's
        // `recorder`, which outlives every call; `configs`, `cluster` and the `CString` live until
        // destroyed or dropped after their last use, `key` outlives the call reading it, and each
        // handle is destroyed once.
        unsafe {
            kafka_producer_Partitioner_configure(handle, configs);
            let partition = kafka_producer_Partitioner_partition(
                handle,
                topic.as_ptr(),
                &key as *const i32 as *const c_void,
                kafka_Bytes_t::from_slice(b"ab"),
                std::ptr::null(),
                kafka_Bytes_t::NULL,
                cluster,
            );
            assert_eq!(partition, 3);
            kafka_producer_Partitioner_close(handle);
            kafka_Map_destroy(configs);
            kafka_common_Cluster_destroy(cluster);
            kafka_producer_Partitioner_destroy(handle);
            kafka_producer_Partitioner_destroy(std::ptr::null_mut());
        }
        assert_eq!(recorder.configured.load(Ordering::SeqCst), 1);
        assert_eq!(recorder.closed.load(Ordering::SeqCst), 1);
        assert_eq!(recorder.last_key_len.load(Ordering::SeqCst), 2);
        assert_eq!(
            recorder.last_node.load(Ordering::SeqCst),
            1,
            "the cluster view carries the nodes"
        );
    }

    #[test]
    fn null_defaults_are_no_ops_and_the_adapter_shares_the_implementation() {
        let recorder = Recorder {
            configured: AtomicI32::new(0),
            closed: AtomicI32::new(0),
            last_key_len: AtomicI32::new(-1),
            last_node: AtomicI32::new(0),
        };
        let handle = kafka_producer_Partitioner_new(&recorder as *const Recorder as *mut c_void, None, partition, None);
        let configs = box_string_map([("bootstrap.servers", "localhost:9092")]);
        // SAFETY: Test code: `handle` comes from `kafka_producer_Partitioner_new` over the test's
        // `recorder`, and `configs` is destroyed once after its last use.
        unsafe {
            kafka_producer_Partitioner_configure(handle, configs);
            kafka_producer_Partitioner_close(handle);
            kafka_Map_destroy(configs);
        }
        assert_eq!(recorder.configured.load(Ordering::SeqCst), 0);
        assert_eq!(recorder.closed.load(Ordering::SeqCst), 0);

        // Taking the owned handle moves the implementation into the adapter.
        // SAFETY: Test code: `handle` is the owned handle above, not used again after
        // `take_partitioner` frees it.
        let shared = unsafe { take_partitioner(handle) };
        let mut adapter = SharedPartitionerAdapter(Arc::clone(&shared));
        adapter.configure(&HashMap::new());
        let key = GenericValue::new(&7i32 as *const i32 as *mut c_void);
        assert_eq!(adapter.partition("topic", Some(&key), Some(b"ab"), None, None, &cluster()), 3);
        adapter.close();
        assert_eq!(Arc::strong_count(&shared), 2);
    }
}
