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

//! `kafka_producer_MockProducer_t`:
//! `org.apache.kafka.clients.producer.MockProducer<K, V>` (CLAUDE.md §4),
//! with the `MockProducerOptions` / `MockProducerOptionsBuilder` pair that
//! stands for the five-parameter constructor (§2).
//!
//! The class handle has the same shape as `kafka_producer_KafkaProducer_t`
//! (see the module docs of [`crate::ffi::producer`]) and its `Producer`
//! methods are reached through [`kafka_producer_MockProducer__as_Producer`];
//! the functions declared here are the mock's own inspection and control
//! methods. Without serializers the key and value `void *` are kept as they
//! are: the mock never reads them, and `history` returns records carrying the
//! same pointers.

use std::collections::HashMap;
use std::ffi::{c_char, c_void};
use std::sync::Arc;

use crate::common::{Cluster, TopicPartition};
use crate::consumer::OffsetAndMetadata;
use crate::ffi::common::cluster::{cluster_ref, kafka_common_Cluster_t};
use crate::ffi::common::metric_name::{kafka_common_MetricName_t, metric_name_ref};
use crate::ffi::common::metrics::kafka_metric::{kafka_common_metrics_KafkaMetric_t, kafka_metric_ref};
use crate::ffi::common::serialization::serializer::{
    DynSerializer, kafka_common_serialization_Serializer_t, take_serializer,
};
use crate::ffi::common::topic_partition::{
    box_topic_partition, kafka_common_TopicPartition_destroy, kafka_common_TopicPartition_t, topic_partition_ref,
};
use crate::ffi::common::{box_error, kafka_common_Error_t, take_error};
use crate::ffi::consumer::{
    box_offset_and_metadata, kafka_consumer_OffsetAndMetadata_destroy, kafka_consumer_OffsetAndMetadata_t,
};
use crate::ffi::producer::kafka_producer::ArcSerializer;
use crate::ffi::producer::partitioner::{
    SharedPartitioner, SharedPartitionerAdapter, kafka_producer_Partitioner_t, take_partitioner,
};
use crate::ffi::producer::producer_record::{box_producer_record, kafka_producer_ProducerRecord_destroy};
use crate::ffi::producer::{ProducerHandle, kafka_producer_Producer_t};
use crate::ffi::util::{
    GenericValue, box_list, box_map, box_string_keyed_map, c_str_to_string, kafka_List_t, kafka_Map_destroy,
    kafka_Map_t,
};
use crate::producer::{MockProducer, MockProducerOptions, MockProducerOptionsBuilder};

/// Opaque handle to a [`MockProducer`] over `void *` key and value.
#[repr(C)]
pub struct kafka_producer_MockProducer_t {
    _private: [u8; 0],
}

/// Opaque handle to built `MockProducerOptions`.
// Rust-only: the `MockProducerOptions` struct CLAUDE.md §2 mandates for the
// overloaded constructors; Java has no such class.
#[doc(alias = "rust-only")]
#[repr(C)]
pub struct kafka_producer_MockProducerOptions_t {
    _private: [u8; 0],
}

/// Opaque handle to a `MockProducerOptionsBuilder`.
// Rust-only: builds `MockProducerOptions` (CLAUDE.md §2)
#[doc(alias = "rust-only")]
#[repr(C)]
pub struct kafka_producer_MockProducerOptionsBuilder_t {
    _private: [u8; 0],
}

type Handle = ProducerHandle<MockProducer<GenericValue, GenericValue>>;
type GenericOptions = MockProducerOptions<GenericValue, GenericValue>;

/// The pieces of a built options value. The Rust options own boxed
/// partitioner and serializers and are consumed by `MockProducer::with_options`,
/// while the C handle is borrowed by `kafka_producer_MockProducer_with_options`;
/// the handle therefore keeps shareable pieces and rebuilds the Rust value for
/// every producer created from it.
struct OptionsInner {
    cluster: Cluster,
    auto_complete: bool,
    partitioner: Option<Arc<SharedPartitioner>>,
    key_serializer: Option<Arc<DynSerializer>>,
    value_serializer: Option<Arc<DynSerializer>>,
}

impl OptionsInner {
    fn build(&self) -> GenericOptions {
        MockProducerOptions {
            cluster: self.cluster.clone(),
            auto_complete: self.auto_complete,
            partitioner: self.partitioner.as_ref().map(|p| {
                Box::new(SharedPartitionerAdapter(Arc::clone(p)))
                    as Box<dyn crate::producer::Partitioner<GenericValue, GenericValue>>
            }),
            key_serializer: self.key_serializer.as_ref().map(|s| {
                Box::new(ArcSerializer(Arc::clone(s)))
                    as Box<dyn crate::common::serialization::Serializer<GenericValue> + Send + Sync>
            }),
            value_serializer: self.value_serializer.as_ref().map(|s| {
                Box::new(ArcSerializer(Arc::clone(s)))
                    as Box<dyn crate::common::serialization::Serializer<GenericValue> + Send + Sync>
            }),
        }
    }
}

/// The builder's state: what the Rust builder holds, with the shareable
/// pieces the C handles hand in.
#[derive(Default)]
struct BuilderInner {
    cluster: Option<Cluster>,
    auto_complete: Option<bool>,
    partitioner: Option<Arc<SharedPartitioner>>,
    key_serializer: Option<Arc<DynSerializer>>,
    value_serializer: Option<Arc<DynSerializer>>,
}

unsafe fn handle<'a>(self_: *const kafka_producer_MockProducer_t) -> &'a Handle {
    unsafe { &*(self_ as *const Handle) }
}

unsafe fn mock<'a>(self_: *const kafka_producer_MockProducer_t) -> &'a MockProducer<GenericValue, GenericValue> {
    unsafe { handle(self_) }.producer()
}

fn box_mock(producer: MockProducer<GenericValue, GenericValue>) -> *mut kafka_producer_MockProducer_t {
    let handle: Box<Handle> = ProducerHandle::new(|| Ok(producer)).expect("the mock constructor cannot fail");
    Box::into_raw(handle) as *mut kafka_producer_MockProducer_t
}

// ---------------------------------------------------------------------------
// Constructors and the view
// ---------------------------------------------------------------------------

/// `new MockProducer(boolean autoComplete, Serializer, Serializer)` without
/// serializers: owned, freed with [`kafka_producer_MockProducer_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_producer_MockProducer_with_auto_complete(
    auto_complete: i8,
) -> *mut kafka_producer_MockProducer_t {
    box_mock(MockProducer::with_auto_complete(auto_complete != 0))
}

/// `new MockProducer(Cluster, boolean autoComplete, Serializer, Serializer)`
/// without serializers; the cluster is copied.
///
/// # Safety
///
/// `cluster` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_with_cluster_auto_complete(
    cluster: *const kafka_common_Cluster_t,
    auto_complete: i8,
) -> *mut kafka_producer_MockProducer_t {
    box_mock(MockProducer::with_cluster_auto_complete(
        unsafe { cluster_ref(cluster) }.clone(),
        auto_complete != 0,
    ))
}

/// `MockProducer::with_options`: the five-parameter constructor through
/// built [`kafka_producer_MockProducerOptions_t`], which stay the caller's.
///
/// # Safety
///
/// `options` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_with_options(
    options: *const kafka_producer_MockProducerOptions_t,
) -> *mut kafka_producer_MockProducer_t {
    box_mock(MockProducer::with_options(
        unsafe { &*(options as *const OptionsInner) }.build(),
    ))
}

/// The class as a `Producer`: a borrowed view valid until the class handle
/// is destroyed (CLAUDE.md §4 rule 3).
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer__as_Producer(
    self_: *const kafka_producer_MockProducer_t,
) -> *const kafka_producer_Producer_t {
    unsafe { handle(self_) }.as_producer()
}

/// Frees the handle (see "Destroy" in the module docs of
/// [`crate::ffi::producer`]); null is a no-op.
///
/// # Safety
///
/// `self_` must be null or a handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_destroy(self_: *mut kafka_producer_MockProducer_t) {
    if !self_.is_null() {
        unsafe { *Box::from_raw(self_ as *mut Handle) }.destroy();
    }
}

// ---------------------------------------------------------------------------
// Inspection
// ---------------------------------------------------------------------------

fn record_list(records: Vec<crate::producer::ProducerRecord<GenericValue, GenericValue>>) -> *mut kafka_List_t {
    let elements = records
        .into_iter()
        .map(|record| box_producer_record(record) as *mut c_void)
        .collect();
    box_list(elements, Some(destroy_record))
}

unsafe fn destroy_record(element: *mut c_void) {
    unsafe { kafka_producer_ProducerRecord_destroy(element as *mut _) }
}

/// A `Map<TopicPartition, OffsetAndMetadata>` as an owned `kafka_Map_t` of
/// owned `kafka_common_TopicPartition_t *` to owned
/// `kafka_consumer_OffsetAndMetadata_t *`, `kafka_Map_get` comparing
/// partitions by value.
fn offsets_map(offsets: HashMap<TopicPartition, OffsetAndMetadata>) -> *mut kafka_Map_t {
    let mut offsets: Vec<_> = offsets.into_iter().collect();
    offsets.sort_by(|(a, _), (b, _)| a.topic().cmp(b.topic()).then(a.partition().cmp(&b.partition())));
    let entries = offsets
        .into_iter()
        .map(|(tp, oam)| {
            (
                box_topic_partition(tp) as *mut c_void,
                box_offset_and_metadata(oam) as *mut c_void,
            )
        })
        .collect();
    box_map(
        entries,
        Some(destroy_topic_partition),
        Some(destroy_offset_and_metadata),
        Some(topic_partition_eq),
    )
}

unsafe fn destroy_topic_partition(element: *mut c_void) {
    unsafe { kafka_common_TopicPartition_destroy(element as *mut kafka_common_TopicPartition_t) }
}

unsafe fn destroy_offset_and_metadata(element: *mut c_void) {
    unsafe { kafka_consumer_OffsetAndMetadata_destroy(element as *mut kafka_consumer_OffsetAndMetadata_t) }
}

unsafe fn topic_partition_eq(a: *mut c_void, b: *mut c_void) -> bool {
    unsafe { topic_partition_ref(a as *const _) == topic_partition_ref(b as *const _) }
}

/// A `Map<String, Map<TopicPartition, OffsetAndMetadata>>` as an owned
/// string-keyed `kafka_Map_t` of owned [`offsets_map`]s.
fn group_offsets_map(groups: HashMap<String, HashMap<TopicPartition, OffsetAndMetadata>>) -> *mut kafka_Map_t {
    let mut groups: Vec<_> = groups.into_iter().collect();
    groups.sort_by(|(a, _), (b, _)| a.cmp(b));
    box_string_keyed_map(
        groups
            .into_iter()
            .map(|(group, offsets)| (group, offsets_map(offsets) as *mut c_void)),
        Some(destroy_map),
    )
}

unsafe fn destroy_map(element: *mut c_void) {
    unsafe { kafka_Map_destroy(element as *mut kafka_Map_t) }
}

/// `MockProducer.history()`: an owned `kafka_List_t` of owned
/// `kafka_producer_ProducerRecord_t *`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_history(
    self_: *const kafka_producer_MockProducer_t,
) -> *mut kafka_List_t {
    record_list(unsafe { mock(self_) }.history())
}

/// `MockProducer.uncommittedRecords()`: an owned `kafka_List_t` of owned
/// `kafka_producer_ProducerRecord_t *`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_uncommitted_records(
    self_: *const kafka_producer_MockProducer_t,
) -> *mut kafka_List_t {
    record_list(unsafe { mock(self_) }.uncommitted_records())
}

/// `MockProducer.consumerGroupOffsetsHistory()`: an owned `kafka_List_t` of
/// owned `kafka_Map_t *`, each a `char *` group id to an owned `kafka_Map_t *`
/// of `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_consumer_group_offsets_history(
    self_: *const kafka_producer_MockProducer_t,
) -> *mut kafka_List_t {
    let elements = unsafe { mock(self_) }
        .consumer_group_offsets_history()
        .into_iter()
        .map(|groups| group_offsets_map(groups) as *mut c_void)
        .collect();
    box_list(elements, Some(destroy_map))
}

/// `MockProducer.committedOffset(String group, TopicPartition)`: owned,
/// `NULL` when none (Java's `null`).
///
/// # Safety
///
/// `self_` and `topic_partition` must be live handles and `group` a
/// NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_committed_offset(
    self_: *const kafka_producer_MockProducer_t,
    group: *const c_char,
    topic_partition: *const kafka_common_TopicPartition_t,
) -> *mut kafka_consumer_OffsetAndMetadata_t {
    let group = unsafe { c_str_to_string(group) };
    let topic_partition = unsafe { topic_partition_ref(topic_partition) };
    unsafe { mock(self_) }
        .committed_offset(&group, topic_partition)
        .map_or(std::ptr::null_mut(), box_offset_and_metadata)
}

/// `MockProducer.uncommittedOffsets()`: an owned `kafka_Map_t` of `char *`
/// group id to an owned `kafka_Map_t *` of `kafka_common_TopicPartition_t *`
/// to `kafka_consumer_OffsetAndMetadata_t *`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_uncommitted_offsets(
    self_: *const kafka_producer_MockProducer_t,
) -> *mut kafka_Map_t {
    group_offsets_map(unsafe { mock(self_) }.uncommitted_offsets())
}

// ---------------------------------------------------------------------------
// Control
// ---------------------------------------------------------------------------

/// `MockProducer.clear()`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_clear(self_: *const kafka_producer_MockProducer_t) {
    unsafe { mock(self_) }.clear();
}

/// `MockProducer.completeNext()`: completes the earliest uncompleted send
/// successfully (its delivery callback is queued); `0` when none was pending.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_complete_next(self_: *const kafka_producer_MockProducer_t) -> i8 {
    i8::from(unsafe { mock(self_) }.complete_next())
}

/// `MockProducer.errorNext(RuntimeException e)`: fails the earliest
/// uncompleted send with `error`, whose ownership moves to the mock. As in
/// Java, a `NULL` error completes the send successfully. `0` when none was
/// pending.
///
/// # Safety
///
/// `self_` must be a live handle and `error` null or an error not yet
/// destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_error_next(
    self_: *const kafka_producer_MockProducer_t,
    error: *mut kafka_common_Error_t,
) -> i8 {
    let mock = unsafe { mock(self_) };
    i8::from(match unsafe { take_error(error) } {
        Some(error) => mock.error_next(error),
        None => mock.complete_next(),
    })
}

/// `MockProducer.fenceProducer()`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_fence_producer(
    self_: *const kafka_producer_MockProducer_t,
) -> *mut kafka_common_Error_t {
    unsafe { mock(self_) }
        .fence_producer()
        .err()
        .map_or(std::ptr::null_mut(), box_error)
}

/// `MockProducer.transactionInitialized()`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_transaction_initialized(
    self_: *const kafka_producer_MockProducer_t,
) -> i8 {
    i8::from(unsafe { mock(self_) }.transaction_initialized())
}

/// `MockProducer.transactionInFlight()`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_transaction_in_flight(
    self_: *const kafka_producer_MockProducer_t,
) -> i8 {
    i8::from(unsafe { mock(self_) }.transaction_in_flight())
}

/// `MockProducer.transactionCommitted()`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_transaction_committed(
    self_: *const kafka_producer_MockProducer_t,
) -> i8 {
    i8::from(unsafe { mock(self_) }.transaction_committed())
}

/// `MockProducer.transactionAborted()`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_transaction_aborted(
    self_: *const kafka_producer_MockProducer_t,
) -> i8 {
    i8::from(unsafe { mock(self_) }.transaction_aborted())
}

/// `MockProducer.sentOffsets()`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_sent_offsets(self_: *const kafka_producer_MockProducer_t) -> i8 {
    i8::from(unsafe { mock(self_) }.sent_offsets())
}

/// `MockProducer.closed()`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_closed(self_: *const kafka_producer_MockProducer_t) -> i8 {
    i8::from(unsafe { mock(self_) }.closed())
}

/// `MockProducer.flushed()`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_flushed(self_: *const kafka_producer_MockProducer_t) -> i8 {
    i8::from(unsafe { mock(self_) }.flushed())
}

/// `MockProducer.commitCount()`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_commit_count(self_: *const kafka_producer_MockProducer_t) -> i64 {
    unsafe { mock(self_) }.commit_count()
}

/// `MockProducer.sendException = e`: the error the next `send` fails with.
/// The error's ownership moves to the mock; `NULL` clears it.
///
/// # Safety
///
/// `self_` must be a live handle and `error` null or an error not yet
/// destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_set_send_error(
    self_: *const kafka_producer_MockProducer_t,
    error: *mut kafka_common_Error_t,
) {
    unsafe { mock(self_) }.set_send_error(unsafe { take_error(error) });
}

/// `MockProducer.flushException = e`.
/// The error's ownership moves to the mock; `NULL` clears it.
///
/// # Safety
///
/// `self_` must be a live handle and `error` null or an error not yet
/// destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_set_flush_error(
    self_: *const kafka_producer_MockProducer_t,
    error: *mut kafka_common_Error_t,
) {
    unsafe { mock(self_) }.set_flush_error(unsafe { take_error(error) });
}

/// `MockProducer.partitionsForException = e`.
/// The error's ownership moves to the mock; `NULL` clears it.
///
/// # Safety
///
/// `self_` must be a live handle and `error` null or an error not yet
/// destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_set_partitions_for_error(
    self_: *const kafka_producer_MockProducer_t,
    error: *mut kafka_common_Error_t,
) {
    unsafe { mock(self_) }.set_partitions_for_error(unsafe { take_error(error) });
}

/// `MockProducer.closeException = e`.
/// The error's ownership moves to the mock; `NULL` clears it.
///
/// # Safety
///
/// `self_` must be a live handle and `error` null or an error not yet
/// destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_set_close_error(
    self_: *const kafka_producer_MockProducer_t,
    error: *mut kafka_common_Error_t,
) {
    unsafe { mock(self_) }.set_close_error(unsafe { take_error(error) });
}

/// `MockProducer.initTransactionException = e`.
/// The error's ownership moves to the mock; `NULL` clears it.
///
/// # Safety
///
/// `self_` must be a live handle and `error` null or an error not yet
/// destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_set_init_transaction_error(
    self_: *const kafka_producer_MockProducer_t,
    error: *mut kafka_common_Error_t,
) {
    unsafe { mock(self_) }.set_init_transaction_error(unsafe { take_error(error) });
}

/// `MockProducer.beginTransactionException = e`.
/// The error's ownership moves to the mock; `NULL` clears it.
///
/// # Safety
///
/// `self_` must be a live handle and `error` null or an error not yet
/// destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_set_begin_transaction_error(
    self_: *const kafka_producer_MockProducer_t,
    error: *mut kafka_common_Error_t,
) {
    unsafe { mock(self_) }.set_begin_transaction_error(unsafe { take_error(error) });
}

/// `MockProducer.sendOffsetsToTransactionException = e`.
/// The error's ownership moves to the mock; `NULL` clears it.
///
/// # Safety
///
/// `self_` must be a live handle and `error` null or an error not yet
/// destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_set_send_offsets_to_transaction_error(
    self_: *const kafka_producer_MockProducer_t,
    error: *mut kafka_common_Error_t,
) {
    unsafe { mock(self_) }.set_send_offsets_to_transaction_error(unsafe { take_error(error) });
}

/// `MockProducer.commitTransactionException = e`.
/// The error's ownership moves to the mock; `NULL` clears it.
///
/// # Safety
///
/// `self_` must be a live handle and `error` null or an error not yet
/// destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_set_commit_transaction_error(
    self_: *const kafka_producer_MockProducer_t,
    error: *mut kafka_common_Error_t,
) {
    unsafe { mock(self_) }.set_commit_transaction_error(unsafe { take_error(error) });
}

/// `MockProducer.abortTransactionException = e`.
/// The error's ownership moves to the mock; `NULL` clears it.
///
/// # Safety
///
/// `self_` must be a live handle and `error` null or an error not yet
/// destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_set_abort_transaction_error(
    self_: *const kafka_producer_MockProducer_t,
    error: *mut kafka_common_Error_t,
) {
    unsafe { mock(self_) }.set_abort_transaction_error(unsafe { take_error(error) });
}

/// `MockProducer.setMockMetrics(MetricName, Metric)`: both are copied
/// (the metric shares its Rust value).
///
/// # Safety
///
/// `self_`, `name` and `metric` must be live handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducer_set_mock_metrics(
    self_: *const kafka_producer_MockProducer_t,
    name: *const kafka_common_MetricName_t,
    metric: *const kafka_common_metrics_KafkaMetric_t,
) {
    let name = unsafe { metric_name_ref(name) }.clone();
    let metric = Arc::clone(unsafe { kafka_metric_ref(metric) });
    unsafe { mock(self_) }.set_mock_metrics(name, metric);
}

// ---------------------------------------------------------------------------
// MockProducerOptions / MockProducerOptionsBuilder
// ---------------------------------------------------------------------------

unsafe fn builder<'a>(self_: *mut kafka_producer_MockProducerOptionsBuilder_t) -> &'a mut BuilderInner {
    unsafe { &mut *(self_ as *mut BuilderInner) }
}

/// `MockProducerOptionsBuilder::new()`: owned, freed with
/// [`kafka_producer_MockProducerOptionsBuilder_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_producer_MockProducerOptionsBuilder_new() -> *mut kafka_producer_MockProducerOptionsBuilder_t {
    Box::into_raw(Box::new(BuilderInner::default())) as *mut kafka_producer_MockProducerOptionsBuilder_t
}

/// `set_cluster`: copied; unset means `Cluster.empty()`.
///
/// # Safety
///
/// `self_` and `cluster` must be live handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducerOptionsBuilder_set_cluster(
    self_: *mut kafka_producer_MockProducerOptionsBuilder_t,
    cluster: *const kafka_common_Cluster_t,
) {
    unsafe { builder(self_) }.cluster = Some(unsafe { cluster_ref(cluster) }.clone());
}

/// `set_auto_complete`: mandatory.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducerOptionsBuilder_set_auto_complete(
    self_: *mut kafka_producer_MockProducerOptionsBuilder_t,
    auto_complete: i8,
) {
    unsafe { builder(self_) }.auto_complete = Some(auto_complete != 0);
}

/// `set_partitioner`: consumes an owned `kafka_producer_Partitioner_t` or
/// shares an `__as_Partitioner` view (the class handle then outlives the
/// producers built from these options); `NULL` means none.
///
/// # Safety
///
/// `self_` must be a live handle and `partitioner` null or a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducerOptionsBuilder_set_partitioner(
    self_: *mut kafka_producer_MockProducerOptionsBuilder_t,
    partitioner: *mut kafka_producer_Partitioner_t,
) {
    unsafe { builder(self_) }.partitioner = (!partitioner.is_null()).then(|| unsafe { take_partitioner(partitioner) });
}

/// `set_key_serializer`: consumes an owned
/// `kafka_common_serialization_Serializer_t` or shares an `__as_Serializer`
/// view; `NULL` means none.
///
/// # Safety
///
/// `self_` must be a live handle and `key_serializer` null or a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducerOptionsBuilder_set_key_serializer(
    self_: *mut kafka_producer_MockProducerOptionsBuilder_t,
    key_serializer: *mut kafka_common_serialization_Serializer_t,
) {
    unsafe { builder(self_) }.key_serializer =
        (!key_serializer.is_null()).then(|| unsafe { take_serializer(key_serializer) });
}

/// `set_value_serializer`: as
/// [`kafka_producer_MockProducerOptionsBuilder_set_key_serializer`].
///
/// # Safety
///
/// `self_` must be a live handle and `value_serializer` null or a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducerOptionsBuilder_set_value_serializer(
    self_: *mut kafka_producer_MockProducerOptionsBuilder_t,
    value_serializer: *mut kafka_common_serialization_Serializer_t,
) {
    unsafe { builder(self_) }.value_serializer =
        (!value_serializer.is_null()).then(|| unsafe { take_serializer(value_serializer) });
}

/// `build`: validates the mandatory `auto_complete` with the Rust builder's
/// own check and delivers the options, owned by the caller
/// ([`kafka_producer_MockProducerOptions_destroy`]). The builder stays
/// usable.
///
/// # Safety
///
/// `self_` must be a live handle and `out_build` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducerOptionsBuilder_build(
    self_: *mut kafka_producer_MockProducerOptionsBuilder_t,
    out_build: *mut *mut kafka_producer_MockProducerOptions_t,
) -> *mut kafka_common_Error_t {
    let builder = unsafe { builder(self_) };
    // The Rust builder owns the validation (and its message); the shareable
    // pieces are not part of it.
    let mut validation = MockProducerOptionsBuilder::<GenericValue, GenericValue>::new();
    if let Some(cluster) = &builder.cluster {
        validation = validation.set_cluster(cluster.clone());
    }
    if let Some(auto_complete) = builder.auto_complete {
        validation = validation.set_auto_complete(auto_complete);
    }
    let validated = match validation.build() {
        Ok(options) => options,
        Err(error) => return box_error(error),
    };
    let inner = OptionsInner {
        cluster: validated.cluster,
        auto_complete: validated.auto_complete,
        partitioner: builder.partitioner.clone(),
        key_serializer: builder.key_serializer.clone(),
        value_serializer: builder.value_serializer.clone(),
    };
    unsafe { *out_build = Box::into_raw(Box::new(inner)) as *mut kafka_producer_MockProducerOptions_t };
    std::ptr::null_mut()
}

/// Frees the builder; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or a handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducerOptionsBuilder_destroy(
    self_: *mut kafka_producer_MockProducerOptionsBuilder_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut BuilderInner) });
    }
}

/// Frees built options; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or a handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_MockProducerOptions_destroy(self_: *mut kafka_producer_MockProducerOptions_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut OptionsInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::sync::atomic::{AtomicI32, Ordering};

    use super::*;
    use crate::ffi::common::{kafka_common_Error_destroy, kafka_common_Error_message};
    use crate::ffi::kafka_future::{
        kafka_common_KafkaFuture_destroy, kafka_common_KafkaFuture_get, kafka_common_KafkaFuture_t,
    };
    use crate::ffi::producer::callback::{kafka_producer_Callback_destroy, kafka_producer_Callback_new};
    use crate::ffi::producer::producer_record::{
        kafka_producer_ProducerRecord_destroy, kafka_producer_ProducerRecord_new, kafka_producer_ProducerRecord_value,
    };
    use crate::ffi::producer::record_metadata::{
        kafka_producer_RecordMetadata_offset, kafka_producer_RecordMetadata_t,
    };
    use crate::ffi::producer::{
        kafka_producer_Producer_begin_transaction, kafka_producer_Producer_commit_transaction,
        kafka_producer_Producer_execute_callbacks, kafka_producer_Producer_flush,
        kafka_producer_Producer_init_transactions, kafka_producer_Producer_send, kafka_producer_Producer_send_cb,
        kafka_producer_Producer_send_with_callback,
    };
    use crate::ffi::util::{kafka_List_destroy, kafka_List_get, kafka_List_size, kafka_string_destroy};

    struct Seen {
        completions: AtomicI32,
        sends: AtomicI32,
        last_offset: AtomicI32,
    }

    unsafe extern "C" fn on_completion(
        self_: *mut c_void,
        metadata: *const kafka_producer_RecordMetadata_t,
        error: *const kafka_common_Error_t,
    ) {
        let seen = unsafe { &*(self_ as *const Seen) };
        assert!(error.is_null());
        seen.last_offset.store(
            unsafe { kafka_producer_RecordMetadata_offset(metadata) } as i32,
            Ordering::SeqCst,
        );
        seen.completions.fetch_add(1, Ordering::SeqCst);
    }

    unsafe extern "C" fn on_sent(
        value: *mut kafka_common_KafkaFuture_t,
        error: *mut kafka_common_Error_t,
        opaque: *mut c_void,
    ) {
        let seen = unsafe { &*(opaque as *const Seen) };
        assert!(error.is_null());
        assert!(!value.is_null());
        unsafe { kafka_common_KafkaFuture_destroy(value) };
        seen.sends.fetch_add(1, Ordering::SeqCst);
    }

    #[test]
    fn blocking_send_records_history_and_queues_the_delivery_callback() {
        let seen = Seen {
            completions: AtomicI32::new(0),
            sends: AtomicI32::new(0),
            last_offset: AtomicI32::new(-1),
        };
        let topic = CString::new("topic").unwrap();
        let value = 42i32;
        unsafe {
            let mock = kafka_producer_MockProducer_with_auto_complete(0);
            let view = kafka_producer_MockProducer__as_Producer(mock);
            let record = kafka_producer_ProducerRecord_new(topic.as_ptr(), &value as *const i32 as *const c_void);
            let callback = kafka_producer_Callback_new(&seen as *const Seen as *mut c_void, on_completion);

            let mut future = std::ptr::null_mut();
            assert!(kafka_producer_Producer_send_with_callback(view, record, callback, &raw mut future).is_null());
            let history = kafka_producer_MockProducer_history(mock);
            assert_eq!(kafka_List_size(history), 1);
            assert_eq!(
                kafka_producer_ProducerRecord_value(kafka_List_get(history, 0) as *const _),
                &value as *const i32 as *mut c_void
            );
            kafka_List_destroy(history);

            // Nothing fired yet: the mock holds the completion.
            assert_eq!(kafka_producer_Producer_execute_callbacks(view), 0);
            assert_eq!(kafka_producer_MockProducer_complete_next(mock), 1);
            assert_eq!(kafka_producer_Producer_execute_callbacks(view), 1);
            assert_eq!(seen.completions.load(Ordering::SeqCst), 1);
            assert_eq!(seen.last_offset.load(Ordering::SeqCst), 0);
            assert_eq!(kafka_producer_MockProducer_complete_next(mock), 0);

            let mut metadata = std::ptr::null_mut();
            assert!(kafka_common_KafkaFuture_get(future, &raw mut metadata).is_null());
            assert_eq!(kafka_producer_RecordMetadata_offset(metadata as *const _), 0);
            // The metadata is borrowed from the future and freed with it.
            kafka_common_KafkaFuture_destroy(future);

            // A send error set on the mock surfaces through the error slot.
            kafka_producer_MockProducer_set_send_error(
                mock,
                box_error(crate::common::Error::local_illegal_state("boom")),
            );
            let error = kafka_producer_Producer_send(view, record, &raw mut future);
            assert_eq!(c_str_to_string(kafka_common_Error_message(error)), "boom");
            kafka_common_Error_destroy(error);

            kafka_producer_Callback_destroy(callback);
            kafka_producer_ProducerRecord_destroy(record);
            kafka_producer_MockProducer_destroy(mock);
            kafka_producer_MockProducer_destroy(std::ptr::null_mut());
        }
    }

    #[test]
    fn send_cb_is_drained_by_flush_and_the_control_operations() {
        let seen = Seen {
            completions: AtomicI32::new(0),
            sends: AtomicI32::new(0),
            last_offset: AtomicI32::new(-1),
        };
        let topic = CString::new("topic").unwrap();
        unsafe {
            let mock = kafka_producer_MockProducer_with_auto_complete(1);
            let view = kafka_producer_MockProducer__as_Producer(mock);
            let record = kafka_producer_ProducerRecord_new(topic.as_ptr(), std::ptr::null());

            assert!(kafka_producer_Producer_init_transactions(view).is_null());
            assert!(kafka_producer_Producer_begin_transaction(view).is_null());
            for _ in 0..3 {
                kafka_producer_Producer_send_cb(view, record, on_sent, &seen as *const Seen as *mut c_void);
            }
            // The commit drains the queued sends first, so they are committed.
            assert!(kafka_producer_Producer_commit_transaction(view).is_null());
            assert_eq!(kafka_producer_MockProducer_transaction_committed(mock), 1);
            let history = kafka_producer_MockProducer_history(mock);
            assert_eq!(kafka_List_size(history), 3);
            kafka_List_destroy(history);
            let uncommitted = kafka_producer_MockProducer_uncommitted_records(mock);
            assert_eq!(kafka_List_size(uncommitted), 0);
            kafka_List_destroy(uncommitted);

            assert!(kafka_producer_Producer_flush(view).is_null());
            assert_eq!(kafka_producer_MockProducer_flushed(mock), 1);
            assert_eq!(kafka_producer_Producer_execute_callbacks(view), 3);
            assert_eq!(seen.sends.load(Ordering::SeqCst), 3);
            assert_eq!(kafka_producer_MockProducer_commit_count(mock), 1);

            kafka_producer_ProducerRecord_destroy(record);
            kafka_producer_MockProducer_destroy(mock);
        }
    }

    #[test]
    fn options_builder_validates_and_builds_a_mock() {
        unsafe {
            let builder = kafka_producer_MockProducerOptionsBuilder_new();
            let mut options = std::ptr::null_mut();
            let error = kafka_producer_MockProducerOptionsBuilder_build(builder, &raw mut options);
            assert_eq!(
                c_str_to_string(kafka_common_Error_message(error)),
                "MockProducerOptionsBuilder::build: mandatory parameter `auto_complete` was not set"
            );
            kafka_common_Error_destroy(error);

            kafka_producer_MockProducerOptionsBuilder_set_auto_complete(builder, 1);
            kafka_producer_MockProducerOptionsBuilder_set_partitioner(builder, std::ptr::null_mut());
            assert!(kafka_producer_MockProducerOptionsBuilder_build(builder, &raw mut options).is_null());
            kafka_producer_MockProducerOptionsBuilder_destroy(builder);

            let mock = kafka_producer_MockProducer_with_options(options);
            assert_eq!(kafka_producer_MockProducer_closed(mock), 0);
            assert_eq!(kafka_producer_MockProducer_transaction_initialized(mock), 0);
            let text = kafka_producer_MockProducer_uncommitted_offsets(mock);
            kafka_Map_destroy(text);
            let history = kafka_producer_MockProducer_consumer_group_offsets_history(mock);
            assert_eq!(kafka_List_size(history), 0);
            kafka_List_destroy(history);
            kafka_producer_MockProducer_destroy(mock);
            kafka_producer_MockProducerOptions_destroy(options);
            kafka_producer_MockProducerOptions_destroy(std::ptr::null_mut());
            kafka_producer_MockProducerOptionsBuilder_destroy(std::ptr::null_mut());
            let _ = kafka_string_destroy;
        }
    }
}
