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

//! `kafka_producer_KafkaProducer_t`:
//! `org.apache.kafka.clients.producer.KafkaProducer<K, V>` (CLAUDE.md §4).
//!
//! The class handle owns the producer, its runtime and its callback vector
//! (see the module docs of [`crate::ffi::producer`]). Its `Producer` methods
//! are reached through [`kafka_producer_KafkaProducer__as_Producer`]; the
//! functions declared here are the ones Java declares on the class itself:
//! the constructor, the transactional methods and `send(record, callback)`.
//!
//! # Serializers
//!
//! The constructor takes `kafka_common_serialization_Serializer_t` handles
//! for key and value (`*mut`: an owned handle is consumed, an `__as_Serializer`
//! view shares its class, which the caller keeps alive until the producer is
//! destroyed). A `NULL` serializer means the key or value `void *` is a
//! `kafka_Bytes_t *` whose bytes are the serialized form.

#![expect(non_camel_case_types)]

use std::ffi::c_void;
use std::sync::Arc;

use crate::common::Error;
use crate::common::header::RecordHeaders;
use crate::common::serialization::Serializer;
use crate::ffi::common::serialization::serializer::{
    DynSerializer, kafka_common_serialization_Serializer_t, take_serializer,
};
use crate::ffi::common::{box_error, deliver_panic, deliver_panic_value, kafka_common_Error_t};
use crate::ffi::consumer::kafka_consumer_ConsumerGroupMetadata_t;
use crate::ffi::ffi_guard;
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::producer::callback::kafka_producer_Callback_t;
use crate::ffi::producer::producer_config::{kafka_producer_ProducerConfig_t, producer_config_ref};
use crate::ffi::producer::producer_record::kafka_producer_ProducerRecord_t;
use crate::ffi::producer::{
    ProducerHandle, completion_sink, kafka_producer_Producer_abort_transaction,
    kafka_producer_Producer_abort_transaction_cb, kafka_producer_Producer_begin_transaction,
    kafka_producer_Producer_begin_transaction_cb, kafka_producer_Producer_commit_transaction,
    kafka_producer_Producer_commit_transaction_cb, kafka_producer_Producer_init_transactions,
    kafka_producer_Producer_init_transactions_cb, kafka_producer_Producer_send_offsets_to_transaction,
    kafka_producer_Producer_send_offsets_to_transaction_cb, kafka_producer_Producer_send_with_callback,
    kafka_producer_Producer_send_with_callback_cb, kafka_producer_Producer_t,
};
use crate::ffi::util::{GenericValue, kafka_Bytes_t, kafka_Map_t};
use crate::producer::KafkaProducer;

/// Opaque handle to a [`KafkaProducer`] over `void *` key and value.
#[repr(C)]
pub struct kafka_producer_KafkaProducer_t {
    _private: [u8; 0],
}

type Handle = ProducerHandle<KafkaProducer<GenericValue, GenericValue>>;

/// Completion of a `void` transactional method on the class: the same shape
/// as the interface's.
pub type kafka_producer_KafkaProducer_init_transactions_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);
/// Completion of a `void` transactional method on the class: the same shape
/// as the interface's.
pub type kafka_producer_KafkaProducer_begin_transaction_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);
/// Completion of a `void` transactional method on the class: the same shape
/// as the interface's.
pub type kafka_producer_KafkaProducer_send_offsets_to_transaction_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);
/// Completion of a `void` transactional method on the class: the same shape
/// as the interface's.
pub type kafka_producer_KafkaProducer_commit_transaction_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);
/// Completion of a `void` transactional method on the class: the same shape
/// as the interface's.
pub type kafka_producer_KafkaProducer_abort_transaction_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);
/// Completion of [`kafka_producer_KafkaProducer_send_cb`]: the same shape as
/// `kafka_producer_Producer_send_cb_t`.
pub type kafka_producer_KafkaProducer_send_cb_t =
    unsafe extern "C" fn(value: *mut kafka_common_KafkaFuture_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The serializer behind a `NULL` handle: the `void *` is a `kafka_Bytes_t *`
/// and its bytes are the serialized form (`data == NULL` is Java's `null`).
pub(crate) struct BytesPassthrough;

impl Serializer<GenericValue> for BytesPassthrough {
    fn serialize(&self, _topic: &str, data: Option<&GenericValue>) -> Result<Option<Vec<u8>>, Error> {
        Ok(data.filter(|v| !v.is_null()).and_then(|v| {
            // SAFETY: by contract a `void *` key or value of a producer built
            // without a serializer points at a live `kafka_Bytes_t`.
            unsafe { (*(v.as_ptr() as *const kafka_Bytes_t)).as_slice() }.map(<[u8]>::to_vec)
        }))
    }
}

/// A C serializer shared with its registration: the producer's
/// `Box<dyn Serializer>` forwarding to the `Arc` the handle hands out.
pub(crate) struct ArcSerializer(pub(crate) Arc<DynSerializer>);

impl Serializer<GenericValue> for ArcSerializer {
    fn serialize(&self, topic: &str, data: Option<&GenericValue>) -> Result<Option<Vec<u8>>, Error> {
        self.0.serialize(topic, data)
    }

    fn serialize_with_headers(
        &self,
        topic: &str,
        headers: &RecordHeaders,
        data: Option<&GenericValue>,
    ) -> Result<Option<Vec<u8>>, Error> {
        self.0.serialize_with_headers(topic, headers, data)
    }

    fn serialize_owned_with_headers(
        &self,
        topic: &str,
        headers: &RecordHeaders,
        data: Option<GenericValue>,
    ) -> Result<Option<Vec<u8>>, Error> {
        self.0.serialize_owned_with_headers(topic, headers, data)
    }
}

/// The Rust serializer a `*mut kafka_common_serialization_Serializer_t`
/// parameter stands for (see the module docs).
///
/// # Safety
///
/// `serializer` must be null or a live handle; an owned one must not be used
/// again by the caller.
pub(crate) unsafe fn serializer_from(
    serializer: *mut kafka_common_serialization_Serializer_t,
) -> Box<dyn Serializer<GenericValue> + Send + Sync> {
    if serializer.is_null() {
        Box::new(BytesPassthrough)
    } else {
        // SAFETY: Per this function's `# Safety`, `serializer` is a live handle (non-null, checked
        // above), as `take_serializer` requires; an owned one is not used again by the caller, as
        // the `# Safety` states.
        Box::new(ArcSerializer(unsafe { take_serializer(serializer) }))
    }
}

/// The class state behind a handle.
///
/// # Safety
///
/// `self_` must be a live handle, kept alive for `'a`.
unsafe fn handle<'a>(self_: *const kafka_producer_KafkaProducer_t) -> &'a Handle {
    // SAFETY: Per this function's `# Safety`, `self_` is a live class handle kept alive for `'a`,
    // i.e. the `Box::into_raw` of a `Handle` in `kafka_producer_KafkaProducer_new`, the only
    // constructor. `Handle` is only ever shared (`destroy` takes it back by `Box::from_raw`), so
    // the shared reference is sound.
    unsafe { &*(self_ as *const Handle) }
}

/// The handle's `__as_Producer` view.
///
/// # Safety
///
/// `self_` must be a live handle.
unsafe fn producer(self_: *const kafka_producer_KafkaProducer_t) -> *const kafka_producer_Producer_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live class handle, as `handle` requires;
    // the view returned is the address of the handle's `client` field, which lives as long as the
    // handle.
    unsafe { handle(self_) }.as_producer()
}

/// `new KafkaProducer(ProducerConfig, Serializer<K>, Serializer<V>)`: the
/// configuration stays the caller's; the producer is delivered owned, freed
/// with [`kafka_producer_KafkaProducer_destroy`].
///
/// # Safety
///
/// `config` must be a live handle, each serializer null or a live handle (an
/// owned one not used again by the caller), `out_new` a valid slot.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_new(
    config: *const kafka_producer_ProducerConfig_t,
    key_serializer: *mut kafka_common_serialization_Serializer_t,
    value_serializer: *mut kafka_common_serialization_Serializer_t,
    out_new: *mut *mut kafka_producer_KafkaProducer_t,
) -> *mut kafka_common_Error_t {
    // The serializers are taken first, so that a configuration error below
    // drops (and so frees) the owned ones the caller handed over.
    // SAFETY: Per this function's `# Safety`, `key_serializer` is null or a live handle, not used
    // again by the caller when owned, as `serializer_from` requires.
    let key_serializer = unsafe { serializer_from(key_serializer) };
    // SAFETY: Per this function's `# Safety`, `value_serializer` is null or a live handle, not used
    // again by the caller when owned, as `serializer_from` requires.
    let value_serializer = unsafe { serializer_from(value_serializer) };
    // SAFETY: Per this function's `# Safety`, `config` is a live handle; no
    // `kafka_producer_ProducerConfig_set_partitioner` call can run on it meanwhile, since that
    // function's `# Safety` excludes any other call using the handle at the same time, so
    // `producer_config_ref`'s requirement holds for this statement, the only use of the reference.
    let config = match unsafe { producer_config_ref(config) }.build() {
        Ok(config) => config,
        Err(error) => return box_error(error),
    };
    match ProducerHandle::new(|| KafkaProducer::new(config, key_serializer, value_serializer), true) {
        Ok(handle) => {
            // SAFETY: Per this function's `# Safety`, `out_new` is a valid slot, so the write is
            // sound; the handle written is the fresh box the caller owns, freed with
            // `kafka_producer_KafkaProducer_destroy`.
            unsafe { *out_new = Box::into_raw(handle) as *mut kafka_producer_KafkaProducer_t };
            std::ptr::null_mut()
        },
        Err(error) => box_error(error),
    }
}

/// The class as a `Producer`: a borrowed view valid until the class handle
/// is destroyed (CLAUDE.md §4 rule 3); there is no
/// `kafka_producer_Producer_destroy`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer__as_Producer(
    self_: *const kafka_producer_KafkaProducer_t,
) -> *const kafka_producer_Producer_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `producer` requires; the
    // view is valid until the class handle is destroyed, as the rustdoc states, since it is the
    // address of a field of that handle.
    unsafe { producer(self_) }
}

/// `KafkaProducer.initTransactions()`: see
/// [`kafka_producer_Producer_init_transactions`].
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_init_transactions(
    self_: *const kafka_producer_KafkaProducer_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `producer` requires, so
    // `producer(self_)` is its live `__as_Producer` view, which is what
    // `kafka_producer_Producer_init_transactions`'s `# Safety` asks for.
    unsafe { kafka_producer_Producer_init_transactions(producer(self_)) }
}

/// `KafkaProducer.initTransactions()`, completion queued.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, so `producer(self_)` is its
    // live `__as_Producer` view, as `completion_sink` requires. The guard runs this only when the
    // body did not return, and the body's one step, the call to
    // `kafka_producer_Producer_init_transactions_cb`, delivers `cb` itself (a panic inside it
    // included) before it returns, so `cb` has not fired for this `opaque`, as `deliver_panic`
    // requires.
    unsafe { deliver_panic(completion_sink(producer(self_)), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_init_transactions_cb(
    self_: *const kafka_producer_KafkaProducer_t,
    cb: kafka_producer_KafkaProducer_init_transactions_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `producer` requires, so
    // `producer(self_)` is its live `__as_Producer` view, which is what
    // `kafka_producer_Producer_init_transactions_cb`'s `# Safety` asks for.
    unsafe { kafka_producer_Producer_init_transactions_cb(producer(self_), cb, opaque) }
}

/// `KafkaProducer.beginTransaction()`: see
/// [`kafka_producer_Producer_begin_transaction`].
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_begin_transaction(
    self_: *const kafka_producer_KafkaProducer_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `producer` requires, so
    // `producer(self_)` is its live `__as_Producer` view, which is what
    // `kafka_producer_Producer_begin_transaction`'s `# Safety` asks for.
    unsafe { kafka_producer_Producer_begin_transaction(producer(self_)) }
}

/// `KafkaProducer.beginTransaction()`, completion queued.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, so `producer(self_)` is its
    // live `__as_Producer` view, as `completion_sink` requires. The guard runs this only when the
    // body did not return, and the body's one step, the call to
    // `kafka_producer_Producer_begin_transaction_cb`, delivers `cb` itself (a panic inside it
    // included) before it returns, so `cb` has not fired for this `opaque`, as `deliver_panic`
    // requires.
    unsafe { deliver_panic(completion_sink(producer(self_)), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_begin_transaction_cb(
    self_: *const kafka_producer_KafkaProducer_t,
    cb: kafka_producer_KafkaProducer_begin_transaction_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `producer` requires, so
    // `producer(self_)` is its live `__as_Producer` view, which is what
    // `kafka_producer_Producer_begin_transaction_cb`'s `# Safety` asks for.
    unsafe { kafka_producer_Producer_begin_transaction_cb(producer(self_), cb, opaque) }
}

/// `KafkaProducer.sendOffsetsToTransaction(...)`: see
/// [`kafka_producer_Producer_send_offsets_to_transaction`].
///
/// # Safety
///
/// `self_` must be a live handle, `offsets` a live map of
/// `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *`
/// that no other call mutates during this one, and `group_metadata` a live
/// handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_send_offsets_to_transaction(
    self_: *const kafka_producer_KafkaProducer_t,
    offsets: *const kafka_Map_t,
    group_metadata: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `producer` requires, so
    // `producer(self_)` is its live `__as_Producer` view, `offsets` a live map of topic-partition
    // to offset-and-metadata handles that no other call mutates meanwhile and `group_metadata` a
    // live handle, which is what
    // `kafka_producer_Producer_send_offsets_to_transaction`'s `# Safety` asks for.
    unsafe { kafka_producer_Producer_send_offsets_to_transaction(producer(self_), offsets, group_metadata) }
}

/// `KafkaProducer.sendOffsetsToTransaction(...)`, completion queued.
///
/// # Safety
///
/// `self_` must be a live handle, `offsets` a live map of
/// `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *`
/// that no other call mutates during this one, and `group_metadata` a live
/// handle.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, so `producer(self_)` is its
    // live `__as_Producer` view, as `completion_sink` requires. The guard runs this only when the
    // body did not return, and the body's one step, the call to
    // `kafka_producer_Producer_send_offsets_to_transaction_cb`, delivers `cb` itself (a panic
    // inside it included) before it returns, so `cb` has not fired for this `opaque`, as
    // `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(producer(self_)), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_send_offsets_to_transaction_cb(
    self_: *const kafka_producer_KafkaProducer_t,
    offsets: *const kafka_Map_t,
    group_metadata: *const kafka_consumer_ConsumerGroupMetadata_t,
    cb: kafka_producer_KafkaProducer_send_offsets_to_transaction_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `producer` requires, so
    // `producer(self_)` is its live `__as_Producer` view, `offsets` a live map of topic-partition
    // to offset-and-metadata handles that no other call mutates meanwhile and `group_metadata` a
    // live handle, which is what
    // `kafka_producer_Producer_send_offsets_to_transaction_cb`'s `# Safety` asks for.
    unsafe {
        kafka_producer_Producer_send_offsets_to_transaction_cb(producer(self_), offsets, group_metadata, cb, opaque)
    }
}

/// `KafkaProducer.commitTransaction()`: see
/// [`kafka_producer_Producer_commit_transaction`].
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_commit_transaction(
    self_: *const kafka_producer_KafkaProducer_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `producer` requires, so
    // `producer(self_)` is its live `__as_Producer` view, which is what
    // `kafka_producer_Producer_commit_transaction`'s `# Safety` asks for.
    unsafe { kafka_producer_Producer_commit_transaction(producer(self_)) }
}

/// `KafkaProducer.commitTransaction()`, completion queued.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, so `producer(self_)` is its
    // live `__as_Producer` view, as `completion_sink` requires. The guard runs this only when the
    // body did not return, and the body's one step, the call to
    // `kafka_producer_Producer_commit_transaction_cb`, delivers `cb` itself (a panic inside it
    // included) before it returns, so `cb` has not fired for this `opaque`, as `deliver_panic`
    // requires.
    unsafe { deliver_panic(completion_sink(producer(self_)), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_commit_transaction_cb(
    self_: *const kafka_producer_KafkaProducer_t,
    cb: kafka_producer_KafkaProducer_commit_transaction_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `producer` requires, so
    // `producer(self_)` is its live `__as_Producer` view, which is what
    // `kafka_producer_Producer_commit_transaction_cb`'s `# Safety` asks for.
    unsafe { kafka_producer_Producer_commit_transaction_cb(producer(self_), cb, opaque) }
}

/// `KafkaProducer.abortTransaction()`: see
/// [`kafka_producer_Producer_abort_transaction`].
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_abort_transaction(
    self_: *const kafka_producer_KafkaProducer_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `producer` requires, so
    // `producer(self_)` is its live `__as_Producer` view, which is what
    // `kafka_producer_Producer_abort_transaction`'s `# Safety` asks for.
    unsafe { kafka_producer_Producer_abort_transaction(producer(self_)) }
}

/// `KafkaProducer.abortTransaction()`, completion queued.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, so `producer(self_)` is its
    // live `__as_Producer` view, as `completion_sink` requires. The guard runs this only when the
    // body did not return, and the body's one step, the call to
    // `kafka_producer_Producer_abort_transaction_cb`, delivers `cb` itself (a panic inside it
    // included) before it returns, so `cb` has not fired for this `opaque`, as `deliver_panic`
    // requires.
    unsafe { deliver_panic(completion_sink(producer(self_)), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_abort_transaction_cb(
    self_: *const kafka_producer_KafkaProducer_t,
    cb: kafka_producer_KafkaProducer_abort_transaction_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `producer` requires, so
    // `producer(self_)` is its live `__as_Producer` view, which is what
    // `kafka_producer_Producer_abort_transaction_cb`'s `# Safety` asks for.
    unsafe { kafka_producer_Producer_abort_transaction_cb(producer(self_), cb, opaque) }
}

/// `KafkaProducer.send(ProducerRecord, Callback)`: see
/// [`kafka_producer_Producer_send_with_callback`]; `callback` may be `NULL`.
///
/// # Safety
///
/// `self_` and `record` must be live handles, `callback` null or a live
/// handle, `out_send` a valid slot.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_send(
    self_: *const kafka_producer_KafkaProducer_t,
    record: *const kafka_producer_ProducerRecord_t,
    callback: *const kafka_producer_Callback_t,
    out_send: *mut *mut kafka_common_KafkaFuture_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `producer` requires, so
    // `producer(self_)` is its live `__as_Producer` view, `record` a live handle, `callback` null
    // or a live handle and `out_send` a valid slot, which is what
    // `kafka_producer_Producer_send_with_callback`'s `# Safety` asks for.
    unsafe { kafka_producer_Producer_send_with_callback(producer(self_), record, callback, out_send) }
}

/// `KafkaProducer.send(ProducerRecord, Callback)`, completion queued: see
/// [`kafka_producer_Producer_send_with_callback_cb`].
///
/// # Safety
///
/// `self_` and `record` must be live handles, `callback` null or a live
/// handle.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, so `producer(self_)` is its
    // live `__as_Producer` view, as `completion_sink` requires. The guard runs this only when the
    // body did not return, and the body's one step, the call to
    // `kafka_producer_Producer_send_with_callback_cb`, delivers `cb` itself (a panic inside it
    // included) before it returns, so `cb` has not fired for this `opaque`, as
    // `deliver_panic_value` requires.
    unsafe { deliver_panic_value(completion_sink(producer(self_)), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_send_cb(
    self_: *const kafka_producer_KafkaProducer_t,
    record: *const kafka_producer_ProducerRecord_t,
    callback: *const kafka_producer_Callback_t,
    cb: kafka_producer_KafkaProducer_send_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `producer` requires, so
    // `producer(self_)` is its live `__as_Producer` view, `record` a live handle and `callback`
    // null or a live handle, which is what `kafka_producer_Producer_send_with_callback_cb`'s
    // `# Safety` asks for.
    unsafe { kafka_producer_Producer_send_with_callback_cb(producer(self_), record, callback, cb, opaque) }
}

/// Frees the handle (see "Destroy" in the module docs of
/// [`crate::ffi::producer`]); null is a no-op. Every `__as_Producer` view of
/// it is invalid afterwards.
///
/// # Safety
///
/// `self_` must be null or a handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_destroy(self_: *mut kafka_producer_KafkaProducer_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, a handle
        // not yet destroyed, i.e. the `Box::into_raw` of `kafka_producer_KafkaProducer_new`, so
        // `Box::from_raw` reclaims it exactly once; its `__as_Producer` views, addresses inside it,
        // are invalid afterwards, as the rustdoc states.
        unsafe { *Box::from_raw(self_ as *mut Handle) }.destroy();
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;

    use super::*;
    use crate::ffi::common::{kafka_common_Error_destroy, kafka_common_Error_message};
    use crate::ffi::kafka_future::kafka_common_KafkaFuture_destroy;
    use crate::ffi::producer::producer_config::{
        kafka_producer_ProducerConfig_destroy, kafka_producer_ProducerConfig_new,
    };
    use crate::ffi::producer::producer_record::{
        kafka_producer_ProducerRecord_destroy, kafka_producer_ProducerRecord_new,
    };
    use crate::ffi::producer::{
        kafka_producer_Producer__execute_callbacks, kafka_producer_Producer_close_cb,
        kafka_producer_Producer_close_with_timeout, kafka_producer_Producer_close_with_timeout_cb,
        kafka_producer_Producer_send_cb,
    };
    use crate::ffi::util::{box_string_map, c_str_to_string, kafka_Map_destroy};

    /// An owned serializer is the producer's from the call on, so a
    /// configuration the constructor cannot build frees it rather than
    /// leaking it.
    #[test]
    fn a_configuration_error_frees_the_owned_serializers() {
        let imp: Arc<DynSerializer> = Arc::new(BytesPassthrough);
        let props = [("bootstrap.servers".to_string(), String::new())].into_iter().collect();
        let config = crate::ffi::producer::producer_config::ProducerConfigInner::unvalidated_for_test(props);
        // SAFETY: Test code: `config` and the two owned serializers are live handles, the
        // serializers handed over to the constructor and not used again, and `config` is destroyed
        // once below.
        unsafe {
            let key = crate::ffi::common::serialization::serializer::SerializerInner::owned(Arc::clone(&imp));
            let value = crate::ffi::common::serialization::serializer::SerializerInner::owned(Arc::clone(&imp));
            assert_eq!(Arc::strong_count(&imp), 3);
            let mut producer = std::ptr::null_mut();
            let error = kafka_producer_KafkaProducer_new(config, key, value, &raw mut producer);
            assert!(!error.is_null(), "the empty bootstrap.servers is rejected");
            assert!(producer.is_null());
            kafka_common_Error_destroy(error);
            kafka_producer_ProducerConfig_destroy(config);
        }
        assert_eq!(Arc::strong_count(&imp), 1, "both owned serializers were freed");
    }

    /// A `KafkaProducer` that can never fetch metadata, so a send waits out
    /// `max.block.ms` (60 s) for it.
    fn unreachable_producer() -> *mut kafka_producer_KafkaProducer_t {
        let props = box_string_map([("bootstrap.servers", "127.0.0.1:1"), ("max.block.ms", "60000")]);
        // SAFETY: Test code: `props` and `config` are live from their constructor to their single
        // destroy below; the producer is handed to the caller, who destroys it.
        unsafe {
            let mut config = std::ptr::null_mut();
            assert!(kafka_producer_ProducerConfig_new(props, &raw mut config).is_null());
            kafka_Map_destroy(props);
            let mut producer = std::ptr::null_mut();
            let error =
                kafka_producer_KafkaProducer_new(config, std::ptr::null_mut(), std::ptr::null_mut(), &raw mut producer);
            assert!(error.is_null());
            kafka_producer_ProducerConfig_destroy(config);
            producer
        }
    }

    /// Records a `send_cb` completion: `None` for a future, the error's
    /// message otherwise.
    unsafe extern "C" fn record_send(
        future: *mut kafka_common_KafkaFuture_t,
        error: *mut kafka_common_Error_t,
        opaque: *mut c_void,
    ) {
        let outcome = if error.is_null() {
            // SAFETY: the completion owns the future; freed once.
            unsafe { kafka_common_KafkaFuture_destroy(future) };
            None
        } else {
            // SAFETY: the completion owns `error`; it is read, then freed once.
            let message = unsafe { c_str_to_string(kafka_common_Error_message(error)) };
            // SAFETY: as above.
            unsafe { kafka_common_Error_destroy(error) };
            Some(message)
        };
        // SAFETY: `opaque` is the test's slot, alive until the callbacks ran.
        unsafe { &*(opaque as *const std::sync::Mutex<Vec<Option<String>>>) }
            .lock()
            .unwrap()
            .push(outcome);
    }

    /// Records a `void` completion: `None` for success, the error's message
    /// otherwise.
    unsafe extern "C" fn record_void(error: *mut kafka_common_Error_t, opaque: *mut c_void) {
        let outcome = (!error.is_null()).then(|| {
            // SAFETY: the completion owns `error`; it is read, then freed once.
            let message = unsafe { c_str_to_string(kafka_common_Error_message(error)) };
            // SAFETY: as above.
            unsafe { kafka_common_Error_destroy(error) };
            message
        });
        // SAFETY: `opaque` is the test's slot, alive until the callbacks ran.
        unsafe { &*(opaque as *const std::sync::Mutex<Vec<Option<String>>>) }
            .lock()
            .unwrap()
            .push(outcome);
    }

    /// Java's `close(Duration)` bounds the caller's whole wait
    /// (`KafkaProducer.java` `close(Duration, boolean)`), so a timed close
    /// behind a `send_cb` waiting out `max.block.ms` for metadata returns
    /// within its timeout, drain included: the record being handed over fails
    /// with the close, and the ones queued behind it as sent after close.
    /// Both forms.
    #[test]
    fn a_timed_close_bounds_the_drain_of_the_queued_sends() {
        let topic = CString::new("topic").unwrap();
        let bytes = kafka_Bytes_t::from_slice(b"v");
        for queued in [false, true] {
            let sends = std::sync::Mutex::new(Vec::<Option<String>>::new());
            let sends_opaque = &sends as *const std::sync::Mutex<Vec<Option<String>>> as *mut c_void;
            // SAFETY: Test code: the producer and `record` are live from their constructor to their
            // single destroy below, `view` is used only before the producer is destroyed, and
            // `sends` and `closed` outlive the callback runs that write them.
            unsafe {
                let producer = unreachable_producer();
                let view = kafka_producer_KafkaProducer__as_Producer(producer);
                let record =
                    kafka_producer_ProducerRecord_new(topic.as_ptr(), &bytes as *const kafka_Bytes_t as *const c_void);
                for _ in 0..3 {
                    kafka_producer_Producer_send_cb(view, record, record_send, sends_opaque);
                }
                let start = std::time::Instant::now();
                if queued {
                    let closed = std::sync::Mutex::new(Vec::new());
                    kafka_producer_Producer_close_with_timeout_cb(
                        view,
                        500,
                        record_void,
                        &closed as *const std::sync::Mutex<Vec<Option<String>>> as *mut c_void,
                    );
                    while closed.lock().unwrap().is_empty() {
                        kafka_producer_Producer__execute_callbacks(view);
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    assert_eq!(*closed.lock().unwrap(), [None]);
                } else {
                    assert!(kafka_producer_Producer_close_with_timeout(view, 500).is_null());
                }
                assert!(start.elapsed() < std::time::Duration::from_secs(10), "{:?}", start.elapsed());
                while sends.lock().unwrap().len() < 3 {
                    assert!(start.elapsed() < std::time::Duration::from_secs(10), "{:?}", start.elapsed());
                    kafka_producer_Producer__execute_callbacks(view);
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                let sends = sends.lock().unwrap().clone();
                assert_eq!(sends[0].as_deref(), Some("Producer closed while send in progress"));
                for later in &sends[1..] {
                    assert_eq!(
                        later.as_deref(),
                        Some("Cannot perform operation after producer has been closed")
                    );
                }
                kafka_producer_KafkaProducer_destroy(producer);
                kafka_producer_ProducerRecord_destroy(record);
            }
        }
    }

    /// A forced `close_with_timeout_cb(0)` queued behind an untimed
    /// `close_cb`, with `_destroy` right after it (what Python's cancelled
    /// async `close()` does), closes the producer: the record the untimed
    /// close waits for fails at once and `_destroy` returns promptly, each
    /// completion firing once. `_destroy` used to close the submission queue
    /// first, which failed the forced close's drain before it closed the
    /// producer, so `_destroy` waited out `max.block.ms` (60 s here).
    #[test]
    fn destroy_right_after_a_forced_close_behind_an_untimed_one_returns_promptly() {
        let topic = CString::new("topic").unwrap();
        let bytes = kafka_Bytes_t::from_slice(b"v");
        let sends = std::sync::Mutex::new(Vec::<Option<String>>::new());
        let closes = std::sync::Mutex::new(Vec::<Option<String>>::new());
        // SAFETY: Test code: the producer and `record` are live from their constructor to their
        // single destroy below, `view` is used only before the producer is destroyed, and `sends`
        // and `closes` outlive the destroy that runs their callbacks.
        unsafe {
            let producer = unreachable_producer();
            let view = kafka_producer_KafkaProducer__as_Producer(producer);
            let record =
                kafka_producer_ProducerRecord_new(topic.as_ptr(), &bytes as *const kafka_Bytes_t as *const c_void);
            kafka_producer_Producer_send_cb(view, record, record_send, &sends as *const _ as *mut c_void);
            kafka_producer_Producer_close_cb(view, record_void, &closes as *const _ as *mut c_void);
            // The send is in flight, waiting for the topic's metadata.
            std::thread::sleep(std::time::Duration::from_millis(100));
            let start = std::time::Instant::now();
            kafka_producer_Producer_close_with_timeout_cb(view, 0, record_void, &closes as *const _ as *mut c_void);
            kafka_producer_KafkaProducer_destroy(producer);
            let elapsed = start.elapsed();
            assert!(elapsed < std::time::Duration::from_secs(10), "destroy took {elapsed:?}");
            assert_eq!(*closes.lock().unwrap(), vec![None, None]);
            assert_eq!(sends.lock().unwrap().len(), 1);
            kafka_producer_ProducerRecord_destroy(record);
        }
    }

    #[test]
    fn passthrough_reads_bytes_and_null_means_null() {
        let bytes = kafka_Bytes_t::from_slice(b"abc");
        let value = GenericValue::new(&bytes as *const kafka_Bytes_t as *mut c_void);
        assert_eq!(BytesPassthrough.serialize("t", Some(&value)).unwrap(), Some(b"abc".to_vec()));
        let null = kafka_Bytes_t::NULL;
        let value = GenericValue::new(&null as *const kafka_Bytes_t as *mut c_void);
        assert_eq!(BytesPassthrough.serialize("t", Some(&value)).unwrap(), None);
        assert_eq!(BytesPassthrough.serialize("t", None).unwrap(), None);
    }

    /// Stores the message of the error a `void` completion was given.
    unsafe extern "C" fn record_rejection(error: *mut kafka_common_Error_t, opaque: *mut c_void) {
        assert!(!error.is_null(), "a negative timeout is rejected");
        // SAFETY: the completion owns `error`; it is read, then freed once.
        let message = unsafe { c_str_to_string(kafka_common_Error_message(error)) };
        // SAFETY: as above.
        unsafe { kafka_common_Error_destroy(error) };
        // SAFETY: `opaque` is the test's slot, alive until the callbacks ran.
        *unsafe { &*(opaque as *const std::sync::Mutex<Option<String>>) }.lock().unwrap() = Some(message);
    }

    #[test]
    fn producer_is_built_closed_and_destroyed_without_a_broker() {
        let props = box_string_map([("bootstrap.servers", "localhost:1"), ("max.block.ms", "100")]);
        let topic = CString::new("topic").unwrap();
        let bytes = kafka_Bytes_t::from_slice(b"v");
        // SAFETY: Test code: `props`, `config`, `record` and the producer are live from their
        // constructor to their single destroy below, `future` and each error are destroyed once
        // after their last use, `view` is used only before the producer is destroyed, and
        // `rejected` outlives the callback run that writes it.
        unsafe {
            let mut config = std::ptr::null_mut();
            assert!(kafka_producer_ProducerConfig_new(props, &raw mut config).is_null());
            kafka_Map_destroy(props);

            let mut producer = std::ptr::null_mut();
            let error =
                kafka_producer_KafkaProducer_new(config, std::ptr::null_mut(), std::ptr::null_mut(), &raw mut producer);
            assert!(error.is_null());
            kafka_producer_ProducerConfig_destroy(config);

            // A send with no reachable broker either fails on the metadata
            // wait or hands back a future that fails later; both go through
            // the error slot and the future handle.
            let record =
                kafka_producer_ProducerRecord_new(topic.as_ptr(), &bytes as *const kafka_Bytes_t as *const c_void);
            let mut future = std::ptr::null_mut();
            let error = kafka_producer_KafkaProducer_send(producer, record, std::ptr::null(), &raw mut future);
            if error.is_null() {
                assert!(!future.is_null());
                kafka_common_KafkaFuture_destroy(future);
            } else {
                assert!(future.is_null());
                let message = c_str_to_string(kafka_common_Error_message(error));
                assert!(!message.is_empty());
                kafka_common_Error_destroy(error);
            }
            kafka_producer_ProducerRecord_destroy(record);

            let view = kafka_producer_KafkaProducer__as_Producer(producer);
            let rejected = std::sync::Mutex::new(None);
            kafka_producer_Producer_close_with_timeout_cb(
                view,
                -1,
                record_rejection,
                &rejected as *const std::sync::Mutex<Option<String>> as *mut c_void,
            );
            assert_eq!(kafka_producer_Producer__execute_callbacks(view), 1);
            assert_eq!(rejected.lock().unwrap().as_deref(), Some("The timeout cannot be negative."));
            let error = kafka_producer_Producer_close_with_timeout(view, -1);
            assert_eq!(
                c_str_to_string(kafka_common_Error_message(error)),
                "The timeout cannot be negative."
            );
            kafka_common_Error_destroy(error);
            assert!(kafka_producer_Producer_close_with_timeout(view, 1_000).is_null());
            assert_eq!(kafka_producer_Producer__execute_callbacks(view), 0);
            kafka_producer_KafkaProducer_destroy(producer);
            kafka_producer_KafkaProducer_destroy(std::ptr::null_mut());
        }
    }
}
