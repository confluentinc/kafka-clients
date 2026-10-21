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

use std::borrow::Cow;
use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::Arc;

use crate::common::Error;
use crate::common::header::RecordHeaders;
use crate::common::serialization::{DynSerializer, Serializer};
use crate::ffi::common::fail_with;
use crate::ffi::common::serialization::serializer::{
    SerializerInner, kafka_common_serialization_Serializer_t, take_serializer,
};
use crate::ffi::common::{deliver_panic, deliver_panic_value, kafka_common_Error_t};
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
/// and its bytes are the serialized form (`data == NULL` is Java's `null`),
/// borrowed rather than copied: the producer copies them once, into the batch.
pub(crate) struct BytesPassthrough;

impl Serializer<GenericValue> for BytesPassthrough {
    async fn serialize<'a>(
        &'a self,
        _topic: &'a str,
        data: Option<&'a GenericValue>,
    ) -> Result<Option<Cow<'a, [u8]>>, Error> {
        Ok(data.filter(|v| !v.is_null()).and_then(|v| {
            // SAFETY: by contract a `void *` key or value of a producer built without a serializer
            // points at a live `kafka_Bytes_t` whose bytes stay unchanged while the record is
            // borrowed (`kafka_producer_Producer_send_cb`: during the call, or until `cb` fires),
            // which covers `'a`, the borrow of the value itself.
            unsafe { (*(v.as_ptr() as *const kafka_Bytes_t)).as_slice() }.map(Cow::Borrowed)
        }))
    }
}

/// A C serializer shared with its registration: the producer's
/// `Box<dyn DynSerializer>` forwarding to the `Arc` the handle hands out,
/// which also keeps the handle pointer valid for an asynchronous
/// implementation's reports.
pub(crate) struct ArcSerializer(pub(crate) Arc<SerializerInner>);

impl Serializer<GenericValue> for ArcSerializer {
    async fn serialize<'a>(
        &'a self,
        topic: &'a str,
        data: Option<&'a GenericValue>,
    ) -> Result<Option<Cow<'a, [u8]>>, Error> {
        Serializer::serialize(self.0.get(), topic, data).await
    }

    async fn serialize_with_headers<'a, 'h>(
        &'a self,
        topic: &'a str,
        headers: &'h mut RecordHeaders,
        data: Option<&'a GenericValue>,
    ) -> Result<Option<Cow<'a, [u8]>>, Error>
    where
        'a: 'h,
    {
        Serializer::serialize_with_headers(self.0.get(), topic, headers, data).await
    }

    /// The handle is shared, so `configure` goes through the shared form,
    /// which reaches a C implementation as `&mut` would.
    async fn configure(&mut self, configs: &HashMap<String, String>, is_key: bool) {
        self.0.get().configure_shared(configs, is_key).await;
    }

    async fn close(&self) {
        Serializer::close(self.0.get()).await;
    }
}

/// The Rust serializer a `*mut kafka_common_serialization_Serializer_t`
/// parameter stands for (see the module docs), and whether it is a C
/// implementation ([`SerializerInner::is_c`]).
///
/// # Safety
///
/// `serializer` must be null or a live handle; an owned one must not be used
/// again by the caller.
pub(crate) unsafe fn serializer_from(
    serializer: *mut kafka_common_serialization_Serializer_t,
) -> (Box<dyn DynSerializer<GenericValue>>, bool) {
    if serializer.is_null() {
        (Box::new(BytesPassthrough), false)
    } else {
        // SAFETY: Per this function's `# Safety`, `serializer` is a live handle (non-null, checked
        // above), as `take_serializer` requires; an owned one is not used again by the caller, as
        // the `# Safety` states.
        let serializer = unsafe { take_serializer(serializer) };
        let is_c = serializer.is_c();
        (Box::new(ArcSerializer(serializer)), is_c)
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
/// producer gets its own copy of the handle's configuration, with the handle's
/// `client.id` (the producers of one handle share it, as Java's producers
/// given one `ProducerConfig` do), so the configuration stays the caller's and
/// can build further producers; the producer is delivered owned, freed with
/// [`kafka_producer_KafkaProducer_destroy`]. A constructor that fails
/// after taking the serializers closes them, as Java's does
/// (`KafkaProducer.java:519-523`), and returns without waiting for a C
/// serializer's `close` to report: that report may arrive after this
/// returned, on the handle, which stays valid for it.
///
/// # Safety
///
/// `config` must be a live handle, each serializer null or a live handle (an
/// owned one not used again by the caller), `out_err` a valid slot pointing at
/// null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_new(
    config: *const kafka_producer_ProducerConfig_t,
    key_serializer: *mut kafka_common_serialization_Serializer_t,
    value_serializer: *mut kafka_common_serialization_Serializer_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_producer_KafkaProducer_t {
    // The serializers are taken first, so that a configuration error below
    // drops (and so frees) the owned ones the caller handed over.
    // SAFETY: Per this function's `# Safety`, `key_serializer` is null or a live handle, not used
    // again by the caller when owned, as `serializer_from` requires.
    let (key_serializer, key_is_c) = unsafe { serializer_from(key_serializer) };
    // SAFETY: Per this function's `# Safety`, `value_serializer` is null or a live handle, not used
    // again by the caller when owned, as `serializer_from` requires.
    let (value_serializer, value_is_c) = unsafe { serializer_from(value_serializer) };
    // SAFETY: Per this function's `# Safety`, `config` is a live handle; no
    // `kafka_producer_ProducerConfig_set_partitioner` call can run on it meanwhile, since that
    // function's `# Safety` excludes any other call using the handle at the same time, so
    // `producer_config_ref`'s requirement holds for this statement, the only use of the reference.
    let config = match unsafe { producer_config_ref(config) }.build() {
        Ok(config) => config,
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => return unsafe { fail_with(out_err, error) },
    };
    let tries_send_inline = !(key_is_c || value_is_c);
    match ProducerHandle::new(
        // The boxes go in as they are: `new` would box them again (see
        // `with_boxed_serializers`).
        || KafkaProducer::with_boxed_serializers(config, key_serializer, value_serializer),
        true,
        tries_send_inline,
    ) {
        Ok(handle) => Box::into_raw(handle) as *mut kafka_producer_KafkaProducer_t,
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
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
/// `self_` must be a live handle. `out_err` must be a valid slot pointing at
/// null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_init_transactions(
    self_: *const kafka_producer_KafkaProducer_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `producer` requires, so
    // `producer(self_)` is its live `__as_Producer` view, and `out_err` as it requires, which is what
    // `kafka_producer_Producer_init_transactions`'s `# Safety` asks for.
    unsafe { kafka_producer_Producer_init_transactions(producer(self_), out_err) }
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
    // `producer(self_)` is its live `__as_Producer` view, and `out_err` as it requires, which is what
    // `kafka_producer_Producer_init_transactions_cb`'s `# Safety` asks for.
    unsafe { kafka_producer_Producer_init_transactions_cb(producer(self_), cb, opaque) }
}

/// `KafkaProducer.beginTransaction()`: see
/// [`kafka_producer_Producer_begin_transaction`].
///
/// # Safety
///
/// `self_` must be a live handle. `out_err` must be a valid slot pointing at
/// null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_begin_transaction(
    self_: *const kafka_producer_KafkaProducer_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `producer` requires, so
    // `producer(self_)` is its live `__as_Producer` view, which is what
    // `kafka_producer_Producer_begin_transaction`'s `# Safety` asks for.
    unsafe { kafka_producer_Producer_begin_transaction(producer(self_), out_err) }
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
/// handle. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_send_offsets_to_transaction(
    self_: *const kafka_producer_KafkaProducer_t,
    offsets: *const kafka_Map_t,
    group_metadata: *const kafka_consumer_ConsumerGroupMetadata_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `producer` requires, so
    // `producer(self_)` is its live `__as_Producer` view, `offsets` a live map of topic-partition
    // to offset-and-metadata handles that no other call mutates meanwhile and `group_metadata` a
    // live handle, which is what
    // `kafka_producer_Producer_send_offsets_to_transaction`'s `# Safety` asks for.
    unsafe { kafka_producer_Producer_send_offsets_to_transaction(producer(self_), offsets, group_metadata, out_err) }
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
/// `self_` must be a live handle. `out_err` must be a valid slot pointing at
/// null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_commit_transaction(
    self_: *const kafka_producer_KafkaProducer_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `producer` requires, so
    // `producer(self_)` is its live `__as_Producer` view, which is what
    // `kafka_producer_Producer_commit_transaction`'s `# Safety` asks for.
    unsafe { kafka_producer_Producer_commit_transaction(producer(self_), out_err) }
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
/// `self_` must be a live handle. `out_err` must be a valid slot pointing at
/// null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_abort_transaction(
    self_: *const kafka_producer_KafkaProducer_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `producer` requires, so
    // `producer(self_)` is its live `__as_Producer` view, which is what
    // `kafka_producer_Producer_abort_transaction`'s `# Safety` asks for.
    unsafe { kafka_producer_Producer_abort_transaction(producer(self_), out_err) }
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
/// `self_` and `record` must be live handles, `callback` null or a live handle
/// (consumed), `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_send(
    self_: *const kafka_producer_KafkaProducer_t,
    record: *const kafka_producer_ProducerRecord_t,
    callback: *mut kafka_producer_Callback_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `producer` requires, so
    // `producer(self_)` is its live `__as_Producer` view, `record` a live handle, `callback` null
    // or a live handle and `out_err` a valid slot, which is what
    // `kafka_producer_Producer_send_with_callback`'s `# Safety` asks for.
    unsafe { kafka_producer_Producer_send_with_callback(producer(self_), record, callback, out_err) }
}

/// `KafkaProducer.send(ProducerRecord, Callback)`, completed through `cb`:
/// when both serializers are built-in, inside the call for a record the
/// producer takes or refuses at once and later for one it would have to wait
/// for; with a C serializer, later for every record. See
/// [`kafka_producer_Producer_send_with_callback_cb`].
///
/// # Safety
///
/// `self_` and `record` must be live handles, `callback` null or a live
/// handle (consumed).
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
    callback: *mut kafka_producer_Callback_t,
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
        unsafe { Box::from_raw(self_ as *mut Handle) }.destroy();
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::time::{Duration, Instant};

    use super::*;

    /// A C serializer's calls go through `ArcSerializer`, whose futures must
    /// fit `InlineFuture`'s buffer, or every record would box one: for a
    /// built-in and a C implementation (a pending report).
    #[test]
    fn arc_serializer_futures_are_stored_inline() {
        use std::ffi::c_void;

        use crate::common::serialization::InlineFuture;
        use crate::ffi::common::serialization::serializer::{
            FfiSerializer, kafka_common_serialization_Serializer_new_async,
        };
        use crate::ffi::common::serialization::string_serializer::GenericStringSerializer;

        // Never called: the futures are only built.
        unsafe extern "C" fn async_fn(_: *mut c_void, _: *const std::ffi::c_char, _: *const c_void, _: i64) {}

        let built_in = ArcSerializer(SerializerInner::view(Arc::new(FfiSerializer::String(
            GenericStringSerializer::default(),
        ))));
        // SAFETY: Test code: each handle comes from its constructor and is consumed once, here; the
        // producer's `Arc` frees it.
        let asynchronous = unsafe {
            ArcSerializer(take_serializer(kafka_common_serialization_Serializer_new_async(
                std::ptr::null_mut(),
                async_fn,
                None,
                None,
                None,
            )))
        };
        let mut headers = RecordHeaders::new();
        for (kind, serializer) in [("built-in", &built_in), ("C", &asynchronous)] {
            let future = Serializer::serialize(serializer, "t", None);
            assert!(
                InlineFuture::<()>::is_inline_for(&future),
                "{kind} serialize: {} bytes",
                size_of_val(&future)
            );
            let future = Serializer::serialize_with_headers(serializer, "t", &mut headers, None);
            assert!(
                InlineFuture::<()>::is_inline_for(&future),
                "{kind} with_headers: {} bytes",
                size_of_val(&future)
            );
        }
    }
    use crate::ffi::common::{kafka_common_Error_destroy, kafka_common_Error_message};
    use crate::ffi::kafka_future::kafka_common_KafkaFuture_destroy;
    use crate::ffi::producer::producer_config::{
        kafka_producer_ProducerConfig_destroy, kafka_producer_ProducerConfig_new,
    };
    use crate::ffi::producer::producer_record::{
        kafka_producer_ProducerRecord_destroy, kafka_producer_ProducerRecord_new,
    };
    use crate::ffi::producer::{
        client_ref, kafka_producer_Producer__execute_callbacks, kafka_producer_Producer_close,
        kafka_producer_Producer_close_cb, kafka_producer_Producer_close_with_timeout,
        kafka_producer_Producer_close_with_timeout_cb, kafka_producer_Producer_flush, kafka_producer_Producer_send_cb,
    };
    use crate::ffi::util::{box_string_map, c_str_to_string, kafka_Map_destroy};

    /// An owned serializer is the producer's from the call on, so a
    /// configuration the constructor cannot build frees it rather than
    /// leaking it.
    #[test]
    fn a_configuration_error_frees_the_owned_serializers() {
        let imp = Arc::new(crate::ffi::common::serialization::serializer::FfiSerializer::ByteArray(
            Default::default(),
        ));
        let props = [("bootstrap.servers".to_string(), String::new())].into_iter().collect();
        let config = crate::ffi::producer::producer_config::ProducerConfigInner::unvalidated_for_test(props);
        // SAFETY: Test code: `config` and the two owned serializers are live handles, the
        // serializers handed over to the constructor and not used again, `err` is a valid slot
        // pointing at null, and `config` and the error are destroyed once below.
        unsafe {
            let key = crate::ffi::common::serialization::serializer::SerializerInner::owned(Arc::clone(&imp));
            let value = crate::ffi::common::serialization::serializer::SerializerInner::owned(Arc::clone(&imp));
            assert_eq!(Arc::strong_count(&imp), 3);
            let mut err = std::ptr::null_mut();
            let producer = kafka_producer_KafkaProducer_new(config, key, value, &raw mut err);
            assert!(!err.is_null(), "the empty bootstrap.servers is rejected");
            assert!(producer.is_null());
            kafka_common_Error_destroy(err);
            kafka_producer_ProducerConfig_destroy(config);
        }
        assert_eq!(Arc::strong_count(&imp), 1, "both owned serializers were freed");
    }

    /// `send_cb` tries the producer on the calling thread only when neither
    /// serializer is a C implementation (COMMENTS.1 Issue 1): a `NULL` handle
    /// and a built-in class keep the fast path, a C implementation sends
    /// every record through the submission task.
    #[test]
    fn send_cb_tries_inline_only_without_a_c_serializer() {
        use crate::ffi::common::serialization::serializer::kafka_common_serialization_Serializer_new_async;
        use crate::ffi::common::serialization::string_serializer::{
            kafka_common_serialization_StringSerializer__as_Serializer,
            kafka_common_serialization_StringSerializer_destroy, kafka_common_serialization_StringSerializer_new,
        };

        // Never called: no record is sent.
        unsafe extern "C" fn async_fn(_: *mut c_void, _: *const std::ffi::c_char, _: *const c_void, _: i64) {}

        let props = box_string_map([("bootstrap.servers", "127.0.0.1:1")]);
        // SAFETY: Test code: `err` is a valid slot pointing at null for each call (no call fails),
        // `props`, `config`, the string serializer and each producer are live from their
        // constructor to their single destroy below, each owned C serializer is handed over to
        // one constructor and not used again, and each view is read only before its producer is
        // destroyed.
        unsafe {
            let mut err = std::ptr::null_mut();
            let config = kafka_producer_ProducerConfig_new(props, &raw mut err);
            assert!(err.is_null());
            kafka_Map_destroy(props);
            let string = kafka_common_serialization_StringSerializer_new();
            let built_in = kafka_common_serialization_StringSerializer__as_Serializer(string);
            let cases = [
                ("NULL", std::ptr::null_mut(), std::ptr::null_mut(), true),
                ("built-in", built_in, built_in, true),
                (
                    "C key",
                    kafka_common_serialization_Serializer_new_async(std::ptr::null_mut(), async_fn, None, None, None),
                    std::ptr::null_mut(),
                    false,
                ),
                (
                    "C value",
                    built_in,
                    kafka_common_serialization_Serializer_new_async(std::ptr::null_mut(), async_fn, None, None, None),
                    false,
                ),
            ];
            for (kind, key, value, inline) in cases {
                let producer = kafka_producer_KafkaProducer_new(config, key, value, &raw mut err);
                assert!(err.is_null());
                let view = kafka_producer_KafkaProducer__as_Producer(producer);
                assert_eq!(client_ref(view).tries_send_inline, inline, "{kind}");
                kafka_producer_KafkaProducer_destroy(producer);
            }
            kafka_common_serialization_StringSerializer_destroy(string);
            kafka_producer_ProducerConfig_destroy(config);
        }
    }

    /// A producer created from a handle runs under the handle's resolved
    /// `client.id`, the one `_get_string` answers, so the producers of one
    /// handle share it, as Java's producers given one `ProducerConfig` do;
    /// the next handle takes the next number from the sequence, as Java's
    /// next `ProducerConfig` does (`producer-1`, then `producer-2`).
    #[test]
    fn producers_run_under_the_client_id_of_their_handle() {
        use crate::ffi::producer::producer_config::kafka_producer_ProducerConfig_get_string;
        // SAFETY: Test code: `producer` is a live class handle; the view is used only within this
        // call, while the handle lives.
        let client_ids = |producer: *mut kafka_producer_KafkaProducer_t| unsafe {
            client_ref(kafka_producer_KafkaProducer__as_Producer(producer))
                .producer()
                .expect("not destroyed")
                .metrics()
                .keys()
                .filter_map(|name| name.tags().get("client-id").cloned())
                .collect::<std::collections::HashSet<_>>()
        };
        let props = box_string_map([("bootstrap.servers", "127.0.0.1:1")]);
        // SAFETY: Test code: the map, both configs and every producer handle are live until they
        // are destroyed once below; the strings `_get_string` lends are copied before their
        // handle is destroyed; `err` is a valid slot pointing at null before each call.
        unsafe {
            let mut err = std::ptr::null_mut();
            let mut handle_ids = Vec::new();
            let mut producers = Vec::new();
            for _ in 0..2 {
                let config = kafka_producer_ProducerConfig_new(props, &raw mut err);
                assert!(err.is_null());
                let id = kafka_producer_ProducerConfig_get_string(config, c"client.id".as_ptr(), &raw mut err);
                assert!(err.is_null());
                let id = c_str_to_string(id);
                for _ in 0..2 {
                    let producer = kafka_producer_KafkaProducer_new(
                        config,
                        std::ptr::null_mut(),
                        std::ptr::null_mut(),
                        &raw mut err,
                    );
                    assert!(err.is_null());
                    assert_eq!(client_ids(producer), std::collections::HashSet::from([id.clone()]));
                    producers.push(producer);
                }
                kafka_producer_ProducerConfig_destroy(config);
                handle_ids.push(id);
            }
            kafka_Map_destroy(props);
            let numbers: Vec<i32> = handle_ids
                .iter()
                .map(|id| id.strip_prefix("producer-").unwrap().parse().unwrap())
                .collect();
            // Other tests take numbers concurrently, so the next one is only later.
            assert!(numbers[1] > numbers[0], "{handle_ids:?}");
            for producer in producers {
                kafka_producer_KafkaProducer_destroy(producer);
            }
        }
    }

    /// A `KafkaProducer` that can never fetch metadata, so a send waits out
    /// `max.block.ms` (`max_block_ms`, in milliseconds) for it.
    fn unreachable_producer(max_block_ms: &str) -> *mut kafka_producer_KafkaProducer_t {
        let props = box_string_map([("bootstrap.servers", "127.0.0.1:1"), ("max.block.ms", max_block_ms)]);
        // SAFETY: Test code: `err` is a valid slot pointing at null for each call (no call fails),
        // `props` and `config` are live from their constructor to their single destroy below; the
        // producer is handed to the caller, who destroys it.
        unsafe {
            let mut err = std::ptr::null_mut();
            let config = kafka_producer_ProducerConfig_new(props, &raw mut err);
            assert!(err.is_null());
            kafka_Map_destroy(props);
            let producer =
                kafka_producer_KafkaProducer_new(config, std::ptr::null_mut(), std::ptr::null_mut(), &raw mut err);
            assert!(err.is_null());
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
    /// behind `send_cb` records the submission task has not reached yet
    /// returns within its timeout, drain included, and the records then fail
    /// as sent after close. Both forms. The test parks the submission task
    /// (`Client::submission_gate`): no record it reached could hold it up, one
    /// that has to wait for metadata waiting on a task of its own.
    #[test]
    fn a_timed_close_bounds_the_drain_of_the_queued_sends() {
        let topic = CString::new("topic").unwrap();
        let bytes = kafka_Bytes_t::from_slice(b"v");
        for queued in [false, true] {
            let sends = std::sync::Mutex::new(Vec::<Option<String>>::new());
            let sends_opaque = &sends as *const std::sync::Mutex<Vec<Option<String>>> as *mut c_void;
            // SAFETY: Test code: the producer and `record` are live from their constructor to their
            // single destroy below, `view` is used only before the producer is destroyed, the gate
            // guard borrows the test's own `client` clone and is dropped before the destroy, and
            // `sends` and `closed` outlive the callback runs that write them.
            unsafe {
                let producer = unreachable_producer("60000");
                let view = kafka_producer_KafkaProducer__as_Producer(producer);
                let client = Arc::clone(client_ref(view));
                let record =
                    kafka_producer_ProducerRecord_new(topic.as_ptr(), &bytes as *const kafka_Bytes_t as *const c_void);
                let gate = client.submission_gate.try_lock().expect("the gate is free");
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
                        assert!(start.elapsed() < std::time::Duration::from_secs(10), "{:?}", start.elapsed());
                        kafka_producer_Producer__execute_callbacks(view);
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    assert_eq!(*closed.lock().unwrap(), [None]);
                } else {
                    let mut err = std::ptr::null_mut();
                    kafka_producer_Producer_close_with_timeout(view, 500, &raw mut err);
                    assert!(err.is_null());
                }
                let elapsed = start.elapsed();
                assert!(
                    elapsed >= std::time::Duration::from_millis(500),
                    "the drain gave up early: {elapsed:?}"
                );
                assert!(elapsed < std::time::Duration::from_secs(10), "{elapsed:?}");
                assert!(sends.lock().unwrap().is_empty(), "the task has not reached the records");

                drop(gate);
                while sends.lock().unwrap().len() < 3 {
                    assert!(start.elapsed() < std::time::Duration::from_secs(10), "{:?}", start.elapsed());
                    kafka_producer_Producer__execute_callbacks(view);
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                let closed = Some("Cannot perform operation after producer has been closed".to_string());
                assert_eq!(*sends.lock().unwrap(), [closed.clone(), closed.clone(), closed]);
                kafka_producer_KafkaProducer_destroy(producer);
                kafka_producer_ProducerRecord_destroy(record);
            }
        }
    }

    /// Whether `message` is the refusal of a record that waited inside the
    /// producer when it closed: "Producer closed while send in progress" when
    /// the close woke its wait, or Java's closed-producer refusal when its
    /// task reached the producer only after the close.
    fn is_closed_producer_refusal(message: &str) -> bool {
        message == "Producer closed while send in progress"
            || message == "Cannot perform operation after producer has been closed"
    }

    /// The tasks the producer behind `view` registered and that have not
    /// ended: its `_cb` tasks and the waiters of the records the submission
    /// task left waiting (`ffi::producer::send_waiter`).
    ///
    /// # Safety
    ///
    /// `view` must be a live view.
    unsafe fn live_tasks(view: *const kafka_producer_Producer_t) -> usize {
        // SAFETY: Per this function's `# Safety`, `view` is a live view, as `client_ref` requires;
        // the reference is used only within this call.
        let client = unsafe { client_ref(view) };
        client.tasks.lock().unwrap().iter().filter(|task| !task.is_finished()).count()
    }

    /// Waits until `done` holds, failing after ten seconds with `what`.
    fn wait_until(what: &str, done: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !done() {
            assert!(Instant::now() < deadline, "{what}");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Records that have to wait for metadata wait side by side, as Java
    /// threads blocked in `send` do, not one after another on the submission
    /// task: two `send_cb` records, each waiting out a `max.block.ms` of
    /// 1000 ms, are both done after about one such wait. The metadata timeout
    /// is an `ApiException`, so each `cb` gets the future, failed with it, as
    /// Java's `send` returns one.
    #[test]
    fn records_waiting_for_metadata_wait_side_by_side() {
        let topic = CString::new("topic").unwrap();
        let bytes = kafka_Bytes_t::from_slice(b"v");
        let sends = std::sync::Mutex::new(Vec::<Option<String>>::new());
        // SAFETY: Test code: `err` is a valid slot pointing at null, the producer and `record` are
        // live from their constructor to their single destroy below, `view` is used only before
        // the producer is destroyed, and `sends` outlives the callback runs that write it.
        unsafe {
            let producer = unreachable_producer("1000");
            let view = kafka_producer_KafkaProducer__as_Producer(producer);
            let record =
                kafka_producer_ProducerRecord_new(topic.as_ptr(), &bytes as *const kafka_Bytes_t as *const c_void);
            let started = Instant::now();
            for _ in 0..2 {
                kafka_producer_Producer_send_cb(view, record, record_send, &sends as *const _ as *mut c_void);
            }
            while sends.lock().unwrap().len() < 2 {
                assert!(started.elapsed() < Duration::from_secs(10), "{:?}", started.elapsed());
                kafka_producer_Producer__execute_callbacks(view);
                std::thread::sleep(Duration::from_millis(1));
            }
            let elapsed = started.elapsed();
            assert!(
                elapsed >= Duration::from_millis(1000),
                "each waited out max.block.ms: {elapsed:?}"
            );
            assert!(
                elapsed < Duration::from_millis(1900),
                "two waits of max.block.ms=1000 overlap, not stack: {elapsed:?}"
            );
            assert_eq!(*sends.lock().unwrap(), [None, None], "each got its future");

            let mut err = std::ptr::null_mut();
            kafka_producer_Producer_close(view, &raw mut err);
            assert!(err.is_null());
            kafka_producer_KafkaProducer_destroy(producer);
            kafka_producer_ProducerRecord_destroy(record);
        }
    }

    /// `flush`, and every transaction-control operation, which shares its
    /// drain, does not wait for a record waiting inside the producer on a
    /// task of its own: that is a `send` still in progress, which Java's
    /// `flush()` does not wait for either. `close` then fails the record, as
    /// Java's does, rather than leave it waiting out `max.block.ms`.
    #[test]
    fn a_flush_does_not_wait_for_a_record_waiting_for_metadata() {
        let topic = CString::new("topic").unwrap();
        let bytes = kafka_Bytes_t::from_slice(b"v");
        let sends = std::sync::Mutex::new(Vec::<Option<String>>::new());
        // SAFETY: Test code: `err` is a valid slot pointing at null before each call, the producer
        // and `record` are live from their constructor to their single destroy below, `view` is
        // used only before the producer is destroyed, and `sends` outlives the callback runs that
        // write it.
        unsafe {
            let producer = unreachable_producer("60000");
            let view = kafka_producer_KafkaProducer__as_Producer(producer);
            let record =
                kafka_producer_ProducerRecord_new(topic.as_ptr(), &bytes as *const kafka_Bytes_t as *const c_void);
            kafka_producer_Producer_send_cb(view, record, record_send, &sends as *const _ as *mut c_void);
            wait_until("the record waits on a task of its own", || live_tasks(view) == 1);
            // Inside its `send`, waiting for the topic's metadata.
            std::thread::sleep(Duration::from_millis(100));

            let started = Instant::now();
            let mut err = std::ptr::null_mut();
            kafka_producer_Producer_flush(view, &raw mut err);
            assert!(err.is_null());
            let elapsed = started.elapsed();
            assert!(elapsed < Duration::from_secs(5), "flush waited for the record: {elapsed:?}");
            assert!(sends.lock().unwrap().is_empty(), "the record is still waiting");
            assert_eq!(live_tasks(view), 1);

            kafka_producer_Producer_close(view, &raw mut err);
            assert!(err.is_null());
            wait_until("the close failed the record", || {
                kafka_producer_Producer__execute_callbacks(view);
                !sends.lock().unwrap().is_empty()
            });
            assert!(started.elapsed() < Duration::from_secs(10), "{:?}", started.elapsed());
            let sends = sends.lock().unwrap().clone();
            assert_eq!(sends.len(), 1);
            let message = sends[0].clone().expect("the close refused the record");
            assert!(is_closed_producer_refusal(&message), "{message}");
            wait_until("the waiter ended", || live_tasks(view) == 0);
            kafka_producer_KafkaProducer_destroy(producer);
            kafka_producer_ProducerRecord_destroy(record);
        }
    }

    /// `_destroy` waits for the records the submission task leaves waiting on
    /// tasks of their own while it hands over the last ones queued, after
    /// `_destroy` stopped it, so each still reports, once. The task is parked
    /// (`Client::submission_gate`) when `_destroy` starts, so the record
    /// reaches the producer only once the queue is closed: without that last
    /// wait the runtime's shutdown would drop its task and `cb` never fired.
    #[test]
    fn destroy_waits_for_a_record_left_waiting_as_the_submission_task_stops() {
        let topic = CString::new("topic").unwrap();
        let bytes = kafka_Bytes_t::from_slice(b"v");
        let sends = std::sync::Mutex::new(Vec::<Option<String>>::new());
        // SAFETY: Test code: the producer and `record` are live from their constructor to their
        // single destroy below, on the helper thread, which is joined before `record` is
        // destroyed; `view` is used only before that destroy starts; the gate guard borrows the
        // test's own `client` clone, which keeps the client alive; and `sends` outlives the
        // destroy that runs its callback.
        unsafe {
            let producer = unreachable_producer("500");
            let view = kafka_producer_KafkaProducer__as_Producer(producer);
            let client = Arc::clone(client_ref(view));
            let record =
                kafka_producer_ProducerRecord_new(topic.as_ptr(), &bytes as *const kafka_Bytes_t as *const c_void);
            let gate = client.submission_gate.try_lock().expect("the gate is free");
            kafka_producer_Producer_send_cb(view, record, record_send, &sends as *const _ as *mut c_void);
            let handle = producer as usize;
            std::thread::scope(|scope| {
                let destroy = scope.spawn(move || kafka_producer_KafkaProducer_destroy(handle as *mut _));
                // `_destroy` found no task to wait for and joins the parked submission task.
                std::thread::sleep(Duration::from_millis(100));
                assert!(!destroy.is_finished());
                drop(gate);
                destroy.join().unwrap();
            });
            assert_eq!(
                *sends.lock().unwrap(),
                [None],
                "`cb` fired once, with the future failed on the metadata timeout"
            );
            kafka_producer_ProducerRecord_destroy(record);
        }
    }

    /// A forced `close_with_timeout_cb(0)` queued behind an untimed
    /// `close_cb`, with `_destroy` right after it (what Python's cancelled
    /// async `close()` does): the record waiting for metadata fails with the
    /// close and `_destroy` returns promptly, each completion firing once.
    /// `_destroy` used to close the submission queue first, which failed the
    /// forced close's drain before it closed the producer, so while the
    /// untimed close waited for that record's handover, `_destroy` waited out
    /// `max.block.ms` (60 s here). The record now waits on a task of its own,
    /// which neither close waits for.
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
            let producer = unreachable_producer("60000");
            let view = kafka_producer_KafkaProducer__as_Producer(producer);
            let record =
                kafka_producer_ProducerRecord_new(topic.as_ptr(), &bytes as *const kafka_Bytes_t as *const c_void);
            kafka_producer_Producer_send_cb(view, record, record_send, &sends as *const _ as *mut c_void);
            kafka_producer_Producer_close_cb(view, record_void, &closes as *const _ as *mut c_void);
            // The send waits for the topic's metadata, on a task of its own.
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

    /// What a delivery callback of [`a_delivery_callback_cannot_flush_and_closes_without_blocking`]
    /// saw: the producer view it calls back into and each outcome.
    struct InCallback {
        view: *const kafka_producer_Producer_t,
        outcomes: std::sync::Mutex<Vec<Option<String>>>,
    }

    /// Flushes (both forms) and closes its own producer from inside its
    /// delivery callback, then reports.
    unsafe extern "C" fn flush_and_close_from_the_callback(
        self_: *mut c_void,
        _metadata: *const crate::ffi::producer::record_metadata::kafka_producer_RecordMetadata_t,
        _error: *const kafka_common_Error_t,
        callback_id: i64,
    ) {
        // SAFETY: Test code: `self_` is the test's `InCallback`, alive until the producer is
        // destroyed.
        let ctx = unsafe { &*(self_ as *const InCallback) };
        let opaque = &ctx.outcomes as *const std::sync::Mutex<Vec<Option<String>>> as *mut c_void;
        // SAFETY: Test code: `ctx.view` is the live view of the producer whose callback this is;
        // each `err` is a valid slot pointing at null, and the error it may get is read, then
        // freed once, by `record_void`.
        unsafe {
            let mut err = std::ptr::null_mut();
            crate::ffi::producer::kafka_producer_Producer_flush(ctx.view, &raw mut err);
            record_void(err, opaque);
            crate::ffi::producer::kafka_producer_Producer_flush_cb(ctx.view, record_void, opaque);
            let start = std::time::Instant::now();
            let mut err = std::ptr::null_mut();
            kafka_producer_Producer_close_with_timeout(ctx.view, 60_000, &raw mut err);
            record_void(err, opaque);
            assert!(
                start.elapsed() < std::time::Duration::from_secs(10),
                "the close timeout was overridden"
            );
            crate::ffi::producer::kafka_producer_Producer__set_callback_result(
                ctx.view,
                callback_id,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
        }
    }

    /// From inside a delivery callback `flush()` fails as Java's does
    /// (`KafkaProducer.java:1217-1220`), and `close(Duration)` closes as for
    /// a zero timeout without waiting for the report of the callback it runs
    /// in (`KafkaProducer.java:1399-1403`, `:1426-1437`).
    #[test]
    fn a_delivery_callback_cannot_flush_and_closes_without_blocking() {
        let props = box_string_map([("bootstrap.servers", "127.0.0.1:1"), ("max.block.ms", "100")]);
        let topic = CString::new("topic").unwrap();
        let bytes = kafka_Bytes_t::from_slice(b"v");
        // SAFETY: Test code: `err` is a valid slot pointing at null before each call (only the last
        // call fails), `props`, `config`, `record`, `callback` and the producer are live from their
        // constructor to their single destroy below, `view` is used only before the producer is
        // destroyed, the future and the error are destroyed once after their last use, and `ctx`
        // outlives the callback runs that read it.
        unsafe {
            let mut err = std::ptr::null_mut();
            let config = kafka_producer_ProducerConfig_new(props, &raw mut err);
            assert!(err.is_null());
            kafka_Map_destroy(props);
            let producer =
                kafka_producer_KafkaProducer_new(config, std::ptr::null_mut(), std::ptr::null_mut(), &raw mut err);
            assert!(err.is_null());
            kafka_producer_ProducerConfig_destroy(config);
            let view = kafka_producer_KafkaProducer__as_Producer(producer);
            let ctx = InCallback { view, outcomes: std::sync::Mutex::new(Vec::new()) };
            let callback = crate::ffi::producer::callback::kafka_producer_Callback_new(
                &ctx as *const InCallback as *mut c_void,
                flush_and_close_from_the_callback,
            );
            let record =
                kafka_producer_ProducerRecord_new(topic.as_ptr(), &bytes as *const kafka_Bytes_t as *const c_void);
            // The metadata wait times out after `max.block.ms`, an `ApiException`, so the callback
            // is invoked and the send hands back a future.
            let future =
                crate::ffi::producer::kafka_producer_Producer_send_with_callback(view, record, callback, &raw mut err);
            assert!(err.is_null());
            assert_eq!(crate::ffi::kafka_future::kafka_common_KafkaFuture_is_done(future), 0);
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while ctx.outcomes.lock().unwrap().len() < 3 {
                assert!(std::time::Instant::now() < deadline, "{:?}", ctx.outcomes.lock().unwrap());
                kafka_producer_Producer__execute_callbacks(view);
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            let flush_error = Some(
                "KafkaProducer.flush() invocation inside a callback is not permitted because it may lead to deadlock."
                    .to_string(),
            );
            let outcomes = ctx.outcomes.lock().unwrap().clone();
            assert_eq!(outcomes[0], flush_error, "the blocking flush");
            assert!(outcomes.contains(&None), "the close succeeded: {outcomes:?}");
            assert_eq!(
                outcomes.iter().filter(|outcome| **outcome == flush_error).count(),
                2,
                "flush_cb too"
            );
            assert_eq!(
                crate::ffi::kafka_future::kafka_common_KafkaFuture_is_done(future),
                1,
                "reported"
            );
            let value = crate::ffi::kafka_future::kafka_common_KafkaFuture_get(future, &raw mut err);
            assert!(!err.is_null(), "the record's own failure");
            assert!(value.is_null());
            kafka_common_Error_destroy(err);
            kafka_common_KafkaFuture_destroy(future);
            kafka_producer_KafkaProducer_destroy(producer);
            kafka_producer_ProducerRecord_destroy(record);
        }
    }

    #[test]
    fn passthrough_borrows_bytes_and_null_means_null() {
        use crate::ffi::common::serialization::ready;
        let bytes = kafka_Bytes_t::from_slice(b"abc");
        let value = GenericValue::new(&bytes as *const kafka_Bytes_t as *mut c_void);
        match ready(Serializer::serialize(&BytesPassthrough, "t", Some(&value)))
            .unwrap()
            .unwrap()
        {
            Some(Cow::Borrowed(out)) => {
                assert_eq!(out, b"abc");
                assert_eq!(out.as_ptr(), bytes.data, "the caller's bytes, not a copy");
            },
            other => panic!("expected borrowed bytes, got {other:?}"),
        }
        let null = kafka_Bytes_t::NULL;
        let value = GenericValue::new(&null as *const kafka_Bytes_t as *mut c_void);
        assert_eq!(
            ready(Serializer::serialize(&BytesPassthrough, "t", Some(&value)))
                .unwrap()
                .unwrap(),
            None
        );
        assert_eq!(
            ready(Serializer::serialize(&BytesPassthrough, "t", None)).unwrap().unwrap(),
            None
        );
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
        // SAFETY: Test code: `err` is a valid slot pointing at null before each call (reset after
        // each error is destroyed), `props`, `config`, `record` and the producer are live from
        // their constructor to their single destroy below, `future` and each error are destroyed
        // once after their last use, `view` is used only before the producer is destroyed, and
        // `rejected` outlives the callback run that writes it.
        unsafe {
            let mut err = std::ptr::null_mut();
            let config = kafka_producer_ProducerConfig_new(props, &raw mut err);
            assert!(err.is_null());
            kafka_Map_destroy(props);

            let producer =
                kafka_producer_KafkaProducer_new(config, std::ptr::null_mut(), std::ptr::null_mut(), &raw mut err);
            assert!(err.is_null());
            kafka_producer_ProducerConfig_destroy(config);

            // A send with no reachable broker either fails on the metadata
            // wait or hands back a future that fails later; both go through
            // the error slot and the future handle.
            let record =
                kafka_producer_ProducerRecord_new(topic.as_ptr(), &bytes as *const kafka_Bytes_t as *const c_void);
            let future = kafka_producer_KafkaProducer_send(producer, record, std::ptr::null_mut(), &raw mut err);
            if err.is_null() {
                assert!(!future.is_null());
                kafka_common_KafkaFuture_destroy(future);
            } else {
                assert!(future.is_null());
                let message = c_str_to_string(kafka_common_Error_message(err));
                assert!(!message.is_empty());
                kafka_common_Error_destroy(err);
                err = std::ptr::null_mut();
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
            kafka_producer_Producer_close_with_timeout(view, -1, &raw mut err);
            assert_eq!(
                c_str_to_string(kafka_common_Error_message(err)),
                "The timeout cannot be negative."
            );
            kafka_common_Error_destroy(err);
            err = std::ptr::null_mut();
            kafka_producer_Producer_close_with_timeout(view, 1_000, &raw mut err);
            assert!(err.is_null());
            assert_eq!(kafka_producer_Producer__execute_callbacks(view), 0);
            kafka_producer_KafkaProducer_destroy(producer);
            kafka_producer_KafkaProducer_destroy(std::ptr::null_mut());
        }
    }

    /// `send_cb` on a producer that would have to wait — here on metadata it
    /// can never fetch — returns at once without firing `cb`: the record was
    /// queued, and `cb` fires later, through the callback vector, with the
    /// future `send` would have returned (failed on the metadata timeout).
    #[test]
    fn send_cb_queues_a_record_the_producer_cannot_take_at_once() {
        let topic = CString::new("topic").unwrap();
        let bytes = kafka_Bytes_t::from_slice(b"v");
        let seen = std::sync::Mutex::new(Vec::new());
        // SAFETY: Test code: `err` is a valid slot pointing at null before each call, `record` and
        // the producer are live from their constructor to their single destroy below, `view` is
        // used only before the producer is destroyed, `bytes` outlives the callback that reads the
        // record, and `seen` outlives the callback run that writes it.
        unsafe {
            let producer = unreachable_producer("1000");
            let view = kafka_producer_KafkaProducer__as_Producer(producer);
            let record =
                kafka_producer_ProducerRecord_new(topic.as_ptr(), &bytes as *const kafka_Bytes_t as *const c_void);
            let mut err = std::ptr::null_mut();
            let started = Instant::now();
            kafka_producer_Producer_send_cb(
                view,
                record,
                record_send,
                &seen as *const std::sync::Mutex<Vec<Option<String>>> as *mut c_void,
            );
            let elapsed = started.elapsed();
            assert!(elapsed < Duration::from_millis(500), "did not wait on metadata: {elapsed:?}");
            assert!(seen.lock().unwrap().is_empty(), "queued: `cb` did not fire inside the call");
            // The submission task tries it again and, the metadata still
            // missing, leaves it waiting on a task of its own.
            wait_until("the record waits on a task of its own", || live_tasks(view) == 1);

            // That task's `send` waits out `max.block.ms`, then delivers `cb`
            // a future failed on the metadata timeout.
            while seen.lock().unwrap().is_empty() {
                kafka_producer_Producer__execute_callbacks(view);
                std::thread::yield_now();
            }
            assert_eq!(*seen.lock().unwrap(), [None]);
            wait_until("the waiter ended", || live_tasks(view) == 0);
            kafka_producer_ProducerRecord_destroy(record);

            kafka_producer_Producer_close(view, &raw mut err);
            assert!(err.is_null());
            kafka_producer_KafkaProducer_destroy(producer);
            assert_eq!(seen.lock().unwrap().len(), 1, "`cb` fired once");
        }
    }

    /// A send the submission channel refuses (its task gone) completes
    /// during the call: `cb` runs on the calling thread before `send_cb`
    /// returns, and nothing is queued, so a caller holding its interpreter
    /// lock across the call never fires the notify hook from inside it. The
    /// record is one the producer would have waited for, on metadata it can
    /// never fetch: one taken or refused at once completes during the call
    /// anyway, before the channel is asked.
    #[test]
    fn a_send_the_submission_channel_refuses_fires_its_completion_on_the_calling_thread_before_returning() {
        let topic = CString::new("topic").unwrap();
        let bytes = kafka_Bytes_t::from_slice(b"v");
        let seen = std::sync::Mutex::new(Vec::new());
        // SAFETY: Test code: `err` is a valid slot pointing at null, `record` and the producer are
        // live from their constructor to their single destroy below, `view` is used only before
        // the producer is destroyed, `bytes` outlives the call that reads the record, and `seen`
        // outlives the one completion that writes it.
        unsafe {
            let producer = unreachable_producer("1000");
            let view = kafka_producer_KafkaProducer__as_Producer(producer);
            let record =
                kafka_producer_ProducerRecord_new(topic.as_ptr(), &bytes as *const kafka_Bytes_t as *const c_void);
            drop(client_ref(view).submit_tx.lock().unwrap().take());
            kafka_producer_Producer_send_cb(
                view,
                record,
                record_send,
                &seen as *const std::sync::Mutex<Vec<Option<String>>> as *mut c_void,
            );
            assert_eq!(
                *seen.lock().unwrap(),
                [Some(
                    "Cannot perform operation after producer has been closed".to_owned()
                )],
                "fired inside the call"
            );
            assert_eq!(kafka_producer_Producer__execute_callbacks(view), 0, "nothing was queued");
            kafka_producer_ProducerRecord_destroy(record);

            let mut err = std::ptr::null_mut();
            kafka_producer_Producer_close(view, &raw mut err);
            assert!(err.is_null());
            kafka_producer_KafkaProducer_destroy(producer);
            assert_eq!(seen.lock().unwrap().len(), 1, "not a second time on destroy");
        }
    }
}
