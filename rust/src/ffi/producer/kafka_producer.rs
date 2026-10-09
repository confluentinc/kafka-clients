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
use crate::ffi::common::{box_error, kafka_common_Error_t};
use crate::ffi::consumer::kafka_consumer_ConsumerGroupMetadata_t;
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::producer::callback::kafka_producer_Callback_t;
use crate::ffi::producer::producer_config::{kafka_producer_ProducerConfig_t, producer_config_ref};
use crate::ffi::producer::producer_record::kafka_producer_ProducerRecord_t;
use crate::ffi::producer::{
    ProducerHandle, kafka_producer_Producer_abort_transaction, kafka_producer_Producer_abort_transaction_cb,
    kafka_producer_Producer_begin_transaction, kafka_producer_Producer_commit_transaction,
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
/// `serializer` must be null or a live handle.
pub(crate) unsafe fn serializer_from(
    serializer: *mut kafka_common_serialization_Serializer_t,
) -> Box<dyn Serializer<GenericValue> + Send + Sync> {
    if serializer.is_null() {
        Box::new(BytesPassthrough)
    } else {
        Box::new(ArcSerializer(unsafe { take_serializer(serializer) }))
    }
}

unsafe fn handle<'a>(self_: *const kafka_producer_KafkaProducer_t) -> &'a Handle {
    unsafe { &*(self_ as *const Handle) }
}

unsafe fn producer(self_: *const kafka_producer_KafkaProducer_t) -> *const kafka_producer_Producer_t {
    unsafe { handle(self_) }.as_producer()
}

/// `new KafkaProducer(ProducerConfig, Serializer<K>, Serializer<V>)`: the
/// configuration stays the caller's; the producer is delivered owned, freed
/// with [`kafka_producer_KafkaProducer_destroy`].
///
/// # Safety
///
/// `config` must be a live handle, each serializer null or a live handle,
/// `out_new` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_new(
    config: *const kafka_producer_ProducerConfig_t,
    key_serializer: *mut kafka_common_serialization_Serializer_t,
    value_serializer: *mut kafka_common_serialization_Serializer_t,
    out_new: *mut *mut kafka_producer_KafkaProducer_t,
) -> *mut kafka_common_Error_t {
    let config = match unsafe { producer_config_ref(config) }.build() {
        Ok(config) => config,
        Err(error) => return box_error(error),
    };
    let key_serializer = unsafe { serializer_from(key_serializer) };
    let value_serializer = unsafe { serializer_from(value_serializer) };
    match ProducerHandle::new(|| KafkaProducer::new(config, key_serializer, value_serializer)) {
        Ok(handle) => {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer__as_Producer(
    self_: *const kafka_producer_KafkaProducer_t,
) -> *const kafka_producer_Producer_t {
    unsafe { producer(self_) }
}

/// `KafkaProducer.initTransactions()`: see
/// [`kafka_producer_Producer_init_transactions`].
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_init_transactions(
    self_: *const kafka_producer_KafkaProducer_t,
) -> *mut kafka_common_Error_t {
    unsafe { kafka_producer_Producer_init_transactions(producer(self_)) }
}

/// `KafkaProducer.initTransactions()`, completion queued.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_init_transactions_cb(
    self_: *const kafka_producer_KafkaProducer_t,
    cb: kafka_producer_KafkaProducer_init_transactions_cb_t,
    opaque: *mut c_void,
) {
    unsafe { kafka_producer_Producer_init_transactions_cb(producer(self_), cb, opaque) }
}

/// `KafkaProducer.beginTransaction()`: see
/// [`kafka_producer_Producer_begin_transaction`].
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_begin_transaction(
    self_: *const kafka_producer_KafkaProducer_t,
) -> *mut kafka_common_Error_t {
    unsafe { kafka_producer_Producer_begin_transaction(producer(self_)) }
}

/// `KafkaProducer.sendOffsetsToTransaction(...)`: see
/// [`kafka_producer_Producer_send_offsets_to_transaction`].
///
/// # Safety
///
/// `self_`, `offsets` and `group_metadata` must be live handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_send_offsets_to_transaction(
    self_: *const kafka_producer_KafkaProducer_t,
    offsets: *const kafka_Map_t,
    group_metadata: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> *mut kafka_common_Error_t {
    unsafe { kafka_producer_Producer_send_offsets_to_transaction(producer(self_), offsets, group_metadata) }
}

/// `KafkaProducer.sendOffsetsToTransaction(...)`, completion queued.
///
/// # Safety
///
/// `self_`, `offsets` and `group_metadata` must be live handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_send_offsets_to_transaction_cb(
    self_: *const kafka_producer_KafkaProducer_t,
    offsets: *const kafka_Map_t,
    group_metadata: *const kafka_consumer_ConsumerGroupMetadata_t,
    cb: kafka_producer_KafkaProducer_send_offsets_to_transaction_cb_t,
    opaque: *mut c_void,
) {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_commit_transaction(
    self_: *const kafka_producer_KafkaProducer_t,
) -> *mut kafka_common_Error_t {
    unsafe { kafka_producer_Producer_commit_transaction(producer(self_)) }
}

/// `KafkaProducer.commitTransaction()`, completion queued.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_commit_transaction_cb(
    self_: *const kafka_producer_KafkaProducer_t,
    cb: kafka_producer_KafkaProducer_commit_transaction_cb_t,
    opaque: *mut c_void,
) {
    unsafe { kafka_producer_Producer_commit_transaction_cb(producer(self_), cb, opaque) }
}

/// `KafkaProducer.abortTransaction()`: see
/// [`kafka_producer_Producer_abort_transaction`].
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_abort_transaction(
    self_: *const kafka_producer_KafkaProducer_t,
) -> *mut kafka_common_Error_t {
    unsafe { kafka_producer_Producer_abort_transaction(producer(self_)) }
}

/// `KafkaProducer.abortTransaction()`, completion queued.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_abort_transaction_cb(
    self_: *const kafka_producer_KafkaProducer_t,
    cb: kafka_producer_KafkaProducer_abort_transaction_cb_t,
    opaque: *mut c_void,
) {
    unsafe { kafka_producer_Producer_abort_transaction_cb(producer(self_), cb, opaque) }
}

/// `KafkaProducer.send(ProducerRecord, Callback)`: see
/// [`kafka_producer_Producer_send_with_callback`]; `callback` may be `NULL`.
///
/// # Safety
///
/// `self_` and `record` must be live handles, `callback` null or a live
/// handle, `out_send` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_send(
    self_: *const kafka_producer_KafkaProducer_t,
    record: *const kafka_producer_ProducerRecord_t,
    callback: *const kafka_producer_Callback_t,
    out_send: *mut *mut kafka_common_KafkaFuture_t,
) -> *mut kafka_common_Error_t {
    unsafe { kafka_producer_Producer_send_with_callback(producer(self_), record, callback, out_send) }
}

/// `KafkaProducer.send(ProducerRecord, Callback)`, completion queued: see
/// [`kafka_producer_Producer_send_with_callback_cb`].
///
/// # Safety
///
/// `self_` and `record` must be live handles, `callback` null or a live
/// handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_send_cb(
    self_: *const kafka_producer_KafkaProducer_t,
    record: *const kafka_producer_ProducerRecord_t,
    callback: *const kafka_producer_Callback_t,
    cb: kafka_producer_KafkaProducer_send_cb_t,
    opaque: *mut c_void,
) {
    unsafe { kafka_producer_Producer_send_with_callback_cb(producer(self_), record, callback, cb, opaque) }
}

/// Frees the handle (see "Destroy" in the module docs of
/// [`crate::ffi::producer`]); null is a no-op. Every `__as_Producer` view of
/// it is invalid afterwards.
///
/// # Safety
///
/// `self_` must be null or a handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_KafkaProducer_destroy(self_: *mut kafka_producer_KafkaProducer_t) {
    if !self_.is_null() {
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
        kafka_producer_Producer__execute_callbacks, kafka_producer_Producer_close_with_timeout,
    };
    use crate::ffi::util::{box_string_map, c_str_to_string, kafka_Map_destroy};

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

    #[test]
    fn producer_is_built_closed_and_destroyed_without_a_broker() {
        let props = box_string_map([("bootstrap.servers", "localhost:1"), ("max.block.ms", "100")]);
        let topic = CString::new("topic").unwrap();
        let bytes = kafka_Bytes_t::from_slice(b"v");
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
