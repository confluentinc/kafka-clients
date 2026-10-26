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

//! C bindings for `org.apache.kafka.clients.consumer.KafkaConsumer`
//! (CLAUDE.md §4).
//!
//! `kafka_consumer_KafkaConsumer_t` is a class handle, owned by the caller
//! and freed with [`kafka_consumer_KafkaConsumer_destroy`];
//! [`kafka_consumer_KafkaConsumer__as_Consumer`] is its borrowed `Consumer`
//! view (rule 3), through which every `kafka_consumer_Consumer_*` function
//! applies. Keys and values are `void *` (rule 6): what the deserializers
//! produce, or `kafka_Bytes_t *` owned by the records when a deserializer is
//! `NULL`.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

use crate::common::Error;
use crate::common::header::RecordHeaders;
use crate::common::serialization::{Deserializer, DynDeserializer};
use crate::consumer::KafkaConsumer;
use crate::ffi::common::deliver_value;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::common::serialization::deserializer::{
    DeserializerInner, kafka_common_serialization_Deserializer_t, take_deserializer,
};
use crate::ffi::consumer::consumer_config::{consumer_config_build, kafka_consumer_ConsumerConfig_t};
use crate::ffi::consumer::{ConsumerClassHandle, ConsumerKind, Owns, kafka_consumer_Consumer_t};
use crate::ffi::ffi_guard;
use crate::ffi::util::{GenericValue, box_bytes};

/// Opaque handle to a [`KafkaConsumer`] over `void *` key and value.
#[repr(C)]
pub struct kafka_consumer_KafkaConsumer_t {
    _private: [u8; 0],
}

/// The deserializer standing for a `NULL` one: the record's `void *` is an
/// owned `kafka_Bytes_t *` over the fetched bytes, sharing the fetch buffer
/// without copying when the receive path offers it as `Bytes`
/// (consumer-threading.md §27).
struct BytesPassthroughDeserializer;

impl Deserializer<GenericValue> for BytesPassthroughDeserializer {
    async fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<GenericValue, Error> {
        // No shared buffer to borrow from: one copy into an owned buffer.
        Ok(GenericValue::new(box_bytes(Bytes::copy_from_slice(data)).cast()))
    }

    async fn deserialize_with_headers_bytes(
        &self,
        _topic: &str,
        _headers: &mut RecordHeaders,
        data: &Bytes,
    ) -> Result<GenericValue, Error> {
        Ok(GenericValue::new(box_bytes(data.clone()).cast()))
    }
}

/// A C deserializer taken over from its handle: the consumer shares the
/// `Arc` with any view of the handle, and keeps the handle pointer valid for
/// an asynchronous implementation's reports, and every call goes through its Tokio
/// mutex, awaited rather than blocked on, so a call waiting for it holds no
/// thread.
struct ArcDeserializer(Arc<DeserializerInner>);

impl Deserializer<GenericValue> for ArcDeserializer {
    async fn deserialize(&self, topic: &str, data: &[u8]) -> Result<GenericValue, Error> {
        Deserializer::deserialize(&*self.0.imp().lock().await, topic, data).await
    }

    async fn deserialize_with_headers(
        &self,
        topic: &str,
        headers: &mut RecordHeaders,
        data: &[u8],
    ) -> Result<GenericValue, Error> {
        Deserializer::deserialize_with_headers(&*self.0.imp().lock().await, topic, headers, data).await
    }

    async fn deserialize_with_headers_bytes(
        &self,
        topic: &str,
        headers: &mut RecordHeaders,
        data: &Bytes,
    ) -> Result<GenericValue, Error> {
        Deserializer::deserialize_with_headers_bytes(&*self.0.imp().lock().await, topic, headers, data).await
    }

    async fn configure(&mut self, configs: &HashMap<String, String>, is_key: bool) -> Result<(), Error> {
        Deserializer::configure(&mut *self.0.imp().lock().await, configs, is_key).await
    }

    async fn close(&mut self) -> Result<(), Error> {
        Deserializer::close(&mut *self.0.imp().lock().await).await
    }
}

/// The Rust deserializer for a C parameter, and whether it is the bytes
/// passthrough (so the records own `kafka_Bytes_t`s).
///
/// # Safety
///
/// `deserializer` must be null or a live handle, not used again by the
/// caller afterwards.
unsafe fn deserializer_from(
    deserializer: *mut kafka_common_serialization_Deserializer_t,
) -> (Box<dyn DynDeserializer<GenericValue>>, bool) {
    if deserializer.is_null() {
        (Box::new(BytesPassthroughDeserializer), true)
    } else {
        // SAFETY: Per this function's `# Safety`, `deserializer` is (non-null, checked above) a
        // live handle not used again by the caller, which is stricter than what `take_deserializer`
        // requires.
        (Box::new(ArcDeserializer(unsafe { take_deserializer(deserializer) })), false)
    }
}

/// A consumer is instantiated by providing a set of key-value pairs as
/// configuration, and a key and a value `Deserializer`.
///
/// Note: after creating a `KafkaConsumer` you must always close it to avoid
/// resource leaks.
///
/// # Parameters
///
/// - `config` - The consumer configs
/// - `key_deserializer` - The deserializer for key that implements `Deserializer`. The configure()
///   method won't be called in the consumer when the deserializer is passed in directly.
/// - `value_deserializer` - The deserializer for value that implements `Deserializer`. The
///   configure() method won't be called in the consumer when the deserializer is passed in
///   directly.
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_argument_error` if `group.protocol` is invalid
/// - `kafka_common_Error_is_local_unsupported_operation_error` if `group.protocol` is
///   `classic`
///
/// # Translation notes
///
/// The consumer gets its own copy of `config`, with its `client.id`, so `config`
/// can build further consumers. A `NULL` deserializer makes that side's `void *`
/// a `kafka_Bytes_t *` owned by the records. A constructor that fails after
/// taking the deserializers closes them, as Java's does, without waiting for a C
/// deserializer's `close` to report.
///
/// # Safety
///
/// `config` must be a live handle, each deserializer null or a live handle (an
/// owned one not used again by the caller; its `self` stays the caller's until the
/// consumer is destroyed), `out_err` a valid slot pointing at null. The consumer is
/// owned by the caller, freed with [`kafka_consumer_KafkaConsumer_destroy`]; a C
/// deserializer's late `close` report arrives on its handle, which stays valid for
/// it.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_KafkaConsumer_new(
    config: *const kafka_consumer_ConsumerConfig_t,
    key_deserializer: *mut kafka_common_serialization_Deserializer_t,
    value_deserializer: *mut kafka_common_serialization_Deserializer_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_consumer_KafkaConsumer_t {
    // SAFETY: Per this function's `# Safety`, `config` is a live handle, kept alive for this
    // statement, as `consumer_config_build` requires.
    let config = unsafe { consumer_config_build(config) };
    // SAFETY: Per this function's `# Safety`, `key_deserializer` is null or a live handle not used
    // afterwards, as `deserializer_from` requires.
    let (key, owns_key) = unsafe { deserializer_from(key_deserializer) };
    // SAFETY: Per this function's `# Safety`, `value_deserializer` is null or a live handle not
    // used afterwards, as `deserializer_from` requires.
    let (value, owns_value) = unsafe { deserializer_from(value_deserializer) };
    let result = ConsumerClassHandle::new(Owns { key: owns_key, value: owns_value }, || {
        // The boxes go in as they are: `new` would box them again (see
        // `with_boxed_deserializers`). The copy parses properties the handle
        // already validated, so the configuration does not fail in practice; if
        // it did, the deserializers would be dropped unclosed, as Java's
        // constructor fails building its `ConsumerConfig` before it takes them.
        config
            .and_then(|config| KafkaConsumer::with_boxed_deserializers(config, key, value))
            .map(|consumer| ConsumerKind::Async(Box::new(consumer)))
    });
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid slot, as `deliver_value` requires;
    // the handle written is the fresh box the caller owns, freed with
    // `kafka_consumer_KafkaConsumer_destroy`.
    unsafe {
        deliver_value(result, out_err, |handle| {
            Box::into_raw(handle) as *mut kafka_consumer_KafkaConsumer_t
        })
    }
}

/// The class as a `Consumer`.
///
/// # Returns
///
/// The `Consumer` view of the class, `*mut` because `poll` and the other
/// operations take `&mut self`.
///
/// # Safety
///
/// `self_` must be a live handle. The view is borrowed, valid until the class
/// handle is destroyed; there is no `kafka_consumer_Consumer_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_KafkaConsumer__as_Consumer(
    self_: *mut kafka_consumer_KafkaConsumer_t,
) -> *mut kafka_consumer_Consumer_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, i.e. a
    // `Box<ConsumerClassHandle>` from `kafka_consumer_KafkaConsumer_new` not yet destroyed, alive
    // for this expression, the only use of the reference; `as_consumer` only takes its address.
    unsafe { &*(self_ as *const ConsumerClassHandle) }.as_consumer()
}

/// Frees a handle returned by [`kafka_consumer_KafkaConsumer_new`]: awaits the
/// `_cb` operations in flight, runs every pending callback (so each fires
/// exactly once), drops the consumer (which closes it if `close` was never
/// called, as Java's finalizer-less contract expects the caller to do) and
/// shuts its runtime down. Never called on the `__as_Consumer` view; a null
/// pointer is a no-op. It first wakes the operation still waiting, which
/// fails with `kafka_common_Error_is_wakeup_error`, so a long `poll_cb` does
/// not hold it up. Called inside a tokio runtime (on a worker thread, or
/// inside a `block_on`), where it cannot wait, it returns at once and all of
/// this happens on a new thread, the pending callbacks included; if that
/// thread cannot be started, the handle is leaked and an error logged.
///
/// # Safety
///
/// `self_` must be null or an owned handle from
/// [`kafka_consumer_KafkaConsumer_new`] not yet destroyed, that no other call
/// uses at the same time; the callbacks this call runs may use its
/// `__as_Consumer` view only to report through
/// `kafka_consumer_Consumer__set_callback_result`, and neither is used once
/// this call returned.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_KafkaConsumer_destroy(self_: *mut kafka_consumer_KafkaConsumer_t) {
    if !self_.is_null() {
        // SAFETY: Per this function's `# Safety`, `self_` (non-null, checked above) is an owned
        // handle not yet destroyed, i.e. the `Box::into_raw` of the `Box<ConsumerClassHandle>`
        // `kafka_consumer_KafkaConsumer_new` delivered, that no other call uses at the same time
        // except the reports of the callbacks this call runs, as `ConsumerClassHandle::destroy`
        // requires; it frees the handle exactly once, after awaiting the tasks still holding the
        // client.
        unsafe { ConsumerClassHandle::destroy(self_ as *mut ConsumerClassHandle) };
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::ffi::util::{kafka_Bytes_destroy, kafka_Bytes_t};

    /// A C deserializer's calls go through `ArcDeserializer`, whose futures
    /// (awaiting the Tokio mutex, then the `FfiDeserializer` variant's future)
    /// must fit `InlineFuture`'s buffer, or every record would box one: for a
    /// built-in and a C implementation (a pending report).
    #[test]
    fn arc_deserializer_futures_are_stored_inline() {
        use std::ffi::{c_char, c_void};

        use crate::common::serialization::InlineFuture;
        use crate::ffi::common::serialization::deserializer::{
            DeserializerInner, FfiDeserializer, kafka_common_serialization_Deserializer_new_async,
        };
        use crate::ffi::common::serialization::string_deserializer::GenericStringDeserializer;

        // Never called: the futures are only built.
        unsafe extern "C" fn async_fn(_: *mut c_void, _: *const c_char, _: kafka_Bytes_t, _: i64) {}

        let built_in = ArcDeserializer(DeserializerInner::view(Arc::new(tokio::sync::Mutex::new(
            FfiDeserializer::String(GenericStringDeserializer::default()),
        ))));
        // SAFETY: Test code: each handle comes from its constructor and is consumed once, here; the
        // consumer's `Arc` frees it.
        let asynchronous = unsafe {
            ArcDeserializer(take_deserializer(kafka_common_serialization_Deserializer_new_async(
                std::ptr::null_mut(),
                async_fn,
                None,
                None,
                None,
                None,
            )))
        };
        let mut headers = RecordHeaders::new();
        let data = Bytes::from_static(b"v");
        for (kind, deserializer) in [("built-in", &built_in), ("C", &asynchronous)] {
            let future = Deserializer::deserialize(deserializer, "t", &data);
            assert!(
                InlineFuture::<()>::is_inline_for(&future),
                "{kind} deserialize: {} bytes",
                size_of_val(&future)
            );
            let future = Deserializer::deserialize_with_headers(deserializer, "t", &mut headers, &data);
            assert!(
                InlineFuture::<()>::is_inline_for(&future),
                "{kind} with_headers: {} bytes",
                size_of_val(&future)
            );
            drop(future);
            let future = Deserializer::deserialize_with_headers_bytes(deserializer, "t", &mut headers, &data);
            assert!(
                InlineFuture::<()>::is_inline_for(&future),
                "{kind} with_headers_bytes: {} bytes",
                size_of_val(&future)
            );
        }
    }

    /// An asynchronous C deserializer costs no allocation per record on the
    /// consumer's path: through `dyn DynDeserializer`, the `ArcDeserializer`
    /// future stays inline, the Tokio lock is uncontended, the registry slot
    /// and its arguments are reused, and the headers (empty or not, moved
    /// into the slot and back) and the fetch buffer are not copied.
    #[test]
    fn async_c_deserializer_through_dyn_allocates_nothing_per_record() {
        use std::ffi::{c_char, c_void};
        use std::sync::atomic::{AtomicPtr, Ordering};

        use crate::AllocTrackingGuard;
        use crate::common::header::Headers;
        use crate::ffi::common::header::internals::record_headers::kafka_common_header_internals_RecordHeaders_t;
        use crate::ffi::common::serialization::deserializer::{
            kafka_common_serialization_Deserializer__set_callback_result,
            kafka_common_serialization_Deserializer_new_async, kafka_common_serialization_Deserializer_t,
        };
        use crate::ffi::common::serialization::ready;

        /// The value every call reports: a static, so reporting allocates nothing.
        static VALUE: u8 = 7;

        /// Reports `VALUE` during the call, through the handle `self_` points at.
        unsafe extern "C" fn report_inline(
            self_: *mut c_void,
            _topic: *const c_char,
            _headers: *mut kafka_common_header_internals_RecordHeaders_t,
            _data: kafka_Bytes_t,
            callback_id: i64,
        ) {
            // SAFETY: Test code: `self_` is the `AtomicPtr` holding the live handle, which the test
            // keeps alive; the value is a static, and the error null.
            unsafe {
                let handle =
                    (*(self_ as *const AtomicPtr<kafka_common_serialization_Deserializer_t>)).load(Ordering::SeqCst);
                kafka_common_serialization_Deserializer__set_callback_result(
                    handle,
                    callback_id,
                    &VALUE as *const u8 as *mut c_void,
                    std::ptr::null_mut(),
                );
            }
        }
        unsafe extern "C" fn unused(_: *mut c_void, _: *const c_char, _: kafka_Bytes_t, _: i64) {
            unreachable!("the consumer calls the headers-and-bytes method")
        }

        let slot = AtomicPtr::new(std::ptr::null_mut());
        let handle = kafka_common_serialization_Deserializer_new_async(
            &slot as *const AtomicPtr<_> as *mut c_void,
            unused,
            None,
            Some(report_inline),
            None,
            None,
        );
        slot.store(handle, Ordering::SeqCst);
        // SAFETY: Test code: `handle` comes from its constructor and is consumed once, here; the
        // `Arc` keeps it valid for the reports, and `slot` outlives the deserializer.
        let deserializer: Box<dyn DynDeserializer<GenericValue>> =
            Box::new(ArcDeserializer(unsafe { take_deserializer(handle) }));
        let mut with_k = RecordHeaders::new();
        with_k.add_with_key_value("k", Some(b"v")).unwrap();
        for mut headers in [RecordHeaders::new(), with_k] {
            let data = Bytes::from_static(b"value");
            let mut record = || {
                let value = ready(DynDeserializer::deserialize_with_headers_bytes(
                    &*deserializer,
                    "topic",
                    &mut headers,
                    &data,
                ))
                .expect("an inline report completes the first poll")
                .unwrap();
                assert_eq!(value.as_ptr(), &VALUE as *const u8 as *mut c_void);
            };
            // Warm up: the registry's slab and the slot's arguments are allocated once.
            record();
            let _guard = AllocTrackingGuard::new();
            for _ in 0..100 {
                record();
            }
            assert_eq!(
                AllocTrackingGuard::count(),
                0,
                "allocations per 100 records with {} headers",
                headers.to_array().len()
            );
        }
    }

    #[test]
    fn passthrough_shares_the_fetch_buffer_without_copying() {
        use crate::ffi::common::serialization::ready;
        let source = Bytes::from_static(b"hello world");
        let data = source.slice(6..);
        let mut headers = RecordHeaders::new();
        let value = ready(Deserializer::deserialize_with_headers_bytes(
            &BytesPassthroughDeserializer,
            "t",
            &mut headers,
            &data,
        ))
        .unwrap()
        .unwrap();
        let bytes = value.as_ptr() as *mut kafka_Bytes_t;
        // SAFETY: Test code: `bytes` is the `kafka_Bytes_t` `deserialize_with_headers_bytes` just
        // boxed over a clone of `data`, which keeps the static buffer alive for this read.
        let slice = unsafe { (*bytes).as_slice() }.unwrap();
        assert_eq!(slice, b"world");
        assert_eq!(slice.as_ptr(), source[6..].as_ptr(), "zero-copy: the same memory");
        // SAFETY: Test code: `bytes` is the box built above, destroyed once, after `slice`'s last
        // use.
        unsafe { kafka_Bytes_destroy(bytes) };

        let value = ready(Deserializer::deserialize(&BytesPassthroughDeserializer, "t", b"copy"))
            .unwrap()
            .unwrap();
        let bytes = value.as_ptr() as *mut kafka_Bytes_t;
        // SAFETY: Test code: `bytes` is the `kafka_Bytes_t` `deserialize` just boxed over its own
        // copy.
        assert_eq!(unsafe { (*bytes).as_slice() }.unwrap(), b"copy");
        // SAFETY: Test code: `bytes` is the box built above, destroyed once.
        unsafe { kafka_Bytes_destroy(bytes) };
    }

    #[test]
    fn constructor_reports_an_invalid_config_through_the_error_slot() {
        // No bootstrap servers: `KafkaConsumer::new` fails before any I/O.
        let props: HashMap<String, String> = HashMap::from([("group.protocol".to_string(), "classic".to_string())]);
        let config = crate::consumer::ConsumerConfig::new(&props);
        let Ok(config) = config else {
            // Construction already rejected the properties; nothing to run.
            return;
        };
        let result = ConsumerClassHandle::new(Owns { key: true, value: true }, || {
            KafkaConsumer::new(config, BytesPassthroughDeserializer, BytesPassthroughDeserializer)
                .map(|consumer| ConsumerKind::Async(Box::new(consumer)))
        });
        assert!(result.is_err());
    }

    /// What the `poll_cb` completion fired with: `(is_wakeup_error, message)`,
    /// or `None` for records.
    type Fired = std::sync::Mutex<Vec<Option<(i8, String)>>>;

    unsafe extern "C" fn record_poll(
        value: *mut crate::ffi::consumer::consumer_records::kafka_consumer_ConsumerRecords_t,
        error: *mut kafka_common_Error_t,
        opaque: *mut std::ffi::c_void,
    ) {
        use crate::ffi::common::{kafka_common_Error_destroy, kafka_common_Error_message};
        use crate::ffi::consumer::consumer_records::kafka_consumer_ConsumerRecords_destroy;
        use crate::ffi::error_predicates::kafka_common_Error_is_wakeup_error;
        // SAFETY: Test code: `opaque` is the `Fired` the test keeps alive until the completion
        // fired; a completion owns the records or error handle it receives.
        unsafe {
            let fired = &*(opaque as *const Fired);
            if error.is_null() {
                kafka_consumer_ConsumerRecords_destroy(value);
                fired.lock().unwrap().push(None);
            } else {
                let message = std::ffi::CStr::from_ptr(kafka_common_Error_message(error))
                    .to_string_lossy()
                    .into_owned();
                fired
                    .lock()
                    .unwrap()
                    .push(Some((kafka_common_Error_is_wakeup_error(error), message)));
                kafka_common_Error_destroy(error);
            }
        }
    }

    /// One configuration handle builds several consumers: each gets its own
    /// configuration built from the handle's properties, so building one
    /// neither consumes the handle nor touches its record of the keys used.
    #[test]
    fn one_config_handle_builds_several_consumers() {
        use crate::ffi::consumer::consumer_config::{
            kafka_consumer_ConsumerConfig_destroy, kafka_consumer_ConsumerConfig_new,
            kafka_consumer_ConsumerConfig_unused,
        };
        use crate::ffi::util::{kafka_List_destroy, kafka_Map_destroy, kafka_Map_new, kafka_Map_put, list_string_set};
        // SAFETY: Test code: the map, the config, the lists and both consumer handles are live
        // until they are destroyed once below; `err` is a valid slot pointing at null before each
        // call.
        unsafe {
            let props = kafka_Map_new();
            for (key, value) in [(c"bootstrap.servers", c"127.0.0.1:1"), (c"group.protocol", c"consumer")] {
                kafka_Map_put(
                    props,
                    key.as_ptr() as *mut std::ffi::c_void,
                    value.as_ptr() as *mut std::ffi::c_void,
                );
            }
            let mut err = std::ptr::null_mut();
            let config = kafka_consumer_ConsumerConfig_new(props, &mut err);
            assert!(err.is_null());
            kafka_Map_destroy(props);
            // `postProcessParsedConfig` read `group.protocol`; nothing else yet.
            let expected_unused = std::collections::HashSet::from(["bootstrap.servers".to_string()]);
            let mut classes = Vec::new();
            for _ in 0..2 {
                let class =
                    kafka_consumer_KafkaConsumer_new(config, std::ptr::null_mut(), std::ptr::null_mut(), &mut err);
                assert!(err.is_null());
                assert!(!class.is_null());
                classes.push(class);
                let unused = kafka_consumer_ConsumerConfig_unused(config);
                assert_eq!(list_string_set(unused), expected_unused, "the consumer marks its own record");
                kafka_List_destroy(unused);
            }
            kafka_consumer_ConsumerConfig_destroy(config);
            for class in classes {
                kafka_consumer_KafkaConsumer_destroy(class);
            }
        }
    }

    /// A consumer created from a handle runs under the handle's resolved
    /// `client.id`, the one `_get_string` answers, so the consumers of one
    /// handle share it, as Java's consumers given one `ConsumerConfig` do;
    /// the next handle takes the next number from the sequence, as Java's
    /// next `ConsumerConfig` does (`consumer-g-1`, then `consumer-g-2`).
    #[test]
    fn consumers_run_under_the_client_id_of_their_handle() {
        use crate::ffi::consumer::consumer_config::{
            kafka_consumer_ConsumerConfig_destroy, kafka_consumer_ConsumerConfig_get_string,
            kafka_consumer_ConsumerConfig_new,
        };
        use crate::ffi::util::{box_string_map, kafka_Map_destroy};
        // SAFETY: Test code: `class` is a live class handle; the view is used only within this
        // call, while the handle lives.
        let client_ids = |class: *mut kafka_consumer_KafkaConsumer_t| unsafe {
            crate::ffi::consumer::client_ref(kafka_consumer_KafkaConsumer__as_Consumer(class))
                .handle()
                .metrics()
                .keys()
                .filter_map(|name| name.tags().get("client-id").cloned())
                .collect::<std::collections::HashSet<_>>()
        };
        let group = "client-id-of-their-handle";
        let props = box_string_map([
            ("bootstrap.servers", "127.0.0.1:1"),
            ("group.protocol", "consumer"),
            ("group.id", group),
        ]);
        // SAFETY: Test code: the map, both configs and every consumer handle are live until they
        // are destroyed once below; the strings `_get_string` lends are copied before their
        // handle is destroyed; `err` is a valid slot pointing at null before each call.
        unsafe {
            let mut err = std::ptr::null_mut();
            let mut handle_ids = Vec::new();
            let mut classes = Vec::new();
            for _ in 0..2 {
                let config = kafka_consumer_ConsumerConfig_new(props, &mut err);
                assert!(err.is_null());
                let id = kafka_consumer_ConsumerConfig_get_string(config, c"client.id".as_ptr(), &mut err);
                assert!(err.is_null());
                let id = std::ffi::CStr::from_ptr(id).to_str().unwrap().to_string();
                for _ in 0..2 {
                    let class =
                        kafka_consumer_KafkaConsumer_new(config, std::ptr::null_mut(), std::ptr::null_mut(), &mut err);
                    assert!(err.is_null());
                    assert_eq!(client_ids(class), std::collections::HashSet::from([id.clone()]));
                    classes.push(class);
                }
                kafka_consumer_ConsumerConfig_destroy(config);
                handle_ids.push(id);
            }
            kafka_Map_destroy(props);
            let prefix = format!("consumer-{group}-");
            let numbers: Vec<i32> = handle_ids
                .iter()
                .map(|id| id.strip_prefix(&prefix).unwrap().parse().unwrap())
                .collect();
            // Other tests take numbers concurrently, so the next one is only later.
            assert!(numbers[1] > numbers[0], "{handle_ids:?}");
            for class in classes {
                kafka_consumer_KafkaConsumer_destroy(class);
            }
        }
    }

    /// Destroying a consumer while a `poll_cb` with a long timeout waits on an
    /// unreachable broker wakes the poll up instead of waiting the timeout out:
    /// the completion fires once, with the `WakeupError` Java's `poll` throws
    /// after `wakeup()`.
    #[test]
    fn destroy_wakes_a_waiting_poll_cb_up() {
        use crate::ffi::common::topic_partition::{
            kafka_common_TopicPartition_destroy, kafka_common_TopicPartition_new,
        };
        use crate::ffi::consumer::consumer_config::{
            kafka_consumer_ConsumerConfig_destroy, kafka_consumer_ConsumerConfig_new,
        };
        use crate::ffi::consumer::{
            kafka_consumer_Consumer__execute_callbacks, kafka_consumer_Consumer_assign, kafka_consumer_Consumer_poll_cb,
        };
        use crate::ffi::util::{
            kafka_List_add, kafka_List_destroy, kafka_List_new, kafka_Map_destroy, kafka_Map_new, kafka_Map_put,
        };
        // SAFETY: Test code: every map, list, config, partition and consumer handle is live until
        // it is destroyed once below; `err` is a valid slot pointing at null before each call;
        // `fired` outlives the destroy, which fires the completion.
        unsafe {
            let props = kafka_Map_new();
            for (key, value) in [
                (c"bootstrap.servers", c"127.0.0.1:1"),
                (c"group.id", c"destroy-wakes-poll"),
                (c"group.protocol", c"consumer"),
            ] {
                kafka_Map_put(
                    props,
                    key.as_ptr() as *mut std::ffi::c_void,
                    value.as_ptr() as *mut std::ffi::c_void,
                );
            }
            let mut err = std::ptr::null_mut();
            let config = kafka_consumer_ConsumerConfig_new(props, &mut err);
            assert!(err.is_null());
            kafka_Map_destroy(props);
            let class = kafka_consumer_KafkaConsumer_new(config, std::ptr::null_mut(), std::ptr::null_mut(), &mut err);
            assert!(err.is_null());
            kafka_consumer_ConsumerConfig_destroy(config);
            let consumer = kafka_consumer_KafkaConsumer__as_Consumer(class);

            let partitions = kafka_List_new();
            let tp = kafka_common_TopicPartition_new(c"t".as_ptr(), 0);
            kafka_List_add(partitions, tp as *mut std::ffi::c_void);
            kafka_consumer_Consumer_assign(consumer, partitions, &mut err);
            assert!(err.is_null());
            kafka_List_destroy(partitions);
            kafka_common_TopicPartition_destroy(tp);

            let fired = Fired::default();
            kafka_consumer_Consumer_poll_cb(
                consumer,
                60_000,
                record_poll,
                &fired as *const Fired as *mut std::ffi::c_void,
            );
            std::thread::sleep(std::time::Duration::from_millis(300));
            kafka_consumer_Consumer__execute_callbacks(consumer);
            assert!(fired.lock().unwrap().is_empty(), "still polling");

            let started = std::time::Instant::now();
            kafka_consumer_KafkaConsumer_destroy(class);
            let elapsed = started.elapsed();
            assert!(elapsed < std::time::Duration::from_secs(10), "destroy took {elapsed:?}");
            assert_eq!(
                *fired.lock().unwrap(),
                [Some((1, String::new()))],
                "a wakeup, with Java's empty message"
            );
        }
    }

    /// Records what a `current_lag_cb` completion delivered.
    type LagFired = std::sync::Mutex<Vec<(i8, i64, bool)>>;

    unsafe extern "C" fn record_lag(
        present: i8,
        lag: i64,
        error: *mut kafka_common_Error_t,
        opaque: *mut std::ffi::c_void,
    ) {
        // SAFETY: Test code: `opaque` is the `LagFired` the test keeps alive until the completion
        // fired; a completion owns the error handle it receives.
        unsafe {
            let fired = &*(opaque as *const LagFired);
            fired.lock().unwrap().push((present, lag, !error.is_null()));
            if !error.is_null() {
                crate::ffi::common::kafka_common_Error_destroy(error);
            }
        }
    }

    /// Java's `currentLag` returns `OptionalLong.empty()` while the consumer
    /// knows no log end for the partition (`AsyncKafkaConsumer.currentLag`, no
    /// high watermark before a fetch). Across C that is the presence flag 0,
    /// with no error and `*out_lag` unwritten; the `_cb` twin delivers
    /// `present` 0 and a `lag` of 0.
    #[test]
    fn current_lag_is_empty_before_any_fetch() {
        use crate::ffi::common::topic_partition::{
            kafka_common_TopicPartition_destroy, kafka_common_TopicPartition_new,
        };
        use crate::ffi::consumer::consumer_config::{
            kafka_consumer_ConsumerConfig_destroy, kafka_consumer_ConsumerConfig_new,
        };
        use crate::ffi::consumer::{
            kafka_consumer_Consumer__execute_callbacks, kafka_consumer_Consumer_assign,
            kafka_consumer_Consumer_current_lag, kafka_consumer_Consumer_current_lag_cb,
        };
        use crate::ffi::util::{
            kafka_List_add, kafka_List_destroy, kafka_List_new, kafka_Map_destroy, kafka_Map_new, kafka_Map_put,
        };
        // SAFETY: Test code: every map, list, config, partition and consumer handle is live until
        // it is destroyed once below; `err` is a valid slot pointing at null before each call;
        // `out_lag` and `fired` outlive the calls that write them.
        unsafe {
            let props = kafka_Map_new();
            for (key, value) in [(c"bootstrap.servers", c"127.0.0.1:1"), (c"group.protocol", c"consumer")] {
                kafka_Map_put(
                    props,
                    key.as_ptr() as *mut std::ffi::c_void,
                    value.as_ptr() as *mut std::ffi::c_void,
                );
            }
            let mut err = std::ptr::null_mut();
            let config = kafka_consumer_ConsumerConfig_new(props, &mut err);
            assert!(err.is_null());
            kafka_Map_destroy(props);
            let class = kafka_consumer_KafkaConsumer_new(config, std::ptr::null_mut(), std::ptr::null_mut(), &mut err);
            assert!(err.is_null());
            kafka_consumer_ConsumerConfig_destroy(config);
            let consumer = kafka_consumer_KafkaConsumer__as_Consumer(class);

            let partitions = kafka_List_new();
            let tp = kafka_common_TopicPartition_new(c"t".as_ptr(), 0);
            kafka_List_add(partitions, tp as *mut std::ffi::c_void);
            kafka_consumer_Consumer_assign(consumer, partitions, &mut err);
            assert!(err.is_null());
            kafka_List_destroy(partitions);

            let mut out_lag = 42;
            let present = kafka_consumer_Consumer_current_lag(consumer, tp, &mut out_lag, &mut err);
            assert!(err.is_null(), "an empty lag is no failure");
            assert_eq!((present, out_lag), (0, 42), "empty: flag 0, *out_lag unwritten");

            let fired = LagFired::default();
            kafka_consumer_Consumer_current_lag_cb(
                consumer,
                tp,
                record_lag,
                &fired as *const LagFired as *mut std::ffi::c_void,
            );
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while fired.lock().unwrap().is_empty() {
                assert!(std::time::Instant::now() < deadline, "current_lag_cb never completed");
                kafka_consumer_Consumer__execute_callbacks(consumer);
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            assert_eq!(*fired.lock().unwrap(), [(0i8, 0i64, false)]);

            kafka_common_TopicPartition_destroy(tp);
            kafka_consumer_KafkaConsumer_destroy(class);
        }
    }
}
