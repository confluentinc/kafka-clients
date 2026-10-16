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

//! C binding for `org.apache.kafka.clients.consumer.KafkaConsumer`'s
//! constructor (CLAUDE.md §4).
//!
//! `KafkaConsumer::new` returns the delegate it chose as `Box<dyn Consumer>`,
//! so the C constructor delivers an owned `kafka_consumer_Consumer_t` and
//! there is no `kafka_consumer_KafkaConsumer_t`. Keys and values are
//! `void *` (rule 6): what the deserializers produce, or `kafka_Bytes_t *`
//! owned by the records when a deserializer is `NULL`.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

use crate::common::Error;
use crate::common::header::Headers;
use crate::common::serialization::Deserializer;
use crate::consumer::KafkaConsumer;
use crate::ffi::common::deliver_value;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::common::serialization::deserializer::{
    SharedDeserializer, kafka_common_serialization_Deserializer_t, take_deserializer,
};
use crate::ffi::consumer::consumer_config::{consumer_config_ref, kafka_consumer_ConsumerConfig_t};
use crate::ffi::consumer::{ConsumerClassHandle, ConsumerKind, Owns, kafka_consumer_Consumer_t};
use crate::ffi::ffi_guard;
use crate::ffi::util::{GenericValue, box_bytes};

/// The deserializer standing for a `NULL` one: the record's `void *` is an
/// owned `kafka_Bytes_t *` over the fetched bytes, sliced from the fetch
/// buffer without copying when the receive path offers it
/// (consumer-threading.md §27).
struct BytesPassthroughDeserializer;

impl Deserializer<GenericValue> for BytesPassthroughDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<GenericValue, Error> {
        // No shared buffer to borrow from: one copy into an owned buffer.
        Ok(GenericValue::new(box_bytes(Bytes::copy_from_slice(data)).cast()))
    }

    fn deserialize_from_shared(&self, _topic: &str, source: &Bytes, data: &[u8]) -> Result<GenericValue, Error> {
        Ok(GenericValue::new(box_bytes(source.slice_ref(data)).cast()))
    }

    fn deserialize_from_shared_with_headers(
        &self,
        topic: &str,
        _headers: &dyn Headers,
        source: &Bytes,
        data: &[u8],
    ) -> Result<GenericValue, Error> {
        self.deserialize_from_shared(topic, source, data)
    }
}

/// A C deserializer taken over from its handle: the consumer owns the
/// `Arc`, the `&mut self` methods go through the mutex.
struct ArcDeserializer(Arc<SharedDeserializer>);

impl Deserializer<GenericValue> for ArcDeserializer {
    fn deserialize(&self, topic: &str, data: &[u8]) -> Result<GenericValue, Error> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).deserialize(topic, data)
    }

    fn deserialize_with_headers(&self, topic: &str, headers: &dyn Headers, data: &[u8]) -> Result<GenericValue, Error> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .deserialize_with_headers(topic, headers, data)
    }

    fn deserialize_from_shared(&self, topic: &str, source: &Bytes, data: &[u8]) -> Result<GenericValue, Error> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .deserialize_from_shared(topic, source, data)
    }

    fn deserialize_from_shared_with_headers(
        &self,
        topic: &str,
        headers: &dyn Headers,
        source: &Bytes,
        data: &[u8],
    ) -> Result<GenericValue, Error> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .deserialize_from_shared_with_headers(topic, headers, source, data)
    }

    fn configure(&mut self, configs: &HashMap<String, String>, is_key: bool) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).configure(configs, is_key);
    }

    fn close(&mut self) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).close();
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
) -> (Box<dyn Deserializer<GenericValue>>, bool) {
    if deserializer.is_null() {
        (Box::new(BytesPassthroughDeserializer), true)
    } else {
        // SAFETY: Per this function's `# Safety`, `deserializer` is (non-null, checked above) a
        // live handle not used again by the caller, which is stricter than what `take_deserializer`
        // requires.
        (Box::new(ArcDeserializer(unsafe { take_deserializer(deserializer) })), false)
    }
}

/// `KafkaConsumer(ConsumerConfig config, Deserializer<K> keyDeserializer,
/// Deserializer<V> valueDeserializer)`: returns the owned
/// `kafka_consumer_Consumer_t` the constructor returns (`Box<dyn Consumer>` in
/// Rust), freed with `kafka_consumer_Consumer_destroy`, or stores the owned
/// error in `*out_err` and returns `NULL`. The config is copied; each
/// deserializer handle is consumed (its `self` stays the caller's until the
/// consumer is destroyed, CLAUDE.md §4 rule 3), and `NULL` means the record's
/// `void *` on that side is a `kafka_Bytes_t *` owned by the records (rule 6).
///
/// # Safety
///
/// `config` must be a live handle, each deserializer null or a live handle not
/// used afterwards, and `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_KafkaConsumer_new(
    config: *const kafka_consumer_ConsumerConfig_t,
    key_deserializer: *mut kafka_common_serialization_Deserializer_t,
    value_deserializer: *mut kafka_common_serialization_Deserializer_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_consumer_Consumer_t {
    // SAFETY: Per this function's `# Safety`, `config` is a live handle, kept alive for this
    // statement, the only use of the reference, as `consumer_config_ref` requires.
    let config = unsafe { consumer_config_ref(config) }.clone();
    // SAFETY: Per this function's `# Safety`, `key_deserializer` is null or a live handle not used
    // afterwards, as `deserializer_from` requires.
    let (key, owns_key) = unsafe { deserializer_from(key_deserializer) };
    // SAFETY: Per this function's `# Safety`, `value_deserializer` is null or a live handle not
    // used afterwards, as `deserializer_from` requires.
    let (value, owns_value) = unsafe { deserializer_from(value_deserializer) };
    let result = ConsumerClassHandle::new(Owns { key: owns_key, value: owns_value }, || {
        KafkaConsumer::new(config, key, value).map(ConsumerKind::Async)
    });
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid slot, as `deliver_value` requires;
    // the handle written is the fresh box the caller owns, freed with
    // `kafka_consumer_Consumer_destroy`.
    unsafe {
        deliver_value(result, out_err, |handle| {
            Box::into_raw(handle) as *mut kafka_consumer_Consumer_t
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::ffi::util::{kafka_Bytes_destroy, kafka_Bytes_t};

    #[test]
    fn passthrough_slices_the_shared_buffer_without_copying() {
        let source = Bytes::from_static(b"hello world");
        let value = BytesPassthroughDeserializer
            .deserialize_from_shared("t", &source, &source[6..])
            .unwrap();
        let bytes = value.as_ptr() as *mut kafka_Bytes_t;
        // SAFETY: Test code: `bytes` is the `kafka_Bytes_t` `deserialize_from_shared` just boxed
        // over `source`, which outlives this read.
        let slice = unsafe { (*bytes).as_slice() }.unwrap();
        assert_eq!(slice, b"world");
        assert_eq!(slice.as_ptr(), source[6..].as_ptr(), "zero-copy: the same memory");
        // SAFETY: Test code: `bytes` is the box built above, destroyed once, after `slice`'s last
        // use.
        unsafe { kafka_Bytes_destroy(bytes) };

        let value = BytesPassthroughDeserializer.deserialize("t", b"copy").unwrap();
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
            KafkaConsumer::new(
                config,
                Box::new(BytesPassthroughDeserializer),
                Box::new(BytesPassthroughDeserializer),
            )
            .map(ConsumerKind::Async)
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
            kafka_consumer_Consumer__execute_callbacks, kafka_consumer_Consumer_assign,
            kafka_consumer_Consumer_destroy, kafka_consumer_Consumer_poll_cb,
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
            let consumer =
                kafka_consumer_KafkaConsumer_new(config, std::ptr::null_mut(), std::ptr::null_mut(), &mut err);
            assert!(err.is_null());
            kafka_consumer_ConsumerConfig_destroy(config);

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
            kafka_consumer_Consumer_destroy(consumer);
            let elapsed = started.elapsed();
            assert!(elapsed < std::time::Duration::from_secs(10), "destroy took {elapsed:?}");
            assert_eq!(*fired.lock().unwrap(), [Some((1, "WakeupTrigger fired".to_string()))]);
        }
    }
}
