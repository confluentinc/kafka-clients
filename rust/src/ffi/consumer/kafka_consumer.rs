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
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::common::serialization::deserializer::{
    SharedDeserializer, kafka_common_serialization_Deserializer_t, take_deserializer,
};
use crate::ffi::consumer::consumer_config::{consumer_config_ref, kafka_consumer_ConsumerConfig_t};
use crate::ffi::consumer::{ConsumerClassHandle, ConsumerKind, Owns, kafka_consumer_Consumer_t, out_slot};
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
        (Box::new(ArcDeserializer(unsafe { take_deserializer(deserializer) })), false)
    }
}

/// `KafkaConsumer(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer)`:
/// delivers the owned `kafka_consumer_Consumer_t` the constructor returns
/// (`Box<dyn Consumer>` in Rust), freed with `kafka_consumer_Consumer_destroy`,
/// or returns the owned error. The config is copied; each deserializer
/// handle is consumed (its `self` stays the caller's until the consumer is
/// destroyed, CLAUDE.md §4 rule 3), and `NULL` means the record's `void *`
/// on that side is a `kafka_Bytes_t *` owned by the records (rule 6).
///
/// # Safety
///
/// `config` must be a live handle, each deserializer null or a live handle
/// not used afterwards, and `out_new` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_KafkaConsumer_new(
    config: *const kafka_consumer_ConsumerConfig_t,
    key_deserializer: *mut kafka_common_serialization_Deserializer_t,
    value_deserializer: *mut kafka_common_serialization_Deserializer_t,
    out_new: *mut *mut kafka_consumer_Consumer_t,
) -> *mut kafka_common_Error_t {
    let config = unsafe { consumer_config_ref(config) }.config().clone();
    let (key, owns_key) = unsafe { deserializer_from(key_deserializer) };
    let (value, owns_value) = unsafe { deserializer_from(value_deserializer) };
    let result = ConsumerClassHandle::new(Owns { key: owns_key, value: owns_value }, || {
        KafkaConsumer::new(config, key, value).map(ConsumerKind::Async)
    });
    unsafe {
        out_slot(result, out_new, |handle| {
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
        let slice = unsafe { (*bytes).as_slice() }.unwrap();
        assert_eq!(slice, b"world");
        assert_eq!(slice.as_ptr(), source[6..].as_ptr(), "zero-copy: the same memory");
        unsafe { kafka_Bytes_destroy(bytes) };

        let value = BytesPassthroughDeserializer.deserialize("t", b"copy").unwrap();
        let bytes = value.as_ptr() as *mut kafka_Bytes_t;
        assert_eq!(unsafe { (*bytes).as_slice() }.unwrap(), b"copy");
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
}
