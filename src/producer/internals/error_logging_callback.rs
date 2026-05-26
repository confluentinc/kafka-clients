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

//! Translation of `org.apache.kafka.clients.producer.internals.ErrorLoggingCallback`.

#![allow(dead_code)] // Phase 6e (KafkaProducer) wires this as the default callback.

use log::error;

use crate::common::errors::KafkaError;
use crate::producer::Callback;
use crate::producer::record_metadata::RecordMetadata;

/// A [`Callback`] that logs each failed send via the `log` crate. Mirrors
/// the Java class of the same name (which uses SLF4J).
///
/// The `key` and `value` byte slices are only retained when
/// `log_as_string == true`, matching Java's `if (logAsString) this.value = value`.
/// Otherwise we record the value-length only.
pub(crate) struct ErrorLoggingCallback {
    topic: String,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    value_length: i32,
    log_as_string: bool,
}

impl ErrorLoggingCallback {
    /// Mirrors `new ErrorLoggingCallback(String topic, byte[] key, byte[] value, boolean logAsString)`.
    pub fn new(topic: impl Into<String>, key: Option<&[u8]>, value: Option<&[u8]>, log_as_string: bool) -> Self {
        let value_length = match value {
            Some(v) => v.len() as i32,
            None => -1,
        };
        ErrorLoggingCallback {
            topic: topic.into(),
            // Java: `this.key = key;` — Java keeps the reference. We
            // only copy when needed for the error path; copying on
            // construction matches Java's reference retention since the
            // caller will typically not mutate the slice after handing
            // it off.
            key: key.map(|k| k.to_vec()),
            value: if log_as_string { value.map(|v| v.to_vec()) } else { None },
            value_length,
            log_as_string,
        }
    }
}

impl Callback for ErrorLoggingCallback {
    fn on_completion(&self, _metadata: Option<&RecordMetadata>, error: Option<&KafkaError>) {
        if let Some(e) = error {
            let key_string = match &self.key {
                None => "null".to_string(),
                Some(k) if self.log_as_string => String::from_utf8_lossy(k).into_owned(),
                Some(k) => format!("{} bytes", k.len()),
            };
            let value_string = if self.value_length == -1 {
                "null".to_string()
            } else if self.log_as_string {
                match &self.value {
                    Some(v) => String::from_utf8_lossy(v).into_owned(),
                    None => format!("{} bytes", self.value_length),
                }
            } else {
                format!("{} bytes", self.value_length)
            };
            // Java's `log.error(format, topic, keyString, valueString, e)`
            // passes the exception as a separate trailing argument so
            // SLF4J appends the full stack trace. Rust's `log::error!`
            // has no equivalent — we surface the same diagnostic via
            // `{:#?}` (pretty Debug) which prints the structured form
            // including any `KafkaError` source chain rather than just
            // the leaf `Display` message.
            error!(
                "Error when sending message to topic {} with key: {}, value: {} with error: {:#?}",
                self.topic, key_string, value_string, e
            );
        }
    }
}

#[cfg(test)]
mod tests {
    //! Java has no dedicated test class for ErrorLoggingCallback. We add
    //! a minimum smoke test that verifies the callback compiles, accepts
    //! the right arguments, and does not panic when invoked with both
    //! success and error states.
    use super::*;

    #[test]
    fn does_not_log_on_success() {
        let cb = ErrorLoggingCallback::new("topic", Some(b"k"), Some(b"v"), true);
        cb.on_completion(None, None);
    }

    #[test]
    fn logs_on_error_with_string_payloads() {
        let cb = ErrorLoggingCallback::new("topic", Some(b"my-key"), Some(b"my-value"), true);
        let err = KafkaError::Network("disconnected".to_string());
        cb.on_completion(None, Some(&err));
    }

    #[test]
    fn logs_on_error_with_byte_lengths() {
        let cb = ErrorLoggingCallback::new("topic", Some(b"my-key"), Some(b"my-value"), false);
        let err = KafkaError::Network("disconnected".to_string());
        cb.on_completion(None, Some(&err));
    }

    #[test]
    fn logs_on_error_with_null_key_and_value() {
        let cb = ErrorLoggingCallback::new("topic", None, None, true);
        let err = KafkaError::Network("disconnected".to_string());
        cb.on_completion(None, Some(&err));
    }
}
