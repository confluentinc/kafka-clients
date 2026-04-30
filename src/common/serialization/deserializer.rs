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

//! Translation of `org.apache.kafka.common.serialization.Deserializer`.

use std::collections::HashMap;

use crate::common::KafkaError;

/// Convert bytes back to objects of type `T`.
///
/// Mirrors Java's `Deserializer<T>` interface. Receives borrowed
/// `Option<&[u8]>` to avoid forcing the caller to own a `Vec<u8>` — a
/// `null` byte-array maps to `None` and concrete impls return `Ok(None)`
/// (matching Java's `null` semantics).
///
/// The trait is `dyn`-compatible.
pub trait Deserializer<T>: Send + Sync {
    /// Configure this deserializer.
    ///
    /// Mirrors Java's `default void configure(Map<String,?>, boolean)`.
    /// Most concrete types are no-ops; `StringDeserializer`/`UUIDDeserializer`
    /// read their charset from the configs.
    #[allow(unused_variables)]
    fn configure(&mut self, configs: &HashMap<String, String>, is_key: bool) -> Result<(), KafkaError> {
        // intentionally left blank — matches Java default impl
        Ok(())
    }

    /// Deserialize a record value from bytes.
    ///
    /// Returns `Ok(None)` for `None` input. Returns `Err(KafkaError::Serialization)`
    /// when the bytes can't be parsed (e.g. fixed-length deserializers
    /// receiving the wrong byte count).
    fn deserialize(&self, topic: &str, data: Option<&[u8]>) -> Result<Option<T>, KafkaError>;

    // Java's `default T deserialize(String, Headers, byte[])` overload
    // defers to the no-headers variant. We omit the headers overload here
    // for the same reason as in `Serializer`: `Headers` carries `&mut Self`
    // builder methods that prevent `dyn Headers` parameterization. See the
    // comment in `serializer.rs` for the contract preservation argument.

    /// Close this deserializer. Idempotent. Default no-op.
    fn close(&mut self) {
        // intentionally left blank — matches Java default impl
    }
}
