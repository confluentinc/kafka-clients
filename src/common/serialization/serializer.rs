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

//! Translation of `org.apache.kafka.common.serialization.Serializer`.

use std::collections::HashMap;

use crate::common::KafkaError;

/// Convert objects of type `T` to bytes.
///
/// Mirrors Java's `Serializer<T>` interface (with `Closeable`). The Rust
/// trait exposes two `serialize` shapes:
///
/// * [`Serializer::serialize`] returns an owned `Option<Vec<u8>>`. This is
///   the direct Java analog (`byte[] serialize(String, T)`) and is meant
///   for callers that need to own the result independently of any batch
///   buffer.
/// * [`Serializer::serialize_to`] writes serialized bytes directly into a
///   caller-supplied `Vec<u8>` (typically the `MemoryRecordsBuilder` batch
///   buffer in Phase 3c/3d). This is the zero-copy hot-path API required by
///   CLAUDE.md rule 12 — concrete impls override it to avoid allocating an
///   intermediate `Vec<u8>` per record. The default impl falls back to
///   `serialize` + `extend_from_slice`, which is correct but allocates.
///
/// `null`/`None` data is propagated through both paths: `serialize` returns
/// `Ok(None)`, `serialize_to` returns `Ok(false)` and writes nothing.
///
/// The trait is `dyn`-compatible: methods take `&self` and use only
/// concrete (non-generic) parameter types so [`crate::common::serialization::Serde`]
/// can hold a `&dyn Serializer<T>`.
pub trait Serializer<T: ?Sized>: Send + Sync {
    /// Configure this serializer.
    ///
    /// Mirrors Java's `default void configure(Map<String,?>, boolean)`.
    /// Most concrete types are no-ops; `StringSerializer`/`UUIDSerializer`
    /// read their charset from the configs.
    ///
    /// Returns `Result` because charset parsing can fail with
    /// `SerializationException` (rule 10: Java's unchecked exception →
    /// Rust `Result<_, KafkaError>`).
    #[allow(unused_variables)]
    fn configure(&mut self, configs: &HashMap<String, String>, is_key: bool) -> Result<(), KafkaError> {
        // intentionally left blank — matches Java default impl
        Ok(())
    }

    /// Convert `data` into a byte vector.
    ///
    /// Returns `Ok(None)` for `None` input. Concrete impls that cannot fail
    /// still wrap their output in `Ok(_)` for trait uniformity.
    fn serialize(&self, topic: &str, data: Option<&T>) -> Result<Option<Vec<u8>>, KafkaError>;

    /// Convert `data` into bytes written directly to `out`. Returns `Ok(true)`
    /// if bytes were written, `Ok(false)` for `None` input (matching Java's
    /// `null` byte-array contract).
    ///
    /// Default impl calls `serialize` then `extend_from_slice` — concrete
    /// hot-path impls (e.g. `ByteArraySerializer`, `StringSerializer`,
    /// `IntegerSerializer`) override to avoid the intermediate allocation.
    fn serialize_to(&self, topic: &str, data: Option<&T>, out: &mut Vec<u8>) -> Result<bool, KafkaError> {
        match self.serialize(topic, data)? {
            Some(bytes) => {
                out.extend_from_slice(&bytes);
                Ok(true)
            },
            None => Ok(false),
        }
    }

    // Java's `default byte[] serialize(String, Headers, T)` overload defers
    // to the no-headers variant. We omit the headers overload from this
    // trait because (a) the `Headers` trait carries `&mut Self` builder
    // methods that prevent `dyn Headers` parameterization, and (b) every
    // Java concrete impl in scope just ignores the `Headers` argument and
    // forwards to the no-headers method. The producer send path
    // (Phase 5) will route headers through the record itself, not through
    // a serializer overload — so the contract is preserved at the higher
    // layer.

    /// Close this serializer. Idempotent. Default no-op.
    fn close(&mut self) {
        // intentionally left blank — matches Java default impl
    }
}
