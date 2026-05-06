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

//! Translation of `org.apache.kafka.common.serialization.Serde`.

use std::collections::HashMap;

use crate::common::KafkaError;
use crate::common::serialization::deserializer::Deserializer;
use crate::common::serialization::serializer::Serializer;

/// Wraps a serializer and deserializer for a given data type.
///
/// Mirrors Java's `Serde<T>` interface. Returns `&dyn` references so a
/// caller holding `&dyn Serde<T>` can route into either side without
/// generic dispatch.
pub trait Serde<T>: Send + Sync {
    /// Configure this serde, propagating to the underlying serializer
    /// and deserializer.
    #[allow(unused_variables)]
    fn configure(&mut self, configs: &HashMap<String, String>, is_key: bool) -> Result<(), KafkaError> {
        Ok(())
    }

    /// Close this serde. Idempotent.
    fn close(&mut self) {}

    /// Get the serializer.
    fn serializer(&self) -> &dyn Serializer<T>;

    /// Get the deserializer.
    fn deserializer(&self) -> &dyn Deserializer<T>;
}
