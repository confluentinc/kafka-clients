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

//! Translation of `org.apache.kafka.common.serialization.VoidSerializer`.

use crate::common::KafkaError;
use crate::common::serialization::Serializer;

/// Serializer that always produces `null` bytes regardless of input.
/// Mirrors Java's `VoidSerializer` — Java accepts `Void data` (which is
/// always null) and returns `null`.
///
/// In Rust, Java's `Void` maps to `()`. We accept `Option<&()>` to match
/// the `Serializer<T>` trait shape, but always return `Ok(None)` —
/// matching Java's "always return null" behavior.
#[derive(Default, Debug, Clone, Copy)]
pub struct VoidSerializer;

impl Serializer<()> for VoidSerializer {
    fn serialize(&self, _topic: &str, _data: Option<&()>) -> Result<Option<Vec<u8>>, KafkaError> {
        Ok(None)
    }

    fn serialize_to(&self, _topic: &str, _data: Option<&()>, _out: &mut Vec<u8>) -> Result<bool, KafkaError> {
        Ok(false)
    }
}
