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

//! Translation of `org.apache.kafka.common.serialization.BytesDeserializer`.

use bytes::Bytes;

use crate::common::KafkaError;
use crate::common::serialization::Deserializer;

/// Identity deserializer for [`bytes::Bytes`]. Mirrors Java's
/// `BytesDeserializer implements Deserializer<Bytes>`.
#[derive(Default, Debug, Clone, Copy)]
pub struct BytesDeserializer;

impl Deserializer<Bytes> for BytesDeserializer {
    fn deserialize(&self, _topic: &str, data: Option<&[u8]>) -> Result<Option<Bytes>, KafkaError> {
        Ok(data.map(Bytes::copy_from_slice))
    }
}
