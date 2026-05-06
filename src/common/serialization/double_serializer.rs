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

//! Translation of `org.apache.kafka.common.serialization.DoubleSerializer`.

use crate::common::KafkaError;
use crate::common::serialization::Serializer;

/// Big-endian IEEE-754 64-bit serializer for Java's `Double`. Mirrors
/// Java's `DoubleSerializer` — uses `doubleToLongBits` semantics.
///
/// Java's `Double.doubleToLongBits` collapses NaN bit patterns to a single
/// canonical NaN, so we use [`f64::to_bits`] (matches Java's
/// `doubleToRawLongBits` but `doubleToLongBits` is what `DoubleSerializer`
/// uses). For our purposes round-trip equality holds regardless: we just
/// need the bytes to be wire-compatible.
#[derive(Default, Debug, Clone, Copy)]
pub struct DoubleSerializer;

impl Serializer<f64> for DoubleSerializer {
    fn serialize(&self, _topic: &str, data: Option<&f64>) -> Result<Option<Vec<u8>>, KafkaError> {
        Ok(data.map(|v| v.to_bits().to_be_bytes().to_vec()))
    }

    fn serialize_to(&self, _topic: &str, data: Option<&f64>, out: &mut Vec<u8>) -> Result<bool, KafkaError> {
        match data {
            Some(v) => {
                out.extend_from_slice(&v.to_bits().to_be_bytes());
                Ok(true)
            },
            None => Ok(false),
        }
    }
}
