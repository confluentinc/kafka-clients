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

//! Raw tagged field for forward compatibility.
//!
//! Corresponds to org.apache.kafka.common.protocol.types.RawTaggedField

/// Raw tagged field for forward compatibility.
/// Stores unknown tagged fields that can be passed through.
///
/// Corresponds to org.apache.kafka.common.protocol.types.RawTaggedField
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RawTaggedField {
    tag: u32,
    data: Vec<u8>,
}

impl RawTaggedField {
    /// Create a new raw tagged field.
    pub fn new(tag: u32, data: Vec<u8>) -> Self {
        RawTaggedField { tag, data }
    }

    /// Get the tag number.
    pub fn tag(&self) -> u32 {
        self.tag
    }

    /// Get the data bytes.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Get the size of the data.
    pub fn size(&self) -> usize {
        self.data.len()
    }
}
