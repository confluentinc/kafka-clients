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

//! Translation of `org.apache.kafka.common.protocol.types.RawTaggedField`.

/// A tagged field whose declared schema is unknown to the reader. Mirrors
/// `org.apache.kafka.common.protocol.types.RawTaggedField`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawTaggedField {
    tag: i32,
    data: Vec<u8>,
}

impl RawTaggedField {
    /// Construct a `RawTaggedField`. Mirrors `new RawTaggedField(int, byte[])`.
    pub fn new(tag: i32, data: Vec<u8>) -> Self {
        RawTaggedField { tag, data }
    }

    /// Tag identifier. Mirrors `RawTaggedField#tag`.
    pub fn tag(&self) -> i32 {
        self.tag
    }

    /// Field bytes. Mirrors `RawTaggedField#data`.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Length of `data()`. Mirrors `RawTaggedField#size`.
    pub fn size(&self) -> usize {
        self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equality_and_size() {
        let a = RawTaggedField::new(7, vec![1, 2, 3]);
        let b = RawTaggedField::new(7, vec![1, 2, 3]);
        let c = RawTaggedField::new(7, vec![1, 2, 4]);
        let d = RawTaggedField::new(8, vec![1, 2, 3]);
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_ne!(a, d);
        assert_eq!(a.size(), 3);
        assert_eq!(a.tag(), 7);
        assert_eq!(a.data(), &[1, 2, 3]);
    }
}
