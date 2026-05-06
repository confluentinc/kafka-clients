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

//! Translation of
//! `org.apache.kafka.common.protocol.MessageSizeAccumulator`.

/// Helper class which facilitates zero-copy network transmission. See
/// [`crate::common::protocol::SendBuilder`].
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MessageSizeAccumulator {
    total_size: i32,
    zero_copy_size: i32,
}

impl MessageSizeAccumulator {
    /// Construct an empty accumulator. Mirrors the implicit `new
    /// MessageSizeAccumulator()`.
    pub fn new() -> Self {
        MessageSizeAccumulator::default()
    }

    /// Get the total size of the message.
    pub fn total_size(&self) -> i32 {
        self.total_size
    }

    /// Size excluding zero-copy fields. Typically the size of the byte buffer
    /// used to serialise messages.
    pub fn size_excluding_zero_copy(&self) -> i32 {
        self.total_size - self.zero_copy_size
    }

    /// Get the zero-copy portion. Mirrors no Java getter directly but is
    /// useful for tests.
    pub fn zero_copy_size(&self) -> i32 {
        self.zero_copy_size
    }

    /// Add `size` zero-copy bytes. Both the zero-copy and total counters
    /// advance.
    pub fn add_zero_copy_bytes(&mut self, size: i32) {
        self.zero_copy_size += size;
        self.total_size += size;
    }

    /// Add `size` regular (non zero-copy) bytes.
    pub fn add_bytes(&mut self, size: i32) {
        self.total_size += size;
    }

    /// Merge another accumulator into this one.
    pub fn add(&mut self, other: &MessageSizeAccumulator) {
        self.total_size += other.total_size;
        self.zero_copy_size += other.zero_copy_size;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_sizes() {
        let acc = MessageSizeAccumulator::new();
        assert_eq!(acc.total_size(), 0);
        assert_eq!(acc.size_excluding_zero_copy(), 0);
        assert_eq!(acc.zero_copy_size(), 0);
    }

    #[test]
    fn add_bytes_only_grows_total() {
        let mut acc = MessageSizeAccumulator::new();
        acc.add_bytes(7);
        assert_eq!(acc.total_size(), 7);
        assert_eq!(acc.size_excluding_zero_copy(), 7);
    }

    #[test]
    fn zero_copy_grows_both() {
        let mut acc = MessageSizeAccumulator::new();
        acc.add_zero_copy_bytes(10);
        assert_eq!(acc.total_size(), 10);
        assert_eq!(acc.size_excluding_zero_copy(), 0);
    }

    #[test]
    fn add_merges() {
        let mut a = MessageSizeAccumulator::new();
        a.add_bytes(3);
        a.add_zero_copy_bytes(5);
        let mut b = MessageSizeAccumulator::new();
        b.add_bytes(7);
        b.add_zero_copy_bytes(11);
        a.add(&b);
        assert_eq!(a.total_size(), 3 + 5 + 7 + 11);
        assert_eq!(a.zero_copy_size(), 5 + 11);
    }
}
