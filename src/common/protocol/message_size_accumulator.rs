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

//! Message size accumulator for two-pass serialization.
//!
//! Helper class which facilitates zero-copy network transmission.
//!
//! Corresponds to org.apache.kafka.common.protocol.MessageSizeAccumulator

/// Accumulates message size with zero-copy optimization.
///
/// Tracks both the total size and the zero-copy size separately,
/// enabling efficient network transmission by distinguishing between
/// bytes that need to be copied into a buffer and bytes that can be
/// sent directly (zero-copy).
#[derive(Debug, Default)]
pub struct MessageSizeAccumulator {
    total_size: i32,
    zero_copy_size: i32,
}

impl MessageSizeAccumulator {
    /// Creates a new empty accumulator.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the total size of the message.
    pub fn total_size(&self) -> i32 {
        self.total_size
    }

    /// Size excluding zero copy fields as specified by [`zero_copy_size`].
    /// This is typically the size of the byte buffer used to serialize messages.
    pub fn size_excluding_zero_copy(&self) -> i32 {
        self.total_size - self.zero_copy_size
    }

    /// Add zero-copy bytes to the accumulator.
    pub fn add_zero_copy_bytes(&mut self, size: i32) {
        self.zero_copy_size += size;
        self.total_size += size;
    }

    /// Add regular (non-zero-copy) bytes to the accumulator.
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
    fn test_new_accumulator_is_zero() {
        let acc = MessageSizeAccumulator::new();
        assert_eq!(acc.total_size(), 0);
        assert_eq!(acc.size_excluding_zero_copy(), 0);
    }

    #[test]
    fn test_add_bytes() {
        let mut acc = MessageSizeAccumulator::new();
        acc.add_bytes(100);
        assert_eq!(acc.total_size(), 100);
        assert_eq!(acc.size_excluding_zero_copy(), 100);
    }

    #[test]
    fn test_add_zero_copy_bytes() {
        let mut acc = MessageSizeAccumulator::new();
        acc.add_zero_copy_bytes(50);
        assert_eq!(acc.total_size(), 50);
        assert_eq!(acc.size_excluding_zero_copy(), 0);
    }

    #[test]
    fn test_mixed_bytes() {
        let mut acc = MessageSizeAccumulator::new();
        acc.add_bytes(100);
        acc.add_zero_copy_bytes(50);
        assert_eq!(acc.total_size(), 150);
        assert_eq!(acc.size_excluding_zero_copy(), 100);
    }

    #[test]
    fn test_add_accumulator() {
        let mut acc1 = MessageSizeAccumulator::new();
        acc1.add_bytes(100);
        acc1.add_zero_copy_bytes(50);

        let mut acc2 = MessageSizeAccumulator::new();
        acc2.add_bytes(200);
        acc2.add_zero_copy_bytes(75);

        acc1.add(&acc2);
        assert_eq!(acc1.total_size(), 425);
        assert_eq!(acc1.size_excluding_zero_copy(), 300);
    }
}
