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

//! Translation of `org.apache.kafka.common.utils.Crc32C`.
//!
//! Computes the CRC-32C (Castagnoli) checksum used by Kafka v2 record
//! batches. We delegate to the `crc32c` crate (a zero-dependency, SSE 4.2 /
//! ARMv8 hardware-accelerated implementation that matches RFC 3720).
//!
//! The Java API exposes a stateful `Checksum` object plus the helper class
//! `Checksums` (`update(Checksum, ByteBuffer, int)`, `updateInt`, `updateLong`,
//! `update(Checksum, ByteBuffer, int, int)`). We collapse the static helpers
//! into a single [`Crc32C`] type with the same semantics — `compute(...)`
//! mirrors `Crc32C.compute`, and the additional [`Crc32C::update_int`] and
//! [`Crc32C::update_long`] helpers mirror the `Checksums` class so we can
//! incrementally checksum a record batch in-place without first writing the
//! header bytes elsewhere.

/// Streaming CRC-32C (Castagnoli) accumulator. Equivalent to `java.util.zip.CRC32C`.
#[derive(Clone, Default)]
pub struct Crc32C {
    state: u32,
}

impl Crc32C {
    /// Construct an accumulator with the canonical seed (`0`).
    pub fn new() -> Self {
        Crc32C { state: 0 }
    }

    /// One-shot helper: compute the CRC-32C of `bytes[offset..offset + size]`.
    /// Mirrors `Crc32C.compute(byte[], int, int)`.
    pub fn compute(bytes: &[u8], offset: usize, size: usize) -> u32 {
        crc32c::crc32c(&bytes[offset..offset + size])
    }

    /// Update the running checksum with the bytes in `data`. Mirrors
    /// `Checksum.update(byte[], int, int)` after slicing.
    pub fn update(&mut self, data: &[u8]) {
        self.state = crc32c::crc32c_append(self.state, data);
    }

    /// Update the running checksum with the big-endian bytes of `value` —
    /// mirrors `Checksums.updateInt(Checksum, int)`.
    pub fn update_int(&mut self, value: i32) {
        self.update(&value.to_be_bytes());
    }

    /// Update the running checksum with the big-endian bytes of `value` —
    /// mirrors `Checksums.updateLong(Checksum, long)`.
    pub fn update_long(&mut self, value: i64) {
        self.update(&value.to_be_bytes());
    }

    /// Returns the running CRC value as an unsigned 32-bit integer.
    /// Mirrors `Checksum.getValue()` (the Java return type is `long` but the
    /// upper 32 bits are always zero for `CRC32C`; producer wire writes use
    /// the lower 32 bits).
    pub fn value(&self) -> u32 {
        self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Java: `Crc32CTest#testValue`.
    #[test]
    fn crc32c_test_value() {
        let bytes = b"Some String";
        assert_eq!(Crc32C::compute(bytes, 0, bytes.len()), 608_512_271);
    }

    /// Java: `ChecksumsTest#testUpdateByteBuffer`. We don't have `ByteBuffer`
    /// — instead we exercise the equivalence between `compute(slice)` and
    /// `update(slice)` followed by `value()` so the streaming form matches
    /// the one-shot form.
    #[test]
    fn streaming_matches_one_shot() {
        let bytes = [0u8, 1, 2, 3, 4, 5];
        let mut crc = Crc32C::new();
        crc.update(&bytes);
        assert_eq!(crc.value(), Crc32C::compute(&bytes, 0, bytes.len()));
    }

    /// Java: `ChecksumsTest#testUpdateByteBufferWithOffsetPosition`. Verifies
    /// that streaming over a sub-slice equals computing on that sub-slice.
    #[test]
    fn streaming_with_offset() {
        let bytes = [(-2i8) as u8, (-1i8) as u8, 0, 1, 2, 3, 4, 5];
        let offset = 2;
        let mut crc = Crc32C::new();
        crc.update(&bytes[offset..]);
        assert_eq!(crc.value(), Crc32C::compute(&bytes, offset, bytes.len() - offset));
    }

    /// Java: `ChecksumsTest#testUpdateInt`.
    #[test]
    fn update_int_matches_be_bytes() {
        let value: i32 = 1000;
        let mut crc1 = Crc32C::new();
        crc1.update_int(value);
        let mut crc2 = Crc32C::new();
        crc2.update(&value.to_be_bytes());
        assert_eq!(crc1.value(), crc2.value());
    }

    /// Java: `ChecksumsTest#testUpdateLong`.
    #[test]
    fn update_long_matches_be_bytes() {
        let value: i64 = (i32::MAX as i64) + 1;
        let mut crc1 = Crc32C::new();
        crc1.update_long(value);
        let mut crc2 = Crc32C::new();
        crc2.update(&value.to_be_bytes());
        assert_eq!(crc1.value(), crc2.value());
    }
}
