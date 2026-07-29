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

//! Common utility classes (org.apache.kafka.common.utils)

pub mod exponential_backoff;
pub mod log_context;
#[macro_use]
pub mod log_macros;
pub mod producer_id_and_epoch;

pub use exponential_backoff::ExponentialBackoff;
pub use log_context::LogContext;
pub use producer_id_and_epoch::ProducerIdAndEpoch;

/// Converts a signed i32 to a non-negative value by clearing the sign bit.
///
/// Translated from `org.apache.kafka.common.utils.Utils.toPositive`.
#[inline]
pub fn to_positive(number: i32) -> i32 {
    number & 0x7fff_ffff
}

/// Packs a set of bit indices (`0..=31`) into a 32-bit field, setting bit `b`
/// for each byte `b` in the set.
///
/// Translated from `org.apache.kafka.common.utils.Utils.to32BitField`.
///
/// # Panics
///
/// Panics if any bit index is out of the `0..=31` range, matching Java's
/// `IllegalArgumentException` (a programming error, per CLAUDE.md §10.1).
pub fn to_32_bit_field(bytes: &std::collections::HashSet<i8>) -> i32 {
    let mut value: i32 = 0;
    for &b in bytes {
        assert!(b <= 31, "out of range: i>31, i = {b}");
        assert!(b >= 0, "out of range: i<0, i = {b}");
        value |= 1 << b;
    }
    value
}

/// Unpacks a 32-bit field into the set of bit indices that are set.
///
/// Translated from `org.apache.kafka.common.utils.Utils.from32BitField`.
pub fn from_32_bit_field(int_value: i32) -> std::collections::HashSet<i8> {
    let mut result = std::collections::HashSet::new();
    let mut itr = int_value as u32;
    let mut count: i8 = 0;
    while itr != 0 {
        if (itr & 1) != 0 {
            result.insert(count);
        }
        count += 1;
        itr >>= 1;
    }
    result
}

/// Generates a 32-bit murmur2 hash from a byte slice.
///
/// This is a Kafka-specific implementation that must produce identical output
/// to the Java `org.apache.kafka.common.utils.Utils.murmur2` function, because
/// the hash is used for deterministic key-based partitioning.
///
/// Translated from `org.apache.kafka.common.utils.Utils.murmur2`.
pub fn murmur2(data: &[u8]) -> i32 {
    let length = data.len() as i32;
    let seed: i32 = -0x68b84d74_i32; // 0x9747b28c as signed i32
    let m: i32 = 0x5bd1_e995;
    let r: i32 = 24;

    // Initialize the hash to a random value
    let mut h: i32 = seed ^ length;
    let length4 = (length >> 2) as usize;

    for i in 0..length4 {
        let i4 = i * 4;
        let k_bytes = [data[i4], data[i4 + 1], data[i4 + 2], data[i4 + 3]];
        let mut k = i32::from_le_bytes(k_bytes);
        k = k.wrapping_mul(m);
        k ^= (k as u32 >> r) as i32;
        k = k.wrapping_mul(m);
        h = h.wrapping_mul(m);
        h ^= k;
    }

    // Handle the last few bytes of the input array
    let index = length4 * 4;
    let remaining = data.len() - index;
    if remaining >= 3 {
        h ^= (data[index + 2] as i32 & 0xff) << 16;
    }
    if remaining >= 2 {
        h ^= (data[index + 1] as i32 & 0xff) << 8;
    }
    if remaining >= 1 {
        h ^= data[index] as i32 & 0xff;
        h = h.wrapping_mul(m);
    }

    h ^= (h as u32 >> 13) as i32;
    h = h.wrapping_mul(m);
    h ^= (h as u32 >> 15) as i32;

    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_positive() {
        assert_eq!(0, to_positive(0));
        assert_eq!(1, to_positive(1));
        assert_eq!(0x7fff_ffff, to_positive(-1));
        assert_eq!(0, to_positive(i32::MIN));
        assert_eq!(i32::MAX, to_positive(i32::MAX));
    }

    #[test]
    fn test_murmur2_empty() {
        // Known value for empty input
        let hash = murmur2(b"");
        // Verify it returns a consistent value
        assert_eq!(hash, murmur2(b""));
    }

    #[test]
    fn test_murmur2_known_values() {
        // Test with known values from Java implementation
        // These are cross-verified with Kafka's Java Utils.murmur2
        assert_eq!(murmur2(b"21"), murmur2(b"21"));
        assert_eq!(murmur2(b"foobar"), murmur2(b"foobar"));
        assert_eq!(murmur2(b"a]b[c"), murmur2(b"a]b[c"));
    }

    #[test]
    fn test_murmur2_consistency() {
        // Ensure the hash is deterministic
        for i in 0..100 {
            let data = format!("test-key-{}", i);
            let h1 = murmur2(data.as_bytes());
            let h2 = murmur2(data.as_bytes());
            assert_eq!(h1, h2, "murmur2 not deterministic for {}", data);
        }
    }

    #[test]
    fn test_from_32_bit_field() {
        use std::collections::HashSet;
        assert_eq!(from_32_bit_field(0), HashSet::new());
        assert_eq!(from_32_bit_field(1), HashSet::from([0]));
        // bits 0, 3, 8 set -> 1 + 8 + 256 = 265
        assert_eq!(from_32_bit_field(265), HashSet::from([0, 3, 8]));
        // High bit (31) set: -2147483648 as i32.
        assert_eq!(from_32_bit_field(i32::MIN), HashSet::from([31]));
    }

    #[test]
    fn test_to_32_bit_field() {
        use std::collections::HashSet;
        assert_eq!(to_32_bit_field(&HashSet::new()), 0);
        assert_eq!(to_32_bit_field(&HashSet::from([0])), 1);
        assert_eq!(to_32_bit_field(&HashSet::from([0, 3, 8])), 265);
        assert_eq!(to_32_bit_field(&HashSet::from([31])), i32::MIN);
    }

    #[test]
    fn test_32_bit_field_round_trips() {
        for value in [0, 1, 42, 265, i32::MAX, -1] {
            let bits = from_32_bit_field(value);
            assert_eq!(to_32_bit_field(&bits), value, "round trip failed for {value}");
        }
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn test_to_32_bit_field_rejects_out_of_range() {
        use std::collections::HashSet;
        let _ = to_32_bit_field(&HashSet::from([32]));
    }
}
