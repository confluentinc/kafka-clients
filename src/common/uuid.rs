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

//! Translation of `org.apache.kafka.common.Uuid`.
//!
//! Kafka's `Uuid` is a 128-bit value split into a most-significant-64-bit
//! field and a least-significant-64-bit field. The string form is the URL-safe
//! base64 encoding of the 16 raw bytes (no padding). Internally we delegate
//! random generation to the `uuid` crate but always store the two halves as
//! `i64` (signed) so that `Ord`/`compareTo` produces the same total order as
//! Java — see CLAUDE.md naming rule about `i64` for fields used in comparisons.
//!
//! `Uuid.compareTo` in Java compares `mostSignificantBits` then
//! `leastSignificantBits` as signed `long`; an unsigned representation
//! flips the ordering of values whose high bit is set.

use std::cmp::Ordering;
use std::fmt;

use base64::Engine;

/// Kafka's `Uuid`. Represents a 128-bit value as two signed 64-bit halves.
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub struct Uuid {
    most_significant_bits: i64,
    least_significant_bits: i64,
}

/// A reserved UUID. Will never be returned by [`Uuid::random`].
pub const ONE_UUID: Uuid = Uuid { most_significant_bits: 0, least_significant_bits: 1 };

/// A UUID for the metadata topic in KRaft mode. Same value as [`ONE_UUID`].
pub const METADATA_TOPIC_ID: Uuid = ONE_UUID;

/// A UUID that represents a null or empty UUID. Will never be returned by
/// [`Uuid::random`].
pub const ZERO_UUID: Uuid = Uuid { most_significant_bits: 0, least_significant_bits: 0 };

impl Uuid {
    /// Construct from explicit halves. Mirrors Java's
    /// `new Uuid(long mostSigBits, long leastSigBits)`.
    pub const fn new(most_significant_bits: i64, least_significant_bits: i64) -> Self {
        Uuid { most_significant_bits, least_significant_bits }
    }

    /// Returns the most significant bits of the UUID's 128 value.
    pub const fn most_significant_bits(&self) -> i64 {
        self.most_significant_bits
    }

    /// Returns the least significant bits of the UUID's 128 value.
    pub const fn least_significant_bits(&self) -> i64 {
        self.least_significant_bits
    }

    /// Static factory to retrieve a type 4 (pseudo-randomly generated) UUID.
    ///
    /// As in Java, this will not generate a UUID equal to 0, 1, or one whose
    /// string representation starts with a dash (`-`).
    pub fn random() -> Self {
        loop {
            let raw = ::uuid::Uuid::new_v4();
            let bytes = raw.as_bytes();
            let msb = i64::from_be_bytes(bytes[0..8].try_into().unwrap());
            let lsb = i64::from_be_bytes(bytes[8..16].try_into().unwrap());
            let candidate = Uuid::new(msb, lsb);
            if candidate == ZERO_UUID || candidate == ONE_UUID {
                continue;
            }
            if candidate.to_string().starts_with('-') {
                continue;
            }
            return candidate;
        }
    }

    /// Convert to the 16-byte big-endian byte representation (most-significant
    /// half first), matching Java's `getBytesFromUuid` private helper.
    pub fn to_bytes(self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[0..8].copy_from_slice(&self.most_significant_bits.to_be_bytes());
        out[8..16].copy_from_slice(&self.least_significant_bits.to_be_bytes());
        out
    }

    /// Parse a base64-URL-safe (no-padding) string into a UUID, mirroring
    /// `Uuid.fromString(String)` in Java.
    ///
    /// Returns an error if the input cannot be base64-decoded into exactly
    /// 16 bytes.
    pub fn from_string(s: &str) -> Result<Self, String> {
        // Java compares against `String.length()` (UTF-16 code units) and
        // slices via `substring(0, 24)`. Counting Rust `char`s and slicing on
        // char boundaries gives the closest equivalent and, importantly,
        // avoids panicking on non-ASCII input (which `&s[..24]` byte slicing
        // would do if byte 24 lands in the middle of a multi-byte UTF-8
        // character). Per CLAUDE.md rule 10.1, public API must not panic.
        if s.chars().take(25).count() > 24 {
            let prefix: String = s.chars().take(24).collect();
            return Err(format!(
                "Input string with prefix `{prefix}` is too long to be decoded as a base64 UUID"
            ));
        }
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(s.as_bytes())
            .map_err(|e| format!("Input string `{}` could not be base64-decoded: {}", s, e))?;
        if bytes.len() != 16 {
            return Err(format!(
                "Input string `{}` decoded as {} bytes, which is not equal to the expected 16 bytes \
                 of a base64-encoded UUID",
                s,
                bytes.len()
            ));
        }
        let msb = i64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let lsb = i64::from_be_bytes(bytes[8..16].try_into().unwrap());
        Ok(Uuid::new(msb, lsb))
    }

    /// Java-compatible hash code. Mirrors `Uuid.hashCode()`:
    ///
    /// ```text
    /// long xor = mostSignificantBits ^ leastSignificantBits;
    /// return (int) (xor >> 32) ^ (int) xor;
    /// ```
    ///
    /// Note: this is intentionally separate from Rust's [`std::hash::Hash`]
    /// derivation (which is non-deterministic and version-dependent). Use
    /// this method when matching Java's wire-visible hash contract; use
    /// `Hash` for `HashMap`/`HashSet` keys.
    pub const fn hash_code(&self) -> i32 {
        let xor = self.most_significant_bits ^ self.least_significant_bits;
        ((xor >> 32) as i32) ^ (xor as i32)
    }
}

impl fmt::Display for Uuid {
    /// Returns a base64 string encoding of the UUID.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.to_bytes();
        let s = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
        f.write_str(&s)
    }
}

impl fmt::Debug for Uuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl PartialOrd for Uuid {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Uuid {
    /// Matches Java's `Uuid.compareTo`: compare `mostSignificantBits` then
    /// `leastSignificantBits` as signed 64-bit integers.
    fn cmp(&self, other: &Self) -> Ordering {
        match self.most_significant_bits.cmp(&other.most_significant_bits) {
            Ordering::Equal => self.least_significant_bits.cmp(&other.least_significant_bits),
            non_equal => non_equal,
        }
    }
}

#[cfg(test)]
mod tests {
    // Translation of `org.apache.kafka.common.UuidTest`.

    use super::*;

    /// Java: `signifyingBitsTest`.
    #[test]
    fn signifying_bits() {
        let id = Uuid::new(34, 98);
        assert_eq!(id.most_significant_bits(), 34);
        assert_eq!(id.least_significant_bits(), 98);
    }

    /// Java: `equalsTest`.
    #[test]
    fn equals() {
        let id1 = Uuid::new(12, 13);
        let id2 = Uuid::new(12, 13);
        let id3 = Uuid::new(24, 38);
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    /// Java: `testStringConversion` — round-trips both a random UUID and
    /// `ZERO_UUID` via base64.
    #[test]
    fn to_string_round_trip() {
        let id = Uuid::random();
        let parsed = Uuid::from_string(&id.to_string()).unwrap();
        assert_eq!(id, parsed);

        // Java also exercises `ZERO_UUID.toString()` round-trip in the same
        // test. See `UuidTest.java:69-78`.
        assert_eq!(Uuid::from_string(&ZERO_UUID.to_string()).unwrap(), ZERO_UUID);
    }

    /// Java: `testHashCode` — explicit hash-code values are part of the
    /// wire-visible contract. See `UuidTest.java:57-66`.
    #[test]
    fn hash_code_matches_java() {
        let id1 = Uuid::new(16, 7);
        let id2 = Uuid::new(1043, 20075);
        let id3 = Uuid::new(104_312_423_523_523, 200_732_425_676_585);
        assert_eq!(id1.hash_code(), 23);
        assert_eq!(id2.hash_code(), 19064);
        assert_eq!(id3.hash_code(), -2_011_255_899);
    }

    /// Java: `fromStringTooLong`.
    #[test]
    fn from_string_too_long() {
        let too_long = "x".repeat(25);
        let err = Uuid::from_string(&too_long).unwrap_err();
        assert!(err.contains("too long"), "expected too-long message, got: {err}");
    }

    /// Non-ASCII input must not panic when computing the prefix slice. The
    /// 25-character input below is 100 bytes long (each emoji is 4 bytes), so
    /// the byte-indexed slice `&s[..24]` would panic in the middle of an
    /// emoji. The fix uses char-boundary slicing instead.
    #[test]
    fn from_string_non_ascii_too_long_does_not_panic() {
        let too_long: String = std::iter::repeat_n('🦀', 25).collect();
        let err = Uuid::from_string(&too_long).unwrap_err();
        assert!(err.contains("too long"), "expected too-long message, got: {err}");
    }

    /// Java: `fromStringWrongLength` — 16 base64 chars decode to 12 bytes.
    #[test]
    fn from_string_wrong_byte_length() {
        // 16 base64 url-safe characters decode to 12 bytes, which is not 16.
        // Use only valid base64-url chars to avoid the decode-error path.
        let s = "AAAAAAAAAAAAAAAA"; // 16 'A's -> 12 zero bytes
        let err = Uuid::from_string(s).unwrap_err();
        assert!(
            err.contains("not equal to the expected 16 bytes"),
            "expected length-mismatch message, got: {err}"
        );
    }

    /// Java: `randomDoesNotReturnReserved`.
    #[test]
    fn random_does_not_return_reserved() {
        for _ in 0..200 {
            let id = Uuid::random();
            assert_ne!(id, ZERO_UUID);
            assert_ne!(id, ONE_UUID);
            assert!(!id.to_string().starts_with('-'));
        }
    }

    /// Java: `compareTest` — verifies signed-comparison ordering: a UUID with
    /// negative MSB sorts before one with a non-negative MSB.
    #[test]
    fn compare_uses_signed_ordering() {
        let a = Uuid::new(i64::MIN, 0);
        let b = Uuid::new(0, 0);
        let c = Uuid::new(i64::MAX, i64::MAX);
        assert!(a < b);
        assert!(b < c);
        assert_eq!(a.cmp(&a), Ordering::Equal);

        // Same MSB, compare LSBs (signed).
        let d = Uuid::new(0, i64::MIN);
        let e = Uuid::new(0, 1);
        assert!(d < e);
    }

    #[test]
    fn to_bytes_is_big_endian() {
        let id = Uuid::new(0x0102_0304_0506_0708, 0x090a_0b0c_0d0e_0f10);
        let bytes = id.to_bytes();
        assert_eq!(
            bytes,
            [
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10
            ]
        );
    }
}
