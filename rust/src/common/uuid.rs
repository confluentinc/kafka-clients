/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::cmp::Ordering;

use crate::common::Error;

/// This class defines an immutable universally unique identifier (UUID).
/// It represents a 128-bit value.
///
/// The toString() method prints using base64 URL encoding without padding (matching Java's implementation).
/// Likewise, the from_string method expects a base64 URL encoded string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[doc(alias = "org.apache.kafka.common.Uuid")]
pub struct Uuid {
    /// The most significant 64 bits of the UUID
    most_sig_bits: u64,
    /// The least significant 64 bits of the UUID
    least_sig_bits: u64,
}

impl Uuid {
    /// A UUID that represents a null or empty UUID. Will never be returned by random_uuid.
    pub const ZERO_UUID: Uuid = Uuid { most_sig_bits: 0, least_sig_bits: 0 };

    /// A reserved UUID. Will never be returned by random_uuid.
    pub const ONE_UUID: Uuid = Uuid { most_sig_bits: 0, least_sig_bits: 1 };

    /// A UUID for the metadata topic in KRaft mode. Will never be returned by random_uuid.
    pub const METADATA_TOPIC_ID: Uuid = Self::ONE_UUID;

    /// Constructs a 128-bit UUID where the first u64 represents the most significant 64 bits
    /// and the second u64 represents the least significant 64 bits.
    #[doc(alias = "org.apache.kafka.common.Uuid#Uuid")]
    pub const fn new(most_sig_bits: u64, least_sig_bits: u64) -> Self {
        Uuid { most_sig_bits, least_sig_bits }
    }

    /// Creates a zero UUID (all bits are zero).
    pub(crate) const fn zero() -> Self {
        Self::ZERO_UUID
    }

    /// Static factory to retrieve a type 4 (pseudo randomly generated) UUID.
    ///
    /// This will not generate a UUID equal to `ZERO_UUID`, `ONE_UUID`, or one whose
    /// string representation contains a dash ("-").
    #[doc(alias = "org.apache.kafka.common.Uuid#randomUuid")]
    pub fn random_uuid() -> Self {
        loop {
            let ju = uuid::Uuid::new_v4();
            let (most, least) = ju.as_u64_pair();
            let uuid = Self::new(most, least);
            if uuid == Self::ZERO_UUID || uuid == Self::ONE_UUID {
                continue;
            }
            if uuid.to_base64_string().contains('-') {
                continue;
            }
            return uuid;
        }
    }

    /// Returns the most significant 64 bits of this UUID.
    #[doc(alias = "org.apache.kafka.common.Uuid#getMostSignificantBits")]
    pub const fn most_significant_bits(&self) -> u64 {
        self.most_sig_bits
    }

    /// Returns the least significant 64 bits of this UUID.
    #[doc(alias = "org.apache.kafka.common.Uuid#getLeastSignificantBits")]
    pub const fn least_significant_bits(&self) -> u64 {
        self.least_sig_bits
    }

    /// Checks if this is the zero UUID (all bits are zero).
    #[cfg(test)]
    pub(crate) const fn is_zero(&self) -> bool {
        self.most_sig_bits == 0 && self.least_sig_bits == 0
    }

    /// Creates a UUID from a 16-byte array in big-endian order.
    ///
    /// Java has no such constructor (only `Uuid(long, long)`); this decodes
    /// `fromString`'s bytes, so it is crate-private.
    pub(crate) fn with_bytes(bytes: [u8; 16]) -> Self {
        let most_sig_bits = u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);
        let least_sig_bits = u64::from_be_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]);
        Uuid::new(most_sig_bits, least_sig_bits)
    }

    /// Converts this UUID to a 16-byte array in big-endian order.
    pub(crate) fn to_bytes(self) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&self.most_sig_bits.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.least_sig_bits.to_be_bytes());
        bytes
    }

    /// Creates a UUID based on a base64 URL encoded string (without padding).
    /// This matches the Java implementation's fromString() method.
    ///
    /// Java throws `IllegalArgumentException` for a string that is not a
    /// base64 UUID; this returns the corresponding `LocalIllegalArgument`
    /// error, with Java's messages.
    ///
    /// As in Java, the length is counted in UTF-16 units and the prefix in the
    /// "too long" message is its first 24 of them. One deviation: when that cut
    /// splits a surrogate pair Java's message ends in a lone surrogate, which a
    /// Rust `String` cannot hold, so it ends in U+FFFD.
    #[doc(alias = "org.apache.kafka.common.Uuid#fromString")]
    pub fn from_string(s: &str) -> Result<Self, Error> {
        // Java's `str.length()` counts UTF-16 units, not the UTF-8 bytes `str::len` counts.
        if s.encode_utf16().count() > 24 {
            // `str.substring(0, 24)` takes 24 UTF-16 units, which can cut a surrogate pair in
            // two; a lone surrogate cannot be held by a Rust `String`, so it becomes U+FFFD.
            let prefix: Vec<u16> = s.encode_utf16().take(24).collect();
            return Err(Error::local_illegal_argument(format!(
                "Input string with prefix `{}` is too long to be decoded as a base64 UUID",
                String::from_utf16_lossy(&prefix)
            )));
        }

        // Decode base64 URL without padding
        let decoded = base64_url_decode(s).map_err(Error::local_illegal_argument)?;

        if decoded.len() != 16 {
            return Err(Error::local_illegal_argument(format!(
                "Input string `{}` decoded as {} bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID",
                s,
                decoded.len()
            )));
        }

        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&decoded);
        Ok(Self::with_bytes(bytes))
    }

    /// Returns a base64 URL encoded string (without padding) of the UUID.
    /// This matches the Java implementation's toString() method.
    pub(crate) fn to_base64_string(self) -> String {
        let bytes = self.to_bytes();
        base64_url_encode(&bytes)
    }
}

/// Base64 URL encode without padding (matches Java's Base64.getUrlEncoder().withoutPadding())
fn base64_url_encode(data: &[u8]) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

    let mut result = String::new();
    let mut i = 0;

    while i + 2 < data.len() {
        let b1 = data[i];
        let b2 = data[i + 1];
        let b3 = data[i + 2];

        result.push(CHARSET[(b1 >> 2) as usize] as char);
        result.push(CHARSET[(((b1 & 0x03) << 4) | (b2 >> 4)) as usize] as char);
        result.push(CHARSET[(((b2 & 0x0f) << 2) | (b3 >> 6)) as usize] as char);
        result.push(CHARSET[(b3 & 0x3f) as usize] as char);

        i += 3;
    }

    // Handle remaining bytes
    if i < data.len() {
        let b1 = data[i];
        result.push(CHARSET[(b1 >> 2) as usize] as char);

        if i + 1 < data.len() {
            let b2 = data[i + 1];
            result.push(CHARSET[(((b1 & 0x03) << 4) | (b2 >> 4)) as usize] as char);
            result.push(CHARSET[((b2 & 0x0f) << 2) as usize] as char);
        } else {
            result.push(CHARSET[((b1 & 0x03) << 4) as usize] as char);
        }
    }

    result
}

/// Base64 URL decode (matches Java's Base64.getUrlDecoder())
fn base64_url_decode(s: &str) -> Result<Vec<u8>, String> {
    let mut result = Vec::new();
    // `Base64.Decoder.decode(String)` reads the string as ISO-8859-1 (`src.getBytes(ISO_8859_1)`):
    // a char it cannot encode, above U+00FF, becomes `?`.
    let bytes: Vec<u8> = s.chars().map(|c| u8::try_from(u32::from(c)).unwrap_or(b'?')).collect();
    let mut i = 0;

    while i < bytes.len() {
        let mut buf = [0u8; 4];
        let mut buf_len = 0;

        // Collect up to 4 valid base64 characters
        while buf_len < 4 && i < bytes.len() {
            let c = bytes[i];
            i += 1;

            let val = match c {
                b'A'..=b'Z' => c - b'A',
                b'a'..=b'z' => c - b'a' + 26,
                b'0'..=b'9' => c - b'0' + 52,
                b'-' => 62,
                b'_' => 63,
                b'=' => break, // Padding character (though we don't use it)
                // Java: `"Illegal base64 character " + Integer.toString(src[sp - 1], 16)`, where
                // `src[sp - 1]` is a signed `byte`.
                _ => {
                    let signed = c as i8;
                    return Err(if signed < 0 {
                        format!("Illegal base64 character -{:x}", -i32::from(signed))
                    } else {
                        format!("Illegal base64 character {signed:x}")
                    });
                },
            };

            buf[buf_len] = val;
            buf_len += 1;
        }

        if buf_len >= 2 {
            result.push((buf[0] << 2) | (buf[1] >> 4));
        }
        if buf_len >= 3 {
            result.push((buf[1] << 4) | (buf[2] >> 2));
        }
        if buf_len >= 4 {
            result.push((buf[2] << 6) | buf[3]);
        }
    }

    Ok(result)
}

impl Default for Uuid {
    fn default() -> Self {
        Uuid::zero()
    }
}

impl std::fmt::Display for Uuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_base64_string())
    }
}

impl PartialOrd for Uuid {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Uuid {
    /// Compares two UUIDs using signed 64-bit comparison to match Java's
    /// `Uuid.compareTo()` which compares `long` (signed) fields.
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.most_sig_bits as i64).cmp(&(other.most_sig_bits as i64)) {
            Ordering::Equal => (self.least_sig_bits as i64).cmp(&(other.least_sig_bits as i64)),
            ord => ord,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_uuid() {
        let uuid = Uuid::zero();
        assert_eq!(uuid.most_significant_bits(), 0);
        assert_eq!(uuid.least_significant_bits(), 0);
        assert!(uuid.is_zero());
        assert_eq!(uuid, Uuid::ZERO_UUID);
    }

    #[test]
    fn test_one_uuid() {
        let uuid = Uuid::ONE_UUID;
        assert_eq!(uuid.most_significant_bits(), 0);
        assert_eq!(uuid.least_significant_bits(), 1);
        assert!(!uuid.is_zero());
    }

    #[test]
    fn test_new_uuid() {
        let uuid = Uuid::new(0x0123456789ABCDEF, 0xFEDCBA9876543210);
        assert_eq!(uuid.most_significant_bits(), 0x0123456789ABCDEF);
        assert_eq!(uuid.least_significant_bits(), 0xFEDCBA9876543210);
        assert!(!uuid.is_zero());
    }

    #[test]
    fn test_new_bytes() {
        let bytes = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10,
        ];
        let uuid = Uuid::with_bytes(bytes);
        assert_eq!(uuid.most_significant_bits(), 0x0123456789ABCDEF);
        assert_eq!(uuid.least_significant_bits(), 0xFEDCBA9876543210);
    }

    #[test]
    fn test_to_bytes() {
        let uuid = Uuid::new(0x0123456789ABCDEF, 0xFEDCBA9876543210);
        let bytes = uuid.to_bytes();
        assert_eq!(
            bytes,
            [
                0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10,
            ]
        );
    }

    #[test]
    fn test_to_string_base64() {
        // Test zero UUID
        let uuid = Uuid::ZERO_UUID;
        assert_eq!(uuid.to_string(), "AAAAAAAAAAAAAAAAAAAAAA");

        // Test one UUID - verified against Java implementation
        let uuid = Uuid::ONE_UUID;
        assert_eq!(uuid.to_string(), "AAAAAAAAAAAAAAAAAAAAAQ");

        // Test a known UUID
        let uuid = Uuid::new(0x0123456789ABCDEF, 0xFEDCBA9876543210);
        let s = uuid.to_string();
        // Verify it's base64 URL encoded (22 chars for 16 bytes without padding)
        assert_eq!(s.len(), 22);
        assert!(s.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_'));
    }

    #[test]
    fn test_from_string_base64() {
        // Test zero UUID
        let uuid = Uuid::from_string("AAAAAAAAAAAAAAAAAAAAAA").unwrap();
        assert_eq!(uuid, Uuid::ZERO_UUID);

        // Test one UUID - verified against Java implementation
        let uuid = Uuid::from_string("AAAAAAAAAAAAAAAAAAAAAQ").unwrap();
        assert_eq!(uuid, Uuid::ONE_UUID);
    }

    #[test]
    fn test_round_trip() {
        let original = Uuid::new(0x0123456789ABCDEF, 0xFEDCBA9876543210);
        let s = original.to_string();
        let parsed = Uuid::from_string(&s).unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn test_bytes_round_trip() {
        let original = Uuid::new(0x0123456789ABCDEF, 0xFEDCBA9876543210);
        let bytes = original.to_bytes();
        let parsed = Uuid::with_bytes(bytes);
        assert_eq!(original, parsed);
    }

    #[test]
    fn test_default() {
        let uuid = Uuid::default();
        assert!(uuid.is_zero());
        assert_eq!(uuid, Uuid::ZERO_UUID);
    }

    #[test]
    fn test_ord() {
        let uuid1 = Uuid::new(1, 0);
        let uuid2 = Uuid::new(2, 0);
        let uuid3 = Uuid::new(1, 1);

        assert!(uuid1 < uuid2);
        assert!(uuid1 < uuid3);
        assert!(uuid3 < uuid2);
    }

    /// Translated from Java UuidTest.testHashCode.
    /// Java uses a custom hashCode (msb ^ lsb), Rust uses derive(Hash).
    /// We verify hash consistency: equal UUIDs produce equal hashes,
    /// different UUIDs produce different hashes.
    #[test]
    #[doc(alias = "org.apache.kafka.common.UuidTest#testHashCode")]
    fn test_hash_code() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash_uuid(uuid: &Uuid) -> u64 {
            let mut hasher = DefaultHasher::new();
            uuid.hash(&mut hasher);
            hasher.finish()
        }

        let id1 = Uuid::new(16, 7);
        let id2 = Uuid::new(1043, 20075);
        let id3 = Uuid::new(104312423523523, 200732425676585);

        // Equal UUIDs produce equal hashes
        assert_eq!(hash_uuid(&id1), hash_uuid(&Uuid::new(16, 7)));
        assert_eq!(hash_uuid(&id2), hash_uuid(&Uuid::new(1043, 20075)));
        assert_eq!(hash_uuid(&id3), hash_uuid(&Uuid::new(104312423523523, 200732425676585)));

        // Different UUIDs produce different hashes (not guaranteed in general,
        // but these specific values should differ)
        assert_ne!(hash_uuid(&id1), hash_uuid(&id2));
        assert_ne!(hash_uuid(&id1), hash_uuid(&id3));
        assert_ne!(hash_uuid(&id2), hash_uuid(&id3));
    }

    /// Translated from Java UuidTest.testRandomUuid (RepeatedTest(100)).
    /// Verifies that random UUIDs are not ZERO_UUID, not METADATA_TOPIC_ID,
    /// and contain no dash (KAFKA-20072).
    #[test]
    #[doc(alias = "org.apache.kafka.common.UuidTest#testRandomUuid")]
    fn test_random_uuid() {
        for _ in 0..100 {
            let random_id = Uuid::random_uuid();
            assert_ne!(Uuid::ZERO_UUID, random_id);
            assert_ne!(Uuid::METADATA_TOPIC_ID, random_id);
            assert!(!random_id.to_string().contains('-'));
        }
    }

    /// Translated from Java UuidTest.testCompareUuids.
    /// Verifies all 9 comparison combinations of UUIDs (0,0), (0,1), (1,0).
    #[test]
    #[doc(alias = "org.apache.kafka.common.UuidTest#testCompareUuids")]
    fn test_compare_uuids() {
        let id00 = Uuid::new(0, 0);
        let id01 = Uuid::new(0, 1);
        let id10 = Uuid::new(1, 0);

        assert_eq!(Ordering::Equal, id00.cmp(&id00));
        assert_eq!(Ordering::Equal, id01.cmp(&id01));
        assert_eq!(Ordering::Equal, id10.cmp(&id10));
        assert_eq!(Ordering::Less, id00.cmp(&id01));
        assert_eq!(Ordering::Less, id00.cmp(&id10));
        assert_eq!(Ordering::Greater, id01.cmp(&id00));
        assert_eq!(Ordering::Greater, id10.cmp(&id00));
        assert_eq!(Ordering::Less, id01.cmp(&id10));
        assert_eq!(Ordering::Greater, id10.cmp(&id01));
    }

    /// Verifies that signed comparison matches Java behavior for UUIDs
    /// with high bits set (values that would be negative as Java long).
    #[test]
    fn test_compare_uuids_signed() {
        // 0xFFFFFFFFFFFFFFFF as i64 is -1, which should be less than 1 (signed comparison)
        let uuid_neg = Uuid::new(0xFFFFFFFFFFFFFFFF, 0);
        let uuid_pos = Uuid::new(1, 0);
        assert!(uuid_neg < uuid_pos, "UUID with high MSB bit set should compare less (signed)");

        // Same for least significant bits
        let uuid_neg_lsb = Uuid::new(0, 0xFFFFFFFFFFFFFFFF);
        let uuid_pos_lsb = Uuid::new(0, 1);
        assert!(
            uuid_neg_lsb < uuid_pos_lsb,
            "UUID with high LSB bit set should compare less (signed)"
        );
    }

    #[test]
    fn test_from_string_too_long() {
        let result = Uuid::from_string("AAAAAAAAAAAAAAAAAAAAAA_THIS_IS_TOO_LONG");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.is_local_illegal_argument_error());
        assert!(error.to_string().contains("too long"));
    }

    #[test]
    fn test_from_string_invalid_length() {
        let result = Uuid::from_string("AAAA");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.is_local_illegal_argument_error());
        assert!(error.to_string().contains("not equal to the expected 16 bytes"));
    }

    /// The expected messages below are what Java's `Uuid.fromString` throws, observed by
    /// running it (`Base64.getUrlDecoder().decode(String)` reads the string as ISO-8859-1,
    /// so a char above U+00FF is `?`, 0x3f, and a Latin-1 char is its *signed* byte).
    fn from_string_message(s: &str) -> String {
        let error = Uuid::from_string(s).expect_err("not a base64 UUID");
        assert!(
            error.is_local_illegal_argument_error(),
            "Java throws IllegalArgumentException: {error}"
        );
        error.message().to_string()
    }

    /// The length check counted UTF-8 bytes where Java's `String.length()` counts
    /// UTF-16 units, and `&s[..24]` is a byte slice, so a multi-byte character straddling
    /// byte 24 panicked instead of throwing `IllegalArgumentException`:
    /// `Uuid.fromString("a€€€€€€€€")` is 9 chars to Java, which fails to decode them.
    #[test]
    fn test_from_string_multibyte_input_is_an_error_not_a_panic() {
        assert_eq!(
            "Illegal base64 character 3f",
            from_string_message("a\u{20ac}\u{20ac}\u{20ac}\u{20ac}\u{20ac}\u{20ac}\u{20ac}\u{20ac}")
        );
    }

    /// 8 chars in 24 bytes, and 13 chars in 26 bytes: Java measures chars, so neither is too long.
    #[test]
    fn test_from_string_length_is_counted_in_utf16_units_not_bytes() {
        assert_eq!("Illegal base64 character 3f", from_string_message(&"\u{20ac}".repeat(8)));
        assert_eq!("Illegal base64 character -17", from_string_message(&"\u{e9}".repeat(13)));
    }

    /// 25 UTF-16 units are too long, and the prefix of the message is the first 24 of them
    /// (`str.substring(0, 24)`), here ending in a whole `€`.
    #[test]
    fn test_from_string_too_long_prefix_is_24_utf16_units() {
        assert_eq!(
            "Input string with prefix `AAAAAAAAAAAAAAAAAAAAAAA\u{20ac}` is too long to be decoded as a base64 UUID",
            from_string_message(&format!("{}\u{20ac}\u{20ac}", "A".repeat(23)))
        );
    }

    /// `substring(0, 24)` can cut a surrogate pair in two, leaving a lone surrogate in the
    /// message, which a Rust `String` cannot hold: it becomes U+FFFD.
    #[test]
    fn test_from_string_too_long_prefix_cutting_a_surrogate_pair() {
        assert_eq!(
            "Input string with prefix `AAAAAAAAAAAAAAAAAAAAAAA\u{fffd}` is too long to be decoded as a base64 UUID",
            from_string_message(&format!("{}\u{1f600}", "A".repeat(23)))
        );
    }

    /// An ASCII character that is not in the URL-safe alphabet is reported as Java reports it.
    #[test]
    fn test_from_string_illegal_base64_character() {
        assert_eq!("Illegal base64 character 2b", from_string_message("+AAAAAAAAAAAAAAAAAAAAA"));
    }
}
