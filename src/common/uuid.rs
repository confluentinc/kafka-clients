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

/// This class defines an immutable universally unique identifier (UUID).
/// It represents a 128-bit value.
///
/// The toString() method prints using base64 URL encoding without padding (matching Java's implementation).
/// Likewise, the from_string method expects a base64 URL encoded string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    pub const fn new(most_sig_bits: u64, least_sig_bits: u64) -> Self {
        Uuid { most_sig_bits, least_sig_bits }
    }

    /// Creates a zero UUID (all bits are zero).
    pub const fn zero() -> Self {
        Self::ZERO_UUID
    }

    /// Returns the most significant 64 bits of this UUID.
    pub const fn most_sig_bits(&self) -> u64 {
        self.most_sig_bits
    }

    /// Returns the least significant 64 bits of this UUID.
    pub const fn least_sig_bits(&self) -> u64 {
        self.least_sig_bits
    }

    /// Checks if this is the zero UUID (all bits are zero).
    pub const fn is_zero(&self) -> bool {
        self.most_sig_bits == 0 && self.least_sig_bits == 0
    }

    /// Creates a UUID from a 16-byte array in big-endian order.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        let most_sig_bits = u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);
        let least_sig_bits = u64::from_be_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]);
        Uuid::new(most_sig_bits, least_sig_bits)
    }

    /// Converts this UUID to a 16-byte array in big-endian order.
    pub fn to_bytes(&self) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&self.most_sig_bits.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.least_sig_bits.to_be_bytes());
        bytes
    }

    /// Creates a UUID based on a base64 URL encoded string (without padding).
    /// This matches the Java implementation's fromString() method.
    pub fn from_string(s: &str) -> Result<Self, String> {
        if s.len() > 24 {
            return Err(format!(
                "Input string with prefix `{}` is too long to be decoded as a base64 UUID",
                &s[..24.min(s.len())]
            ));
        }

        // Decode base64 URL without padding
        let decoded = base64_url_decode(s).map_err(|e| format!("Failed to decode base64 string: {}", e))?;

        if decoded.len() != 16 {
            return Err(format!(
                "Input string `{}` decoded as {} bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID",
                s,
                decoded.len()
            ));
        }

        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&decoded);
        Ok(Self::from_bytes(bytes))
    }

    /// Returns a base64 URL encoded string (without padding) of the UUID.
    /// This matches the Java implementation's toString() method.
    pub fn to_base64_string(&self) -> String {
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
    let bytes = s.as_bytes();
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
                _ => return Err(format!("Invalid base64 character: {}", c as char)),
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
    fn cmp(&self, other: &Self) -> Ordering {
        match self.most_sig_bits.cmp(&other.most_sig_bits) {
            Ordering::Equal => self.least_sig_bits.cmp(&other.least_sig_bits),
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_uuid() {
        let uuid = Uuid::zero();
        assert_eq!(uuid.most_sig_bits(), 0);
        assert_eq!(uuid.least_sig_bits(), 0);
        assert!(uuid.is_zero());
        assert_eq!(uuid, Uuid::ZERO_UUID);
    }

    #[test]
    fn test_one_uuid() {
        let uuid = Uuid::ONE_UUID;
        assert_eq!(uuid.most_sig_bits(), 0);
        assert_eq!(uuid.least_sig_bits(), 1);
        assert!(!uuid.is_zero());
    }

    #[test]
    fn test_new_uuid() {
        let uuid = Uuid::new(0x0123456789ABCDEF, 0xFEDCBA9876543210);
        assert_eq!(uuid.most_sig_bits(), 0x0123456789ABCDEF);
        assert_eq!(uuid.least_sig_bits(), 0xFEDCBA9876543210);
        assert!(!uuid.is_zero());
    }

    #[test]
    fn test_from_bytes() {
        let bytes = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10,
        ];
        let uuid = Uuid::from_bytes(bytes);
        assert_eq!(uuid.most_sig_bits(), 0x0123456789ABCDEF);
        assert_eq!(uuid.least_sig_bits(), 0xFEDCBA9876543210);
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
        let parsed = Uuid::from_bytes(bytes);
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

    #[test]
    fn test_from_string_too_long() {
        let result = Uuid::from_string("AAAAAAAAAAAAAAAAAAAAAA_THIS_IS_TOO_LONG");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("too long"));
    }

    #[test]
    fn test_from_string_invalid_length() {
        let result = Uuid::from_string("AAAA");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not equal to the expected 16 bytes"));
    }
}
