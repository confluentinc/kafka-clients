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

//! Translation of the producer-relevant slice of
//! `org.apache.kafka.common.utils.Utils`.
//!
//! Java's `Utils` class is ~100 static helpers spanning UTF-8 conversion,
//! hashing, host/port parsing, file I/O, reflective instantiation, and more.
//! Per the Phase 1 plan we translate only the helpers used by the producer
//! call paths up through Phase 7:
//!
//! * `utf8(...)` — UTF-8 encoding helpers.
//! * `abs(int)` — `Math.abs` with `Integer.MIN_VALUE` saturation to 0.
//! * `murmur2(byte[])` — 32-bit MurmurHash2 used by `BuiltInPartitioner`.
//! * `getHost`, `getPort`, `validHostPattern`, `formatAddress` — bootstrap
//!   address parsing in Phase 4 / 5.
//!
//! Skipped Java helpers (the rest of `Utils.java`):
//!
//! * Reflection (`newInstance`, `loadClass`) — the Rust client wires concrete
//!   types instead of Java's `Class.forName` indirection.
//! * `Properties` / file loading — the Rust client takes typed configuration
//!   structs; we translate `ConfigDef.parse(Map<String, String>)` not
//!   property-file parsing.
//! * `Closeable` cleanup helpers, `mkSet`, `mkMap`, etc. — replaced with the
//!   stdlib `IntoIterator` / `HashMap::from(...)` idioms at call sites.
//!
//! Each translated helper carries an explicit `// Java: <method>` comment so
//! reviewers can cross-reference the source.

use std::sync::LazyLock;

use regex::Regex;

/// `Utils.utf8(byte[])` — turn a UTF-8 byte slice into a `String`. Lossy for
/// invalid sequences to match Java's `new String(bytes, UTF_8)` behaviour
/// (Java replaces invalid sequences with the replacement character).
pub fn utf8_from_bytes(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_owned(),
        Err(_) => String::from_utf8_lossy(bytes).into_owned(),
    }
}

/// `Utils.utf8(String)` — turn a `&str` into a UTF-8 byte slice.
/// (Trivial in Rust: `s.as_bytes()`. We keep the function for translation
/// readability.)
pub fn utf8_from_str(s: &str) -> &[u8] {
    s.as_bytes()
}

/// `Utils.abs(int)` — absolute value, returning 0 for `i32::MIN`.
/// The standard `i32::abs()` panics in debug for `MIN`; this helper matches
/// Java's documented "different from `Math.abs`" behaviour.
pub fn abs(n: i32) -> i32 {
    if n == i32::MIN { 0 } else { n.unsigned_abs() as i32 }
}

/// `Utils.murmur2(byte[])` — 32-bit MurmurHash2 with the seed Kafka uses
/// for `BuiltInPartitioner`. Wire-compatible with the Java client.
///
/// Translated literally from the Java source (line 498) so the per-key
/// partitioner output matches the Java client byte-for-byte.
pub fn murmur2(data: &[u8]) -> i32 {
    let length = data.len();
    let seed: i32 = 0x9747b28c_u32 as i32;
    // 'm' and 'r' are mixing constants generated offline.
    let m: i32 = 0x5bd1e995_u32 as i32;
    let r: u32 = 24;

    let mut h: i32 = seed ^ (length as i32);
    let length4 = length >> 2;

    // Process input four bytes at a time, little-endian (matches the Java
    // INT_HANDLE that uses ByteOrder.LITTLE_ENDIAN).
    for i in 0..length4 {
        let i4 = i << 2;
        let k_bytes: [u8; 4] = data[i4..i4 + 4].try_into().unwrap();
        let mut k = i32::from_le_bytes(k_bytes);
        // Java multiplies as 32-bit signed with wrapping; emulate via wrapping ops.
        k = k.wrapping_mul(m);
        k ^= ((k as u32) >> r) as i32;
        k = k.wrapping_mul(m);
        h = h.wrapping_mul(m);
        h ^= k;
    }

    // Handle remaining bytes (fall-through switch in Java). Java masks
    // each byte with `& 0xff` because Java's `byte` is signed (`i8`); in
    // Rust `data[i]` is already `u8`, so the mask is a no-op and clippy
    // flags it.
    let index = length4 << 2;
    let remainder = length - index;
    if remainder >= 3 {
        h ^= (data[index + 2] as i32) << 16;
    }
    if remainder >= 2 {
        h ^= (data[index + 1] as i32) << 8;
    }
    if remainder >= 1 {
        h ^= data[index] as i32;
        h = h.wrapping_mul(m);
    }

    h ^= ((h as u32) >> 13) as i32;
    h = h.wrapping_mul(m);
    h ^= ((h as u32) >> 15) as i32;

    h
}

// Compiled once per process. Patterns are identical to the Java source.
static HOST_PORT_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(?:[0-9a-zA-Z\-%._]*://)?\[?([0-9a-zA-Z\-%._:]*)]?:([0-9]+)").unwrap());

static VALID_HOST_CHARACTERS: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^([0-9a-zA-Z\-%._:]*)$").unwrap());

/// `Utils.getHost(String)` — extract the hostname from a `host:port` (or
/// `protocol://host:port`) address. Returns `None` if the input does not
/// match the expected pattern.
pub fn get_host(address: &str) -> Option<&str> {
    HOST_PORT_PATTERN.captures(address).map(|c| c.get(1).unwrap().as_str())
}

/// `Utils.getPort(String)` — extract the port number from an address.
pub fn get_port(address: &str) -> Option<u16> {
    HOST_PORT_PATTERN
        .captures(address)
        .and_then(|c| c.get(2).unwrap().as_str().parse().ok())
}

/// `Utils.validHostPattern(String)` — validate that the input contains only
/// the characters allowed in a hostname.
pub fn valid_host_pattern(address: &str) -> bool {
    VALID_HOST_CHARACTERS.is_match(address)
}

/// `Utils.formatAddress(String, Integer)` — format `host` and `port` as a
/// `host:port` (or `[ipv6]:port`) string.
pub fn format_address(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
mod tests {
    // Translation of the `UtilsTest` cases that exercise the helpers above.
    // The full UtilsTest covers reflection, file I/O, sleep, etc., which are
    // not in scope for Phase 1; we only translate the cases that match
    // translated helpers.

    use super::*;

    #[test]
    fn utf8_round_trip() {
        let s = "hello";
        let bytes = utf8_from_str(s);
        assert_eq!(bytes, b"hello");
        assert_eq!(utf8_from_bytes(bytes), "hello");
    }

    #[test]
    fn utf8_handles_invalid_sequences_lossily() {
        let bytes = [0x68u8, 0xff, 0x69]; // "h", invalid, "i"
        let s = utf8_from_bytes(&bytes);
        assert!(s.starts_with('h'));
        assert!(s.ends_with('i'));
    }

    #[test]
    fn abs_handles_min_value() {
        // `i32::MIN.unsigned_abs() == 2^31`, which doesn't fit in i32.
        // Java returns 0 for this special case; we follow.
        assert_eq!(abs(i32::MIN), 0);
        assert_eq!(abs(-1), 1);
        assert_eq!(abs(0), 0);
        assert_eq!(abs(42), 42);
    }

    /// Java: `UtilsTest#testMurmur2`. Vectors copied verbatim from
    /// `org.apache.kafka.common.utils.UtilsTest.testMurmur2`.
    #[test]
    fn murmur2_known_vectors() {
        assert_eq!(murmur2(b"21"), -973_932_308);
        assert_eq!(murmur2(b"foobar"), -790_332_482);
        assert_eq!(murmur2(b"a-little-bit-long-string"), -985_981_536);
        assert_eq!(murmur2(b"a-little-bit-longer-string"), -1_486_304_829);
        assert_eq!(murmur2(b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8"), -58_897_971);
        assert_eq!(murmur2(b"abc"), 479_470_107);
    }

    #[test]
    fn get_host_and_port_parse_ipv4() {
        assert_eq!(get_host("localhost:9092"), Some("localhost"));
        assert_eq!(get_port("localhost:9092"), Some(9092));
    }

    #[test]
    fn get_host_and_port_parse_with_protocol() {
        assert_eq!(get_host("PLAINTEXT://broker0.example.com:9092"), Some("broker0.example.com"));
        assert_eq!(get_port("PLAINTEXT://broker0.example.com:9092"), Some(9092));
    }

    #[test]
    fn get_host_and_port_parse_ipv6() {
        assert_eq!(get_host("[::1]:9093"), Some("::1"));
        assert_eq!(get_port("[::1]:9093"), Some(9093));
    }

    #[test]
    fn get_host_returns_none_for_invalid() {
        assert_eq!(get_host("not-an-address"), None);
        assert_eq!(get_port("not-an-address"), None);
    }

    #[test]
    fn valid_host_pattern_accepts_valid_chars() {
        assert!(valid_host_pattern("localhost"));
        assert!(valid_host_pattern("broker-1.example.com"));
        assert!(valid_host_pattern("::1"));
    }

    #[test]
    fn valid_host_pattern_rejects_invalid_chars() {
        assert!(!valid_host_pattern("broker!example"));
        assert!(!valid_host_pattern("a b"));
    }

    #[test]
    fn format_address_brackets_ipv6() {
        assert_eq!(format_address("localhost", 9092), "localhost:9092");
        assert_eq!(format_address("::1", 9092), "[::1]:9092");
    }
}
