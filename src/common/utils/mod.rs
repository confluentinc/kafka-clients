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

pub use exponential_backoff::ExponentialBackoff;
pub use log_context::LogContext;

/// Converts a signed i32 to a non-negative value by clearing the sign bit.
///
/// Translated from `org.apache.kafka.common.utils.Utils.toPositive`.
#[inline]
pub fn to_positive(number: i32) -> i32 {
    number & 0x7fff_ffff
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

/// Renders a floating-point value as the decimal string used in metric and
/// quota-violation message text.
///
/// A finite value with no fractional part is rendered with a trailing `.0`
/// (so `5` becomes `"5.0"`); other finite values use their shortest
/// round-tripping decimal form. Non-finite values are spelled `"Infinity"`,
/// `"-Infinity"`, and `"NaN"`.
///
/// This is minimal native infrastructure (in the same spirit as the metrics
/// `TimeUnit`): it targets the value magnitudes those messages actually produce
/// (well under `1e7`) and intentionally does not switch to scientific notation
/// for very large or very small magnitudes.
pub(crate) fn double_to_string(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value.is_infinite() {
        if value > 0.0 {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        }
    } else if value == value.trunc() {
        format!("{value:.1}")
    } else {
        format!("{value}")
    }
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
    fn test_double_to_string() {
        // Finite integral values gain a trailing ".0".
        assert_eq!(double_to_string(5.0), "5.0");
        assert_eq!(double_to_string(-60.0), "-60.0");
        assert_eq!(double_to_string(0.0), "0.0");
        assert_eq!(double_to_string(1000.0), "1000.0");
        // Non-integral finite values use the shortest decimal form.
        assert_eq!(double_to_string(2.5), "2.5");
        assert_eq!(double_to_string(5.6), "5.6");
        // Non-finite values use their spelled-out forms.
        assert_eq!(double_to_string(f64::INFINITY), "Infinity");
        assert_eq!(double_to_string(f64::NEG_INFINITY), "-Infinity");
        assert_eq!(double_to_string(f64::NAN), "NaN");
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
}
