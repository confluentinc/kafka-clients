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

//! SCRAM message salt and hash functions defined in
//! [RFC 5802](https://tools.ietf.org/html/rfc5802).
//!
//! Corresponds to
//! `org.apache.kafka.common.security.scram.internals.ScramFormatter`.
//!
//! Only the `hi()` primitive is translated here — the single function the Admin
//! `alterUserScramCredentials` path needs to compute a salted password. The rest
//! of the Java `ScramFormatter` (the full SASL/SCRAM client handshake) is out of
//! scope for the admin client.

use std::num::NonZeroU32;

use aws_lc_rs::pbkdf2;

use super::ScramMechanism;

/// SCRAM salt/hash helper bound to a specific [`ScramMechanism`].
///
/// Mirrors `new ScramFormatter(ScramMechanism)`: the mechanism selects the hash
/// / MAC algorithm used by [`ScramFormatter::hi`].
pub(crate) struct ScramFormatter {
    mechanism: ScramMechanism,
}

impl ScramFormatter {
    /// Creates a formatter for the given mechanism.
    ///
    /// Mirrors the Java constructor, minus the `NoSuchAlgorithmException` throw:
    /// both supported mechanisms map to algorithms `aws-lc-rs` always provides,
    /// so construction is infallible.
    pub(crate) fn new(mechanism: ScramMechanism) -> Self {
        Self { mechanism }
    }

    /// Computes the RFC 5802 `Hi(str, salt, i)` iterated hash — PBKDF2-HMAC with
    /// a single output block whose length equals the hash's digest length.
    ///
    /// Mirrors `ScramFormatter.hi(byte[] str, byte[] salt, int iterations)`:
    /// `U1 = HMAC(str, salt || INT(1))`, then `Ui = HMAC(str, Ui-1)` for
    /// `i in 2..=iterations`, returning `U1 xor U2 xor ... xor Ui`. With the
    /// output length pinned to one digest block, this is exactly PBKDF2's first
    /// (and only) block `T_1`, so `aws_lc_rs::pbkdf2::derive` computes it
    /// directly.
    ///
    /// Java's loop runs `for i = 2; i <= iterations`, so `iterations <= 1`
    /// yields just `U1` (no XOR). PBKDF2 with `c = 1` produces the same `U1`, so
    /// we clamp `iterations` to a minimum of 1 to preserve Java's behavior for
    /// zero/negative counts while satisfying PBKDF2's `c >= 1` requirement (and
    /// keeping `NonZeroU32::new(..).unwrap()` panic-free).
    pub(crate) fn hi(&self, str: &[u8], salt: &[u8], iterations: i32) -> Vec<u8> {
        let (algorithm, digest_len) = match self.mechanism {
            ScramMechanism::ScramSha256 => (pbkdf2::PBKDF2_HMAC_SHA256, 32),
            ScramMechanism::ScramSha512 => (pbkdf2::PBKDF2_HMAC_SHA512, 64),
        };
        // `max(1)` mirrors Java treating `iterations <= 1` as a single block;
        // it also guarantees a non-zero value, so the unwrap never panics.
        let iterations = NonZeroU32::new(iterations.max(1) as u32).expect("iterations clamped to >= 1");
        let mut out = vec![0u8; digest_len];
        pbkdf2::derive(algorithm, iterations, salt, str, &mut out);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Byte-level correctness for SHA-256 `hi()`.
    ///
    /// Provenance: [RFC 7914 (scrypt) §11] publishes PBKDF2-HMAC-SHA-256 test
    /// vectors. For `P="passwd", S="salt", c=1, dkLen=64` the derived key begins
    /// with block `T_1`, and `hi()` is exactly `T_1` at one digest block (32
    /// bytes). This nails the INT(1) block-counter, iteration-boundary and
    /// output-length details a round-trip test would silently pass with the
    /// wrong PBKDF2 (a credential the broker rejects only at auth time).
    ///
    /// [RFC 7914 (scrypt) §11]: https://www.rfc-editor.org/rfc/rfc7914#section-11
    #[test]
    fn hi_sha256_matches_rfc7914_vector() {
        let formatter = ScramFormatter::new(ScramMechanism::ScramSha256);
        let out = formatter.hi(b"passwd", b"salt", 1);
        let expected: [u8; 32] = [
            0x55, 0xac, 0x04, 0x6e, 0x56, 0xe3, 0x08, 0x9f, 0xec, 0x16, 0x91, 0xc2, 0x25, 0x44, 0xb6, 0x05, 0xf9, 0x41,
            0x85, 0x21, 0x6d, 0xde, 0x04, 0x65, 0xe6, 0x8b, 0x9d, 0x57, 0xc2, 0x0d, 0xac, 0xbc,
        ];
        assert_eq!(out.as_slice(), expected.as_slice());
    }

    /// Byte-level correctness for SHA-512 `hi()`.
    ///
    /// Provenance: cross-checked against an independent PBKDF2 implementation
    /// (Python `hashlib.pbkdf2_hmac('sha512', b'pencil', b'salt', 4096, 64)`,
    /// first 64-byte block). RFC 7914 only publishes SHA-256 vectors, so an
    /// independent reference stands in here. A wrong SHA-512 digest length (e.g.
    /// truncating to 32) or block-counter change would fail this.
    #[test]
    fn hi_sha512_matches_reference_vector() {
        let formatter = ScramFormatter::new(ScramMechanism::ScramSha512);
        let out = formatter.hi(b"pencil", b"salt", 4096);
        let expected: [u8; 64] = [
            0x2c, 0xfe, 0x3a, 0x1c, 0x15, 0x16, 0x62, 0xb1, 0xea, 0x49, 0xd1, 0x3f, 0x59, 0x56, 0x74, 0xa1, 0xc6, 0x66,
            0xad, 0xd7, 0x0d, 0xf1, 0x5d, 0x3d, 0x02, 0x25, 0x4e, 0x99, 0x05, 0x99, 0x38, 0x78, 0x26, 0x1d, 0xa7, 0x40,
            0x7f, 0xd1, 0x1c, 0x2f, 0xee, 0x4b, 0x0a, 0x30, 0xdf, 0x51, 0x54, 0xb1, 0xa7, 0x52, 0xf8, 0x6a, 0x13, 0x38,
            0x0d, 0xdd, 0x4b, 0xdd, 0x9a, 0x7c, 0x95, 0x8e, 0xc7, 0x69,
        ];
        assert_eq!(out.as_slice(), expected.as_slice());
    }

    /// The SHA-256 output is one digest block (32 bytes); SHA-512 is 64.
    #[test]
    fn hi_output_length_is_digest_length() {
        assert_eq!(ScramFormatter::new(ScramMechanism::ScramSha256).hi(b"p", b"s", 4096).len(), 32);
        assert_eq!(ScramFormatter::new(ScramMechanism::ScramSha512).hi(b"p", b"s", 4096).len(), 64);
    }

    /// `iterations <= 1` collapses to a single PBKDF2 block, matching Java's loop
    /// that starts at `i = 2`.
    #[test]
    fn hi_clamps_non_positive_iterations_to_one() {
        let formatter = ScramFormatter::new(ScramMechanism::ScramSha256);
        let one = formatter.hi(b"passwd", b"salt", 1);
        assert_eq!(formatter.hi(b"passwd", b"salt", 0), one);
        assert_eq!(formatter.hi(b"passwd", b"salt", -5), one);
    }
}
