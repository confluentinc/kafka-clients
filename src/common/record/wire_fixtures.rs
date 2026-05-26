// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Wire-format byte fixtures captured from the Java client.
//!
//! Each fixture under `test_fixtures/*.hex` was produced by running
//! [`RustFixtureCapture`] against Kafka 4.2's
//! `MemoryRecords.withRecords` / `withIdempotentRecords` factories with
//! known inputs. The Rust implementation here builds the same payloads
//! and asserts byte-for-byte equality.
//!
//! Phase 3e DoD line (`design/history/Milestone-1/PLAN.md`):
//!
//! > Byte-level encoding test: build a batch with two known records,
//! > assert the bytes equal a hex fixture captured from the Java client.
//!
//! See `test_fixtures/README.md` for the capture command and known
//! Rust-vs-Java divergences (Snappy framing, LZ4 level).
//!
//! [`RustFixtureCapture`]: https://github.com/apache/kafka — see
//! `kafka/clients/src/test/java/org/apache/kafka/common/record/RustFixtureCapture.java`

#![cfg(test)]

#[cfg(test)]
mod tests {
    use crate::common::record::{CompressionType, MemoryRecords, SimpleRecord};
    use bytes::Bytes;

    /// Decode a hex string (lowercase, no separators) into a `Vec<u8>`.
    /// Panics on malformed input — fixtures are checked in alongside this
    /// code, so any malformed input is a developer error.
    fn decode_hex(s: &str) -> Vec<u8> {
        let trimmed: String = s.chars().filter(|c| !c.is_whitespace()).collect();
        assert!(trimmed.len().is_multiple_of(2), "hex fixture has odd length: {}", trimmed.len());
        let mut out = Vec::with_capacity(trimmed.len() / 2);
        let bytes = trimmed.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let hi = nibble(bytes[i]);
            let lo = nibble(bytes[i + 1]);
            out.push((hi << 4) | lo);
            i += 2;
        }
        out
    }

    fn nibble(b: u8) -> u8 {
        match b {
            b'0'..=b'9' => b - b'0',
            b'a'..=b'f' => b - b'a' + 10,
            b'A'..=b'F' => b - b'A' + 10,
            _ => panic!("invalid hex digit: {b}"),
        }
    }

    /// Java `Compression.NONE` + `withRecords` short form, one record:
    /// `(offset_delta=0, ts=0, key="key", value="value")`.
    const FIXTURE_UNCOMPRESSED_ONE: &str = include_str!("test_fixtures/uncompressed_one_record.hex");
    /// Java `Compression.NONE` + `withRecords`, two records.
    const FIXTURE_UNCOMPRESSED_TWO: &str = include_str!("test_fixtures/uncompressed_two_records.hex");
    /// Java `Compression.gzip().build()` + `withRecords`, two records.
    const FIXTURE_GZIP_TWO: &str = include_str!("test_fixtures/gzip_two_records.hex");
    /// Java `Compression.snappy().build()` + `withRecords`, two records.
    /// Java emits xerial framing; Rust emits RFC framing — wire-incompat.
    /// Captured for reference; not asserted (see Phase 3c snappy gap).
    #[allow(dead_code)]
    const FIXTURE_SNAPPY_TWO: &str = include_str!("test_fixtures/snappy_two_records.hex");
    /// Java `Compression.lz4().build()` + `withRecords`, two records.
    const FIXTURE_LZ4_TWO: &str = include_str!("test_fixtures/lz4_two_records.hex");
    /// Java `Compression.zstd().build()` + `withRecords`, two records.
    const FIXTURE_ZSTD_TWO: &str = include_str!("test_fixtures/zstd_two_records.hex");
    /// Java `withIdempotentRecords(NONE, producerId=12345, epoch=7,
    /// baseSeq=42)`, two records.
    const FIXTURE_UNCOMPRESSED_TWO_IDEMPOTENT: &str =
        include_str!("test_fixtures/uncompressed_two_records_idempotent.hex");
    /// Java `withIdempotentRecords(GZIP, producerId=12345, epoch=7,
    /// baseSeq=42)`, two records.
    const FIXTURE_GZIP_TWO_IDEMPOTENT: &str = include_str!("test_fixtures/gzip_two_records_idempotent.hex");

    /// Two-record SimpleRecord payload shared across the multi-record
    /// fixtures: `(ts=1234, key="k1", value="v1")` and
    /// `(ts=1235, key="k2", value="v2")`.
    fn two_records_payload() -> Vec<SimpleRecord> {
        vec![
            SimpleRecord::new(1234i64, Some(Bytes::from_static(b"k1")), Some(Bytes::from_static(b"v1")), &[]),
            SimpleRecord::new(1235i64, Some(Bytes::from_static(b"k2")), Some(Bytes::from_static(b"v2")), &[]),
        ]
    }

    /// Pretty-print a byte slice as `aa bb cc` for assertion failures.
    fn fmt_bytes(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect::<Vec<_>>().join(" ")
    }

    fn assert_bytes_eq(label: &str, got: &[u8], expected: &[u8]) {
        if got != expected {
            panic!(
                "{label}: bytes mismatch\n  got      ({} bytes): {}\n  expected ({} bytes): {}",
                got.len(),
                fmt_bytes(got),
                expected.len(),
                fmt_bytes(expected),
            );
        }
    }

    /// Sanity check: the hex decoder round-trips a small known input.
    #[test]
    fn hex_decoder_round_trip() {
        assert_eq!(decode_hex("00ff"), vec![0x00, 0xff]);
        assert_eq!(decode_hex(" de ad \n be ef "), vec![0xde, 0xad, 0xbe, 0xef]);
    }

    /// Java fixture 1 — uncompressed, single record (`key`/`value`).
    /// Anchors header layout, varint encoding, CRC32C placement.
    #[test]
    fn matches_java_uncompressed_one_record() {
        let expected = decode_hex(FIXTURE_UNCOMPRESSED_ONE);
        let recs = vec![SimpleRecord::new(
            0i64,
            Some(Bytes::from_static(b"key")),
            Some(Bytes::from_static(b"value")),
            &[],
        )];
        let mr = MemoryRecords::with_records_default(CompressionType::None, &recs).expect("with_records succeeds");
        assert_bytes_eq("uncompressed one record", mr.buffer().as_ref(), &expected);
    }

    /// Java fixture 2 — uncompressed, two records.
    /// Anchors offset_delta=1, ts_delta encoding, and per-record varint
    /// headers across multiple records in one batch.
    #[test]
    fn matches_java_uncompressed_two_records() {
        let expected = decode_hex(FIXTURE_UNCOMPRESSED_TWO);
        let mr = MemoryRecords::with_records_default(CompressionType::None, &two_records_payload())
            .expect("with_records succeeds");
        assert_bytes_eq("uncompressed two records", mr.buffer().as_ref(), &expected);
    }

    /// Java fixture 3 — gzip-compressed two records. This test is the
    /// gzip-codec wire lock: any change to gzip framing, level, or
    /// header bytes that diverges from Java's `GZIPOutputStream`
    /// default will trip this assertion.
    #[test]
    fn matches_java_gzip_two_records() {
        let expected = decode_hex(FIXTURE_GZIP_TWO);
        let mr = MemoryRecords::with_records_default(CompressionType::Gzip, &two_records_payload())
            .expect("with_records succeeds");
        assert_bytes_eq("gzip two records", mr.buffer().as_ref(), &expected);
    }

    /// Java fixture 4 — LZ4-compressed two records. This test locks
    /// Kafka's LZ4 framed format (block size = 64 KiB, no content
    /// checksum, no block checksums in the default config).
    #[test]
    fn matches_java_lz4_two_records() {
        let expected = decode_hex(FIXTURE_LZ4_TWO);
        let mr = MemoryRecords::with_records_default(CompressionType::Lz4, &two_records_payload())
            .expect("with_records succeeds");
        assert_bytes_eq("lz4 two records", mr.buffer().as_ref(), &expected);
    }

    /// Java fixture 5 — Zstd-compressed two records (default level 3).
    #[test]
    fn matches_java_zstd_two_records() {
        let expected = decode_hex(FIXTURE_ZSTD_TWO);
        let mr = MemoryRecords::with_records_default(CompressionType::Zstd, &two_records_payload())
            .expect("with_records succeeds");
        assert_bytes_eq("zstd two records", mr.buffer().as_ref(), &expected);
    }

    /// Java fixture 6 — idempotent uncompressed
    /// (`producerId=12345, epoch=7, baseSeq=42`). Locks producer-state
    /// fields in the v2 batch header.
    #[test]
    fn matches_java_uncompressed_two_records_idempotent() {
        let expected = decode_hex(FIXTURE_UNCOMPRESSED_TWO_IDEMPOTENT);
        let mr = MemoryRecords::with_idempotent_records_default(
            CompressionType::None,
            12345i64,
            7i16,
            42i32,
            &two_records_payload(),
        )
        .expect("with_idempotent_records succeeds");
        assert_bytes_eq("idempotent uncompressed two records", mr.buffer().as_ref(), &expected);
    }

    /// Java fixture 7 — idempotent gzip-compressed
    /// (`producerId=12345, epoch=7, baseSeq=42`). Locks producer-state
    /// fields combined with the compressed records section.
    #[test]
    fn matches_java_gzip_two_records_idempotent() {
        let expected = decode_hex(FIXTURE_GZIP_TWO_IDEMPOTENT);
        let mr = MemoryRecords::with_idempotent_records_default(
            CompressionType::Gzip,
            12345i64,
            7i16,
            42i32,
            &two_records_payload(),
        )
        .expect("with_idempotent_records succeeds");
        assert_bytes_eq("idempotent gzip two records", mr.buffer().as_ref(), &expected);
    }

    /// Snappy is a known wire-incompat: Rust's `snap` crate emits RFC
    /// framing while Java's `org.xerial.snappy.SnappyOutputStream` emits
    /// xerial-framed bytes. The fixture is captured for reference but
    /// the assertion is intentionally skipped here. Tracked in actor
    /// memory `phase3c_snappy_framing_gap.md`; resolution deferred to
    /// Phase 5 (network compat) per Phase 3c review issue 11.
    ///
    /// This stub test still loads and decodes the fixture so a renamed
    /// or deleted file fails CI (a tripwire on the fixture asset).
    #[test]
    fn snappy_fixture_present_but_assertion_deferred() {
        let expected = decode_hex(FIXTURE_SNAPPY_TWO);
        // Sanity: the captured Java snappy bytes start with the
        // xerial magic header `82 53 4E 41 50 50 59 00` (i.e. the
        // SnappyOutputStream stream header) inside the records section
        // (after the 61-byte v2 batch header). If this header drifts,
        // the fixture itself is wrong.
        let xerial_magic_offset = 61;
        assert!(expected.len() > xerial_magic_offset + 8);
        assert_eq!(
            &expected[xerial_magic_offset..xerial_magic_offset + 8],
            b"\x82SNAPPY\x00",
            "snappy fixture missing xerial magic header — capture is wrong"
        );
    }
}
