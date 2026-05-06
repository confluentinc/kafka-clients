# Java-derived wire fixtures

These hex files capture the byte output of Apache Kafka 4.2's
`MemoryRecords.withRecords` / `withIdempotentRecords` so the Rust port's
encoder is anchored against Java's known-good output.

## How they were captured

Source program: `kafka/clients/src/test/java/org/apache/kafka/common/record/RustFixtureCapture.java`

Run command (from repo root):

```
SNAPPY=$(find ~/.gradle/caches/modules-2/files-2.1/org.xerial.snappy -name "snappy-java-1.1.10.7.jar" ! -name "*sources*" | head -1)
LZ4=$(find ~/.gradle/caches/modules-2/files-2.1/at.yawk.lz4 -name "lz4-java-1.10.1.jar" ! -name "*sources*" | head -1)
ZSTD=$(find ~/.gradle/caches/modules-2/files-2.1/com.github.luben -name "zstd-jni-1.5.6-10.jar" ! -name "*sources*" | head -1)
SLF4J=$(find ~/.gradle/caches/modules-2/files-2.1/org.slf4j -name "slf4j-api-1.7.36.jar" ! -name "*sources*" | head -1)
KCLIENTS=kafka/clients/build/libs/kafka-clients-4.2.0.jar

mkdir -p /tmp/rust-fixture-capture
javac -d /tmp/rust-fixture-capture \
    -cp "$KCLIENTS:$SNAPPY:$LZ4:$ZSTD:$SLF4J" \
    kafka/clients/src/test/java/org/apache/kafka/common/record/RustFixtureCapture.java

java -cp "/tmp/rust-fixture-capture:$KCLIENTS:$SNAPPY:$LZ4:$ZSTD:$SLF4J" \
    org.apache.kafka.common.record.RustFixtureCapture
```

Versions used (from `kafka/gradle/dependencies.gradle`):

- `kafka-clients` 4.2.0
- `snappy-java` 1.1.10.7
- `lz4-java` 1.10.1
- `zstd-jni` 1.5.6-10

Capture date: 2026-04-30.

## Fixtures

| File | Java factory | Records |
| ---- | ------------ | ------- |
| `uncompressed_one_record.hex` | `MemoryRecords.withRecords(NONE)` | 1 record (ts=0, key="key", value="value") |
| `uncompressed_two_records.hex` | `MemoryRecords.withRecords(NONE)` | 2 records (ts=1234/1235, key="k1"/"k2", value="v1"/"v2") |
| `gzip_two_records.hex` | `MemoryRecords.withRecords(GZIP)` | same payload as above |
| `snappy_two_records.hex` | `MemoryRecords.withRecords(SNAPPY)` | same payload (xerial framing — Rust differs, see below) |
| `lz4_two_records.hex` | `MemoryRecords.withRecords(LZ4)` | same payload |
| `zstd_two_records.hex` | `MemoryRecords.withRecords(ZSTD)` | same payload |
| `uncompressed_two_records_idempotent.hex` | `MemoryRecords.withIdempotentRecords(NONE)` | producerId=12345, epoch=7, baseSeq=42 |
| `gzip_two_records_idempotent.hex` | `MemoryRecords.withIdempotentRecords(GZIP)` | producerId=12345, epoch=7, baseSeq=42 |

## Known Rust-vs-Java divergences

- **Snappy** — Java uses xerial framing (org.xerial.snappy.SnappyOutputStream),
  Rust's `snap` crate emits RFC framing. Wire-incompatible. Captured for
  reference but not asserted by Rust tests yet (tracked in Phase 3c memory note
  `phase3c_snappy_framing_gap.md`; resolution deferred to Phase 5).
- **LZ4 level** — `lz4_flex` (Rust) ignores the level parameter and always
  emits LZ4_compress_default. Java's lz4-java honors level. The default-level
  fixture matches; level=9 fixtures would not. Phase 3c memory note tracks.
- **Gzip** — Java's `GZIPOutputStream` defaults to deflate level 6;
  Phase 3c's `GzipCompression::None` build also emits level 6. The captured
  fixture asserts Rust matches Java byte-for-byte.
- **Zstd** — both sides default to zstd level 3.
