---
name: Phase 3c compression module summary
description: What landed in Phase 3c (compress/, codec dispatch, ratio estimator) and what's deferred
type: project
---

## Phase 3c scope (delivered)

### `src/common/compress/`
- `compression.rs` — `Compression` trait. Methods: `compression_type()`,
  `wrap_for_output(&mut ByteBufferOutputStream, i8) -> Box<dyn Write + 'a>`,
  `wrap_for_input(&[u8], i8, BufferSupplier) -> Box<dyn Read + 'a>`,
  `decompression_output_size() -> usize` (default 0).
- `no_compression.rs` — `NoCompression` (pass-through).
- `gzip_compression.rs` — `GzipCompression` (level-validated).
  `Builder::level(level)` returns `Result<Self, KafkaError::Config>` —
  Java's `IllegalArgumentException` → `KafkaError::Config`.
- `gzip_output_stream.rs` — thin wrapper over `flate2::write::GzEncoder`,
  takes `(out, size, level)` for parity with Java's `GzipOutputStream`.
  Note: `flate2` does not expose the JDK `Deflater` output-buffer-size knob;
  the `size` parameter is recorded for getter parity but ignored.
- `snappy_compression.rs` — `SnappyCompression`. **Wire-incompatible with
  Java/xerial-snappy framing**; see `phase3c_snappy_framing_gap.md`.
- `lz4_block_output_stream.rs` — `Lz4BlockOutputStream` reproducing Kafka's
  framed-LZ4 byte-for-byte (magic + FLG/BD/HC + per-block size+data+optional
  block checksum + end-mark). XXH32 via `twox-hash` crate (added direct dep).
  Block compression via `lz4_flex::block::compress_into`. `lz4_flex` does
  not accept a level knob — recorded for API parity.
- `lz4_block_input_stream.rs` — `Lz4BlockInputStream` reading the same
  format. Mirrors Java's PREMATURE_EOS / NOT_SUPPORTED / DESCRIPTOR_HASH_MISMATCH
  / BLOCK_HASH_MISMATCH error strings. Skip is supported.
  Behavioural difference: Java slices the input directly for uncompressed
  blocks; we copy into the supplier-allocated decompression buffer for
  lifetime simplicity.
- `lz4_compression.rs` — `Lz4Compression` codec. V0 magic → broken FD
  checksum (matches Java's `useBrokenFlagDescriptorChecksum`).
- `zstd_compression.rs` — `ZstdCompression`. Owns `Encoder`/`Decoder`
  via `zstd::stream`.

### `src/common/record/compression_ratio_estimator.rs`
Translation of Java's static utility. Module-level free functions:
`update_estimation`, `estimation`, `reset_estimation`, `set_estimation`.
Storage: `dashmap::DashMap<String, Mutex<[f32; 5]>>` behind a `OnceLock`
(mirrors Java's `static final ConcurrentMap`). The `Mutex<[f32; 5]>` per
entry mirrors Java's `synchronized (compressionRatioForTopic)` block.

### `src/common/record/compression_type.rs` (extended)
Added Phase 3c dispatch:
- `wrap_for_output(buffer_stream, message_version) -> Box<dyn Write + 'a>`
  — dispatches to the matching codec at default level.
- `wrap_for_input(buffer, message_version, supplier) -> Box<dyn Read + 'a>`.
- `level_validator() -> Box<dyn Fn(i32) -> Result<(), KafkaError>>` —
  closure equivalent of Java's `ConfigDef.Validator`. Gzip allows the
  default level (`-1`) outside `[1, 9]`; Lz4/Zstd are strict; None/Snappy
  always error (codec doesn't accept a level).

## Tests translated
- `NoCompressionTest` — `testCompressionDecompression`.
- `GzipCompressionTest` — all three: `testCompressionDecompression`,
  `testCompressionLevels`, `testLevelValidator`.
- `SnappyCompressionTest` — `testCompressionDecompression`.
- `Lz4CompressionTest` — all 11: framing V0/V1, compression decompression
  matrix, levels, and the parameterized header-premature-end / not-supported
  / bad-frame-checksum / bad-block-size / compression frame structure /
  array-backed-buffer / array-backed-buffer-slice / skip. Direct-buffer
  test elided (Rust uses `&[u8]` for both heap and direct equivalents).
- `ZstdCompressionTest` — both: `testCompressionDecompression`,
  `testCompressionLevels`.
- `CompressionRatioEstimatorTest` — `testUpdateEstimation` plus 2
  added Rust-only initial-rate / reset coverage.

Tests added Phase 3c: 30 (446 total lib tests, baseline 416).

## Deferred items

- **Snappy xerial framing**: see `phase3c_snappy_framing_gap.md`. Required
  for wire compat with Java; out-of-scope for Phase 3c since no popular
  Rust crate emits xerial framing.
- **LZ4 level knob**: `lz4_flex` always uses `LZ4_compress_default`. The
  level parameter is accepted (and validated) but never lowered into the
  compressor. Behavioural impact: produced bytes still decompress, but
  compressed size is constant w.r.t. level. Java's `lz4-java` gives a
  meaningful level → bytes-saved curve.
- **`Compression::of(name)`/`Compression.NONE`**: Java exposes static
  factories on the interface. Rust callers go through the per-codec
  `Builder::new()` or `CompressionType::wrap_for_output` directly; the
  trait does not need static methods (and they'd not be object-safe).

## Dependencies added
- `twox-hash = { version = "2", features = ["xxhash32"] }` — direct dep
  (was already transitive via `lz4_flex`). XXH32 is part of Kafka's LZ4
  frame wire format.

## Cross-phase impact
- Phase 3a's `phase3a_compression_dispatch_gap.md` is RESOLVED.
- `CompressionType::level_validator()` is the API surface that Phase 4's
  `ProducerConfig` will call to validate `compression.gzip.level` etc.
- `Compression` trait's object-safety is the API surface
  `MemoryRecordsBuilder` (Phase 3d) will use to take a `Box<dyn Compression>`.
