---
name: Phase 3e wire fixtures
description: Java-derived byte vectors lock the v2 record format; codec close-path divergences resolved
type: project
---

Phase 3e closed the wire-format DoD on Phase 3 by capturing 8 hex
byte fixtures from Apache Kafka 4.2's `MemoryRecords.withRecords` /
`withIdempotentRecords` and asserting byte-for-byte equality from
Rust's `MemoryRecords::with_records_default` /
`with_idempotent_records_default`.

**Why:** the existing fixtures in `default_record.rs:1039` and
`default_record_batch.rs:1830/1918` were hand-computed from the v2
spec — they locked Rust against itself, not against Java. Phase 3e
adds a Java-authoritative anchor.

**How to apply:** when changing any of the codec wrappers (gzip /
zstd / lz4) or the v2 batch header packing in
`memory_records_builder::write_default_batch_header`, run
`cargo test --lib wire_fixtures` first — these tests will trip
immediately on any byte-level divergence.

## Capture toolchain

- Java program: `kafka/clients/src/test/java/org/apache/kafka/common/record/RustFixtureCapture.java`
  (committed inside the `kafka/` submodule, detached commit `011b88b`).
- Run command in `src/common/record/test_fixtures/README.md`.
- Resolved jar versions: kafka-clients 4.2.0, snappy-java 1.1.10.7,
  lz4-java 1.10.1, zstd-jni 1.5.6-10, slf4j-api 1.7.36 (cached under
  `~/.gradle/caches/modules-2/files-2.1/...`).
- Output is one `NAME=hex` line per fixture; dump to per-fixture
  `.hex` files under `src/common/record/test_fixtures/`.

## Codec close-path divergences (resolved)

Two byte-level mismatches surfaced when the captured fixtures were
first compared with Rust's output. Both are encoded into the
codec wrappers and remembered here so they are not re-introduced.

### Gzip — extra Z_SYNC_FLUSH block

`memory_records_builder::close()` previously called
`Write::flush()` on `append_stream` before dropping it.
`flate2::GzEncoder::flush` emits a `Z_SYNC_FLUSH` deflate block
(`00 00 00 00 ff ff`); the subsequent drop-driven `finish()` then
emits a second final block. Java's `GZIPOutputStream.close()`
invokes only `def.finish()`, producing a single final block. Fix:
remove the explicit `flush()` call before drop. The boxed writer's
Drop runs the codec's `try_finish()`, which is the byte-for-byte
analogue of Java's `out.close()`.

### Zstd — missing flush + finish chain

Java's wrapper is `BufferedOutputStream(ZstdOutputStream, 16K)`.
Its `close()` chain is `flush()` → `out.close()`. Java's
`ZstdOutputStream.flush()` (with `closeFrameOnFlush=false`, the
default Kafka uses) emits a `Z_FLUSH`-terminated block; the
subsequent `close()` emits an empty-last-block marker. Rust's
`BufWriter::Drop` does NOT propagate `flush()` to the inner; only
the `flush_buf` step happens. Fix: `ZstdWriter::Drop` now calls
`encoder.flush()` (`Z_FLUSH`) then `encoder.finish()` (`Z_END`),
matching Java byte-for-byte.

### Snappy — deferred (xerial vs RFC framing)

Captured `snappy_two_records.hex` for reference but the byte-level
assertion is intentionally skipped. The Rust `snap` crate emits
RFC framing; Java uses xerial framing. Tracked in
`phase3c_snappy_framing_gap.md` and
`design/history/Milestone-1/Phase-3/COMMENTS.0.md` issue 11.
Resolution deferred to Phase 5. A tripwire test
(`snappy_fixture_present_but_assertion_deferred`) verifies the
captured fixture still has the xerial magic header `82 SNAPPY 00`
so the asset itself can't silently rot.

### LZ4 — passes byte-for-byte at default level

Per Phase 3c notes, `lz4_flex` ignores level. At default level the
fixtures match exactly. If a future caller ever sets a non-default
level, the assertion will trip — which is the correct outcome
(it's the deferred wire-incompat, not a regression).

## DoD ledger

- `cargo build` clean
- `cargo test --lib`: 577 → 586 (+9: hex_decoder_round_trip + 7
  byte-equality + 1 snappy tripwire)
- `cargo xtask format-check` clean
- `cargo xtask lint` clean
- `cargo xtask check-generated` clean (199 generated files
  unchanged)

## Commit graph

1. `31bfceb` — submodule pointer bump for `RustFixtureCapture.java`
2. `<next>` — Rust fixtures, tests, codec close-path fixes
3. `<this>` — actor memory note
