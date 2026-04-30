---
name: Phase 3a CompressionType codec-dispatch gap
description: CompressionType level methods return KafkaError::InvalidRequest until Phase 3c wires the codec dispatch
type: project
---

`src/common/record/compression_type.rs` exposes `id()`, `name()`,
`for_id`, `for_name`, and the per-codec level metadata (`default_level`,
`min_level`, `max_level`). What it does *not* yet expose is the
`Compression` instance dispatch — the Java enum overrides `levelValidator()`
and various stream-builder methods that wrap the JDK Deflater /
LZ4FrameInputStream / Zstd library.

**Why:** The actual codec dispatch lands in Phase 3c
(`common/compress/Compression.java` and friends), per PLAN.md lines 203–204.
Building the dispatch shim here would have to either stub the codecs or
pull `flate2`/`snap`/`lz4_flex`/`zstd` into Phase 3a — both reject the
"every translated method must do real work" principle of CLAUDE.md
rule 5.

**How to apply:**
- Phase 3c will add a `Compression` enum (or trait) with `compress()` and
  `decompress()` methods that own the codec dispatch.
- Phase 3c should wire `CompressionType::level_validator()` (which we
  did *not* add) at the same time — it returns a closure in Java, mirror
  it as a function pointer or trait object then.
- The existing `default_level`/`min_level`/`max_level` returns are
  deliberately level-only metadata; they do not require a codec, so
  they stay in Phase 3a.

**Search anchor:** `src/common/record/compression_type.rs` head doc-comment
calls this gap out for any future agent grepping for "Phase 3c".
