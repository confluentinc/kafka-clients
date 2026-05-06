---
name: Phase 3a CompressionType codec-dispatch gap (RESOLVED in Phase 3c)
description: Historical note — codec dispatch was deferred from Phase 3a, wired in Phase 3c
type: project
---

**Status: RESOLVED in Phase 3c.**

Phase 3a left `CompressionType` with no dispatch into actual codecs. The
`Compression` trait, the per-codec types (`NoCompression`, `GzipCompression`,
`SnappyCompression`, `Lz4Compression`, `ZstdCompression`), the LZ4 frame
streams, and the `level_validator()` closure now exist, and
`CompressionType::wrap_for_output` / `wrap_for_input` dispatch through them
using each codec's default level.

Look at `phase3c_compression.md` for the Phase 3c summary.

**Search anchor:** see `src/common/record/compression_type.rs` head doc-comment
and `src/common/compress/`.
