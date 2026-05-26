---
name: Phase 3c Snappy framing gap (xerial vs RFC)
description: Snappy codec produces snap-crate framing, not xerial-snappy framing — wire-incompatible with Java brokers
type: project
---

`src/common/compress/snappy_compression.rs` uses
`snap::write::FrameEncoder` / `snap::read::FrameDecoder`. These produce the
*standard Snappy framing format* (magic bytes
`0xff 0x06 0x00 0x00 0x73 0x4e 0x61 0x50 0x70 0x59`).

Java's `CompressionType.SNAPPY` uses `org.xerial.snappy.SnappyOutputStream`
which produces the *xerial* framing format (magic
`-126, 'S', 'N', 'A', 'P', 'P', 'Y', 0` followed by version + compatible
version + per-block headers).

**Why:** No popular Rust crate currently emits xerial-snappy framing.
`snap` only does RFC framing.

**How to apply:**
- Roundtrip within this client works (`SnappyCompressionTest` is roundtrip-only).
- Wire compatibility with Java brokers/clients is **broken** for SNAPPY
  until a xerial-framed encoder is added.
- Phase 5 / interop work: either add a small xerial-framed wrapper around
  `snap::raw::Encoder` / `Decoder`, or pull in a maintained xerial-snappy
  crate if one becomes available.

**Search anchor:** `src/common/compress/snappy_compression.rs` head
doc-comment calls this gap out for any future agent grepping for "xerial".
