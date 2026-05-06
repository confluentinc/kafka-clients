---
name: Phase 3c review patterns
description: High-yield review areas for Java compress/codec translations — LZ4 framing, snappy framing variants, level knobs, atomic ratio estimators
type: project
---

## Phase 3c yielded zero issues — what worked

The compression module was the cleanest translation reviewed so far.
Pattern: large translation with 2 documented deferrals up-front
(Snappy xerial framing, LZ4 level knob). Both flagged in actor-memory
notes BEFORE I reviewed. Critic time was spent verifying the framing
fidelity, not chasing surprises.

## High-yield review checks for codec translations

1. **Wire-format byte-exact verification**: For every codec translated
   from Kafka, the framing bytes must match Java exactly. Approach:
   read the Java `*OutputStream.java` line-by-line and assert each
   field width, endianness, bit packing, and checksum coverage range.
   The unit test that re-implements the checksum from scratch (e.g.
   LZ4's `compression_frame_structure` recomputing XXH32 from
   post-magic bytes) is the highest-value test — a single-impl
   round-trip will pass even with a swapped seed.

2. **Checksum seed/algorithm verification**: A wrong seed (`XxHash32::with_seed(N)`
   for any N != 0) silently passes round-trips but fails wire compat.
   Look for a `*_known_vectors` test asserting a hardcoded canonical
   value (`xxh32(&[]) == 0x02CC5D05` for XXH32-empty-string-seed-0).

3. **Bit-packed descriptor validation**: FLG/BD-style bytes have
   reserved=0 bits that must not collide with version/flag bits. Check
   the `validate()` calls reject the same conditions as Java
   (`reserved != 0`, `version != 1`, `block_independence != 1`).

4. **`useBrokenFlagDescriptorChecksum` style legacy compat**: If the
   Java codec has a "broken for V0" mode, verify the Rust translation
   wires it through the magic-byte version. LZ4 uses
   `message_version == MAGIC_VALUE_V0` to enable the broken mode in
   `wrap_for_output`. Easy to forget if writing from scratch.

5. **Snappy framing trap**: `snap` Rust crate produces RFC framing,
   NOT xerial framing that Kafka uses. This is the single most likely
   wire-compat issue in any Snappy translation. Always check the
   producer output magic bytes against
   `[0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0x00, ...]`.

6. **Compression level knob honored or documented**: `lz4_flex`
   doesn't accept levels (always uses LZ4_compress_default). Verify
   the level field is recorded for parity but the divergence is
   documented. Acceptable for non-wire-format performance knobs.

7. **`CompressionRatioEstimator` step-direction mistake**: The Java
   constants are deliberately named the opposite of what intuition
   suggests:
   - `DETERIORATE_STEP = 0.05` is used when observed > current
     (the ratio just got worse — react quickly).
   - `IMPROVING_STEP = 0.005` is used when observed < current
     (the ratio just got better — react slowly).
   Easy to swap. Verify by reading the Java code, not the constant
   names.

8. **`Compression::of(name)` static factory not object-safe**: Java's
   interface has static `Compression.of(name)` factories. Rust trait
   methods can't be static while remaining object-safe. Acceptable
   to skip if callers go through the per-codec `Builder` or
   `CompressionType::wrap_for_output`. Check that no caller would
   actually want the static factory.

## False-positive avoidance learned this phase

1. `Lz4BlockInputStream` copying uncompressed blocks (instead of
   slicing `in` directly like Java) is a **performance** issue not a
   **correctness** issue, and lifetime-justified — flag as Performance
   if at all, not as a Bug.

2. `BufferSupplier` split into two layers in `Lz4Compression::wrap_for_input`
   is a documented behavioural difference, not a bug. Java's
   `BufferSupplier` is single-threaded so sharing one across both
   layers is safe in Java; Rust's ownership requires either two
   suppliers or `Rc<RefCell<…>>`. The translation chose two suppliers
   (cleaner ownership). Don't flag.

3. `std::mem::forget(lz4)` in test code to skip the destructor's
   end-mark is intentional (mirrors Java's `flush()`-without-`close()`).
   Don't flag the leak.

4. `DashMap` as the per-topic store in `CompressionRatioEstimator`
   replaces Java's `ConcurrentMap`. `dashmap` is a popular crate
   already direct-deped from earlier phases. Don't flag.

## Phase 3c deferral pattern

Two documented deferrals (Snappy xerial framing, LZ4 level knob) were
flagged by the actor BEFORE the review. Both have explicit
actor-memory notes (`phase3c_snappy_framing_gap.md`,
`phase3c_compression.md`). The review confirmed the deferrals are
acceptable for Phase 3c (no broker yet) but tracked them for Phase 5
(Snappy is wire-compat-blocking) and ongoing performance work
(LZ4 level is a performance-only divergence). DEFERRED-OK is the
right severity for both.
