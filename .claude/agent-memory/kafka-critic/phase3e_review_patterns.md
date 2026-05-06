---
name: Phase 3e wire-fixture review patterns
description: Verifying Java-captured byte fixtures and their associated codec close-path fixes
type: project
---

# Phase 3e — wire-format byte fixtures vs Java

Phase 3e locks the Rust v2 batch encoder against hex byte vectors captured from Apache Kafka 4.2's `MemoryRecords.withRecords` / `withIdempotentRecords` via a `RustFixtureCapture` Java `main()` program in the kafka submodule.

**Why:** byte-equality against Java-captured fixtures is the ONLY reliable guard against silent encoder drift. Round-trip tests pass even if both sides are consistently wrong.

**How to apply during review:**

- **Codec close-path is the highest-yield bug source.** Java's `BufferedOutputStream(codec).close()` chains `flush()` → `out.close()`; Rust must mirror exactly:
  - `flate2::GzEncoder::flush` injects a `Z_SYNC_FLUSH` block (`00 00 00 00 ff ff`) that Java does NOT emit. The `MemoryRecordsBuilder::close` path must drop the writer (which calls `try_finish` in `Drop`) WITHOUT a prior `flush()`.
  - `zstd::Encoder::Drop` does NOT auto-finish. Wrap it in a struct whose `Drop` calls `flush()` then `finish()` to match Java's `flushStream()` + `endStream()` chain.
  - Both fixes are byte-verified by the Phase 3e fixture assertions.

- **Snappy is a known wire-incompat (xerial vs RFC framing)**: the fixture is captured for reference but assertion is deferred. Look for a tripwire test that asserts the xerial magic `82 53 4E 41 50 50 59 00` is at offset 61 (= `RECORD_BATCH_OVERHEAD`) — this protects the asset from silent rot until Phase 5 resolves the gap.

- **Determinism in Java capture**: confirm timestamps are explicit (not `currentTimeMillis()`) and that the factory uses `CreateTime` (not `LogAppendTime`). Java's `withRecords(...)` and `withIdempotentRecords(...)` both default to `CreateTime`.

- **Submodule bumps for capture programs**: verify the diff is *only* the new program with no unrelated upstream changes. Submodule SHA should be a direct descendant of the previously-pinned commit.

- **Hex parser**: a project-local hex parser (no `hex` crate dep) is fine if there's a round-trip sanity test. Whitespace handling via `chars().filter(!is_whitespace)` is sufficient for single-line `.hex` files.

- **Module gating**: fixture test modules should use both `#[cfg(test)]` on the `mod` declaration AND `#![cfg(test)]` inside the file. Hex bytes loaded via `include_str!` (compile-time, no runtime IO).
