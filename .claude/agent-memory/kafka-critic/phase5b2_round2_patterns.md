---
name: Phase-5b-2 Round-2 verification patterns
description: TLS read state-machine bookkeeping bugs and verification heuristics for fixup commits
type: project
---

# Phase-5b-2 Round-2 verification patterns

Round-2 review of fixup `5fa1bed` (Actor's response to 6 Round-1 SSL
suggestions) surfaced a real residual bug despite the fixup's tests
passing. Pattern worth remembering for future TLS / state-machine
review.

## Pattern: "early-return drain bypasses the bookkeeping update"

When a Java method has a single bookkeeping update at the end
(`updateBytesBuffered(...)` reading both buffer positions), translating
it to Rust by capturing state inside a loop body is **not** sufficient
if there's an early-drain step *before* the loop. The early-drain may
satisfy the loop's exit condition, causing the loop body — and the
state capture — to be skipped entirely.

**Why:** Java's `appReadBuffer` is a persistent member field, so the
final `updateBytesBuffered` reads the latest `position()` regardless of
which code path produced the data. Rustls's `IoState` is *transient* —
it's only available as the return of `process_new_packets`. Capturing
it inside the loop creates a dependency on the loop running at least
once.

**How to apply:**
- When reviewing a `read`-method translation that has a "drain
  already-queued data" preamble before a network-read loop, trace the
  case where the preamble fully fills dst. Verify the post-loop
  bookkeeping is still correct in that case.
- A correct fix initializes the state from rustls *before* the
  preamble drain (a precondition `process_new_packets` call) so the
  variable is populated regardless of which exit path is taken.
- Or restructure to collapse the two drain sites — eliminate the
  preamble entirely.

## Pattern: "test uses oversized destination buffer, masks edge case"

The new regression test `has_bytes_buffered_false_after_full_drain`
uses `buf = [0u8; 4096]` for a 10-byte payload. This is fine for the
"queue empty after full drain" assertion but does **not** exercise the
opposite edge — "queue non-empty because dst is too small". Always
verify behaviour-mismatch tests cover BOTH directions of the binary
state they assert.

## Pattern: "doc-comment claims approximation when value is exact"

The fixup's `PLAINTEXT_BUFFER_LIMIT = 64 * 1024` comment claims "same
order of magnitude as rustls's internal `DEFAULT_BUFFER_LIMIT`". The
actual value is **exactly equal** (`pub(crate) const DEFAULT_BUFFER_LIMIT
: usize = 64 * 1024;` in rustls 0.23). Not a bug, but worth noting:
when the actor's doc-comment under-claims precision, double-check the
chosen constant against the upstream value.

## Pattern: "macOS test flake → broaden error matches"

ConnectionReset added to a `matches!(err.kind(), UnexpectedEof |
InvalidData)` is acceptable when:
- The original assertion is "peer dropped" semantics, not a specific
  IO error code.
- All accepted error kinds map to the same upper-layer event.
- The flake reproduces under load (verify the kernel-RST-vs-FIN race
  is plausible).

Reject this kind of broadening if any of:
- The test was asserting a specific failure mode and ConnectionReset
  represents a different one.
- The broadening makes the test pass against a real defect (e.g. a
  spurious early disconnect).

## Verification heuristic

For SSL/TLS read-path Round-2 review:
1. Read the Java `read(ByteBuffer dst)` end-to-end. Identify all paths
   that update `hasBytesBuffered` (or its equivalent).
2. In Rust, trace each path. Confirm the bookkeeping fires in all of
   them, not just the typical-case loop body.
3. Look for "drain before loop" patterns and check the
   `dst-fully-filled-before-loop` corner case.
4. Verify any new regression tests exercise BOTH directions of the
   asserted state, not just the easy one.
