---
name: Phase 5a review fixes
description: Patterns learned from Phase 5a network primitives review round (NetworkReceive, ClientResponse)
type: project
---

Phase 5a Critic round produced 5 Suggestion-severity comments. Three fixes,
two rejections.

**Fixes:**
- `NetworkReceive::with_buffer` must NOT synthesise the size header — Java leaves
  `size` as a fresh `ByteBuffer.allocate(4)` so `complete()=false`,
  `bytes_read()=0`, `size()` falls back to `payload.len()+4`.
- `NetworkReceive::read_from` payload-fill path: pre-allocate the buffer once at
  `requested_buffer_size` (zeroed so `&mut [u8]` is valid), then read in place
  via `&mut buf[payload_pos..total]`. Track write cursor in a separate
  `payload_pos: usize` field. **Don't** rely on `BytesMut::len` as the
  cursor — it forces per-call extend_from_slice copies.
- Removed `ClientResponse::try_with_timed_out` (no production caller) — kept
  only the panicking `with_timed_out`.

**Rejections (with rationale captured in COMMENTS.DONE.0.md):**
- `size()` panic vs `Option`: panic mirrors Java NPE; CLAUDE.md rule 10.1
  endorses panic for unrecoverable invariants.
- `ListenerName` ASCII vs Unicode case folding: listener names are ASCII by
  Kafka spec; CLAUDE.md rule 11 prefers ASCII case folding.

**Why:** Critic 0 catches API-surface drift (extra methods Java doesn't have),
Java semantic divergence in constructors, and per-message hot-path allocations.
Always go back to the Java source and trace the exact field state through every
constructor — eager pre-population of fields is a common drift pattern.

**How to apply:** When translating a Java constructor with multiple overloads,
each overload's *uninitialised* field state matters as much as its
initialised state. `field = ByteBuffer.allocate(N)` is NOT the same as
"already populated with N bytes" — it's an *empty* buffer with capacity N.
