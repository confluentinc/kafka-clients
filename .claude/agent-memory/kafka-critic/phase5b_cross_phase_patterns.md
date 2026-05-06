---
name: Phase 5b cross-phase review patterns
description: Patterns that recurred across all three Phase 5b sub-phases (TransportLayer, SslTransportLayer, KafkaChannel) — carry to Phase 5c (Selector + Authenticator)
type: project
---

Cross-phase patterns from Phase 5b (5b-1 TransportLayer, 5b-2 SslTransportLayer, 5b-3 KafkaChannel + ChannelBuilders), all accepted. Worth carrying to Phase 5c.

**Why:** Phase 5c (Selector wiring, Authenticator hierarchy, complete connection lifecycle) sits on top of these primitives. The same translation-bug shapes recur whenever the Java NIO mental model meets Tokio/rustls.

**How to apply:** When reviewing Phase 5c, sweep these specific axes — they were each independently caught in 5b-1/5b-2/5b-3 and are very likely to repeat at the Selector layer.

## Recurring high-yield axes

1. **EOF / disconnect signal preservation across the stack.** Java's NIO uses sentinel return values: `read() == -1` (EOF), `IOException` (peer reset). Rust+Tokio split these into `Ok(0)`, `WouldBlock`, `UnexpectedEof`, `ConnectionReset` — and several other variants on macOS. Each layer (`PlaintextTransportLayer`, `SslTransportLayer`, `KafkaChannel`, and now `Selector`) must translate the lower-layer signal correctly into the upper-layer's contract. 5b-1 had `read()` collapsing peer-EOF into `WouldBlock`; 5b-2 had `has_bytes_buffered` lying about queued plaintext after partial drain. Expect the same in `Selector::poll` / `attemptRead`.

2. **State desync after lifecycle transitions.** After `disconnect()` / `close()` / authentication-failure, Java keeps several flags consistent (`isOpen`, `isMuted`, `interestOps`, `isReady`). Rust translations have repeatedly had one flag updated and others left stale (5b-1 disconnect/key-cancel divergence, 5b-3 `is_mute` true on not-yet-ready SSL). For Phase 5c, audit `Selector::close(channelId)` vs `Selector::poll` — every flag the Java version touches must also be touched in Rust.

3. **Pre-filled / pre-baked test fixtures masking real bugs.** Multiple times in 5b a fixture pre-populated buffer state that the production code path would never produce, so the test passed for the wrong reason (5a-1 `NetworkReceive` size header, 5b-2 oversized buffers masking back-pressure). For Phase 5c selector tests, if a fixture inserts state directly into `KafkaChannel`/`SocketChannel` instead of going through the Selector's own register/connect path, that's a smell.

4. **Java requireNonNull / null-check translated as `assert!(!is_empty())`.** This is the wrong shape (5b-1 `KafkaPrincipal::new`, 5a-3). Java accepts empty strings; the requireNonNull only forbids null. Rust types already forbid null via `&str`. Don't add semantic constraints that weren't in Java.

5. **Eager caching of values that should be lazy.** 5b-3 `peer_principal()` was cached at construction time — Java computes it on demand after handshake completes. For the Selector + Authenticator pair in 5c, watch for any `Arc<Cached>` field that's populated before the underlying handshake/auth has finished — verify by reading what the corresponding Java getter returns when called too early.

6. **Trait surface freeze risk.** Each sub-phase added a trait (`TransportLayer`, `Authenticator`-likes); each one had at least one accessor missed that the next sub-phase needed (5b-1 missed `peer_addr`/`local_addr`, 5b-2 needed cipher/principal accessors). For 5c, look at the Java `Selector` and ensure every public-method/getter that downstream consumers (`NetworkClient`, etc.) call is on the Rust trait — not just the ones used inside the current phase's tests.

7. **`pub` vs `pub(crate)` for `internal` packages.** CLAUDE.md rule 2 says `internal` packages must be `pub(crate)`. 5b-3 had three methods (`mute`, `maybe_unmute`, `complete_close_on_authentication_failure`) as `pub`. Sweep 5c on the same axis — anything in `org.apache.kafka.common.network.internals` or callable only from `Selector` should be `pub(crate)`.

8. **Constant file audit + deferred-test mapping verified.** Phase 4c showed that Actor's "deferred to next sub-phase" claims sometimes drop tests on the floor. For 5c, when tests are deferred to a follow-up phase, verify the manifest line-by-line against the Java file at end of phase.

## Process patterns that worked

- **Round-2 sweeps catch what Round-1 missed.** In every Phase 5b sub-phase, Round 2 found 1–2 real issues (mostly Suggestion-level but legitimate). Don't skip Round 2.
- **Trivial-fixup verification can be 1-shot.** When a Round-2 issue is a clear single-line fix (5b-3 Issue 6), verifying the resulting fixup in one tool-call set (show + test) is enough — no need for full re-review.
- **Working-tree COMMENTS.0.md is gitignored but valid.** The Phase 4 force-add precedent at end-of-phase boundary means the working-tree file is the authoritative open-issue list during a phase. Don't be confused by `git status` showing it as untracked.
