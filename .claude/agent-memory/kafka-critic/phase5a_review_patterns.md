---
name: Phase-5a review patterns — network primitives & framing
description: Translation gotchas in Send/Receive traits, NetworkReceive read paths, and ChannelState/SecurityProtocol erasure
type: project
---

Phase 5a translated `common/network/` primitives plus `clients/ClientRequest`/
`ClientResponse`/`RequestCompletionHandler` and added `AbstractRequestBuilder`
for the first time. Key patterns I saw:

**Why:** Phase 5a is the first network sub-phase, no Tokio yet — value types
plus framing logic. The same patterns will repeat in 5b/5c when `KafkaChannel`
and `Selector` arrive, so getting them right here matters.

**How to apply:**

1. **Test-fixture constructors that pre-fill state for convenience can silently
   diverge from Java semantics.** `NetworkReceive::with_buffer` pre-set
   `size_pos = 4` to make `size()` return the same value as Java's, but this
   side-effects `complete()` and `bytes_read()` to return Java's "wrong"
   answers. When reviewing a constructor that "looks like" a Java constructor,
   compute every observable method on the resulting object and confirm both
   languages agree.

2. **`vec![0u8; n]` scratch buffers in a translated `read_from` are hot-path
   regressions.** Java's `ByteBuffer.read(channel)` reads directly into the
   backing buffer with no temporary. Rust translations that use a scratch
   `Vec` plus `extend_from_slice` double-copy and double-allocate. Flag these
   even when the brief says "the read code will be replaced in a later phase"
   — the API shape established in the trait survives the implementation
   replacement.

3. **NPE-mirroring panics: file as Suggestion, not Blocking.** Java's
   `NetworkReceive.size()` NPEs through `payload().limit()` when the receive
   isn't ready. The Round-2 disposition kept the panic and tightened the
   rustdoc precondition — defensible since CLAUDE.md rule 10.1 endorses
   panic for unrecoverable invariants and the only legitimate caller
   (metrics emission) gates on `complete()` first. Lesson: before
   recommending `Option<T>` over `panic!`, audit every call site. If they
   all already check the precondition, the panic is fine.

4. **`Locale.ROOT` case folding is NOT equivalent to `to_ascii_uppercase`.**
   Rust's `to_ascii_uppercase` only handles A-Z↔a-z. Java's
   `toUpperCase(Locale.ROOT)` does locale-independent Unicode folding (Turkish
   dotted-I, German `ß`, etc.). Whether to flag depends on context: for
   listener names / security-protocol names that are ASCII by spec, the
   divergence is theoretical and CLAUDE.md rule 11 prefers ASCII folding.
   Before flagging, check (a) is there a Java test exercising non-ASCII
   inputs? (b) is there a config validation that rejects non-ASCII
   upstream? If both answers are no, the divergence is real. If either is
   yes, the rejection is defensible.

5. **Rust API duplication for "panic vs Result" is rarely justified.** When a
   Java constructor throws `IllegalStateException` for an invariant violation,
   the Rust translation should pick exactly one shape (panic or Result) — not
   both. Adding a `try_*` variant alongside the panicking one creates
   unjustified API surface (CLAUDE.md rule 7 / DoD #7). Test-only callers do
   not justify a public method.

6. **Send trait collision with `std::marker::Send`** is handled by writing
   `Box<dyn Send + std::marker::Send>` where the unqualified `Send` resolves
   to the local `super::Send`. Unusual but readable when accompanied by a
   docstring on the trait file. Don't flag.

7. **`Arc<dyn Trait>` for callbacks/builders is correct** for `ClientRequest`
   even though `ClientRequest` is conceptually per-message: it's actually
   per-batch (one `ProduceRequest` carries N records). Verify before flagging
   `Box`/`Arc` allocations as hot-path issues.

8. **Behavior-equivalence tests for value types**: when CipherInformation /
   ClientInformation collapse "" to "unknown" in the constructor, ensure
   the test exercises both populated and empty paths. The Actor's tests do.

9. **AbstractRequestBuilder trait erasure**: Java `Builder<T>` is generic; Rust
   has to erase to `dyn` because `ClientRequest` holds heterogeneous builders.
   Don't flag the erasure as a CLAUDE.md rule 7 violation — Phase 5a's
   producer hot path uses it once per produce request, not per message.
