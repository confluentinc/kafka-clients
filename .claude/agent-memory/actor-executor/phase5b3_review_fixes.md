---
name: Phase 5b-3 review fixes
description: Patterns from Phase 5b-3 reviewer fixes — lazy-lookup via trait arg, mock partial-write cap, pub(crate) for Java package-private
type: feedback
---

Phase 5b-3 review (commit 253c383) had 5 Suggestion comments. Two patterns
worth keeping for future translations:

**Lazy-lookup via trait method argument.** Java's `SslAuthenticator` stores
a transport reference and reads `transportLayer.sslSession()` lazily on
every `principal()` call. The first Rust translation cached the principal
at construction time, which froze the pre-handshake anonymous value.

**Why:** Caching at construction means the handshake-completion event
never updates the cached value. The Java pattern is "store reference,
look up state on demand."

**How to apply:** When the Java class holds a reference to another struct
purely for lazy state queries (not ownership), don't wrap it in
`Arc<Mutex<_>>` or duplicate the reference in Rust — change the trait
method signature to take the relevant `&dyn Trait` from the owning parent.
The owning struct (here `KafkaChannel`) forwards `self.transport.as_ref()`
into `authenticator.principal(transport)` on every call. Stateless impl,
single ownership chain, lazy semantics preserved.

---

**Mock partial-write cap for Mockito chain translation.** Java's
`KafkaChannelTest.testSending` configures
`when(transport.write(any())).thenReturn(4, 64, 64)` to drive partial
writes. A simple Rust mock that always writes everything collapses the
test to a single tick and silently drops the partial-progress contract.

**Why:** The producer write path's `bytes_remaining > 0 →
maybe_complete_send() == None` invariant is what lets the Selector
schedule another tick. A test that only exercises full-write completion
won't catch a regression that breaks the partial branch.

**How to apply:** Give your mock state an `Option<usize>` cap field
(`max_bytes_per_write`) plus a setter helper. Default `None` keeps the
existing tests one-shot; tests that need the multi-tick contract set the
cap before each tick. The `write_vectored` impl iterates buffers and
truncates on `cap.saturating_sub(total)` to mimic the kernel-buffer-full
return path.

---

**Java package-private → `pub(crate)` for cross-module-but-in-crate use.**
When a Java method is package-private (no modifier) and the only Rust
caller is a sibling module that hasn't been written yet, prefer
`pub(crate)` + `#[allow(dead_code)]` over `pub`.

**Why:** `pub` widens the API beyond what Java exposed. `pub(crate)`
faithfully mirrors the boundary while still being callable from sibling
modules in the same crate (the Phase 5c Selector here). The `dead_code`
lint fires until a real caller exists, but the annotation is honest about
the deferral and doesn't quietly leak the method to downstream consumers.

**How to apply:** Default to `pub(crate)` for any Java package-private
method whose only legitimate caller is internal to the crate. Add
`#[allow(dead_code)]` with a one-line rustdoc note pointing to the phase
that will wire it up. Don't widen to `pub` just to silence the lint —
that's the wrong fix.
