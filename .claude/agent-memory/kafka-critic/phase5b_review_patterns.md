---
name: Phase-5b review patterns — Tokio↔Java NIO transport bridge
description: Recurring semantic-bridge gotchas when translating Java `SocketChannel`/`SelectionKey` to a Tokio `TcpStream` wrapper
type: project
---

Phase 5b-1 translated `TransportLayer` (trait) and `PlaintextTransportLayer`
(`tokio::net::TcpStream` wrapper). The class is small (~460 LOC including
tests) but the semantic bridge from Java NIO non-blocking semantics to
Tokio's `try_read`/`try_write` is dense with traps.

**Why:** the same patterns will recur in `SslTransportLayer` (5b-2),
`KafkaChannel` (5b-3), and `Selector` (5c). Catching them once in 5b-1
saves repeated debugging in later phases.

**How to apply (all of these are real defects I caught or near-missed
in 5b-1):**

1. **`Tokio try_read` returns three outcomes, not two.** Reviewers must
   walk through:
   - `Ok(n)` with `n > 0` → bytes copied (Java equivalent: positive `read()`)
   - `Err(WouldBlock)` → socket open, no data ready (Java equivalent: `read() == 0`)
   - `Ok(0)` with non-empty buf → **peer-side EOF** (Java equivalent: `read() == -1`, throws `EOFException`)
   The Java NIO contract translates `-1` to `EOFException` inside
   `NetworkReceive.readFrom`. Any Tokio-based `read()` that pattern-matches
   only WouldBlock and lets `Ok(0)` flow through without distinguishing it
   from "no data ready" is **wrong** — peer disconnect becomes invisible.
   Look for `match stream.try_read(dst)` blocks and verify the EOF arm
   exists. If the test suite only has a "quiet socket → Ok(0)" test, that
   is not enough; demand a "drop the server side, expect Err(UnexpectedEof)"
   regression test.

2. **`is_connected()` cached field is not enough on its own.** Java's
   `socketChannel.isConnected()` is a cached field (correct) — but the
   *EOF detection* in Java does not flow through that field. It comes
   from the `read() == -1` return. So caching `connected: bool` to
   avoid `getpeername()` is fine, but it does not absolve the read path
   from surfacing peer EOF. Don't let the actor's "we cache connected
   like Java" defense distract from the read-side EOF gap.

3. **Java's `disconnect()` (== `key.cancel()`) does NOT close the socket.**
   The transport's `is_open()` after `disconnect()` should still report
   `true` for the *socket* (Java: `socketChannel.isOpen() == true` until
   you call `close()`). If the Rust `disconnect()` flips a single
   `is_open: bool` flag that's also wired into `is_open()`, the semantic
   shifts: subsequent `Selector` polls in 5c will see "channel closed"
   too early. Either split into `key_valid: bool` + `socket_open: bool`,
   or document the semantic shift on the trait.

4. **Java NIO `interestOps()` / `addInterestOps()` after `key.cancel()`
   throws `CancelledKeyException`.** Rust translations that silently
   no-op `add_interest_ops` after disconnect have a divergence that
   may be benign (caller code rarely re-enters that path) but worth
   documenting. Filing as Suggestion is right; Blocking only if a
   downstream caller in 5c depends on the throw.

5. **Trait surface freeze risk.** The actor will lock in a trait method
   set during 5b-1 ("stable before SSL slots in"). Audit it against
   *all* Java callers of the trait (`KafkaChannel.java`, `Selector.java`)
   before accepting. Specifically check `transportLayer.socketChannel()
   .socket().getInetAddress()` / `.getPort()` chains — those imply
   `peer_addr()` / `local_addr()` accessors must be on the trait, not
   only on the concrete struct. Inherent methods on
   `PlaintextTransportLayer` are not visible through `dyn TransportLayer`.

6. **`requireNonNull` ≠ `assert!(!is_empty())`.** Java's
   `Objects.requireNonNull` rejects null only; Rust `String` is
   non-nullable, so the moral equivalent is *no check at all*.
   Translations that add `assert!(!s.is_empty(), ...)` reject inputs
   Java would accept, panic instead of fault-on-precondition, and
   change the public-API contract (CLAUDE.md rule 4). Always file as
   at least Suggestion; Blocking if the affected constructor is
   exposed in `pub` API and called with potentially-empty strings
   from translated code.

7. **Java `static final` singletons → Rust requires explicit caching.**
   `KafkaPrincipal.ANONYMOUS` is one allocation in Java (class init).
   A Rust `pub fn anonymous() -> Self { Self::new(...) }` is two `String`
   allocations per call. Filed as Performance Suggestion. The fix is
   typically `OnceLock<T>` + return-by-clone, or `Arc<str>` for the
   inner fields.

8. **Test pattern audit for non-blocking transports.** The actor's
   inline test suite typically covers (a) construction, (b) addr
   round-trip, (c) interest-op flips, (d) close, (e) WouldBlock →
   Ok(0). Watch for missing (f) **peer-EOF → expected error**, (g)
   **partial write under backpressure**, (h) **connect-pending → ready
   transition**. Of these, (f) is the highest-yield: it directly
   exercises the Tokio↔Java NIO bridge.

9. **`pending_connect` constructor in Tokio has limited utility.**
   Tokio's `TcpStream::connect` is fully async (resolves only when
   connect completes), so there is no observable "still connecting"
   `TcpStream`. The `pending_connect` / `finish_connect` pair exists
   for parity with Java, but its actual use site in the Rust Selector
   is suspect — `finish_connect` will always immediately succeed
   because `peer_addr()` succeeds the moment Tokio hands you the
   stream. Don't flag this as a bug, but note it: Phase 5c may end
   up with `finish_connect` as effectively dead code.
