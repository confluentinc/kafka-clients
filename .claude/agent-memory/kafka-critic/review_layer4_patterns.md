---
name: Layer 4 Network Transport review patterns
description: Common translation pitfalls in Java NIO to Rust I/O — EOF semantics, trait composability, sync vs async
type: feedback
---

Java NIO vs Rust I/O EOF semantics are a critical translation pitfall:
- Java NIO `channel.read()` returns `-1` for EOF, `0` for no-data-available (non-blocking)
- Rust `std::io::Read::read()` returns `Ok(0)` for EOF, `Err(WouldBlock)` for no-data-available
- Translating `bytesRead < 0 -> throw EOFException` requires mapping to `Ok(0) -> Err(UnexpectedEof)` in Rust

**Why:** This causes silent connection hangs — the client keeps polling a closed connection forever instead of detecting disconnect.

**How to apply:** Any Java NIO read that checks for `< 0` / throws `EOFException` must be translated to check for `Ok(0)` in Rust when the underlying stream is non-blocking. Watch for this in all network I/O code (Selector, KafkaChannel, etc.).

Also watch for trait composability gaps — Java uses interface inheritance (TransportLayer extends ScatteringByteChannel) so types compose naturally. Rust requires explicit trait implementations or supertraits. If a method takes `&mut dyn TraitA` but the concrete type only implements `TraitB`, there's a composability gap even if `TraitB` has all the same methods.

Additional pattern found in second review pass: When translating Java NIO code that makes multiple I/O calls in a single method (e.g., read header then read payload), be careful not to introduce conditional EOF detection based on `total_read`. Java throws EOFException unconditionally on each `channel.read()` returning -1, never swallowing it because "we already read some bytes earlier in this call". The Rust async translation can introduce this subtle bug because `Ok(0)` has different semantic weight when combined with a running total — but Java's -1 check is always independent.

Additional pattern found in third review pass: Java `SocketChannel.close()` fully closes the channel (isOpen returns false, I/O throws ClosedChannelException). Tokio `TcpStream::shutdown()` only sends TCP FIN — the socket FD remains valid until the struct is dropped. This means `stream.local_addr()` still returns Ok after shutdown, and I/O may partially succeed. Translations of Java close() should either (a) wrap stream in Option and take/drop it, or (b) track an explicit `open` flag. Prefer (a) to match resource-release semantics.
