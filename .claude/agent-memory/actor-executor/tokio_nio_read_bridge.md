---
name: Tokio↔Java NIO read bridge
description: Three-way read outcome translation when wrapping tokio::net::TcpStream behind Java NIO `SocketChannel.read(ByteBuffer)` semantics
type: feedback
---

When translating any Java NIO `SocketChannel.read(buf) → int` adapter to a sync `io::Read` over Tokio's `TcpStream::try_read`, distinguish all THREE outcomes — never collapse two.

| Tokio outcome                      | Java NIO equivalent     | Adapter must return                        |
| ---                                | ---                     | ---                                        |
| `Ok(n)` with `n > 0`               | `read() == n` (positive) | `Ok(n)`                                    |
| `Err(WouldBlock)`                  | `read() == 0`            | `Ok(0)` (Java's "would block" signal)     |
| `Ok(0)` with **non-empty** buf     | `read() == -1` (EOF)     | `Err(io::ErrorKind::UnexpectedEof)`       |

**Why:** Tokio docs explicitly say "If the buffer is non-empty and `Ok(0)` is returned, this indicates EOF". Java's `NetworkReceive.readFrom` translates `read() == -1` into `throw new EOFException()`, and the upper Selector relies on that signal to mark the channel disconnected. Collapsing both `WouldBlock` and `Ok(0)` into `Ok(0)` makes a half-closed socket look identical to a quiet socket — `Selector::poll` will spin on a dead fd forever, and `is_connected()` will keep returning true since it reads a cached flag.

**How to apply:**
- For `PlaintextTransportLayer::read` and any `SslTransportLayer::read` — distinguish the three outcomes explicitly; don't share a `match Ok(n) => Ok(n), Err(WouldBlock) => Ok(0)` arm without the `Ok(0) =>` arm above.
- Regression test pattern: build a `connected_pair()` (TcpListener + TcpStream::connect), wrap the client in the transport, drop the server, await `stream.readable()` (FIN lands), then assert next `read()` returns `UnexpectedEof`. Add a second test for the `io::Read` forwarder used by `NetworkReceive`.
- For the *write* side (`try_write` / `try_write_vectored`), Tokio does not have an "EOF on write" outcome the same way — `WouldBlock → Ok(0)` is the only translation needed.
- Callers that compose `io::Read::read` (like `NetworkReceive::read_from`) can rely on `Ok(0)` meaning "quiet, retry on next select wake" because the transport surfaces EOF as `Err`. Don't add EOF detection at the `NetworkReceive` layer — it would double-translate.

Origin: Phase 5b-1 review found the bug in commit `63fac0c` and fixed it in fixup `2c9050b`. The bug was silent because the existing unit test `read_returns_ok_zero_when_no_data_ready` only exercised the WouldBlock path.
