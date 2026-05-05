---
name: Phase 5b-1 TransportLayer + PlaintextTransportLayer
description: Trait-shape decisions for `TransportLayer` and how `SslTransportLayer` (Phase 5b-2) plugs in
type: project
---

Phase 5b-1 landed `TransportLayer` (super-trait of `TransferableChannel`) and `PlaintextTransportLayer` (Tokio `TcpStream` wrapper).

**Why:** the trait shape needs to be stable before SSL slots in. Locking the surface here means 5b-2 only adds a struct, not a refactor.

**How to apply (esp. for Phase 5b-2 / 5c):**

1. **Trait method set**: `ready` / `is_open` / `is_connected` / `finish_connect` / `disconnect` / `close` / `read(&mut [u8])` / `handshake` / `peer_principal` / `add_interest_ops` / `remove_interest_ops` / `interest_ops` / `is_mute` / `has_bytes_buffered`. SSL adds nothing new — overrides `ready` (false until handshake completes), `handshake` (drives `rustls::ClientConnection`), `peer_principal` (extracts from cert chain), `has_bytes_buffered` (true when `rustls` has decrypted bytes pending `reader().read`).

2. **Java's `socketChannel()` and `selectionKey()` getters were intentionally dropped.** Tokio provides readiness; the upper Selector reaches state via `add_interest_ops`/`remove_interest_ops`/`is_mute` directly on the transport. Do **not** add them to the trait when translating SSL or KafkaChannel — the Java-side callers always pass through `interestOps()` which is already exposed.

3. **`OP_READ`/`OP_WRITE`/`OP_CONNECT` constants** live at `crate::common::network::transport_layer::{OP_READ,OP_WRITE,OP_CONNECT}`. Use the same bit values as Java's `SelectionKey` (1, 4, 8) so any Java code logic that bitwise-ORs constants ports literally. SSL uses these the same way: handshake sets `OP_WRITE` when `rustls.wants_write()`, clears it when only `wants_read()`.

4. **Non-blocking I/O contract**: `read`/`write_vectored` return `Ok(0)` on `WouldBlock` (Java NIO "would block" semantic, matching `NetworkReceive::read_from`'s expectation). SSL must mirror this — when `rustls` `process_new_packets()` reports no progress, return `Ok(0)`, not an error.

5. **`io::Read` impl on PlaintextTransportLayer**: lets the transport be passed directly as `&mut dyn io::Read` to `Receive::read_from`. SSL needs the same impl so `KafkaChannel::receive(&mut transport)` works uniformly. The impl is `TransportLayer::read(self, buf)` forwarder.

6. **`peer_principal` returns `KafkaPrincipal` by value** (not `&KafkaPrincipal`). RVO + small struct (3 fields). 5b-2's SSL impl will construct a fresh `KafkaPrincipal::new(USER_TYPE, peer_x500_dn)` from the rustls peer cert chain.

7. **`connected: bool` cached field** (mirrors Java's `SocketChannel.isConnected()` cached state — avoids `getpeername()` syscall on every Selector tick). SSL must hold the same flag, set true when the handshake completes.

8. **`disconnect` vs `close`**: `disconnect` only flips state flags (mirrors Java `selectionKey.cancel()`); `close` also drops the `TcpStream`. SSL must drop both the rustls session and the TCP stream on `close`.

9. **Test pattern for transport tests**: localhost `TcpListener` + `TcpStream::connect`, joined via `tokio::join!`. The accept side returns `Result<(TcpStream, SocketAddr), Error>` — destructure carefully: `let (server, _) = accepted.expect("accept");`.

10. **`KafkaPrincipal`** lives at `crate::common::security::auth::KafkaPrincipal` (newly added in 5b-1). `PartialEq`/`Eq`/`Hash` ignore `token_authenticated` to match Java semantics — manual impls, not derive.
