# Layer 4: Network Transport — Implementation Plan

## Overview

Layer 4 establishes low-level TCP I/O and Kafka protocol framing. It sits between the wire protocol (Layer 2) and the higher-level channel abstraction (Layer 5).

**7 classes to implement** (~700 lines of Java source):

| Class | Java File | Lines | Rust Module |
|-------|-----------|-------|-------------|
| TransportLayer | TransportLayer.java | 93 | `common::network::transport_layer` |
| PlaintextTransportLayer | PlaintextTransportLayer.java | 216 | `common::network::plaintext_transport_layer` |
| Send | Send.java | 45 | `common::network::send` |
| Receive | Receive.java | 55 | `common::network::receive` |
| NetworkSend | NetworkSend.java | 53 | `common::network::network_send` |
| NetworkReceive | NetworkReceive.java | 154 | `common::network::network_receive` |
| ByteBufferSend | ByteBufferSend.java | 84 | `common::network::byte_buffer_send` |

Supporting: `TransferableChannel` (interface), `InvalidReceiveException` (→ error variant)

## Java NIO → Rust Mapping

| Java NIO | Rust Equivalent |
|----------|----------------|
| `SocketChannel` | `tokio::net::TcpStream` (per CLAUDE.md rule 8) |
| `SelectionKey` | `tokio::io::Interest` + readiness state |
| `ScatteringByteChannel` | `tokio::io::AsyncRead` / vectored read |
| `GatheringByteChannel` | `tokio::io::AsyncWrite` / vectored write |
| `ByteBuffer` | `Vec<u8>` with position tracking or `bytes::BytesMut` |
| `FileChannel.transferTo` | `tokio::io::copy` or sendfile syscall |
| `MemoryPool` | Simple `Vec<u8>` allocation (no pooling initially) |
| `InvalidReceiveException` | `KafkaError` variant or `std::io::Error` |

## Rust-Specific Design Decisions

### 1. Async vs Sync
Per CLAUDE.md rule 8: "Use non-blocking IO (Tokio) with a single Selector for multiple TCP connections."

All I/O methods (`read`, `write`, `connect`) will be `async`. The `TransportLayer` trait methods that do I/O will return `Future`s.

### 2. Send/Receive as traits
- `Send` trait (renamed to `KafkaSend` to avoid conflict with `std::marker::Send`) — or use `SendData` / `Sendable`
- `Receive` trait — no naming conflict

### 3. TransportLayer trait
Java's `TransportLayer` extends `ScatteringByteChannel` + `TransferableChannel`. In Rust:
- Implement `AsyncRead` + `AsyncWrite` on the transport layer struct
- Add Kafka-specific methods (ready, handshake, mute/unmute, interest ops)
- `SelectionKey` management becomes internal state (interest ops tracked as bitflags)

### 4. NetworkReceive size-delimited protocol
Two-phase reading:
1. Read 4-byte big-endian size header
2. Allocate and read N-byte payload

```
Wire format: [4 bytes: size (big-endian)] [N bytes: payload]
```

### 5. ByteBufferSend scatter-gather
Multiple buffers written in sequence. In Rust, use `IoSlice` for vectored writes.

## Implementation Order

### Step 1: Core traits and types
1. **`KafkaSend` trait** (`common/network/send.rs`)
   - `fn completed(&self) -> bool`
   - `async fn write_to(&mut self, channel: &mut impl AsyncWrite) -> io::Result<usize>`
   - `fn size(&self) -> usize`

2. **`Receive` trait** (`common/network/receive.rs`)
   - `fn source(&self) -> &str`
   - `fn complete(&self) -> bool`
   - `async fn read_from(&mut self, channel: &mut impl AsyncRead) -> io::Result<usize>`
   - `fn required_memory_amount_known(&self) -> bool`
   - `fn memory_allocated(&self) -> bool`

### Step 2: Concrete implementations
3. **`ByteBufferSend`** (`common/network/byte_buffer_send.rs`)
   - Fields: `buffers: Vec<Vec<u8>>`, `size: usize`, `remaining: usize`, `pending: bool`
   - `fn size_prefixed(buffer: Vec<u8>) -> Self` — prepends 4-byte size header
   - Implements `KafkaSend`

4. **`NetworkReceive`** (`common/network/network_receive.rs`)
   - Fields: `source: String`, `size_buf: [u8; 4]`, `size_bytes_read: usize`, `max_size: i32`, `buffer: Option<Vec<u8>>`, `buffer_bytes_read: usize`
   - Constants: `UNKNOWN_SOURCE`, `UNLIMITED`
   - `fn payload(&self) -> &[u8]`
   - `fn bytes_read(&self) -> usize`
   - Implements `Receive`

5. **`NetworkSend`** (`common/network/network_send.rs`)
   - Fields: `destination_id: String`, `send: Box<dyn KafkaSend>`
   - Delegates to inner send
   - Implements `KafkaSend`

### Step 3: Transport layer
6. **`TransportLayer` trait** (`common/network/transport_layer.rs`)
   - `fn ready(&self) -> bool`
   - `async fn finish_connect(&mut self) -> io::Result<bool>`
   - `fn disconnect(&mut self)`
   - `fn is_connected(&self) -> bool`
   - `async fn handshake(&mut self) -> io::Result<()>`
   - `fn add_interest_ops(&mut self, ops: Interest)`
   - `fn remove_interest_ops(&mut self, ops: Interest)`
   - `fn is_mute(&self) -> bool`
   - `fn has_bytes_buffered(&self) -> bool`
   - `async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize>`
   - `async fn write(&mut self, buf: &[u8]) -> io::Result<usize>`

7. **`PlaintextTransportLayer`** (`common/network/plaintext_transport_layer.rs`)
   - Fields: `stream: TcpStream`, `connected: bool`, `interest_ops: Interest`
   - `ready()` → always `true`
   - `handshake()` → no-op
   - `has_bytes_buffered()` → `false`
   - Delegates read/write to `TcpStream`

## Tests to Translate

### From Java
- `NetworkReceiveTest.java` (5 tests): bytes_read tracking, memory amount known states, size calculations
- `ByteBufferSendTest.java`: if exists, send completion and size tracking
- Any `PlaintextTransportLayerTest.java`

### Additional Rust tests
- NetworkReceive: round-trip with mock channel, size validation (negative, overflow), zero-size payload
- ByteBufferSend: single buffer, multiple buffers, size_prefixed helper, partial writes
- PlaintextTransportLayer: connect, read, write with actual TcpStream (integration test)

## Dependencies to Add

```toml
[dependencies]
tokio = { version = "1", features = ["net", "io-util", "rt", "macros"] }
```

## File Structure

```
src/common/
├── network/
│   ├── mod.rs
│   ├── send.rs              # KafkaSend trait
│   ├── receive.rs           # Receive trait
│   ├── byte_buffer_send.rs  # ByteBufferSend
│   ├── network_send.rs      # NetworkSend
│   ├── network_receive.rs   # NetworkReceive
│   ├── transport_layer.rs   # TransportLayer trait
│   └── plaintext_transport_layer.rs  # PlaintextTransportLayer
```

## Definition of Done

Per project rules:
1. All methods from Java classes implemented
2. All corresponding Java tests translated
3. `cargo build`, `cargo test`, `cargo xtask format-check`, `cargo xtask lint` all pass
4. No TODOs or FIXMEs
