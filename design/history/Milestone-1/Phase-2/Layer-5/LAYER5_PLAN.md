# Layer 5: Channel & Selection — Implementation Plan

## 1. Scope and Overview

Layer 5 translates the higher-level network multiplexing classes from `org.apache.kafka.common.network`. These sit atop Layer 4 (transport) and provide the connection management, I/O multiplexing, and channel lifecycle that Layer 6 (NetworkClient) will consume.

**Java classes to translate (in dependency order):**

| # | Java Class | Lines | Rust Module | Complexity |
|---|-----------|-------|-------------|------------|
| 1 | `ChannelState` | 106 | `common::network::channel_state` | Low |
| 2 | `ClientInformation` | 66 | `common::network::client_information` | Low |
| 3 | `CipherInformation` | 62 | `common::network::cipher_information` | Low |
| 4 | `ChannelMetadataRegistry` | 56 | `common::network::channel_metadata_registry` | Low |
| 5 | `Authenticator` (interface) | 167 | `common::network::authenticator` | Medium |
| 6 | `KafkaChannel` | 695 | `common::network::kafka_channel` | High |
| 7 | `ChannelBuilder` (interface) | 50 | `common::network::channel_builder` | Low |
| 8 | `PlaintextChannelBuilder` | 122 | `common::network::plaintext_channel_builder` | Medium |
| 9 | `Selectable` (interface) | 130 | `common::network::selectable` | Medium |
| 10 | `Selector` | 1482 | `common::network::selector` | Very High |

**Supporting types needed:**
- `MemoryPool` trait (from `o.a.k.common.memory.MemoryPool`) — stubbed as `NoopMemoryPool`
- `ListenerName` (from `o.a.k.common.network.ListenerName`) — simple wrapper
- `ConnectionMode` enum — trivial, needed by future SSL/SASL builders

## 2. Implementation Order and Details

### Step 1: Supporting Value Types (Low complexity, no dependencies)

#### 1a. `ChannelState` — `src/common/network/channel_state.rs`

- Inner enum `State` with 7 variants: `NotConnected`, `Authenticate`, `Ready`, `Expired`, `FailedSend`, `AuthenticationFailed`, `LocalClose`
- Struct `ChannelState` with fields: `state: State`, `exception: Option<String>`, `remote_address: Option<String>`
- Static constants become constructor functions like `ChannelState::not_connected()`

#### 1b. `ClientInformation` — `src/common/network/client_information.rs`

Simple struct with `software_name: String` and `software_version: String`, plus `UNKNOWN_NAME_OR_VERSION` and `EMPTY` constants.

#### 1c. `CipherInformation` — `src/common/network/cipher_information.rs`

Simple struct with `cipher: String` and `protocol: String`.

#### 1d. `ChannelMetadataRegistry` — `src/common/network/channel_metadata_registry.rs`

Trait with `register_cipher_information`, `cipher_information`, `register_client_information`, `client_information`, `close`. Default implementation: `DefaultChannelMetadataRegistry` that simply stores values (metrics deferred).

#### 1e. `ListenerName` — `src/common/network/listener_name.rs`

Newtype wrapper around `String`. Methods: `value()`, `config_prefix()`, `for_security_protocol()`, `normalised()`.

#### 1f. `ConnectionMode` — `src/common/network/connection_mode.rs`

Trivial enum: `Client`, `Server`.

#### 1g. `MemoryPool` — `src/common/memory/memory_pool.rs`

Trait with `try_allocate`, `release`, `size`, `available_memory`, `is_out_of_memory`. `NoopMemoryPool` struct always allocates from the heap. New module: `src/common/memory/`.

### Step 2: Authenticator trait

#### 2a. `Authenticator` — `src/common/network/authenticator.rs`

Trait translated from Java interface. For PLAINTEXT: `authenticate()` is a no-op, `complete()` always returns `true`. Re-authentication methods get default no-op implementations. `PlaintextAuthenticator` struct provides the trivial implementation.

### Step 3: KafkaChannel

#### 3a. `KafkaChannel` — `src/common/network/kafka_channel.rs`

**Key fields:**
```rust
id: String,
transport_layer: Box<dyn TransportLayer>,
authenticator: Box<dyn Authenticator>,
max_receive_size: i32,
receive: Option<NetworkReceive>,
send: Option<NetworkSend>,
disconnected: bool,
mute_state: ChannelMuteState,
state: ChannelState,
remote_address: Option<SocketAddr>,
successful_authentications: u32,
mid_write: bool,
network_thread_time_nanos: u64,
```

**Key methods:** `close()`, `prepare()`, `disconnect()`, `finish_connect()`, `ready()`, `set_send()`, `maybe_complete_send()`, `read()`, `write()`, `maybe_complete_receive()`, `mute()`, `maybe_unmute()`, `handle_channel_mute_event()`.

**Rust-specific adaptations:**
- `read()`, `write()`, `prepare()`, `finish_connect()`, `close()` become `async fn`
- `ChannelMuteState` and `ChannelMuteEvent` as nested enums with state machine via `match`
- `selectionKey()` eliminated — Selector uses `HashMap<String, KafkaChannel>` keyed by ID
- Add `peer_addr()` to `TransportLayer` trait

### Step 4: ChannelBuilder + PlaintextChannelBuilder

#### 4a. `ChannelBuilder` — `src/common/network/channel_builder.rs`

Trait with:
- `fn build_channel(id, transport_layer, max_receive_size) -> Result<KafkaChannel, io::Error>`
- `fn close(&mut self)`

Takes `Box<dyn TransportLayer>` instead of Java's `SelectionKey`.

#### 4b. `PlaintextChannelBuilder` — `src/common/network/plaintext_channel_builder.rs`

Creates `PlaintextTransportLayer` + `PlaintextAuthenticator` → `KafkaChannel`.

### Step 5: Selectable + Selector

#### 5a. `Selectable` — `src/common/network/selectable.rs`

Trait with: `connect`, `wakeup`, `close`, `send`, `poll`, `completed_sends`, `completed_receives`, `disconnected`, `connected`, `mute`, `unmute`, `mute_all`, `unmute_all`, `is_channel_ready`.

#### 5b. `Selector` — `src/common/network/selector.rs`

**NIO → Tokio mapping:**

Per CLAUDE.md rule 8: single Selector for multiple TCP connections. The `poll()` method:
1. Iterates all channels, attempts non-blocking I/O (connect/read/write) using `try_read`/`try_write`
2. If no progress and timeout > 0, uses `tokio::time::sleep` + `tokio::sync::Notify` for wakeup
3. Matches Java's sequential iteration over `selectedKeys`

**Key fields:**
```rust
channels: HashMap<String, KafkaChannel>,
explicitly_muted_channels: HashSet<String>,
completed_sends: Vec<NetworkSend>,
completed_receives: LinkedHashMap<String, NetworkReceive>,
immediately_connected: HashSet<String>,
closing_channels: HashMap<String, KafkaChannel>,
disconnected: HashMap<String, ChannelState>,
connected: Vec<String>,
failed_sends: Vec<String>,
channel_builder: Box<dyn ChannelBuilder>,
max_receive_size: i32,
idle_expiry_manager: Option<IdleExpiryManager>,
notify: Arc<Notify>,  // for wakeup()
```

**Key methods:**
- `async fn connect(id, address, send_buffer_size, receive_buffer_size)` — Creates `TcpStream`, wraps in transport layer, builds channel
- `fn send(send)` — Queues send on target channel
- `async fn poll(timeout_ms)` — Core I/O loop: clear results, process channels, handle idle expiry
- `fn close_channel(id)`, `fn close()` — Channel/selector lifecycle
- `fn wakeup()` — `self.notify.notify_one()`

**Inner types:**
- `IdleExpiryManager` — Uses `indexmap::IndexMap` for LRU tracking
- `CloseMode` enum — `Graceful`, `NotifyOnly`, `DiscardNoNotify`
- Metrics: deferred (no-op stubs)

## 3. Dependency Graph

```
Step 1 (parallel, no interdependencies):
  ChannelState, ClientInformation, CipherInformation,
  ChannelMetadataRegistry, ListenerName, ConnectionMode, MemoryPool

Step 2 (depends on Step 1):
  Authenticator trait + PlaintextAuthenticator

Step 3 (depends on Steps 1-2 + Layer 4):
  KafkaChannel + peer_addr() on TransportLayer

Step 4 (depends on Step 3):
  ChannelBuilder trait + PlaintextChannelBuilder

Step 5 (depends on all above):
  Selectable trait + Selector
```

## 4. Tests to Translate

### KafkaChannelTest (2 tests)
From `kafka/clients/src/test/java/org/apache/kafka/common/network/KafkaChannelTest.java`:
- `test_sending()` — write, maybeCompleteSend, hasSend, duplicate send error
- `test_receiving()` — read, currentReceive, bytesRead, maybeCompleteReceive

Use `MockTransportLayer` pattern from Layer 4.

### SelectorTest (~13 priority tests)
From `kafka/clients/src/test/java/org/apache/kafka/common/network/SelectorTest.java`:

**Must translate:**
1. `test_server_disconnect` — disconnect detection
2. `test_cant_send_with_in_progress` — duplicate send error
3. `test_send_without_connecting` — error on unknown channel
4. `test_no_route_to_host` — DNS failure
5. `test_connection_refused` — connect to non-listening port
6. `test_normal_operation` — multi-connection send/receive
7. `test_send_large_request` — large message round-trip
8. `test_empty_request` — zero-length message
9. `test_existing_connection_id` — duplicate ID error
10. `test_mute` — mute/unmute correctness
11. `test_close_all_channels` — close behavior
12. `test_close_oldest_connection` — idle expiry
13. `test_immediately_connected_cleaned` — immediate connect path

**Deferred (metrics/server-side):**
- Metrics-asserting tests (require Metrics infrastructure)
- `testMuteOnOOM` (requires SimpleMemoryPool)
- Mockito-heavy tests

### Test Infrastructure: EchoServer
TCP server test fixture using `tokio::net::TcpListener`:
1. Accept connections, read 4-byte big-endian size + payload
2. Echo size + payload back
3. Support `close_connections()` to force-close clients

## 5. New Dependencies

### Cargo.toml
```toml
tokio = { version = "1", features = ["net", "io-util", "rt", "macros", "time", "sync"] }
indexmap = "2"  # For IdleExpiryManager LRU tracking
```

### New Modules
```
src/common/
  memory/
    mod.rs
    memory_pool.rs
  network/
    authenticator.rs
    channel_builder.rs
    channel_metadata_registry.rs
    channel_state.rs
    cipher_information.rs
    client_information.rs
    connection_mode.rs
    kafka_channel.rs
    listener_name.rs
    plaintext_channel_builder.rs
    selectable.rs
    selector.rs
```

### Modifications to Existing Files
- `src/common/mod.rs` — add `pub mod memory;`
- `src/common/network/mod.rs` — add all new submodules
- `src/common/network/transport_layer.rs` — add `fn peer_addr()` method
- `src/common/network/plaintext_transport_layer.rs` — implement `peer_addr()`

## 6. Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| NIO Selector mapping | Sequential try_read/try_write + sleep | Matches Java's single-threaded model |
| Metrics | Deferred (no-op stubs) | Metrics subsystem not yet translated |
| MemoryPool | NoopMemoryPool only | Client PLAINTEXT path doesn't need pooling |
| Re-authentication | Structural stubs, no-op | Only for SASL; preserves API shape |
| SelectionKey | Eliminated | Channel lookup by ID in HashMap |
| Wakeup | `tokio::sync::Notify` | Async-native alternative to `nioSelector.wakeup()` |
| ChannelBuilder.build_channel | Takes `Box<dyn TransportLayer>` | Selector creates transport, builder wraps it |

## 7. Challenges

1. **Selector.poll() async multiplexing** — Sequential try_read/try_write avoids FuturesUnordered complexity
2. **Ownership in Selector** — Careful borrow management with HashMap channels during poll
3. **Async KafkaChannel methods** — Sequential processing within poll() matches Java
4. **Metrics deferral** — ~200 lines of Java metrics code deferred; some tests untranslatable yet
5. **Test infrastructure** — EchoServer + NetworkTestUtils must be built as Rust fixtures
