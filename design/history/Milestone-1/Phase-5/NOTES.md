# Phase 5 — Network Layer (Manager plan)

Phase 5 translates the Java `common/network/` package and the
`clients/` networking spine (`NetworkClient`, `InFlightRequests`,
`ClusterConnectionStates`, `ApiVersions`) into a Tokio-based
implementation. See `design/history/Milestone-1/PLAN.md` lines
246–282 for the goal.

## Sub-phase split

Phase 5 is ~6,500 lines of Java production + ~5,500 lines of tests —
roughly double Phase 4. Splitting into 4 Actor/Critic rounds keeps
each commit reviewable.

### Phase 5a — Network primitives & framing (~750 LOC)

**Java sources:**
- `common/network/Send.java` (interface), `ByteBufferSend.java` (84),
  `NetworkSend.java` (53), `Receive.java` (55), `NetworkReceive.java` (154),
  `TransferableChannel.java` (51), `InvalidReceiveException.java` (~10)
- `common/network/ChannelState.java` (105), `ConnectionMode.java` (~15),
  `ListenerName.java` (87)
- `common/network/CipherInformation.java` (61), `ClientInformation.java` (65),
  `ServerConnectionId.java` (145), `ChannelMetadataRegistry.java` (55)
- `common/security/auth/SecurityProtocol.java` (75) — PLAINTEXT, SSL only
- `clients/ClientRequest.java` (119), `ClientResponse.java` (173),
  `RequestCompletionHandler.java` (26)

**Tests:** `ServerConnectionIdTest` (209)

**Why first:** No Tokio yet. These are mostly value types + framing logic
(length-prefix read/write helpers) that everything else depends on.

### Phase 5b — Transport, KafkaChannel, ChannelBuilders (~2.5k LOC)

**Java sources:**
- `common/network/TransportLayer.java` (93), `PlaintextTransportLayer.java` (216),
  `SslTransportLayer.java` (1,063)
- `common/network/KafkaChannel.java` (694)
- `common/network/ChannelBuilder.java` (49), `ChannelBuilders.java` (240),
  `PlaintextChannelBuilder.java` (121), `SslChannelBuilder.java` (183)

**Tests:** `ChannelBuildersTest` (132), `SslTransportLayerTest` subset

**TLS architecture (CRITICAL — saved as memory `feedback_tls_rustls_low_level`):**
Use raw `rustls::ClientConnection` directly. Do NOT use `tokio_rustls::TlsStream`.

The Java `SslTransportLayer` calls `SSLEngine.wrap()` / `unwrap()` which
processes bytes between in-memory buffers, decoupling crypto from network
I/O. `tokio_rustls::TlsStream` couples them on a single task and breaks
the architectural mirror. Rust's `rustls::ClientConnection` exposes
`read_tls` / `write_tls` / `process_new_packets` / `reader` / `writer` —
the exact equivalent of `SSLEngine`. The Rust `SslTransportLayer` should
hold a `rustls::ClientConnection` and drive the underlying
`tokio::net::TcpStream` separately.

**This supersedes PLAN.md line 260's mention of `tokio_rustls::client::TlsStream`** —
update PLAN.md when reaching this sub-phase.

**Dependencies to add (require user approval):** `rustls`, `webpki-roots`,
`rustls-pemfile` (cert loading). `rcgen` already in dev-deps.

### Phase 5c — Selector + connection state + ApiVersions (~2.5k LOC)

**Java sources:**
- `common/network/Selectable.java` (129) — trait
- `common/network/Selector.java` (1,481) — Tokio rewrite per PLAN.md 258–264
- `clients/InFlightRequests.java` (186), `ClusterConnectionStates.java` (567),
  `ConnectionState.java` (~30 inner enum), `LeastLoadedNode.java` (43)
- `clients/ApiVersions.java` (69), `NodeApiVersions.java` (268)
- `clients/MetadataUpdater.java` (110), `ManualMetadataUpdater.java` (87)
- `clients/KafkaClient.java` (216) — trait

**Tests:** `SelectorTest` subset (connect, send, receive, disconnect,
multi-connection, idle expiry), `InFlightRequestsTest` (129),
`ClusterConnectionStatesTest` (469), `ApiVersionsTest` (66),
`NodeApiVersionsTest` (196)

**Tokio Selector pattern (CLAUDE.md rule 9.6 + PLAN.md 263):**
- One read task per `KafkaChannel` (length-prefix framed reads).
- Write side: `mpsc::UnboundedSender<NetworkSend>` to per-channel write task.
- Never share a `tokio::select!` arm with state mutations.
- Never hold a `MutexGuard` across `.await`.
- Vectored I/O for sends: `write_vectored` so framing header + payload
  are not concatenated (CLAUDE.md rule 12).

### Phase 5d — NetworkClient + DoD integration tests (~1.7k LOC)

**Java sources:**
- `clients/NetworkClient.java` (1,607)
- `clients/NetworkClientUtils.java` (154)

**Tests:** `NetworkClientTest` producer-relevant subset
(connect-before-send, request-correlation, timeout-on-send,
version-negotiation handoff)

**DoD additions (PLAN.md 279–282):**
- Loopback test: send a `MetadataRequest` to an in-process echo server
  and decode the response via the generated `MetadataResponse` type —
  round-trip green.
- TLS handshake test against a self-signed `rcgen` broker.
- A connection-close-mid-request test asserts the in-flight request is
  failed with a `DisconnectError`-equivalent.

## Module layout

Per CLAUDE.md naming rules:

```
src/
  common/
    network/
      send.rs                 // trait
      byte_buffer_send.rs
      network_send.rs
      receive.rs              // trait
      network_receive.rs
      transferable_channel.rs // trait
      invalid_receive_error.rs
      channel_state.rs
      connection_mode.rs
      listener_name.rs
      cipher_information.rs
      client_information.rs
      server_connection_id.rs
      channel_metadata_registry.rs
      transport_layer.rs              // 5b: trait
      plaintext_transport_layer.rs    // 5b
      ssl_transport_layer.rs          // 5b: rustls::ClientConnection
      kafka_channel.rs                // 5b
      channel_builder.rs              // 5b: trait
      channel_builders.rs             // 5b
      plaintext_channel_builder.rs    // 5b
      ssl_channel_builder.rs          // 5b
      selectable.rs                   // 5c: trait
      selector.rs                     // 5c: Tokio rewrite
    security/
      auth/
        security_protocol.rs
  client_request.rs           // org.apache.kafka.clients.ClientRequest
  client_response.rs
  request_completion_handler.rs       // trait
  in_flight_requests.rs               // 5c
  cluster_connection_states.rs        // 5c
  connection_state.rs                 // 5c
  least_loaded_node.rs                // 5c
  api_versions.rs                     // 5c
  node_api_versions.rs                // 5c
  metadata_updater.rs                 // 5c: trait
  manual_metadata_updater.rs          // 5c
  kafka_client.rs                     // 5c: trait
  network_client.rs                   // 5d
  network_client_utils.rs             // 5d
```

## Skip / defer notes

Per PLAN.md lines 266–268:
- `SaslChannelBuilder`, `Authenticator`, `PlaintextAuthenticator`,
  `SaslClientAuthenticator`, `ReauthenticationContext`,
  `DelayedResponseAuthenticationException` — **skip** (Phase 9).
- All Kerberos / OAUTHBEARER / SCRAM under `common/security/` — **skip**.
- `SecurityProtocol.SASL_PLAINTEXT` and `SASL_SSL` enum variants — **defer to
  Phase 9.** Translate the enum with PLAINTEXT and SSL only this phase.
  Producer config validation will reject SASL_* with `ConfigError` until
  Phase 9 lands them.

## Java equivalence guards

- `Selector::poll()` in Java is blocking with a millisecond timeout. Rust
  signature must be `async fn poll(&mut self, timeout_ms: i64) -> Vec<...>`
  driven by a `tokio::time::sleep` racing channel events.
- `KafkaChannel::send()` enqueues a `Send` and returns immediately — the
  actual write happens on the next `poll` tick. The Rust translation
  must preserve this asynchrony (do not eagerly write inside `send`).
- `InFlightRequests` is a per-node `Deque<NetworkClient.InFlightRequest>`
  in Java. Rust uses `HashMap<i32, VecDeque<InFlightRequest>>`. Order is
  oldest→newest at the back; `completeNext()` pops from the front.
- `ClusterConnectionStates` uses `ExponentialBackoff` for reconnect /
  connectionSetupTimeout — already translated in Phase 4c. Re-use it.
- `NetworkClient` request correlation uses a monotonically increasing
  `int correlationId`. Use `i32` (signed) to match wire protocol.

## Hot-path identifier interning (CLAUDE.md rule 11)

`NetworkClient` indexes `InFlightRequests` by node id (`String` in Java).
Use `i32` node id everywhere — the Java string is a legacy artifact of
the old `Selector` interface and the producer code path always converts
from `Node.id() (int)` anyway. This avoids per-request `String` clone.
