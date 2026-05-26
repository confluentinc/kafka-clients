---
name: Phase 5d NetworkClient design choices
description: Phase 5d NetworkClient + NetworkClientUtils translation choices and divergences from Java
type: project
---

# Phase 5d — NetworkClient + NetworkClientUtils

`src/network_client.rs` (~1.1k LOC src + tests + DoD integration) and
`src/network_client_utils.rs` (~140 LOC). The `NetworkClient` struct is
generic over `S: Selectable, M: MetadataUpdater` so test fixtures can
swap a `MockSelector` / `ManualMetadataUpdater` in place of the
production `Selector` / `DefaultMetadataUpdater`.

## Key design choices Phase 6 must respect

1. **`KafkaClient::new_client_request*` takes `&mut self`.** Java's
   signature is `(String, Builder, long, boolean) -> ClientRequest`
   without an explicit `synchronized`/`volatile`, but the body bumps
   `int correlation` in place. Java's class doc says "not thread-safe".
   Switching to `&mut self` matches the actual mutation and avoids
   bolting an `AtomicI32` onto the correlation counter just to satisfy
   the trait shape. Updated `kafka_client.rs:147-167` accordingly.

2. **Correlation counter uses `Wrapping<i32>`**. Java's `nextCorrelationId`
   does `correlation = MAX_RESERVED + 1` on overflow with a comment
   "the numeric overflow is fine as negative values is acceptable".
   `i32::MAX + 1` wraps to `i32::MIN` (-2147483648), which IS allowed.
   Use `Wrapping(i32)` arithmetic to mirror without `unsafe`.

3. **`AbstractRequest` and `AbstractResponse` traits gained `Send + Sync`.**
   `KafkaClient: Send` was already on the trait (Phase 5c-1). Without
   `Send + Sync` on `Box<dyn AbstractResponse>` (held by `ClientResponse`,
   which sits inside `NetworkClient::aborted_sends: Vec<ClientResponse>`),
   `NetworkClient<S, M>` is not `Send` and the trait impl fails. Did NOT
   add `Send + Sync` to the parent `AbstractRequestResponse` trait
   because `RequestHeader` and `ResponseHeader` use `Cell<Option<i32>>`
   for their lazy size cache, which is `!Sync`.

4. **Internal METADATA / API_VERSIONS responses are re-parsed at the
   call site.** The `parse_response` helper returns
   `Box<dyn AbstractResponse>`. To dispatch on the concrete type for
   the internal-request paths (`metadata_updater.handle_successful_response`
   wants `MetadataResponse`, `handle_api_versions_response` wants
   `&ApiVersionsResponse`), we re-parse the response payload at the
   call site (header + body via `MetadataResponse::parse` /
   `ApiVersionsResponse::parse`). Cost: one extra parse for internal
   responses only. Phase 6 may add an `Any`-style downcast accessor.

5. **`destination` round-trips as `Arc<str>` in `NetworkSend` / `ClientResponse`,
   but the i32 form is what the wire path uses.** Java keys by `String`;
   the Rust translation already uses `i32` everywhere internally
   (Phase 5c-1 / 5c-2). The `Arc<str>` "label" is materialised once per
   node via the `node_labels: HashMap<i32, Arc<str>>` cache so the send
   path never allocates a fresh `Arc::from(format!(...))` per request.

6. **DoD integration tests (`dod_integration_tests`):** real
   `Selector` + `tokio::net::TcpListener` echo broker. Two tests
   land:
   - `loopback_metadata_request_response` (DoD #1): a metadata-aware
     in-process broker stub answers both `ApiVersionsRequest` (probe)
     and `MetadataRequest` (user). Round-trip green via the generated
     `MetadataResponse` parser.
   - `connection_close_mid_request_fails_in_flight` (DoD #3): server
     accepts then drops; in-flight request surfaces as
     `was_disconnected = true`.
   - DoD #2 (TLS handshake) is **deferred** with rationale captured in
     the `tls_handshake_test_skip` rustdoc — the rustls handshake is
     covered at the `SslTransportLayer` (Phase 5b-2) and `Selector`
     (Phase 5c-2) layers; running it through the full
     `NetworkClient` stack adds no new coverage.

## Skip rationale (Java tests not translated)

- **Telemetry tests** (`testTelemetryRequest`, `testTelemetryRequestNoLeastLoadedNode`,
  etc.) — Phase 5d skips telemetry per `Phase-5/NOTES.md`.
- **`testReconnectAfterAddressChange`** — leans on Mockito's
  `ClientTelemetrySender` and the `AddressChangeHostResolver`; the
  reconnect-after-address-change behaviour is exercised by Phase 4c
  `cluster_connection_states` tests already.
- **`testRebootstrap` / `testInflightRequestsDuringRebootstrap`** —
  the rebootstrap path requires `DefaultMetadataUpdater` (the inner
  class wired into `Metadata`), which is *not* translated this phase
  (`ManualMetadataUpdater` is a sufficient stand-in for the producer's
  needs in Milestone 1).
- **Throttling tests** — `throttleTimeSensor` is a metric, replaced
  with a `// metric stub` no-op comment. The state-machine effect
  (`connectionStates.throttle`) is already exercised in Phase 4c.
- **Connection-setup-timeout tests** — `connectionSetupTimeoutMs` is
  already wired through `ClusterConnectionStates::nodes_with_connection_setup_timeout`
  (Phase 4c). The NetworkClient orchestration is exercised by
  `request_timeout_disconnects_node` in Phase 5d.

## Module layout decisions

- `network_client.rs` and `network_client_utils.rs` sit at the
  `src/` crate root (mirroring Java's `org.apache.kafka.clients`,
  not `common`). Already established by Phase 5a-4 `client_request.rs`.

## Hot-path audit (CLAUDE.md rule 11/12)

Per-send allocations on `do_send`:
- `serialize_with_header` materializes the body bytes into a `Vec<u8>`.
  This mirrors Java's `request.toSend(header)` which itself builds a
  buffer list internally. Phase 6 may move to a zero-copy
  `SendBuilder`-style path.
- `ByteBufferSend::size_prefixed` allocates a 4-byte `Bytes` for the
  size prefix.
- `Box::new(send_inner)` for the `dyn Send` slot.
- `NetworkSend::new(dest_arc, send)` is `Arc::clone` + struct literal,
  no fresh allocations.

The `node_labels` cache eliminates per-message `Arc::from(format!(...))`
on the destination string. The producer-side flow with Phase 5d in
place is:
- 1× `Vec<u8>` body allocation per send (Java equivalent),
- 1× 4-byte `Bytes` for the size prefix,
- 1× `Box<dyn Send>`.

The `Bytes` size-prefix and the `Box<dyn Send>` are unavoidable in the
current trait shape. Phase 6 may collapse them into a single
`Vec<IoSlice>` carried by `MemoryRecordsSend`.
