---
name: Phase 5b-3 KafkaChannel + ChannelBuilders
description: KafkaChannel/ChannelBuilder trait shape + Authenticator stub design for Phase 5b-3
type: project
---

Phase 5b-3 landed `KafkaChannel`, `Authenticator` (trait + Plaintext/Ssl impls), `ChannelBuilder` trait, `PlaintextChannelBuilder`, `SslChannelBuilder`, and `ChannelBuilders` factory.

**Why:** Phase 5c (Selector) needs a finished `KafkaChannel` API. Phase 5b-3 is the last piece before the Tokio Selector wiring; locking the shape here keeps 5c additive.

**How to apply (esp. for Phase 5c / 9):**

1. **Authenticator stub trait.** Lives at `crate::common::network::authenticator::Authenticator` with 4 methods: `authenticate / principal / complete / close`. Skipped vs Java: SASL methods (`reauthenticate`, `serverSessionExpirationTimeNanos`, `clientSessionReauthenticationTimeNanos`, `pollResponseReceivedDuringReauthentication`, `connectedClientSupportsReauthentication`, `principalSerde`) — all deferred to Phase 9. Phase 9 should add them as default-impl methods on the trait so existing callers don't break.

2. **`PlaintextAuthenticator` returns `KafkaPrincipal::anonymous()` even though Java's client-side throws IllegalStateException.** Java's behavior is server-side-only — client-mode never calls `principal()`. We return anonymous so logging/metrics get a stable value; the divergence is unobservable on the client.

3. **`SslAuthenticator` is stateless; `Authenticator::principal` takes `&dyn TransportLayer`.** Round-1 review revision: the original eager-capture design froze the pre-handshake anonymous principal forever. The fix matches Java's lazy lookup pattern — the trait method receives the transport reference from the owning channel on every call, so SSL re-queries `transport.peer_principal()` post-handshake. Plaintext ignores the argument. See `phase5b3_review_fixes.md` for the broader pattern.

4. **`KafkaChannel` field set vs Java**: dropped `MemoryPool` (Phase 5a's `NetworkReceive` allocates eagerly), `Supplier<Authenticator>` (no client-side reauth in Milestone 1), `successfulAuthentications` counter, `lastReauthenticationStartNanos`, `networkThreadTimeNanos` accumulator. Server-only fields (`MUTED_AND_*` mute states) are KEPT verbatim because the state machine table must match Java byte-for-byte even on the producer.

5. **`mute()`, `maybeUnmute()`, `completeCloseOnAuthenticationFailure()` are `pub(crate)` + `#[allow(dead_code)]`.** Round-1 review revision: Java package-private maps closer to `pub(crate)` than `pub` (the latter widens beyond Java's surface). The `dead_code` lint annotation is honest about the "Phase 5c Selector will wire it up" deferral; widening to `pub` to silence the lint is the wrong fix.

6. **`KafkaChannel::read()` takes the boxed transport via `as_mut()` and wraps it in a stack-allocated `TransportReader<'a>` adapter** (closure-style trait impl inside the function body). This bridges `&mut dyn TransportLayer` → `&mut dyn io::Read` without requiring the `TransportLayer` trait to extend `io::Read` (which would constrain SSL's adapter pattern). No allocation on the read path.

7. **`ChannelBuilder` trait method `build_channel(id, stream, max_receive_size, metadata_registry)`.** Java's `SelectionKey` parameter is replaced with the connected `TcpStream` directly. `MemoryPool` is dropped. The trait carries the *common* shape; SSL needs an SNI server name not on the trait, so `SslChannelBuilder` exposes an additional `build_ssl_channel(..., server_name, ...)` method and the trait method returns `KafkaError::IllegalState` for SSL — clear-error on misuse rather than a silent default.

8. **`ChannelBuilders::client_channel_builder(security_protocol, listener_name, ssl_config)`** — the producer's entry point. SSL requires `Some(Arc<ClientConfig>)`; PLAINTEXT ignores it. SASL_PLAINTEXT/SASL_SSL are NOT in the `SecurityProtocol` enum (Phase 5a deferred them), so the factory function only handles 2 variants. The legacy "SASL string was passed in stringly-typed config" path is handled by `reject_sasl_until_phase_9(value: &str)` which the producer config validator (Phase 5d) will call.

9. **`channel_builder_configs(originals: &HashMap<String, String>, listener_name: Option<&ListenerName>)`** — the listener-prefix override helper. Java's version operates on `AbstractConfig` (typed); we operate on a flat string map. Two divergences from Java are pinned in the test (Round-1 review revision): (a) `plain.sasl.server.callback.handler.class` is **kept** in our impl (Java drops it via typed-field filter); (b) `listener.name.listener1.gssapi.config1.key` is **unwrapped** to `gssapi.config1.key` in our impl (Java keeps the original prefix for non-typed fields). Both are documented in the test as `assert_*` blocks so Phase 9 SASL `ConfigDef` translation will need to flip them.

10. **MockTransport pattern for KafkaChannel tests.** Use `Arc<Mutex<MockState>>` so the test can enqueue canned reads through a handle even after the transport has been moved into the channel. **DO NOT** use `unsafe` raw-pointer downcast — clippy lets it through, but it's a footgun in any future refactor that switches the boxed transport type. The shared-state pattern is the same one Mockito uses behind the scenes (refs to a stub). For partial-write tests (mirroring `when(transport.write(...)).thenReturn(4, 64, 64)`), the `MockState` carries an `Option<usize> max_bytes_per_write` cap that `write_vectored` honors via `cap.saturating_sub(total)`.
