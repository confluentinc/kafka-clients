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

3. **`SslAuthenticator::new` captures the principal eagerly** at construction (not lazily on `principal()`). Since the channel builder constructs the authenticator immediately after the transport, the principal is anonymous (handshake hasn't run) — that's the same as Java's `peerPrincipal` throwing pre-handshake. Callers must invoke `principal()` post-handshake.

4. **`KafkaChannel` field set vs Java**: dropped `MemoryPool` (Phase 5a's `NetworkReceive` allocates eagerly), `Supplier<Authenticator>` (no client-side reauth in Milestone 1), `successfulAuthentications` counter, `lastReauthenticationStartNanos`, `networkThreadTimeNanos` accumulator. Server-only fields (`MUTED_AND_*` mute states) are KEPT verbatim because the state machine table must match Java byte-for-byte even on the producer.

5. **`mute()`, `maybeUnmute()`, `completeCloseOnAuthenticationFailure()` are `pub`, NOT `pub(crate)`.** Java-package-private maps to "same `crate::common::network` module" in our layout — Phase 5c `Selector` is a sibling. `pub(crate)` triggers `dead_code` on the lib build because no in-crate caller exists yet. Documenting "exposed as `pub` because Selector is sibling" is the right pattern; don't paper over with `#[allow(dead_code)]`.

6. **`KafkaChannel::read()` takes the boxed transport via `as_mut()` and wraps it in a stack-allocated `TransportReader<'a>` adapter** (closure-style trait impl inside the function body). This bridges `&mut dyn TransportLayer` → `&mut dyn io::Read` without requiring the `TransportLayer` trait to extend `io::Read` (which would constrain SSL's adapter pattern). No allocation on the read path.

7. **`ChannelBuilder` trait method `build_channel(id, stream, max_receive_size, metadata_registry)`.** Java's `SelectionKey` parameter is replaced with the connected `TcpStream` directly. `MemoryPool` is dropped. The trait carries the *common* shape; SSL needs an SNI server name not on the trait, so `SslChannelBuilder` exposes an additional `build_ssl_channel(..., server_name, ...)` method and the trait method returns `KafkaError::IllegalState` for SSL — clear-error on misuse rather than a silent default.

8. **`ChannelBuilders::client_channel_builder(security_protocol, listener_name, ssl_config)`** — the producer's entry point. SSL requires `Some(Arc<ClientConfig>)`; PLAINTEXT ignores it. SASL_PLAINTEXT/SASL_SSL are NOT in the `SecurityProtocol` enum (Phase 5a deferred them), so the factory function only handles 2 variants. The legacy "SASL string was passed in stringly-typed config" path is handled by `reject_sasl_until_phase_9(value: &str)` which the producer config validator (Phase 5d) will call.

9. **`channel_builder_configs(originals: &HashMap<String, String>, listener_name: Option<&ListenerName>)`** — the listener-prefix override helper. Java's version operates on `AbstractConfig` (typed); we operate on a flat string map. The listener-prefix unwrapping logic is reproduced byte-for-byte; the Java-test assertion about `plain.sasl.server.callback.handler.class` being filtered out is **driven by Java's typed `AbstractConfig.values()` schema, not by the listener-prefix logic** — we don't have AbstractConfig and the test docstring documents this divergence explicitly. The Java-test assertions about gssapi.*, sasl.kerberos.*, custom.config2 (the listener-prefix-driven facts) are mirrored byte-for-byte.

10. **MockTransport pattern for KafkaChannel tests.** Use `Arc<Mutex<MockState>>` so the test can enqueue canned reads through a handle even after the transport has been moved into the channel. **DO NOT** use `unsafe` raw-pointer downcast — clippy lets it through, but it's a footgun in any future refactor that switches the boxed transport type. The shared-state pattern is the same one Mockito uses behind the scenes (refs to a stub).
