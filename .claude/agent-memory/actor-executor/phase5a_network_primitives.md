---
name: Phase 5a network primitives layout
description: Module layout, naming choices, and Java→Rust translation notes for `common::network`, `common::security::auth`, and the crate-root client_* types
type: project
---

Module layout decided in Phase 5a:
- `src/common/network/` — `Send`/`Receive`/`TransferableChannel` traits, `ByteBufferSend`, `NetworkSend`, `NetworkReceive`, `InvalidReceiveError`, `ChannelState`/`ChannelStateName`, `ConnectionMode`, `ListenerName`, `CipherInformation`, `ClientInformation`, `ServerConnectionId`, `ChannelMetadataRegistry`/`DefaultChannelMetadataRegistry`.
- `src/common/security/auth/security_protocol.rs` — `SecurityProtocol` (Plaintext, Ssl only; SASL_PLAINTEXT/SSL deferred to Phase 9 with reserved id constants).
- Crate root: `client_request.rs`, `client_response.rs`, `request_completion_handler.rs` (Java packages `org.apache.kafka.clients.*`).
- `src/common/requests/abstract_request_builder.rs` — added because Phase 5a needs the abstract Builder trait that Phase 4-era code skipped (we tracked `AbstractRequest` only). Trait erases the build target to `Box<dyn AbstractRequest>` so `ClientRequest` can hold a heterogeneous list.

**Why:** Phase 5a sits at the seam between transport-agnostic primitives and the future Selector. Test-only types like `DefaultChannelMetadataRegistry` (Java `kafka/clients/src/test/...`) are translated into `src/` because the trait method set is small enough to live alongside the trait.

**How to apply:** Phase 5b (transport layers + KafkaChannel + Selector) will plug into these traits. The `TransferableChannel` trait needs a real impl (`PlaintextTransportLayer` / `SslTransportLayer`) over `tokio::net::TcpStream`. The `Send`/`Receive` traits' `read_from`/`write_to` use `&mut dyn io::Read` / `&mut dyn TransferableChannel`, deliberately sync — Phase 5b should wrap the Tokio non-blocking reader behind an `io::Read` adapter (or accept the equivalent of a tokio `ReadBuf`) before calling these.

Notable design choices:
- `Send` trait deliberately named `Send` (not `KafkaSend`) per the brief, must be imported qualified to avoid clashing with `std::marker::Send`.
- `NetworkReceive::read_from` no longer treats `Ok(0)` as EOF — Java's NIO `read()==0` is "would block", and the Java tests rely on that semantic. Upper-layer EOF detection lives in Phase 5b's transport.
- `ChannelState` collapses Java's `AuthenticationException` field onto `KafkaError` (specifically the `Authentication` variant carries the message).
- `ServerConnectionId::generate_connection_id` does NOT take a Socket — the Socket-mocked Java tests pass `local_host`/`remote_host` strings directly in the Rust translation (Phase 5b will add a Socket-like wrapper that pulls these from `local_addr`/`peer_addr`).
- `ClientResponse` has both `with_timed_out` (panicking, mirrors Java IllegalStateException) and `try_with_timed_out` (returns `Result<_, KafkaError::IllegalState>`).
- `AbstractRequestBuilder` requires `Send + Sync + Debug` so `ClientRequest` can hold `Arc<dyn AbstractRequestBuilder>` across threads.

Bytes/zero-copy:
- `ByteBufferSend` keeps `Vec<bytes::Bytes>` + per-buffer offsets, uses `IoSlice`-based vectored writes via `TransferableChannel::write_vectored`. `size_prefixed` does NOT pre-concatenate header+payload (CLAUDE.md rule 12).
- `NetworkReceive::take_payload` returns `Bytes::freeze()` for zero-copy hand-off downstream.
- `NetworkSend::destination_id` returns `&str`; `destination_id_arc` returns `Arc<str>` clone (cheap refcount bump).
