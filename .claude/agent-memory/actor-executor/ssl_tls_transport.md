---
name: SSL/TLS Transport Implementation
description: Key decisions and patterns for the rustls-based SSL/TLS transport layer (Phase 2, Milestone 3)
type: project
---

SSL/TLS transport layer uses rustls (ring backend) + tokio-rustls instead of Java's SSLEngine.

**Why:** Java's SslTransportLayer has complex 3-buffer NEED_WRAP/NEED_UNWRAP state machine because SSLEngine is low-level. rustls handles buffering internally, making the Rust implementation much simpler.

**How to apply:**
- `SslFactory` must call `rustls::crypto::ring::default_provider().install_default()` before building configs
- Test PEM certificates must be real (generated via openssl), not hand-crafted — base64 padding matters
- `TlsStream` doesn't expose `readable()`/`try_read()` like `TcpStream` — use `AsyncReadExt::read()` directly
- `TlsStream<TcpStream>` is ~1104 bytes, must be `Box`ed in enum variants to avoid clippy `large_enum_variant`
- `ChannelBuilder::build_channel` takes `TcpStream + peer_host: &str` (not `Box<dyn TransportLayer>`) so SSL builders can wrap with TLS
- NoHostnameVerifier catches `CertificateError::NotValidForName` and converts to success, delegating all other checks to WebPkiServerVerifier
