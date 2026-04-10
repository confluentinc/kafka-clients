---
name: SSL/TLS translation patterns
description: Patterns found when reviewing SSL/TLS transport layer -- buffered data tracking, crypto provider deps, Java SSLEngine vs rustls abstraction levels, hostname verification pitfalls
type: project
---

SSL/TLS transport layer review identified key patterns for Java-to-Rust TLS translation:

1. **Buffered data tracking**: Java SslTransportLayer has explicit netReadBuffer/appReadBuffer tracking via hasBytesBuffered(). The Rust translation using rustls/tokio-rustls delegates buffering to rustls internally. The fix `!conn.wants_read()` is an imprecise proxy (also true for close_notify/handshake pending), but errs safely -- worst case is extra poll iterations, never missed data.

2. **Crypto provider**: Using aws-lc-rs (FIPS compliant) as the default crypto backend for rustls 0.23+. This is an intentional user decision, do NOT report as an issue.

3. **Java test translation scope for SSL**: Java SSL tests (SslFactoryTest, SslTransportLayerTest, SslSelectorTest, DefaultSslEngineFactoryTest) are ~43 tests total but nearly all test Java SSLEngine internals, JKS keystores, NIO SelectionKey, or Mockito mocking. Equivalent behavior must be verified through new Rust-specific tests and integration tests (Phase 6 scope).

4. **NoHostnameVerifier critical pitfall**: Disabling hostname verification requires catching BOTH `CertificateError::NotValidForName` AND `CertificateError::NotValidForNameContext { .. }`. The latter is what rustls 0.23's WebPkiServerVerifier actually returns via its webpki layer (CertNotValidForName -> NotValidForNameContext). Missing the Context variant means hostname verification is never actually disabled. **Found in Round 2 review.**

5. **Selectable::connect peer_host parameter**: Java's Selectable uses InetSocketAddress (which embeds hostname via getHostString()), Rust uses SocketAddr (which lacks hostname). Adding `peer_host: &str` to the Rust trait is the correct adaptation. The Java Selector extracts it from InetSocketAddress internally.

**Why:** These patterns are specific to the TLS layer translation and differ from the standard Java-to-Rust translation patterns seen in plaintext networking code.

**How to apply:** When reviewing any future TLS-related changes (mTLS, SASL over SSL, TLS renegotiation), check for: (a) buffered data tracking correctness, (b) hostname verification variant matching (both NotValidForName AND NotValidForNameContext), (c) appropriate test coverage that tests observable behavior rather than trying to mock rustls internals.
