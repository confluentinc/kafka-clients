---
name: from_config factory method patterns
description: Config wiring bugs in KafkaProducer::from_config — hardcoded defaults that should use config values, missing validation steps from Java constructor
type: project
---

When translating Java's multi-step constructors into Rust factory methods, watch for:

1. **Hardcoded constants replacing config values**: The Selector was created with `NO_IDLE_TIMEOUT_MS` instead of `config.connections_max_idle_ms`. Java's `createNetworkClient` pulls each value from config; the Rust translation shortcut hardcoded some.

2. **Missing validation steps**: Java's `configureDeliveryTimeout` validates `delivery.timeout.ms >= linger.ms + request.timeout.ms` and either throws or auto-adjusts. The Rust factory method skipped this entirely.

3. **Config coupling**: `rebootstrap_trigger_ms` was set to `metadata_max_age_ms` instead of having its own config field. While defaults match, the values are semantically independent in Java.

4. **URL parsing differences**: Java uses `getHost`/`getPort` with a regex that accepts optional scheme prefixes (`PLAINTEXT://host:port`). Rust uses `to_socket_addrs()` which only handles `host:port`. Java also explicitly validates and provides specific error messages for missing port, invalid port, etc.

5. **SocketAddr vs InetSocketAddress**: Java's `InetSocketAddress` preserves the original hostname; Rust's `SocketAddr` only stores the resolved IP address. This affects logging and debugging.

**Why:** Factory methods that wire up multiple components are translation-intensive and prone to subtle misses where Java reads a config value but Rust uses a constant or wrong config field.

**How to apply:** When reviewing factory/constructor methods, diff every parameter passed to sub-component constructors against the Java source's corresponding `config.get*()` calls.
