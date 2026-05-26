---
name: Phase 8.0 KafkaProducer::new production wiring
description: How new/with_serializers/from_config compose the production dependency graph
type: project
---

Phase 8.0 (2/N) replaces the `Err(UnsupportedOperation)` stub at
`KafkaProducer::new` / `with_serializers` with real production wiring.

**Why:** The Phase 7 deferred-stub blocked any real-broker integration test.
Phase 8.0 unblocks Phase 8a-f.

**How to apply:** The three constructor layers compose as:

```
new(props)            -> Self::with_serializers(props, K::default_serializer(), V::default_serializer())
with_serializers(...) -> Self::from_config(ProducerConfig::new(props)?, key_ser, value_ser)
from_config(...)      -> builds ProducerMetadata + NetworkClient
                          -> KafkaProducer::new_for_test(...)
```

`from_config` is the new internal lift point — programmatic callers (perf
tests, examples) use it directly without going through HashMap parsing.

**SupportsDefaultSerializer marker trait** fills the role of Java's reflective
FQCN-based serializer lookup. Vec<u8> is the only impl in Milestone-1.
Callers with non-Vec<u8> types use `with_serializers` and supply the
serializer explicitly.

**Two Milestone-1 deviations from Java**, both documented inline:

1. **SSL/SASL rejection:** `from_config` returns
   `KafkaError::UnsupportedOperation` when `security.protocol != PLAINTEXT`.
   Java accepts the config at construction time and only fails at handshake.
   Rationale: Phase 8e (TLS variant) / Phase 9 (SASL) own the
   config-to-SslConfig plumbing. Until then a half-wired path would
   silently swallow auth config.

2. **ApiVersions divergence:** Java shares the same instance between
   `KafkaProducer` and `NetworkClient`. The Rust `NetworkClient::new`
   takes `ApiVersions` by value (Phase 5d translation), while the
   producer holds `Arc<ApiVersions>`. Since the producer side never
   reads it after construction (Sender doesn't either), each owner
   gets its own instance. If a future phase introduces a producer-side
   read path on api_versions, the producer-side instance is the test
   surface and Sender-dispatched RPCs go through the NetworkClient's
   instance.

**Selector construction** uses `Selector::new(connections_max_idle_ms, time,
channel_builder)`. The channel builder comes from
`channel_builders::client_channel_builder(SecurityProtocol::Plaintext, None, None)`
— `PlaintextChannelBuilder` always succeeds. `None` for listener_name and
config is fine because the plaintext builder doesn't read either.

**ApiVersions on NetworkClient**: pass `crate::ApiVersions::new()` directly
since `ApiVersions` is not `Clone`. Don't bother with `Arc::try_unwrap` —
the producer-side `Arc<ApiVersions>` is constructed independently by
`new_for_test` when None is passed for that param.
