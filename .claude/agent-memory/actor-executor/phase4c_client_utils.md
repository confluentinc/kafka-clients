---
name: Phase 4c client utils & common configs
description: Phase 4c decisions — InetSocketAddress shim, sync HostResolver, deferral markers for Phase 5
type: project
---

# Phase 4c — what's load-bearing for Phase 5

## InetSocketAddress is a Rust-only shim (not a Java analogue elsewhere)

Java's `java.net.InetSocketAddress` wraps a hostname + port and tracks a
resolved/unresolved bit. Rust's `std::net::SocketAddr` only stores the
numeric IP, so we keep our own minimal struct in `src/client_utils.rs`
with `host_name()`, `port()`, `is_unresolved()`, `address()` accessors.
Constructors are `new(host, port, IpAddr)` (resolved) and
`create_unresolved(host, port)`. Used only by
`parse_and_validate_addresses` and tests.

**Do not** promote this struct to a crate-wide module. The
`Cluster::bootstrap(&[(String, u16)])` API takes tuples and is
unchanged. If a future caller wants to feed `parseAndValidateAddresses`
results into `Cluster::bootstrap`, project the `InetSocketAddress` to
`(host_name().to_string(), port())` at the boundary.

## HostResolver is sync (Java analogue), called off the hot path

`HostResolver::resolve(&str) -> Result<Vec<IpAddr>, KafkaError>` is
deliberately synchronous. Rationale:

- Java's `InetAddress.getAllByName(...)` is itself blocking.
- Producer only invokes it during bootstrap and metadata refresh.
- `ToSocketAddrs::to_socket_addrs()` is the closest stdlib analogue
  and it is blocking.
- Phase 5 producer code that calls into the resolver from an async
  context wraps the call in `tokio::task::spawn_blocking`.

`UnknownHostException` → `KafkaError::Network(_)` (retriable,
librdkafka `_TRANSPORT`). I considered adding a dedicated
`UnknownHost` variant but `Network` already fits — see Phase 4c
prompt's "Don't add a new KafkaError variant unless none of the
existing ones fit" guidance.

## Deferral markers are file-level rustdoc, not TODO comments

Per CLAUDE.md rule 5 (no TODO/FIXME), Phase-5-deferred Java methods
are listed in the file-level rustdoc of `client_utils.rs` and
`common_client_configs.rs`, **not** as in-body markers. The methods
simply don't exist in the Rust file; Phase 5 will add them.

Items deferred:
- `client_utils.rs`: `createChannelBuilder`, `createNetworkClient` (3
  overloads), `configuredInterceptors`, `configureClusterResourceListeners`
- `common_client_configs.rs`: `metricsReporters` (3 overloads),
  `telemetryReporter`

Test-side deferrals (`testMetricsReporters`, the `AbstractConfig`-
backed parts of `testExponentialBackoffDefaults` and
`testInvalidSaslMechanism`) are documented at the top of the test
module with Java line ranges and the pure-logic Rust tests that cover
the equivalent assertions.

## SASL_MECHANISM is inlined as a string literal

`SaslConfigs.SASL_MECHANISM` is the string `"sasl.mechanism"` —
referenced by `post_validate_sasl_mechanism_config`. The
`SaslConfigs` Java class is Phase 5/6. Rather than create a half-
empty `sasl_configs` module, we inline the literal as a private const
`SASL_MECHANISM_KEY` in `common_client_configs.rs` with a comment
flagging the eventual replacement.

## ClientDnsLookup has 2 variants, not 3

The Phase-4c prompt mentions a `DEFAULT` variant; the Java 4.2 source
only has `USE_ALL_DNS_IPS` and `RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY`.
The `DEFAULT` variant existed in older Kafka but has been removed.
Trust the source.

## DEFAULT_METADATA_RECOVERY_STRATEGY parity is asserted at compile time

Java sets the default to `MetadataRecoveryStrategy.REBOOTSTRAP.name`
(an instance-field reference, not a literal). In Rust we hard-code
`"rebootstrap"` and verify at compile time via a `const _: () = { ... }`
block, plus a runtime test that compares to
`MetadataRecoveryStrategy::Rebootstrap.name()`. Keeps the literal
string in one place and catches drift.

## Canonical-name mode for parseAndValidateAddresses

`RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY` in Java calls
`InetAddress.getCanonicalHostName()` for each resolved IP. The Rust
stdlib has no reverse-DNS API, and Java's behavior falls back to the
IP textual form when reverse-DNS fails. Phase 4c always exhibits the
fallback behavior: each resolved IP becomes a separate
`InetSocketAddress` keyed by the IP-literal as the canonical name.
This is documented in the function's rustdoc. If the producer ever
genuinely needs reverse-DNS resolution we'll add a `dns-lookup` crate
dependency or extend `HostResolver` with a `canonical_names` method.
