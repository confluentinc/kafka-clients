---
name: Phase 4c review patterns
description: Lessons from reviewing the ClientUtils / CommonClientConfigs / ClientDnsLookup translation. Constant-literal correctness, deferred-test verification done right, public-API doc deviations.
type: project
---

Phase 4c translated 5 files: `client_dns_lookup.rs`, `host_resolver.rs`, `default_host_resolver.rs`, `common_client_configs.rs`, `client_utils.rs`. Translation was very clean — only one MINOR finding (Issue 17). Useful patterns surfaced:

1. **Constant-file translations**: when reviewing a file that's mostly `pub const`, the high-yield checks are (a) every config string literal exact byte match (`"bootstrap.servers"`, `"retry.backoff.ms"`, etc.), (b) every default integer value (especially `*_MAX_*` versus `*_MS_*` swaps), and (c) every multi-line `_DOC` string preserving Java's `+`-concatenation byte-for-byte through `concat!`. Java often inlines other constants (`+ DEFAULT_LIST_KEY_SERDE_INNER_CLASS +`) which Rust must inline as the literal value, not the name. Spot-check these expansions explicitly.

2. **Deferred test verification — done right looks like this**: Phase 4b had a "covered elsewhere" pattern that was bogus (Issue 7). Phase 4c had the same shape ("covered by 4 pure-logic tests") and on inspection it was genuine: the per-test note named line ranges, and each named Rust test exercised exactly one branch of the deferred Java assertion. The pattern to verify: (a) does the deferral note name the Rust test by function name, (b) does each Rust test's assertion match a Java assertion 1:1, (c) is the only thing the Java integration test does on top of the pure logic actually `AbstractConfig` plumbing? If yes to all three, accept the deferral.

3. **Reverse-DNS deviation from `std::net`**: `std::net::ToSocketAddrs` does forward DNS (`hostname → IP`) but has no reverse-DNS API. Java uses `InetAddress.getCanonicalHostName()` which does reverse-DNS. The Rust translation must fall back to the IP textual form. Document this on the public-API enum variant (`ClientDnsLookup::ResolveCanonicalBootstrapServersOnly`), not just on internal helpers — users reading the variant rustdoc need to learn about the fallback. SASL-Kerberos SPN computation depends on the real canonical name, so this becomes a real bug if SASL is added without first reintroducing reverse-DNS.

4. **`InetSocketAddress` shim adapter pattern**: Rust's `std::net::SocketAddr` does not preserve hostnames after resolution; Java's `InetSocketAddress` does. When translating Java code that reads `getHostName()` post-resolve, the shim pattern (a Rust struct with `host: String, port: u16, resolved: Option<IpAddr>`) is the right adaptation. Verify it's local to the translating module, not re-exported, until a downstream caller actually needs it.

5. **`@ParameterizedTest` + `@MethodSource` translation**: becomes a `&[Vec<&str>]` (or similar) literal table iterated in a loop. Per CLAUDE.md DoD #3 sub-bullet on `@RepeatedTest`, the loop form is required, not three separate test functions, and not just one of the cases.

6. **Sync `HostResolver` trait in pre-async-IO phase**: when a translated method does blocking IO (DNS resolution) and the wider runtime is not yet using Tokio for that subsystem, keeping the trait synchronous is correct — but the rustdoc must explicitly call out (a) that it is blocking, (b) that calling from async code requires `spawn_blocking`. Phase 5/6 will revisit. Don't flag this as a bug at Phase 4c.

7. **Constant dedupe across phases**: when a Phase-N+1 file becomes the canonical owner of a constant that Phase N had inlined privately, verify with `grep -n CONST_NAME src/<phase-N-file>.rs` that only the `use` line remains (no leftover `pub const`/`const` definition). Two-line dedupe commits like `b64b30a` are easy to verify mechanically.

8. **`KafkaError` variant audit on enum-translation phases**: when reviewing a phase that adds a new public enum (`ClientDnsLookup`) or a new public function that surfaces an error path (`for_config`, `parse_and_validate_addresses`), grep for newly-added `KafkaError::` variants. Reusing existing variants (`Network`, `Config`, `IllegalArgument`) is the right answer; new variants should require justification (no existing variant fits).
